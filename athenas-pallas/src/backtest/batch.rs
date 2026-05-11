//! Parallel replay of multiple synthetic or recorded scenarios.

use crate::engine::dispatch_event;
use crate::error::Result;
use crate::events::{Event, MarketEvent};
use crate::execution::ExecutionGateway;
use crate::metrics::{summarize, PerformanceSummary};
use crate::risk::RiskPipeline;
use crate::state::GlobalState;
use crate::strategy::Strategy;
use crate::types::{EquityPoint, InstrumentId};
use std::sync::Arc;
use time::OffsetDateTime;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

/// Named replay bundle.
#[derive(Clone, Debug)]
pub struct Scenario {
    /// Label for reporting.
    pub name: String,
    /// Deterministic event stream.
    pub events: Vec<Event>,
    /// Optional RNG seed for strategies that use one (reporting only).
    pub seed: Option<u64>,
}

/// Aggregated performance for one scenario run.
#[derive(Clone, Debug)]
pub struct RunReport {
    /// Scenario name.
    pub name: String,
    /// Metrics summary from mark-to-market equity samples.
    pub summary: PerformanceSummary,
    /// Echo of [`Scenario::seed`] when provided.
    pub seed: Option<u64>,
}

fn equity_ts(ev: &Event) -> OffsetDateTime {
    match ev {
        Event::Market(MarketEvent::Trade { ts, .. }) => *ts,
        Event::Market(MarketEvent::BookL1 { ts, .. }) => *ts,
        Event::Market(MarketEvent::BookL2Snapshot(s)) => s.ts,
        Event::Timer(t) => t.ts,
        _ => OffsetDateTime::now_utc(),
    }
}

/// Run many scenarios concurrently with a bounded worker pool.
///
/// `build` constructs fresh `(GlobalState, S)` per scenario; `equity_instrument` selects which
/// pair [`GlobalState::mark_to_market_equity`] uses for the equity curve.
pub async fn run_scenarios_parallel<S, E, B>(
    scenarios: Vec<Scenario>,
    concurrency: usize,
    build: Arc<B>,
    risk: Arc<RiskPipeline>,
    exec: Arc<E>,
    equity_instrument: InstrumentId,
    periods_per_year: f64,
) -> Vec<RunReport>
where
    B: Fn(&Scenario) -> (GlobalState, S) + Send + Sync + 'static,
    S: Strategy + Send + 'static,
    E: ExecutionGateway + Send + Sync + 'static,
{
    let sem = Arc::new(Semaphore::new(concurrency.max(1)));
    let mut set = JoinSet::new();
    for sc in scenarios {
        let permit = sem.clone().acquire_owned().await.unwrap_or_else(|_| {
            panic!("semaphore closed");
        });
        let build = build.clone();
        let risk = risk.clone();
        let exec = exec.clone();
        let eq_inst = equity_instrument.clone();
        set.spawn(async move {
            let _p = permit;
            let (mut state, mut strat) = build(&sc);
            let mut curve: Vec<EquityPoint> = Vec::new();
            for ev in sc.events {
                let ts = equity_ts(&ev);
                let _ = dispatch_event(&mut state, &mut strat, &risk, exec.as_ref(), ev).await;
                if let Some(eq) = state.mark_to_market_equity(&eq_inst) {
                    curve.push(EquityPoint {
                        ts,
                        equity_quote: eq,
                    });
                }
            }
            let summary = summarize(curve, periods_per_year);
            RunReport {
                name: sc.name,
                summary,
                seed: sc.seed,
            }
        });
    }
    let mut out = Vec::new();
    while let Some(res) = set.join_next().await {
        if let Ok(r) = res {
            out.push(r);
        }
    }
    out.sort_by(|a, b| a.name.cmp(&b.name));
    out
}

/// Deterministic single-threaded batch (preserves event order within each scenario).
pub async fn run_scenarios_serial<S, E, B>(
    scenarios: Vec<Scenario>,
    build: &B,
    risk: &RiskPipeline,
    exec: &E,
    equity_instrument: InstrumentId,
    periods_per_year: f64,
) -> Result<Vec<RunReport>>
where
    B: Fn(&Scenario) -> (GlobalState, S),
    S: Strategy,
    E: ExecutionGateway,
{
    let mut out = Vec::new();
    for sc in scenarios {
        let (mut state, mut strat) = build(&sc);
        let mut curve: Vec<EquityPoint> = Vec::new();
        for ev in sc.events {
            let ts = equity_ts(&ev);
            dispatch_event(&mut state, &mut strat, risk, exec, ev).await?;
            if let Some(eq) = state.mark_to_market_equity(&equity_instrument) {
                curve.push(EquityPoint {
                    ts,
                    equity_quote: eq,
                });
            }
        }
        let summary = summarize(curve, periods_per_year);
        out.push(RunReport {
            name: sc.name,
            summary,
            seed: sc.seed,
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::OrderIntent;
    use crate::execution::{PaperConfig, SimGateway};
    use crate::risk::PauseCheck;
    use crate::state::InstrumentMeta;
    use crate::strategy::{Strategy, StrategyContext};
    use crate::types::Asset;
    use rust_decimal::Decimal;
    use std::collections::HashMap;

    struct Noop;

    impl Strategy for Noop {
        fn on_event(&mut self, _ctx: &StrategyContext<'_>, _event: &Event) -> Vec<OrderIntent> {
            vec![]
        }
    }

    fn mk_state(inst: &InstrumentId) -> GlobalState {
        let mut instruments = HashMap::new();
        instruments.insert(
            inst.clone(),
            InstrumentMeta {
                base: Asset("BTC".into()),
                quote: Asset("USDT".into()),
            },
        );
        let mut balances = HashMap::new();
        balances.insert(Asset("USDT".into()), Decimal::new(10_000, 0));
        balances.insert(Asset("BTC".into()), Decimal::ZERO);
        GlobalState::new(instruments, balances)
    }

    fn mk_events(inst: InstrumentId, ts: OffsetDateTime) -> Vec<Event> {
        vec![Event::Market(MarketEvent::BookL1 {
            instrument: inst,
            ts,
            bid: Decimal::new(40_000, 0),
            ask: Decimal::new(40_010, 0),
        })]
    }

    #[tokio::test]
    async fn parallel_three_scenarios_sim() {
        let inst = InstrumentId::new("binance", "BTCUSDT");
        let t0 = OffsetDateTime::UNIX_EPOCH;
        let inst_for_build = inst.clone();
        let scenarios = vec![
            Scenario {
                name: "a".into(),
                events: mk_events(inst.clone(), t0),
                seed: Some(1),
            },
            Scenario {
                name: "b".into(),
                events: mk_events(inst.clone(), t0),
                seed: Some(2),
            },
            Scenario {
                name: "c".into(),
                events: mk_events(inst.clone(), t0),
                seed: Some(3),
            },
        ];
        let build = Arc::new(move |_: &Scenario| (mk_state(&inst_for_build), Noop));
        let risk = Arc::new(RiskPipeline::new(vec![Box::new(PauseCheck::default())]));
        let exec = Arc::new(SimGateway::new(PaperConfig::default()));
        let reports = run_scenarios_parallel(
            scenarios,
            2,
            build,
            risk,
            exec,
            inst.clone(),
            252.0,
        )
        .await;
        assert_eq!(reports.len(), 3);
        assert!(reports.iter().all(|r| r.summary.equity.len() >= 1));
    }
}
