//! Engine: single consumer loop — market → (passive fills) → strategy → risk → execution.

use crate::error::{Error, Result};
use crate::events::{ControlEvent, Event, OrderIntent, OrderIntentSource, TimerEvent};
use crate::execution::ExecutionGateway;
use crate::risk::RiskPipeline;
use crate::state::GlobalState;
use crate::strategy::{Strategy, StrategyContext};
use crate::types::{OrderType, Side};
use rust_decimal::Decimal;
use std::sync::Arc;
use std::time::Duration;
use time::OffsetDateTime;
use tokio::sync::mpsc;
use tracing::{error, warn};

/// Handle to inject events from connectors or control plane.
#[derive(Clone)]
pub struct EngineHandle {
    tx: mpsc::Sender<Event>,
}

impl EngineHandle {
    /// Clone the underlying sender (for connectors / control fan-in).
    pub fn sender(&self) -> mpsc::Sender<Event> {
        self.tx.clone()
    }

    /// Send one event into the engine (non-blocking).
    pub fn try_send(&self, event: Event) -> Result<()> {
        self.tx.try_send(event).map_err(|_| Error::EngineShutdown)
    }

    /// Async send.
    pub async fn send(&self, event: Event) -> Result<()> {
        self.tx.send(event).await.map_err(|_| Error::EngineShutdown)
    }
}

/// Wall-clock timer schedule injected by [`EngineBuilder::spawn`].
#[derive(Clone, Debug)]
pub struct TimerSchedule {
    /// Tick interval.
    pub interval: Duration,
    /// Identifier delivered on each [`crate::events::TimerEvent`].
    pub id: u32,
}

/// Engine wiring (channel capacity, optional periodic timers).
#[derive(Clone, Debug)]
pub struct EngineConfig {
    /// Channel capacity.
    pub channel_capacity: usize,
    /// Interval timers forwarding [`Event::Timer`] into the engine loop.
    pub timer_schedules: Vec<TimerSchedule>,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            channel_capacity: 1024,
            timer_schedules: Vec::new(),
        }
    }
}

pub(crate) fn spawn_timer_tasks(handle: EngineHandle, schedules: Vec<TimerSchedule>) {
    for s in schedules {
        let h = handle.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(s.interval);
            loop {
                interval.tick().await;
                let ts = OffsetDateTime::now_utc();
                let _ = h
                    .send(Event::Timer(TimerEvent { ts, id: s.id }))
                    .await;
            }
        });
    }
}

/// Builder for spawning the engine task.
pub struct EngineBuilder;

impl EngineBuilder {
    /// Spawn background consumer. Returns control handle and join handle.
    pub fn spawn<S, E>(
        cfg: EngineConfig,
        mut state: GlobalState,
        mut strategy: S,
        risk: RiskPipeline,
        exec: Arc<E>,
    ) -> (EngineHandle, tokio::task::JoinHandle<Result<()>>)
    where
        S: Strategy + Send + 'static,
        E: ExecutionGateway + 'static,
    {
        let (tx, mut rx) = mpsc::channel(cfg.channel_capacity);
        let handle = EngineHandle { tx };
        spawn_timer_tasks(handle.clone(), cfg.timer_schedules.clone());
        let join = tokio::spawn(async move {
            while let Some(ev) = rx.recv().await {
                if let Err(e) =
                    dispatch_event(&mut state, &mut strategy, &risk, exec.as_ref(), ev).await
                {
                    warn!(target: "athenas_pallas::engine", "engine step: {e}");
                }
            }
            Ok(())
        });
        (handle, join)
    }

    /// Offline replay helper (CSV / synthetic) without background tasks.
    pub async fn run_batch<S, E>(
        mut state: GlobalState,
        mut strategy: S,
        risk: &RiskPipeline,
        exec: &E,
        events: Vec<Event>,
    ) -> Result<GlobalState>
    where
        S: Strategy,
        E: ExecutionGateway,
    {
        for ev in events {
            process_event(&mut state, &mut strategy, risk, exec, ev).await?;
        }
        Ok(state)
    }
}

/// One deterministic engine iteration (backtests / unit tests).
pub async fn engine_step<S, E>(
    state: &mut GlobalState,
    strategy: &mut S,
    risk: &RiskPipeline,
    exec: &E,
    ev: Event,
) -> Result<()>
where
    S: Strategy,
    E: ExecutionGateway,
{
    process_event(state, strategy, risk, exec, ev).await
}

/// Legacy type alias — prefer [`EngineBuilder::spawn`].
pub type Engine = EngineBuilder;

/// Process a single event (market/account/control/timer) through strategy → risk → execution.
/// Use this in backtests or tests; live engines use [`EngineBuilder::spawn`].
pub async fn dispatch_event<S, E>(
    state: &mut GlobalState,
    strategy: &mut S,
    risk: &RiskPipeline,
    exec: &E,
    ev: Event,
) -> Result<()>
where
    S: Strategy,
    E: ExecutionGateway,
{
    process_event(state, strategy, risk, exec, ev).await
}

async fn process_event<S, E>(
    state: &mut GlobalState,
    strategy: &mut S,
    risk: &RiskPipeline,
    exec: &E,
    ev: Event,
) -> Result<()>
where
    S: Strategy,
    E: ExecutionGateway,
{
    match &ev {
        Event::Market(m) => {
            state.apply_market(m);
            let passive = exec.poll_after_market(&state.snapshot()).await?;
            for a in passive {
                state.apply_account(&a);
            }
        }
        Event::Account(a) => state.apply_account(a),
        Event::Control(c) => apply_control(state, exec, risk, c).await?,
        Event::Timer(_) => {}
    }

    let now = event_time(&ev);
    state.refresh_daily_risk_anchor(now);

    if state.paused {
        return Ok(());
    }

    let snap = state.snapshot();
    let ctx = StrategyContext {
        now,
        state: &snap,
    };
    let intents = strategy.on_event(&ctx, &ev);

    for intent in intents {
        let snap = state.snapshot();
        if let Err(e) = risk.validate(&snap, &intent) {
            warn!(target: "athenas_pallas::engine", "risk: {e}");
            continue;
        }
        let events = dispatch_intent(exec, &snap, &intent).await;
        match events {
            Ok(evs) => {
                for a in evs {
                    state.apply_account(&a);
                }
            }
            Err(e) => error!(target: "athenas_pallas::engine", "execution: {e}"),
        }
    }

    Ok(())
}

fn event_time(ev: &Event) -> OffsetDateTime {
    match ev {
        Event::Market(crate::events::MarketEvent::Trade { ts, .. }) => *ts,
        Event::Market(crate::events::MarketEvent::BookL1 { ts, .. }) => *ts,
        Event::Market(crate::events::MarketEvent::BookL2Snapshot(s)) => s.ts,
        Event::Timer(t) => t.ts,
        _ => OffsetDateTime::now_utc(),
    }
}

async fn apply_control<E: ExecutionGateway>(
    state: &mut GlobalState,
    exec: &E,
    risk: &RiskPipeline,
    c: &ControlEvent,
) -> Result<()> {
    match c {
        ControlEvent::Pause => state.paused = true,
        ControlEvent::Resume => state.paused = false,
        ControlEvent::CancelAll => {
            let evs = exec.cancel_all(&state.snapshot()).await?;
            for a in evs {
                state.apply_account(&a);
            }
        }
        ControlEvent::Flatten => {
            let evs = exec.cancel_all(&state.snapshot()).await?;
            for a in evs {
                state.apply_account(&a);
            }
            let insts: Vec<_> = state
                .positions
                .iter()
                .filter(|(_, p)| !p.is_zero())
                .map(|(k, _)| k.clone())
                .collect();
            for inst in insts {
                let snap = state.snapshot();
                let pos = snap
                    .positions
                    .get(&inst)
                    .copied()
                    .unwrap_or(Decimal::ZERO);
                if pos.is_zero() {
                    continue;
                }
                let intent = OrderIntent {
                    instrument: inst,
                    side: if pos > Decimal::ZERO {
                        Side::Sell
                    } else {
                        Side::Buy
                    },
                    order_type: OrderType::Market,
                    price: None,
                    qty: pos.abs(),
                    client_order_id: None,
                    source: OrderIntentSource::Flatten,
                };
                if let Err(e) = risk.validate(&snap, &intent) {
                    warn!(target: "athenas_pallas::engine", "flatten risk: {e}");
                    continue;
                }
                match dispatch_intent(exec, &snap, &intent).await {
                    Ok(evs) => {
                        for a in evs {
                            state.apply_account(&a);
                        }
                    }
                    Err(e) => error!(target: "athenas_pallas::engine", "flatten execution: {e}"),
                }
            }
        }
    }
    Ok(())
}

async fn dispatch_intent<E: ExecutionGateway>(
    exec: &E,
    state: &GlobalState,
    intent: &OrderIntent,
) -> Result<Vec<crate::events::AccountEvent>> {
    match intent.order_type {
        OrderType::Limit => exec.place_limit(state, intent).await,
        OrderType::Market => exec.place_market(state, intent).await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::{Event, TimerEvent};
    use std::time::Duration;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn timer_schedule_emits_id() {
        let (tx, mut rx) = mpsc::channel(16);
        let handle = EngineHandle { tx };
        spawn_timer_tasks(
            handle,
            vec![TimerSchedule {
                interval: Duration::from_millis(25),
                id: 7,
            }],
        );
        let mut got = None;
        for _ in 0..80 {
            tokio::time::sleep(Duration::from_millis(20)).await;
            while let Ok(ev) = rx.try_recv() {
                if let Event::Timer(TimerEvent { id, .. }) = ev {
                    got = Some(id);
                }
            }
            if got.is_some() {
                break;
            }
        }
        assert_eq!(got, Some(7));
    }
}
