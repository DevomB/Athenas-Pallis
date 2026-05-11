//! Backtest example: replay OHLCV rows + simulated execution + metrics.
//!
//! ```text
//! cargo run -p backtest_csv
//! ```

use athenas_pallas::backtest::{CsvBarSource, HistoricalSource, OhlcvRow};
use athenas_pallas::dispatch_event;
use athenas_pallas::events::{Event, OrderIntent};
use athenas_pallas::execution::{PaperConfig, SimGateway};
use athenas_pallas::metrics::summarize;
use athenas_pallas::risk::{PauseCheck, RiskPipeline};
use athenas_pallas::state::{GlobalState, InstrumentMeta};
use athenas_pallas::strategy::{Strategy, StrategyContext};
use athenas_pallas::types::{Asset, EquityPoint, ExchangeId, InstrumentId, OrderType, Side, Symbol};
use rust_decimal::Decimal;
use rust_decimal::prelude::FromPrimitive;
use std::collections::HashMap;

/// Buys a small amount on the first bar, then holds.
struct BuyAndHold {
    instrument: InstrumentId,
    done: bool,
}

impl Strategy for BuyAndHold {
    fn on_event(&mut self, ctx: &StrategyContext, _event: &Event) -> Vec<OrderIntent> {
        if self.done {
            return vec![];
        }
        if ctx.state.mid_or_last(&self.instrument).is_none() {
            return vec![];
        }
        self.done = true;
        let qty = Decimal::from_f64(0.01).unwrap_or(Decimal::ZERO);
        vec![OrderIntent {
            instrument: self.instrument.clone(),
            side: Side::Buy,
            order_type: OrderType::Market,
            price: None,
            qty,
            client_order_id: None,
            source: athenas_pallas::events::OrderIntentSource::User,
        }]
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let instrument = InstrumentId::new("binance", "BTCUSDT");
    let mut instruments = HashMap::new();
    instruments.insert(
        instrument.clone(),
        InstrumentMeta {
            base: Asset("BTC".into()),
            quote: Asset("USDT".into()),
        },
    );
    let mut balances = HashMap::new();
    balances.insert(Asset("USDT".into()), Decimal::new(10_000, 0));
    balances.insert(Asset("BTC".into()), Decimal::ZERO);

    let mut state = GlobalState::new(instruments, balances);
    let mut strategy = BuyAndHold {
        instrument: instrument.clone(),
        done: false,
    };
    let risk = RiskPipeline::new(vec![Box::new(PauseCheck::default())]);
    let exec = SimGateway::new(PaperConfig::default());

    let rows = vec![
        OhlcvRow {
            ts: "2024-01-01T00:00:00Z".into(),
            open: Decimal::new(40_000, 0),
            high: Decimal::new(41_000, 0),
            low: Decimal::new(39_000, 0),
            close: Decimal::new(40_500, 0),
            volume: Decimal::ONE,
        },
        OhlcvRow {
            ts: "2024-01-02T00:00:00Z".into(),
            open: Decimal::new(40_500, 0),
            high: Decimal::new(42_000, 0),
            low: Decimal::new(40_000, 0),
            close: Decimal::new(41_800, 0),
            volume: Decimal::ONE,
        },
        OhlcvRow {
            ts: "2024-01-03T00:00:00Z".into(),
            open: Decimal::new(41_800, 0),
            high: Decimal::new(42_500, 0),
            low: Decimal::new(41_000, 0),
            close: Decimal::new(41_200, 0),
            volume: Decimal::ONE,
        },
    ];

    let mut src = CsvBarSource::from_ohlcv_rows(
        ExchangeId("binance".into()),
        Symbol("BTCUSDT".into()),
        rows,
    );
    let mut curve: Vec<EquityPoint> = Vec::new();
    while let Some(ev) = src.next_event() {
        let ts = match &ev {
            Event::Market(athenas_pallas::events::MarketEvent::BookL1 { ts, .. }) => *ts,
            Event::Market(athenas_pallas::events::MarketEvent::Trade { ts, .. }) => *ts,
            Event::Market(athenas_pallas::events::MarketEvent::BookL2Snapshot(s)) => s.ts,
            _ => time::OffsetDateTime::now_utc(),
        };
        dispatch_event(&mut state, &mut strategy, &risk, &exec, ev).await?;
        if let Some(eq) = state.mark_to_market_equity(&instrument) {
            curve.push(EquityPoint {
                ts,
                equity_quote: eq,
            });
        }
    }

    let summary = summarize(curve, 252.0);
    println!("PnL: {}", summary.pnl);
    println!("PnL %: {}", summary.pnl_pct);
    println!("Max drawdown (fraction): {}", summary.max_drawdown);
    println!("Sharpe (scaled): {}", summary.sharpe);
    println!("Sortino (scaled): {}", summary.sortino);
    println!("Per-step returns: {:?}", summary.returns);
    Ok(())
}
