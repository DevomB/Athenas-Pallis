//! Flatten control path cancels then submits reduce-only market intents.

use athenas_pallas::dispatch_event;
use athenas_pallas::events::{ControlEvent, Event, MarketEvent, OrderIntent};
use athenas_pallas::execution::{PaperConfig, PaperGateway};
use athenas_pallas::risk::{PauseCheck, RiskPipeline};
use athenas_pallas::state::{GlobalState, InstrumentMeta};
use athenas_pallas::strategy::{Strategy, StrategyContext};
use athenas_pallas::types::{Asset, InstrumentId};
use rust_decimal::Decimal;
use std::collections::HashMap;
use time::OffsetDateTime;

struct Quiet;

impl Strategy for Quiet {
    fn on_event(&mut self, _ctx: &StrategyContext<'_>, _event: &Event) -> Vec<OrderIntent> {
        vec![]
    }
}

#[tokio::test]
async fn flatten_closes_position_when_paused() {
    let inst = InstrumentId::new("binance", "BTCUSDT");
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
    balances.insert(Asset("BTC".into()), Decimal::new(1, 3));

    let mut state = GlobalState::new(instruments, balances);
    state.positions.insert(inst.clone(), Decimal::new(1, 3));
    state.paused = true;

    let mut strat = Quiet;
    let risk = RiskPipeline::new(vec![Box::new(PauseCheck::default())]);
    let exec = PaperGateway::new(PaperConfig::default());

    let ts = OffsetDateTime::now_utc();
    dispatch_event(
        &mut state,
        &mut strat,
        &risk,
        &exec,
        Event::Market(MarketEvent::BookL1 {
            instrument: inst.clone(),
            ts,
            bid: Decimal::new(40_000, 0),
            ask: Decimal::new(40_010, 0),
        }),
    )
    .await
    .unwrap();

    dispatch_event(
        &mut state,
        &mut strat,
        &risk,
        &exec,
        Event::Control(ControlEvent::Flatten),
    )
    .await
    .unwrap();

    assert!(
        state
            .positions
            .get(&inst)
            .copied()
            .unwrap_or(Decimal::ZERO)
            .abs()
            < Decimal::new(1, 6)
    );
}
