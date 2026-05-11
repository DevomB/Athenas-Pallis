//! Execution backends: paper, simulation, and live stub.

mod paper;
mod sim;

#[cfg(all(feature = "live-trading", not(feature = "binance-live")))]
mod live;

#[cfg(all(feature = "live-trading", not(feature = "binance-live")))]
pub use live::LiveGateway;

#[cfg(feature = "binance-live")]
mod binance_live;

#[cfg(feature = "binance-live")]
pub use binance_live::{BinanceCredentials, BinanceLiveGateway};

pub use paper::{PaperConfig, PaperGateway};
pub use sim::SimGateway;

use crate::error::Result;
use crate::events::OrderIntent;
use crate::state::GlobalState;
use crate::types::{OrderId, Side};
use async_trait::async_trait;
use rust_decimal::Decimal;

/// Async venue bridge invoked by [`crate::Engine`].
#[async_trait]
pub trait ExecutionGateway: Send + Sync {
    /// Resting or crossing limit.
    async fn place_limit(
        &self,
        state: &GlobalState,
        intent: &OrderIntent,
    ) -> Result<Vec<crate::events::AccountEvent>>;
    /// Immediate market.
    async fn place_market(
        &self,
        state: &GlobalState,
        intent: &OrderIntent,
    ) -> Result<Vec<crate::events::AccountEvent>>;
    /// Cancel one order.
    async fn cancel(
        &self,
        state: &GlobalState,
        order_id: OrderId,
    ) -> Result<Vec<crate::events::AccountEvent>>;
    /// Cancel all working orders.
    async fn cancel_all(&self, state: &GlobalState)
        -> Result<Vec<crate::events::AccountEvent>>;
    /// Passive fills after a market event (paper/sim).
    async fn poll_after_market(
        &self,
        state: &GlobalState,
    ) -> Result<Vec<crate::events::AccountEvent>> {
        let _ = state;
        Ok(vec![])
    }
}

/// Apply market-order slippage in basis points around `mid`.
pub(crate) fn apply_slippage(side: Side, mid: Decimal, bps: Decimal) -> Decimal {
    let adj = mid * bps / Decimal::from(10_000u64);
    match side {
        Side::Buy => mid + adj,
        Side::Sell => mid - adj,
    }
}
