//! Simulation gateway — delegates to [`super::paper::PaperGateway`] fill logic.

use async_trait::async_trait;

use super::{ExecutionGateway, PaperGateway, PaperConfig};
use crate::error::Result;
use crate::events::{AccountEvent, OrderIntent};
use crate::state::GlobalState;
use crate::types::OrderId;

/// Backtest / replay gateway (same crossing rules as paper).
#[derive(Clone)]
pub struct SimGateway {
    inner: PaperGateway,
}

impl SimGateway {
    /// New simulator with fee and sl slippage knobs.
    pub fn new(cfg: PaperConfig) -> Self {
        Self {
            inner: PaperGateway::new(cfg),
        }
    }
}

impl Default for SimGateway {
    fn default() -> Self {
        Self::new(PaperConfig::default())
    }
}

#[async_trait]
impl ExecutionGateway for SimGateway {
    async fn place_limit(
        &self,
        state: &GlobalState,
        intent: &OrderIntent,
    ) -> Result<Vec<AccountEvent>> {
        self.inner.place_limit(state, intent).await
    }

    async fn place_market(
        &self,
        state: &GlobalState,
        intent: &OrderIntent,
    ) -> Result<Vec<AccountEvent>> {
        self.inner.place_market(state, intent).await
    }

    async fn cancel(&self, state: &GlobalState, order_id: OrderId) -> Result<Vec<AccountEvent>> {
        self.inner.cancel(state, order_id).await
    }

    async fn cancel_all(&self, state: &GlobalState) -> Result<Vec<AccountEvent>> {
        self.inner.cancel_all(state).await
    }

    async fn poll_after_market(&self, state: &GlobalState) -> Result<Vec<AccountEvent>> {
        self.inner.poll_after_market(state).await
    }
}
