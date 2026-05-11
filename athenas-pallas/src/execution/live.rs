//! Live trading scaffold — wire signed REST / private WS in a venue adapter.

use async_trait::async_trait;

use crate::error::{Error, Result};
use crate::events::{AccountEvent, OrderIntent};
use crate::state::GlobalState;
use crate::types::OrderId;

/// HTTP client placeholder for future authenticated calls.
pub struct LiveGateway {
    /// Reserved — configure timeouts, proxies, and HMAC signing middleware here.
    pub client: reqwest::Client,
}

impl LiveGateway {
    /// New stub gateway.
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }
}

impl Default for LiveGateway {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl super::ExecutionGateway for LiveGateway {
    async fn place_limit(
        &self,
        _state: &GlobalState,
        _intent: &OrderIntent,
    ) -> Result<Vec<AccountEvent>> {
        let _ = &self.client;
        Err(Error::ExecutionRejected(
            "LiveGateway stub: implement venue signing (never commit API secrets).".into(),
        ))
    }

    async fn place_market(
        &self,
        _state: &GlobalState,
        _intent: &OrderIntent,
    ) -> Result<Vec<AccountEvent>> {
        Err(Error::ExecutionRejected(
            "LiveGateway stub: implement venue signing.".into(),
        ))
    }

    async fn cancel(&self, _state: &GlobalState, _order_id: OrderId) -> Result<Vec<AccountEvent>> {
        Err(Error::ExecutionRejected(
            "LiveGateway stub: implement cancel REST.".into(),
        ))
    }

    async fn cancel_all(&self, _state: &GlobalState) -> Result<Vec<AccountEvent>> {
        Err(Error::ExecutionRejected(
            "LiveGateway stub: implement mass cancel.".into(),
        ))
    }
}
