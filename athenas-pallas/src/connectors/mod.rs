//! Exchange connectors (public market data).

use crate::engine::EngineHandle;
use crate::error::Result;

/// Pluggable market connector feeding [`EngineHandle`].
#[async_trait::async_trait]
pub trait MarketConnector: Send {
    /// Run until error or shutdown.
    async fn run(self, out: EngineHandle) -> Result<()>;
}

#[cfg(feature = "binance")]
pub mod binance_spot;

#[cfg(feature = "binance-live")]
pub mod binance_user_data;

#[cfg(feature = "binance")]
pub use binance_spot::{BinanceCombinedStream, BinanceDepthStream};

#[cfg(feature = "binance-live")]
pub use binance_user_data::BinanceUserDataStream;
