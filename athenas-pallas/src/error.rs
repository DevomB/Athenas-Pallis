//! Error types.

use thiserror::Error;

/// Top-level framework error.
#[derive(Debug, Error)]
pub enum Error {
    /// I/O or transport failure.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    /// JSON parse/serialize.
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    /// Decimal parse.
    #[error("decimal parse: {0}")]
    Decimal(#[from] rust_decimal::Error),
    /// WebSocket (binance feature).
    #[cfg(feature = "binance")]
    #[error("websocket: {0}")]
    Ws(#[from] tokio_tungstenite::tungstenite::Error),
    /// HTTP client (live-trading feature).
    #[cfg(feature = "live-trading")]
    #[error("http: {0}")]
    Http(#[from] reqwest::Error),
    /// Engine channel closed.
    #[error("engine shutdown")]
    EngineShutdown,
    /// Risk rejected order.
    #[error("risk rejected: {0}")]
    RiskRejected(String),
    /// Execution rejected.
    #[error("execution rejected: {0}")]
    ExecutionRejected(String),
    /// Invalid configuration or state.
    #[error("invalid: {0}")]
    Invalid(String),
}

/// Result alias.
pub type Result<T> = std::result::Result<T, Error>;
