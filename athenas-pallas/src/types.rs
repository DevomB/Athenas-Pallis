//! Core domain types.

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::fmt;
use time::OffsetDateTime;
use uuid::Uuid;

/// Exchange identifier (e.g. Binance).
#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ExchangeId(pub String);

impl fmt::Display for ExchangeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Trading pair symbol (e.g. BTCUSDT).
#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Symbol(pub String);

impl fmt::Display for Symbol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Instrument = exchange + symbol.
#[derive(Clone, Debug, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstrumentId {
    /// Venue.
    pub exchange: ExchangeId,
    /// Pair.
    pub symbol: Symbol,
}

impl InstrumentId {
    /// New instrument.
    pub fn new(exchange: impl Into<String>, symbol: impl Into<String>) -> Self {
        Self {
            exchange: ExchangeId(exchange.into()),
            symbol: Symbol(symbol.into()),
        }
    }
}

impl fmt::Display for InstrumentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.exchange, self.symbol)
    }
}

/// Order side.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Side {
    /// Buy base asset.
    Buy,
    /// Sell base asset.
    Sell,
}

impl Side {
    /// Opposite side.
    pub fn opposite(self) -> Self {
        match self {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        }
    }
}

/// Limit vs market.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderType {
    /// Limit order.
    Limit,
    /// Market order.
    Market,
}

/// Stable order id (paper/sim).
#[derive(Clone, Debug, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderId(pub Uuid);

impl OrderId {
    /// Random id.
    pub fn new_v4() -> Self {
        Self(Uuid::new_v4())
    }

    /// Deterministic id from a venue numeric order id (e.g. Binance `orderId`).
    pub fn from_venue_u64(id: u64) -> Self {
        Self(Uuid::from_u128(id as u128))
    }
}

/// Client-supplied correlation id.
#[derive(Clone, Debug, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientOrderId(pub String);

/// Order status in local state.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderStatus {
    /// Accepted, resting or pending.
    Open,
    /// Fully filled.
    Filled,
    /// User/system canceled.
    Canceled,
    /// Rejected by venue or simulator.
    Rejected,
}

/// Asset code for balances (e.g. USDT, BTC).
#[derive(Clone, Debug, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct Asset(pub String);

/// Working order snapshot.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OpenOrder {
    /// Id.
    pub id: OrderId,
    /// Instrument.
    pub instrument: InstrumentId,
    /// Side.
    pub side: Side,
    /// Type.
    pub order_type: OrderType,
    /// Limit price if limit.
    pub price: Option<Decimal>,
    /// Remaining base quantity.
    pub remaining_qty: Decimal,
    /// Original quantity.
    pub original_qty: Decimal,
    /// Status.
    pub status: OrderStatus,
}

/// Point on equity curve for metrics.
#[derive(Clone, Debug)]
pub struct EquityPoint {
    /// Timestamp.
    pub ts: OffsetDateTime,
    /// Mark-to-market equity in quote (e.g. USDT).
    pub equity_quote: Decimal,
}
