//! # Athena's Pallas
//!
//! Unified event-driven engine for **live**, **paper**, and **backtest** trading.
//!
//! ## Modes
//! - Swap the [`execution::ExecutionGateway`] implementation and event sources; keep
//!   [`strategy::Strategy`] and [`risk::RiskCheck`] unchanged.
//!
//! ## Extension
//! - Add venues: implement [`connectors::MarketConnector`] (see `connectors::binance_spot` with feature `binance`).
//! - Add fill logic: document custom models via [`backtest::FillModel`].
//!
//! ## Features
//! - `binance` — public WebSocket connector for Binance Spot.
//! - `control-server` — localhost HTTP control (`/pause`, `/resume`, `/cancel-all`).
//! - `binance-live` — signed Spot REST ([`execution::BinanceLiveGateway`]), user stream connector, signing deps (`hmac`, `sha2`, `hex`).

#![warn(missing_docs)]

pub mod backtest;
pub mod connectors;
pub mod engine;
pub mod error;
pub mod events;
pub mod execution;
pub mod metrics;
pub mod risk;
pub mod state;
pub mod strategy;
pub mod types;

#[cfg(feature = "control-server")]
pub mod control;

pub use engine::{
    dispatch_event, engine_step, Engine, EngineBuilder, EngineConfig, EngineHandle, TimerSchedule,
};
pub use error::{Error, Result};
pub use events::{BookL2Snapshot, Event, OrderIntentSource};
pub use types::{EquityPoint, ExchangeId, InstrumentId, Side, Symbol};
