//! **Real-money capable example**: Binance Spot public market data + signed REST execution +
//! user-data WebSocket. Misconfiguration can place **live orders**. Prefer Spot **testnet** URLs
//! until you explicitly switch to mainnet.
//!
//! Environment (never commit secrets):
//!
//! | Variable | Purpose |
//! |----------|---------|
//! | `BINANCE_BASE_URL` | REST root (`https://testnet.binance.vision` or `https://api.binance.com`) |
//! | `BINANCE_WS_URL` | Stream root (`wss://testnet.binance.vision` or `wss://stream.binance.com:9443`) |
//! | `BINANCE_API_KEY` | API key header |
//! | `BINANCE_SECRET` | Signing secret |
//! | `PALLAS_CONTROL_TOKEN` | Optional control-plane secret (default `dev`) |
//!
//! ```text
//! cargo run -p live_binance
//! ```

use athenas_pallas::connectors::binance_spot::BinanceCombinedStream;
use athenas_pallas::connectors::binance_user_data::BinanceUserDataStream;
use athenas_pallas::connectors::MarketConnector;
use athenas_pallas::control::{serve, ControlServerConfig, HEADER_SECRET};
use athenas_pallas::engine::{EngineBuilder, EngineConfig};
use athenas_pallas::events::{Event, OrderIntent};
use athenas_pallas::execution::{BinanceCredentials, BinanceLiveGateway};
use athenas_pallas::risk::{PauseCheck, RiskPipeline};
use athenas_pallas::state::{GlobalState, InstrumentMeta};
use athenas_pallas::strategy::{Strategy, StrategyContext};
use athenas_pallas::types::Asset;
use athenas_pallas::types::InstrumentId;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{info, warn};

struct Hold {}

impl Strategy for Hold {
    fn on_event(&mut self, _ctx: &StrategyContext<'_>, _event: &Event) -> Vec<OrderIntent> {
        vec![]
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let base = std::env::var("BINANCE_BASE_URL")
        .unwrap_or_else(|_| "https://testnet.binance.vision".into());
    let ws = std::env::var("BINANCE_WS_URL")
        .unwrap_or_else(|_| "wss://testnet.binance.vision".into());
    let api_key = std::env::var("BINANCE_API_KEY").unwrap_or_default();
    let secret = std::env::var("BINANCE_SECRET").unwrap_or_default();

    if api_key.is_empty() || secret.is_empty() {
        warn!("BINANCE_API_KEY / BINANCE_SECRET not set — REST signing will fail until configured.");
    }

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
    balances.insert(Asset("USDT".into()), Decimal::ZERO);
    balances.insert(Asset("BTC".into()), Decimal::ZERO);

    let state = GlobalState::new(instruments, balances);
    let strategy = Hold {};
    let risk = RiskPipeline::new(vec![Box::new(PauseCheck::default())]);
    let exec = Arc::new(BinanceLiveGateway::new(
        base.clone(),
        BinanceCredentials {
            api_key: api_key.clone(),
            secret,
        },
    ));

    let (handle, _join) = EngineBuilder::spawn(
        EngineConfig::default(),
        state,
        strategy,
        risk,
        exec,
    );

    let token = std::env::var("PALLAS_CONTROL_TOKEN").unwrap_or_else(|_| "dev".into());
    let ctl_handle = handle.clone();
    let bind = "127.0.0.1:9848".to_string();
    let token_clone = token.clone();
    tokio::spawn(async move {
        if let Err(e) = serve(
            ctl_handle,
            ControlServerConfig {
                bind,
                secret: token_clone,
            },
        )
        .await
        {
            warn!("control server ended: {e}");
        }
    });

    info!(
        "control: POST http://127.0.0.1:9848/{{pause,resume,cancel-all,flatten}} header {}: {}",
        HEADER_SECRET,
        token
    );

    let pub_connector = BinanceCombinedStream {
        instrument: instrument.clone(),
        stream_symbol: "btcusdt".into(),
        ws_base: ws.clone(),
    };
    let md_handle = handle.clone();
    tokio::spawn(async move {
        if let Err(e) = pub_connector.run(md_handle).await {
            warn!("public stream: {e}");
        }
    });

    let ud = BinanceUserDataStream {
        api_key,
        rest_base: base,
        ws_base: ws,
    };
    let ud_handle = handle.clone();
    tokio::spawn(async move {
        if let Err(e) = ud.run(ud_handle).await {
            warn!("user data stream: {e}");
        }
    });

    info!("live_binance running — Ctrl+C to exit");
    tokio::signal::ctrl_c().await?;
    Ok(())
}
