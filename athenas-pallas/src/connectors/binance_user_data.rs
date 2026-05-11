//! Binance Spot user data stream (listen key + private WebSocket).

use crate::connectors::MarketConnector;
use crate::engine::EngineHandle;
use crate::error::{Error, Result};
use crate::events::{AccountEvent, Event};
use crate::types::{Asset, InstrumentId, OrderId, OrderStatus, OrderType, Side, Symbol};
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::Value;
use tokio_tungstenite::{connect_async, tungstenite::Message};

/// REST + WebSocket user stream (`executionReport`, balances).
#[derive(Clone, Debug)]
pub struct BinanceUserDataStream {
    /// `X-MBX-APIKEY` value.
    pub api_key: String,
    /// REST root (`https://api.binance.com` or testnet).
    pub rest_base: String,
    /// WebSocket root (`wss://stream.binance.com:9443`).
    pub ws_base: String,
}

#[derive(Debug, Deserialize)]
struct ListenKeyResp {
    #[serde(rename = "listenKey")]
    listen_key: String,
}

async fn create_listen_key(api_key: &str, rest_base: &str) -> Result<String> {
    let url = format!("{}/api/v3/userDataStream", rest_base.trim_end_matches('/'));
    let client = reqwest::Client::new();
    let resp = client
        .post(&url)
        .header("X-MBX-APIKEY", api_key)
        .send()
        .await?;
    let text = resp.text().await?;
    let k: ListenKeyResp = serde_json::from_str(&text)
        .map_err(|_| Error::Invalid(format!("listenKey parse failed: {text}")))?;
    Ok(k.listen_key)
}

async fn keepalive_listen_key(api_key: &str, rest_base: &str, key: &str) -> Result<()> {
    let url = format!(
        "{}/api/v3/userDataStream?listenKey={}",
        rest_base.trim_end_matches('/'),
        key
    );
    let client = reqwest::Client::new();
    client
        .put(&url)
        .header("X-MBX-APIKEY", api_key)
        .send()
        .await?;
    Ok(())
}

fn inst(sym: &str) -> InstrumentId {
    InstrumentId {
        exchange: crate::types::ExchangeId("binance".into()),
        symbol: Symbol(sym.to_uppercase()),
    }
}

fn parse_generic(ev: &Value) -> Option<Vec<AccountEvent>> {
    let typ = ev.get("e")?.as_str()?;
    match typ {
        "executionReport" => parse_execution_report(ev),
        "outboundAccountPosition" => parse_outbound(ev),
        "balanceUpdate" => parse_balance_update(ev),
        _ => None,
    }
}

fn parse_execution_report(ev: &Value) -> Option<Vec<AccountEvent>> {
    let symbol = ev.get("s")?.as_str()?;
    let instrument = inst(symbol);
    let side = match ev.get("S")?.as_str()? {
        "BUY" => Side::Buy,
        _ => Side::Sell,
    };
    let order_type = match ev.get("o")?.as_str()? {
        "LIMIT" => OrderType::Limit,
        _ => OrderType::Market,
    };
    let status_str = ev.get("X")?.as_str()?;
    let status = match status_str {
        "FILLED" => OrderStatus::Filled,
        "CANCELED" | "EXPIRED" => OrderStatus::Canceled,
        "REJECTED" => OrderStatus::Rejected,
        "NEW" | "PARTIALLY_FILLED" => OrderStatus::Open,
        _ => OrderStatus::Open,
    };
    let oid = ev.get("i")?.as_u64()?;
    let orig_qty: Decimal = ev.get("q")?.as_str()?.parse().ok()?;
    let exec_qty: Decimal = ev.get("z")?.as_str()?.parse().unwrap_or(Decimal::ZERO);
    let remaining_qty = (orig_qty - exec_qty).max(Decimal::ZERO);
    let price = ev
        .get("p")
        .and_then(|p| p.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.parse())
        .transpose()
        .ok()
        .flatten();

    let mut out = vec![AccountEvent::OrderUpdate {
        id: OrderId::from_venue_u64(oid),
        instrument,
        side,
        order_type,
        price,
        remaining_qty,
        original_qty: orig_qty,
        status,
    }];

    if status == OrderStatus::Filled && exec_qty > Decimal::ZERO {
        let px: Decimal = ev
            .get("L")
            .and_then(|p| p.as_str())
            .and_then(|s| s.parse().ok())
            .unwrap_or(Decimal::ZERO);
        let fee: Decimal = ev
            .get("n")
            .and_then(|p| p.as_str())
            .and_then(|s| s.parse().ok())
            .unwrap_or(Decimal::ZERO);
        let fee_asset = Asset(
            ev.get("N")
                .and_then(|a| a.as_str())
                .unwrap_or("USDT")
                .to_string(),
        );
        out.push(AccountEvent::Fill {
            order_id: OrderId::from_venue_u64(oid),
            instrument: inst(symbol),
            side,
            price: px,
            qty: exec_qty,
            fee,
            fee_asset,
        });
    }

    Some(out)
}

fn parse_outbound(ev: &Value) -> Option<Vec<AccountEvent>> {
    let balances = ev.get("B")?.as_array()?;
    let mut out = Vec::new();
    for b in balances {
        let asset = Asset(b.get("a")?.as_str()?.to_string());
        let free: Decimal = b.get("f")?.as_str()?.parse().ok()?;
        out.push(AccountEvent::Balance { asset, free });
    }
    if out.is_empty() {
        None
    } else {
        Some(out)
    }
}

fn parse_balance_update(ev: &Value) -> Option<Vec<AccountEvent>> {
    let _asset = ev.get("a")?.as_str()?;
    let _delta: Decimal = ev.get("d")?.as_str()?.parse().ok()?;
    None
}

#[async_trait]
impl MarketConnector for BinanceUserDataStream {
    async fn run(self, out: EngineHandle) -> Result<()> {
        let key = create_listen_key(&self.api_key, &self.rest_base).await?;
        let ka_key = key.clone();
        let ka_api = self.api_key.clone();
        let ka_rest = self.rest_base.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(30 * 60));
            loop {
                interval.tick().await;
                if keepalive_listen_key(&ka_api, &ka_rest, &ka_key).await.is_err() {
                    break;
                }
            }
        });

        let url = format!(
            "{}/ws/{}",
            self.ws_base.trim_end_matches('/'),
            key
        );
        let (ws, _) = connect_async(&url).await?;
        let (mut write, mut read) = ws.split();

        while let Some(msg) = read.next().await {
            let msg = msg?;
            let text = match msg {
                Message::Text(t) => t.to_string(),
                Message::Ping(p) => {
                    let _ = write.send(Message::Pong(p)).await;
                    continue;
                }
                Message::Close(_) => break,
                _ => continue,
            };
            if let Ok(v) = serde_json::from_str::<Value>(&text) {
                if let Some(evs) = parse_generic(&v) {
                    for a in evs {
                        let _ = out.send(Event::Account(a)).await;
                    }
                }
            }
        }
        Err(Error::Invalid("user websocket closed".into()))
    }
}
