//! Binance Spot public WebSocket (combined stream).

use crate::connectors::MarketConnector;
use crate::engine::EngineHandle;
use crate::error::{Error, Result};
use crate::events::{BookL2Snapshot, Event, MarketEvent};
use crate::types::InstrumentId;
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use serde::Deserialize;
use time::OffsetDateTime;
use tokio_tungstenite::{connect_async, tungstenite::Message};

/// Combined `trade` + `bookTicker` for one symbol (e.g. btcusdt lower case).
pub struct BinanceCombinedStream {
    /// Instrument id for normalized events.
    pub instrument: InstrumentId,
    /// Lowercase Binance stream symbol (e.g. btcusdt).
    pub stream_symbol: String,
    /// WebSocket base (e.g. `wss://stream.binance.com:9443` or testnet stream host).
    pub ws_base: String,
}

#[derive(Debug, Deserialize)]
struct Wrapper {
    stream: String,
    data: serde_json::Value,
}

#[derive(Debug, Deserialize)]
struct TradeMsg {
    #[serde(rename = "E")]
    event_time: i64,
    #[serde(rename = "p")]
    price: String,
    #[serde(rename = "q")]
    qty: String,
}

#[derive(Debug, Deserialize)]
struct BookTickerMsg {
    #[serde(rename = "E")]
    event_time: Option<i64>,
    #[serde(rename = "b")]
    bid: String,
    #[serde(rename = "a")]
    ask: String,
}

#[async_trait]
impl MarketConnector for BinanceCombinedStream {
    async fn run(self, out: EngineHandle) -> Result<()> {
        let url = format!(
            "{}/stream?streams={}@trade/{}@bookTicker",
            self.ws_base.trim_end_matches('/'),
            self.stream_symbol,
            self.stream_symbol
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
            if let Ok(w) = serde_json::from_str::<Wrapper>(&text) {
                if w.stream.contains("@trade") {
                    if let Ok(t) = serde_json::from_value::<TradeMsg>(w.data) {
                        let ts = OffsetDateTime::from_unix_timestamp_nanos(
                            (t.event_time as i128) * 1_000_000,
                        )
                        .unwrap_or_else(|_| OffsetDateTime::now_utc());
                        let price: Decimal = t.price.parse()?;
                        let qty: Decimal = t.qty.parse()?;
                        let ev = Event::Market(MarketEvent::Trade {
                            instrument: self.instrument.clone(),
                            ts,
                            price,
                            qty,
                        });
                        out.send(ev).await.ok();
                    }
                } else if w.stream.contains("bookTicker") {
                    if let Ok(b) = serde_json::from_value::<BookTickerMsg>(w.data) {
                        let ts = if let Some(ms) = b.event_time {
                            OffsetDateTime::from_unix_timestamp_nanos((ms as i128) * 1_000_000)
                                .unwrap_or_else(|_| OffsetDateTime::now_utc())
                        } else {
                            OffsetDateTime::now_utc()
                        };
                        let bid: Decimal = b.bid.parse()?;
                        let ask: Decimal = b.ask.parse()?;
                        let ev = Event::Market(MarketEvent::BookL1 {
                            instrument: self.instrument.clone(),
                            ts,
                            bid,
                            ask,
                        });
                        out.send(ev).await.ok();
                    }
                }
            }
        }
        Err(Error::Invalid("websocket closed".into()))
    }
}

/// Shallow order book depth (`@depth5|10|20@100ms`).
pub struct BinanceDepthStream {
    /// Instrument id for normalized events.
    pub instrument: InstrumentId,
    /// Lowercase stream symbol (e.g. btcusdt).
    pub stream_symbol: String,
    /// Target depth (clamped to Binance-supported 5 / 10 / 20).
    pub depth_levels: usize,
    /// WebSocket base (e.g. `wss://stream.binance.com:9443` or testnet).
    pub ws_base: String,
}

fn depth_bucket(n: usize) -> usize {
    if n <= 5 {
        5
    } else if n <= 10 {
        10
    } else {
        20
    }
}

#[derive(Debug, Deserialize)]
struct DepthPartial {
    #[serde(rename = "E")]
    event_time: Option<i64>,
    bids: Vec<Vec<String>>,
    asks: Vec<Vec<String>>,
}

#[async_trait]
impl MarketConnector for BinanceDepthStream {
    async fn run(self, out: EngineHandle) -> Result<()> {
        let bucket = depth_bucket(self.depth_levels);
        let url = format!(
            "{}/stream?streams={}@depth{}@100ms",
            self.ws_base.trim_end_matches('/'),
            self.stream_symbol,
            bucket
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
            if let Ok(w) = serde_json::from_str::<Wrapper>(&text) {
                if w.stream.contains("depth") {
                    if let Ok(d) = serde_json::from_value::<DepthPartial>(w.data) {
                        let ts = if let Some(ms) = d.event_time {
                            OffsetDateTime::from_unix_timestamp_nanos((ms as i128) * 1_000_000)
                                .unwrap_or_else(|_| OffsetDateTime::now_utc())
                        } else {
                            OffsetDateTime::now_utc()
                        };
                        let bids = parse_levels(&d.bids);
                        let asks = parse_levels(&d.asks);
                        let ev = Event::Market(MarketEvent::BookL2Snapshot(BookL2Snapshot {
                            instrument: self.instrument.clone(),
                            ts,
                            bids,
                            asks,
                        }));
                        out.send(ev).await.ok();
                    }
                }
            }
        }
        Err(Error::Invalid("websocket closed".into()))
    }
}

fn parse_levels(rows: &[Vec<String>]) -> Vec<(Decimal, Decimal)> {
    let mut out = Vec::with_capacity(rows.len());
    for r in rows {
        if r.len() >= 2 {
            if let (Ok(px), Ok(q)) = (r[0].parse(), r[1].parse()) {
                out.push((px, q));
            }
        }
    }
    out
}
