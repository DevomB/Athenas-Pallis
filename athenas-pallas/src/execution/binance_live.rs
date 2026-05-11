//! Binance Spot signed REST execution (`/api/v3`).

use async_trait::async_trait;
use hmac::{Hmac, Mac};
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::Value;
use sha2::Sha256;

use crate::error::{Error, Result};
use crate::events::{AccountEvent, OrderIntent};
use crate::state::GlobalState;
use crate::types::{Asset, InstrumentId, OrderId, OrderStatus, OrderType, Side, Symbol};

type HmacSha256 = Hmac<Sha256>;

/// API credentials (load from environment in binaries — never log secrets).
#[derive(Clone, Debug)]
pub struct BinanceCredentials {
    /// `X-MBX-APIKEY` header value.
    pub api_key: String,
    /// HMAC SHA256 secret.
    pub secret: String,
}

/// Live Spot gateway using Binance signed REST.
#[derive(Clone, Debug)]
pub struct BinanceLiveGateway {
    /// HTTP client.
    pub client: reqwest::Client,
    /// REST root, e.g. `https://api.binance.com` or testnet base.
    pub base_url: String,
    /// API key + secret used for signing (never log the secret).
    pub credentials: BinanceCredentials,
    /// `recvWindow` query parameter (ms).
    pub recv_window: u64,
}

impl BinanceLiveGateway {
    /// Construct with defaults (`recv_window` 5000 ms).
    pub fn new(base_url: impl Into<String>, credentials: BinanceCredentials) -> Self {
        Self {
            client: reqwest::Client::new(),
            base_url: base_url.into().trim_end_matches('/').to_string(),
            credentials,
            recv_window: 5000,
        }
    }

    fn sign(&self, query: &str) -> Result<String> {
        let mut mac = HmacSha256::new_from_slice(self.credentials.secret.as_bytes())
            .map_err(|e| Error::Invalid(e.to_string()))?;
        mac.update(query.as_bytes());
        Ok(hex::encode(mac.finalize().into_bytes()))
    }

    fn timestamp_ms() -> i64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0)
    }

    async fn signed_get(&self, path: &str, mut params: Vec<(String, String)>) -> Result<String> {
        params.push(("timestamp".into(), Self::timestamp_ms().to_string()));
        params.push(("recvWindow".into(), self.recv_window.to_string()));
        params.sort_by(|a, b| a.0.cmp(&b.0));
        let mut query = String::new();
        for (i, (k, v)) in params.iter().enumerate() {
            if i > 0 {
                query.push('&');
            }
            query.push_str(k);
            query.push('=');
            query.push_str(v);
        }
        let sig = self.sign(&query)?;
        let url = format!(
            "{}{}?{}&signature={}",
            self.base_url, path, query, sig
        );
        let resp = self
            .client
            .get(&url)
            .header("X-MBX-APIKEY", &self.credentials.api_key)
            .send()
            .await?;
        let text = resp.text().await?;
        Self::maybe_binance_err(&text)?;
        Ok(text)
    }

    async fn signed_post_form(
        &self,
        path: &str,
        mut params: Vec<(String, String)>,
    ) -> Result<String> {
        params.push(("timestamp".into(), Self::timestamp_ms().to_string()));
        params.push(("recvWindow".into(), self.recv_window.to_string()));
        params.sort_by(|a, b| a.0.cmp(&b.0));
        let mut query = String::new();
        for (i, (k, v)) in params.iter().enumerate() {
            if i > 0 {
                query.push('&');
            }
            query.push_str(k);
            query.push('=');
            query.push_str(v);
        }
        let sig = self.sign(&query)?;
        let body = format!("{}&signature={}", query, sig);
        let url = format!("{}{}", self.base_url, path);
        let resp = self
            .client
            .post(&url)
            .header("X-MBX-APIKEY", &self.credentials.api_key)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .body(body)
            .send()
            .await?;
        let text = resp.text().await?;
        Self::maybe_binance_err(&text)?;
        Ok(text)
    }

    async fn signed_delete(&self, path: &str, mut params: Vec<(String, String)>) -> Result<String> {
        params.push(("timestamp".into(), Self::timestamp_ms().to_string()));
        params.push(("recvWindow".into(), self.recv_window.to_string()));
        params.sort_by(|a, b| a.0.cmp(&b.0));
        let mut query = String::new();
        for (i, (k, v)) in params.iter().enumerate() {
            if i > 0 {
                query.push('&');
            }
            query.push_str(k);
            query.push('=');
            query.push_str(v);
        }
        let sig = self.sign(&query)?;
        let url = format!(
            "{}{}?{}&signature={}",
            self.base_url, path, query, sig
        );
        let resp = self
            .client
            .delete(&url)
            .header("X-MBX-APIKEY", &self.credentials.api_key)
            .send()
            .await?;
        let text = resp.text().await?;
        Self::maybe_binance_err(&text)?;
        Ok(text)
    }

    fn maybe_binance_err(text: &str) -> Result<()> {
        if let Ok(v) = serde_json::from_str::<Value>(text) {
            if let Some(code) = v.get("code").and_then(|c| c.as_i64()) {
                if code != 0 {
                    let msg = v
                        .get("msg")
                        .and_then(|m| m.as_str())
                        .unwrap_or("binance error");
                    return Err(Error::ExecutionRejected(format!("binance {code}: {msg}")));
                }
            }
        }
        Ok(())
    }

    fn parse_symbol_to_instrument(sym: &str) -> InstrumentId {
        InstrumentId {
            exchange: crate::types::ExchangeId("binance".into()),
            symbol: Symbol(sym.to_uppercase()),
        }
    }

    fn parse_order_json(
        text: &str,
        instrument_hint: &InstrumentId,
        quote_fee: Asset,
    ) -> Result<Vec<AccountEvent>> {
        let v: Value = serde_json::from_str(text)?;
        if v.is_array() {
            let mut out = Vec::new();
            for item in v.as_array().unwrap() {
                let mut evs = Self::single_order_value(item, instrument_hint, quote_fee.clone())?;
                out.append(&mut evs);
            }
            return Ok(out);
        }
        Self::single_order_value(&v, instrument_hint, quote_fee)
    }

    fn single_order_value(
        v: &Value,
        instrument_hint: &InstrumentId,
        quote_fee: Asset,
    ) -> Result<Vec<AccountEvent>> {
        let order_id = v
            .get("orderId")
            .and_then(|x| x.as_u64())
            .ok_or_else(|| Error::Invalid("missing orderId".into()))?;
        let symbol = v
            .get("symbol")
            .and_then(|s| s.as_str())
            .unwrap_or(&instrument_hint.symbol.0);
        let instrument_id = Self::parse_symbol_to_instrument(symbol);
        let side = match v.get("side").and_then(|s| s.as_str()) {
            Some("BUY") => Side::Buy,
            Some("SELL") => Side::Sell,
            _ => Side::Buy,
        };
        let order_type = match v.get("type").and_then(|s| s.as_str()) {
            Some("LIMIT") => OrderType::Limit,
            _ => OrderType::Market,
        };
        let status_str = v.get("status").and_then(|s| s.as_str()).unwrap_or("NEW");
        let status = match status_str {
            "FILLED" => OrderStatus::Filled,
            "CANCELED" | "EXPIRED" => OrderStatus::Canceled,
            "REJECTED" => OrderStatus::Rejected,
            _ => OrderStatus::Open,
        };
        let orig_qty: Decimal = v
            .get("origQty")
            .and_then(|q| q.as_str())
            .unwrap_or("0")
            .parse()?;
        let mut exec_qty: Decimal = v
            .get("executedQty")
            .and_then(|q| q.as_str())
            .unwrap_or("0")
            .parse()?;
        if exec_qty.is_zero() {
            if let Some(fs) = v.get("fills").and_then(|f| f.as_array()) {
                for f in fs {
                    let q: Decimal = f
                        .get("qty")
                        .and_then(|x| x.as_str())
                        .unwrap_or("0")
                        .parse()
                        .unwrap_or(Decimal::ZERO);
                    exec_qty += q;
                }
            }
        }
        let remaining_qty = (orig_qty - exec_qty).max(Decimal::ZERO);
        let price = v
            .get("price")
            .and_then(|p| p.as_str())
            .filter(|s| !s.is_empty())
            .map(|s| s.parse())
            .transpose()?;

        let mut out = vec![AccountEvent::OrderUpdate {
            id: OrderId::from_venue_u64(order_id),
            instrument: instrument_id.clone(),
            side,
            order_type,
            price,
            remaining_qty,
            original_qty: orig_qty,
            status,
        }];

        if status == OrderStatus::Filled && exec_qty > Decimal::ZERO {
            let avg_px = if let Some(cum) = v.get("cummulativeQuoteQty").and_then(|x| x.as_str()) {
                let cum_q: Decimal = cum.parse().unwrap_or(Decimal::ZERO);
                cum_q / exec_qty
            } else if let Some(p) = price {
                p
            } else {
                Decimal::ZERO
            };
            out.push(AccountEvent::Fill {
                order_id: OrderId::from_venue_u64(order_id),
                instrument: instrument_id,
                side,
                price: avg_px,
                qty: exec_qty,
                fee: Decimal::ZERO,
                fee_asset: quote_fee,
            });
        }

        Ok(out)
    }
}

#[async_trait]
impl super::ExecutionGateway for BinanceLiveGateway {
    async fn place_limit(
        &self,
        state: &GlobalState,
        intent: &OrderIntent,
    ) -> Result<Vec<AccountEvent>> {
        let sym = intent.instrument.symbol.0.clone();
        let price = intent
            .price
            .ok_or_else(|| Error::Invalid("limit needs price".into()))?;
        let mut params = vec![
            ("symbol".into(), sym),
            ("side".into(), match intent.side {
                Side::Buy => "BUY".into(),
                Side::Sell => "SELL".into(),
            }),
            ("type".into(), "LIMIT".into()),
            ("timeInForce".into(), "GTC".into()),
            ("quantity".into(), intent.qty.to_string()),
            ("price".into(), price.to_string()),
        ];
        if let Some(ref cid) = intent.client_order_id {
            params.push(("newClientOrderId".into(), cid.0.clone()));
        }
        let quote_fee = state
            .instruments
            .get(&intent.instrument)
            .map(|m| m.quote.clone())
            .unwrap_or_else(|| Asset("USDT".into()));
        let text = self.signed_post_form("/api/v3/order", params).await?;
        Self::parse_order_json(&text, &intent.instrument, quote_fee)
    }

    async fn place_market(
        &self,
        state: &GlobalState,
        intent: &OrderIntent,
    ) -> Result<Vec<AccountEvent>> {
        let sym = intent.instrument.symbol.0.clone();
        let mut params = vec![
            ("symbol".into(), sym),
            ("side".into(), match intent.side {
                Side::Buy => "BUY".into(),
                Side::Sell => "SELL".into(),
            }),
            ("type".into(), "MARKET".into()),
            ("quantity".into(), intent.qty.to_string()),
        ];
        if let Some(ref cid) = intent.client_order_id {
            params.push(("newClientOrderId".into(), cid.0.clone()));
        }
        let quote_fee = state
            .instruments
            .get(&intent.instrument)
            .map(|m| m.quote.clone())
            .unwrap_or_else(|| Asset("USDT".into()));
        let text = self.signed_post_form("/api/v3/order", params).await?;
        Self::parse_order_json(&text, &intent.instrument, quote_fee)
    }

    async fn cancel(&self, state: &GlobalState, order_id: OrderId) -> Result<Vec<AccountEvent>> {
        let mut sym_for_symbol = None;
        for o in state.open_orders.values() {
            if o.id == order_id {
                sym_for_symbol = Some(o.instrument.symbol.0.clone());
                break;
            }
        }
        let sym = sym_for_symbol.ok_or_else(|| Error::Invalid("order not in local book".into()))?;
        let oid = order_id
            .0
            .as_u128()
            .min(u64::MAX as u128) as u64;
        let text = self
            .signed_delete(
                "/api/v3/order",
                vec![
                    ("symbol".into(), sym),
                    ("orderId".into(), oid.to_string()),
                ],
            )
            .await?;
        let inst = state
            .open_orders
            .get(&order_id)
            .map(|o| o.instrument.clone())
            .unwrap_or_else(|| InstrumentId::new("binance", "UNKNOWN"));
        let quote_fee = state
            .open_orders
            .get(&order_id)
            .and_then(|o| state.instruments.get(&o.instrument))
            .map(|m| m.quote.clone())
            .unwrap_or_else(|| Asset("USDT".into()));
        Self::parse_order_json(&text, &inst, quote_fee)
    }

    async fn cancel_all(&self, state: &GlobalState) -> Result<Vec<AccountEvent>> {
        let text = self.signed_get("/api/v3/openOrders", vec![]).await?;
        let orders: Vec<OpenOrderLite> = serde_json::from_str(&text).unwrap_or_default();
        let mut symbols: Vec<String> = orders.iter().map(|o| o.symbol.clone()).collect();
        symbols.sort();
        symbols.dedup();
        if symbols.is_empty() {
            for o in state.open_orders.values() {
                symbols.push(o.instrument.symbol.0.clone());
            }
            symbols.sort();
            symbols.dedup();
        }
        let mut out = Vec::new();
        for sym in symbols {
            let cancel_text = self
                .signed_delete("/api/v3/openOrders", vec![("symbol".into(), sym.clone())])
                .await?;
            let inst = InstrumentId {
                exchange: crate::types::ExchangeId("binance".into()),
                symbol: Symbol(sym),
            };
            let quote_fee = state
                .instruments
                .get(&inst)
                .map(|m| m.quote.clone())
                .unwrap_or_else(|| Asset("USDT".into()));
            let mut evs = Self::parse_order_json(&cancel_text, &inst, quote_fee)?;
            out.append(&mut evs);
        }
        Ok(out)
    }
}

#[derive(Debug, Deserialize)]
struct OpenOrderLite {
    symbol: String,
}
