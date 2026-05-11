//! Paper execution: local balances and simple touch / last-trade fill rules.

use super::{apply_slippage, ExecutionGateway};
use crate::error::{Error, Result};
use crate::events::{AccountEvent, OrderIntent};
use crate::state::{GlobalState, InstrumentMeta};
use crate::types::{OpenOrder, OrderId, OrderStatus, OrderType, Side};
use async_trait::async_trait;
use rust_decimal::Decimal;
/// Paper trading gateway configuration.
#[derive(Clone, Debug)]
pub struct PaperConfig {
    /// Taker fee in basis points.
    pub fee_bps: Decimal,
    /// Market order slippage in bps applied to mid/last.
    pub market_slippage_bps: Decimal,
}

impl Default for PaperConfig {
    fn default() -> Self {
        Self {
            fee_bps: Decimal::from(10u64),
            market_slippage_bps: Decimal::from(5u64),
        }
    }
}

/// Thread-safe paper gateway (shared across engine tasks).
#[derive(Clone)]
pub struct PaperGateway {
    cfg: PaperConfig,
}

impl PaperGateway {
    /// New paper gateway.
    pub fn new(cfg: PaperConfig) -> Self {
        Self { cfg }
    }

    fn meta<'a>(
        state: &'a GlobalState,
        inst: &crate::types::InstrumentId,
    ) -> Result<&'a InstrumentMeta> {
        state
            .instruments
            .get(inst)
            .ok_or_else(|| Error::Invalid("unknown instrument".into()))
    }

    fn fee_notional(notional: Decimal, bps: Decimal) -> Decimal {
        notional * bps / Decimal::from(10_000u64)
    }

    fn build_open(
        id: OrderId,
        intent: &OrderIntent,
        qty: Decimal,
        price: Option<Decimal>,
        ot: OrderType,
    ) -> OpenOrder {
        OpenOrder {
            id,
            instrument: intent.instrument.clone(),
            side: intent.side,
            order_type: ot,
            price,
            remaining_qty: qty,
            original_qty: qty,
            status: OrderStatus::Open,
        }
    }

    fn emit_order_update(o: &OpenOrder) -> AccountEvent {
        AccountEvent::OrderUpdate {
            id: o.id.clone(),
            instrument: o.instrument.clone(),
            side: o.side,
            order_type: o.order_type,
            price: o.price,
            remaining_qty: o.remaining_qty,
            original_qty: o.original_qty,
            status: o.status,
        }
    }

    /// Append balance adjustment events after a fill (quote spent/received, fee).
    fn balance_updates_after_fill(
        state: &GlobalState,
        order: &OpenOrder,
        meta: &InstrumentMeta,
        price: Decimal,
        qty: Decimal,
        fee: Decimal,
    ) -> Vec<AccountEvent> {
        let mut evs = Vec::new();
        let base_free = *state.balances.get(&meta.base).unwrap_or(&Decimal::ZERO);
        let quote_free = *state.balances.get(&meta.quote).unwrap_or(&Decimal::ZERO);
        let (new_base, new_quote) = match order.side {
            Side::Buy => {
                let cost = price * qty + fee;
                (base_free + qty, quote_free - cost)
            }
            Side::Sell => {
                let proceeds = price * qty - fee;
                (base_free - qty, quote_free + proceeds)
            }
        };
        evs.push(AccountEvent::Balance {
            asset: meta.base.clone(),
            free: new_base,
        });
        evs.push(AccountEvent::Balance {
            asset: meta.quote.clone(),
            free: new_quote,
        });
        evs
    }

    fn crossing_limit(
        side: Side,
        limit: Decimal,
        bid: Decimal,
        ask: Decimal,
    ) -> Option<Decimal> {
        match side {
            Side::Buy if limit >= ask => Some(ask),
            Side::Sell if limit <= bid => Some(bid),
            _ => None,
        }
    }
}

#[async_trait]
impl ExecutionGateway for PaperGateway {
    async fn place_limit(&self, state: &GlobalState, intent: &OrderIntent) -> Result<Vec<AccountEvent>> {
        if intent.order_type != OrderType::Limit {
            return Err(Error::Invalid("place_limit requires limit intent".into()));
        }
        let price = intent
            .price
            .ok_or_else(|| Error::Invalid("limit needs price".into()))?;
        let meta = Self::meta(state, &intent.instrument)?;
        let id = OrderId::new_v4();
        let mut o = Self::build_open(id, intent, intent.qty, Some(price), OrderType::Limit);

        // Crossing check against L1
        if let Some((_, bid, ask)) = state.l1.get(&intent.instrument) {
            if let Some(px) = Self::crossing_limit(intent.side, price, *bid, *ask) {
                o.remaining_qty = Decimal::ZERO;
                o.status = OrderStatus::Filled;
                let mut evs = vec![Self::emit_order_update(&o)];
                let fee = Self::fee_notional(px * intent.qty, self.cfg.fee_bps);
                evs.push(AccountEvent::Fill {
                    order_id: o.id.clone(),
                    instrument: o.instrument.clone(),
                    side: o.side,
                    price: px,
                    qty: intent.qty,
                    fee,
                    fee_asset: meta.quote.clone(),
                });
                evs.extend(Self::balance_updates_after_fill(
                    state, &o, meta, px, intent.qty, fee,
                ));
                return Ok(evs);
            }
        }

        Ok(vec![Self::emit_order_update(&o)])
    }

    async fn place_market(&self, state: &GlobalState, intent: &OrderIntent) -> Result<Vec<AccountEvent>> {
        let meta = Self::meta(state, &intent.instrument)?;
        let mid = state
            .mid_or_last(&intent.instrument)
            .ok_or_else(|| Error::Invalid("no mid/last for market order".into()))?;
        let px = apply_slippage(intent.side, mid, self.cfg.market_slippage_bps);
        let id = OrderId::new_v4();
        let o = Self::build_open(id, intent, intent.qty, None, OrderType::Market);
        let mut o_filled = o.clone();
        o_filled.remaining_qty = Decimal::ZERO;
        o_filled.status = OrderStatus::Filled;
        let fee = Self::fee_notional(px * intent.qty, self.cfg.fee_bps);
        let mut evs = vec![Self::emit_order_update(&o_filled)];
        evs.push(AccountEvent::Fill {
            order_id: o_filled.id.clone(),
            instrument: o_filled.instrument.clone(),
            side: o_filled.side,
            price: px,
            qty: intent.qty,
            fee,
            fee_asset: meta.quote.clone(),
        });
        evs.extend(Self::balance_updates_after_fill(
            state, &o_filled, meta, px, intent.qty, fee,
        ));
        Ok(evs)
    }

    async fn cancel(&self, state: &GlobalState, order_id: OrderId) -> Result<Vec<AccountEvent>> {
        let o = state
            .open_orders
            .get(&order_id)
            .cloned()
            .ok_or_else(|| Error::Invalid("unknown order".into()))?;
        let mut c = o;
        c.status = OrderStatus::Canceled;
        c.remaining_qty = Decimal::ZERO;
        Ok(vec![Self::emit_order_update(&c)])
    }

    async fn cancel_all(&self, state: &GlobalState) -> Result<Vec<AccountEvent>> {
        let mut out = Vec::new();
        for (_, o) in state.open_orders.iter() {
            let mut c = o.clone();
            c.status = OrderStatus::Canceled;
            c.remaining_qty = Decimal::ZERO;
            out.push(Self::emit_order_update(&c));
        }
        Ok(out)
    }

    async fn poll_after_market(&self, state: &GlobalState) -> Result<Vec<AccountEvent>> {
        let mut out = Vec::new();
        let orders: Vec<OpenOrder> = state.open_orders.values().cloned().collect();
        for o in orders {
            if o.order_type != OrderType::Limit {
                continue;
            }
            let limit = match o.price {
                Some(p) => p,
                None => continue,
            };
            let last = state.last_trade.get(&o.instrument).map(|(_, p)| *p);
            let Some(last_px) = last else { continue };
            let meta = match Self::meta(state, &o.instrument) {
                Ok(m) => m,
                Err(_) => continue,
            };
            let fill = match o.side {
                Side::Buy if last_px <= limit => Some(last_px),
                Side::Sell if last_px >= limit => Some(last_px),
                _ => None,
            };
            if let Some(px) = fill {
                let qty = o.remaining_qty;
                let fee = Self::fee_notional(px * qty, self.cfg.fee_bps);
                let mut filled = o.clone();
                filled.remaining_qty = Decimal::ZERO;
                filled.status = OrderStatus::Filled;
                out.push(Self::emit_order_update(&filled));
                out.push(AccountEvent::Fill {
                    order_id: filled.id.clone(),
                    instrument: filled.instrument.clone(),
                    side: filled.side,
                    price: px,
                    qty,
                    fee,
                    fee_asset: meta.quote.clone(),
                });
                out.extend(Self::balance_updates_after_fill(
                    state, &filled, meta, px, qty, fee,
                ));
            }
        }
        Ok(out)
    }
}
