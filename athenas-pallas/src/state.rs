//! Authoritative in-memory book, balances, orders, and positions.

use crate::events::{AccountEvent, BookL2Snapshot, MarketEvent};
use crate::types::{
    Asset, InstrumentId, OpenOrder, OrderId, OrderStatus, Side,
};
use rust_decimal::Decimal;
use std::collections::HashMap;
use time::{Date, OffsetDateTime};

/// Static metadata for an instrument (base/quote assets).
#[derive(Clone, Debug)]
pub struct InstrumentMeta {
    /// Base asset (e.g. BTC).
    pub base: Asset,
    /// Quote asset (e.g. USDT).
    pub quote: Asset,
}

/// Central view updated serially by the engine.
#[derive(Clone, Debug)]
pub struct GlobalState {
    /// Registered instruments.
    pub instruments: HashMap<InstrumentId, InstrumentMeta>,
    /// Last trade price.
    pub last_trade: HashMap<InstrumentId, (OffsetDateTime, Decimal)>,
    /// Best bid/ask.
    pub l1: HashMap<InstrumentId, (OffsetDateTime, Decimal, Decimal)>,
    /// Open orders by id.
    pub open_orders: HashMap<OrderId, OpenOrder>,
    /// Free balances.
    pub balances: HashMap<Asset, Decimal>,
    /// Net base position per instrument (signed).
    pub positions: HashMap<InstrumentId, Decimal>,
    /// Latest shallow L2 snapshot per instrument (if subscribed).
    pub l2: HashMap<InstrumentId, BookL2Snapshot>,
    /// Mark-to-market equity anchor for UTC calendar daily loss (quote asset must match risk rule).
    pub risk_day_anchor: Option<(Date, Decimal)>,
    /// When set, [`Self::refresh_daily_risk_anchor`] uses this quote for equity day rollover (UTC date).
    pub daily_risk_quote: Option<Asset>,
    /// Trading paused (risk/engine).
    pub paused: bool,
}

impl GlobalState {
    /// New empty state with optional initial balances.
    pub fn new(
        instruments: HashMap<InstrumentId, InstrumentMeta>,
        initial_balances: HashMap<Asset, Decimal>,
    ) -> Self {
        Self {
            instruments,
            last_trade: HashMap::new(),
            l1: HashMap::new(),
            open_orders: HashMap::new(),
            balances: initial_balances,
            positions: HashMap::new(),
            l2: HashMap::new(),
            risk_day_anchor: None,
            daily_risk_quote: None,
            paused: false,
        }
    }

    /// Refresh UTC-day equity anchor for [`crate::risk::MaxDailyLossQuote`] after rollover or first tick.
    pub fn refresh_daily_risk_anchor(&mut self, now: OffsetDateTime) {
        let Some(ref quote) = self.daily_risk_quote else {
            return;
        };
        let today = now.date();
        match self.risk_day_anchor {
            None => {
                self.risk_day_anchor = Some((today, self.mark_equity_quote(quote)));
            }
            Some((day, _)) if day != today => {
                self.risk_day_anchor = Some((today, self.mark_equity_quote(quote)));
            }
            _ => {}
        }
    }

    /// Mark-to-market equity in `quote` using mids (open positions valued at [`Self::mid_or_last`]).
    pub fn mark_equity_quote(&self, quote: &crate::types::Asset) -> Decimal {
        let mut eq = *self.balances.get(quote).unwrap_or(&Decimal::ZERO);
        for (inst, pos) in &self.positions {
            if pos.is_zero() {
                continue;
            }
            let px = self.mid_or_last(inst).unwrap_or(Decimal::ZERO);
            eq += *pos * px;
        }
        eq
    }

    /// Mid price or last trade fallback.
    pub fn mid_or_last(&self, inst: &InstrumentId) -> Option<Decimal> {
        if let Some((_, bid, ask)) = self.l1.get(inst) {
            Some((*bid + *ask) / Decimal::from(2u64))
        } else {
            self.last_trade.get(inst).map(|(_, p)| *p)
        }
    }

    /// Apply a market event (read-only book/trade updates).
    pub fn apply_market(&mut self, ev: &MarketEvent) {
        match ev {
            MarketEvent::Trade {
                instrument, ts, price, ..
            } => {
                self.last_trade.insert(instrument.clone(), (*ts, *price));
            }
            MarketEvent::BookL1 {
                instrument,
                ts,
                bid,
                ask,
            } => {
                self.l1.insert(instrument.clone(), (*ts, *bid, *ask));
            }
            MarketEvent::BookL2Snapshot(snap) => {
                self.l2.insert(snap.instrument.clone(), snap.clone());
            }
        }
    }

    /// Apply account event (balances, orders, fills).
    pub fn apply_account(&mut self, ev: &AccountEvent) {
        match ev {
            AccountEvent::Balance { asset, free } => {
                self.balances.insert(asset.clone(), *free);
            }
            AccountEvent::OrderUpdate {
                id,
                instrument,
                side,
                order_type,
                price,
                remaining_qty,
                original_qty,
                status,
            } => {
                let o = OpenOrder {
                    id: id.clone(),
                    instrument: instrument.clone(),
                    side: *side,
                    order_type: *order_type,
                    price: *price,
                    remaining_qty: *remaining_qty,
                    original_qty: *original_qty,
                    status: *status,
                };
                if matches!(status, OrderStatus::Filled | OrderStatus::Canceled | OrderStatus::Rejected)
                {
                    self.open_orders.remove(id);
                } else {
                    self.open_orders.insert(id.clone(), o);
                }
            }
            AccountEvent::Fill {
                instrument,
                side,
                price: _,
                qty,
                ..
            } => {
                let sign = match side {
                    Side::Buy => Decimal::ONE,
                    Side::Sell => -Decimal::ONE,
                };
                let delta = sign * qty;
                *self.positions.entry(instrument.clone()).or_insert(Decimal::ZERO) += delta;
            }
        }
    }

    /// Mark-to-market equity in quote using mid or last trade.
    pub fn mark_to_market_equity(&self, inst: &InstrumentId) -> Option<Decimal> {
        let mid = self.mid_or_last(inst)?;
        let meta = self.instruments.get(inst)?;
        let base = self
            .balances
            .get(&meta.base)
            .copied()
            .unwrap_or(Decimal::ZERO);
        let quote = self
            .balances
            .get(&meta.quote)
            .copied()
            .unwrap_or(Decimal::ZERO);
        Some(quote + base * mid)
    }

    /// Snapshot for risk checks (cheap clone of maps).
    pub fn snapshot(&self) -> GlobalState {
        self.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Asset;
    use rust_decimal::Decimal;
    use std::collections::HashMap;

    #[test]
    fn mark_equity_includes_cash() {
        let i = crate::types::InstrumentId::new("x", "BTCUSDT");
        let mut inst = HashMap::new();
        inst.insert(
            i.clone(),
            InstrumentMeta {
                base: Asset("BTC".into()),
                quote: Asset("USDT".into()),
            },
        );
        let mut bal = HashMap::new();
        bal.insert(Asset("USDT".into()), Decimal::from(1000u64));
        let s = GlobalState::new(inst, bal);
        assert_eq!(s.mark_equity_quote(&Asset("USDT".into())), Decimal::from(1000u64));
    }
}
