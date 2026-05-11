//! Historical and synthetic event sources for deterministic replay.

pub mod batch;

use std::fs::File;
use std::io::Read as _;
use std::path::Path;

use rust_decimal::Decimal;
use serde::Deserialize;
use time::format_description::well_known::Rfc3339;
use time::macros::format_description;
use time::{Duration, OffsetDateTime, PrimitiveDateTime};

use crate::events::{Event, MarketEvent};
use crate::types::{ExchangeId, InstrumentId, Symbol};

/// Produces timestamped [`Event`] values.
pub trait HistoricalSource: Send {
    /// Next event in order.
    fn next_event(&mut self) -> Option<Event>;
}

/// OHLCV row for [`CsvBarSource`].
#[derive(Clone, Debug, Deserialize)]
pub struct OhlcvRow {
    /// Timestamp string (RFC3339 or `YYYY-MM-DD HH:MM:SS` or date-only).
    pub ts: String,
    /// Open.
    pub open: Decimal,
    /// High.
    pub high: Decimal,
    /// Low.
    pub low: Decimal,
    /// Close.
    pub close: Decimal,
    /// Volume.
    pub volume: Decimal,
}

/// CSV bar replay (`ts,open,high,low,close,volume` header).
pub struct CsvBarSource {
    rows: Vec<OhlcvRow>,
    idx: usize,
    instrument: InstrumentId,
}

impl CsvBarSource {
    /// Load from disk.
    pub fn from_path(
        path: &Path,
        exchange: ExchangeId,
        symbol: Symbol,
    ) -> std::io::Result<Self> {
        let mut buf = String::new();
        File::open(path)?.read_to_string(&mut buf)?;
        Self::from_str(&buf, exchange, symbol).map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, e)
        })
    }

    /// Parse UTF-8 CSV text.
    pub fn from_str(data: &str, exchange: ExchangeId, symbol: Symbol) -> Result<Self, String> {
        let mut rdr = csv::Reader::from_reader(data.as_bytes());
        let mut rows = Vec::new();
        for rec in rdr.deserialize() {
            let row: OhlcvRow = rec.map_err(|e| e.to_string())?;
            rows.push(row);
        }
        if rows.is_empty() {
            return Err("empty csv".into());
        }
        Ok(Self {
            rows,
            idx: 0,
            instrument: InstrumentId { exchange, symbol },
        })
    }

    /// In-memory rows (already parsed).
    pub fn from_ohlcv_rows(exchange: ExchangeId, symbol: Symbol, rows: Vec<OhlcvRow>) -> Self {
        Self {
            rows,
            idx: 0,
            instrument: InstrumentId { exchange, symbol },
        }
    }
}

fn parse_ts(s: &str) -> Option<OffsetDateTime> {
    if let Ok(t) = OffsetDateTime::parse(s, &Rfc3339) {
        return Some(t);
    }
    let fmt = format_description!(
        "[year]-[month]-[day] [hour]:[minute]:[second]"
    );
    if let Ok(p) = PrimitiveDateTime::parse(s, &fmt) {
        return Some(p.assume_utc());
    }
    let fmt2 = format_description!("[year]-[month]-[day]");
    PrimitiveDateTime::parse(s, &fmt2)
        .ok()
        .map(|p| p.assume_utc())
}

impl HistoricalSource for CsvBarSource {
    fn next_event(&mut self) -> Option<Event> {
        let row = self.rows.get(self.idx)?;
        self.idx += 1;
        let ts = parse_ts(&row.ts).unwrap_or_else(|| OffsetDateTime::now_utc());
        let spread = row.high - row.low;
        let pad = if spread.is_zero() {
            row.close / Decimal::from(10_000u64)
        } else {
            spread / Decimal::from(100u64)
        };
        let mid = row.close;
        let bid = mid - pad;
        let ask = mid + pad;
        Some(Event::Market(MarketEvent::BookL1 {
            instrument: self.instrument.clone(),
            ts,
            bid,
            ask,
        }))
    }
}

/// Deterministic toy random walk for smoke tests.
pub struct SyntheticGmSource {
    steps: usize,
    taken: usize,
    price: Decimal,
    instrument: InstrumentId,
    start: OffsetDateTime,
    drift: Decimal,
    vol: Decimal,
    seed: u64,
}

impl SyntheticGmSource {
    /// Multiplicative steps around `drift` with noise scaled by `vol`.
    pub fn new(
        instrument: InstrumentId,
        start: OffsetDateTime,
        start_price: Decimal,
        steps: usize,
        drift: Decimal,
        vol: Decimal,
        seed: u64,
    ) -> Self {
        Self {
            steps,
            taken: 0,
            price: start_price,
            instrument,
            start,
            drift,
            vol,
            seed,
        }
    }

    fn rng_next(&mut self) -> f64 {
        self.seed ^= self.seed << 13;
        self.seed ^= self.seed >> 7;
        self.seed ^= self.seed << 17;
        (self.seed as f64) / (u64::MAX as f64)
    }
}

impl HistoricalSource for SyntheticGmSource {
    fn next_event(&mut self) -> Option<Event> {
        if self.taken >= self.steps {
            return None;
        }
        self.taken += 1;
        let u = self.rng_next() - 0.5;
        let shock = Decimal::new((u * 1_000_000.0).round() as i64, 6) * self.vol;
        self.price = (self.price * (self.drift + shock)).max(Decimal::new(1, 2));
        let ts = self.start + Duration::seconds(self.taken as i64);
        let pad = self.price / Decimal::from(2000u64);
        Some(Event::Market(MarketEvent::BookL1 {
            instrument: self.instrument.clone(),
            ts,
            bid: self.price - pad,
            ask: self.price + pad,
        }))
    }
}

/// OHLC row without volume (embedded example / tight CSV).
#[derive(Clone, Debug, Deserialize)]
pub struct OhlcRow {
    /// Timestamp (RFC3339 recommended).
    pub ts: String,
    /// Open.
    pub open: Decimal,
    /// High.
    pub high: Decimal,
    /// Low.
    pub low: Decimal,
    /// Close.
    pub close: Decimal,
}

/// In-memory OHLC replay as synthetic [`MarketEvent::BookL1`].
pub struct CsvOhlcSource {
    instrument: InstrumentId,
    rows: Vec<OhlcRow>,
    idx: usize,
    half_spread_from_mid_bps: Decimal,
}

impl CsvOhlcSource {
    /// Iterate owned rows.
    pub fn from_rows(
        instrument: InstrumentId,
        rows: Vec<OhlcRow>,
        spread_bps: Decimal,
    ) -> Self {
        Self {
            instrument,
            rows,
            idx: 0,
            half_spread_from_mid_bps: spread_bps / Decimal::from(2u64),
        }
    }
}

impl HistoricalSource for CsvOhlcSource {
    fn next_event(&mut self) -> Option<Event> {
        let row = self.rows.get(self.idx)?;
        self.idx += 1;
        let ts = parse_ts(&row.ts).unwrap_or_else(|| OffsetDateTime::now_utc());
        let mid = row.close;
        let half_spread = mid * self.half_spread_from_mid_bps / Decimal::from(10_000u64);
        let bid = mid - half_spread;
        let ask = mid + half_spread;
        Some(Event::Market(MarketEvent::BookL1 {
            instrument: self.instrument.clone(),
            ts,
            bid,
            ask,
        }))
    }
}

/// Optional hook for custom microstructure models.
pub trait FillModel: Send {
    /// Label for logging.
    fn name(&self) -> &'static str;
}

/// Default touch-based fill assumption (fills use paper/sim gateways).
#[derive(Default)]
pub struct TouchCrossFillModel;

impl FillModel for TouchCrossFillModel {
    fn name(&self) -> &'static str {
        "touch_cross"
    }
}

pub use batch::{run_scenarios_parallel, run_scenarios_serial, RunReport, Scenario};
