//! Performance metrics from an equity curve.

use crate::types::EquityPoint;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;

/// Summary statistics after a run.
#[derive(Clone, Debug)]
pub struct PerformanceSummary {
    /// Final minus initial equity.
    pub pnl: Decimal,
    /// PnL / initial equity.
    pub pnl_pct: Decimal,
    /// Max drawdown as positive fraction (0..1).
    pub max_drawdown: f64,
    /// Annualized-ish Sharpe using per-step returns (252 bars ~ 1y if bars are daily).
    pub sharpe: f64,
    /// Sortino using downside deviation.
    pub sortino: f64,
    /// Per-point simple returns (length n-1).
    pub returns: Vec<f64>,
    /// Copy of equity series.
    pub equity: Vec<EquityPoint>,
}

/// Compute summary. `periods_per_year` scales Sharpe/Sortino (e.g. 252 for daily bars).
pub fn summarize(equity: Vec<EquityPoint>, periods_per_year: f64) -> PerformanceSummary {
    if equity.is_empty() {
        return PerformanceSummary {
            pnl: Decimal::ZERO,
            pnl_pct: Decimal::ZERO,
            max_drawdown: 0.0,
            sharpe: 0.0,
            sortino: 0.0,
            returns: vec![],
            equity,
        };
    }
    let pnl = equity[equity.len() - 1].equity_quote - equity[0].equity_quote;
    let pnl_pct = if equity[0].equity_quote.is_zero() {
        Decimal::ZERO
    } else {
        pnl / equity[0].equity_quote
    };

    let rets: Vec<f64> = equity
        .windows(2)
        .map(|w| {
            let a = w[0].equity_quote.to_f64().unwrap_or(1.0);
            let b = w[1].equity_quote.to_f64().unwrap_or(1.0);
            if a.abs() < 1e-12 {
                0.0
            } else {
                (b - a) / a
            }
        })
        .collect();

    let max_dd = max_drawdown(&equity);
    let sharpe = sharpe_ratio(&rets, periods_per_year);
    let sortino = sortino_ratio(&rets, periods_per_year);

    PerformanceSummary {
        pnl,
        pnl_pct,
        max_drawdown: max_dd,
        sharpe,
        sortino,
        returns: rets,
        equity,
    }
}

fn max_drawdown(equity: &[EquityPoint]) -> f64 {
    let mut peak = f64::MIN;
    let mut max_dd = 0.0f64;
    for pt in equity {
        let v = pt.equity_quote.to_f64().unwrap_or(0.0);
        peak = peak.max(v);
        if peak > 0.0 {
            let dd = (peak - v) / peak;
            max_dd = max_dd.max(dd);
        }
    }
    max_dd
}

fn mean(xs: &[f64]) -> f64 {
    if xs.is_empty() {
        return 0.0;
    }
    xs.iter().sum::<f64>() / xs.len() as f64
}

fn std_dev(xs: &[f64]) -> f64 {
    if xs.len() < 2 {
        return 0.0;
    }
    let m = mean(xs);
    let v: f64 = xs.iter().map(|x| (x - m).powi(2)).sum::<f64>() / (xs.len() as f64 - 1.0);
    v.sqrt()
}

fn downside_std(xs: &[f64]) -> f64 {
    let neg: Vec<f64> = xs.iter().copied().filter(|r| *r < 0.0).collect();
    if neg.len() < 2 {
        return 0.0;
    }
    let m = mean(&neg);
    let v: f64 = neg.iter().map(|x| (x - m).powi(2)).sum::<f64>() / (neg.len() as f64 - 1.0);
    v.sqrt()
}

fn sharpe_ratio(rets: &[f64], periods_per_year: f64) -> f64 {
    let m = mean(rets);
    let s = std_dev(rets);
    if s < 1e-12 {
        return 0.0;
    }
    (m / s) * periods_per_year.sqrt()
}

fn sortino_ratio(rets: &[f64], periods_per_year: f64) -> f64 {
    let m = mean(rets);
    let ds = downside_std(rets);
    if ds < 1e-12 {
        return 0.0;
    }
    (m / ds) * periods_per_year.sqrt()
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::OffsetDateTime;

    fn curve() -> Vec<EquityPoint> {
        let t0 = OffsetDateTime::UNIX_EPOCH;
        vec![
            EquityPoint {
                ts: t0,
                equity_quote: Decimal::from(100),
            },
            EquityPoint {
                ts: t0,
                equity_quote: Decimal::from(110),
            },
            EquityPoint {
                ts: t0,
                equity_quote: Decimal::from(105),
            },
            EquityPoint {
                ts: t0,
                equity_quote: Decimal::from(120),
            },
        ]
    }

    #[test]
    fn mdd_nonzero() {
        let s = summarize(curve(), 252.0);
        assert!(s.max_drawdown > 0.0);
    }

    #[test]
    fn pnl_matches() {
        let s = summarize(curve(), 252.0);
        assert_eq!(s.pnl, Decimal::from(20));
    }
}
