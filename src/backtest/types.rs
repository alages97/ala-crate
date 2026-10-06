use std::cmp::Ordering;
use std::collections::BTreeMap;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::types::Venue;

/// Stable identity for one historical market.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MarketId {
    pub venue: Venue,
    pub symbol: String,
}

impl MarketId {
    pub fn new(venue: Venue, symbol: impl Into<String>) -> Self {
        Self {
            venue,
            symbol: symbol.into(),
        }
    }
}

impl Ord for MarketId {
    fn cmp(&self, other: &Self) -> Ordering {
        venue_rank(self.venue)
            .cmp(&venue_rank(other.venue))
            .then_with(|| self.symbol.cmp(&other.symbol))
    }
}

impl PartialOrd for MarketId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

const fn venue_rank(venue: Venue) -> u8 {
    match venue {
        Venue::Binance => 0,
        Venue::Bybit => 1,
        Venue::Hyperliquid => 2,
        Venue::Lighter => 3,
    }
}

/// One completed OHLC bar plus point-in-time research attributes.
///
/// `available_at_ms` is when every field in this row became knowable. Optional
/// funding is the effective cash rate applied once at the end-of-bar valuation
/// mark: positive means longs pay shorts. `mark_price` falls back to `close`
/// when absent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Bar {
    pub market: MarketId,
    pub open_time_ms: u64,
    pub close_time_ms: u64,
    pub available_at_ms: u64,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub volume_usd: Option<Decimal>,
    pub open_interest_usd: Option<Decimal>,
    pub market_cap_usd: Option<Decimal>,
    pub mark_price: Option<Decimal>,
    pub funding_rate: Option<Decimal>,
    pub quantity_step: Option<Decimal>,
}

impl Bar {
    pub fn valuation_price(&self) -> Decimal {
        self.mark_price.unwrap_or(self.close)
    }
}

/// Every market row sharing one interval. Strategies are invoked once per
/// batch, so file ordering cannot bias a cross-sectional ranking.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BarBatch {
    pub index: usize,
    pub open_time_ms: u64,
    pub close_time_ms: u64,
    pub available_at_ms: u64,
    pub bars: Vec<Bar>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PositionSnapshot {
    pub market: MarketId,
    #[serde(with = "rust_decimal::serde::str")]
    pub quantity: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub average_entry_price: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub mark_price: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub unrealized_pnl: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub notional: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PortfolioSnapshot {
    pub timestamp_ms: u64,
    #[serde(with = "rust_decimal::serde::str")]
    pub collateral: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub equity: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub gross_exposure: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub net_exposure: Decimal,
    pub positions: Vec<PositionSnapshot>,
}

/// A complete desired portfolio expressed as signed fractions of equity.
/// Missing markets mean flat at the next executable open.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TargetBook {
    pub targets: BTreeMap<MarketId, Decimal>,
}

impl TargetBook {
    pub fn flat() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, market: MarketId, weight: Decimal) -> Option<Decimal> {
        self.targets.insert(market, weight)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StrategyDecision {
    Hold,
    Rebalance(TargetBook),
}

pub struct StrategyContext<'a> {
    pub batch: &'a BarBatch,
    pub portfolio: &'a PortfolioSnapshot,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum StrategyError {
    #[error("invalid strategy configuration: {0}")]
    InvalidConfig(String),
    #[error("strategy could not produce a target book: {0}")]
    Decision(String),
}

/// Pluggable research strategy. The context exposes only the current completed
/// batch and current portfolio; any rolling history must be accumulated from
/// prior calls, making future access impossible through this API.
pub trait Strategy {
    fn name(&self) -> &str;

    fn on_bar_close(
        &mut self,
        context: StrategyContext<'_>,
    ) -> Result<StrategyDecision, StrategyError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn market_order_is_stable_across_venues_and_symbols() {
        let mut markets = [
            MarketId::new(Venue::Lighter, "BTC"),
            MarketId::new(Venue::Hyperliquid, "ETH"),
            MarketId::new(Venue::Hyperliquid, "BTC"),
        ];
        markets.sort();
        assert_eq!(markets[0], MarketId::new(Venue::Hyperliquid, "BTC"));
        assert_eq!(markets[1], MarketId::new(Venue::Hyperliquid, "ETH"));
        assert_eq!(markets[2], MarketId::new(Venue::Lighter, "BTC"));
    }
}
