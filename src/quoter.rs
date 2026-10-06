//! Venue-neutral wide two-sided quoting logic.
//!
//! This module deliberately performs no I/O and calls no gateway. A main loop
//! feeds it locally timestamped BBOs and translates [`QuoteChange`] into order
//! manager commands. Keeping that boundary synchronous makes pricing and risk
//! decisions deterministic and straightforward to test.

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::market_data::Bbo;
use crate::types::{decimal_is_step_aligned, Instrument, Venue};

pub const METRIC_FAIR_VALUE_LATENCY: &str = "quoter_fair_value_latency_seconds";
pub const METRIC_QUOTE_DECISION_LATENCY: &str = "quoter_quote_decision_latency_seconds";
pub const METRIC_STALE_SOURCES: &str = "quoter_stale_source_total";
pub const METRIC_NO_QUOTE: &str = "quoter_no_quote_total";
pub const METRIC_REPLACE_RATE_LIMITED: &str = "quoter_replace_rate_limited_total";

const BASIS_POINTS_PER_UNIT: u32 = 10_000;

/// A market-data stream used to calculate fair value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReferenceSource {
    pub venue: Venue,
    /// Symbol exactly as it appears in the normalized BBO stream.
    pub symbol: String,
    #[serde(with = "rust_decimal::serde::str")]
    pub weight: Decimal,
}

/// Configuration for one target instrument.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QuoterSettings {
    pub instrument: Instrument,
    /// The normalized market-data symbol for the target venue. This is kept
    /// separate from `Instrument::venue_symbol`, which may be a numeric venue
    /// market ID or another execution-only identifier.
    pub target_market_data_symbol: String,
    pub reference_sources: Vec<ReferenceSource>,
    /// Freshness is based solely on the local [`Instant`] supplied when a BBO
    /// reaches this process, not on either remote wall-clock timestamp.
    pub max_source_age_ms: u64,
    pub min_fresh_sources: usize,
    #[serde(with = "rust_decimal::serde::str")]
    pub half_spread_bps: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub quote_quantity: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub max_order_notional: Decimal,
    /// A replace is requested when either rounded quote price has moved by at
    /// least this many basis points from the last requested quote.
    #[serde(with = "rust_decimal::serde::str")]
    pub reprice_threshold_bps: Decimal,
    /// Minimum monotonic time between order-increasing replace actions. This
    /// applies to ordinary reprices and to re-entry after quotes were canceled.
    /// Risk-reducing cancellation is never delayed by this guard.
    pub min_quote_interval_ms: u64,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum QuoterConfigError {
    #[error("instrument canonical_symbol must not be empty")]
    EmptyCanonicalSymbol,
    #[error("instrument venue_symbol must not be empty")]
    EmptyVenueSymbol,
    #[error("target market-data symbol must not be empty")]
    EmptyTargetSymbol,
    #[error("at least one reference source is required")]
    NoReferenceSources,
    #[error("reference source {venue}:{symbol} has a non-positive weight")]
    NonPositiveWeight { venue: Venue, symbol: String },
    #[error("reference source {venue}:{symbol} appears more than once")]
    DuplicateSource { venue: Venue, symbol: String },
    #[error("reference source {venue} has an empty symbol")]
    EmptyReferenceSymbol { venue: Venue },
    #[error("the target market-data source is not present in reference_sources")]
    MissingTargetSource,
    #[error("min_fresh_sources must be between 1 and the number of reference sources")]
    InvalidMinimumFreshSources,
    #[error("max_source_age_ms must be positive")]
    InvalidSourceAge,
    #[error("instrument price_tick must be positive")]
    InvalidPriceTick,
    #[error("instrument quantity_step must be positive")]
    InvalidQuantityStep,
    #[error("half_spread_bps must be non-negative and less than 10,000")]
    InvalidHalfSpread,
    #[error("quote_quantity must be positive and aligned to quantity_step")]
    InvalidQuoteQuantity,
    #[error("max_order_notional must be positive")]
    InvalidMaxOrderNotional,
    #[error("reprice_threshold_bps must be non-negative")]
    InvalidRepriceThreshold,
    #[error("min_quote_interval_ms must be positive")]
    InvalidQuoteInterval,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuoteLevel {
    pub price: Decimal,
    pub quantity: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DesiredQuotes {
    pub bid: QuoteLevel,
    pub ask: QuoteLevel,
}

/// The state transition a caller should apply to its order manager.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuoteChange {
    /// Existing desired orders remain unchanged.
    None,
    /// Submit/replace both sides with `QuoteDecision::desired`.
    Replace,
    /// Cancel both sides; `QuoteDecision::desired` is `None`.
    Cancel,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuoteDecisionReason {
    InitialQuote,
    RepriceThresholdMet,
    PricesUnchanged,
    BelowRepriceThreshold,
    MinimumQuoteIntervalNotElapsed,
    TargetSourceNotFresh,
    InsufficientFreshSources,
    MaxOrderNotionalExceeded,
    InvalidCalculatedQuote,
    ArithmeticFailure,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuoteDecision {
    /// Weighted fair value from all currently fresh configured sources. It can
    /// be present even when another guard prevents quoting.
    pub fair_value: Option<Decimal>,
    /// The desired state after applying this decision. On `QuoteChange::None`,
    /// this is the existing desired quote, if one exists.
    pub desired: Option<DesiredQuotes>,
    pub change: QuoteChange,
    pub reason: QuoteDecisionReason,
}

impl QuoteDecision {
    pub const fn should_update_orders(&self) -> bool {
        matches!(self.change, QuoteChange::Replace | QuoteChange::Cancel)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SourceKey {
    venue: Venue,
    symbol: String,
}

impl SourceKey {
    fn new(venue: Venue, symbol: impl Into<String>) -> Self {
        Self {
            venue,
            symbol: symbol.into(),
        }
    }

    fn from_bbo(bbo: &Bbo) -> Self {
        Self::new(bbo.venue, bbo.symbol.clone())
    }
}

#[derive(Debug, Clone)]
struct CachedBbo {
    bbo: Bbo,
    received_at: Instant,
}

#[derive(Debug)]
struct FairSnapshot {
    fair_value: Option<Decimal>,
    fresh_sources: usize,
    target_fresh: bool,
    arithmetic_failed: bool,
}

/// Stateful pricing engine for a single target instrument.
pub struct WideQuoter {
    settings: QuoterSettings,
    source_weights: HashMap<SourceKey, Decimal>,
    target_source: SourceKey,
    latest: HashMap<SourceKey, CachedBbo>,
    last_desired: Option<DesiredQuotes>,
    last_replace_at: Option<Instant>,
}

/// Concise alias for callers that do not need to distinguish quote styles.
pub type Quoter = WideQuoter;

impl WideQuoter {
    pub fn new(settings: QuoterSettings) -> Result<Self, QuoterConfigError> {
        validate_settings(&settings)?;

        let source_weights = settings
            .reference_sources
            .iter()
            .map(|source| {
                (
                    SourceKey::new(source.venue, source.symbol.clone()),
                    source.weight,
                )
            })
            .collect();
        let target_source = SourceKey::new(
            settings.instrument.venue,
            settings.target_market_data_symbol.clone(),
        );

        Ok(Self {
            settings,
            source_weights,
            target_source,
            latest: HashMap::new(),
            last_desired: None,
            last_replace_at: None,
        })
    }

    pub const fn settings(&self) -> &QuoterSettings {
        &self.settings
    }

    pub const fn current_desired(&self) -> Option<&DesiredQuotes> {
        self.last_desired.as_ref()
    }

    /// Cache a configured source BBO. Returns false for market data that is not
    /// explicitly listed in `reference_sources`.
    pub fn update_bbo(&mut self, bbo: Bbo, received_at: Instant) -> bool {
        let key = SourceKey::from_bbo(&bbo);
        if !self.source_weights.contains_key(&key) {
            return false;
        }

        if self
            .latest
            .get(&key)
            .is_some_and(|existing| existing.received_at > received_at)
        {
            // Protect the local freshness clock from an accidentally reordered
            // caller. Sequence ordering is enforced at the market-data edge.
            return false;
        }

        self.latest.insert(key, CachedBbo { bbo, received_at });
        true
    }

    /// Consume one BBO and immediately evaluate at its local receive instant.
    /// Unconfigured `(venue, symbol)` streams are ignored and return `None`.
    pub fn on_bbo(&mut self, bbo: Bbo, received_at: Instant) -> Option<QuoteDecision> {
        self.update_bbo(bbo, received_at)
            .then(|| self.evaluate(received_at))
    }

    /// Evaluate freshness and desired orders at a caller-provided local time.
    /// Calling this from a periodic timer withdraws quotes even if the market-
    /// data socket becomes silent and no new BBO arrives.
    pub fn evaluate(&mut self, now: Instant) -> QuoteDecision {
        let decision_started = Instant::now();
        let fair = self.compute_fair(now);

        let decision = if fair.arithmetic_failed {
            self.no_quote(fair.fair_value, QuoteDecisionReason::ArithmeticFailure)
        } else if !fair.target_fresh {
            self.no_quote(fair.fair_value, QuoteDecisionReason::TargetSourceNotFresh)
        } else if fair.fresh_sources < self.settings.min_fresh_sources {
            self.no_quote(
                fair.fair_value,
                QuoteDecisionReason::InsufficientFreshSources,
            )
        } else {
            match fair
                .fair_value
                .and_then(|fair_value| self.calculate_quotes(fair_value).map(|q| (fair_value, q)))
            {
                Some((fair_value, quotes)) => {
                    let bid_notional = quotes.bid.price.checked_mul(quotes.bid.quantity);
                    let ask_notional = quotes.ask.price.checked_mul(quotes.ask.quantity);
                    match (bid_notional, ask_notional) {
                        (Some(bid), Some(ask))
                            if bid <= self.settings.max_order_notional
                                && ask <= self.settings.max_order_notional =>
                        {
                            self.apply_reprice_policy(fair_value, quotes, now)
                        }
                        (Some(_), Some(_)) => self.no_quote(
                            Some(fair_value),
                            QuoteDecisionReason::MaxOrderNotionalExceeded,
                        ),
                        _ => {
                            self.no_quote(Some(fair_value), QuoteDecisionReason::ArithmeticFailure)
                        }
                    }
                }
                None => self.no_quote(fair.fair_value, QuoteDecisionReason::InvalidCalculatedQuote),
            }
        };

        metrics::histogram!(
            METRIC_QUOTE_DECISION_LATENCY,
            "venue" => self.settings.instrument.venue.to_string(),
            "symbol" => self.settings.target_market_data_symbol.clone()
        )
        .record(decision_started.elapsed().as_secs_f64());
        decision
    }

    fn compute_fair(&self, now: Instant) -> FairSnapshot {
        let started = Instant::now();
        let max_age = Duration::from_millis(self.settings.max_source_age_ms);
        let mut weighted_sum = Decimal::ZERO;
        let mut total_weight = Decimal::ZERO;
        let mut fresh_sources = 0usize;
        let mut target_fresh = false;
        let mut arithmetic_failed = false;

        for (key, weight) in &self.source_weights {
            let cached = self.latest.get(key);
            let freshness = match cached {
                None => SourceFreshness::Missing,
                Some(cached)
                    if now
                        .checked_duration_since(cached.received_at)
                        .unwrap_or(Duration::ZERO)
                        <= max_age =>
                {
                    SourceFreshness::Fresh(cached)
                }
                Some(_) => SourceFreshness::Expired,
            };

            let cached = match freshness {
                SourceFreshness::Fresh(cached) => cached,
                SourceFreshness::Missing => {
                    self.record_stale_source(key, "missing");
                    continue;
                }
                SourceFreshness::Expired => {
                    self.record_stale_source(key, "expired");
                    continue;
                }
            };

            fresh_sources += 1;
            if key == &self.target_source {
                target_fresh = true;
            }

            let contribution = cached
                .bbo
                .bid_price
                .checked_add(cached.bbo.ask_price)
                .and_then(|sum| sum.checked_div(Decimal::TWO))
                .and_then(|mid| mid.checked_mul(*weight));
            let next_sum = contribution.and_then(|value| weighted_sum.checked_add(value));
            let next_weight = total_weight.checked_add(*weight);
            match (next_sum, next_weight) {
                (Some(sum), Some(weight_sum)) => {
                    weighted_sum = sum;
                    total_weight = weight_sum;
                }
                _ => {
                    arithmetic_failed = true;
                    break;
                }
            }
        }

        let fair_value = if arithmetic_failed || total_weight <= Decimal::ZERO {
            None
        } else {
            weighted_sum.checked_div(total_weight)
        };
        if fair_value.is_none() && total_weight > Decimal::ZERO {
            arithmetic_failed = true;
        }

        metrics::histogram!(
            METRIC_FAIR_VALUE_LATENCY,
            "venue" => self.settings.instrument.venue.to_string(),
            "symbol" => self.settings.target_market_data_symbol.clone()
        )
        .record(started.elapsed().as_secs_f64());

        FairSnapshot {
            fair_value,
            fresh_sources,
            target_fresh,
            arithmetic_failed,
        }
    }

    fn calculate_quotes(&self, fair_value: Decimal) -> Option<DesiredQuotes> {
        let one = Decimal::ONE;
        let spread_fraction = self
            .settings
            .half_spread_bps
            .checked_div(Decimal::from(BASIS_POINTS_PER_UNIT))?;
        let bid_multiplier = one.checked_sub(spread_fraction)?;
        let ask_multiplier = one.checked_add(spread_fraction)?;
        let raw_bid = fair_value.checked_mul(bid_multiplier)?;
        let raw_ask = fair_value.checked_mul(ask_multiplier)?;
        let bid_price = floor_to_tick(raw_bid, self.settings.instrument.price_tick)?;
        let ask_price = ceil_to_tick(raw_ask, self.settings.instrument.price_tick)?;

        if bid_price <= Decimal::ZERO || ask_price <= Decimal::ZERO || bid_price > ask_price {
            return None;
        }

        Some(DesiredQuotes {
            bid: QuoteLevel {
                price: bid_price,
                quantity: self.settings.quote_quantity,
            },
            ask: QuoteLevel {
                price: ask_price,
                quantity: self.settings.quote_quantity,
            },
        })
    }

    fn apply_reprice_policy(
        &mut self,
        fair_value: Decimal,
        candidate: DesiredQuotes,
        now: Instant,
    ) -> QuoteDecision {
        let Some(current) = self.last_desired.as_ref() else {
            return self.apply_replace_guard(
                fair_value,
                candidate,
                now,
                QuoteDecisionReason::InitialQuote,
            );
        };

        if current == &candidate {
            return QuoteDecision {
                fair_value: Some(fair_value),
                desired: Some(current.clone()),
                change: QuoteChange::None,
                reason: QuoteDecisionReason::PricesUnchanged,
            };
        }

        let bid_move = price_move_bps(current.bid.price, candidate.bid.price);
        let ask_move = price_move_bps(current.ask.price, candidate.ask.price);
        let threshold_met = match (bid_move, ask_move) {
            (Some(bid), Some(ask)) => bid.max(ask) >= self.settings.reprice_threshold_bps,
            _ => return self.no_quote(Some(fair_value), QuoteDecisionReason::ArithmeticFailure),
        };

        if threshold_met {
            self.apply_replace_guard(
                fair_value,
                candidate,
                now,
                QuoteDecisionReason::RepriceThresholdMet,
            )
        } else {
            QuoteDecision {
                fair_value: Some(fair_value),
                desired: Some(current.clone()),
                change: QuoteChange::None,
                reason: QuoteDecisionReason::BelowRepriceThreshold,
            }
        }
    }

    fn apply_replace_guard(
        &mut self,
        fair_value: Decimal,
        candidate: DesiredQuotes,
        now: Instant,
        replace_reason: QuoteDecisionReason,
    ) -> QuoteDecision {
        let interval = Duration::from_millis(self.settings.min_quote_interval_ms);
        let interval_elapsed = self.last_replace_at.is_none_or(|last_replace_at| {
            now.checked_duration_since(last_replace_at)
                .is_some_and(|elapsed| elapsed >= interval)
        });

        if !interval_elapsed {
            metrics::counter!(
                METRIC_REPLACE_RATE_LIMITED,
                "venue" => self.settings.instrument.venue.to_string(),
                "symbol" => self.settings.target_market_data_symbol.clone(),
                "reason" => quote_decision_metric_reason(
                    QuoteDecisionReason::MinimumQuoteIntervalNotElapsed
                )
            )
            .increment(1);
            // A materially changed quote must not remain live merely because
            // its replacement is paced. Pull it immediately, then stay flat
            // until a fresh candidate may be submitted at the interval
            // boundary.
            let change = if self.last_desired.take().is_some() {
                QuoteChange::Cancel
            } else {
                QuoteChange::None
            };
            return QuoteDecision {
                fair_value: Some(fair_value),
                desired: None,
                change,
                reason: QuoteDecisionReason::MinimumQuoteIntervalNotElapsed,
            };
        }

        self.last_desired = Some(candidate.clone());
        self.last_replace_at = Some(now);
        QuoteDecision {
            fair_value: Some(fair_value),
            desired: Some(candidate),
            change: QuoteChange::Replace,
            reason: replace_reason,
        }
    }

    fn no_quote(
        &mut self,
        fair_value: Option<Decimal>,
        reason: QuoteDecisionReason,
    ) -> QuoteDecision {
        metrics::counter!(
            METRIC_NO_QUOTE,
            "venue" => self.settings.instrument.venue.to_string(),
            "symbol" => self.settings.target_market_data_symbol.clone(),
            "reason" => quote_decision_metric_reason(reason)
        )
        .increment(1);

        let change = if self.last_desired.take().is_some() {
            QuoteChange::Cancel
        } else {
            QuoteChange::None
        };
        QuoteDecision {
            fair_value,
            desired: None,
            change,
            reason,
        }
    }

    fn record_stale_source(&self, source: &SourceKey, reason: &'static str) {
        metrics::counter!(
            METRIC_STALE_SOURCES,
            "target_venue" => self.settings.instrument.venue.to_string(),
            "source_venue" => source.venue.to_string(),
            "source_symbol" => source.symbol.clone(),
            "reason" => reason
        )
        .increment(1);
    }
}

enum SourceFreshness<'a> {
    Missing,
    Fresh(&'a CachedBbo),
    Expired,
}

fn validate_settings(settings: &QuoterSettings) -> Result<(), QuoterConfigError> {
    if settings.instrument.canonical_symbol.trim().is_empty() {
        return Err(QuoterConfigError::EmptyCanonicalSymbol);
    }
    if settings.instrument.venue_symbol.trim().is_empty() {
        return Err(QuoterConfigError::EmptyVenueSymbol);
    }
    if settings.target_market_data_symbol.trim().is_empty() {
        return Err(QuoterConfigError::EmptyTargetSymbol);
    }
    if settings.reference_sources.is_empty() {
        return Err(QuoterConfigError::NoReferenceSources);
    }
    if settings.min_fresh_sources == 0
        || settings.min_fresh_sources > settings.reference_sources.len()
    {
        return Err(QuoterConfigError::InvalidMinimumFreshSources);
    }
    if settings.max_source_age_ms == 0 {
        return Err(QuoterConfigError::InvalidSourceAge);
    }
    if settings.instrument.price_tick <= Decimal::ZERO {
        return Err(QuoterConfigError::InvalidPriceTick);
    }
    if settings.instrument.quantity_step <= Decimal::ZERO {
        return Err(QuoterConfigError::InvalidQuantityStep);
    }
    if settings.half_spread_bps < Decimal::ZERO
        || settings.half_spread_bps >= Decimal::from(BASIS_POINTS_PER_UNIT)
    {
        return Err(QuoterConfigError::InvalidHalfSpread);
    }
    if settings.quote_quantity <= Decimal::ZERO
        || !decimal_is_step_aligned(settings.quote_quantity, settings.instrument.quantity_step)
    {
        return Err(QuoterConfigError::InvalidQuoteQuantity);
    }
    if settings.max_order_notional <= Decimal::ZERO {
        return Err(QuoterConfigError::InvalidMaxOrderNotional);
    }
    if settings.reprice_threshold_bps < Decimal::ZERO {
        return Err(QuoterConfigError::InvalidRepriceThreshold);
    }
    if settings.min_quote_interval_ms == 0 {
        return Err(QuoterConfigError::InvalidQuoteInterval);
    }

    let mut keys = HashSet::with_capacity(settings.reference_sources.len());
    for source in &settings.reference_sources {
        if source.symbol.trim().is_empty() {
            return Err(QuoterConfigError::EmptyReferenceSymbol {
                venue: source.venue,
            });
        }
        if source.weight <= Decimal::ZERO {
            return Err(QuoterConfigError::NonPositiveWeight {
                venue: source.venue,
                symbol: source.symbol.clone(),
            });
        }
        if !keys.insert(SourceKey::new(source.venue, source.symbol.clone())) {
            return Err(QuoterConfigError::DuplicateSource {
                venue: source.venue,
                symbol: source.symbol.clone(),
            });
        }
    }

    let target = SourceKey::new(
        settings.instrument.venue,
        settings.target_market_data_symbol.clone(),
    );
    if !keys.contains(&target) {
        return Err(QuoterConfigError::MissingTargetSource);
    }
    Ok(())
}

fn floor_to_tick(value: Decimal, tick: Decimal) -> Option<Decimal> {
    value.checked_div(tick)?.floor().checked_mul(tick)
}

fn ceil_to_tick(value: Decimal, tick: Decimal) -> Option<Decimal> {
    value.checked_div(tick)?.ceil().checked_mul(tick)
}

fn price_move_bps(previous: Decimal, candidate: Decimal) -> Option<Decimal> {
    candidate
        .checked_sub(previous)?
        .abs()
        .checked_div(previous)?
        .checked_mul(Decimal::from(BASIS_POINTS_PER_UNIT))
}

fn quote_decision_metric_reason(reason: QuoteDecisionReason) -> &'static str {
    match reason {
        QuoteDecisionReason::InitialQuote => "initial_quote",
        QuoteDecisionReason::RepriceThresholdMet => "reprice_threshold_met",
        QuoteDecisionReason::PricesUnchanged => "prices_unchanged",
        QuoteDecisionReason::BelowRepriceThreshold => "below_reprice_threshold",
        QuoteDecisionReason::MinimumQuoteIntervalNotElapsed => "min_quote_interval",
        QuoteDecisionReason::TargetSourceNotFresh => "target_source_not_fresh",
        QuoteDecisionReason::InsufficientFreshSources => "insufficient_fresh_sources",
        QuoteDecisionReason::MaxOrderNotionalExceeded => "max_order_notional",
        QuoteDecisionReason::InvalidCalculatedQuote => "invalid_calculated_quote",
        QuoteDecisionReason::ArithmeticFailure => "arithmetic_failure",
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use pretty_assertions::assert_eq;

    use crate::market_data::{MarketDataEnvironment, MARKET_DATA_SCHEMA_VERSION};

    use super::*;

    fn d(value: &str) -> Decimal {
        Decimal::from_str(value).unwrap()
    }

    fn instrument() -> Instrument {
        Instrument {
            venue: Venue::Hyperliquid,
            canonical_symbol: "BTC-USD-PERP".to_owned(),
            venue_symbol: "BTC".to_owned(),
            price_tick: d("0.1"),
            quantity_step: d("0.00001"),
        }
    }

    fn settings() -> QuoterSettings {
        QuoterSettings {
            instrument: instrument(),
            target_market_data_symbol: "BTC-USD-PERP".to_owned(),
            reference_sources: vec![
                ReferenceSource {
                    venue: Venue::Hyperliquid,
                    symbol: "BTC-USD-PERP".to_owned(),
                    weight: d("1"),
                },
                ReferenceSource {
                    venue: Venue::Binance,
                    symbol: "BTC-USDT-PERP".to_owned(),
                    weight: d("3"),
                },
            ],
            max_source_age_ms: 100,
            min_fresh_sources: 2,
            half_spread_bps: d("100"),
            quote_quantity: d("0.01000"),
            max_order_notional: d("1000000"),
            reprice_threshold_bps: d("20"),
            min_quote_interval_ms: 10,
        }
    }

    fn bbo(venue: Venue, symbol: &str, bid: &str, ask: &str) -> Bbo {
        Bbo {
            schema_version: MARKET_DATA_SCHEMA_VERSION,
            environment: MarketDataEnvironment::Live,
            venue,
            symbol: symbol.to_owned(),
            bid_price: d(bid),
            bid_qty: d("1"),
            ask_price: d(ask),
            ask_qty: d("1"),
            // Deliberately unrelated to the local Instant used for freshness.
            exchange_timestamp_ms: 1,
            publisher_timestamp_ms: u64::MAX,
            sequence: 1,
            stream_id: "test".to_owned(),
        }
    }

    fn seed_two_sources(quoter: &mut WideQuoter, now: Instant) -> QuoteDecision {
        assert!(quoter.update_bbo(
            bbo(Venue::Hyperliquid, "BTC-USD-PERP", "99.9", "100.1"),
            now,
        ));
        quoter
            .on_bbo(bbo(Venue::Binance, "BTC-USDT-PERP", "109.9", "110.1"), now)
            .unwrap()
    }

    #[test]
    fn computes_weighted_mid_and_rounds_outward() {
        let mut quoter = WideQuoter::new(settings()).unwrap();
        let decision = seed_two_sources(&mut quoter, Instant::now());

        // (100 * 1 + 110 * 3) / 4 = 107.5. A 1% half-spread gives
        // 106.425 / 108.575, rounded away from fair to the 0.1 tick.
        assert_eq!(decision.fair_value, Some(d("107.5")));
        assert_eq!(decision.change, QuoteChange::Replace);
        assert_eq!(decision.reason, QuoteDecisionReason::InitialQuote);
        assert_eq!(
            decision.desired,
            Some(DesiredQuotes {
                bid: QuoteLevel {
                    price: d("106.4"),
                    quantity: d("0.01000"),
                },
                ask: QuoteLevel {
                    price: d("108.6"),
                    quantity: d("0.01000"),
                },
            })
        );
    }

    #[test]
    fn ignores_unconfigured_venue_symbol_pairs() {
        let mut quoter = WideQuoter::new(settings()).unwrap();
        let ignored = quoter.on_bbo(
            bbo(Venue::Bybit, "BTC-USDT-PERP", "100", "101"),
            Instant::now(),
        );

        assert_eq!(ignored, None);
        assert_eq!(quoter.current_desired(), None);
    }

    #[test]
    fn freshness_uses_local_receive_instant_and_cancels_only_once() {
        let mut quoter = WideQuoter::new(settings()).unwrap();
        let received_at = Instant::now();
        let initial = seed_two_sources(&mut quoter, received_at);
        assert_eq!(initial.change, QuoteChange::Replace);

        let stale = quoter.evaluate(received_at + Duration::from_millis(101));
        assert_eq!(stale.reason, QuoteDecisionReason::TargetSourceNotFresh);
        assert_eq!(stale.change, QuoteChange::Cancel);
        assert_eq!(stale.desired, None);

        let still_stale = quoter.evaluate(received_at + Duration::from_millis(102));
        assert_eq!(still_stale.change, QuoteChange::None);
        assert_eq!(still_stale.desired, None);
    }

    #[test]
    fn target_must_be_fresh_even_when_minimum_is_met_elsewhere() {
        let mut config = settings();
        config.reference_sources.push(ReferenceSource {
            venue: Venue::Bybit,
            symbol: "BTC-USDT-PERP".to_owned(),
            weight: Decimal::ONE,
        });
        let mut quoter = WideQuoter::new(config).unwrap();
        let now = Instant::now();
        quoter.update_bbo(bbo(Venue::Binance, "BTC-USDT-PERP", "100", "101"), now);
        quoter.update_bbo(bbo(Venue::Bybit, "BTC-USDT-PERP", "100", "101"), now);

        let decision = quoter.evaluate(now);
        assert_eq!(decision.reason, QuoteDecisionReason::TargetSourceNotFresh);
        assert_eq!(decision.change, QuoteChange::None);
    }

    #[test]
    fn minimum_fresh_source_guard_is_independent() {
        let mut quoter = WideQuoter::new(settings()).unwrap();
        let now = Instant::now();
        quoter.update_bbo(bbo(Venue::Hyperliquid, "BTC-USD-PERP", "100", "101"), now);

        let decision = quoter.evaluate(now);
        assert_eq!(
            decision.reason,
            QuoteDecisionReason::InsufficientFreshSources
        );
        assert_eq!(decision.change, QuoteChange::None);
    }

    #[test]
    fn max_notional_suppresses_instead_of_resizing_exact_quantity() {
        let mut config = settings();
        config.max_order_notional = d("1.00");
        let mut quoter = WideQuoter::new(config).unwrap();
        let decision = seed_two_sources(&mut quoter, Instant::now());

        assert_eq!(
            decision.reason,
            QuoteDecisionReason::MaxOrderNotionalExceeded
        );
        assert_eq!(decision.desired, None);
        assert_eq!(decision.change, QuoteChange::None);
    }

    #[test]
    fn reprice_requires_threshold_after_tick_rounding() {
        let mut quoter = WideQuoter::new(settings()).unwrap();
        let now = Instant::now();
        let initial = seed_two_sources(&mut quoter, now);
        let initial_quotes = initial.desired.unwrap();

        // This moves the weighted fair enough to change a rounded price, but
        // by less than the configured 20 bps replacement threshold.
        let below = quoter
            .on_bbo(
                bbo(Venue::Binance, "BTC-USDT-PERP", "110.1", "110.3"),
                now + Duration::from_millis(1),
            )
            .unwrap();
        assert_eq!(below.reason, QuoteDecisionReason::BelowRepriceThreshold);
        assert_eq!(below.change, QuoteChange::None);
        assert_eq!(below.desired, Some(initial_quotes.clone()));

        let replace = quoter
            .on_bbo(
                bbo(Venue::Binance, "BTC-USDT-PERP", "111.9", "112.1"),
                now + Duration::from_millis(10),
            )
            .unwrap();
        assert_eq!(replace.reason, QuoteDecisionReason::RepriceThresholdMet);
        assert_eq!(replace.change, QuoteChange::Replace);
        assert_ne!(replace.desired, Some(initial_quotes));
    }

    #[test]
    fn replace_actions_wait_for_the_monotonic_interval_boundary() {
        let mut config = settings();
        config.min_quote_interval_ms = 10;
        let mut quoter = WideQuoter::new(config).unwrap();
        let now = Instant::now();
        let initial = seed_two_sources(&mut quoter, now);
        let initial_quotes = initial.desired.unwrap();

        let limited = quoter
            .on_bbo(
                bbo(Venue::Binance, "BTC-USDT-PERP", "111.9", "112.1"),
                now + Duration::from_millis(2),
            )
            .unwrap();
        assert_eq!(limited.change, QuoteChange::Cancel);
        assert_eq!(
            limited.reason,
            QuoteDecisionReason::MinimumQuoteIntervalNotElapsed
        );
        assert_eq!(limited.desired, None);
        assert_eq!(quoter.current_desired(), None);

        let still_limited = quoter
            .on_bbo(
                bbo(Venue::Binance, "BTC-USDT-PERP", "112.9", "113.1"),
                now + Duration::from_millis(3),
            )
            .unwrap();
        assert_eq!(still_limited.change, QuoteChange::None);
        assert_eq!(still_limited.desired, None);
        assert_eq!(
            still_limited.reason,
            QuoteDecisionReason::MinimumQuoteIntervalNotElapsed
        );

        let at_boundary = quoter
            .on_bbo(
                bbo(Venue::Binance, "BTC-USDT-PERP", "111.9", "112.1"),
                now + Duration::from_millis(10),
            )
            .unwrap();
        assert_eq!(at_boundary.change, QuoteChange::Replace);
        assert_eq!(at_boundary.reason, QuoteDecisionReason::InitialQuote);
        assert_ne!(at_boundary.desired, Some(initial_quotes));
    }

    #[test]
    fn stale_fresh_flapping_cancels_immediately_but_does_not_bypass_rate_guard() {
        let mut config = settings();
        config.min_quote_interval_ms = 1_000;
        let mut quoter = WideQuoter::new(config).unwrap();
        let now = Instant::now();

        assert_eq!(
            seed_two_sources(&mut quoter, now).change,
            QuoteChange::Replace
        );

        let stale = quoter.evaluate(now + Duration::from_millis(101));
        assert_eq!(stale.change, QuoteChange::Cancel);
        assert_eq!(stale.reason, QuoteDecisionReason::TargetSourceNotFresh);

        for refreshed_at_ms in [102, 204] {
            let refreshed_at = now + Duration::from_millis(refreshed_at_ms);
            assert!(quoter.update_bbo(
                bbo(Venue::Hyperliquid, "BTC-USD-PERP", "99.9", "100.1"),
                refreshed_at,
            ));
            assert!(quoter.update_bbo(
                bbo(Venue::Binance, "BTC-USDT-PERP", "109.9", "110.1"),
                refreshed_at,
            ));

            let suppressed_reentry = quoter.evaluate(refreshed_at);
            assert_eq!(suppressed_reentry.change, QuoteChange::None);
            assert_eq!(suppressed_reentry.desired, None);
            assert_eq!(
                suppressed_reentry.reason,
                QuoteDecisionReason::MinimumQuoteIntervalNotElapsed
            );

            let stale_again = quoter.evaluate(refreshed_at + Duration::from_millis(101));
            assert_eq!(stale_again.change, QuoteChange::None);
            assert_eq!(stale_again.desired, None);
            assert_eq!(
                stale_again.reason,
                QuoteDecisionReason::TargetSourceNotFresh
            );
        }

        let boundary = now + Duration::from_millis(1_000);
        assert!(quoter.update_bbo(
            bbo(Venue::Hyperliquid, "BTC-USD-PERP", "99.9", "100.1"),
            boundary,
        ));
        assert!(quoter.update_bbo(
            bbo(Venue::Binance, "BTC-USDT-PERP", "109.9", "110.1"),
            boundary,
        ));
        let reentered = quoter.evaluate(boundary);
        assert_eq!(reentered.change, QuoteChange::Replace);
        assert_eq!(reentered.reason, QuoteDecisionReason::InitialQuote);
        assert!(reentered.desired.is_some());
    }

    #[test]
    fn validates_exact_quantity_and_target_source() {
        let mut config = settings();
        config.quote_quantity = d("0.000015");
        assert!(matches!(
            WideQuoter::new(config),
            Err(QuoterConfigError::InvalidQuoteQuantity)
        ));

        let mut config = settings();
        config.reference_sources[0].symbol = "OTHER".to_owned();
        assert!(matches!(
            WideQuoter::new(config),
            Err(QuoterConfigError::MissingTargetSource)
        ));
    }

    #[test]
    fn rejects_zero_quote_interval_and_exposes_stable_metric_reason() {
        let mut config = settings();
        config.min_quote_interval_ms = 0;
        assert!(matches!(
            WideQuoter::new(config),
            Err(QuoterConfigError::InvalidQuoteInterval)
        ));
        assert_eq!(
            quote_decision_metric_reason(QuoteDecisionReason::MinimumQuoteIntervalNotElapsed),
            "min_quote_interval"
        );
    }
}
