use std::collections::{BTreeMap, BTreeSet};

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use super::indicators::{Ema, RollingAverage, RollingReturn, WilderRsi};
use super::types::{
    Bar, MarketId, Strategy, StrategyContext, StrategyDecision, StrategyError, TargetBook,
};
use crate::types::Venue;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MomentumConfig {
    pub venue: Venue,
    pub fast_ema_period: usize,
    pub slow_ema_period: usize,
    pub rsi_period: usize,
    pub short_return_lookback_bars: usize,
    pub long_return_lookback_bars: usize,
    pub rebalance_every_bars: usize,
    pub min_eligible_assets: usize,
    #[serde(default, with = "rust_decimal::serde::str_option")]
    pub min_open_interest_usd: Option<Decimal>,
    #[serde(default, with = "rust_decimal::serde::str_option")]
    pub min_market_cap_usd: Option<Decimal>,
    #[serde(default, with = "rust_decimal::serde::str_option")]
    pub min_average_volume_usd: Option<Decimal>,
    pub volume_lookback_bars: usize,
    pub long_count: usize,
    pub short_count: usize,
    pub min_positions_per_side: usize,
    #[serde(with = "rust_decimal::serde::str")]
    pub long_gross: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub short_gross: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub max_asset_weight: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub long_rsi_min: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub short_rsi_max: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub short_return_weight: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub long_return_weight: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub ema_gap_weight: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub rsi_weight: Decimal,
    pub require_both_sides: bool,
}

impl Default for MomentumConfig {
    fn default() -> Self {
        Self {
            venue: Venue::Hyperliquid,
            fast_ema_period: 20,
            slow_ema_period: 50,
            rsi_period: 14,
            short_return_lookback_bars: 7,
            long_return_lookback_bars: 30,
            rebalance_every_bars: 1,
            min_eligible_assets: 20,
            min_open_interest_usd: Some(Decimal::from(5_000_000)),
            min_market_cap_usd: None,
            min_average_volume_usd: Some(Decimal::from(10_000_000)),
            volume_lookback_bars: 7,
            long_count: 4,
            short_count: 4,
            min_positions_per_side: 4,
            long_gross: Decimal::new(5, 1),
            short_gross: Decimal::new(5, 1),
            max_asset_weight: Decimal::new(125, 3),
            long_rsi_min: Decimal::from(55),
            short_rsi_max: Decimal::from(45),
            short_return_weight: Decimal::new(45, 2),
            long_return_weight: Decimal::new(30, 2),
            ema_gap_weight: Decimal::new(15, 2),
            rsi_weight: Decimal::new(10, 2),
            require_both_sides: true,
        }
    }
}

impl MomentumConfig {
    pub fn validate(&self) -> Result<(), StrategyError> {
        if self.venue != Venue::Hyperliquid {
            return invalid("momentum strategy currently supports only hyperliquid");
        }
        if self.fast_ema_period == 0 {
            return invalid("fast_ema_period must be positive");
        }
        if self.slow_ema_period <= self.fast_ema_period {
            return invalid("slow_ema_period must be greater than fast_ema_period");
        }
        if self.rsi_period == 0 {
            return invalid("rsi_period must be positive");
        }
        if self.short_return_lookback_bars == 0 {
            return invalid("short_return_lookback_bars must be positive");
        }
        if self.long_return_lookback_bars <= self.short_return_lookback_bars {
            return invalid(
                "long_return_lookback_bars must be greater than short_return_lookback_bars",
            );
        }
        if self.rebalance_every_bars == 0 {
            return invalid("rebalance_every_bars must be positive");
        }
        if self.volume_lookback_bars == 0 {
            return invalid("volume_lookback_bars must be positive");
        }
        if self.min_eligible_assets == 0 {
            return invalid("min_eligible_assets must be positive");
        }
        if self.long_count == 0 || self.short_count == 0 {
            return invalid("long_count and short_count must be positive");
        }
        if self.min_positions_per_side == 0 {
            return invalid("min_positions_per_side must be positive");
        }
        if self.min_positions_per_side > self.long_count
            || self.min_positions_per_side > self.short_count
        {
            return invalid("min_positions_per_side cannot exceed long_count or short_count");
        }
        let requested_positions =
            self.long_count
                .checked_add(self.short_count)
                .ok_or_else(|| {
                    StrategyError::InvalidConfig("long_count + short_count overflows".to_owned())
                })?;
        if requested_positions > self.min_eligible_assets {
            return invalid("min_eligible_assets must be at least long_count + short_count");
        }

        for (name, value) in [
            ("min_open_interest_usd", self.min_open_interest_usd),
            ("min_market_cap_usd", self.min_market_cap_usd),
            ("min_average_volume_usd", self.min_average_volume_usd),
        ] {
            if value.is_some_and(|minimum| minimum < Decimal::ZERO) {
                return invalid(format!("{name} must be nonnegative when configured"));
            }
        }

        if self.long_gross <= Decimal::ZERO || self.short_gross <= Decimal::ZERO {
            return invalid("long_gross and short_gross must be positive");
        }
        if self.max_asset_weight <= Decimal::ZERO {
            return invalid("max_asset_weight must be positive");
        }

        let hundred = Decimal::ONE_HUNDRED;
        if self.short_rsi_max < Decimal::ZERO || self.short_rsi_max > hundred {
            return invalid("short_rsi_max must be between 0 and 100");
        }
        if self.long_rsi_min < Decimal::ZERO || self.long_rsi_min > hundred {
            return invalid("long_rsi_min must be between 0 and 100");
        }
        if self.short_rsi_max >= self.long_rsi_min {
            return invalid("short_rsi_max must be less than long_rsi_min");
        }

        let score_weights = [
            ("short_return_weight", self.short_return_weight),
            ("long_return_weight", self.long_return_weight),
            ("ema_gap_weight", self.ema_gap_weight),
            ("rsi_weight", self.rsi_weight),
        ];
        if let Some((name, _)) = score_weights
            .iter()
            .find(|(_, weight)| *weight < Decimal::ZERO)
        {
            return invalid(format!("{name} must be nonnegative"));
        }
        if score_weights
            .iter()
            .map(|(_, weight)| *weight)
            .sum::<Decimal>()
            .is_zero()
        {
            return invalid("at least one score weight must be positive");
        }

        Ok(())
    }
}

fn invalid<T>(message: impl Into<String>) -> Result<T, StrategyError> {
    Err(StrategyError::InvalidConfig(message.into()))
}

#[derive(Debug, Clone)]
struct MarketState {
    fast_ema: Ema,
    slow_ema: Ema,
    rsi: WilderRsi,
    short_return: RollingReturn,
    long_return: RollingReturn,
    average_volume: RollingAverage,
    last_batch_index: Option<usize>,
    last_close_time_ms: Option<u64>,
}

impl MarketState {
    fn new(config: &MomentumConfig) -> Self {
        Self {
            fast_ema: Ema::new(config.fast_ema_period).expect("validated EMA period"),
            slow_ema: Ema::new(config.slow_ema_period).expect("validated EMA period"),
            rsi: WilderRsi::new(config.rsi_period).expect("validated RSI period"),
            short_return: RollingReturn::new(config.short_return_lookback_bars)
                .expect("validated return lookback"),
            long_return: RollingReturn::new(config.long_return_lookback_bars)
                .expect("validated return lookback"),
            average_volume: RollingAverage::new(config.volume_lookback_bars)
                .expect("validated volume lookback"),
            last_batch_index: None,
            last_close_time_ms: None,
        }
    }

    fn reset(&mut self) {
        self.fast_ema.reset();
        self.slow_ema.reset();
        self.rsi.reset();
        self.short_return.reset();
        self.long_return.reset();
        self.average_volume.reset();
        self.last_batch_index = None;
        self.last_close_time_ms = None;
    }

    fn update(&mut self, batch_index: usize, bar: &Bar) -> FeatureValues {
        let skipped_batch = self
            .last_batch_index
            .is_some_and(|last| last.checked_add(1) != Some(batch_index));
        let skipped_interval = self
            .last_close_time_ms
            .is_some_and(|last_close| last_close != bar.open_time_ms);
        if skipped_batch || skipped_interval {
            self.reset();
        }

        let fast_ema = self.fast_ema.update(bar.close);
        let slow_ema = self.slow_ema.update(bar.close);
        let rsi = self.rsi.update(bar.close);
        let short_return = self.short_return.update(bar.close);
        let long_return = self.long_return.update(bar.close);
        let average_volume = match bar.volume_usd {
            Some(volume) => self.average_volume.update(volume),
            None => {
                // Missing volume breaks the fixed-time window. Retaining older
                // observations would turn a point-in-time gate into stale data.
                self.average_volume.reset();
                None
            }
        };
        self.last_batch_index = Some(batch_index);
        self.last_close_time_ms = Some(bar.close_time_ms);

        FeatureValues {
            fast_ema,
            slow_ema,
            rsi,
            short_return,
            long_return,
            average_volume,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct FeatureValues {
    fast_ema: Option<Decimal>,
    slow_ema: Option<Decimal>,
    rsi: Option<Decimal>,
    short_return: Option<Decimal>,
    long_return: Option<Decimal>,
    average_volume: Option<Decimal>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct EligibleAsset {
    market: MarketId,
    close: Decimal,
    fast_ema: Decimal,
    slow_ema: Decimal,
    rsi: Decimal,
    short_return: Decimal,
    long_return: Decimal,
    ema_gap: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ScoredAsset {
    features: EligibleAsset,
    score: Decimal,
}

pub struct CrossSectionalMomentum {
    config: MomentumConfig,
    states: BTreeMap<MarketId, MarketState>,
    last_batch_index: Option<usize>,
}

impl CrossSectionalMomentum {
    pub fn new(config: MomentumConfig) -> Result<Self, StrategyError> {
        config.validate()?;
        Ok(Self {
            config,
            states: BTreeMap::new(),
            last_batch_index: None,
        })
    }

    pub fn config(&self) -> &MomentumConfig {
        &self.config
    }

    fn update_current_batch(
        &mut self,
        context: StrategyContext<'_>,
    ) -> Result<Vec<EligibleAsset>, StrategyError> {
        let batch = context.batch;
        if self
            .last_batch_index
            .is_some_and(|last| batch.index <= last)
        {
            return Err(StrategyError::Decision(format!(
                "bar batch indices must increase strictly: previous {}, current {}",
                self.last_batch_index.unwrap_or_default(),
                batch.index
            )));
        }

        let mut bars = batch
            .bars
            .iter()
            .filter(|bar| bar.market.venue == self.config.venue)
            .collect::<Vec<_>>();
        bars.sort_by(|left, right| left.market.cmp(&right.market));

        let mut seen = BTreeSet::new();
        let mut eligible = Vec::new();
        for bar in bars {
            if !seen.insert(bar.market.clone()) {
                return Err(StrategyError::Decision(format!(
                    "duplicate market {} in bar batch {}",
                    bar.market.symbol, batch.index
                )));
            }
            if bar.close_time_ms > batch.close_time_ms
                || bar.available_at_ms > batch.available_at_ms
            {
                return Err(StrategyError::Decision(format!(
                    "market {} contains data unavailable at batch close",
                    bar.market.symbol
                )));
            }

            let state = self
                .states
                .entry(bar.market.clone())
                .or_insert_with(|| MarketState::new(&self.config));
            let values = state.update(batch.index, bar);
            if let Some(asset) = eligible_asset(&self.config, bar, values) {
                eligible.push(asset);
            }
        }

        self.last_batch_index = Some(batch.index);
        Ok(eligible)
    }

    fn target_book(&self, eligible: Vec<EligibleAsset>) -> TargetBook {
        if eligible.len() < self.config.min_eligible_assets {
            return TargetBook::flat();
        }

        let short_return_ranks = normalized_average_ranks(
            eligible
                .iter()
                .map(|asset| (asset.market.clone(), asset.short_return)),
        );
        let long_return_ranks = normalized_average_ranks(
            eligible
                .iter()
                .map(|asset| (asset.market.clone(), asset.long_return)),
        );
        let ema_gap_ranks = normalized_average_ranks(
            eligible
                .iter()
                .map(|asset| (asset.market.clone(), asset.ema_gap)),
        );
        let rsi_ranks = normalized_average_ranks(
            eligible
                .iter()
                .map(|asset| (asset.market.clone(), asset.rsi)),
        );

        let mut scored = eligible
            .into_iter()
            .map(|features| {
                let market = &features.market;
                let score = self.config.short_return_weight * short_return_ranks[market]
                    + self.config.long_return_weight * long_return_ranks[market]
                    + self.config.ema_gap_weight * ema_gap_ranks[market]
                    + self.config.rsi_weight * rsi_ranks[market];
                ScoredAsset { features, score }
            })
            .collect::<Vec<_>>();

        let mut longs = scored
            .iter()
            .filter(|asset| {
                asset.features.close > asset.features.fast_ema
                    && asset.features.fast_ema > asset.features.slow_ema
                    && asset.features.rsi >= self.config.long_rsi_min
            })
            .cloned()
            .collect::<Vec<_>>();
        longs.sort_by(|left, right| {
            right
                .score
                .cmp(&left.score)
                .then_with(|| left.features.market.cmp(&right.features.market))
        });
        longs.truncate(self.config.long_count);

        let mut shorts = scored
            .drain(..)
            .filter(|asset| {
                asset.features.close < asset.features.fast_ema
                    && asset.features.fast_ema < asset.features.slow_ema
                    && asset.features.rsi <= self.config.short_rsi_max
            })
            .collect::<Vec<_>>();
        shorts.sort_by(|left, right| {
            left.score
                .cmp(&right.score)
                .then_with(|| left.features.market.cmp(&right.features.market))
        });
        shorts.truncate(self.config.short_count);

        let long_ready = longs.len() >= self.config.min_positions_per_side;
        let short_ready = shorts.len() >= self.config.min_positions_per_side;
        let sides_ready = if self.config.require_both_sides {
            long_ready && short_ready
        } else {
            long_ready || short_ready
        };
        if !sides_ready {
            return TargetBook::flat();
        }

        let mut book = TargetBook::flat();
        if long_ready {
            insert_equal_side(
                &mut book,
                &longs,
                self.config.long_gross,
                self.config.max_asset_weight,
            );
        }
        if short_ready {
            insert_equal_side(
                &mut book,
                &shorts,
                -self.config.short_gross,
                self.config.max_asset_weight,
            );
        }
        book
    }
}

impl Strategy for CrossSectionalMomentum {
    fn name(&self) -> &str {
        "cross_sectional_momentum"
    }

    fn on_bar_close(
        &mut self,
        context: StrategyContext<'_>,
    ) -> Result<StrategyDecision, StrategyError> {
        let batch_index = context.batch.index;
        let eligible = self.update_current_batch(context)?;
        if batch_index % self.config.rebalance_every_bars != 0 {
            return Ok(StrategyDecision::Hold);
        }

        Ok(StrategyDecision::Rebalance(self.target_book(eligible)))
    }
}

fn eligible_asset(
    config: &MomentumConfig,
    bar: &Bar,
    values: FeatureValues,
) -> Option<EligibleAsset> {
    if !passes_optional_minimum(bar.open_interest_usd, config.min_open_interest_usd)
        || !passes_optional_minimum(bar.market_cap_usd, config.min_market_cap_usd)
        || !passes_optional_minimum(values.average_volume, config.min_average_volume_usd)
    {
        return None;
    }

    let fast_ema = values.fast_ema?;
    let slow_ema = values.slow_ema?;
    if slow_ema.is_zero() {
        return None;
    }

    Some(EligibleAsset {
        market: bar.market.clone(),
        close: bar.close,
        fast_ema,
        slow_ema,
        rsi: values.rsi?,
        short_return: values.short_return?,
        long_return: values.long_return?,
        ema_gap: fast_ema / slow_ema - Decimal::ONE,
    })
}

fn passes_optional_minimum(value: Option<Decimal>, minimum: Option<Decimal>) -> bool {
    minimum.is_none_or(|minimum| value.is_some_and(|value| value >= minimum))
}

fn normalized_average_ranks(
    values: impl IntoIterator<Item = (MarketId, Decimal)>,
) -> BTreeMap<MarketId, Decimal> {
    let mut values = values.into_iter().collect::<Vec<_>>();
    values.sort_by(|(left_market, left_value), (right_market, right_value)| {
        left_value
            .cmp(right_value)
            .then_with(|| left_market.cmp(right_market))
    });

    let count = values.len();
    let mut ranks = BTreeMap::new();
    if count == 0 {
        return ranks;
    }
    if count == 1 {
        ranks.insert(values[0].0.clone(), Decimal::ZERO);
        return ranks;
    }

    let denominator = Decimal::from((count - 1) as u64);
    let mut start = 0;
    while start < count {
        let mut end = start + 1;
        while end < count && values[end].1 == values[start].1 {
            end += 1;
        }

        // Average the zero-based rank positions occupied by this tie group,
        // then map [0, n-1] linearly to [-1, +1].
        let average_rank = Decimal::from((start + end - 1) as u64) / Decimal::TWO;
        let normalized = Decimal::TWO * average_rank / denominator - Decimal::ONE;
        for (market, _) in &values[start..end] {
            ranks.insert(market.clone(), normalized);
        }
        start = end;
    }
    ranks
}

fn insert_equal_side(
    book: &mut TargetBook,
    selected: &[ScoredAsset],
    signed_gross: Decimal,
    max_asset_weight: Decimal,
) {
    if selected.is_empty() {
        return;
    }

    let uncapped = signed_gross.abs() / Decimal::from(selected.len() as u64);
    let absolute_weight = uncapped.min(max_asset_weight);
    let weight = if signed_gross.is_sign_negative() {
        -absolute_weight
    } else {
        absolute_weight
    };
    for asset in selected {
        book.insert(asset.features.market.clone(), weight);
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::super::types::{BarBatch, PortfolioSnapshot};
    use super::*;

    const DAY_MS: u64 = 86_400_000;

    fn d(value: &str) -> Decimal {
        Decimal::from_str(value).expect("valid test decimal")
    }

    fn test_config() -> MomentumConfig {
        MomentumConfig {
            fast_ema_period: 2,
            slow_ema_period: 3,
            rsi_period: 2,
            short_return_lookback_bars: 1,
            long_return_lookback_bars: 2,
            rebalance_every_bars: 1,
            min_eligible_assets: 4,
            min_open_interest_usd: None,
            min_market_cap_usd: None,
            min_average_volume_usd: None,
            volume_lookback_bars: 2,
            long_count: 1,
            short_count: 1,
            min_positions_per_side: 1,
            long_gross: d("0.5"),
            short_gross: d("0.5"),
            max_asset_weight: d("0.6"),
            long_rsi_min: d("55"),
            short_rsi_max: d("45"),
            short_return_weight: d("0.45"),
            long_return_weight: d("0.30"),
            ema_gap_weight: d("0.15"),
            rsi_weight: d("0.10"),
            ..MomentumConfig::default()
        }
    }

    fn portfolio(timestamp_ms: u64) -> PortfolioSnapshot {
        PortfolioSnapshot {
            timestamp_ms,
            collateral: d("1000000"),
            equity: d("1000000"),
            gross_exposure: Decimal::ZERO,
            net_exposure: Decimal::ZERO,
            positions: Vec::new(),
        }
    }

    fn make_bar(index: usize, symbol: &str, close: Decimal) -> Bar {
        let open_time_ms = index as u64 * DAY_MS;
        let close_time_ms = open_time_ms + DAY_MS;
        Bar {
            market: MarketId::new(Venue::Hyperliquid, symbol),
            open_time_ms,
            close_time_ms,
            available_at_ms: close_time_ms,
            open: close,
            high: close,
            low: close,
            close,
            volume_usd: Some(d("20000000")),
            open_interest_usd: Some(d("10000000")),
            market_cap_usd: Some(d("1000000000")),
            mark_price: Some(close),
            funding_rate: Some(Decimal::ZERO),
            quantity_step: Some(d("0.001")),
        }
    }

    fn make_batch(index: usize, prices: &[(&str, &str)], reverse: bool) -> BarBatch {
        let mut bars = prices
            .iter()
            .map(|(symbol, price)| make_bar(index, symbol, d(price)))
            .collect::<Vec<_>>();
        if reverse {
            bars.reverse();
        }
        BarBatch {
            index,
            open_time_ms: index as u64 * DAY_MS,
            close_time_ms: index as u64 * DAY_MS + DAY_MS,
            available_at_ms: index as u64 * DAY_MS + DAY_MS,
            bars,
        }
    }

    fn decide(strategy: &mut CrossSectionalMomentum, batch: &BarBatch) -> StrategyDecision {
        let snapshot = portfolio(batch.available_at_ms);
        strategy
            .on_bar_close(StrategyContext {
                batch,
                portfolio: &snapshot,
            })
            .unwrap()
    }

    fn price_history() -> [Vec<(&'static str, &'static str)>; 3] {
        [
            vec![("A", "100"), ("B", "100"), ("C", "100"), ("D", "100")],
            vec![("A", "110"), ("B", "105"), ("C", "95"), ("D", "90")],
            vec![("A", "120"), ("B", "108"), ("C", "92"), ("D", "80")],
        ]
    }

    #[test]
    fn config_rejects_unknown_fields_and_invalid_parameters() {
        let text = r#"
            venue = "hyperliquid"
            fast_ema_period = 2
            slow_ema_period = 3
            rsi_period = 2
            short_return_lookback_bars = 1
            long_return_lookback_bars = 2
            rebalance_every_bars = 1
            min_eligible_assets = 4
            volume_lookback_bars = 2
            long_count = 1
            short_count = 1
            min_positions_per_side = 1
            long_gross = "0.5"
            short_gross = "0.5"
            max_asset_weight = "0.5"
            long_rsi_min = "55"
            short_rsi_max = "45"
            short_return_weight = "0.45"
            long_return_weight = "0.30"
            ema_gap_weight = "0.15"
            rsi_weight = "0.10"
            require_both_sides = true
            typo = 1
        "#;
        assert!(toml::from_str::<MomentumConfig>(text).is_err());

        let mut config = test_config();
        config.venue = Venue::Bybit;
        assert!(config.validate().is_err());
        config = test_config();
        config.fast_ema_period = config.slow_ema_period;
        assert!(config.validate().is_err());
        config = test_config();
        config.rsi_weight = d("-0.1");
        assert!(config.validate().is_err());
        config = test_config();
        config.short_rsi_max = config.long_rsi_min;
        assert!(config.validate().is_err());
    }

    #[test]
    fn strategy_stays_flat_during_warmup_then_selects_extremes() {
        let mut strategy = CrossSectionalMomentum::new(test_config()).unwrap();
        let history = price_history();

        for (index, prices) in history.iter().enumerate() {
            let decision = decide(&mut strategy, &make_batch(index, prices, false));
            let StrategyDecision::Rebalance(book) = decision else {
                panic!("daily cadence should rebalance");
            };
            if index < 2 {
                assert!(book.targets.is_empty());
            } else {
                assert_eq!(book.targets.len(), 2);
                assert_eq!(
                    book.targets[&MarketId::new(Venue::Hyperliquid, "A")],
                    d("0.5")
                );
                assert_eq!(
                    book.targets[&MarketId::new(Venue::Hyperliquid, "D")],
                    d("-0.5")
                );
            }
        }
    }

    #[test]
    fn input_order_cannot_change_cross_sectional_output() {
        let mut ordered = CrossSectionalMomentum::new(test_config()).unwrap();
        let mut reversed = CrossSectionalMomentum::new(test_config()).unwrap();
        let mut ordered_decision = StrategyDecision::Hold;
        let mut reversed_decision = StrategyDecision::Hold;

        for (index, prices) in price_history().iter().enumerate() {
            ordered_decision = decide(&mut ordered, &make_batch(index, prices, false));
            reversed_decision = decide(&mut reversed, &make_batch(index, prices, true));
        }
        assert_eq!(ordered_decision, reversed_decision);
    }

    #[test]
    fn configured_gate_never_reuses_a_previous_optional_value() {
        for gate in ["open_interest", "market_cap", "average_volume"] {
            let mut config = test_config();
            match gate {
                "open_interest" => config.min_open_interest_usd = Some(Decimal::ONE),
                "market_cap" => config.min_market_cap_usd = Some(Decimal::ONE),
                "average_volume" => config.min_average_volume_usd = Some(Decimal::ONE),
                _ => unreachable!(),
            }
            let mut strategy = CrossSectionalMomentum::new(config).unwrap();

            for (index, prices) in price_history().iter().enumerate() {
                let mut batch = make_batch(index, prices, false);
                if index == 2 {
                    let a = batch
                        .bars
                        .iter_mut()
                        .find(|bar| bar.market.symbol == "A")
                        .unwrap();
                    match gate {
                        "open_interest" => a.open_interest_usd = None,
                        "market_cap" => a.market_cap_usd = None,
                        "average_volume" => a.volume_usd = None,
                        _ => unreachable!(),
                    }
                }
                let decision = decide(&mut strategy, &batch);
                if index == 2 {
                    let StrategyDecision::Rebalance(book) = decision else {
                        panic!("expected rebalance");
                    };
                    assert!(book.targets.is_empty(), "gate {gate} reused stale data");
                }
            }
        }
    }

    #[test]
    fn require_both_sides_controls_one_sided_portfolios() {
        let all_up = [
            vec![("A", "100"), ("B", "100"), ("C", "100"), ("D", "100")],
            vec![("A", "110"), ("B", "108"), ("C", "106"), ("D", "104")],
            vec![("A", "120"), ("B", "116"), ("C", "112"), ("D", "108")],
        ];

        let mut both = CrossSectionalMomentum::new(test_config()).unwrap();
        let mut optional_config = test_config();
        optional_config.require_both_sides = false;
        let mut optional = CrossSectionalMomentum::new(optional_config).unwrap();
        let mut both_final = StrategyDecision::Hold;
        let mut optional_final = StrategyDecision::Hold;
        for (index, prices) in all_up.iter().enumerate() {
            let batch = make_batch(index, prices, false);
            both_final = decide(&mut both, &batch);
            optional_final = decide(&mut optional, &batch);
        }

        let StrategyDecision::Rebalance(both_book) = both_final else {
            panic!("expected rebalance");
        };
        assert!(both_book.targets.is_empty());
        let StrategyDecision::Rebalance(optional_book) = optional_final else {
            panic!("expected rebalance");
        };
        assert_eq!(optional_book.targets.len(), 1);
        assert!(optional_book
            .targets
            .values()
            .all(|weight| *weight > Decimal::ZERO));
    }

    #[test]
    fn non_rebalance_bars_update_state_but_emit_hold() {
        let mut config = test_config();
        config.rebalance_every_bars = 2;
        let mut strategy = CrossSectionalMomentum::new(config).unwrap();
        let history = price_history();

        assert!(matches!(
            decide(&mut strategy, &make_batch(0, &history[0], false)),
            StrategyDecision::Rebalance(_)
        ));
        assert_eq!(
            decide(&mut strategy, &make_batch(1, &history[1], false)),
            StrategyDecision::Hold
        );
        let StrategyDecision::Rebalance(book) =
            decide(&mut strategy, &make_batch(2, &history[2], false))
        else {
            panic!("expected cadence rebalance");
        };
        assert_eq!(book.targets.len(), 2);
    }

    #[test]
    fn a_time_gap_resets_warmup_instead_of_spanning_stale_bars() {
        let mut strategy = CrossSectionalMomentum::new(test_config()).unwrap();
        let history = price_history();
        for (index, prices) in history[..2].iter().enumerate() {
            decide(&mut strategy, &make_batch(index, prices, false));
        }

        let mut after_gap = make_batch(2, &history[2], false);
        after_gap.open_time_ms += DAY_MS;
        after_gap.close_time_ms += DAY_MS;
        after_gap.available_at_ms += DAY_MS;
        for bar in &mut after_gap.bars {
            bar.open_time_ms += DAY_MS;
            bar.close_time_ms += DAY_MS;
            bar.available_at_ms += DAY_MS;
        }
        let StrategyDecision::Rebalance(book) = decide(&mut strategy, &after_gap) else {
            panic!("expected cadence rebalance");
        };
        assert!(book.targets.is_empty());
    }

    #[test]
    fn average_ties_receive_identical_normalized_ranks() {
        let a = MarketId::new(Venue::Hyperliquid, "A");
        let b = MarketId::new(Venue::Hyperliquid, "B");
        let c = MarketId::new(Venue::Hyperliquid, "C");
        let ranks = normalized_average_ranks([
            (c.clone(), d("3")),
            (b.clone(), d("1")),
            (a.clone(), d("1")),
        ]);

        assert_eq!(ranks[&a], d("-0.5"));
        assert_eq!(ranks[&b], d("-0.5"));
        assert_eq!(ranks[&c], Decimal::ONE);
    }

    #[test]
    fn equal_weighting_obeys_per_name_cap() {
        let selected = [ScoredAsset {
            features: EligibleAsset {
                market: MarketId::new(Venue::Hyperliquid, "A"),
                close: Decimal::ONE,
                fast_ema: Decimal::ONE,
                slow_ema: Decimal::ONE,
                rsi: d("50"),
                short_return: Decimal::ZERO,
                long_return: Decimal::ZERO,
                ema_gap: Decimal::ZERO,
            },
            score: Decimal::ZERO,
        }];
        let mut long_book = TargetBook::flat();
        insert_equal_side(&mut long_book, &selected, d("0.5"), d("0.1"));
        assert_eq!(long_book.targets.values().next(), Some(&d("0.1")));

        let mut short_book = TargetBook::flat();
        insert_equal_side(&mut short_book, &selected, d("-0.5"), d("0.1"));
        assert_eq!(short_book.targets.values().next(), Some(&d("-0.1")));
    }
}
