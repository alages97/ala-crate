//! Versioned configuration for reproducible offline backtests.

use std::path::{Path, PathBuf};

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::engine::EngineConfig;
use super::momentum::MomentumConfig;

pub const BACKTEST_CONFIG_SCHEMA_VERSION: u16 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BacktestConfig {
    pub schema_version: u16,
    pub data: BacktestDataConfig,
    pub engine: EngineConfig,
    pub strategy: BuiltInStrategyConfig,
    pub output: BacktestOutputConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BacktestDataConfig {
    pub path: PathBuf,
    pub interval_ms: u64,
    pub require_contiguous_batches: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum BuiltInStrategyConfig {
    Momentum(MomentumConfig),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BacktestOutputConfig {
    pub directory: PathBuf,
}

#[derive(Debug, Error)]
pub enum BacktestConfigError {
    #[error("read backtest config: {0}")]
    Read(#[from] std::io::Error),
    #[error("parse backtest TOML: {0}")]
    Parse(#[from] toml::de::Error),
    #[error("invalid backtest config: {0}")]
    Invalid(String),
}

impl BacktestConfig {
    /// Read and validate a TOML configuration without touching the configured
    /// input or output paths.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, BacktestConfigError> {
        let contents = std::fs::read_to_string(path)?;
        Self::from_toml_str(&contents)
    }

    pub fn from_toml_str(contents: &str) -> Result<Self, BacktestConfigError> {
        let config: Self = toml::from_str(contents)?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), BacktestConfigError> {
        if self.schema_version != BACKTEST_CONFIG_SCHEMA_VERSION {
            return Err(BacktestConfigError::Invalid(format!(
                "unsupported schema_version {}; expected {}",
                self.schema_version, BACKTEST_CONFIG_SCHEMA_VERSION
            )));
        }
        if self.data.path.as_os_str().is_empty() {
            return Err(BacktestConfigError::Invalid(
                "data.path must not be empty".to_owned(),
            ));
        }
        if self.data.interval_ms == 0 {
            return Err(BacktestConfigError::Invalid(
                "data.interval_ms must be positive".to_owned(),
            ));
        }
        if self.output.directory.as_os_str().is_empty() {
            return Err(BacktestConfigError::Invalid(
                "output.directory must not be empty".to_owned(),
            ));
        }

        self.engine
            .validate()
            .map_err(|error| BacktestConfigError::Invalid(error.to_string()))?;
        match &self.strategy {
            BuiltInStrategyConfig::Momentum(config) => {
                config
                    .validate()
                    .map_err(|error| BacktestConfigError::Invalid(error.to_string()))?;
                validate_momentum_engine_compatibility(config, &self.engine)?;
            }
        }
        Ok(())
    }
}

fn validate_momentum_engine_compatibility(
    strategy: &MomentumConfig,
    engine: &EngineConfig,
) -> Result<(), BacktestConfigError> {
    if strategy.max_asset_weight > engine.max_asset_weight {
        return invalid(format!(
            "strategy.max_asset_weight {} exceeds engine.max_asset_weight {}",
            strategy.max_asset_weight, engine.max_asset_weight
        ));
    }

    // A ready side selects any number of candidates from the configured
    // minimum through its side-specific maximum. Equal weighting followed by
    // the per-name cap gives min(requested gross, count * cap) total exposure.
    // This is monotone in count, so the two endpoints fully bound every
    // possible candidate count without iterating over caller-sized ranges.
    let long_min = capped_side_gross(
        strategy.long_gross,
        strategy.max_asset_weight,
        strategy.min_positions_per_side,
    )?;
    let long_max = capped_side_gross(
        strategy.long_gross,
        strategy.max_asset_weight,
        strategy.long_count,
    )?;
    let short_min = capped_side_gross(
        strategy.short_gross,
        strategy.max_asset_weight,
        strategy.min_positions_per_side,
    )?;
    let short_max = capped_side_gross(
        strategy.short_gross,
        strategy.max_asset_weight,
        strategy.short_count,
    )?;

    let max_gross = checked_add(long_max, short_max, "bounding strategy gross exposure")?;
    if max_gross > engine.max_gross_leverage {
        return invalid(format!(
            "momentum strategy can emit gross weight {max_gross}, exceeding engine.max_gross_leverage {}",
            engine.max_gross_leverage
        ));
    }

    // For two monotone exposure intervals, the largest absolute difference is
    // attained at opposite endpoints. If one-sided books are permitted, their
    // largest long-only and short-only exposures must also fit the net bound.
    let mut max_abs_net = checked_sub(long_max, short_min, "bounding strategy net exposure")?.abs();
    max_abs_net =
        max_abs_net.max(checked_sub(long_min, short_max, "bounding strategy net exposure")?.abs());
    if !strategy.require_both_sides {
        max_abs_net = max_abs_net.max(long_max).max(short_max);
    }
    if max_abs_net > engine.max_abs_net_weight {
        return invalid(format!(
            "momentum strategy can emit absolute net weight {max_abs_net}, exceeding engine.max_abs_net_weight {}",
            engine.max_abs_net_weight
        ));
    }

    Ok(())
}

fn capped_side_gross(
    requested_gross: Decimal,
    max_asset_weight: Decimal,
    candidate_count: usize,
) -> Result<Decimal, BacktestConfigError> {
    let count = u64::try_from(candidate_count).map_err(|_| {
        BacktestConfigError::Invalid(
            "strategy candidate count cannot be represented safely".to_owned(),
        )
    })?;
    let capped_total = max_asset_weight
        .checked_mul(Decimal::from(count))
        .ok_or_else(|| {
            BacktestConfigError::Invalid(
                "decimal overflow while bounding strategy side exposure".to_owned(),
            )
        })?;
    Ok(requested_gross.min(capped_total))
}

fn checked_add(
    left: Decimal,
    right: Decimal,
    context: &'static str,
) -> Result<Decimal, BacktestConfigError> {
    left.checked_add(right)
        .ok_or_else(|| BacktestConfigError::Invalid(format!("decimal overflow while {context}")))
}

fn checked_sub(
    left: Decimal,
    right: Decimal,
    context: &'static str,
) -> Result<Decimal, BacktestConfigError> {
    left.checked_sub(right)
        .ok_or_else(|| BacktestConfigError::Invalid(format!("decimal overflow while {context}")))
}

fn invalid<T>(message: impl Into<String>) -> Result<T, BacktestConfigError> {
    Err(BacktestConfigError::Invalid(message.into()))
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    fn decimal(value: &str) -> Decimal {
        Decimal::from_str(value).expect("valid test decimal")
    }

    fn momentum(config: &mut BacktestConfig) -> &mut MomentumConfig {
        match &mut config.strategy {
            BuiltInStrategyConfig::Momentum(momentum) => momentum,
        }
    }

    fn invalid_reason(result: Result<(), BacktestConfigError>) -> String {
        match result {
            Err(BacktestConfigError::Invalid(reason)) => reason,
            other => panic!("expected invalid configuration, received {other:?}"),
        }
    }

    fn valid_config() -> String {
        r#"
schema_version = 1

[data]
path = "/definitely/not/required/to/exist/bars.csv"
interval_ms = 86400000
require_contiguous_batches = true

[engine]
initial_collateral = "100000"
fee_bps = "2"
slippage_bps = "3"
max_gross_leverage = "1"
max_abs_net_weight = "0.10"
max_asset_weight = "0.125"
min_order_notional = "10"
apply_funding = true
require_funding_for_open_positions = true
liquidate_at_end = true

[strategy]
type = "momentum"
venue = "hyperliquid"
fast_ema_period = 20
slow_ema_period = 50
rsi_period = 14
short_return_lookback_bars = 7
long_return_lookback_bars = 30
rebalance_every_bars = 1
min_eligible_assets = 4
min_open_interest_usd = "5000000"
min_market_cap_usd = "100000000"
min_average_volume_usd = "10000000"
volume_lookback_bars = 7
long_count = 2
short_count = 2
min_positions_per_side = 2
long_gross = "0.5"
short_gross = "0.5"
max_asset_weight = "0.125"
long_rsi_min = "55"
short_rsi_max = "45"
short_return_weight = "0.45"
long_return_weight = "0.30"
ema_gap_weight = "0.15"
rsi_weight = "0.10"
require_both_sides = true

[output]
directory = "/also/not/required/to/exist"
"#
        .to_owned()
    }

    #[test]
    fn parses_and_validates_without_accessing_configured_paths() {
        let config = BacktestConfig::from_toml_str(&valid_config()).expect("valid config");
        assert_eq!(config.schema_version, BACKTEST_CONFIG_SCHEMA_VERSION);
        assert_eq!(config.data.interval_ms, 86_400_000);
        assert!(matches!(
            config.strategy,
            BuiltInStrategyConfig::Momentum(_)
        ));
    }

    #[test]
    fn rejects_unknown_fields_and_versions() {
        let unknown = valid_config().replace(
            "require_contiguous_batches = true",
            "require_contiguous_batches = true\nmisspelled = true",
        );
        assert!(matches!(
            BacktestConfig::from_toml_str(&unknown),
            Err(BacktestConfigError::Parse(_))
        ));

        let version = valid_config().replace("schema_version = 1", "schema_version = 2");
        assert!(matches!(
            BacktestConfig::from_toml_str(&version),
            Err(BacktestConfigError::Invalid(_))
        ));
    }

    #[test]
    fn rejects_zero_interval() {
        let config = valid_config().replace("interval_ms = 86400000", "interval_ms = 0");
        assert!(matches!(
            BacktestConfig::from_toml_str(&config),
            Err(BacktestConfigError::Invalid(_))
        ));
    }

    #[test]
    fn rejects_strategy_asset_cap_above_engine_cap() {
        let mut config = BacktestConfig::from_toml_str(&valid_config()).expect("base config");
        momentum(&mut config).max_asset_weight = decimal("0.126");

        let reason = invalid_reason(config.validate());
        assert!(reason.contains("strategy.max_asset_weight"));
        assert!(reason.contains("engine.max_asset_weight"));
    }

    #[test]
    fn gross_bound_accounts_for_per_name_caps() {
        let mut config = BacktestConfig::from_toml_str(&valid_config()).expect("base config");

        // Each two-name side is capped at 0.25, so the strategy can emit 0.50
        // gross even though its requested long_gross + short_gross is 1.0.
        config.engine.max_gross_leverage = decimal("0.50");
        config.validate().expect("capped gross fits exactly");

        config.engine.max_gross_leverage = decimal("0.49");
        let reason = invalid_reason(config.validate());
        assert!(reason.contains("gross weight 0.500"));
        assert!(reason.contains("engine.max_gross_leverage"));
    }

    #[test]
    fn net_bound_checks_every_ready_side_count() {
        let mut config = BacktestConfig::from_toml_str(&valid_config()).expect("base config");
        momentum(&mut config).min_positions_per_side = 1;

        // One side may have one selected name (0.125 gross) while the other
        // has two (0.25 gross), even though the requested side grosses match.
        let reason = invalid_reason(config.validate());
        assert!(reason.contains("absolute net weight 0.125"));
        assert!(reason.contains("engine.max_abs_net_weight"));
    }

    #[test]
    fn net_bound_includes_single_sided_books_when_allowed() {
        let mut config = BacktestConfig::from_toml_str(&valid_config()).expect("base config");
        momentum(&mut config).require_both_sides = false;

        let reason = invalid_reason(config.validate());
        assert!(reason.contains("absolute net weight 0.250"));
        assert!(reason.contains("engine.max_abs_net_weight"));
    }

    #[test]
    fn repository_sample_is_cross_component_compatible() {
        BacktestConfig::from_toml_str(include_str!("../../config/backtest-momentum.toml"))
            .expect("sample backtest config");
    }
}
