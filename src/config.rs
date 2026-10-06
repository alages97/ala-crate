use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use serde::Deserialize;
use thiserror::Error;

use crate::gateway::{HyperliquidGatewayConfig, LighterGatewayConfig};
use crate::market_data::MarketDataSourceConfig;
use crate::quoter::{QuoterConfigError, QuoterSettings, WideQuoter};
use crate::types::Venue;

pub const CONFIG_SCHEMA_VERSION: u16 = 1;
pub const LIGHTER_MIN_QUOTE_INTERVAL_MS: u64 = 10_000;
const LIGHTER_MAINNET_HOST: &str = "mainnet.zklighter.elliot.ai";

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppConfig {
    pub schema_version: u16,
    pub instance_id: String,
    /// This binary has no paper/testnet production mode. The explicit bit
    /// prevents an operator from mistaking a sample config for a harmless run.
    pub live_trading: bool,
    pub gateway: GatewayConfig,
    pub market_data: MarketDataSourceConfig,
    pub quoter: QuoterSettings,
    #[serde(default)]
    pub runtime: RuntimeConfig,
    #[serde(default)]
    pub telemetry: TelemetryConfig,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "venue", rename_all = "lowercase", deny_unknown_fields)]
pub enum GatewayConfig {
    Hyperliquid {
        #[serde(flatten)]
        settings: HyperliquidGatewayConfig,
    },
    Lighter {
        #[serde(flatten)]
        settings: LighterGatewayConfig,
    },
}

impl GatewayConfig {
    pub const fn venue(&self) -> Venue {
        match self {
            Self::Hyperliquid { .. } => Venue::Hyperliquid,
            Self::Lighter { .. } => Venue::Lighter,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RuntimeConfig {
    pub market_data_channel_capacity: usize,
    pub order_manager_command_capacity: usize,
    pub order_manager_event_capacity: usize,
    pub shutdown_timeout_ms: u64,
    pub decision_interval_ms: u64,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            market_data_channel_capacity: 4_096,
            order_manager_command_capacity: 1_024,
            order_manager_event_capacity: 1_024,
            shutdown_timeout_ms: 5_000,
            decision_interval_ms: 100,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TelemetryConfig {
    pub metrics_listen_address: String,
    pub log_filter: String,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            metrics_listen_address: "127.0.0.1:9898".to_owned(),
            log_filter: "info,wide_quoter=debug".to_owned(),
        }
    }
}

impl TelemetryConfig {
    pub fn metrics_socket_address(&self) -> Result<SocketAddr, ConfigError> {
        self.metrics_listen_address.parse().map_err(|_| {
            ConfigError::Invalid(format!(
                "telemetry.metrics_listen_address is invalid: {}",
                self.metrics_listen_address
            ))
        })
    }
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("read config: {0}")]
    Read(#[from] std::io::Error),
    #[error("parse TOML config: {0}")]
    Parse(#[from] toml::de::Error),
    #[error("invalid config: {0}")]
    Invalid(String),
    #[error("invalid quoter config: {0}")]
    Quoter(#[from] QuoterConfigError),
}

impl AppConfig {
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let contents = std::fs::read_to_string(path)?;
        let config: Self = toml::from_str(&contents)?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.schema_version != CONFIG_SCHEMA_VERSION {
            return Err(ConfigError::Invalid(format!(
                "unsupported schema_version {}; expected {}",
                self.schema_version, CONFIG_SCHEMA_VERSION
            )));
        }
        if self.instance_id.trim().is_empty() {
            return Err(ConfigError::Invalid(
                "instance_id must not be empty".to_owned(),
            ));
        }
        if !self.live_trading {
            return Err(ConfigError::Invalid(
                "only live mainnet execution is implemented; set live_trading=true intentionally"
                    .to_owned(),
            ));
        }
        if self.gateway.venue() != self.quoter.instrument.venue {
            return Err(ConfigError::Invalid(format!(
                "gateway venue {} does not match target instrument venue {}",
                self.gateway.venue(),
                self.quoter.instrument.venue
            )));
        }
        let gateway_request_timeout_ms = match &self.gateway {
            GatewayConfig::Hyperliquid { settings } => {
                require_env_name("gateway.private_key_env", &settings.private_key_env)?;
                require_env_name("gateway.account_address_env", &settings.account_address_env)?;
                if let Some(name) = settings.vault_address_env.as_deref() {
                    require_env_name("gateway.vault_address_env", name)?;
                }
                if settings.request_timeout_ms == 0 {
                    return Err(ConfigError::Invalid(
                        "gateway.request_timeout_ms must be positive".to_owned(),
                    ));
                }
                settings.request_timeout_ms
            }
            GatewayConfig::Lighter { settings } => {
                if self.quoter.min_quote_interval_ms < LIGHTER_MIN_QUOTE_INTERVAL_MS {
                    return Err(ConfigError::Invalid(format!(
                        "quoter.min_quote_interval_ms must be at least {LIGHTER_MIN_QUOTE_INTERVAL_MS} for Lighter to preserve conservative live transaction and cancel capacity"
                    )));
                }
                if !is_official_lighter_mainnet_host(&settings.host) {
                    return Err(ConfigError::Invalid(
                        format!(
                            "gateway.host must be the official Lighter mainnet endpoint {LIGHTER_MAINNET_HOST}"
                        ),
                    ));
                }
                if settings.market_symbol.trim().is_empty() {
                    return Err(ConfigError::Invalid(
                        "gateway.market_symbol must not be empty".to_owned(),
                    ));
                }
                if !settings
                    .market_symbol
                    .eq_ignore_ascii_case(&self.quoter.instrument.venue_symbol)
                {
                    return Err(ConfigError::Invalid(format!(
                        "Lighter gateway market_symbol {} does not match execution venue_symbol {}",
                        settings.market_symbol, self.quoter.instrument.venue_symbol
                    )));
                }
                for (field, name) in [
                    (
                        "gateway.signer_library_path_env",
                        settings.signer_library_path_env.as_str(),
                    ),
                    (
                        "gateway.account_index_env",
                        settings.account_index_env.as_str(),
                    ),
                    (
                        "gateway.api_key_index_env",
                        settings.api_key_index_env.as_str(),
                    ),
                    (
                        "gateway.api_private_key_env",
                        settings.api_private_key_env.as_str(),
                    ),
                ] {
                    require_env_name(field, name)?;
                }
                if settings.request_timeout_ms == 0
                    || settings.order_expiry_ms == 0
                    || settings.reconnect_delay_ms == 0
                {
                    return Err(ConfigError::Invalid(
                        "Lighter gateway timeouts, expiry, and reconnect delay must be positive"
                            .to_owned(),
                    ));
                }
                settings.request_timeout_ms
            }
        };
        if self.market_data.name.trim().is_empty() || self.market_data.address.trim().is_empty() {
            return Err(ConfigError::Invalid(
                "market_data name and address must not be empty".to_owned(),
            ));
        }
        if self.runtime.market_data_channel_capacity == 0
            || self.runtime.order_manager_command_capacity == 0
            || self.runtime.order_manager_event_capacity == 0
        {
            return Err(ConfigError::Invalid(
                "runtime channel capacities must be positive".to_owned(),
            ));
        }
        if self.runtime.shutdown_timeout_ms == 0 || self.runtime.decision_interval_ms == 0 {
            return Err(ConfigError::Invalid(
                "runtime timeouts and intervals must be positive".to_owned(),
            ));
        }
        if self.market_data.reconnect_delay_ms == 0 {
            return Err(ConfigError::Invalid(
                "market_data.reconnect_delay_ms must be positive".to_owned(),
            ));
        }
        if self.market_data.max_publisher_lag_ms == 0 || self.market_data.max_exchange_lag_ms == 0 {
            return Err(ConfigError::Invalid(
                "market_data publisher and exchange lag limits must be positive".to_owned(),
            ));
        }
        if self.runtime.decision_interval_ms > self.quoter.max_source_age_ms {
            return Err(ConfigError::Invalid(
                "runtime.decision_interval_ms must not exceed quoter.max_source_age_ms; stale quotes must be checked in time"
                    .to_owned(),
            ));
        }
        // The runtime owns two quote slots. Venue gateways may serialize their
        // cancellation requests (Lighter does so for nonce safety), so the
        // shutdown window must cover two full request timeouts plus margin.
        let minimum_shutdown_ms = gateway_request_timeout_ms.checked_mul(2).ok_or_else(|| {
            ConfigError::Invalid("gateway request timeout is too large".to_owned())
        })?;
        if self.runtime.shutdown_timeout_ms <= minimum_shutdown_ms {
            return Err(ConfigError::Invalid(format!(
                "runtime.shutdown_timeout_ms must exceed twice the gateway request timeout ({minimum_shutdown_ms} ms) so both quote-side cancels can complete"
            )));
        }
        if self.telemetry.log_filter.trim().is_empty() {
            return Err(ConfigError::Invalid(
                "telemetry.log_filter must not be empty".to_owned(),
            ));
        }
        self.telemetry.metrics_socket_address()?;
        WideQuoter::new(self.quoter.clone())?;
        Ok(())
    }

    pub fn shutdown_timeout(&self) -> Duration {
        Duration::from_millis(self.runtime.shutdown_timeout_ms)
    }
}

fn require_env_name(field: &str, name: &str) -> Result<(), ConfigError> {
    if name.trim().is_empty() {
        Err(ConfigError::Invalid(format!("{field} must not be empty")))
    } else {
        Ok(())
    }
}

fn is_official_lighter_mainnet_host(host: &str) -> bool {
    let normalized = host.trim().to_ascii_lowercase();
    if normalized.starts_with("http://")
        || (normalized.contains("://") && !normalized.starts_with("https://"))
    {
        return false;
    }
    normalized
        .strip_prefix("https://")
        .unwrap_or(&normalized)
        .trim_end_matches('/')
        == LIGHTER_MAINNET_HOST
}

#[cfg(test)]
mod tests {
    use super::{AppConfig, GatewayConfig};

    fn hyperliquid_sample() -> AppConfig {
        toml::from_str(include_str!("../config/hyperliquid-btc.toml")).expect("parse sample")
    }

    fn lighter_sample() -> AppConfig {
        toml::from_str(include_str!("../config/lighter-btc.toml")).expect("parse sample")
    }

    #[test]
    fn parses_hyperliquid_sample() {
        let config = hyperliquid_sample();
        config.validate().expect("valid sample");
        assert_eq!(config.quoter.min_quote_interval_ms, 1_000);
    }

    #[test]
    fn parses_lighter_sample() {
        let config = lighter_sample();
        config.validate().expect("valid sample");
        assert_eq!(config.quoter.min_quote_interval_ms, 10_000);
    }

    #[test]
    fn rejects_lighter_quote_interval_below_conservative_floor() {
        let mut config = lighter_sample();
        config.quoter.min_quote_interval_ms = 9_999;
        assert!(config.validate().is_err());
    }

    #[test]
    fn rejects_arbitrary_lighter_https_host_during_validation() {
        let mut config = lighter_sample();
        {
            let GatewayConfig::Lighter { settings } = &mut config.gateway else {
                unreachable!();
            };
            settings.host = "https://example.com".to_owned();
        }
        assert!(config.validate().is_err());

        {
            let GatewayConfig::Lighter { settings } = &mut config.gateway else {
                unreachable!();
            };
            settings.host = "https://mainnet.zklighter.elliot.ai/".to_owned();
        }
        config.validate().expect("official mainnet URL is accepted");
    }

    #[test]
    fn rejects_a_stale_check_slower_than_the_freshness_limit() {
        let mut config = hyperliquid_sample();
        config.runtime.decision_interval_ms = config.quoter.max_source_age_ms + 1;
        assert!(config.validate().is_err());
    }

    #[test]
    fn shutdown_window_must_outlast_a_gateway_request() {
        let mut config = hyperliquid_sample();
        let GatewayConfig::Hyperliquid { settings } = &config.gateway else {
            unreachable!();
        };
        config.runtime.shutdown_timeout_ms = settings.request_timeout_ms * 2;
        assert!(config.validate().is_err());
    }

    #[test]
    fn rejects_empty_secret_environment_variable_names() {
        let mut config = hyperliquid_sample();
        let GatewayConfig::Hyperliquid { settings } = &mut config.gateway else {
            unreachable!();
        };
        settings.private_key_env.clear();
        assert!(config.validate().is_err());
    }

    #[test]
    fn rejects_unknown_live_safety_fields() {
        let sample = include_str!("../config/hyperliquid-btc.toml");
        for typo in [
            (
                "max_publisher_lag_ms = 500",
                "max_publisher_lag_ms = 500\nmax_publishr_lag_ms = 500",
            ),
            (
                "max_source_age_ms = 1500",
                "max_source_age_ms = 1500\nmax_sorce_age_ms = 1500",
            ),
            (
                "min_quote_interval_ms = 1000",
                "min_quote_interval_ms = 1000\nmin_quote_intervl_ms = 1000",
            ),
            (
                "price_tick = \"1\"",
                "price_tick = \"1\"\nprice_tik = \"1\"",
            ),
            ("weight = \"1\"", "weight = \"1\"\nweigth = \"1\""),
            (
                "request_timeout_ms = 5000",
                "request_timeout_ms = 5000\nrequest_timout_ms = 5000",
            ),
        ] {
            let malformed = sample.replacen(typo.0, typo.1, 1);
            assert!(
                toml::from_str::<AppConfig>(&malformed).is_err(),
                "unknown field introduced by replacement {:?} was accepted",
                typo.0
            );
        }
    }
}
