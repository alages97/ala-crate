//! Live public BBO collection and bounded TCP fanout.
//!
//! This module deliberately contains no order-entry or private-account code. It
//! connects only to hard-wired official mainnet public endpoints, normalizes
//! exact decimal BBOs, and publishes protocol-v1 NDJSON frames to local trading
//! processes.

mod binance_bybit;
mod hyperliquid_lighter;

use std::collections::HashMap;
use std::future::pending;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use futures_util::{Sink, SinkExt, StreamExt};
use metrics::{counter, gauge, histogram};
use rust_decimal::Decimal;
use serde::Deserialize;
use thiserror::Error;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;
use tokio::time::{self, Instant as TokioInstant, MissedTickBehavior};
use tokio_tungstenite::connect_async_with_config;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

use crate::market_data::{
    Bbo, MarketDataEnvironment, MarketDataMessage, MARKET_DATA_SCHEMA_VERSION,
    MAX_MARKET_DATA_LINE_BYTES,
};
use crate::types::{unix_timestamp_ms, Venue};

pub const LIVE_MARKET_DATA_CONFIG_SCHEMA_VERSION: u16 = 1;

const BINANCE_WS_ROOT: &str = "wss://fstream.binance.com/public/ws";
const BYBIT_WS_URL: &str = "wss://stream.bybit.com/v5/public/linear";
const HYPERLIQUID_WS_URL: &str = "wss://api.hyperliquid.xyz/ws";
const LIGHTER_HTTP_URL: &str = "https://mainnet.zklighter.elliot.ai";
const LIGHTER_WS_URL: &str = "wss://mainnet.zklighter.elliot.ai/stream?readonly=true";
const MAX_LIGHTER_METADATA_BYTES: usize = 4 * 1024 * 1024;
const HEALTHY_CONNECTION_AGE: Duration = Duration::from_secs(60);
const BINANCE_ROTATION_AGE: Duration = Duration::from_secs(23 * 60 * 60 + 30 * 60);

fn default_log_filter() -> String {
    "info,wide_quoter=debug".to_owned()
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LiveMarketDataConfig {
    pub schema_version: u16,
    pub listen_address: String,
    pub metrics_listen_address: String,
    #[serde(default = "default_log_filter")]
    pub log_filter: String,
    #[serde(default)]
    pub runtime: LiveMarketDataRuntimeConfig,
    pub feeds: LiveFeedConfigs,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LiveMarketDataRuntimeConfig {
    /// Maximum publication frequency per market. Raw updates inside one period
    /// are coalesced to the newest complete BBO.
    pub publish_interval_ms: u64,
    pub fanout_capacity: usize,
    pub max_clients: usize,
    pub client_write_timeout_ms: u64,
    pub client_max_frame_age_ms: u64,
    pub reconnect_initial_ms: u64,
    pub reconnect_max_ms: u64,
    pub websocket_operation_timeout_ms: u64,
    pub websocket_read_timeout_ms: u64,
    pub bbo_timeout_ms: u64,
    pub http_timeout_ms: u64,
    pub max_websocket_message_bytes: usize,
    pub max_exchange_lag_ms: u64,
    pub max_future_skew_ms: u64,
    pub shutdown_timeout_ms: u64,
}

impl Default for LiveMarketDataRuntimeConfig {
    fn default() -> Self {
        Self {
            publish_interval_ms: 50,
            fanout_capacity: 256,
            max_clients: 16,
            client_write_timeout_ms: 250,
            client_max_frame_age_ms: 250,
            reconnect_initial_ms: 250,
            reconnect_max_ms: 30_000,
            websocket_operation_timeout_ms: 10_000,
            websocket_read_timeout_ms: 90_000,
            bbo_timeout_ms: 30_000,
            http_timeout_ms: 10_000,
            max_websocket_message_bytes: 2 * 1024 * 1024,
            max_exchange_lag_ms: 2_500,
            max_future_skew_ms: 1_000,
            shutdown_timeout_ms: 5_000,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LiveFeedConfigs {
    pub binance: SymbolFeedConfig,
    pub bybit: SymbolFeedConfig,
    pub hyperliquid: SymbolFeedConfig,
    pub lighter: LighterFeedConfig,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SymbolFeedConfig {
    pub enabled: bool,
    pub symbol: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LighterFeedConfig {
    pub enabled: bool,
    pub symbol: String,
    pub market_index: u8,
}

#[derive(Debug, Error)]
pub enum LiveMarketDataConfigError {
    #[error("read live market-data config: {0}")]
    Read(#[from] std::io::Error),
    #[error("parse live market-data TOML: {0}")]
    Parse(#[from] toml::de::Error),
    #[error("invalid live market-data config: {0}")]
    Invalid(String),
}

impl LiveMarketDataConfig {
    pub fn load(path: impl AsRef<Path>) -> Result<Self, LiveMarketDataConfigError> {
        let contents = std::fs::read_to_string(path)?;
        let config: Self = toml::from_str(&contents)?;
        config.validate()?;
        Ok(config)
    }

    pub fn listen_socket_address(&self) -> Result<SocketAddr, LiveMarketDataConfigError> {
        parse_socket_address("listen_address", &self.listen_address)
    }

    pub fn metrics_socket_address(&self) -> Result<SocketAddr, LiveMarketDataConfigError> {
        parse_socket_address("metrics_listen_address", &self.metrics_listen_address)
    }

    pub fn validate(&self) -> Result<(), LiveMarketDataConfigError> {
        if self.schema_version != LIVE_MARKET_DATA_CONFIG_SCHEMA_VERSION {
            return Err(LiveMarketDataConfigError::Invalid(format!(
                "unsupported schema_version {}; expected {}",
                self.schema_version, LIVE_MARKET_DATA_CONFIG_SCHEMA_VERSION
            )));
        }
        let listen = self.listen_socket_address()?;
        let metrics = self.metrics_socket_address()?;
        if listen == metrics {
            return Err(LiveMarketDataConfigError::Invalid(
                "listen_address and metrics_listen_address must differ".to_owned(),
            ));
        }
        if self.log_filter.trim().is_empty() {
            return Err(LiveMarketDataConfigError::Invalid(
                "log_filter must not be empty".to_owned(),
            ));
        }
        EnvFilter::try_new(&self.log_filter).map_err(|error| {
            LiveMarketDataConfigError::Invalid(format!("invalid log_filter: {error}"))
        })?;

        let definitions = self.enabled_feeds()?;
        if definitions.is_empty() {
            return Err(LiveMarketDataConfigError::Invalid(
                "at least one live feed must be enabled".to_owned(),
            ));
        }

        let runtime = &self.runtime;
        if runtime.publish_interval_ms == 0
            || runtime.fanout_capacity == 0
            || runtime.max_clients == 0
            || runtime.client_write_timeout_ms == 0
            || runtime.client_max_frame_age_ms == 0
            || runtime.reconnect_initial_ms == 0
            || runtime.reconnect_max_ms < runtime.reconnect_initial_ms
            || runtime.websocket_operation_timeout_ms == 0
            || runtime.websocket_read_timeout_ms == 0
            || runtime.bbo_timeout_ms == 0
            || runtime.http_timeout_ms == 0
            || runtime.max_websocket_message_bytes < 1_024
            || runtime.max_websocket_message_bytes > 16 * 1024 * 1024
            || runtime.max_exchange_lag_ms == 0
            || runtime.shutdown_timeout_ms == 0
        {
            return Err(LiveMarketDataConfigError::Invalid(
                "runtime capacities, latency limits, and timeouts are inconsistent or nonpositive"
                    .to_owned(),
            ));
        }
        if runtime.publish_interval_ms > runtime.client_max_frame_age_ms {
            return Err(LiveMarketDataConfigError::Invalid(
                "runtime.publish_interval_ms must not exceed client_max_frame_age_ms".to_owned(),
            ));
        }
        if runtime.fanout_capacity > 65_536 || runtime.max_clients > 1_024 {
            return Err(LiveMarketDataConfigError::Invalid(
                "runtime fanout_capacity or max_clients exceeds the hard resource bound".to_owned(),
            ));
        }
        Ok(())
    }

    fn enabled_feeds(&self) -> Result<Vec<FeedDefinition>, LiveMarketDataConfigError> {
        let candidates = [
            (
                FeedKind::Binance,
                self.feeds.binance.enabled,
                self.feeds.binance.symbol.as_str(),
                None,
            ),
            (
                FeedKind::Bybit,
                self.feeds.bybit.enabled,
                self.feeds.bybit.symbol.as_str(),
                None,
            ),
            (
                FeedKind::Hyperliquid,
                self.feeds.hyperliquid.enabled,
                self.feeds.hyperliquid.symbol.as_str(),
                None,
            ),
            (
                FeedKind::Lighter,
                self.feeds.lighter.enabled,
                self.feeds.lighter.symbol.as_str(),
                Some(self.feeds.lighter.market_index),
            ),
        ];
        let mut definitions = Vec::new();
        for (kind, enabled, symbol, market_index) in candidates {
            if enabled {
                validate_symbol(kind, symbol)?;
                definitions.push(FeedDefinition {
                    kind,
                    symbol: symbol.to_owned(),
                    market_index,
                });
            }
        }
        Ok(definitions)
    }
}

fn parse_socket_address(
    field: &'static str,
    value: &str,
) -> Result<SocketAddr, LiveMarketDataConfigError> {
    value.parse().map_err(|_| {
        LiveMarketDataConfigError::Invalid(format!(
            "{field} is not a valid socket address: {value}"
        ))
    })
}

fn validate_symbol(kind: FeedKind, symbol: &str) -> Result<(), LiveMarketDataConfigError> {
    let valid_length = !symbol.is_empty() && symbol.len() <= 64;
    let valid_chars = symbol
        .bytes()
        .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || b"_-:".contains(&byte));
    if !valid_length || !valid_chars {
        return Err(LiveMarketDataConfigError::Invalid(format!(
            "feeds.{}.symbol must be 1-64 uppercase ASCII letters, digits, '_', '-', or ':'",
            kind.label()
        )));
    }
    if matches!(kind, FeedKind::Binance | FeedKind::Bybit)
        && !symbol.bytes().all(|byte| byte.is_ascii_alphanumeric())
    {
        return Err(LiveMarketDataConfigError::Invalid(format!(
            "feeds.{}.symbol must be uppercase ASCII letters and digits",
            kind.label()
        )));
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum FeedKind {
    Binance,
    Bybit,
    Hyperliquid,
    Lighter,
}

impl FeedKind {
    const ALL: [Self; 4] = [Self::Binance, Self::Bybit, Self::Hyperliquid, Self::Lighter];

    const fn index(self) -> usize {
        match self {
            Self::Binance => 0,
            Self::Bybit => 1,
            Self::Hyperliquid => 2,
            Self::Lighter => 3,
        }
    }

    const fn venue(self) -> Venue {
        match self {
            Self::Binance => Venue::Binance,
            Self::Bybit => Venue::Bybit,
            Self::Hyperliquid => Venue::Hyperliquid,
            Self::Lighter => Venue::Lighter,
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::Binance => "binance",
            Self::Bybit => "bybit",
            Self::Hyperliquid => "hyperliquid",
            Self::Lighter => "lighter",
        }
    }
}

#[derive(Debug, Clone)]
struct FeedDefinition {
    kind: FeedKind,
    symbol: String,
    market_index: Option<u8>,
}

#[derive(Debug, Clone)]
struct PreparedFeed {
    definition: FeedDefinition,
}

impl PreparedFeed {
    fn url(&self) -> String {
        match self.definition.kind {
            FeedKind::Binance => format!(
                "{BINANCE_WS_ROOT}/{}@bookTicker",
                self.definition.symbol.to_ascii_lowercase()
            ),
            FeedKind::Bybit => BYBIT_WS_URL.to_owned(),
            FeedKind::Hyperliquid => HYPERLIQUID_WS_URL.to_owned(),
            FeedKind::Lighter => LIGHTER_WS_URL.to_owned(),
        }
    }

    fn wait_for_connected_before_subscribe(&self) -> bool {
        self.definition.kind == FeedKind::Lighter
    }

    fn subscription_message(&self) -> Option<Message> {
        let symbol = &self.definition.symbol;
        let value = match self.definition.kind {
            FeedKind::Binance => return None,
            FeedKind::Bybit => serde_json::json!({
                "req_id": "live-md-bbo",
                "op": "subscribe",
                "args": [format!("orderbook.1.{symbol}")],
            }),
            FeedKind::Hyperliquid => serde_json::json!({
                "method": "subscribe",
                "subscription": {"type": "bbo", "coin": symbol},
            }),
            FeedKind::Lighter => serde_json::json!({
                "type": "subscribe",
                "channel": format!(
                    "ticker/{}",
                    self.definition
                        .market_index
                        .expect("validated Lighter feed has a market index")
                ),
            }),
        };
        Some(Message::Text(value.to_string().into()))
    }

    fn heartbeat_interval(&self) -> Duration {
        match self.definition.kind {
            FeedKind::Binance => Duration::from_secs(150),
            FeedKind::Bybit => Duration::from_secs(20),
            FeedKind::Hyperliquid | FeedKind::Lighter => Duration::from_secs(30),
        }
    }

    fn heartbeat_message(&self) -> Message {
        match self.definition.kind {
            FeedKind::Binance => Message::Ping(Vec::new().into()),
            FeedKind::Bybit => Message::Text(
                serde_json::json!({"req_id": "live-md-ping", "op": "ping"})
                    .to_string()
                    .into(),
            ),
            FeedKind::Hyperliquid => {
                Message::Text(serde_json::json!({"method": "ping"}).to_string().into())
            }
            FeedKind::Lighter => {
                Message::Text(serde_json::json!({"type": "ping"}).to_string().into())
            }
        }
    }

    fn maximum_connection_age(&self) -> Option<Duration> {
        (self.definition.kind == FeedKind::Binance).then_some(BINANCE_ROTATION_AGE)
    }

    fn parse_text(&self, text: &str) -> Result<ParsedFeedEvent, FeedError> {
        match self.definition.kind {
            FeedKind::Binance => {
                binance_bybit::parse_binance_message(text, &self.definition.symbol)
            }
            FeedKind::Bybit => binance_bybit::parse_bybit_message(text, &self.definition.symbol),
            FeedKind::Hyperliquid => {
                hyperliquid_lighter::parse_hyperliquid_message(text, &self.definition.symbol)
            }
            FeedKind::Lighter => hyperliquid_lighter::parse_lighter_message(
                text,
                &self.definition.symbol,
                self.definition
                    .market_index
                    .expect("validated Lighter feed has a market index"),
            ),
        }
    }

    fn allow_equal_source_cursor(&self) -> bool {
        self.definition.kind == FeedKind::Bybit
    }
}

#[derive(Debug)]
pub(super) struct ParsedBbo {
    pub(super) symbol: String,
    pub(super) bid_price: Decimal,
    pub(super) bid_qty: Decimal,
    pub(super) ask_price: Decimal,
    pub(super) ask_qty: Decimal,
    pub(super) exchange_timestamp_ms: u64,
    /// Optional earlier matching-engine/transaction timestamp used only for a
    /// second latency measurement; the normalized protocol timestamp remains
    /// `exchange_timestamp_ms`.
    pub(super) origin_timestamp_ms: Option<u64>,
    pub(super) source_cursor: Option<u64>,
}

#[derive(Debug)]
pub(super) enum ParsedFeedEvent {
    Bbo(ParsedBbo),
    Connected,
    SubscriptionAck,
    ServerPing,
    Pong,
    Ignore,
}

#[derive(Debug, Error)]
pub(super) enum FeedError {
    #[error("transport failure: {0}")]
    Transport(String),
    #[error("invalid upstream protocol: {0}")]
    Protocol(String),
    #[error("upstream BBO inactivity timeout")]
    BboTimeout,
    #[error("upstream WebSocket read timeout")]
    ReadTimeout,
    #[error("upstream WebSocket {0} timed out")]
    OperationTimeout(&'static str),
    #[error("feed operation canceled")]
    Cancelled,
    #[error("planned connection rotation")]
    Rotation,
}

impl FeedError {
    const fn metric_reason(&self) -> &'static str {
        match self {
            Self::Transport(_) => "transport",
            Self::Protocol(_) => "protocol",
            Self::BboTimeout => "bbo_timeout",
            Self::ReadTimeout => "read_timeout",
            Self::OperationTimeout(_) => "operation_timeout",
            Self::Cancelled => "cancelled",
            Self::Rotation => "planned_rotation",
        }
    }
}

pub(super) fn parsed_bbo(
    symbol: &str,
    bid: (&str, &str),
    ask: (&str, &str),
    exchange_timestamp_ms: u64,
    origin_timestamp_ms: Option<u64>,
    source_cursor: Option<u64>,
) -> Result<ParsedBbo, FeedError> {
    if symbol.is_empty() {
        return Err(FeedError::Protocol("empty upstream symbol".to_owned()));
    }
    if exchange_timestamp_ms == 0 || origin_timestamp_ms == Some(0) {
        return Err(FeedError::Protocol(
            "upstream timestamp must be positive".to_owned(),
        ));
    }
    let bid_price = parse_positive_decimal("bid_price", bid.0)?;
    let bid_qty = parse_positive_decimal("bid_qty", bid.1)?;
    let ask_price = parse_positive_decimal("ask_price", ask.0)?;
    let ask_qty = parse_positive_decimal("ask_qty", ask.1)?;
    if bid_price > ask_price {
        return Err(FeedError::Protocol(format!(
            "crossed upstream BBO {bid_price}/{ask_price}"
        )));
    }
    Ok(ParsedBbo {
        symbol: symbol.to_owned(),
        bid_price,
        bid_qty,
        ask_price,
        ask_qty,
        exchange_timestamp_ms,
        origin_timestamp_ms,
        source_cursor,
    })
}

fn parse_positive_decimal(field: &'static str, value: &str) -> Result<Decimal, FeedError> {
    let decimal = value
        .parse::<Decimal>()
        .map_err(|_| FeedError::Protocol(format!("{field} is not an exact decimal string")))?;
    if decimal <= Decimal::ZERO {
        return Err(FeedError::Protocol(format!("{field} must be positive")));
    }
    Ok(decimal)
}

#[derive(Debug)]
struct FeedUpdate {
    kind: FeedKind,
    generation: u64,
    bbo: ParsedBbo,
    received_at: Instant,
}

#[derive(Clone, Default)]
struct LatestMailbox {
    slots: Arc<Mutex<HashMap<FeedKind, FeedUpdate>>>,
}

impl LatestMailbox {
    fn put(&self, update: FeedUpdate) {
        let venue = update.kind.label();
        let mut slots = lock_unpoisoned(&self.slots);
        if slots.insert(update.kind, update).is_some() {
            counter!("market_data_publisher_coalesced_updates_total", "venue" => venue)
                .increment(1);
        }
        gauge!("market_data_publisher_mailbox_entries").set(slots.len() as f64);
    }

    fn drain(&self) -> Vec<FeedUpdate> {
        let mut slots = lock_unpoisoned(&self.slots);
        let mut updates: Vec<_> = slots.drain().map(|(_, update)| update).collect();
        updates.sort_by_key(|update| update.kind.index());
        gauge!("market_data_publisher_mailbox_entries").set(0.0);
        updates
    }
}

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

struct FeedHealth {
    state: Mutex<FeedHealthState>,
}

struct FeedHealthState {
    enabled: [bool; 4],
    up: [bool; 4],
    generation: [u64; 4],
}

impl FeedHealth {
    fn new(definitions: &[FeedDefinition]) -> Self {
        let mut enabled = [false; 4];
        for definition in definitions {
            enabled[definition.kind.index()] = true;
        }
        let state = FeedHealthState {
            enabled,
            up: [false; 4],
            generation: [0; 4],
        };
        for kind in FeedKind::ALL {
            if enabled[kind.index()] {
                gauge!("market_data_feed_up", "venue" => kind.label()).set(0.0);
            }
        }
        gauge!("market_data_publisher_ready").set(0.0);
        Self {
            state: Mutex::new(state),
        }
    }

    fn begin_generation(&self, kind: FeedKind) -> Option<u64> {
        let mut state = lock_unpoisoned(&self.state);
        let generation = state.generation[kind.index()].checked_add(1)?;
        state.generation[kind.index()] = generation;
        Self::set_locked(&mut state, kind, false);
        Some(generation)
    }

    fn invalidate_generation(&self, kind: FeedKind, generation: u64) -> bool {
        let mut state = lock_unpoisoned(&self.state);
        if state.generation[kind.index()] != generation {
            return false;
        }
        let Some(next_generation) = generation.checked_add(1) else {
            Self::set_locked(&mut state, kind, false);
            return false;
        };
        state.generation[kind.index()] = next_generation;
        Self::set_locked(&mut state, kind, false);
        true
    }

    fn normalize_and_publish_if_current(
        &self,
        update: FeedUpdate,
        sequencer: &mut Sequencer,
        fanout: &broadcast::Sender<PublishedFrame>,
    ) -> Result<bool, LiveMarketDataServiceError> {
        let kind = update.kind;
        let mut state = lock_unpoisoned(&self.state);
        if state.generation[kind.index()] != update.generation {
            return Ok(false);
        }
        match sequencer.normalize(update)? {
            Some(frame) => {
                let _ = fanout.send(frame);
                Self::set_locked(&mut state, kind, true);
            }
            None => Self::set_locked(&mut state, kind, false),
        }
        Ok(true)
    }

    fn set_locked(state: &mut FeedHealthState, kind: FeedKind, up: bool) {
        state.up[kind.index()] = up;
        gauge!("market_data_feed_up", "venue" => kind.label()).set(if up { 1.0 } else { 0.0 });
        let ready = FeedKind::ALL
            .iter()
            .all(|candidate| !state.enabled[candidate.index()] || state.up[candidate.index()]);
        gauge!("market_data_publisher_ready").set(if ready { 1.0 } else { 0.0 });
    }

    fn is_up_for_generation(&self, kind: FeedKind, generation: u64) -> bool {
        let state = lock_unpoisoned(&self.state);
        state.generation[kind.index()] == generation && state.up[kind.index()]
    }

    fn mark_all_down(&self) {
        let mut state = lock_unpoisoned(&self.state);
        for kind in FeedKind::ALL {
            if state.enabled[kind.index()] {
                state.up[kind.index()] = false;
                gauge!("market_data_feed_up", "venue" => kind.label()).set(0.0);
            }
        }
        gauge!("market_data_publisher_ready").set(0.0);
    }

    #[cfg(test)]
    fn is_up(&self, kind: FeedKind) -> bool {
        lock_unpoisoned(&self.state).up[kind.index()]
    }
}

#[derive(Debug, Clone)]
struct PublishedFrame {
    bytes: Arc<Vec<u8>>,
    published_at: Instant,
}

struct Sequencer {
    stream_id: String,
    sequence: HashMap<(Venue, String), u64>,
    last_exchange_timestamp: HashMap<(Venue, String), u64>,
    last_publisher_timestamp: HashMap<(Venue, String), u64>,
    max_exchange_lag_ms: u64,
    max_future_skew_ms: u64,
}

impl Sequencer {
    fn new(stream_id: String, runtime: &LiveMarketDataRuntimeConfig) -> Self {
        Self {
            stream_id,
            sequence: HashMap::new(),
            last_exchange_timestamp: HashMap::new(),
            last_publisher_timestamp: HashMap::new(),
            max_exchange_lag_ms: runtime.max_exchange_lag_ms,
            max_future_skew_ms: runtime.max_future_skew_ms,
        }
    }

    fn normalize(
        &mut self,
        update: FeedUpdate,
    ) -> Result<Option<PublishedFrame>, LiveMarketDataServiceError> {
        let started = Instant::now();
        let now_ms = unix_timestamp_ms();
        let venue = update.kind.venue();
        let venue_label = update.kind.label();
        let key = (venue, update.bbo.symbol.clone());

        histogram!("market_data_feed_receive_to_publish_seconds", "venue" => venue_label)
            .record(update.received_at.elapsed().as_secs_f64());
        if update.bbo.exchange_timestamp_ms > now_ms.saturating_add(self.max_future_skew_ms) {
            self.reject(update.kind, "future_exchange_timestamp");
            return Ok(None);
        }
        if now_ms.saturating_sub(update.bbo.exchange_timestamp_ms) > self.max_exchange_lag_ms {
            self.reject(update.kind, "stale_exchange_timestamp");
            return Ok(None);
        }
        if self
            .last_exchange_timestamp
            .get(&key)
            .is_some_and(|previous| update.bbo.exchange_timestamp_ms < *previous)
        {
            self.reject(update.kind, "non_monotonic_exchange_timestamp");
            return Ok(None);
        }
        if self
            .last_publisher_timestamp
            .get(&key)
            .is_some_and(|previous| now_ms < *previous)
        {
            self.reject(update.kind, "publisher_clock_rollback");
            return Ok(None);
        }

        record_clock_delta(
            "market_data_feed_exchange_to_publisher_lag_seconds",
            "market_data_feed_exchange_clock_ahead_seconds",
            update.bbo.exchange_timestamp_ms,
            now_ms,
            venue_label,
        );
        if let Some(origin_timestamp_ms) = update.bbo.origin_timestamp_ms {
            record_clock_delta(
                "market_data_feed_origin_to_publisher_lag_seconds",
                "market_data_feed_origin_clock_ahead_seconds",
                origin_timestamp_ms,
                now_ms,
                venue_label,
            );
        }

        let next_sequence = self
            .sequence
            .get(&key)
            .copied()
            .unwrap_or(0)
            .checked_add(1)
            .ok_or_else(|| LiveMarketDataServiceError::SequenceExhausted {
                venue,
                symbol: update.bbo.symbol.clone(),
            })?;
        let bbo = Bbo {
            schema_version: MARKET_DATA_SCHEMA_VERSION,
            environment: MarketDataEnvironment::Live,
            venue,
            symbol: update.bbo.symbol,
            bid_price: update.bbo.bid_price,
            bid_qty: update.bbo.bid_qty,
            ask_price: update.bbo.ask_price,
            ask_qty: update.bbo.ask_qty,
            exchange_timestamp_ms: update.bbo.exchange_timestamp_ms,
            publisher_timestamp_ms: now_ms,
            sequence: next_sequence,
            stream_id: self.stream_id.clone(),
        };
        let mut bytes = serde_json::to_vec(&MarketDataMessage::Bbo(bbo.clone()))?;
        if bytes.len() > MAX_MARKET_DATA_LINE_BYTES {
            return Err(LiveMarketDataServiceError::EncodedFrameTooLarge(
                bytes.len(),
            ));
        }
        bytes.push(b'\n');

        self.sequence.insert(key.clone(), next_sequence);
        self.last_exchange_timestamp
            .insert(key.clone(), bbo.exchange_timestamp_ms);
        self.last_publisher_timestamp.insert(key, now_ms);
        counter!("market_data_publisher_frames_total", "venue" => venue_label).increment(1);
        counter!("market_data_publisher_bytes_total", "venue" => venue_label)
            .increment(bytes.len() as u64);
        histogram!("market_data_publisher_normalize_seconds", "venue" => venue_label)
            .record(started.elapsed().as_secs_f64());
        gauge!("market_data_feed_last_published_timestamp_seconds", "venue" => venue_label)
            .set(now_ms as f64 / 1_000.0);
        Ok(Some(PublishedFrame {
            bytes: Arc::new(bytes),
            published_at: Instant::now(),
        }))
    }

    fn reject(&self, kind: FeedKind, reason: &'static str) {
        counter!(
            "market_data_publisher_rejected_updates_total",
            "venue" => kind.label(),
            "reason" => reason
        )
        .increment(1);
    }
}

fn record_clock_delta(
    lag_metric: &'static str,
    ahead_metric: &'static str,
    earlier_ms: u64,
    now_ms: u64,
    venue: &'static str,
) {
    if earlier_ms <= now_ms {
        histogram!(lag_metric, "venue" => venue).record((now_ms - earlier_ms) as f64 / 1_000.0);
    } else {
        histogram!(ahead_metric, "venue" => venue).record((earlier_ms - now_ms) as f64 / 1_000.0);
    }
}

#[derive(Debug, Error)]
pub enum LiveMarketDataServiceError {
    #[error(transparent)]
    Config(#[from] LiveMarketDataConfigError),
    #[error("bind live market-data listener: {0}")]
    Bind(#[source] std::io::Error),
    #[error("initialize TLS crypto provider: {0}")]
    TlsInitialization(String),
    #[error("serialize normalized market-data frame: {0}")]
    Serialize(#[from] serde_json::Error),
    #[error("encoded market-data frame is {0} bytes, above the protocol maximum")]
    EncodedFrameTooLarge(usize),
    #[error("publisher sequence exhausted for {venue}:{symbol}")]
    SequenceExhausted { venue: Venue, symbol: String },
    #[error("feed connection generation exhausted for {0}")]
    FeedGenerationExhausted(Venue),
    #[error("feed supervisor task failed: {0}")]
    FeedTask(String),
    #[error("client writer task failed: {0}")]
    ClientTask(String),
    #[error("live market-data shutdown exceeded its configured deadline")]
    ShutdownTimeout,
}

/// Run the live public market-data collector until `cancellation` is canceled.
pub async fn run_live_market_data(
    config: LiveMarketDataConfig,
    cancellation: CancellationToken,
) -> Result<(), LiveMarketDataServiceError> {
    config.validate()?;
    crate::telemetry::install_tls_crypto_provider()
        .map_err(|error| LiveMarketDataServiceError::TlsInitialization(error.to_string()))?;
    let listen_address = config.listen_socket_address()?;
    let listener = TcpListener::bind(listen_address)
        .await
        .map_err(LiveMarketDataServiceError::Bind)?;
    let definitions = config.enabled_feeds()?;
    let runtime = Arc::new(config.runtime.clone());
    let mailbox = LatestMailbox::default();
    let health = Arc::new(FeedHealth::new(&definitions));
    let stream_id = format!("live-md-{}", Uuid::new_v4());
    let mut sequencer = Sequencer::new(stream_id, &runtime);
    let (fanout, _) = broadcast::channel::<PublishedFrame>(runtime.fanout_capacity);
    let permits = Arc::new(Semaphore::new(runtime.max_clients));
    let client_count = Arc::new(AtomicUsize::new(0));
    let mut feed_tasks = JoinSet::new();
    let mut client_tasks = JoinSet::new();

    for definition in definitions {
        feed_tasks.spawn(supervise_feed(
            definition,
            Arc::clone(&runtime),
            mailbox.clone(),
            Arc::clone(&health),
            cancellation.clone(),
        ));
    }

    let mut publish_timer = time::interval(Duration::from_millis(runtime.publish_interval_ms));
    publish_timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
    publish_timer.tick().await;
    tracing::info!(%listen_address, "live public market-data publisher started");

    let outcome = 'service: loop {
        tokio::select! {
            _ = cancellation.cancelled() => break Ok(()),
            _ = publish_timer.tick() => {
                for update in mailbox.drain() {
                    let kind = update.kind;
                    match health.normalize_and_publish_if_current(update, &mut sequencer, &fanout) {
                        Ok(true) => {}
                        Ok(false) => counter!(
                            "market_data_publisher_rejected_updates_total",
                            "venue" => kind.label(),
                            "reason" => "stale_connection_generation"
                        ).increment(1),
                        Err(error) => break 'service Err(error),
                    }
                }
            }
            accepted = listener.accept() => {
                let (stream, _) = match accepted {
                    Ok(accepted) => accepted,
                    Err(error) => break Err(LiveMarketDataServiceError::Bind(error)),
                };
                let permit = match Arc::clone(&permits).try_acquire_owned() {
                    Ok(permit) => permit,
                    Err(_) => {
                        counter!("market_data_publisher_rejected_clients_total", "reason" => "capacity")
                            .increment(1);
                        continue;
                    }
                };
                if stream.set_nodelay(true).is_err() {
                    counter!("market_data_publisher_rejected_clients_total", "reason" => "tcp_nodelay")
                        .increment(1);
                    continue;
                }
                let receiver = fanout.subscribe();
                let client_runtime = Arc::clone(&runtime);
                let client_cancellation = cancellation.clone();
                let client_count = Arc::clone(&client_count);
                client_tasks.spawn(async move {
                    if let Err(error) = serve_client(
                        stream,
                        receiver,
                        permit,
                        client_count,
                        client_runtime,
                        client_cancellation,
                    ).await {
                        counter!(
                            "market_data_publisher_client_disconnects_total",
                            "reason" => error.metric_reason()
                        ).increment(1);
                        tracing::warn!(%error, "market-data consumer disconnected");
                    }
                });
            }
            joined = feed_tasks.join_next(), if !feed_tasks.is_empty() => {
                match joined {
                    Some(Ok(Ok(()))) if cancellation.is_cancelled() => {},
                    Some(Ok(Ok(()))) => break Err(LiveMarketDataServiceError::FeedTask(
                        "supervisor exited unexpectedly".to_owned()
                    )),
                    Some(Ok(Err(error))) => break Err(error),
                    Some(Err(error)) => break Err(LiveMarketDataServiceError::FeedTask(error.to_string())),
                    None => {},
                }
            }
            joined = client_tasks.join_next(), if !client_tasks.is_empty() => {
                if let Some(Err(error)) = joined {
                    break Err(LiveMarketDataServiceError::ClientTask(error.to_string()));
                }
            }
        }
    };

    cancellation.cancel();
    health.mark_all_down();
    let shutdown = async {
        while let Some(joined) = feed_tasks.join_next().await {
            if let Err(error) = joined {
                tracing::warn!(%error, "feed task failed during shutdown");
            }
        }
        while let Some(joined) = client_tasks.join_next().await {
            if let Err(error) = joined {
                tracing::warn!(%error, "client task failed during shutdown");
            }
        }
    };
    if time::timeout(Duration::from_millis(runtime.shutdown_timeout_ms), shutdown)
        .await
        .is_err()
    {
        feed_tasks.abort_all();
        client_tasks.abort_all();
        return Err(LiveMarketDataServiceError::ShutdownTimeout);
    }
    outcome
}

async fn supervise_feed(
    definition: FeedDefinition,
    runtime: Arc<LiveMarketDataRuntimeConfig>,
    mailbox: LatestMailbox,
    health: Arc<FeedHealth>,
    cancellation: CancellationToken,
) -> Result<(), LiveMarketDataServiceError> {
    let mut backoff = Duration::from_millis(runtime.reconnect_initial_ms);
    let maximum_backoff = Duration::from_millis(runtime.reconnect_max_ms);
    loop {
        if cancellation.is_cancelled() {
            return Ok(());
        }
        let generation = health.begin_generation(definition.kind).ok_or(
            LiveMarketDataServiceError::FeedGenerationExhausted(definition.kind.venue()),
        )?;
        let attempt_started = Instant::now();
        let result = run_feed_once(
            definition.clone(),
            generation,
            &runtime,
            &mailbox,
            &health,
            &cancellation,
        )
        .await;
        if !health.invalidate_generation(definition.kind, generation) {
            return Err(LiveMarketDataServiceError::FeedGenerationExhausted(
                definition.kind.venue(),
            ));
        }
        if cancellation.is_cancelled() {
            return Ok(());
        }
        let error = match result {
            Ok(()) => FeedError::Transport("WebSocket ended without a close reason".to_owned()),
            Err(error) => error,
        };
        counter!(
            "market_data_feed_reconnects_total",
            "venue" => definition.kind.label(),
            "reason" => error.metric_reason()
        )
        .increment(1);
        tracing::warn!(
            venue = definition.kind.label(),
            %error,
            retry_ms = backoff.as_millis(),
            "live market-data feed unavailable; reconnecting"
        );
        if attempt_started.elapsed() >= HEALTHY_CONNECTION_AGE {
            backoff = Duration::from_millis(runtime.reconnect_initial_ms);
        }
        tokio::select! {
            _ = cancellation.cancelled() => return Ok(()),
            _ = time::sleep(backoff) => {},
        }
        backoff = backoff.saturating_mul(2).min(maximum_backoff);
    }
}

async fn prepare_feed(
    definition: FeedDefinition,
    runtime: &LiveMarketDataRuntimeConfig,
) -> Result<PreparedFeed, FeedError> {
    if definition.kind == FeedKind::Lighter {
        let market_index = definition
            .market_index
            .ok_or_else(|| FeedError::Protocol("Lighter market index is missing".to_owned()))?;
        let url = format!("{LIGHTER_HTTP_URL}/api/v1/orderBookDetails");
        let started = Instant::now();
        let client = reqwest::Client::builder()
            .timeout(Duration::from_millis(runtime.http_timeout_ms))
            .build()
            .map_err(|error| FeedError::Transport(error.to_string()))?;
        let mut response = client
            .get(url)
            .query(&[("market_id", market_index)])
            .send()
            .await
            .map_err(|error| FeedError::Transport(error.to_string()))?;
        histogram!("market_data_feed_metadata_seconds", "venue" => "lighter")
            .record(started.elapsed().as_secs_f64());
        if !response.status().is_success() {
            return Err(FeedError::Transport(format!(
                "Lighter metadata returned HTTP {}",
                response.status()
            )));
        }
        if response
            .content_length()
            .is_some_and(|length| length > MAX_LIGHTER_METADATA_BYTES as u64)
        {
            return Err(FeedError::Protocol(
                "Lighter metadata response exceeded the size limit".to_owned(),
            ));
        }
        let initial_capacity = response
            .content_length()
            .and_then(|length| usize::try_from(length).ok())
            .unwrap_or(0)
            .min(MAX_LIGHTER_METADATA_BYTES);
        let mut body = Vec::with_capacity(initial_capacity);
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|error| FeedError::Transport(error.to_string()))?
        {
            let next_length = body.len().checked_add(chunk.len()).ok_or_else(|| {
                FeedError::Protocol("Lighter metadata response size overflowed".to_owned())
            })?;
            if next_length > MAX_LIGHTER_METADATA_BYTES {
                return Err(FeedError::Protocol(
                    "Lighter metadata response exceeded the size limit".to_owned(),
                ));
            }
            body.extend_from_slice(&chunk);
        }
        hyperliquid_lighter::validate_lighter_market_details(
            &body,
            &definition.symbol,
            market_index,
        )?;
    }
    Ok(PreparedFeed { definition })
}

async fn run_feed_once(
    definition: FeedDefinition,
    generation: u64,
    runtime: &LiveMarketDataRuntimeConfig,
    mailbox: &LatestMailbox,
    health: &FeedHealth,
    cancellation: &CancellationToken,
) -> Result<(), FeedError> {
    let feed = tokio::select! {
        _ = cancellation.cancelled() => return Err(FeedError::Cancelled),
        result = prepare_feed(definition, runtime) => result?,
    };
    let venue = feed.definition.kind.label();
    let websocket_config = WebSocketConfig::default()
        .read_buffer_size(64 * 1024)
        .write_buffer_size(0)
        .max_write_buffer_size(64 * 1024)
        .max_message_size(Some(runtime.max_websocket_message_bytes))
        .max_frame_size(Some(runtime.max_websocket_message_bytes));
    let connect_started = Instant::now();
    let operation_timeout = Duration::from_millis(runtime.websocket_operation_timeout_ms);
    let connect_result = tokio::select! {
        _ = cancellation.cancelled() => return Err(FeedError::Cancelled),
        result = time::timeout(
            operation_timeout,
            connect_async_with_config(feed.url(), Some(websocket_config), true),
        ) => result.map_err(|_| FeedError::OperationTimeout("connect"))?,
    };
    let (stream, _) = connect_result.map_err(|error| FeedError::Transport(error.to_string()))?;
    histogram!("market_data_feed_ws_connect_seconds", "venue" => venue)
        .record(connect_started.elapsed().as_secs_f64());
    let (mut writer, mut reader) = stream.split();
    let mut subscription_sent = false;
    if !feed.wait_for_connected_before_subscribe() {
        if let Some(message) = feed.subscription_message() {
            send_websocket_message(
                &mut writer,
                message,
                operation_timeout,
                venue,
                "subscribe",
                cancellation,
            )
            .await?;
        }
        subscription_sent = true;
    }

    let mut heartbeat = time::interval(feed.heartbeat_interval());
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
    heartbeat.tick().await;
    let read_deadline = time::sleep(Duration::from_millis(runtime.websocket_read_timeout_ms));
    tokio::pin!(read_deadline);
    let bbo_deadline = time::sleep(Duration::from_millis(runtime.bbo_timeout_ms));
    tokio::pin!(bbo_deadline);
    let rotation = async {
        match feed.maximum_connection_age() {
            Some(age) => time::sleep(age).await,
            None => pending::<()>().await,
        }
    };
    tokio::pin!(rotation);
    let mut last_source_cursor = None;

    loop {
        tokio::select! {
            _ = cancellation.cancelled() => return Ok(()),
            _ = &mut rotation => return Err(FeedError::Rotation),
            _ = &mut read_deadline => return Err(FeedError::ReadTimeout),
            _ = &mut bbo_deadline => return Err(FeedError::BboTimeout),
            _ = heartbeat.tick() => {
                send_websocket_message(
                    &mut writer,
                    feed.heartbeat_message(),
                    operation_timeout,
                    venue,
                    "heartbeat",
                    cancellation,
                ).await?;
            }
            message = reader.next() => {
                let message = message
                    .ok_or_else(|| FeedError::Transport("upstream WebSocket closed".to_owned()))?
                    .map_err(|error| FeedError::Transport(error.to_string()))?;
                let received_at = Instant::now();
                read_deadline
                    .as_mut()
                    .reset(TokioInstant::now() + Duration::from_millis(runtime.websocket_read_timeout_ms));
                match message {
                    Message::Text(text) => {
                        let parse_started = Instant::now();
                        let event = feed.parse_text(&text)?;
                        histogram!("market_data_feed_ws_parse_seconds", "venue" => venue)
                            .record(parse_started.elapsed().as_secs_f64());
                        match event {
                            ParsedFeedEvent::Bbo(bbo) => {
                                if let Some(cursor) = bbo.source_cursor {
                                    if last_source_cursor.is_some_and(|previous| cursor < previous) {
                                        return Err(FeedError::Protocol(
                                            "upstream source cursor regressed".to_owned()
                                        ));
                                    }
                                    if last_source_cursor == Some(cursor)
                                        && !feed.allow_equal_source_cursor()
                                    {
                                        counter!(
                                            "market_data_feed_duplicate_updates_total",
                                            "venue" => venue
                                        ).increment(1);
                                        continue;
                                    }
                                    last_source_cursor = Some(cursor);
                                }
                                counter!("market_data_feed_updates_total", "venue" => venue).increment(1);
                                gauge!("market_data_feed_last_received_timestamp_seconds", "venue" => venue)
                                    .set(unix_timestamp_ms() as f64 / 1_000.0);
                                mailbox.put(FeedUpdate {
                                    kind: feed.definition.kind,
                                    generation,
                                    bbo,
                                    received_at,
                                });
                                if health.is_up_for_generation(feed.definition.kind, generation) {
                                    bbo_deadline.as_mut().reset(
                                        TokioInstant::now()
                                            + Duration::from_millis(runtime.bbo_timeout_ms),
                                    );
                                }
                            }
                            ParsedFeedEvent::Connected => {
                                if feed.wait_for_connected_before_subscribe() && !subscription_sent {
                                    let message = feed.subscription_message().ok_or_else(|| {
                                        FeedError::Protocol("missing subscription message".to_owned())
                                    })?;
                                    send_websocket_message(
                                        &mut writer,
                                        message,
                                        operation_timeout,
                                        venue,
                                        "subscribe",
                                        cancellation,
                                    ).await?;
                                    subscription_sent = true;
                                }
                            }
                            ParsedFeedEvent::ServerPing => {
                                send_websocket_message(
                                    &mut writer,
                                    Message::Text(
                                        serde_json::json!({"type": "pong"}).to_string().into()
                                    ),
                                    operation_timeout,
                                    venue,
                                    "application pong",
                                    cancellation,
                                ).await?;
                            }
                            ParsedFeedEvent::SubscriptionAck
                            | ParsedFeedEvent::Pong
                            | ParsedFeedEvent::Ignore => {}
                        }
                    }
                    Message::Ping(payload) => {
                        send_websocket_message(
                            &mut writer,
                            Message::Pong(payload),
                            operation_timeout,
                            venue,
                            "protocol pong",
                            cancellation,
                        ).await?;
                    }
                    Message::Pong(_) => {}
                    Message::Close(frame) => {
                        return Err(FeedError::Transport(format!(
                            "upstream WebSocket closed ({})",
                            frame.map_or("no status".to_owned(), |frame| frame.code.to_string())
                        )));
                    }
                    Message::Binary(_) => {
                        return Err(FeedError::Protocol(
                            "unexpected binary WebSocket message".to_owned()
                        ));
                    }
                    Message::Frame(_) => {}
                }
            }
        }
    }
}

async fn send_websocket_message<S>(
    writer: &mut S,
    message: Message,
    timeout: Duration,
    venue: &'static str,
    operation: &'static str,
    cancellation: &CancellationToken,
) -> Result<(), FeedError>
where
    S: Sink<Message> + Unpin,
    S::Error: std::fmt::Display,
{
    let started = Instant::now();
    let outcome = tokio::select! {
        _ = cancellation.cancelled() => return Err(FeedError::Cancelled),
        outcome = time::timeout(timeout, writer.send(message)) => outcome,
    };
    histogram!(
        "market_data_feed_ws_write_seconds",
        "venue" => venue,
        "operation" => operation
    )
    .record(started.elapsed().as_secs_f64());
    match outcome {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => Err(FeedError::Transport(error.to_string())),
        Err(_) => Err(FeedError::OperationTimeout(operation)),
    }
}

#[derive(Debug, Error)]
enum ClientError {
    #[error("consumer fell behind by {0} frames")]
    Lagged(u64),
    #[error("consumer frame exceeded its queue-age budget")]
    StaleFrame,
    #[error("consumer write timed out")]
    WriteTimeout,
    #[error("consumer write failed: {0}")]
    Write(std::io::Error),
}

impl ClientError {
    const fn metric_reason(&self) -> &'static str {
        match self {
            Self::Lagged(_) => "lagged",
            Self::StaleFrame => "stale_frame",
            Self::WriteTimeout => "write_timeout",
            Self::Write(_) => "write_error",
        }
    }
}

struct ClientCountGuard {
    count: Arc<AtomicUsize>,
}

impl ClientCountGuard {
    fn new(count: Arc<AtomicUsize>) -> Self {
        let current = count.fetch_add(1, Ordering::AcqRel) + 1;
        gauge!("market_data_publisher_clients").set(current as f64);
        Self { count }
    }
}

impl Drop for ClientCountGuard {
    fn drop(&mut self) {
        let current = self.count.fetch_sub(1, Ordering::AcqRel) - 1;
        gauge!("market_data_publisher_clients").set(current as f64);
    }
}

async fn serve_client(
    mut stream: TcpStream,
    mut receiver: broadcast::Receiver<PublishedFrame>,
    _permit: OwnedSemaphorePermit,
    client_count: Arc<AtomicUsize>,
    runtime: Arc<LiveMarketDataRuntimeConfig>,
    cancellation: CancellationToken,
) -> Result<(), ClientError> {
    let _count = ClientCountGuard::new(client_count);
    loop {
        let frame = tokio::select! {
            _ = cancellation.cancelled() => {
                let _ = time::timeout(
                    Duration::from_millis(runtime.client_write_timeout_ms),
                    stream.shutdown(),
                ).await;
                return Ok(());
            }
            received = receiver.recv() => match received {
                Ok(frame) => frame,
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    return Err(ClientError::Lagged(skipped));
                }
                Err(broadcast::error::RecvError::Closed) => return Ok(()),
            }
        };
        let queue_latency = frame.published_at.elapsed();
        histogram!("market_data_publisher_client_queue_seconds")
            .record(queue_latency.as_secs_f64());
        if queue_latency > Duration::from_millis(runtime.client_max_frame_age_ms) {
            return Err(ClientError::StaleFrame);
        }
        let write_started = Instant::now();
        match time::timeout(
            Duration::from_millis(runtime.client_write_timeout_ms),
            stream.write_all(&frame.bytes),
        )
        .await
        {
            Ok(Ok(())) => {
                histogram!("market_data_publisher_client_write_seconds")
                    .record(write_started.elapsed().as_secs_f64());
            }
            Ok(Err(error)) => return Err(ClientError::Write(error)),
            Err(_) => return Err(ClientError::WriteTimeout),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::market_data::decode_market_data_frame;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use tokio::io::AsyncReadExt;

    struct PendingSink;

    impl Sink<Message> for PendingSink {
        type Error = std::io::Error;

        fn poll_ready(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Pending
        }

        fn start_send(self: Pin<&mut Self>, _item: Message) -> Result<(), Self::Error> {
            Ok(())
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Pending
        }

        fn poll_close(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
    }

    fn config() -> LiveMarketDataConfig {
        LiveMarketDataConfig {
            schema_version: LIVE_MARKET_DATA_CONFIG_SCHEMA_VERSION,
            listen_address: "127.0.0.1:9101".to_owned(),
            metrics_listen_address: "127.0.0.1:9900".to_owned(),
            log_filter: "info".to_owned(),
            runtime: LiveMarketDataRuntimeConfig::default(),
            feeds: LiveFeedConfigs {
                binance: SymbolFeedConfig {
                    enabled: true,
                    symbol: "BTCUSDT".to_owned(),
                },
                bybit: SymbolFeedConfig {
                    enabled: true,
                    symbol: "BTCUSDT".to_owned(),
                },
                hyperliquid: SymbolFeedConfig {
                    enabled: true,
                    symbol: "BTC".to_owned(),
                },
                lighter: LighterFeedConfig {
                    enabled: true,
                    symbol: "BTC".to_owned(),
                    market_index: 1,
                },
            },
        }
    }

    fn update(timestamp_ms: u64) -> FeedUpdate {
        FeedUpdate {
            kind: FeedKind::Hyperliquid,
            generation: 1,
            bbo: parsed_bbo(
                "BTC",
                ("99999.9", "1.25"),
                ("100000.0", "2.50"),
                timestamp_ms,
                None,
                None,
            )
            .expect("valid BBO"),
            received_at: Instant::now(),
        }
    }

    #[test]
    fn sample_config_is_valid_and_live_only_endpoints_are_hard_wired() {
        config().validate().expect("valid config");
        assert!(!BINANCE_WS_ROOT.contains("testnet"));
        assert!(!BYBIT_WS_URL.contains("testnet"));
        assert!(!HYPERLIQUID_WS_URL.contains("testnet"));
        assert!(!LIGHTER_WS_URL.contains("testnet"));
    }

    #[test]
    fn shipped_live_market_data_config_parses() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("config/live-market-data.toml");
        LiveMarketDataConfig::load(path).expect("shipped config is valid");
    }

    #[test]
    fn no_feed_is_rejected() {
        let mut config = config();
        config.feeds.binance.enabled = false;
        config.feeds.bybit.enabled = false;
        config.feeds.hyperliquid.enabled = false;
        config.feeds.lighter.enabled = false;
        assert!(config.validate().is_err());
    }

    #[test]
    fn invalid_log_filter_is_rejected_during_validation() {
        let mut config = config();
        config.log_filter = "wide_quoter=[".to_owned();
        assert!(config.validate().is_err());
    }

    #[test]
    fn sequencer_emits_protocol_v1_live_frame() {
        let now = unix_timestamp_ms();
        let mut sequencer = Sequencer::new(
            "live-md-test".to_owned(),
            &LiveMarketDataRuntimeConfig::default(),
        );
        let frame = sequencer
            .normalize(update(now))
            .expect("normalize")
            .expect("accepted");
        let line = std::str::from_utf8(&frame.bytes).expect("UTF-8");
        let bbo = decode_market_data_frame(line.trim_end()).expect("wire frame decodes");
        assert_eq!(bbo.environment, MarketDataEnvironment::Live);
        assert_eq!(bbo.venue, Venue::Hyperliquid);
        assert_eq!(bbo.sequence, 1);
        assert_eq!(bbo.stream_id, "live-md-test");
    }

    #[test]
    fn timestamp_regression_does_not_advance_sequence() {
        let now = unix_timestamp_ms();
        let runtime = LiveMarketDataRuntimeConfig {
            max_exchange_lag_ms: 10_000,
            ..LiveMarketDataRuntimeConfig::default()
        };
        let mut sequencer = Sequencer::new("live-md-test".to_owned(), &runtime);
        assert!(sequencer.normalize(update(now)).expect("first").is_some());
        assert!(sequencer
            .normalize(update(now - 1))
            .expect("regression")
            .is_none());
        let next = sequencer
            .normalize(update(now))
            .expect("next")
            .expect("accepted");
        let line = std::str::from_utf8(&next.bytes).expect("UTF-8");
        let bbo = decode_market_data_frame(line.trim_end()).expect("wire frame decodes");
        assert_eq!(bbo.sequence, 2);
    }

    #[test]
    fn mailbox_keeps_only_the_latest_update_per_venue() {
        let mailbox = LatestMailbox::default();
        let now = unix_timestamp_ms();
        mailbox.put(update(now));
        mailbox.put(update(now + 1));
        let drained = mailbox.drain();
        assert_eq!(drained.len(), 1);
        assert_eq!(drained[0].bbo.exchange_timestamp_ms, now + 1);
    }

    #[test]
    fn stale_connection_generation_cannot_restore_feed_health() {
        let definition = FeedDefinition {
            kind: FeedKind::Hyperliquid,
            symbol: "BTC".to_owned(),
            market_index: None,
        };
        let health = FeedHealth::new(&[definition]);
        let generation = health
            .begin_generation(FeedKind::Hyperliquid)
            .expect("generation");
        let runtime = LiveMarketDataRuntimeConfig::default();
        let mut sequencer = Sequencer::new("live-md-test".to_owned(), &runtime);
        let (fanout, mut receiver) = broadcast::channel(4);
        let mut first = update(unix_timestamp_ms());
        first.generation = generation;
        assert!(health
            .normalize_and_publish_if_current(first, &mut sequencer, &fanout)
            .expect("publish current generation"));
        assert!(health.is_up(FeedKind::Hyperliquid));
        receiver.try_recv().expect("current frame published");

        assert!(health.invalidate_generation(FeedKind::Hyperliquid, generation));
        let mut stale = update(unix_timestamp_ms());
        stale.generation = generation;
        assert!(!health
            .normalize_and_publish_if_current(stale, &mut sequencer, &fanout)
            .expect("reject stale generation"));
        assert!(!health.is_up(FeedKind::Hyperliquid));
        assert!(matches!(
            receiver.try_recv(),
            Err(broadcast::error::TryRecvError::Empty)
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn websocket_send_is_deadline_bounded_and_cancelable() {
        let mut sink = PendingSink;
        let cancellation = CancellationToken::new();
        let result = send_websocket_message(
            &mut sink,
            Message::Text("heartbeat".into()),
            Duration::from_millis(10),
            "test",
            "heartbeat",
            &cancellation,
        )
        .await;
        assert!(matches!(
            result,
            Err(FeedError::OperationTimeout("heartbeat"))
        ));

        cancellation.cancel();
        let result = send_websocket_message(
            &mut sink,
            Message::Text("heartbeat".into()),
            Duration::from_secs(1),
            "test",
            "heartbeat",
            &cancellation,
        )
        .await;
        assert!(matches!(result, Err(FeedError::Cancelled)));
    }

    #[tokio::test]
    async fn two_tcp_consumers_receive_identical_globally_encoded_frame() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let address = listener.local_addr().expect("address");
        let (client_one, accepted_one) =
            tokio::join!(TcpStream::connect(address), listener.accept());
        let mut client_one = client_one.expect("connect first client");
        let (server_one, _) = accepted_one.expect("accept first client");
        let (client_two, accepted_two) =
            tokio::join!(TcpStream::connect(address), listener.accept());
        let mut client_two = client_two.expect("connect second client");
        let (server_two, _) = accepted_two.expect("accept second client");

        let runtime = Arc::new(LiveMarketDataRuntimeConfig::default());
        let cancellation = CancellationToken::new();
        let permits = Arc::new(Semaphore::new(2));
        let count = Arc::new(AtomicUsize::new(0));
        let (fanout, _) = broadcast::channel(4);
        let first = tokio::spawn(serve_client(
            server_one,
            fanout.subscribe(),
            Arc::clone(&permits)
                .acquire_owned()
                .await
                .expect("first permit"),
            Arc::clone(&count),
            Arc::clone(&runtime),
            cancellation.clone(),
        ));
        let second = tokio::spawn(serve_client(
            server_two,
            fanout.subscribe(),
            permits.acquire_owned().await.expect("second permit"),
            count,
            runtime,
            cancellation.clone(),
        ));

        let expected = b"{\"type\":\"test\"}\n".to_vec();
        fanout
            .send(PublishedFrame {
                bytes: Arc::new(expected.clone()),
                published_at: Instant::now(),
            })
            .expect("fanout");
        let mut received_one = vec![0; expected.len()];
        let mut received_two = vec![0; expected.len()];
        time::timeout(
            Duration::from_secs(1),
            client_one.read_exact(&mut received_one),
        )
        .await
        .expect("first read deadline")
        .expect("first read");
        time::timeout(
            Duration::from_secs(1),
            client_two.read_exact(&mut received_two),
        )
        .await
        .expect("second read deadline")
        .expect("second read");
        assert_eq!(received_one, expected);
        assert_eq!(received_two, expected);

        cancellation.cancel();
        first.await.expect("first task").expect("first shutdown");
        second.await.expect("second task").expect("second shutdown");
    }
}
