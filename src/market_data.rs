//! Client for the local market-data publisher.
//!
//! The publisher speaks newline-delimited JSON over TCP.  Keeping this boundary
//! deliberately small lets the trading process consume the same normalized BBO
//! stream regardless of which process owns the exchange WebSocket connections.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use futures_util::StreamExt;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tokio::task::JoinHandle;
use tokio_util::codec::{FramedRead, LinesCodec, LinesCodecError};

use crate::types::{unix_timestamp_ms, Venue};

/// Wire schema understood by this release.
pub const MARKET_DATA_SCHEMA_VERSION: u16 = 1;

/// Maximum JSON payload, excluding the trailing newline.
pub const MAX_MARKET_DATA_LINE_BYTES: usize = 64 * 1024;

/// A single publisher is expected to expose only a small, stable set of
/// markets and session streams. Hard limits make caller-controlled symbols and
/// stream IDs a bounded-memory input while still allowing years of restarts.
const MAX_TRACKED_MARKETS: usize = 1_024;
const MAX_TRACKED_STREAMS: usize = 4_096;

pub const METRIC_PARSE_LATENCY: &str = "market_data_parse_latency_seconds";
pub const METRIC_PUBLISHER_LAG: &str = "market_data_publisher_to_consumer_lag_seconds";
pub const METRIC_EXCHANGE_LAG: &str = "market_data_exchange_to_consumer_lag_seconds";
pub const METRIC_RECONNECTS: &str = "market_data_reconnects_total";
pub const METRIC_INVALID_FRAMES: &str = "market_data_invalid_frames_total";
pub const METRIC_SEQUENCE_GAPS: &str = "market_data_sequence_gaps_total";
pub const METRIC_CONSUMER_DROPS: &str = "market_data_consumer_queue_drops_total";

fn default_reconnect_delay_ms() -> u64 {
    500
}

const fn default_max_publisher_lag_ms() -> u64 {
    500
}

const fn default_max_exchange_lag_ms() -> u64 {
    2_500
}

const fn default_max_future_skew_ms() -> u64 {
    1_000
}

/// One TCP source published by the external market-data process.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketDataSourceConfig {
    /// Stable, low-cardinality name used in logs and telemetry.
    pub name: String,
    /// TCP endpoint, for example `127.0.0.1:9101`.
    pub address: String,
    #[serde(default = "default_reconnect_delay_ms")]
    pub reconnect_delay_ms: u64,
    /// Frames older than this according to the publisher's wall clock are
    /// rejected before they can refresh a quote source.
    #[serde(default = "default_max_publisher_lag_ms")]
    pub max_publisher_lag_ms: u64,
    /// Frames older than this according to the exchange event timestamp are
    /// rejected before they can refresh a quote source.
    #[serde(default = "default_max_exchange_lag_ms")]
    pub max_exchange_lag_ms: u64,
    /// Tolerated positive clock skew for either remote timestamp.
    #[serde(default = "default_max_future_skew_ms")]
    pub max_future_skew_ms: u64,
}

/// Best bid and offer in the venue's native price and quantity units.
///
/// Decimal values are strings on the wire. This prevents an intermediate JSON
/// floating-point representation from silently changing a price or quantity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Bbo {
    pub schema_version: u16,
    /// Live execution refuses deliberately synthetic feeds. This does not
    /// authenticate a publisher; operators must still treat the configured IPC
    /// endpoint as a trusted boundary.
    pub environment: MarketDataEnvironment,
    pub venue: Venue,
    pub symbol: String,
    #[serde(with = "rust_decimal::serde::str")]
    pub bid_price: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub bid_qty: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub ask_price: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub ask_qty: Decimal,
    pub exchange_timestamp_ms: u64,
    pub publisher_timestamp_ms: u64,
    pub sequence: u64,
    /// Changes whenever a publisher restarts a logical stream. This permits a
    /// new stream to restart its sequence without looking like a regression.
    pub stream_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketDataEnvironment {
    Live,
    Synthetic,
}

/// Versioned market-data wire envelope. It serializes as a flat object with a
/// `"type":"bbo"` discriminator followed by the fields in [`Bbo`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MarketDataMessage {
    Bbo(Bbo),
}

/// Events delivered to strategies. This enum is intentionally distinct from
/// the wire envelope so future wire-only control messages cannot leak into a
/// strategy by accident.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MarketDataEvent {
    Bbo {
        bbo: Bbo,
        /// Captured as soon as this process parses the socket frame, before it
        /// waits in the bounded consumer queue.
        received_at: Instant,
    },
}

impl MarketDataEvent {
    pub const fn as_bbo(&self) -> &Bbo {
        match self {
            Self::Bbo { bbo, .. } => bbo,
        }
    }

    pub fn into_bbo(self) -> Bbo {
        match self {
            Self::Bbo { bbo, .. } => bbo,
        }
    }

    pub const fn received_at(&self) -> Instant {
        match self {
            Self::Bbo { received_at, .. } => *received_at,
        }
    }

    pub fn into_parts(self) -> (Bbo, Instant) {
        match self {
            Self::Bbo { bbo, received_at } => (bbo, received_at),
        }
    }
}

#[derive(Debug, Error)]
pub enum MarketDataFrameError {
    #[error("invalid JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("unsupported market-data schema version {received}; expected {expected}")]
    UnsupportedSchemaVersion { expected: u16, received: u16 },
    #[error("symbol must not be empty")]
    EmptySymbol,
    #[error("stream_id must not be empty")]
    EmptyStreamId,
    #[error("{field} must be positive")]
    NonPositive { field: &'static str },
    #[error("crossed BBO: bid {bid_price} is above ask {ask_price}")]
    Crossed {
        bid_price: Decimal,
        ask_price: Decimal,
    },
    #[error(
        "non-monotonic sequence for stream {stream_id}: received {received}, previous {previous}"
    )]
    NonMonotonicSequence {
        stream_id: String,
        previous: u64,
        received: u64,
    },
    #[error(
        "non-monotonic {field} for {venue}:{symbol}: received {received}, previous {previous}"
    )]
    NonMonotonicTimestamp {
        field: &'static str,
        venue: Venue,
        symbol: String,
        previous: u64,
        received: u64,
    },
    #[error("market-data tracker {kind} capacity exceeded (maximum {maximum})")]
    TrackerCapacityExceeded { kind: &'static str, maximum: usize },
    #[error("{field} timestamp is stale by {lag_ms} ms; configured maximum is {max_lag_ms} ms")]
    StaleTimestamp {
        field: &'static str,
        lag_ms: u64,
        max_lag_ms: u64,
    },
    #[error(
        "{field} timestamp is {ahead_ms} ms in the future; configured tolerance is {max_future_skew_ms} ms"
    )]
    FutureTimestamp {
        field: &'static str,
        ahead_ms: u64,
        max_future_skew_ms: u64,
    },
}

impl MarketDataFrameError {
    fn metric_reason(&self) -> &'static str {
        match self {
            Self::Json(_) => "invalid_json",
            Self::UnsupportedSchemaVersion { .. } => "unsupported_schema_version",
            Self::EmptySymbol => "empty_symbol",
            Self::EmptyStreamId => "empty_stream_id",
            Self::NonPositive { .. } => "nonpositive_bbo",
            Self::Crossed { .. } => "crossed_bbo",
            Self::NonMonotonicSequence { .. } => "non_monotonic_sequence",
            Self::NonMonotonicTimestamp {
                field: "publisher_timestamp_ms",
                ..
            } => "non_monotonic_publisher_timestamp",
            Self::NonMonotonicTimestamp {
                field: "exchange_timestamp_ms",
                ..
            } => "non_monotonic_exchange_timestamp",
            Self::NonMonotonicTimestamp { .. } => "non_monotonic_timestamp",
            Self::TrackerCapacityExceeded { kind, .. } => kind,
            Self::StaleTimestamp {
                field: "publisher_timestamp_ms",
                ..
            } => "stale_publisher_timestamp",
            Self::StaleTimestamp {
                field: "exchange_timestamp_ms",
                ..
            } => "stale_exchange_timestamp",
            Self::FutureTimestamp { .. } => "future_timestamp",
            Self::StaleTimestamp { .. } => "stale_timestamp",
        }
    }
}

/// Decode and validate one complete NDJSON payload.
///
/// Sequence validation is connection-client state and is therefore performed
/// by [`MarketDataClient`] after this stateless validation step.
pub fn decode_market_data_frame(frame: &str) -> Result<Bbo, MarketDataFrameError> {
    let message: MarketDataMessage = serde_json::from_str(frame)?;
    let MarketDataMessage::Bbo(bbo) = message;
    validate_bbo(&bbo)?;
    Ok(bbo)
}

fn validate_bbo(bbo: &Bbo) -> Result<(), MarketDataFrameError> {
    if bbo.schema_version != MARKET_DATA_SCHEMA_VERSION {
        return Err(MarketDataFrameError::UnsupportedSchemaVersion {
            expected: MARKET_DATA_SCHEMA_VERSION,
            received: bbo.schema_version,
        });
    }
    if bbo.symbol.trim().is_empty() {
        return Err(MarketDataFrameError::EmptySymbol);
    }
    if bbo.stream_id.trim().is_empty() {
        return Err(MarketDataFrameError::EmptyStreamId);
    }
    for (field, value) in [
        ("bid_price", bbo.bid_price),
        ("bid_qty", bbo.bid_qty),
        ("ask_price", bbo.ask_price),
        ("ask_qty", bbo.ask_qty),
    ] {
        if value <= Decimal::ZERO {
            return Err(MarketDataFrameError::NonPositive { field });
        }
    }
    if bbo.bid_price > bbo.ask_price {
        return Err(MarketDataFrameError::Crossed {
            bid_price: bbo.bid_price,
            ask_price: bbo.ask_price,
        });
    }
    Ok(())
}

fn validate_wall_clock_freshness(
    bbo: &Bbo,
    now_ms: u64,
    max_publisher_lag_ms: u64,
    max_exchange_lag_ms: u64,
    max_future_skew_ms: u64,
) -> Result<(), MarketDataFrameError> {
    validate_remote_timestamp(
        "publisher_timestamp_ms",
        bbo.publisher_timestamp_ms,
        now_ms,
        max_publisher_lag_ms,
        max_future_skew_ms,
    )?;
    validate_remote_timestamp(
        "exchange_timestamp_ms",
        bbo.exchange_timestamp_ms,
        now_ms,
        max_exchange_lag_ms,
        max_future_skew_ms,
    )
}

fn validate_remote_timestamp(
    field: &'static str,
    timestamp_ms: u64,
    now_ms: u64,
    max_lag_ms: u64,
    max_future_skew_ms: u64,
) -> Result<(), MarketDataFrameError> {
    if timestamp_ms > now_ms {
        let ahead_ms = timestamp_ms - now_ms;
        if ahead_ms > max_future_skew_ms {
            return Err(MarketDataFrameError::FutureTimestamp {
                field,
                ahead_ms,
                max_future_skew_ms,
            });
        }
    } else {
        let lag_ms = now_ms - timestamp_ms;
        if lag_ms > max_lag_ms {
            return Err(MarketDataFrameError::StaleTimestamp {
                field,
                lag_ms,
                max_lag_ms,
            });
        }
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct StreamKey {
    venue: Venue,
    symbol: String,
    stream_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct MarketKey {
    venue: Venue,
    symbol: String,
}

impl From<&Bbo> for MarketKey {
    fn from(bbo: &Bbo) -> Self {
        Self {
            venue: bbo.venue,
            symbol: bbo.symbol.clone(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct LastTimestamps {
    publisher: u64,
    exchange: u64,
}

impl From<&Bbo> for StreamKey {
    fn from(bbo: &Bbo) -> Self {
        Self {
            venue: bbo.venue,
            symbol: bbo.symbol.clone(),
            stream_id: bbo.stream_id.clone(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SequenceGap {
    missing: u64,
}

struct SequenceTracker {
    previous_sequences: HashMap<StreamKey, u64>,
    previous_timestamps: HashMap<MarketKey, LastTimestamps>,
    max_streams: usize,
    max_markets: usize,
}

impl Default for SequenceTracker {
    fn default() -> Self {
        Self {
            previous_sequences: HashMap::new(),
            previous_timestamps: HashMap::new(),
            max_streams: MAX_TRACKED_STREAMS,
            max_markets: MAX_TRACKED_MARKETS,
        }
    }
}

impl SequenceTracker {
    fn observe(&mut self, bbo: &Bbo) -> Result<Option<SequenceGap>, MarketDataFrameError> {
        let stream_key = StreamKey::from(bbo);
        let market_key = MarketKey::from(bbo);

        if !self.previous_sequences.contains_key(&stream_key)
            && self.previous_sequences.len() >= self.max_streams
        {
            return Err(MarketDataFrameError::TrackerCapacityExceeded {
                kind: "stream_capacity_exceeded",
                maximum: self.max_streams,
            });
        }
        if !self.previous_timestamps.contains_key(&market_key)
            && self.previous_timestamps.len() >= self.max_markets
        {
            return Err(MarketDataFrameError::TrackerCapacityExceeded {
                kind: "market_capacity_exceeded",
                maximum: self.max_markets,
            });
        }

        if let Some(previous) = self.previous_timestamps.get(&market_key) {
            validate_monotonic_timestamp(
                "publisher_timestamp_ms",
                bbo,
                previous.publisher,
                bbo.publisher_timestamp_ms,
            )?;
            validate_monotonic_timestamp(
                "exchange_timestamp_ms",
                bbo,
                previous.exchange,
                bbo.exchange_timestamp_ms,
            )?;
        }

        let gap = if let Some(previous) = self.previous_sequences.get(&stream_key).copied() {
            if bbo.sequence <= previous {
                return Err(MarketDataFrameError::NonMonotonicSequence {
                    stream_id: bbo.stream_id.clone(),
                    previous,
                    received: bbo.sequence,
                });
            }

            // `sequence` is greater than `previous`, so `previous` cannot be
            // u64::MAX and this addition is safe.
            let expected = previous + 1;
            (bbo.sequence > expected).then_some(SequenceGap {
                missing: bbo.sequence - expected,
            })
        } else {
            None
        };

        self.previous_sequences.insert(stream_key, bbo.sequence);
        self.previous_timestamps.insert(
            market_key,
            LastTimestamps {
                publisher: bbo.publisher_timestamp_ms,
                exchange: bbo.exchange_timestamp_ms,
            },
        );
        Ok(gap)
    }

    #[cfg(test)]
    fn with_limits(max_streams: usize, max_markets: usize) -> Self {
        Self {
            max_streams,
            max_markets,
            ..Self::default()
        }
    }
}

fn validate_monotonic_timestamp(
    field: &'static str,
    bbo: &Bbo,
    previous: u64,
    received: u64,
) -> Result<(), MarketDataFrameError> {
    if received < previous {
        return Err(MarketDataFrameError::NonMonotonicTimestamp {
            field,
            venue: bbo.venue,
            symbol: bbo.symbol.clone(),
            previous,
            received,
        });
    }
    Ok(())
}

/// Reconnecting reader for a single external market-data TCP source.
pub struct MarketDataClient {
    config: MarketDataSourceConfig,
    sequences: SequenceTracker,
}

impl MarketDataClient {
    pub fn new(config: MarketDataSourceConfig) -> Self {
        Self {
            config,
            sequences: SequenceTracker::default(),
        }
    }

    /// Start a detached reader and return its bounded event channel plus task.
    pub fn spawn(
        config: MarketDataSourceConfig,
        channel_capacity: usize,
    ) -> (mpsc::Receiver<MarketDataEvent>, JoinHandle<()>) {
        let (sender, receiver) = mpsc::channel(channel_capacity.max(1));
        let task = tokio::spawn(Self::new(config).run(sender));
        (receiver, task)
    }

    /// Run until every receiver is dropped. Transport and framing failures do
    /// not terminate the client; it reconnects after the configured delay.
    pub async fn run(mut self, sender: mpsc::Sender<MarketDataEvent>) {
        let reconnect_delay = Duration::from_millis(self.config.reconnect_delay_ms.max(1));

        loop {
            if sender.is_closed() {
                return;
            }

            let connection = tokio::select! {
                _ = sender.closed() => return,
                result = TcpStream::connect(&self.config.address) => result,
            };

            let stream = match connection {
                Ok(stream) => stream,
                Err(error) => {
                    tracing::warn!(
                        source = %self.config.name,
                        address = %self.config.address,
                        %error,
                        "market-data connection failed"
                    );
                    self.record_reconnect("connect_error");
                    if wait_for_reconnect_or_close(reconnect_delay, &sender).await {
                        return;
                    }
                    continue;
                }
            };

            tracing::info!(
                source = %self.config.name,
                address = %self.config.address,
                "market-data source connected"
            );

            match self.consume_connection(stream, &sender).await {
                ConnectionEnd::ReceiverClosed => return,
                ConnectionEnd::Eof => self.record_reconnect("eof"),
                ConnectionEnd::ReadError => self.record_reconnect("read_error"),
            }

            if wait_for_reconnect_or_close(reconnect_delay, &sender).await {
                return;
            }
        }
    }

    async fn consume_connection(
        &mut self,
        stream: TcpStream,
        sender: &mpsc::Sender<MarketDataEvent>,
    ) -> ConnectionEnd {
        let mut frames = FramedRead::new(
            stream,
            LinesCodec::new_with_max_length(MAX_MARKET_DATA_LINE_BYTES),
        );

        loop {
            let next = tokio::select! {
                _ = sender.closed() => return ConnectionEnd::ReceiverClosed,
                next = frames.next() => next,
            };

            match next {
                Some(Ok(frame)) => {
                    if let Some(event) = self.process_frame(&frame) {
                        match sender.try_send(event) {
                            Ok(()) => {}
                            Err(TrySendError::Full(event)) => {
                                let bbo = event.as_bbo();
                                metrics::counter!(
                                    METRIC_CONSUMER_DROPS,
                                    "source" => self.config.name.clone(),
                                    "venue" => bbo.venue.to_string(),
                                    "symbol" => bbo.symbol.clone()
                                )
                                .increment(1);
                            }
                            Err(TrySendError::Closed(_)) => {
                                return ConnectionEnd::ReceiverClosed;
                            }
                        }
                    }
                }
                Some(Err(LinesCodecError::MaxLineLengthExceeded)) => {
                    self.record_invalid("line_too_long");
                    tracing::warn!(
                        source = %self.config.name,
                        max_bytes = MAX_MARKET_DATA_LINE_BYTES,
                        "discarded oversized market-data frame"
                    );
                }
                Some(Err(LinesCodecError::Io(error))) => {
                    tracing::warn!(source = %self.config.name, %error, "market-data read failed");
                    return ConnectionEnd::ReadError;
                }
                None => return ConnectionEnd::Eof,
            }
        }
    }

    fn process_frame(&mut self, frame: &str) -> Option<MarketDataEvent> {
        let received_at = Instant::now();
        let started = Instant::now();
        let parsed = decode_market_data_frame(frame).and_then(|bbo| {
            validate_wall_clock_freshness(
                &bbo,
                unix_timestamp_ms(),
                self.config.max_publisher_lag_ms,
                self.config.max_exchange_lag_ms,
                self.config.max_future_skew_ms,
            )?;
            self.sequences.observe(&bbo).map(|gap| (bbo, gap))
        });

        metrics::histogram!(
            METRIC_PARSE_LATENCY,
            "source" => self.config.name.clone()
        )
        .record(started.elapsed().as_secs_f64());

        match parsed {
            Ok((bbo, gap)) => {
                if let Some(gap) = gap {
                    metrics::counter!(
                        METRIC_SEQUENCE_GAPS,
                        "source" => self.config.name.clone(),
                        "venue" => bbo.venue.to_string(),
                        "symbol" => bbo.symbol.clone()
                    )
                    .increment(gap.missing);
                    tracing::warn!(
                        source = %self.config.name,
                        venue = %bbo.venue,
                        symbol = %bbo.symbol,
                        stream_id = %bbo.stream_id,
                        missing = gap.missing,
                        sequence = bbo.sequence,
                        "market-data sequence gap"
                    );
                }
                self.record_lag(&bbo);
                Some(MarketDataEvent::Bbo { bbo, received_at })
            }
            Err(error) => {
                self.record_invalid(error.metric_reason());
                tracing::warn!(source = %self.config.name, %error, "discarded market-data frame");
                None
            }
        }
    }

    fn record_lag(&self, bbo: &Bbo) {
        let now = unix_timestamp_ms();
        let source = self.config.name.clone();
        let venue = bbo.venue.to_string();
        let symbol = bbo.symbol.clone();

        if bbo.publisher_timestamp_ms <= now {
            metrics::histogram!(
                METRIC_PUBLISHER_LAG,
                "source" => source.clone(),
                "venue" => venue.clone(),
                "symbol" => symbol.clone()
            )
            .record((now - bbo.publisher_timestamp_ms) as f64 / 1_000.0);
        }
        if bbo.exchange_timestamp_ms <= now {
            metrics::histogram!(
                METRIC_EXCHANGE_LAG,
                "source" => source,
                "venue" => venue,
                "symbol" => symbol
            )
            .record((now - bbo.exchange_timestamp_ms) as f64 / 1_000.0);
        }
    }

    fn record_invalid(&self, reason: &'static str) {
        metrics::counter!(
            METRIC_INVALID_FRAMES,
            "source" => self.config.name.clone(),
            "reason" => reason
        )
        .increment(1);
    }

    fn record_reconnect(&self, reason: &'static str) {
        metrics::counter!(
            METRIC_RECONNECTS,
            "source" => self.config.name.clone(),
            "reason" => reason
        )
        .increment(1);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConnectionEnd {
    ReceiverClosed,
    Eof,
    ReadError,
}

/// Returns true when the consumer closed while waiting.
async fn wait_for_reconnect_or_close(
    delay: Duration,
    sender: &mpsc::Sender<MarketDataEvent>,
) -> bool {
    tokio::select! {
        _ = sender.closed() => true,
        _ = tokio::time::sleep(delay) => false,
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use pretty_assertions::assert_eq;
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpListener;

    use super::*;

    fn sample_bbo(sequence: u64, stream_id: &str) -> Bbo {
        let now = unix_timestamp_ms();
        Bbo {
            schema_version: MARKET_DATA_SCHEMA_VERSION,
            environment: MarketDataEnvironment::Live,
            venue: Venue::Binance,
            symbol: "BTC-USDT-PERP".to_owned(),
            bid_price: Decimal::from_str("64231.10").unwrap(),
            bid_qty: Decimal::from_str("0.12500").unwrap(),
            ask_price: Decimal::from_str("64231.20").unwrap(),
            ask_qty: Decimal::from_str("0.25000").unwrap(),
            exchange_timestamp_ms: now - 2,
            publisher_timestamp_ms: now - 1,
            sequence,
            stream_id: stream_id.to_owned(),
        }
    }

    fn encode(bbo: Bbo) -> String {
        serde_json::to_string(&MarketDataMessage::Bbo(bbo)).unwrap()
    }

    fn config(address: String) -> MarketDataSourceConfig {
        MarketDataSourceConfig {
            name: "test-publisher".to_owned(),
            address,
            reconnect_delay_ms: 5,
            max_publisher_lag_ms: 500,
            max_exchange_lag_ms: 2_500,
            max_future_skew_ms: 1_000,
        }
    }

    #[test]
    fn round_trip_preserves_decimal_strings_and_wire_shape() {
        let expected = sample_bbo(7, "publisher-a");
        let encoded = encode(expected.clone());
        let json: serde_json::Value = serde_json::from_str(&encoded).unwrap();

        assert_eq!(json["type"], "bbo");
        assert_eq!(json["schema_version"], 1);
        assert_eq!(json["environment"], "live");
        assert_eq!(json["bid_price"], "64231.10");
        assert_eq!(json["bid_qty"], "0.12500");
        assert_eq!(decode_market_data_frame(&encoded).unwrap(), expected);
    }

    #[test]
    fn rejects_numeric_decimal_fields() {
        let mut json = serde_json::to_value(MarketDataMessage::Bbo(sample_bbo(1, "a"))).unwrap();
        json["bid_price"] = serde_json::json!(64231.10);

        assert!(matches!(
            decode_market_data_frame(&json.to_string()),
            Err(MarketDataFrameError::Json(_))
        ));
    }

    #[test]
    fn requires_an_explicit_market_data_environment() {
        let mut json = serde_json::to_value(MarketDataMessage::Bbo(sample_bbo(1, "a"))).unwrap();
        json.as_object_mut().unwrap().remove("environment");

        assert!(matches!(
            decode_market_data_frame(&json.to_string()),
            Err(MarketDataFrameError::Json(_))
        ));
    }

    #[test]
    fn rejects_unknown_schema_nonpositive_and_crossed_books() {
        let mut bbo = sample_bbo(1, "a");
        bbo.schema_version = 2;
        assert!(matches!(
            decode_market_data_frame(&encode(bbo)),
            Err(MarketDataFrameError::UnsupportedSchemaVersion {
                expected: 1,
                received: 2
            })
        ));

        let mut bbo = sample_bbo(1, "a");
        bbo.ask_qty = Decimal::ZERO;
        assert!(matches!(
            decode_market_data_frame(&encode(bbo)),
            Err(MarketDataFrameError::NonPositive { field: "ask_qty" })
        ));

        let mut bbo = sample_bbo(1, "a");
        bbo.bid_price = bbo.ask_price + Decimal::ONE;
        assert!(matches!(
            decode_market_data_frame(&encode(bbo)),
            Err(MarketDataFrameError::Crossed { .. })
        ));
    }

    #[test]
    fn detects_gaps_and_drops_regressions_per_stream() {
        let mut tracker = SequenceTracker::default();
        assert_eq!(tracker.observe(&sample_bbo(10, "a")).unwrap(), None);
        assert_eq!(tracker.observe(&sample_bbo(11, "a")).unwrap(), None);
        assert_eq!(
            tracker.observe(&sample_bbo(14, "a")).unwrap(),
            Some(SequenceGap { missing: 2 })
        );
        assert!(matches!(
            tracker.observe(&sample_bbo(14, "a")),
            Err(MarketDataFrameError::NonMonotonicSequence {
                previous: 14,
                received: 14,
                ..
            })
        ));
        assert!(matches!(
            tracker.observe(&sample_bbo(13, "a")),
            Err(MarketDataFrameError::NonMonotonicSequence {
                previous: 14,
                received: 13,
                ..
            })
        ));

        // A publisher restart must use a new stream ID; that new stream gets
        // an independent sequence baseline.
        assert_eq!(tracker.observe(&sample_bbo(1, "b")).unwrap(), None);
    }

    #[test]
    fn rejects_timestamp_rollback_across_stream_rotation() {
        let mut tracker = SequenceTracker::default();
        let mut first = sample_bbo(10, "session-a");
        first.publisher_timestamp_ms = 1_000;
        first.exchange_timestamp_ms = 900;
        tracker.observe(&first).expect("initial observation");

        let mut publisher_replay = sample_bbo(1, "session-b");
        publisher_replay.publisher_timestamp_ms = 999;
        publisher_replay.exchange_timestamp_ms = 901;
        assert!(matches!(
            tracker.observe(&publisher_replay),
            Err(MarketDataFrameError::NonMonotonicTimestamp {
                field: "publisher_timestamp_ms",
                previous: 1_000,
                received: 999,
                ..
            })
        ));

        // A rejected frame must not advance either timestamp baseline.
        let mut exchange_replay = sample_bbo(1, "session-c");
        exchange_replay.publisher_timestamp_ms = 1_001;
        exchange_replay.exchange_timestamp_ms = 899;
        assert!(matches!(
            tracker.observe(&exchange_replay),
            Err(MarketDataFrameError::NonMonotonicTimestamp {
                field: "exchange_timestamp_ms",
                previous: 900,
                received: 899,
                ..
            })
        ));
    }

    #[test]
    fn tracker_rejects_new_keys_at_its_memory_bound() {
        let mut tracker = SequenceTracker::with_limits(2, 2);
        let mut first = sample_bbo(1, "session-a");
        first.publisher_timestamp_ms = 1_000;
        first.exchange_timestamp_ms = 900;
        tracker.observe(&first).expect("first stream");

        let mut second = sample_bbo(1, "session-b");
        second.publisher_timestamp_ms = 1_001;
        second.exchange_timestamp_ms = 901;
        tracker.observe(&second).expect("second stream");

        let mut third = sample_bbo(1, "session-c");
        third.publisher_timestamp_ms = 1_002;
        third.exchange_timestamp_ms = 902;
        assert!(matches!(
            tracker.observe(&third),
            Err(MarketDataFrameError::TrackerCapacityExceeded {
                kind: "stream_capacity_exceeded",
                maximum: 2,
            })
        ));

        let mut market_tracker = SequenceTracker::with_limits(4, 1);
        market_tracker.observe(&first).expect("first market");
        let mut other_market = sample_bbo(1, "other-market");
        other_market.symbol = "ETH-USDT-PERP".to_owned();
        assert!(matches!(
            market_tracker.observe(&other_market),
            Err(MarketDataFrameError::TrackerCapacityExceeded {
                kind: "market_capacity_exceeded",
                maximum: 1,
            })
        ));
    }

    #[test]
    fn locked_book_is_not_crossed() {
        let mut bbo = sample_bbo(1, "a");
        bbo.ask_price = bbo.bid_price;
        assert!(decode_market_data_frame(&encode(bbo)).is_ok());
    }

    #[test]
    fn rejects_stale_and_implausibly_future_remote_timestamps() {
        let now = 1_800_000_000_000_u64;
        let mut bbo = sample_bbo(1, "a");
        bbo.publisher_timestamp_ms = now - 501;
        bbo.exchange_timestamp_ms = now;
        assert!(matches!(
            validate_wall_clock_freshness(&bbo, now, 500, 2_500, 1_000),
            Err(MarketDataFrameError::StaleTimestamp {
                field: "publisher_timestamp_ms",
                ..
            })
        ));

        bbo.publisher_timestamp_ms = now;
        bbo.exchange_timestamp_ms = now + 1_001;
        assert!(matches!(
            validate_wall_clock_freshness(&bbo, now, 500, 2_500, 1_000),
            Err(MarketDataFrameError::FutureTimestamp {
                field: "exchange_timestamp_ms",
                ..
            })
        ));
    }

    #[tokio::test]
    async fn reconnects_and_keeps_sequence_state_across_connections() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let server = tokio::spawn(async move {
            for sequence in [41, 42] {
                let (mut socket, _) = listener.accept().await.unwrap();
                let line = encode(sample_bbo(sequence, "publisher-a"));
                socket.write_all(line.as_bytes()).await.unwrap();
                socket.write_all(b"\n").await.unwrap();
                socket.shutdown().await.unwrap();
            }
        });

        let (mut receiver, client) = MarketDataClient::spawn(config(address), 4);
        let first = tokio::time::timeout(Duration::from_secs(2), receiver.recv())
            .await
            .unwrap()
            .unwrap()
            .into_bbo();
        let second = tokio::time::timeout(Duration::from_secs(2), receiver.recv())
            .await
            .unwrap()
            .unwrap()
            .into_bbo();

        assert_eq!((first.sequence, second.sequence), (41, 42));
        server.await.unwrap();
        drop(receiver);
        tokio::time::timeout(Duration::from_secs(1), client)
            .await
            .expect("client should stop when receiver closes")
            .unwrap();
    }

    #[tokio::test]
    async fn oversized_line_is_discarded_without_reaching_consumer() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            socket
                .write_all(&vec![b'x'; MAX_MARKET_DATA_LINE_BYTES + 1])
                .await
                .unwrap();
            socket.write_all(b"\n").await.unwrap();
            socket.shutdown().await.unwrap();
        });

        let (mut receiver, client) = MarketDataClient::spawn(config(address), 1);
        server.await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(50), receiver.recv())
                .await
                .is_err()
        );
        drop(receiver);
        tokio::time::timeout(Duration::from_secs(1), client)
            .await
            .unwrap()
            .unwrap();
    }
}
