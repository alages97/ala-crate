//! Causal lead/lag analysis for raw [`crate::pump_recorder`] captures.
//!
//! The analysis clock is always `receive_monotonic_ns`. Exchange timestamps
//! are deliberately ignored for ordering because the venues do not share a
//! clock. A follower quote is selected with a strict as-of lookup, and it must
//! pass the configured local-receive freshness limit at every endpoint.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use crate::pump_recorder::PUMP_CAPTURE_SCHEMA_VERSION;

pub const PUMP_LEAD_LAG_SCHEMA_VERSION: u16 = 3;
pub const DEFAULT_HORIZONS_MS: [u64; 7] = [25, 50, 100, 250, 500, 1_000, 2_000];

const NANOS_PER_MILLISECOND: u64 = 1_000_000;
const BASIS_POINTS: f64 = 10_000.0;

const BINANCE_SOURCE: &str = "binance_pump_perp";
const HYPERLIQUID_SPOT_SOURCE: &str = "hyperliquid_upump_spot";
const HYPERLIQUID_PERP_SOURCE: &str = "hyperliquid_pump_perp";
const LIGHTER_SOURCE: &str = "lighter_pump_perp";
const PUMPSWAP_SOURCE: &str = "pumpswap_spot";

const PUMPSWAP_BUY_DISCRIMINATOR: [u8; 8] = [103, 244, 82, 31, 44, 245, 119, 119];
const PUMPSWAP_SELL_DISCRIMINATOR: [u8; 8] = [62, 47, 55, 10, 165, 3, 220, 42];
const PUMPSWAP_POOL_RAW: [u8; 32] = [
    0x1c, 0x3d, 0xc9, 0xa3, 0x61, 0xc8, 0x25, 0xcc, 0x92, 0xc1, 0x57, 0xb0, 0x3f, 0x88, 0x35, 0xfc,
    0x0a, 0x48, 0x12, 0xe7, 0x7b, 0x9c, 0x57, 0x6b, 0xd9, 0x65, 0xfc, 0x42, 0x5d, 0xab, 0x55, 0xf2,
];

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PumpMarket {
    BinancePerp,
    HyperliquidSpot,
    HyperliquidPerp,
    LighterPerp,
    #[serde(rename = "pumpswap_spot")]
    PumpSwapSpot,
}

impl PumpMarket {
    const fn label(self) -> &'static str {
        match self {
            Self::BinancePerp => "binance_perp",
            Self::HyperliquidSpot => "hyperliquid_spot",
            Self::HyperliquidPerp => "hyperliquid_perp",
            Self::LighterPerp => "lighter_perp",
            Self::PumpSwapSpot => "pumpswap_spot",
        }
    }
}

impl std::fmt::Display for PumpMarket {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.label())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AnalysisSegment {
    Full,
    FirstHalf,
    SecondHalf,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PumpLeadLagConfig {
    pub max_quote_age_ms: u64,
    pub min_leader_move_bps: f64,
    #[serde(default = "default_horizons")]
    pub horizons_ms: Vec<u64>,
}

fn default_horizons() -> Vec<u64> {
    DEFAULT_HORIZONS_MS.to_vec()
}

impl Default for PumpLeadLagConfig {
    fn default() -> Self {
        Self {
            max_quote_age_ms: 1_000,
            min_leader_move_bps: 0.0,
            horizons_ms: default_horizons(),
        }
    }
}

impl PumpLeadLagConfig {
    pub fn validate(&self) -> Result<(), PumpLeadLagError> {
        if self.max_quote_age_ms == 0 {
            return Err(PumpLeadLagError::InvalidConfig(
                "max_quote_age_ms must be positive".to_owned(),
            ));
        }
        if !self.min_leader_move_bps.is_finite() || self.min_leader_move_bps < 0.0 {
            return Err(PumpLeadLagError::InvalidConfig(
                "min_leader_move_bps must be finite and non-negative".to_owned(),
            ));
        }
        if self.horizons_ms.is_empty()
            || self.horizons_ms.contains(&0)
            || self.horizons_ms.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return Err(PumpLeadLagError::InvalidConfig(
                "horizons_ms must be non-empty, positive, and strictly increasing".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct PumpMarketSummary {
    pub market: PumpMarket,
    pub events: usize,
    pub price_changes: usize,
    pub first_receive_monotonic_ns: u64,
    pub last_receive_monotonic_ns: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct PumpLeadLagMetric {
    pub leader: PumpMarket,
    pub follower: PumpMarket,
    pub segment: AnalysisSegment,
    pub segment_start_monotonic_ns: u64,
    pub segment_end_monotonic_ns: u64,
    pub horizon_ms: u64,
    pub candidate_leader_moves: usize,
    pub paired_samples: usize,
    pub paired_coverage: Option<f64>,
    /// Approximate number of response windows simultaneously represented by
    /// the paired samples. Values above one make dependence from overlap clear.
    pub overlap_load: Option<f64>,
    pub positive_follower_responses: usize,
    pub correlation: Option<f64>,
    pub beta: Option<f64>,
    pub mean_signed_leader_move_bps: Option<f64>,
    pub mean_absolute_leader_move_bps: Option<f64>,
    pub mean_signed_follower_response_bps: Option<f64>,
    /// Positive means the leader/follower mid-price gap closed in the direction
    /// implied by the leader's initiating move.
    pub mean_paired_spread_convergence_bps: Option<f64>,
    pub paired_spread_convergence_bps_p50: Option<f64>,
    pub paired_spread_convergence_bps_p95: Option<f64>,
    pub paired_spread_convergence_bps_p99: Option<f64>,
    pub paired_spread_convergence_bps_max: Option<f64>,
    /// The same response statistics after accepting at most one signal per
    /// horizon. These samples do not reuse time within this ordered pair.
    pub non_overlapping_samples: usize,
    pub non_overlapping_positive_follower_responses: usize,
    pub non_overlapping_mean_signed_follower_response_bps: Option<f64>,
    pub non_overlapping_mean_paired_spread_convergence_bps: Option<f64>,
    pub non_overlapping_spread_convergence_bps_p50: Option<f64>,
    pub non_overlapping_spread_convergence_bps_p95: Option<f64>,
    pub non_overlapping_spread_convergence_bps_p99: Option<f64>,
    pub non_overlapping_spread_convergence_bps_max: Option<f64>,
    pub follower_now_age_ms_p50: Option<f64>,
    pub follower_now_age_ms_p95: Option<f64>,
    pub leader_future_age_ms_p50: Option<f64>,
    pub leader_future_age_ms_p95: Option<f64>,
    pub follower_future_age_ms_p50: Option<f64>,
    pub follower_future_age_ms_p95: Option<f64>,
}

/// Mutually exclusive first-arrival episodes for one unordered market pair.
/// A qualifying shock on either venue starts an episode and suppresses shocks
/// from both venues until the response horizon has elapsed.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct PumpCommonShockMetric {
    pub pair_market_a: PumpMarket,
    pub pair_market_b: PumpMarket,
    pub leader: PumpMarket,
    pub follower: PumpMarket,
    pub segment: AnalysisSegment,
    pub segment_start_monotonic_ns: u64,
    pub segment_end_monotonic_ns: u64,
    pub horizon_ms: u64,
    pub episodes: usize,
    pub paired_samples: usize,
    pub paired_coverage: Option<f64>,
    pub leader_episode_share: Option<f64>,
    pub positive_follower_responses: usize,
    pub mean_absolute_leader_move_bps: Option<f64>,
    pub mean_signed_follower_response_bps: Option<f64>,
    pub mean_paired_spread_convergence_bps: Option<f64>,
    pub paired_spread_convergence_bps_p50: Option<f64>,
    pub paired_spread_convergence_bps_p95: Option<f64>,
    pub paired_spread_convergence_bps_p99: Option<f64>,
    pub paired_spread_convergence_bps_max: Option<f64>,
    pub follower_now_age_ms_p50: Option<f64>,
    pub follower_now_age_ms_p95: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct PumpLeadLagReport {
    pub schema_version: u16,
    pub capture_id: String,
    pub input_path: PathBuf,
    pub input_records: u64,
    pub ignored_records: u64,
    pub decoded_mid_events: usize,
    pub max_quote_age_ms: u64,
    pub min_leader_move_bps: f64,
    pub horizons_ms: Vec<u64>,
    pub markets: Vec<PumpMarketSummary>,
    pub metrics: Vec<PumpLeadLagMetric>,
    pub common_shock_metrics: Vec<PumpCommonShockMetric>,
    pub caveats: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PumpLeadLagReportPaths {
    pub summary_json: PathBuf,
    pub metrics_csv: PathBuf,
    pub common_shocks_csv: PathBuf,
}

#[derive(Debug, Error)]
pub enum PumpLeadLagError {
    #[error("invalid PUMP lead/lag configuration: {0}")]
    InvalidConfig(String),
    #[error("read PUMP capture: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid capture JSON on line {line}: {source}")]
    CaptureJson {
        line: usize,
        #[source]
        source: serde_json::Error,
    },
    #[error("invalid capture record on line {line}: {reason}")]
    InvalidRecord { line: usize, reason: String },
    #[error("capture contains fewer than two decodable markets")]
    InsufficientMarkets,
    #[error("serialize PUMP lead/lag JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("serialize PUMP lead/lag CSV: {0}")]
    Csv(#[from] csv::Error),
}

#[derive(Debug, Deserialize)]
struct CaptureRecord {
    schema_version: u16,
    capture_id: String,
    local_sequence: u64,
    record_type: String,
    source: String,
    stream: String,
    connection_generation: u64,
    receive_monotonic_ns: u64,
    payload: Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct EventKey {
    receive_monotonic_ns: u64,
    local_sequence: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PumpSwapEventKind {
    Other,
    Bootstrap,
    VaultPair,
    SwapLog,
}

#[derive(Debug, Clone)]
struct MidEvent {
    market: PumpMarket,
    key: EventKey,
    connection_generation: u64,
    mid: f64,
    pumpswap_slot: Option<u64>,
    pumpswap_kind: PumpSwapEventKind,
}

impl MidEvent {
    fn new(market: PumpMarket, record: &CaptureRecord, mid: f64) -> Result<Self, String> {
        validate_price(mid)?;
        Ok(Self {
            market,
            key: EventKey {
                receive_monotonic_ns: record.receive_monotonic_ns,
                local_sequence: record.local_sequence,
            },
            connection_generation: record.connection_generation,
            mid,
            pumpswap_slot: None,
            pumpswap_kind: PumpSwapEventKind::Other,
        })
    }
}

#[derive(Debug, Default)]
struct PartialVaultSlot {
    base: Option<u64>,
    quote: Option<u64>,
}

#[derive(Debug, Default)]
struct PumpSwapDecoder {
    connection_generation: Option<u64>,
    bootstrap_floor_slot: Option<u64>,
    pending_vaults: BTreeMap<u64, PartialVaultSlot>,
}

#[derive(Debug)]
struct CaptureData {
    capture_id: String,
    input_records: u64,
    ignored_records: u64,
    events: BTreeMap<PumpMarket, Vec<MidEvent>>,
}

pub fn analyze_pump_capture(
    input_path: impl AsRef<Path>,
    config: &PumpLeadLagConfig,
) -> Result<PumpLeadLagReport, PumpLeadLagError> {
    config.validate()?;
    let input_path = input_path.as_ref();
    let data = load_capture(input_path)?;
    if data.events.len() < 2 {
        return Err(PumpLeadLagError::InsufficientMarkets);
    }

    let markets = summarize_markets(&data.events);
    let decoded_mid_events = markets.iter().map(|market| market.events).sum();
    let max_age_ns = config
        .max_quote_age_ms
        .checked_mul(NANOS_PER_MILLISECOND)
        .ok_or_else(|| {
            PumpLeadLagError::InvalidConfig("max_quote_age_ms is too large".to_owned())
        })?;
    let mut metrics = Vec::new();
    let available: Vec<_> = data.events.keys().copied().collect();
    for leader in &available {
        for follower in &available {
            if leader == follower {
                continue;
            }
            let leader_events = &data.events[leader];
            let follower_events = &data.events[follower];
            let pair_start = leader_events
                .first()
                .expect("non-empty event vectors")
                .key
                .receive_monotonic_ns
                .max(
                    follower_events
                        .first()
                        .expect("non-empty event vectors")
                        .key
                        .receive_monotonic_ns,
                );
            let pair_end = leader_events
                .last()
                .expect("non-empty event vectors")
                .key
                .receive_monotonic_ns
                .min(
                    follower_events
                        .last()
                        .expect("non-empty event vectors")
                        .key
                        .receive_monotonic_ns,
                );
            if pair_start >= pair_end {
                continue;
            }
            let midpoint = pair_start + (pair_end - pair_start) / 2;
            let segments = [
                (AnalysisSegment::Full, pair_start, pair_end),
                (AnalysisSegment::FirstHalf, pair_start, midpoint),
                (AnalysisSegment::SecondHalf, midpoint, pair_end),
            ];
            for horizon_ms in &config.horizons_ms {
                let horizon_ns =
                    horizon_ms
                        .checked_mul(NANOS_PER_MILLISECOND)
                        .ok_or_else(|| {
                            PumpLeadLagError::InvalidConfig("horizon is too large".to_owned())
                        })?;
                for (segment, start, end) in segments {
                    metrics.push(analyze_ordered_pair(
                        *leader,
                        *follower,
                        leader_events,
                        follower_events,
                        segment,
                        start,
                        end,
                        *horizon_ms,
                        horizon_ns,
                        max_age_ns,
                        config.min_leader_move_bps,
                    ));
                }
            }
        }
    }
    let common_shock_metrics = analyze_common_shocks(
        &available,
        &data.events,
        &config.horizons_ms,
        max_age_ns,
        config.min_leader_move_bps,
    )?;

    Ok(PumpLeadLagReport {
        schema_version: PUMP_LEAD_LAG_SCHEMA_VERSION,
        capture_id: data.capture_id,
        input_path: input_path.to_path_buf(),
        input_records: data.input_records,
        ignored_records: data.ignored_records,
        decoded_mid_events,
        max_quote_age_ms: config.max_quote_age_ms,
        min_leader_move_bps: config.min_leader_move_bps,
        horizons_ms: config.horizons_ms.clone(),
        markets,
        metrics,
        common_shock_metrics,
        caveats: vec![
            "Ordering uses local receive_monotonic_ns, not unsynchronized venue timestamps."
                .to_owned(),
            "Results measure mid-price response and paired spread convergence, not executable fills or net PnL."
                .to_owned(),
            "Public RPC/WebSocket delivery latency can be mistaken for venue price-discovery latency."
                .to_owned(),
            "Overlapping event-conditioned horizons are descriptive and do not imply independent samples."
                .to_owned(),
            "Non-overlapping rows use a per-direction refractory window; they are still a descriptive screen, not a matched-shock significance test."
                .to_owned(),
            "Common-shock rows use one first-arrival clock per pair and horizon, but do not standardize the two venues' shock-size distributions or provide confidence intervals."
                .to_owned(),
            "Binance is USDT-denominated while the other series are USDC-denominated or collateralized."
                .to_owned(),
            "PumpSwap account and log subscriptions use processed commitment and may roll back; only successful transaction logs are decoded."
                .to_owned(),
        ],
    })
}

pub fn write_pump_lead_lag_report(
    report: &PumpLeadLagReport,
    output_directory: impl AsRef<Path>,
) -> Result<PumpLeadLagReportPaths, PumpLeadLagError> {
    let output_directory = output_directory.as_ref();
    fs::create_dir_all(output_directory)?;
    let paths = PumpLeadLagReportPaths {
        summary_json: output_directory.join("summary.json"),
        metrics_csv: output_directory.join("lead_lag.csv"),
        common_shocks_csv: output_directory.join("common_shocks.csv"),
    };

    let mut summary = BufWriter::new(File::create(&paths.summary_json)?);
    serde_json::to_writer_pretty(&mut summary, report)?;
    summary.write_all(b"\n")?;
    summary.flush()?;

    let mut metrics = csv::Writer::from_path(&paths.metrics_csv)?;
    for row in &report.metrics {
        metrics.serialize(row)?;
    }
    metrics.flush()?;

    let mut common_shocks = csv::Writer::from_path(&paths.common_shocks_csv)?;
    for row in &report.common_shock_metrics {
        common_shocks.serialize(row)?;
    }
    common_shocks.flush()?;
    Ok(paths)
}

#[allow(clippy::too_many_arguments)]
fn analyze_ordered_pair(
    leader: PumpMarket,
    follower: PumpMarket,
    leader_events: &[MidEvent],
    follower_events: &[MidEvent],
    segment: AnalysisSegment,
    segment_start: u64,
    segment_end: u64,
    horizon_ms: u64,
    horizon_ns: u64,
    max_age_ns: u64,
    min_leader_move_bps: f64,
) -> PumpLeadLagMetric {
    let mut accumulator = BivariateAccumulator::default();
    let mut candidates = 0_usize;
    let mut positive_responses = 0_usize;
    let mut signed_response_sum = 0.0;
    let mut convergence_sum = 0.0;
    let mut leader_move_bps_sum = 0.0;
    let mut absolute_leader_move_bps_sum = 0.0;
    let mut next_non_overlapping_signal_time = segment_start;
    let mut non_overlapping_samples = 0_usize;
    let mut non_overlapping_positive_responses = 0_usize;
    let mut non_overlapping_signed_response_sum = 0.0;
    let mut non_overlapping_convergence_sum = 0.0;
    let mut follower_now_ages = Vec::new();
    let mut leader_future_ages = Vec::new();
    let mut follower_future_ages = Vec::new();
    let mut convergence_samples = Vec::new();
    let mut non_overlapping_convergence_samples = Vec::new();

    for pair in leader_events.windows(2) {
        let previous = &pair[0];
        let current = &pair[1];
        let signal_time = current.key.receive_monotonic_ns;
        let Some(future_time) = signal_time.checked_add(horizon_ns) else {
            continue;
        };
        if signal_time < segment_start || future_time > segment_end {
            continue;
        }
        if current.connection_generation != previous.connection_generation {
            continue;
        }
        let Some(leader_observation_gap) =
            signal_time.checked_sub(previous.key.receive_monotonic_ns)
        else {
            continue;
        };
        if leader_observation_gap > max_age_ns {
            continue;
        }
        let leader_move = (current.mid / previous.mid).ln();
        if !leader_move.is_finite()
            || leader_move == 0.0
            || leader_move.abs() * BASIS_POINTS < min_leader_move_bps
        {
            continue;
        }
        candidates += 1;

        let Some((follower_now, follower_now_age)) =
            as_of_with_age(follower_events, current.key, signal_time, max_age_ns)
        else {
            continue;
        };
        let future_key = EventKey {
            receive_monotonic_ns: future_time,
            local_sequence: u64::MAX,
        };
        let Some((leader_future, leader_future_age)) =
            as_of_with_age(leader_events, future_key, future_time, max_age_ns)
        else {
            continue;
        };
        let Some((follower_future, follower_future_age)) =
            as_of_with_age(follower_events, future_key, future_time, max_age_ns)
        else {
            continue;
        };
        if leader_future.connection_generation != current.connection_generation
            || follower_future.connection_generation != follower_now.connection_generation
        {
            continue;
        }

        let follower_response = (follower_future.mid / follower_now.mid).ln();
        let spread_now = (current.mid / follower_now.mid).ln();
        let spread_future = (leader_future.mid / follower_future.mid).ln();
        if !follower_response.is_finite() || !spread_now.is_finite() || !spread_future.is_finite() {
            continue;
        }
        let direction = leader_move.signum();
        let signed_response_bps = direction * follower_response * BASIS_POINTS;
        let convergence_bps = direction * (spread_now - spread_future) * BASIS_POINTS;
        if signed_response_bps > 0.0 {
            positive_responses += 1;
        }
        accumulator.push(leader_move, follower_response);
        signed_response_sum += signed_response_bps;
        convergence_sum += convergence_bps;
        convergence_samples.push(convergence_bps);
        leader_move_bps_sum += leader_move * BASIS_POINTS;
        absolute_leader_move_bps_sum += leader_move.abs() * BASIS_POINTS;
        follower_now_ages.push(follower_now_age);
        leader_future_ages.push(leader_future_age);
        follower_future_ages.push(follower_future_age);
        if signal_time >= next_non_overlapping_signal_time {
            non_overlapping_samples += 1;
            if signed_response_bps > 0.0 {
                non_overlapping_positive_responses += 1;
            }
            non_overlapping_signed_response_sum += signed_response_bps;
            non_overlapping_convergence_sum += convergence_bps;
            non_overlapping_convergence_samples.push(convergence_bps);
            next_non_overlapping_signal_time = future_time;
        }
    }

    let samples = accumulator.count();
    let segment_duration_ns = segment_end.saturating_sub(segment_start);
    PumpLeadLagMetric {
        leader,
        follower,
        segment,
        segment_start_monotonic_ns: segment_start,
        segment_end_monotonic_ns: segment_end,
        horizon_ms,
        candidate_leader_moves: candidates,
        paired_samples: samples,
        paired_coverage: ratio_or_none(samples, candidates),
        overlap_load: (segment_duration_ns > 0)
            .then(|| samples as f64 * horizon_ns as f64 / segment_duration_ns as f64),
        positive_follower_responses: positive_responses,
        correlation: accumulator.correlation(),
        beta: accumulator.beta(),
        mean_signed_leader_move_bps: mean_or_none(leader_move_bps_sum, samples),
        mean_absolute_leader_move_bps: mean_or_none(absolute_leader_move_bps_sum, samples),
        mean_signed_follower_response_bps: mean_or_none(signed_response_sum, samples),
        mean_paired_spread_convergence_bps: mean_or_none(convergence_sum, samples),
        paired_spread_convergence_bps_p50: percentile_f64(&mut convergence_samples, 0.50),
        paired_spread_convergence_bps_p95: percentile_f64(&mut convergence_samples, 0.95),
        paired_spread_convergence_bps_p99: percentile_f64(&mut convergence_samples, 0.99),
        paired_spread_convergence_bps_max: maximum_f64(&convergence_samples),
        non_overlapping_samples,
        non_overlapping_positive_follower_responses: non_overlapping_positive_responses,
        non_overlapping_mean_signed_follower_response_bps: mean_or_none(
            non_overlapping_signed_response_sum,
            non_overlapping_samples,
        ),
        non_overlapping_mean_paired_spread_convergence_bps: mean_or_none(
            non_overlapping_convergence_sum,
            non_overlapping_samples,
        ),
        non_overlapping_spread_convergence_bps_p50: percentile_f64(
            &mut non_overlapping_convergence_samples,
            0.50,
        ),
        non_overlapping_spread_convergence_bps_p95: percentile_f64(
            &mut non_overlapping_convergence_samples,
            0.95,
        ),
        non_overlapping_spread_convergence_bps_p99: percentile_f64(
            &mut non_overlapping_convergence_samples,
            0.99,
        ),
        non_overlapping_spread_convergence_bps_max: maximum_f64(
            &non_overlapping_convergence_samples,
        ),
        follower_now_age_ms_p50: percentile_milliseconds(&mut follower_now_ages, 0.50),
        follower_now_age_ms_p95: percentile_milliseconds(&mut follower_now_ages, 0.95),
        leader_future_age_ms_p50: percentile_milliseconds(&mut leader_future_ages, 0.50),
        leader_future_age_ms_p95: percentile_milliseconds(&mut leader_future_ages, 0.95),
        follower_future_age_ms_p50: percentile_milliseconds(&mut follower_future_ages, 0.50),
        follower_future_age_ms_p95: percentile_milliseconds(&mut follower_future_ages, 0.95),
    }
}

#[derive(Debug, Clone, Copy)]
struct ShockEvent {
    market: PumpMarket,
    key: EventKey,
    connection_generation: u64,
    mid: f64,
    move_log: f64,
}

#[derive(Debug, Default)]
struct CommonShockAccumulator {
    episodes: usize,
    paired_samples: usize,
    positive_follower_responses: usize,
    absolute_leader_move_bps_sum: f64,
    signed_follower_response_bps_sum: f64,
    convergence_bps_sum: f64,
    convergence_samples: Vec<f64>,
    follower_now_ages: Vec<u64>,
}

impl CommonShockAccumulator {
    #[allow(clippy::too_many_arguments)]
    fn into_metric(
        mut self,
        pair_market_a: PumpMarket,
        pair_market_b: PumpMarket,
        leader: PumpMarket,
        follower: PumpMarket,
        segment: AnalysisSegment,
        segment_start: u64,
        segment_end: u64,
        horizon_ms: u64,
        pair_episode_total: usize,
    ) -> PumpCommonShockMetric {
        PumpCommonShockMetric {
            pair_market_a,
            pair_market_b,
            leader,
            follower,
            segment,
            segment_start_monotonic_ns: segment_start,
            segment_end_monotonic_ns: segment_end,
            horizon_ms,
            episodes: self.episodes,
            paired_samples: self.paired_samples,
            paired_coverage: ratio_or_none(self.paired_samples, self.episodes),
            leader_episode_share: ratio_or_none(self.episodes, pair_episode_total),
            positive_follower_responses: self.positive_follower_responses,
            mean_absolute_leader_move_bps: mean_or_none(
                self.absolute_leader_move_bps_sum,
                self.paired_samples,
            ),
            mean_signed_follower_response_bps: mean_or_none(
                self.signed_follower_response_bps_sum,
                self.paired_samples,
            ),
            mean_paired_spread_convergence_bps: mean_or_none(
                self.convergence_bps_sum,
                self.paired_samples,
            ),
            paired_spread_convergence_bps_p50: percentile_f64(&mut self.convergence_samples, 0.50),
            paired_spread_convergence_bps_p95: percentile_f64(&mut self.convergence_samples, 0.95),
            paired_spread_convergence_bps_p99: percentile_f64(&mut self.convergence_samples, 0.99),
            paired_spread_convergence_bps_max: maximum_f64(&self.convergence_samples),
            follower_now_age_ms_p50: percentile_milliseconds(&mut self.follower_now_ages, 0.50),
            follower_now_age_ms_p95: percentile_milliseconds(&mut self.follower_now_ages, 0.95),
        }
    }
}

fn analyze_common_shocks(
    markets: &[PumpMarket],
    events: &BTreeMap<PumpMarket, Vec<MidEvent>>,
    horizons_ms: &[u64],
    max_age_ns: u64,
    min_leader_move_bps: f64,
) -> Result<Vec<PumpCommonShockMetric>, PumpLeadLagError> {
    let mut metrics = Vec::new();
    for left_index in 0..markets.len() {
        for right_index in (left_index + 1)..markets.len() {
            let market_a = markets[left_index];
            let market_b = markets[right_index];
            let events_a = &events[&market_a];
            let events_b = &events[&market_b];
            let pair_start = events_a
                .first()
                .expect("non-empty event vectors")
                .key
                .receive_monotonic_ns
                .max(
                    events_b
                        .first()
                        .expect("non-empty event vectors")
                        .key
                        .receive_monotonic_ns,
                );
            let pair_end = events_a
                .last()
                .expect("non-empty event vectors")
                .key
                .receive_monotonic_ns
                .min(
                    events_b
                        .last()
                        .expect("non-empty event vectors")
                        .key
                        .receive_monotonic_ns,
                );
            if pair_start >= pair_end {
                continue;
            }
            let midpoint = pair_start + (pair_end - pair_start) / 2;
            let segments = [
                (AnalysisSegment::Full, pair_start, pair_end),
                (AnalysisSegment::FirstHalf, pair_start, midpoint),
                (AnalysisSegment::SecondHalf, midpoint, pair_end),
            ];
            for &horizon_ms in horizons_ms {
                let horizon_ns =
                    horizon_ms
                        .checked_mul(NANOS_PER_MILLISECOND)
                        .ok_or_else(|| {
                            PumpLeadLagError::InvalidConfig("horizon is too large".to_owned())
                        })?;
                for (segment, segment_start, segment_end) in segments {
                    metrics.extend(analyze_common_shock_pair(
                        market_a,
                        market_b,
                        events_a,
                        events_b,
                        segment,
                        segment_start,
                        segment_end,
                        horizon_ms,
                        horizon_ns,
                        max_age_ns,
                        min_leader_move_bps,
                    ));
                }
            }
        }
    }
    Ok(metrics)
}

#[allow(clippy::too_many_arguments)]
fn analyze_common_shock_pair(
    market_a: PumpMarket,
    market_b: PumpMarket,
    events_a: &[MidEvent],
    events_b: &[MidEvent],
    segment: AnalysisSegment,
    segment_start: u64,
    segment_end: u64,
    horizon_ms: u64,
    horizon_ns: u64,
    max_age_ns: u64,
    min_leader_move_bps: f64,
) -> [PumpCommonShockMetric; 2] {
    let mut shocks = collect_shocks(
        market_a,
        events_a,
        segment_start,
        segment_end,
        max_age_ns,
        min_leader_move_bps,
    );
    shocks.extend(collect_shocks(
        market_b,
        events_b,
        segment_start,
        segment_end,
        max_age_ns,
        min_leader_move_bps,
    ));
    shocks.sort_by_key(|shock| shock.key);

    let mut accumulator_a = CommonShockAccumulator::default();
    let mut accumulator_b = CommonShockAccumulator::default();
    let mut next_eligible_signal_time = segment_start;
    for shock in shocks {
        let signal_time = shock.key.receive_monotonic_ns;
        let Some(future_time) = signal_time.checked_add(horizon_ns) else {
            continue;
        };
        if signal_time < next_eligible_signal_time || future_time > segment_end {
            continue;
        }
        let (follower, leader_events, follower_events, accumulator) = if shock.market == market_a {
            (market_b, events_a, events_b, &mut accumulator_a)
        } else {
            (market_a, events_b, events_a, &mut accumulator_b)
        };
        let Some((follower_now, follower_now_age)) =
            as_of_with_age(follower_events, shock.key, signal_time, max_age_ns)
        else {
            continue;
        };

        // Eligibility depends only on information available at signal time.
        // Once an episode starts, missing future data must not allow a later
        // shock inside the same window to replace it with hindsight.
        accumulator.episodes += 1;
        next_eligible_signal_time = future_time;

        let future_key = EventKey {
            receive_monotonic_ns: future_time,
            local_sequence: u64::MAX,
        };
        let Some((leader_future, _)) =
            as_of_with_age(leader_events, future_key, future_time, max_age_ns)
        else {
            continue;
        };
        let Some((follower_future, _)) =
            as_of_with_age(follower_events, future_key, future_time, max_age_ns)
        else {
            continue;
        };
        if leader_future.connection_generation != shock.connection_generation
            || follower_future.connection_generation != follower_now.connection_generation
        {
            continue;
        }

        let follower_response = (follower_future.mid / follower_now.mid).ln();
        let spread_now = (shock.mid / follower_now.mid).ln();
        let spread_future = (leader_future.mid / follower_future.mid).ln();
        if !follower_response.is_finite() || !spread_now.is_finite() || !spread_future.is_finite() {
            continue;
        }
        let direction = shock.move_log.signum();
        let signed_response_bps = direction * follower_response * BASIS_POINTS;
        let convergence_bps = direction * (spread_now - spread_future) * BASIS_POINTS;
        accumulator.paired_samples += 1;
        if signed_response_bps > 0.0 {
            accumulator.positive_follower_responses += 1;
        }
        accumulator.absolute_leader_move_bps_sum += shock.move_log.abs() * BASIS_POINTS;
        accumulator.signed_follower_response_bps_sum += signed_response_bps;
        accumulator.convergence_bps_sum += convergence_bps;
        accumulator.convergence_samples.push(convergence_bps);
        accumulator.follower_now_ages.push(follower_now_age);

        debug_assert_ne!(shock.market, follower);
    }

    let pair_episode_total = accumulator_a.episodes + accumulator_b.episodes;
    [
        accumulator_a.into_metric(
            market_a,
            market_b,
            market_a,
            market_b,
            segment,
            segment_start,
            segment_end,
            horizon_ms,
            pair_episode_total,
        ),
        accumulator_b.into_metric(
            market_a,
            market_b,
            market_b,
            market_a,
            segment,
            segment_start,
            segment_end,
            horizon_ms,
            pair_episode_total,
        ),
    ]
}

fn collect_shocks(
    market: PumpMarket,
    events: &[MidEvent],
    segment_start: u64,
    segment_end: u64,
    max_age_ns: u64,
    min_leader_move_bps: f64,
) -> Vec<ShockEvent> {
    let mut shocks = Vec::new();
    for pair in events.windows(2) {
        let previous = &pair[0];
        let current = &pair[1];
        let signal_time = current.key.receive_monotonic_ns;
        if signal_time < segment_start || signal_time > segment_end {
            continue;
        }
        if current.connection_generation != previous.connection_generation {
            continue;
        }
        let Some(observation_gap) = signal_time.checked_sub(previous.key.receive_monotonic_ns)
        else {
            continue;
        };
        if observation_gap > max_age_ns {
            continue;
        }
        let move_log = (current.mid / previous.mid).ln();
        if !move_log.is_finite()
            || move_log == 0.0
            || move_log.abs() * BASIS_POINTS < min_leader_move_bps
        {
            continue;
        }
        shocks.push(ShockEvent {
            market,
            key: current.key,
            connection_generation: current.connection_generation,
            mid: current.mid,
            move_log,
        });
    }
    shocks
}

fn ratio_or_none(numerator: usize, denominator: usize) -> Option<f64> {
    (denominator > 0).then(|| numerator as f64 / denominator as f64)
}

fn percentile_milliseconds(values: &mut [u64], quantile: f64) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    values.sort_unstable();
    let index = ((values.len() - 1) as f64 * quantile).round() as usize;
    Some(values[index] as f64 / NANOS_PER_MILLISECOND as f64)
}

fn percentile_f64(values: &mut [f64], quantile: f64) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    values.sort_by(f64::total_cmp);
    let index = ((values.len() - 1) as f64 * quantile).round() as usize;
    Some(values[index])
}

fn maximum_f64(values: &[f64]) -> Option<f64> {
    values.iter().copied().reduce(f64::max)
}

#[cfg(test)]
fn as_of(
    events: &[MidEvent],
    key: EventKey,
    query_time_ns: u64,
    max_age_ns: u64,
) -> Option<&MidEvent> {
    as_of_with_age(events, key, query_time_ns, max_age_ns).map(|(event, _)| event)
}

fn as_of_with_age(
    events: &[MidEvent],
    key: EventKey,
    query_time_ns: u64,
    max_age_ns: u64,
) -> Option<(&MidEvent, u64)> {
    let index = events.partition_point(|event| event.key <= key);
    let event = index.checked_sub(1).and_then(|index| events.get(index))?;
    let age = query_time_ns.checked_sub(event.key.receive_monotonic_ns)?;
    (age <= max_age_ns).then_some((event, age))
}

fn mean_or_none(sum: f64, count: usize) -> Option<f64> {
    (count > 0).then(|| sum / count as f64)
}

#[derive(Debug, Default)]
struct BivariateAccumulator {
    count: usize,
    mean_x: f64,
    mean_y: f64,
    m2_x: f64,
    m2_y: f64,
    co_moment: f64,
}

impl BivariateAccumulator {
    fn push(&mut self, x: f64, y: f64) {
        self.count += 1;
        let count = self.count as f64;
        let delta_x = x - self.mean_x;
        self.mean_x += delta_x / count;
        let delta_y = y - self.mean_y;
        self.mean_y += delta_y / count;
        self.m2_x += delta_x * (x - self.mean_x);
        self.m2_y += delta_y * (y - self.mean_y);
        self.co_moment += delta_x * (y - self.mean_y);
    }

    const fn count(&self) -> usize {
        self.count
    }

    fn correlation(&self) -> Option<f64> {
        if self.count < 2 || self.m2_x <= 0.0 || self.m2_y <= 0.0 {
            return None;
        }
        finite_option(self.co_moment / (self.m2_x * self.m2_y).sqrt())
    }

    fn beta(&self) -> Option<f64> {
        if self.count < 2 || self.m2_x <= 0.0 {
            return None;
        }
        finite_option(self.co_moment / self.m2_x)
    }
}

fn finite_option(value: f64) -> Option<f64> {
    value.is_finite().then_some(value)
}

fn summarize_markets(events: &BTreeMap<PumpMarket, Vec<MidEvent>>) -> Vec<PumpMarketSummary> {
    events
        .iter()
        .map(|(market, events)| PumpMarketSummary {
            market: *market,
            events: events.len(),
            price_changes: events
                .windows(2)
                .filter(|pair| pair[0].mid != pair[1].mid)
                .count(),
            first_receive_monotonic_ns: events
                .first()
                .expect("market event vectors are non-empty")
                .key
                .receive_monotonic_ns,
            last_receive_monotonic_ns: events
                .last()
                .expect("market event vectors are non-empty")
                .key
                .receive_monotonic_ns,
        })
        .collect()
}

fn load_capture(path: &Path) -> Result<CaptureData, PumpLeadLagError> {
    let reader = BufReader::new(File::open(path)?);
    let mut capture_id: Option<String> = None;
    let mut previous_sequence = None;
    let mut records = 0_u64;
    let mut ignored_records = 0_u64;
    let mut events = Vec::new();
    let mut pumpswap = PumpSwapDecoder::default();

    for (zero_based_line, line) in reader.lines().enumerate() {
        let line_number = zero_based_line + 1;
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let record: CaptureRecord =
            serde_json::from_str(&line).map_err(|source| PumpLeadLagError::CaptureJson {
                line: line_number,
                source,
            })?;
        records += 1;
        validate_capture_record(
            &record,
            line_number,
            &mut capture_id,
            &mut previous_sequence,
        )?;
        if record.record_type != "data" {
            ignored_records += 1;
            continue;
        }
        let parsed = parse_record(&record, &mut pumpswap).map_err(|reason| {
            PumpLeadLagError::InvalidRecord {
                line: line_number,
                reason,
            }
        })?;
        if parsed.is_empty() {
            ignored_records += 1;
        } else {
            events.extend(parsed);
        }
    }

    let capture_id = capture_id.ok_or(PumpLeadLagError::InsufficientMarkets)?;
    suppress_pumpswap_vaults_when_logs_exist(&mut events);
    let mut by_market = BTreeMap::<PumpMarket, Vec<MidEvent>>::new();
    for event in events {
        by_market.entry(event.market).or_default().push(event);
    }
    for market_events in by_market.values_mut() {
        market_events.sort_by_key(|event| event.key);
        market_events.dedup_by(|right, left| right.key == left.key && right.mid == left.mid);
    }
    by_market.retain(|_, events| !events.is_empty());
    Ok(CaptureData {
        capture_id,
        input_records: records,
        ignored_records,
        events: by_market,
    })
}

fn validate_capture_record(
    record: &CaptureRecord,
    line: usize,
    capture_id: &mut Option<String>,
    previous_sequence: &mut Option<u64>,
) -> Result<(), PumpLeadLagError> {
    if record.schema_version != PUMP_CAPTURE_SCHEMA_VERSION {
        return Err(PumpLeadLagError::InvalidRecord {
            line,
            reason: format!(
                "unsupported schema version {}; expected {}",
                record.schema_version, PUMP_CAPTURE_SCHEMA_VERSION
            ),
        });
    }
    match capture_id {
        Some(expected) if expected != &record.capture_id => {
            return Err(PumpLeadLagError::InvalidRecord {
                line,
                reason: format!(
                    "capture_id changed from {expected} to {}",
                    record.capture_id
                ),
            });
        }
        None => *capture_id = Some(record.capture_id.clone()),
        Some(_) => {}
    }
    if let Some(previous) = *previous_sequence {
        let expected = previous
            .checked_add(1)
            .ok_or_else(|| PumpLeadLagError::InvalidRecord {
                line,
                reason: "local_sequence overflow".to_owned(),
            })?;
        if record.local_sequence != expected {
            return Err(PumpLeadLagError::InvalidRecord {
                line,
                reason: format!(
                    "local_sequence is not contiguous: expected {expected}, received {}",
                    record.local_sequence
                ),
            });
        }
    }
    *previous_sequence = Some(record.local_sequence);
    Ok(())
}

fn parse_record(
    record: &CaptureRecord,
    pumpswap: &mut PumpSwapDecoder,
) -> Result<Vec<MidEvent>, String> {
    let event = match (record.source.as_str(), record.stream.as_str()) {
        (BINANCE_SOURCE, "depth20") => Some(parse_binance(record)?),
        (HYPERLIQUID_SPOT_SOURCE, "bbo") => {
            return Ok(parse_hyperliquid(record, PumpMarket::HyperliquidSpot)?
                .into_iter()
                .collect())
        }
        (HYPERLIQUID_PERP_SOURCE, "bbo") => {
            return Ok(parse_hyperliquid(record, PumpMarket::HyperliquidPerp)?
                .into_iter()
                .collect())
        }
        (LIGHTER_SOURCE, "ticker") => Some(parse_lighter(record)?),
        (PUMPSWAP_SOURCE, "bootstrap") => Some(pumpswap.parse_bootstrap(record)?),
        (PUMPSWAP_SOURCE, "base_vault") => {
            return Ok(pumpswap.parse_vault(record, true)?.into_iter().collect())
        }
        (PUMPSWAP_SOURCE, "quote_vault") => {
            return Ok(pumpswap.parse_vault(record, false)?.into_iter().collect())
        }
        (PUMPSWAP_SOURCE, "logs") => return pumpswap.parse_logs(record),
        _ => None,
    };
    Ok(event.into_iter().collect())
}

fn parse_binance(record: &CaptureRecord) -> Result<MidEvent, String> {
    if record.payload.get("e").and_then(Value::as_str) != Some("depthUpdate") {
        return Err("Binance payload is not a depthUpdate".to_owned());
    }
    if record.payload.get("s").and_then(Value::as_str) != Some("PUMPUSDT") {
        return Err("Binance payload is not PUMPUSDT".to_owned());
    }
    let bids = record
        .payload
        .get("b")
        .and_then(Value::as_array)
        .ok_or_else(|| "Binance depth payload has no bids".to_owned())?;
    let asks = record
        .payload
        .get("a")
        .and_then(Value::as_array)
        .ok_or_else(|| "Binance depth payload has no asks".to_owned())?;
    let bid = prices_from_levels(bids)?.into_iter().reduce(f64::max);
    let ask = prices_from_levels(asks)?.into_iter().reduce(f64::min);
    mid_event(record, PumpMarket::BinancePerp, bid, ask)
}

fn parse_hyperliquid(
    record: &CaptureRecord,
    market: PumpMarket,
) -> Result<Option<MidEvent>, String> {
    if record.payload.get("channel").and_then(Value::as_str) != Some("bbo") {
        return Err("Hyperliquid record is not a BBO channel payload".to_owned());
    }
    let expected_coin = match market {
        PumpMarket::HyperliquidSpot => "@188",
        PumpMarket::HyperliquidPerp => "PUMP",
        _ => return Err("invalid Hyperliquid PUMP market".to_owned()),
    };
    if record.payload.pointer("/data/coin").and_then(Value::as_str) != Some(expected_coin) {
        return Err(format!("Hyperliquid payload is not {expected_coin}"));
    }
    let bbo = record
        .payload
        .pointer("/data/bbo")
        .and_then(Value::as_array)
        .ok_or_else(|| "Hyperliquid BBO payload has no data.bbo".to_owned())?;
    if bbo.len() != 2 {
        return Err("Hyperliquid BBO must contain exactly two sides".to_owned());
    }
    if bbo.iter().any(Value::is_null) {
        return Ok(None);
    }
    let bid = value_price(&bbo[0], "px")?;
    let ask = value_price(&bbo[1], "px")?;
    mid_event(record, market, Some(bid), Some(ask)).map(Some)
}

fn parse_lighter(record: &CaptureRecord) -> Result<MidEvent, String> {
    if record.payload.get("channel").and_then(Value::as_str) != Some("ticker:45") {
        return Err("Lighter payload is not ticker:45".to_owned());
    }
    if record.payload.pointer("/ticker/s").and_then(Value::as_str) != Some("PUMP") {
        return Err("Lighter ticker payload is not PUMP".to_owned());
    }
    let bid = value_price(
        record
            .payload
            .pointer("/ticker/b")
            .ok_or_else(|| "Lighter ticker has no bid".to_owned())?,
        "price",
    )?;
    let ask = value_price(
        record
            .payload
            .pointer("/ticker/a")
            .ok_or_else(|| "Lighter ticker has no ask".to_owned())?,
        "price",
    )?;
    mid_event(record, PumpMarket::LighterPerp, Some(bid), Some(ask))
}

fn mid_event(
    record: &CaptureRecord,
    market: PumpMarket,
    bid: Option<f64>,
    ask: Option<f64>,
) -> Result<MidEvent, String> {
    let bid = bid.ok_or_else(|| "book has no bid".to_owned())?;
    let ask = ask.ok_or_else(|| "book has no ask".to_owned())?;
    validate_price(bid)?;
    validate_price(ask)?;
    if bid > ask {
        return Err(format!("crossed book: bid {bid} exceeds ask {ask}"));
    }
    MidEvent::new(market, record, (bid + ask) / 2.0)
}

fn prices_from_levels(levels: &[Value]) -> Result<Vec<f64>, String> {
    if levels.is_empty() {
        return Err("book side is empty".to_owned());
    }
    levels
        .iter()
        .map(|level| {
            let values = level
                .as_array()
                .ok_or_else(|| "book level is not an array".to_owned())?;
            parse_number(
                values
                    .first()
                    .ok_or_else(|| "book level has no price".to_owned())?,
            )
        })
        .collect()
}

fn value_price(value: &Value, field: &str) -> Result<f64, String> {
    parse_number(
        value
            .get(field)
            .ok_or_else(|| format!("level has no {field}"))?,
    )
}

fn parse_number(value: &Value) -> Result<f64, String> {
    let parsed = match value {
        Value::String(value) => value
            .parse::<f64>()
            .map_err(|_| format!("invalid numeric string {value:?}"))?,
        Value::Number(value) => value
            .as_f64()
            .ok_or_else(|| format!("invalid JSON number {value}"))?,
        _ => return Err("price is neither a string nor number".to_owned()),
    };
    validate_price(parsed)?;
    Ok(parsed)
}

fn validate_price(price: f64) -> Result<(), String> {
    if price.is_finite() && price > 0.0 {
        Ok(())
    } else {
        Err(format!(
            "price must be positive and finite, received {price}"
        ))
    }
}

impl PumpSwapDecoder {
    fn parse_bootstrap(&mut self, record: &CaptureRecord) -> Result<MidEvent, String> {
        self.prepare_generation(record.connection_generation);
        let accounts = record
            .payload
            .pointer("/result/value")
            .and_then(Value::as_array)
            .ok_or_else(|| "PumpSwap bootstrap has no account array".to_owned())?;
        if accounts.len() < 2 {
            return Err("PumpSwap bootstrap contains fewer than two vaults".to_owned());
        }
        let base = token_account_amount(&accounts[0])?;
        let quote = token_account_amount(&accounts[1])?;
        let slot = record
            .payload
            .pointer("/result/context/slot")
            .and_then(Value::as_u64)
            .ok_or_else(|| "PumpSwap bootstrap has no context slot".to_owned())?;
        // The HTTP snapshot is requested after WebSocket subscriptions are
        // established. Notifications at or below this slot can be delivered
        // later from the socket queue, but they are already represented by the
        // bootstrap state and must not be replayed after it.
        self.bootstrap_floor_slot = Some(slot);
        self.pending_vaults.clear();
        let mut event = reserve_event(record, base, quote)?;
        event.pumpswap_slot = Some(slot);
        event.pumpswap_kind = PumpSwapEventKind::Bootstrap;
        Ok(event)
    }

    fn parse_vault(
        &mut self,
        record: &CaptureRecord,
        is_base: bool,
    ) -> Result<Option<MidEvent>, String> {
        self.prepare_generation(record.connection_generation);
        let slot = record
            .payload
            .pointer("/params/result/context/slot")
            .and_then(Value::as_u64)
            .ok_or_else(|| "PumpSwap vault update has no slot".to_owned())?;
        if self.bootstrap_floor_slot.is_some_and(|floor| slot <= floor) {
            return Ok(None);
        }
        let account = record
            .payload
            .pointer("/params/result/value")
            .ok_or_else(|| "PumpSwap vault update has no account value".to_owned())?;
        let amount = token_account_amount(account)?;
        let partial = self.pending_vaults.entry(slot).or_default();
        if is_base {
            partial.base = Some(amount);
        } else {
            partial.quote = Some(amount);
        }
        let complete = partial.base.zip(partial.quote);
        if self.pending_vaults.len() > 256 {
            let retain_from = slot.saturating_sub(128);
            self.pending_vaults
                .retain(|candidate, _| *candidate >= retain_from);
        }
        let Some((base, quote)) = complete else {
            return Ok(None);
        };
        self.pending_vaults.remove(&slot);
        let mut event = reserve_event(record, base, quote)?;
        event.pumpswap_slot = Some(slot);
        event.pumpswap_kind = PumpSwapEventKind::VaultPair;
        Ok(Some(event))
    }

    fn parse_logs(&mut self, record: &CaptureRecord) -> Result<Vec<MidEvent>, String> {
        self.prepare_generation(record.connection_generation);
        let slot = record
            .payload
            .pointer("/params/result/context/slot")
            .and_then(Value::as_u64)
            .ok_or_else(|| "PumpSwap log notification has no slot".to_owned())?;
        if self.bootstrap_floor_slot.is_some_and(|floor| slot <= floor) {
            return Ok(Vec::new());
        }
        parse_pumpswap_logs(record, slot)
    }

    fn prepare_generation(&mut self, generation: u64) {
        if self.connection_generation != Some(generation) {
            self.connection_generation = Some(generation);
            self.bootstrap_floor_slot = None;
            self.pending_vaults.clear();
        }
    }
}

fn token_account_amount(account: &Value) -> Result<u64, String> {
    let encoded = account
        .pointer("/data/0")
        .and_then(Value::as_str)
        .ok_or_else(|| "token account has no base64 data".to_owned())?;
    let bytes = BASE64
        .decode(encoded)
        .map_err(|error| format!("decode token account base64: {error}"))?;
    let offset = if bytes.len() >= 72 {
        64
    } else if bytes.len() == 8 {
        0
    } else {
        return Err(format!(
            "token account data has unexpected length {}",
            bytes.len()
        ));
    };
    read_u64(&bytes, offset)
}

fn reserve_event(record: &CaptureRecord, base: u64, quote: u64) -> Result<MidEvent, String> {
    if base == 0 || quote == 0 {
        return Err("PumpSwap reserves must be positive".to_owned());
    }
    MidEvent::new(PumpMarket::PumpSwapSpot, record, quote as f64 / base as f64)
}

fn parse_pumpswap_logs(record: &CaptureRecord, slot: u64) -> Result<Vec<MidEvent>, String> {
    if !record
        .payload
        .pointer("/params/result/value/err")
        .is_some_and(Value::is_null)
    {
        return Ok(Vec::new());
    }
    let logs = record
        .payload
        .pointer("/params/result/value/logs")
        .and_then(Value::as_array)
        .ok_or_else(|| "PumpSwap log notification has no logs array".to_owned())?;
    let mut events = Vec::new();
    for log in logs.iter().filter_map(Value::as_str) {
        let Some(encoded) = log.strip_prefix("Program data: ") else {
            continue;
        };
        let Ok(bytes) = BASE64.decode(encoded) else {
            continue;
        };
        let Some((base_after, quote_after)) = decode_pumpswap_swap(&bytes)? else {
            continue;
        };
        let mut event = reserve_event(record, base_after, quote_after)?;
        event.pumpswap_slot = Some(slot);
        event.pumpswap_kind = PumpSwapEventKind::SwapLog;
        events.push(event);
    }
    // Multiple swaps in one transaction have the same receive timestamp and
    // capture sequence. The last event is the only unambiguous post-transaction
    // state at recorder resolution.
    Ok(events.pop().into_iter().collect())
}

fn decode_pumpswap_swap(bytes: &[u8]) -> Result<Option<(u64, u64)>, String> {
    if bytes.len() < 152 {
        return Ok(None);
    }
    let discriminator: [u8; 8] = bytes[0..8]
        .try_into()
        .map_err(|_| "invalid PumpSwap discriminator".to_owned())?;
    let is_buy = if discriminator == PUMPSWAP_BUY_DISCRIMINATOR {
        true
    } else if discriminator == PUMPSWAP_SELL_DISCRIMINATOR {
        false
    } else {
        return Ok(None);
    };
    if bytes[120..152] != PUMPSWAP_POOL_RAW {
        return Ok(None);
    }
    let base_amount = read_u64(bytes, 16)?;
    let pre_base = read_u64(bytes, 48)?;
    let pre_quote = read_u64(bytes, 56)?;
    let quote_with_or_without_lp_fee = read_u64(bytes, 104)?;
    let (base_after, quote_after) = if is_buy {
        (
            pre_base
                .checked_sub(base_amount)
                .ok_or_else(|| "PumpSwap buy base reserve underflow".to_owned())?,
            pre_quote
                .checked_add(quote_with_or_without_lp_fee)
                .ok_or_else(|| "PumpSwap buy quote reserve overflow".to_owned())?,
        )
    } else {
        (
            pre_base
                .checked_add(base_amount)
                .ok_or_else(|| "PumpSwap sell base reserve overflow".to_owned())?,
            pre_quote
                .checked_sub(quote_with_or_without_lp_fee)
                .ok_or_else(|| "PumpSwap sell quote reserve underflow".to_owned())?,
        )
    };
    if base_after == 0 || quote_after == 0 {
        return Err("PumpSwap post-swap reserves must be positive".to_owned());
    }
    Ok(Some((base_after, quote_after)))
}

fn read_u64(bytes: &[u8], offset: usize) -> Result<u64, String> {
    let end = offset
        .checked_add(8)
        .ok_or_else(|| "byte offset overflow".to_owned())?;
    let slice = bytes
        .get(offset..end)
        .ok_or_else(|| format!("payload is shorter than byte offset {end}"))?;
    Ok(u64::from_le_bytes(
        slice
            .try_into()
            .map_err(|_| "invalid u64 byte slice".to_owned())?,
    ))
}

fn suppress_pumpswap_vaults_when_logs_exist(events: &mut Vec<MidEvent>) {
    let log_slots: BTreeSet<_> = events
        .iter()
        .filter(|event| event.pumpswap_kind == PumpSwapEventKind::SwapLog)
        .filter_map(|event| event.pumpswap_slot)
        .collect();
    events.retain(|event| {
        event.pumpswap_kind != PumpSwapEventKind::VaultPair
            || event
                .pumpswap_slot
                .is_none_or(|slot| !log_slots.contains(&slot))
    });
}

#[cfg(test)]
mod tests {
    use std::io::Write as _;

    use serde_json::json;
    use tempfile::NamedTempFile;

    use super::*;

    fn record(
        source: &str,
        stream: &str,
        sequence: u64,
        timestamp: u64,
        payload: Value,
    ) -> CaptureRecord {
        CaptureRecord {
            schema_version: PUMP_CAPTURE_SCHEMA_VERSION,
            capture_id: "capture".to_owned(),
            local_sequence: sequence,
            record_type: "data".to_owned(),
            source: source.to_owned(),
            stream: stream.to_owned(),
            connection_generation: 1,
            receive_monotonic_ns: timestamp,
            payload,
        }
    }

    fn event(market: PumpMarket, timestamp: u64, sequence: u64, mid: f64) -> MidEvent {
        event_with_generation(market, timestamp, sequence, 1, mid)
    }

    fn event_with_generation(
        market: PumpMarket,
        timestamp: u64,
        sequence: u64,
        connection_generation: u64,
        mid: f64,
    ) -> MidEvent {
        MidEvent {
            market,
            key: EventKey {
                receive_monotonic_ns: timestamp,
                local_sequence: sequence,
            },
            connection_generation,
            mid,
            pumpswap_slot: None,
            pumpswap_kind: PumpSwapEventKind::Other,
        }
    }

    #[test]
    fn parses_three_centralized_quote_shapes() {
        let binance = record(
            BINANCE_SOURCE,
            "depth20",
            1,
            1,
            json!({"e":"depthUpdate","s":"PUMPUSDT","b":[["1.00","2"],["0.99","3"]],"a":[["1.02","2"],["1.03","3"]]}),
        );
        assert_eq!(parse_binance(&binance).unwrap().mid, 1.01);

        let hyperliquid = record(
            HYPERLIQUID_PERP_SOURCE,
            "bbo",
            2,
            2,
            json!({"channel":"bbo","data":{"coin":"PUMP","bbo":[{"px":"2.00","sz":"1"},{"px":"2.02","sz":"1"}]}}),
        );
        assert_eq!(
            parse_hyperliquid(&hyperliquid, PumpMarket::HyperliquidPerp)
                .unwrap()
                .unwrap()
                .mid,
            2.01
        );

        let empty_side = record(
            HYPERLIQUID_SPOT_SOURCE,
            "bbo",
            3,
            3,
            json!({"channel":"bbo","data":{"coin":"@188","bbo":[null,{"px":"2.02","sz":"1"}]}}),
        );
        assert!(parse_hyperliquid(&empty_side, PumpMarket::HyperliquidSpot)
            .unwrap()
            .is_none());

        let lighter = record(
            LIGHTER_SOURCE,
            "ticker",
            4,
            4,
            json!({"channel":"ticker:45","ticker":{"s":"PUMP","b":{"price":"3.00"},"a":{"price":"3.02"}}}),
        );
        assert_eq!(parse_lighter(&lighter).unwrap().mid, 3.01);
    }

    #[test]
    fn decodes_pumpswap_bootstrap_and_vault_pair() {
        fn token_account(amount: u64) -> Value {
            let mut bytes = vec![0_u8; 72];
            bytes[64..72].copy_from_slice(&amount.to_le_bytes());
            json!({"data":[BASE64.encode(bytes),"base64"]})
        }
        let bootstrap = record(
            PUMPSWAP_SOURCE,
            "bootstrap",
            1,
            10,
            json!({"result":{"context":{"slot":9},"value":[token_account(1_000),token_account(5),{}]}}),
        );
        let mut decoder = PumpSwapDecoder::default();
        let event = decoder.parse_bootstrap(&bootstrap).unwrap();
        assert_eq!(event.mid, 0.005);
        assert_eq!(event.pumpswap_slot, Some(9));

        let stale_base = record(
            PUMPSWAP_SOURCE,
            "base_vault",
            2,
            11,
            json!({"params":{"result":{"context":{"slot":9},"value":token_account(2_000)}}}),
        );
        assert!(decoder.parse_vault(&stale_base, true).unwrap().is_none());

        let base = record(
            PUMPSWAP_SOURCE,
            "base_vault",
            3,
            20,
            json!({"params":{"result":{"context":{"slot":10},"value":token_account(2_000)}}}),
        );
        let quote = record(
            PUMPSWAP_SOURCE,
            "quote_vault",
            4,
            21,
            json!({"params":{"result":{"context":{"slot":10},"value":token_account(12)}}}),
        );
        assert!(decoder.parse_vault(&base, true).unwrap().is_none());
        assert_eq!(
            decoder.parse_vault(&quote, false).unwrap().unwrap().mid,
            0.006
        );
    }

    #[test]
    fn decodes_pumpswap_buy_and_sell_reserves() {
        let mut buy = vec![0_u8; 152];
        buy[0..8].copy_from_slice(&PUMPSWAP_BUY_DISCRIMINATOR);
        buy[16..24].copy_from_slice(&100_u64.to_le_bytes());
        buy[48..56].copy_from_slice(&1_000_u64.to_le_bytes());
        buy[56..64].copy_from_slice(&5_000_u64.to_le_bytes());
        buy[104..112].copy_from_slice(&600_u64.to_le_bytes());
        buy[120..152].copy_from_slice(&PUMPSWAP_POOL_RAW);
        assert_eq!(decode_pumpswap_swap(&buy).unwrap(), Some((900, 5_600)));

        let mut sell = buy;
        sell[0..8].copy_from_slice(&PUMPSWAP_SELL_DISCRIMINATOR);
        assert_eq!(decode_pumpswap_swap(&sell).unwrap(), Some((1_100, 4_400)));
    }

    #[test]
    fn as_of_is_causal_and_freshness_boundary_is_inclusive() {
        let events = vec![
            event(PumpMarket::LighterPerp, 100, 1, 1.0),
            event(PumpMarket::LighterPerp, 200, 3, 2.0),
        ];
        let before_later_sequence = as_of(
            &events,
            EventKey {
                receive_monotonic_ns: 200,
                local_sequence: 2,
            },
            200,
            100,
        )
        .unwrap();
        assert_eq!(before_later_sequence.mid, 1.0);
        assert!(as_of(
            &events,
            EventKey {
                receive_monotonic_ns: 201,
                local_sequence: u64::MAX,
            },
            201,
            100,
        )
        .is_some());
        assert!(as_of(
            &events,
            EventKey {
                receive_monotonic_ns: 301,
                local_sequence: u64::MAX,
            },
            301,
            100,
        )
        .is_none());
    }

    #[test]
    fn capture_sequence_must_be_contiguous() {
        let mut capture_id = None;
        let mut previous_sequence = None;
        let first = record(BINANCE_SOURCE, "depth20", 1, 1, json!({}));
        validate_capture_record(&first, 1, &mut capture_id, &mut previous_sequence).unwrap();
        let gap = record(BINANCE_SOURCE, "depth20", 3, 2, json!({}));
        let error =
            validate_capture_record(&gap, 2, &mut capture_id, &mut previous_sequence).unwrap_err();
        assert!(error.to_string().contains("expected 2, received 3"));
    }

    #[test]
    fn event_conditioned_response_uses_only_future_as_of_state() {
        let ms = NANOS_PER_MILLISECOND;
        let leader = vec![
            event(PumpMarket::HyperliquidPerp, 50 * ms, 1, 100.0),
            event(PumpMarket::HyperliquidPerp, 100 * ms, 3, 101.0),
            event(PumpMarket::HyperliquidPerp, 200 * ms, 5, 101.0),
        ];
        let follower = vec![
            event(PumpMarket::LighterPerp, 50 * ms, 2, 100.0),
            // This quote has a later global sequence at the signal timestamp,
            // so it must not be used as the t=100ms starting state.
            event(PumpMarket::LighterPerp, 100 * ms, 4, 100.5),
            event(PumpMarket::LighterPerp, 150 * ms, 5, 100.8),
        ];
        let result = analyze_ordered_pair(
            PumpMarket::HyperliquidPerp,
            PumpMarket::LighterPerp,
            &leader,
            &follower,
            AnalysisSegment::Full,
            0,
            200 * ms,
            50,
            50 * ms,
            500 * ms,
            0.0,
        );
        assert_eq!(result.candidate_leader_moves, 1);
        assert_eq!(result.paired_samples, 1);
        let expected = (100.8_f64 / 100.0).ln() * BASIS_POINTS;
        assert!((result.mean_signed_follower_response_bps.unwrap() - expected).abs() < 1e-9);
    }

    #[test]
    fn event_conditioned_response_never_crosses_reconnects() {
        let ms = NANOS_PER_MILLISECOND;
        let leader = vec![
            event(PumpMarket::HyperliquidPerp, 50 * ms, 1, 100.0),
            event(PumpMarket::HyperliquidPerp, 100 * ms, 3, 101.0),
            event_with_generation(PumpMarket::HyperliquidPerp, 140 * ms, 5, 2, 102.0),
        ];
        let follower = vec![
            event(PumpMarket::LighterPerp, 50 * ms, 2, 100.0),
            event(PumpMarket::LighterPerp, 100 * ms, 4, 100.0),
            event_with_generation(PumpMarket::LighterPerp, 145 * ms, 6, 2, 100.8),
        ];
        let result = analyze_ordered_pair(
            PumpMarket::HyperliquidPerp,
            PumpMarket::LighterPerp,
            &leader,
            &follower,
            AnalysisSegment::Full,
            0,
            200 * ms,
            50,
            50 * ms,
            500 * ms,
            0.0,
        );
        assert_eq!(result.candidate_leader_moves, 1);
        assert_eq!(result.paired_samples, 0);
    }

    #[test]
    fn stale_cumulative_leader_move_is_not_a_candidate() {
        let ms = NANOS_PER_MILLISECOND;
        let leader = vec![
            event(PumpMarket::HyperliquidPerp, 10 * ms, 1, 100.0),
            event(PumpMarket::HyperliquidPerp, 1_000 * ms, 3, 101.0),
            event(PumpMarket::HyperliquidPerp, 1_050 * ms, 5, 101.0),
        ];
        let follower = vec![
            event(PumpMarket::LighterPerp, 990 * ms, 2, 100.0),
            event(PumpMarket::LighterPerp, 1_050 * ms, 4, 100.5),
        ];
        let result = analyze_ordered_pair(
            PumpMarket::HyperliquidPerp,
            PumpMarket::LighterPerp,
            &leader,
            &follower,
            AnalysisSegment::Full,
            0,
            1_100 * ms,
            50,
            50 * ms,
            100 * ms,
            0.0,
        );
        assert_eq!(result.candidate_leader_moves, 0);
        assert_eq!(result.paired_samples, 0);
    }

    #[test]
    fn common_shock_clock_assigns_a_burst_only_to_the_first_venue() {
        let ms = NANOS_PER_MILLISECOND;
        let market_a = vec![
            event(PumpMarket::BinancePerp, 50 * ms, 1, 100.0),
            event(PumpMarket::BinancePerp, 100 * ms, 3, 101.0),
            event(PumpMarket::BinancePerp, 200 * ms, 6, 101.0),
        ];
        let market_b = vec![
            event(PumpMarket::LighterPerp, 50 * ms, 2, 100.0),
            event(PumpMarket::LighterPerp, 150 * ms, 4, 100.8),
            event(PumpMarket::LighterPerp, 200 * ms, 5, 100.8),
        ];
        let [a_leads, b_leads] = analyze_common_shock_pair(
            PumpMarket::BinancePerp,
            PumpMarket::LighterPerp,
            &market_a,
            &market_b,
            AnalysisSegment::Full,
            0,
            250 * ms,
            100,
            100 * ms,
            500 * ms,
            0.5,
        );
        assert_eq!(a_leads.episodes, 1);
        assert_eq!(a_leads.paired_samples, 1);
        assert_eq!(a_leads.leader_episode_share, Some(1.0));
        assert!(a_leads.mean_paired_spread_convergence_bps.unwrap() > 0.0);
        assert_eq!(b_leads.episodes, 0);
        assert_eq!(b_leads.paired_samples, 0);
        assert_eq!(b_leads.leader_episode_share, Some(0.0));
    }

    #[test]
    fn writes_json_and_csv_reports_from_capture() {
        let mut input = NamedTempFile::new().unwrap();
        let rows = [
            json!({
                "schema_version":1,"capture_id":"test","local_sequence":1,
                "record_type":"data","source":BINANCE_SOURCE,"stream":"depth20",
                "connection_generation":1,"receive_unix_ns":1,"receive_monotonic_ns":1,
                "decode_done_monotonic_ns":2,
                "payload":{"e":"depthUpdate","s":"PUMPUSDT","b":[["1","1"]],"a":[["1.01","1"]]}
            }),
            json!({
                "schema_version":1,"capture_id":"test","local_sequence":2,
                "record_type":"data","source":LIGHTER_SOURCE,"stream":"ticker",
                "connection_generation":1,"receive_unix_ns":1,"receive_monotonic_ns":1,
                "decode_done_monotonic_ns":2,
                "payload":{"channel":"ticker:45","ticker":{"s":"PUMP","b":{"price":"1"},"a":{"price":"1.01"}}}
            }),
            json!({
                "schema_version":1,"capture_id":"test","local_sequence":3,
                "record_type":"data","source":BINANCE_SOURCE,"stream":"depth20",
                "connection_generation":1,"receive_unix_ns":2,"receive_monotonic_ns":100_000_001,
                "decode_done_monotonic_ns":2,
                "payload":{"e":"depthUpdate","s":"PUMPUSDT","b":[["1.01","1"]],"a":[["1.02","1"]]}
            }),
            json!({
                "schema_version":1,"capture_id":"test","local_sequence":4,
                "record_type":"data","source":LIGHTER_SOURCE,"stream":"ticker",
                "connection_generation":1,"receive_unix_ns":2,"receive_monotonic_ns":150_000_001,
                "decode_done_monotonic_ns":2,
                "payload":{"channel":"ticker:45","ticker":{"s":"PUMP","b":{"price":"1.005"},"a":{"price":"1.015"}}}
            }),
        ];
        for row in rows {
            writeln!(input, "{}", serde_json::to_string(&row).unwrap()).unwrap();
        }
        let report = analyze_pump_capture(input.path(), &PumpLeadLagConfig::default()).unwrap();
        assert_eq!(report.capture_id, "test");
        assert_eq!(report.markets.len(), 2);
        let output = tempfile::tempdir().unwrap();
        let paths = write_pump_lead_lag_report(&report, output.path()).unwrap();
        assert!(paths.summary_json.exists());
        assert!(paths.metrics_csv.exists());
        assert!(paths.common_shocks_csv.exists());
    }
}
