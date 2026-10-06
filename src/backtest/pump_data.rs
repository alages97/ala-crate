//! Live historical-data collection for the PUMP cross-venue basis backtest.
//!
//! `UPUMP` (Hyperliquid spot API symbol `@188`) is treated as a one-for-one
//! representation of the official pump.fun PUMP token. The collector never
//! substitutes similarly named Hyperliquid assets such as `@20` or `HPUMP`.

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use reqwest::{Client, Response};
use rust_decimal::Decimal;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::{json, Value};
use thiserror::Error;

use super::{PumpBar, PumpFrame, PumpLeg};

pub const PUMP_ONE_MINUTE_MS: u64 = 60_000;
pub const HYPERLIQUID_INFO_URL: &str = "https://api.hyperliquid.xyz/info";
pub const HYPERLIQUID_SPOT_COIN: &str = "@188";
pub const HYPERLIQUID_PERP_COIN: &str = "PUMP";
pub const LIGHTER_API_URL: &str = "https://mainnet.zklighter.elliot.ai/api/v1";
pub const LIGHTER_MARKET_ID: u16 = 45;
pub const GECKO_TERMINAL_API_URL: &str = "https://api.geckoterminal.com/api/v2";
pub const PUMPSWAP_POOL: &str = "2uF4Xh61rDwxnG9woyxsVQP7zuA6kLFpb3NvnRQeoiSd";
pub const PUMP_MINT: &str = "pumpCmXqMfrsAkQ5r49WcJnRayYRqmXz6ae8H7H9Dfn";

const MAX_BARS: u32 = 5_000;
const LIGHTER_PAGE_BARS: u64 = 500;
const GECKO_PAGE_BARS: u32 = 1_000;
const HYPERLIQUID_FUNDING_PAGE_ROWS: u64 = 500;
const LIGHTER_FUNDING_PAGE_ROWS: u64 = 750;
const FUNDING_INTERVAL_MS: u64 = 60 * 60 * 1_000;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PumpDataConfig {
    #[serde(default)]
    pub interval: PumpDataInterval,
    pub bars: u32,
    #[serde(default = "default_gecko_page_delay_ms")]
    pub gecko_page_delay_ms: u64,
}

impl Default for PumpDataConfig {
    fn default() -> Self {
        Self {
            interval: PumpDataInterval::OneMinute,
            bars: MAX_BARS,
            gecko_page_delay_ms: default_gecko_page_delay_ms(),
        }
    }
}

impl PumpDataConfig {
    pub fn validate(&self) -> Result<(), PumpDataError> {
        if !(2..=MAX_BARS).contains(&self.bars) {
            return Err(PumpDataError::InvalidConfig(format!(
                "bars must be between 2 and {MAX_BARS}"
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum PumpDataInterval {
    #[default]
    #[serde(rename = "1m")]
    OneMinute,
    #[serde(rename = "4h")]
    FourHours,
}

impl PumpDataInterval {
    pub const fn duration_ms(self) -> u64 {
        match self {
            Self::OneMinute => PUMP_ONE_MINUTE_MS,
            Self::FourHours => 4 * 60 * 60 * 1_000,
        }
    }

    const fn venue_resolution(self) -> &'static str {
        match self {
            Self::OneMinute => "1m",
            Self::FourHours => "4h",
        }
    }

    const fn gecko_period(self) -> (&'static str, &'static str) {
        match self {
            Self::OneMinute => ("minute", "1"),
            Self::FourHours => ("hour", "4"),
        }
    }
}

impl std::fmt::Display for PumpDataInterval {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.venue_resolution())
    }
}

const fn default_gecko_page_delay_ms() -> u64 {
    6_100
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PumpSourceManifest {
    pub fetched_at_ms: u64,
    pub requested_start_ms: u64,
    pub requested_end_ms_exclusive: u64,
    pub interval_ms: u64,
    pub interval: PumpDataInterval,
    pub requested_bars: u32,
    pub aligned_start_ms: u64,
    pub aligned_end_ms_exclusive: u64,
    pub aligned_frames: usize,
    pub alignment_policy: String,
    pub execution_warning: String,
    pub instrument_identity: String,
    pub sources: Vec<PumpSourceManifestEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PumpSourceManifestEntry {
    pub leg: PumpLeg,
    pub venue: String,
    pub instrument: String,
    pub endpoint: String,
    pub fetched_candle_rows: usize,
    pub rows_in_aligned_window: usize,
    pub funding_rows: usize,
    pub notes: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PumpHistoricalDataset {
    pub frames: Vec<PumpFrame>,
    pub manifest: PumpSourceManifest,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PumpDatasetPaths {
    pub normalized_csv: PathBuf,
    pub source_manifest_json: PathBuf,
}

#[derive(Debug, Error)]
pub enum PumpDataError {
    #[error("invalid PUMP data configuration: {0}")]
    InvalidConfig(String),
    #[error("system clock is before the Unix epoch")]
    InvalidClock,
    #[error("HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("{feed} returned HTTP {status}: {body}")]
    HttpStatus {
        feed: &'static str,
        status: u16,
        body: String,
    },
    #[error("could not decode {feed}: {message}")]
    Decode { feed: &'static str, message: String },
    #[error("invalid decimal in {feed}: {value}")]
    Decimal { feed: &'static str, value: String },
    #[error("timestamp overflow while {0}")]
    Timestamp(&'static str),
    #[error("no common timestamps exist across all four PUMP legs")]
    NoCommonWindow,
    #[error("PUMP data I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("serialize PUMP data CSV: {0}")]
    Csv(#[from] csv::Error),
    #[error("serialize PUMP source manifest: {0}")]
    Json(#[from] serde_json::Error),
}

/// Fetches the most recent completed history from all four legs.
pub async fn fetch_pump_historical_data(
    config: &PumpDataConfig,
) -> Result<PumpHistoricalDataset, PumpDataError> {
    config.validate()?;
    let fetched_at_ms = current_time_ms()?;
    let interval_ms = config.interval.duration_ms();
    let requested_end_ms_exclusive = fetched_at_ms / interval_ms * interval_ms;
    let requested_span_ms = u64::from(config.bars)
        .checked_mul(interval_ms)
        .ok_or(PumpDataError::Timestamp("calculating requested span"))?;
    let requested_start_ms = requested_end_ms_exclusive
        .checked_sub(requested_span_ms)
        .ok_or(PumpDataError::Timestamp("calculating requested start"))?;
    let client = Client::builder()
        .user_agent("ala-crate-pump-arb/0.1")
        .timeout(Duration::from_secs(30))
        .build()?;

    let (hyperliquid_spot, hyperliquid_perp, lighter_perp, solana_spot) = tokio::try_join!(
        fetch_hyperliquid_candles(
            &client,
            HYPERLIQUID_SPOT_COIN,
            PumpLeg::HyperliquidSpot,
            config.interval,
            requested_start_ms,
            requested_end_ms_exclusive,
        ),
        fetch_hyperliquid_candles(
            &client,
            HYPERLIQUID_PERP_COIN,
            PumpLeg::HyperliquidPerp,
            config.interval,
            requested_start_ms,
            requested_end_ms_exclusive,
        ),
        fetch_lighter_candles(
            &client,
            config.interval,
            requested_start_ms,
            requested_end_ms_exclusive,
        ),
        fetch_gecko_candles(
            &client,
            config.interval,
            requested_start_ms,
            requested_end_ms_exclusive,
            config.gecko_page_delay_ms,
        ),
    )?;

    let (hyperliquid_funding, lighter_funding) = tokio::try_join!(
        fetch_hyperliquid_funding(&client, requested_start_ms, fetched_at_ms,),
        fetch_lighter_funding(&client, requested_start_ms, fetched_at_ms,),
    )?;

    let funding_row_counts = (hyperliquid_funding.len(), lighter_funding.len());
    let mut hyperliquid_perp = hyperliquid_perp;
    let mut lighter_perp = lighter_perp;
    attach_funding(&mut hyperliquid_perp, hyperliquid_funding, interval_ms);
    attach_funding(&mut lighter_perp, lighter_funding, interval_ms);

    let fetched_counts = [
        (PumpLeg::SolanaSpot, solana_spot.len()),
        (PumpLeg::HyperliquidSpot, hyperliquid_spot.len()),
        (PumpLeg::HyperliquidPerp, hyperliquid_perp.len()),
        (PumpLeg::LighterPerp, lighter_perp.len()),
    ];
    let series = BTreeMap::from([
        (PumpLeg::SolanaSpot, solana_spot),
        (PumpLeg::HyperliquidSpot, hyperliquid_spot),
        (PumpLeg::HyperliquidPerp, hyperliquid_perp),
        (PumpLeg::LighterPerp, lighter_perp),
    ]);
    let frames = align_longest_contiguous_window(&series, interval_ms)?;
    let aligned_start_ms = frames.first().expect("alignment is non-empty").open_time_ms;
    let aligned_end_ms_exclusive = frames.last().expect("alignment is non-empty").close_time_ms;

    let aligned_count = frames.len();
    let sources = vec![
        PumpSourceManifestEntry {
            leg: PumpLeg::SolanaSpot,
            venue: "PumpSwap via GeckoTerminal".to_owned(),
            instrument: format!("PUMP mint {PUMP_MINT} / USDC, pool {PUMPSWAP_POOL}"),
            endpoint: format!(
                "{GECKO_TERMINAL_API_URL}/networks/solana/pools/{PUMPSWAP_POOL}/ohlcv/{}",
                config.interval.gecko_period().0,
            ),
            fetched_candle_rows: fetched_count(&fetched_counts, PumpLeg::SolanaSpot),
            rows_in_aligned_window: aligned_count,
            funding_rows: 0,
            notes: "Public aggregate OHLCV. Config uses the current 30 bps noncanonical PumpSwap pool fee; this is conservative for the pre-May-2026 part of a 180-day sample, when the pool fee was 25 bps. Slippage is modelled separately.".to_owned(),
        },
        PumpSourceManifestEntry {
            leg: PumpLeg::HyperliquidSpot,
            venue: "Hyperliquid spot".to_owned(),
            instrument: "UPUMP (@188), the Unit PUMP wrapper tracking official PUMP 1:1".to_owned(),
            endpoint: HYPERLIQUID_INFO_URL.to_owned(),
            fetched_candle_rows: fetched_count(&fetched_counts, PumpLeg::HyperliquidSpot),
            rows_in_aligned_window: aligned_count,
            funding_rows: 0,
            notes: "Quote volume is approximated as base volume times candle close.".to_owned(),
        },
        PumpSourceManifestEntry {
            leg: PumpLeg::HyperliquidPerp,
            venue: "Hyperliquid perpetual".to_owned(),
            instrument: "PUMP perpetual".to_owned(),
            endpoint: HYPERLIQUID_INFO_URL.to_owned(),
            fetched_candle_rows: fetched_count(&fetched_counts, PumpLeg::HyperliquidPerp),
            rows_in_aligned_window: aligned_count,
            funding_rows: funding_row_counts.0,
            notes: format!("Hourly funding is attached to each right-closed {} bar; positive means longs pay.", config.interval),
        },
        PumpSourceManifestEntry {
            leg: PumpLeg::LighterPerp,
            venue: "Lighter perpetual".to_owned(),
            instrument: format!("PUMP perpetual, market_id={LIGHTER_MARKET_ID}"),
            endpoint: LIGHTER_API_URL.to_owned(),
            fetched_candle_rows: fetched_count(&fetched_counts, PumpLeg::LighterPerp),
            rows_in_aligned_window: aligned_count,
            funding_rows: funding_row_counts.1,
            notes: "Lighter funding rate is reported in percent; it is divided by 100 and signed so positive means longs pay. Candle execution cannot resolve Lighter's taker delay, so config slippage must absorb latency stress.".to_owned(),
        },
    ];

    Ok(PumpHistoricalDataset {
        frames,
        manifest: PumpSourceManifest {
            fetched_at_ms,
            requested_start_ms,
            requested_end_ms_exclusive,
            interval_ms,
            interval: config.interval,
            requested_bars: config.bars,
            aligned_start_ms,
            aligned_end_ms_exclusive,
            aligned_frames: aligned_count,
            alignment_policy: format!("Latest longest run of exact {} timestamps present on every leg; no forward filling.", config.interval),
            execution_warning: "Candle data is a persistent-basis screen, not proof of executable arbitrage: historical synchronized bid/ask and depth are unavailable.".to_owned(),
            instrument_identity: "Hyperliquid UPUMP (@188) is treated as a 1:1 representation of canonical pump.fun PUMP.".to_owned(),
            sources,
        },
    })
}

pub fn write_pump_dataset(
    dataset: &PumpHistoricalDataset,
    output_directory: impl AsRef<Path>,
) -> Result<PumpDatasetPaths, PumpDataError> {
    let output_directory = output_directory.as_ref();
    fs::create_dir_all(output_directory)?;
    let paths = PumpDatasetPaths {
        normalized_csv: output_directory.join("normalized.csv"),
        source_manifest_json: output_directory.join("source_manifest.json"),
    };

    let mut csv = csv::Writer::from_path(&paths.normalized_csv)?;
    csv.write_record([
        "leg",
        "open_time_ms",
        "close_time_ms",
        "open",
        "close",
        "volume_usd",
        "funding_rate",
    ])?;
    for frame in &dataset.frames {
        for (leg, bar) in &frame.bars {
            csv.write_record([
                leg.to_string(),
                bar.open_time_ms.to_string(),
                bar.close_time_ms.to_string(),
                bar.open.to_string(),
                bar.close.to_string(),
                bar.volume_usd.to_string(),
                bar.funding_rate
                    .map_or_else(String::new, |rate| rate.to_string()),
            ])?;
        }
    }
    csv.flush()?;

    let mut manifest = BufWriter::new(File::create(&paths.source_manifest_json)?);
    serde_json::to_writer_pretty(&mut manifest, &dataset.manifest)?;
    manifest.write_all(b"\n")?;
    manifest.flush()?;
    Ok(paths)
}

/// Intersects all four feeds and returns the latest longest contiguous run.
pub fn align_longest_contiguous_window(
    series: &BTreeMap<PumpLeg, Vec<PumpBar>>,
    interval_ms: u64,
) -> Result<Vec<PumpFrame>, PumpDataError> {
    if interval_ms == 0 {
        return Err(PumpDataError::InvalidConfig(
            "alignment interval must be positive".to_owned(),
        ));
    }
    let required_legs = [
        PumpLeg::SolanaSpot,
        PumpLeg::HyperliquidSpot,
        PumpLeg::HyperliquidPerp,
        PumpLeg::LighterPerp,
    ];
    let indexed: BTreeMap<PumpLeg, BTreeMap<u64, &PumpBar>> = required_legs
        .iter()
        .filter_map(|leg| {
            series.get(leg).map(|bars| {
                (
                    *leg,
                    bars.iter().map(|bar| (bar.open_time_ms, bar)).collect(),
                )
            })
        })
        .collect();
    if indexed.len() != required_legs.len() {
        return Err(PumpDataError::NoCommonWindow);
    }

    let first = indexed
        .get(&required_legs[0])
        .expect("required leg was checked");
    let common: Vec<u64> = first
        .keys()
        .copied()
        .filter(|timestamp| {
            required_legs[1..]
                .iter()
                .all(|leg| indexed[leg].contains_key(timestamp))
        })
        .collect();
    if common.is_empty() {
        return Err(PumpDataError::NoCommonWindow);
    }

    let mut best_start = 0_usize;
    let mut best_len = 1_usize;
    let mut run_start = 0_usize;
    for cursor in 1..=common.len() {
        let continues = cursor < common.len()
            && common[cursor] == common[cursor - 1].saturating_add(interval_ms);
        if !continues {
            let run_len = cursor - run_start;
            if run_len >= best_len {
                best_start = run_start;
                best_len = run_len;
            }
            run_start = cursor;
        }
    }

    common[best_start..best_start + best_len]
        .iter()
        .enumerate()
        .map(|(index, timestamp)| {
            let bars = required_legs
                .iter()
                .map(|leg| (*leg, indexed[leg][timestamp].clone()))
                .collect();
            Ok(PumpFrame {
                index,
                open_time_ms: *timestamp,
                close_time_ms: timestamp
                    .checked_add(interval_ms)
                    .ok_or(PumpDataError::Timestamp("closing aligned frame"))?,
                bars,
            })
        })
        .collect()
}

async fn fetch_hyperliquid_candles(
    client: &Client,
    coin: &'static str,
    leg: PumpLeg,
    interval: PumpDataInterval,
    start_ms: u64,
    end_ms_exclusive: u64,
) -> Result<Vec<PumpBar>, PumpDataError> {
    let end_ms = end_ms_exclusive
        .checked_sub(1)
        .ok_or(PumpDataError::Timestamp("closing Hyperliquid request"))?;
    let response = client
        .post(HYPERLIQUID_INFO_URL)
        .json(&json!({
            "type": "candleSnapshot",
            "req": {
                "coin": coin,
                "interval": interval.venue_resolution(),
                "startTime": start_ms,
                "endTime": end_ms,
            }
        }))
        .send()
        .await?;
    let feed = if leg == PumpLeg::HyperliquidSpot {
        "Hyperliquid UPUMP candles"
    } else {
        "Hyperliquid PUMP perpetual candles"
    };
    let body = checked_body(response, feed).await?;
    parse_hyperliquid_candles(&body, feed, interval.duration_ms())
}

async fn fetch_lighter_candles(
    client: &Client,
    interval: PumpDataInterval,
    start_ms: u64,
    end_ms_exclusive: u64,
) -> Result<Vec<PumpBar>, PumpDataError> {
    let start_seconds = start_ms / 1_000;
    let interval_ms = interval.duration_ms();
    // Lighter treats end_timestamp as exclusive, matching our window.
    let mut cursor_seconds = end_ms_exclusive / 1_000;
    let mut bars = Vec::new();
    let maximum_pages = ((u64::from(MAX_BARS) / LIGHTER_PAGE_BARS) + 3) as usize;
    for _ in 0..maximum_pages {
        let response = client
            .get(format!("{LIGHTER_API_URL}/candles"))
            .query(&[
                ("market_id", LIGHTER_MARKET_ID.to_string()),
                ("resolution", interval.venue_resolution().to_owned()),
                ("start_timestamp", start_seconds.to_string()),
                ("end_timestamp", cursor_seconds.to_string()),
                ("count_back", "0".to_owned()),
            ])
            .send()
            .await?;
        let body = checked_body(response, "Lighter PUMP candles").await?;
        let page = parse_lighter_candles(&body, interval_ms)?;
        let oldest_ms = page.iter().map(|bar| bar.open_time_ms).min();
        bars.extend(
            page.into_iter()
                .filter(|bar| bar.open_time_ms >= start_ms && bar.open_time_ms < end_ms_exclusive),
        );
        let Some(oldest_ms) = oldest_ms else {
            break;
        };
        if oldest_ms <= start_ms {
            break;
        }
        let previous_cursor = cursor_seconds;
        cursor_seconds = oldest_ms / 1_000;
        if cursor_seconds >= previous_cursor {
            break;
        }
    }
    Ok(deduplicate_bars(bars))
}

async fn fetch_gecko_candles(
    client: &Client,
    interval: PumpDataInterval,
    start_ms: u64,
    end_ms_exclusive: u64,
    page_delay_ms: u64,
) -> Result<Vec<PumpBar>, PumpDataError> {
    let interval_ms = interval.duration_ms();
    let (period, aggregate) = interval.gecko_period();
    let mut before_seconds = end_ms_exclusive / 1_000;
    let mut bars = Vec::new();
    let maximum_pages = ((MAX_BARS / GECKO_PAGE_BARS) + 3) as usize;
    for page_index in 0..maximum_pages {
        let response = client
            .get(format!(
                "{GECKO_TERMINAL_API_URL}/networks/solana/pools/{PUMPSWAP_POOL}/ohlcv/{period}"
            ))
            .query(&[
                ("aggregate", aggregate.to_owned()),
                ("limit", GECKO_PAGE_BARS.to_string()),
                ("before_timestamp", before_seconds.to_string()),
                ("currency", "usd".to_owned()),
                ("token", "base".to_owned()),
            ])
            .send()
            .await?;
        let body = checked_body(response, "GeckoTerminal PumpSwap candles").await?;
        let page = parse_gecko_candles(&body, interval_ms)?;
        let oldest_ms = page.iter().map(|bar| bar.open_time_ms).min();
        bars.extend(
            page.into_iter()
                .filter(|bar| bar.open_time_ms >= start_ms && bar.open_time_ms < end_ms_exclusive),
        );
        let Some(oldest_ms) = oldest_ms else {
            break;
        };
        if oldest_ms <= start_ms {
            break;
        }
        let previous_cursor = before_seconds;
        before_seconds = oldest_ms
            .checked_div(1_000)
            .and_then(|value| value.checked_sub(1))
            .ok_or(PumpDataError::Timestamp("paging GeckoTerminal candles"))?;
        if before_seconds >= previous_cursor {
            break;
        }
        if page_index + 1 < maximum_pages && page_delay_ms > 0 {
            tokio::time::sleep(Duration::from_millis(page_delay_ms)).await;
        }
    }
    Ok(deduplicate_bars(bars))
}

async fn fetch_hyperliquid_funding(
    client: &Client,
    start_ms: u64,
    query_end_ms: u64,
) -> Result<Vec<(u64, Decimal)>, PumpDataError> {
    let maximum_pages = funding_page_limit(start_ms, query_end_ms, HYPERLIQUID_FUNDING_PAGE_ROWS);
    let mut cursor_ms = start_ms;
    let mut funding = Vec::new();
    for _ in 0..maximum_pages {
        let response = client
            .post(HYPERLIQUID_INFO_URL)
            .json(&json!({
                "type": "fundingHistory",
                "coin": HYPERLIQUID_PERP_COIN,
                "startTime": cursor_ms,
                // Query through wall-clock fetch time so a settlement at the
                // final candle close is included in that right-closed candle.
                "endTime": query_end_ms,
            }))
            .send()
            .await?;
        let body = checked_body(response, "Hyperliquid PUMP funding").await?;
        let page = parse_hyperliquid_funding(&body)?;
        let newest_ms = page.iter().map(|(timestamp, _)| *timestamp).max();
        funding.extend(page);
        let Some(newest_ms) = newest_ms else {
            break;
        };
        if newest_ms >= query_end_ms {
            break;
        }
        let next_cursor = newest_ms
            .checked_add(1)
            .ok_or(PumpDataError::Timestamp("paging Hyperliquid funding"))?;
        if next_cursor <= cursor_ms {
            break;
        }
        cursor_ms = next_cursor;
    }
    Ok(deduplicate_funding(funding))
}

async fn fetch_lighter_funding(
    client: &Client,
    start_ms: u64,
    query_end_ms: u64,
) -> Result<Vec<(u64, Decimal)>, PumpDataError> {
    // Lighter accepts millisecond request bounds even though funding rows use
    // Unix seconds in their response payload.
    let mut cursor_ms = query_end_ms;
    let maximum_pages = funding_page_limit(start_ms, query_end_ms, LIGHTER_FUNDING_PAGE_ROWS);
    let mut funding = Vec::new();
    for _ in 0..maximum_pages {
        let response = client
            .get(format!("{LIGHTER_API_URL}/fundings"))
            .query(&[
                ("market_id", LIGHTER_MARKET_ID.to_string()),
                ("resolution", "1h".to_owned()),
                ("start_timestamp", start_ms.to_string()),
                ("end_timestamp", cursor_ms.to_string()),
                ("count_back", "0".to_owned()),
            ])
            .send()
            .await?;
        let body = checked_body(response, "Lighter PUMP funding").await?;
        let page = parse_lighter_funding(&body)?;
        let oldest_ms = page.iter().map(|(timestamp, _)| *timestamp).min();
        funding.extend(page);
        let Some(oldest_ms) = oldest_ms else {
            break;
        };
        // If the oldest returned settlement is the first hourly boundary
        // after our start, there cannot be another in-range funding row.
        // Lighter rejects a final sub-hour query after internal rounding.
        if oldest_ms.saturating_sub(start_ms) <= FUNDING_INTERVAL_MS {
            break;
        }
        let previous_cursor = cursor_ms;
        cursor_ms = oldest_ms;
        if cursor_ms >= previous_cursor {
            break;
        }
    }
    Ok(deduplicate_funding(funding))
}

fn funding_page_limit(start_ms: u64, end_ms: u64, page_rows: u64) -> usize {
    let hours = end_ms.saturating_sub(start_ms) / FUNDING_INTERVAL_MS;
    usize::try_from(hours / page_rows + 3).unwrap_or(usize::MAX)
}

async fn checked_body(response: Response, feed: &'static str) -> Result<String, PumpDataError> {
    let status = response.status();
    let body = response.text().await?;
    if !status.is_success() {
        return Err(PumpDataError::HttpStatus {
            feed,
            status: status.as_u16(),
            body: body.chars().take(500).collect(),
        });
    }
    Ok(body)
}

#[derive(Debug, Deserialize)]
struct HyperliquidCandle {
    t: u64,
    o: Value,
    c: Value,
    v: Value,
}

fn parse_hyperliquid_candles(
    body: &str,
    feed: &'static str,
    interval_ms: u64,
) -> Result<Vec<PumpBar>, PumpDataError> {
    let rows: Vec<HyperliquidCandle> = decode(body, feed)?;
    rows.into_iter()
        .map(|row| {
            let open = parse_decimal(&row.o, feed)?;
            let close = parse_decimal(&row.c, feed)?;
            let base_volume = parse_decimal(&row.v, feed)?;
            Ok(PumpBar {
                open_time_ms: row.t,
                close_time_ms: row
                    .t
                    .checked_add(interval_ms)
                    .ok_or(PumpDataError::Timestamp("normalizing Hyperliquid candle"))?,
                open,
                close,
                volume_usd: base_volume * close,
                funding_rate: None,
            })
        })
        .collect()
}

#[derive(Debug, Deserialize)]
struct LighterCandleResponse {
    code: u16,
    c: Vec<LighterCandle>,
}

#[derive(Debug, Deserialize)]
struct LighterCandle {
    t: u64,
    o: Value,
    c: Value,
    #[serde(rename = "V")]
    quote_volume: Value,
}

fn parse_lighter_candles(body: &str, interval_ms: u64) -> Result<Vec<PumpBar>, PumpDataError> {
    const FEED: &str = "Lighter PUMP candles";
    let response: LighterCandleResponse = decode(body, FEED)?;
    if response.code != 200 {
        return Err(PumpDataError::Decode {
            feed: FEED,
            message: format!("application response code {}", response.code),
        });
    }
    response
        .c
        .into_iter()
        .map(|row| {
            Ok(PumpBar {
                open_time_ms: row.t,
                close_time_ms: row
                    .t
                    .checked_add(interval_ms)
                    .ok_or(PumpDataError::Timestamp("normalizing Lighter candle"))?,
                open: parse_decimal(&row.o, FEED)?,
                close: parse_decimal(&row.c, FEED)?,
                volume_usd: parse_decimal(&row.quote_volume, FEED)?,
                funding_rate: None,
            })
        })
        .collect()
}

#[derive(Debug, Deserialize)]
struct GeckoResponse {
    data: GeckoData,
}

#[derive(Debug, Deserialize)]
struct GeckoData {
    attributes: GeckoAttributes,
}

#[derive(Debug, Deserialize)]
struct GeckoAttributes {
    ohlcv_list: Vec<Vec<Value>>,
}

fn parse_gecko_candles(body: &str, interval_ms: u64) -> Result<Vec<PumpBar>, PumpDataError> {
    const FEED: &str = "GeckoTerminal PumpSwap candles";
    let response: GeckoResponse = decode(body, FEED)?;
    response
        .data
        .attributes
        .ohlcv_list
        .into_iter()
        .map(|row| {
            if row.len() < 6 {
                return Err(PumpDataError::Decode {
                    feed: FEED,
                    message: format!("OHLCV row has {} fields, expected 6", row.len()),
                });
            }
            let timestamp_seconds = row[0].as_u64().ok_or_else(|| PumpDataError::Decode {
                feed: FEED,
                message: format!("invalid timestamp {}", row[0]),
            })?;
            let open_time_ms = timestamp_seconds
                .checked_mul(1_000)
                .ok_or(PumpDataError::Timestamp("normalizing GeckoTerminal candle"))?;
            Ok(PumpBar {
                open_time_ms,
                close_time_ms: open_time_ms
                    .checked_add(interval_ms)
                    .ok_or(PumpDataError::Timestamp("closing GeckoTerminal candle"))?,
                open: parse_decimal(&row[1], FEED)?,
                close: parse_decimal(&row[4], FEED)?,
                volume_usd: parse_decimal(&row[5], FEED)?,
                funding_rate: None,
            })
        })
        .collect()
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct HyperliquidFunding {
    funding_rate: Value,
    time: u64,
}

fn parse_hyperliquid_funding(body: &str) -> Result<Vec<(u64, Decimal)>, PumpDataError> {
    const FEED: &str = "Hyperliquid PUMP funding";
    let rows: Vec<HyperliquidFunding> = decode(body, FEED)?;
    rows.into_iter()
        .map(|row| Ok((row.time, parse_decimal(&row.funding_rate, FEED)?)))
        .collect()
}

#[derive(Debug, Deserialize)]
struct LighterFundingResponse {
    code: u16,
    fundings: Vec<LighterFunding>,
}

#[derive(Debug, Deserialize)]
struct LighterFunding {
    timestamp: u64,
    rate: Value,
    direction: String,
}

fn parse_lighter_funding(body: &str) -> Result<Vec<(u64, Decimal)>, PumpDataError> {
    const FEED: &str = "Lighter PUMP funding";
    let response: LighterFundingResponse = decode(body, FEED)?;
    if response.code != 200 {
        return Err(PumpDataError::Decode {
            feed: FEED,
            message: format!("application response code {}", response.code),
        });
    }
    response
        .fundings
        .into_iter()
        .map(|row| {
            let percentage = parse_decimal(&row.rate, FEED)?;
            let magnitude = percentage / Decimal::ONE_HUNDRED;
            let rate = match row.direction.as_str() {
                "long" => magnitude,
                "short" => -magnitude,
                direction => {
                    return Err(PumpDataError::Decode {
                        feed: FEED,
                        message: format!("unknown funding direction {direction:?}"),
                    });
                }
            };
            let timestamp_ms = row
                .timestamp
                .checked_mul(1_000)
                .ok_or(PumpDataError::Timestamp("normalizing Lighter funding"))?;
            Ok((timestamp_ms, rate))
        })
        .collect()
}

fn attach_funding(bars: &mut [PumpBar], funding: Vec<(u64, Decimal)>, interval_ms: u64) {
    let mut by_bar = BTreeMap::<u64, Decimal>::new();
    for (timestamp_ms, rate) in funding {
        // Both venues settle on the hour. Hyperliquid timestamps can arrive a
        // few milliseconds after that scheduled boundary, so first normalize
        // to the hour. Right-closed assignment means a fill at the settlement
        // time cannot collect funding that was just paid.
        let settlement_ms = timestamp_ms / (60 * 60 * 1_000) * (60 * 60 * 1_000);
        let bar_open_ms = settlement_ms
            .saturating_sub(1)
            .checked_div(interval_ms)
            .unwrap_or(0)
            * interval_ms;
        *by_bar.entry(bar_open_ms).or_default() += rate;
    }
    for bar in bars {
        bar.funding_rate = by_bar.remove(&bar.open_time_ms);
    }
}

fn deduplicate_bars(bars: Vec<PumpBar>) -> Vec<PumpBar> {
    bars.into_iter()
        .map(|bar| (bar.open_time_ms, bar))
        .collect::<BTreeMap<_, _>>()
        .into_values()
        .collect()
}

fn deduplicate_funding(funding: Vec<(u64, Decimal)>) -> Vec<(u64, Decimal)> {
    funding
        .into_iter()
        .collect::<BTreeMap<_, _>>()
        .into_iter()
        .collect()
}

fn fetched_count(counts: &[(PumpLeg, usize)], leg: PumpLeg) -> usize {
    counts
        .iter()
        .find_map(|(candidate, count)| (*candidate == leg).then_some(*count))
        .unwrap_or(0)
}

fn parse_decimal(value: &Value, feed: &'static str) -> Result<Decimal, PumpDataError> {
    let value = match value {
        Value::String(value) => value.clone(),
        Value::Number(value) => value.to_string(),
        other => {
            return Err(PumpDataError::Decimal {
                feed,
                value: other.to_string(),
            });
        }
    };
    Decimal::from_str(&value)
        .or_else(|_| Decimal::from_scientific(&value))
        .map_err(|_| PumpDataError::Decimal { feed, value })
}

fn decode<T: DeserializeOwned>(body: &str, feed: &'static str) -> Result<T, PumpDataError> {
    serde_json::from_str(body).map_err(|error| PumpDataError::Decode {
        feed,
        message: error.to_string(),
    })
}

fn current_time_ms() -> Result<u64, PumpDataError> {
    let milliseconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| PumpDataError::InvalidClock)?
        .as_millis();
    u64::try_from(milliseconds).map_err(|_| PumpDataError::Timestamp("reading system time"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::prelude::FromPrimitive;
    use std::collections::BTreeSet;

    fn bar(timestamp: u64, price: i64) -> PumpBar {
        PumpBar {
            open_time_ms: timestamp,
            close_time_ms: timestamp + PUMP_ONE_MINUTE_MS,
            open: Decimal::from(price),
            close: Decimal::from(price),
            volume_usd: Decimal::from(1_000),
            funding_rate: None,
        }
    }

    #[test]
    fn parses_each_live_candle_shape_without_binary_floats() {
        let hyperliquid = parse_hyperliquid_candles(
            r#"[{"t":60000,"o":"0.0039","c":"0.0040","v":"2500.0"}]"#,
            "fixture",
            PUMP_ONE_MINUTE_MS,
        )
        .unwrap();
        assert_eq!(hyperliquid[0].open, Decimal::from_str("0.0039").unwrap());
        assert_eq!(hyperliquid[0].volume_usd, Decimal::from(10));

        let lighter = parse_lighter_candles(
            r#"{"code":200,"c":[{"t":60000,"o":0.0039,"c":0.004,"V":12.345}]}"#,
            PUMP_ONE_MINUTE_MS,
        )
        .unwrap();
        assert_eq!(lighter[0].volume_usd, Decimal::from_str("12.345").unwrap());

        let gecko = parse_gecko_candles(
            r#"{"data":{"attributes":{"ohlcv_list":[[60,0.0039,0.0041,0.0038,0.0040,99.5]]}}}"#,
            PUMP_ONE_MINUTE_MS,
        )
        .unwrap();
        assert_eq!(gecko[0].open_time_ms, 60_000);
        assert_eq!(gecko[0].volume_usd, Decimal::from_str("99.5").unwrap());
    }

    #[test]
    fn converts_lighter_percentage_and_direction_to_cash_rate() {
        let funding = parse_lighter_funding(
            r#"{"code":200,"fundings":[{"timestamp":3600,"rate":"0.0012","direction":"long"},{"timestamp":7200,"rate":"0.0020","direction":"short"}]}"#,
        )
        .unwrap();
        assert_eq!(
            funding[0],
            (3_600_000, Decimal::from_str("0.000012").unwrap())
        );
        assert_eq!(
            funding[1],
            (7_200_000, Decimal::from_str("-0.000020").unwrap())
        );
    }

    #[test]
    fn picks_latest_longest_exact_common_contiguous_window() {
        let all = [
            PumpLeg::SolanaSpot,
            PumpLeg::HyperliquidSpot,
            PumpLeg::HyperliquidPerp,
            PumpLeg::LighterPerp,
        ];
        let series = all
            .into_iter()
            .map(|leg| {
                let mut timestamps = vec![0, 60_000, 180_000, 240_000];
                if leg == PumpLeg::LighterPerp {
                    timestamps.push(300_000);
                }
                (
                    leg,
                    timestamps.into_iter().map(|time| bar(time, 1)).collect(),
                )
            })
            .collect();
        let frames = align_longest_contiguous_window(&series, PUMP_ONE_MINUTE_MS).unwrap();
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].open_time_ms, 180_000);
        assert_eq!(frames[1].index, 1);
        assert!(frames.iter().all(|frame| frame.bars.len() == 4));
    }

    #[test]
    fn funding_is_attached_only_to_its_event_minute() {
        let mut bars = vec![bar(0, 1), bar(60_000, 1)];
        attach_funding(
            &mut bars,
            vec![
                (3_600_038, Decimal::from_f64(0.1).unwrap()),
                (7_200_001, Decimal::from_f64(0.2).unwrap()),
            ],
            PUMP_ONE_MINUTE_MS,
        );
        assert_eq!(bars[0].funding_rate, None);
        assert_eq!(bars[1].funding_rate, None);

        let mut settlement_bars = vec![bar(3_540_000, 1), bar(3_600_000, 1)];
        attach_funding(
            &mut settlement_bars,
            vec![(3_600_038, Decimal::from_f64(0.1).unwrap())],
            PUMP_ONE_MINUTE_MS,
        );
        assert_eq!(settlement_bars[0].funding_rate, Decimal::from_f64(0.1));
        assert_eq!(settlement_bars[1].funding_rate, None);
    }

    #[test]
    fn required_leg_missing_is_not_silently_forward_filled() {
        let series = BTreeMap::from([(PumpLeg::SolanaSpot, vec![bar(PUMP_ONE_MINUTE_MS, 1)])]);
        assert!(matches!(
            align_longest_contiguous_window(&series, PUMP_ONE_MINUTE_MS),
            Err(PumpDataError::NoCommonWindow)
        ));
    }

    #[test]
    fn parser_rejects_unknown_lighter_funding_direction() {
        let error = parse_lighter_funding(
            r#"{"code":200,"fundings":[{"timestamp":3600,"rate":"0.0012","direction":"both"}]}"#,
        )
        .unwrap_err();
        assert!(matches!(error, PumpDataError::Decode { .. }));
    }

    #[test]
    fn common_timestamps_are_unique_after_indexing() {
        let timestamps: BTreeSet<u64> = [0, 0, 60_000].into_iter().collect();
        assert_eq!(timestamps.len(), 2);
    }
}
