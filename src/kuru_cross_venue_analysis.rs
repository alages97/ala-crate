//! Causal publisher-time decision analysis between pinned Kuru spot quotes and live CEX BBOs.
//!
//! The Kuru side is pinned to an earlier Monad block, while the CEX side is the
//! latest BBO published by the decision time. Publisher time does not prove that
//! a downstream consumer had received the BBO. This module does not model an
//! atomic transaction, fills beyond displayed top size, fees, funding, gas,
//! transfer or inventory risk, and therefore never labels the result as realized
//! PnL.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::str::FromStr;

use anyhow::{anyhow, bail, Context, Result};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::market_data::{decode_market_data_frame, Bbo, MarketDataEnvironment};
use crate::types::Venue;

pub const KURU_CROSS_VENUE_ANALYSIS_SCHEMA_VERSION: u16 = 2;

#[derive(Debug, Clone)]
pub struct KuruCrossVenueAnalysisConfig {
    pub symbol: String,
    pub max_bbo_age_ms: u64,
    pub threshold_bps: String,
    pub base_decimals: u32,
    pub quote_decimals: u32,
}

impl Default for KuruCrossVenueAnalysisConfig {
    fn default() -> Self {
        Self {
            symbol: "MON".to_owned(),
            max_bbo_age_ms: 500,
            threshold_bps: "5".to_owned(),
            base_decimals: 18,
            quote_decimals: 6,
        }
    }
}

impl KuruCrossVenueAnalysisConfig {
    fn validate(&self) -> Result<Decimal> {
        if self.symbol.trim().is_empty() {
            bail!("symbol must not be empty");
        }
        if self.max_bbo_age_ms > 60_000 {
            bail!("max_bbo_age_ms must be at most 60000");
        }
        if self.base_decimals > 28 || self.quote_decimals > 28 {
            bail!("base_decimals and quote_decimals must be at most 28");
        }
        let threshold = parse_decimal(&self.threshold_bps).context("parse threshold_bps")?;
        if threshold.is_sign_negative() || threshold > Decimal::from(10_000_u64) {
            bail!("threshold_bps must be between 0 and 10000");
        }
        Ok(threshold)
    }
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct KuruCrossVenueAnalysisReport {
    pub schema_version: u16,
    pub kuru_input_path: PathBuf,
    pub bbo_input_path: PathBuf,
    pub symbol: String,
    pub venues: Vec<&'static str>,
    pub max_bbo_age_ms: u64,
    pub threshold_bps: String,
    pub base_decimals: u32,
    pub quote_decimals: u32,
    pub capture_complete: bool,
    pub run_ids: Vec<String>,
    pub kuru_records: KuruInputCounts,
    pub bbo_records: BboInputCounts,
    pub kuru_head_to_decision_ms: Option<ExactDecimalDistribution>,
    pub results: Vec<CrossVenueResult>,
    pub interpretation: Vec<&'static str>,
}

#[derive(Debug, Clone, Default, Serialize, PartialEq, Eq)]
pub struct KuruInputCounts {
    pub total_lines: u64,
    pub startup: u64,
    pub block_proof: u64,
    pub rpc_error: u64,
    pub proof_error: u64,
    pub shutdown: u64,
    pub canonical_blocks: u64,
    pub implementation_consistent_blocks: u64,
    pub first_decision_timestamp_ms: Option<u64>,
    pub last_decision_timestamp_ms: Option<u64>,
}

#[derive(Debug, Clone, Default, Serialize, PartialEq, Eq)]
pub struct BboInputCounts {
    pub total_lines: u64,
    pub target_live_records: u64,
    pub ignored_symbol_records: u64,
    pub ignored_venue_records: u64,
    pub distinct_streams: u64,
    pub first_publisher_timestamp_ms: Option<u64>,
    pub last_publisher_timestamp_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct CrossVenueResult {
    pub venue: String,
    pub direction: String,
    pub notional_usdc: String,
    pub observations: u64,
    pub kuru_quote_available: u64,
    pub canonical_kuru_quotes: u64,
    pub implementation_consistent_kuru_quotes: u64,
    pub same_block_kuru_quotes: u64,
    pub full_input_consumption_proven: u64,
    pub kuru_leg_diagnostics: u64,
    pub no_causal_bbo: u64,
    pub stale_bbo: u64,
    pub fresh_bbo: u64,
    pub screenable_basis_observations: u64,
    pub top_size_sufficient: u64,
    pub top_size_insufficient: u64,
    pub positive_basis: u64,
    pub threshold_exceedances: u64,
    pub positive_top_size_sufficient_basis: u64,
    pub threshold_top_size_sufficient_exceedances: u64,
    /// Requires canonical/same-implementation Kuru data, fresh BBO, sufficient
    /// displayed top size, and proven Kuru input consumption. It remains a
    /// non-atomic entry screen, not an execution or PnL observation.
    pub strict_screen_observations: u64,
    /// Age of the selected BBO according to the local publisher clock. This is
    /// nonnegative because the join is causal on publisher timestamp.
    pub bbo_age_ms: Option<ExactDecimalDistribution>,
    /// Decision time minus the exchange event timestamp. This can be negative
    /// when the exchange clock is ahead of the local publisher clock.
    pub bbo_exchange_age_ms: Option<ExactDecimalDistribution>,
    pub basis_bps: Option<ExactDecimalDistribution>,
    pub top_size_sufficient_basis_bps: Option<ExactDecimalDistribution>,
    pub maximum_basis_observation: Option<MaximumBasisObservation>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct ExactDecimalDistribution {
    pub count: u64,
    pub min: String,
    pub mean: String,
    pub p05: String,
    pub p50: String,
    pub p95: String,
    pub p99: String,
    pub max: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct MaximumBasisObservation {
    pub basis_bps: String,
    pub monad_block: u64,
    pub decision_timestamp_ms: u64,
    pub bbo_publisher_timestamp_ms: u64,
    pub bbo_age_ms: u64,
    pub displayed_top_size_sufficient: bool,
    pub full_input_consumption_proven: bool,
}

#[derive(Debug, Deserialize)]
struct Envelope {
    record_type: String,
}

#[derive(Debug, Deserialize)]
struct RunRecord {
    run_id: String,
}

#[derive(Debug, Deserialize)]
struct BlockProofRecord {
    run_id: String,
    observed_unix_ns: u64,
    proof_duration_ns: u64,
    head_to_decision_ns: u64,
    block: CapturedBlock,
    canonical_hash_verified_after: bool,
    implementation_matches_startup: Option<bool>,
    observations: Vec<NotionalProof>,
}

#[derive(Debug, Deserialize)]
struct CapturedBlock {
    number_u64: u64,
}

#[derive(Debug, Deserialize)]
struct NotionalProof {
    notional_usdc: String,
    uniswap_buy_kuru_sell: RouteProof,
    kuru_buy_uniswap_sell: RouteProof,
}

#[derive(Debug, Deserialize)]
struct RouteProof {
    first_leg: LegProof,
    second_leg: LegProof,
    same_block_composed: bool,
}

#[derive(Debug, Deserialize)]
struct LegProof {
    venue: String,
    requested_input_raw: String,
    submitted_input_raw: Option<String>,
    amount_in_used_raw: Option<String>,
    amount_out_raw: Option<String>,
    full_input_consumption_proven: bool,
    error: Option<String>,
}

#[derive(Debug)]
struct IndexedBbo {
    bbo: Bbo,
    ordinal: u64,
}

#[derive(Debug)]
struct KuruQuote {
    effective_price: Decimal,
    hedge_quantity: Decimal,
    full_input_consumption_proven: bool,
    diagnostic: bool,
    same_block_composed: bool,
}

#[derive(Debug, Clone, Copy)]
enum Direction {
    KuruBuyShortPerp,
    KuruSellLongPerp,
}

impl Direction {
    const fn name(self) -> &'static str {
        match self {
            Self::KuruBuyShortPerp => "kuru_buy_short_perp",
            Self::KuruSellLongPerp => "kuru_sell_long_perp",
        }
    }
}

#[derive(Debug, Default)]
struct ResultAccumulator {
    observations: u64,
    kuru_quote_available: u64,
    canonical_kuru_quotes: u64,
    implementation_consistent_kuru_quotes: u64,
    same_block_kuru_quotes: u64,
    full_input_consumption_proven: u64,
    kuru_leg_diagnostics: u64,
    no_causal_bbo: u64,
    stale_bbo: u64,
    fresh_bbo: u64,
    screenable_basis_observations: u64,
    top_size_sufficient: u64,
    top_size_insufficient: u64,
    positive_basis: u64,
    threshold_exceedances: u64,
    positive_top_size_sufficient_basis: u64,
    threshold_top_size_sufficient_exceedances: u64,
    strict_screen_observations: u64,
    ages_ms: Vec<Decimal>,
    exchange_ages_ms: Vec<Decimal>,
    basis_bps: Vec<Decimal>,
    sufficient_basis_bps: Vec<Decimal>,
    maximum: Option<(Decimal, MaximumBasisObservation)>,
}

/// Join pinned Kuru quote proofs to the latest BBO available by publisher time.
pub fn analyze_kuru_cross_venue(
    kuru_input_path: &Path,
    bbo_input_path: &Path,
    config: &KuruCrossVenueAnalysisConfig,
) -> Result<KuruCrossVenueAnalysisReport> {
    let threshold = config.validate()?;
    let (bbo_index, bbo_counts) = load_bbo_index(bbo_input_path, config)?;
    let file = File::open(kuru_input_path)
        .with_context(|| format!("open Kuru capture {}", kuru_input_path.display()))?;
    let reader = BufReader::new(file);
    let mut counts = KuruInputCounts::default();
    let mut run_ids = BTreeSet::new();
    let mut shutdown_runs = BTreeSet::new();
    let mut seen_run_blocks = BTreeSet::new();
    let mut head_to_decision_ms = Vec::new();
    let mut results: BTreeMap<(String, String, String), ResultAccumulator> = BTreeMap::new();

    for (line_index, line) in reader.lines().enumerate() {
        let line_number = line_index + 1;
        let line =
            line.with_context(|| format!("read {} line {line_number}", kuru_input_path.display()))?;
        if line.trim().is_empty() {
            continue;
        }
        counts.total_lines += 1;
        let envelope: Envelope = parse_record(&line, kuru_input_path, line_number)?;
        match envelope.record_type.as_str() {
            "startup" => {
                let record: RunRecord = parse_record(&line, kuru_input_path, line_number)?;
                counts.startup += 1;
                run_ids.insert(record.run_id);
            }
            "block_proof" => {
                let record: BlockProofRecord = parse_record(&line, kuru_input_path, line_number)?;
                counts.block_proof += 1;
                run_ids.insert(record.run_id.clone());
                if !seen_run_blocks.insert((record.run_id.clone(), record.block.number_u64)) {
                    bail!(
                        "duplicate Kuru block proof for run {} block {} at line {}",
                        record.run_id,
                        record.block.number_u64,
                        line_number
                    );
                }
                counts.canonical_blocks += u64::from(record.canonical_hash_verified_after);
                counts.implementation_consistent_blocks +=
                    u64::from(record.implementation_matches_startup == Some(true));
                head_to_decision_ms.push(checked_div(
                    Decimal::from(record.head_to_decision_ns),
                    Decimal::from(1_000_000_u64),
                    "Kuru head-to-decision milliseconds",
                )?);
                let decision_timestamp_ms = record
                    .observed_unix_ns
                    .checked_add(record.proof_duration_ns)
                    .context("Kuru decision timestamp overflow")?
                    / 1_000_000;
                counts.first_decision_timestamp_ms = Some(
                    counts
                        .first_decision_timestamp_ms
                        .map_or(decision_timestamp_ms, |value| {
                            value.min(decision_timestamp_ms)
                        }),
                );
                counts.last_decision_timestamp_ms = Some(
                    counts
                        .last_decision_timestamp_ms
                        .map_or(decision_timestamp_ms, |value| {
                            value.max(decision_timestamp_ms)
                        }),
                );
                for observation in &record.observations {
                    let buy = extract_kuru_quote(
                        Direction::KuruBuyShortPerp,
                        &observation.kuru_buy_uniswap_sell,
                        config,
                    )?;
                    let sell = extract_kuru_quote(
                        Direction::KuruSellLongPerp,
                        &observation.uniswap_buy_kuru_sell,
                        config,
                    )?;
                    for venue in ["hyperliquid", "lighter"] {
                        let samples = bbo_index.get(venue).map(Vec::as_slice).unwrap_or_default();
                        accumulate(
                            &mut results,
                            venue,
                            Direction::KuruBuyShortPerp,
                            &observation.notional_usdc,
                            buy.as_ref(),
                            &record,
                            decision_timestamp_ms,
                            samples,
                            config.max_bbo_age_ms,
                            threshold,
                        )?;
                        accumulate(
                            &mut results,
                            venue,
                            Direction::KuruSellLongPerp,
                            &observation.notional_usdc,
                            sell.as_ref(),
                            &record,
                            decision_timestamp_ms,
                            samples,
                            config.max_bbo_age_ms,
                            threshold,
                        )?;
                    }
                }
            }
            "rpc_error" => {
                let record: RunRecord = parse_record(&line, kuru_input_path, line_number)?;
                counts.rpc_error += 1;
                run_ids.insert(record.run_id);
            }
            "proof_error" => {
                let record: RunRecord = parse_record(&line, kuru_input_path, line_number)?;
                counts.proof_error += 1;
                run_ids.insert(record.run_id);
            }
            "shutdown" => {
                let record: RunRecord = parse_record(&line, kuru_input_path, line_number)?;
                counts.shutdown += 1;
                run_ids.insert(record.run_id.clone());
                shutdown_runs.insert(record.run_id);
            }
            other => bail!(
                "unknown Kuru record_type {other:?} at {} line {line_number}",
                kuru_input_path.display()
            ),
        }
    }
    if counts.startup == 0 {
        bail!(
            "Kuru capture {} has no startup record",
            kuru_input_path.display()
        );
    }
    let run_ids = run_ids.into_iter().collect::<Vec<_>>();
    let capture_complete =
        !run_ids.is_empty() && run_ids.iter().all(|run_id| shutdown_runs.contains(run_id));
    let mut result_rows = results
        .into_iter()
        .map(|((venue, direction, notional), accumulator)| {
            finish_result(venue, direction, notional, accumulator)
        })
        .collect::<Vec<_>>();
    result_rows.sort_by(|left, right| {
        left.venue
            .cmp(&right.venue)
            .then(left.direction.cmp(&right.direction))
            .then_with(|| decimal_sort(&left.notional_usdc, &right.notional_usdc))
    });

    Ok(KuruCrossVenueAnalysisReport {
        schema_version: KURU_CROSS_VENUE_ANALYSIS_SCHEMA_VERSION,
        kuru_input_path: kuru_input_path.to_path_buf(),
        bbo_input_path: bbo_input_path.to_path_buf(),
        symbol: config.symbol.clone(),
        venues: vec!["hyperliquid", "lighter"],
        max_bbo_age_ms: config.max_bbo_age_ms,
        threshold_bps: decimal_string(threshold),
        base_decimals: config.base_decimals,
        quote_decimals: config.quote_decimals,
        capture_complete,
        run_ids,
        kuru_records: counts,
        bbo_records: bbo_counts,
        kuru_head_to_decision_ms: distribution(&head_to_decision_ms),
        results: result_rows,
        interpretation: vec![
            "Basis is a gross publisher-time decision screen, not PnL or a realized arbitrage return.",
            "The Kuru quote is pinned to an earlier Monad block; head-to-decision age measures how stale that state is when the comparison becomes available.",
            "Kuru-buy/short-perp basis is CEX bid divided by Kuru effective ask minus one; Kuru-sell/long-perp basis is Kuru effective bid divided by CEX ask minus one.",
            "Only BBOs published no later than Kuru proof completion are eligible; publisher timestamps do not prove downstream consumer receipt and same-millisecond ordering is ambiguous.",
            "BBOs older than max_bbo_age_ms by publisher time are excluded; exchange age is reported separately and may be signed because clocks can differ.",
            "Top-size sufficiency only checks displayed BBO quantity and does not model queue position, slippage beyond top, or fill probability.",
            "Legacy Kuru quotes cannot prove full input consumption, so every legacy strict_screen_observations count is zero by construction.",
            "Kuru and perp legs are not atomic; fees, gas, funding, latency during submission, margin, transfer, and unwind risk are excluded.",
            "capture_complete covers the Kuru input only; protocol-v1 BBO captures have no completion trailer.",
        ],
    })
}

fn load_bbo_index(
    input_path: &Path,
    config: &KuruCrossVenueAnalysisConfig,
) -> Result<(BTreeMap<String, Vec<IndexedBbo>>, BboInputCounts)> {
    let file = File::open(input_path)
        .with_context(|| format!("open BBO capture {}", input_path.display()))?;
    let reader = BufReader::new(file);
    let mut counts = BboInputCounts::default();
    let mut streams = BTreeSet::new();
    let mut index: BTreeMap<String, Vec<IndexedBbo>> = BTreeMap::new();
    for (line_index, line) in reader.lines().enumerate() {
        let line_number = line_index + 1;
        let line =
            line.with_context(|| format!("read {} line {line_number}", input_path.display()))?;
        if line.trim().is_empty() {
            continue;
        }
        counts.total_lines += 1;
        let bbo = decode_market_data_frame(&line).with_context(|| {
            format!(
                "decode normalized BBO at {} line {line_number}",
                input_path.display()
            )
        })?;
        if bbo.symbol != config.symbol {
            counts.ignored_symbol_records += 1;
            continue;
        }
        let venue = match bbo.venue {
            Venue::Hyperliquid => "hyperliquid",
            Venue::Lighter => "lighter",
            _ => {
                counts.ignored_venue_records += 1;
                continue;
            }
        };
        if bbo.environment != MarketDataEnvironment::Live {
            bail!(
                "matching BBO at {} line {} is not live",
                input_path.display(),
                line_number
            );
        }
        counts.target_live_records += 1;
        streams.insert((venue, bbo.stream_id.clone()));
        counts.first_publisher_timestamp_ms = Some(
            counts
                .first_publisher_timestamp_ms
                .map_or(bbo.publisher_timestamp_ms, |value| {
                    value.min(bbo.publisher_timestamp_ms)
                }),
        );
        counts.last_publisher_timestamp_ms = Some(
            counts
                .last_publisher_timestamp_ms
                .map_or(bbo.publisher_timestamp_ms, |value| {
                    value.max(bbo.publisher_timestamp_ms)
                }),
        );
        index.entry(venue.to_owned()).or_default().push(IndexedBbo {
            bbo,
            ordinal: counts.total_lines,
        });
    }
    for samples in index.values_mut() {
        samples.sort_by(|left, right| {
            left.bbo
                .publisher_timestamp_ms
                .cmp(&right.bbo.publisher_timestamp_ms)
                .then(left.ordinal.cmp(&right.ordinal))
        });
    }
    counts.distinct_streams = streams.len() as u64;
    Ok((index, counts))
}

fn extract_kuru_quote(
    direction: Direction,
    route: &RouteProof,
    config: &KuruCrossVenueAnalysisConfig,
) -> Result<Option<KuruQuote>> {
    let leg = match direction {
        Direction::KuruBuyShortPerp => &route.first_leg,
        Direction::KuruSellLongPerp => &route.second_leg,
    };
    if !leg.venue.starts_with("kuru") {
        bail!(
            "{} selected a non-Kuru leg ({})",
            direction.name(),
            leg.venue
        );
    }
    let Some(output_raw) = leg.amount_out_raw.as_deref() else {
        return Ok(None);
    };
    let input_raw = leg
        .amount_in_used_raw
        .as_deref()
        .or(leg.submitted_input_raw.as_deref())
        .unwrap_or(&leg.requested_input_raw);
    let (quote_amount, base_amount) = match direction {
        Direction::KuruBuyShortPerp => (
            raw_amount(input_raw, config.quote_decimals)?,
            raw_amount(output_raw, config.base_decimals)?,
        ),
        Direction::KuruSellLongPerp => (
            raw_amount(output_raw, config.quote_decimals)?,
            raw_amount(input_raw, config.base_decimals)?,
        ),
    };
    if quote_amount <= Decimal::ZERO || base_amount <= Decimal::ZERO {
        return Ok(None);
    }
    let effective_price = checked_div(quote_amount, base_amount, "Kuru effective price")?;
    Ok(Some(KuruQuote {
        effective_price,
        hedge_quantity: base_amount,
        full_input_consumption_proven: leg.full_input_consumption_proven,
        diagnostic: leg.error.is_some(),
        same_block_composed: route.same_block_composed,
    }))
}

#[allow(clippy::too_many_arguments)]
fn accumulate(
    results: &mut BTreeMap<(String, String, String), ResultAccumulator>,
    venue: &str,
    direction: Direction,
    notional_usdc: &str,
    quote: Option<&KuruQuote>,
    block: &BlockProofRecord,
    decision_timestamp_ms: u64,
    bbo_samples: &[IndexedBbo],
    max_bbo_age_ms: u64,
    threshold: Decimal,
) -> Result<()> {
    let key = (
        venue.to_owned(),
        direction.name().to_owned(),
        notional_usdc.to_owned(),
    );
    let accumulator = results.entry(key).or_default();
    accumulator.observations += 1;
    if let Some(quote) = quote {
        accumulator.kuru_quote_available += 1;
        accumulator.canonical_kuru_quotes += u64::from(block.canonical_hash_verified_after);
        accumulator.implementation_consistent_kuru_quotes +=
            u64::from(block.implementation_matches_startup == Some(true));
        accumulator.same_block_kuru_quotes += u64::from(quote.same_block_composed);
        accumulator.full_input_consumption_proven += u64::from(quote.full_input_consumption_proven);
        accumulator.kuru_leg_diagnostics += u64::from(quote.diagnostic);
    }

    let causal_count = bbo_samples
        .partition_point(|sample| sample.bbo.publisher_timestamp_ms <= decision_timestamp_ms);
    let Some(sample) = causal_count
        .checked_sub(1)
        .and_then(|index| bbo_samples.get(index))
    else {
        accumulator.no_causal_bbo += 1;
        return Ok(());
    };
    let age_ms = decision_timestamp_ms - sample.bbo.publisher_timestamp_ms;
    if age_ms > max_bbo_age_ms {
        accumulator.stale_bbo += 1;
        return Ok(());
    }
    accumulator.fresh_bbo += 1;
    accumulator.ages_ms.push(Decimal::from(age_ms));
    accumulator.exchange_ages_ms.push(
        Decimal::from(decision_timestamp_ms)
            .checked_sub(Decimal::from(sample.bbo.exchange_timestamp_ms))
            .context("BBO exchange-age subtraction overflow")?,
    );

    let Some(quote) = quote else {
        return Ok(());
    };
    let screenable = block.canonical_hash_verified_after
        && block.implementation_matches_startup == Some(true)
        && quote.same_block_composed;
    if !screenable {
        return Ok(());
    }
    let (cex_price, cex_top_quantity) = match direction {
        Direction::KuruBuyShortPerp => (sample.bbo.bid_price, sample.bbo.bid_qty),
        Direction::KuruSellLongPerp => (sample.bbo.ask_price, sample.bbo.ask_qty),
    };
    let ratio = match direction {
        Direction::KuruBuyShortPerp => checked_div(
            cex_price,
            quote.effective_price,
            "Kuru-buy/short-perp basis ratio",
        )?,
        Direction::KuruSellLongPerp => checked_div(
            quote.effective_price,
            cex_price,
            "Kuru-sell/long-perp basis ratio",
        )?,
    };
    let basis_bps = checked_mul(
        ratio
            .checked_sub(Decimal::ONE)
            .context("basis subtraction overflow")?,
        Decimal::from(10_000_u64),
        "basis bps",
    )?;
    let sufficient = cex_top_quantity >= quote.hedge_quantity;
    accumulator.screenable_basis_observations += 1;
    accumulator.basis_bps.push(basis_bps);
    accumulator.top_size_sufficient += u64::from(sufficient);
    accumulator.top_size_insufficient += u64::from(!sufficient);
    accumulator.positive_basis += u64::from(basis_bps > Decimal::ZERO);
    accumulator.threshold_exceedances += u64::from(basis_bps >= threshold);
    if sufficient {
        accumulator.sufficient_basis_bps.push(basis_bps);
        accumulator.positive_top_size_sufficient_basis += u64::from(basis_bps > Decimal::ZERO);
        accumulator.threshold_top_size_sufficient_exceedances += u64::from(basis_bps >= threshold);
        accumulator.strict_screen_observations +=
            u64::from(quote.full_input_consumption_proven && !quote.diagnostic);
    }
    let observation = MaximumBasisObservation {
        basis_bps: decimal_string(basis_bps),
        monad_block: block.block.number_u64,
        decision_timestamp_ms,
        bbo_publisher_timestamp_ms: sample.bbo.publisher_timestamp_ms,
        bbo_age_ms: age_ms,
        displayed_top_size_sufficient: sufficient,
        full_input_consumption_proven: quote.full_input_consumption_proven,
    };
    if accumulator
        .maximum
        .as_ref()
        .is_none_or(|(current, _)| basis_bps > *current)
    {
        accumulator.maximum = Some((basis_bps, observation));
    }
    Ok(())
}

fn finish_result(
    venue: String,
    direction: String,
    notional_usdc: String,
    accumulator: ResultAccumulator,
) -> CrossVenueResult {
    CrossVenueResult {
        venue,
        direction,
        notional_usdc,
        observations: accumulator.observations,
        kuru_quote_available: accumulator.kuru_quote_available,
        canonical_kuru_quotes: accumulator.canonical_kuru_quotes,
        implementation_consistent_kuru_quotes: accumulator.implementation_consistent_kuru_quotes,
        same_block_kuru_quotes: accumulator.same_block_kuru_quotes,
        full_input_consumption_proven: accumulator.full_input_consumption_proven,
        kuru_leg_diagnostics: accumulator.kuru_leg_diagnostics,
        no_causal_bbo: accumulator.no_causal_bbo,
        stale_bbo: accumulator.stale_bbo,
        fresh_bbo: accumulator.fresh_bbo,
        screenable_basis_observations: accumulator.screenable_basis_observations,
        top_size_sufficient: accumulator.top_size_sufficient,
        top_size_insufficient: accumulator.top_size_insufficient,
        positive_basis: accumulator.positive_basis,
        threshold_exceedances: accumulator.threshold_exceedances,
        positive_top_size_sufficient_basis: accumulator.positive_top_size_sufficient_basis,
        threshold_top_size_sufficient_exceedances: accumulator
            .threshold_top_size_sufficient_exceedances,
        strict_screen_observations: accumulator.strict_screen_observations,
        bbo_age_ms: distribution(&accumulator.ages_ms),
        bbo_exchange_age_ms: distribution(&accumulator.exchange_ages_ms),
        basis_bps: distribution(&accumulator.basis_bps),
        top_size_sufficient_basis_bps: distribution(&accumulator.sufficient_basis_bps),
        maximum_basis_observation: accumulator.maximum.map(|(_, observation)| observation),
    }
}

fn distribution(values: &[Decimal]) -> Option<ExactDecimalDistribution> {
    if values.is_empty() {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort();
    let sum = sorted
        .iter()
        .try_fold(Decimal::ZERO, |sum, value| sum.checked_add(*value))?;
    Some(ExactDecimalDistribution {
        count: sorted.len() as u64,
        min: decimal_string(sorted[0]),
        mean: decimal_string(sum.checked_div(Decimal::from(sorted.len() as u64))?),
        p05: decimal_string(quantile(&sorted, 5, 100)),
        p50: decimal_string(quantile(&sorted, 50, 100)),
        p95: decimal_string(quantile(&sorted, 95, 100)),
        p99: decimal_string(quantile(&sorted, 99, 100)),
        max: decimal_string(*sorted.last().expect("non-empty")),
    })
}

fn quantile(sorted: &[Decimal], numerator: usize, denominator: usize) -> Decimal {
    let span = sorted.len() - 1;
    let index = (span * numerator + denominator / 2) / denominator;
    sorted[index]
}

fn raw_amount(value: &str, decimals: u32) -> Result<Decimal> {
    let raw = parse_decimal(value).with_context(|| format!("parse raw token amount {value:?}"))?;
    checked_div(raw, decimal_pow10(decimals)?, "scale raw token amount")
}

fn decimal_pow10(exponent: u32) -> Result<Decimal> {
    (0..exponent).try_fold(Decimal::ONE, |value, _| {
        value
            .checked_mul(Decimal::TEN)
            .context("token decimal scale overflow")
    })
}

fn checked_div(left: Decimal, right: Decimal, label: &str) -> Result<Decimal> {
    if right.is_zero() {
        bail!("{label} divides by zero");
    }
    left.checked_div(right)
        .with_context(|| format!("{label} decimal overflow"))
}

fn checked_mul(left: Decimal, right: Decimal, label: &str) -> Result<Decimal> {
    left.checked_mul(right)
        .with_context(|| format!("{label} decimal overflow"))
}

fn parse_record<T: for<'de> Deserialize<'de>>(
    line: &str,
    input_path: &Path,
    line_number: usize,
) -> Result<T> {
    serde_json::from_str(line).with_context(|| {
        format!(
            "parse record at {} line {line_number}",
            input_path.display()
        )
    })
}

fn parse_decimal(value: &str) -> Result<Decimal> {
    Decimal::from_str(value).map_err(|error| anyhow!(error))
}

fn decimal_string(value: Decimal) -> String {
    value.normalize().to_string()
}

fn decimal_sort(left: &str, right: &str) -> std::cmp::Ordering {
    let left = Decimal::from_str(left).unwrap_or(Decimal::MAX);
    let right = Decimal::from_str(right).unwrap_or(Decimal::MAX);
    left.cmp(&right)
}

/// Render a compact table. JSON output retains every validity and distribution field.
pub fn format_kuru_cross_venue_text(report: &KuruCrossVenueAnalysisReport) -> String {
    let mut output = String::new();
    output.push_str(&format!(
        "Kuru: {}\nBBO: {}\nproofs: {}, BBOs: {}, max age: {} ms, threshold: {} bps\n",
        report.kuru_input_path.display(),
        report.bbo_input_path.display(),
        report.kuru_records.block_proof,
        report.bbo_records.target_live_records,
        report.max_bbo_age_ms,
        report.threshold_bps,
    ));
    if let Some(age) = report.kuru_head_to_decision_ms.as_ref() {
        output.push_str(&format!(
            "Kuru pinned-block head-to-decision ms: p50 {}, p95 {}, p99 {}, max {}\n",
            format_decimal(&age.p50),
            format_decimal(&age.p95),
            format_decimal(&age.p99),
            format_decimal(&age.max),
        ));
    }
    for venue in &report.venues {
        if let Some(row) = report.results.iter().find(|row| row.venue == *venue) {
            if let (Some(publisher), Some(exchange)) =
                (row.bbo_age_ms.as_ref(), row.bbo_exchange_age_ms.as_ref())
            {
                output.push_str(&format!(
                    "{venue} selected BBO age ms: publisher p50 {}, p95 {}, max {}; exchange p50 {}, p95 {}, max {}\n",
                    format_decimal(&publisher.p50),
                    format_decimal(&publisher.p95),
                    format_decimal(&publisher.max),
                    format_decimal(&exchange.p50),
                    format_decimal(&exchange.p95),
                    format_decimal(&exchange.max),
                ));
            }
        }
    }
    output.push_str(
        "\nBasis columns use only canonical/same-implementation Kuru quotes, fresh BBOs, and sufficient displayed CEX top size.\n",
    );
    output.push_str("venue         direction                   size     n fresh stale no_bbo size_ok       p50       p95       max >=thr+size strict\n");
    for row in &report.results {
        let metric = row.top_size_sufficient_basis_bps.as_ref();
        let value = |selector: fn(&ExactDecimalDistribution) -> &String| -> String {
            metric
                .map(selector)
                .and_then(|value| Decimal::from_str(value).ok())
                .map_or_else(|| "-".to_owned(), |value| format!("{value:.3}"))
        };
        output.push_str(&format!(
            "{:<13} {:<27} {:>6} {:>5} {:>5} {:>5} {:>7} {:>10} {:>9} {:>9} {:>9} {:>4} {:>5}\n",
            row.venue,
            row.direction,
            row.notional_usdc,
            row.observations,
            row.fresh_bbo,
            row.stale_bbo,
            row.no_causal_bbo,
            row.top_size_sufficient,
            value(|item| &item.p50),
            value(|item| &item.p95),
            value(|item| &item.max),
            row.threshold_top_size_sufficient_exceedances,
            row.strict_screen_observations,
        ));
    }
    output.push_str(
        "\nGross publisher-time decision screens only: the Kuru state is from an earlier pinned block, publisher time is not consumer receipt, and fees/funding/gas/execution latency are excluded.\nLegacy Kuru captures have strict=0 because full input consumption cannot be proven.\n",
    );
    output
}

fn format_decimal(value: &str) -> String {
    Decimal::from_str(value).map_or_else(|_| value.to_owned(), |value| format!("{value:.3}"))
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use pretty_assertions::assert_eq;
    use serde_json::{json, Value};
    use tempfile::NamedTempFile;

    use super::*;

    fn leg(venue: &str, requested: &str, submitted: &str, output: &str, full: bool) -> Value {
        json!({
            "venue": venue,
            "requested_input_raw": requested,
            "submitted_input_raw": submitted,
            "amount_in_used_raw": null,
            "amount_out_raw": output,
            "full_input_consumption_proven": full,
            "error": null,
        })
    }

    fn proof(block: u64, decision_ms: u64) -> Value {
        json!({
            "record_type": "block_proof",
            "run_id": "run-one",
            "observed_unix_ns": decision_ms * 1_000_000 - 100,
            "proof_duration_ns": 100,
            "head_to_decision_ns": 2_000_000,
            "block": {"number_u64": block},
            "canonical_hash_verified_after": true,
            "implementation_matches_startup": true,
            "observations": [{
                "notional_usdc": "100",
                "kuru_buy_uniswap_sell": {
                    "first_leg": leg("kuru_legacy", "100000000", "100000000", "100000000000000000000", false),
                    "second_leg": leg("uniswap_v4", "1", "1", "1", true),
                    "same_block_composed": true
                },
                "uniswap_buy_kuru_sell": {
                    "first_leg": leg("uniswap_v4", "1", "1", "1", true),
                    "second_leg": leg("kuru_legacy", "100000000000000000000", "100000000000000000000", "99000000", false),
                    "same_block_composed": true
                }
            }]
        })
    }

    fn bbo(
        venue: &str,
        timestamp_ms: u64,
        bid: &str,
        bid_qty: &str,
        ask: &str,
        ask_qty: &str,
        sequence: u64,
    ) -> Value {
        bbo_with_exchange(
            venue,
            timestamp_ms,
            timestamp_ms,
            bid,
            bid_qty,
            ask,
            ask_qty,
            sequence,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn bbo_with_exchange(
        venue: &str,
        publisher_timestamp_ms: u64,
        exchange_timestamp_ms: u64,
        bid: &str,
        bid_qty: &str,
        ask: &str,
        ask_qty: &str,
        sequence: u64,
    ) -> Value {
        json!({
            "type": "bbo",
            "schema_version": 1,
            "environment": "live",
            "venue": venue,
            "symbol": "MON",
            "bid_price": bid,
            "bid_qty": bid_qty,
            "ask_price": ask,
            "ask_qty": ask_qty,
            "exchange_timestamp_ms": exchange_timestamp_ms,
            "publisher_timestamp_ms": publisher_timestamp_ms,
            "sequence": sequence,
            "stream_id": format!("{venue}-stream")
        })
    }

    fn write_lines(file: &mut NamedTempFile, values: &[Value]) {
        for value in values {
            writeln!(file, "{value}").unwrap();
        }
    }

    #[test]
    fn joins_causally_and_excludes_stale_bbo() {
        let mut kuru = NamedTempFile::new().unwrap();
        write_lines(
            &mut kuru,
            &[
                json!({"record_type":"startup", "run_id":"run-one"}),
                proof(10, 1_000),
                proof(11, 2_000),
            ],
        );
        let mut bbo_file = NamedTempFile::new().unwrap();
        write_lines(
            &mut bbo_file,
            &[
                bbo("hyperliquid", 990, "1.01", "200", "1.02", "200", 1),
                // Must not leak backward into the 1,000 ms decision.
                bbo("hyperliquid", 1_001, "2", "200", "2.1", "200", 2),
                bbo("lighter", 990, "1", "50", "1.01", "50", 1),
            ],
        );
        let report = analyze_kuru_cross_venue(
            kuru.path(),
            bbo_file.path(),
            &KuruCrossVenueAnalysisConfig::default(),
        )
        .unwrap();
        let row = report
            .results
            .iter()
            .find(|row| {
                row.venue == "hyperliquid"
                    && row.direction == "kuru_buy_short_perp"
                    && row.notional_usdc == "100"
            })
            .unwrap();
        assert_eq!(row.observations, 2);
        assert_eq!(row.fresh_bbo, 1);
        assert_eq!(row.stale_bbo, 1);
        assert_eq!(row.basis_bps.as_ref().unwrap().max, "100");
        assert_eq!(row.positive_basis, 1);
        assert_eq!(row.top_size_sufficient, 1);
        assert_eq!(row.bbo_exchange_age_ms.as_ref().unwrap().max, "10");
        assert_eq!(report.kuru_head_to_decision_ms.as_ref().unwrap().p50, "2");
        assert_eq!(
            row.maximum_basis_observation
                .as_ref()
                .unwrap()
                .bbo_publisher_timestamp_ms,
            990
        );
    }

    #[test]
    fn distinguishes_top_size_and_full_input_proof() {
        let mut kuru = NamedTempFile::new().unwrap();
        write_lines(
            &mut kuru,
            &[
                json!({"record_type":"startup", "run_id":"run-one"}),
                proof(10, 1_000),
            ],
        );
        let mut bbo_file = NamedTempFile::new().unwrap();
        write_lines(
            &mut bbo_file,
            &[
                bbo("hyperliquid", 999, "1.01", "50", "1.02", "50", 1),
                bbo("lighter", 999, "1.01", "200", "1.02", "200", 1),
            ],
        );
        let report = analyze_kuru_cross_venue(
            kuru.path(),
            bbo_file.path(),
            &KuruCrossVenueAnalysisConfig::default(),
        )
        .unwrap();
        let hyperliquid = report
            .results
            .iter()
            .find(|row| row.venue == "hyperliquid" && row.direction == "kuru_buy_short_perp")
            .unwrap();
        let lighter = report
            .results
            .iter()
            .find(|row| row.venue == "lighter" && row.direction == "kuru_buy_short_perp")
            .unwrap();
        assert_eq!(hyperliquid.top_size_insufficient, 1);
        assert_eq!(hyperliquid.positive_top_size_sufficient_basis, 0);
        assert_eq!(lighter.top_size_sufficient, 1);
        assert_eq!(lighter.positive_top_size_sufficient_basis, 1);
        // Legacy Kuru deliberately remains non-strict despite enough CEX size.
        assert_eq!(lighter.strict_screen_observations, 0);

        let text = format_kuru_cross_venue_text(&report);
        assert!(!text.contains("simultaneous"));
        let hyperliquid_line = text
            .lines()
            .find(|line| line.starts_with("hyperliquid") && line.contains("kuru_buy_short_perp"))
            .unwrap();
        let columns = hyperliquid_line.split_whitespace().collect::<Vec<_>>();
        assert_eq!(
            &columns[7..],
            &["0", "-", "-", "-", "0", "0"],
            "the primary text metrics must not use the insufficient gross sample"
        );
    }

    #[test]
    fn uses_kuru_effective_bid_for_sell_long_direction() {
        let mut kuru = NamedTempFile::new().unwrap();
        write_lines(
            &mut kuru,
            &[
                json!({"record_type":"startup", "run_id":"run-one"}),
                proof(10, 1_000),
            ],
        );
        let mut bbo_file = NamedTempFile::new().unwrap();
        write_lines(
            &mut bbo_file,
            &[
                bbo("hyperliquid", 999, "0.98", "200", "0.99", "200", 1),
                bbo("lighter", 999, "0.98", "200", "0.99", "200", 1),
            ],
        );
        let report = analyze_kuru_cross_venue(
            kuru.path(),
            bbo_file.path(),
            &KuruCrossVenueAnalysisConfig::default(),
        )
        .unwrap();
        let row = report
            .results
            .iter()
            .find(|row| row.venue == "hyperliquid" && row.direction == "kuru_sell_long_perp")
            .unwrap();
        // Kuru bid and CEX ask are both exactly 0.99.
        assert_eq!(row.basis_bps.as_ref().unwrap().max, "0");
        assert_eq!(row.positive_basis, 0);
        assert_eq!(row.threshold_exceedances, 0);
    }

    #[test]
    fn exchange_age_is_signed_and_publisher_causality_is_preserved() {
        let mut kuru = NamedTempFile::new().unwrap();
        write_lines(
            &mut kuru,
            &[
                json!({"record_type":"startup", "run_id":"run-one"}),
                proof(10, 1_000),
            ],
        );
        let mut bbo_file = NamedTempFile::new().unwrap();
        write_lines(
            &mut bbo_file,
            &[
                bbo_with_exchange("hyperliquid", 999, 1_010, "1.01", "200", "1.02", "200", 1),
                // A future publisher record must not leak into the decision.
                bbo_with_exchange("hyperliquid", 1_001, 1_001, "2", "200", "2.1", "200", 2),
                bbo("lighter", 999, "1", "200", "1.01", "200", 1),
            ],
        );
        let report = analyze_kuru_cross_venue(
            kuru.path(),
            bbo_file.path(),
            &KuruCrossVenueAnalysisConfig::default(),
        )
        .unwrap();
        let row = report
            .results
            .iter()
            .find(|row| row.venue == "hyperliquid" && row.direction == "kuru_buy_short_perp")
            .unwrap();
        assert_eq!(row.bbo_age_ms.as_ref().unwrap().p50, "1");
        assert_eq!(row.bbo_exchange_age_ms.as_ref().unwrap().p50, "-10");
        assert_eq!(row.basis_bps.as_ref().unwrap().p50, "100");
    }
}
