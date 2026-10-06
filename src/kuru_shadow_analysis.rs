//! Offline analysis for [`crate::kuru_shadow`] NDJSON captures.
//!
//! The analyzer deliberately keeps economic availability, canonicality, full-input
//! proof, and leg diagnostics separate. A legacy Kuru quote can therefore be a
//! useful screen without being mislabeled as an executable arbitrage proof.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::str::FromStr;

use anyhow::{anyhow, bail, Context, Result};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

pub const KURU_SHADOW_ANALYSIS_SCHEMA_VERSION: u16 = 1;

#[derive(Debug, Clone, Default)]
pub struct KuruShadowAnalysisConfig {
    /// Re-screen captured edges at this threshold. When absent, the capture's
    /// startup threshold is used.
    pub threshold_bps: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct KuruShadowAnalysisReport {
    pub schema_version: u16,
    pub input_path: PathBuf,
    pub run_ids: Vec<String>,
    pub detected_kuru_versions: Vec<String>,
    pub capture_complete: bool,
    pub threshold_bps: String,
    pub quantile_method: &'static str,
    pub records: RecordCounts,
    pub block_coverage: BlockCoverage,
    pub block_latency_ms: BlockLatencySummary,
    pub leg_latency_ms_by_venue: Vec<VenueLatencySummary>,
    pub routes: Vec<RouteAnalysis>,
    pub interpretation: Vec<&'static str>,
}

#[derive(Debug, Clone, Default, Serialize, PartialEq, Eq)]
pub struct RecordCounts {
    pub total_lines: u64,
    pub startup: u64,
    pub block_proof: u64,
    pub rpc_error: u64,
    pub proof_error: u64,
    pub shutdown: u64,
}

#[derive(Debug, Clone, Default, Serialize, PartialEq, Eq)]
pub struct BlockCoverage {
    pub first_block: Option<u64>,
    pub last_block: Option<u64>,
    pub block_span_inclusive: u64,
    pub unique_blocks_probed: u64,
    pub unprobed_blocks_within_span: u64,
    pub canonical_hash_verified: u64,
    pub canonical_hash_unverified: u64,
    pub implementation_matched_startup: u64,
    pub implementation_mismatch_or_unknown: u64,
}

#[derive(Debug, Clone, Default, Serialize, PartialEq)]
pub struct BlockLatencySummary {
    pub block_fetch: Option<DistributionF64>,
    pub head_age_at_probe_start: Option<DistributionF64>,
    pub head_to_decision: Option<DistributionF64>,
    pub proof_duration: Option<DistributionF64>,
    pub post_proof_verification: Option<DistributionF64>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct VenueLatencySummary {
    pub venue: String,
    pub rpc_latency: DistributionF64,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct DistributionF64 {
    pub count: u64,
    pub min: f64,
    pub mean: f64,
    pub p50: f64,
    pub p95: f64,
    pub p99: f64,
    pub max: f64,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct DecimalDistribution {
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
pub struct OpportunityPersistence {
    /// Runs split on a non-opportunity, a missing chain block, or a run change.
    pub consecutive_block_runs: u64,
    pub longest_consecutive_blocks: u64,
    pub longest_run_start_block: Option<u64>,
    pub longest_run_end_block: Option<u64>,
    /// Ignores skipped chain heights but still splits on observed non-opportunities.
    pub longest_observed_opportunity_streak: u64,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct RouteAnalysis {
    pub direction: String,
    pub notional_usdc: String,
    pub observations: u64,
    pub economic_quotes: u64,
    pub quote_errors: u64,
    pub positive_edges: u64,
    pub threshold_exceedances: u64,
    pub recorded_candidate_screens: u64,
    pub candidate_flag_mismatches: u64,
    pub canonical_quotes: u64,
    pub implementation_consistent_quotes: u64,
    pub same_block_composed_quotes: u64,
    pub full_input_consumption_proven_quotes: u64,
    pub error_free_leg_pairs: u64,
    pub leg_diagnostics: u64,
    /// Economic, canonical, implementation-consistent, same-block composed quote.
    pub research_valid_quotes: u64,
    /// Research-valid plus full-input proof and no leg diagnostic. Still neither
    /// atomic nor gas-adjusted unless the capture says otherwise.
    pub fully_valid_composed_quotes: u64,
    pub atomic_simulated_quotes: u64,
    pub gas_adjusted_quotes: u64,
    pub edge_bps: Option<DecimalDistribution>,
    pub research_valid_edge_bps: Option<DecimalDistribution>,
    pub opportunity_persistence: OpportunityPersistence,
}

#[derive(Debug, Deserialize)]
struct Envelope {
    record_type: String,
}

#[derive(Debug, Deserialize)]
struct StartupRecord {
    run_id: String,
    detected_kuru_version: String,
    candidate_threshold_bps: String,
}

#[derive(Debug, Deserialize)]
struct ErrorRecord {
    #[allow(dead_code)]
    run_id: String,
}

#[derive(Debug, Deserialize)]
struct ShutdownRecord {
    run_id: String,
}

#[derive(Debug, Deserialize)]
struct BlockProofRecord {
    run_id: String,
    block: CapturedBlock,
    block_fetch_latency_ns: u64,
    block_age_at_probe_start_ns: u64,
    head_to_decision_ns: u64,
    proof_duration_ns: u64,
    canonical_hash_verified_after: bool,
    verification_latency_ns: Option<u64>,
    implementation_matches_startup: Option<bool>,
    detected_kuru_version: String,
    observations: Vec<NotionalProof>,
}

#[derive(Debug, Deserialize)]
struct CapturedBlock {
    number_u64: u64,
}

#[derive(Debug, Deserialize)]
struct NotionalProof {
    notional_usdc: String,
    #[serde(default)]
    uniswap_buy_kuru_sell: Option<RouteProof>,
    #[serde(default)]
    kuru_buy_uniswap_sell: Option<RouteProof>,
    #[serde(default)]
    routes: Vec<RouteProof>,
}

impl NotionalProof {
    fn route_iter(&self) -> impl Iterator<Item = &RouteProof> {
        self.uniswap_buy_kuru_sell
            .iter()
            .chain(self.kuru_buy_uniswap_sell.iter())
            .chain(self.routes.iter())
    }
}

#[derive(Debug, Deserialize)]
struct RouteProof {
    direction: String,
    first_leg: LegProof,
    second_leg: LegProof,
    gross_edge_bps: Option<String>,
    candidate_screen: bool,
    full_input_consumption_proven: bool,
    same_block_composed: bool,
    atomic_simulated: bool,
    gas_adjusted: bool,
}

#[derive(Debug, Deserialize)]
struct LegProof {
    venue: String,
    amount_out_raw: Option<String>,
    rpc_latency_ns: u64,
    error: Option<String>,
}

#[derive(Debug, Clone)]
struct OpportunitySample {
    run_id: String,
    block: u64,
    qualified: bool,
}

#[derive(Debug, Default)]
struct RouteAccumulator {
    observations: u64,
    economic_quotes: u64,
    quote_errors: u64,
    positive_edges: u64,
    threshold_exceedances: u64,
    recorded_candidate_screens: u64,
    candidate_flag_mismatches: u64,
    canonical_quotes: u64,
    implementation_consistent_quotes: u64,
    same_block_composed_quotes: u64,
    full_input_consumption_proven_quotes: u64,
    error_free_leg_pairs: u64,
    leg_diagnostics: u64,
    research_valid_quotes: u64,
    fully_valid_composed_quotes: u64,
    atomic_simulated_quotes: u64,
    gas_adjusted_quotes: u64,
    edges: Vec<Decimal>,
    research_valid_edges: Vec<Decimal>,
    opportunity_samples: Vec<OpportunitySample>,
}

/// Analyze a complete or in-progress single shadow capture.
pub fn analyze_kuru_shadow_capture(
    input_path: &Path,
    config: &KuruShadowAnalysisConfig,
) -> Result<KuruShadowAnalysisReport> {
    let threshold_override = config
        .threshold_bps
        .as_deref()
        .map(parse_decimal)
        .transpose()
        .context("parse analysis threshold_bps")?;
    if threshold_override
        .is_some_and(|value| value.is_sign_negative() || value > Decimal::from(10_000_u64))
    {
        bail!("analysis threshold_bps must be between 0 and 10000");
    }

    let file = File::open(input_path)
        .with_context(|| format!("open Kuru shadow capture {}", input_path.display()))?;
    let reader = BufReader::new(file);
    let mut counts = RecordCounts::default();
    let mut run_ids = BTreeSet::new();
    let mut shutdown_run_ids = BTreeSet::new();
    let mut versions = BTreeSet::new();
    let mut capture_thresholds = BTreeSet::new();
    let mut blocks = BTreeSet::new();
    let mut seen_run_blocks = BTreeSet::new();
    let mut canonical_blocks = 0_u64;
    let mut implementation_blocks = 0_u64;
    let mut block_fetch_ns = Vec::new();
    let mut head_age_ns = Vec::new();
    let mut head_to_decision_ns = Vec::new();
    let mut proof_duration_ns = Vec::new();
    let mut verification_ns = Vec::new();
    let mut venue_latency_ns: BTreeMap<String, Vec<u64>> = BTreeMap::new();
    let mut routes: BTreeMap<(String, String), RouteAccumulator> = BTreeMap::new();

    for (line_index, line) in reader.lines().enumerate() {
        let line_number = line_index + 1;
        let line =
            line.with_context(|| format!("read {} line {line_number}", input_path.display()))?;
        if line.trim().is_empty() {
            continue;
        }
        counts.total_lines += 1;
        let envelope: Envelope = serde_json::from_str(&line).with_context(|| {
            format!(
                "parse record envelope at {} line {line_number}",
                input_path.display()
            )
        })?;
        match envelope.record_type.as_str() {
            "startup" => {
                let record: StartupRecord = parse_record(&line, input_path, line_number)?;
                counts.startup += 1;
                run_ids.insert(record.run_id);
                versions.insert(record.detected_kuru_version);
                capture_thresholds.insert(record.candidate_threshold_bps);
            }
            "block_proof" => {
                let record: BlockProofRecord = parse_record(&line, input_path, line_number)?;
                counts.block_proof += 1;
                run_ids.insert(record.run_id.clone());
                versions.insert(record.detected_kuru_version.clone());
                if !seen_run_blocks.insert((record.run_id.clone(), record.block.number_u64)) {
                    bail!(
                        "duplicate block proof for run {} block {} at {} line {}",
                        record.run_id,
                        record.block.number_u64,
                        input_path.display(),
                        line_number
                    );
                }
                blocks.insert(record.block.number_u64);
                canonical_blocks += u64::from(record.canonical_hash_verified_after);
                implementation_blocks +=
                    u64::from(record.implementation_matches_startup == Some(true));
                block_fetch_ns.push(record.block_fetch_latency_ns);
                head_age_ns.push(record.block_age_at_probe_start_ns);
                head_to_decision_ns.push(record.head_to_decision_ns);
                proof_duration_ns.push(record.proof_duration_ns);
                if let Some(value) = record.verification_latency_ns {
                    verification_ns.push(value);
                }

                let threshold =
                    resolve_threshold(threshold_override, &capture_thresholds, input_path)?;
                for observation in &record.observations {
                    for route in observation.route_iter() {
                        accumulate_route(
                            &mut routes,
                            &mut venue_latency_ns,
                            &record,
                            &observation.notional_usdc,
                            route,
                            threshold,
                        )?;
                    }
                }
            }
            "rpc_error" => {
                let record: ErrorRecord = parse_record(&line, input_path, line_number)?;
                counts.rpc_error += 1;
                run_ids.insert(record.run_id);
            }
            "proof_error" => {
                let record: ErrorRecord = parse_record(&line, input_path, line_number)?;
                counts.proof_error += 1;
                run_ids.insert(record.run_id);
            }
            "shutdown" => {
                let record: ShutdownRecord = parse_record(&line, input_path, line_number)?;
                counts.shutdown += 1;
                run_ids.insert(record.run_id.clone());
                shutdown_run_ids.insert(record.run_id);
            }
            other => bail!(
                "unknown record_type {other:?} at {} line {line_number}",
                input_path.display()
            ),
        }
    }

    if counts.startup == 0 {
        bail!("capture {} has no startup record", input_path.display());
    }
    let threshold = resolve_threshold(threshold_override, &capture_thresholds, input_path)?;
    let first_block = blocks.first().copied();
    let last_block = blocks.last().copied();
    let block_span = first_block
        .zip(last_block)
        .map_or(0, |(first, last)| last.saturating_sub(first) + 1);
    let unique_blocks = blocks.len() as u64;
    let block_coverage = BlockCoverage {
        first_block,
        last_block,
        block_span_inclusive: block_span,
        unique_blocks_probed: unique_blocks,
        unprobed_blocks_within_span: block_span.saturating_sub(unique_blocks),
        canonical_hash_verified: canonical_blocks,
        canonical_hash_unverified: counts.block_proof.saturating_sub(canonical_blocks),
        implementation_matched_startup: implementation_blocks,
        implementation_mismatch_or_unknown: counts
            .block_proof
            .saturating_sub(implementation_blocks),
    };
    let mut route_reports = routes
        .into_iter()
        .map(|((direction, notional_usdc), accumulator)| {
            route_report(direction, notional_usdc, accumulator)
        })
        .collect::<Vec<_>>();
    route_reports.sort_by(|left, right| {
        left.direction.cmp(&right.direction).then_with(|| {
            let left_notional = Decimal::from_str(&left.notional_usdc).unwrap_or(Decimal::MAX);
            let right_notional = Decimal::from_str(&right.notional_usdc).unwrap_or(Decimal::MAX);
            left_notional.cmp(&right_notional)
        })
    });
    let venue_reports = venue_latency_ns
        .into_iter()
        .filter_map(|(venue, values)| {
            distribution_ns_ms(&values)
                .map(|rpc_latency| VenueLatencySummary { venue, rpc_latency })
        })
        .collect();
    let run_ids = run_ids.into_iter().collect::<Vec<_>>();
    let capture_complete = !run_ids.is_empty()
        && run_ids
            .iter()
            .all(|run_id| shutdown_run_ids.contains(run_id));

    Ok(KuruShadowAnalysisReport {
        schema_version: KURU_SHADOW_ANALYSIS_SCHEMA_VERSION,
        input_path: input_path.to_path_buf(),
        run_ids,
        detected_kuru_versions: versions.into_iter().collect(),
        capture_complete,
        threshold_bps: decimal_string(threshold),
        quantile_method: "nearest index: round((n - 1) * q)",
        records: counts,
        block_coverage,
        block_latency_ms: BlockLatencySummary {
            block_fetch: distribution_ns_ms(&block_fetch_ns),
            head_age_at_probe_start: distribution_ns_ms(&head_age_ns),
            head_to_decision: distribution_ns_ms(&head_to_decision_ns),
            proof_duration: distribution_ns_ms(&proof_duration_ns),
            post_proof_verification: distribution_ns_ms(&verification_ns),
        },
        leg_latency_ms_by_venue: venue_reports,
        routes: route_reports,
        interpretation: vec![
            "Positive and threshold counts are gross screens before gas and execution costs.",
            "Research-valid means economic, canonical, implementation-consistent, and same-block composed; it does not mean atomic.",
            "Fully-valid composed additionally requires proven full input consumption and no leg diagnostic; it still does not prove atomic execution.",
            "Consecutive-block runs require adjacent Monad block heights, so skipped capture blocks break a run.",
        ],
    })
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

fn resolve_threshold(
    override_value: Option<Decimal>,
    captured: &BTreeSet<String>,
    input_path: &Path,
) -> Result<Decimal> {
    if let Some(value) = override_value {
        return Ok(value);
    }
    if captured.len() != 1 {
        bail!(
            "capture {} must contain exactly one candidate threshold (found {:?}); pass an override",
            input_path.display(),
            captured
        );
    }
    parse_decimal(captured.first().expect("checked one captured threshold"))
        .context("parse capture candidate_threshold_bps")
}

fn accumulate_route(
    routes: &mut BTreeMap<(String, String), RouteAccumulator>,
    venue_latency_ns: &mut BTreeMap<String, Vec<u64>>,
    block: &BlockProofRecord,
    notional_usdc: &str,
    route: &RouteProof,
    threshold: Decimal,
) -> Result<()> {
    let key = (route.direction.clone(), notional_usdc.to_owned());
    let accumulator = routes.entry(key).or_default();
    accumulator.observations += 1;
    venue_latency_ns
        .entry(route.first_leg.venue.clone())
        .or_default()
        .push(route.first_leg.rpc_latency_ns);
    venue_latency_ns
        .entry(route.second_leg.venue.clone())
        .or_default()
        .push(route.second_leg.rpc_latency_ns);

    let edge = route
        .gross_edge_bps
        .as_deref()
        .map(parse_decimal)
        .transpose()
        .with_context(|| {
            format!(
                "parse gross_edge_bps for {} ${} at block {}",
                route.direction, notional_usdc, block.block.number_u64
            )
        })?;
    let output_complete = route.first_leg.amount_out_raw.is_some()
        && route.second_leg.amount_out_raw.is_some()
        && edge.is_some();
    let diagnostic_free = route.first_leg.error.is_none() && route.second_leg.error.is_none();
    let implementation_consistent = block.implementation_matches_startup == Some(true);
    let research_valid = output_complete
        && block.canonical_hash_verified_after
        && implementation_consistent
        && route.same_block_composed;
    let threshold_exceeded = edge.is_some_and(|value| value >= threshold);
    let qualified = research_valid && threshold_exceeded;

    accumulator.economic_quotes += u64::from(edge.is_some());
    accumulator.quote_errors += u64::from(!output_complete);
    accumulator.positive_edges += u64::from(edge.is_some_and(|value| value > Decimal::ZERO));
    accumulator.threshold_exceedances += u64::from(threshold_exceeded);
    accumulator.recorded_candidate_screens += u64::from(route.candidate_screen);
    accumulator.candidate_flag_mismatches +=
        u64::from(route.candidate_screen != threshold_exceeded);
    accumulator.canonical_quotes += u64::from(block.canonical_hash_verified_after);
    accumulator.implementation_consistent_quotes += u64::from(implementation_consistent);
    accumulator.same_block_composed_quotes += u64::from(route.same_block_composed);
    accumulator.full_input_consumption_proven_quotes +=
        u64::from(route.full_input_consumption_proven);
    accumulator.error_free_leg_pairs += u64::from(diagnostic_free);
    accumulator.leg_diagnostics += u64::from(!diagnostic_free);
    accumulator.research_valid_quotes += u64::from(research_valid);
    accumulator.fully_valid_composed_quotes +=
        u64::from(research_valid && route.full_input_consumption_proven && diagnostic_free);
    accumulator.atomic_simulated_quotes += u64::from(route.atomic_simulated);
    accumulator.gas_adjusted_quotes += u64::from(route.gas_adjusted);
    if let Some(value) = edge {
        accumulator.edges.push(value);
        if research_valid {
            accumulator.research_valid_edges.push(value);
        }
    }
    accumulator.opportunity_samples.push(OpportunitySample {
        run_id: block.run_id.clone(),
        block: block.block.number_u64,
        qualified,
    });
    Ok(())
}

fn route_report(
    direction: String,
    notional_usdc: String,
    accumulator: RouteAccumulator,
) -> RouteAnalysis {
    RouteAnalysis {
        direction,
        notional_usdc,
        observations: accumulator.observations,
        economic_quotes: accumulator.economic_quotes,
        quote_errors: accumulator.quote_errors,
        positive_edges: accumulator.positive_edges,
        threshold_exceedances: accumulator.threshold_exceedances,
        recorded_candidate_screens: accumulator.recorded_candidate_screens,
        candidate_flag_mismatches: accumulator.candidate_flag_mismatches,
        canonical_quotes: accumulator.canonical_quotes,
        implementation_consistent_quotes: accumulator.implementation_consistent_quotes,
        same_block_composed_quotes: accumulator.same_block_composed_quotes,
        full_input_consumption_proven_quotes: accumulator.full_input_consumption_proven_quotes,
        error_free_leg_pairs: accumulator.error_free_leg_pairs,
        leg_diagnostics: accumulator.leg_diagnostics,
        research_valid_quotes: accumulator.research_valid_quotes,
        fully_valid_composed_quotes: accumulator.fully_valid_composed_quotes,
        atomic_simulated_quotes: accumulator.atomic_simulated_quotes,
        gas_adjusted_quotes: accumulator.gas_adjusted_quotes,
        edge_bps: decimal_distribution(&accumulator.edges),
        research_valid_edge_bps: decimal_distribution(&accumulator.research_valid_edges),
        opportunity_persistence: opportunity_persistence(&accumulator.opportunity_samples),
    }
}

fn opportunity_persistence(samples: &[OpportunitySample]) -> OpportunityPersistence {
    let mut samples = samples.to_vec();
    samples.sort_by(|left, right| {
        left.run_id
            .cmp(&right.run_id)
            .then(left.block.cmp(&right.block))
    });
    let mut runs = 0_u64;
    let mut current_consecutive = 0_u64;
    let mut current_start = None;
    let mut longest_consecutive = 0_u64;
    let mut longest_start = None;
    let mut longest_end = None;
    let mut current_observed = 0_u64;
    let mut longest_observed = 0_u64;
    let mut previous: Option<&OpportunitySample> = None;

    for sample in &samples {
        let same_run = previous.is_some_and(|prior| prior.run_id == sample.run_id);
        if !same_run || !sample.qualified {
            current_observed = 0;
        }
        if sample.qualified {
            current_observed += 1;
            longest_observed = longest_observed.max(current_observed);
        }

        let extends = sample.qualified
            && previous.is_some_and(|prior| {
                prior.qualified
                    && prior.run_id == sample.run_id
                    && prior.block.checked_add(1) == Some(sample.block)
            });
        if sample.qualified {
            if extends {
                current_consecutive += 1;
            } else {
                runs += 1;
                current_consecutive = 1;
                current_start = Some(sample.block);
            }
            if current_consecutive > longest_consecutive {
                longest_consecutive = current_consecutive;
                longest_start = current_start;
                longest_end = Some(sample.block);
            }
        } else {
            current_consecutive = 0;
            current_start = None;
        }
        previous = Some(sample);
    }
    OpportunityPersistence {
        consecutive_block_runs: runs,
        longest_consecutive_blocks: longest_consecutive,
        longest_run_start_block: longest_start,
        longest_run_end_block: longest_end,
        longest_observed_opportunity_streak: longest_observed,
    }
}

fn decimal_distribution(values: &[Decimal]) -> Option<DecimalDistribution> {
    if values.is_empty() {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort();
    let sum: Decimal = sorted.iter().copied().sum();
    Some(DecimalDistribution {
        count: sorted.len() as u64,
        min: decimal_string(sorted[0]),
        mean: decimal_string(sum / Decimal::from(sorted.len() as u64)),
        p05: decimal_string(decimal_quantile(&sorted, 5, 100)),
        p50: decimal_string(decimal_quantile(&sorted, 50, 100)),
        p95: decimal_string(decimal_quantile(&sorted, 95, 100)),
        p99: decimal_string(decimal_quantile(&sorted, 99, 100)),
        max: decimal_string(*sorted.last().expect("non-empty")),
    })
}

fn decimal_quantile(sorted: &[Decimal], numerator: usize, denominator: usize) -> Decimal {
    let span = sorted.len() - 1;
    let rounded_index = (span * numerator + denominator / 2) / denominator;
    sorted[rounded_index]
}

fn distribution_ns_ms(values: &[u64]) -> Option<DistributionF64> {
    if values.is_empty() {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let to_ms = |value: u64| value as f64 / 1_000_000.0;
    let sum = sorted.iter().map(|value| *value as u128).sum::<u128>();
    Some(DistributionF64 {
        count: sorted.len() as u64,
        min: to_ms(sorted[0]),
        mean: sum as f64 / sorted.len() as f64 / 1_000_000.0,
        p50: to_ms(integer_quantile(&sorted, 50, 100)),
        p95: to_ms(integer_quantile(&sorted, 95, 100)),
        p99: to_ms(integer_quantile(&sorted, 99, 100)),
        max: to_ms(*sorted.last().expect("non-empty")),
    })
}

fn integer_quantile(sorted: &[u64], numerator: usize, denominator: usize) -> u64 {
    let span = sorted.len() - 1;
    let rounded_index = (span * numerator + denominator / 2) / denominator;
    sorted[rounded_index]
}

fn parse_decimal(value: &str) -> Result<Decimal> {
    Decimal::from_str(value).map_err(|error| anyhow!(error))
}

fn decimal_string(value: Decimal) -> String {
    value.normalize().to_string()
}

/// Produce a compact terminal report while retaining JSON as the machine-readable form.
pub fn format_kuru_shadow_analysis_text(report: &KuruShadowAnalysisReport) -> String {
    let mut output = String::new();
    output.push_str(&format!(
        "capture: {} ({})\nblocks: {} probed / {} span, {} missing, {} canonical\nthreshold: {} bps\n",
        report.input_path.display(),
        if report.capture_complete {
            "complete"
        } else {
            "in progress or incomplete"
        },
        report.block_coverage.unique_blocks_probed,
        report.block_coverage.block_span_inclusive,
        report.block_coverage.unprobed_blocks_within_span,
        report.block_coverage.canonical_hash_verified,
        report.threshold_bps,
    ));
    output.push_str("\ndirection                     size     n      mean       p05       p50       p95       max   pos   >=thr  full  run\n");
    for route in &report.routes {
        let distribution = route.research_valid_edge_bps.as_ref();
        let value = |selector: fn(&DecimalDistribution) -> &String| -> String {
            distribution
                .map(selector)
                .and_then(|item| item.parse::<f64>().ok())
                .map_or_else(|| "-".to_owned(), |item| format!("{item:.3}"))
        };
        output.push_str(&format!(
            "{:<29} {:>7} {:>5} {:>9} {:>9} {:>9} {:>9} {:>9} {:>5} {:>7} {:>5} {:>4}\n",
            route.direction,
            route.notional_usdc,
            route.observations,
            value(|item| &item.mean),
            value(|item| &item.p05),
            value(|item| &item.p50),
            value(|item| &item.p95),
            value(|item| &item.max),
            route.positive_edges,
            route.threshold_exceedances,
            route.fully_valid_composed_quotes,
            route.opportunity_persistence.longest_consecutive_blocks,
        ));
    }
    if let Some(latency) = &report.block_latency_ms.head_to_decision {
        output.push_str(&format!(
            "\nhead-to-decision ms: p50 {:.2}, p95 {:.2}, p99 {:.2}, max {:.2}\n",
            latency.p50, latency.p95, latency.p99, latency.max
        ));
    }
    output.push_str(&format!(
        "record errors: rpc {}, proof {}; every edge is gross and non-atomic\n",
        report.records.rpc_error, report.records.proof_error
    ));
    output
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use pretty_assertions::assert_eq;
    use serde_json::json;
    use tempfile::NamedTempFile;

    use super::*;

    fn leg(venue: &str, latency_ns: u64, error: Option<&str>) -> serde_json::Value {
        json!({
            "venue": venue,
            "amount_out_raw": "100",
            "rpc_latency_ns": latency_ns,
            "error": error,
        })
    }

    fn route(direction: &str, edge: &str, candidate: bool, full: bool) -> serde_json::Value {
        json!({
            "direction": direction,
            "first_leg": leg("kuru_legacy", 1_000_000, None),
            "second_leg": leg("uniswap_v4", 2_000_000, None),
            "gross_edge_bps": edge,
            "candidate_screen": candidate,
            "full_input_consumption_proven": full,
            "same_block_composed": true,
            "atomic_simulated": false,
            "gas_adjusted": false,
        })
    }

    fn block(run_id: &str, number: u64, edge: &str, candidate: bool) -> serde_json::Value {
        json!({
            "record_type": "block_proof",
            "run_id": run_id,
            "block": {"number_u64": number},
            "block_fetch_latency_ns": 1_000_000,
            "block_age_at_probe_start_ns": 2_000_000,
            "head_to_decision_ns": 10_000_000,
            "proof_duration_ns": 3_000_000,
            "canonical_hash_verified_after": true,
            "verification_latency_ns": 4_000_000,
            "implementation_matches_startup": true,
            "detected_kuru_version": "legacy",
            "observations": [{
                "notional_usdc": "100",
                "uniswap_buy_kuru_sell": route("uniswap_buy_kuru_sell", edge, candidate, false),
                "kuru_buy_uniswap_sell": route("kuru_buy_uniswap_sell", "-2", false, false),
            }]
        })
    }

    #[test]
    fn analyzes_edges_validity_latency_and_consecutive_runs() {
        let run_id = "run-one";
        let mut capture = NamedTempFile::new().unwrap();
        let records = [
            json!({
                "record_type": "startup",
                "run_id": run_id,
                "detected_kuru_version": "legacy",
                "candidate_threshold_bps": "5"
            }),
            block(run_id, 10, "6", true),
            block(run_id, 11, "8", true),
            block(run_id, 13, "7", true),
            block(run_id, 14, "-1", false),
            json!({"record_type": "shutdown", "run_id": run_id}),
        ];
        for record in records {
            writeln!(capture, "{}", serde_json::to_string(&record).unwrap()).unwrap();
        }

        let report =
            analyze_kuru_shadow_capture(capture.path(), &KuruShadowAnalysisConfig::default())
                .unwrap();
        assert!(report.capture_complete);
        assert_eq!(report.block_coverage.unique_blocks_probed, 4);
        assert_eq!(report.block_coverage.block_span_inclusive, 5);
        assert_eq!(report.block_coverage.unprobed_blocks_within_span, 1);
        let route = report
            .routes
            .iter()
            .find(|item| item.direction == "uniswap_buy_kuru_sell")
            .unwrap();
        assert_eq!(route.observations, 4);
        assert_eq!(route.positive_edges, 3);
        assert_eq!(route.threshold_exceedances, 3);
        assert_eq!(route.research_valid_quotes, 4);
        assert_eq!(route.fully_valid_composed_quotes, 0);
        assert_eq!(route.edge_bps.as_ref().unwrap().mean, "5");
        assert_eq!(route.edge_bps.as_ref().unwrap().p50, "7");
        assert_eq!(
            route.opportunity_persistence,
            OpportunityPersistence {
                consecutive_block_runs: 2,
                longest_consecutive_blocks: 2,
                longest_run_start_block: Some(10),
                longest_run_end_block: Some(11),
                longest_observed_opportunity_streak: 3,
            }
        );
        assert_eq!(report.block_latency_ms.head_to_decision.unwrap().p95, 10.0);
    }

    #[test]
    fn threshold_override_rescreens_and_detects_recorded_flag_mismatch() {
        let run_id = "run-one";
        let mut capture = NamedTempFile::new().unwrap();
        writeln!(
            capture,
            "{}",
            json!({
                "record_type": "startup",
                "run_id": run_id,
                "detected_kuru_version": "legacy",
                "candidate_threshold_bps": "5"
            })
        )
        .unwrap();
        writeln!(capture, "{}", block(run_id, 10, "6", true)).unwrap();

        let report = analyze_kuru_shadow_capture(
            capture.path(),
            &KuruShadowAnalysisConfig {
                threshold_bps: Some("10".to_owned()),
            },
        )
        .unwrap();
        let route = report
            .routes
            .iter()
            .find(|item| item.direction == "uniswap_buy_kuru_sell")
            .unwrap();
        assert!(!report.capture_complete);
        assert_eq!(route.threshold_exceedances, 0);
        assert_eq!(route.recorded_candidate_screens, 1);
        assert_eq!(route.candidate_flag_mismatches, 1);
        assert_eq!(route.opportunity_persistence.longest_consecutive_blocks, 0);
    }

    #[test]
    fn rejects_duplicate_run_block_records() {
        let run_id = "run-one";
        let mut capture = NamedTempFile::new().unwrap();
        writeln!(
            capture,
            "{}",
            json!({
                "record_type": "startup",
                "run_id": run_id,
                "detected_kuru_version": "legacy",
                "candidate_threshold_bps": "5"
            })
        )
        .unwrap();
        writeln!(capture, "{}", block(run_id, 10, "6", true)).unwrap();
        writeln!(capture, "{}", block(run_id, 10, "6", true)).unwrap();

        let error =
            analyze_kuru_shadow_capture(capture.path(), &KuruShadowAnalysisConfig::default())
                .unwrap_err();
        assert!(error.to_string().contains("duplicate block proof"));
    }

    #[test]
    fn normalizes_v2_crystal_routes_without_breaking_v1_records() {
        let run_id = "crystal-run";
        let mut capture = NamedTempFile::new().unwrap();
        writeln!(
            capture,
            "{}",
            json!({
                "schema_version": 2,
                "record_type": "startup",
                "run_id": run_id,
                "detected_kuru_version": "v2",
                "candidate_threshold_bps": "5"
            })
        )
        .unwrap();
        writeln!(
            capture,
            "{}",
            json!({
                "schema_version": 2,
                "record_type": "block_proof",
                "run_id": run_id,
                "block": {"number_u64": 22},
                "block_fetch_latency_ns": 1_000_000,
                "block_age_at_probe_start_ns": 2_000_000,
                "head_to_decision_ns": 10_000_000,
                "proof_duration_ns": 3_000_000,
                "canonical_hash_verified_after": true,
                "verification_latency_ns": 4_000_000,
                "implementation_matches_startup": true,
                "detected_kuru_version": "v2",
                "counterparty": "crystal",
                "observations": [{
                    "notional_usdc": "100",
                    "routes": [
                        {
                            "direction": "crystal_buy_kuru_sell",
                            "first_leg": leg("crystal", 1_500_000, None),
                            "second_leg": leg("kuru_v2", 2_500_000, None),
                            "gross_edge_bps": "7",
                            "candidate_screen": true,
                            "full_input_consumption_proven": true,
                            "same_block_composed": true,
                            "atomic_simulated": false,
                            "gas_adjusted": false,
                            "inter_leg_conversion": "wmon_to_native_mon_1_to_1_raw_units_unwrap_call_not_simulated"
                        },
                        {
                            "direction": "kuru_buy_crystal_sell",
                            "first_leg": leg("kuru_v2", 2_500_000, None),
                            "second_leg": leg("crystal", 1_500_000, None),
                            "gross_edge_bps": "-3",
                            "candidate_screen": false,
                            "full_input_consumption_proven": true,
                            "same_block_composed": true,
                            "atomic_simulated": false,
                            "gas_adjusted": false,
                            "inter_leg_conversion": "native_mon_to_wmon_1_to_1_raw_units_wrap_call_not_simulated"
                        }
                    ]
                }]
            })
        )
        .unwrap();
        writeln!(
            capture,
            "{}",
            json!({"schema_version":2,"record_type":"shutdown","run_id":run_id})
        )
        .unwrap();

        let report =
            analyze_kuru_shadow_capture(capture.path(), &KuruShadowAnalysisConfig::default())
                .unwrap();
        assert!(report.capture_complete);
        assert_eq!(report.routes.len(), 2);
        let route = report
            .routes
            .iter()
            .find(|item| item.direction == "crystal_buy_kuru_sell")
            .unwrap();
        assert_eq!(route.threshold_exceedances, 1);
        assert_eq!(route.fully_valid_composed_quotes, 1);
        assert!(report
            .leg_latency_ms_by_venue
            .iter()
            .any(|item| item.venue == "crystal"));
    }
}
