//! Live-mainnet, non-broadcasting Kuru/counterparty same-block arbitrage prover.

use std::collections::BTreeSet;
use std::fs::{self, OpenOptions};
use std::io::{BufWriter, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, bail, Context, Result};
use ethers::types::{Address, U256};
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::time::{self, MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::kuru_chain::{self, KuruMarketParams, USDC_ADDRESS};
use crate::kuru_v2::{self, KuruV2Identity};
use crate::monad_crystal::{self, CrystalMarketIdentity};
use crate::monad_rpc::{self, MonadBlockRef};

pub const KURU_SHADOW_CONFIG_SCHEMA_VERSION: u16 = 1;
pub const KURU_SHADOW_RECORD_SCHEMA_VERSION_V1: u16 = 1;
pub const KURU_SHADOW_RECORD_SCHEMA_VERSION_V2: u16 = 2;
/// Retained for callers that used the original config-version constant.
pub const KURU_SHADOW_SCHEMA_VERSION: u16 = KURU_SHADOW_CONFIG_SCHEMA_VERSION;
const MONAD_CHAIN_ID: u64 = 143;

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ConfiguredKuruVersion {
    Auto,
    Legacy,
    V2,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CounterpartyVenue {
    #[default]
    UniswapV4,
    Crystal,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KuruShadowConfig {
    pub schema_version: u16,
    pub rpc_http_url: String,
    pub kuru_market: String,
    pub kuru_version: ConfiguredKuruVersion,
    #[serde(default)]
    pub counterparty: CounterpartyVenue,
    #[serde(default)]
    pub crystal_market: Option<String>,
    pub output_path: PathBuf,
    pub notionals_usdc: Vec<String>,
    pub duration_seconds: u64,
    pub block_poll_interval_ms: u64,
    pub request_timeout_ms: u64,
    pub candidate_threshold_bps: String,
    pub estimate_legacy_kuru_gas: bool,
    pub metrics_bind: Option<SocketAddr>,
}

impl KuruShadowConfig {
    pub fn from_path(path: &Path) -> Result<Self> {
        let contents = fs::read_to_string(path)
            .with_context(|| format!("read Kuru shadow config {}", path.display()))?;
        let config: Self = toml::from_str(&contents)
            .with_context(|| format!("parse Kuru shadow config {}", path.display()))?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<()> {
        if self.schema_version != KURU_SHADOW_CONFIG_SCHEMA_VERSION {
            bail!(
                "unsupported Kuru shadow config schema {}, expected {}",
                self.schema_version,
                KURU_SHADOW_CONFIG_SCHEMA_VERSION
            );
        }
        if self.rpc_http_url.trim().is_empty() {
            bail!("rpc_http_url must not be empty");
        }
        Address::from_str(&self.kuru_market).context("invalid Kuru market address")?;
        match self.counterparty {
            CounterpartyVenue::UniswapV4 => {}
            CounterpartyVenue::Crystal => {
                let market = self.crystal_market.as_deref().ok_or_else(|| {
                    anyhow!("crystal_market is required for counterparty=crystal")
                })?;
                Address::from_str(market).context("invalid Crystal market address")?;
            }
        }
        if self
            .output_path
            .extension()
            .and_then(|extension| extension.to_str())
            != Some("ndjson")
        {
            bail!("output_path must end in .ndjson");
        }
        if self.notionals_usdc.is_empty() || self.notionals_usdc.len() > 8 {
            bail!("notionals_usdc must contain between one and eight values");
        }
        let notionals = self.parsed_notionals()?;
        let unique: BTreeSet<_> = notionals.iter().map(|notional| notional.raw).collect();
        if unique.len() != notionals.len() {
            bail!("notionals_usdc contains duplicate values");
        }
        if self.duration_seconds == 0 || self.duration_seconds > 24 * 60 * 60 {
            bail!("duration_seconds must be between 1 and 86400");
        }
        if !(25..=5_000).contains(&self.block_poll_interval_ms) {
            bail!("block_poll_interval_ms must be between 25 and 5000");
        }
        if !(250..=60_000).contains(&self.request_timeout_ms) {
            bail!("request_timeout_ms must be between 250 and 60000");
        }
        let threshold = Decimal::from_str(&self.candidate_threshold_bps)
            .context("candidate_threshold_bps must be a decimal string")?;
        if threshold.is_sign_negative() || threshold > Decimal::from(10_000_u32) {
            bail!("candidate_threshold_bps must be between 0 and 10000");
        }
        Ok(())
    }

    fn parsed_notionals(&self) -> Result<Vec<Notional>> {
        self.notionals_usdc
            .iter()
            .map(|value| Notional::parse(value))
            .collect()
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct KuruShadowSummary {
    pub schema_version: u16,
    pub run_id: String,
    pub output_path: PathBuf,
    pub started_unix_ns: u64,
    pub ended_unix_ns: u64,
    pub termination_reason: String,
    pub blocks_observed: u64,
    pub blocks_probed: u64,
    pub blocks_skipped: u64,
    pub proof_errors: u64,
    pub candidate_screens: u64,
}

#[derive(Debug, Clone, Copy)]
struct Notional {
    raw: u128,
}

impl Notional {
    fn parse(value: &str) -> Result<Self> {
        let decimal =
            Decimal::from_str(value).with_context(|| format!("invalid USDC notional {value:?}"))?;
        if decimal <= Decimal::ZERO {
            bail!("USDC notional must be positive: {value}");
        }
        let raw_decimal = decimal * Decimal::from(1_000_000_u64);
        if !raw_decimal.fract().is_zero() {
            bail!("USDC notional has more than six decimal places: {value}");
        }
        let raw = raw_decimal
            .to_u128()
            .ok_or_else(|| anyhow!("USDC notional is too large: {value}"))?;
        if U256::from(raw).bits() > 96 {
            bail!("USDC notional does not fit Kuru legacy uint96: {value}");
        }
        Ok(Self { raw })
    }

    fn display(&self) -> String {
        let value =
            Decimal::from_u128(self.raw).expect("u128 fits Decimal") / Decimal::from(1_000_000_u64);
        value.normalize().to_string()
    }
}

#[derive(Debug, Clone)]
enum KuruRuntime {
    Legacy(KuruMarketParams),
    V2(KuruV2Identity),
}

#[derive(Debug, Clone)]
enum CounterpartyRuntime {
    UniswapV4,
    Crystal {
        identity: Box<CrystalMarketIdentity>,
        singleton_code_hash: String,
        market_code_hash: String,
    },
}

impl CounterpartyRuntime {
    fn venue(&self) -> &'static str {
        match self {
            Self::UniswapV4 => "uniswap_v4",
            Self::Crystal { .. } => "crystal",
        }
    }

    fn target(&self) -> &str {
        match self {
            Self::UniswapV4 => crate::monad_v4::QUOTER_ADDRESS,
            Self::Crystal { identity, .. } => &identity.singleton,
        }
    }

    fn record_schema_version(&self) -> u16 {
        match self {
            Self::UniswapV4 => KURU_SHADOW_RECORD_SCHEMA_VERSION_V1,
            Self::Crystal { .. } => KURU_SHADOW_RECORD_SCHEMA_VERSION_V2,
        }
    }

    fn identity_json(&self) -> Value {
        match self {
            Self::UniswapV4 => json!({
                "venue":"uniswap_v4",
                "quoter":crate::monad_v4::QUOTER_ADDRESS,
                "pool_manager":crate::monad_v4::POOL_MANAGER_ADDRESS,
                "pool_key":{
                    "currency0":crate::monad_v4::NATIVE_MON_ADDRESS,
                    "currency1":crate::monad_v4::USDC_ADDRESS,
                    "fee":crate::monad_v4::POOL_FEE,
                    "tick_spacing":crate::monad_v4::POOL_TICK_SPACING,
                    "hooks":crate::monad_v4::POOL_HOOKS_ADDRESS,
                }
            }),
            Self::Crystal {
                identity,
                singleton_code_hash,
                market_code_hash,
            } => json!({
                "venue":"crystal",
                "market_identity":identity,
                "singleton_code_hash":singleton_code_hash,
                "market_code_hash":market_code_hash,
            }),
        }
    }
}

impl KuruRuntime {
    fn name(&self) -> &'static str {
        match self {
            Self::Legacy(_) => "legacy",
            Self::V2(_) => "v2",
        }
    }

    fn identity_json(&self) -> Value {
        match self {
            Self::Legacy(params) => json!(params),
            Self::V2(identity) => json!(identity),
        }
    }
}

#[derive(Debug, Serialize)]
struct StartupRecord {
    schema_version: u16,
    record_type: &'static str,
    run_id: String,
    observed_unix_ns: u64,
    proof_kind: &'static str,
    atomic_simulated: bool,
    chain_id: u64,
    block: MonadBlockRef,
    kuru_market: String,
    configured_kuru_version: ConfiguredKuruVersion,
    detected_kuru_version: String,
    kuru_identity: Value,
    counterparty: &'static str,
    counterparty_identity: Value,
    proxy_code_hash: String,
    proxy_implementation: Option<String>,
    implementation_code_hash: Option<String>,
    notionals_usdc: Vec<String>,
    candidate_threshold_bps: String,
    caveats: Vec<&'static str>,
}

#[derive(Debug, Serialize)]
struct BlockProofRecord {
    schema_version: u16,
    record_type: &'static str,
    run_id: String,
    sequence: u64,
    observed_unix_ns: u64,
    proof_kind: &'static str,
    atomic_simulated: bool,
    block: MonadBlockRef,
    block_fetch_latency_ns: u64,
    block_age_at_probe_start_ns: u64,
    head_to_decision_ns: u64,
    proof_duration_ns: u64,
    canonical_hash_verified_after: bool,
    verification_latency_ns: Option<u64>,
    proxy_implementation: Option<String>,
    implementation_matches_startup: Option<bool>,
    detected_kuru_version: &'static str,
    counterparty: &'static str,
    observations: Vec<NotionalProof>,
}

#[derive(Debug, Serialize)]
struct ErrorRecord {
    schema_version: u16,
    record_type: &'static str,
    run_id: String,
    sequence: u64,
    observed_unix_ns: u64,
    block: Option<MonadBlockRef>,
    error: String,
}

#[derive(Debug, Serialize)]
struct ShutdownRecord<'a> {
    record_type: &'static str,
    #[serde(flatten)]
    summary: &'a KuruShadowSummary,
}

#[derive(Debug, thiserror::Error)]
#[error(
    "Kuru proxy implementation changed within the capture: startup={startup}, current={current}"
)]
struct KuruImplementationChanged {
    startup: String,
    current: String,
}

#[derive(Debug, Serialize)]
struct NotionalProof {
    notional_usdc: String,
    notional_usdc_raw: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    uniswap_buy_kuru_sell: Option<RouteProof>,
    #[serde(skip_serializing_if = "Option::is_none")]
    kuru_buy_uniswap_sell: Option<RouteProof>,
    #[serde(skip_serializing_if = "Option::is_none")]
    routes: Option<Vec<RouteProof>>,
    round_one_duration_ns: u64,
    round_two_duration_ns: u64,
}

impl NotionalProof {
    fn route_iter(&self) -> impl Iterator<Item = &RouteProof> {
        self.uniswap_buy_kuru_sell
            .iter()
            .chain(self.kuru_buy_uniswap_sell.iter())
            .chain(self.routes.iter().flatten())
    }
}

#[derive(Debug, Serialize)]
struct RouteProof {
    direction: &'static str,
    first_leg: LegProof,
    second_leg: LegProof,
    final_usdc_raw: Option<String>,
    gross_pnl_usdc_raw: Option<String>,
    gross_pnl_usdc: Option<String>,
    gross_edge_bps: Option<String>,
    candidate_screen: bool,
    full_input_consumption_proven: bool,
    same_block_composed: bool,
    atomic_simulated: bool,
    gas_adjusted: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    inter_leg_conversion: Option<&'static str>,
}

#[derive(Debug, Clone, Serialize)]
struct LegProof {
    venue: &'static str,
    operation: &'static str,
    target: String,
    requested_input_raw: String,
    submitted_input_raw: Option<String>,
    contract_input_units: Option<String>,
    amount_in_used_raw: Option<String>,
    residual_input_raw: Option<String>,
    amount_out_raw: Option<String>,
    full_input_consumption_proven: bool,
    block_reference: Value,
    calldata: Option<String>,
    rpc_latency_ns: u64,
    gas_estimate: Option<String>,
    gas_estimate_kind: Option<&'static str>,
    error: Option<String>,
}

impl LegProof {
    fn error(
        venue: &'static str,
        operation: &'static str,
        target: &str,
        requested_input: U256,
        block_reference: &Value,
        latency_ns: u64,
        error: anyhow::Error,
    ) -> Self {
        Self {
            venue,
            operation,
            target: target.to_owned(),
            requested_input_raw: requested_input.to_string(),
            submitted_input_raw: None,
            contract_input_units: None,
            amount_in_used_raw: None,
            residual_input_raw: None,
            amount_out_raw: None,
            full_input_consumption_proven: false,
            block_reference: block_reference.clone(),
            calldata: None,
            rpc_latency_ns: latency_ns,
            gas_estimate: None,
            gas_estimate_kind: None,
            error: Some(format!("{error:#}")),
        }
    }

    fn output(&self) -> Option<U256> {
        self.amount_out_raw
            .as_deref()
            .and_then(|value| U256::from_dec_str(value).ok())
    }
}

pub async fn run_kuru_shadow(
    mut config: KuruShadowConfig,
    output_override: Option<PathBuf>,
    once: bool,
    cancellation: CancellationToken,
) -> Result<KuruShadowSummary> {
    if let Some(output) = output_override {
        config.output_path = output;
    }
    config.validate()?;
    if let Some(parent) = config.output_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create shadow output directory {}", parent.display()))?;
    }
    let file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&config.output_path)
        .with_context(|| format!("create {}", config.output_path.display()))?;
    let mut writer = BufWriter::with_capacity(1024 * 1024, file);
    let client = reqwest::Client::builder()
        .user_agent("ala-crate-kuru-shadow/0.1")
        .timeout(Duration::from_millis(config.request_timeout_ms))
        .build()
        .context("build Monad shadow RPC client")?;
    let run_id = format!("kuru-shadow-{}", Uuid::new_v4());
    let started_unix_ns = unix_timestamp_ns();
    let started = Instant::now();
    let notionals = config.parsed_notionals()?;
    let candidate_threshold =
        Decimal::from_str(&config.candidate_threshold_bps).context("parse candidate threshold")?;

    let chain_id = monad_rpc::chain_id(&client, &config.rpc_http_url).await?;
    if chain_id != MONAD_CHAIN_ID {
        bail!("RPC chain ID is {chain_id}, expected Monad mainnet {MONAD_CHAIN_ID}");
    }
    let (startup_block, _) = monad_rpc::latest_block(&client, &config.rpc_http_url).await?;
    let startup_reference = startup_block.eip1898_parameter();
    let runtime = detect_kuru_runtime(&client, &config, &startup_reference).await?;
    let counterparty = initialize_counterparty(&client, &config, &startup_reference).await?;
    let record_schema_version = counterparty.record_schema_version();
    let (proxy_code_hash, proxy_implementation, implementation_code_hash) =
        contract_identity(&client, &config, &startup_reference).await?;

    let startup = StartupRecord {
        schema_version: record_schema_version,
        record_type: "startup",
        run_id: run_id.clone(),
        observed_unix_ns: unix_timestamp_ns(),
        proof_kind: "same_block_composed_quotes",
        atomic_simulated: false,
        chain_id,
        block: startup_block,
        kuru_market: config.kuru_market.clone(),
        configured_kuru_version: config.kuru_version,
        detected_kuru_version: runtime.name().to_owned(),
        kuru_identity: runtime.identity_json(),
        counterparty: counterparty.venue(),
        counterparty_identity: counterparty.identity_json(),
        proxy_code_hash,
        proxy_implementation: proxy_implementation.clone(),
        implementation_code_hash,
        notionals_usdc: notionals.iter().map(Notional::display).collect(),
        candidate_threshold_bps: candidate_threshold.normalize().to_string(),
        caveats: startup_caveats(&counterparty),
    };
    write_json_line(&mut writer, &startup)?;
    writer.flush().context("flush Kuru shadow startup record")?;

    let mut interval = time::interval(Duration::from_millis(config.block_poll_interval_ms));
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let deadline = started + Duration::from_secs(config.duration_seconds);
    let mut last_block: Option<(u64, String)> = None;
    let mut sequence = 0_u64;
    let mut blocks_observed = 0_u64;
    let mut blocks_probed = 0_u64;
    let mut blocks_skipped = 0_u64;
    let mut proof_errors = 0_u64;
    let mut candidate_screens = 0_u64;
    let termination_reason;

    loop {
        if !once && Instant::now() >= deadline {
            termination_reason = "completed_duration";
            break;
        }
        tokio::select! {
            _ = cancellation.cancelled() => {
                termination_reason = "cancelled";
                break;
            }
            _ = interval.tick() => {}
        }

        let fetched = monad_rpc::latest_block(&client, &config.rpc_http_url).await;
        let (block, block_fetch_latency_ns) = match fetched {
            Ok(value) => value,
            Err(error) => {
                proof_errors += 1;
                sequence += 1;
                write_json_line(
                    &mut writer,
                    &ErrorRecord {
                        schema_version: record_schema_version,
                        record_type: "rpc_error",
                        run_id: run_id.clone(),
                        sequence,
                        observed_unix_ns: unix_timestamp_ns(),
                        block: None,
                        error: format!("fetch latest block: {error:#}"),
                    },
                )?;
                writer.flush().context("flush block-fetch error")?;
                metrics::counter!("arb_rpc_failures_total", "method" => "eth_getBlockByNumber")
                    .increment(1);
                if once {
                    return Err(error).context("fetch latest block for one-shot proof");
                }
                continue;
            }
        };
        blocks_observed += 1;
        metrics::counter!("arb_heads_observed_total").increment(1);
        metrics::histogram!("arb_rpc_call_seconds", "venue" => "monad", "method" => "eth_getBlockByNumber")
            .record(block_fetch_latency_ns as f64 / 1_000_000_000.0);

        if last_block
            .as_ref()
            .is_some_and(|(number, hash)| *number == block.number_u64 && *hash == block.hash)
        {
            if once {
                unreachable!("one-shot has not yet recorded a block");
            }
            continue;
        }
        if let Some((previous, _)) = &last_block {
            if block.number_u64 > previous.saturating_add(1) {
                let skipped = block.number_u64 - previous - 1;
                blocks_skipped = blocks_skipped.saturating_add(skipped);
                metrics::counter!("arb_heads_coalesced_total").increment(skipped);
            }
        }
        last_block = Some((block.number_u64, block.hash.clone()));
        sequence += 1;

        match probe_block(
            &client,
            &config,
            &runtime,
            &counterparty,
            &block,
            block_fetch_latency_ns,
            &notionals,
            candidate_threshold,
            &run_id,
            sequence,
            proxy_implementation.as_deref(),
        )
        .await
        {
            Ok(record) => {
                candidate_screens += record
                    .observations
                    .iter()
                    .flat_map(NotionalProof::route_iter)
                    .filter(|route| route.candidate_screen)
                    .count() as u64;
                write_json_line(&mut writer, &record)?;
                writer.flush().context("flush Kuru shadow block proof")?;
                blocks_probed += 1;
                metrics::counter!("arb_blocks_probed_total").increment(1);
            }
            Err(error) => {
                let implementation_changed =
                    error.downcast_ref::<KuruImplementationChanged>().is_some();
                proof_errors += 1;
                metrics::counter!("arb_proof_failures_total").increment(1);
                write_json_line(
                    &mut writer,
                    &ErrorRecord {
                        schema_version: record_schema_version,
                        record_type: "proof_error",
                        run_id: run_id.clone(),
                        sequence,
                        observed_unix_ns: unix_timestamp_ns(),
                        block: Some(block),
                        error: format!("{error:#}"),
                    },
                )?;
                writer.flush().context("flush Kuru shadow proof error")?;
                if once {
                    return Err(error).context("run one-shot block proof");
                }
                if implementation_changed {
                    termination_reason = "kuru_implementation_changed";
                    break;
                }
            }
        }
        if once {
            termination_reason = "completed_one_shot";
            break;
        }
    }

    let summary = KuruShadowSummary {
        schema_version: record_schema_version,
        run_id,
        output_path: config.output_path,
        started_unix_ns,
        ended_unix_ns: unix_timestamp_ns(),
        termination_reason: termination_reason.to_owned(),
        blocks_observed,
        blocks_probed,
        blocks_skipped,
        proof_errors,
        candidate_screens,
    };
    write_json_line(
        &mut writer,
        &ShutdownRecord {
            record_type: "shutdown",
            summary: &summary,
        },
    )?;
    writer.flush().context("flush Kuru shadow output")?;
    writer
        .get_ref()
        .sync_all()
        .context("sync Kuru shadow output")?;
    Ok(summary)
}

#[allow(clippy::too_many_arguments)]
async fn probe_block(
    client: &reqwest::Client,
    config: &KuruShadowConfig,
    runtime: &KuruRuntime,
    counterparty: &CounterpartyRuntime,
    block: &MonadBlockRef,
    block_fetch_latency_ns: u64,
    notionals: &[Notional],
    candidate_threshold: Decimal,
    run_id: &str,
    sequence: u64,
    startup_implementation: Option<&str>,
) -> Result<BlockProofRecord> {
    let proof_started = Instant::now();
    let observed_unix_ns = unix_timestamp_ns();
    let block_age_at_probe_start_ns =
        observed_unix_ns.saturating_sub(block.timestamp_unix_s.saturating_mul(1_000_000_000));
    metrics::histogram!("arb_head_age_at_probe_start_seconds")
        .record(block_age_at_probe_start_ns as f64 / 1_000_000_000.0);
    let block_reference = block.eip1898_parameter();

    let proofs = futures_util::future::join_all(notionals.iter().copied().map(|notional| {
        probe_notional(
            client,
            config,
            runtime,
            counterparty,
            &block_reference,
            notional,
            candidate_threshold,
        )
    }))
    .await;
    let observations = proofs.into_iter().collect::<Result<Vec<_>>>()?;

    let verification_started = Instant::now();
    let (verification, implementation) = tokio::join!(
        monad_rpc::block_by_number(client, &config.rpc_http_url, &block.number),
        monad_rpc::eip1967_implementation_at(
            client,
            &config.rpc_http_url,
            &config.kuru_market,
            &block_reference,
        )
    );
    let verification_latency_ns = duration_ns(verification_started.elapsed());
    let (verified_block, _) = verification.context("verify quoted block after proof")?;
    let canonical_hash_verified_after = verified_block.hash == block.hash;
    if !canonical_hash_verified_after {
        metrics::counter!("arb_hash_mismatches_total").increment(1);
    }
    let implementation = implementation.ok().and_then(|(value, _)| value);
    let implementation_matches_startup = match (startup_implementation, implementation.as_deref()) {
        (Some(startup), Some(current)) => Some(startup.eq_ignore_ascii_case(current)),
        (None, None) => Some(true),
        _ => None,
    };
    if let (Some(startup), Some(current), Some(false)) = (
        startup_implementation,
        implementation.as_deref(),
        implementation_matches_startup,
    ) {
        metrics::counter!("arb_kuru_implementation_changes_total").increment(1);
        return Err(KuruImplementationChanged {
            startup: startup.to_owned(),
            current: current.to_owned(),
        }
        .into());
    }

    let proof_duration_ns = duration_ns(proof_started.elapsed());
    metrics::histogram!("arb_block_proof_seconds")
        .record(proof_duration_ns as f64 / 1_000_000_000.0);
    let head_to_decision_ns =
        unix_timestamp_ns().saturating_sub(block.timestamp_unix_s.saturating_mul(1_000_000_000));
    metrics::histogram!("arb_head_to_decision_seconds")
        .record(head_to_decision_ns as f64 / 1_000_000_000.0);
    metrics::gauge!("arb_last_completed_block").set(block.number_u64 as f64);

    Ok(BlockProofRecord {
        schema_version: counterparty.record_schema_version(),
        record_type: "block_proof",
        run_id: run_id.to_owned(),
        sequence,
        observed_unix_ns,
        proof_kind: "same_block_composed_quotes",
        atomic_simulated: false,
        block: block.clone(),
        block_fetch_latency_ns,
        block_age_at_probe_start_ns,
        head_to_decision_ns,
        proof_duration_ns,
        canonical_hash_verified_after,
        verification_latency_ns: Some(verification_latency_ns),
        proxy_implementation: implementation,
        implementation_matches_startup,
        detected_kuru_version: runtime.name(),
        counterparty: counterparty.venue(),
        observations,
    })
}

async fn probe_notional(
    client: &reqwest::Client,
    config: &KuruShadowConfig,
    runtime: &KuruRuntime,
    counterparty: &CounterpartyRuntime,
    block_reference: &Value,
    notional: Notional,
    candidate_threshold: Decimal,
) -> Result<NotionalProof> {
    let usdc_input = U256::from(notional.raw);
    let round_one_started = Instant::now();
    let (kuru_buy, counterparty_buy) = tokio::join!(
        quote_kuru(client, config, runtime, true, usdc_input, block_reference,),
        quote_counterparty(
            client,
            config,
            counterparty,
            true,
            usdc_input,
            block_reference,
        )
    );
    let round_one_duration_ns = duration_ns(round_one_started.elapsed());
    let kuru_buy = leg_result_or_error(
        kuru_buy,
        "kuru",
        "buy_mon",
        &config.kuru_market,
        usdc_input,
        block_reference,
    );
    let counterparty_buy = leg_result_or_error(
        counterparty_buy,
        counterparty.venue(),
        counterparty_operation(counterparty, true),
        counterparty.target(),
        usdc_input,
        block_reference,
    );

    let round_two_started = Instant::now();
    let kuru_sell_future = async {
        match second_leg_input(&counterparty_buy) {
            Some(mon_input) => {
                quote_kuru(client, config, runtime, false, mon_input, block_reference).await
            }
            None => Err(anyhow!(
                "{} buy leg produced no MON/WMON output",
                counterparty.venue()
            )),
        }
    };
    let counterparty_sell_future = async {
        match second_leg_input(&kuru_buy) {
            Some(mon_input) => {
                quote_counterparty(
                    client,
                    config,
                    counterparty,
                    false,
                    mon_input,
                    block_reference,
                )
                .await
            }
            None => Err(anyhow!("Kuru buy leg produced no MON output")),
        }
    };
    let (kuru_sell, counterparty_sell) = tokio::join!(kuru_sell_future, counterparty_sell_future);
    let round_two_duration_ns = duration_ns(round_two_started.elapsed());
    let kuru_sell_input = counterparty_buy.output().unwrap_or_default();
    let counterparty_sell_input = kuru_buy.output().unwrap_or_default();
    let kuru_sell = leg_result_or_error(
        kuru_sell,
        "kuru",
        "sell_mon",
        &config.kuru_market,
        kuru_sell_input,
        block_reference,
    );
    let counterparty_sell = leg_result_or_error(
        counterparty_sell,
        counterparty.venue(),
        counterparty_operation(counterparty, false),
        counterparty.target(),
        counterparty_sell_input,
        block_reference,
    );

    let counterparty_to_kuru = route_proof(
        counterparty_to_kuru_direction(counterparty),
        counterparty_buy,
        kuru_sell,
        usdc_input,
        candidate_threshold,
        &notional.display(),
        counterparty_to_kuru_conversion(counterparty),
    );
    let kuru_to_counterparty = route_proof(
        kuru_to_counterparty_direction(counterparty),
        kuru_buy,
        counterparty_sell,
        usdc_input,
        candidate_threshold,
        &notional.display(),
        kuru_to_counterparty_conversion(counterparty),
    );
    let (uniswap_buy_kuru_sell, kuru_buy_uniswap_sell, routes) = match counterparty {
        CounterpartyRuntime::UniswapV4 => {
            (Some(counterparty_to_kuru), Some(kuru_to_counterparty), None)
        }
        CounterpartyRuntime::Crystal { .. } => (
            None,
            None,
            Some(vec![counterparty_to_kuru, kuru_to_counterparty]),
        ),
    };
    Ok(NotionalProof {
        notional_usdc: notional.display(),
        notional_usdc_raw: notional.raw.to_string(),
        uniswap_buy_kuru_sell,
        kuru_buy_uniswap_sell,
        routes,
        round_one_duration_ns,
        round_two_duration_ns,
    })
}

fn second_leg_input(first_leg: &LegProof) -> Option<U256> {
    first_leg.output()
}

fn counterparty_operation(counterparty: &CounterpartyRuntime, is_buy: bool) -> &'static str {
    match (counterparty, is_buy) {
        (CounterpartyRuntime::UniswapV4, true) => "buy_mon",
        (CounterpartyRuntime::UniswapV4, false) => "sell_mon",
        (CounterpartyRuntime::Crystal { .. }, true) => "buy_wmon",
        (CounterpartyRuntime::Crystal { .. }, false) => "sell_wmon",
    }
}

fn counterparty_to_kuru_direction(counterparty: &CounterpartyRuntime) -> &'static str {
    match counterparty {
        CounterpartyRuntime::UniswapV4 => "uniswap_buy_kuru_sell",
        CounterpartyRuntime::Crystal { .. } => "crystal_buy_kuru_sell",
    }
}

fn kuru_to_counterparty_direction(counterparty: &CounterpartyRuntime) -> &'static str {
    match counterparty {
        CounterpartyRuntime::UniswapV4 => "kuru_buy_uniswap_sell",
        CounterpartyRuntime::Crystal { .. } => "kuru_buy_crystal_sell",
    }
}

fn counterparty_to_kuru_conversion(counterparty: &CounterpartyRuntime) -> Option<&'static str> {
    match counterparty {
        CounterpartyRuntime::UniswapV4 => None,
        CounterpartyRuntime::Crystal { .. } => {
            Some("wmon_to_native_mon_1_to_1_raw_units_unwrap_call_not_simulated")
        }
    }
}

fn kuru_to_counterparty_conversion(counterparty: &CounterpartyRuntime) -> Option<&'static str> {
    match counterparty {
        CounterpartyRuntime::UniswapV4 => None,
        CounterpartyRuntime::Crystal { .. } => {
            Some("native_mon_to_wmon_1_to_1_raw_units_wrap_call_not_simulated")
        }
    }
}

fn leg_result_or_error(
    result: Result<LegProof>,
    venue: &'static str,
    operation: &'static str,
    target: &str,
    requested_input: U256,
    block_reference: &Value,
) -> LegProof {
    match result {
        Ok(leg) => leg,
        Err(error) => {
            metrics::counter!("arb_rpc_failures_total", "method" => "eth_call", "venue" => venue)
                .increment(1);
            LegProof::error(
                venue,
                operation,
                target,
                requested_input,
                block_reference,
                0,
                error,
            )
        }
    }
}

fn route_proof(
    direction: &'static str,
    first_leg: LegProof,
    second_leg: LegProof,
    initial_usdc: U256,
    candidate_threshold: Decimal,
    notional_label: &str,
    inter_leg_conversion: Option<&'static str>,
) -> RouteProof {
    let final_usdc = second_leg.output();
    let economics = final_usdc.and_then(|output| route_economics(output, initial_usdc).ok());
    let (pnl_raw, pnl_usdc, edge_bps, candidate_screen) = match economics {
        Some((pnl_raw, pnl_usdc, edge_bps)) => {
            let edge = Decimal::from_str(&edge_bps).unwrap_or(Decimal::MIN);
            let candidate = edge >= candidate_threshold;
            metrics::gauge!(
                "arb_latest_gross_edge_bps",
                "direction" => direction,
                "notional_usdc" => notional_label.to_owned()
            )
            .set(edge.to_f64().unwrap_or(f64::NAN));
            if candidate {
                metrics::counter!(
                    "arb_candidate_screens_total",
                    "direction" => direction,
                    "notional_usdc" => notional_label.to_owned()
                )
                .increment(1);
            }
            (Some(pnl_raw), Some(pnl_usdc), Some(edge_bps), candidate)
        }
        None => (None, None, None, false),
    };
    let full_input_consumption_proven =
        first_leg.full_input_consumption_proven && second_leg.full_input_consumption_proven;
    RouteProof {
        direction,
        first_leg,
        second_leg,
        final_usdc_raw: final_usdc.map(|value| value.to_string()),
        gross_pnl_usdc_raw: pnl_raw,
        gross_pnl_usdc: pnl_usdc,
        gross_edge_bps: edge_bps,
        candidate_screen,
        full_input_consumption_proven,
        same_block_composed: true,
        atomic_simulated: false,
        gas_adjusted: false,
        inter_leg_conversion,
    }
}

fn route_economics(output: U256, input: U256) -> Result<(String, String, String)> {
    let output = u256_to_u128(output, "route USDC output")?;
    let input = u256_to_u128(input, "route USDC input")?;
    if input == 0 {
        bail!("route input is zero");
    }
    let signed_raw = if output >= input {
        (output - input).to_string()
    } else {
        format!("-{}", input - output)
    };
    let output_decimal = Decimal::from_u128(output).context("USDC output exceeds Decimal")?;
    let input_decimal = Decimal::from_u128(input).context("USDC input exceeds Decimal")?;
    let pnl_raw_decimal = output_decimal - input_decimal;
    let pnl_usdc = (pnl_raw_decimal / Decimal::from(1_000_000_u64))
        .normalize()
        .to_string();
    let edge_bps = (pnl_raw_decimal * Decimal::from(10_000_u64) / input_decimal)
        .normalize()
        .to_string();
    Ok((signed_raw, pnl_usdc, edge_bps))
}

async fn quote_counterparty(
    client: &reqwest::Client,
    config: &KuruShadowConfig,
    counterparty: &CounterpartyRuntime,
    is_buy: bool,
    amount_in: U256,
    block_reference: &Value,
) -> Result<LegProof> {
    match counterparty {
        CounterpartyRuntime::UniswapV4 => {
            quote_uniswap(
                client,
                config,
                !is_buy,
                amount_in,
                counterparty_operation(counterparty, is_buy),
                block_reference,
            )
            .await
        }
        CounterpartyRuntime::Crystal { identity, .. } => {
            let quote = monad_crystal::quote_exact_input_at(
                client,
                &config.rpc_http_url,
                &identity.market,
                is_buy,
                amount_in,
                block_reference,
            )
            .await?;
            metrics::histogram!(
                "arb_rpc_call_seconds",
                "venue" => "crystal",
                "method" => "eth_call"
            )
            .record(quote.quote_latency_ns as f64 / 1_000_000_000.0);
            Ok(LegProof {
                venue: "crystal",
                operation: counterparty_operation(counterparty, is_buy),
                target: quote.singleton,
                requested_input_raw: quote.requested_input_raw.clone(),
                submitted_input_raw: Some(quote.requested_input_raw),
                contract_input_units: Some(amount_in.to_string()),
                amount_in_used_raw: Some(quote.amount_in_used_raw),
                residual_input_raw: Some(quote.residual_input_raw),
                amount_out_raw: Some(quote.amount_out_raw),
                full_input_consumption_proven: quote.full_input_consumed,
                block_reference: quote.block_reference,
                calldata: Some(quote.calldata),
                rpc_latency_ns: quote.quote_latency_ns,
                gas_estimate: None,
                gas_estimate_kind: None,
                error: None,
            })
        }
    }
}

async fn quote_uniswap(
    client: &reqwest::Client,
    config: &KuruShadowConfig,
    zero_for_one: bool,
    amount_in: U256,
    operation: &'static str,
    block_reference: &Value,
) -> Result<LegProof> {
    let amount_u128 = u256_to_u128(amount_in, "Uniswap amount")?;
    let started = Instant::now();
    let quote = crate::monad_v4::quote_exact_input_single_at(
        client,
        &config.rpc_http_url,
        zero_for_one,
        amount_u128,
        block_reference,
    )
    .await;
    let latency_ns = duration_ns(started.elapsed());
    metrics::histogram!("arb_rpc_call_seconds", "venue" => "uniswap_v4", "method" => "eth_call")
        .record(latency_ns as f64 / 1_000_000_000.0);
    let quote = quote?;
    if quote.get("ok").and_then(Value::as_bool) != Some(true) {
        bail!("Uniswap v4 quote failed: {quote}");
    }
    let amount_out = quote
        .get("amount_out")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("Uniswap quote has no amount_out"))?;
    U256::from_dec_str(amount_out).context("invalid Uniswap amount_out")?;
    let calldata = quote
        .pointer("/request_params/0/data")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let gas_estimate = quote
        .get("gas_estimate")
        .and_then(Value::as_str)
        .map(str::to_owned);
    Ok(LegProof {
        venue: "uniswap_v4",
        operation,
        target: crate::monad_v4::QUOTER_ADDRESS.to_owned(),
        requested_input_raw: amount_in.to_string(),
        submitted_input_raw: Some(amount_in.to_string()),
        contract_input_units: Some(amount_in.to_string()),
        amount_in_used_raw: Some(amount_in.to_string()),
        residual_input_raw: Some("0".to_owned()),
        amount_out_raw: Some(amount_out.to_owned()),
        full_input_consumption_proven: true,
        block_reference: block_reference.clone(),
        calldata,
        rpc_latency_ns: latency_ns,
        gas_estimate,
        gas_estimate_kind: Some("v4_quoter_swap_core"),
        error: None,
    })
}

async fn quote_kuru(
    client: &reqwest::Client,
    config: &KuruShadowConfig,
    runtime: &KuruRuntime,
    is_buy: bool,
    amount_in_raw: U256,
    block_reference: &Value,
) -> Result<LegProof> {
    let started = Instant::now();
    let result = match runtime {
        KuruRuntime::Legacy(params) => {
            let (contract_units, submitted_raw, residual_raw) = if is_buy {
                (
                    params.quote_raw_to_price_units(amount_in_raw)?,
                    amount_in_raw,
                    U256::zero(),
                )
            } else {
                let (units, dust) = params.base_raw_to_size_units(amount_in_raw)?;
                (units, amount_in_raw.saturating_sub(dust), dust)
            };
            let quote = if is_buy {
                kuru_chain::quote_market_buy(
                    client,
                    &config.rpc_http_url,
                    &config.kuru_market,
                    contract_units,
                    block_reference,
                    config.estimate_legacy_kuru_gas,
                )
                .await?
            } else {
                kuru_chain::quote_market_sell(
                    client,
                    &config.rpc_http_url,
                    &config.kuru_market,
                    contract_units,
                    block_reference,
                    config.estimate_legacy_kuru_gas,
                )
                .await?
            };
            LegProof {
                venue: "kuru_legacy",
                operation: if is_buy { "buy_mon" } else { "sell_mon" },
                target: config.kuru_market.clone(),
                requested_input_raw: amount_in_raw.to_string(),
                submitted_input_raw: Some(submitted_raw.to_string()),
                contract_input_units: Some(contract_units.to_string()),
                amount_in_used_raw: None,
                residual_input_raw: Some(residual_raw.to_string()),
                amount_out_raw: Some(quote.amount_out_raw),
                full_input_consumption_proven: false,
                block_reference: quote.block_reference,
                calldata: Some(quote.calldata),
                rpc_latency_ns: quote.quote_latency_ns,
                gas_estimate: quote.estimator_gas,
                gas_estimate_kind: Some("zero_sender_estimator_lower_bound"),
                error: quote
                    .estimator_gas_error
                    .map(|error| format!("price quote succeeded; gas lower-bound failed: {error}")),
            }
        }
        KuruRuntime::V2(_) => {
            let quote = kuru_v2::estimate_swap(
                client,
                &config.rpc_http_url,
                &config.kuru_market,
                is_buy,
                amount_in_raw,
                block_reference,
            )
            .await?;
            let used = U256::from_dec_str(&quote.amount_in_used_raw)
                .context("invalid Kuru V2 amountInUsed")?;
            LegProof {
                venue: "kuru_v2",
                operation: if is_buy { "buy_mon" } else { "sell_mon" },
                target: config.kuru_market.clone(),
                requested_input_raw: amount_in_raw.to_string(),
                submitted_input_raw: Some(amount_in_raw.to_string()),
                contract_input_units: Some(amount_in_raw.to_string()),
                amount_in_used_raw: Some(quote.amount_in_used_raw),
                residual_input_raw: Some(amount_in_raw.saturating_sub(used).to_string()),
                amount_out_raw: Some(quote.amount_out_raw),
                full_input_consumption_proven: quote.full_input_consumed,
                block_reference: quote.block_reference,
                calldata: Some(quote.calldata),
                rpc_latency_ns: quote.quote_latency_ns,
                gas_estimate: None,
                gas_estimate_kind: None,
                error: None,
            }
        }
    };
    let latency_ns = duration_ns(started.elapsed());
    metrics::histogram!("arb_rpc_call_seconds", "venue" => runtime.name(), "method" => "eth_call")
        .record(latency_ns as f64 / 1_000_000_000.0);
    Ok(result)
}

async fn initialize_counterparty(
    client: &reqwest::Client,
    config: &KuruShadowConfig,
    block_reference: &Value,
) -> Result<CounterpartyRuntime> {
    match config.counterparty {
        CounterpartyVenue::UniswapV4 => Ok(CounterpartyRuntime::UniswapV4),
        CounterpartyVenue::Crystal => {
            let market = config
                .crystal_market
                .as_deref()
                .ok_or_else(|| anyhow!("crystal_market is required for counterparty=crystal"))?;
            let identity = monad_crystal::identify_market(
                client,
                &config.rpc_http_url,
                market,
                block_reference,
            )
            .await
            .context("identify configured Crystal MON/USDC market")?;
            let (singleton_code, singleton_code_hash, _) = monad_rpc::code_at(
                client,
                &config.rpc_http_url,
                monad_crystal::CRYSTAL_SINGLETON_ADDRESS,
                block_reference,
            )
            .await
            .context("read Crystal singleton bytecode")?;
            if singleton_code == "0x" {
                bail!("Crystal singleton has no bytecode at the pinned startup block");
            }
            let (market_code, market_code_hash, _) =
                monad_rpc::code_at(client, &config.rpc_http_url, market, block_reference)
                    .await
                    .context("read Crystal market bytecode")?;
            if market_code == "0x" {
                bail!("Crystal market has no bytecode at the pinned startup block");
            }
            Ok(CounterpartyRuntime::Crystal {
                identity: Box::new(identity),
                singleton_code_hash,
                market_code_hash,
            })
        }
    }
}

fn startup_caveats(counterparty: &CounterpartyRuntime) -> Vec<&'static str> {
    let mut caveats = vec![
        "This process never signs or broadcasts a transaction.",
        "Each route composes two quotes against one canonical block hash; it is not a combined atomic transaction simulation.",
        "Legacy Kuru zero-sender quotes return before fill-or-kill and minimum-output checks, so full input consumption is not proven.",
        "Kuru V2 estimateSwap reports amountInUsed and can prove full input consumption when a V2 market is configured.",
        "Gas-adjusted profit remains unknown until a purpose-built executor is simulated with real transfer and approval context.",
    ];
    if matches!(counterparty, CounterpartyRuntime::Crystal { .. }) {
        caveats.extend([
            "Crystal getQuote uses price-level shortcuts and its own source warns that it is not directly equivalent to actual trade execution.",
            "Crystal trades WMON while Kuru trades native MON; the proof assumes 1:1 raw-unit wrap/unwrap but does not simulate those calls or their gas.",
            "Crystal complete-fill mode is not accepted as proof by itself; returned amountIn must equal the requested exact input.",
        ]);
    }
    caveats
}

async fn detect_kuru_runtime(
    client: &reqwest::Client,
    config: &KuruShadowConfig,
    block_reference: &Value,
) -> Result<KuruRuntime> {
    match config.kuru_version {
        ConfiguredKuruVersion::V2 => {
            let identity = kuru_v2::identify_market(
                client,
                &config.rpc_http_url,
                &config.kuru_market,
                block_reference,
            )
            .await
            .context("configured Kuru V2 market does not expose the V2 interface")?;
            validate_v2_identity(&identity)?;
            Ok(KuruRuntime::V2(identity))
        }
        ConfiguredKuruVersion::Legacy => {
            let params = kuru_chain::market_params(
                client,
                &config.rpc_http_url,
                &config.kuru_market,
                block_reference,
            )
            .await?;
            params.validate_mon_usdc()?;
            Ok(KuruRuntime::Legacy(params))
        }
        ConfiguredKuruVersion::Auto => {
            match kuru_v2::identify_market(
                client,
                &config.rpc_http_url,
                &config.kuru_market,
                block_reference,
            )
            .await
            {
                Ok(identity) => {
                    validate_v2_identity(&identity)?;
                    Ok(KuruRuntime::V2(identity))
                }
                Err(v2_error) => {
                    let params = kuru_chain::market_params(
                        client,
                        &config.rpc_http_url,
                        &config.kuru_market,
                        block_reference,
                    )
                    .await
                    .with_context(|| {
                        format!("market matched neither Kuru V2 nor legacy; V2 probe: {v2_error:#}")
                    })?;
                    params.validate_mon_usdc()?;
                    Ok(KuruRuntime::Legacy(params))
                }
            }
        }
    }
}

fn validate_v2_identity(identity: &KuruV2Identity) -> Result<()> {
    if !identity
        .base_token
        .eq_ignore_ascii_case(kuru_chain::ZERO_ADDRESS)
    {
        bail!(
            "Kuru V2 market base token is {}, expected native MON",
            identity.base_token
        );
    }
    if !identity.quote_token.eq_ignore_ascii_case(USDC_ADDRESS) {
        bail!(
            "Kuru V2 market quote token is {}, expected Monad USDC",
            identity.quote_token
        );
    }
    Ok(())
}

async fn contract_identity(
    client: &reqwest::Client,
    config: &KuruShadowConfig,
    block_reference: &Value,
) -> Result<(String, Option<String>, Option<String>)> {
    let (_, proxy_code_hash, _) = monad_rpc::code_at(
        client,
        &config.rpc_http_url,
        &config.kuru_market,
        block_reference,
    )
    .await?;
    let (implementation, _) = monad_rpc::eip1967_implementation_at(
        client,
        &config.rpc_http_url,
        &config.kuru_market,
        block_reference,
    )
    .await?;
    let implementation_code_hash = match implementation.as_deref() {
        Some(address) => Some(
            monad_rpc::code_at(client, &config.rpc_http_url, address, block_reference)
                .await?
                .1,
        ),
        None => None,
    };
    Ok((proxy_code_hash, implementation, implementation_code_hash))
}

fn write_json_line<T: Serialize>(writer: &mut BufWriter<std::fs::File>, value: &T) -> Result<()> {
    serde_json::to_writer(&mut *writer, value).context("serialize Kuru shadow record")?;
    writer
        .write_all(b"\n")
        .context("write Kuru shadow record newline")?;
    Ok(())
}

fn u256_to_u128(value: U256, field: &str) -> Result<u128> {
    if value.bits() > 128 {
        bail!("{field} does not fit uint128");
    }
    Ok(value.as_u128())
}

fn unix_timestamp_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .min(u128::from(u64::MAX)) as u64
}

fn duration_ns(duration: Duration) -> u64 {
    duration.as_nanos().min(u128::from(u64::MAX)) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_leg(
        venue: &'static str,
        operation: &'static str,
        input: U256,
        output: U256,
    ) -> LegProof {
        LegProof {
            venue,
            operation,
            target: "0x0000000000000000000000000000000000000001".to_owned(),
            requested_input_raw: input.to_string(),
            submitted_input_raw: Some(input.to_string()),
            contract_input_units: Some(input.to_string()),
            amount_in_used_raw: Some(input.to_string()),
            residual_input_raw: Some("0".to_owned()),
            amount_out_raw: Some(output.to_string()),
            full_input_consumption_proven: true,
            block_reference: json!({
                "blockHash":"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "requireCanonical":true
            }),
            calldata: Some("0x1234".to_owned()),
            rpc_latency_ns: 1,
            gas_estimate: None,
            gas_estimate_kind: None,
            error: None,
        }
    }

    fn crystal_runtime() -> CounterpartyRuntime {
        CounterpartyRuntime::Crystal {
            identity: Box::new(CrystalMarketIdentity {
                singleton: monad_crystal::CRYSTAL_SINGLETON_ADDRESS.to_owned(),
                market: monad_crystal::CRYSTAL_MON_USDC_MARKET.to_owned(),
                quote_asset: USDC_ADDRESS.to_owned(),
                base_asset: kuru_chain::WMON_ADDRESS.to_owned(),
                market_type: "0".to_owned(),
                highest_bid: "1".to_owned(),
                lowest_ask: "2".to_owned(),
                scale_factor: "1000000".to_owned(),
                tick_size: "1".to_owned(),
                max_price: "100".to_owned(),
                min_size: "1".to_owned(),
                taker_fee: "99900".to_owned(),
                maker_rebate: "100000".to_owned(),
                reserve_quote: "0".to_owned(),
                reserve_base: "0".to_owned(),
                is_amm_enabled: false,
                block_reference: json!({
                    "blockHash":"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "requireCanonical":true
                }),
                calldata: "0x".to_owned(),
                rpc_latency_ns: 1,
            }),
            singleton_code_hash: "0x11".to_owned(),
            market_code_hash: "0x22".to_owned(),
        }
    }

    #[test]
    fn notionals_require_exact_positive_usdc_units() {
        assert_eq!(Notional::parse("100").expect("100 USDC").raw, 100_000_000);
        assert_eq!(Notional::parse("0.000001").expect("one atom").raw, 1);
        assert!(Notional::parse("0").is_err());
        assert!(Notional::parse("1.0000001").is_err());
    }

    #[test]
    fn route_economics_are_exact_for_profit_and_loss() {
        let profit = route_economics(U256::from(100_050_000_u64), U256::from(100_000_000_u64))
            .expect("profit");
        assert_eq!(profit.0, "50000");
        assert_eq!(profit.1, "0.05");
        assert_eq!(profit.2, "5");

        let loss =
            route_economics(U256::from(99_900_000_u64), U256::from(100_000_000_u64)).expect("loss");
        assert_eq!(loss.0, "-100000");
        assert_eq!(loss.1, "-0.1");
        assert_eq!(loss.2, "-10");
    }

    #[test]
    fn default_market_constant_is_the_live_legacy_mon_usdc_market() {
        assert_eq!(
            crate::kuru_chain::KURU_MON_USDC_MARKET,
            "0x065C9d28E428A0db40191a54d33d5b7c71a9C394"
        );
    }

    #[test]
    fn old_config_defaults_to_uniswap_v4() {
        let config: KuruShadowConfig = toml::from_str(
            r#"
schema_version = 1
rpc_http_url = "https://rpc.monad.xyz"
kuru_market = "0x065C9d28E428A0db40191a54d33d5b7c71a9C394"
kuru_version = "auto"
output_path = "capture.ndjson"
notionals_usdc = ["100"]
duration_seconds = 60
block_poll_interval_ms = 100
request_timeout_ms = 5000
candidate_threshold_bps = "5"
estimate_legacy_kuru_gas = false
"#,
        )
        .unwrap();
        config.validate().unwrap();
        assert_eq!(config.counterparty, CounterpartyVenue::UniswapV4);
        assert!(config.crystal_market.is_none());
    }

    #[test]
    fn crystal_routes_label_direction_conversion_and_exact_second_input() {
        let crystal = crystal_runtime();
        assert_eq!(
            counterparty_to_kuru_direction(&crystal),
            "crystal_buy_kuru_sell"
        );
        assert_eq!(
            kuru_to_counterparty_direction(&crystal),
            "kuru_buy_crystal_sell"
        );
        assert_eq!(
            counterparty_to_kuru_conversion(&crystal),
            Some("wmon_to_native_mon_1_to_1_raw_units_unwrap_call_not_simulated")
        );
        assert_eq!(
            kuru_to_counterparty_conversion(&crystal),
            Some("native_mon_to_wmon_1_to_1_raw_units_wrap_call_not_simulated")
        );

        let exact_output = U256::MAX - U256::from(123_u16);
        let first = test_leg("crystal", "buy_wmon", U256::from(100_u8), exact_output);
        let chained = second_leg_input(&first).unwrap();
        assert_eq!(chained, exact_output);
        let second = test_leg("kuru_v2", "sell_mon", chained, U256::from(101_u8));
        let route = route_proof(
            "crystal_buy_kuru_sell",
            first,
            second,
            U256::from(100_u8),
            Decimal::from(5_u8),
            "0.0001",
            counterparty_to_kuru_conversion(&crystal),
        );
        assert_eq!(
            route.second_leg.requested_input_raw,
            exact_output.to_string()
        );
        assert!(route.full_input_consumption_proven);
    }
}
