//! Raw, uncoalesced public market-data capture for PUMP lead/lag research.
//!
//! This recorder is intentionally separate from the production BBO publisher:
//! it preserves each upstream payload and records a common process-local
//! monotonic receive timestamp before decoding. The resulting file is a
//! research artifact, not a trading signal feed.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, bail, Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio::time::{self, MissedTickBehavior};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

pub const PUMP_CAPTURE_SCHEMA_VERSION: u16 = 1;

const BINANCE_SOURCE: &str = "binance_pump_perp";
const HYPERLIQUID_SPOT_SOURCE: &str = "hyperliquid_upump_spot";
const HYPERLIQUID_PERP_SOURCE: &str = "hyperliquid_pump_perp";
const LIGHTER_SOURCE: &str = "lighter_pump_perp";
const PUMPSWAP_SOURCE: &str = "pumpswap_spot";
const INITIAL_SNAPSHOT_OR_BACKFILL: &str = "initial_snapshot_or_backfill";

const BINANCE_WS_URL: &str = "wss://fstream.binance.com/public/ws/pumpusdt@depth20@100ms";
const HYPERLIQUID_WS_URL: &str = "wss://api.hyperliquid.xyz/ws";
const LIGHTER_WS_URL: &str = "wss://mainnet.zklighter.elliot.ai/stream?readonly=true";
const SOLANA_WS_URL: &str = "wss://api.mainnet-beta.solana.com";
const SOLANA_HTTP_URL: &str = "https://api.mainnet-beta.solana.com";

pub const PUMPSWAP_POOL: &str = "2uF4Xh61rDwxnG9woyxsVQP7zuA6kLFpb3NvnRQeoiSd";
pub const PUMPSWAP_BASE_VAULT: &str = "8TqK4PL3x7zWR2dNinr1LMi8Uf4vTRSFE2Ev1yYW9bhC";
pub const PUMPSWAP_QUOTE_VAULT: &str = "68Vdm7mQJ7RBxWioLVEUXbeTTpTtyiR1CL9vLRxmdr8t";

const REQUIRED_SOURCES: [&str; 5] = [
    BINANCE_SOURCE,
    HYPERLIQUID_SPOT_SOURCE,
    HYPERLIQUID_PERP_SOURCE,
    LIGHTER_SOURCE,
    PUMPSWAP_SOURCE,
];

#[derive(Debug, Clone)]
pub struct PumpRecorderConfig {
    pub output_path: PathBuf,
    pub duration: Duration,
    pub warmup_timeout: Duration,
    pub flush_interval: Duration,
    pub channel_capacity: usize,
}

impl PumpRecorderConfig {
    pub fn validate(&self) -> Result<()> {
        if self.duration.is_zero() || self.duration > Duration::from_secs(24 * 60 * 60) {
            bail!("duration must be between 1 second and 24 hours");
        }
        if self.warmup_timeout.is_zero() || self.warmup_timeout > Duration::from_secs(10 * 60) {
            bail!("warmup timeout must be between 1 second and 10 minutes");
        }
        if self.flush_interval.is_zero() || self.flush_interval > Duration::from_secs(60) {
            bail!("flush interval must be between 1 millisecond and 60 seconds");
        }
        if !(1_024..=1_000_000).contains(&self.channel_capacity) {
            bail!("channel capacity must be between 1,024 and 1,000,000");
        }
        if self
            .output_path
            .extension()
            .and_then(|value| value.to_str())
            != Some("ndjson")
        {
            bail!("output path must end in .ndjson");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PumpCaptureSummary {
    pub schema_version: u16,
    pub capture_id: String,
    /// Actual retained artifact. This is the requested `.ndjson` only after
    /// the full timed interval, otherwise it is the `.ndjson.part` file.
    pub output_path: PathBuf,
    pub requested_output_path: PathBuf,
    pub manifest_path: PathBuf,
    pub termination_reason: String,
    pub termination_detail: Option<String>,
    pub started_unix_ns: u64,
    pub started_monotonic_ns: u64,
    pub ready_unix_ns: u64,
    pub ready_monotonic_ns: Option<u64>,
    pub deadline_monotonic_ns: Option<u64>,
    pub ended_unix_ns: u64,
    pub ended_monotonic_ns: u64,
    pub requested_duration_ms: u64,
    pub captured_duration_ms: u64,
    pub completed_requested_duration: bool,
    pub records: u64,
    pub data_records_by_source: BTreeMap<String, u64>,
    pub status_records_by_source: BTreeMap<String, u64>,
}

#[derive(Debug, Serialize)]
struct CaptureManifest<'a> {
    #[serde(flatten)]
    summary: &'a PumpCaptureSummary,
    clock: &'static str,
    ordering: &'static str,
    instruments: Vec<InstrumentManifest>,
    known_unavailable: Vec<UnavailableInstrument>,
    caveats: Vec<&'static str>,
}

#[derive(Debug, Serialize)]
struct InstrumentManifest {
    source: &'static str,
    venue: &'static str,
    instrument: &'static str,
    streams: &'static [&'static str],
    endpoint: &'static str,
}

#[derive(Debug, Serialize)]
struct UnavailableInstrument {
    source: &'static str,
    reason: &'static str,
}

#[derive(Debug)]
struct PendingRecord {
    record_type: &'static str,
    source: &'static str,
    stream: String,
    connection_generation: u64,
    receive_unix_ns: u64,
    receive_monotonic_ns: u64,
    decode_done_monotonic_ns: u64,
    delivery_semantics: Option<&'static str>,
    payload: Value,
}

#[derive(Debug, Serialize)]
struct CaptureRecord<'a> {
    schema_version: u16,
    capture_id: &'a str,
    local_sequence: u64,
    record_type: &'a str,
    source: &'a str,
    stream: &'a str,
    connection_generation: u64,
    receive_unix_ns: u64,
    receive_monotonic_ns: u64,
    decode_done_monotonic_ns: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    delivery_semantics: Option<&'static str>,
    payload: &'a Value,
}

#[derive(Debug)]
struct CaptureClock {
    origin: Instant,
}

impl CaptureClock {
    fn new() -> Self {
        Self {
            origin: Instant::now(),
        }
    }

    fn stamp(&self) -> (u64, u64) {
        let (unix_ns, monotonic_ns, _) = self.boundary_stamp();
        (unix_ns, monotonic_ns)
    }

    fn boundary_stamp(&self) -> (u64, u64, Instant) {
        let instant = Instant::now();
        let monotonic_ns = duration_ns(instant.duration_since(self.origin));
        (unix_timestamp_ns(), monotonic_ns, instant)
    }

    fn monotonic_ns_at(&self, instant: Instant) -> u64 {
        duration_ns(instant.duration_since(self.origin))
    }

    fn elapsed_ns(&self) -> u64 {
        duration_ns(self.origin.elapsed())
    }
}

pub async fn run_pump_recorder(
    config: PumpRecorderConfig,
    cancellation: CancellationToken,
) -> Result<PumpCaptureSummary> {
    config.validate()?;
    if let Some(parent) = config.output_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create capture directory {}", parent.display()))?;
    }
    let part_path = part_path(&config.output_path)?;
    let manifest_path = manifest_path(&config.output_path)?;
    let partial_manifest_path = partial_manifest_path(&config.output_path)?;
    if config.output_path.exists()
        || part_path.exists()
        || manifest_path.exists()
        || partial_manifest_path.exists()
    {
        bail!(
            "capture output already exists: {}, {}, {}, or {}",
            config.output_path.display(),
            part_path.display(),
            manifest_path.display(),
            partial_manifest_path.display()
        );
    }
    let file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&part_path)
        .with_context(|| format!("create {}", part_path.display()))?;
    let mut writer = BufWriter::with_capacity(1024 * 1024, file);

    let capture_id = format!("pump-lead-lag-{}", Uuid::new_v4());
    let clock = Arc::new(CaptureClock::new());
    let (started_unix_ns, started_monotonic_ns, _) = clock.boundary_stamp();
    let (sender, mut receiver) = mpsc::channel(config.channel_capacity);
    let mut tasks = JoinSet::new();
    tasks.spawn(supervise_binance(
        sender.clone(),
        Arc::clone(&clock),
        cancellation.clone(),
    ));
    tasks.spawn(supervise_hyperliquid(
        sender.clone(),
        Arc::clone(&clock),
        cancellation.clone(),
    ));
    tasks.spawn(supervise_lighter(
        sender.clone(),
        Arc::clone(&clock),
        cancellation.clone(),
    ));
    tasks.spawn(supervise_pumpswap(
        sender.clone(),
        Arc::clone(&clock),
        cancellation.clone(),
    ));
    drop(sender);

    let mut local_sequence = 0_u64;
    let mut ready_sources = BTreeSet::new();
    let mut ready_unix_ns = None;
    let mut ready_monotonic_ns = None;
    let mut deadline_monotonic_ns = None;
    let warmup_deadline = time::sleep(config.warmup_timeout);
    tokio::pin!(warmup_deadline);
    let completion_deadline = time::sleep(Duration::from_secs(24 * 60 * 60));
    tokio::pin!(completion_deadline);
    let mut completion_armed = false;
    let mut flush_timer = time::interval(config.flush_interval);
    flush_timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
    flush_timer.tick().await;
    let mut data_records_by_source = BTreeMap::<String, u64>::new();
    let mut status_records_by_source = BTreeMap::<String, u64>::new();
    let (termination_reason, terminal_error) = loop {
        tokio::select! {
            _ = cancellation.cancelled() => break ("cancelled", None),
            _ = &mut warmup_deadline, if !completion_armed => {
                let missing: Vec<_> = REQUIRED_SOURCES
                    .iter()
                    .copied()
                    .filter(|source| !ready_sources.contains(source))
                    .collect();
                break (
                    "warmup_timeout",
                    Some(anyhow!(
                        "capture warmup timed out; missing live data from {}",
                        missing.join(", ")
                    )),
                );
            }
            _ = &mut completion_deadline, if completion_armed => {
                break ("completed_requested_duration", None);
            }
            _ = flush_timer.tick() => {
                writer.flush().context("periodically flush capture")?;
            }
            maybe_record = receiver.recv() => {
                let Some(record) = maybe_record else {
                    if cancellation.is_cancelled() {
                        break ("cancelled", None);
                    }
                    break (
                        "all_supervisors_exited",
                        Some(anyhow!("all recorder tasks exited before capture completed")),
                    );
                };
                write_record(&mut writer, &capture_id, &mut local_sequence, &record)?;
                let counts = if record.record_type == "data" {
                    if is_readiness_record(&record) {
                        ready_sources.insert(record.source);
                    }
                    &mut data_records_by_source
                } else {
                    &mut status_records_by_source
                };
                let count = counts.entry(record.source.to_owned()).or_default();
                *count = count.checked_add(1).ok_or_else(|| anyhow!("record count overflow"))?;

                if !completion_armed
                    && REQUIRED_SOURCES
                        .iter()
                        .all(|source| ready_sources.contains(source))
                {
                    let (ready_unix, ready_monotonic, ready_instant) = clock.boundary_stamp();
                    let deadline_instant = ready_instant + config.duration;
                    completion_deadline
                        .as_mut()
                        .reset(time::Instant::from_std(deadline_instant));
                    completion_armed = true;
                    ready_unix_ns = Some(ready_unix);
                    ready_monotonic_ns = Some(ready_monotonic);
                    deadline_monotonic_ns = Some(clock.monotonic_ns_at(deadline_instant));
                    tracing::info!(
                        capture_id,
                        duration_seconds = config.duration.as_secs(),
                        "all PUMP sources ready; timed capture started"
                    );
                }
            }
            joined = tasks.join_next(), if !tasks.is_empty() => {
                match joined {
                    Some(Ok(Ok(()))) if cancellation.is_cancelled() => {
                        break ("cancelled", None);
                    },
                    Some(Ok(Ok(()))) => {
                        break (
                            "supervisor_exited",
                            Some(anyhow!("a recorder supervisor exited unexpectedly")),
                        );
                    }
                    Some(Ok(Err(error))) => {
                        break (
                            "supervisor_failed",
                            Some(error.context("recorder supervisor failed")),
                        );
                    }
                    Some(Err(error)) => {
                        break (
                            "supervisor_task_failed",
                            Some(anyhow!("recorder task failed: {error}")),
                        );
                    }
                    None => {}
                }
            }
        }
    };

    let (ended_unix_ns, ended_monotonic_ns) = clock.stamp();
    cancellation.cancel();
    shutdown_tasks(&mut tasks).await;
    while let Ok(record) = receiver.try_recv() {
        write_record(&mut writer, &capture_id, &mut local_sequence, &record)?;
        let counts = if record.record_type == "data" {
            &mut data_records_by_source
        } else {
            &mut status_records_by_source
        };
        *counts.entry(record.source.to_owned()).or_default() += 1;
    }
    writer.flush().context("flush retained capture")?;
    writer
        .get_ref()
        .sync_all()
        .context("sync retained capture")?;
    drop(writer);
    let completed_requested_duration = termination_reason == "completed_requested_duration";
    let (artifact_path, retained_manifest_path) = if completed_requested_duration {
        fs::rename(&part_path, &config.output_path).with_context(|| {
            format!(
                "rename completed capture {} to {}",
                part_path.display(),
                config.output_path.display()
            )
        })?;
        (config.output_path.clone(), manifest_path)
    } else {
        (part_path, partial_manifest_path)
    };

    let ready_unix_ns = ready_unix_ns.unwrap_or(started_unix_ns);
    let captured_duration_ms = ready_monotonic_ns
        .map(|ready| ended_monotonic_ns.saturating_sub(ready) / 1_000_000)
        .unwrap_or(0);
    let summary = PumpCaptureSummary {
        schema_version: PUMP_CAPTURE_SCHEMA_VERSION,
        capture_id,
        output_path: artifact_path,
        requested_output_path: config.output_path,
        manifest_path: retained_manifest_path.clone(),
        termination_reason: termination_reason.to_owned(),
        termination_detail: terminal_error.as_ref().map(|error| format!("{error:#}")),
        started_unix_ns,
        started_monotonic_ns,
        ready_unix_ns,
        ready_monotonic_ns,
        deadline_monotonic_ns,
        ended_unix_ns,
        ended_monotonic_ns,
        requested_duration_ms: duration_ms(config.duration),
        captured_duration_ms,
        completed_requested_duration,
        records: local_sequence,
        data_records_by_source,
        status_records_by_source,
    };
    write_manifest(&retained_manifest_path, &summary)?;
    if let Some(error) = terminal_error {
        return Err(error.context(format!(
            "partial capture retained at {}; manifest written to {}",
            summary.output_path.display(),
            summary.manifest_path.display()
        )));
    }
    Ok(summary)
}

fn write_record(
    writer: &mut BufWriter<std::fs::File>,
    capture_id: &str,
    local_sequence: &mut u64,
    record: &PendingRecord,
) -> Result<()> {
    *local_sequence = local_sequence
        .checked_add(1)
        .ok_or_else(|| anyhow!("capture sequence exhausted"))?;
    let encoded = CaptureRecord {
        schema_version: PUMP_CAPTURE_SCHEMA_VERSION,
        capture_id,
        local_sequence: *local_sequence,
        record_type: record.record_type,
        source: record.source,
        stream: &record.stream,
        connection_generation: record.connection_generation,
        receive_unix_ns: record.receive_unix_ns,
        receive_monotonic_ns: record.receive_monotonic_ns,
        decode_done_monotonic_ns: record.decode_done_monotonic_ns,
        delivery_semantics: record.delivery_semantics,
        payload: &record.payload,
    };
    serde_json::to_writer(&mut *writer, &encoded).context("serialize capture record")?;
    writer.write_all(b"\n").context("write capture newline")?;
    Ok(())
}

fn is_readiness_record(record: &PendingRecord) -> bool {
    match record.source {
        BINANCE_SOURCE => record.stream == "depth20",
        HYPERLIQUID_SPOT_SOURCE | HYPERLIQUID_PERP_SOURCE => {
            record.stream == "bbo" || record.stream == "l2Book"
        }
        LIGHTER_SOURCE => record.stream == "ticker" || record.stream == "order_book",
        PUMPSWAP_SOURCE => record.stream == "bootstrap",
        _ => false,
    }
}

async fn shutdown_tasks(tasks: &mut JoinSet<Result<()>>) {
    if time::timeout(Duration::from_secs(5), async {
        while tasks.join_next().await.is_some() {}
    })
    .await
    .is_err()
    {
        tasks.abort_all();
    }
}

async fn supervise_binance(
    sender: mpsc::Sender<PendingRecord>,
    clock: Arc<CaptureClock>,
    cancellation: CancellationToken,
) -> Result<()> {
    let mut generation = 0_u64;
    let mut backoff = Duration::from_millis(250);
    loop {
        if cancellation.is_cancelled() {
            return Ok(());
        }
        generation = generation
            .checked_add(1)
            .ok_or_else(|| anyhow!("Binance connection generation exhausted"))?;
        send_status(
            &sender,
            &clock,
            BINANCE_SOURCE,
            "connecting",
            generation,
            json!({"endpoint": BINANCE_WS_URL}),
        )
        .await?;
        let result = run_binance_once(&sender, &clock, generation, &cancellation).await;
        if cancellation.is_cancelled() {
            return Ok(());
        }
        send_status(
            &sender,
            &clock,
            BINANCE_SOURCE,
            "disconnected",
            generation,
            json!({"error": display_result(&result)}),
        )
        .await?;
        sleep_or_cancel(backoff, &cancellation).await?;
        backoff = (backoff * 2).min(Duration::from_secs(10));
    }
}

async fn run_binance_once(
    sender: &mpsc::Sender<PendingRecord>,
    clock: &CaptureClock,
    generation: u64,
    cancellation: &CancellationToken,
) -> Result<()> {
    let (stream, _) = connect_async(BINANCE_WS_URL)
        .await
        .context("connect Binance PUMP depth")?;
    let (mut writer, mut reader) = stream.split();
    send_status(
        sender,
        clock,
        BINANCE_SOURCE,
        "connected",
        generation,
        json!({"endpoint": BINANCE_WS_URL}),
    )
    .await?;
    loop {
        let message = tokio::select! {
            _ = cancellation.cancelled() => return Ok(()),
            message = reader.next() => message,
        };
        let message = message
            .ok_or_else(|| anyhow!("Binance WebSocket closed"))?
            .context("read Binance WebSocket")?;
        let (receive_unix_ns, receive_monotonic_ns) = clock.stamp();
        match message {
            Message::Text(text) => {
                let payload: Value =
                    serde_json::from_str(text.as_ref()).context("decode Binance depth payload")?;
                let symbol = payload.get("s").and_then(Value::as_str);
                if symbol != Some("PUMPUSDT") {
                    bail!("unexpected Binance symbol {symbol:?}");
                }
                send_data_with_stamp(
                    sender,
                    clock,
                    BINANCE_SOURCE,
                    "depth20",
                    generation,
                    receive_unix_ns,
                    receive_monotonic_ns,
                    payload,
                )
                .await?;
            }
            Message::Ping(payload) => writer.send(Message::Pong(payload)).await?,
            Message::Pong(_) | Message::Frame(_) => {}
            Message::Close(frame) => bail!("Binance WebSocket closed: {frame:?}"),
            Message::Binary(_) => bail!("unexpected binary Binance payload"),
        }
    }
}

async fn supervise_hyperliquid(
    sender: mpsc::Sender<PendingRecord>,
    clock: Arc<CaptureClock>,
    cancellation: CancellationToken,
) -> Result<()> {
    let mut generation = 0_u64;
    let mut backoff = Duration::from_millis(250);
    loop {
        if cancellation.is_cancelled() {
            return Ok(());
        }
        generation = generation
            .checked_add(1)
            .ok_or_else(|| anyhow!("Hyperliquid connection generation exhausted"))?;
        send_status(
            &sender,
            &clock,
            "hyperliquid",
            "connecting",
            generation,
            json!({"endpoint": HYPERLIQUID_WS_URL}),
        )
        .await?;
        let result = run_hyperliquid_once(&sender, &clock, generation, &cancellation).await;
        if cancellation.is_cancelled() {
            return Ok(());
        }
        send_status(
            &sender,
            &clock,
            "hyperliquid",
            "disconnected",
            generation,
            json!({"error": display_result(&result)}),
        )
        .await?;
        sleep_or_cancel(backoff, &cancellation).await?;
        backoff = (backoff * 2).min(Duration::from_secs(10));
    }
}

async fn run_hyperliquid_once(
    sender: &mpsc::Sender<PendingRecord>,
    clock: &CaptureClock,
    generation: u64,
    cancellation: &CancellationToken,
) -> Result<()> {
    let (stream, _) = connect_async(HYPERLIQUID_WS_URL)
        .await
        .context("connect Hyperliquid PUMP streams")?;
    let (mut writer, mut reader) = stream.split();
    for coin in ["@188", "PUMP"] {
        for kind in ["bbo", "l2Book", "trades"] {
            writer
                .send(Message::Text(
                    json!({"method":"subscribe","subscription":{"type":kind,"coin":coin}})
                        .to_string()
                        .into(),
                ))
                .await
                .with_context(|| format!("subscribe Hyperliquid {coin} {kind}"))?;
        }
    }
    send_status(
        sender,
        clock,
        "hyperliquid",
        "connected",
        generation,
        json!({"endpoint": HYPERLIQUID_WS_URL, "coins": ["@188", "PUMP"]}),
    )
    .await?;
    let mut heartbeat = time::interval(Duration::from_secs(30));
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
    heartbeat.tick().await;
    let mut initial_trade_batches = BTreeSet::new();
    loop {
        tokio::select! {
            _ = cancellation.cancelled() => return Ok(()),
            _ = heartbeat.tick() => {
                writer.send(Message::Text(json!({"method":"ping"}).to_string().into())).await?;
            }
            message = reader.next() => {
                let message = message
                    .ok_or_else(|| anyhow!("Hyperliquid WebSocket closed"))?
                    .context("read Hyperliquid WebSocket")?;
                let (receive_unix_ns, receive_monotonic_ns) = clock.stamp();
                match message {
                    Message::Text(text) => {
                        let payload: Value = serde_json::from_str(text.as_ref())
                            .context("decode Hyperliquid payload")?;
                        let channel = payload
                            .get("channel")
                            .and_then(Value::as_str)
                            .unwrap_or("unknown")
                            .to_owned();
                        if matches!(channel.as_str(), "bbo" | "l2Book" | "trades") {
                            let coin = hyperliquid_coin(&payload, &channel)
                                .ok_or_else(|| anyhow!("Hyperliquid {channel} payload has no coin"))?;
                            let source = match coin {
                                "@188" => HYPERLIQUID_SPOT_SOURCE,
                                "PUMP" => HYPERLIQUID_PERP_SOURCE,
                                other => bail!("unexpected Hyperliquid coin {other}"),
                            };
                            let delivery_semantics = hyperliquid_delivery_semantics(
                                &channel,
                                source,
                                &mut initial_trade_batches,
                            );
                            send_data_with_stamp_and_semantics(
                                sender,
                                clock,
                                source,
                                &channel,
                                generation,
                                receive_unix_ns,
                                receive_monotonic_ns,
                                delivery_semantics,
                                payload,
                            ).await?;
                        } else if channel != "pong" {
                            send_status_with_stamp(
                                sender,
                                clock,
                                "hyperliquid",
                                &channel,
                                generation,
                                receive_unix_ns,
                                receive_monotonic_ns,
                                payload,
                            ).await?;
                        }
                    }
                    Message::Ping(payload) => writer.send(Message::Pong(payload)).await?,
                    Message::Pong(_) | Message::Frame(_) => {}
                    Message::Close(frame) => bail!("Hyperliquid WebSocket closed: {frame:?}"),
                    Message::Binary(_) => bail!("unexpected binary Hyperliquid payload"),
                }
            }
        }
    }
}

fn hyperliquid_coin<'a>(payload: &'a Value, channel: &str) -> Option<&'a str> {
    if channel == "trades" {
        payload
            .get("data")?
            .as_array()?
            .first()?
            .get("coin")?
            .as_str()
    } else {
        payload.get("data")?.get("coin")?.as_str()
    }
}

fn hyperliquid_delivery_semantics(
    channel: &str,
    source: &'static str,
    initial_trade_batches: &mut BTreeSet<&'static str>,
) -> Option<&'static str> {
    (channel == "trades" && initial_trade_batches.insert(source))
        .then_some(INITIAL_SNAPSHOT_OR_BACKFILL)
}

async fn supervise_lighter(
    sender: mpsc::Sender<PendingRecord>,
    clock: Arc<CaptureClock>,
    cancellation: CancellationToken,
) -> Result<()> {
    let mut generation = 0_u64;
    let mut backoff = Duration::from_millis(250);
    loop {
        if cancellation.is_cancelled() {
            return Ok(());
        }
        generation = generation
            .checked_add(1)
            .ok_or_else(|| anyhow!("Lighter connection generation exhausted"))?;
        send_status(
            &sender,
            &clock,
            LIGHTER_SOURCE,
            "connecting",
            generation,
            json!({"endpoint": LIGHTER_WS_URL, "market_id": 45}),
        )
        .await?;
        let result = run_lighter_once(&sender, &clock, generation, &cancellation).await;
        if cancellation.is_cancelled() {
            return Ok(());
        }
        send_status(
            &sender,
            &clock,
            LIGHTER_SOURCE,
            "disconnected",
            generation,
            json!({"error": display_result(&result)}),
        )
        .await?;
        sleep_or_cancel(backoff, &cancellation).await?;
        backoff = (backoff * 2).min(Duration::from_secs(10));
    }
}

async fn run_lighter_once(
    sender: &mpsc::Sender<PendingRecord>,
    clock: &CaptureClock,
    generation: u64,
    cancellation: &CancellationToken,
) -> Result<()> {
    let (stream, _) = connect_async(LIGHTER_WS_URL)
        .await
        .context("connect Lighter PUMP streams")?;
    let (mut writer, mut reader) = stream.split();
    let mut subscribed = false;
    let mut heartbeat = time::interval(Duration::from_secs(30));
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
    heartbeat.tick().await;
    loop {
        tokio::select! {
            _ = cancellation.cancelled() => return Ok(()),
            _ = heartbeat.tick() => {
                writer.send(Message::Text(json!({"type":"ping"}).to_string().into())).await?;
            }
            message = reader.next() => {
                let message = message
                    .ok_or_else(|| anyhow!("Lighter WebSocket closed"))?
                    .context("read Lighter WebSocket")?;
                let (receive_unix_ns, receive_monotonic_ns) = clock.stamp();
                match message {
                    Message::Text(text) => {
                        let payload: Value = serde_json::from_str(text.as_ref())
                            .context("decode Lighter payload")?;
                        let kind = payload
                            .get("type")
                            .and_then(Value::as_str)
                            .unwrap_or("unknown")
                            .to_owned();
                        if kind == "connected" && !subscribed {
                            for channel in ["ticker/45", "order_book/45", "trade/45"] {
                                writer.send(Message::Text(
                                    json!({"type":"subscribe","channel":channel}).to_string().into()
                                )).await.with_context(|| format!("subscribe Lighter {channel}"))?;
                            }
                            subscribed = true;
                            send_status_with_stamp(
                                sender,
                                clock,
                                LIGHTER_SOURCE,
                                "connected",
                                generation,
                                receive_unix_ns,
                                receive_monotonic_ns,
                                payload,
                            ).await?;
                            continue;
                        }
                        if kind == "ping" {
                            writer.send(Message::Text(json!({"type":"pong"}).to_string().into())).await?;
                            continue;
                        }
                        if kind == "pong" {
                            continue;
                        }
                        let channel = payload
                            .get("channel")
                            .and_then(Value::as_str)
                            .unwrap_or("unknown");
                        let stream_name = if channel == "ticker:45" {
                            "ticker"
                        } else if channel == "order_book:45" {
                            "order_book"
                        } else if channel == "trade:45" {
                            "trade"
                        } else {
                            send_status_with_stamp(
                                sender,
                                clock,
                                LIGHTER_SOURCE,
                                &kind,
                                generation,
                                receive_unix_ns,
                                receive_monotonic_ns,
                                payload,
                            ).await?;
                            continue;
                        };
                        let delivery_semantics = lighter_delivery_semantics(&kind, stream_name);
                        send_data_with_stamp_and_semantics(
                            sender,
                            clock,
                            LIGHTER_SOURCE,
                            stream_name,
                            generation,
                            receive_unix_ns,
                            receive_monotonic_ns,
                            delivery_semantics,
                            payload,
                        ).await?;
                    }
                    Message::Ping(payload) => writer.send(Message::Pong(payload)).await?,
                    Message::Pong(_) | Message::Frame(_) => {}
                    Message::Close(frame) => bail!("Lighter WebSocket closed: {frame:?}"),
                    Message::Binary(_) => bail!("unexpected binary Lighter payload"),
                }
            }
        }
    }
}

fn lighter_delivery_semantics(kind: &str, stream: &str) -> Option<&'static str> {
    (kind == "subscribed/trade" && stream == "trade").then_some(INITIAL_SNAPSHOT_OR_BACKFILL)
}

async fn supervise_pumpswap(
    sender: mpsc::Sender<PendingRecord>,
    clock: Arc<CaptureClock>,
    cancellation: CancellationToken,
) -> Result<()> {
    let client = reqwest::Client::builder()
        .user_agent("ala-crate-pump-lead-lag/0.1")
        .timeout(Duration::from_secs(10))
        .build()
        .context("build Solana bootstrap client")?;
    let mut generation = 0_u64;
    let mut backoff = Duration::from_millis(250);
    loop {
        if cancellation.is_cancelled() {
            return Ok(());
        }
        generation = generation
            .checked_add(1)
            .ok_or_else(|| anyhow!("PumpSwap connection generation exhausted"))?;
        send_status(
            &sender,
            &clock,
            PUMPSWAP_SOURCE,
            "connecting",
            generation,
            json!({"endpoint": SOLANA_WS_URL, "pool": PUMPSWAP_POOL}),
        )
        .await?;
        let result = run_pumpswap_once(&client, &sender, &clock, generation, &cancellation).await;
        if cancellation.is_cancelled() {
            return Ok(());
        }
        send_status(
            &sender,
            &clock,
            PUMPSWAP_SOURCE,
            "disconnected",
            generation,
            json!({"error": display_result(&result)}),
        )
        .await?;
        sleep_or_cancel(backoff, &cancellation).await?;
        backoff = (backoff * 2).min(Duration::from_secs(10));
    }
}

async fn run_pumpswap_once(
    client: &reqwest::Client,
    sender: &mpsc::Sender<PendingRecord>,
    clock: &CaptureClock,
    generation: u64,
    cancellation: &CancellationToken,
) -> Result<()> {
    let (stream, _) = connect_async(SOLANA_WS_URL)
        .await
        .context("connect Solana PumpSwap streams")?;
    let (mut writer, mut reader) = stream.split();
    let subscriptions = [
        (
            1_u64,
            "base_vault",
            json!({
                "jsonrpc":"2.0","id":1,"method":"accountSubscribe",
                "params":[PUMPSWAP_BASE_VAULT,{"encoding":"base64","commitment":"processed","dataSlice":{"offset":64,"length":8}}]
            }),
        ),
        (
            2_u64,
            "quote_vault",
            json!({
                "jsonrpc":"2.0","id":2,"method":"accountSubscribe",
                "params":[PUMPSWAP_QUOTE_VAULT,{"encoding":"base64","commitment":"processed","dataSlice":{"offset":64,"length":8}}]
            }),
        ),
        (
            3_u64,
            "logs",
            json!({
                "jsonrpc":"2.0","id":3,"method":"logsSubscribe",
                "params":[{"mentions":[PUMPSWAP_POOL]},{"commitment":"processed"}]
            }),
        ),
        (
            4_u64,
            "pool_state",
            json!({
                "jsonrpc":"2.0","id":4,"method":"accountSubscribe",
                "params":[PUMPSWAP_POOL,{"encoding":"base64","commitment":"processed","dataSlice":{"offset":245,"length":16}}]
            }),
        ),
    ];
    for (_, _, request) in &subscriptions {
        writer
            .send(Message::Text(request.to_string().into()))
            .await
            .context("subscribe Solana PumpSwap stream")?;
    }

    let request_streams: BTreeMap<u64, &str> = subscriptions
        .iter()
        .map(|(id, stream, _)| (*id, *stream))
        .collect();
    let mut subscription_streams = BTreeMap::<u64, &str>::new();
    let mut buffered_notifications = Vec::<(u64, u64, Value)>::new();
    while subscription_streams.len() < subscriptions.len() {
        let message = tokio::select! {
            _ = cancellation.cancelled() => return Ok(()),
            message = reader.next() => message,
        };
        let message = message
            .ok_or_else(|| anyhow!("Solana WebSocket closed before subscription readiness"))?
            .context("read Solana subscription acknowledgement")?;
        let (receive_unix_ns, receive_monotonic_ns) = clock.stamp();
        match message {
            Message::Text(text) => {
                let payload: Value = serde_json::from_str(text.as_ref())
                    .context("decode Solana subscription acknowledgement")?;
                if let (Some(request_id), Some(subscription_id)) = (
                    payload.get("id").and_then(Value::as_u64),
                    payload.get("result").and_then(Value::as_u64),
                ) {
                    let stream_name =
                        request_streams.get(&request_id).copied().ok_or_else(|| {
                            anyhow!("unexpected Solana subscription request id {request_id}")
                        })?;
                    subscription_streams.insert(subscription_id, stream_name);
                    send_status_with_stamp(
                        sender,
                        clock,
                        PUMPSWAP_SOURCE,
                        "subscribed",
                        generation,
                        receive_unix_ns,
                        receive_monotonic_ns,
                        json!({"stream":stream_name,"response":payload}),
                    )
                    .await?;
                } else if payload.get("error").is_some() {
                    bail!("Solana subscription error: {payload}");
                } else {
                    payload
                        .pointer("/params/subscription")
                        .and_then(Value::as_u64)
                        .ok_or_else(|| {
                            anyhow!("unexpected Solana message before subscription readiness")
                        })?;
                    buffered_notifications.push((receive_unix_ns, receive_monotonic_ns, payload));
                }
            }
            Message::Ping(payload) => writer.send(Message::Pong(payload)).await?,
            Message::Pong(_) | Message::Frame(_) => {}
            Message::Close(frame) => {
                bail!("Solana WebSocket closed before subscription readiness: {frame:?}")
            }
            Message::Binary(_) => bail!("unexpected binary Solana payload"),
        }
    }

    // Every subscription has been acknowledged before taking the HTTP snapshot.
    // Notifications arriving during the request remain queued and retain their
    // raw slot; any notification interleaved with the acknowledgements is
    // buffered with its original local receive timestamp.
    let bootstrap_request = json!({
        "jsonrpc":"2.0",
        "id":100,
        "method":"getMultipleAccounts",
        "params":[
            [PUMPSWAP_BASE_VAULT, PUMPSWAP_QUOTE_VAULT, PUMPSWAP_POOL],
            {"encoding":"base64","commitment":"processed"}
        ]
    });
    let bootstrap_response = client
        .post(SOLANA_HTTP_URL)
        .json(&bootstrap_request)
        .send()
        .await
        .context("request PumpSwap bootstrap accounts")?
        .error_for_status()
        .context("PumpSwap bootstrap HTTP status")?
        .json::<Value>()
        .await
        .context("decode PumpSwap bootstrap accounts")?;
    if bootstrap_response.get("error").is_some()
        || bootstrap_response
            .pointer("/result/value")
            .and_then(Value::as_array)
            .is_none_or(|accounts| accounts.len() != 3 || accounts.iter().any(Value::is_null))
    {
        bail!("invalid PumpSwap bootstrap response: {bootstrap_response}");
    }
    send_data(
        sender,
        clock,
        PUMPSWAP_SOURCE,
        "bootstrap",
        generation,
        bootstrap_response,
    )
    .await?;
    send_status(
        sender,
        clock,
        PUMPSWAP_SOURCE,
        "connected",
        generation,
        json!({
            "endpoint": SOLANA_WS_URL,
            "pool": PUMPSWAP_POOL,
            "base_vault": PUMPSWAP_BASE_VAULT,
            "quote_vault": PUMPSWAP_QUOTE_VAULT
        }),
    )
    .await?;

    for (receive_unix_ns, receive_monotonic_ns, payload) in buffered_notifications {
        send_pumpswap_notification(
            sender,
            clock,
            generation,
            &subscription_streams,
            receive_unix_ns,
            receive_monotonic_ns,
            payload,
        )
        .await?;
    }

    loop {
        let message = tokio::select! {
            _ = cancellation.cancelled() => return Ok(()),
            message = reader.next() => message,
        };
        let message = message
            .ok_or_else(|| anyhow!("Solana WebSocket closed"))?
            .context("read Solana WebSocket")?;
        let (receive_unix_ns, receive_monotonic_ns) = clock.stamp();
        match message {
            Message::Text(text) => {
                let payload: Value = serde_json::from_str(text.as_ref())
                    .context("decode Solana PumpSwap payload")?;
                if let (Some(request_id), Some(subscription_id)) = (
                    payload.get("id").and_then(Value::as_u64),
                    payload.get("result").and_then(Value::as_u64),
                ) {
                    let stream_name =
                        request_streams.get(&request_id).copied().ok_or_else(|| {
                            anyhow!("unexpected Solana subscription request id {request_id}")
                        })?;
                    subscription_streams.insert(subscription_id, stream_name);
                    send_status_with_stamp(
                        sender,
                        clock,
                        PUMPSWAP_SOURCE,
                        "subscribed",
                        generation,
                        receive_unix_ns,
                        receive_monotonic_ns,
                        json!({"stream":stream_name,"response":payload}),
                    )
                    .await?;
                    continue;
                }
                if payload.get("error").is_some() {
                    bail!("Solana subscription error: {payload}");
                }
                send_pumpswap_notification(
                    sender,
                    clock,
                    generation,
                    &subscription_streams,
                    receive_unix_ns,
                    receive_monotonic_ns,
                    payload,
                )
                .await?;
            }
            Message::Ping(payload) => writer.send(Message::Pong(payload)).await?,
            Message::Pong(_) | Message::Frame(_) => {}
            Message::Close(frame) => bail!("Solana WebSocket closed: {frame:?}"),
            Message::Binary(_) => bail!("unexpected binary Solana payload"),
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn send_pumpswap_notification(
    sender: &mpsc::Sender<PendingRecord>,
    clock: &CaptureClock,
    generation: u64,
    subscription_streams: &BTreeMap<u64, &str>,
    receive_unix_ns: u64,
    receive_monotonic_ns: u64,
    payload: Value,
) -> Result<()> {
    let subscription_id = payload
        .pointer("/params/subscription")
        .and_then(Value::as_u64)
        .ok_or_else(|| anyhow!("Solana notification has no subscription id"))?;
    let stream_name = subscription_streams
        .get(&subscription_id)
        .copied()
        .ok_or_else(|| anyhow!("unknown Solana subscription id {subscription_id}"))?;
    send_data_with_stamp(
        sender,
        clock,
        PUMPSWAP_SOURCE,
        stream_name,
        generation,
        receive_unix_ns,
        receive_monotonic_ns,
        payload,
    )
    .await
}

async fn send_data(
    sender: &mpsc::Sender<PendingRecord>,
    clock: &CaptureClock,
    source: &'static str,
    stream: &'static str,
    generation: u64,
    payload: Value,
) -> Result<()> {
    let (receive_unix_ns, receive_monotonic_ns) = clock.stamp();
    send_data_with_stamp(
        sender,
        clock,
        source,
        stream,
        generation,
        receive_unix_ns,
        receive_monotonic_ns,
        payload,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn send_data_with_stamp(
    sender: &mpsc::Sender<PendingRecord>,
    clock: &CaptureClock,
    source: &'static str,
    stream: &str,
    generation: u64,
    receive_unix_ns: u64,
    receive_monotonic_ns: u64,
    payload: Value,
) -> Result<()> {
    send_data_with_stamp_and_semantics(
        sender,
        clock,
        source,
        stream,
        generation,
        receive_unix_ns,
        receive_monotonic_ns,
        None,
        payload,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn send_data_with_stamp_and_semantics(
    sender: &mpsc::Sender<PendingRecord>,
    clock: &CaptureClock,
    source: &'static str,
    stream: &str,
    generation: u64,
    receive_unix_ns: u64,
    receive_monotonic_ns: u64,
    delivery_semantics: Option<&'static str>,
    payload: Value,
) -> Result<()> {
    sender
        .send(PendingRecord {
            record_type: "data",
            source,
            stream: stream.to_owned(),
            connection_generation: generation,
            receive_unix_ns,
            receive_monotonic_ns,
            decode_done_monotonic_ns: clock.elapsed_ns(),
            delivery_semantics,
            payload,
        })
        .await
        .map_err(|_| anyhow!("capture writer stopped"))
}

async fn send_status(
    sender: &mpsc::Sender<PendingRecord>,
    clock: &CaptureClock,
    source: &'static str,
    stream: &'static str,
    generation: u64,
    payload: Value,
) -> Result<()> {
    let (receive_unix_ns, receive_monotonic_ns) = clock.stamp();
    send_status_with_stamp(
        sender,
        clock,
        source,
        stream,
        generation,
        receive_unix_ns,
        receive_monotonic_ns,
        payload,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn send_status_with_stamp(
    sender: &mpsc::Sender<PendingRecord>,
    clock: &CaptureClock,
    source: &'static str,
    stream: &str,
    generation: u64,
    receive_unix_ns: u64,
    receive_monotonic_ns: u64,
    payload: Value,
) -> Result<()> {
    sender
        .send(PendingRecord {
            record_type: "status",
            source,
            stream: stream.to_owned(),
            connection_generation: generation,
            receive_unix_ns,
            receive_monotonic_ns,
            decode_done_monotonic_ns: clock.elapsed_ns(),
            delivery_semantics: None,
            payload,
        })
        .await
        .map_err(|_| anyhow!("capture writer stopped"))
}

async fn sleep_or_cancel(delay: Duration, cancellation: &CancellationToken) -> Result<()> {
    tokio::select! {
        _ = cancellation.cancelled() => Ok(()),
        _ = time::sleep(delay) => Ok(()),
    }
}

fn display_result(result: &Result<()>) -> String {
    match result {
        Ok(()) => "connection ended without an error".to_owned(),
        Err(error) => format!("{error:#}"),
    }
}

fn write_manifest(path: &Path, summary: &PumpCaptureSummary) -> Result<()> {
    let instruments = vec![
        InstrumentManifest {
            source: BINANCE_SOURCE,
            venue: "Binance USD-M",
            instrument: "PUMPUSDT perpetual",
            streams: &["depth20@100ms"],
            endpoint: BINANCE_WS_URL,
        },
        InstrumentManifest {
            source: HYPERLIQUID_SPOT_SOURCE,
            venue: "Hyperliquid spot",
            instrument: "UPUMP/USDC (@188), 1:1 PUMP wrapper",
            streams: &["bbo", "l2Book", "trades"],
            endpoint: HYPERLIQUID_WS_URL,
        },
        InstrumentManifest {
            source: HYPERLIQUID_PERP_SOURCE,
            venue: "Hyperliquid",
            instrument: "PUMP perpetual",
            streams: &["bbo", "l2Book", "trades"],
            endpoint: HYPERLIQUID_WS_URL,
        },
        InstrumentManifest {
            source: LIGHTER_SOURCE,
            venue: "Lighter",
            instrument: "PUMP perpetual, market 45",
            streams: &["ticker", "order_book", "trade"],
            endpoint: LIGHTER_WS_URL,
        },
        InstrumentManifest {
            source: PUMPSWAP_SOURCE,
            venue: "PumpSwap on Solana",
            instrument: "native PUMP/USDC pool 2uF4...oiSd",
            streams: &["base_vault", "quote_vault", "pool_state", "logs"],
            endpoint: SOLANA_WS_URL,
        },
    ];
    let manifest = CaptureManifest {
        summary,
        clock: "all *_monotonic_ns fields are elapsed std::time::Instant time from one shared process origin; receive_unix_ns and boundary *_unix_ns fields are diagnostic wall time",
        ordering: "local_sequence is assigned by one writer in receive-channel order; no market-data update is coalesced or deliberately deduplicated",
        instruments,
        known_unavailable: vec![UnavailableInstrument {
            source: "bybit_pump_perp",
            reason: "Bybit reported PUMPUSDT linear perpetual status Closed immediately before this capture",
        }],
        caveats: vec![
            "Public RPC and WebSocket delivery latency is not colocated venue latency.",
            "PumpSwap vault notifications for the two sides are separate; group by slot and use decoded swap events when available.",
            "Raw records preserve upstream payloads. Analysis must validate continuity, freshness, target-size depth, and fees.",
            "delivery_semantics=initial_snapshot_or_backfill marks startup trade history and must not be treated as causally live at receipt time.",
            "Binance quotes USDT while the other instruments quote or collateralize in USDC; stablecoin basis is not separately captured.",
        ],
    };
    let file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .with_context(|| format!("create {}", path.display()))?;
    let mut writer = BufWriter::new(file);
    serde_json::to_writer_pretty(&mut writer, &manifest).context("serialize capture manifest")?;
    writer.write_all(b"\n").context("write manifest newline")?;
    writer.flush().context("flush capture manifest")?;
    writer
        .get_ref()
        .sync_all()
        .context("sync capture manifest")?;
    Ok(())
}

fn part_path(output_path: &Path) -> Result<PathBuf> {
    let file_name = output_path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| anyhow!("output path has no UTF-8 filename"))?;
    Ok(output_path.with_file_name(format!("{file_name}.part")))
}

fn manifest_path(output_path: &Path) -> Result<PathBuf> {
    let stem = output_path
        .file_stem()
        .and_then(|value| value.to_str())
        .ok_or_else(|| anyhow!("output path has no UTF-8 stem"))?;
    Ok(output_path.with_file_name(format!("{stem}.manifest.json")))
}

fn partial_manifest_path(output_path: &Path) -> Result<PathBuf> {
    let part_path = part_path(output_path)?;
    let file_name = part_path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| anyhow!("partial output path has no UTF-8 filename"))?;
    Ok(part_path.with_file_name(format!("{file_name}.manifest.json")))
}

fn unix_timestamp_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(duration_ns)
        .unwrap_or(0)
}

fn duration_ns(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

fn duration_ms(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_recorder_config() {
        let valid = PumpRecorderConfig {
            output_path: PathBuf::from("capture.ndjson"),
            duration: Duration::from_secs(3_600),
            warmup_timeout: Duration::from_secs(60),
            flush_interval: Duration::from_secs(1),
            channel_capacity: 65_536,
        };
        valid.validate().expect("valid config");
        let mut invalid = valid.clone();
        invalid.output_path = PathBuf::from("capture.csv");
        assert!(invalid.validate().is_err());
        invalid = valid;
        invalid.duration = Duration::ZERO;
        assert!(invalid.validate().is_err());
    }

    #[test]
    fn classifies_hyperliquid_coins() {
        let bbo = json!({"channel":"bbo","data":{"coin":"@188"}});
        assert_eq!(hyperliquid_coin(&bbo, "bbo"), Some("@188"));
        let trades = json!({"channel":"trades","data":[{"coin":"PUMP"}]});
        assert_eq!(hyperliquid_coin(&trades, "trades"), Some("PUMP"));
    }

    #[test]
    fn tags_only_initial_trade_backfills() {
        let mut hyperliquid_batches = BTreeSet::new();
        assert_eq!(
            hyperliquid_delivery_semantics(
                "trades",
                HYPERLIQUID_SPOT_SOURCE,
                &mut hyperliquid_batches
            ),
            Some(INITIAL_SNAPSHOT_OR_BACKFILL)
        );
        assert_eq!(
            hyperliquid_delivery_semantics(
                "trades",
                HYPERLIQUID_SPOT_SOURCE,
                &mut hyperliquid_batches
            ),
            None
        );
        assert_eq!(
            hyperliquid_delivery_semantics(
                "trades",
                HYPERLIQUID_PERP_SOURCE,
                &mut hyperliquid_batches
            ),
            Some(INITIAL_SNAPSHOT_OR_BACKFILL)
        );
        assert_eq!(
            lighter_delivery_semantics("subscribed/trade", "trade"),
            Some(INITIAL_SNAPSHOT_OR_BACKFILL)
        );
        assert_eq!(lighter_delivery_semantics("update/trade", "trade"), None);
    }

    #[test]
    fn derives_noncolliding_paths() {
        let output = Path::new("data/capture.ndjson");
        assert_eq!(
            part_path(output).unwrap(),
            Path::new("data/capture.ndjson.part")
        );
        assert_eq!(
            manifest_path(output).unwrap(),
            Path::new("data/capture.manifest.json")
        );
        assert_eq!(
            partial_manifest_path(output).unwrap(),
            Path::new("data/capture.ndjson.part.manifest.json")
        );
    }

    #[tokio::test]
    async fn cancelled_capture_retains_partial_artifact_and_manifest() {
        let directory = std::env::temp_dir().join(format!(
            "ala-crate-pump-recorder-cancelled-{}",
            Uuid::new_v4()
        ));
        fs::create_dir_all(&directory).expect("create test directory");
        let output = directory.join("capture.ndjson");
        let cancellation = CancellationToken::new();
        cancellation.cancel();

        let summary = run_pump_recorder(
            PumpRecorderConfig {
                output_path: output.clone(),
                duration: Duration::from_secs(1),
                warmup_timeout: Duration::from_secs(1),
                flush_interval: Duration::from_millis(10),
                channel_capacity: 1_024,
            },
            cancellation,
        )
        .await
        .expect("cancelled capture should finalize cleanly");

        let expected_part = part_path(&output).expect("part path");
        let expected_manifest = partial_manifest_path(&output).expect("partial manifest path");
        assert_eq!(summary.output_path, expected_part);
        assert_eq!(summary.manifest_path, expected_manifest);
        assert_eq!(summary.termination_reason, "cancelled");
        assert!(summary.termination_detail.is_none());
        assert!(!summary.completed_requested_duration);
        assert!(summary.ready_monotonic_ns.is_none());
        assert!(summary.deadline_monotonic_ns.is_none());
        assert_eq!(summary.captured_duration_ms, 0);
        assert!(summary.output_path.exists());
        assert!(summary.manifest_path.exists());
        assert!(!output.exists());

        let manifest: Value = serde_json::from_slice(
            &fs::read(&summary.manifest_path).expect("read partial manifest"),
        )
        .expect("parse partial manifest");
        assert_eq!(manifest["termination_reason"], "cancelled");
        assert_eq!(manifest["completed_requested_duration"], false);
        assert!(manifest["ready_monotonic_ns"].is_null());

        fs::remove_file(&summary.output_path).expect("remove partial artifact");
        fs::remove_file(&summary.manifest_path).expect("remove partial manifest");
        fs::remove_dir(&directory).expect("remove test directory");
    }
}
