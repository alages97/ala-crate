//! Raw mainnet capture for Kuru/Monad arbitrage and price-discovery research.
//!
//! Every upstream payload is retained. WebSocket receive timestamps are taken
//! before JSON decoding from one process-local monotonic clock. HTTP quote
//! records additionally carry request start, response completion, and latency.

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
use tokio::sync::{mpsc, RwLock};
use tokio::task::JoinSet;
use tokio::time::{self, MissedTickBehavior};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

pub const KURU_CAPTURE_SCHEMA_VERSION: u16 = 1;

pub use crate::kuru_chain::{KURU_MON_USDC_MARKET, USDC_ADDRESS, WMON_ADDRESS};

const KURU_SOURCE: &str = "kuru_mon_usdc_book";
const FLOW_SOURCE: &str = "kuru_flow_mon_usdc_quote";
const BINANCE_SOURCE: &str = "binance_mon_perp";
const BYBIT_SOURCE: &str = "bybit_mon_perp";
const FX_SOURCE: &str = "binance_usdc_usdt_spot";
const MONAD_SOURCE: &str = "monad_blocks";
const UNISWAP_V4_SOURCE: &str = "uniswap_v4_mon_usdc_quote";
const RECORDER_SOURCE: &str = "recorder_health";
const SOURCE_STALE_AFTER: Duration = Duration::from_secs(30);

const KURU_WS_URL: &str = "wss://ws.kuru.io/";
const KURU_FLOW_URL: &str = "https://ws.kuru.io/api";
const BINANCE_MON_WS_URL: &str =
    "wss://fstream.binance.com/public/stream?streams=monusdt@depth20@100ms/monusdt@aggTrade";
const BYBIT_WS_URL: &str = "wss://stream.bybit.com/v5/public/linear";
const BINANCE_FX_WS_URL: &str = "wss://stream.binance.com:9443/ws/usdcusdt@bookTicker";
const MONAD_WS_URL: &str = "wss://rpc.monad.xyz";
const MONAD_HTTP_URL: &str = "https://rpc.monad.xyz";
const READ_ONLY_USER: &str = "0x1111111111111111111111111111111111111111";

const REQUIRED_SOURCES: [&str; 7] = [
    KURU_SOURCE,
    FLOW_SOURCE,
    BINANCE_SOURCE,
    BYBIT_SOURCE,
    FX_SOURCE,
    MONAD_SOURCE,
    UNISWAP_V4_SOURCE,
];

#[derive(Debug, Clone)]
pub struct KuruRecorderConfig {
    pub output_path: PathBuf,
    pub duration: Duration,
    pub warmup_timeout: Duration,
    pub flush_interval: Duration,
    pub channel_capacity: usize,
    pub flow_quote_interval: Duration,
    pub quote_notional_usdc: u64,
}

impl KuruRecorderConfig {
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
        if self.flow_quote_interval < Duration::from_millis(1_100)
            || self.flow_quote_interval > Duration::from_secs(60)
        {
            bail!("Flow quote interval must be between 1,100 milliseconds and 60 seconds");
        }
        if self.quote_notional_usdc == 0
            || self.quote_notional_usdc.checked_mul(1_000_000).is_none()
        {
            bail!("quote notional USDC must be positive and fit raw six-decimal units");
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
pub struct KuruCaptureSummary {
    pub schema_version: u16,
    pub capture_id: String,
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

#[derive(Debug)]
struct PendingRecord {
    record_type: &'static str,
    source: &'static str,
    stream: String,
    connection_generation: u64,
    receive_unix_ns: u64,
    receive_monotonic_ns: u64,
    decode_done_monotonic_ns: u64,
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
    writer_dequeue_monotonic_ns: u64,
    decode_latency_ns: u64,
    writer_queue_latency_ns: u64,
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
        (
            unix_timestamp_ns(),
            duration_ns(instant.duration_since(self.origin)),
            instant,
        )
    }

    fn monotonic_ns_at(&self, instant: Instant) -> u64 {
        duration_ns(instant.duration_since(self.origin))
    }

    fn elapsed_ns(&self) -> u64 {
        duration_ns(self.origin.elapsed())
    }
}

pub async fn run_kuru_recorder(
    config: KuruRecorderConfig,
    cancellation: CancellationToken,
) -> Result<KuruCaptureSummary> {
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
    let capture_id = format!("kuru-monad-{}", Uuid::new_v4());
    let clock = Arc::new(CaptureClock::new());
    let (started_unix_ns, started_monotonic_ns, _) = clock.boundary_stamp();
    let (sender, mut receiver) = mpsc::channel(config.channel_capacity);
    let latest_monad_block = Arc::new(RwLock::new(None::<String>));
    let mut tasks = JoinSet::new();
    tasks.spawn(supervise_kuru_book(
        sender.clone(),
        Arc::clone(&clock),
        cancellation.clone(),
    ));
    tasks.spawn(supervise_flow_quotes(
        sender.clone(),
        Arc::clone(&clock),
        cancellation.clone(),
        config.flow_quote_interval,
        config.quote_notional_usdc,
    ));
    tasks.spawn(supervise_binance_mon(
        sender.clone(),
        Arc::clone(&clock),
        cancellation.clone(),
    ));
    tasks.spawn(supervise_bybit_mon(
        sender.clone(),
        Arc::clone(&clock),
        cancellation.clone(),
    ));
    tasks.spawn(supervise_binance_fx(
        sender.clone(),
        Arc::clone(&clock),
        cancellation.clone(),
    ));
    tasks.spawn(supervise_monad_blocks(
        sender.clone(),
        Arc::clone(&clock),
        cancellation.clone(),
        Arc::clone(&latest_monad_block),
    ));
    tasks.spawn(supervise_uniswap_v4_quotes(
        sender.clone(),
        Arc::clone(&clock),
        cancellation.clone(),
        config.flow_quote_interval,
        config.quote_notional_usdc,
        latest_monad_block,
    ));
    drop(sender);

    let mut local_sequence = 0_u64;
    let mut ready_sources = BTreeSet::new();
    let mut last_data_by_source = BTreeMap::<&'static str, u64>::new();
    let mut reported_stale_sources = BTreeSet::<&'static str>::new();
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
    let mut health_timer = time::interval(Duration::from_secs(5));
    health_timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
    health_timer.tick().await;
    let mut data_records_by_source = BTreeMap::<String, u64>::new();
    let mut status_records_by_source = BTreeMap::<String, u64>::new();

    let (termination_reason, terminal_error) = loop {
        tokio::select! {
            _ = cancellation.cancelled() => break ("cancelled", None),
            _ = &mut warmup_deadline, if !completion_armed => {
                let now = clock.elapsed_ns();
                let missing: Vec<_> = REQUIRED_SOURCES.iter().copied()
                    .filter(|source| {
                        !ready_sources.contains(source)
                            || last_data_by_source.get(source).is_none_or(|last| {
                                now.saturating_sub(*last) > duration_ns(SOURCE_STALE_AFTER)
                            })
                    }).collect();
                break ("warmup_timeout", Some(anyhow!(
                    "capture warmup timed out; missing live data from {}", missing.join(", ")
                )));
            }
            _ = &mut completion_deadline, if completion_armed => {
                break ("completed_requested_duration", None);
            }
            _ = flush_timer.tick() => {
                writer.flush().context("periodically flush capture")?;
            }
            _ = health_timer.tick(), if completion_armed => {
                let now = clock.elapsed_ns();
                let stale_now: BTreeSet<_> = REQUIRED_SOURCES.iter().copied()
                    .filter(|source| last_data_by_source.get(source).is_none_or(|last| {
                        now.saturating_sub(*last) > duration_ns(SOURCE_STALE_AFTER)
                    }))
                    .collect();
                for &source in stale_now.difference(&reported_stale_sources) {
                    let age_ns = last_data_by_source.get(source)
                        .map(|last| now.saturating_sub(*last));
                    let record = internal_status(&clock, "source_stale", json!({
                        "source":source,
                        "age_ns":age_ns,
                        "stale_after_ns":duration_ns(SOURCE_STALE_AFTER)
                    }));
                    write_record(&mut writer, &capture_id, &clock, &mut local_sequence, &record)?;
                    *status_records_by_source.entry(RECORDER_SOURCE.to_owned()).or_default() += 1;
                    tracing::warn!(source, ?age_ns, "capture source is stale");
                }
                for &source in reported_stale_sources.difference(&stale_now) {
                    let record = internal_status(&clock, "source_recovered", json!({"source":source}));
                    write_record(&mut writer, &capture_id, &clock, &mut local_sequence, &record)?;
                    *status_records_by_source.entry(RECORDER_SOURCE.to_owned()).or_default() += 1;
                    tracing::info!(source, "capture source recovered");
                }
                reported_stale_sources = stale_now;
            }
            maybe_record = receiver.recv() => {
                let Some(record) = maybe_record else {
                    if cancellation.is_cancelled() { break ("cancelled", None); }
                    break ("all_supervisors_exited", Some(anyhow!(
                        "all recorder tasks exited before capture completed"
                    )));
                };
                write_record(&mut writer, &capture_id, &clock, &mut local_sequence, &record)?;
                let counts = if record.record_type == "data" {
                    if is_readiness_record(&record) {
                        ready_sources.insert(record.source);
                        last_data_by_source.insert(record.source, record.receive_monotonic_ns);
                    }
                    &mut data_records_by_source
                } else {
                    if !completion_armed && record.stream == "disconnected" {
                        ready_sources.remove(record.source);
                    }
                    &mut status_records_by_source
                };
                *counts.entry(record.source.to_owned()).or_default() += 1;

                let now = clock.elapsed_ns();
                if !completion_armed && REQUIRED_SOURCES.iter().all(|source| {
                    ready_sources.contains(source)
                        && last_data_by_source.get(source).is_some_and(|last| {
                            now.saturating_sub(*last) <= duration_ns(SOURCE_STALE_AFTER)
                        })
                })
                {
                    let (ready_unix, ready_mono, ready_instant) = clock.boundary_stamp();
                    let deadline = ready_instant + config.duration;
                    completion_deadline.as_mut().reset(time::Instant::from_std(deadline));
                    completion_armed = true;
                    ready_unix_ns = Some(ready_unix);
                    ready_monotonic_ns = Some(ready_mono);
                    deadline_monotonic_ns = Some(clock.monotonic_ns_at(deadline));
                    tracing::info!(capture_id, duration_seconds = config.duration.as_secs(),
                        "all Kuru/Monad sources ready; timed capture started");
                }
            }
            joined = tasks.join_next(), if !tasks.is_empty() => {
                match joined {
                    Some(Ok(Ok(()))) if cancellation.is_cancelled() => break ("cancelled", None),
                    Some(Ok(Ok(()))) => break ("supervisor_exited", Some(anyhow!(
                        "a recorder supervisor exited unexpectedly"
                    ))),
                    Some(Ok(Err(error))) => break ("supervisor_failed", Some(
                        error.context("recorder supervisor failed")
                    )),
                    Some(Err(error)) => break ("supervisor_task_failed", Some(anyhow!(
                        "recorder task failed: {error}"
                    ))),
                    None => {}
                }
            }
        }
    };

    let (ended_unix_ns, ended_monotonic_ns) = clock.stamp();
    cancellation.cancel();
    shutdown_tasks(&mut tasks).await;
    while let Ok(record) = receiver.try_recv() {
        write_record(
            &mut writer,
            &capture_id,
            &clock,
            &mut local_sequence,
            &record,
        )?;
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
                "rename {} to {}",
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
    let summary = KuruCaptureSummary {
        schema_version: KURU_CAPTURE_SCHEMA_VERSION,
        capture_id,
        output_path: artifact_path,
        requested_output_path: config.output_path.clone(),
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
    write_manifest(&retained_manifest_path, &summary, &config)?;
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
    clock: &CaptureClock,
    local_sequence: &mut u64,
    record: &PendingRecord,
) -> Result<()> {
    *local_sequence = local_sequence
        .checked_add(1)
        .ok_or_else(|| anyhow!("capture sequence exhausted"))?;
    let writer_dequeue_monotonic_ns = clock.elapsed_ns();
    let encoded = CaptureRecord {
        schema_version: KURU_CAPTURE_SCHEMA_VERSION,
        capture_id,
        local_sequence: *local_sequence,
        record_type: record.record_type,
        source: record.source,
        stream: &record.stream,
        connection_generation: record.connection_generation,
        receive_unix_ns: record.receive_unix_ns,
        receive_monotonic_ns: record.receive_monotonic_ns,
        decode_done_monotonic_ns: record.decode_done_monotonic_ns,
        writer_dequeue_monotonic_ns,
        decode_latency_ns: record
            .decode_done_monotonic_ns
            .saturating_sub(record.receive_monotonic_ns),
        writer_queue_latency_ns: writer_dequeue_monotonic_ns
            .saturating_sub(record.decode_done_monotonic_ns),
        payload: &record.payload,
    };
    serde_json::to_writer(&mut *writer, &encoded).context("serialize capture record")?;
    writer.write_all(b"\n").context("write capture newline")?;
    Ok(())
}

fn is_readiness_record(record: &PendingRecord) -> bool {
    matches!(
        (record.source, record.stream.as_str()),
        (KURU_SOURCE, "order_book_snapshot" | "order_book_update")
            | (FLOW_SOURCE, "usdc_to_mon" | "mon_to_usdc")
            | (BINANCE_SOURCE, "depth20")
            | (BYBIT_SOURCE, "orderbook")
            | (FX_SOURCE, "book_ticker")
            | (MONAD_SOURCE, "new_heads")
            | (UNISWAP_V4_SOURCE, "usdc_to_mon" | "mon_to_usdc")
    )
}

async fn shutdown_tasks(tasks: &mut JoinSet<Result<()>>) {
    if time::timeout(Duration::from_secs(5), async {
        while tasks.join_next().await.is_some() {}
    })
    .await
    .is_err()
    {
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
    }
}

async fn supervise_kuru_book(
    sender: mpsc::Sender<PendingRecord>,
    clock: Arc<CaptureClock>,
    cancellation: CancellationToken,
) -> Result<()> {
    supervise(
        &sender,
        &clock,
        &cancellation,
        KURU_SOURCE,
        json!({"endpoint":KURU_WS_URL,"market":KURU_MON_USDC_MARKET}),
        |generation| run_kuru_book_once(&sender, &clock, generation, &cancellation),
    )
    .await
}

async fn run_kuru_book_once(
    sender: &mpsc::Sender<PendingRecord>,
    clock: &CaptureClock,
    generation: u64,
    cancellation: &CancellationToken,
) -> Result<()> {
    let (stream, _) = connect_async(KURU_WS_URL)
        .await
        .context("connect Kuru frontend order book")?;
    let (mut writer, mut reader) = stream.split();
    writer
        .send(Message::Text(
            json!({
                "type":"subscribe",
                "channel":"frontendOrderbook",
                "market":KURU_MON_USDC_MARKET.to_ascii_lowercase()
            })
            .to_string()
            .into(),
        ))
        .await
        .context("subscribe Kuru frontend order book")?;
    send_status(
        sender,
        clock,
        KURU_SOURCE,
        "connected",
        generation,
        json!({"endpoint":KURU_WS_URL,"market":KURU_MON_USDC_MARKET}),
    )
    .await?;
    let mut heartbeat = time::interval(Duration::from_secs(20));
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
    heartbeat.tick().await;
    loop {
        tokio::select! {
            _ = cancellation.cancelled() => return Ok(()),
            _ = heartbeat.tick() => {
                writer.send(Message::Ping(Vec::new().into())).await?;
            }
            message = reader.next() => {
                let message = message.ok_or_else(|| anyhow!("Kuru WebSocket closed"))?
                    .context("read Kuru WebSocket")?;
                let (receive_unix_ns, receive_monotonic_ns) = clock.stamp();
                match message {
                    Message::Text(text) => {
                        let payload: Value = serde_json::from_str(text.as_ref())
                            .context("decode Kuru order-book payload")?;
                        let snapshot = payload.get("type").and_then(Value::as_str) == Some("subscribed")
                            && payload.get("status").and_then(Value::as_str) == Some("success")
                            && payload.pointer("/data/b").and_then(Value::as_array).is_some()
                            && payload.pointer("/data/a").and_then(Value::as_array).is_some();
                        let update = payload.get("b").and_then(Value::as_array).is_some()
                            && payload.get("a").and_then(Value::as_array).is_some();
                        if snapshot || update {
                            let stream = if snapshot { "order_book_snapshot" } else { "order_book_update" };
                            send_data_with_stamp(sender, clock, KURU_SOURCE, stream, generation,
                                receive_unix_ns, receive_monotonic_ns, payload).await?;
                        } else {
                            let kind = payload.get("type").and_then(Value::as_str)
                                .unwrap_or("message").to_owned();
                            if payload.get("status").and_then(Value::as_str) == Some("error") {
                                bail!("Kuru subscription error: {payload}");
                            }
                            send_status_with_stamp(sender, clock, KURU_SOURCE, &kind, generation,
                                receive_unix_ns, receive_monotonic_ns, payload).await?;
                        }
                    }
                    Message::Ping(payload) => writer.send(Message::Pong(payload)).await?,
                    Message::Pong(_) | Message::Frame(_) => {}
                    Message::Close(frame) => bail!("Kuru WebSocket closed: {frame:?}"),
                    Message::Binary(_) => bail!("unexpected binary Kuru payload"),
                }
            }
        }
    }
}

async fn supervise_flow_quotes(
    sender: mpsc::Sender<PendingRecord>,
    clock: Arc<CaptureClock>,
    cancellation: CancellationToken,
    quote_interval: Duration,
    quote_notional_usdc: u64,
) -> Result<()> {
    let client = reqwest::Client::builder()
        .user_agent("ala-crate-kuru-monad-recorder/0.1")
        .timeout(Duration::from_secs(10))
        .build()
        .context("build Kuru Flow client")?;
    let mut generation = 0_u64;
    let mut backoff = quote_interval;
    loop {
        if cancellation.is_cancelled() {
            return Ok(());
        }
        generation = generation
            .checked_add(1)
            .ok_or_else(|| anyhow!("Kuru Flow generation exhausted"))?;
        send_status(&sender, &clock, FLOW_SOURCE, "connecting", generation,
            json!({"endpoint":KURU_FLOW_URL,"minimum_request_spacing_ms":quote_interval.as_millis()})).await?;
        let result = run_flow_quotes_once(
            &client,
            &sender,
            &clock,
            generation,
            &cancellation,
            quote_interval,
            quote_notional_usdc,
        )
        .await;
        if cancellation.is_cancelled() {
            return Ok(());
        }
        send_status(
            &sender,
            &clock,
            FLOW_SOURCE,
            "disconnected",
            generation,
            json!({"error":display_result(&result)}),
        )
        .await?;
        sleep_or_cancel(backoff, &cancellation).await?;
        backoff = (backoff * 2).min(Duration::from_secs(10));
    }
}

async fn run_flow_quotes_once(
    client: &reqwest::Client,
    sender: &mpsc::Sender<PendingRecord>,
    clock: &CaptureClock,
    generation: u64,
    cancellation: &CancellationToken,
    quote_interval: Duration,
    quote_notional_usdc: u64,
) -> Result<()> {
    let auth_response = tokio::select! {
        _ = cancellation.cancelled() => return Ok(()),
        response = client.post(format!("{KURU_FLOW_URL}/generate-token"))
            .json(&json!({"user_address":READ_ONLY_USER})).send() => response,
    }
    .context("request Kuru Flow read-only token")?;
    let auth_status = auth_response.status();
    let auth: Value = auth_response
        .json()
        .await
        .context("decode Kuru Flow token response")?;
    if !auth_status.is_success() {
        bail!(
            "Kuru Flow token HTTP status {auth_status}: {}",
            redact_token(auth)
        );
    }
    let token = auth
        .get("token")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            anyhow!(
                "Kuru Flow token response has no token: {}",
                redact_token(auth.clone())
            )
        })?
        .to_owned();
    send_status(
        sender,
        clock,
        FLOW_SOURCE,
        "authenticated",
        generation,
        json!({
            "endpoint":KURU_FLOW_URL,
            "expires_at":auth.get("expires_at"),
            "rate_limit":auth.get("rate_limit")
        }),
    )
    .await?;

    let usdc_raw = quote_notional_usdc
        .checked_mul(1_000_000)
        .ok_or_else(|| anyhow!("USDC quote amount overflow"))?;
    let mut last_mon_output: Option<String> = None;
    let mut ticker = time::interval(quote_interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = cancellation.cancelled() => return Ok(()),
            _ = ticker.tick() => {}
        }
        let (stream, token_in, token_out, amount) = if let Some(mon_amount) = last_mon_output.take()
        {
            ("mon_to_usdc", WMON_ADDRESS, USDC_ADDRESS, mon_amount)
        } else {
            (
                "usdc_to_mon",
                USDC_ADDRESS,
                WMON_ADDRESS,
                usdc_raw.to_string(),
            )
        };
        let request = json!({
            "userAddress":READ_ONLY_USER,
            "tokenIn":token_in,
            "tokenOut":token_out,
            "amount":amount,
            "slippageTolerance":50,
            "autoSlippage":true
        });
        let (request_started_unix_ns, request_started_monotonic_ns) = clock.stamp();
        let response = tokio::select! {
            _ = cancellation.cancelled() => return Ok(()),
            response = client.post(format!("{KURU_FLOW_URL}/quote"))
                .bearer_auth(&token).json(&request).send() => response,
        }
        .context("request Kuru Flow quote")?;
        let http_status = response.status();
        let response_bytes = response
            .bytes()
            .await
            .context("read Kuru Flow quote response body")?;
        let (response_unix_ns, response_monotonic_ns) = clock.stamp();
        let body: Value =
            serde_json::from_slice(&response_bytes).context("decode Kuru Flow quote response")?;
        let response_decoded_monotonic_ns = clock.elapsed_ns();
        let output = body
            .get("output")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()))
            .map(str::to_owned);
        let failure = if !http_status.is_success() {
            Some(format!("Kuru Flow quote HTTP status {http_status}: {body}"))
        } else if body.get("status").and_then(Value::as_str) != Some("success") {
            Some(format!("Kuru Flow quote failed: {body}"))
        } else if output.is_none() {
            Some(format!("Kuru Flow quote has invalid output: {body}"))
        } else {
            None
        };
        let payload = json!({
            "request":request,
            "response":body,
            "http_status":http_status.as_u16(),
            "request_started_unix_ns":request_started_unix_ns,
            "request_started_monotonic_ns":request_started_monotonic_ns,
            "response_body_received_unix_ns":response_unix_ns,
            "response_body_received_monotonic_ns":response_monotonic_ns,
            "response_decoded_monotonic_ns":response_decoded_monotonic_ns,
            "request_duration_ns":response_monotonic_ns.saturating_sub(request_started_monotonic_ns),
            "response_decode_latency_ns":response_decoded_monotonic_ns.saturating_sub(response_monotonic_ns)
        });
        if let Some(failure) = failure {
            send_status_with_stamp(
                sender,
                clock,
                FLOW_SOURCE,
                "quote_error",
                generation,
                response_unix_ns,
                response_monotonic_ns,
                payload,
            )
            .await?;
            bail!(failure);
        }
        let output = output.expect("validated Flow output");
        if stream == "usdc_to_mon" {
            last_mon_output = Some(output);
        }
        send_data_with_stamp(
            sender,
            clock,
            FLOW_SOURCE,
            stream,
            generation,
            response_unix_ns,
            response_monotonic_ns,
            payload,
        )
        .await?;
    }
}

fn redact_token(mut payload: Value) -> Value {
    if let Some(object) = payload.as_object_mut() {
        if object.contains_key("token") {
            object.insert("token".to_owned(), Value::String("[redacted]".to_owned()));
        }
    }
    payload
}

async fn supervise_uniswap_v4_quotes(
    sender: mpsc::Sender<PendingRecord>,
    clock: Arc<CaptureClock>,
    cancellation: CancellationToken,
    quote_interval: Duration,
    quote_notional_usdc: u64,
    latest_monad_block: Arc<RwLock<Option<String>>>,
) -> Result<()> {
    let client = reqwest::Client::builder()
        .user_agent("ala-crate-kuru-monad-recorder/0.1")
        .timeout(Duration::from_secs(10))
        .build()
        .context("build Monad Uniswap v4 quote client")?;
    let mut generation = 0_u64;
    let mut backoff = quote_interval;
    loop {
        if cancellation.is_cancelled() {
            return Ok(());
        }
        generation = generation
            .checked_add(1)
            .ok_or_else(|| anyhow!("Uniswap v4 quote generation exhausted"))?;
        send_status(
            &sender,
            &clock,
            UNISWAP_V4_SOURCE,
            "connecting",
            generation,
            json!({
                "rpc":MONAD_HTTP_URL,
                "quoter":crate::monad_v4::QUOTER_ADDRESS,
                "pool_manager":crate::monad_v4::POOL_MANAGER_ADDRESS,
                "minimum_request_spacing_ms":quote_interval.as_millis()
            }),
        )
        .await?;
        let result = run_uniswap_v4_quotes_once(
            &client,
            &sender,
            &clock,
            generation,
            &cancellation,
            quote_interval,
            quote_notional_usdc,
            &latest_monad_block,
        )
        .await;
        if cancellation.is_cancelled() {
            return Ok(());
        }
        send_status(
            &sender,
            &clock,
            UNISWAP_V4_SOURCE,
            "disconnected",
            generation,
            json!({"error":display_result(&result)}),
        )
        .await?;
        sleep_or_cancel(backoff, &cancellation).await?;
        backoff = (backoff * 2).min(Duration::from_secs(10));
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_uniswap_v4_quotes_once(
    client: &reqwest::Client,
    sender: &mpsc::Sender<PendingRecord>,
    clock: &CaptureClock,
    generation: u64,
    cancellation: &CancellationToken,
    quote_interval: Duration,
    quote_notional_usdc: u64,
    latest_monad_block: &RwLock<Option<String>>,
) -> Result<()> {
    send_status(
        sender,
        clock,
        UNISWAP_V4_SOURCE,
        "connected",
        generation,
        json!({
            "rpc":MONAD_HTTP_URL,
            "pool_fee_hundredths_of_bp":crate::monad_v4::POOL_FEE,
            "tick_spacing":crate::monad_v4::POOL_TICK_SPACING
        }),
    )
    .await?;
    let usdc_raw = quote_notional_usdc
        .checked_mul(1_000_000)
        .ok_or_else(|| anyhow!("USDC quote amount overflow"))?;
    let mut last_mon_output: Option<u128> = None;
    let mut ticker = time::interval(quote_interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = cancellation.cancelled() => return Ok(()),
            _ = ticker.tick() => {}
        }
        let (stream, zero_for_one, amount_in) = if let Some(mon_amount) = last_mon_output.take() {
            ("mon_to_usdc", true, mon_amount)
        } else {
            ("usdc_to_mon", false, u128::from(usdc_raw))
        };
        let Some(block_tag) = latest_monad_block.read().await.clone() else {
            continue;
        };
        let (request_started_unix_ns, request_started_monotonic_ns) = clock.stamp();
        let quote = tokio::select! {
            _ = cancellation.cancelled() => return Ok(()),
            quote = crate::monad_v4::quote_exact_input_single(
                client, MONAD_HTTP_URL, zero_for_one, amount_in, Some(&block_tag)
            ) => quote,
        }
        .context("quote Monad Uniswap v4 MON/USDC")?;
        let (response_unix_ns, response_monotonic_ns) = clock.stamp();
        let quote_ok = quote.get("ok").and_then(Value::as_bool) == Some(true);
        let amount_out = quote
            .get("amount_out")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let failure = (!quote_ok || amount_out.is_none())
            .then(|| format!("Uniswap v4 quote failed: {quote}"));
        let payload = json!({
            "quote":quote,
            "request_started_unix_ns":request_started_unix_ns,
            "request_started_monotonic_ns":request_started_monotonic_ns,
            "response_decoded_unix_ns":response_unix_ns,
            "response_decoded_monotonic_ns":response_monotonic_ns,
            "request_and_decode_duration_ns":response_monotonic_ns.saturating_sub(request_started_monotonic_ns)
        });
        if let Some(failure) = failure {
            send_status_with_stamp(
                sender,
                clock,
                UNISWAP_V4_SOURCE,
                "quote_error",
                generation,
                response_unix_ns,
                response_monotonic_ns,
                payload,
            )
            .await?;
            bail!(failure);
        }
        let amount_out = amount_out.expect("validated Uniswap v4 output");
        if stream == "usdc_to_mon" {
            last_mon_output = Some(
                amount_out
                    .parse::<u128>()
                    .context("Uniswap v4 MON output does not fit u128")?,
            );
        }
        send_data_with_stamp(
            sender,
            clock,
            UNISWAP_V4_SOURCE,
            stream,
            generation,
            response_unix_ns,
            response_monotonic_ns,
            payload,
        )
        .await?;
    }
}

async fn supervise_binance_mon(
    sender: mpsc::Sender<PendingRecord>,
    clock: Arc<CaptureClock>,
    cancellation: CancellationToken,
) -> Result<()> {
    supervise(
        &sender,
        &clock,
        &cancellation,
        BINANCE_SOURCE,
        json!({"endpoint":BINANCE_MON_WS_URL,"symbol":"MONUSDT"}),
        |generation| run_binance_mon_once(&sender, &clock, generation, &cancellation),
    )
    .await
}

async fn run_binance_mon_once(
    sender: &mpsc::Sender<PendingRecord>,
    clock: &CaptureClock,
    generation: u64,
    cancellation: &CancellationToken,
) -> Result<()> {
    let (stream, _) = connect_async(BINANCE_MON_WS_URL)
        .await
        .context("connect Binance MON futures streams")?;
    let (mut writer, mut reader) = stream.split();
    send_status(
        sender,
        clock,
        BINANCE_SOURCE,
        "connected",
        generation,
        json!({"endpoint":BINANCE_MON_WS_URL,"symbol":"MONUSDT"}),
    )
    .await?;
    loop {
        let message = tokio::select! {
            _ = cancellation.cancelled() => return Ok(()),
            message = reader.next() => message,
        };
        let message = message
            .ok_or_else(|| anyhow!("Binance MON WebSocket closed"))?
            .context("read Binance MON WebSocket")?;
        let (receive_unix_ns, receive_monotonic_ns) = clock.stamp();
        match message {
            Message::Text(text) => {
                let payload: Value =
                    serde_json::from_str(text.as_ref()).context("decode Binance MON payload")?;
                let stream_name = payload
                    .get("stream")
                    .and_then(Value::as_str)
                    .ok_or_else(|| anyhow!("Binance combined payload has no stream"))?;
                let symbol = payload.pointer("/data/s").and_then(Value::as_str);
                if symbol != Some("MONUSDT") {
                    bail!("unexpected Binance MON symbol {symbol:?}");
                }
                let normalized_stream = if stream_name.ends_with("@depth20@100ms") {
                    "depth20"
                } else if stream_name.ends_with("@aggTrade") {
                    "agg_trade"
                } else {
                    bail!("unexpected Binance MON stream {stream_name}");
                };
                send_data_with_stamp(
                    sender,
                    clock,
                    BINANCE_SOURCE,
                    normalized_stream,
                    generation,
                    receive_unix_ns,
                    receive_monotonic_ns,
                    payload,
                )
                .await?;
            }
            Message::Ping(payload) => writer.send(Message::Pong(payload)).await?,
            Message::Pong(_) | Message::Frame(_) => {}
            Message::Close(frame) => bail!("Binance MON WebSocket closed: {frame:?}"),
            Message::Binary(_) => bail!("unexpected binary Binance MON payload"),
        }
    }
}

async fn supervise_bybit_mon(
    sender: mpsc::Sender<PendingRecord>,
    clock: Arc<CaptureClock>,
    cancellation: CancellationToken,
) -> Result<()> {
    supervise(
        &sender,
        &clock,
        &cancellation,
        BYBIT_SOURCE,
        json!({"endpoint":BYBIT_WS_URL,"symbol":"MONUSDT"}),
        |generation| run_bybit_mon_once(&sender, &clock, generation, &cancellation),
    )
    .await
}

async fn run_bybit_mon_once(
    sender: &mpsc::Sender<PendingRecord>,
    clock: &CaptureClock,
    generation: u64,
    cancellation: &CancellationToken,
) -> Result<()> {
    let (stream, _) = connect_async(BYBIT_WS_URL)
        .await
        .context("connect Bybit MON streams")?;
    let (mut writer, mut reader) = stream.split();
    writer
        .send(Message::Text(
            json!({
                "op":"subscribe",
                "args":["orderbook.50.MONUSDT","publicTrade.MONUSDT"]
            })
            .to_string()
            .into(),
        ))
        .await
        .context("subscribe Bybit MON streams")?;
    send_status(
        sender,
        clock,
        BYBIT_SOURCE,
        "connected",
        generation,
        json!({"endpoint":BYBIT_WS_URL,"symbol":"MONUSDT"}),
    )
    .await?;
    let mut heartbeat = time::interval(Duration::from_secs(20));
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
    heartbeat.tick().await;
    loop {
        tokio::select! {
            _ = cancellation.cancelled() => return Ok(()),
            _ = heartbeat.tick() => {
                writer.send(Message::Text(json!({"op":"ping"}).to_string().into())).await?;
            }
            message = reader.next() => {
                let message = message.ok_or_else(|| anyhow!("Bybit MON WebSocket closed"))?
                    .context("read Bybit MON WebSocket")?;
                let (receive_unix_ns, receive_monotonic_ns) = clock.stamp();
                match message {
                    Message::Text(text) => {
                        let payload: Value = serde_json::from_str(text.as_ref())
                            .context("decode Bybit MON payload")?;
                        if let Some(topic) = payload.get("topic").and_then(Value::as_str) {
                            let stream = match topic {
                                "orderbook.50.MONUSDT" => "orderbook",
                                "publicTrade.MONUSDT" => "trade",
                                other => bail!("unexpected Bybit topic {other}"),
                            };
                            send_data_with_stamp(sender, clock, BYBIT_SOURCE, stream, generation,
                                receive_unix_ns, receive_monotonic_ns, payload).await?;
                        } else if payload.get("op").and_then(Value::as_str) != Some("pong") {
                            if payload.get("success").and_then(Value::as_bool) == Some(false) {
                                bail!("Bybit subscription failed: {payload}");
                            }
                            let kind = payload.get("op").and_then(Value::as_str)
                                .unwrap_or("message").to_owned();
                            send_status_with_stamp(sender, clock, BYBIT_SOURCE, &kind, generation,
                                receive_unix_ns, receive_monotonic_ns, payload).await?;
                        }
                    }
                    Message::Ping(payload) => writer.send(Message::Pong(payload)).await?,
                    Message::Pong(_) | Message::Frame(_) => {}
                    Message::Close(frame) => bail!("Bybit MON WebSocket closed: {frame:?}"),
                    Message::Binary(_) => bail!("unexpected binary Bybit MON payload"),
                }
            }
        }
    }
}

async fn supervise_binance_fx(
    sender: mpsc::Sender<PendingRecord>,
    clock: Arc<CaptureClock>,
    cancellation: CancellationToken,
) -> Result<()> {
    supervise(
        &sender,
        &clock,
        &cancellation,
        FX_SOURCE,
        json!({"endpoint":BINANCE_FX_WS_URL,"symbol":"USDCUSDT"}),
        |generation| run_binance_fx_once(&sender, &clock, generation, &cancellation),
    )
    .await
}

async fn run_binance_fx_once(
    sender: &mpsc::Sender<PendingRecord>,
    clock: &CaptureClock,
    generation: u64,
    cancellation: &CancellationToken,
) -> Result<()> {
    let (stream, _) = connect_async(BINANCE_FX_WS_URL)
        .await
        .context("connect Binance USDCUSDT book ticker")?;
    let (mut writer, mut reader) = stream.split();
    send_status(
        sender,
        clock,
        FX_SOURCE,
        "connected",
        generation,
        json!({"endpoint":BINANCE_FX_WS_URL,"symbol":"USDCUSDT"}),
    )
    .await?;
    loop {
        let message = tokio::select! {
            _ = cancellation.cancelled() => return Ok(()),
            message = reader.next() => message,
        };
        let message = message
            .ok_or_else(|| anyhow!("Binance FX WebSocket closed"))?
            .context("read Binance FX WebSocket")?;
        let (receive_unix_ns, receive_monotonic_ns) = clock.stamp();
        match message {
            Message::Text(text) => {
                let payload: Value =
                    serde_json::from_str(text.as_ref()).context("decode Binance FX payload")?;
                if payload.get("s").and_then(Value::as_str) != Some("USDCUSDT") {
                    bail!("unexpected Binance FX payload: {payload}");
                }
                send_data_with_stamp(
                    sender,
                    clock,
                    FX_SOURCE,
                    "book_ticker",
                    generation,
                    receive_unix_ns,
                    receive_monotonic_ns,
                    payload,
                )
                .await?;
            }
            Message::Ping(payload) => writer.send(Message::Pong(payload)).await?,
            Message::Pong(_) | Message::Frame(_) => {}
            Message::Close(frame) => bail!("Binance FX WebSocket closed: {frame:?}"),
            Message::Binary(_) => bail!("unexpected binary Binance FX payload"),
        }
    }
}

async fn supervise_monad_blocks(
    sender: mpsc::Sender<PendingRecord>,
    clock: Arc<CaptureClock>,
    cancellation: CancellationToken,
    latest_monad_block: Arc<RwLock<Option<String>>>,
) -> Result<()> {
    supervise(
        &sender,
        &clock,
        &cancellation,
        MONAD_SOURCE,
        json!({"endpoint":MONAD_WS_URL,"chain_id":143}),
        |generation| {
            run_monad_blocks_once(
                &sender,
                &clock,
                generation,
                &cancellation,
                &latest_monad_block,
            )
        },
    )
    .await
}

async fn run_monad_blocks_once(
    sender: &mpsc::Sender<PendingRecord>,
    clock: &CaptureClock,
    generation: u64,
    cancellation: &CancellationToken,
    latest_monad_block: &RwLock<Option<String>>,
) -> Result<()> {
    let (stream, _) = connect_async(MONAD_WS_URL)
        .await
        .context("connect Monad newHeads stream")?;
    let (mut writer, mut reader) = stream.split();
    writer
        .send(Message::Text(
            json!({
                "jsonrpc":"2.0","id":1,"method":"eth_subscribe","params":["newHeads"]
            })
            .to_string()
            .into(),
        ))
        .await
        .context("subscribe Monad newHeads")?;
    send_status(
        sender,
        clock,
        MONAD_SOURCE,
        "connected",
        generation,
        json!({"endpoint":MONAD_WS_URL,"chain_id":143}),
    )
    .await?;
    let mut heartbeat = time::interval(Duration::from_secs(20));
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
    heartbeat.tick().await;
    loop {
        tokio::select! {
            _ = cancellation.cancelled() => return Ok(()),
            _ = heartbeat.tick() => writer.send(Message::Ping(Vec::new().into())).await?,
            message = reader.next() => {
                let message = message.ok_or_else(|| anyhow!("Monad WebSocket closed"))?
                    .context("read Monad WebSocket")?;
                let (receive_unix_ns, receive_monotonic_ns) = clock.stamp();
                match message {
                    Message::Text(text) => {
                        let payload: Value = serde_json::from_str(text.as_ref())
                            .context("decode Monad payload")?;
                        if payload.get("method").and_then(Value::as_str) == Some("eth_subscription")
                            && payload.pointer("/params/result/number").and_then(Value::as_str).is_some()
                        {
                            let block_number = payload.pointer("/params/result/number")
                                .and_then(Value::as_str)
                                .expect("validated block number")
                                .to_owned();
                            *latest_monad_block.write().await = Some(block_number);
                            send_data_with_stamp(sender, clock, MONAD_SOURCE, "new_heads", generation,
                                receive_unix_ns, receive_monotonic_ns, payload).await?;
                        } else if payload.get("error").is_some() {
                            bail!("Monad subscription error: {payload}");
                        } else {
                            send_status_with_stamp(sender, clock, MONAD_SOURCE, "subscribed", generation,
                                receive_unix_ns, receive_monotonic_ns, payload).await?;
                        }
                    }
                    Message::Ping(payload) => writer.send(Message::Pong(payload)).await?,
                    Message::Pong(_) | Message::Frame(_) => {}
                    Message::Close(frame) => bail!("Monad WebSocket closed: {frame:?}"),
                    Message::Binary(_) => bail!("unexpected binary Monad payload"),
                }
            }
        }
    }
}

async fn supervise<F, Fut>(
    sender: &mpsc::Sender<PendingRecord>,
    clock: &CaptureClock,
    cancellation: &CancellationToken,
    source: &'static str,
    connection_payload: Value,
    mut run_once: F,
) -> Result<()>
where
    F: FnMut(u64) -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    let mut generation = 0_u64;
    let mut backoff = Duration::from_millis(250);
    loop {
        if cancellation.is_cancelled() {
            return Ok(());
        }
        generation = generation
            .checked_add(1)
            .ok_or_else(|| anyhow!("{source} connection generation exhausted"))?;
        send_status(
            sender,
            clock,
            source,
            "connecting",
            generation,
            connection_payload.clone(),
        )
        .await?;
        let result = run_once(generation).await;
        if cancellation.is_cancelled() {
            return Ok(());
        }
        send_status(
            sender,
            clock,
            source,
            "disconnected",
            generation,
            json!({"error":display_result(&result)}),
        )
        .await?;
        sleep_or_cancel(backoff, cancellation).await?;
        backoff = (backoff * 2).min(Duration::from_secs(10));
    }
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
    sender
        .send(PendingRecord {
            record_type: "data",
            source,
            stream: stream.to_owned(),
            connection_generation: generation,
            receive_unix_ns,
            receive_monotonic_ns,
            decode_done_monotonic_ns: clock.elapsed_ns(),
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

fn internal_status(clock: &CaptureClock, stream: &str, payload: Value) -> PendingRecord {
    let (receive_unix_ns, receive_monotonic_ns) = clock.stamp();
    PendingRecord {
        record_type: "status",
        source: RECORDER_SOURCE,
        stream: stream.to_owned(),
        connection_generation: 0,
        receive_unix_ns,
        receive_monotonic_ns,
        decode_done_monotonic_ns: clock.elapsed_ns(),
        payload,
    }
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

fn write_manifest(
    path: &Path,
    summary: &KuruCaptureSummary,
    config: &KuruRecorderConfig,
) -> Result<()> {
    let metadata = json!({
        "clock": "all *_monotonic_ns fields are elapsed std::time::Instant time from one shared process origin; *_unix_ns fields are diagnostic wall time",
        "ordering": "local_sequence is assigned by one writer in receive-channel order; no market-data update is deliberately coalesced or deduplicated",
        "latency_fields": {
            "decode_latency_ns": "decode_done_monotonic_ns - receive_monotonic_ns",
            "writer_queue_latency_ns": "writer_dequeue_monotonic_ns - decode_done_monotonic_ns",
            "request_duration_ns": "Flow HTTP response-body completion - request start",
            "request_and_decode_duration_ns": "Uniswap RPC response decoding completion - request start"
        },
        "quote_notional_usdc": config.quote_notional_usdc,
        "quote_interval_ms": duration_ms(config.flow_quote_interval),
        "instruments": [
            {
                "source":KURU_SOURCE,"venue":"Kuru CLOB + integrated AMM",
                "instrument":"native MON/USDC","market":KURU_MON_USDC_MARKET,
                "streams":["frontendOrderbook snapshot and full-book updates"],"endpoint":KURU_WS_URL
            },
            {
                "source":FLOW_SOURCE,"venue":"Kuru Flow aggregate router",
                "instrument":"USDC/WMON executable route","tokens":[USDC_ADDRESS,WMON_ADDRESS],
                "streams":["alternating exact-input round trip"],"endpoint":KURU_FLOW_URL
            },
            {
                "source":UNISWAP_V4_SOURCE,"venue":"Uniswap v4 on Monad",
                "instrument":"native MON/USDC 5 bp pool",
                "pool_id":"0x18a9fc874581f3ba12b7898f80a683c66fd5877fd74b26a85ba9a3a79c549954",
                "quoter":crate::monad_v4::QUOTER_ADDRESS,"endpoint":MONAD_HTTP_URL
            },
            {
                "source":BINANCE_SOURCE,"venue":"Binance USD-M","instrument":"MONUSDT perpetual",
                "streams":["depth20@100ms","aggTrade"],"endpoint":BINANCE_MON_WS_URL
            },
            {
                "source":BYBIT_SOURCE,"venue":"Bybit linear","instrument":"MONUSDT perpetual",
                "streams":["orderbook.50","publicTrade"],"endpoint":BYBIT_WS_URL
            },
            {
                "source":FX_SOURCE,"venue":"Binance spot","instrument":"USDCUSDT",
                "streams":["bookTicker"],"endpoint":BINANCE_FX_WS_URL
            },
            {
                "source":MONAD_SOURCE,"venue":"Monad mainnet","instrument":"chain head",
                "streams":["newHeads"],"endpoint":MONAD_WS_URL,"chain_id":143
            }
        ],
        "caveats": [
            "Public API and RPC receipt latency is not validator- or matching-engine-colocated latency.",
            "Kuru Flow is an aggregate executable-price benchmark and may route through Kuru itself; inspect transaction calldata before treating it as an independent venue.",
            "Flow authentication advertises one request per second; this capture intentionally spaces requests by at least 1,100 milliseconds and never stores its JWT.",
            "Alternating quote directions are not simultaneous. Each sell amount is the exact output of the preceding fixed-USDC buy quote.",
            "Binance and Bybit perpetuals quote USDT while Kuru and Uniswap quote USDC; normalize with the captured USDCUSDT book.",
            "Quoted output is not a guaranteed fill. Analysis must include transaction inclusion, gas, slippage limits, reverts, CEX fees, hedge fills, and inventory rebalancing.",
            "A newer chain does not itself imply a durable arbitrage edge; Monad's default transaction ordering uses a priority gas auction.",
            "recorder_health source_stale/source_recovered status records mark any required source whose usable data pauses for more than 30 seconds."
        ]
    });
    let mut manifest = serde_json::to_value(summary).context("serialize capture summary")?;
    let manifest_object = manifest
        .as_object_mut()
        .ok_or_else(|| anyhow!("capture summary did not serialize as an object"))?;
    let metadata_object = metadata
        .as_object()
        .ok_or_else(|| anyhow!("capture metadata is not an object"))?;
    manifest_object.extend(metadata_object.clone());

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
    let part = part_path(output_path)?;
    let file_name = part
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| anyhow!("partial output path has no UTF-8 filename"))?;
    Ok(part.with_file_name(format!("{file_name}.manifest.json")))
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

    fn valid_config(output_path: PathBuf) -> KuruRecorderConfig {
        KuruRecorderConfig {
            output_path,
            duration: Duration::from_secs(3_600),
            warmup_timeout: Duration::from_secs(90),
            flush_interval: Duration::from_secs(1),
            channel_capacity: 65_536,
            flow_quote_interval: Duration::from_millis(1_200),
            quote_notional_usdc: 100,
        }
    }

    #[test]
    fn validates_recorder_config() {
        let valid = valid_config(PathBuf::from("capture.ndjson"));
        valid.validate().expect("valid config");
        let mut invalid = valid.clone();
        invalid.flow_quote_interval = Duration::from_secs(1);
        assert!(invalid.validate().is_err());
        invalid = valid.clone();
        invalid.output_path = PathBuf::from("capture.csv");
        assert!(invalid.validate().is_err());
        invalid = valid;
        invalid.quote_notional_usdc = 0;
        assert!(invalid.validate().is_err());
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

    #[test]
    fn redacts_flow_token() {
        let redacted = redact_token(json!({"token":"secret","expires_at":123}));
        assert_eq!(redacted["token"], "[redacted]");
        assert_eq!(redacted["expires_at"], 123);
    }

    #[tokio::test]
    async fn cancelled_capture_retains_partial_artifact_and_manifest() {
        let directory = std::env::temp_dir().join(format!(
            "ala-crate-kuru-recorder-cancelled-{}",
            Uuid::new_v4()
        ));
        fs::create_dir_all(&directory).expect("create test directory");
        let output = directory.join("capture.ndjson");
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let summary = run_kuru_recorder(valid_config(output.clone()), cancellation)
            .await
            .expect("cancelled capture should finalize cleanly");
        assert_eq!(summary.output_path, part_path(&output).unwrap());
        assert_eq!(
            summary.manifest_path,
            partial_manifest_path(&output).unwrap()
        );
        assert_eq!(summary.termination_reason, "cancelled");
        assert!(!summary.completed_requested_duration);
        assert!(summary.output_path.exists());
        assert!(summary.manifest_path.exists());
        let manifest: Value = serde_json::from_slice(
            &fs::read(&summary.manifest_path).expect("read partial manifest"),
        )
        .expect("parse partial manifest");
        assert_eq!(manifest["termination_reason"], "cancelled");
        assert!(manifest.get("summary").is_none());
        fs::remove_file(&summary.output_path).expect("remove partial artifact");
        fs::remove_file(&summary.manifest_path).expect("remove partial manifest");
        fs::remove_dir(&directory).expect("remove test directory");
    }
}
