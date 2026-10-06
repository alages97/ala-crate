//! Raw log capture. Commitments, receipt times and conflicts are retained.
use crate::{
    decoder::Decoder,
    model::{AMM, PUMP},
    rpc::atomic_json,
};
use anyhow::{ensure, Context, Result};
use flate2::{write::GzEncoder, Compression};
use futures_util::{SinkExt, StreamExt};
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::{json, Value};
use std::{
    fs,
    io::Write,
    path::Path,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Error as WsError, Message},
};

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
pub fn open_store(path: &Path) -> Result<Connection> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let conn = Connection::open(path)?;
    conn.busy_timeout(Duration::from_secs(5))?;
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
      CREATE TABLE IF NOT EXISTS capture_sessions(id INTEGER PRIMARY KEY, started_ms INTEGER NOT NULL, ended_ms INTEGER, status TEXT NOT NULL, commitment TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS stream_messages(signature TEXT PRIMARY KEY, slot INTEGER NOT NULL, received_ms INTEGER NOT NULL, session_id INTEGER NOT NULL, failed INTEGER NOT NULL, payload_gzip BLOB NOT NULL, decode_error TEXT);
      CREATE INDEX IF NOT EXISTS stream_slot ON stream_messages(slot);
      CREATE TABLE IF NOT EXISTS stream_events(signature TEXT NOT NULL, event_index INTEGER NOT NULL, program TEXT NOT NULL, event_name TEXT NOT NULL, fields_json TEXT NOT NULL, PRIMARY KEY(signature,event_index));
      CREATE TABLE IF NOT EXISTS stream_conflicts(signature TEXT NOT NULL, received_ms INTEGER NOT NULL, prior_slot INTEGER NOT NULL, observed_slot INTEGER NOT NULL, payload_gzip BLOB NOT NULL, PRIMARY KEY(signature,received_ms,observed_slot));
      CREATE TABLE IF NOT EXISTS archive_blocks(slot INTEGER PRIMARY KEY, blockhash TEXT NOT NULL, block_time_ms INTEGER NOT NULL, batch_gzip BLOB NOT NULL);
      CREATE TABLE IF NOT EXISTS wallet_history(wallet TEXT NOT NULL, signature TEXT NOT NULL, slot INTEGER NOT NULL, block_time INTEGER, failed INTEGER NOT NULL, payload_gzip BLOB, PRIMARY KEY(wallet,signature));
      CREATE TABLE IF NOT EXISTS capture_diagnostics(session_id INTEGER PRIMARY KEY, details_json TEXT NOT NULL);")?;
    let has_totals: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='capture_totals')",
        [],
        |r| r.get(0),
    )?;
    if !has_totals {
        conn.execute_batch("BEGIN IMMEDIATE;
          CREATE TABLE IF NOT EXISTS capture_totals(id INTEGER PRIMARY KEY CHECK(id=1), messages INTEGER NOT NULL, first_slot INTEGER NOT NULL, last_slot INTEGER NOT NULL, failed INTEGER NOT NULL, errors INTEGER NOT NULL, events INTEGER NOT NULL, last_received INTEGER NOT NULL);
          INSERT OR IGNORE INTO capture_totals SELECT 1,count(*),coalesce(min(slot),0),coalesce(max(slot),0),coalesce(sum(failed),0),coalesce(sum(decode_error IS NOT NULL),0),(SELECT count(*) FROM stream_events),coalesce(max(received_ms),0) FROM stream_messages;
          CREATE TRIGGER IF NOT EXISTS capture_message_totals AFTER INSERT ON stream_messages BEGIN UPDATE capture_totals SET first_slot=CASE WHEN messages=0 THEN NEW.slot ELSE min(first_slot,NEW.slot) END,last_slot=max(last_slot,NEW.slot),messages=messages+1,failed=failed+NEW.failed,errors=errors+(NEW.decode_error IS NOT NULL),last_received=max(last_received,NEW.received_ms) WHERE id=1; END;
          CREATE TRIGGER IF NOT EXISTS capture_event_totals AFTER INSERT ON stream_events BEGIN UPDATE capture_totals SET events=events+1 WHERE id=1; END;
          COMMIT;")?;
    }
    Ok(conn)
}
pub fn gzip<T: serde::Serialize>(value: &T) -> Result<Vec<u8>> {
    let mut z = GzEncoder::new(Vec::new(), Compression::fast());
    // Compress a complete buffer: serde's tiny writes otherwise repeatedly enter
    // the deflater and can stall a busy stream in development builds.
    z.write_all(&serde_json::to_vec(value)?)?;
    Ok(z.finish()?)
}

/// Only stores/decodes observations. Never interprets arrival order as chain order.
pub fn insert_message(
    conn: &mut Connection,
    session: i64,
    payload: &Value,
    received: u64,
    decoder: &Decoder,
) -> Result<bool> {
    let tx = conn.transaction()?;
    let inserted = insert_in_transaction(&tx, session, payload, received, decoder)?;
    tx.commit()?;
    Ok(inserted)
}

fn insert_in_transaction(
    conn: &Connection,
    session: i64,
    payload: &Value,
    received: u64,
    decoder: &Decoder,
) -> Result<bool> {
    let Some(value) = payload.pointer("/params/result/value") else {
        return Ok(false);
    };
    let signature = value["signature"]
        .as_str()
        .context("log signature missing")?;
    let slot = payload
        .pointer("/params/result/context/slot")
        .and_then(Value::as_u64)
        .context("log slot missing")?;
    let existing: Option<(u64, Vec<u8>)> = conn
        .query_row(
            "SELECT slot,payload_gzip FROM stream_messages WHERE signature=?1",
            [signature],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;
    if let Some((old_slot, bytes)) = existing {
        let previous: Value =
            serde_json::from_reader(flate2::read::GzDecoder::new(bytes.as_slice()))?;
        if old_slot != slot || previous.pointer("/params/result/value") != Some(value) {
            conn.execute(
                "INSERT OR IGNORE INTO stream_conflicts VALUES(?1,?2,?3,?4,?5)",
                params![signature, received, old_slot, slot, gzip(payload)?],
            )?;
        }
        return Ok(false);
    }
    let failed = !value["err"].is_null();
    let synthetic = json!({"transaction":{"message":{"accountKeys":[PUMP,AMM]}},"meta":{"err":value["err"],"logMessages":value["logs"],"innerInstructions":[]}});
    let decoded = decoder.transaction_events(&synthetic);
    let error = decoded.as_ref().err().map(|e| e.to_string());
    let inserted=conn.execute("INSERT OR IGNORE INTO stream_messages(signature,slot,received_ms,session_id,failed,payload_gzip,decode_error) VALUES(?1,?2,?3,?4,?5,?6,?7)",params![signature,slot,received,session,failed,gzip(payload)?,error])?;
    if inserted > 0 {
        if let Ok(events) = decoded {
            for (index, e) in events.iter().enumerate() {
                conn.execute(
                    "INSERT INTO stream_events VALUES(?1,?2,?3,?4,?5)",
                    params![
                        signature,
                        index,
                        e.program,
                        e.name,
                        serde_json::to_string(&e.fields)?
                    ],
                )?;
            }
        }
    }
    Ok(inserted > 0)
}
pub fn status(conn: &Connection) -> Result<Value> {
    let totals=conn.query_row("SELECT messages,first_slot,last_slot,failed,errors,events,last_received FROM capture_totals WHERE id=1",[],|r|Ok((r.get::<_,u64>(0)?,r.get::<_,u64>(1)?,r.get::<_,u64>(2)?,r.get::<_,u64>(3)?,r.get::<_,u64>(4)?,r.get::<_,u64>(5)?,r.get::<_,u64>(6)?)))?;
    let events = totals.5;
    let mut q = conn.prepare(
        "SELECT id,started_ms,ended_ms,status,commitment FROM capture_sessions ORDER BY id",
    )?;
    let mut sessions=q.query_map([],|r|Ok(json!({"id":r.get::<_,i64>(0)?,"started_ms":r.get::<_,u64>(1)?,"ended_ms":r.get::<_,Option<u64>>(2)?,"status":r.get::<_,String>(3)?,"commitment":r.get::<_,String>(4)?})))?.collect::<rusqlite::Result<Vec<_>>>()?;
    let has_diagnostics: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='capture_diagnostics')",
        [],
        |r| r.get(0),
    )?;
    if has_diagnostics {
        for session in &mut sessions {
            let details: Option<String> = conn
                .query_row(
                    "SELECT details_json FROM capture_diagnostics WHERE session_id=?1",
                    [session["id"].as_i64().context("session id")?],
                    |r| r.get(0),
                )
                .optional()?;
            if let Some(details) = details {
                session["diagnostics"] = serde_json::from_str(&details)?;
            }
        }
    }
    let conflicts: u64 =
        conn.query_row("SELECT count(*) FROM stream_conflicts", [], |r| r.get(0))?;
    let last_received = (totals.0 > 0).then_some(totals.6);
    let pages: u64 = conn.query_row("PRAGMA page_count", [], |r| r.get(0))?;
    let page_size: u64 = conn.query_row("PRAGMA page_size", [], |r| r.get(0))?;
    Ok(
        json!({"as_of_ms":now_ms(),"last_received_ms":last_received,"database_bytes":pages*page_size,"messages":totals.0,"first_slot":totals.1,"last_slot":totals.2,"failed_transactions":totals.3,"decode_errors":totals.4,"events":events,"sessions":sessions,"conflicting_observations":conflicts,"coverage":"Log stream only; reconnects and subscription failures create potential gaps. Confirmed observations require finality reconciliation. Conflicting signatures must be excluded or reconciled. Not a complete backtest archive."}),
    )
}

/// Global program-feed liveness bounds. Shorter values are useful for local fault tests.
#[derive(Clone, Debug)]
pub struct RecorderPolicy {
    pub connect_timeout: Duration,
    pub acknowledgement_timeout: Duration,
    pub notification_timeout: Duration,
    pub ping_interval: Duration,
    pub healthy_reset_after: Duration,
    pub backoff_base: Duration,
    pub backoff_max: Duration,
    pub max_consecutive_failures: u32,
    pub flush_interval: Duration,
    pub status_interval: Duration,
    pub batch_size: usize,
}
impl Default for RecorderPolicy {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(20),
            acknowledgement_timeout: Duration::from_secs(20),
            notification_timeout: Duration::from_secs(90),
            ping_interval: Duration::from_secs(20),
            healthy_reset_after: Duration::from_secs(60),
            backoff_base: Duration::from_secs(10),
            backoff_max: Duration::from_secs(60),
            max_consecutive_failures: 10,
            flush_interval: Duration::from_millis(250),
            status_interval: Duration::from_secs(15),
            batch_size: 64,
        }
    }
}
impl RecorderPolicy {
    fn validate(&self) -> Result<()> {
        ensure!(
            self.batch_size > 0 && self.batch_size <= 1024,
            "invalid batch size"
        );
        ensure!(
            self.max_consecutive_failures > 0,
            "positive retry bound required"
        );
        ensure!(
            [
                self.connect_timeout,
                self.acknowledgement_timeout,
                self.notification_timeout,
                self.ping_interval,
                self.healthy_reset_after,
                self.backoff_base,
                self.backoff_max,
                self.flush_interval,
                self.status_interval,
            ]
            .iter()
            .all(|d| !d.is_zero()),
            "positive recorder intervals required"
        );
        ensure!(
            self.backoff_base <= self.backoff_max,
            "invalid backoff bounds"
        );
        Ok(())
    }
}

#[derive(Default)]
struct RetryState {
    consecutive: u32,
    total: u64,
}
impl RetryState {
    fn failed(
        &mut self,
        healthy: bool,
        policy: &RecorderPolicy,
        retry_after: Option<Duration>,
    ) -> Duration {
        if healthy {
            self.consecutive = 0;
        }
        self.consecutive = self.consecutive.saturating_add(1);
        self.total += 1;
        let multiplier = 1u32 << self.consecutive.saturating_sub(1).min(16);
        let delay = policy
            .backoff_base
            .saturating_mul(multiplier)
            .min(policy.backoff_max);
        // A provider's numeric Retry-After can extend our usual backoff, never the deadline.
        delay.max(
            retry_after
                .unwrap_or_default()
                .min(Duration::from_secs(3600)),
        )
    }
}
#[derive(Debug)]
struct Disconnect {
    reason: String,
    retry_after: Option<Duration>,
}
impl Disconnect {
    fn new(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
            retry_after: None,
        }
    }
}
fn ws_failure(error: WsError) -> Disconnect {
    // Keep actionable codes, never URL-bearing errors, provider bodies or close text.
    match error {
        WsError::Http(response) => Disconnect {
            reason: format!("websocket_http_{}", response.status().as_u16()),
            retry_after: response
                .headers()
                .get("retry-after")
                .and_then(|s| s.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok())
                .map(Duration::from_secs),
        },
        WsError::Io(e) => Disconnect::new(format!("websocket_io_{:?}", e.kind())),
        WsError::ConnectionClosed | WsError::AlreadyClosed => Disconnect::new("websocket_closed"),
        WsError::Tls(_) => Disconnect::new("websocket_tls_error"),
        WsError::Capacity(_) => Disconnect::new("websocket_capacity_error"),
        _ => Disconnect::new("websocket_protocol_or_handshake_error"),
    }
}
enum SessionEnd {
    Stop(&'static str),
    Disconnected(Disconnect),
}
#[derive(Default)]
struct Health {
    acknowledged_at: Option<Instant>,
    acknowledged_ms: Option<u64>,
    last_frame_ms: Option<u64>,
    last_notification_ms: Option<u64>,
    notifications: u64,
    received_text_bytes: u64,
    healthy: bool,
    max_flush_ms: u64,
    max_buffered_messages: usize,
}
impl Health {
    fn json(&self, reason: &str, retry: &RetryState, backoff: Duration) -> Value {
        json!({"reason":reason,"acknowledged_ms":self.acknowledged_ms,
            "last_frame_ms":self.last_frame_ms,"last_notification_ms":self.last_notification_ms,
            "notification_frames":self.notifications,"received_text_bytes":self.received_text_bytes,
            "healthy_session":self.healthy,"consecutive_failures":retry.consecutive,
            "total_disconnects":retry.total,"next_backoff_ms":backoff.as_millis(),
            "max_database_flush_ms":self.max_flush_ms,"max_buffered_messages":self.max_buffered_messages,
            "receipt_timestamp":"Local WebSocket text delivery before JSON decoding and database persistence; not hardware network-arrival or execution time."})
    }
}
type Pending = Vec<(Value, u64)>;
fn flush(
    conn: &mut Connection,
    session: i64,
    pending: &mut Pending,
    decoder: &Decoder,
    health: &mut Health,
) -> Result<()> {
    if pending.is_empty() {
        return Ok(());
    }
    let at = Instant::now();
    let tx = conn.transaction()?;
    for (payload, received) in pending.iter() {
        insert_in_transaction(&tx, session, payload, *received, decoder)?;
    }
    tx.commit()?;
    pending.clear();
    health.max_flush_ms = health.max_flush_ms.max(at.elapsed().as_millis() as u64);
    Ok(())
}
fn diagnostics(conn: &Connection, session: i64, value: &Value) -> Result<()> {
    conn.execute(
        "INSERT OR REPLACE INTO capture_diagnostics VALUES(?1,?2)",
        params![session, serde_json::to_string(value)?],
    )?;
    Ok(())
}
fn disk_bytes(db: &Path) -> u64 {
    [
        db.to_path_buf(),
        format!("{}-wal", db.display()).into(),
        format!("{}-shm", db.display()).into(),
    ]
    .iter()
    .filter_map(|p| fs::metadata(p).ok())
    .map(|m| m.len())
    .sum()
}

pub async fn record(
    ws_env: &str,
    db: &Path,
    seconds: u64,
    max_bytes: u64,
    status_path: &Path,
) -> Result<()> {
    record_at(ws_env, db, seconds, max_bytes, status_path, "finalized").await
}
pub async fn record_at(
    ws_env: &str,
    db: &Path,
    seconds: u64,
    max_bytes: u64,
    status_path: &Path,
    commitment: &str,
) -> Result<()> {
    record_with_policy(
        ws_env,
        db,
        Duration::from_secs(seconds),
        max_bytes,
        status_path,
        commitment,
        &RecorderPolicy::default(),
    )
    .await
}
pub async fn record_with_policy(
    ws_env: &str,
    db: &Path,
    duration: Duration,
    max_bytes: u64,
    status_path: &Path,
    commitment: &str,
    policy: &RecorderPolicy,
) -> Result<()> {
    policy.validate()?;
    ensure!(
        matches!(commitment, "confirmed" | "finalized"),
        "supported capture commitment is confirmed or finalized"
    );
    ensure!(
        !duration.is_zero() && max_bytes > 0,
        "positive duration and storage cap required"
    );
    let end = Instant::now()
        .checked_add(duration)
        .context("capture duration overflow")?;
    let _ = rustls::crypto::ring::default_provider().install_default();
    let url = std::env::var(ws_env).with_context(|| format!("set {ws_env}"))?;
    let parsed = reqwest::Url::parse(&url).map_err(|_| anyhow::anyhow!("invalid WebSocket URL"))?;
    ensure!(
        parsed.scheme() == "wss"
            || (parsed.scheme() == "ws"
                && matches!(parsed.host_str(), Some("localhost" | "127.0.0.1"))),
        "secure WebSocket required"
    );
    let mut conn = open_store(db)?;
    let mixed: u64 = conn.query_row(
        "SELECT count(*) FROM capture_sessions WHERE commitment!=?1",
        [commitment],
        |r| r.get(0),
    )?;
    ensure!(mixed == 0, "use a separate database for each commitment");
    let capture_lock = Connection::open(db.with_extension("capture-lock.sqlite"))?;
    capture_lock.busy_timeout(Duration::ZERO)?;
    capture_lock
        .execute_batch("BEGIN IMMEDIATE")
        .context("another recorder holds this database's capture lock")?;
    conn.execute(
        "UPDATE capture_sessions SET status='interrupted' WHERE ended_ms IS NULL",
        [],
    )?;
    let decoder = Decoder::default();
    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);
    let deadline = tokio::time::sleep_until(end.into());
    tokio::pin!(deadline);
    let mut retry = RetryState::default();
    let mut outcome = "deadline_reached";
    let mut fatal: Option<anyhow::Error> = None;
    while Instant::now() < end {
        if disk_bytes(db) >= max_bytes {
            outcome = "storage_cap";
            break;
        }
        conn.execute(
            "INSERT INTO capture_sessions(started_ms,status,commitment) VALUES(?1,'connecting',?2)",
            params![now_ms(), commitment],
        )?;
        let session = conn.last_insert_rowid();
        let mut health = Health::default();
        let mut pending = Vec::with_capacity(policy.batch_size);
        let result = tokio::select! {
            biased;
            _=&mut shutdown => Ok(SessionEnd::Stop("user_interrupt")),
            _=&mut deadline => Ok(SessionEnd::Stop("deadline_reached")),
            r=run_session(&url,&mut conn,session,&decoder,end,max_bytes,db,status_path,policy,&retry,&mut health,&mut pending) => r,
        };
        // The batch lives outside the cancelled future: deadline, errors and Ctrl-C all flush it.
        let result = match flush(&mut conn, session, &mut pending, &decoder, &mut health) {
            Ok(()) => result,
            Err(e) => Err(e.context("capture storage flush failed")),
        };
        let (state, reason, backoff) = match &result {
            Ok(SessionEnd::Stop(reason)) => ("closed", (*reason).to_owned(), Duration::ZERO),
            Ok(SessionEnd::Disconnected(e)) => (
                "disconnected_gap_requires_backfill",
                e.reason.clone(),
                retry.failed(health.healthy, policy, e.retry_after),
            ),
            Err(_) => (
                "storage_or_local_processing_failed",
                "storage_or_local_processing_failed".to_owned(),
                Duration::ZERO,
            ),
        };
        conn.execute(
            "UPDATE capture_sessions SET ended_ms=?1,status=?2 WHERE id=?3",
            params![now_ms(), state, session],
        )?;
        diagnostics(&conn, session, &health.json(&reason, &retry, backoff))?;
        atomic_json(status_path, &status(&conn)?)?;
        match result {
            Ok(SessionEnd::Stop(reason)) => {
                outcome = reason;
                break;
            }
            Err(e) => {
                outcome = "storage_or_local_processing_failed";
                fatal = Some(e);
                break;
            }
            Ok(SessionEnd::Disconnected(e)) => {
                eprintln!(
                    "{}; consecutive failures {}, total disconnects {}; potential gap recorded",
                    e.reason, retry.consecutive, retry.total
                );
                if retry.consecutive >= policy.max_consecutive_failures {
                    outcome = "consecutive_failure_limit";
                    fatal = Some(anyhow::anyhow!(
                        "stream failed after {} consecutive unsuccessful sessions; see diagnostics",
                        retry.consecutive
                    ));
                    break;
                }
            }
        }
        tokio::select! {
            biased;
            _=&mut shutdown => {outcome="user_interrupt";break;},
            _=&mut deadline => {outcome="deadline_reached";break;},
            _=tokio::time::sleep(backoff)=>{},
        }
    }
    let mut report = status(&conn)?;
    report["recorder_state"] = json!(outcome);
    report["capture_file_bytes"] = json!(disk_bytes(db));
    report["total_disconnects"] = json!(retry.total);
    atomic_json(status_path, &report)?;
    if let Some(e) = fatal {
        return Err(e);
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn run_session(
    url: &str,
    conn: &mut Connection,
    session: i64,
    decoder: &Decoder,
    end: Instant,
    max_bytes: u64,
    db: &Path,
    status_path: &Path,
    policy: &RecorderPolicy,
    retry: &RetryState,
    health: &mut Health,
    pending: &mut Pending,
) -> Result<SessionEnd> {
    let (mut ws, _) = match tokio::time::timeout(policy.connect_timeout, connect_async(url)).await {
        Err(_) => {
            return Ok(SessionEnd::Disconnected(Disconnect::new(
                "websocket_connect_timeout",
            )))
        }
        Ok(Err(e)) => return Ok(SessionEnd::Disconnected(ws_failure(e))),
        Ok(Ok(ws)) => ws,
    };
    for (i, program) in [PUMP, AMM].iter().enumerate() {
        let payload=json!({"jsonrpc":"2.0","id":i+1,"method":"logsSubscribe","params":[{"mentions":[program]},{"commitment":conn.query_row("SELECT commitment FROM capture_sessions WHERE id=?1",[session],|r|r.get::<_,String>(0))?}]}).to_string();
        match tokio::time::timeout(
            policy.acknowledgement_timeout,
            ws.send(Message::Text(payload.into())),
        )
        .await
        {
            Err(_) => {
                return Ok(SessionEnd::Disconnected(Disconnect::new(
                    "subscribe_send_timeout",
                )))
            }
            Ok(Err(e)) => return Ok(SessionEnd::Disconnected(ws_failure(e))),
            Ok(Ok(())) => {}
        }
    }
    let mut ready = std::collections::BTreeMap::new();
    let connected = Instant::now();
    let mut last_notification = connected;
    let mut last_status = connected;
    let mut last_ping = connected;
    let mut ticks = tokio::time::interval(policy.flush_interval);
    loop {
        if Instant::now() >= end {
            return Ok(SessionEnd::Stop("deadline_reached"));
        }
        if health
            .acknowledged_at
            .is_some_and(|at| at.elapsed() >= policy.healthy_reset_after)
            && health.notifications > 0
            && last_notification.elapsed() < policy.notification_timeout
        {
            health.healthy = true;
        }
        tokio::select! {
            _=ticks.tick()=>{
                flush(conn,session,pending,decoder,health)?;
                if ready.len()<2 && connected.elapsed()>=policy.acknowledgement_timeout {return Ok(SessionEnd::Disconnected(Disconnect::new("subscription_acknowledgement_timeout")));}
                if ready.len()==2 && last_notification.elapsed()>=policy.notification_timeout {return Ok(SessionEnd::Disconnected(Disconnect::new("notification_stall")));}
                if last_ping.elapsed()>=policy.ping_interval {
                    match tokio::time::timeout(policy.connect_timeout,ws.send(Message::Ping(vec![].into()))).await {
                        Err(_)=>return Ok(SessionEnd::Disconnected(Disconnect::new("ping_send_timeout"))),
                        Ok(Err(e))=>return Ok(SessionEnd::Disconnected(ws_failure(e))),
                        Ok(Ok(()))=>last_ping=Instant::now(),
                    }
                }
                if disk_bytes(db)>=max_bytes {return Ok(SessionEnd::Stop("storage_cap"));}
            },
            item=ws.next()=>{
                let received=now_ms();health.last_frame_ms=Some(received);
                let message=match item {
                    None=>return Ok(SessionEnd::Disconnected(Disconnect::new("websocket_eof"))),
                    Some(Err(e))=>return Ok(SessionEnd::Disconnected(ws_failure(e))),
                    Some(Ok(message))=>message,
                };
                match message {
                    Message::Text(text)=>{
                        health.received_text_bytes+=text.len() as u64;
                        if text.len()>2*1024*1024 {return Ok(SessionEnd::Disconnected(Disconnect::new("notification_size_limit")));}
                        let payload:Value=match serde_json::from_str(text.as_ref()) {
                            Ok(p)=>p,Err(_)=>return Ok(SessionEnd::Disconnected(Disconnect::new("invalid_websocket_json"))),
                        };
                        if let Some(error)=payload.get("error") {
                            return Ok(SessionEnd::Disconnected(Disconnect::new(format!("subscription_rpc_error_{}",error["code"].as_i64().unwrap_or(0)))));
                        }
                        if let Some(id)=payload["id"].as_u64() {
                            let Some(sub)=payload["result"].as_u64() else {return Ok(SessionEnd::Disconnected(Disconnect::new("invalid_subscription_acknowledgement")));};
                            if !matches!(id,1|2) || ready.contains_key(&id) || ready.values().any(|v|*v==sub) {return Ok(SessionEnd::Disconnected(Disconnect::new("invalid_subscription_acknowledgement")));}
                            ready.insert(id,sub);
                            if ready.len()==2 {
                                health.acknowledged_at=Some(Instant::now());health.acknowledged_ms=Some(received);
                                last_notification=Instant::now();
                                conn.execute("UPDATE capture_sessions SET status='subscribed' WHERE id=?1",[session])?;
                            }
                        } else {
                            let value=&payload["params"]["result"]["value"];
                            if payload["method"]!="logsNotification" || !payload["params"]["subscription"].as_u64().is_some_and(|s|ready.values().any(|v|*v==s))
                                || value["signature"].as_str().is_none() || !value["logs"].is_array() || value.get("err").is_none()
                                || payload["params"]["result"]["context"]["slot"].as_u64().is_none() {
                                return Ok(SessionEnd::Disconnected(Disconnect::new("invalid_log_notification")));
                            }
                            health.notifications+=1;health.last_notification_ms=Some(received);last_notification=Instant::now();
                            pending.push((payload,received));health.max_buffered_messages=health.max_buffered_messages.max(pending.len());
                            if pending.len()>=policy.batch_size {flush(conn,session,pending,decoder,health)?;tokio::task::yield_now().await;}
                        }
                    },
                    Message::Ping(bytes)=>{
                        match tokio::time::timeout(policy.connect_timeout,ws.send(Message::Pong(bytes))).await {
                            Err(_)=>return Ok(SessionEnd::Disconnected(Disconnect::new("pong_send_timeout"))),
                            Ok(Err(e))=>return Ok(SessionEnd::Disconnected(ws_failure(e))),
                            Ok(Ok(()))=>{},
                        }
                    },
                    Message::Close(frame)=>return Ok(SessionEnd::Disconnected(Disconnect::new(format!("websocket_close_{}",frame.map(|f|u16::from(f.code)).unwrap_or(1005))))),
                    _=>{},
                }
            },
        }
        if last_status.elapsed() >= policy.status_interval {
            diagnostics(
                conn,
                session,
                &health.json("recording", retry, Duration::ZERO),
            )?;
            let mut report = status(conn)?;
            report["recorder_state"] = json!("collecting");
            report["capture_file_bytes"] = json!(disk_bytes(db));
            atomic_json(status_path, &report)?;
            last_status = Instant::now();
        }
    }
}

#[cfg(test)]
mod recorder_policy_tests {
    use super::*;
    #[test]
    fn failures_reset_after_healthy_session_and_backoff_stays_bounded() {
        let p = RecorderPolicy::default();
        let mut r = RetryState::default();
        assert_eq!(r.failed(false, &p, None), Duration::from_secs(10));
        for _ in 0..8 {
            assert!(r.failed(false, &p, None) <= p.backoff_max);
        }
        assert_eq!(r.consecutive, 9);
        assert_eq!(r.failed(true, &p, None), Duration::from_secs(10));
        assert_eq!(r.consecutive, 1);
        assert_eq!(r.total, 10);
        assert_eq!(
            r.failed(false, &p, Some(Duration::from_secs(120))),
            Duration::from_secs(120)
        );
    }
    #[test]
    fn provider_secrets_are_not_in_diagnostics() {
        let response = tokio_tungstenite::tungstenite::http::Response::builder()
            .status(429)
            .header("retry-after", "31")
            .body(Some(b"secret URL and API key".to_vec()))
            .unwrap();
        let error = ws_failure(WsError::Http(Box::new(response)));
        assert_eq!(error.reason, "websocket_http_429");
        assert_eq!(error.retry_after, Some(Duration::from_secs(31)));
    }
}
