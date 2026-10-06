use futures_util::{SinkExt, StreamExt};
use pump_copy::stream::{record_with_policy, RecorderPolicy};
use serde_json::{json, Value};
use std::time::{Duration, Instant};
use tokio_tungstenite::tungstenite::Message;

fn policy() -> RecorderPolicy {
    RecorderPolicy {
        connect_timeout: Duration::from_millis(100),
        acknowledgement_timeout: Duration::from_millis(100),
        notification_timeout: Duration::from_millis(80),
        ping_interval: Duration::from_millis(25),
        healthy_reset_after: Duration::from_millis(30),
        backoff_base: Duration::from_millis(5),
        backoff_max: Duration::from_millis(20),
        max_consecutive_failures: 3,
        flush_interval: Duration::from_millis(10),
        status_interval: Duration::from_millis(10),
        batch_size: 64,
    }
}
fn report(dir: &tempfile::TempDir) -> Value {
    serde_json::from_slice(&std::fs::read(dir.path().join("status.json")).unwrap()).unwrap()
}
async fn record(
    port: u16,
    name: &str,
    dir: &tempfile::TempDir,
    duration: Duration,
    p: &RecorderPolicy,
) -> anyhow::Result<()> {
    std::env::set_var(name, format!("ws://127.0.0.1:{port}"));
    record_with_policy(
        name,
        &dir.path().join("capture.sqlite"),
        duration,
        100_000_000,
        &dir.path().join("status.json"),
        "confirmed",
        p,
    )
    .await
}
async fn acknowledge(ws: &mut tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>) {
    for _ in 0..2 {
        ws.next().await.unwrap().unwrap();
    }
    for id in 1..=2 {
        ws.send(Message::Text(
            json!({"jsonrpc":"2.0","id":id,"result":id+10})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    }
}
fn notification(signature: &str) -> Message {
    Message::Text(json!({"method":"logsNotification","params":{"subscription":11,"result":{"context":{"slot":1},"value":{"signature":signature,"err":null,"logs":[]}}}}).to_string().into())
}

#[tokio::test]
async fn healthy_sessions_can_survive_more_than_ten_total_disconnects() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(async move {
        for session in 0..30 {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
            acknowledge(&mut ws).await;
            for n in 0..10 {
                if ws
                    .send(notification(&format!("s-{session}-{n}")))
                    .await
                    .is_err()
                {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(6)).await;
            }
            let _ = ws.close(None).await;
        }
    });
    let dir = tempfile::tempdir().unwrap();
    let mut p = policy();
    p.notification_timeout = Duration::from_secs(1);
    record(
        port,
        "TEST_WS_HEALTHY",
        &dir,
        Duration::from_millis(1600),
        &p,
    )
    .await
    .unwrap();
    let s = report(&dir);
    assert_eq!(s["recorder_state"], "deadline_reached");
    assert!(s["total_disconnects"].as_u64().unwrap() > 10);
    assert!(s["messages"].as_u64().unwrap() > 100);
    assert!(
        s["sessions"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|s| s["diagnostics"]["healthy_session"] == true)
            .count()
            > 10
    );
    server.abort();
}

#[tokio::test]
async fn deadline_interrupts_stalled_handshake() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(async move {
        let (_stream, _) = listener.accept().await.unwrap();
        std::future::pending::<()>().await;
    });
    let dir = tempfile::tempdir().unwrap();
    let mut p = policy();
    p.connect_timeout = Duration::from_secs(10);
    let start = Instant::now();
    record(
        port,
        "TEST_WS_HANDSHAKE",
        &dir,
        Duration::from_millis(60),
        &p,
    )
    .await
    .unwrap();
    assert!(start.elapsed() < Duration::from_secs(1));
    assert_eq!(report(&dir)["recorder_state"], "deadline_reached");
    server.abort();
}

#[tokio::test]
async fn retry_after_is_logged_without_provider_secrets_and_cannot_extend_deadline() {
    use tokio::io::AsyncWriteExt;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        stream.write_all(b"HTTP/1.1 429 Too Many Requests\r\nRetry-After: 120\r\nContent-Length: 0\r\nX-Secret: dont-log-me\r\n\r\n").await.unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
    });
    let dir = tempfile::tempdir().unwrap();
    let start = Instant::now();
    record(
        port,
        "TEST_WS_BACKOFF",
        &dir,
        Duration::from_millis(100),
        &policy(),
    )
    .await
    .unwrap();
    let s = report(&dir);
    assert!(start.elapsed() < Duration::from_secs(1));
    assert_eq!(s["recorder_state"], "deadline_reached");
    assert_eq!(
        s["sessions"][0]["diagnostics"]["reason"],
        "websocket_http_429"
    );
    assert_eq!(s["sessions"][0]["diagnostics"]["next_backoff_ms"], 120000);
    assert!(!s.to_string().contains("dont-log-me"));
    server.abort();
}

#[tokio::test]
async fn acknowledged_but_silent_feed_is_marked_as_gap() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            tokio::spawn(async move {
                let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
                acknowledge(&mut ws).await;
                while let Some(Ok(message)) = ws.next().await {
                    if let Message::Ping(p) = message {
                        if ws.send(Message::Pong(p)).await.is_err() {
                            break;
                        }
                    }
                }
            });
        }
    });
    let dir = tempfile::tempdir().unwrap();
    record(
        port,
        "TEST_WS_STALL",
        &dir,
        Duration::from_millis(150),
        &policy(),
    )
    .await
    .unwrap();
    let s = report(&dir);
    assert_eq!(
        s["sessions"][0]["diagnostics"]["reason"],
        "notification_stall"
    );
    assert_eq!(
        s["sessions"][0]["status"],
        "disconnected_gap_requires_backfill"
    );
    server.abort();
}

#[tokio::test]
async fn pending_batch_survives_deadline_and_strict_decode_failure() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let mut raw: Value =
        serde_json::from_str(include_str!("fixtures/unknown-tail-notification.json")).unwrap();
    raw["method"] = json!("logsNotification");
    raw["params"]["subscription"] = json!(11);
    let expected = raw.clone();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
        acknowledge(&mut ws).await;
        tokio::time::sleep(Duration::from_millis(20)).await;
        ws.send(Message::Text(raw.to_string().into()))
            .await
            .unwrap();
        std::future::pending::<()>().await;
    });
    let dir = tempfile::tempdir().unwrap();
    let mut p = policy();
    p.flush_interval = Duration::from_secs(5);
    p.notification_timeout = Duration::from_secs(10);
    record(port, "TEST_WS_BATCH", &dir, Duration::from_millis(100), &p)
        .await
        .unwrap();
    let s = report(&dir);
    assert_eq!(s["messages"], 1);
    assert_eq!(s["decode_errors"], 1);
    assert_eq!(s["events"], 0);
    let c = rusqlite::Connection::open(dir.path().join("capture.sqlite")).unwrap();
    let bytes: Vec<u8> = c
        .query_row("SELECT payload_gzip FROM stream_messages", [], |r| r.get(0))
        .unwrap();
    let stored: Value =
        serde_json::from_reader(flate2::read::GzDecoder::new(bytes.as_slice())).unwrap();
    assert_eq!(stored, expected);
    server.abort();
}

#[tokio::test]
async fn consecutive_failures_stop_with_explicit_reason() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            drop(stream);
        }
    });
    let dir = tempfile::tempdir().unwrap();
    assert!(record(
        port,
        "TEST_WS_LIMIT",
        &dir,
        Duration::from_secs(2),
        &policy()
    )
    .await
    .is_err());
    let s = report(&dir);
    assert_eq!(s["recorder_state"], "consecutive_failure_limit");
    assert_eq!(s["total_disconnects"], 3);
    server.abort();
}
