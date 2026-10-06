use pump_copy::{
    decoder::{Decoder, Pools},
    discovery,
    history::{audit_transactions, token_changes},
    model::{PUMP, WSOL},
    stream::{insert_message, open_store},
};
use serde_json::{json, Value};

fn fixture() -> Value {
    serde_json::from_str(include_str!("fixtures/mainnet-451893882.json")).unwrap()
}
fn pools() -> Pools {
    serde_json::from_str(include_str!("fixtures/mainnet-pools.json")).unwrap()
}
fn basic(wallet: &str, failed: bool) -> Value {
    json!({"slot":1,"blockTime":1000,"transaction":{"signatures":["s"],"message":{"header":{"numRequiredSignatures":1},"accountKeys":[wallet],"instructions":[]}},"meta":{"err":if failed {json!({"InstructionError":[0,"Custom"]})}else{Value::Null},"fee":5000,"preBalances":[1000000],"postBalances":[995000],"preTokenBalances":[],"postTokenBalances":[],"innerInstructions":[],"logMessages":[]}})
}
#[test]
fn stream_deduplicates_both_program_subscriptions_and_retains_failed_logs() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open_store(&dir.path().join("s.db")).unwrap();
    let d = Decoder::default();
    let f = fixture();
    let tx = f["transactions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|tx| {
            !d.transaction_events(tx).unwrap().is_empty()
                && tx["meta"]["logMessages"].as_array().is_some_and(|l| {
                    l.iter()
                        .any(|s| s.as_str().unwrap().starts_with("Program data: "))
                })
        })
        .unwrap();
    let p = json!({"params":{"result":{"context":{"slot":99},"value":{"signature":"s","err":null,"logs":tx["meta"]["logMessages"]}}}});
    assert!(insert_message(&mut c, 1, &p, 1000, &d).unwrap());
    assert!(!insert_message(&mut c, 1, &p, 1001, &d).unwrap());
    let n: u64 = c
        .query_row("SELECT count(*) FROM stream_events", [], |r| r.get(0))
        .unwrap();
    assert!(n > 0);
    let mut failed = p.clone();
    failed["params"]["result"]["value"]["signature"] = json!("failed");
    failed["params"]["result"]["value"]["err"] = json!({"Custom":1});
    insert_message(&mut c, 1, &failed, 1002, &d).unwrap();
    let s = pump_copy::stream::status(&c).unwrap();
    assert_eq!(s["messages"], 2);
    assert_eq!(s["failed_transactions"], 1);
    assert_eq!(s["events"], n);
    let blob: Vec<u8> = c
        .query_row(
            "SELECT payload_gzip FROM stream_messages WHERE signature='failed'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    let stored: Value = serde_json::from_reader(flate2::read::GzDecoder::new(&blob[..])).unwrap();
    assert_eq!(stored, failed);
}
#[test]
fn stream_retains_conflicts_without_counting_a_second_trade() {
    let dir = tempfile::tempdir().unwrap();
    let mut conn = open_store(&dir.path().join("s.db")).unwrap();
    let decoder = Decoder::default();
    let mut p = json!({"params":{"result":{"context":{"slot":1},"value":{"signature":"same","err":null,"logs":[]}}}});
    assert!(insert_message(&mut conn, 1, &p, 1000, &decoder).unwrap());
    p["params"]["result"]["context"]["slot"] = json!(2);
    assert!(!insert_message(&mut conn, 1, &p, 1100, &decoder).unwrap());
    p["params"]["result"]["context"]["slot"] = json!(1);
    p["params"]["result"]["value"]["err"] = json!({"Custom":1});
    assert!(!insert_message(&mut conn, 1, &p, 1200, &decoder).unwrap());
    let s = pump_copy::stream::status(&conn).unwrap();
    assert_eq!(s["messages"], 1);
    assert_eq!(s["conflicting_observations"], 2);
}
#[test]
fn fees_are_charged_only_to_actual_payer_and_failed_fees_are_preserved() {
    let tx = basic("wallet", true);
    let r =
        audit_transactions("wallet", &[tx.clone()], &Decoder::default(), &Pools::new()).unwrap();
    assert_eq!(r["failed_transaction_fees_lamports"], 5000);
    assert_eq!(
        r["cashflow_after_all_network_fees_and_extra_swap_outflows_lamports"],
        -5000
    );
    assert_eq!(r["transactions_detail"][0]["failed"], true);
    let r = audit_transactions("other", &[tx], &Decoder::default(), &Pools::new()).unwrap();
    assert_eq!(r["network_fees_paid_lamports"], 0);
}
#[test]
fn sol_wrapping_does_not_appear_as_income_or_a_loss() {
    let mut tx = basic("wallet", false);
    tx["meta"]["preBalances"] = json!([2005000]);
    tx["meta"]["postBalances"] = json!([1000000]);
    tx["meta"]["postTokenBalances"] =
        json!([{"owner":"wallet","mint":WSOL,"uiTokenAmount":{"amount":"1000000"}}]);
    let r = audit_transactions("wallet", &[tx], &Decoder::default(), &Pools::new()).unwrap();
    assert_eq!(r["native_plus_wsol_change_lamports"], -5000);
    assert_eq!(r["supported_swap_cashflow_lamports"], 0);
}
#[test]
fn external_tokens_are_flagged_and_never_certified_as_profit() {
    let mut tx = basic("wallet", false);
    tx["meta"]["postTokenBalances"] =
        json!([{"owner":"wallet","mint":"token","uiTokenAmount":{"amount":"500"}}]);
    assert_eq!(token_changes(&tx, "wallet").unwrap()["token"], 500);
    let r = audit_transactions("wallet", &[tx], &Decoder::default(), &Pools::new()).unwrap();
    assert_eq!(r["token_delta_mismatches"], 1);
    assert_eq!(r["eligible_for_funded_copy"], false);
}
#[test]
fn real_swap_token_delta_mutation_is_detected() {
    let d = Decoder::default();
    let p = pools();
    let f = fixture();
    let mut found = false;
    for tx in f["transactions"].as_array().unwrap() {
        let b = json!({"transactions":[tx],"blockTime":f["blockTime"],"blockhash":"h"});
        let batch = d.block(451893882, &b, &p).unwrap();
        let Some(t) = batch.trades.first() else {
            continue;
        };
        let mut full = tx.clone();
        full["slot"] = json!(451893882);
        full["blockTime"] = f["blockTime"].clone();
        let report = audit_transactions(&t.wallet, &[full.clone()], &d, &p).unwrap();
        if report["token_delta_mismatches"] == 0 {
            let balances = full["meta"]["postTokenBalances"].as_array_mut().unwrap();
            if let Some(balance) = balances
                .iter_mut()
                .find(|b| b["owner"] == t.wallet && b["mint"] == t.mint)
            {
                let amount = balance["uiTokenAmount"]["amount"]
                    .as_str()
                    .unwrap()
                    .parse::<u64>()
                    .unwrap();
                balance["uiTokenAmount"]["amount"] = json!((amount + 1).to_string());
                let after = audit_transactions(&t.wallet, &[full], &d, &p).unwrap();
                assert_eq!(after["token_delta_mismatches"], 1);
                found = true;
                break;
            }
        }
    }
    assert!(found);
}
#[test]
fn receipt_window_requires_distinct_net_buyers_and_expires_old_buys() {
    let cfg: pump_copy::config::Config =
        toml::from_str(include_str!("../config/default.toml")).unwrap();
    let mut flow = cfg.flow;
    flow.window_ms = 1000;
    flow.min_whales = 2;
    flow.whale_buy_lamports = 10;
    flow.min_net_inflow_lamports = 20;
    let d = Decoder::default();
    let f = fixture();
    let b = d.block(451893882, &f, &pools()).unwrap();
    let mut t = b.trades[0].clone();
    t.side = pump_copy::model::Side::Buy;
    t.wallet = "a".into();
    t.quote_lamports = 10;
    let mut tracker = discovery::FlowTracker::default();
    assert!(tracker.observe(&t, 1000, &flow).is_none());
    assert!(tracker.observe(&t, 1001, &flow).is_none());
    t.wallet = "b".into();
    assert!(tracker.observe(&t, 1002, &flow).is_some());
    let mut tracker = discovery::FlowTracker::default();
    t.wallet = "a".into();
    tracker.observe(&t, 1000, &flow);
    t.wallet = "b".into();
    assert!(tracker.observe(&t, 2001, &flow).is_none());
}
#[test]
fn discovery_snapshot_does_not_include_later_messages_or_qualify_wallets() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open_store(&dir.path().join("s.db")).unwrap();
    let d = Decoder::default();
    let f = fixture();
    let tx = f["transactions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|tx| {
            d.transaction_events(tx)
                .unwrap()
                .iter()
                .any(|e| e.program == PUMP)
        })
        .unwrap();
    let mut p = json!({"params":{"result":{"context":{"slot":99},"value":{"signature":"s","err":null,"logs":tx["meta"]["logMessages"]}}}});
    insert_message(&mut c, 1, &p, 1000, &d).unwrap();
    let cfg: pump_copy::config::Config =
        toml::from_str(include_str!("../config/default.toml")).unwrap();
    let before = discovery::analyze(&c, 1, &pools(), &cfg.flow).unwrap();
    p["params"]["result"]["value"]["signature"] = json!("later");
    insert_message(&mut c, 1, &p, 2000, &d).unwrap();
    let after = discovery::analyze(&c, 1, &pools(), &cfg.flow).unwrap();
    assert_eq!(before, after);
    assert_eq!(after["qualified_copy_wallets"], json!([]));
}

#[test]
fn inventory_lookup_keys_cannot_hide_target_programs() {
    let signature = bs58::encode([1u8; 64]).into_string();
    let mut v = json!({"transaction":{"signatures":[signature],"accountKeys":[{"pubkey":"wallet","signer":true,"source":"transaction"},{"pubkey":PUMP,"signer":false,"source":"lookupTable"}]},"meta":{"err":null,"fee":5000}});
    let (stub, hydrate) = pump_copy::rpc::inventory_stub(&v).unwrap();
    assert!(hydrate);
    assert_eq!(stub["transaction"]["message"]["accountKeys"][1], PUMP);
    v["meta"]["err"] = json!({"Custom":1});
    let (_, hydrate) = pump_copy::rpc::inventory_stub(&v).unwrap();
    assert!(!hydrate);
    v["meta"].as_object_mut().unwrap().remove("err");
    assert!(pump_copy::rpc::inventory_stub(&v).is_err());
}
#[test]
fn raw_archive_without_committed_coverage_is_not_replayable() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir(dir.path().join("raw")).unwrap();
    assert!(pump_copy::rpc::load_batches(dir.path())
        .unwrap_err()
        .to_string()
        .contains("coverage manifest"));
}

fn mock_rpc(
    replies: Vec<(&'static str, u16, Value)>,
) -> (pump_copy::rpc::Rpc, std::thread::JoinHandle<Vec<Value>>) {
    use std::io::{BufRead, Read, Write};
    static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let env = format!(
        "PUMP_TEST_RPC_{}",
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    );
    std::env::set_var(&env, format!("http://127.0.0.1:{port}"));
    let rpc = pump_copy::rpc::Rpc::from_env(&env, 1).unwrap();
    std::env::remove_var(&env);
    let thread = std::thread::spawn(move || {
        let mut requests = vec![];
        for (method, status, result) in replies {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(std::time::Duration::from_secs(10)))
                .unwrap();
            let mut reader = std::io::BufReader::new(stream.try_clone().unwrap());
            let mut len = 0;
            loop {
                let mut s = String::new();
                reader.read_line(&mut s).unwrap();
                if s == "\r\n" {
                    break;
                }
                if let Some(v) = s.to_lowercase().strip_prefix("content-length:") {
                    len = v.trim().parse::<usize>().unwrap();
                }
            }
            let mut body = vec![0; len];
            reader.read_exact(&mut body).unwrap();
            let body: Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(body["method"], method);
            requests.push(body);
            let reply = json!({"jsonrpc":"2.0","id":1,"result":result}).to_string();
            write!(stream,"HTTP/1.1 {status} Test\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",reply.len(),reply).unwrap();
        }
        requests
    });
    (rpc, thread)
}
#[tokio::test]
async fn helius_partial_final_page_is_never_marked_complete() {
    let wallet = PUMP;
    let mut one = basic(wallet, false);
    let mut two = one.clone();
    one["transaction"]["signatures"] = json!(["one"]);
    two["transaction"]["signatures"] = json!(["two"]);
    let (rpc, server) = mock_rpc(vec![(
        "getTransactionsForAddress",
        200,
        json!({"data":[one,two],"paginationToken":null}),
    )]);
    let dir = tempfile::tempdir().unwrap();
    let r =
        pump_copy::history::backfill(&rpc, &dir.path().join("h.db"), wallet, 1, 2000000, 1, true)
            .await
            .unwrap();
    assert_eq!(r["complete"], false);
    assert_eq!(r["transactions_in_run"], 1);
    let requests = server.join().unwrap();
    assert_eq!(requests[0]["params"][1]["filters"]["status"], "any");
    assert_eq!(requests[0]["params"][1]["filters"]["tokenAccounts"], "all");
    assert_eq!(
        requests[0]["params"][1]["maxSupportedTransactionVersion"],
        1
    );
}
#[tokio::test]
async fn missing_history_payload_leaves_incomplete_run_without_skipping() {
    let (rpc, server) = mock_rpc(vec![
        (
            "getSignaturesForAddress",
            200,
            json!([{"signature":"s","slot":1,"blockTime":1000}]),
        ),
        ("getTransaction", 200, Value::Null),
    ]);
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("h.db");
    assert!(
        pump_copy::history::backfill(&rpc, &db, PUMP, 1, 2000000, 5, false)
            .await
            .is_err()
    );
    let c = open_store(&db).unwrap();
    let (complete, fetched): (bool, u64) = c
        .query_row("SELECT complete,fetched FROM history_runs", [], |r| {
            Ok((r.get(0)?, r.get(1)?))
        })
        .unwrap();
    assert!(!complete);
    assert_eq!(fetched, 0);
    server.join().unwrap();
}
#[tokio::test]
async fn oversized_block_fallback_preserves_full_order_and_proves_omissions() {
    let s1 = bs58::encode([1u8; 64]).into_string();
    let s2 = bs58::encode([2u8; 64]).into_string();
    let s3 = bs58::encode([3u8; 64]).into_string();
    let entry = |s: &str, program: &str, err: Value| json!({"transaction":{"signatures":[s],"accountKeys":[{"pubkey":"payer","signer":true,"source":"transaction"},{"pubkey":program,"signer":false,"source":"lookupTable"}]},"meta":{"err":err,"fee":5000,"preBalances":[1,1],"postBalances":[1,1],"preTokenBalances":[],"postTokenBalances":[]}});
    let inventory = json!({"blockhash":"h","blockTime":1000,"transactions":[entry(&s1,"other",Value::Null),entry(&s2,PUMP,Value::Null),entry(&s3,PUMP,json!({"Custom":1}))]});
    let mut full = basic("payer", false);
    full["slot"] = json!(7);
    full["transaction"]["signatures"] = json!([s2]);
    full["meta"]["loadedAddresses"] = json!({"writable":[],"readonly":[PUMP]});
    let (rpc, server) = mock_rpc(vec![
        ("getBlock", 413, Value::Null),
        ("getBlock", 200, inventory),
        ("getTransaction", 200, full),
    ]);
    let dir = tempfile::tempdir().unwrap();
    let b = rpc
        .capture(7, dir.path(), &Decoder::default(), &mut Pools::new())
        .await
        .unwrap();
    assert_eq!(b.slot, 7);
    assert!(b.trades.is_empty());
    server.join().unwrap();
    let raw: Value = serde_json::from_reader(flate2::read::GzDecoder::new(
        std::fs::File::open(dir.path().join("raw/7.json.gz")).unwrap(),
    ))
    .unwrap();
    assert_eq!(raw["transactions"].as_array().unwrap().len(), 3);
    assert_eq!(raw["transactions"][1]["transaction"]["signatures"][0], s2);
    assert_eq!(
        raw["transactions"][0]["capture_detail"],
        "account_inventory_only"
    );
    assert_eq!(raw["full_target_transactions"], 1);
}

#[test]
fn real_zero_proceeds_sale_is_retained_with_its_network_fee() {
    let b: Value = serde_json::from_str(include_str!("fixtures/mainnet-dust-sale.json")).unwrap();
    let batch = Decoder::default()
        .block(451897812, &b, &Pools::new())
        .unwrap();
    assert_eq!(batch.trades.len(), 1);
    let t = &batch.trades[0];
    assert_eq!(t.side, pump_copy::model::Side::Sell);
    assert_eq!(t.quote_lamports, 0);
    assert_eq!(t.network_fee_lamports, 14900);
    let mut buy = t.clone();
    buy.side = pump_copy::model::Side::Buy;
    buy.quote_lamports = 100000;
    buy.network_fee_lamports = 0;
    let mut wallets = pump_copy::wallets::Wallets::default();
    wallets.observe(&buy, 1);
    wallets.observe(t, 2);
    let c: pump_copy::config::Config =
        toml::from_str(include_str!("../config/default.toml")).unwrap();
    let reports = wallets.report(&c.selection, 2);
    assert_eq!(reports[0].realized_pnl_lamports, -114900);
    assert_eq!(reports[0].closed_positions, 1);
}

#[cfg(unix)]
#[tokio::test]
async fn busy_websocket_recorder_exits_cleanly_on_ctrl_c() {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let ready = std::sync::Arc::new(tokio::sync::Notify::new());
    let notify = ready.clone();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
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
        notify.notify_one();
        for i in 0..100000 {
            let payload = json!({"method":"logsNotification","params":{"subscription":11,"result":{"context":{"slot":99},"value":{"signature":format!("message-{i}"),"err":null,"logs":[]}}}});
            if ws
                .send(Message::Text(payload.to_string().into()))
                .await
                .is_err()
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    });
    struct ChildGuard(std::process::Child);
    impl Drop for ChildGuard {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let mut child = ChildGuard(
        std::process::Command::new(env!("CARGO_BIN_EXE_pump-copy"))
            .args([
                "--config",
                concat!(env!("CARGO_MANIFEST_DIR"), "/config/default.toml"),
                "record-stream",
                "--db",
            ])
            .arg(dir.path().join("stream.db"))
            .arg("--status")
            .arg(dir.path().join("status.json"))
            .args(["--seconds", "60", "--max-bytes", "10000000"])
            .env("SOLANA_WS_URL", format!("ws://127.0.0.1:{port}"))
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap(),
    );
    tokio::time::timeout(std::time::Duration::from_secs(5), ready.notified())
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert!(std::process::Command::new("kill")
        .args(["-INT", &child.0.id().to_string()])
        .status()
        .unwrap()
        .success());
    let end = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        if let Some(status) = child.0.try_wait().unwrap() {
            assert!(status.success());
            break;
        }
        assert!(
            tokio::time::Instant::now() < end,
            "recorder ignored Ctrl-C under load"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    let s: Value =
        serde_json::from_reader(std::fs::File::open(dir.path().join("status.json")).unwrap())
            .unwrap();
    assert!(s["messages"].as_u64().unwrap() > 0);
    assert_eq!(s["sessions"][0]["status"], "closed");
    server.abort();
}

#[test]
fn unknown_tail_is_routable_but_never_a_verified_trade() {
    let p: Value =
        serde_json::from_str(include_str!("fixtures/unknown-tail-notification.json")).unwrap();
    let v = &p["params"]["result"]["value"];
    let tx = json!({"transaction":{"message":{"accountKeys":[PUMP,pump_copy::model::AMM]}},"meta":{"err":null,"logMessages":v["logs"],"innerInstructions":[]}});
    let d = Decoder::default();
    assert!(d
        .transaction_events(&tx)
        .unwrap_err()
        .to_string()
        .contains("unknown trailing"));
    let identities = d.capture_identity_events(&tx).unwrap();
    assert!(!identities.is_empty());
    for e in identities {
        assert!(e.fields["_unparsed_tail_bytes"].as_u64().unwrap() > 0);
        assert!(e.fields.get("quote_amount_out").is_none());
        assert!(d.trade_from_event(e, "s", 0, 0, 0, &Pools::new()).is_err());
    }
    let mut failed = tx.clone();
    failed["meta"]["err"] = json!({"Custom":1});
    assert!(d.capture_identity_events(&failed).unwrap().is_empty());
    let mut truncated = tx;
    truncated["meta"]["logMessages"]
        .as_array_mut()
        .unwrap()
        .push(json!("Log truncated"));
    assert!(d.capture_identity_events(&truncated).is_err());
}
