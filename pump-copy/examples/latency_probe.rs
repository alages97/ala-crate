//! Bounded public-RPC commitment comparison; metadata only, no trading.
use anyhow::{bail, Context, Result};
use futures_util::{SinkExt, StreamExt};
use pump_copy::{
    model::{AMM, PUMP},
    rpc::atomic_json,
};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    hash::{Hash, Hasher},
    path::PathBuf,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio_tungstenite::{connect_async, tungstenite::Message};

#[derive(Default)]
struct Seen {
    times: [Option<u64>; 3],
    hashes: [Option<u64>; 3],
    failed: [Option<bool>; 3],
}
fn stats(mut x: Vec<i64>) -> Value {
    x.sort();
    if x.is_empty() {
        return json!({"n":0});
    }
    json!({"n":x.len(),"median_ms":x[x.len()/2],"p95_ms":x[(x.len()-1)*95/100],"min_ms":x[0],"max_ms":x[x.len()-1],"negative":x.iter().filter(|&&v|v<0).count()})
}
#[tokio::main]
async fn main() -> Result<()> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let args: Vec<_> = std::env::args().collect();
    let output = PathBuf::from(args.get(1).context("output directory required")?);
    let seconds: u64 = args.get(2).map(String::as_str).unwrap_or("300").parse()?;
    let max_bytes: u64 = args
        .get(3)
        .map(String::as_str)
        .unwrap_or("1500000000")
        .parse()?;
    let url = std::env::var("SOLANA_WS_URL").context("set SOLANA_WS_URL")?;
    let (mut ws, _) = tokio::time::timeout(Duration::from_secs(20), connect_async(url))
        .await?
        .map_err(|_| anyhow::anyhow!("WebSocket connect failed"))?;
    let start = Instant::now();
    let start_ms = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64;
    let mut subscriptions = BTreeMap::new();
    let mut entries: BTreeMap<(String, u64), Seen> = BTreeMap::new();
    for (p, program) in [PUMP, AMM].iter().enumerate() {
        for (c, commitment) in ["processed", "confirmed", "finalized"].iter().enumerate() {
            ws.send(Message::Text(json!({"jsonrpc":"2.0","id":p*3+c+1,"method":"logsSubscribe","params":[{"mentions":[program]},{"commitment":commitment}]}).to_string().into())).await?;
        }
    }
    let mut bytes = 0u64;
    let mut notifications = [0u64; 3];
    let mut conflicting_duplicates = 0;
    let mut samples = BTreeMap::new();
    let mut active_since = None;
    let mut last_status = Instant::now();
    let mut last_ping = Instant::now();
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    let reason = loop {
        let elapsed = start.elapsed().as_millis() as u64;
        if active_since.is_some_and(|at| elapsed >= at + seconds * 1000) {
            break "duration_complete";
        }
        if bytes >= max_bytes {
            break "byte_cap_reached";
        }
        if elapsed > 20000 && subscriptions.len() != 6 {
            break "subscription_timeout";
        }
        tokio::select! {
            _=tokio::signal::ctrl_c()=>break "interrupted",
            _=tick.tick()=>{},
            item=ws.next()=>{
                let received_ms=start.elapsed().as_millis() as u64;
                match item {Some(Ok(Message::Text(s)))=>{
                    bytes+=s.len() as u64;
                    let v:Value=serde_json::from_str(s.as_ref())?;
                    if v.get("error").is_some(){bail!("RPC subscription rejected; no endpoint details logged");}
                    if let Some(id)=v["id"].as_u64(){
                        let sub=v["result"].as_u64().context("subscription id")?;
                        subscriptions.insert(sub,((id-1)%3) as usize);
                        if subscriptions.len()==6&&active_since.is_none(){active_since=Some(received_ms);}
                    }else if let Some(sub)=v.pointer("/params/subscription").and_then(Value::as_u64){
                        let c=*subscriptions.get(&sub).context("unknown subscription")?;
                        let value=v.pointer("/params/result/value").context("notification value")?;
                        let slot=v.pointer("/params/result/context/slot").and_then(Value::as_u64).context("slot")?;
                        let sig=value["signature"].as_str().context("signature")?;
                        notifications[c]+=1;
                        let mut h=std::collections::hash_map::DefaultHasher::new(); value.to_string().hash(&mut h);let hash=h.finish();
                        let e=entries.entry((sig.to_owned(),slot)).or_default();
                        if e.times[c].is_none(){e.times[c]=Some(received_ms);e.hashes[c]=Some(hash);e.failed[c]=Some(!value["err"].is_null());}
                        else if e.hashes[c]!=Some(hash){conflicting_duplicates+=1;}
                        samples.entry(c).or_insert(v);
                    }
                },Some(Ok(Message::Ping(b)))=>ws.send(Message::Pong(b)).await?,Some(Ok(Message::Close(_)))|None=>break "connection_closed",Some(Err(_))=>break "connection_error",_=>{}}
            }
        }
        if last_ping.elapsed() > Duration::from_secs(20) {
            ws.send(Message::Ping(vec![].into())).await?;
            last_ping = Instant::now();
        }
        if last_status.elapsed() > Duration::from_secs(15) {
            atomic_json(
                &output.join("status.json"),
                &json!({"elapsed_ms":start.elapsed().as_millis(),"active_since_ms":active_since,"subscriptions":subscriptions.len(),"bytes":bytes,"notifications":notifications,"unique_signature_slots":entries.len()}),
            )?;
            last_status = Instant::now();
        }
    };
    let end = start.elapsed().as_millis() as u64;
    let warm = active_since.unwrap_or(0) + 20000;
    let tail = end.saturating_sub(20000);
    let mut pf = Vec::new();
    let mut pc = Vec::new();
    let mut cf = Vec::new();
    let mut missing_final = 0;
    let mut payload_mismatch = 0;
    let mut rows = Vec::new();
    for ((sig, slot), e) in entries {
        if let Some(p) = e.times[0].filter(|&p| p >= warm && p <= tail) {
            if let Some(c) = e.times[1] {
                pc.push(c as i64 - p as i64);
            }
            if let Some(f) = e.times[2] {
                pf.push(f as i64 - p as i64);
                if let Some(c) = e.times[1] {
                    cf.push(f as i64 - c as i64);
                }
                if e.hashes[0] != e.hashes[2] {
                    payload_mismatch += 1;
                }
            } else {
                missing_final += 1;
            }
            rows.push(json!({"signature":sig,"slot":slot,"receipt_elapsed_ms":{"processed":e.times[0],"confirmed":e.times[1],"finalized":e.times[2]},"failed":e.failed}));
        }
    }
    atomic_json(&output.join("pairs.json"), &rows)?;
    atomic_json(&output.join("raw-samples.json"), &samples)?;
    let report = json!({"started_at_unix_ms":start_ms,"duration_ms":end,"reason":reason,"subscriptions":subscriptions.len(),"wire_text_bytes":bytes,"notifications":notifications,"measurement_window_elapsed_ms":[warm,tail],"cohort_processed_notifications":rows.len(),"processed_to_confirmed":stats(pc),"processed_to_finalized":stats(pf),"confirmed_to_finalized":stats(cf),"processed_without_observed_finalized":missing_final,"processed_finalized_payload_mismatches":payload_mismatch,"conflicting_same_commitment_duplicates":conflicting_duplicates,"method":"Same WebSocket and monotonic receipt clock; matched transaction signature AND slot; earliest duplicate receipt; 20-second start/end exclusions. No chain-clock subtraction.","limitations":["Not a measurement of source transaction inclusion to processed receipt.","Missing finalized observation is not proof of a dropped fork; provider gaps remain possible.","No transaction is submitted; this is not an execution latency benchmark."]});
    atomic_json(&output.join("report.json"), &report)?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    let _ = ws.close(None).await;
    Ok(())
}
