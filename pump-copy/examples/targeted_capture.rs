//! Bounded raw research capture. No strategy evaluation, signing or LLM calls.
use anyhow::{bail, ensure, Context, Result};
use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use pump_copy::{
    decoder::{Decoder, Event, Pools},
    model::{AMM, PUMP},
    rpc::atomic_json,
    stream::{insert_message, open_store, status},
};
use rusqlite::{params, Connection};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, OpenOptions},
    io::Write,
    path::PathBuf,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio_tungstenite::{connect_async, tungstenite::Message};

#[derive(Parser)]
struct Args {
    #[arg(long)]
    output: PathBuf,
    #[arg(long)]
    status: PathBuf,
    #[arg(long)]
    wallets: PathBuf,
    #[arg(long)]
    pools: PathBuf,
    #[arg(long, default_value_t = 86400)]
    seconds: u64,
    #[arg(long, default_value_t = 43200)]
    admission_seconds: u64,
    #[arg(long, default_value_t = 64)]
    max_markets: usize,
    #[arg(long, default_value_t = 8_000_000_000)]
    max_bytes: u64,
    #[arg(long, default_value_t = 40_000_000_000)]
    max_wire_bytes: u64,
    #[arg(long)]
    seed_markets: Option<PathBuf>,
}
fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
// Pool subscriptions cover unknown post-graduation coins without guessing their mint.
// Mint subscriptions cover the pre-graduation coin and later instructions mentioning it.
fn source_markets(events: &[Event], wallets: &BTreeSet<String>, pools: &Pools) -> BTreeSet<String> {
    events
        .iter()
        .filter(|e| wallets.contains(e.fields["user"].as_str().unwrap_or("")))
        .filter_map(|e| {
            if e.name == "TradeEvent" && e.fields["is_buy"] == true {
                e.fields["mint"].as_str().map(str::to_owned)
            } else if e.name == "BuyEvent" {
                e.fields["pool"].as_str().map(|p| {
                    pools
                        .get(p)
                        .map(|i| i.base_mint.clone())
                        .unwrap_or_else(|| p.to_owned())
                })
            } else {
                None
            }
        })
        .collect()
}
fn log_line(f: &mut fs::File, v: Value) -> Result<()> {
    serde_json::to_writer(&mut *f, &v)?;
    f.write_all(b"\n")?;
    f.flush()?;
    Ok(())
}
fn total_disk(args: &Args) -> u64 {
    fs::read_dir(&args.output)
        .into_iter()
        .flatten()
        .filter_map(|e| e.ok())
        .filter_map(|e| e.metadata().ok())
        .filter(|m| m.is_file())
        .map(|m| m.len())
        .sum()
}
#[allow(clippy::too_many_arguments)]
fn snapshot(
    args: &Args,
    conns: &[Connection],
    started: u64,
    state: &str,
    targets: &BTreeMap<String, Value>,
    wire: u64,
    notices: u64,
    excluded: u64,
) -> Result<()> {
    atomic_json(
        &args.status,
        &json!({"as_of_ms":now(),"started_ms":started,"planned_end_ms":started+args.seconds*1000,"admission_end_ms":started+args.admission_seconds*1000,"state":state,"wire_bytes":wire,"notifications":notices,"database_and_log_bytes":total_disk(args),"tracked_markets":targets.len(),"market_cap":args.max_markets,"untracked_new_market_notices":excluded,"confirmed":status(&conns[0])?,"finalized":status(&conns[1])?,"scope":"Target wallet address mentions, followed by newly bought mint/pool address mentions. Subscription-start and reconnect gaps are explicit. No whole-market whale discovery, no portfolio slot limits, no automatic strategy analysis.","identity_events":[conns[0].query_row("SELECT count(*) FROM capture_identity_events",[],|r|r.get::<_,u64>(0))?,conns[1].query_row("SELECT count(*) FROM capture_identity_events",[],|r|r.get::<_,u64>(0))?],"financial_event_decode_complete":status(&conns[0])?["decode_errors"]==0,"identity_only_routing":"Unknown event tails may be used only to route raw market subscriptions. Strict trade decoding still rejects them; no price or return validation is implied.","wallets_qualified":false,"live_orders":false}),
    )?;
    atomic_json(&args.output.join("targets.json"), targets)?;
    Ok(())
}
#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    ensure!(
        args.seconds > 0
            && args.admission_seconds <= args.seconds
            && args.max_markets > 0
            && args.max_markets <= 128
            && args.max_bytes > 0,
        "invalid bounded capture plan"
    );
    fs::create_dir_all(&args.output)?;
    ensure!(
        !args.output.join("confirmed.sqlite").exists(),
        "refusing to overwrite an existing capture"
    );
    let wallets: BTreeSet<String> = serde_json::from_reader(fs::File::open(&args.wallets)?)?;
    ensure!(
        !wallets.is_empty() && wallets.len() <= 10,
        "1..10 wallets required"
    );
    for w in &wallets {
        ensure!(bs58::decode(w).into_vec()?.len() == 32, "invalid wallet");
    }
    let pools: Pools = serde_json::from_reader(fs::File::open(&args.pools)?)?;
    let _ = rustls::crypto::ring::default_provider().install_default();
    let url = std::env::var("SOLANA_WS_URL").context("set SOLANA_WS_URL")?;
    let parsed = reqwest::Url::parse(&url).map_err(|_| anyhow::anyhow!("invalid WebSocket URL"))?;
    ensure!(
        parsed.scheme() == "wss" || matches!(parsed.host_str(), Some("localhost" | "127.0.0.1")),
        "secure WebSocket required"
    );
    let mut conns = vec![
        open_store(&args.output.join("confirmed.sqlite"))?,
        open_store(&args.output.join("finalized.sqlite"))?,
    ];
    for c in &conns {
        c.execute_batch("CREATE TABLE IF NOT EXISTS capture_identity_events(signature TEXT NOT NULL,event_index INTEGER NOT NULL,program TEXT NOT NULL,event_name TEXT NOT NULL,fields_json TEXT NOT NULL,PRIMARY KEY(signature,event_index));")?;
    }
    let mut journal = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(args.output.join("subscriptions.jsonl"))?;
    let started = now();
    let end = Instant::now() + Duration::from_secs(args.seconds);
    let admission_end = started + args.admission_seconds * 1000;
    let mut targets: BTreeMap<String, Value> = match &args.seed_markets {
        Some(p) => serde_json::from_reader(fs::File::open(p)?)?,
        None => BTreeMap::new(),
    };
    ensure!(
        targets.len() <= args.max_markets,
        "seed market cap exceeded"
    );
    for address in targets.keys() {
        ensure!(
            bs58::decode(address).into_vec()?.len() == 32,
            "invalid seed market"
        );
    }
    let mut wire = 0u64;
    let mut notices = 0u64;
    let mut excluded = 0u64;
    let mut reconnects = 0;
    let decoder = Decoder::default();
    let mut final_state = "time_limit";
    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);
    while Instant::now() < end {
        let mut sessions = Vec::new();
        for (i, c) in conns.iter().enumerate() {
            c.execute("INSERT INTO capture_sessions(started_ms,status,commitment) VALUES(?1,'connecting',?2)",params![now(),(["confirmed","finalized"][i])])?;
            sessions.push(c.last_insert_rowid());
        }
        let attempt:Result<()>=async {
   let (mut ws,_)=tokio::time::timeout(Duration::from_secs(20),connect_async(&url)).await.context("connect timeout")?.map_err(|_|anyhow::anyhow!("WebSocket connect failed"))?;
   let mut pending:BTreeMap<u64,(usize,String,Instant)>=BTreeMap::new();let mut subs:BTreeMap<u64,(usize,String)>=BTreeMap::new();let mut requested=BTreeSet::new();let mut next_id=1u64;
   for address in wallets.iter().chain(targets.keys()) {
    for i in 0..2 {if requested.insert((i,address.clone())) {ws.send(Message::Text(json!({"jsonrpc":"2.0","id":next_id,"method":"logsSubscribe","params":[{"mentions":[address]},{"commitment":(["confirmed","finalized"][i])}]}).to_string().into())).await?;pending.insert(next_id,(i,address.clone(),Instant::now()));next_id+=1;}}
   }
   let mut ticks=tokio::time::interval(Duration::from_secs(1));let mut last_status=Instant::now();let mut last_ping=Instant::now();let mut last_response=Instant::now();
   loop {
    if Instant::now()>=end {return Ok(());}
    tokio::select! {
     _=&mut shutdown=>{final_state="user_interrupt";return Ok(());},
     _=ticks.tick()=>{
      ensure!(pending.values().all(|(_,_,at)|at.elapsed()<Duration::from_secs(30)),"subscription acknowledgement timeout");
      ensure!(last_response.elapsed()<Duration::from_secs(90),"WebSocket unresponsive");
      if last_ping.elapsed()>Duration::from_secs(20){ws.send(Message::Ping(vec![].into())).await?;last_ping=Instant::now();}
     },
     item=ws.next()=>{
      last_response=Instant::now();
      match item.context("WebSocket closed")?.map_err(|_|anyhow::anyhow!("WebSocket receive failed"))? {
       Message::Text(s)=>{
        wire+=s.len() as u64;let p:Value=serde_json::from_str(s.as_ref())?;
        if p.get("error").is_some(){log_line(&mut journal,json!({"kind":"subscription_rejected","received_ms":now(),"response":p}))?;bail!("subscription rejected");}
        if let Some(id)=p["id"].as_u64(){let (i,address,_)=pending.remove(&id).context("unexpected response id")?;let sub=p["result"].as_u64().context("subscription result")?;subs.insert(sub,(i,address.clone()));log_line(&mut journal,json!({"kind":"subscribed","address":address,"commitment":(["confirmed","finalized"][i]),"received_ms":now(),"session_id":sessions[i]}))?;conns[i].execute("UPDATE capture_sessions SET status='subscribed' WHERE id=?1",[sessions[i]])?;}
        else if p["method"]=="logsNotification" {
         notices+=1;let recv=now();let sub=p.pointer("/params/subscription").and_then(Value::as_u64).context("subscription id")?;let (i,_)=subs.get(&sub).context("unknown subscription")?.clone();
         let inserted=insert_message(&mut conns[i],sessions[i],&p,recv,&decoder)?;
         if inserted && p.pointer("/params/result/value/signature").and_then(Value::as_str)!=Some(&"1".repeat(64)) {
          let v=&p["params"]["result"]["value"];let tx=json!({"transaction":{"message":{"accountKeys":[PUMP,AMM]}},"meta":{"err":v["err"],"logMessages":v["logs"],"innerInstructions":[]}});
          if let Ok(events)=decoder.capture_identity_events(&tx) {
           for (idx,event) in events.iter().enumerate() {conns[i].execute("INSERT OR IGNORE INTO capture_identity_events VALUES(?1,?2,?3,?4,?5)",params![v["signature"].as_str(),idx,event.program,event.name,serde_json::to_string(&event.fields)?])?;}
           for address in source_markets(&events,&wallets,&pools).into_iter().filter(|_|recv<=admission_end) {
            if targets.contains_key(&address){continue;}
            if targets.len()>=args.max_markets {excluded+=1;log_line(&mut journal,json!({"kind":"market_cap_no_followup","address":address,"received_ms":recv,"signature":v["signature"]}))?;continue;}
            let cause=json!({"first_observed_ms":recv,"source_signature":v["signature"],"commitment":(["confirmed","finalized"][i]),"session_id":sessions[i],"subscription_gap":"Trades before acknowledgement are not guaranteed observed."});targets.insert(address.clone(),cause.clone());log_line(&mut journal,json!({"kind":"market_admitted","address":address,"cause":cause}))?;
            for j in 0..2 {if requested.insert((j,address.clone())) {ws.send(Message::Text(json!({"jsonrpc":"2.0","id":next_id,"method":"logsSubscribe","params":[{"mentions":[address]},{"commitment":(["confirmed","finalized"][j])}]}).to_string().into())).await?;pending.insert(next_id,(j,address.clone(),Instant::now()));next_id+=1;}}
           }
          }
         }
        }
       },
       Message::Ping(b)=>ws.send(Message::Pong(b)).await?,Message::Close(_)=>bail!("WebSocket closed"),_=>{}
      }
     }
    }
    if wire>=args.max_wire_bytes {final_state="wire_limit";return Ok(());}
    if last_status.elapsed()>=Duration::from_secs(15){snapshot(&args,&conns,started,"collecting",&targets,wire,notices,excluded)?;if total_disk(&args)>=args.max_bytes {final_state="disk_limit";return Ok(());}last_status=Instant::now();}
   }
  }.await;
        let state = if attempt.is_ok() {
            "closed"
        } else {
            "disconnected_gap_requires_backfill"
        };
        for (i, c) in conns.iter().enumerate() {
            c.execute(
                "UPDATE capture_sessions SET ended_ms=?1,status=?2 WHERE id=?3",
                params![now(), state, sessions[i]],
            )?;
        }
        log_line(
            &mut journal,
            json!({"kind":"session_end","received_ms":now(),"state":state,"error":attempt.as_ref().err().map(|e|e.to_string())}),
        )?;
        snapshot(
            &args, &conns, started, state, &targets, wire, notices, excluded,
        )?;
        if attempt.is_ok() {
            break;
        }
        reconnects += 1;
        if reconnects >= 10 {
            final_state = "reconnect_limit";
            break;
        }
        tokio::select! {_=&mut shutdown=>{final_state="user_interrupt";break;},_=tokio::time::sleep(Duration::from_secs(30))=>{}}
    }
    for c in &conns {
        c.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")?;
    }
    snapshot(
        &args,
        &conns,
        started,
        final_state,
        &targets,
        wire,
        notices,
        excluded,
    )?;
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn only_watched_buys_admit_markets() {
        let wallets = BTreeSet::from(["w".to_owned()]);
        let events = vec![
            Event {
                program: PUMP.into(),
                name: "TradeEvent".into(),
                fields: json!({"user":"w","mint":"m","is_buy":true}),
            },
            Event {
                program: PUMP.into(),
                name: "TradeEvent".into(),
                fields: json!({"user":"w","mint":"sold","is_buy":false}),
            },
            Event {
                program: AMM.into(),
                name: "BuyEvent".into(),
                fields: json!({"user":"other","pool":"p"}),
            },
        ];
        assert_eq!(
            source_markets(&events, &wallets, &Pools::new()),
            BTreeSet::from(["m".into()])
        );
    }
    #[test]
    fn unmapped_pool_is_still_observed() {
        let events = vec![Event {
            program: AMM.into(),
            name: "BuyEvent".into(),
            fields: json!({"user":"w","pool":"p"}),
        }];
        assert_eq!(
            source_markets(&events, &BTreeSet::from(["w".into()]), &Pools::new()),
            BTreeSet::from(["p".into()])
        );
    }
}
