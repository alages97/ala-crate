//! Read-only activity diagnostics. Identity-only events never become financial quotes.
use anyhow::{ensure, Result};
use flate2::{read::GzDecoder, write::GzEncoder, Compression};
use pump_copy::{
    decoder::{Decoder, Pools},
    model::{AMM, PUMP},
    rpc::atomic_json,
};
use rusqlite::{Connection, OpenFlags};
use serde::Serialize;
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::{BufWriter, Write},
    path::PathBuf,
};

#[derive(Default, Serialize)]
struct Activity {
    buys: u64,
    sells: u64,
    pump_fun: u64,
    pump_swap: u64,
    first_received_ms: u64,
    last_received_ms: u64,
    markets: BTreeSet<String>,
    mints: BTreeSet<String>,
    #[serde(skip)]
    minutes: BTreeSet<u64>,
}

fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
    ensure!(
        args.len() == 5 || args.len() == 6,
        "usage: inspect_raw_capture DB POOLS WATCH_WALLETS OUTPUT [MAX_ROWID]"
    );
    let output = PathBuf::from(&args[4]);
    fs::create_dir_all(&output)?;
    ensure!(
        !output.join("identity-observations.jsonl.gz").exists(),
        "refusing to overwrite export"
    );
    let conn = Connection::open_with_flags(
        &args[1],
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI,
    )?;
    conn.execute_batch("PRAGMA cache_size=-65536; PRAGMA mmap_size=1073741824;")?;
    // The writer only appends messages. Pin a row boundary, then release each
    // short read before decoding so a live recorder can checkpoint its WAL.
    let max_rowid: u64 = match args.get(5) {
        Some(value) => value.parse()?,
        None => conn.query_row(
            "SELECT coalesce(max(rowid),0) FROM stream_messages",
            [],
            |r| r.get(0),
        )?,
    };
    let capture_at_analysis_start = pump_copy::stream::status(&conn)?;
    let pools: Pools = serde_json::from_reader(fs::File::open(&args[2])?)?;
    let watched: BTreeSet<String> = serde_json::from_reader(fs::File::open(&args[3])?)?;
    let conflicts: BTreeSet<String> = conn
        .prepare("SELECT DISTINCT signature FROM stream_conflicts")?
        .query_map([], |r| r.get(0))?
        .collect::<rusqlite::Result<_>>()?;
    let mut errors: BTreeMap<String, u64> = BTreeMap::new();
    let decoder = Decoder::default();
    let mut wallets: BTreeMap<String, Activity> = BTreeMap::new();
    let mut market_counts: BTreeMap<String, [u64; 2]> = BTreeMap::new();
    let mut mints = BTreeSet::new();
    let mut unresolved_pools = BTreeSet::new();
    let mut counts: BTreeMap<String, u64> = BTreeMap::new();
    let mut recover_errors: BTreeMap<String, u64> = BTreeMap::new();
    let mut tails: BTreeMap<String, u64> = BTreeMap::new();
    let mut samples: BTreeMap<String, Vec<Value>> = BTreeMap::new();
    let mut dest = GzEncoder::new(
        BufWriter::new(fs::File::create(
            output.join("identity-observations.jsonl.gz"),
        )?),
        Compression::fast(),
    );
    let mut processed = 0u64;
    let mut inspected_messages = 0u64;
    let mut lower = 0u64;
    while lower < max_rowid {
        let upper = (lower + 2048).min(max_rowid);
        let page = conn.prepare("SELECT signature,slot,received_ms,failed,CASE WHEN failed=0 THEN payload_gzip END,coalesce(decode_error,'none') FROM stream_messages WHERE rowid>?1 AND rowid<=?2 ORDER BY rowid")?
            .query_map([lower, upper], |r| Ok((r.get::<_,String>(0)?,r.get::<_,u64>(1)?,r.get::<_,u64>(2)?,r.get::<_,bool>(3)?,r.get::<_,Option<Vec<u8>>>(4)?,r.get::<_,String>(5)?)))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        lower = upper;
        for (sig, slot, received, failed, blob, error) in page {
            inspected_messages += 1;
            *errors.entry(error).or_default() += 1;
            if failed {
                continue;
            }
            processed += 1;
            if processed % 250_000 == 0 {
                eprintln!(
                    "inspected {processed} successful notifications; {} wallet identities",
                    wallets.len()
                );
            }
            if sig == "1".repeat(64) || conflicts.contains(&sig) {
                *counts
                    .entry("excluded_conflicting_or_placeholder".into())
                    .or_default() += 1;
                continue;
            }
            let blob = blob.expect("successful rows include the payload");
            let payload: Value = serde_json::from_reader(GzDecoder::new(blob.as_slice()))?;
            let v = &payload["params"]["result"]["value"];
            let tx = json!({"transaction":{"message":{"accountKeys":[PUMP,AMM]}},"meta":{"err":v["err"],"logMessages":v["logs"],"innerInstructions":[]}});
            let events = match decoder.capture_identity_events(&tx) {
                Ok(events) => events,
                Err(err) => {
                    *recover_errors.entry(err.to_string()).or_default() += 1;
                    continue;
                }
            };
            if events.is_empty() {
                *counts
                    .entry("successful_notifications_without_supported_trade_identity".into())
                    .or_default() += 1;
            }
            for (index, e) in events.into_iter().enumerate() {
                if !matches!(e.name.as_str(), "TradeEvent" | "BuyEvent" | "SellEvent") {
                    continue;
                }
                let Some(user) = e.fields["user"].as_str() else {
                    continue;
                };
                let buy = if e.name == "TradeEvent" {
                    e.fields["is_buy"].as_bool().unwrap_or(false)
                } else {
                    e.name == "BuyEvent"
                };
                let (market, mint) = if e.program == PUMP {
                    let Some(mint) = e.fields["mint"].as_str() else {
                        continue;
                    };
                    (format!("curve:{mint}"), Some(mint.to_owned()))
                } else {
                    let Some(pool) = e.fields["pool"].as_str() else {
                        continue;
                    };
                    let mint = pools.get(pool).map(|x| x.base_mint.clone());
                    if mint.is_none() {
                        unresolved_pools.insert(pool.to_owned());
                        *counts
                            .entry("unresolved_pool_identity_events".into())
                            .or_default() += 1;
                    }
                    (pool.to_owned(), mint)
                };
                let venue = if e.program == PUMP {
                    "pump_fun"
                } else {
                    "pump_swap"
                };
                let tail = e.fields["_unparsed_tail_bytes"].as_u64().unwrap_or(0);
                *tails.entry(format!("{}:{tail}", e.name)).or_default() += 1;
                *counts.entry("trade_identity_events".into()).or_default() += 1;
                *counts.entry(venue.into()).or_default() += 1;
                *counts
                    .entry(if buy { "buys" } else { "sells" }.into())
                    .or_default() += 1;
                let activity = wallets.entry(user.to_owned()).or_default();
                if activity.first_received_ms == 0 {
                    activity.first_received_ms = received;
                }
                activity.last_received_ms = activity.last_received_ms.max(received);
                activity.minutes.insert(received / 60_000);
                activity.markets.insert(market.clone());
                if let Some(ref mint) = mint {
                    activity.mints.insert(mint.clone());
                    mints.insert(mint.clone());
                }
                if buy {
                    activity.buys += 1;
                } else {
                    activity.sells += 1;
                }
                if venue == "pump_fun" {
                    activity.pump_fun += 1;
                } else {
                    activity.pump_swap += 1;
                }
                market_counts.entry(market.clone()).or_default()[if buy { 0 } else { 1 }] += 1;
                let item =
                    json!([sig, slot, received, index, venue, user, market, mint, buy, tail]);
                serde_json::to_writer(&mut dest, &item)?;
                dest.write_all(b"\n")?;
                let sample_key = format!("{venue}:{}", if buy { "buy" } else { "sell" });
                let sample = samples.entry(sample_key).or_default();
                if sample.len() < 3 {
                    sample.push(
                    json!({"signature":sig,"slot":slot,"event_index":index,"identity":e.fields}),
                );
                }
            }
        }
    }
    dest.finish()?.flush()?;
    let summaries: Vec<_> = wallets.iter().map(|(wallet,a)|json!({"wallet":wallet,"buy_observations":a.buys,"sell_observations":a.sells,"trade_observations":a.buys+a.sells,"pump_fun":a.pump_fun,"pump_swap":a.pump_swap,"distinct_markets":a.markets.len(),"known_mints":a.mints.len(),"active_minutes":a.minutes.len(),"first_received_ms":a.first_received_ms,"last_received_ms":a.last_received_ms,"financial_rank_eligible":false})).collect();
    let mut ranked = summaries.clone();
    ranked.sort_by_key(|x| std::cmp::Reverse(x["trade_observations"].as_u64().unwrap_or(0)));
    let watched_activity: Vec<_> = watched
        .iter()
        .map(|w| {
            summaries
                .iter()
                .find(|s| s["wallet"] == w.as_str())
                .cloned()
                .unwrap_or(
                    json!({"wallet":w,"trade_observations":0,"financial_rank_eligible":false}),
                )
        })
        .collect();
    atomic_json(&output.join("wallet-activity.json"), &json!(summaries))?;
    atomic_json(&output.join("samples.json"), &json!(samples))?;
    atomic_json(
        &output.join("unresolved-pools.json"),
        &json!(unresolved_pools),
    )?;
    let report = json!({"snapshot_max_rowid":max_rowid,"notifications_inspected":inspected_messages,"successful_notifications_inspected":processed,"capture":capture_at_analysis_start,"capture_scope":"Capture status at analysis start; census is bounded by snapshot_max_rowid. Conflict exclusions are those known at analysis start. Later data is not included.","strict_decode_error_counts":errors,"identity_counts":counts,"identity_decode_errors":recover_errors,"event_tail_lengths":tails,"unique_wallet_identities":wallets.len(),"unique_markets":market_counts.len(),"known_distinct_mints":mints.len(),"unresolved_pool_identities":unresolved_pools.len(),"top_activity_wallets":ranked.into_iter().take(20).collect::<Vec<_>>(),"previous_watchlist_activity":watched_activity,"financial_rank_eligible":false,"limitation":"Activity-only diagnostic under the published identity-prefix layout; no amounts, fees, reserve quotes, volume rankings, returns or execution results inferred from unknown layouts.","identity_export_columns":["signature","slot","received_ms","event_index","venue","user","market","known_mint","is_buy","unparsed_tail_bytes"]});
    atomic_json(&output.join("inspection.json"), &report)?;
    println!(
        "{}",
        serde_json::to_string(
            &json!({"counts":report["identity_counts"],"wallets":wallets.len(),"markets":market_counts.len()})
        )?
    );
    Ok(())
}
