//! Offline export for diagnostic analysis; receipt order is not execution order.
use anyhow::Result;
use flate2::{write::GzEncoder, Compression};
use pump_copy::{
    config::Config,
    decoder::{Decoder, Event, PoolInfo, Pools},
    model::{Side, AMM, PUMP},
    rpc::atomic_json,
};
use rusqlite::{Connection, OpenFlags};
use serde_json::{json, Value};
use solana_pubkey::Pubkey;
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::{BufWriter, Write},
    path::PathBuf,
    str::FromStr,
};

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let db = &args[1];
    let cached = &args[2];
    let output = PathBuf::from(&args[3]);
    fs::create_dir_all(&output)?;
    let conn = Connection::open_with_flags(
        format!("file:{db}?immutable=1"),
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI,
    )?;
    conn.execute_batch("PRAGMA cache_size=-262144; PRAGMA mmap_size=1073741824;")?;
    let mut pools: Pools = serde_json::from_reader(fs::File::open(cached)?)?;
    let mut created = 0;
    let reuse = std::env::var_os("FROZEN_REUSE_POOL_CACHE").is_some();
    if reuse {
        let cached_count = pools.len();
        pools = serde_json::from_reader(fs::File::open(output.join("pools.json"))?)?;
        created = pools.len().saturating_sub(cached_count);
    }
    let mut q = conn.prepare(if reuse {
        "SELECT fields_json FROM stream_events WHERE 0 AND program=?1"
    } else {
        "SELECT fields_json FROM stream_events WHERE program=?1 AND event_name='CreatePoolEvent'"
    })?;
    for r in q.query_map([AMM], |r| r.get::<_, String>(0))? {
        let f: Value = serde_json::from_str(&r?)?;
        let mint = f["base_mint"].as_str().unwrap();
        let creator = f["creator"].as_str().unwrap();
        let canonical = f["index"].as_u64() == Some(0)
            && Pubkey::find_program_address(
                &[b"pool-authority", Pubkey::from_str(mint)?.as_ref()],
                &Pubkey::from_str(PUMP)?,
            )
            .0
            .to_string()
                == creator;
        let info = PoolInfo {
            base_mint: mint.into(),
            quote_mint: f["quote_mint"].as_str().unwrap().into(),
            creator: creator.into(),
            canonical,
        };
        if pools
            .insert(f["pool"].as_str().unwrap().into(), info)
            .is_none()
        {
            created += 1;
        }
    }
    atomic_json(&output.join("pools.json"), &pools)?;
    eprintln!(
        "Pool metadata ready: {} total, {} added from captured creation events",
        pools.len(),
        created
    );
    let decoder = Decoder::default();
    let has_conflicts: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='stream_conflicts')",
        [],
        |r| r.get(0),
    )?;
    let conflicts: BTreeSet<String> = if has_conflicts {
        conn.prepare("SELECT DISTINCT signature FROM stream_conflicts")?
            .query_map([], |r| r.get(0))?
            .collect::<rusqlite::Result<_>>()?
    } else {
        BTreeSet::new()
    };
    let mut dest = GzEncoder::new(
        BufWriter::new(fs::File::create(output.join("observed-trades.jsonl.gz"))?),
        Compression::fast(),
    );
    let mut counts: BTreeMap<&str, u64> = BTreeMap::new();
    let mut errors = Vec::new();
    // Events are inserted in the same transaction as their receipt row, so event
    // rowid preserves receipt/instruction order without sorting gigabytes of JSON.
    let mut q=conn.prepare("SELECT e.signature,e.event_index,e.program,e.event_name,e.fields_json,m.received_ms,m.slot,m.session_id,m.rowid FROM stream_events e JOIN stream_messages m USING(signature) ORDER BY e.rowid")?;
    let mut rows = q.query([])?;
    let mut previous_receipt = 0;
    while let Some(r) = rows.next()? {
        *counts.entry("events").or_default() += 1;
        let signature: String = r.get(0)?;
        if conflicts.contains(&signature) {
            *counts.entry("excluded_conflicting_events").or_default() += 1;
            continue;
        }
        let index: u32 = r.get(1)?;
        let program: String = r.get(2)?;
        let name: String = r.get(3)?;
        let fields: Value = serde_json::from_str(&r.get::<_, String>(4)?)?;
        if !matches!(name.as_str(), "TradeEvent" | "BuyEvent" | "SellEvent") {
            continue;
        }
        if program == AMM
            && !fields["pool"]
                .as_str()
                .is_some_and(|p| pools.contains_key(p))
        {
            *counts.entry("unresolved_pool_trades").or_default() += 1;
            continue;
        }
        let timestamp = fields["timestamp"].as_u64();
        let t = match decoder.trade_from_event(
            Event {
                program,
                name,
                fields,
            },
            &signature,
            0,
            index,
            0,
            &pools,
        ) {
            Ok((Some(t), _)) => t,
            Ok((None, non_sol)) => {
                *counts
                    .entry(if non_sol {
                        "non_sol"
                    } else {
                        "noncanonical_or_other"
                    })
                    .or_default() += 1;
                continue;
            }
            Err(e) => {
                *counts.entry("rejected").or_default() += 1;
                if errors.len() < 10 {
                    errors.push(e.to_string());
                }
                continue;
            }
        };
        if t.reserve.validate().is_err()
            || t.tokens == 0
            || (t.side == Side::Buy && t.quote_lamports == 0)
        {
            *counts.entry("rejected").or_default() += 1;
            continue;
        }
        let recv: u64 = r.get(5)?;
        if recv < previous_receipt {
            *counts.entry("receipt_order_inversions").or_default() += 1;
            let lag = counts.entry("max_receipt_lateness_ms").or_default();
            *lag = (*lag).max(previous_receipt - recv);
        }
        previous_receipt = previous_receipt.max(recv);
        let slot: u64 = r.get(6)?;
        let session: u64 = r.get(7)?;
        let rowid: u64 = r.get(8)?;
        // Compact schema documented in metadata.json; all amounts are raw integers.
        let encoded = serde_json::to_vec(&json!([
            t.mint,
            t.wallet,
            t.side == Side::Buy,
            t.quote_lamports,
            t.tokens,
            recv,
            slot,
            signature,
            index,
            t.reserve.quote,
            t.reserve.base,
            t.reserve.real_base,
            t.reserve.real_quote,
            t.venue,
            t.reserve.fee_bps,
            timestamp,
            session,
            rowid
        ]))?;
        dest.write_all(&encoded)?;
        dest.write_all(b"\n")?;
        let n = counts.entry("supported_trades").or_default();
        *n += 1;
        if *n % 500000 == 0 {
            eprintln!("Exported {n} observed trades");
        }
    }
    dest.finish()?.flush()?;
    let config: Config = toml::from_str(&fs::read_to_string("config/default.toml")?)?;
    atomic_json(
        &output.join("metadata.json"),
        &json!({"counts":counts,"errors":errors,"pools_added_from_capture":created,"pool_count":pools.len(),"flow":config.flow,"columns":["mint","wallet","buy","quote_lamports","tokens","received_ms","slot","signature","event_index","effective_quote_reserve","base_reserve","real_base_reserve","real_quote_reserve","venue","fee_bps","event_timestamp","session_id","message_rowid"],"limitation":"Offline observed-log diagnostics only. Not a complete, execution-ordered backtest; gaps and unresolved pools remain."}),
    )?;
    println!("{}", serde_json::to_string(&counts)?);
    Ok(())
}
