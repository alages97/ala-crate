//! Recover only routing identities from saved notifications, without pricing unknown layouts.
use anyhow::Result;
use pump_copy::{
    decoder::{Decoder, Pools},
    model::{AMM, PUMP},
    rpc::atomic_json,
};
use rusqlite::{Connection, OpenFlags};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    env, fs,
    path::Path,
};
fn main() -> Result<()> {
    let a: Vec<_> = env::args().collect();
    let wallets: BTreeSet<String> = serde_json::from_reader(fs::File::open(&a[2])?)?;
    let pools: Pools = serde_json::from_reader(fs::File::open(&a[3])?)?;
    let c = Connection::open_with_flags(
        format!("file:{}?immutable=1", a[1]),
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI,
    )?;
    let d = Decoder::default();
    let mut q = c.prepare(
        "SELECT signature,received_ms,payload_gzip FROM stream_messages ORDER BY received_ms,rowid",
    )?;
    let mut events = Vec::new();
    let mut errors = Vec::new();
    let mut seeds = BTreeMap::new();
    let mut messages = 0;
    let mut strict_failures = 0;
    for row in q.query_map([], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, u64>(1)?,
            r.get::<_, Vec<u8>>(2)?,
        ))
    })? {
        let (sig, received, blob) = row?;
        messages += 1;
        let p: Value = serde_json::from_reader(flate2::read::GzDecoder::new(blob.as_slice()))?;
        let v = &p["params"]["result"]["value"];
        let tx = json!({"transaction":{"message":{"accountKeys":[PUMP,AMM]}},"meta":{"err":v["err"],"logMessages":v["logs"],"innerInstructions":[]}});
        if d.transaction_events(&tx).is_err() {
            strict_failures += 1;
        }
        match d.capture_identity_events(&tx) {
            Ok(ids) => {
                for (index, e) in ids.iter().enumerate() {
                    events.push(json!({"signature":sig,"received_ms":received,"event_index":index,"program":e.program,"event_name":e.name,"identity":e.fields}));
                    if sig == "1".repeat(64)
                        || !wallets.contains(e.fields["user"].as_str().unwrap_or(""))
                    {
                        continue;
                    }
                    let address = if e.name == "TradeEvent" && e.fields["is_buy"] == true {
                        e.fields["mint"].as_str().map(str::to_owned)
                    } else if e.name == "BuyEvent" {
                        e.fields["pool"].as_str().map(|p| {
                            pools
                                .get(p)
                                .map(|v| v.base_mint.clone())
                                .unwrap_or_else(|| p.into())
                        })
                    } else {
                        None
                    };
                    if let Some(address) = address {
                        if seeds.len() < 64 || seeds.contains_key(&address) {
                            seeds.entry(address).or_insert(json!({"original_source_receipt_ms":received,"source_signature":sig,"recovered_after_schema_error":true,"prior_market_followup_missing":true,"identity_only":true}));
                        }
                    }
                }
            }
            Err(e) => errors.push(json!({"signature":sig,"error":e.to_string()})),
        }
    }
    atomic_json(
        Path::new(&a[4]),
        &json!({"messages":messages,"strict_failures_retained":strict_failures,"identity_events":events,"identity_decode_errors":errors,"seed_markets":seeds,"limitation":"No historical market trades reconstructed. These identifiers are routing hints only; unknown event extensions remain excluded from price/P&L normalization."}),
    )?;
    if let Some(path) = a.get(5) {
        atomic_json(Path::new(path), &seeds)?;
    }
    Ok(())
}
