//! Offline wallet reconciliation from cached transactions and saved pool identities.
use anyhow::Result;
use pump_copy::{
    decoder::{Decoder, Pools},
    history::audit_transactions,
    rpc::atomic_json,
};
use rusqlite::{Connection, OpenFlags};
use serde_json::{json, Value};
use std::{env, fs, path::Path};
fn main() -> Result<()> {
    let a: Vec<_> = env::args().collect();
    let c = Connection::open_with_flags(
        format!("file:{}?immutable=1", a[1]),
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI,
    )?;
    let wallet = &a[3];
    let mut pools: Pools = serde_json::from_reader(fs::File::open(&a[2])?)?;
    let mut q=c.prepare("SELECT t.payload_gzip FROM historical_transactions t JOIN wallet_history h USING(signature) WHERE h.wallet=?1 ORDER BY t.slot,t.signature")?;
    let txs: Vec<Value> = q
        .query_map([wallet], |r| r.get::<_, Vec<u8>>(0))?
        .map(|v| {
            let b = v?;
            Ok(serde_json::from_reader(flate2::read::GzDecoder::new(
                b.as_slice(),
            ))?)
        })
        .collect::<Result<_>>()?;
    let decoder = Decoder::default();
    let (missing, errors) = pump_copy::history::auditable_pools(&decoder, &txs, &mut pools);
    let mut report = audit_transactions(wallet, &txs, &decoder, &pools)?;
    report["missing_pools"] = json!(missing);
    report["pool_scan_errors"] = json!(errors);
    report["pool_identities"] = json!(pools);
    let mut trades = Vec::new();
    for tx in &txs {
        let block =
            json!({"blockTime":tx["blockTime"],"blockhash":"audit-only","transactions":[tx]});
        if let Ok(b) = decoder.block(tx["slot"].as_u64().unwrap(), &block, &pools) {
            for t in b.trades {
                if t.wallet == *wallet {
                    trades.push(t);
                }
            }
        }
    }
    report["cached_supported_trades"] = json!(trades);
    atomic_json(Path::new(&a[4]), &report)?;
    Ok(())
}
