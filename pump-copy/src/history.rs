//! Bounded, resumable address history acquisition and balance reconciliation.
use crate::{
    decoder::{Decoder, Pools},
    model::{AMM, PUMP, WSOL},
    rpc::{atomic_json, Rpc},
    stream::{gzip, open_store},
};
use anyhow::{ensure, Context, Result};
use rusqlite::{params, Connection};
use serde_json::{json, Value};
use std::{collections::BTreeMap, fs, path::Path};

fn schema(conn: &Connection) -> Result<()> {
    conn.execute_batch("CREATE TABLE IF NOT EXISTS historical_transactions(signature TEXT PRIMARY KEY, slot INTEGER NOT NULL, block_time INTEGER, payload_gzip BLOB NOT NULL);
       CREATE TABLE IF NOT EXISTS history_runs(wallet TEXT NOT NULL, since_ms INTEGER NOT NULL, until_ms INTEGER NOT NULL, provider TEXT NOT NULL, complete INTEGER NOT NULL, reason TEXT NOT NULL, fetched INTEGER NOT NULL, PRIMARY KEY(wallet,since_ms,until_ms,provider));")?;
    Ok(())
}
fn unzip(bytes: Vec<u8>) -> Result<Value> {
    Ok(serde_json::from_reader(flate2::read::GzDecoder::new(
        bytes.as_slice(),
    ))?)
}
pub async fn backfill(
    rpc: &Rpc,
    db: &Path,
    wallet: &str,
    since_ms: u64,
    until_ms: u64,
    max_transactions: usize,
    helius: bool,
) -> Result<Value> {
    ensure!(
        since_ms < until_ms && max_transactions > 0,
        "invalid history bounds"
    );
    ensure!(
        bs58::decode(wallet).into_vec()?.len() == 32,
        "invalid wallet address"
    );
    let mut conn = open_store(db)?;
    schema(&conn)?;
    let provider = if helius { "helius" } else { "standard_rpc" };
    let mut cursor: Option<String> = None;
    let mut fetched = 0usize;
    let mut complete = false;
    let mut reason = "transaction_cap_reached";
    let started = std::time::Instant::now();
    let mut pages = 0usize;
    conn.execute(
        "INSERT OR REPLACE INTO history_runs VALUES(?1,?2,?3,?4,0,'acquisition_incomplete',0)",
        params![wallet, since_ms, until_ms, provider],
    )?;
    while fetched < max_transactions {
        if pages >= 100 {
            reason = "signature_page_cap_reached";
            break;
        }
        pages += 1;
        let page = if helius {
            let mut options = json!({"transactionDetails":"full","encoding":"json","maxSupportedTransactionVersion":rpc.max_transaction_version,"sortOrder":"desc","limit":(max_transactions-fetched).min(1000),"commitment":"finalized","filters":{"blockTime":{"gte":since_ms/1000,"lte":until_ms/1000},"status":"any","tokenAccounts":"all"}});
            if let Some(c) = &cursor {
                options["paginationToken"] = json!(c);
            }
            rpc.call("getTransactionsForAddress", json!([wallet, options]))
                .await?
        } else {
            let mut options = json!({"limit":1000,"commitment":"finalized"});
            if let Some(c) = &cursor {
                options["before"] = json!(c);
            }
            json!({"data":rpc.call("getSignaturesForAddress",json!([wallet,options])).await?})
        };
        let entries = page["data"].as_array().context("history page data")?;
        if entries.is_empty() {
            complete = true;
            reason = "reached_history_end";
            break;
        }
        let mut reached_start = false;
        let mut page_exhausted = true;
        for entry in entries {
            if fetched >= max_transactions {
                page_exhausted = false;
                break;
            }
            let timestamp = entry["blockTime"].as_u64();
            if timestamp.is_some_and(|t| t * 1000 > until_ms) {
                continue;
            }
            if timestamp.is_some_and(|t| t * 1000 < since_ms) {
                reached_start = true;
                continue;
            }
            let signature = if helius {
                entry
                    .pointer("/transaction/signatures/0")
                    .and_then(Value::as_str)
            } else {
                entry["signature"].as_str()
            }
            .context("history signature")?;
            let stored = conn.query_row(
                "SELECT payload_gzip FROM historical_transactions WHERE signature=?1",
                [signature],
                |r| r.get::<_, Vec<u8>>(0),
            );
            let full = match stored {
                Ok(bytes) => unzip(bytes)?,
                Err(rusqlite::Error::QueryReturnedNoRows) => {
                    if helius {
                        entry.clone()
                    } else {
                        let v=rpc.call("getTransaction",json!([signature,{"encoding":"json","maxSupportedTransactionVersion":rpc.max_transaction_version,"commitment":"finalized"}])).await?;
                        ensure!(
                            !v.is_null(),
                            "history transaction unavailable; acquisition stopped without skipping"
                        );
                        v
                    }
                }
                Err(e) => return Err(e.into()),
            };
            let slot = full["slot"]
                .as_u64()
                .or_else(|| entry["slot"].as_u64())
                .context("history slot")?;
            ensure!(
                full.pointer("/transaction/signatures/0")
                    .and_then(Value::as_str)
                    == Some(signature),
                "history signature mismatch"
            );
            let time = full["blockTime"].as_u64().or(timestamp);
            let failed = !full
                .pointer("/meta/err")
                .context("transaction status")?
                .is_null();
            let tx = conn.transaction()?;
            tx.execute(
                "INSERT OR IGNORE INTO historical_transactions VALUES(?1,?2,?3,?4)",
                params![signature, slot, time, gzip(&full)?],
            )?;
            tx.execute("INSERT OR IGNORE INTO wallet_history(wallet,signature,slot,block_time,failed) VALUES(?1,?2,?3,?4,?5)",params![wallet,signature,slot,time,failed])?;
            tx.commit()?;
            fetched += 1;
            conn.execute("UPDATE history_runs SET fetched=?1 WHERE wallet=?2 AND since_ms=?3 AND until_ms=?4 AND provider=?5",params![fetched,wallet,since_ms,until_ms,provider])?;
            if fetched % 50 == 0 {
                eprintln!("wallet {wallet}: {fetched} history transactions");
            }
        }
        if !page_exhausted {
            break;
        }
        if reached_start {
            complete = true;
            reason = "reached_requested_start";
            break;
        }
        let next = if helius {
            page["paginationToken"].as_str().map(str::to_owned)
        } else {
            entries
                .last()
                .and_then(|e| e["signature"].as_str())
                .map(str::to_owned)
        };
        if next.is_none() {
            complete = true;
            reason = "reached_history_end";
            break;
        }
        ensure!(next != cursor, "history pagination did not advance");
        cursor = next;
    }
    conn.execute(
        "INSERT OR REPLACE INTO history_runs VALUES(?1,?2,?3,?4,?5,?6,?7)",
        params![wallet, since_ms, until_ms, provider, complete, reason, fetched],
    )?;
    Ok(
        json!({"wallet":wallet,"since_ms":since_ms,"until_ms":until_ms,"provider":provider,"transactions_in_run":fetched,"history_pages":pages,"elapsed_ms":started.elapsed().as_millis(),"complete":complete,"reason":reason,"token_account_coverage":if helius{"associated token accounts included"}else{"direct address references only; ATA-only history requires enrichment"}}),
    )
}

fn account_keys(tx: &Value) -> Result<Vec<String>> {
    let mut keys = tx
        .pointer("/transaction/message/accountKeys")
        .and_then(Value::as_array)
        .context("account keys")?
        .iter()
        .map(|v| {
            v.as_str()
                .map(str::to_owned)
                .context("RPC encoding must be json")
        })
        .collect::<Result<Vec<_>>>()?;
    for p in [
        "/meta/loadedAddresses/writable",
        "/meta/loadedAddresses/readonly",
    ] {
        if let Some(a) = tx.pointer(p).and_then(Value::as_array) {
            for v in a {
                keys.push(v.as_str().context("loaded key")?.into());
            }
        }
    }
    Ok(keys)
}
pub fn token_changes(tx: &Value, wallet: &str) -> Result<BTreeMap<String, i128>> {
    let mut deltas = BTreeMap::new();
    for (path, sign) in [
        ("/meta/preTokenBalances", -1i128),
        ("/meta/postTokenBalances", 1i128),
    ] {
        for b in tx
            .pointer(path)
            .and_then(Value::as_array)
            .context("missing token balances")?
        {
            if b["owner"].as_str() != Some(wallet) {
                continue;
            }
            let mint = b["mint"].as_str().context("token mint")?;
            let amount = b
                .pointer("/uiTokenAmount/amount")
                .and_then(Value::as_str)
                .context("token raw amount")?
                .parse::<u64>()?;
            *deltas.entry(mint.to_owned()).or_default() += sign * amount as i128;
        }
    }
    deltas.retain(|_, v| *v != 0);
    Ok(deltas)
}
pub fn audit_transactions(
    wallet: &str,
    transactions: &[Value],
    decoder: &Decoder,
    pools: &Pools,
) -> Result<Value> {
    let mut swaps = 0u64;
    let mut fees = 0u128;
    let mut failed_fees = 0u128;
    let mut failed = 0u64;
    let mut signer = 0u64;
    let mut quote_change = 0i128;
    let mut swap_cashflow = 0i128;
    let mut unmatched = 0u64;
    let mut unknown = 0u64;
    let mut external = 0u64;
    let mut extra_outflows = 0u128;
    let mut residual_inflows = 0u128;
    let mut unsupported = 0u64;
    let mut balance_receipt_errors = 0u64;
    let mut rows = vec![];
    let mut mints = std::collections::BTreeSet::new();
    for full in transactions {
        let balance_receipt = match crate::balances::receipt(full, wallet) {
            Ok(v) => {
                if v["metadata_complete"] != true {
                    balance_receipt_errors += 1;
                }
                v
            }
            Err(e) => {
                balance_receipt_errors += 1;
                json!({"metadata_complete":false,"error":e.to_string(),"eligible_for_profit_ranking":false})
            }
        };
        let keys = account_keys(full)?;
        let meta = &full["meta"];
        let index = keys.iter().position(|k| k == wallet);
        if index.is_some_and(|i| {
            i < full
                .pointer("/transaction/message/header/numRequiredSignatures")
                .and_then(Value::as_u64)
                .unwrap_or(0) as usize
        }) {
            signer += 1;
        }
        let fee = if keys.first().is_some_and(|k| k == wallet) {
            meta["fee"].as_u64().context("network fee")?
        } else {
            0
        };
        fees += fee as u128;
        let native = if let Some(i) = index {
            meta["postBalances"][i].as_u64().context("post lamports")? as i128
                - meta["preBalances"][i].as_u64().context("pre lamports")? as i128
        } else {
            0
        };
        let actual_tokens = token_changes(full, wallet)?;
        let quote = native + actual_tokens.get(WSOL).copied().unwrap_or(0);
        quote_change += quote;
        if !meta["err"].is_null() {
            failed += 1;
            failed_fees += fee as u128;
            rows.push(json!({"signature":full.pointer("/transaction/signatures/0"),"slot":full["slot"],"failed":true,"network_fee_paid_lamports":fee,"native_plus_wsol_delta_lamports":quote,"balance_receipt":balance_receipt}));
            continue;
        }
        let slot = full["slot"].as_u64().context("slot")?;
        let block = json!({"blockTime":full["blockTime"],"blockhash":"history-audit","transactions":[full]});
        let parsed = decoder.block(slot, &block, pools);
        let mut expected = BTreeMap::new();
        let mut expected_quote = 0i128;
        let mut n = 0u64;
        let mut decode_error = None;
        match parsed {
            Ok(b) => {
                unsupported += b.skipped_non_sol;
                for t in b.trades.iter().filter(|t| t.wallet == wallet) {
                    n += 1;
                    mints.insert(t.mint.clone());
                    let sign = if t.side == crate::model::Side::Buy {
                        1i128
                    } else {
                        -1
                    };
                    *expected.entry(t.mint.clone()).or_insert(0i128) += sign * t.tokens as i128;
                    expected_quote -= sign * t.quote_lamports as i128;
                }
            }
            Err(e) => {
                unknown += 1;
                decode_error = Some(e.to_string());
            }
        }
        swaps += n;
        swap_cashflow += expected_quote;
        let token_mismatch = expected
            .iter()
            .any(|(m, v)| actual_tokens.get(m).copied().unwrap_or(0) != *v)
            || actual_tokens
                .iter()
                .any(|(m, v)| m != WSOL && expected.get(m).copied().unwrap_or(0) != *v);
        if token_mismatch {
            unmatched += 1;
        }
        let residual = quote - expected_quote + fee as i128;
        if n > 0 {
            if residual < 0 {
                extra_outflows += (-residual) as u128;
            } else {
                residual_inflows += residual as u128;
            }
        } else {
            external += 1;
        }
        rows.push(json!({"signature":full.pointer("/transaction/signatures/0"),"slot":slot,"supported_swaps":n,"native_plus_wsol_delta_lamports":quote,"swap_cashflow_lamports":if decode_error.is_none(){Some(expected_quote)}else{None},"network_fee_paid_lamports":fee,"unexplained_quote_residual_lamports":if decode_error.is_none(){Some(residual)}else{None},"token_delta_mismatch":token_mismatch,"decode_error":decode_error,"balance_receipt":balance_receipt,"observed_token_deltas":actual_tokens}));
    }
    let complete =
        unknown == 0 && unsupported == 0 && unmatched == 0 && balance_receipt_errors == 0;
    Ok(
        json!({"wallet":wallet,"transactions":transactions.len(),"direct_signer_transactions":signer,"supported_swaps":swaps,"distinct_supported_mints":mints.len(),"failed_transactions":failed,"network_fees_paid_lamports":fees,"failed_transaction_fees_lamports":failed_fees,"supported_swap_cashflow_lamports":swap_cashflow,"cashflow_after_all_network_fees_and_extra_swap_outflows_lamports":if complete {Some(swap_cashflow-fees as i128-extra_outflows as i128)}else{None},"swap_accounting_complete":complete,"incomplete_balance_receipts":balance_receipt_errors,"extra_swap_outflows_lamports":extra_outflows,"unexplained_swap_inflows_lamports":residual_inflows,"token_delta_mismatches":unmatched,"unparsed_transactions":unknown,"unsupported_quote_events":unsupported,"other_successful_transactions":external,"native_plus_wsol_change_lamports":quote_change,"eligible_for_funded_copy":false,"note":"Audit only, not P&L certification. Open/initial inventory, external venues, transfer origins, missing ATA history and incomplete capture must be reconciled. Balance changes include funding and are not investment returns. Unsupported transactions retain balance evidence but their swap cashflow remains unknown.","transactions_detail":rows}),
    )
}
pub async fn audit(rpc: &Rpc, db: &Path, wallet: &str, output: &Path) -> Result<()> {
    let conn = open_store(db)?;
    schema(&conn)?;
    let decoder = Decoder::default();
    let mut pools = Pools::new();
    let mut q=conn.prepare("SELECT t.payload_gzip FROM historical_transactions t JOIN wallet_history h ON h.signature=t.signature WHERE h.wallet=?1 ORDER BY t.slot,t.signature")?;
    let txs = q
        .query_map([wallet], |r| r.get::<_, Vec<u8>>(0))?
        .map(|b| unzip(b?))
        .collect::<Result<Vec<_>>>()?;
    ensure!(!txs.is_empty(), "no acquired wallet history");
    let (missing, pool_scan_errors) = auditable_pools(&decoder, &txs, &mut pools);
    {
        for addresses in missing.chunks(100) {
            let response = rpc
                .call(
                    "getMultipleAccounts",
                    json!([addresses,{"encoding":"base64","commitment":"finalized"}]),
                )
                .await?;
            use base64::Engine as _;
            for (address, a) in addresses
                .iter()
                .zip(response["value"].as_array().context("account values")?)
            {
                if a["owner"].as_str() != Some(AMM) {
                    continue;
                }
                let bytes = base64::engine::general_purpose::STANDARD
                    .decode(a["data"][0].as_str().context("account data")?)?;
                pools.insert(address.clone(), crate::decoder::pool_info(&bytes)?);
            }
        }
    }
    let mut report = audit_transactions(wallet, &txs, &decoder, &pools)?;
    report["pool_scan_errors"] = json!(pool_scan_errors);
    let mut q=conn.prepare("SELECT since_ms,until_ms,provider,complete,reason,fetched FROM history_runs WHERE wallet=?1")?;
    report["history_runs"]=json!(q.query_map([wallet],|r|Ok(json!({"since_ms":r.get::<_,u64>(0)?,"until_ms":r.get::<_,u64>(1)?,"provider":r.get::<_,String>(2)?,"complete":r.get::<_,bool>(3)?,"reason":r.get::<_,String>(4)?,"fetched":r.get::<_,u64>(5)?})))?.collect::<rusqlite::Result<Vec<_>>>()?);
    atomic_json(output, &report)?;
    Ok(())
}

/// Unsupported event layouts cannot prevent independent balance evidence from being audited.
pub fn auditable_pools(
    decoder: &Decoder,
    txs: &[Value],
    pools: &mut Pools,
) -> (Vec<String>, Vec<Value>) {
    let mut missing = std::collections::BTreeSet::new();
    let mut errors = vec![];
    for tx in txs {
        match decoder.required_pools(&json!({"transactions":[tx]}), pools) {
            Ok(found) => missing.extend(found),
            Err(e) => errors.push(
                json!({"signature":tx["transaction"]["signatures"][0],"error":e.to_string()}),
            ),
        }
    }
    (
        missing
            .into_iter()
            .filter(|p| !pools.contains_key(p))
            .collect(),
        errors,
    )
}

pub fn import_archive(db: &Path, archive: &Path) -> Result<Value> {
    let mut conn = open_store(db)?;
    schema(&conn)?;
    let mut paths = fs::read_dir(archive.join("raw"))?
        .map(|e| Ok(e?.path()))
        .collect::<Result<Vec<_>>>()?;
    paths.sort();
    let mut transactions = 0usize;
    for path in paths {
        if path.extension().is_some_and(|s| s == "tmp") {
            continue;
        }
        let raw: Value = if path.extension().is_some_and(|s| s == "gz") {
            serde_json::from_reader(flate2::read::GzDecoder::new(fs::File::open(&path)?))?
        } else {
            serde_json::from_reader(fs::File::open(&path)?)?
        };
        let slot = path
            .file_name()
            .and_then(|s| s.to_str())
            .context("slot filename")?
            .split('.')
            .next()
            .context("slot")?
            .parse::<u64>()?;
        let dbtx = conn.transaction()?;
        for (index, t) in raw["transactions"]
            .as_array()
            .context("raw transactions")?
            .iter()
            .enumerate()
        {
            let keys = account_keys(t)?;
            if !keys.iter().any(|k| k == PUMP || k == AMM) {
                continue;
            }
            let mut t = t.clone();
            t["slot"] = json!(slot);
            t["blockTime"] = raw["blockTime"].clone();
            t["transactionIndex"] = json!(index);
            let signature = t
                .pointer("/transaction/signatures/0")
                .and_then(Value::as_str)
                .context("signature")?;
            dbtx.execute(
                "INSERT OR IGNORE INTO historical_transactions VALUES(?1,?2,?3,?4)",
                params![signature, slot, raw["blockTime"].as_u64(), gzip(&t)?],
            )?;
            transactions += 1;
        }
        dbtx.commit()?;
    }
    Ok(
        json!({"cached_program_transactions":transactions,"coverage":"Program transaction cache only; address-history enumeration is still required."}),
    )
}
