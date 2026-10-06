use crate::{
    decoder::{pool_info, Decoder, Pools},
    model::{Batch, AMM, PUMP},
};
use anyhow::{bail, ensure, Context, Result};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use serde_json::{json, Value};
use std::{
    fs,
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
    time::Duration,
};

pub struct Rpc {
    client: reqwest::Client,
    url: String,
    pub max_transaction_version: u8,
    interval: Duration,
    last_request: tokio::sync::Mutex<Option<tokio::time::Instant>>,
}
#[derive(Debug)]
struct HttpFailure(u16);
impl std::fmt::Display for HttpFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RPC HTTP status {}", self.0)
    }
}
impl std::error::Error for HttpFailure {}

/// Account inventories contain lookup-table keys as well as static keys.
/// Preserve a clearly marked stub only when full event data cannot affect the
/// supported successful-trade universe (failed or unrelated transactions).
pub fn inventory_stub(tx: &Value) -> Result<(Value, bool)> {
    let accounts = tx
        .pointer("/transaction/accountKeys")
        .and_then(Value::as_array)
        .context("inventory account keys")?;
    let keys = accounts
        .iter()
        .map(|a| {
            a["pubkey"]
                .as_str()
                .map(str::to_owned)
                .context("inventory pubkey")
        })
        .collect::<Result<Vec<_>>>()?;
    let failed = !tx
        .pointer("/meta/err")
        .context("inventory status")?
        .is_null();
    let needs_full = !failed && keys.iter().any(|k| k == PUMP || k == AMM);
    let signers = accounts.iter().filter(|a| a["signer"] == true).count();
    let mut result = tx.clone();
    result["transaction"] = json!({"signatures":tx["transaction"]["signatures"],"message":{"accountKeys":keys,"header":{"numRequiredSignatures":signers}}});
    result["capture_detail"] = json!("account_inventory_only");
    Ok((result, needs_full))
}
impl Rpc {
    pub fn from_env(env: &str, max_transaction_version: u8) -> Result<Self> {
        let url =
            std::env::var(env).with_context(|| format!("set {env} to an HTTP Solana RPC URL"))?;
        let parsed = reqwest::Url::parse(&url).map_err(|_| anyhow::anyhow!("invalid RPC URL"))?;
        ensure!(
            parsed.scheme() == "https"
                || (parsed.scheme() == "http"
                    && matches!(parsed.host_str(), Some("localhost" | "127.0.0.1"))),
            "RPC must use HTTPS or localhost HTTP"
        );
        Ok(Self {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()?,
            url,
            max_transaction_version,
            interval: Duration::from_millis(
                std::env::var("PUMP_COPY_RPC_INTERVAL_MS")
                    .ok()
                    .map(|s| s.parse::<u64>())
                    .transpose()
                    .context("invalid RPC interval")?
                    .unwrap_or(1200),
            ),
            last_request: tokio::sync::Mutex::new(None),
        })
    }
    pub async fn call(&self, method: &str, params: Value) -> Result<Value> {
        for attempt in 0..5u32 {
            {
                let mut last = self.last_request.lock().await;
                if let Some(at) = *last {
                    tokio::time::sleep_until(at + self.interval).await;
                }
                *last = Some(tokio::time::Instant::now());
            }
            // Never attach reqwest's URL-bearing errors: URLs may contain keys.
            if let Ok(response) = self
                .client
                .post(&self.url)
                .json(&json!({"jsonrpc":"2.0","id":1,"method":method,"params":params}))
                .send()
                .await
            {
                let status = response.status();
                if status.as_u16() == 429 {
                    let wait = response
                        .headers()
                        .get("retry-after")
                        .and_then(|v| v.to_str().ok())
                        .and_then(|s| s.parse::<u64>().ok())
                        .unwrap_or(15)
                        .clamp(1, 60);
                    tokio::time::sleep(Duration::from_secs(wait)).await;
                    continue;
                }
                if status.is_success() {
                    let body: Value = response
                        .json()
                        .await
                        .map_err(|_| anyhow::anyhow!("invalid RPC JSON for {method}"))?;
                    if let Some(error) = body.get("error") {
                        let code = error["code"].as_i64().unwrap_or(0);
                        if !matches!(code, -32005 | -32016 | 429) {
                            bail!("RPC {method} error code {code}; endpoint details withheld");
                        }
                    } else {
                        return body.get("result").cloned().context("missing RPC result");
                    }
                } else if status.as_u16() != 429 && !status.is_server_error() {
                    return Err(HttpFailure(status.as_u16()).into());
                }
            }
            if attempt < 4 {
                tokio::time::sleep(Duration::from_millis(500u64 << attempt)).await;
            }
        }
        bail!("RPC {method} failed after 5 attempts; endpoint details withheld")
    }
    pub async fn finalized_slot(&self) -> Result<u64> {
        self.call("getSlot", json!([{"commitment":"finalized"}]))
            .await?
            .as_u64()
            .context("invalid finalized slot")
    }
    pub async fn slots(&self, start: u64, end: u64) -> Result<Vec<u64>> {
        ensure!(
            end >= start && end - start < 1000,
            "RPC range must be at most 1000 slots"
        );
        let slots: Vec<_> = self
            .call("getBlocks", json!([start,end,{"commitment":"finalized"}]))
            .await?
            .as_array()
            .context("invalid getBlocks result")?
            .iter()
            .map(|v| v.as_u64().context("invalid slot"))
            .collect::<Result<_>>()?;
        ensure!(
            slots.windows(2).all(|s| s[0] < s[1]) && slots.iter().all(|s| *s >= start && *s <= end),
            "invalid RPC slot ordering/range"
        );
        Ok(slots)
    }
    pub async fn capture(
        &self,
        slot: u64,
        archive: &Path,
        decoder: &Decoder,
        pools: &mut Pools,
    ) -> Result<Batch> {
        let normalized = archive.join("blocks").join(format!("{slot}.json"));
        if normalized.exists() {
            let b: Batch = serde_json::from_reader(fs::File::open(normalized)?)?;
            b.validate()?;
            return Ok(b);
        }
        let raw_path = archive.join("raw").join(format!("{slot}.json"));
        let compressed_path = archive.join("raw").join(format!("{slot}.json.gz"));
        let raw: Value = if raw_path.exists() {
            serde_json::from_reader(fs::File::open(&raw_path)?)?
        } else if compressed_path.exists() {
            serde_json::from_reader(flate2::read::GzDecoder::new(fs::File::open(
                &compressed_path,
            )?))?
        } else {
            let value = if archive
                .join("inventories")
                .join(format!("{slot}.json.gz"))
                .exists()
            {
                self.capture_inventory(slot, archive).await?
            } else {
                match self.call("getBlock",json!([slot,{"encoding":"json","transactionDetails":"full","rewards":false,"commitment":"finalized","maxSupportedTransactionVersion":self.max_transaction_version}])).await {
                    Ok(value)=>value,
                    Err(error) if error.downcast_ref::<HttpFailure>().is_some_and(|e|e.0==413)=>{
                        eprintln!("slot {slot}: full response too large; recovering from ordered account inventory");
                        self.capture_inventory(slot,archive).await?
                    },
                    Err(error)=>return Err(error),
                }
            };
            ensure!(
                !value.is_null(),
                "block {slot} unavailable; capture stopped without skipping"
            );
            fs::create_dir_all(archive.join("raw"))?;
            let temp = compressed_path.with_extension("gz.tmp");
            let mut file = fs::File::create(&temp)?;
            file.write_all(&crate::stream::gzip(&value)?)?;
            file.sync_all()?;
            fs::rename(temp, &compressed_path)?;
            value
        };
        let missing = decoder
            .required_pools(&raw, pools)
            .with_context(|| format!("events in slot {slot}"))?;
        for group in missing.chunks(100) {
            let accounts = self
                .call(
                    "getMultipleAccounts",
                    json!([group,{"encoding":"base64","commitment":"finalized"}]),
                )
                .await?;
            let values = accounts["value"].as_array().context("pool accounts")?;
            ensure!(values.len() == group.len(), "pool account count mismatch");
            for (address, account) in group.iter().zip(values) {
                ensure!(
                    account["owner"].as_str() == Some(AMM),
                    "pool {address} unavailable or wrong owner"
                );
                let bytes = STANDARD.decode(account["data"][0].as_str().context("pool data")?)?;
                pools.insert(address.clone(), pool_info(&bytes)?);
            }
        }
        atomic_json(&archive.join("pools.json"), pools)?;
        let batch = decoder
            .block(slot, &raw, pools)
            .with_context(|| format!("normalize slot {slot}"))?;
        atomic_json(&normalized, &batch)?;
        Ok(batch)
    }
    async fn capture_inventory(&self, slot: u64, archive: &Path) -> Result<Value> {
        let path = archive.join("inventories").join(format!("{slot}.json.gz"));
        let mut block: Value = if path.exists() {
            serde_json::from_reader(flate2::read::GzDecoder::new(fs::File::open(&path)?))?
        } else {
            let value=self.call("getBlock",json!([slot,{"encoding":"json","transactionDetails":"accounts","rewards":false,"commitment":"finalized","maxSupportedTransactionVersion":self.max_transaction_version}])).await?;
            ensure!(!value.is_null(), "inventory block unavailable");
            atomic_gzip(&path, &value)?;
            value
        };
        let mut full_count = 0usize;
        for entry in block["transactions"]
            .as_array_mut()
            .context("inventory transactions")?
        {
            let (stub, needs_full) = inventory_stub(entry)?;
            if needs_full {
                let signature = entry
                    .pointer("/transaction/signatures/0")
                    .and_then(Value::as_str)
                    .context("inventory signature")?;
                ensure!(
                    bs58::decode(signature).into_vec()?.len() == 64,
                    "invalid transaction signature"
                );
                let path = archive
                    .join("transactions")
                    .join(format!("{signature}.json.gz"));
                let full: Value = if path.exists() {
                    serde_json::from_reader(flate2::read::GzDecoder::new(fs::File::open(&path)?))?
                } else {
                    let value=self.call("getTransaction",json!([signature,{"encoding":"json","commitment":"finalized","maxSupportedTransactionVersion":self.max_transaction_version}])).await?;
                    ensure!(!value.is_null(), "inventory transaction unavailable");
                    atomic_gzip(&path, &value)?;
                    value
                };
                ensure!(
                    full["slot"] == slot
                        && full
                            .pointer("/transaction/signatures/0")
                            .and_then(Value::as_str)
                            == Some(signature),
                    "inventory transaction identity mismatch"
                );
                ensure!(
                    full["meta"]["err"] == entry["meta"]["err"],
                    "inventory transaction status mismatch"
                );
                let mut keys = full
                    .pointer("/transaction/message/accountKeys")
                    .and_then(Value::as_array)
                    .context("full transaction keys")?
                    .clone();
                for group in ["writable", "readonly"] {
                    if let Some(a) = full["meta"]["loadedAddresses"][group].as_array() {
                        keys.extend(a.iter().cloned());
                    }
                }
                ensure!(
                    json!(keys) == stub["transaction"]["message"]["accountKeys"],
                    "inventory/full account-key mismatch"
                );
                *entry = full;
                full_count += 1;
            } else {
                *entry = stub;
            }
        }
        block["capture_mode"] =
            json!("ordered_account_inventory_plus_full_successful_target_transactions");
        block["full_target_transactions"] = json!(full_count);
        Ok(block)
    }
}
pub fn atomic_gzip<T: serde::Serialize>(path: &Path, value: &T) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let temp = path.with_extension("gz.tmp");
    let mut file = fs::File::create(&temp)?;
    file.write_all(&crate::stream::gzip(value)?)?;
    file.sync_all()?;
    fs::rename(temp, path)?;
    Ok(())
}
pub fn atomic_json<T: serde::Serialize>(path: &Path, value: &T) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let temp = path.with_extension("json.tmp");
    let mut file = fs::File::create(&temp)?;
    file.write_all(&serde_json::to_vec_pretty(value)?)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    fs::rename(temp, path)?;
    Ok(())
}
pub fn load_pools(archive: &Path) -> Result<Pools> {
    let path = archive.join("pools.json");
    if path.exists() {
        Ok(serde_json::from_reader(fs::File::open(path)?)?)
    } else {
        Ok(Pools::new())
    }
}
pub fn load_batches(path: &Path) -> Result<Vec<Batch>> {
    ensure!(
        !path.is_dir() || !path.join("raw").exists() || path.join("coverage.json").exists(),
        "archive has no completed coverage manifest; resume capture before replay"
    );
    let batches: Vec<Batch> = if path.is_dir() {
        let dir = if path.join("blocks").is_dir() {
            path.join("blocks")
        } else {
            path.to_path_buf()
        };
        let mut paths: Vec<(u64, PathBuf)> = fs::read_dir(dir)?
            .filter_map(|e| e.ok())
            .filter_map(|e| {
                let p = e.path();
                if p.extension()?.to_str()? != "json" {
                    return None;
                }
                Some((p.file_stem()?.to_str()?.parse().ok()?, p))
            })
            .collect();
        paths.sort_by_key(|(slot, _)| *slot);
        paths
            .into_iter()
            .map(|(_, p)| Ok(serde_json::from_reader(fs::File::open(p)?)?))
            .collect::<Result<_>>()?
    } else {
        BufReader::new(fs::File::open(path)?)
            .lines()
            .enumerate()
            .filter_map(|(i, l)| match l {
                Ok(l) if l.trim().is_empty() => None,
                other => Some((i, other)),
            })
            .map(|(i, l)| {
                serde_json::from_str(&l?).with_context(|| format!("batch line {}", i + 1))
            })
            .collect::<Result<_>>()?
    };
    ensure!(!batches.is_empty(), "no normalized batches found");
    for b in &batches {
        b.validate()?;
    }
    ensure!(
        batches.windows(2).all(|b| b[0].slot < b[1].slot),
        "duplicate or unordered archive slots"
    );
    // Coverage manifests are written only after every produced block in a range
    // has been persisted. Reject holes inside the covered interval.
    if path.is_dir() && path.join("coverage.json").exists() {
        let coverage: Coverage =
            serde_json::from_reader(fs::File::open(path.join("coverage.json"))?)?;
        let actual: std::collections::BTreeSet<_> = batches.iter().map(|b| b.slot).collect();
        ensure!(
            coverage.produced_slots.iter().all(|s| actual.contains(s)),
            "archive missing a captured block"
        );
        let covered: std::collections::BTreeSet<_> = coverage.produced_slots.into_iter().collect();
        ensure!(actual == covered, "archive has uncommitted capture files; rerun the interrupted collect range before replay");
    }
    Ok(batches)
}
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct Coverage {
    pub start_slot: u64,
    pub end_slot: u64,
    pub produced_slots: Vec<u64>,
    pub commitment: String,
}
