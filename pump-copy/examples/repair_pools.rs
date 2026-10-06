//! Bounded pool-identity enrichment. Current balances never become historical quotes.
use anyhow::{Context, Result};
use base64::{engine::general_purpose::STANDARD, Engine};
use pump_copy::{
    decoder::{pool_info, Pools},
    model::AMM,
    rpc::{atomic_json, Rpc},
};
use serde_json::{json, Value};
use std::{
    env, fs,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};
#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<_> = env::args().collect();
    let mut pools: Pools = serde_json::from_reader(fs::File::open(&args[1])?)?;
    let requested: Vec<Value> = serde_json::from_reader(fs::File::open(&args[2])?)?;
    let out = PathBuf::from(&args[3]);
    fs::create_dir_all(&out)?;
    let rpc = Rpc::from_env("SOLANA_RPC_URL", 1)?;
    let mut results = Vec::new();
    let mut error = None;
    for (i, group) in requested.chunks(100).take(20).enumerate() {
        let addresses: Vec<_> = group
            .iter()
            .map(|v| v["address"].as_str().unwrap())
            .collect();
        let response = match rpc
            .call(
                "getMultipleAccounts",
                json!([addresses,{"encoding":"base64","commitment":"finalized"}]),
            )
            .await
        {
            Ok(r) => r,
            Err(e) => {
                error = Some(e.to_string());
                break;
            }
        };
        atomic_json(&out.join(format!("accounts-{i:03}.json")), &response)?;
        let values = response["value"]
            .as_array()
            .context("pool account values")?;
        anyhow::ensure!(values.len() == addresses.len(), "account count mismatch");
        for ((address, a), wanted) in addresses.iter().zip(values).zip(group) {
            let decoded = (|| -> Result<_> {
                anyhow::ensure!(a["owner"] == AMM, "missing account or wrong owner");
                let bytes = STANDARD.decode(a["data"][0].as_str().context("account data")?)?;
                pool_info(&bytes)
            })();
            match decoded {Ok(info)=>{pools.insert((*address).into(),info);results.push(json!({"address":address,"events":wanted["events"],"status":"identity_recovered","context_slot":response["context"]["slot"]}));},Err(e)=>results.push(json!({"address":address,"events":wanted["events"],"status":"unresolved","reason":e.to_string()}))}
        }
        atomic_json(&out.join("pools.json"), &pools)?;
    }
    atomic_json(&out.join("pools.json"), &pools)?;
    atomic_json(
        &out.join("report.json"),
        &json!({"observed_at_ms":SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis(),"requested_pools":requested.len(),"pool_count":pools.len(),"results":results,"stopped_error":error,"max_account_requests":20,"historical_identity_caveat":"Current account metadata is a reconstruction aid, not proof of historical availability. Raw responses and context slots are retained. No current reserves or balances are used in historical replay."}),
    )?;
    Ok(())
}
