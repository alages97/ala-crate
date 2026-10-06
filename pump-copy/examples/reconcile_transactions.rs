//! Offline reconciliation of a directory of saved getTransaction results.
use anyhow::Result;
use serde_json::json;
use std::{env, fs, path::PathBuf};
fn main() -> Result<()> {
    let a: Vec<_> = env::args().collect();
    let mut paths = fs::read_dir(&a[1])?
        .map(|e| e.map(|e| e.path()))
        .collect::<std::io::Result<Vec<_>>>()?;
    paths.sort();
    let mut rows = vec![];
    for path in paths
        .into_iter()
        .filter(|p| p.extension().is_some_and(|e| e == "json"))
    {
        let tx: serde_json::Value = serde_json::from_reader(fs::File::open(&path)?)?;
        rows.push(match pump_copy::reconcile::transaction(&tx) {Ok(v)=>v,Err(e)=>json!({"signature":tx["transaction"]["signatures"][0],"slot":tx["slot"],"error":e.to_string(),"verified_swaps":[],"profit_rank_eligible":false})});
    }
    pump_copy::rpc::atomic_json(&PathBuf::from(&a[2]), &json!({"transactions":rows}))?;
    Ok(())
}
