//! Nominate audit candidates using only the earlier frozen observation period.
use anyhow::{Context, Result};
use flate2::read::GzDecoder;
use pump_copy::{
    config::Config,
    model::{Reserve, Side, Trade},
    rpc::atomic_json,
    wallets::Wallets,
};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    fs::File,
    io::{BufRead, BufReader},
    path::PathBuf,
};
fn observe(v: Value, wallets: &mut Wallets, fee: u64) -> Result<()> {
    let a = v.as_array().context("row")?;
    let n = |i: usize| a[i].as_u64().context("integer");
    let s = |i: usize| a[i].as_str().context("string");
    let trade = Trade {
        signature: s(7)?.into(),
        tx_index: 0,
        event_index: n(8)? as u32,
        mint: s(0)?.into(),
        wallet: s(1)?.into(),
        pool: "observed-identity".into(),
        venue: serde_json::from_value(a[13].clone())?,
        side: if a[2].as_bool().context("side")? {
            Side::Buy
        } else {
            Side::Sell
        },
        tokens: n(4)?,
        quote_lamports: n(3)?,
        network_fee_lamports: fee,
        reserve: Reserve {
            quote: n(9)?,
            base: n(10)?,
            real_base: n(11)?,
            real_quote: n(12)?,
            fee_bps: n(14)? as u32,
        },
    };
    wallets.observe(&trade, n(5)?);
    Ok(())
}
fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
    let cutoff: u64 = args[2].parse()?;
    let output = PathBuf::from(&args[3]);
    let config: Config = toml::from_str(&std::fs::read_to_string("config/default.toml")?)?;
    let mut wallets = Wallets::default();
    let mut queue = BTreeMap::new();
    let mut max = 0;
    let mut count = 0;
    for line in BufReader::new(GzDecoder::new(File::open(&args[1])?)).lines() {
        let v: Value = serde_json::from_str(&line?)?;
        let at = v[5].as_u64().context("receipt")?;
        if at > cutoff + 73 {
            break;
        }
        max = max.max(at);
        if at <= cutoff {
            queue.insert(
                (
                    at,
                    v[17].as_u64().context("rowid")?,
                    v[8].as_u64().context("event")?,
                ),
                v,
            );
        }
        while queue
            .first_key_value()
            .is_some_and(|(k, _)| k.0 <= max.saturating_sub(73))
        {
            let (_, v) = queue.pop_first().unwrap();
            observe(v, &mut wallets, config.strategy.network_fee_lamports)?;
            count += 1;
        }
    }
    for (_, v) in queue {
        observe(v, &mut wallets, config.strategy.network_fee_lamports)?;
        count += 1;
    }
    let mut nomination = config.selection.clone();
    nomination.min_history_ms = 20 * 60 * 1000;
    let reports = wallets.report(&nomination, cutoff);
    let labeled = reports
        .iter()
        .map(|r| {
            let mut v = serde_json::to_value(r).unwrap();
            let eligible = v.as_object_mut().unwrap().remove("eligible").unwrap();
            v["nomination_eligible"] = eligible;
            v["qualified_for_copy"] = json!(false);
            v
        })
        .collect::<Vec<_>>();
    let candidates: Vec<_> = labeled
        .iter()
        .filter(|r| r["nomination_eligible"] == true)
        .take(10)
        .collect();
    atomic_json(
        &output.join("screen.json"),
        &json!({"cutoff_ms":cutoff,"input_trades":count,"wallets":reports.len(),"nomination_selection":nomination,"nominees":candidates,"nominee_count":reports.iter().filter(|r|r.eligible).count(),"assumptions":["Only observations before cutoff are used. Receipt order is buffered by the measured maximum 73ms inversion.","This is audit nomination, not qualification: history requirement is only 20 minutes here, source network fees are assumed at 0.0001 SOL per swap, and raw logs do not certify complete inventory or transfers.","The unchanged funded-research qualification requires at least one day of history and a separate balance audit."]}),
    )?;
    atomic_json(&output.join("wallet-ranking.json"), &labeled)?;
    println!("{}", serde_json::to_string_pretty(&candidates)?);
    Ok(())
}
