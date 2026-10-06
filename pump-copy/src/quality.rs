//! Compare log observations with independently acquired complete finalized blocks.
use crate::{
    decoder::{Decoder, Event},
    model::Trade,
    rpc::{load_batches, load_pools},
    stream::open_store,
};
use anyhow::Result;
use serde_json::{json, Value};
use std::{collections::BTreeMap, path::Path};
fn key(t: &Trade) -> String {
    format!(
        "{}|{}|{}|{:?}|{}|{}",
        t.signature, t.wallet, t.mint, t.side, t.tokens, t.quote_lamports
    )
}
pub fn compare_stream(db: &Path, archive: &Path) -> Result<Value> {
    let conn = open_store(db)?;
    let decoder = Decoder::default();
    let pools = load_pools(archive)?;
    let batches = load_batches(archive)?;
    let mut rows = vec![];
    let (mut expected_total, mut observed_total, mut missing_total, mut extra_total) =
        (0u64, 0u64, 0u64, 0u64);
    for b in batches {
        let mut expected: BTreeMap<String, u64> = BTreeMap::new();
        for t in &b.trades {
            *expected.entry(key(t)).or_default() += 1;
        }
        let mut observed: BTreeMap<String, u64> = BTreeMap::new();
        let mut errors = 0u64;
        let mut q=conn.prepare("SELECT e.signature,e.event_index,e.program,e.event_name,e.fields_json FROM stream_events e JOIN stream_messages m USING(signature) WHERE m.slot=?1")?;
        let mut values = q.query([b.slot])?;
        while let Some(r) = values.next()? {
            let s: String = r.get(0)?;
            let e = Event {
                program: r.get(2)?,
                name: r.get(3)?,
                fields: serde_json::from_str(&r.get::<_, String>(4)?)?,
            };
            match decoder.trade_from_event(e, &s, 0, r.get(1)?, 0, &pools) {
                Ok((Some(t), _)) => *observed.entry(key(&t)).or_default() += 1,
                Ok(_) => {}
                Err(_) => errors += 1,
            }
        }
        let missing: Vec<_> = expected
            .iter()
            .filter_map(|(k, n)| {
                let missing = n.saturating_sub(*observed.get(k).unwrap_or(&0));
                (missing > 0).then(|| json!({"identity":k,"count":missing}))
            })
            .collect();
        let extra: Vec<_> = observed
            .iter()
            .filter_map(|(k, n)| {
                let extra = n.saturating_sub(*expected.get(k).unwrap_or(&0));
                (extra > 0).then(|| json!({"identity":k,"count":extra}))
            })
            .collect();
        let missing_count = missing
            .iter()
            .map(|r| r["count"].as_u64().unwrap())
            .sum::<u64>();
        let extra_count = extra
            .iter()
            .map(|r| r["count"].as_u64().unwrap())
            .sum::<u64>();
        let expected_count = expected.values().sum::<u64>();
        let observed_count = observed.values().sum::<u64>();
        expected_total += expected_count;
        observed_total += observed_count;
        missing_total += missing_count;
        extra_total += extra_count;
        rows.push(json!({"slot":b.slot,"expected_supported_events":expected_count,"observed_supported_events":observed_count,"missing":missing,"extra":extra,"normalization_errors":errors}));
    }
    Ok(
        json!({"expected_supported_events":expected_total,"observed_supported_events":observed_total,"missing_supported_events":missing_total,"extra_supported_events":extra_total,"blocks":rows,"scope":"Only the sampled finalized blocks and supported SOL-quoted venues. A match does not certify uninterrupted stream coverage."}),
    )
}
