//! Unordered log observations discover activity; they never certify profitability.
use crate::{
    config::Flow,
    decoder::{pool_info, Decoder, Event, Pools},
    model::{Side, AMM},
    rpc::{atomic_json, load_pools, Rpc},
    stream::open_store,
};
use anyhow::{ensure, Context, Result};
use base64::Engine as _;
use rusqlite::Connection;
use serde::Serialize;
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    path::Path,
};

#[derive(Default, Serialize)]
struct Activity {
    wallet: String,
    buys: u64,
    sells: u64,
    buy_lamports: u128,
    sell_lamports: u128,
    distinct_mints: usize,
    observed_net_cashflow_lamports: i128,
    mints_with_more_tokens_sold_than_bought: usize,
    #[serde(skip)]
    tokens: BTreeMap<String, i128>,
    #[serde(skip)]
    mints: BTreeSet<String>,
}
struct Point {
    time: u64,
    wallet: String,
    buy: bool,
    quote: u64,
}
#[derive(Default)]
pub struct FlowTracker {
    points: BTreeMap<String, VecDeque<Point>>,
    last_alert: BTreeMap<String, u64>,
}
impl FlowTracker {
    pub fn current_windows(&self, now: u64, config: &Flow) -> Vec<Value> {
        let mut result = vec![];
        for (mint, points) in &self.points {
            let mut wallets: BTreeMap<&str, (u128, u128)> = BTreeMap::new();
            for p in points
                .iter()
                .filter(|p| now.saturating_sub(p.time) <= config.window_ms)
            {
                let a = wallets.entry(&p.wallet).or_default();
                if p.buy {
                    a.0 += p.quote as u128;
                } else {
                    a.1 += p.quote as u128;
                }
            }
            let buys = wallets.values().map(|v| v.0).sum::<u128>();
            let sells = wallets.values().map(|v| v.1).sum::<u128>();
            let whales = wallets
                .values()
                .filter(|v| v.0 >= config.whale_buy_lamports as u128 && v.0 > v.1)
                .count();
            if !wallets.is_empty() {
                result.push(json!({"mint":mint,"as_of_receipt_ms":now,"window_ms":config.window_ms,"buy_lamports":buys,"sell_lamports":sells,"net_inflow_lamports":buys as i128-sells as i128,"distinct_addresses":wallets.len(),"whale_count":whales}));
            }
        }
        result.sort_by(|a, b| {
            b["net_inflow_lamports"]
                .as_i64()
                .cmp(&a["net_inflow_lamports"].as_i64())
        });
        result.truncate(100);
        result
    }
    /// Time is local receipt time. A wallet is one address, not a proven entity.
    pub fn observe(&mut self, t: &crate::model::Trade, time: u64, config: &Flow) -> Option<Value> {
        let q = self.points.entry(t.mint.clone()).or_default();
        q.push_back(Point {
            time,
            wallet: t.wallet.clone(),
            buy: t.side == Side::Buy,
            quote: t.quote_lamports,
        });
        while q
            .front()
            .is_some_and(|p| time.saturating_sub(p.time) > config.window_ms)
        {
            q.pop_front();
        }
        let mut wallets: BTreeMap<&str, (u128, u128)> = BTreeMap::new();
        let (mut buy, mut sell) = (0u128, 0u128);
        for p in q.iter() {
            let w = wallets.entry(&p.wallet).or_default();
            if p.buy {
                w.0 += p.quote as u128;
                buy += p.quote as u128;
            } else {
                w.1 += p.quote as u128;
                sell += p.quote as u128;
            }
        }
        let whales: Vec<_> = wallets
            .iter()
            .filter(|(_, v)| v.0 >= config.whale_buy_lamports as u128 && v.0 > v.1)
            .map(|(w, _)| *w)
            .collect();
        let net = buy as i128 - sell as i128;
        let hit = t.side == Side::Buy
            && whales.len() >= config.min_whales
            && net >= config.min_net_inflow_lamports as i128
            && buy * 10000 >= sell * config.min_buy_sell_ratio_bps as u128
            && self
                .last_alert
                .get(&t.mint)
                .is_none_or(|at| time.saturating_sub(*at) >= config.cooldown_ms);
        let alert=hit.then(||json!({"observed_at_ms":time,"signature":t.signature,"event_index":t.event_index,"mint":t.mint,"venue":t.venue,"window_ms":config.window_ms,"buy_lamports":buy,"sell_lamports":sell,"net_inflow_lamports":net,"whale_addresses":whales,"whale_count":whales.len(),"basis":"finalized log receipt window; address independence and complete coverage not established"}));
        if hit {
            self.last_alert.insert(t.mint.clone(), time);
        }
        self.points.retain(|_, q| {
            q.back()
                .is_some_and(|p| time.saturating_sub(p.time) <= config.window_ms)
        });
        self.last_alert
            .retain(|_, at| time.saturating_sub(*at) <= config.cooldown_ms);
        alert
    }
}

pub fn analyze(conn: &Connection, max_rowid: i64, pools: &Pools, flow: &Flow) -> Result<Value> {
    let decoder = Decoder::default();
    let mut activity: BTreeMap<String, Activity> = BTreeMap::new();
    let mut q=conn.prepare("SELECT e.signature,e.event_index,e.program,e.event_name,e.fields_json,m.received_ms,m.slot FROM stream_events e JOIN stream_messages m USING(signature) WHERE m.rowid<=?1 ORDER BY m.received_ms,m.rowid,e.event_index")?;
    let mut rows = q.query([max_rowid])?;
    let mut unresolved = 0u64;
    let mut non_sol = 0u64;
    let mut supported = 0u64;
    let mut rejected = 0u64;
    let mut rejection_examples = vec![];
    let mut first = None::<u64>;
    let mut last = 0u64;
    let mut max_event_ms = 0u64;
    let mut tracker = FlowTracker::default();
    let mut alerts = vec![];
    while let Some(r) = rows.next()? {
        let signature: String = r.get(0)?;
        let index: u32 = r.get(1)?;
        let fields: Value = serde_json::from_str(&r.get::<_, String>(4)?)?;
        let event = Event {
            program: r.get(2)?,
            name: r.get(3)?,
            fields,
        };
        let time: u64 = r.get(5)?;
        first = Some(first.map_or(time, |f| f.min(time)));
        last = last.max(time);
        max_event_ms = max_event_ms.max(
            event.fields["timestamp"]
                .as_u64()
                .unwrap_or(0)
                .saturating_mul(1000),
        );
        if event.program == AMM
            && event.name != "CreatePoolEvent"
            && !event.fields["pool"]
                .as_str()
                .is_some_and(|p| pools.contains_key(p))
        {
            unresolved += 1;
            continue;
        }
        let normalized = decoder.trade_from_event(event, &signature, 0, index, 0, pools);
        let (t, skip) = match normalized {
            Ok(v) => v,
            Err(error) => {
                rejected += 1;
                if rejection_examples.len() < 20 {
                    rejection_examples.push(json!({"signature":signature,"event_index":index,"reason":error.to_string()}));
                }
                continue;
            }
        };
        non_sol += u64::from(skip);
        let Some(t) = t else {
            continue;
        };
        if t.reserve.validate().is_err()
            || t.tokens == 0
            || (t.quote_lamports == 0 && t.side == Side::Buy)
        {
            rejected += 1;
            if rejection_examples.len() < 20 {
                rejection_examples.push(json!({"signature":signature,"event_index":index,"reason":"invalid reserve or zero buy/token amount"}));
            }
            continue;
        }
        supported += 1;
        let a = activity
            .entry(t.wallet.clone())
            .or_insert_with(|| Activity {
                wallet: t.wallet.clone(),
                ..Default::default()
            });
        a.mints.insert(t.mint.clone());
        let delta = a.tokens.entry(t.mint.clone()).or_default();
        if t.side == Side::Buy {
            a.buys += 1;
            a.buy_lamports += t.quote_lamports as u128;
            *delta += t.tokens as i128;
        } else {
            a.sells += 1;
            a.sell_lamports += t.quote_lamports as u128;
            *delta -= t.tokens as i128;
        }
        if let Some(alert) = tracker.observe(&t, time, flow) {
            alerts.push(alert);
        }
    }
    let mut wallets: Vec<_> = activity
        .into_values()
        .map(|mut a| {
            a.distinct_mints = a.mints.len();
            a.observed_net_cashflow_lamports = a.sell_lamports as i128 - a.buy_lamports as i128;
            a.mints_with_more_tokens_sold_than_bought =
                a.tokens.values().filter(|v| **v < 0).count();
            a
        })
        .collect();
    // Discovery priority only: diverse two-sided activity before raw size.
    wallets.sort_by(|a, b| {
        let priority = |w: &Activity| w.buys > 0 && w.sells > 0 && w.distinct_mints >= 3;
        priority(b)
            .cmp(&priority(a))
            .then(b.distinct_mints.cmp(&a.distinct_mints))
            .then((b.buy_lamports + b.sell_lamports).cmp(&(a.buy_lamports + a.sell_lamports)))
            .then(a.wallet.cmp(&b.wallet))
    });
    let current_windows = tracker.current_windows(last, flow);
    Ok(
        json!({"snapshot_message_rowid":max_rowid,"first_observed_ms":first,"last_observed_ms":last,"latest_event_timestamp_ms":max_event_ms,"latest_event_to_receipt_lag_ms_estimate":last.saturating_sub(max_event_ms),"supported_trades":supported,"unresolved_pool_events":unresolved,"non_sol_events":non_sol,"rejected_events":rejected,"rejection_examples":rejection_examples,"wallet_count":wallets.len(),"wallets":wallets,"flow_alerts":alerts,"current_flow_windows":current_windows,"qualified_copy_wallets":[],"note":"Discovery only. Cash flow is not profit: inventory, fees, transfers, other venues and capture gaps are not reconciled. Feed receipt order is not intra-block order. No wallet in this report is qualified for funded copying."}),
    )
}

pub async fn snapshot(
    rpc: &Rpc,
    db: &Path,
    output: &Path,
    flow: &Flow,
    max_pools: usize,
) -> Result<()> {
    ensure!(
        max_pools > 0 && max_pools <= 10000,
        "pool cap must be 1..10000"
    );
    let conn = open_store(db)?;
    let rowid: i64 = conn.query_row(
        "SELECT coalesce(max(rowid),0) FROM stream_messages",
        [],
        |r| r.get(0),
    )?;
    let mut pools = load_pools(output)?;
    let mut q=conn.prepare("SELECT DISTINCT json_extract(e.fields_json,'$.pool') FROM stream_events e JOIN stream_messages m USING(signature) WHERE m.rowid<=?1 AND e.program=?2 AND e.event_name IN ('BuyEvent','SellEvent') ORDER BY 1")?;
    let all = q
        .query_map(rusqlite::params![rowid, AMM], |r| r.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let missing: Vec<_> = all
        .into_iter()
        .filter(|p| !pools.contains_key(p))
        .take(max_pools)
        .collect();
    for addresses in missing.chunks(100) {
        let response = rpc
            .call(
                "getMultipleAccounts",
                json!([addresses,{"encoding":"base64","commitment":"finalized"}]),
            )
            .await?;
        let values = response["value"].as_array().context("pool values")?;
        ensure!(values.len() == addresses.len(), "pool response length");
        for (address, a) in addresses.iter().zip(values) {
            if a["owner"].as_str() != Some(AMM) {
                continue;
            }
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(a["data"][0].as_str().context("pool data")?)?;
            if let Ok(info) = pool_info(&bytes) {
                pools.insert(address.clone(), info);
            }
        }
        atomic_json(&output.join("pools.json"), &pools)?;
    }
    let mut report = analyze(&conn, rowid, &pools, flow)?;
    let generated = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis() as u64;
    let age = generated.saturating_sub(report["last_observed_ms"].as_u64().unwrap_or(0));
    report["generated_at_ms"] = json!(generated);
    report["observation_age_ms"] = json!(age);
    report["feed_stale"] = json!(age > flow.window_ms);
    if age > flow.window_ms {
        report["current_flow_windows"] = json!([]);
    }
    atomic_json(&output.join("activity-candidates.json"), &report["wallets"])?;
    atomic_json(&output.join("flow-alerts.json"), &report["flow_alerts"])?;
    atomic_json(
        &output.join("current-flow.json"),
        &report["current_flow_windows"],
    )?;
    atomic_json(&output.join("discovery.json"), &report)?;
    println!(
        "{}",
        json!({"wallets":report["wallet_count"],"supported_trades":report["supported_trades"],"flow_alerts":report["flow_alerts"].as_array().map(Vec::len),"unresolved_pool_events":report["unresolved_pool_events"],"qualified_copy_wallets":0})
    );
    Ok(())
}
