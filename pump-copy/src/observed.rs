//! Conservative, two-clock diagnostics for incomplete observed logs.
//! This is deliberately separate from the coverage-checked block backtester.
use crate::{
    config::Strategy,
    model::{Reserve, Venue},
    rpc::atomic_json,
};
use anyhow::{ensure, Context, Result};
use flate2::read::GzDecoder;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    cmp::Reverse,
    collections::{BTreeMap, BinaryHeap},
    fs::File,
    io::{BufRead, BufReader},
    path::Path,
};

pub mod entry_rules;
pub mod signal_study;

#[derive(Clone, Deserialize)]
pub struct Quote {
    pub mint: String,
    pub received_ms: u64,
    pub chain_ms: u64,
    pub slot: u64,
    pub signature: String,
    pub event_index: u32,
    pub venue: Venue,
    pub reserve: Reserve,
}
#[derive(Clone, Deserialize)]
pub struct Alert {
    pub mint: String,
    pub observed_at_ms: u64,
    pub venue: Venue,
    pub whale_buy_slots: Vec<u64>,
}
#[derive(Clone, Deserialize)]
pub struct Coverage {
    pub chain_start_ms: u64,
    pub chain_end_ms: u64,
}
#[derive(Clone, Deserialize)]
pub struct Input {
    pub end_ms: u64,
    pub coverage: Vec<Coverage>,
    pub alerts: Vec<Alert>,
}
#[derive(Clone)]
struct Block {
    mint: String,
    slot: u64,
    chain_ms: u64,
    observed_ms: u64,
    quotes: Vec<Quote>,
}
pub struct Dataset {
    input: Input,
    blocks: Vec<Block>,
    by_mint: BTreeMap<String, Vec<usize>>,
    alerts: Vec<Alert>,
}
impl Dataset {
    pub fn new(input: Input, quotes: Vec<Quote>) -> Result<Self> {
        ensure!(input.end_ms > 0, "missing capture cutoff");
        let mut groups: BTreeMap<(String, u64), Vec<Quote>> = BTreeMap::new();
        for q in quotes {
            q.reserve.validate()?;
            ensure!(
                q.chain_ms <= q.received_ms,
                "event clock exceeds receipt clock"
            );
            ensure!(
                q.received_ms <= input.end_ms,
                "quote exceeds frozen capture"
            );
            groups.entry((q.mint.clone(), q.slot)).or_default().push(q);
        }
        let mut blocks = Vec::new();
        for ((mint, slot), quotes) in groups {
            let chain_ms = quotes[0].chain_ms;
            ensure!(
                quotes.iter().all(|q| q.chain_ms == chain_ms),
                "inconsistent event times within slot {slot}"
            );
            blocks.push(Block {
                mint,
                slot,
                chain_ms,
                observed_ms: quotes.iter().map(|q| q.received_ms).max().unwrap(),
                quotes,
            });
        }
        blocks.sort_by_key(|b| (b.chain_ms, b.slot, b.mint.clone()));
        let mut by_mint: BTreeMap<String, Vec<usize>> = BTreeMap::new();
        for (i, b) in blocks.iter().enumerate() {
            by_mint.entry(b.mint.clone()).or_default().push(i);
        }
        let mut alerts = input.alerts.clone();
        alerts.sort_by_key(|a| (a.observed_at_ms, a.mint.clone()));
        let mut seen = std::collections::BTreeSet::new();
        alerts.retain(|a| seen.insert(a.mint.clone()));
        ensure!(
            alerts.iter().all(|a| a.observed_at_ms <= input.end_ms),
            "signal exceeds cutoff"
        );
        Ok(Self {
            input,
            blocks,
            by_mint,
            alerts,
        })
    }
    fn execution(&self, mint: &str, decision: u64, due: u64, max_wait: u64) -> Option<usize> {
        // Events carry second-resolution chain time. Require the next whole
        // second strictly after submission, never a same-second favorable tick.
        let earliest = due.checked_div(1000)?.checked_add(1)?.checked_mul(1000)?;
        let ids = self.by_mint.get(mint)?;
        let i = ids.partition_point(|i| self.blocks[*i].chain_ms < earliest);
        let id = *ids.get(i)?;
        let b = &self.blocks[id];
        if b.chain_ms > self.input.end_ms || b.chain_ms.saturating_sub(due) > max_wait {
            return None;
        }
        // Do not bridge a recorder reconnect or startup gap to invent a fill.
        self.input
            .coverage
            .iter()
            .any(|c| decision >= c.chain_start_ms && b.chain_ms <= c.chain_end_ms)
            .then_some(id)
    }
}

#[derive(Clone, Serialize)]
pub struct Settings {
    pub target_bps: u32,
    pub extra_delay_ms: u64,
    pub adverse_slippage_bps: u32,
    pub order_lamports: u64,
    pub initial_cash_lamports: u64,
    pub network_fee_lamports: u64,
    pub entry_rent_lamports: u64,
    pub max_positions: usize,
    pub max_exposure_lamports: u64,
    pub max_reserve_participation_bps: u32,
    pub max_quote_wait_ms: u64,
    pub max_mark_age_ms: u64,
    pub retry_cooldown_ms: u64,
}
impl Settings {
    fn from_strategy(s: &Strategy, target: u32, delay: u64, haircut: u32) -> Self {
        Self {
            target_bps: target,
            extra_delay_ms: delay,
            adverse_slippage_bps: haircut,
            order_lamports: s.order_lamports,
            initial_cash_lamports: s.initial_cash_lamports,
            network_fee_lamports: s.network_fee_lamports,
            entry_rent_lamports: s.entry_rent_lamports,
            max_positions: s.max_positions,
            max_exposure_lamports: s.max_exposure_lamports,
            max_reserve_participation_bps: s.max_reserve_participation_bps,
            max_quote_wait_ms: 3000,
            max_mark_age_ms: s.max_mark_age_ms,
            retry_cooldown_ms: 5000,
        }
    }
    fn cost(&self) -> u64 {
        self.order_lamports + self.network_fee_lamports + self.entry_rent_lamports
    }
}
#[derive(Clone, Serialize)]
struct Position {
    mint: String,
    cohort: String,
    tokens: Option<u64>,
    cost_lamports: u64,
    entry_ms: u64,
    entry_slot: u64,
    pending: bool,
    next_attempt_ms: u64,
    unknown_entry: bool,
}
#[derive(Clone, Serialize)]
pub struct ExecutionRecord {
    pub mint: String,
    pub side: String,
    pub decision_ms: u64,
    pub due_ms: u64,
    pub execution_ms: u64,
    pub recognized_ms: u64,
    pub slot: Option<u64>,
    pub witness_signature: Option<String>,
    pub witness_event_index: Option<u32>,
    pub conservative_reserve: Option<Reserve>,
    pub outcome: String,
    pub tokens: Option<u64>,
    pub cash_lamports: u64,
    pub position_cost_lamports: u64,
    pub net_target_lamports: u64,
}
#[derive(Serialize)]
pub struct ResultRow {
    pub mode: &'static str,
    pub settings: Settings,
    pub signals: usize,
    pub entries: usize,
    pub exits: usize,
    pub unknown_entries: usize,
    pub failed_buys: usize,
    pub failed_sell_attempts: usize,
    pub unobserved_sell_attempts: usize,
    pub skipped_capacity: usize,
    pub free_cash_lamports: u64,
    pub closed_pnl_lamports: i128,
    pub open_cost_lamports: u64,
    pub zero_recovery_pnl_lamports: i128,
    pub estimated_marked_pnl_lamports: i128,
    pub peak_open_positions: usize,
    pub peak_capital_tied_lamports: u64,
    pub stale_or_unknown_open_marks: usize,
    pub open_positions: Vec<Value>,
    pub executions: Vec<ExecutionRecord>,
}
enum Action {
    Observe(usize),
    Signal(usize),
    Execute {
        mint: String,
        sell: bool,
        decision: u64,
        due: u64,
        block: Option<usize>,
    },
}

fn quote_buy(b: &Block, s: &Settings) -> Option<(u64, usize)> {
    let mut worst = None;
    for (i, q) in b.quotes.iter().enumerate() {
        if (q.venue == Venue::PumpFun && q.reserve.real_base == 0)
            || s.order_lamports as u128 * 10000
                > q.reserve.real_quote as u128 * s.max_reserve_participation_bps as u128
        {
            return None;
        }
        let tokens = q.reserve.buy(s.order_lamports, s.adverse_slippage_bps)?;
        if worst.is_none_or(|(v, _)| tokens < v) {
            worst = Some((tokens, i));
        }
    }
    worst
}
fn quote_sell(b: &Block, tokens: u64, s: &Settings) -> Option<(u64, usize)> {
    let mut worst = None;
    for (i, q) in b.quotes.iter().enumerate() {
        if (q.venue == Venue::PumpFun && q.reserve.real_base == 0)
            || tokens as u128 * 10000
                > q.reserve.real_base as u128 * s.max_reserve_participation_bps as u128
        {
            return None;
        }
        let net = q
            .reserve
            .sell(tokens, s.adverse_slippage_bps)?
            .saturating_sub(s.network_fee_lamports);
        if worst.is_none_or(|(v, _)| net < v) {
            worst = Some((net, i));
        }
    }
    worst
}
fn target(cost: u64, s: &Settings) -> u64 {
    ((cost as u128 * s.target_bps as u128).div_ceil(10000)) as u64
}

pub fn replay(data: &Dataset, s: Settings) -> Result<ResultRow> {
    ensure!(
        s.target_bps > 10000
            && s.adverse_slippage_bps < 10000
            && s.order_lamports > 0
            && s.max_positions > 0,
        "invalid replay settings"
    );
    let mut actions = Vec::new();
    let mut queue = BinaryHeap::new();
    for (i, b) in data.blocks.iter().enumerate() {
        let n = actions.len();
        actions.push(Action::Observe(i));
        queue.push(Reverse((b.observed_ms, 1u8, n)));
    }
    for (i, a) in data.alerts.iter().enumerate() {
        let n = actions.len();
        actions.push(Action::Signal(i));
        queue.push(Reverse((a.observed_at_ms, 2u8, n)));
    }
    let mut pos: BTreeMap<String, Position> = BTreeMap::new();
    let mut latest: BTreeMap<String, usize> = BTreeMap::new();
    let mut cash = s.initial_cash_lamports;
    let mut realized = 0i128;
    let mut entries = 0;
    let mut exits = 0;
    let mut unknown_entries = 0;
    let mut failed_buys = 0;
    let mut failed_sells = 0;
    let mut unknown_sells = 0;
    let mut capacity = 0;
    let mut peak_open = 0;
    let mut peak_cost = 0;
    let mut records = Vec::new();
    while let Some(Reverse((now, _, action_id))) = queue.pop() {
        if now > data.input.end_ms {
            break;
        }
        // Move an action out without cloning its strings or data.
        let action = std::mem::replace(&mut actions[action_id], Action::Observe(usize::MAX));
        match action {
            Action::Signal(i) => {
                let a = &data.alerts[i];
                let exposure: u64 = pos.values().map(|p| p.cost_lamports).sum();
                let reserved_sell_fees = pos
                    .values()
                    .filter(|p| p.pending && p.tokens.is_some())
                    .count() as u64
                    * s.network_fee_lamports;
                if pos.len() >= s.max_positions
                    || exposure + s.cost() > s.max_exposure_lamports
                    || cash.saturating_sub(reserved_sell_fees) < s.cost()
                {
                    capacity += 1;
                    continue;
                }
                cash -= s.cost();
                pos.insert(
                    a.mint.clone(),
                    Position {
                        mint: a.mint.clone(),
                        cohort: if a.venue == Venue::PumpSwap {
                            "post_graduation"
                        } else if a.whale_buy_slots.len() == 1 {
                            "pre_same_slot"
                        } else {
                            "pre_spread_slots"
                        }
                        .into(),
                        tokens: None,
                        cost_lamports: s.cost(),
                        entry_ms: now,
                        entry_slot: 0,
                        pending: true,
                        next_attempt_ms: 0,
                        unknown_entry: false,
                    },
                );
                let due = now + s.extra_delay_ms;
                let block = data.execution(&a.mint, now, due, s.max_quote_wait_ms);
                let at = block
                    .map(|i| data.blocks[i].observed_ms)
                    .unwrap_or(due + s.max_quote_wait_ms);
                let n = actions.len();
                actions.push(Action::Execute {
                    mint: a.mint.clone(),
                    sell: false,
                    decision: now,
                    due,
                    block,
                });
                queue.push(Reverse((at, 0, n)));
            }
            Action::Observe(i) => {
                let b = &data.blocks[i];
                if latest
                    .get(&b.mint)
                    .is_some_and(|old| data.blocks[*old].slot > b.slot)
                {
                    continue;
                }
                latest.insert(b.mint.clone(), i);
                let reserved_sell_fees = pos
                    .values()
                    .filter(|p| p.pending && p.tokens.is_some())
                    .count() as u64
                    * s.network_fee_lamports;
                let Some(p) = pos.get_mut(&b.mint) else {
                    continue;
                };
                let Some(tokens) = p.tokens else {
                    continue;
                };
                if p.pending
                    || now < p.next_attempt_ms
                    || b.slot <= p.entry_slot
                    || now.saturating_sub(b.chain_ms) > s.max_mark_age_ms
                    || cash < reserved_sell_fees + s.network_fee_lamports
                {
                    continue;
                }
                if quote_sell(b, tokens, &s)
                    .is_some_and(|(net, _)| net >= target(p.cost_lamports, &s))
                {
                    p.pending = true;
                    let due = now + s.extra_delay_ms;
                    let block = data.execution(&b.mint, now, due, s.max_quote_wait_ms);
                    let at = block
                        .map(|i| data.blocks[i].observed_ms)
                        .unwrap_or(due + s.max_quote_wait_ms);
                    let n = actions.len();
                    actions.push(Action::Execute {
                        mint: b.mint.clone(),
                        sell: true,
                        decision: now,
                        due,
                        block,
                    });
                    queue.push(Reverse((at, 0, n)));
                }
            }
            Action::Execute {
                mint,
                sell,
                decision,
                due,
                block,
            } => {
                let Some(mut p) = pos.remove(&mint) else {
                    continue;
                };
                p.pending = false;
                let b = block.map(|i| &data.blocks[i]);
                let execution_ms = b.map(|b| b.chain_ms).unwrap_or(now);
                p.next_attempt_ms = execution_ms + s.retry_cooldown_ms;
                let mut record = ExecutionRecord {
                    mint: mint.clone(),
                    side: if sell { "sell" } else { "buy" }.into(),
                    decision_ms: decision,
                    due_ms: due,
                    execution_ms,
                    recognized_ms: now,
                    slot: b.map(|b| b.slot),
                    witness_signature: None,
                    witness_event_index: None,
                    conservative_reserve: None,
                    outcome: String::new(),
                    tokens: p.tokens,
                    cash_lamports: 0,
                    position_cost_lamports: p.cost_lamports,
                    net_target_lamports: target(p.cost_lamports, &s),
                };
                if sell {
                    let quoted = b.and_then(|b| quote_sell(b, p.tokens.unwrap(), &s));
                    if let Some((net, qi)) = quoted {
                        let q = &b.unwrap().quotes[qi];
                        record.witness_signature = Some(q.signature.clone());
                        record.witness_event_index = Some(q.event_index);
                        record.conservative_reserve = Some(q.reserve.clone());
                        if net >= target(p.cost_lamports, &s) {
                            cash += net;
                            realized += net as i128 - p.cost_lamports as i128;
                            exits += 1;
                            record.outcome = "modeled_fill".into();
                            record.cash_lamports = net;
                            records.push(record);
                            continue;
                        }
                    }
                    cash = cash
                        .checked_sub(s.network_fee_lamports)
                        .context("insufficient retry cash")?;
                    p.cost_lamports += s.network_fee_lamports;
                    record.cash_lamports = s.network_fee_lamports;
                    if b.is_none() {
                        unknown_sells += 1;
                        record.outcome = "unobserved_fill_assumed_failed".into();
                    } else {
                        failed_sells += 1;
                        record.outcome = "minimum_out_or_liquidity_failure".into();
                    }
                    pos.insert(mint, p);
                } else if let Some(b) = b {
                    if let Some((tokens, qi)) = quote_buy(b, &s) {
                        let q = &b.quotes[qi];
                        record.witness_signature = Some(q.signature.clone());
                        record.witness_event_index = Some(q.event_index);
                        record.conservative_reserve = Some(q.reserve.clone());
                        record.tokens = Some(tokens);
                        record.cash_lamports = s.cost();
                        record.outcome = "modeled_fill".into();
                        p.tokens = Some(tokens);
                        p.entry_ms = execution_ms;
                        p.entry_slot = b.slot;
                        entries += 1;
                        pos.insert(mint, p);
                    } else {
                        cash += p.cost_lamports - s.network_fee_lamports;
                        realized -= s.network_fee_lamports as i128;
                        failed_buys += 1;
                        record.outcome = "liquidity_failure".into();
                        record.cash_lamports = s.network_fee_lamports;
                    }
                } else {
                    unknown_entries += 1;
                    p.unknown_entry = true;
                    record.outcome = "unknown_entry_full_cost_locked".into();
                    record.cash_lamports = p.cost_lamports;
                    pos.insert(mint, p);
                }
                records.push(record);
            }
        }
        peak_open = peak_open.max(pos.len());
        peak_cost = peak_cost.max(pos.values().map(|p| p.cost_lamports).sum());
    }
    let mut marked = 0u64;
    let mut missing = 0;
    let mut open = Vec::new();
    for p in pos.values_mut() {
        if p.tokens.is_none() && !p.unknown_entry {
            unknown_entries += 1;
            p.unknown_entry = true;
        }
        let quote = p.tokens.and_then(|tokens| {
            latest.get(&p.mint).and_then(|i| {
                let b = &data.blocks[*i];
                (b.slot >= p.entry_slot
                    && data.input.end_ms.saturating_sub(b.chain_ms) <= s.max_mark_age_ms)
                    .then(|| quote_sell(b, tokens, &s))
                    .flatten()
            })
        });
        if let Some((net, _)) = quote {
            marked += net;
        } else {
            missing += 1;
        }
        open.push(json!({"position":p,"fresh_net_liquidation_lamports":quote.map(|(net,_)|net),"unknown_or_stale_mark":quote.is_none()}));
    }
    let open_cost: u64 = pos.values().map(|p| p.cost_lamports).sum();
    ensure!(
        cash as i128 == s.initial_cash_lamports as i128 + realized - open_cost as i128,
        "cash ledger does not reconcile"
    );
    Ok(ResultRow {
        mode: "incomplete_log_execution_diagnostic",
        signals: data.alerts.len(),
        entries,
        exits,
        unknown_entries,
        failed_buys,
        failed_sell_attempts: failed_sells,
        unobserved_sell_attempts: unknown_sells,
        skipped_capacity: capacity,
        free_cash_lamports: cash,
        closed_pnl_lamports: realized,
        open_cost_lamports: open_cost,
        zero_recovery_pnl_lamports: cash as i128 - s.initial_cash_lamports as i128,
        estimated_marked_pnl_lamports: cash as i128 + marked as i128
            - s.initial_cash_lamports as i128,
        peak_open_positions: peak_open,
        peak_capital_tied_lamports: peak_cost,
        stale_or_unknown_open_marks: missing,
        open_positions: open,
        executions: records,
        settings: s,
    })
}

pub fn sweep(input: &Path, quotes: &Path, output: &Path, strategy: &Strategy) -> Result<()> {
    let input: Input = serde_json::from_reader(File::open(input)?)?;
    let rows = BufReader::new(GzDecoder::new(File::open(quotes)?))
        .lines()
        .map(|line| Ok(serde_json::from_str::<Quote>(&line?)?))
        .collect::<Result<Vec<_>>>()?;
    let data = Dataset::new(input, rows)?;
    let mut reports = Vec::new();
    for haircut in [100, 200] {
        for delay in [1000, 3000, 10000] {
            for target in [15000, 20000, 30000] {
                reports.push(replay(
                    &data,
                    Settings::from_strategy(strategy, target, delay, haircut),
                )?);
            }
        }
    }
    let table:Vec<_>=reports.iter().map(|r|json!({"target_multiple":r.settings.target_bps as f64/10000.,"extra_delay_seconds":r.settings.extra_delay_ms/1000,"slippage_bps_per_leg":r.settings.adverse_slippage_bps,"entries":r.entries,"exits":r.exits,"unknown_entries":r.unknown_entries,"open_positions":r.open_positions.len(),"skipped_capacity":r.skipped_capacity,"failed_sell_attempts":r.failed_sell_attempts,"unobserved_sell_attempts":r.unobserved_sell_attempts,"cash_sol":r.free_cash_lamports as f64/1e9,"closed_pnl_sol":r.closed_pnl_lamports as f64/1e9,"capital_tied_sol":r.open_cost_lamports as f64/1e9,"zero_recovery_pnl_sol":r.zero_recovery_pnl_lamports as f64/1e9,"marked_pnl_sol":r.estimated_marked_pnl_lamports as f64/1e9,"stale_or_unknown_marks":r.stale_or_unknown_open_marks})).collect();
    atomic_json(
        &output.join("comparison.json"),
        &json!({"mode":"incomplete_log_execution_diagnostic","first_alert_mints":data.alerts.len(),"quote_blocks":data.blocks.len(),"results":table,"funded_ready":false,"assumptions":["Decisions use recorded receipt time; executions use on-chain event time plus a separate 1/3/10-second submission delay. The approximately ten-second finalized-feed delay is not removed.","Fill results become usable only when the witness block is received; sale proceeds and position capacity cannot be recycled at the earlier chain timestamp. This receipt is a proxy for own-order confirmation, not a measured execution acknowledgement.","Execution uses the first observed mint block in the next whole second after submission, within three seconds. This is a model, not proof of transaction inclusion. Gaps cannot bridge a fill.","Unknown entry fills lock the full intended cost as unvalued exposure. Unknown sell fills are assumed failed and charged the modeled network fee. These are explicit pessimistic branches, not observed outcomes.","Without transaction order, the least favorable quote among all observed swaps in the execution block is used. Intra-block and intervening LP state remain uncertain.","DEX fees from observed reserve snapshots, 1%/2% adverse slippage per leg, configured network fees and entry rent are included. Network/tip assumptions are not measured execution costs.","Each sell must still meet the net target after the delay; failed attempts add their modeled fee to position cost. Retries require a new observation after a five-second cooldown.","Only the first alert per mint is admitted. Ten open positions and a two-SOL exposure cap are enforced; positions have no stop loss or holding timeout.","Mark-to-market uses only fresh observed quotes (maximum 30-second chain age); absent marks are separately counted and valued at zero. No actual liquidation occurs.","All scenarios use a sample already inspected during research. They are not independent tests or out-of-sample validation."]}),
    )?;
    atomic_json(&output.join("details.json"), &reports)?;
    println!("{}", serde_json::to_string_pretty(&table)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    pub(super) fn settings() -> Settings {
        Settings {
            target_bps: 15000,
            extra_delay_ms: 1000,
            adverse_slippage_bps: 0,
            order_lamports: 100_000_000,
            initial_cash_lamports: 10_000_000_000,
            network_fee_lamports: 100_000,
            entry_rent_lamports: 2_100_000,
            max_positions: 10,
            max_exposure_lamports: 2_000_000_000,
            max_reserve_participation_bps: 100,
            max_quote_wait_ms: 3000,
            max_mark_age_ms: 30000,
            retry_cooldown_ms: 5000,
        }
    }
    pub(super) fn quote(mint: &str, chain: u64, received: u64, price: u64) -> Quote {
        Quote {
            mint: mint.into(),
            chain_ms: chain,
            received_ms: received,
            slot: chain / 1000,
            signature: format!("{mint}-{chain}-{price}"),
            event_index: 0,
            venue: Venue::PumpSwap,
            reserve: Reserve {
                base: 1_000_000_000_000,
                quote: price * 100_000_000_000,
                real_base: 1_000_000_000_000,
                real_quote: price * 100_000_000_000,
                fee_bps: 0,
            },
        }
    }
    pub(super) fn data(quotes: Vec<Quote>, end: u64) -> Dataset {
        Dataset::new(
            Input {
                end_ms: end,
                coverage: vec![Coverage {
                    chain_start_ms: 0,
                    chain_end_ms: end,
                }],
                alerts: vec![Alert {
                    mint: "m".into(),
                    observed_at_ms: 10000,
                    venue: Venue::PumpFun,
                    whale_buy_slots: vec![1],
                }],
            },
            quotes,
        )
        .unwrap()
    }
    #[test]
    fn stale_arriving_quote_cannot_fill_entry() {
        let d = data(vec![quote("m", 2000, 12000, 1)], 20000);
        let r = replay(&d, settings()).unwrap();
        assert_eq!(r.entries, 0);
        assert_eq!(r.unknown_entries, 1);
        assert_eq!(r.open_cost_lamports, 102200000);
    }
    #[test]
    fn old_peak_cannot_fill_delayed_exit() {
        let d = data(
            vec![
                quote("m", 12000, 22000, 1),
                quote("m", 14000, 24000, 2),
                quote("m", 26000, 36000, 1),
            ],
            40000,
        );
        let r = replay(&d, settings()).unwrap();
        assert_eq!(r.entries, 1);
        assert_eq!(r.exits, 0);
        assert_eq!(r.failed_sell_attempts, 1);
        assert_eq!(r.open_cost_lamports, 102300000);
    }
    #[test]
    fn later_target_retry_can_close_after_failure() {
        let d = data(
            vec![
                quote("m", 12000, 22000, 1),
                quote("m", 14000, 24000, 2),
                quote("m", 26000, 36000, 1),
                quote("m", 30000, 40000, 2),
                quote("m", 42000, 52000, 2),
            ],
            55000,
        );
        let r = replay(&d, settings()).unwrap();
        assert_eq!(r.failed_sell_attempts, 1);
        assert_eq!(r.exits, 1);
        assert!(r.closed_pnl_lamports > 0);
        assert!(r.open_positions.is_empty());
    }
    #[test]
    fn least_favorable_same_block_quote_is_used() {
        let d = data(
            vec![quote("m", 12000, 22000, 1), quote("m", 12000, 22001, 2)],
            23000,
        );
        let r = replay(&d, settings()).unwrap();
        let buy = &r.executions[0];
        assert_eq!(
            buy.conservative_reserve.as_ref().unwrap().quote,
            200_000_000_000
        );
    }
    #[test]
    fn capture_gap_locks_unknown_cost() {
        let mut d = data(vec![quote("m", 12000, 22000, 1)], 23000);
        d.input.coverage = vec![
            Coverage {
                chain_start_ms: 0,
                chain_end_ms: 11000,
            },
            Coverage {
                chain_start_ms: 12000,
                chain_end_ms: 23000,
            },
        ];
        let r = replay(&d, settings()).unwrap();
        assert_eq!(r.entries, 0);
        assert_eq!(r.unknown_entries, 1);
    }
    #[test]
    fn missing_entries_consume_capacity() {
        let mut d = data(vec![], 30000);
        d.alerts.push(Alert {
            mint: "n".into(),
            observed_at_ms: 15000,
            venue: Venue::PumpSwap,
            whale_buy_slots: vec![1],
        });
        let mut s = settings();
        s.max_positions = 1;
        let r = replay(&d, s).unwrap();
        assert_eq!(r.unknown_entries, 1);
        assert_eq!(r.skipped_capacity, 1);
    }
    #[test]
    fn end_of_sample_does_not_close_or_mark_stale_inventory() {
        let d = data(vec![quote("m", 12000, 22000, 1)], 100000);
        let r = replay(&d, settings()).unwrap();
        assert_eq!(r.exits, 0);
        assert_eq!(r.open_positions.len(), 1);
        assert_eq!(r.stale_or_unknown_open_marks, 1);
        assert_eq!(
            r.estimated_marked_pnl_lamports,
            r.zero_recovery_pnl_lamports
        );
    }
    #[test]
    fn simultaneous_retries_reserve_their_network_fee() {
        let mut quotes = Vec::new();
        for mint in ["m", "n"] {
            quotes.extend([
                quote(mint, 12000, 22000, 1),
                quote(mint, 14000, 24000, 2),
                quote(mint, 26000, 36000, 1),
            ]);
        }
        let mut d = data(quotes, 40000);
        d.alerts.push(Alert {
            mint: "n".into(),
            observed_at_ms: 10000,
            venue: Venue::PumpSwap,
            whale_buy_slots: vec![1],
        });
        let mut s = settings();
        s.initial_cash_lamports = 2 * s.cost() + 150000;
        let r = replay(&d, s).unwrap();
        assert_eq!(r.failed_sell_attempts, 1);
        assert_eq!(r.free_cash_lamports, 50000);
    }
    #[test]
    fn new_entry_cannot_spend_a_pending_sell_fee() {
        let mut d = data(
            vec![
                quote("m", 12000, 22000, 1),
                quote("m", 14000, 24000, 2),
                quote("m", 26000, 36000, 1),
            ],
            40000,
        );
        d.alerts.push(Alert {
            mint: "n".into(),
            observed_at_ms: 25000,
            venue: Venue::PumpSwap,
            whale_buy_slots: vec![1],
        });
        let mut s = settings();
        s.initial_cash_lamports = 2 * s.cost() + 50000;
        let r = replay(&d, s).unwrap();
        assert_eq!(r.skipped_capacity, 1);
        assert_eq!(r.unknown_entries, 0);
        assert_eq!(r.failed_sell_attempts, 1);
    }
    #[test]
    fn sale_cash_is_unavailable_until_execution_is_observed() {
        let mut d = data(
            vec![
                quote("m", 12000, 22000, 1),
                quote("m", 14000, 24000, 2),
                quote("m", 26000, 36000, 2),
                quote("n", 32000, 42000, 1),
            ],
            45000,
        );
        d.alerts.push(Alert {
            mint: "n".into(),
            observed_at_ms: 30000,
            venue: Venue::PumpSwap,
            whale_buy_slots: vec![1],
        });
        let mut s = settings();
        s.initial_cash_lamports = s.cost() + s.network_fee_lamports;
        let r = replay(&d, s).unwrap();
        assert_eq!(r.entries, 1);
        assert_eq!(r.exits, 1);
        assert_eq!(r.skipped_capacity, 1);
        let sell = r.executions.iter().find(|e| e.side == "sell").unwrap();
        assert_eq!(sell.execution_ms, 26000);
        assert_eq!(sell.recognized_ms, 36000);
    }
}
