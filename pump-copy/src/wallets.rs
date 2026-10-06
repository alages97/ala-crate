use crate::{
    config::Selection,
    model::{Side, Trade},
};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Default, Debug)]
struct Inventory {
    tokens: u128,
    cost: u128,
    cycle_pnl: i128,
}

#[derive(Default, Debug)]
pub struct Wallet {
    inventory: BTreeMap<String, Inventory>,
    mints: BTreeSet<String>,
    profit_by_mint: BTreeMap<String, i128>,
    first_ms: Option<u64>,
    last_ms: u64,
    buys: u128,
    sales: i128,
    realized: i128,
    closed: u64,
    wins: u64,
    unknown_cost_sales: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct WalletReport {
    pub wallet: String,
    pub eligible: bool,
    pub rejection_reasons: Vec<String>,
    pub distinct_mints: usize,
    pub closed_positions: u64,
    pub winning_positions: u64,
    pub realized_pnl_lamports: i128,
    pub conservative_pnl_lamports: i128,
    pub invested_lamports: u128,
    pub open_cost_lamports: u128,
    pub conservative_roi_bps: i128,
    pub largest_profit_share_bps: u128,
    pub unknown_cost_sales: u64,
    pub first_seen_ms: u64,
    pub last_seen_ms: u64,
}

impl Wallet {
    pub fn observe(&mut self, t: &Trade, now: u64) {
        self.first_ms.get_or_insert(now);
        self.last_ms = now;
        self.mints.insert(t.mint.clone());
        let p = self.inventory.entry(t.mint.clone()).or_default();
        match t.side {
            Side::Buy => {
                let cost = t.quote_lamports as u128 + t.network_fee_lamports as u128;
                self.buys += cost;
                p.cost += cost;
                p.tokens += t.tokens as u128;
            }
            Side::Sell => {
                let qty = t.tokens as u128;
                let known = qty.min(p.tokens);
                if known < qty {
                    self.unknown_cost_sales += 1;
                }
                if known == 0 {
                    return;
                }
                // Unknown inventory does not receive a zero cost basis.
                let proceeds = (t.quote_lamports as i128 - t.network_fee_lamports as i128)
                    * known as i128
                    / qty as i128;
                let basis = if known == p.tokens {
                    p.cost
                } else {
                    p.cost * known / p.tokens
                };
                p.tokens -= known;
                p.cost -= basis;
                let pnl = proceeds - basis as i128;
                p.cycle_pnl += pnl;
                self.sales += proceeds;
                self.realized += pnl;
                *self.profit_by_mint.entry(t.mint.clone()).or_default() += pnl;
                if p.tokens == 0 {
                    self.closed += 1;
                    self.wins += u64::from(p.cycle_pnl > 0);
                    p.cycle_pnl = 0;
                }
            }
        }
    }
    pub fn report(&self, address: &str, s: &Selection, now: u64) -> WalletReport {
        let conservative = self.sales - self.buys as i128;
        let roi = if self.buys == 0 {
            0
        } else {
            conservative * 10_000 / self.buys as i128
        };
        let positive: Vec<_> = self
            .profit_by_mint
            .values()
            .copied()
            .filter(|p| *p > 0)
            .collect();
        let sum: i128 = positive.iter().sum();
        let share = if sum == 0 {
            10_000
        } else {
            positive.iter().max().copied().unwrap_or(0) * 10_000 / sum
        } as u128;
        let mut reasons = Vec::new();
        if self.closed < s.min_closed_positions {
            reasons.push("insufficient_closed_positions".into());
        }
        if self.mints.len() < s.min_distinct_mints {
            reasons.push("insufficient_mints".into());
        }
        if now.saturating_sub(self.first_ms.unwrap_or(now)) < s.min_history_ms {
            reasons.push("insufficient_history".into());
        }
        if now.saturating_sub(self.last_ms) > s.max_inactivity_ms {
            reasons.push("inactive".into());
        }
        if conservative < s.min_conservative_profit_lamports as i128 {
            reasons.push("insufficient_profit_after_open_losses".into());
        }
        if roi < s.min_roi_bps as i128 {
            reasons.push("insufficient_roi".into());
        }
        if share > s.max_profit_concentration_bps as u128 {
            reasons.push("profit_concentration".into());
        }
        if self.unknown_cost_sales > 0 {
            reasons.push("unknown_cost_inventory".into());
        }
        WalletReport {
            wallet: address.into(),
            eligible: reasons.is_empty(),
            rejection_reasons: reasons,
            distinct_mints: self.mints.len(),
            closed_positions: self.closed,
            winning_positions: self.wins,
            realized_pnl_lamports: self.realized,
            conservative_pnl_lamports: conservative,
            invested_lamports: self.buys,
            open_cost_lamports: self.inventory.values().map(|x| x.cost).sum(),
            conservative_roi_bps: roi,
            largest_profit_share_bps: share,
            unknown_cost_sales: self.unknown_cost_sales,
            first_seen_ms: self.first_ms.unwrap_or(0),
            last_seen_ms: self.last_ms,
        }
    }
}

#[derive(Default)]
pub struct Wallets {
    pub items: BTreeMap<String, Wallet>,
}
impl Wallets {
    pub fn eligible(&self, wallet: &str, s: &Selection, now: u64) -> bool {
        self.items
            .get(wallet)
            .is_some_and(|w| w.report(wallet, s, now).eligible)
    }
    pub fn observe(&mut self, t: &Trade, now: u64) {
        self.items
            .entry(t.wallet.clone())
            .or_default()
            .observe(t, now);
    }
    pub fn report(&self, s: &Selection, now: u64) -> Vec<WalletReport> {
        let mut rows: Vec<_> = self
            .items
            .iter()
            .map(|(a, w)| w.report(a, s, now))
            .collect();
        rows.sort_by(|a, b| {
            b.eligible
                .cmp(&a.eligible)
                .then(
                    b.conservative_pnl_lamports
                        .cmp(&a.conservative_pnl_lamports),
                )
                .then(a.wallet.cmp(&b.wallet))
        });
        rows
    }
}
