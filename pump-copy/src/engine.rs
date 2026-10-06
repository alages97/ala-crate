use crate::{
    config::{Config, EntryMode},
    model::{Batch, Reserve, Side, Venue},
    wallets::{WalletReport, Wallets},
};
use anyhow::{ensure, Result};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet, VecDeque};

#[derive(Clone)]
struct Market {
    reserve: Reserve,
    venue: Venue,
    pool: String,
    time: u64,
    slot: u64,
}
#[derive(Debug, Clone, Serialize)]
pub struct Position {
    pub mint: String,
    pub tokens: u64,
    pub cost_lamports: u64,
    pub entry_time_ms: u64,
    pub entry_slot: u64,
    pub entry_venue: Venue,
    pub source_wallet: Option<String>,
    pub reason: String,
}
#[derive(Clone)]
struct Pending {
    mint: String,
    source: Option<String>,
    reason: String,
    created: u64,
    slot: u64,
    sell: bool,
}
#[derive(Clone)]
struct FlowPoint {
    time: u64,
    wallet: String,
    buy: bool,
    quote: u64,
}
#[derive(Debug, Clone, Serialize)]
pub struct Signal {
    pub time_ms: u64,
    pub slot: u64,
    pub mint: String,
    pub venue: Venue,
    pub kind: String,
    pub source_wallet: Option<String>,
    pub whale_count: usize,
    pub buy_lamports: u128,
    pub sell_lamports: u128,
    pub net_inflow_lamports: i128,
}
#[derive(Debug, Clone, Serialize)]
pub struct Fill {
    pub time_ms: u64,
    pub slot: u64,
    pub mint: String,
    pub venue: Venue,
    pub pool: String,
    pub side: Side,
    pub tokens: u64,
    pub cash_lamports: u64,
    pub realized_pnl_lamports: Option<i128>,
    pub reason: String,
    pub source_wallet: Option<String>,
}
#[derive(Debug, Default, Clone, Serialize)]
pub struct CopyPerformance {
    /// None groups entries driven solely by the flow signal.
    pub source_wallet: Option<String>,
    pub entries: u64,
    pub exits: u64,
    pub realized_pnl_lamports: i128,
    pub zero_recovery_pnl_lamports: i128,
    pub open_cost_lamports: u128,
}
#[derive(Debug, Clone, Serialize)]
pub struct Equity {
    pub time_ms: u64,
    pub cash_lamports: u64,
    pub liquidation_equity_lamports: u128,
    pub open_positions: usize,
}
#[derive(Debug, Clone, Serialize)]
pub struct OpenPosition {
    #[serde(flatten)]
    pub position: Position,
    pub estimated_net_liquidation_lamports: u64,
    pub mark_available: bool,
}
#[derive(Serialize)]
pub struct Report {
    pub mode: &'static str,
    pub config: Config,
    pub batches: u64,
    pub trades: u64,
    pub end_time_ms: u64,
    pub initial_cash_lamports: u64,
    pub cash_lamports: u64,
    pub estimated_liquidation_equity_lamports: u128,
    pub zero_recovery_equity_lamports: u64,
    pub realized_pnl_lamports: i128,
    pub max_drawdown_bps: u128,
    pub closed_positions: u64,
    pub pending_orders: usize,
    pub expired_entries: u64,
    pub rejected_entries: u64,
    pub open_positions: Vec<OpenPosition>,
    pub wallets: Vec<WalletReport>,
    pub copy_performance: Vec<CopyPerformance>,
    pub signals: Vec<Signal>,
    pub fills: Vec<Fill>,
    pub equity: Vec<Equity>,
    pub assumptions: Vec<&'static str>,
}

pub struct Engine {
    pub config: Config,
    wallets: Wallets,
    markets: BTreeMap<String, Market>,
    flows: BTreeMap<String, VecDeque<FlowPoint>>,
    cooldowns: BTreeMap<String, u64>,
    positions: BTreeMap<String, Position>,
    pending: Vec<Pending>,
    pub signals: Vec<Signal>,
    pub fills: Vec<Fill>,
    equity: Vec<Equity>,
    cash: u64,
    realized: i128,
    closed: u64,
    last_slot: Option<u64>,
    last_hash: String,
    now: u64,
    batches: u64,
    trades: u64,
    expired: u64,
    rejected: u64,
    peak_equity: u128,
    max_drawdown_bps: u128,
}
impl Engine {
    pub fn new(config: Config) -> Result<Self> {
        config.validate()?;
        let cash = config.strategy.initial_cash_lamports;
        Ok(Self {
            config,
            wallets: Wallets::default(),
            markets: BTreeMap::new(),
            flows: BTreeMap::new(),
            cooldowns: BTreeMap::new(),
            positions: BTreeMap::new(),
            pending: vec![],
            signals: vec![],
            fills: vec![],
            equity: vec![],
            cash,
            realized: 0,
            closed: 0,
            last_slot: None,
            last_hash: String::new(),
            now: 0,
            batches: 0,
            trades: 0,
            expired: 0,
            rejected: 0,
            peak_equity: cash as u128,
            max_drawdown_bps: 0,
        })
    }
    pub fn process(&mut self, b: &Batch) -> Result<()> {
        b.validate()?;
        if self.last_slot == Some(b.slot) {
            ensure!(
                self.last_hash == b.blockhash,
                "conflicting block for finalized slot"
            );
            return Ok(());
        }
        ensure!(
            self.last_slot.is_none_or(|s| b.slot > s),
            "input must have strictly increasing slots"
        );
        // Solana block-time estimates can regress; event clock never does.
        self.now = self.now.max(b.block_time_ms);
        self.last_slot = Some(b.slot);
        self.last_hash = b.blockhash.clone();
        self.batches += 1;
        // A finalized block is atomic to this model. Fills use final reserves,
        // never an intermediate swap in the same transaction or block.
        for t in &b.trades {
            if self
                .markets
                .get(&t.mint)
                .is_some_and(|m| m.venue == Venue::PumpSwap && t.venue == Venue::PumpFun)
            {
                continue;
            }
            self.markets.insert(
                t.mint.clone(),
                Market {
                    reserve: t.reserve.clone(),
                    venue: t.venue,
                    pool: t.pool.clone(),
                    time: self.now,
                    slot: b.slot,
                },
            );
        }
        self.fill_pending(b.slot)?;
        for t in &b.trades {
            self.trades += 1;
            // Determine copy eligibility BEFORE this trade changes performance.
            let copy = t.side == Side::Buy
                && self
                    .wallets
                    .eligible(&t.wallet, &self.config.selection, self.now);
            self.wallets.observe(t, self.now);
            let flow = self.flows.entry(t.mint.clone()).or_default();
            flow.push_back(FlowPoint {
                time: self.now,
                wallet: t.wallet.clone(),
                buy: t.side == Side::Buy,
                quote: t.quote_lamports,
            });
            while flow
                .front()
                .is_some_and(|p| self.now.saturating_sub(p.time) > self.config.flow.window_ms)
            {
                flow.pop_front();
            }
            let mut per_wallet: BTreeMap<&str, (u128, u128)> = BTreeMap::new();
            let (mut buy, mut sell) = (0u128, 0u128);
            for p in flow.iter() {
                let v = per_wallet.entry(&p.wallet).or_default();
                if p.buy {
                    buy += p.quote as u128;
                    v.0 += p.quote as u128;
                } else {
                    sell += p.quote as u128;
                    v.1 += p.quote as u128;
                }
            }
            let whales = per_wallet
                .values()
                .filter(|(a, z)| *a >= self.config.flow.whale_buy_lamports as u128 && a > z)
                .count();
            let net = buy as i128 - sell as i128;
            let flow_hit = t.side == Side::Buy
                && whales >= self.config.flow.min_whales
                && net >= self.config.flow.min_net_inflow_lamports as i128
                && buy * 10_000 >= sell * self.config.flow.min_buy_sell_ratio_bps as u128;
            let flow_alert = flow_hit
                && self
                    .cooldowns
                    .get(&t.mint)
                    .is_none_or(|at| self.now.saturating_sub(*at) >= self.config.flow.cooldown_ms);
            if copy || flow_alert {
                self.signals.push(Signal {
                    time_ms: self.now,
                    slot: b.slot,
                    mint: t.mint.clone(),
                    venue: t.venue,
                    kind: if copy && flow_hit {
                        "copy_and_flow"
                    } else if copy {
                        "copy"
                    } else {
                        "flow"
                    }
                    .into(),
                    source_wallet: copy.then(|| t.wallet.clone()),
                    whale_count: whales,
                    buy_lamports: buy,
                    sell_lamports: sell,
                    net_inflow_lamports: net,
                });
            }
            if flow_alert {
                self.cooldowns.insert(t.mint.clone(), self.now);
            }
            let trigger = match self.config.strategy.entry_mode {
                EntryMode::Copy => copy,
                EntryMode::Flow => flow_alert,
                EntryMode::Either => copy || flow_alert,
                EntryMode::Both => copy && flow_hit,
            };
            let venue_allowed = match t.venue {
                Venue::PumpFun => self.config.strategy.allow_pre_graduation,
                Venue::PumpSwap => self.config.strategy.allow_post_graduation,
            };
            if trigger
                && venue_allowed
                && self.now >= self.config.strategy.trade_start_ms
                && !self.positions.contains_key(&t.mint)
                && !self.pending.iter().any(|p| p.mint == t.mint)
            {
                self.pending.push(Pending {
                    mint: t.mint.clone(),
                    source: copy.then(|| t.wallet.clone()),
                    reason: if copy { "copy" } else { "flow" }.into(),
                    created: self.now,
                    slot: b.slot,
                    sell: false,
                });
            }
        }
        // No sell on source-wallet exits, no stop loss, no time-based liquidation.
        for (mint, p) in &self.positions {
            if p.entry_slot == b.slot || self.pending.iter().any(|o| o.mint == *mint) {
                continue;
            }
            if let Some(net) = self.mark(mint, p.tokens) {
                if self.target_met(net, p.cost_lamports) {
                    self.pending.push(Pending {
                        mint: mint.clone(),
                        source: p.source_wallet.clone(),
                        reason: "target".into(),
                        created: self.now,
                        slot: b.slot,
                        sell: true,
                    });
                }
            }
        }
        // Bound inactive flow state independently of token popularity.
        self.flows.retain(|_, q| {
            q.back()
                .is_some_and(|p| self.now.saturating_sub(p.time) <= self.config.flow.window_ms)
        });
        self.cooldowns
            .retain(|_, t| self.now.saturating_sub(*t) <= self.config.flow.cooldown_ms);
        self.record_equity();
        Ok(())
    }
    fn target_met(&self, net: u64, cost: u64) -> bool {
        net as u128 * 10_000 >= cost as u128 * self.config.strategy.target_multiple_bps as u128
    }
    fn mark(&self, mint: &str, tokens: u64) -> Option<u64> {
        let m = self.markets.get(mint)?;
        if m.venue == Venue::PumpFun && m.reserve.real_base == 0 {
            // Completed curves cannot execute swaps while migration is pending.
            return None;
        }
        if self.now.saturating_sub(m.time) > self.config.strategy.max_mark_age_ms {
            return None;
        }
        m.reserve
            .sell(tokens, self.config.strategy.adverse_slippage_bps)
            .map(|x| x.saturating_sub(self.config.strategy.network_fee_lamports))
    }
    fn fill_pending(&mut self, slot: u64) -> Result<()> {
        let mut keep = vec![];
        for order in std::mem::take(&mut self.pending) {
            let age = self.now.saturating_sub(order.created);
            if age > self.config.strategy.pending_ttl_ms {
                if !order.sell {
                    self.expired += 1;
                }
                continue;
            }
            if age < self.config.strategy.latency_ms || slot <= order.slot {
                keep.push(order);
                continue;
            }
            let Some(m) = self.markets.get(&order.mint).cloned() else {
                keep.push(order);
                continue;
            };
            // Require a fresh update for THIS mint in the execution block.
            if m.slot != slot {
                keep.push(order);
                continue;
            }
            if order.sell {
                let Some(p) = self.positions.get(&order.mint).cloned() else {
                    continue;
                };
                let Some(net) = self.mark(&order.mint, p.tokens) else {
                    keep.push(order);
                    continue;
                };
                // Delayed price fell back below target: cancel, keep the position.
                if !self.target_met(net, p.cost_lamports) {
                    continue;
                }
                self.cash = self
                    .cash
                    .checked_add(net)
                    .ok_or_else(|| anyhow::anyhow!("cash overflow"))?;
                let pnl = net as i128 - p.cost_lamports as i128;
                self.realized += pnl;
                self.closed += 1;
                self.positions.remove(&order.mint);
                self.fills.push(Fill {
                    time_ms: self.now,
                    slot,
                    mint: order.mint,
                    venue: m.venue,
                    pool: m.pool,
                    side: Side::Sell,
                    tokens: p.tokens,
                    cash_lamports: net,
                    realized_pnl_lamports: Some(pnl),
                    reason: order.reason,
                    source_wallet: order.source,
                });
            } else {
                let s = &self.config.strategy;
                let cost = s.order_lamports + s.network_fee_lamports + s.entry_rent_lamports;
                let exposure: u128 = self
                    .positions
                    .values()
                    .map(|p| p.cost_lamports as u128)
                    .sum();
                let allowed = match m.venue {
                    Venue::PumpFun => s.allow_pre_graduation,
                    Venue::PumpSwap => s.allow_post_graduation,
                };
                if !allowed
                    || self.positions.len() >= s.max_positions
                    || self.positions.contains_key(&order.mint)
                    || self.cash < cost
                    || exposure + cost as u128 > s.max_exposure_lamports as u128
                    || s.order_lamports as u128 * 10_000
                        > m.reserve.quote as u128 * s.max_reserve_participation_bps as u128
                {
                    self.rejected += 1;
                    continue;
                }
                let Some(tokens) = m.reserve.buy(s.order_lamports, s.adverse_slippage_bps) else {
                    self.rejected += 1;
                    continue;
                };
                self.cash -= cost;
                self.positions.insert(
                    order.mint.clone(),
                    Position {
                        mint: order.mint.clone(),
                        tokens,
                        cost_lamports: cost,
                        entry_time_ms: self.now,
                        entry_slot: slot,
                        entry_venue: m.venue,
                        source_wallet: order.source.clone(),
                        reason: order.reason.clone(),
                    },
                );
                self.fills.push(Fill {
                    time_ms: self.now,
                    slot,
                    mint: order.mint,
                    venue: m.venue,
                    pool: m.pool,
                    side: Side::Buy,
                    tokens,
                    cash_lamports: cost,
                    realized_pnl_lamports: None,
                    reason: order.reason,
                    source_wallet: order.source,
                });
            }
        }
        self.pending = keep;
        Ok(())
    }
    fn record_equity(&mut self) {
        let equity = self.cash as u128
            + self
                .positions
                .iter()
                .filter_map(|(m, p)| self.mark(m, p.tokens).map(u128::from))
                .sum::<u128>();
        self.peak_equity = self.peak_equity.max(equity);
        self.max_drawdown_bps = self
            .max_drawdown_bps
            .max((self.peak_equity - equity) * 10_000 / self.peak_equity.max(1));
        self.equity.push(Equity {
            time_ms: self.now,
            cash_lamports: self.cash,
            liquidation_equity_lamports: equity,
            open_positions: self.positions.len(),
        });
    }
    pub fn report(&self) -> Report {
        let mut per_source: BTreeMap<Option<String>, CopyPerformance> = BTreeMap::new();
        for fill in &self.fills {
            let row = per_source
                .entry(fill.source_wallet.clone())
                .or_insert_with(|| CopyPerformance {
                    source_wallet: fill.source_wallet.clone(),
                    ..Default::default()
                });
            match fill.side {
                Side::Buy => {
                    row.entries += 1;
                    row.zero_recovery_pnl_lamports -= fill.cash_lamports as i128;
                }
                Side::Sell => {
                    row.exits += 1;
                    row.zero_recovery_pnl_lamports += fill.cash_lamports as i128;
                    row.realized_pnl_lamports += fill.realized_pnl_lamports.unwrap_or(0);
                }
            }
        }
        for position in self.positions.values() {
            if let Some(row) = per_source.get_mut(&position.source_wallet) {
                row.open_cost_lamports += position.cost_lamports as u128;
            }
        }
        let open: Vec<_> = self
            .positions
            .iter()
            .map(|(m, p)| {
                let mark = self.mark(m, p.tokens);
                OpenPosition {
                    position: p.clone(),
                    estimated_net_liquidation_lamports: mark.unwrap_or(0),
                    mark_available: mark.is_some(),
                }
            })
            .collect();
        Report {mode:"paper_simulation",config:self.config.clone(),batches:self.batches,trades:self.trades,end_time_ms:self.now,
            initial_cash_lamports:self.config.strategy.initial_cash_lamports,cash_lamports:self.cash,
            estimated_liquidation_equity_lamports:self.cash as u128+open.iter().map(|p|p.estimated_net_liquidation_lamports as u128).sum::<u128>(),
            zero_recovery_equity_lamports:self.cash,realized_pnl_lamports:self.realized,max_drawdown_bps:self.max_drawdown_bps,
            closed_positions:self.closed,pending_orders:self.pending.len(),expired_entries:self.expired,rejected_entries:self.rejected,
            open_positions:open,wallets:self.wallets.report(&self.config.selection,self.now),copy_performance:per_source.into_values().collect(),signals:self.signals.clone(),fills:self.fills.clone(),equity:self.equity.clone(),
            assumptions:vec!["No live orders. Integer constant-product approximation, observed fees, configured costs and delay.",
                "Entries/exits require a later block with a trade in the mint; fills use end-of-block reserves.",
                "Open inventory remains open indefinitely; missing/stale/unliquidatable marks have zero estimated value.",
                "Wallet scores treat all open inventory as zero and exclude wallets with unknown-cost sales.",
                "SOL pairs only. Flow counts addresses, not independently verified entities. Transfers, bundles, and common owners are not classified.",
                "Network fees are fully charged to every successful source swap; failed-transaction fees, separate tips and account rent are not inferred for source wallets.",
                "Replay is event-sampled and does not modify subsequent historical reserves for hypothetical trades. LP changes between trades are unobserved.",
                "Dynamic fee changes and exact venue rounding require transaction-level execution validation before funded use."] }
    }
    pub fn wallet_reports(&self) -> Vec<WalletReport> {
        self.wallets.report(&self.config.selection, self.now)
    }
    pub fn active_mints(&self) -> BTreeSet<String> {
        self.positions.keys().cloned().collect()
    }
}
