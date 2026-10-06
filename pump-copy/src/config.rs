use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub strategy: Strategy,
    pub selection: Selection,
    pub flow: Flow,
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EntryMode {
    Copy,
    Flow,
    Either,
    Both,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Strategy {
    pub entry_mode: EntryMode,
    pub initial_cash_lamports: u64,
    pub order_lamports: u64,
    /// 20_000 means 2x ALL-IN entry cost in net exit proceeds.
    pub target_multiple_bps: u32,
    pub latency_ms: u64,
    pub pending_ttl_ms: u64,
    pub network_fee_lamports: u64,
    pub entry_rent_lamports: u64,
    pub adverse_slippage_bps: u32,
    pub max_reserve_participation_bps: u32,
    pub max_positions: usize,
    pub max_exposure_lamports: u64,
    pub max_mark_age_ms: u64,
    pub trade_start_ms: u64,
    pub allow_pre_graduation: bool,
    pub allow_post_graduation: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Selection {
    pub min_closed_positions: u64,
    pub min_distinct_mints: usize,
    pub min_history_ms: u64,
    pub max_inactivity_ms: u64,
    pub min_conservative_profit_lamports: u64,
    pub min_roi_bps: i64,
    pub max_profit_concentration_bps: u32,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Flow {
    pub window_ms: u64,
    pub whale_buy_lamports: u64,
    pub min_whales: usize,
    pub min_net_inflow_lamports: u64,
    pub min_buy_sell_ratio_bps: u32,
    pub cooldown_ms: u64,
}
impl Config {
    pub fn validate(&self) -> Result<()> {
        let s = &self.strategy;
        ensure!(
            s.order_lamports > 0 && s.initial_cash_lamports > 0,
            "positive order/cash required"
        );
        ensure!(s.target_multiple_bps > 10_000, "target must exceed 1x");
        ensure!(
            s.pending_ttl_ms > s.latency_ms,
            "pending TTL must exceed latency"
        );
        ensure!(s.adverse_slippage_bps < 10_000, "invalid slippage");
        ensure!(
            (1..=1000).contains(&s.max_reserve_participation_bps),
            "reserve participation must be 1..1000 bps"
        );
        ensure!(
            s.max_positions > 0 && s.max_mark_age_ms > 0,
            "positive limits required"
        );
        ensure!(
            s.max_exposure_lamports <= s.initial_cash_lamports,
            "exposure exceeds cash"
        );
        ensure!(
            s.allow_pre_graduation || s.allow_post_graduation,
            "no enabled venue"
        );
        let all_in = s
            .order_lamports
            .checked_add(s.network_fee_lamports)
            .and_then(|x| x.checked_add(s.entry_rent_lamports));
        ensure!(
            all_in.is_some_and(|x| x <= s.max_exposure_lamports),
            "order with costs exceeds exposure"
        );
        ensure!(
            self.selection.min_closed_positions > 0 && self.selection.min_distinct_mints > 0,
            "positive selection sample required"
        );
        ensure!(
            self.selection.max_profit_concentration_bps <= 10_000,
            "invalid concentration"
        );
        ensure!(
            self.flow.window_ms > 0
                && self.flow.min_whales > 0
                && self.flow.whale_buy_lamports > 0
                && self.flow.min_net_inflow_lamports > 0,
            "invalid flow limits"
        );
        Ok(())
    }
}
