use crate::{config::Config, engine::Engine, model::Batch};
use anyhow::{ensure, Context, Result};
use serde::Serialize;

#[derive(Serialize)]
pub struct Candidate {
    pub target_multiple_bps: u32,
    pub zero_recovery_equity_lamports: u64,
    pub realized_pnl_lamports: i128,
    pub closed_positions: u64,
    pub open_positions: usize,
    pub entries: usize,
}
pub struct WalkForward {
    pub selected: Config,
    pub candidates: Vec<Candidate>,
    pub training: Engine,
    pub holdout: Engine,
}

/// Select a target using ONLY the training interval. Latency remains fixed:
/// it is an execution constraint, not a knob to optimize for artificial returns.
pub fn walk_forward(
    config: &Config,
    batches: &[Batch],
    train_end_ms: u64,
    test_end_ms: u64,
    targets: &[u32],
) -> Result<WalkForward> {
    ensure!(!targets.is_empty(), "no target candidates");
    ensure!(
        train_end_ms < test_end_ms,
        "training must end before holdout"
    );
    let train: Vec<_> = batches
        .iter()
        .take_while(|b| b.block_time_ms <= train_end_ms)
        .collect();
    ensure!(!train.is_empty(), "empty training interval");
    ensure!(
        batches
            .iter()
            .any(|b| b.block_time_ms > train_end_ms && b.block_time_ms <= test_end_ms),
        "empty holdout interval"
    );
    ensure!(
        config.strategy.trade_start_ms <= train_end_ms,
        "trading starts after training ends"
    );
    let mut candidates = vec![];
    let mut best: Option<(u64, u128, u32, Engine)> = None;
    for target in targets {
        let mut c = config.clone();
        c.strategy.target_multiple_bps = *target;
        let mut e = Engine::new(c)?;
        for b in &train {
            e.process(b)?;
        }
        let r = e.report();
        let entries = r
            .fills
            .iter()
            .filter(|f| f.side == crate::model::Side::Buy)
            .count();
        candidates.push(Candidate {
            target_multiple_bps: *target,
            zero_recovery_equity_lamports: r.zero_recovery_equity_lamports,
            realized_pnl_lamports: r.realized_pnl_lamports,
            closed_positions: r.closed_positions,
            open_positions: r.open_positions.len(),
            entries,
        });
        // Higher cash after writing all open positions to zero wins. Ties prefer
        // lower drawdown, then the smaller target; results are deterministic.
        let replace = best.as_ref().is_none_or(|(cash, dd, t, _)| {
            r.cash_lamports > *cash
                || (r.cash_lamports == *cash
                    && (r.max_drawdown_bps < *dd || (r.max_drawdown_bps == *dd && *target < *t)))
        });
        if entries > 0 && replace {
            best = Some((r.cash_lamports, r.max_drawdown_bps, *target, e));
        }
    }
    let (_, _, _, training) =
        best.context("no training entries: collect more history or review selection settings")?;
    let selected = training.config.clone();
    let mut c = selected.clone();
    c.strategy.trade_start_ms = train_end_ms.checked_add(1).context("time overflow")?;
    let mut holdout = Engine::new(c)?;
    // Replay historical wallets to warm up; the holdout portfolio starts flat.
    // Open training positions are not force-sold or transferred between runs.
    for b in batches
        .iter()
        .take_while(|b| b.block_time_ms <= test_end_ms)
    {
        holdout.process(b)?;
    }
    Ok(WalkForward {
        selected,
        candidates,
        training,
        holdout,
    })
}
