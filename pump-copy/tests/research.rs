use pump_copy::{
    config::{Config, EntryMode},
    decoder::{Decoder, Pools},
    engine::Engine,
    model::{Batch, Reserve, Side, Trade, Venue, SOL},
    wallets::Wallets,
};

fn config() -> Config {
    let mut c: Config = toml::from_str(include_str!("../config/default.toml")).unwrap();
    c.strategy.entry_mode = EntryMode::Flow;
    c.strategy.latency_ms = 1000;
    c.strategy.network_fee_lamports = 0;
    c.strategy.entry_rent_lamports = 0;
    c.strategy.adverse_slippage_bps = 0;
    c.flow.min_whales = 1;
    c.flow.whale_buy_lamports = 1;
    c.flow.min_net_inflow_lamports = 1;
    c.selection.min_closed_positions = 1;
    c.selection.min_distinct_mints = 1;
    c.selection.min_history_ms = 0;
    c.selection.min_conservative_profit_lamports = 1;
    c.selection.max_profit_concentration_bps = 10_000;
    c
}
fn trade(mint: &str, wallet: &str, side: Side, quote: u64) -> Trade {
    Trade {
        signature: "signature".into(),
        tx_index: 0,
        event_index: 0,
        mint: mint.into(),
        wallet: wallet.into(),
        pool: "pool".into(),
        venue: Venue::PumpFun,
        side,
        tokens: 1_000_000,
        quote_lamports: quote,
        network_fee_lamports: 0,
        reserve: Reserve {
            base: 1_000_000_000,
            quote: 100 * SOL,
            real_base: 1_000_000_000,
            real_quote: 100 * SOL,
            fee_bps: 0,
        },
    }
}
fn batch(slot: u64, t: Trade) -> Batch {
    Batch {
        schema_version: 1,
        slot,
        block_time_ms: 1_000_000 + slot * 2000,
        blockhash: format!("block{slot}"),
        trades: vec![t],
        skipped_non_sol: 0,
    }
}
fn buy(slot: u64) -> Batch {
    batch(slot, trade("mint", "wallet", Side::Buy, SOL))
}

#[test]
fn reserves_account_for_impact_fees_and_real_liquidity() {
    let r = Reserve {
        base: 1000,
        quote: 1000,
        real_base: 1000,
        real_quote: 1000,
        fee_bps: 100,
    };
    assert_eq!(r.buy(101, 0), Some(90));
    assert!(r.sell(90, 0).unwrap() < 101);
    assert!(Reserve {
        real_base: 50,
        ..r.clone()
    }
    .buy(101, 0)
    .is_none());
    assert!(Reserve {
        real_quote: 1,
        ..r.clone()
    }
    .sell(90, 0)
    .is_none());
    assert!(r.buy(101, 100).unwrap() < r.buy(101, 0).unwrap());
}
#[test]
fn source_fees_partial_sales_and_open_losers_are_charged() {
    let c = config();
    let mut w = Wallets::default();
    let mut b = trade("mint", "wallet", Side::Buy, SOL);
    b.network_fee_lamports = 100;
    w.observe(&b, 1);
    let mut s = trade("mint", "wallet", Side::Sell, SOL);
    s.tokens = 500_000;
    s.network_fee_lamports = 100;
    w.observe(&s, 2);
    let r = &w.report(&c.selection, 2)[0];
    assert_eq!(r.realized_pnl_lamports, 499_999_850);
    assert_eq!(r.open_cost_lamports, 500_000_050);
    assert_eq!(r.conservative_pnl_lamports, -200);
    assert!(!r.eligible);
}
#[test]
fn unknown_inventory_never_gets_free_cost_basis() {
    let c = config();
    let mut w = Wallets::default();
    w.observe(&trade("mint", "wallet", Side::Sell, 100 * SOL), 1);
    let r = &w.report(&c.selection, 1)[0];
    assert_eq!(r.realized_pnl_lamports, 0);
    assert_eq!(r.unknown_cost_sales, 1);
    assert!(!r.eligible);
}
#[test]
fn no_entry_on_signal_block_even_with_zero_delay() {
    let mut c = config();
    c.strategy.latency_ms = 0;
    let mut e = Engine::new(c).unwrap();
    e.process(&buy(1)).unwrap();
    assert!(e.fills.is_empty());
    e.process(&buy(2)).unwrap();
    assert_eq!(e.fills.len(), 1);
    assert_eq!(e.fills[0].slot, 2);
}
#[test]
fn later_transactions_in_same_block_determine_fill_reserves() {
    let mut e = Engine::new(config()).unwrap();
    e.process(&buy(1)).unwrap();
    let mut b = buy(2);
    let mut later = b.trades[0].clone();
    later.tx_index = 1;
    later.reserve.quote = 200 * SOL;
    later.reserve.real_quote = 200 * SOL;
    b.trades.push(later);
    e.process(&b).unwrap();
    assert_eq!(
        e.fills[0].tokens,
        b.trades[1].reserve.buy(SOL / 10, 0).unwrap()
    );
}
#[test]
fn requires_updated_mint_to_fill_not_an_unrelated_tick() {
    let mut e = Engine::new(config()).unwrap();
    e.process(&buy(1)).unwrap();
    e.process(&batch(2, trade("other", "other", Side::Sell, SOL)))
        .unwrap();
    assert!(e.fills.is_empty());
}
#[test]
fn only_target_sells_and_migration_preserves_position() {
    let mut e = Engine::new(config()).unwrap();
    e.process(&buy(1)).unwrap();
    e.process(&buy(2)).unwrap();
    let mut high = buy(3);
    high.trades[0].venue = Venue::PumpSwap;
    high.trades[0].reserve.quote = 300 * SOL;
    high.trades[0].reserve.real_quote = 300 * SOL;
    e.process(&high).unwrap();
    assert_eq!(e.fills.len(), 1);
    high.slot = 4;
    high.blockhash = "block4".into();
    high.block_time_ms += 2000;
    e.process(&high).unwrap();
    let r = e.report();
    assert_eq!(r.closed_positions, 1);
    assert!(r.open_positions.is_empty());
    assert_eq!(r.fills[1].venue, Venue::PumpSwap);
    assert!(r.fills[1].cash_lamports as u128 * 10_000 >= r.fills[0].cash_lamports as u128 * 20_000);
}
#[test]
fn delayed_exit_rechecks_target_and_keeps_crashed_position() {
    let mut e = Engine::new(config()).unwrap();
    e.process(&buy(1)).unwrap();
    e.process(&buy(2)).unwrap();
    let mut high = buy(3);
    high.trades[0].reserve.quote = 300 * SOL;
    high.trades[0].reserve.real_quote = 300 * SOL;
    e.process(&high).unwrap();
    let mut crash = buy(4);
    crash.trades[0].reserve.quote = SOL;
    crash.trades[0].reserve.real_quote = SOL;
    e.process(&crash).unwrap();
    let r = e.report();
    assert_eq!(r.closed_positions, 0);
    assert_eq!(r.open_positions.len(), 1);
    assert_eq!(r.fills.len(), 1);
    assert_eq!(r.zero_recovery_equity_lamports, 10 * SOL - SOL / 10);
}
#[test]
fn no_forced_exit_or_cash_recycling_when_data_ends() {
    let mut e = Engine::new(config()).unwrap();
    e.process(&buy(1)).unwrap();
    e.process(&buy(2)).unwrap();
    let mut empty = buy(100_000);
    empty.trades.clear();
    e.process(&empty).unwrap();
    let r = e.report();
    assert_eq!(r.open_positions.len(), 1);
    assert_eq!(r.cash_lamports, 10 * SOL - SOL / 10);
    assert!(!r.open_positions[0].mark_available);
    assert_eq!(
        r.estimated_liquidation_equity_lamports,
        r.cash_lamports as u128
    );
    assert_eq!(
        r.copy_performance[0].zero_recovery_pnl_lamports,
        -(SOL as i128 / 10)
    );
    assert_eq!(r.copy_performance[0].open_cost_lamports, SOL as u128 / 10);
}
#[test]
fn future_winner_cannot_be_copied_before_qualification() {
    let mut c = config();
    c.strategy.entry_mode = EntryMode::Copy;
    c.flow.min_whales = 99;
    let mut e = Engine::new(c).unwrap();
    e.process(&batch(1, trade("a", "leader", Side::Buy, SOL)))
        .unwrap();
    assert!(e.signals.is_empty());
    e.process(&batch(2, trade("a", "leader", Side::Sell, 3 * SOL)))
        .unwrap();
    assert!(e.signals.is_empty());
    e.process(&batch(3, trade("b", "leader", Side::Buy, SOL)))
        .unwrap();
    assert_eq!(e.signals.len(), 1);
    assert_eq!(e.signals[0].slot, 3);
}
#[test]
fn repeated_buys_by_one_address_are_not_multiple_whales() {
    let mut c = config();
    c.flow.min_whales = 2;
    let mut e = Engine::new(c).unwrap();
    e.process(&buy(1)).unwrap();
    e.process(&buy(2)).unwrap();
    assert!(e.signals.is_empty());
    e.process(&batch(3, trade("mint", "second", Side::Buy, SOL)))
        .unwrap();
    assert_eq!(e.signals.len(), 1);
    assert_eq!(e.signals[0].whale_count, 2);
}
#[test]
fn net_selling_prevents_a_volume_only_signal() {
    let mut e = Engine::new(config()).unwrap();
    e.process(&batch(1, trade("mint", "seller", Side::Sell, 10 * SOL)))
        .unwrap();
    e.process(&buy(2)).unwrap();
    assert!(e.signals.is_empty());
}
#[test]
fn expired_signals_do_not_fill_late() {
    let mut e = Engine::new(config()).unwrap();
    e.process(&buy(1)).unwrap();
    e.process(&buy(100)).unwrap();
    assert!(e.fills.is_empty());
    assert_eq!(e.report().expired_entries, 1);
}
#[test]
fn duplicates_are_idempotent_and_conflicting_blocks_rejected() {
    let mut e = Engine::new(config()).unwrap();
    let b = buy(1);
    e.process(&b).unwrap();
    e.process(&b).unwrap();
    assert_eq!(e.report().trades, 1);
    let mut conflict = b;
    conflict.blockhash = "different".into();
    assert!(e.process(&conflict).is_err());
}
#[test]
fn cap_counts_open_losing_positions() {
    let mut c = config();
    c.strategy.max_positions = 1;
    let mut e = Engine::new(c).unwrap();
    e.process(&buy(1)).unwrap();
    e.process(&buy(2)).unwrap();
    e.process(&batch(3, trade("other", "other", Side::Buy, SOL)))
        .unwrap();
    e.process(&batch(4, trade("other", "other", Side::Buy, SOL)))
        .unwrap();
    assert_eq!(e.report().open_positions.len(), 1);
    assert_eq!(e.report().rejected_entries, 1);
}
#[test]
fn holdout_start_uses_prior_history_without_prior_positions() {
    let mut c = config();
    c.strategy.trade_start_ms = 1_010_000;
    let mut e = Engine::new(c).unwrap();
    for i in 1..5 {
        e.process(&buy(i)).unwrap();
    }
    assert!(e.fills.is_empty());
    assert_eq!(e.report().trades, 4);
}
#[test]
fn real_mainnet_v1_events_decode_and_failures_are_excluded() {
    let block: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/mainnet-451893882.json")).unwrap();
    let d = Decoder::default();
    let mut count = 0;
    for tx in block["transactions"].as_array().unwrap() {
        let e = d.transaction_events(tx).unwrap();
        if !tx["meta"]["err"].is_null() {
            assert!(e.is_empty());
        }
        count += e.len();
    }
    assert!(count > 50, "should decode substantial real event coverage");
    assert!(!d
        .required_pools(&block, &mut Pools::new())
        .unwrap()
        .is_empty());
}
#[test]
fn invalid_config_and_duplicate_events_rejected() {
    let mut c = config();
    c.strategy.target_multiple_bps = 10_000;
    assert!(Engine::new(c).is_err());
    let mut b = buy(1);
    b.trades.push(b.trades[0].clone());
    assert!(b.validate().is_err());
}

#[test]
fn mainnet_normalization_does_not_add_fee_allocation_fields_twice() {
    let block: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/mainnet-451893882.json")).unwrap();
    let pools: Pools = serde_json::from_str(include_str!("fixtures/mainnet-pools.json")).unwrap();
    let decoder = Decoder::default();
    let batch = decoder.block(451893882, &block, &pools).unwrap();
    assert_eq!(batch.trades.len(), 37);
    assert_eq!(
        batch
            .trades
            .iter()
            .filter(|t| t.venue == Venue::PumpFun)
            .count(),
        8
    );
    assert_eq!(batch.skipped_non_sol, 42);
    let first = batch
        .trades
        .iter()
        .find(|t| t.venue == Venue::PumpFun)
        .unwrap();
    // Mainnet event reports protocol=95bps, creator=30bps, and a 5000bps
    // buyback SHARE of protocol fees. Holder rewards likewise duplicate creator fees.
    assert_eq!(first.reserve.fee_bps, 125);
    assert_eq!(first.quote_lamports, 120_428_139 - 1_144_068 - 361_285);
    for t in &batch.trades {
        let tx = &block["transactions"][t.tx_index as usize];
        let event = decoder
            .transaction_events(tx)
            .unwrap()
            .into_iter()
            .nth(t.event_index as usize)
            .unwrap();
        let f = event.fields;
        let expected = if t.venue == Venue::PumpFun {
            f["fee_basis_points"].as_u64().unwrap()
                + f["creator_fee_basis_points"].as_u64().unwrap()
        } else {
            f["lp_fee_basis_points"].as_u64().unwrap()
                + f["protocol_fee_basis_points"].as_u64().unwrap()
                + f["coin_creator_fee_basis_points"].as_u64().unwrap()
        };
        assert_eq!(t.reserve.fee_bps as u64, expected);
    }
}

#[test]
fn foreign_program_event_logs_cannot_spoof_trades() {
    let block: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/mainnet-451893882.json")).unwrap();
    let mut tx = block["transactions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|t| {
            t["meta"]["err"].is_null()
                && t["meta"]["logMessages"].as_array().is_some_and(|l| {
                    l.iter()
                        .any(|x| x.as_str().is_some_and(|s| s.starts_with("Program data: ")))
                })
        })
        .unwrap()
        .clone();
    let data = tx["meta"]["logMessages"]
        .as_array()
        .unwrap()
        .iter()
        .find(|l| l.as_str().is_some_and(|s| s.starts_with("Program data: ")))
        .unwrap()
        .clone();
    tx["meta"]["innerInstructions"] = serde_json::json!([]);
    tx["meta"]["logMessages"] = serde_json::json!([
        "Program Foreign invoke [1]",
        data,
        "Program Foreign success"
    ]);
    assert!(Decoder::default()
        .transaction_events(&tx)
        .unwrap()
        .is_empty());
}

#[test]
fn archive_coverage_rejects_a_missing_produced_slot() {
    let d = tempfile::tempdir().unwrap();
    pump_copy::rpc::atomic_json(&d.path().join("blocks/1.json"), &buy(1)).unwrap();
    let coverage = pump_copy::rpc::Coverage {
        start_slot: 1,
        end_slot: 2,
        produced_slots: vec![1, 2],
        commitment: "finalized".into(),
    };
    pump_copy::rpc::atomic_json(&d.path().join("coverage.json"), &coverage).unwrap();
    assert!(pump_copy::rpc::load_batches(d.path()).is_err());
}

#[test]
fn completed_curve_waits_for_migration_without_fabricating_an_exit() {
    let mut e = Engine::new(config()).unwrap();
    e.process(&buy(1)).unwrap();
    e.process(&buy(2)).unwrap();
    let mut completed = buy(3);
    completed.trades[0].reserve.quote = 300 * SOL;
    completed.trades[0].reserve.real_quote = 300 * SOL;
    completed.trades[0].reserve.real_base = 0;
    e.process(&completed).unwrap();
    completed.slot = 4;
    completed.block_time_ms += 2000;
    completed.blockhash = "block4".into();
    e.process(&completed).unwrap();
    assert_eq!(e.fills.len(), 1);
    assert!(!e.report().open_positions[0].mark_available);
}

#[test]
fn target_selection_never_uses_holdout_and_warms_rankings_only() {
    let c = config();
    let mut data = vec![buy(1), buy(2), buy(3), buy(4), buy(5), buy(6)];
    for b in &mut data[2..] {
        b.trades[0].reserve.quote = 300 * SOL;
        b.trades[0].reserve.real_quote = 300 * SOL;
    }
    let split = data[3].block_time_ms;
    let end = data[5].block_time_ms;
    let first =
        pump_copy::research::walk_forward(&c, &data, split, end, &[20_000, 50_000]).unwrap();
    data[4].trades[0].reserve.quote = SOL;
    data[5].trades[0].reserve.quote = 10_000 * SOL;
    let changed =
        pump_copy::research::walk_forward(&c, &data, split, end, &[20_000, 50_000]).unwrap();
    assert_eq!(first.selected.strategy.target_multiple_bps, 20_000);
    assert_eq!(
        first.selected.strategy.target_multiple_bps,
        changed.selected.strategy.target_multiple_bps
    );
    assert!(first.holdout.fills.iter().all(|f| f.time_ms > split));
    // Training portfolio positions are never imported into the holdout.
    assert_eq!(first.holdout.report().closed_positions, 0);
}
