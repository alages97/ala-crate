//! Bounded checks of diagnostic quote witnesses against complete blocks.
use anyhow::{Context, Result};
use pump_copy::{
    decoder::{Decoder, Pools},
    model::Reserve,
    rpc::{atomic_json, Rpc},
};
use serde_json::{json, Value};
use std::{fs::File, path::PathBuf};

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let plan: Value = serde_json::from_reader(File::open(&args[1])?)?;
    let mut pools: Pools = serde_json::from_reader(File::open(&args[2])?)?;
    let archive = PathBuf::from(&args[3]);
    let output = PathBuf::from(&args[4]);
    let rpc = Rpc::from_env("SOLANA_RPC_URL", 1)?;
    let decoder = Decoder::default();
    let mut results = Vec::new();
    for check in plan["checks"].as_array().context("checks")? {
        let slot = check["slot"].as_u64().context("slot")?;
        let captured = rpc.capture(slot, &archive, &decoder, &mut pools).await;
        let batch = match captured {
            Ok(b) => b,
            Err(e) => {
                results.push(json!({"slot":slot,"status":"unavailable","error":e.to_string()}));
                atomic_json(
                    &output,
                    &json!({"checks":results,"certifies_strategy":false}),
                )?;
                break;
            }
        };
        let mint = check["mint"].as_str().unwrap();
        let sig = check["witness_signature"].as_str().unwrap();
        let expected: Reserve = serde_json::from_value(check["conservative_reserve"].clone())?;
        let candidates: Vec<_> = batch
            .trades
            .iter()
            .filter(|t| t.mint == mint && t.signature == sig)
            .collect();
        let matched = candidates.iter().any(|t| {
            serde_json::to_value(&t.reserve).unwrap() == serde_json::to_value(&expected).unwrap()
        });
        let final_trade = batch.trades.iter().rev().find(|t| t.mint == mint);
        let setting = &plan["settings"];
        let haircut = setting["adverse_slippage_bps"].as_u64().unwrap() as u32;
        let fee = setting["network_fee_lamports"].as_u64().unwrap();
        let actual = final_trade.and_then(|t| {
            if check["side"] == "buy" {
                t.reserve
                    .buy(setting["order_lamports"].as_u64().unwrap(), haircut)
            } else {
                t.reserve
                    .sell(check["tokens"].as_u64().unwrap(), haircut)
                    .map(|x| x.saturating_sub(fee))
            }
        });
        results.push(json!({"slot":slot,"mint":mint,"side":check["side"],"modeled_outcome":check["outcome"],"status":if matched{"witness_matches_full_transaction"}else{"witness_mismatch"},"witness_signature":sig,"full_block_supported_trades":batch.trades.len(),"mint_trades_in_block":batch.trades.iter().filter(|t|t.mint==mint).count(),"matching_signature_trades":candidates.len(),"observed_witness_network_fee_lamports":candidates.iter().map(|t|t.network_fee_lamports).max(),"conservative_quote_value":if check["side"]=="buy"{expected.buy(setting["order_lamports"].as_u64().unwrap(),haircut)}else{expected.sell(check["tokens"].as_u64().unwrap(),haircut).map(|x|x.saturating_sub(fee))},"value_unit":if check["side"]=="buy"{"raw_token_units"}else{"lamports_net_of_modeled_network_fee"},"full_block_final_quote_value":actual,"full_block_final_reserve":final_trade.map(|t|&t.reserve),"minimum_target_lamports":check["net_target_lamports"],"full_block_final_quote_meets_target":if check["side"]=="sell"{actual.map(|v|v>=check["net_target_lamports"].as_u64().unwrap())}else{None}}));
        atomic_json(
            &output,
            &json!({"checks":results,"certifies_strategy":false,"limitation":"Representative historical witness and final observed swap-state checks only. No simulated order was submitted; intervening liquidity changes, inclusion, dynamic fees and all unverified execution windows remain uncertain."}),
        )?;
        println!("verified slot {slot}: witness match {matched}");
    }
    Ok(())
}
