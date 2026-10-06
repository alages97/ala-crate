use pump_copy::{decoder::Decoder, model::WSOL, reconcile};
use serde_json::{json, Value};

fn fixtures() -> Vec<Value> {
    serde_json::from_str(include_str!("fixtures/reconciliation-transactions.json")).unwrap()
}
fn one(label: &str) -> Value {
    fixtures()
        .into_iter()
        .find(|v| v["label"].as_str().unwrap() == label)
        .unwrap()["transaction"]
        .clone()
}
fn keys(tx: &Value) -> Vec<String> {
    let mut keys: Vec<String> = tx["transaction"]["message"]["accountKeys"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap().into())
        .collect();
    for side in ["writable", "readonly"] {
        if let Some(a) = tx["meta"]["loadedAddresses"][side].as_array() {
            keys.extend(a.iter().map(|v| v.as_str().unwrap().to_owned()));
        }
    }
    keys
}

#[test]
fn real_buys_sells_and_reverse_sol_pools_are_normalized_without_event_amounts() {
    let mut seen = std::collections::BTreeSet::new();
    for f in fixtures()
        .into_iter()
        .filter(|f| f["label"].as_str().unwrap().starts_with('('))
    {
        let tx = &f["transaction"];
        let result = reconcile::transaction(tx).unwrap();
        assert!(Decoder::default().transaction_events(tx).is_err());
        assert_eq!(result["event_amount_fields_used"], false);
        assert_eq!(result["executable_backtest_eligible"], false);
        for row in result["verified_swaps"].as_array().unwrap() {
            let quote: i128 = row["quote_cashflow_lamports"]
                .as_str()
                .unwrap()
                .parse()
                .unwrap();
            assert_eq!(quote < 0, row["is_buy"].as_bool().unwrap());
            assert_ne!(row["mint"], WSOL);
            if row["sol_is_pool_quote"] == false {
                assert_ne!(row["is_buy"], row["instruction_is_buy"]);
            }
            assert_eq!(row["reserve_quote_available"], false);
            assert!(row.get("reserve").is_none());
            seen.insert((
                row["venue"].to_string(),
                row["is_buy"].to_string(),
                row["sol_is_pool_quote"].to_string(),
            ));
        }
    }
    assert!(seen.len() >= 6);
}

#[test]
fn one_lamport_or_token_discrepancy_is_not_silently_accepted() {
    let mut tx = one("('pump_swap', True, True)");
    let original = reconcile::transaction(&tx).unwrap();
    let row = &original["verified_swaps"][0];
    let wallet = row["wallet"].clone();
    let mint = row["mint"].clone();
    let b = tx["meta"]["postTokenBalances"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|b| b["owner"] == wallet && b["mint"] == mint)
        .unwrap();
    let n: u64 = b["uiTokenAmount"]["amount"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    b["uiTokenAmount"]["amount"] = json!((n + 1).to_string());
    let result = reconcile::transaction(&tx).unwrap();
    assert!(result["verified_swaps"]
        .as_array()
        .unwrap()
        .iter()
        .all(|v| v["wallet"] != wallet));
    assert!(result["rejected_swaps"]
        .to_string()
        .contains("do not reconcile"));
}

#[test]
fn token_owner_mismatch_is_rejected() {
    let mut tx = one("('pump_swap', True, True)");
    let before = reconcile::transaction(&tx).unwrap();
    let row = &before["verified_swaps"][0];
    for name in ["preTokenBalances", "postTokenBalances"] {
        for b in tx["meta"][name].as_array_mut().unwrap() {
            if b["owner"] == row["wallet"] && b["mint"] == row["mint"] {
                b["owner"] = json!("11111111111111111111111111111111");
            }
        }
    }
    let result = reconcile::transaction(&tx).unwrap();
    assert!(result["rejected_swaps"]
        .to_string()
        .contains("owner/mint mismatch"));
}

#[test]
fn failed_caught_or_truncated_execution_and_missing_stack_are_rejected() {
    let original = one("('pump_swap', True, True)");
    for log in [
        "Program example failed: custom program error: 0x1",
        "Log truncated",
    ] {
        let mut tx = original.clone();
        tx["meta"]["logMessages"]
            .as_array_mut()
            .unwrap()
            .push(json!(log));
        assert!(reconcile::transaction(&tx).is_err());
    }
    let mut tx = original.clone();
    tx["meta"]["err"] = json!({"InstructionError":[0,"Custom"]});
    assert!(reconcile::transaction(&tx).is_err());
    let mut tx = original;
    tx["meta"]["innerInstructions"][0]["instructions"][0]
        .as_object_mut()
        .unwrap()
        .remove("stackHeight");
    assert!(reconcile::transaction(&tx).is_err());
}

#[test]
fn native_accounting_rejects_unknown_funding_even_when_total_lamports_balance() {
    let mut tx = one("('pump_fun', False, True)");
    let before = reconcile::transaction(&tx).unwrap();
    let row = &before["verified_swaps"][0];
    let k = keys(&tx);
    let user = k
        .iter()
        .position(|k| Some(k.as_str()) == row["wallet"].as_str())
        .unwrap();
    let program = k
        .iter()
        .position(|k| k == "11111111111111111111111111111111")
        .unwrap();
    let original = tx["meta"]["postBalances"][program].as_u64().unwrap();
    tx["meta"]["postBalances"][program] = json!(original - 1);
    tx["meta"]["postBalances"][user] =
        json!(tx["meta"]["postBalances"][user].as_u64().unwrap() + 1);
    let result = reconcile::transaction(&tx).unwrap();
    assert!(result["rejected_swaps"]
        .to_string()
        .contains("unattributed native"));
}

#[test]
fn unpublished_instruction_is_visible_as_unaccounted_identity() {
    let result = reconcile::transaction(&one("unmapped_instruction")).unwrap();
    assert_eq!(result["all_trade_identities_accounted_for"], false);
    assert_eq!(result["profit_rank_eligible"], false);
}

#[test]
fn event_payloads_cannot_supply_fallback_amounts() {
    let mut tx = one("('pump_swap', True, True)");
    let before = reconcile::transaction(&tx).unwrap();
    let k = keys(&tx);
    tx["meta"]["logMessages"]
        .as_array_mut()
        .unwrap()
        .retain(|v| !v.as_str().unwrap().starts_with("Program data: "));
    for g in tx["meta"]["innerInstructions"].as_array_mut().unwrap() {
        for ix in g["instructions"].as_array_mut().unwrap() {
            let program = &k[ix["programIdIndex"].as_u64().unwrap() as usize];
            let data = bs58::decode(ix["data"].as_str().unwrap())
                .into_vec()
                .unwrap();
            if (program == pump_copy::model::PUMP || program == pump_copy::model::AMM)
                && data.starts_with(&[228, 69, 165, 46, 81, 203, 154, 29])
            {
                ix["data"] = json!(bs58::encode([0u8; 8]).into_string());
            }
        }
    }
    let after = reconcile::transaction(&tx).unwrap();
    assert_eq!(before["verified_swaps"], after["verified_swaps"]);
    assert_eq!(after["all_trade_identities_accounted_for"], false);
}

#[test]
fn native_sale_keeps_network_fee_and_external_tip_separate() {
    let tx = one("('pump_fun', False, True)");
    let r = reconcile::transaction(&tx).unwrap();
    let s = &r["verified_swaps"][0];
    assert_eq!(s["quote_lamports"], "193565999");
    assert_eq!(s["external_native_cashflow_lamports"], "-100000");
    assert_eq!(
        s["wallet_balance_receipt"]["native_delta_lamports"],
        "193460600"
    );
    assert_eq!(
        s["wallet_balance_receipt"]["network_fee_paid_lamports"],
        5399
    );
}
