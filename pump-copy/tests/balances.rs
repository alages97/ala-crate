use pump_copy::{
    balances::receipt,
    decoder::{Decoder, Pools},
    history::{audit_transactions, auditable_pools},
    model::WSOL,
};
use serde_json::{json, Value};

fn simple() -> Value {
    json!({"slot":1,"transaction":{"signatures":["s"],"message":{"accountKeys":["payer","wallet","ata","loaded-ata"],"header":{"numRequiredSignatures":1}}},"meta":{"err":null,"fee":5000,"preBalances":[10000,20000000,0,0],"postBalances":[5000,20000000,0,0],"preTokenBalances":[],"postTokenBalances":[]}})
}
fn token(index: u64, owner: &str, mint: &str, amount: &str, decimals: u8) -> Value {
    json!({"accountIndex":index,"owner":owner,"mint":mint,"programId":"token-program","uiTokenAmount":{"amount":amount,"decimals":decimals}})
}
#[test]
fn wrapping_sol_is_neutral_except_fee_and_rent_is_explicit() {
    let mut tx = simple();
    tx["transaction"]["message"]["accountKeys"][0] = json!("wallet");
    tx["transaction"]["message"]["accountKeys"][1] = json!("other");
    tx["meta"]["preBalances"] = json!([10000000, 0, 0, 0]);
    tx["meta"]["postBalances"] = json!([6955720, 0, 3039280, 0]);
    tx["meta"]["postTokenBalances"] = json!([token(2, "wallet", WSOL, "1000000", 9)]);
    let r = receipt(&tx, "wallet").unwrap();
    assert_eq!(r["native_plus_wsol_delta_lamports"], "-2044280");
    assert_eq!(r["native_and_owned_token_account_lamports_delta"], "-5000");
    assert_eq!(r["network_fee_paid_lamports"], 5000);
    assert!(r["profit_lamports"].is_null());
    assert_eq!(r["eligible_for_profit_ranking"], false);
    // Closing the token account returns its rent and WSOL backing, not trading profit.
    let pre = tx["meta"]["postBalances"].clone();
    tx["meta"]["preBalances"] = pre;
    tx["meta"]["postBalances"] = json!([9990000, 0, 0, 0]);
    tx["meta"]["preTokenBalances"] = tx["meta"]["postTokenBalances"].clone();
    tx["meta"]["postTokenBalances"] = json!([]);
    assert_eq!(
        receipt(&tx, "wallet").unwrap()["native_and_owned_token_account_lamports_delta"],
        "-5000"
    );
}
#[test]
fn loaded_addresses_and_funding_are_not_guessed_as_swaps() {
    let mut tx = simple();
    tx["transaction"]["message"]["accountKeys"] = json!(["payer", "wallet", "ata"]);
    tx["transaction"]["message"]["addressTableLookups"] = json!([{"accountKey":"table"}]);
    tx["meta"]["loadedAddresses"] = json!({"writable":["loaded-ata"],"readonly":[]});
    tx["meta"]["postTokenBalances"] =
        json!([token(3, "wallet", "mint", "18446744073709551615", 6)]);
    let r = receipt(&tx, "wallet").unwrap();
    assert_eq!(r["network_fee_paid_lamports"], 0);
    assert_eq!(r["token_deltas"][0]["raw_delta"], "18446744073709551615");
    assert_eq!(r["eligible_for_profit_ranking"], false);
    tx["meta"]
        .as_object_mut()
        .unwrap()
        .remove("loadedAddresses");
    assert!(receipt(&tx, "wallet").is_err());
}
#[test]
fn incomplete_or_ambiguous_metadata_cannot_be_accepted_silently() {
    let mut tx = simple();
    tx["meta"]["preTokenBalances"] = json!([token(2, "wallet", "mint", "1", 6)]);
    tx["meta"]["postTokenBalances"] = json!([token(2, "new-owner", "mint", "1", 6)]);
    assert_eq!(receipt(&tx, "wallet").unwrap()["metadata_complete"], false);
    tx["meta"]["preTokenBalances"][0]
        .as_object_mut()
        .unwrap()
        .remove("owner");
    assert!(receipt(&tx, "wallet").is_err());
    let mut tx = simple();
    tx["meta"]["postTokenBalances"] = json!([
        token(2, "wallet", "mint", "1", 6),
        token(2, "wallet", "mint", "1", 6)
    ]);
    assert!(receipt(&tx, "wallet").is_err());
    tx["meta"]["postTokenBalances"] = json!([token(99, "wallet", "mint", "1", 6)]);
    assert!(receipt(&tx, "wallet").is_err());
    let mut tx = simple();
    tx["meta"]["postBalances"] = json!([0]);
    assert!(receipt(&tx, "wallet").is_err());
}
#[test]
fn saved_current_transactions_keep_balance_evidence_without_accepting_unknown_events() {
    let txs: Vec<Value> =
        serde_json::from_str(include_str!("fixtures/current-transactions.json")).unwrap();
    let d = Decoder::default();
    let mut pools = Pools::new();
    let (_, errors) = auditable_pools(&d, &txs, &mut pools);
    assert_eq!(errors.len(), 4);
    for tx in &txs {
        assert!(d.transaction_events(tx).is_err());
        for id in d.capture_identity_events(tx).unwrap() {
            let Some(user) = id.fields["user"].as_str() else {
                continue;
            };
            let r = receipt(tx, user).unwrap();
            assert_eq!(
                r["evidence"],
                "RPC pre/post balances; no Pump event amount or reserve fields used"
            );
            assert_eq!(r["eligible_for_profit_ranking"], false);
            let audit = audit_transactions(user, &[tx.clone()], &d, &pools).unwrap();
            assert_eq!(audit["unparsed_transactions"], 1);
            assert_eq!(audit["swap_accounting_complete"], false);
            assert!(
                audit["cashflow_after_all_network_fees_and_extra_swap_outflows_lamports"].is_null()
            );
            assert!(audit["transactions_detail"][0]["swap_cashflow_lamports"].is_null());
            assert_eq!(audit["transactions_detail"][0]["balance_receipt"], r);
        }
    }
}
#[test]
fn failed_fee_is_only_assigned_to_actual_payer() {
    let mut tx = simple();
    tx["meta"]["err"] = json!({"InstructionError":[0,"x"]});
    assert_eq!(
        receipt(&tx, "payer").unwrap()["network_fee_paid_lamports"],
        5000
    );
    assert_eq!(
        receipt(&tx, "wallet").unwrap()["network_fee_paid_lamports"],
        0
    );
}
