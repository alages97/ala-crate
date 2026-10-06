//! Transaction balance evidence independent of Pump event serialization.
//! These are wallet asset changes, never inferred swaps, cost basis or P&L.
use crate::model::WSOL;
use anyhow::{ensure, Context, Result};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone)]
struct TokenBalance {
    owner: String,
    mint: String,
    program: String,
    amount: u64,
    decimals: u64,
}
fn token_balances(
    meta: &Value,
    name: &str,
    key_count: usize,
) -> Result<BTreeMap<usize, TokenBalance>> {
    let mut result = BTreeMap::new();
    for row in meta[name]
        .as_array()
        .with_context(|| format!("missing {name}"))?
    {
        let index = usize::try_from(
            row["accountIndex"]
                .as_u64()
                .context("token account index")?,
        )?;
        ensure!(index < key_count, "token account index out of range");
        let balance = TokenBalance {
            owner: row["owner"]
                .as_str()
                .context("missing token owner; attribution incomplete")?
                .into(),
            mint: row["mint"].as_str().context("missing token mint")?.into(),
            program: row["programId"]
                .as_str()
                .context("missing token program")?
                .into(),
            amount: row["uiTokenAmount"]["amount"]
                .as_str()
                .context("missing raw token amount")?
                .parse()?,
            decimals: row["uiTokenAmount"]["decimals"]
                .as_u64()
                .context("token decimals")?,
        };
        ensure!(
            result.insert(index, balance).is_none(),
            "duplicate token balance account index"
        );
    }
    Ok(result)
}

pub fn receipt(tx: &Value, wallet: &str) -> Result<Value> {
    let message = &tx["transaction"]["message"];
    let meta = tx
        .get("meta")
        .filter(|v| v.is_object())
        .context("full transaction metadata required")?;
    let failed = !meta
        .get("err")
        .context("transaction error state required")?
        .is_null();
    let mut keys: Vec<&str> = message["accountKeys"]
        .as_array()
        .context("RPC encoding=json account keys required")?
        .iter()
        .map(|k| k.as_str().context("unparsed account key required"))
        .collect::<Result<_>>()?;
    ensure!(!keys.is_empty(), "empty account keys");
    let has_lookups = message["addressTableLookups"]
        .as_array()
        .is_some_and(|a| !a.is_empty());
    ensure!(
        !has_lookups || meta["loadedAddresses"].is_object(),
        "missing loaded addresses"
    );
    if let Some(loaded) = meta.get("loadedAddresses") {
        for name in ["writable", "readonly"] {
            for key in loaded[name].as_array().context("loaded address list")? {
                keys.push(key.as_str().context("loaded account key")?);
            }
        }
    }
    ensure!(
        keys.iter().collect::<BTreeSet<_>>().len() == keys.len(),
        "duplicate account key"
    );
    let pre = meta["preBalances"]
        .as_array()
        .context("pre lamport balances")?;
    let post = meta["postBalances"]
        .as_array()
        .context("post lamport balances")?;
    ensure!(
        pre.len() == keys.len() && post.len() == keys.len(),
        "account/balance lengths differ"
    );
    let pre: Vec<u64> = pre
        .iter()
        .map(|v| v.as_u64().context("invalid pre lamports"))
        .collect::<Result<_>>()?;
    let post: Vec<u64> = post
        .iter()
        .map(|v| v.as_u64().context("invalid post lamports"))
        .collect::<Result<_>>()?;
    let before = token_balances(meta, "preTokenBalances", keys.len())?;
    let after = token_balances(meta, "postTokenBalances", keys.len())?;
    let mut deltas: BTreeMap<String, i128> = BTreeMap::new();
    let mut decimals: BTreeMap<String, u64> = BTreeMap::new();
    let mut owned_pre = 0i128;
    let mut owned_post = 0i128;
    let mut warnings = BTreeSet::new();
    for (&index, balance) in &before {
        if let Some(other) = after.get(&index) {
            if balance.owner != other.owner {
                warnings.insert("token_account_owner_changed");
            }
            if balance.mint != other.mint
                || balance.program != other.program
                || balance.decimals != other.decimals
            {
                warnings.insert("token_account_identity_changed");
            }
        } else if post[index] != 0 {
            warnings.insert("missing_post_token_metadata_for_existing_account");
        }
    }
    for &index in after.keys() {
        if !before.contains_key(&index) && pre[index] != 0 {
            warnings.insert("missing_pre_token_metadata_for_existing_account");
        }
    }
    for (balances, sign) in [(&before, -1i128), (&after, 1i128)] {
        for (&index, balance) in balances {
            if balance.owner != wallet {
                continue;
            }
            if let Some(old) = decimals.insert(balance.mint.clone(), balance.decimals) {
                ensure!(old == balance.decimals, "inconsistent token decimals");
            }
            *deltas.entry(balance.mint.clone()).or_default() += sign * balance.amount as i128;
            if sign < 0 {
                owned_pre += pre[index] as i128;
            } else {
                owned_post += post[index] as i128;
            }
        }
    }
    let native = keys
        .iter()
        .position(|k| *k == wallet)
        .map(|i| post[i] as i128 - pre[i] as i128)
        .unwrap_or(0);
    let fee = meta["fee"].as_u64().context("network fee")?;
    let fee_paid = if keys[0] == wallet { fee } else { 0 };
    let wsol = deltas.get(WSOL).copied().unwrap_or(0);
    let token_rows:Vec<_>=deltas.iter().filter(|(_,amount)|**amount!=0).map(|(mint,amount)|json!({"mint":mint,"raw_delta":amount.to_string(),"decimals":decimals[mint]})).collect();
    Ok(
        json!({"signature":tx["transaction"]["signatures"][0],"slot":tx["slot"],"wallet":wallet,"failed":failed,
        "network_fee_paid_lamports":fee_paid,"native_delta_lamports":native.to_string(),"wsol_token_delta_lamports":wsol.to_string(),
        "native_plus_wsol_delta_lamports":(native+wsol).to_string(),
        "native_and_owned_token_account_lamports_delta":(native+owned_post-owned_pre).to_string(),
        "owned_token_account_lamports_note":"Includes rent deposits and WSOL backing; WSOL token units must not be added again. Other owned account types are outside this measure.",
        "token_deltas":token_rows,"metadata_complete":warnings.is_empty(),"warnings":warnings,
        "evidence":"RPC pre/post balances; no Pump event amount or reserve fields used",
        "profit_lamports":null,"eligible_for_profit_ranking":false,
        "limitation":"Net asset changes can include funding, transfers, rent, multiple swaps and other venues. They do not identify trade-level amounts, initial cost basis, execution prices or strategy returns."}),
    )
}
