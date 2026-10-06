//! Pinned IDL decoding. Reject partial/unknown event tails rather than guess offsets.
use crate::model::{Batch, Reserve, Side, Trade, Venue, AMM, PUMP, WSOL};
use anyhow::{anyhow, bail, ensure, Context, Result};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use solana_pubkey::Pubkey;
use std::{collections::BTreeMap, str::FromStr};

const CPI_EVENT: [u8; 8] = [228, 69, 165, 46, 81, 203, 154, 29];
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolInfo {
    pub base_mint: String,
    pub quote_mint: String,
    pub creator: String,
    pub canonical: bool,
}
pub type Pools = BTreeMap<String, PoolInfo>;

pub fn pool_info(data: &[u8]) -> Result<PoolInfo> {
    ensure!(data.len() >= 107, "short Pool account");
    // Anchor account:Pool discriminator, followed by bump, index, creator, mints.
    ensure!(
        data[..8] == [241, 154, 109, 4, 17, 177, 109, 188],
        "wrong Pool discriminator"
    );
    let creator = bs58::encode(&data[11..43]).into_string();
    let base_mint = bs58::encode(&data[43..75]).into_string();
    let quote_mint = bs58::encode(&data[75..107]).into_string();
    let canonical = canonical_creator(&base_mint, &creator)?
        && u16::from_le_bytes(data[9..11].try_into()?) == 0;
    Ok(PoolInfo {
        base_mint,
        quote_mint,
        creator,
        canonical,
    })
}
fn canonical_creator(mint: &str, creator: &str) -> Result<bool> {
    let mint = Pubkey::from_str(mint)?;
    let program = Pubkey::from_str(PUMP)?;
    Ok(
        Pubkey::find_program_address(&[b"pool-authority", mint.as_ref()], &program)
            .0
            .to_string()
            == creator,
    )
}
#[derive(Clone, Debug)]
pub struct Event {
    pub program: String,
    pub name: String,
    pub fields: Value,
}
pub struct Decoder {
    pump: Value,
    amm: Value,
}
impl Default for Decoder {
    fn default() -> Self {
        Self {
            pump: serde_json::from_str(include_str!("../schema/pump.json")).expect("pinned schema"),
            amm: serde_json::from_str(include_str!("../schema/pump_amm.json"))
                .expect("pinned schema"),
        }
    }
}
struct Reader<'a> {
    data: &'a [u8],
    at: usize,
}
impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self.at.checked_add(n).context("length overflow")?;
        let bytes = self
            .data
            .get(self.at..end)
            .context("truncated event field")?;
        self.at = end;
        Ok(bytes)
    }
    fn value(&mut self, ty: &Value, schema: &Value) -> Result<Value> {
        Ok(match ty.as_str() {
            Some("u8") => json!(self.take(1)?[0]),
            Some("bool") => {
                let n = self.take(1)?[0];
                ensure!(n <= 1, "invalid bool");
                json!(n == 1)
            }
            Some("u16") => json!(u16::from_le_bytes(self.take(2)?.try_into()?)),
            Some("u32") => json!(u32::from_le_bytes(self.take(4)?.try_into()?)),
            Some("u64") => json!(u64::from_le_bytes(self.take(8)?.try_into()?)),
            Some("i64") => json!(i64::from_le_bytes(self.take(8)?.try_into()?)),
            Some("i128") => json!(i128::from_le_bytes(self.take(16)?.try_into()?).to_string()),
            Some("pubkey") => json!(bs58::encode(self.take(32)?).into_string()),
            Some("string") => {
                let n = u32::from_le_bytes(self.take(4)?.try_into()?) as usize;
                ensure!(n <= 4096, "oversized event string");
                json!(std::str::from_utf8(self.take(n)?)?)
            }
            None if ty.get("vec").is_some() => {
                let n = u32::from_le_bytes(self.take(4)?.try_into()?) as usize;
                ensure!(n <= 256, "oversized event vector");
                Value::Array(
                    (0..n)
                        .map(|_| self.value(&ty["vec"], schema))
                        .collect::<Result<_>>()?,
                )
            }
            None if ty.get("defined").is_some() => {
                let name = ty["defined"]["name"]
                    .as_str()
                    .context("missing defined name")?;
                let spec = schema["types"]
                    .as_array()
                    .context("missing types")?
                    .iter()
                    .find(|s| s["name"] == name)
                    .context("missing defined type")?;
                let mut fields = Map::new();
                for f in spec["type"]["fields"]
                    .as_array()
                    .context("missing fields")?
                {
                    fields.insert(
                        f["name"].as_str().context("missing field name")?.into(),
                        self.value(&f["type"], schema)?,
                    );
                }
                Value::Object(fields)
            }
            _ => bail!("unsupported IDL type: {ty}"),
        })
    }
}
impl Decoder {
    pub fn decode(&self, program: &str, bytes: &[u8]) -> Result<Option<Event>> {
        self.decode_with_capture_tail(program, bytes, false)
    }
    fn decode_with_capture_tail(
        &self,
        program: &str,
        bytes: &[u8],
        capture_only: bool,
    ) -> Result<Option<Event>> {
        let schema = match program {
            PUMP => &self.pump,
            AMM => &self.amm,
            _ => return Ok(None),
        };
        if bytes.len() < 8 {
            return Ok(None);
        }
        let Some(event) = schema["events"]
            .as_array()
            .context("missing events")?
            .iter()
            .find(|e| {
                e["discriminator"].as_array().is_some_and(|d| {
                    d.iter()
                        .map(|v| v.as_u64().unwrap_or(256))
                        .eq(bytes[..8].iter().map(|x| *x as u64))
                })
            })
        else {
            return Ok(None);
        };
        let name = event["name"].as_str().context("event name")?;
        let spec = schema["types"]
            .as_array()
            .context("types")?
            .iter()
            .find(|t| t["name"] == name)
            .context("event type")?;
        let mut r = Reader { data: bytes, at: 8 };
        let mut fields = Map::new();
        // Old append-only layouts are accepted only after the known core.
        let min_fields = match name {
            "TradeEvent" => 17,
            "BuyEvent" | "SellEvent" => 23,
            "CreatePoolEvent" => 26,
            _ => bail!("unsupported event"),
        };
        for (i, f) in spec["type"]["fields"]
            .as_array()
            .context("fields")?
            .iter()
            .enumerate()
        {
            if r.at == bytes.len() && i >= min_fields {
                break;
            }
            fields.insert(
                f["name"].as_str().context("field name")?.into(),
                r.value(&f["type"], schema)
                    .with_context(|| format!("decode {name} field {}", f["name"]))?,
            );
        }
        if capture_only {
            fields.insert("_unparsed_tail_bytes".into(), json!(bytes.len() - r.at));
        } else {
            ensure!(
                r.at == bytes.len(),
                "unknown trailing event fields; refresh pinned schema"
            );
        }
        Ok(Some(Event {
            program: program.into(),
            name: name.into(),
            fields: Value::Object(fields),
        }))
    }
    pub fn transaction_events(&self, tx: &Value) -> Result<Vec<Event>> {
        self.transaction_events_with_capture_tail(tx, false)
    }
    /// Identity-only routing for raw observation. No amounts/reserves escape this API.
    /// Unknown event tails stay unsupported by the strict trade decoder.
    pub fn capture_identity_events(&self, tx: &Value) -> Result<Vec<Event>> {
        Ok(self.transaction_events_with_capture_tail(tx, true)?.into_iter().map(|e| {
            let f = &e.fields;
            Event { program: e.program, name: e.name, fields: json!({
                "user":f["user"], "mint":f["mint"], "pool":f["pool"], "is_buy":f["is_buy"],
                "_capture_identity_only":true, "_unparsed_tail_bytes":f["_unparsed_tail_bytes"]
            }) }
        }).collect())
    }
    fn transaction_events_with_capture_tail(
        &self,
        tx: &Value,
        capture_only: bool,
    ) -> Result<Vec<Event>> {
        let meta = tx.get("meta").context("missing transaction metadata")?;
        if !meta
            .get("err")
            .context("missing transaction error state")?
            .is_null()
        {
            return Ok(vec![]);
        }
        let mut keys: Vec<&str> = tx
            .pointer("/transaction/message/accountKeys")
            .and_then(Value::as_array)
            .context("accountKeys")?
            .iter()
            .map(|k| k.as_str().context("use RPC encoding=json"))
            .collect::<Result<_>>()?;
        for path in ["/loadedAddresses/writable", "/loadedAddresses/readonly"] {
            if let Some(a) = meta.pointer(path).and_then(Value::as_array) {
                for k in a {
                    keys.push(k.as_str().context("loaded address")?);
                }
            }
        }
        if !keys.contains(&PUMP) && !keys.contains(&AMM) {
            return Ok(vec![]);
        }
        let mut cpi: Vec<(String, Vec<u8>, Event)> = vec![];
        if let Some(groups) = meta["innerInstructions"].as_array() {
            let mut groups: Vec<_> = groups.iter().collect();
            groups.sort_by_key(|g| g["index"].as_u64());
            for g in groups {
                for ix in g["instructions"].as_array().context("inner instructions")? {
                    let idx = ix["programIdIndex"]
                        .as_u64()
                        .context("inner program index")? as usize;
                    let program = *keys.get(idx).context("program index out of bounds")?;
                    if program != PUMP && program != AMM {
                        continue;
                    }
                    let data =
                        bs58::decode(ix["data"].as_str().context("inner data")?).into_vec()?;
                    if data.starts_with(&CPI_EVENT) {
                        if let Some(e) =
                            self.decode_with_capture_tail(program, &data[8..], capture_only)?
                        {
                            cpi.push((program.into(), data[8..].to_vec(), e));
                        }
                    }
                }
            }
        }
        let mut stack: Vec<&str> = vec![];
        let mut logs = vec![];
        let mut truncated = false;
        if let Some(lines) = meta["logMessages"].as_array() {
            for line in lines {
                let line = line.as_str().context("log line")?;
                if line.contains("Log truncated") {
                    truncated = true;
                }
                if let Some(rest) = line.strip_prefix("Program ") {
                    if let Some((program, _)) = rest.split_once(" invoke [") {
                        stack.push(program);
                    } else if rest.ends_with(" success") || rest.contains(" failed:") {
                        stack.pop();
                    }
                }
                if let Some(data) = line.strip_prefix("Program data: ") {
                    let program = stack.last().copied().unwrap_or("");
                    if program != PUMP && program != AMM {
                        continue;
                    }
                    let bytes = STANDARD.decode(data).context("invalid event base64")?;
                    if let Some(e) = self.decode_with_capture_tail(program, &bytes, capture_only)? {
                        logs.push((program.to_owned(), bytes, e));
                    }
                }
            }
        }
        if !cpi.is_empty() {
            // Do not double count emit! + emit_cpi! copies. Mixed disjoint
            // streams have no reliable ordering here; stop instead of losing trades.
            ensure!(
                logs.iter()
                    .all(|(p, b, _)| cpi.iter().any(|(cp, cb, _)| p == cp && b == cb)),
                "mixed event streams cannot be ordered safely"
            );
            Ok(cpi.into_iter().map(|(_, _, e)| e).collect())
        } else {
            ensure!(!truncated, "truncated logs without CPI events");
            ensure!(
                meta["logMessages"].is_array(),
                "missing logs and CPI events"
            );
            Ok(logs.into_iter().map(|(_, _, e)| e).collect())
        }
    }
    pub fn required_pools(&self, block: &Value, pools: &mut Pools) -> Result<Vec<String>> {
        let mut missing = std::collections::BTreeSet::new();
        for tx in block["transactions"]
            .as_array()
            .context("block transactions")?
        {
            for e in self.transaction_events(tx)? {
                if e.name == "CreatePoolEvent" {
                    let f = &e.fields;
                    let mint = string(f, "base_mint")?;
                    let creator = string(f, "creator")?;
                    pools.insert(
                        string(f, "pool")?,
                        PoolInfo {
                            canonical: canonical_creator(&mint, &creator)?
                                && number(f, "index")? == 0,
                            base_mint: mint,
                            quote_mint: string(f, "quote_mint")?,
                            creator,
                        },
                    );
                } else if e.program == AMM {
                    let pool = string(&e.fields, "pool")?;
                    if !pools.contains_key(&pool) {
                        missing.insert(pool);
                    }
                }
            }
        }
        Ok(missing
            .into_iter()
            .filter(|p| !pools.contains_key(p))
            .collect())
    }
    /// Shared event normalization for complete blocks and discovery-only log feeds.
    /// The boolean counts unsupported non-SOL quote events.
    pub fn trade_from_event(
        &self,
        e: Event,
        signature: &str,
        tx_index: u32,
        event_index: u32,
        network_fee: u64,
        pools: &Pools,
    ) -> Result<(Option<Trade>, bool)> {
        ensure!(
            e.fields.get("_capture_identity_only").is_none(),
            "capture-only identity cannot be used as a trade quote"
        );
        if e.name == "CreatePoolEvent" {
            return Ok((None, false));
        }
        let f = &e.fields;
        let (mint, pool, venue, side, tokens, quote, reserve) = if e.name == "TradeEvent" {
            let quote_mint = f["quote_mint"].as_str().unwrap_or(WSOL);
            if quote_mint != WSOL && quote_mint != "11111111111111111111111111111111" {
                return Ok((None, true));
            }
            let side = if f["is_buy"].as_bool().context("buy flag")? {
                Side::Buy
            } else {
                Side::Sell
            };
            let gross = nonzero_or(f, "quote_amount", "sol_amount")?;
            // Buyback/cashback/holder-reward fields describe allocations
            // of these fees, not extra fees charged on top. In particular
            // buyback_fee_basis_points is a share of protocol fees.
            let fee = sum(f, &["fee", "creator_fee"])?;
            let quote = if side == Side::Buy {
                gross.checked_add(fee)
            } else {
                gross.checked_sub(fee)
            }
            .context("trade quote arithmetic")?;
            let mint = string(f, "mint")?;
            let reserve = Reserve {
                base: number(f, "virtual_token_reserves")?,
                quote: nonzero_or(f, "virtual_quote_reserves", "virtual_sol_reserves")?,
                real_base: number(f, "real_token_reserves")?,
                real_quote: nonzero_or(f, "real_quote_reserves", "real_sol_reserves")?,
                fee_bps: u32::try_from(sum(f, &["fee_basis_points", "creator_fee_basis_points"])?)?,
            };
            (
                mint.clone(),
                format!("curve:{mint}"),
                Venue::PumpFun,
                side,
                number(f, "token_amount")?,
                quote,
                reserve,
            )
        } else {
            let pool = string(f, "pool")?;
            let info = pools
                .get(&pool)
                .with_context(|| format!("missing pool metadata {pool}"))?;
            if info.quote_mint != WSOL {
                return Ok((None, true));
            }
            if !info.canonical {
                return Ok((None, false));
            }
            let buy = e.name == "BuyEvent";
            let tokens = number(
                f,
                if buy {
                    "base_amount_out"
                } else {
                    "base_amount_in"
                },
            )?;
            let base = number(f, "pool_base_token_reserves")?;
            let cash = number(f, "pool_quote_token_reserves")?;
            let delta = number(
                f,
                if buy {
                    "quote_amount_in_with_lp_fee"
                } else {
                    "quote_amount_out_without_lp_fee"
                },
            )?;
            let post_base = if buy {
                base.checked_sub(tokens)
            } else {
                base.checked_add(tokens)
            }
            .context("base reserves overflow")?;
            let post_cash = if buy {
                cash.checked_add(delta)
            } else {
                cash.checked_sub(delta)
            }
            .context("quote reserves overflow")?;
            let virtual_quote = f["virtual_quote_reserves"]
                .as_str()
                .unwrap_or("0")
                .parse::<i128>()?;
            let effective = u64::try_from(post_cash as i128 + virtual_quote)
                .context("effective quote out of range")?;
            let reserve = Reserve {
                base: post_base,
                quote: effective,
                real_base: post_base,
                real_quote: post_cash,
                fee_bps: u32::try_from(sum(
                    f,
                    &[
                        "lp_fee_basis_points",
                        "protocol_fee_basis_points",
                        "coin_creator_fee_basis_points",
                    ],
                )?)?,
            };
            (
                info.base_mint.clone(),
                pool,
                Venue::PumpSwap,
                if buy { Side::Buy } else { Side::Sell },
                tokens,
                number(
                    f,
                    if buy {
                        "user_quote_amount_in"
                    } else {
                        "user_quote_amount_out"
                    },
                )?,
                reserve,
            )
        };
        Ok((
            Some(Trade {
                signature: signature.into(),
                tx_index,
                event_index,
                mint,
                wallet: string(f, "user")?,
                pool,
                venue,
                side,
                tokens,
                quote_lamports: quote,
                network_fee_lamports: network_fee,
                reserve,
            }),
            false,
        ))
    }
    pub fn block(&self, slot: u64, block: &Value, pools: &Pools) -> Result<Batch> {
        let mut trades = vec![];
        let mut skipped = 0;
        for (tx_index, tx) in block["transactions"]
            .as_array()
            .context("block transactions")?
            .iter()
            .enumerate()
        {
            let events = self.transaction_events(tx)?;
            if events.is_empty() {
                continue;
            }
            let signature = tx
                .pointer("/transaction/signatures/0")
                .and_then(Value::as_str)
                .context("signature")?;
            let network_fee = tx
                .pointer("/meta/fee")
                .and_then(Value::as_u64)
                .context("network fee")?;
            for (event_index, e) in events.into_iter().enumerate() {
                let (trade, non_sol) = self.trade_from_event(
                    e,
                    signature,
                    u32::try_from(tx_index)?,
                    u32::try_from(event_index)?,
                    network_fee,
                    pools,
                )?;
                skipped += u64::from(non_sol);
                if let Some(trade) = trade {
                    trades.push(trade);
                }
            }
        }
        let result = Batch {
            schema_version: 1,
            slot,
            block_time_ms: block["blockTime"]
                .as_u64()
                .context("missing blockTime")?
                .checked_mul(1000)
                .context("block time overflow")?,
            blockhash: block["blockhash"].as_str().context("blockhash")?.into(),
            trades,
            skipped_non_sol: skipped,
        };
        result.validate()?;
        Ok(result)
    }
}
fn string(f: &Value, key: &str) -> Result<String> {
    Ok(f[key]
        .as_str()
        .with_context(|| format!("missing {key}"))?
        .into())
}
fn number(f: &Value, key: &str) -> Result<u64> {
    f[key]
        .as_u64()
        .with_context(|| format!("missing integer {key}"))
}
fn sum(f: &Value, keys: &[&str]) -> Result<u64> {
    keys.iter().try_fold(0u64, |a, k| {
        a.checked_add(f[*k].as_u64().unwrap_or(0))
            .ok_or_else(|| anyhow!("fee overflow"))
    })
}
fn nonzero_or(f: &Value, new: &str, old: &str) -> Result<u64> {
    if let Some(n) = f[new].as_u64().filter(|n| *n > 0) {
        Ok(n)
    } else {
        number(f, old)
    }
}
