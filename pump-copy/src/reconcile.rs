//! Executed-transfer accounting independent of Pump event amount/reserve fields.
//! This emits cash-flow evidence, never synthetic reserves or executable quotes.
use crate::{
    balances,
    decoder::Decoder,
    model::{AMM, PUMP, WSOL},
};
use anyhow::{bail, ensure, Context, Result};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};

const TOKEN: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const TOKEN22: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";
const SYSTEM: &str = "11111111111111111111111111111111";
const BUY: [u8; 8] = [102, 6, 61, 18, 1, 218, 235, 234];
const SELL: [u8; 8] = [51, 230, 133, 164, 1, 127, 131, 173];
const PUMP_EXACT: [u8; 8] = [56, 252, 116, 8, 158, 223, 205, 95];
const AMM_EXACT: [u8; 8] = [198, 46, 21, 82, 180, 217, 232, 112];
const PUMP_BUY_V2: [u8; 8] = [184, 23, 238, 97, 103, 197, 211, 61];
const PUMP_SELL_V2: [u8; 8] = [93, 246, 130, 60, 231, 233, 64, 178];
const PUMP_EXACT_V2: [u8; 8] = [194, 171, 28, 70, 104, 77, 91, 47];
fn pump_v2(n: &Node) -> bool {
    n.program == PUMP
        && [PUMP_BUY_V2, PUMP_SELL_V2, PUMP_EXACT_V2]
            .iter()
            .any(|d| n.data.starts_with(d))
}
fn user_role(n: &Node) -> usize {
    if n.program == AMM {
        1
    } else if pump_v2(n) {
        13
    } else {
        6
    }
}

#[derive(Clone)]
struct Node {
    program: String,
    accounts: Vec<usize>,
    data: Vec<u8>,
    parent: Option<usize>,
    outer: usize,
}
#[derive(Clone)]
struct Token {
    mint: String,
    owner: String,
    program: String,
    decimals: Option<u64>,
    pre: u64,
    post: u64,
    has_pre: bool,
    has_post: bool,
    initialized: bool,
}
#[derive(Clone)]
struct Transfer {
    node: usize,
    source: usize,
    destination: usize,
    amount: u64,
    mint: String,
}
struct NativeMove {
    node: usize,
    source: usize,
    destination: usize,
    amount: i128,
}
struct ContextTx {
    keys: Vec<String>,
    nodes: Vec<Node>,
    tokens: BTreeMap<usize, Token>,
    transfers: Vec<Transfer>,
    token_delta: BTreeMap<usize, i128>,
    pre: Vec<u64>,
    post: Vec<u64>,
    unstable: BTreeSet<usize>,
}

fn index(v: &Value) -> Result<usize> {
    usize::try_from(v.as_u64().context("missing account index")?).context("account index overflow")
}
fn amount(data: &[u8], at: usize) -> Result<u64> {
    Ok(u64::from_le_bytes(
        data.get(at..at + 8)
            .context("short instruction amount")?
            .try_into()?,
    ))
}
fn account(n: &Node, at: usize) -> Result<usize> {
    n.accounts
        .get(at)
        .copied()
        .context("missing instruction account")
}
fn node(v: &Value, keys: &[String], parent: Option<usize>, outer: usize) -> Result<Node> {
    let program = keys
        .get(index(&v["programIdIndex"])?)
        .context("program index out of bounds")?
        .clone();
    let accounts = v["accounts"]
        .as_array()
        .context("compiled accounts required")?
        .iter()
        .map(index)
        .collect::<Result<Vec<_>>>()?;
    ensure!(
        accounts.iter().all(|x| *x < keys.len()),
        "instruction account out of bounds"
    );
    Ok(Node {
        program,
        accounts,
        data: bs58::decode(
            v["data"]
                .as_str()
                .context("compiled instruction data required")?,
        )
        .into_vec()?,
        parent,
        outer,
    })
}
fn swap(n: &Node) -> Option<bool> {
    if n.program != AMM && n.program != PUMP {
        return None;
    }
    if n.data.starts_with(&SELL) || (n.program == PUMP && n.data.starts_with(&PUMP_SELL_V2)) {
        Some(false)
    } else if n.data.starts_with(&BUY)
        || (n.program == AMM && n.data.starts_with(&AMM_EXACT))
        || (n.program == PUMP
            && (n.data.starts_with(&PUMP_EXACT)
                || n.data.starts_with(&PUMP_BUY_V2)
                || n.data.starts_with(&PUMP_EXACT_V2)))
    {
        Some(true)
    } else {
        None
    }
}
impl ContextTx {
    fn parse(tx: &Value) -> Result<Self> {
        let logs = tx["meta"]["logMessages"]
            .as_array()
            .context("complete execution logs required")?;
        ensure!(
            logs.iter().all(|v| v
                .as_str()
                .is_some_and(|s| !s.contains("Log truncated") && !s.contains(" failed:"))),
            "truncated logs or caught inner failure prevent execution validation"
        );
        // Validate full balance vectors, loaded keys, ownership metadata and fee.
        let first = tx
            .pointer("/transaction/message/accountKeys/0")
            .and_then(Value::as_str)
            .context("full compiled transaction required")?;
        balances::receipt(tx, first)?;
        let mut keys: Vec<String> = tx["transaction"]["message"]["accountKeys"]
            .as_array()
            .context("keys")?
            .iter()
            .map(|v| v.as_str().map(str::to_owned).context("key"))
            .collect::<Result<_>>()?;
        for side in ["writable", "readonly"] {
            if let Some(a) = tx["meta"]["loadedAddresses"][side].as_array() {
                for v in a {
                    keys.push(v.as_str().context("loaded key")?.into());
                }
            }
        }
        let mut groups = BTreeMap::new();
        for g in tx["meta"]["innerInstructions"]
            .as_array()
            .context("inner instruction metadata required")?
        {
            ensure!(
                groups.insert(index(&g["index"])?, g).is_none(),
                "duplicate inner instruction group"
            );
        }
        let outer = tx["transaction"]["message"]["instructions"]
            .as_array()
            .context("outer instructions")?;
        ensure!(
            groups.keys().all(|i| *i < outer.len()),
            "inner group outside transaction"
        );
        let mut nodes = vec![];
        for (outer_index, ix) in outer.iter().enumerate() {
            let root = nodes.len();
            nodes.push(node(ix, &keys, None, outer_index)?);
            let mut stack = vec![root];
            if let Some(g) = groups.get(&outer_index) {
                for ix in g["instructions"].as_array().context("inner instructions")? {
                    let height = index(&ix["stackHeight"])?;
                    ensure!(
                        height >= 2 && height <= stack.len() + 1,
                        "missing or inconsistent CPI stack height"
                    );
                    stack.truncate(height - 1);
                    let parent = stack.last().copied();
                    let next = nodes.len();
                    nodes.push(node(ix, &keys, parent, outer_index)?);
                    stack.push(next);
                }
            }
        }
        let pre = tx["meta"]["preBalances"]
            .as_array()
            .context("pre balances")?
            .iter()
            .map(|v| v.as_u64().context("pre balance"))
            .collect::<Result<Vec<_>>>()?;
        let post = tx["meta"]["postBalances"]
            .as_array()
            .context("post balances")?
            .iter()
            .map(|v| v.as_u64().context("post balance"))
            .collect::<Result<Vec<_>>>()?;
        let mut tokens: BTreeMap<usize, Token> = BTreeMap::new();
        let mut unstable = BTreeSet::new();
        for (name, before) in [("preTokenBalances", true), ("postTokenBalances", false)] {
            for b in tx["meta"][name].as_array().context("token metadata")? {
                let i = index(&b["accountIndex"])?;
                let mint = b["mint"].as_str().context("mint")?;
                let owner = b["owner"].as_str().context("owner")?;
                let program = b["programId"].as_str().context("token program")?;
                let decimals = b["uiTokenAmount"]["decimals"]
                    .as_u64()
                    .context("decimals")?;
                let raw = b["uiTokenAmount"]["amount"]
                    .as_str()
                    .context("raw amount")?
                    .parse::<u64>()?;
                let t = tokens.entry(i).or_insert(Token {
                    mint: mint.into(),
                    owner: owner.into(),
                    program: program.into(),
                    decimals: Some(decimals),
                    pre: 0,
                    post: 0,
                    has_pre: false,
                    has_post: false,
                    initialized: false,
                });
                if t.mint != mint
                    || t.owner != owner
                    || t.program != program
                    || t.decimals != Some(decimals)
                {
                    unstable.insert(i);
                }
                if before {
                    t.pre = raw;
                    t.has_pre = true;
                } else {
                    t.post = raw;
                    t.has_post = true;
                }
            }
        }
        // Retain ownership of accounts opened and closed inside this transaction.
        for n in &nodes {
            if n.program != TOKEN && n.program != TOKEN22 {
                continue;
            }
            let Some(op) = n.data.first() else {
                continue;
            };
            if [1, 16, 18].contains(op) {
                let i = account(n, 0)?;
                let mint = keys[account(n, 1)?].clone();
                let owner = if *op == 1 {
                    keys[account(n, 2)?].clone()
                } else {
                    bs58::encode(n.data.get(1..33).context("short initialize owner")?).into_string()
                };
                let t = tokens.entry(i).or_insert(Token {
                    mint: mint.clone(),
                    owner: owner.clone(),
                    program: n.program.clone(),
                    decimals: None,
                    pre: 0,
                    post: 0,
                    has_pre: false,
                    has_post: false,
                    initialized: true,
                });
                if t.mint != mint || t.owner != owner || t.program != n.program || t.has_pre {
                    unstable.insert(i);
                }
                t.initialized = true;
            }
            if *op == 6 {
                unstable.insert(account(n, 0)?);
            } // SetAuthority.
        }
        let mut transfers = vec![];
        let mut token_delta: BTreeMap<usize, i128> = BTreeMap::new();
        for (j, n) in nodes.iter().enumerate() {
            if n.program != TOKEN && n.program != TOKEN22 {
                continue;
            }
            match n.data.first().copied() {
                Some(op @ (3 | 12)) => {
                    ensure!(
                        n.data.len() == if op == 3 { 9 } else { 10 },
                        "unsupported token transfer encoding"
                    );
                    let source = account(n, 0)?;
                    let destination = account(n, if op == 3 { 1 } else { 2 })?;
                    let raw = amount(&n.data, 1)?;
                    let source_info = tokens
                        .get(&source)
                        .context("source token identity unavailable")?;
                    let dest_info = tokens
                        .get(&destination)
                        .context("destination token identity unavailable")?;
                    ensure!(
                        source_info.mint == dest_info.mint
                            && source_info.program == n.program
                            && dest_info.program == n.program,
                        "transfer token identity mismatch"
                    );
                    if op == 12 {
                        ensure!(
                            keys[account(n, 1)?] == source_info.mint
                                && source_info.decimals.is_none_or(|d| d == n.data[9] as u64)
                                && dest_info.decimals.is_none_or(|d| d == n.data[9] as u64),
                            "checked transfer mint/decimals mismatch"
                        );
                    }
                    *token_delta.entry(source).or_default() -= raw as i128;
                    *token_delta.entry(destination).or_default() += raw as i128;
                    transfers.push(Transfer {
                        node: j,
                        source,
                        destination,
                        amount: raw,
                        mint: source_info.mint.clone(),
                    });
                }
                Some(op @ (7 | 14 | 8 | 15)) => {
                    let i = account(n, if matches!(op, 7 | 14) { 1 } else { 0 })?;
                    *token_delta.entry(i).or_default() += if matches!(op, 7 | 14) {
                        amount(&n.data, 1)? as i128
                    } else {
                        -(amount(&n.data, 1)? as i128)
                    };
                }
                Some(26) => {
                    for i in &n.accounts {
                        unstable.insert(*i);
                    }
                } // Transfer-fee extension requires its own decoder.
                _ => {}
            }
        }
        Ok(Self {
            keys,
            nodes,
            tokens,
            transfers,
            token_delta,
            pre,
            post,
            unstable,
        })
    }
    fn within(&self, mut n: usize, ancestor: usize) -> bool {
        while let Some(parent) = self.nodes[n].parent {
            if parent == ancestor {
                return true;
            }
            n = parent;
        }
        false
    }
    fn verify_token(&self, i: usize) -> Result<&Token> {
        let t = self.tokens.get(&i).context("missing token identity")?;
        ensure!(
            !self.unstable.contains(&i),
            "unstable token identity or unsupported extension"
        );
        ensure!(
            t.has_pre || (self.pre[i] == 0 && t.initialized),
            "unverified initial token balance"
        );
        ensure!(
            t.has_post || self.post[i] == 0,
            "unverified final token balance"
        );
        ensure!(
            t.post as i128 - t.pre as i128 == self.token_delta.get(&i).copied().unwrap_or(0),
            "executed token transfers do not reconcile to balances"
        );
        Ok(t)
    }
    fn owner(&self, i: usize, wallet: &str, mint: &str) -> Result<()> {
        let t = self
            .tokens
            .get(&i)
            .context("token account owner unavailable")?;
        ensure!(
            t.owner == wallet && t.mint == mint && !self.unstable.contains(&i),
            "trade token account owner/mint mismatch"
        );
        Ok(())
    }
    fn native_moves(&self, fee: u64) -> Result<(Vec<NativeMove>, Vec<i128>)> {
        let mut projected: Vec<i128> = self.pre.iter().map(|v| *v as i128).collect();
        projected[0] -= fee as i128;
        let mut movements = vec![];
        for (j, n) in self.nodes.iter().enumerate() {
            let movement = if n.program == SYSTEM {
                let tag = u32::from_le_bytes(
                    n.data
                        .get(..4)
                        .context("short system instruction")?
                        .try_into()?,
                );
                match tag {
                    0 | 2 | 3 | 5 | 11 => {
                        let offset = if tag == 3 {
                            let len = usize::try_from(amount(&n.data, 36)?)?;
                            ensure!(
                                len <= 32 && n.data.len() == 92 + len,
                                "invalid seeded creation"
                            );
                            44 + len
                        } else {
                            4
                        };
                        Some((
                            account(n, 0)?,
                            account(n, if tag == 11 { 2 } else { 1 })?,
                            amount(&n.data, offset)? as i128,
                        ))
                    }
                    1 | 4 | 6 | 7 | 8 | 9 | 10 | 12 => None,
                    _ => bail!("unsupported system instruction in native ledger"),
                }
            } else if n.program == TOKEN || n.program == TOKEN22 {
                if n.data.first() == Some(&9) {
                    let source = account(n, 0)?;
                    ensure!(
                        self.tokens.contains_key(&source),
                        "close account ownership unavailable"
                    );
                    ensure!(
                        projected[source] >= 0,
                        "unknown funding before token account closure"
                    );
                    Some((source, account(n, 1)?, projected[source]))
                } else {
                    self.transfers
                        .iter()
                        .find(|t| t.node == j && t.mint == WSOL)
                        .map(|t| (t.source, t.destination, t.amount as i128))
                }
            } else {
                None
            };
            if let Some((source, destination, value)) = movement {
                projected[source] -= value;
                projected[destination] += value;
                movements.push(NativeMove {
                    node: j,
                    source,
                    destination,
                    amount: value,
                });
            }
        }
        let residual = self
            .post
            .iter()
            .zip(projected)
            .map(|(actual, known)| *actual as i128 - known)
            .collect();
        Ok((movements, residual))
    }
    fn amm(&self, at: usize, buy: bool) -> Result<Value> {
        self.no_internal_mint_burn(at)?;
        let n = &self.nodes[at];
        ensure!(n.data.len() >= 24, "short swap arguments");
        let pool = account(n, 0)?;
        let user = account(n, 1)?;
        let mint = account(n, 3)?;
        let quote_mint = account(n, 4)?;
        let ub = account(n, 5)?;
        let uq = account(n, 6)?;
        let pb = account(n, 7)?;
        let pq = account(n, 8)?;
        let quote_is_sol = self.keys[quote_mint] == WSOL;
        ensure!(
            quote_is_sol != (self.keys[mint] == WSOL),
            "swap must contain exactly one SOL asset"
        );
        self.owner(ub, &self.keys[user], &self.keys[mint])?;
        self.owner(uq, &self.keys[user], &self.keys[quote_mint])?;
        self.owner(pb, &self.keys[pool], &self.keys[mint])?;
        self.owner(pq, &self.keys[pool], &self.keys[quote_mint])?;
        for i in [if quote_is_sol { ub } else { uq }, pb, pq] {
            self.verify_token(i)?;
        }
        let mut base = 0i128;
        let mut quote = 0i128;
        let mut others = BTreeMap::<String, u128>::new();
        let mut evidence = vec![];
        for t in self.transfers.iter().filter(|t| self.within(t.node, at)) {
            // Nested Pump swaps require separate attribution; don't double count.
            let mut parent = self.nodes[t.node].parent;
            while let Some(p) = parent {
                if p == at {
                    break;
                }
                ensure!(
                    swap(&self.nodes[p]).is_none(),
                    "nested swap attribution ambiguous"
                );
                parent = self.nodes[p].parent;
            }
            if t.mint == self.keys[mint] {
                ensure!(
                    (t.source == ub && t.destination == pb)
                        || (t.source == pb && t.destination == ub),
                    "unexpected base-token movement inside swap"
                );
                base += if t.destination == ub {
                    t.amount as i128
                } else {
                    -(t.amount as i128)
                };
            } else if t.mint == self.keys[quote_mint] {
                if t.source == uq {
                    quote -= t.amount as i128;
                }
                if t.destination == uq {
                    quote += t.amount as i128;
                }
                if t.destination != uq && t.destination != pq {
                    self.verify_token(t.destination)?;
                    *others.entry(self.keys[t.destination].clone()).or_default() +=
                        t.amount as u128;
                }
            } else {
                bail!("unexpected asset inside swap");
            }
            evidence.push(json!({"instruction":t.node,"source":self.keys[t.source],"destination":self.keys[t.destination],"mint":t.mint,"raw_amount":t.amount.to_string()}));
        }
        ensure!(
            if buy {
                base > 0 && quote < 0
            } else {
                base < 0 && quote >= 0
            },
            "swap transfer directions disagree"
        );
        let tokens = u64::try_from(base.abs())?;
        let cash = u64::try_from(quote.abs())?;
        if n.data.starts_with(&BUY) || !buy {
            ensure!(
                tokens == amount(&n.data, 8)?,
                "base amount disagrees with instruction argument"
            );
        }
        if n.data.starts_with(&BUY) {
            ensure!(
                cash <= amount(&n.data, 16)?,
                "buy exceeds instruction limit"
            );
        }
        if !buy {
            ensure!(
                cash >= amount(&n.data, 16)?,
                "sale violates instruction minimum"
            );
        }
        if n.data.starts_with(&AMM_EXACT) {
            ensure!(
                cash <= amount(&n.data, 8)? && tokens >= amount(&n.data, 16)?,
                "exact-input instruction limits violated"
            );
        }
        let (token_mint, token_amount, sol_amount, sol_delta, normalized_buy) = if quote_is_sol {
            (mint, tokens, cash, quote, buy)
        } else {
            (quote_mint, cash, tokens, base, !buy)
        };
        ensure!(token_amount > 0, "zero normalized coin amount");
        Ok(
            json!({"venue":"pump_swap","wallet":self.keys[user],"mint":self.keys[token_mint],"pool":self.keys[pool],"is_buy":normalized_buy,"instruction_is_buy":buy,"sol_is_pool_quote":quote_is_sol,"token_amount_raw":token_amount.to_string(),"quote_lamports":sol_amount.to_string(),"quote_cashflow_lamports":sol_delta.to_string(),"quote_basis":"executed SOL transfers into/out of the user token account; token debit includes fees charged in the non-SOL asset when SOL is pool base","quote_paid_to_other_accounts":others.into_iter().map(|(a,v)|json!({"account":a,"mint":self.keys[quote_mint],"raw_amount":v.to_string()})).collect::<Vec<_>>(),"fee_breakdown_complete":false,"transfer_evidence":evidence,"pool_identity_source":"swap instruction roles plus token-vault mint/owner metadata"}),
        )
    }
    fn pump(&self, at: usize, buy: bool, tx: &Value) -> Result<Value> {
        self.no_internal_mint_burn(at)?;
        let n = &self.nodes[at];
        ensure!(n.data.len() >= 24, "short swap arguments");
        // Native balances are transaction-wide. Reconstruct every standard
        // lamport movement, allowing remaining direct writes only at the known
        // Pump curve/user/fee endpoints of this single native swap.
        let v2 = pump_v2(n);
        if v2 {
            ensure!(
                self.keys[account(n, 2)?] == WSOL,
                "non-SOL native-curve quote unsupported"
            );
        }
        let mint = account(n, if v2 { 1 } else { 2 })?;
        let curve = account(n, if v2 { 10 } else { 3 })?;
        let vault = account(n, if v2 { 11 } else { 4 })?;
        let user_token = account(n, if v2 { 14 } else { 5 })?;
        let user = account(n, user_role(n))?;
        ensure!(
            self.nodes
                .iter()
                .filter(|x| swap(x).is_some()
                    && (x.program != PUMP || x.accounts.get(user_role(x)) == Some(&user)))
                .count()
                == 1,
            "multiple swaps prevent native cash-flow attribution"
        );
        ensure!(
            self.pre[curve] > 0 && self.post[curve] > 0,
            "curve creation/closure requires separate native accounting"
        );
        self.owner(user_token, &self.keys[user], &self.keys[mint])?;
        self.owner(vault, &self.keys[curve], &self.keys[mint])?;
        self.verify_token(user_token)?;
        self.verify_token(vault)?;
        let mut base = 0i128;
        for t in self.transfers.iter().filter(|t| self.within(t.node, at)) {
            ensure!(
                t.mint == self.keys[mint]
                    && ((t.source == user_token && t.destination == vault)
                        || (t.source == vault && t.destination == user_token)),
                "unexpected transfer inside native swap"
            );
            base += if t.destination == user_token {
                t.amount as i128
            } else {
                -(t.amount as i128)
            };
        }
        ensure!(
            if buy { base > 0 } else { base < 0 },
            "native swap token direction mismatch"
        );
        let tokens = u64::try_from(base.abs())?;
        let exact = n.data.starts_with(&PUMP_EXACT) || n.data.starts_with(&PUMP_EXACT_V2);
        if !exact {
            ensure!(
                tokens == amount(&n.data, 8)?,
                "token transfer differs from native swap amount"
            );
        }
        let owned: BTreeSet<usize> = self
            .tokens
            .iter()
            .filter(|(_, t)| t.owner == self.keys[user])
            .map(|(i, _)| *i)
            .chain([user])
            .collect();
        ensure!(
            owned.iter().all(|i| !self.unstable.contains(i)),
            "unstable owned account prevents native accounting"
        );
        ensure!(
            self.nodes
                .iter()
                .enumerate()
                .filter(|(j, n)| *j != at && n.parent.is_none() && n.program == PUMP)
                .all(|(_, n)| !n.accounts.iter().any(|i| owned.contains(i))),
            "another Pump operation touches wallet assets"
        );
        let total_fee = tx["meta"]["fee"].as_u64().context("network fee")?;
        let (movements, residual) = self.native_moves(total_fee)?;
        let allowed: BTreeSet<usize> = [
            user,
            curve,
            account(n, if v2 { 6 } else { 1 })?,
            account(
                n,
                if v2 {
                    16
                } else if buy {
                    9
                } else {
                    8
                },
            )?,
        ]
        .into_iter()
        .collect();
        ensure!(
            residual.iter().enumerate().all(|(i, v)| *v == 0
                || allowed.contains(&i)
                || (*v > 0 && n.accounts.contains(&i))),
            "unattributed native balance changes outside Pump endpoints"
        );
        let mut external = 0i128;
        for m in movements.iter().filter(|m| !self.within(m.node, at)) {
            if owned.contains(&m.source) {
                external -= m.amount;
            }
            if owned.contains(&m.destination) {
                external += m.amount;
            }
        }
        let assets: i128 = owned
            .iter()
            .map(|i| self.post[*i] as i128 - self.pre[*i] as i128)
            .sum();
        let fee = if user == 0 {
            tx["meta"]["fee"].as_u64().context("fee")?
        } else {
            0
        };
        let cashflow = assets + fee as i128 - external;
        ensure!(
            if buy { cashflow < 0 } else { cashflow >= 0 },
            "native quote direction mismatch"
        );
        let cash = u64::try_from(cashflow.abs())?;
        if buy && !exact {
            ensure!(cash <= amount(&n.data, 16)?, "native buy exceeds limit");
        }
        if exact {
            ensure!(
                cash <= amount(&n.data, 8)? && tokens >= amount(&n.data, 16)?,
                "native exact-input limits violated"
            );
        }
        if !buy {
            ensure!(cash >= amount(&n.data, 16)?, "native sell violates minimum");
        }
        Ok(
            json!({"venue":"pump_fun","wallet":self.keys[user],"mint":self.keys[mint],"pool":format!("curve:{}",self.keys[mint]),"is_buy":buy,"token_amount_raw":tokens.to_string(),"quote_lamports":cash.to_string(),"quote_cashflow_lamports":cashflow.to_string(),"quote_basis":"wallet and owned token-account lamport changes, network fee added back, explicit outside movements removed; whole-transaction native ledger residual restricted to known Pump endpoints","external_native_cashflow_lamports":external.to_string(),"native_residual_endpoints":residual.iter().enumerate().filter(|(_,v)|**v!=0).map(|(i,v)|json!({"account":self.keys[i],"lamports":v.to_string()})).collect::<Vec<_>>(),"fee_breakdown_complete":false,"pool_identity_source":"instruction roles and curve token-vault ownership"}),
        )
    }
    fn no_internal_mint_burn(&self, at: usize) -> Result<()> {
        ensure!(
            !self
                .nodes
                .iter()
                .enumerate()
                .any(|(i, n)| self.within(i, at)
                    && (n.program == TOKEN || n.program == TOKEN22)
                    && n.data
                        .first()
                        .is_some_and(|op| matches!(*op, 7 | 8 | 14 | 15))),
            "mint/burn inside swap requires separate net-amount accounting"
        );
        Ok(())
    }
}

pub fn transaction(tx: &Value) -> Result<Value> {
    ensure!(tx["meta"].get("err").is_some(), "missing execution status");
    ensure!(
        tx["meta"]["err"].is_null(),
        "failed transaction has no successful swaps"
    );
    let c = ContextTx::parse(tx)?;
    let mut verified = vec![];
    let mut rejected = vec![];
    for (at, n) in c.nodes.iter().enumerate() {
        let Some(buy) = swap(n) else {
            continue;
        };
        let result = if n.program == AMM {
            c.amm(at, buy)
        } else {
            c.pump(at, buy, tx)
        };
        match result {
            Ok(mut row) => {
                row["instruction_index"] = json!(at);
                row["outer_instruction_index"] = json!(n.outer);
                row["signature"] = tx["transaction"]["signatures"][0].clone();
                row["slot"] = tx["slot"].clone();
                row["block_time"] = tx["blockTime"].clone();
                let wallet = row["wallet"].as_str().context("wallet")?;
                let signed = tx["transaction"]["message"]["header"]["numRequiredSignatures"]
                    .as_u64()
                    .context("signer count")? as usize;
                let wallet_signed = c.keys.iter().take(signed).any(|k| k == wallet);
                let receipt = balances::receipt(tx, wallet)?;
                row["wallet_signed"] = json!(wallet_signed);
                row["wallet_balance_receipt"] = receipt;
                row["network_fee_lamports"] = tx["meta"]["fee"].clone();
                row["network_fee_payer"] = json!(c.keys[0]);
                row["financial_evidence"] =
                    json!("executed_transfers_and_full_transaction_balances");
                row["reserve_quote_available"] = json!(false);
                row["profit_rank_eligible"] = json!(false);
                verified.push(row);
            }
            Err(e) => rejected.push(json!({"instruction_index":at,"program":n.program,"wallet":n.accounts.get(user_role(n)).and_then(|i|c.keys.get(*i)),"reason":e.to_string()})),
        }
    }
    let identity = Decoder::default().capture_identity_events(tx);
    let identities = identity
        .as_ref()
        .map(|v| {
            v.iter()
                .filter(|e| matches!(e.name.as_str(), "TradeEvent" | "BuyEvent" | "SellEvent"))
                .count()
        })
        .ok();
    let all_instructions_accounted = identities == Some(verified.len() + rejected.len());
    Ok(
        json!({"signature":tx["transaction"]["signatures"][0],"slot":tx["slot"],"verified_swaps":verified,"rejected_swaps":rejected,"event_trade_identity_count":identities,"identity_error":identity.err().map(|e|e.to_string()),"all_trade_identities_accounted_for":all_instructions_accounted,"strict_event_error":Decoder::default().transaction_events(tx).err().map(|e|e.to_string()),"event_amount_fields_used":false,"profit_rank_eligible":false,"executable_backtest_eligible":false,"limitation":"Verified historical cash flows are not wallet profits or executable reserve quotes. Ambiguous native writes, asset flows and unsupported token extensions are rejected. Network fees are transaction-wide and must not be counted once per swap."}),
    )
}
