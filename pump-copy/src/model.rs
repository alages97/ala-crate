use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};

pub const PUMP: &str = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";
pub const AMM: &str = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA";
pub const WSOL: &str = "So11111111111111111111111111111111111111112";
pub const SOL: u64 = 1_000_000_000;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Venue {
    PumpFun,
    PumpSwap,
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Side {
    Buy,
    Sell,
}

/// All quantities are raw integer units. No token decimals are needed for quotes.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Trade {
    pub signature: String,
    pub tx_index: u32,
    pub event_index: u32,
    pub mint: String,
    pub wallet: String,
    pub pool: String,
    pub venue: Venue,
    pub side: Side,
    pub tokens: u64,
    /// Wallet debit on buy / credit on sell, including DEX fees, excluding tx fee.
    pub quote_lamports: u64,
    /// Conservatively allocated transaction fee (full fee per swap).
    pub network_fee_lamports: u64,
    pub reserve: Reserve,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Reserve {
    pub base: u64,
    /// Effective (virtual + real, where applicable) quote reserves.
    pub quote: u64,
    pub real_base: u64,
    pub real_quote: u64,
    pub fee_bps: u32,
}

impl Reserve {
    pub fn validate(&self) -> Result<()> {
        ensure!(self.base > 0 && self.quote > 0, "zero effective reserves");
        ensure!(self.fee_bps < 10_000, "invalid total fee bps");
        Ok(())
    }
    pub fn buy(&self, budget: u64, haircut_bps: u32) -> Option<u64> {
        let input = budget as u128 * 10_000 / (10_000 + self.fee_bps as u128);
        let out = self.base as u128 * input / (self.quote as u128 + input);
        let out = out * (10_000u32.checked_sub(haircut_bps)? as u128) / 10_000;
        let out = u64::try_from(out).ok()?;
        (out > 0 && out <= self.real_base).then_some(out)
    }
    pub fn sell(&self, tokens: u64, haircut_bps: u32) -> Option<u64> {
        let gross = self.quote as u128 * tokens as u128 / (self.base as u128 + tokens as u128);
        if gross > self.real_quote as u128 {
            return None;
        }
        let fee = (gross * self.fee_bps as u128).div_ceil(10_000);
        let net = gross.checked_sub(fee)? * 10_000u32.checked_sub(haircut_bps)? as u128 / 10_000;
        u64::try_from(net).ok()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Batch {
    pub schema_version: u32,
    pub slot: u64,
    pub block_time_ms: u64,
    pub blockhash: String,
    pub trades: Vec<Trade>,
    #[serde(default)]
    pub skipped_non_sol: u64,
}

impl Batch {
    pub fn validate(&self) -> Result<()> {
        ensure!(self.schema_version == 1, "unsupported data schema");
        ensure!(self.block_time_ms > 0, "missing block time");
        let mut last = None;
        for t in &self.trades {
            // A real dust sale can return zero lamports after integer rounding.
            // Retain it: dropping it would hide a realized loss and network fee.
            ensure!(
                t.tokens > 0 && (t.quote_lamports > 0 || t.side == Side::Sell),
                "invalid zero trade amount"
            );
            ensure!(
                !t.wallet.is_empty()
                    && !t.mint.is_empty()
                    && !t.signature.is_empty()
                    && !t.pool.is_empty(),
                "empty identity"
            );
            t.reserve.validate()?;
            let key = (t.tx_index, t.event_index);
            ensure!(
                last.is_none_or(|p| p < key),
                "duplicate or unordered events"
            );
            last = Some(key);
        }
        Ok(())
    }
}
