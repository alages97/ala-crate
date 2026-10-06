//! Read-only support for Kuru's V2 spot-market interface.
//!
//! V2's native `estimateSwap` view returns both `amountInUsed` and
//! `amountOut`. That removes the legacy zero-sender estimator's main
//! ambiguity: callers can prove whether the requested input was completely
//! consumed. The current public MON/USDC market may still be a legacy market;
//! callers must detect the interface at runtime instead of assuming rollout.

use std::str::FromStr;

use anyhow::{bail, Context, Result};
use ethers::abi::{Abi, Function, Token};
use ethers::types::{Address, U256};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::monad_rpc;

const V2_SPOT_ABI: &str = r#"
[
  {
    "type":"function",
    "name":"accountCore",
    "stateMutability":"view",
    "inputs":[],
    "outputs":[{"name":"","type":"address"}]
  },
  {
    "type":"function",
    "name":"baseToken",
    "stateMutability":"view",
    "inputs":[],
    "outputs":[{"name":"","type":"address"}]
  },
  {
    "type":"function",
    "name":"verifiedSpotOrderBook",
    "stateMutability":"view",
    "inputs":[{"name":"market","type":"address"}],
    "outputs":[{"name":"","type":"bool"}]
  },
  {
    "type":"function",
    "name":"quoteToken",
    "stateMutability":"view",
    "inputs":[],
    "outputs":[{"name":"","type":"address"}]
  },
  {
    "type":"function",
    "name":"getMarketParams",
    "stateMutability":"view",
    "inputs":[],
    "outputs":[
      {"name":"pricePrecision","type":"uint32"},
      {"name":"sizePrecision","type":"uint96"},
      {"name":"tickSize","type":"uint32"},
      {"name":"minQuoteNotional","type":"uint96"},
      {"name":"maxQuoteNotional","type":"uint96"},
      {"name":"takerFeePps","type":"uint256"},
      {"name":"makerFeePps","type":"uint256"}
    ]
  },
  {
    "type":"function",
    "name":"getL2Book",
    "stateMutability":"view",
    "inputs":[{"name":"levels","type":"uint256"}],
    "outputs":[
      {"name":"bidPrices","type":"uint32[]"},
      {"name":"bidSizes","type":"uint96[]"},
      {"name":"askPrices","type":"uint32[]"},
      {"name":"askSizes","type":"uint96[]"}
    ]
  },
  {
    "type":"function",
    "name":"estimateSwap",
    "stateMutability":"view",
    "inputs":[
      {"name":"isBuy","type":"bool"},
      {"name":"amountIn","type":"uint128"}
    ],
    "outputs":[
      {
        "name":"result",
        "type":"tuple",
        "components":[
          {"name":"amountInUsed","type":"uint128"},
          {"name":"amountOut","type":"uint128"}
        ]
      }
    ]
  }
]
"#;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct KuruV2Identity {
    pub market: String,
    pub account_core: String,
    pub base_token: String,
    pub quote_token: String,
    pub price_precision: String,
    pub size_precision: String,
    pub tick_size: String,
    pub min_quote_notional: String,
    pub max_quote_notional: String,
    pub taker_fee_pps: String,
    pub maker_fee_pps: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KuruV2Quote {
    pub market: String,
    pub block_reference: Value,
    pub is_buy: bool,
    pub amount_in_raw: String,
    pub amount_in_used_raw: String,
    pub amount_out_raw: String,
    pub full_input_consumed: bool,
    pub calldata: String,
    pub quote_latency_ns: u64,
}

pub async fn identify_market(
    client: &reqwest::Client,
    rpc_url: &str,
    market: &str,
    block_reference: &Value,
) -> Result<KuruV2Identity> {
    Address::from_str(market).context("invalid Kuru V2 market address")?;
    probe_unique_v2_interface(client, rpc_url, market, block_reference).await?;
    let account_core_raw =
        call_no_args(client, rpc_url, market, "accountCore", block_reference).await?;
    let base = call_no_args(client, rpc_url, market, "baseToken", block_reference).await?;
    let quote = call_no_args(client, rpc_url, market, "quoteToken", block_reference).await?;
    let account_core = decode_address("accountCore", &account_core_raw)?;
    if account_core.eq_ignore_ascii_case("0x0000000000000000000000000000000000000000") {
        bail!("Kuru V2 accountCore is the zero address");
    }
    verify_registered_market(client, rpc_url, &account_core, market, block_reference).await?;
    let base_token = decode_address("baseToken", &base)?;
    let quote_token = decode_address("quoteToken", &quote)?;

    let function = function("getMarketParams")?;
    let calldata = encode_hex(
        &function
            .encode_input(&[])
            .context("encode Kuru V2 getMarketParams")?,
    );
    let response = monad_rpc::request(
        client,
        rpc_url,
        "eth_call",
        json!([{"to":market,"data":calldata},block_reference]),
    )
    .await?;
    let raw = monad_rpc::result_str(&response.value, "Kuru V2 getMarketParams")?;
    let bytes = decode_hex(raw)?;
    let output = function
        .decode_output(&bytes)
        .context("decode Kuru V2 getMarketParams")?;
    let [Token::Uint(price_precision), Token::Uint(size_precision), Token::Uint(tick_size), Token::Uint(min_quote_notional), Token::Uint(max_quote_notional), Token::Uint(taker_fee_pps), Token::Uint(maker_fee_pps)] =
        output.as_slice()
    else {
        bail!("Kuru V2 getMarketParams returned an unexpected ABI layout");
    };

    Ok(KuruV2Identity {
        market: market.to_owned(),
        account_core,
        base_token,
        quote_token,
        price_precision: price_precision.to_string(),
        size_precision: size_precision.to_string(),
        tick_size: tick_size.to_string(),
        min_quote_notional: min_quote_notional.to_string(),
        max_quote_notional: max_quote_notional.to_string(),
        taker_fee_pps: taker_fee_pps.to_string(),
        maker_fee_pps: maker_fee_pps.to_string(),
    })
}

async fn verify_registered_market(
    client: &reqwest::Client,
    rpc_url: &str,
    account_core: &str,
    market: &str,
    block_reference: &Value,
) -> Result<()> {
    let function = function("verifiedSpotOrderBook")?;
    let market_address = Address::from_str(market).context("invalid Kuru V2 market address")?;
    let calldata = encode_hex(
        &function
            .encode_input(&[Token::Address(market_address)])
            .context("encode Kuru V2 verifiedSpotOrderBook")?,
    );
    let response = monad_rpc::request(
        client,
        rpc_url,
        "eth_call",
        json!([{"to":account_core,"data":calldata},block_reference]),
    )
    .await?;
    let raw = monad_rpc::result_str(&response.value, "Kuru V2 verifiedSpotOrderBook")?;
    let output = function
        .decode_output(&decode_hex(raw)?)
        .context("decode Kuru V2 verifiedSpotOrderBook")?;
    match output.as_slice() {
        [Token::Bool(true)] => Ok(()),
        [Token::Bool(false)] => bail!("Kuru V2 AccountCore has not verified market {market}"),
        _ => bail!("Kuru V2 verifiedSpotOrderBook returned an unexpected ABI layout"),
    }
}

/// Require a V2-only call before decoding fields whose selectors or output
/// prefixes can overlap older order-book implementations. A successful
/// four-array decode protects `auto` mode from classifying a legacy proxy as V2.
async fn probe_unique_v2_interface(
    client: &reqwest::Client,
    rpc_url: &str,
    market: &str,
    block_reference: &Value,
) -> Result<()> {
    let function = function("getL2Book")?;
    let calldata = encode_hex(
        &function
            .encode_input(&[Token::Uint(U256::one())])
            .context("encode Kuru V2 getL2Book")?,
    );
    let response = monad_rpc::request(
        client,
        rpc_url,
        "eth_call",
        json!([{"to":market,"data":calldata},block_reference]),
    )
    .await?;
    let raw = monad_rpc::result_str(&response.value, "Kuru V2 getL2Book")?;
    let bytes = decode_hex(raw)?;
    let output = function
        .decode_output(&bytes)
        .context("decode Kuru V2 getL2Book")?;
    if output.len() != 4 || !output.iter().all(|token| matches!(token, Token::Array(_))) {
        bail!("Kuru V2 getL2Book returned an unexpected ABI layout");
    }
    Ok(())
}

pub async fn estimate_swap(
    client: &reqwest::Client,
    rpc_url: &str,
    market: &str,
    is_buy: bool,
    amount_in_raw: U256,
    block_reference: &Value,
) -> Result<KuruV2Quote> {
    if amount_in_raw.is_zero() {
        bail!("Kuru V2 swap amount must be greater than zero");
    }
    if amount_in_raw.bits() > 128 {
        bail!("Kuru V2 swap amount does not fit uint128");
    }
    Address::from_str(market).context("invalid Kuru V2 market address")?;
    if block_reference.is_null() {
        bail!("Kuru V2 block reference must not be null");
    }

    let function = function("estimateSwap")?;
    let calldata = encode_hex(
        &function
            .encode_input(&[Token::Bool(is_buy), Token::Uint(amount_in_raw)])
            .context("encode Kuru V2 estimateSwap")?,
    );
    let response = monad_rpc::request(
        client,
        rpc_url,
        "eth_call",
        json!([{"to":market,"data":calldata},block_reference]),
    )
    .await?;
    let raw = monad_rpc::result_str(&response.value, "Kuru V2 estimateSwap")?;
    let bytes = decode_hex(raw)?;
    let output = function
        .decode_output(&bytes)
        .context("decode Kuru V2 estimateSwap")?;
    let [Token::Tuple(result)] = output.as_slice() else {
        bail!("Kuru V2 estimateSwap returned an unexpected ABI layout");
    };
    let [Token::Uint(amount_in_used), Token::Uint(amount_out)] = result.as_slice() else {
        bail!("Kuru V2 SwapResult returned an unexpected ABI layout");
    };

    Ok(KuruV2Quote {
        market: market.to_owned(),
        block_reference: block_reference.clone(),
        is_buy,
        amount_in_raw: amount_in_raw.to_string(),
        amount_in_used_raw: amount_in_used.to_string(),
        amount_out_raw: amount_out.to_string(),
        full_input_consumed: *amount_in_used == amount_in_raw,
        calldata,
        quote_latency_ns: response.latency_ns,
    })
}

async fn call_no_args(
    client: &reqwest::Client,
    rpc_url: &str,
    market: &str,
    name: &str,
    block_reference: &Value,
) -> Result<Vec<u8>> {
    let function = function(name)?;
    let calldata = encode_hex(
        &function
            .encode_input(&[])
            .with_context(|| format!("encode Kuru V2 {name}"))?,
    );
    let response = monad_rpc::request(
        client,
        rpc_url,
        "eth_call",
        json!([{"to":market,"data":calldata},block_reference]),
    )
    .await?;
    let raw = monad_rpc::result_str(&response.value, &format!("Kuru V2 {name}"))?;
    decode_hex(raw)
}

fn decode_address(name: &str, bytes: &[u8]) -> Result<String> {
    let function = function(name)?;
    let output = function
        .decode_output(bytes)
        .with_context(|| format!("decode Kuru V2 {name}"))?;
    let [Token::Address(address)] = output.as_slice() else {
        bail!("Kuru V2 {name} returned an unexpected ABI layout");
    };
    Ok(format!("{address:#x}"))
}

fn function(name: &str) -> Result<Function> {
    let abi: Abi = serde_json::from_str(V2_SPOT_ABI).context("parse embedded Kuru V2 ABI")?;
    abi.function(name)
        .cloned()
        .with_context(|| format!("embedded Kuru V2 ABI is missing {name}"))
}

fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(2 + bytes.len() * 2);
    encoded.push_str("0x");
    for byte in bytes {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

fn decode_hex(value: &str) -> Result<Vec<u8>> {
    let digits = value
        .strip_prefix("0x")
        .or_else(|| value.strip_prefix("0X"))
        .unwrap_or(value);
    if !digits.len().is_multiple_of(2) {
        bail!("hex string has an odd number of digits");
    }
    digits
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| Ok((decode_nibble(pair[0])? << 4) | decode_nibble(pair[1])?))
        .collect()
}

fn decode_nibble(value: u8) -> Result<u8> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        b'A'..=b'F' => Ok(value - b'A' + 10),
        _ => bail!("invalid hex digit {:?}", value as char),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn estimate_swap_abi_round_trips_and_returns_consumed_input() {
        let function = function("estimateSwap").expect("estimateSwap ABI");
        let amount = U256::from(100_000_000_u64);
        let calldata = function
            .encode_input(&[Token::Bool(true), Token::Uint(amount)])
            .expect("encode");
        let decoded = function.decode_input(&calldata[4..]).expect("decode");
        assert_eq!(decoded, vec![Token::Bool(true), Token::Uint(amount)]);

        let output = ethers::abi::encode(&[Token::Tuple(vec![
            Token::Uint(amount),
            Token::Uint(U256::from_dec_str("3600000000000000000000").expect("output")),
        ])]);
        let decoded = function.decode_output(&output).expect("decode output");
        let Token::Tuple(result) = &decoded[0] else {
            panic!("expected tuple");
        };
        assert_eq!(result[0], Token::Uint(amount));
    }

    #[test]
    fn token_identity_calls_have_distinct_selectors() {
        let base = function("baseToken").expect("baseToken ABI");
        let quote = function("quoteToken").expect("quoteToken ABI");
        assert_ne!(base.short_signature(), quote.short_signature());
        assert_eq!(encode_hex(&base.short_signature()), "0xc55dae63");
        assert_eq!(encode_hex(&quote.short_signature()), "0x217a4b70");
    }

    #[test]
    fn l2_book_is_a_v2_unique_probe() {
        let get_l2_book = function("getL2Book").expect("getL2Book ABI");
        assert_eq!(encode_hex(&get_l2_book.short_signature()), "0xb4cc4b1a");
    }
}
