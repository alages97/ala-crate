//! Block-pinned, read-only access to the deployed Kuru MON/USDC market.
//!
//! Kuru's order-book contract contains an intentional estimator path: calling
//! `placeAndExecuteMarketBuy` or `placeAndExecuteMarketSell` with
//! `from = address(0)` skips wallet/margin transfers and returns the amount
//! matched by the same on-chain matching code used for a real market order.
//! Kuru's official TypeScript SDK uses this path for its cost estimator.
//!
//! The estimator returns before the contract's final fill-or-kill and minimum
//! output checks. A successful result is therefore a same-state executable
//! price indication, but it is not proof that the entire input was consumed or
//! that an independently submitted transaction would fill.

use std::str::FromStr;

use anyhow::{anyhow, bail, Context, Result};
use ethers::abi::{Abi, Function, Token};
use ethers::types::{Address, U256};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::monad_rpc;

/// Deployed Kuru native MON/USDC order-book market on Monad mainnet.
pub const KURU_MON_USDC_MARKET: &str = "0x065C9d28E428A0db40191a54d33d5b7c71a9C394";

/// Wrapped MON used by route builders. The Kuru market itself uses native MON.
pub const WMON_ADDRESS: &str = "0x3bd359C1119dA7Da1D913D1C4D2B7c461115433A";

/// Native Circle USDC on Monad mainnet.
pub const USDC_ADDRESS: &str = "0x754704Bc059F8C67012fEd69BC8A327a5aafb603";

pub const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";

const ORDER_BOOK_ABI: &str = r#"
[
  {
    "type":"function",
    "name":"getMarketParams",
    "stateMutability":"view",
    "inputs":[],
    "outputs":[
      {"name":"pricePrecision","type":"uint32"},
      {"name":"sizePrecision","type":"uint96"},
      {"name":"baseAsset","type":"address"},
      {"name":"baseAssetDecimals","type":"uint256"},
      {"name":"quoteAsset","type":"address"},
      {"name":"quoteAssetDecimals","type":"uint256"},
      {"name":"tickSize","type":"uint32"},
      {"name":"minSize","type":"uint96"},
      {"name":"maxSize","type":"uint96"},
      {"name":"takerFeeBps","type":"uint256"},
      {"name":"makerFeeBps","type":"uint256"}
    ]
  },
  {
    "type":"function",
    "name":"placeAndExecuteMarketBuy",
    "stateMutability":"payable",
    "inputs":[
      {"name":"quoteSize","type":"uint96"},
      {"name":"minAmountOut","type":"uint256"},
      {"name":"isMargin","type":"bool"},
      {"name":"isFillOrKill","type":"bool"}
    ],
    "outputs":[{"name":"baseTokensCredited","type":"uint256"}]
  },
  {
    "type":"function",
    "name":"placeAndExecuteMarketSell",
    "stateMutability":"payable",
    "inputs":[
      {"name":"size","type":"uint96"},
      {"name":"minAmountOut","type":"uint256"},
      {"name":"isMargin","type":"bool"},
      {"name":"isFillOrKill","type":"bool"}
    ],
    "outputs":[{"name":"quoteCredited","type":"uint256"}]
  }
]
"#;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct KuruMarketParams {
    pub price_precision: String,
    pub size_precision: String,
    pub base_asset: String,
    pub base_asset_decimals: u32,
    pub quote_asset: String,
    pub quote_asset_decimals: u32,
    pub tick_size: String,
    pub min_size: String,
    pub max_size: String,
    pub taker_fee_bps: String,
    pub maker_fee_bps: String,
}

impl KuruMarketParams {
    pub fn price_precision_u256(&self) -> Result<U256> {
        U256::from_dec_str(&self.price_precision).context("invalid Kuru price precision")
    }

    pub fn size_precision_u256(&self) -> Result<U256> {
        U256::from_dec_str(&self.size_precision).context("invalid Kuru size precision")
    }

    /// Convert a raw quote-token amount into Kuru's `pricePrecision` units.
    ///
    /// The conversion must be exact. Silently rounding a quote input would
    /// make the paired route use different notionals on its two venues.
    pub fn quote_raw_to_price_units(&self, raw_quote: U256) -> Result<U256> {
        scale_exact(
            raw_quote,
            self.price_precision_u256()?,
            pow10(self.quote_asset_decimals)?,
            "quote raw units to Kuru price precision",
        )
    }

    /// Convert raw base-token units to Kuru size units, rounding down.
    ///
    /// Kuru cannot accept sub-size-precision dust. The returned remainder is
    /// retained by the hypothetical atomic executor and is reported by the
    /// shadow prover instead of being silently valued as sale proceeds.
    pub fn base_raw_to_size_units(&self, raw_base: U256) -> Result<(U256, U256)> {
        let numerator = raw_base
            .checked_mul(self.size_precision_u256()?)
            .ok_or_else(|| anyhow!("base raw to Kuru size conversion overflow"))?;
        let denominator = pow10(self.base_asset_decimals)?;
        let units = numerator / denominator;
        let represented_raw = units
            .checked_mul(denominator)
            .ok_or_else(|| anyhow!("Kuru represented base conversion overflow"))?
            / self.size_precision_u256()?;
        Ok((units, raw_base.saturating_sub(represented_raw)))
    }

    pub fn validate_mon_usdc(&self) -> Result<()> {
        if !self.base_asset.eq_ignore_ascii_case(ZERO_ADDRESS) {
            bail!(
                "Kuru market base asset is {}, expected native MON",
                self.base_asset
            );
        }
        if !self.quote_asset.eq_ignore_ascii_case(USDC_ADDRESS) {
            bail!(
                "Kuru market quote asset is {}, expected Monad USDC",
                self.quote_asset
            );
        }
        if self.base_asset_decimals != 18 || self.quote_asset_decimals != 6 {
            bail!(
                "unexpected Kuru decimals: base={}, quote={}",
                self.base_asset_decimals,
                self.quote_asset_decimals
            );
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum KuruQuoteSide {
    BuyMon,
    SellMon,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KuruQuote {
    pub market: String,
    pub block_reference: Value,
    pub side: KuruQuoteSide,
    pub amount_in_contract_units: String,
    pub amount_out_raw: String,
    pub calldata: String,
    pub quote_latency_ns: u64,
    /// Lower-bound estimate for the zero-sender estimator path. It excludes
    /// real token transfers, approvals and an atomic executor's overhead.
    pub estimator_gas: Option<String>,
    pub estimator_gas_latency_ns: Option<u64>,
    pub estimator_gas_error: Option<String>,
    pub full_input_consumption_proven: bool,
}

/// Read and decode the Kuru market parameters at an explicit block.
pub async fn market_params(
    client: &reqwest::Client,
    rpc_url: &str,
    market: &str,
    block_reference: &Value,
) -> Result<KuruMarketParams> {
    let function = order_book_function("getMarketParams")?;
    let calldata = function
        .encode_input(&[])
        .context("encode Kuru getMarketParams calldata")?;
    let response = eth_call(
        client,
        rpc_url,
        market,
        ZERO_ADDRESS,
        &encode_hex(&calldata),
        block_reference,
    )
    .await?;
    let bytes = rpc_result_bytes(&response.value, "Kuru getMarketParams")?;
    let decoded = function
        .decode_output(&bytes)
        .context("decode Kuru getMarketParams output")?;
    let [Token::Uint(price_precision), Token::Uint(size_precision), Token::Address(base_asset), Token::Uint(base_decimals), Token::Address(quote_asset), Token::Uint(quote_decimals), Token::Uint(tick_size), Token::Uint(min_size), Token::Uint(max_size), Token::Uint(taker_fee_bps), Token::Uint(maker_fee_bps)] =
        decoded.as_slice()
    else {
        bail!("Kuru getMarketParams returned an unexpected ABI layout");
    };

    Ok(KuruMarketParams {
        price_precision: price_precision.to_string(),
        size_precision: size_precision.to_string(),
        base_asset: format!("{base_asset:#x}"),
        base_asset_decimals: u256_to_u32(*base_decimals, "base asset decimals")?,
        quote_asset: format!("{quote_asset:#x}"),
        quote_asset_decimals: u256_to_u32(*quote_decimals, "quote asset decimals")?,
        tick_size: tick_size.to_string(),
        min_size: min_size.to_string(),
        max_size: max_size.to_string(),
        taker_fee_bps: taker_fee_bps.to_string(),
        maker_fee_bps: maker_fee_bps.to_string(),
    })
}

pub async fn quote_market_buy(
    client: &reqwest::Client,
    rpc_url: &str,
    market: &str,
    quote_size: U256,
    block_reference: &Value,
    estimate_gas: bool,
) -> Result<KuruQuote> {
    quote_market_order(
        client,
        rpc_url,
        market,
        KuruQuoteSide::BuyMon,
        quote_size,
        block_reference,
        estimate_gas,
    )
    .await
}

pub async fn quote_market_sell(
    client: &reqwest::Client,
    rpc_url: &str,
    market: &str,
    size: U256,
    block_reference: &Value,
    estimate_gas: bool,
) -> Result<KuruQuote> {
    quote_market_order(
        client,
        rpc_url,
        market,
        KuruQuoteSide::SellMon,
        size,
        block_reference,
        estimate_gas,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn quote_market_order(
    client: &reqwest::Client,
    rpc_url: &str,
    market: &str,
    side: KuruQuoteSide,
    amount_in: U256,
    block_reference: &Value,
    estimate_gas: bool,
) -> Result<KuruQuote> {
    if amount_in.is_zero() {
        bail!("Kuru quote amount must be greater than zero");
    }
    if amount_in.bits() > 96 {
        bail!("Kuru quote amount does not fit uint96");
    }
    Address::from_str(market).context("invalid Kuru market address")?;
    if block_reference.is_null() {
        bail!("Kuru quote block reference must not be null");
    }

    let function_name = match side {
        KuruQuoteSide::BuyMon => "placeAndExecuteMarketBuy",
        KuruQuoteSide::SellMon => "placeAndExecuteMarketSell",
    };
    let function = order_book_function(function_name)?;
    let calldata = function
        .encode_input(&[
            Token::Uint(amount_in),
            Token::Uint(U256::zero()),
            Token::Bool(false),
            Token::Bool(false),
        ])
        .with_context(|| format!("encode Kuru {function_name} calldata"))?;
    let calldata = encode_hex(&calldata);

    let call = eth_call(
        client,
        rpc_url,
        market,
        ZERO_ADDRESS,
        &calldata,
        block_reference,
    );
    let gas = async {
        if estimate_gas {
            Some(
                eth_estimate_gas(
                    client,
                    rpc_url,
                    market,
                    ZERO_ADDRESS,
                    &calldata,
                    block_reference,
                )
                .await,
            )
        } else {
            None
        }
    };
    let (call, gas) = tokio::join!(call, gas);
    let call = call?;
    let output_bytes = rpc_result_bytes(&call.value, function_name)?;
    let output = function
        .decode_output(&output_bytes)
        .with_context(|| format!("decode Kuru {function_name} output"))?;
    let [Token::Uint(amount_out)] = output.as_slice() else {
        bail!("Kuru {function_name} returned an unexpected ABI layout");
    };

    let (estimator_gas, estimator_gas_latency_ns, estimator_gas_error) = match gas {
        Some(Ok(response)) => match rpc_result_u256(&response.value, "Kuru eth_estimateGas") {
            Ok(value) => (Some(value.to_string()), Some(response.latency_ns), None),
            Err(error) => (None, Some(response.latency_ns), Some(format!("{error:#}"))),
        },
        Some(Err(error)) => (None, None, Some(format!("{error:#}"))),
        None => (None, None, None),
    };

    Ok(KuruQuote {
        market: market.to_owned(),
        block_reference: block_reference.clone(),
        side,
        amount_in_contract_units: amount_in.to_string(),
        amount_out_raw: amount_out.to_string(),
        calldata,
        quote_latency_ns: call.latency_ns,
        estimator_gas,
        estimator_gas_latency_ns,
        estimator_gas_error,
        // The contract returns early for `msg.sender == address(0)`, before
        // checking the remaining input and `_isFillOrKill`.
        full_input_consumption_proven: false,
    })
}

async fn eth_call(
    client: &reqwest::Client,
    rpc_url: &str,
    to: &str,
    from: &str,
    calldata: &str,
    block_reference: &Value,
) -> Result<monad_rpc::RpcResponse> {
    monad_rpc::request(
        client,
        rpc_url,
        "eth_call",
        json!([{"to":to,"from":from,"data":calldata},block_reference]),
    )
    .await
}

async fn eth_estimate_gas(
    client: &reqwest::Client,
    rpc_url: &str,
    to: &str,
    from: &str,
    calldata: &str,
    block_reference: &Value,
) -> Result<monad_rpc::RpcResponse> {
    monad_rpc::request(
        client,
        rpc_url,
        "eth_estimateGas",
        json!([{"to":to,"from":from,"data":calldata},block_reference]),
    )
    .await
}

fn rpc_result_bytes(response: &Value, context: &str) -> Result<Vec<u8>> {
    if let Some(error) = response.get("error") {
        bail!("{context} JSON-RPC error: {error}");
    }
    let value = response
        .get("result")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("{context} response has no string result: {response}"))?;
    decode_hex(value).with_context(|| format!("decode {context} result hex"))
}

fn rpc_result_u256(response: &Value, context: &str) -> Result<U256> {
    if let Some(error) = response.get("error") {
        bail!("{context} JSON-RPC error: {error}");
    }
    let value = response
        .get("result")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("{context} response has no string result: {response}"))?;
    U256::from_str(value).with_context(|| format!("decode {context} quantity"))
}

fn order_book_function(name: &str) -> Result<Function> {
    let abi: Abi = serde_json::from_str(ORDER_BOOK_ABI).context("parse embedded Kuru ABI")?;
    abi.function(name)
        .cloned()
        .with_context(|| format!("embedded Kuru ABI is missing {name}"))
}

fn scale_exact(value: U256, numerator: U256, denominator: U256, context: &str) -> Result<U256> {
    let scaled = value
        .checked_mul(numerator)
        .ok_or_else(|| anyhow!("{context} overflow"))?;
    if scaled % denominator != U256::zero() {
        bail!("{context} is not exactly representable");
    }
    Ok(scaled / denominator)
}

fn pow10(exponent: u32) -> Result<U256> {
    let mut value = U256::one();
    for _ in 0..exponent {
        value = value
            .checked_mul(U256::from(10_u8))
            .ok_or_else(|| anyhow!("10^{exponent} does not fit U256"))?;
    }
    Ok(value)
}

fn u256_to_u32(value: U256, field: &str) -> Result<u32> {
    if value > U256::from(u32::MAX) {
        bail!("Kuru {field} does not fit u32");
    }
    Ok(value.as_u32())
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

    fn expected_params() -> KuruMarketParams {
        KuruMarketParams {
            price_precision: "100000000".to_owned(),
            size_precision: "10000000000".to_owned(),
            base_asset: ZERO_ADDRESS.to_owned(),
            base_asset_decimals: 18,
            quote_asset: USDC_ADDRESS.to_owned(),
            quote_asset_decimals: 6,
            tick_size: "100".to_owned(),
            min_size: "2000000000000".to_owned(),
            max_size: "2000000000000000000".to_owned(),
            taker_fee_bps: "0".to_owned(),
            maker_fee_bps: "0".to_owned(),
        }
    }

    #[test]
    fn market_order_selectors_match_the_official_abi() {
        let buy = order_book_function("placeAndExecuteMarketBuy").expect("buy ABI");
        let sell = order_book_function("placeAndExecuteMarketSell").expect("sell ABI");
        assert_eq!(encode_hex(&buy.short_signature()), "0x7c51d6cf");
        assert_eq!(encode_hex(&sell.short_signature()), "0x532c46db");
    }

    #[test]
    fn whole_usdc_converts_exactly_to_price_precision() {
        let params = expected_params();
        let raw = U256::from(100_000_000_u64);
        assert_eq!(
            params.quote_raw_to_price_units(raw).expect("exact units"),
            U256::from(10_000_000_000_u64)
        );
    }

    #[test]
    fn raw_mon_conversion_reports_unsellable_dust() {
        let params = expected_params();
        let raw = U256::from_dec_str("3689257561985561961033").expect("raw MON");
        let (units, dust) = params.base_raw_to_size_units(raw).expect("size units");
        assert_eq!(units.to_string(), "36892575619855");
        assert_eq!(dust.to_string(), "61961033");
    }

    #[test]
    fn embedded_calls_round_trip() {
        for (name, input) in [
            ("placeAndExecuteMarketBuy", U256::from(10_000_000_000_u64)),
            (
                "placeAndExecuteMarketSell",
                U256::from(36_000_000_000_000_u64),
            ),
        ] {
            let function = order_book_function(name).expect("function");
            let encoded = function
                .encode_input(&[
                    Token::Uint(input),
                    Token::Uint(U256::zero()),
                    Token::Bool(false),
                    Token::Bool(false),
                ])
                .expect("encode");
            let decoded = function.decode_input(&encoded[4..]).expect("decode");
            assert_eq!(decoded[0], Token::Uint(input));
            assert_eq!(decoded[1], Token::Uint(U256::zero()));
            assert_eq!(decoded[2], Token::Bool(false));
            assert_eq!(decoded[3], Token::Bool(false));
        }
    }

    #[test]
    fn mon_usdc_identity_is_strict() {
        expected_params()
            .validate_mon_usdc()
            .expect("expected market");
        let mut wrong = expected_params();
        wrong.quote_asset = ZERO_ADDRESS.to_owned();
        assert!(wrong.validate_mon_usdc().is_err());
    }
}
