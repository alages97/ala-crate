//! Minimal, read-only Crystal quoting support for Monad mainnet.
//!
//! Crystal exposes its market registry and quote entry point on one singleton.
//! Every call in this module requires an EIP-1898 block-hash object so callers
//! cannot accidentally mix state from different Monad heads.

use std::str::FromStr;

use anyhow::{anyhow, bail, Context, Result};
use ethers::abi::{Abi, Function, Token};
use ethers::types::{Address, U256};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::kuru_chain::{USDC_ADDRESS, WMON_ADDRESS};
use crate::monad_rpc;

/// Crystal singleton/router from Monad's official mainnet protocol registry.
pub const CRYSTAL_SINGLETON_ADDRESS: &str = "0x508254c838B2e936B0631440c5C6E3AB3a4a98BD";

/// Crystal's WMON/USDC spot market from Monad's official registry.
pub const CRYSTAL_MON_USDC_MARKET: &str = "0x39fAE95717cfD4bdA22317F1c124660A166b6BEc";

/// `getQuote(address,bool,bool,bool,uint256,uint256)`.
pub const GET_QUOTE_SELECTOR: [u8; 4] = [0x3b, 0x5b, 0x1f, 0xcc];

const CRYSTAL_ABI: &str = r#"
[
  {
    "type":"function",
    "name":"getMarket",
    "stateMutability":"view",
    "inputs":[{"name":"market","type":"address"}],
    "outputs":[{
      "name":"info",
      "type":"tuple",
      "components":[
        {"name":"quoteAsset","type":"address"},
        {"name":"baseAsset","type":"address"},
        {"name":"marketType","type":"uint256"},
        {"name":"highestBid","type":"uint256"},
        {"name":"lowestAsk","type":"uint256"},
        {"name":"scaleFactor","type":"uint256"},
        {"name":"tickSize","type":"uint256"},
        {"name":"maxPrice","type":"uint256"},
        {"name":"minSize","type":"uint256"},
        {"name":"takerFee","type":"uint256"},
        {"name":"makerRebate","type":"uint256"},
        {"name":"reserveQuote","type":"uint256"},
        {"name":"reserveBase","type":"uint256"},
        {"name":"isAMMEnabled","type":"bool"}
      ]
    }]
  },
  {
    "type":"function",
    "name":"getQuote",
    "stateMutability":"nonpayable",
    "inputs":[
      {"name":"market","type":"address"},
      {"name":"isBuy","type":"bool"},
      {"name":"isExactInput","type":"bool"},
      {"name":"isCompleteFill","type":"bool"},
      {"name":"size","type":"uint256"},
      {"name":"worstPrice","type":"uint256"}
    ],
    "outputs":[
      {"name":"amountIn","type":"uint256"},
      {"name":"amountOut","type":"uint256"}
    ]
  }
]
"#;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CrystalMarketIdentity {
    pub singleton: String,
    pub market: String,
    pub quote_asset: String,
    pub base_asset: String,
    pub market_type: String,
    pub highest_bid: String,
    pub lowest_ask: String,
    pub scale_factor: String,
    pub tick_size: String,
    pub max_price: String,
    pub min_size: String,
    pub taker_fee: String,
    pub maker_rebate: String,
    pub reserve_quote: String,
    pub reserve_base: String,
    pub is_amm_enabled: bool,
    pub block_reference: Value,
    pub calldata: String,
    pub rpc_latency_ns: u64,
}

impl CrystalMarketIdentity {
    /// Reject any Crystal market that is not exactly USDC quote / WMON base.
    pub fn validate_mon_usdc(&self) -> Result<()> {
        if !self.quote_asset.eq_ignore_ascii_case(USDC_ADDRESS) {
            bail!(
                "Crystal market quote asset is {}, expected Monad USDC {}",
                self.quote_asset,
                USDC_ADDRESS
            );
        }
        if !self.base_asset.eq_ignore_ascii_case(WMON_ADDRESS) {
            bail!(
                "Crystal market base asset is {}, expected WMON {}",
                self.base_asset,
                WMON_ADDRESS
            );
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CrystalQuote {
    pub singleton: String,
    pub market: String,
    pub is_buy: bool,
    pub is_exact_input: bool,
    pub is_complete_fill: bool,
    pub worst_price: String,
    pub requested_input_raw: String,
    pub amount_in_used_raw: String,
    pub residual_input_raw: String,
    pub amount_out_raw: String,
    pub full_input_consumed: bool,
    pub block_reference: Value,
    pub calldata: String,
    pub quote_latency_ns: u64,
}

/// Read and decode Crystal market identity at one canonical block hash.
pub async fn identify_market(
    client: &reqwest::Client,
    rpc_url: &str,
    market: &str,
    block_reference: &Value,
) -> Result<CrystalMarketIdentity> {
    validate_block_reference(block_reference)?;
    let market_address = Address::from_str(market).context("invalid Crystal market address")?;
    let function = crystal_function("getMarket")?;
    let calldata = function
        .encode_input(&[Token::Address(market_address)])
        .context("encode Crystal getMarket calldata")?;
    let calldata_hex = encode_hex(&calldata);
    let params = eth_call_params(CRYSTAL_SINGLETON_ADDRESS, &calldata_hex, block_reference)?;
    let response = monad_rpc::request(client, rpc_url, "eth_call", params).await?;
    let output = decode_result(&response.value, "Crystal getMarket")?;
    let decoded = function
        .decode_output(&output)
        .context("ABI-decode Crystal getMarket output")?;
    let fields = match decoded.as_slice() {
        [Token::Tuple(fields)] => fields,
        _ => bail!("Crystal getMarket returned an unexpected output layout"),
    };
    let identity = decode_market_identity(
        fields,
        market,
        block_reference,
        calldata_hex,
        response.latency_ns,
    )?;
    identity.validate_mon_usdc()?;
    Ok(identity)
}

/// Quote one exact-input Crystal trade at one canonical block hash.
///
/// `is_buy = true` means USDC -> WMON for the validated market; `false`
/// means WMON -> USDC. `isCompleteFill` is requested, but complete consumption
/// is proven independently from Crystal's returned `amountIn`.
pub async fn quote_exact_input_at(
    client: &reqwest::Client,
    rpc_url: &str,
    market: &str,
    is_buy: bool,
    amount_in: U256,
    block_reference: &Value,
) -> Result<CrystalQuote> {
    if amount_in.is_zero() {
        bail!("Crystal exact-input quote amount must be greater than zero");
    }
    validate_block_reference(block_reference)?;
    let market_address = Address::from_str(market).context("invalid Crystal market address")?;
    let function = crystal_function("getQuote")?;
    let calldata = encode_quote_call(&function, market_address, is_buy, amount_in)?;
    let calldata_hex = encode_hex(&calldata);
    let params = eth_call_params(CRYSTAL_SINGLETON_ADDRESS, &calldata_hex, block_reference)?;
    let response = monad_rpc::request(client, rpc_url, "eth_call", params).await?;
    let output = decode_result(&response.value, "Crystal getQuote")?;
    let decoded = function
        .decode_output(&output)
        .context("ABI-decode Crystal getQuote output")?;
    let (amount_in_used, amount_out) = decode_quote_output(&decoded)?;
    let (residual, full_input_consumed) = classify_consumption(amount_in, amount_in_used)?;

    Ok(CrystalQuote {
        singleton: CRYSTAL_SINGLETON_ADDRESS.to_owned(),
        market: market.to_owned(),
        is_buy,
        is_exact_input: true,
        is_complete_fill: true,
        worst_price: "0".to_owned(),
        requested_input_raw: amount_in.to_string(),
        amount_in_used_raw: amount_in_used.to_string(),
        residual_input_raw: residual.to_string(),
        amount_out_raw: amount_out.to_string(),
        full_input_consumed,
        block_reference: block_reference.clone(),
        calldata: calldata_hex,
        quote_latency_ns: response.latency_ns,
    })
}

fn crystal_function(name: &str) -> Result<Function> {
    let abi: Abi = serde_json::from_str(CRYSTAL_ABI).context("parse embedded Crystal ABI")?;
    abi.function(name)
        .cloned()
        .with_context(|| format!("embedded Crystal ABI is missing {name}"))
}

fn encode_quote_call(
    function: &Function,
    market: Address,
    is_buy: bool,
    amount_in: U256,
) -> Result<Vec<u8>> {
    function
        .encode_input(&[
            Token::Address(market),
            Token::Bool(is_buy),
            Token::Bool(true),
            Token::Bool(true),
            Token::Uint(amount_in),
            Token::Uint(U256::zero()),
        ])
        .context("encode Crystal getQuote calldata")
}

fn eth_call_params(target: &str, calldata: &str, block_reference: &Value) -> Result<Value> {
    validate_block_reference(block_reference)?;
    Ok(json!([
        {"to": target, "data": calldata},
        block_reference
    ]))
}

fn validate_block_reference(block_reference: &Value) -> Result<()> {
    let object = block_reference
        .as_object()
        .ok_or_else(|| anyhow!("Crystal calls require an EIP-1898 block-hash object"))?;
    let hash = object
        .get("blockHash")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("Crystal calls require EIP-1898 blockHash"))?;
    if hash.len() != 66 || !hash.starts_with("0x") {
        bail!("Crystal EIP-1898 blockHash must be a 32-byte 0x hex string");
    }
    if object.get("requireCanonical").and_then(Value::as_bool) != Some(true) {
        bail!("Crystal calls require EIP-1898 requireCanonical=true");
    }
    if object.contains_key("blockNumber") {
        bail!("Crystal EIP-1898 reference must not mix blockHash and blockNumber");
    }
    Ok(())
}

fn decode_result(response: &Value, context: &str) -> Result<Vec<u8>> {
    let value = monad_rpc::result_str(response, context)?;
    decode_hex(value).with_context(|| format!("decode {context} result bytes"))
}

fn decode_market_identity(
    fields: &[Token],
    market: &str,
    block_reference: &Value,
    calldata: String,
    rpc_latency_ns: u64,
) -> Result<CrystalMarketIdentity> {
    if fields.len() != 14 {
        bail!(
            "Crystal getMarket returned {} fields, expected 14",
            fields.len()
        );
    }
    Ok(CrystalMarketIdentity {
        singleton: CRYSTAL_SINGLETON_ADDRESS.to_owned(),
        market: market.to_owned(),
        quote_asset: token_address(&fields[0], "quoteAsset")?,
        base_asset: token_address(&fields[1], "baseAsset")?,
        market_type: token_uint(&fields[2], "marketType")?.to_string(),
        highest_bid: token_uint(&fields[3], "highestBid")?.to_string(),
        lowest_ask: token_uint(&fields[4], "lowestAsk")?.to_string(),
        scale_factor: token_uint(&fields[5], "scaleFactor")?.to_string(),
        tick_size: token_uint(&fields[6], "tickSize")?.to_string(),
        max_price: token_uint(&fields[7], "maxPrice")?.to_string(),
        min_size: token_uint(&fields[8], "minSize")?.to_string(),
        taker_fee: token_uint(&fields[9], "takerFee")?.to_string(),
        maker_rebate: token_uint(&fields[10], "makerRebate")?.to_string(),
        reserve_quote: token_uint(&fields[11], "reserveQuote")?.to_string(),
        reserve_base: token_uint(&fields[12], "reserveBase")?.to_string(),
        is_amm_enabled: token_bool(&fields[13], "isAMMEnabled")?,
        block_reference: block_reference.clone(),
        calldata,
        rpc_latency_ns,
    })
}

fn decode_quote_output(tokens: &[Token]) -> Result<(U256, U256)> {
    match tokens {
        [Token::Uint(amount_in), Token::Uint(amount_out)] => Ok((*amount_in, *amount_out)),
        _ => bail!("Crystal getQuote returned an unexpected output layout"),
    }
}

fn classify_consumption(requested: U256, used: U256) -> Result<(U256, bool)> {
    if used > requested {
        bail!(
            "Crystal returned amountIn {} greater than requested {}",
            used,
            requested
        );
    }
    Ok((requested - used, used == requested))
}

fn token_address(token: &Token, field: &str) -> Result<String> {
    match token {
        Token::Address(value) => Ok(format!("{value:#x}")),
        _ => bail!("Crystal getMarket {field} was not an address"),
    }
}

fn token_uint(token: &Token, field: &str) -> Result<U256> {
    match token {
        Token::Uint(value) => Ok(*value),
        _ => bail!("Crystal getMarket {field} was not a uint256"),
    }
}

fn token_bool(token: &Token, field: &str) -> Result<bool> {
    match token {
        Token::Bool(value) => Ok(*value),
        _ => bail!("Crystal getMarket {field} was not a bool"),
    }
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

    fn block_reference() -> Value {
        json!({
            "blockHash":"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "requireCanonical":true
        })
    }

    fn identity(quote_asset: &str, base_asset: &str) -> CrystalMarketIdentity {
        CrystalMarketIdentity {
            singleton: CRYSTAL_SINGLETON_ADDRESS.to_owned(),
            market: CRYSTAL_MON_USDC_MARKET.to_owned(),
            quote_asset: quote_asset.to_owned(),
            base_asset: base_asset.to_owned(),
            market_type: "0".to_owned(),
            highest_bid: "1".to_owned(),
            lowest_ask: "2".to_owned(),
            scale_factor: "1000000".to_owned(),
            tick_size: "1".to_owned(),
            max_price: "100".to_owned(),
            min_size: "1".to_owned(),
            taker_fee: "99900".to_owned(),
            maker_rebate: "100000".to_owned(),
            reserve_quote: "0".to_owned(),
            reserve_base: "0".to_owned(),
            is_amm_enabled: false,
            block_reference: block_reference(),
            calldata: "0x".to_owned(),
            rpc_latency_ns: 1,
        }
    }

    #[test]
    fn quote_selector_and_all_six_arguments_round_trip() {
        let function = crystal_function("getQuote").unwrap();
        assert_eq!(function.short_signature(), GET_QUOTE_SELECTOR);
        let market = Address::from_str(CRYSTAL_MON_USDC_MARKET).unwrap();
        let amount = U256::MAX - U256::from(17_u8);
        let calldata = encode_quote_call(&function, market, true, amount).unwrap();
        let decoded = function.decode_input(&calldata[4..]).unwrap();
        assert_eq!(
            decoded,
            vec![
                Token::Address(market),
                Token::Bool(true),
                Token::Bool(true),
                Token::Bool(true),
                Token::Uint(amount),
                Token::Uint(U256::zero()),
            ]
        );
    }

    #[test]
    fn request_keeps_eip1898_object_exactly() {
        let reference = block_reference();
        let params = eth_call_params(CRYSTAL_SINGLETON_ADDRESS, "0x1234", &reference).unwrap();
        assert_eq!(params[1], reference);
        assert_eq!(params[0]["to"], CRYSTAL_SINGLETON_ADDRESS);
        assert_eq!(params[0]["data"], "0x1234");
        assert!(eth_call_params(
            CRYSTAL_SINGLETON_ADDRESS,
            "0x1234",
            &Value::String("latest".to_owned())
        )
        .is_err());
    }

    #[test]
    fn strict_market_identity_accepts_only_usdc_quote_and_wmon_base() {
        identity(USDC_ADDRESS, WMON_ADDRESS)
            .validate_mon_usdc()
            .unwrap();
        let zero = format!("{:#x}", Address::zero());
        assert!(identity(&zero, WMON_ADDRESS).validate_mon_usdc().is_err());
        assert!(identity(USDC_ADDRESS, &zero).validate_mon_usdc().is_err());
    }

    #[test]
    fn market_tuple_decodes_exact_fields() {
        let fields = vec![
            Token::Address(Address::from_str(USDC_ADDRESS).unwrap()),
            Token::Address(Address::from_str(WMON_ADDRESS).unwrap()),
            Token::Uint(U256::from(4_u8)),
            Token::Uint(U256::from(10_u8)),
            Token::Uint(U256::from(11_u8)),
            Token::Uint(U256::from(1_000_000_u64)),
            Token::Uint(U256::from(2_u8)),
            Token::Uint(U256::from(3_u8)),
            Token::Uint(U256::from(4_u8)),
            Token::Uint(U256::from(5_u8)),
            Token::Uint(U256::from(6_u8)),
            Token::Uint(U256::from(7_u8)),
            Token::Uint(U256::from(8_u8)),
            Token::Bool(true),
        ];
        let decoded = decode_market_identity(
            &fields,
            CRYSTAL_MON_USDC_MARKET,
            &block_reference(),
            "0x1234".to_owned(),
            9,
        )
        .unwrap();
        decoded.validate_mon_usdc().unwrap();
        assert_eq!(decoded.market_type, "4");
        assert_eq!(decoded.scale_factor, "1000000");
        assert!(decoded.is_amm_enabled);
    }

    #[test]
    fn quote_output_preserves_full_u256_precision() {
        let input = U256::MAX - U256::from(1_u8);
        let output = U256::MAX - U256::from(2_u8);
        let function = crystal_function("getQuote").unwrap();
        let encoded = ethers::abi::encode(&[Token::Uint(input), Token::Uint(output)]);
        let tokens = function.decode_output(&encoded).unwrap();
        let decoded = decode_quote_output(&tokens).unwrap();
        assert_eq!(decoded, (input, output));
        assert_eq!(decoded.1.to_string(), output.to_string());
    }

    #[test]
    fn consumption_requires_returned_input_to_equal_request() {
        assert_eq!(
            classify_consumption(U256::from(100_u8), U256::from(100_u8)).unwrap(),
            (U256::zero(), true)
        );
        assert_eq!(
            classify_consumption(U256::from(100_u8), U256::from(60_u8)).unwrap(),
            (U256::from(40_u8), false)
        );
        assert!(classify_consumption(U256::from(100_u8), U256::from(101_u8)).is_err());
    }
}
