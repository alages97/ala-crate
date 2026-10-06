//! Minimal read-only Uniswap v4 quoting support for Monad mainnet.
//!
//! The deployment addresses come from Uniswap's official deployment registry:
//! <https://developers.uniswap.org/docs/protocols/v4/deployments#monad-143>.
//! The call ABI follows `IV4Quoter.QuoteExactSingleParams` in the official
//! `Uniswap/v4-periphery` repository. `quoteExactInputSingle` is deliberately
//! not declared `view` in Solidity because it simulates a swap and catches an
//! internal revert; invoking it with `eth_call` remains read-only.

use std::str::FromStr;

use anyhow::{anyhow, bail, Context, Result};
use ethers::abi::{Abi, Function, Token};
use ethers::types::{Address, U256};
use serde_json::{json, Value};

/// Monad mainnet chain ID.
pub const MONAD_CHAIN_ID: u64 = 143;

/// Official Uniswap v4 PoolManager deployment on Monad mainnet.
pub const POOL_MANAGER_ADDRESS: &str = "0x188d586ddcf52439676ca21a244753fa19f9ea8e";

/// Official Uniswap v4 V4Quoter deployment on Monad mainnet.
pub const QUOTER_ADDRESS: &str = "0xa222dd357a9076d1091ed6aa2e16c9742dd26891";

/// Uniswap v4 represents a chain's native currency with the zero address.
pub const NATIVE_MON_ADDRESS: &str = "0x0000000000000000000000000000000000000000";

/// Native USDC on Monad mainnet (six decimals).
pub const USDC_ADDRESS: &str = "0x754704Bc059F8C67012fEd69BC8A327a5aafb603";

/// The MON/USDC pool's LP fee, in hundredths of a basis point (0.05%).
pub const POOL_FEE: u32 = 500;

/// The MON/USDC pool's tick spacing.
pub const POOL_TICK_SPACING: i32 = 10;

/// This pool has no hook contract.
pub const POOL_HOOKS_ADDRESS: &str = "0x0000000000000000000000000000000000000000";

/// `quoteExactInputSingle(((address,address,uint24,int24,address),bool,uint128,bytes))`.
pub const QUOTE_EXACT_INPUT_SINGLE_SELECTOR: [u8; 4] = [0xaa, 0x9d, 0x21, 0xcb];

// Pin the ABI shape in this module instead of depending on an explorer ABI.
// Source validated against Uniswap/v4-periphery IV4Quoter.sol at commit
// 9969eec44cfdf07e24b41de47f40276a58401976 (2026-09-28).
const V4_QUOTER_ABI: &str = r#"
[
  {
    "type": "function",
    "name": "quoteExactInputSingle",
    "stateMutability": "nonpayable",
    "inputs": [
      {
        "name": "params",
        "type": "tuple",
        "components": [
          {
            "name": "poolKey",
            "type": "tuple",
            "components": [
              {"name": "currency0", "type": "address"},
              {"name": "currency1", "type": "address"},
              {"name": "fee", "type": "uint24"},
              {"name": "tickSpacing", "type": "int24"},
              {"name": "hooks", "type": "address"}
            ]
          },
          {"name": "zeroForOne", "type": "bool"},
          {"name": "exactAmount", "type": "uint128"},
          {"name": "hookData", "type": "bytes"}
        ]
      }
    ],
    "outputs": [
      {"name": "amountOut", "type": "uint256"},
      {"name": "gasEstimate", "type": "uint256"}
    ]
  }
]
"#;

/// Quote an exact-input swap against the canonical Monad MON/USDC v4 pool.
///
/// `zero_for_one = true` quotes native MON to USDC; `false` quotes USDC to
/// native MON. Amounts are raw token units (MON has 18 decimals, USDC has 6).
/// `block` accepts any JSON-RPC block tag or hex block number and defaults to
/// `latest`.
///
/// A successfully decoded JSON-RPC result populates `amount_out` and
/// `gas_estimate` as decimal strings so values cannot lose precision in JSON.
/// RPC-level quote failures are returned as structured JSON with those fields
/// set to `null`; transport failures and malformed HTTP response JSON are
/// returned as `Err`.
pub async fn quote_exact_input_single(
    client: &reqwest::Client,
    rpc_url: &str,
    zero_for_one: bool,
    amount_in: u128,
    block: Option<&str>,
) -> Result<Value> {
    let block_reference = Value::String(block.unwrap_or("latest").to_owned());
    quote_exact_input_single_at(client, rpc_url, zero_for_one, amount_in, &block_reference).await
}

/// Quote at an explicit JSON-RPC block reference.
///
/// This variant accepts an EIP-1898 block object, allowing callers to pin the
/// quote to a canonical block hash rather than only a block number. The legacy
/// string-tag wrapper above is retained for the existing recorder.
pub async fn quote_exact_input_single_at(
    client: &reqwest::Client,
    rpc_url: &str,
    zero_for_one: bool,
    amount_in: u128,
    block_reference: &Value,
) -> Result<Value> {
    if rpc_url.trim().is_empty() {
        bail!("Monad JSON-RPC URL must not be empty");
    }
    if amount_in == 0 {
        bail!("exact-input quote amount must be greater than zero");
    }

    if block_reference.is_null()
        || block_reference
            .as_str()
            .is_some_and(|block_tag| block_tag.trim().is_empty())
    {
        bail!("block reference must not be empty or null");
    }

    let function = quote_function()?;
    let calldata = encode_quote_call(&function, zero_for_one, amount_in)?;
    let calldata_hex = encode_hex(&calldata);
    let request_params = json!([
        {
            "to": QUOTER_ADDRESS,
            "data": calldata_hex,
        },
        block_reference,
    ]);
    let request = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_call",
        "params": request_params,
    });

    let response = client
        .post(rpc_url)
        .json(&request)
        .send()
        .await
        .with_context(|| format!("sending Monad Uniswap v4 quote to {rpc_url}"))?;
    let status = response.status();
    let response_metadata = json!({
        "http_status": status.as_u16(),
        "http_status_text": status.to_string(),
        "http_version": format!("{:?}", response.version()),
        "content_type": response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok()),
    });
    let raw_response: Value = response
        .json()
        .await
        .context("decoding Monad JSON-RPC quote response")?;

    let (amount_out, gas_estimate, decode_error) = decode_rpc_quote(&function, &raw_response);
    let rpc_error = raw_response.get("error").cloned();
    let ok = status.is_success()
        && rpc_error.is_none()
        && decode_error.is_none()
        && amount_out.is_some();

    Ok(json!({
        "ok": ok,
        "chain_id": MONAD_CHAIN_ID,
        "pool_manager": POOL_MANAGER_ADDRESS,
        "quoter": QUOTER_ADDRESS,
        "pool_key": {
            "currency0": NATIVE_MON_ADDRESS,
            "currency1": USDC_ADDRESS,
            "fee": POOL_FEE,
            "tick_spacing": POOL_TICK_SPACING,
            "hooks": POOL_HOOKS_ADDRESS,
        },
        "zero_for_one": zero_for_one,
        "amount_in": amount_in.to_string(),
        "block_tag": block_reference.as_str(),
        "block_reference": block_reference,
        "request_params": request_params,
        "response_metadata": response_metadata,
        "raw_response": raw_response,
        "amount_out": amount_out,
        "gas_estimate": gas_estimate,
        "rpc_error": rpc_error,
        "decode_error": decode_error,
    }))
}

fn quote_function() -> Result<Function> {
    let abi: Abi = serde_json::from_str(V4_QUOTER_ABI).context("parsing embedded V4Quoter ABI")?;
    abi.function("quoteExactInputSingle")
        .cloned()
        .context("embedded V4Quoter ABI is missing quoteExactInputSingle")
}

fn encode_quote_call(function: &Function, zero_for_one: bool, amount_in: u128) -> Result<Vec<u8>> {
    let native_mon = Address::from_str(NATIVE_MON_ADDRESS).context("invalid native MON address")?;
    let usdc = Address::from_str(USDC_ADDRESS).context("invalid Monad USDC address")?;
    let hooks = Address::from_str(POOL_HOOKS_ADDRESS).context("invalid pool hooks address")?;

    function
        .encode_input(&[Token::Tuple(vec![
            Token::Tuple(vec![
                Token::Address(native_mon),
                Token::Address(usdc),
                Token::Uint(U256::from(POOL_FEE)),
                Token::Int(signed_abi_word(POOL_TICK_SPACING)),
                Token::Address(hooks),
            ]),
            Token::Bool(zero_for_one),
            Token::Uint(U256::from(amount_in)),
            Token::Bytes(Vec::new()),
        ])])
        .context("encoding V4Quoter quoteExactInputSingle calldata")
}

fn signed_abi_word(value: i32) -> U256 {
    if value >= 0 {
        U256::from(value as u32)
    } else {
        // ABI signed integers are encoded as two's-complement values, sign
        // extended to the full 256-bit word.
        U256::MAX - U256::from(value.unsigned_abs()) + U256::one()
    }
}

fn decode_rpc_quote(
    function: &Function,
    raw_response: &Value,
) -> (Option<String>, Option<String>, Option<String>) {
    if let Some(error) = raw_response.get("error") {
        return (
            None,
            None,
            Some(format!("JSON-RPC returned an error: {error}")),
        );
    }

    let Some(result_hex) = raw_response.get("result").and_then(Value::as_str) else {
        return (
            None,
            None,
            Some("JSON-RPC response did not contain a string result".to_owned()),
        );
    };

    let decoded = decode_hex(result_hex)
        .and_then(|bytes| {
            function
                .decode_output(&bytes)
                .context("ABI-decoding V4Quoter output")
        })
        .and_then(|tokens| match tokens.as_slice() {
            [Token::Uint(amount_out), Token::Uint(gas_estimate)] => {
                Ok((amount_out.to_string(), gas_estimate.to_string()))
            }
            _ => Err(anyhow!(
                "V4Quoter returned an unexpected output token layout"
            )),
        });

    match decoded {
        Ok((amount_out, gas_estimate)) => (Some(amount_out), Some(gas_estimate), None),
        Err(error) => (None, None, Some(format!("{error:#}"))),
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
        .map(|pair| {
            let high = decode_nibble(pair[0])?;
            let low = decode_nibble(pair[1])?;
            Ok((high << 4) | low)
        })
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
    fn official_signature_has_expected_selector() {
        let function = quote_function().expect("embedded ABI should parse");
        assert_eq!(
            function.short_signature(),
            QUOTE_EXACT_INPUT_SINGLE_SELECTOR
        );
    }

    #[test]
    fn calldata_round_trips_through_official_tuple_shape() {
        let function = quote_function().expect("embedded ABI should parse");
        let amount_in = 1_000_000_000_000_000_000_u128;
        let calldata =
            encode_quote_call(&function, true, amount_in).expect("calldata should encode");

        assert_eq!(&calldata[..4], &QUOTE_EXACT_INPUT_SINGLE_SELECTOR);
        let decoded = function
            .decode_input(&calldata[4..])
            .expect("calldata arguments should decode");
        let Token::Tuple(params) = &decoded[0] else {
            panic!("top-level argument was not a tuple");
        };
        let Token::Tuple(pool_key) = &params[0] else {
            panic!("pool key was not a tuple");
        };

        assert_eq!(pool_key[0], Token::Address(Address::zero()));
        assert_eq!(
            pool_key[1],
            Token::Address(Address::from_str(USDC_ADDRESS).expect("valid USDC address"))
        );
        assert_eq!(pool_key[2], Token::Uint(U256::from(POOL_FEE)));
        assert_eq!(
            pool_key[3],
            Token::Int(U256::from(POOL_TICK_SPACING as u32))
        );
        assert_eq!(pool_key[4], Token::Address(Address::zero()));
        assert_eq!(params[1], Token::Bool(true));
        assert_eq!(params[2], Token::Uint(U256::from(amount_in)));
        assert_eq!(params[3], Token::Bytes(Vec::new()));

        // Keep this assertion independent from `Function::short_signature` so
        // a tuple-shape regression is caught at the wire boundary too.
        assert!(encode_hex(&calldata).starts_with("0xaa9d21cb"));
    }

    #[test]
    fn quote_output_decodes_without_json_precision_loss() {
        let function = quote_function().expect("embedded ABI should parse");
        let amount_out = U256::MAX - U256::from(7_u8);
        let gas_estimate = U256::from(49_583_u64);
        let encoded = ethers::abi::encode(&[Token::Uint(amount_out), Token::Uint(gas_estimate)]);
        let response = json!({"jsonrpc": "2.0", "id": 1, "result": encode_hex(&encoded)});

        let (decoded_amount, decoded_gas, error) = decode_rpc_quote(&function, &response);
        let expected_amount = amount_out.to_string();
        assert_eq!(decoded_amount.as_deref(), Some(expected_amount.as_str()));
        assert_eq!(decoded_gas.as_deref(), Some("49583"));
        assert!(error.is_none());
    }

    #[test]
    fn rpc_error_is_preserved_as_decode_context() {
        let function = quote_function().expect("embedded ABI should parse");
        let response = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "error": {"code": -32000, "message": "execution reverted"}
        });

        let (amount_out, gas_estimate, error) = decode_rpc_quote(&function, &response);
        assert!(amount_out.is_none());
        assert!(gas_estimate.is_none());
        assert!(error
            .as_deref()
            .is_some_and(|message| message.contains("execution reverted")));
    }
}
