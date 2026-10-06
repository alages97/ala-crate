//! Small typed JSON-RPC boundary shared by Monad research tools.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use anyhow::{anyhow, bail, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

pub const EIP1967_IMPLEMENTATION_SLOT: &str =
    "0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc";

static NEXT_RPC_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MonadBlockRef {
    pub number: String,
    pub number_u64: u64,
    pub hash: String,
    pub parent_hash: String,
    pub timestamp: String,
    pub timestamp_unix_s: u64,
    pub base_fee_per_gas_wei: Option<String>,
}

impl MonadBlockRef {
    /// EIP-1898 pins calls to a block hash and rejects a non-canonical block.
    pub fn eip1898_parameter(&self) -> Value {
        json!({"blockHash":self.hash,"requireCanonical":true})
    }
}

#[derive(Debug, Clone)]
pub struct RpcResponse {
    pub value: Value,
    pub latency_ns: u64,
}

pub async fn chain_id(client: &reqwest::Client, rpc_url: &str) -> Result<u64> {
    let response = request(client, rpc_url, "eth_chainId", json!([])).await?;
    let value = result_str(&response.value, "eth_chainId")?;
    parse_hex_u64(value, "eth_chainId")
}

pub async fn latest_block(client: &reqwest::Client, rpc_url: &str) -> Result<(MonadBlockRef, u64)> {
    let response = request(
        client,
        rpc_url,
        "eth_getBlockByNumber",
        json!(["latest", false]),
    )
    .await?;
    let block = parse_block_result(&response.value)?;
    Ok((block, response.latency_ns))
}

pub async fn block_by_number(
    client: &reqwest::Client,
    rpc_url: &str,
    number: &str,
) -> Result<(MonadBlockRef, u64)> {
    let response = request(
        client,
        rpc_url,
        "eth_getBlockByNumber",
        json!([number, false]),
    )
    .await?;
    let block = parse_block_result(&response.value)?;
    Ok((block, response.latency_ns))
}

pub async fn code_at(
    client: &reqwest::Client,
    rpc_url: &str,
    address: &str,
    block_reference: &Value,
) -> Result<(String, String, u64)> {
    let response = request(
        client,
        rpc_url,
        "eth_getCode",
        json!([address, block_reference]),
    )
    .await?;
    let code = result_str(&response.value, "eth_getCode")?.to_owned();
    let code_bytes = decode_hex(&code).context("decode contract bytecode")?;
    let hash = ethers::utils::keccak256(code_bytes);
    Ok((code, encode_hex(&hash), response.latency_ns))
}

pub async fn eip1967_implementation_at(
    client: &reqwest::Client,
    rpc_url: &str,
    proxy: &str,
    block_reference: &Value,
) -> Result<(Option<String>, u64)> {
    let response = request(
        client,
        rpc_url,
        "eth_getStorageAt",
        json!([proxy, EIP1967_IMPLEMENTATION_SLOT, block_reference]),
    )
    .await?;
    let word = result_str(&response.value, "eth_getStorageAt")?;
    let bytes = decode_hex(word).context("decode EIP-1967 implementation slot")?;
    if bytes.len() != 32 {
        bail!(
            "EIP-1967 implementation slot returned {} bytes",
            bytes.len()
        );
    }
    let address = &bytes[12..];
    let implementation = address
        .iter()
        .any(|byte| *byte != 0)
        .then(|| encode_hex(address));
    Ok((implementation, response.latency_ns))
}

pub async fn request(
    client: &reqwest::Client,
    rpc_url: &str,
    method: &str,
    params: Value,
) -> Result<RpcResponse> {
    if rpc_url.trim().is_empty() {
        bail!("Monad JSON-RPC URL must not be empty");
    }
    let id = NEXT_RPC_ID.fetch_add(1, Ordering::Relaxed);
    let started = Instant::now();
    let response = client
        .post(rpc_url)
        .json(&json!({"jsonrpc":"2.0","id":id,"method":method,"params":params}))
        .send()
        .await
        .with_context(|| format!("send Monad JSON-RPC method {method}"))?;
    let status = response.status();
    let bytes = response
        .bytes()
        .await
        .with_context(|| format!("read Monad JSON-RPC body for {method}"))?;
    let latency_ns = duration_ns(started.elapsed());
    if !status.is_success() {
        let body = String::from_utf8_lossy(&bytes);
        let body = body.chars().take(2_048).collect::<String>();
        bail!("Monad JSON-RPC {method} returned HTTP {status}: {body}");
    }
    let value: Value = serde_json::from_slice(&bytes)
        .with_context(|| format!("decode Monad JSON-RPC body for {method}"))?;
    if let Some(error) = value.get("error") {
        bail!("Monad JSON-RPC {method} error: {error}");
    }
    Ok(RpcResponse { value, latency_ns })
}

pub fn result_str<'a>(response: &'a Value, context: &str) -> Result<&'a str> {
    if let Some(error) = response.get("error") {
        bail!("{context} JSON-RPC error: {error}");
    }
    response
        .get("result")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("{context} response has no string result: {response}"))
}

fn parse_block_result(response: &Value) -> Result<MonadBlockRef> {
    if let Some(error) = response.get("error") {
        bail!("eth_getBlockByNumber JSON-RPC error: {error}");
    }
    let block = response
        .get("result")
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow!("eth_getBlockByNumber returned no block: {response}"))?;
    let number = required_string(block, "number")?.to_owned();
    let hash = required_string(block, "hash")?.to_owned();
    let parent_hash = required_string(block, "parentHash")?.to_owned();
    let timestamp = required_string(block, "timestamp")?.to_owned();
    let number_u64 = parse_hex_u64(&number, "block number")?;
    let timestamp_unix_s = parse_hex_u64(&timestamp, "block timestamp")?;
    let base_fee_per_gas_wei = block
        .get("baseFeePerGas")
        .and_then(Value::as_str)
        .map(parse_hex_quantity_decimal)
        .transpose()
        .context("decode block baseFeePerGas")?;
    Ok(MonadBlockRef {
        number,
        number_u64,
        hash,
        parent_hash,
        timestamp,
        timestamp_unix_s,
        base_fee_per_gas_wei,
    })
}

fn required_string<'a>(object: &'a serde_json::Map<String, Value>, field: &str) -> Result<&'a str> {
    object
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("block field {field} is missing or not a string"))
}

fn parse_hex_quantity_decimal(value: &str) -> Result<String> {
    let digits = value
        .strip_prefix("0x")
        .or_else(|| value.strip_prefix("0X"))
        .unwrap_or(value);
    if digits.is_empty() {
        bail!("empty hex quantity");
    }
    let parsed = ethers::types::U256::from_str_radix(digits, 16)
        .with_context(|| format!("invalid hex quantity {value}"))?;
    Ok(parsed.to_string())
}

fn parse_hex_u64(value: &str, field: &str) -> Result<u64> {
    let digits = value
        .strip_prefix("0x")
        .or_else(|| value.strip_prefix("0X"))
        .unwrap_or(value);
    u64::from_str_radix(digits, 16).with_context(|| format!("invalid {field} {value}"))
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

fn duration_ns(duration: std::time::Duration) -> u64 {
    duration.as_nanos().min(u128::from(u64::MAX)) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_block_and_builds_canonical_reference() {
        let response = json!({
            "jsonrpc":"2.0",
            "id":1,
            "result":{
                "number":"0x2a",
                "hash":"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "parentHash":"0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                "timestamp":"0x64",
                "baseFeePerGas":"0x174876e800"
            }
        });
        let block = parse_block_result(&response).expect("valid block");
        assert_eq!(block.number_u64, 42);
        assert_eq!(block.timestamp_unix_s, 100);
        assert_eq!(block.base_fee_per_gas_wei.as_deref(), Some("100000000000"));
        assert_eq!(
            block.eip1898_parameter(),
            json!({"blockHash":block.hash,"requireCanonical":true})
        );
    }

    #[test]
    fn null_block_is_rejected() {
        assert!(parse_block_result(&json!({"result":null})).is_err());
    }
}
