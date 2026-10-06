use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::Value;

use super::{parsed_bbo, FeedError, ParsedFeedEvent};

#[derive(Debug, Deserialize)]
struct BinanceMessageKind {
    #[serde(rename = "e")]
    event: Option<String>,
    code: Option<i64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct BinanceBookTicker {
    #[serde(rename = "e")]
    event: String,
    #[serde(rename = "u")]
    update_id: u64,
    #[serde(rename = "E")]
    event_timestamp_ms: u64,
    #[serde(rename = "T")]
    transaction_timestamp_ms: u64,
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "ps")]
    pair_symbol: String,
    #[serde(rename = "b")]
    bid_price: String,
    #[serde(rename = "B")]
    bid_qty: String,
    #[serde(rename = "a")]
    ask_price: String,
    #[serde(rename = "A")]
    ask_qty: String,
    #[serde(rename = "st")]
    symbol_type: u8,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum BinanceRequestId {
    Integer(u64),
    Text(String),
}

impl BinanceRequestId {
    fn is_valid(&self) -> bool {
        match self {
            // The stream-control contract calls this an unsigned integer, so
            // zero is a valid caller-selected ID. Keep it in the portable
            // signed JSON range used by the rest of the integrations.
            Self::Integer(value) => *value <= i64::MAX as u64,
            Self::Text(value) => !value.is_empty() && value.len() <= 128,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct BinanceControlAck {
    result: Value,
    id: BinanceRequestId,
}

/// Parse one message from Binance's routed USD-M public `bookTicker` stream.
pub(super) fn parse_binance_message(
    text: &str,
    expected_symbol: &str,
) -> Result<ParsedFeedEvent, FeedError> {
    let kind: BinanceMessageKind = decode_strict(text, "Binance")?;
    if kind.event.is_some() {
        let message: BinanceBookTicker = decode_strict(text, "Binance bookTicker")?;
        if message.event != "bookTicker" {
            return Err(FeedError::Protocol(
                "unexpected Binance event type".to_owned(),
            ));
        }
        if message.symbol != expected_symbol || message.pair_symbol != expected_symbol {
            return Err(FeedError::Protocol(
                "Binance bookTicker symbol did not match the configured USD-M market".to_owned(),
            ));
        }
        if message.symbol_type != 1 {
            return Err(FeedError::Protocol(
                "Binance bookTicker was not a USD-M payload".to_owned(),
            ));
        }
        if message.update_id == 0 {
            return Err(FeedError::Protocol(
                "Binance bookTicker update ID must be positive".to_owned(),
            ));
        }
        return parsed_bbo(
            &message.symbol,
            (&message.bid_price, &message.bid_qty),
            (&message.ask_price, &message.ask_qty),
            message.event_timestamp_ms,
            Some(message.transaction_timestamp_ms),
            Some(message.update_id),
        )
        .map(ParsedFeedEvent::Bbo);
    }

    if let Some(code) = kind.code {
        return Err(FeedError::Protocol(format!(
            "Binance upstream control reported error code {code}"
        )));
    }

    // A successful response to a WebSocket stream control request contains
    // exactly a null result and its request ID. The production collector uses
    // a raw URL and normally sees no such response, but accepting the official
    // acknowledgement keeps the parser correct if subscription style changes.
    let acknowledgement: BinanceControlAck = decode_strict(text, "Binance control")?;
    if !acknowledgement.result.is_null() || !acknowledgement.id.is_valid() {
        return Err(FeedError::Protocol(
            "invalid Binance WebSocket control acknowledgement".to_owned(),
        ));
    }
    Ok(ParsedFeedEvent::Ignore)
}

#[derive(Debug, Deserialize)]
struct BybitMessageKind {
    topic: Option<String>,
    op: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct BybitOrderBook {
    topic: String,
    #[serde(rename = "type")]
    message_type: String,
    ts: u64,
    data: BybitOrderBookData,
    cts: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct BybitOrderBookData {
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "b")]
    bids: [[String; 2]; 1],
    #[serde(rename = "a")]
    asks: [[String; 2]; 1],
    #[serde(rename = "u")]
    update_id: u64,
    seq: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct BybitControl {
    success: bool,
    ret_msg: String,
    conn_id: String,
    #[serde(rename = "req_id")]
    _request_id: String,
    op: String,
}

/// Parse one message from Bybit's public linear `orderbook.1` stream.
pub(super) fn parse_bybit_message(
    text: &str,
    expected_symbol: &str,
) -> Result<ParsedFeedEvent, FeedError> {
    let kind: BybitMessageKind = decode_strict(text, "Bybit")?;
    if kind.topic.is_some() {
        let message: BybitOrderBook = decode_strict(text, "Bybit orderbook.1")?;
        let expected_topic = format!("orderbook.1.{expected_symbol}");
        if message.topic != expected_topic || message.data.symbol != expected_symbol {
            return Err(FeedError::Protocol(
                "Bybit orderbook symbol or topic did not match the configured market".to_owned(),
            ));
        }
        if message.message_type != "snapshot" {
            return Err(FeedError::Protocol(
                "Bybit orderbook.1 message was not a snapshot".to_owned(),
            ));
        }
        if message.data.update_id == 0 || message.data.seq == 0 {
            return Err(FeedError::Protocol(
                "Bybit orderbook update identifiers must be positive".to_owned(),
            ));
        }
        let bid = &message.data.bids[0];
        let ask = &message.data.asks[0];
        return parsed_bbo(
            &message.data.symbol,
            (&bid[0], &bid[1]),
            (&ask[0], &ask[1]),
            message.ts,
            Some(message.cts),
            // Bybit deliberately repeats the same `u` after three seconds of
            // L1 inactivity. The feed runner permits equality for this venue.
            Some(message.data.update_id),
        )
        .map(ParsedFeedEvent::Bbo);
    }

    if kind.op.is_some() {
        let control: BybitControl = decode_strict(text, "Bybit control")?;
        if control.conn_id.is_empty() {
            return Err(FeedError::Protocol(
                "Bybit control response had an empty connection ID".to_owned(),
            ));
        }
        return match control.op.as_str() {
            "subscribe" if control.success => Ok(ParsedFeedEvent::SubscriptionAck),
            "subscribe" => Err(FeedError::Protocol(
                "Bybit rejected the orderbook subscription".to_owned(),
            )),
            "ping" if control.success && control.ret_msg == "pong" => Ok(ParsedFeedEvent::Pong),
            "ping" => Err(FeedError::Protocol(
                "Bybit rejected or malformed a heartbeat response".to_owned(),
            )),
            _ => Err(FeedError::Protocol(
                "unexpected Bybit WebSocket control operation".to_owned(),
            )),
        };
    }

    Err(FeedError::Protocol(
        "unrecognized Bybit WebSocket message".to_owned(),
    ))
}

fn decode_strict<T: DeserializeOwned>(text: &str, label: &str) -> Result<T, FeedError> {
    serde_json::from_str(text)
        .map_err(|error| FeedError::Protocol(format!("invalid {label} JSON: {error}")))
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use super::*;

    const BINANCE_BBO: &str = r#"{
        "e":"bookTicker",
        "u":400900217,
        "E":1568014460893,
        "T":1568014460891,
        "s":"BTCUSDT",
        "ps":"BTCUSDT",
        "b":"99999.90",
        "B":"0.125",
        "a":"100000.00",
        "A":"0.250",
        "st":1
    }"#;

    const BYBIT_BBO: &str = r#"{
        "topic":"orderbook.1.BTCUSDT",
        "ts":1672304484978,
        "type":"snapshot",
        "data":{
            "s":"BTCUSDT",
            "b":[["99999.90","0.125"]],
            "a":[["100000.00","0.250"]],
            "u":18521288,
            "seq":7961638724
        },
        "cts":1672304484976
    }"#;

    #[test]
    fn parses_current_binance_usdm_book_ticker() {
        let ParsedFeedEvent::Bbo(bbo) =
            parse_binance_message(BINANCE_BBO, "BTCUSDT").expect("valid Binance BBO")
        else {
            panic!("expected BBO");
        };
        assert_eq!(bbo.symbol, "BTCUSDT");
        assert_eq!(bbo.bid_price, Decimal::new(9_999_990, 2));
        assert_eq!(bbo.ask_price, Decimal::new(10_000_000, 2));
        assert_eq!(bbo.exchange_timestamp_ms, 1_568_014_460_893);
        assert_eq!(bbo.origin_timestamp_ms, Some(1_568_014_460_891));
        assert_eq!(bbo.source_cursor, Some(400_900_217));
    }

    #[test]
    fn binance_requires_current_usdm_identity_and_schema() {
        assert!(
            parse_binance_message(&BINANCE_BBO.replace("\"st\":1", "\"st\":2"), "BTCUSDT").is_err()
        );
        assert!(parse_binance_message(
            &BINANCE_BBO.replace("\"ps\":\"BTCUSDT\"", "\"ps\":\"ETHUSDT\""),
            "BTCUSDT"
        )
        .is_err());
        assert!(parse_binance_message(
            &BINANCE_BBO.replace("\"st\":1", "\"st\":1,\"unexpected\":true"),
            "BTCUSDT"
        )
        .is_err());
    }

    #[test]
    fn binance_ignores_successful_controls_and_rejects_errors() {
        assert!(matches!(
            parse_binance_message(r#"{"result":null,"id":0}"#, "BTCUSDT")
                .expect("valid acknowledgement"),
            ParsedFeedEvent::Ignore
        ));
        assert!(
            parse_binance_message(r#"{"code":2,"msg":"invalid subscription"}"#, "BTCUSDT").is_err()
        );
    }

    #[test]
    fn parses_bybit_level_one_snapshot() {
        let ParsedFeedEvent::Bbo(bbo) =
            parse_bybit_message(BYBIT_BBO, "BTCUSDT").expect("valid Bybit BBO")
        else {
            panic!("expected BBO");
        };
        assert_eq!(bbo.symbol, "BTCUSDT");
        assert_eq!(bbo.bid_qty, Decimal::new(125, 3));
        assert_eq!(bbo.ask_qty, Decimal::new(250, 3));
        assert_eq!(bbo.exchange_timestamp_ms, 1_672_304_484_978);
        assert_eq!(bbo.origin_timestamp_ms, Some(1_672_304_484_976));
        assert_eq!(bbo.source_cursor, Some(18_521_288));
    }

    #[test]
    fn bybit_repeated_update_id_remains_a_valid_snapshot() {
        let first = parse_bybit_message(BYBIT_BBO, "BTCUSDT").expect("first snapshot");
        let repeated = parse_bybit_message(BYBIT_BBO, "BTCUSDT").expect("repeated snapshot");
        let (ParsedFeedEvent::Bbo(first), ParsedFeedEvent::Bbo(repeated)) = (first, repeated)
        else {
            panic!("expected BBOs");
        };
        assert_eq!(first.source_cursor, repeated.source_cursor);
    }

    #[test]
    fn bybit_recognizes_controls_and_rejects_failed_subscription() {
        let subscribed = r#"{
            "success":true,"ret_msg":"","conn_id":"connection-1",
            "req_id":"live-md-bbo","op":"subscribe"
        }"#;
        assert!(matches!(
            parse_bybit_message(subscribed, "BTCUSDT").expect("subscription ack"),
            ParsedFeedEvent::SubscriptionAck
        ));

        let pong = r#"{
            "success":true,"ret_msg":"pong","conn_id":"connection-1",
            "req_id":"","op":"ping"
        }"#;
        assert!(matches!(
            parse_bybit_message(pong, "BTCUSDT").expect("pong"),
            ParsedFeedEvent::Pong
        ));

        let rejected = r#"{
            "success":false,"ret_msg":"rejected","conn_id":"connection-1",
            "req_id":"live-md-bbo","op":"subscribe"
        }"#;
        assert!(parse_bybit_message(rejected, "BTCUSDT").is_err());
    }

    #[test]
    fn bybit_rejects_non_l1_snapshot_or_wrong_market() {
        assert!(parse_bybit_message(
            &BYBIT_BBO.replace("\"type\":\"snapshot\"", "\"type\":\"delta\""),
            "BTCUSDT"
        )
        .is_err());
        assert!(parse_bybit_message(BYBIT_BBO, "ETHUSDT").is_err());
        assert!(parse_bybit_message(
            &BYBIT_BBO.replace(
                "\"cts\":1672304484976",
                "\"cts\":1672304484976,\"unexpected\":true"
            ),
            "BTCUSDT"
        )
        .is_err());
    }
}
