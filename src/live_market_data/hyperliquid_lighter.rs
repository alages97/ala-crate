use serde::Deserialize;

use super::{parsed_bbo, FeedError, ParsedFeedEvent};

#[derive(Debug, Deserialize)]
#[serde(tag = "channel")]
enum HyperliquidMessage {
    #[serde(rename = "bbo")]
    Bbo { data: HyperliquidBbo },
    #[serde(rename = "subscriptionResponse")]
    SubscriptionResponse { data: HyperliquidSubscriptionAck },
    #[serde(rename = "pong")]
    Pong,
    #[serde(rename = "error")]
    Error,
    #[serde(other)]
    Other,
}

#[derive(Debug, Deserialize)]
struct HyperliquidBbo {
    coin: String,
    time: u64,
    bbo: [Option<HyperliquidLevel>; 2],
}

#[derive(Debug, Deserialize)]
struct HyperliquidLevel {
    px: String,
    sz: String,
    #[allow(dead_code)]
    n: u64,
}

#[derive(Debug, Deserialize)]
struct HyperliquidSubscriptionAck {
    method: String,
    subscription: HyperliquidSubscription,
}

#[derive(Debug, Deserialize)]
struct HyperliquidSubscription {
    #[serde(rename = "type")]
    kind: String,
    coin: String,
}

pub(super) fn parse_hyperliquid_message(
    text: &str,
    expected_symbol: &str,
) -> Result<ParsedFeedEvent, FeedError> {
    let value: serde_json::Value =
        serde_json::from_str(text).map_err(|error| protocol_error("Hyperliquid", error))?;
    if value.get("error").is_some() {
        return Err(FeedError::Protocol(
            "Hyperliquid returned an upstream error".to_owned(),
        ));
    }
    let message: HyperliquidMessage =
        serde_json::from_value(value).map_err(|error| protocol_error("Hyperliquid", error))?;

    match message {
        HyperliquidMessage::Bbo { data } => {
            if data.coin != expected_symbol {
                return Err(FeedError::Protocol(format!(
                    "Hyperliquid BBO symbol mismatch: expected {expected_symbol}, received {}",
                    data.coin
                )));
            }
            let [bid, ask] = data.bbo;
            let bid = bid.ok_or_else(|| {
                FeedError::Protocol("Hyperliquid BBO has no bid level".to_owned())
            })?;
            let ask = ask.ok_or_else(|| {
                FeedError::Protocol("Hyperliquid BBO has no ask level".to_owned())
            })?;
            parsed_bbo(
                &data.coin,
                (&bid.px, &bid.sz),
                (&ask.px, &ask.sz),
                data.time,
                None,
                None,
            )
            .map(ParsedFeedEvent::Bbo)
        }
        HyperliquidMessage::SubscriptionResponse { data } => {
            if data.method != "subscribe"
                || data.subscription.kind != "bbo"
                || data.subscription.coin != expected_symbol
            {
                return Err(FeedError::Protocol(
                    "Hyperliquid acknowledged an unexpected subscription".to_owned(),
                ));
            }
            Ok(ParsedFeedEvent::SubscriptionAck)
        }
        HyperliquidMessage::Pong => Ok(ParsedFeedEvent::Pong),
        HyperliquidMessage::Error => Err(FeedError::Protocol(
            "Hyperliquid returned an upstream error".to_owned(),
        )),
        HyperliquidMessage::Other => Ok(ParsedFeedEvent::Ignore),
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
enum LighterMessage {
    #[serde(rename = "connected")]
    Connected { session_id: String },
    #[serde(rename = "ping")]
    Ping,
    #[serde(rename = "pong")]
    Pong,
    #[serde(rename = "subscribed/ticker")]
    SubscribedTicker(LighterTickerMessage),
    #[serde(rename = "update/ticker")]
    UpdateTicker(LighterTickerMessage),
    #[serde(other)]
    Other,
}

#[derive(Debug, Deserialize)]
struct LighterTickerMessage {
    channel: String,
    last_updated_at: u64,
    nonce: u64,
    ticker: LighterTicker,
}

#[derive(Debug, Deserialize)]
struct LighterTicker {
    s: String,
    a: LighterLevel,
    b: LighterLevel,
    last_updated_at: u64,
}

#[derive(Debug, Deserialize)]
struct LighterLevel {
    price: String,
    size: String,
}

pub(super) fn parse_lighter_message(
    text: &str,
    expected_symbol: &str,
    market_index: u8,
) -> Result<ParsedFeedEvent, FeedError> {
    let value: serde_json::Value =
        serde_json::from_str(text).map_err(|error| protocol_error("Lighter", error))?;
    if value.get("error").is_some() {
        return Err(FeedError::Protocol(
            "Lighter returned an upstream error".to_owned(),
        ));
    }
    let message: LighterMessage =
        serde_json::from_value(value).map_err(|error| protocol_error("Lighter", error))?;

    match message {
        LighterMessage::Connected { session_id } => {
            if session_id.trim().is_empty() {
                return Err(FeedError::Protocol(
                    "Lighter connected message has an empty session_id".to_owned(),
                ));
            }
            Ok(ParsedFeedEvent::Connected)
        }
        LighterMessage::Ping => Ok(ParsedFeedEvent::ServerPing),
        LighterMessage::Pong => Ok(ParsedFeedEvent::Pong),
        LighterMessage::SubscribedTicker(data) | LighterMessage::UpdateTicker(data) => {
            parse_lighter_ticker(data, expected_symbol, market_index)
        }
        LighterMessage::Other => Ok(ParsedFeedEvent::Ignore),
    }
}

fn parse_lighter_ticker(
    data: LighterTickerMessage,
    expected_symbol: &str,
    market_index: u8,
) -> Result<ParsedFeedEvent, FeedError> {
    let expected_channel = format!("ticker:{market_index}");
    if data.channel != expected_channel {
        return Err(FeedError::Protocol(format!(
            "Lighter ticker channel mismatch: expected {expected_channel}, received {}",
            data.channel
        )));
    }
    if data.ticker.s != expected_symbol {
        return Err(FeedError::Protocol(format!(
            "Lighter ticker symbol mismatch: expected {expected_symbol}, received {}",
            data.ticker.s
        )));
    }
    if data.last_updated_at != data.ticker.last_updated_at {
        return Err(FeedError::Protocol(
            "Lighter ticker timestamps disagree".to_owned(),
        ));
    }
    if data.nonce == 0 {
        return Err(FeedError::Protocol(
            "Lighter ticker nonce must be positive".to_owned(),
        ));
    }

    let exchange_timestamp_ms = data.last_updated_at / 1_000;
    parsed_bbo(
        &data.ticker.s,
        (&data.ticker.b.price, &data.ticker.b.size),
        (&data.ticker.a.price, &data.ticker.a.size),
        exchange_timestamp_ms,
        None,
        Some(data.nonce),
    )
    .map(ParsedFeedEvent::Bbo)
}

#[derive(Debug, Deserialize)]
struct LighterMarketDetailsResponse {
    code: u16,
    order_book_details: Vec<LighterMarketDetail>,
}

#[derive(Debug, Deserialize)]
struct LighterMarketDetail {
    symbol: String,
    market_id: u64,
    market_type: String,
    status: String,
}

pub(super) fn validate_lighter_market_details(
    body: &[u8],
    expected_symbol: &str,
    expected_index: u8,
) -> Result<(), FeedError> {
    let response: LighterMarketDetailsResponse =
        serde_json::from_slice(body).map_err(|error| protocol_error("Lighter metadata", error))?;
    if response.code != 200 {
        return Err(FeedError::Protocol(format!(
            "Lighter metadata returned application code {}",
            response.code
        )));
    }

    let mut matching = response
        .order_book_details
        .iter()
        .filter(|detail| detail.market_id == u64::from(expected_index));
    let detail = matching.next().ok_or_else(|| {
        FeedError::Protocol(format!(
            "Lighter metadata did not contain market index {expected_index}"
        ))
    })?;
    if matching.next().is_some() {
        return Err(FeedError::Protocol(format!(
            "Lighter metadata contained duplicate market index {expected_index}"
        )));
    }
    if detail.symbol != expected_symbol {
        return Err(FeedError::Protocol(format!(
            "Lighter metadata symbol mismatch for market {expected_index}: expected {expected_symbol}, received {}",
            detail.symbol
        )));
    }
    if detail.market_type != "perp" {
        return Err(FeedError::Protocol(format!(
            "Lighter market {expected_index} is not a perpetual"
        )));
    }
    if detail.status != "active" {
        return Err(FeedError::Protocol(format!(
            "Lighter market {expected_index} is not active"
        )));
    }
    Ok(())
}

fn protocol_error(context: &str, error: serde_json::Error) -> FeedError {
    FeedError::Protocol(format!("invalid {context} JSON: {error}"))
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use super::*;

    #[test]
    fn parses_hyperliquid_bbo() {
        let event = parse_hyperliquid_message(
            r#"{"channel":"bbo","data":{"coin":"BTC","time":1789387665453,"bbo":[{"px":"77767.0","sz":"6.1386","n":27},{"px":"77768.0","sz":"1.10124","n":16}]}}"#,
            "BTC",
        )
        .expect("valid BBO");
        let ParsedFeedEvent::Bbo(bbo) = event else {
            panic!("expected BBO");
        };
        assert_eq!(bbo.symbol, "BTC");
        assert_eq!(bbo.bid_price, Decimal::new(777_670, 1));
        assert_eq!(bbo.bid_qty, Decimal::new(61_386, 4));
        assert_eq!(bbo.ask_price, Decimal::new(777_680, 1));
        assert_eq!(bbo.ask_qty, Decimal::new(110_124, 5));
        assert_eq!(bbo.exchange_timestamp_ms, 1_789_387_665_453);
        assert_eq!(bbo.source_cursor, None);
    }

    #[test]
    fn parses_hyperliquid_control_messages() {
        assert!(matches!(
            parse_hyperliquid_message(
                r#"{"channel":"subscriptionResponse","data":{"method":"subscribe","subscription":{"type":"bbo","coin":"BTC"}}}"#,
                "BTC"
            ),
            Ok(ParsedFeedEvent::SubscriptionAck)
        ));
        assert!(matches!(
            parse_hyperliquid_message(r#"{"channel":"pong"}"#, "BTC"),
            Ok(ParsedFeedEvent::Pong)
        ));
    }

    #[test]
    fn rejects_invalid_hyperliquid_messages() {
        assert!(parse_hyperliquid_message(
            r#"{"channel":"bbo","data":{"coin":"ETH","time":1,"bbo":[{"px":"1","sz":"1","n":1},{"px":"2","sz":"1","n":1}]}}"#,
            "BTC"
        )
        .is_err());
        assert!(parse_hyperliquid_message(
            r#"{"channel":"bbo","data":{"coin":"BTC","time":1,"bbo":[null,{"px":"2","sz":"1","n":1}]}}"#,
            "BTC"
        )
        .is_err());
        assert!(
            parse_hyperliquid_message(r#"{"channel":"error","data":"bad request"}"#, "BTC")
                .is_err()
        );
        assert!(parse_hyperliquid_message(r#"{"error":"bad request"}"#, "BTC").is_err());
    }

    #[test]
    fn parses_lighter_ticker_and_uses_matching_engine_timestamp() {
        let event = parse_lighter_message(
            r#"{"channel":"ticker:1","last_updated_at":1789388049810618,"nonce":21917275396,"ticker":{"s":"BTC","a":{"price":"77776.7","size":"0.05000"},"b":{"price":"77776.6","size":"0.00020"},"last_updated_at":1789388049810618},"timestamp":1789388049999,"type":"subscribed/ticker"}"#,
            "BTC",
            1,
        )
        .expect("valid ticker");
        let ParsedFeedEvent::Bbo(bbo) = event else {
            panic!("expected BBO");
        };
        assert_eq!(bbo.bid_price, Decimal::new(777_766, 1));
        assert_eq!(bbo.bid_qty, Decimal::new(20, 5));
        assert_eq!(bbo.ask_price, Decimal::new(777_767, 1));
        assert_eq!(bbo.ask_qty, Decimal::new(5_000, 5));
        assert_eq!(bbo.exchange_timestamp_ms, 1_789_388_049_810);
        assert_eq!(bbo.origin_timestamp_ms, None);
        assert_eq!(bbo.source_cursor, Some(21_917_275_396));
    }

    #[test]
    fn parses_lighter_control_messages() {
        assert!(matches!(
            parse_lighter_message(r#"{"session_id":"session-1","type":"connected"}"#, "BTC", 1),
            Ok(ParsedFeedEvent::Connected)
        ));
        assert!(matches!(
            parse_lighter_message(r#"{"type":"ping"}"#, "BTC", 1),
            Ok(ParsedFeedEvent::ServerPing)
        ));
        assert!(matches!(
            parse_lighter_message(r#"{"type":"pong"}"#, "BTC", 1),
            Ok(ParsedFeedEvent::Pong)
        ));
    }

    #[test]
    fn rejects_wrong_lighter_ticker_identity_and_inconsistent_timestamp() {
        let base = r#"{"channel":"ticker:2","last_updated_at":1789388049810618,"nonce":1,"ticker":{"s":"BTC","a":{"price":"2","size":"1"},"b":{"price":"1","size":"1"},"last_updated_at":1789388049810618},"type":"update/ticker"}"#;
        assert!(parse_lighter_message(base, "BTC", 1).is_err());

        let wrong_symbol = r#"{"channel":"ticker:1","last_updated_at":1789388049810618,"nonce":1,"ticker":{"s":"ETH","a":{"price":"2","size":"1"},"b":{"price":"1","size":"1"},"last_updated_at":1789388049810618},"type":"update/ticker"}"#;
        assert!(parse_lighter_message(wrong_symbol, "BTC", 1).is_err());

        let disagree = r#"{"channel":"ticker:1","last_updated_at":1789388049810618,"nonce":1,"ticker":{"s":"BTC","a":{"price":"2","size":"1"},"b":{"price":"1","size":"1"},"last_updated_at":1789388049810619},"type":"update/ticker"}"#;
        assert!(parse_lighter_message(disagree, "BTC", 1).is_err());
    }

    #[test]
    fn validates_active_lighter_perpetual_metadata() {
        let body = br#"{"code":200,"order_book_details":[{"symbol":"ETH","market_id":0,"market_type":"perp","status":"active"},{"symbol":"BTC","market_id":1,"market_type":"perp","status":"active","new_field":"ignored"}]}"#;
        validate_lighter_market_details(body, "BTC", 1).expect("valid BTC market");
    }

    #[test]
    fn rejects_wrong_or_inactive_lighter_metadata() {
        let wrong_symbol = br#"{"code":200,"order_book_details":[{"symbol":"ETH","market_id":1,"market_type":"perp","status":"active"}]}"#;
        assert!(validate_lighter_market_details(wrong_symbol, "BTC", 1).is_err());

        let inactive = br#"{"code":200,"order_book_details":[{"symbol":"BTC","market_id":1,"market_type":"perp","status":"inactive"}]}"#;
        assert!(validate_lighter_market_details(inactive, "BTC", 1).is_err());

        let spot = br#"{"code":200,"order_book_details":[{"symbol":"BTC","market_id":1,"market_type":"spot","status":"active"}]}"#;
        assert!(validate_lighter_market_details(spot, "BTC", 1).is_err());

        let failed = br#"{"code":500,"order_book_details":[]}"#;
        assert!(validate_lighter_market_details(failed, "BTC", 1).is_err());
    }
}
