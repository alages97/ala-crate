use std::fmt;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Venue {
    Binance,
    Bybit,
    Hyperliquid,
    Lighter,
}

impl fmt::Display for Venue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            Self::Binance => "binance",
            Self::Bybit => "bybit",
            Self::Hyperliquid => "hyperliquid",
            Self::Lighter => "lighter",
        };
        f.write_str(value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Side {
    Buy,
    Sell,
}

impl Side {
    pub const fn opposite(self) -> Self {
        match self {
            Self::Buy => Self::Sell,
            Self::Sell => Self::Buy,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimeInForce {
    PostOnly,
    GoodTilCanceled,
    ImmediateOrCancel,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Instrument {
    pub venue: Venue,
    /// Stable symbol used inside the strategy and order manager.
    pub canonical_symbol: String,
    /// Symbol or market identifier understood by the venue adapter.
    pub venue_symbol: String,
    #[serde(with = "rust_decimal::serde::str")]
    pub price_tick: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub quantity_step: Decimal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ClientOrderId(pub u64);

impl fmt::Display for ClientOrderId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct VenueOrderId(pub String);

impl fmt::Display for VenueOrderId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewOrder {
    pub client_order_id: ClientOrderId,
    pub instrument: Instrument,
    pub side: Side,
    #[serde(with = "rust_decimal::serde::str")]
    pub price: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub quantity: Decimal,
    pub time_in_force: TimeInForce,
    pub reduce_only: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelOrder {
    pub instrument: Instrument,
    pub client_order_id: ClientOrderId,
    pub venue_order_id: Option<VenueOrderId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderAck {
    pub client_order_id: ClientOrderId,
    pub venue_order_id: Option<VenueOrderId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelAck {
    pub client_order_id: ClientOrderId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionEvent {
    pub venue: Venue,
    pub timestamp_ms: u64,
    pub client_order_id: Option<ClientOrderId>,
    pub kind: ExecutionEventKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ExecutionEventKind {
    Accepted {
        venue_order_id: VenueOrderId,
    },
    Rejected {
        reason: String,
    },
    Canceled,
    CancelRejected {
        reason: String,
    },
    Fill {
        trade_id: String,
        #[serde(with = "rust_decimal::serde::str")]
        price: Decimal,
        #[serde(with = "rust_decimal::serde::str")]
        quantity: Decimal,
        #[serde(with = "rust_decimal::serde::str")]
        cumulative_quantity: Decimal,
        is_final: bool,
    },
    Disconnected {
        reason: String,
    },
    Reconnected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ManagedOrderStatus {
    PendingNew,
    Open,
    PartiallyFilled,
    PendingCancel,
}

pub fn decimal_is_step_aligned(value: Decimal, step: Decimal) -> bool {
    step > Decimal::ZERO && value % step == Decimal::ZERO
}

pub fn unix_timestamp_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
