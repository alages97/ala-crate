use std::collections::HashMap;
use std::ffi::{c_char, c_void, CStr, CString};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use libloading::Library;
use metrics::{counter, gauge, histogram};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use serde::Deserialize;
use tokio::sync::{broadcast, oneshot, Mutex};
use tokio::time::{Instant as TokioInstant, MissedTickBehavior};
use tokio_tungstenite::{connect_async, tungstenite::Message};

use super::{
    observe_private_fill, require_execution_ready, BoundedDedupe, GatewayError, LifecycleKind,
    LifecycleWait, OrderGateway, PRIVATE_LIFECYCLE_TIMEOUT,
};
use crate::types::{
    unix_timestamp_ms, CancelAck, CancelOrder, ClientOrderId, ExecutionEvent, ExecutionEventKind,
    Instrument, NewOrder, OrderAck, TimeInForce, Venue, VenueOrderId,
};

const MAINNET_CHAIN_ID: i32 = 304;
const LIMIT_ORDER: i32 = 0;
const IMMEDIATE_OR_CANCEL: i32 = 0;
const GOOD_TILL_TIME: i32 = 1;
const POST_ONLY: i32 = 2;
const MAX_CLIENT_ORDER_INDEX: u64 = (1 << 48) - 1;
const PRIVATE_STREAM_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(30);
const PRIVATE_STREAM_READ_TIMEOUT: Duration = Duration::from_secs(90);
const PRIVATE_STREAM_AUTH_ROTATION_INTERVAL: Duration = Duration::from_secs(6 * 60 * 60);
const LIGHTER_MAINNET_HOST: &str = "mainnet.zklighter.elliot.ai";

fn validate_live_host(host: &str) -> Result<String, GatewayError> {
    let normalized = host.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        return Err(GatewayError::InvalidOrder(
            "Lighter host must not be empty".to_owned(),
        ));
    }
    if normalized.contains("testnet") {
        return Err(GatewayError::InvalidOrder(
            "testnet host is disabled; configure the Lighter mainnet host".to_owned(),
        ));
    }
    if normalized.starts_with("http://") {
        return Err(GatewayError::InvalidOrder(
            "Lighter live trading requires an HTTPS host".to_owned(),
        ));
    }
    if normalized.contains("://") && !normalized.starts_with("https://") {
        return Err(GatewayError::InvalidOrder(
            "Lighter host must use HTTPS".to_owned(),
        ));
    }
    let normalized = normalized
        .strip_prefix("https://")
        .unwrap_or(&normalized)
        .trim_end_matches('/');
    if normalized != LIGHTER_MAINNET_HOST {
        return Err(GatewayError::InvalidOrder(format!(
            "Lighter live host must be {LIGHTER_MAINNET_HOST}"
        )));
    }
    Ok(LIGHTER_MAINNET_HOST.to_owned())
}

fn lighter_symbol_matches(left: &str, right: &str) -> bool {
    left.eq_ignore_ascii_case(right)
}

fn checked_order_notional(price: Decimal, quantity: Decimal) -> Result<Decimal, GatewayError> {
    price.checked_mul(quantity).ok_or_else(|| {
        GatewayError::InvalidOrder(format!(
            "price {price} times quantity {quantity} exceeds decimal range"
        ))
    })
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LighterGatewayConfig {
    #[serde(default = "default_host")]
    pub host: String,
    pub market_symbol: String,
    pub signer_library_path_env: String,
    pub account_index_env: String,
    pub api_key_index_env: String,
    pub api_private_key_env: String,
    #[serde(default = "default_request_timeout_ms")]
    pub request_timeout_ms: u64,
    #[serde(default = "default_order_expiry_ms")]
    pub order_expiry_ms: u64,
    #[serde(default = "default_reconnect_delay_ms")]
    pub reconnect_delay_ms: u64,
}

fn default_host() -> String {
    "mainnet.zklighter.elliot.ai".to_owned()
}

const fn default_request_timeout_ms() -> u64 {
    5_000
}

const fn default_order_expiry_ms() -> u64 {
    24 * 60 * 60 * 1_000
}

const fn default_reconnect_delay_ms() -> u64 {
    1_000
}

#[derive(Debug, Clone)]
struct MarketSpec {
    market_index: i32,
    symbol: String,
    size_decimals: u32,
    price_decimals: u32,
    min_base_amount: Decimal,
    min_quote_amount: Decimal,
}

#[derive(Debug, Clone)]
struct TrackedOrder {
    requested_quantity: Decimal,
    cumulative_quantity: Decimal,
    venue_order_id: Option<VenueOrderId>,
    last_status: Option<String>,
    pending_terminal: Option<PendingTerminal>,
    lifecycle_wait: Option<LifecycleWait>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum PendingTerminalKind {
    Filled,
    Canceled,
    Rejected(String),
}

#[derive(Debug, Clone)]
struct PendingTerminal {
    kind: PendingTerminalKind,
    expected_cumulative_quantity: Decimal,
    timestamp_ms: u64,
}

#[derive(Default)]
struct GatewayState {
    orders: HashMap<ClientOrderId, TrackedOrder>,
    seen_trades: BoundedDedupe<i64>,
    terminal_client_ids: BoundedDedupe<ClientOrderId>,
    http_rejected_client_ids: BoundedDedupe<ClientOrderId>,
}

fn classify_submit_rejection(
    state: &mut GatewayState,
    client_order_id: ClientOrderId,
    rejection: GatewayError,
) -> GatewayError {
    if let Some(order) = state.orders.get(&client_order_id) {
        let has_private_progress = order.venue_order_id.is_some()
            || order.cumulative_quantity > Decimal::ZERO
            || order.pending_terminal.is_some()
            || order.last_status.is_some()
            || order
                .lifecycle_wait
                .as_ref()
                .is_none_or(|wait| wait.kind != LifecycleKind::PrivateAcceptance);
        if has_private_progress {
            return GatewayError::Protocol(format!(
                "Lighter HTTP submit rejection contradicted private execution progress for {client_order_id}: {rejection}"
            ));
        }
        state.orders.remove(&client_order_id);
        state.terminal_client_ids.insert(client_order_id);
        state.http_rejected_client_ids.insert(client_order_id);
        rejection
    } else {
        GatewayError::Protocol(format!(
            "Lighter HTTP submit rejection arrived after private terminal progress for {client_order_id}: {rejection}"
        ))
    }
}

fn stalled_lifecycle(state: &GatewayState, now: Instant) -> Option<(ClientOrderId, LifecycleKind)> {
    state.orders.iter().find_map(|(client_order_id, order)| {
        order
            .lifecycle_wait
            .as_ref()
            .filter(|wait| wait.is_stalled_at(now))
            .map(|wait| (*client_order_id, wait.kind))
    })
}

struct NonceLane {
    next: i64,
}

fn validate_lighter_nonce(nonce: i64) -> Result<i64, GatewayError> {
    if nonce < 0 || nonce == i64::MAX {
        return Err(GatewayError::Protocol(format!(
            "Lighter returned unusable nonce {nonce}"
        )));
    }
    Ok(nonce)
}

fn validate_lighter_account_index(account_index: i64) -> Result<i64, GatewayError> {
    if account_index < 0 {
        return Err(GatewayError::Authentication(
            "Lighter account index must be nonnegative".to_owned(),
        ));
    }
    Ok(account_index)
}

fn validate_lighter_market_index(market_index: i64) -> Result<i32, GatewayError> {
    if market_index < 0 {
        return Err(GatewayError::Protocol(format!(
            "Lighter market id {market_index} must be nonnegative"
        )));
    }
    // SignCreateOrder accepts an int16 market index even though the C ABI
    // exposes it as `int`; reject metadata we cannot encode losslessly.
    let market_index = i16::try_from(market_index).map_err(|_| {
        GatewayError::Protocol("Lighter market id exceeds signer int16 range".to_owned())
    })?;
    Ok(i32::from(market_index))
}

fn nonce_with_successor(nonce: i64) -> Result<(i64, i64), GatewayError> {
    let nonce = validate_lighter_nonce(nonce)?;
    let successor = nonce
        .checked_add(1)
        .and_then(|next| (next != i64::MAX).then_some(next))
        .ok_or_else(|| GatewayError::Protocol("Lighter nonce space is exhausted".to_owned()))?;
    Ok((nonce, successor))
}

struct PrivateStreamContext {
    api: Arc<LighterApi>,
    signer: Arc<LighterSigner>,
    host: String,
    account_index: i64,
    api_key_index: i32,
    market_index: i32,
    events: broadcast::Sender<ExecutionEvent>,
    state: Arc<Mutex<GatewayState>>,
    execution_ready: Arc<AtomicBool>,
    nonce: Arc<Mutex<NonceLane>>,
}

/// Live Lighter adapter backed by the official `lighter-go` signer shared
/// library. Native Rust owns HTTP, WebSocket parsing, state and telemetry.
pub struct LighterGateway {
    api: Arc<LighterApi>,
    signer: Arc<LighterSigner>,
    account_index: i64,
    api_key_index: i32,
    market: MarketSpec,
    order_expiry_ms: u64,
    nonce: Arc<Mutex<NonceLane>>,
    events: broadcast::Sender<ExecutionEvent>,
    state: Arc<Mutex<GatewayState>>,
    execution_ready: Arc<AtomicBool>,
}

impl LighterGateway {
    pub async fn connect(config: LighterGatewayConfig) -> Result<Self, GatewayError> {
        crate::telemetry::install_tls_crypto_provider()
            .map_err(|error| GatewayError::Transport(error.to_string()))?;
        let host = validate_live_host(&config.host)?;

        let account_index =
            validate_lighter_account_index(read_env_parse::<i64>(&config.account_index_env)?)?;
        let api_key_index = read_env_parse::<i32>(&config.api_key_index_env)?;
        if !(0..=254).contains(&api_key_index) {
            return Err(GatewayError::Authentication(
                "Lighter API key index must be in 0..=254".to_owned(),
            ));
        }
        let private_key = read_required_env(&config.api_private_key_env, true)?;
        let signer_path = PathBuf::from(read_required_env(&config.signer_library_path_env, false)?);

        let api = Arc::new(LighterApi::new(
            &host,
            Duration::from_millis(config.request_timeout_ms),
        )?);
        let market_started = Instant::now();
        let market = api.discover_market(&config.market_symbol).await?;
        histogram!("gateway_market_discovery_seconds", "venue" => "lighter")
            .record(market_started.elapsed().as_secs_f64());

        let signer = Arc::new(LighterSigner::load(signer_path)?);
        let signer_started = Instant::now();
        signer.create_client(
            api.base_url(),
            &private_key,
            MAINNET_CHAIN_ID,
            api_key_index,
            account_index,
        )?;
        signer.check_client(api_key_index, account_index)?;
        histogram!("gateway_signer_initialization_seconds", "venue" => "lighter")
            .record(signer_started.elapsed().as_secs_f64());

        let nonce = api
            .next_nonce(account_index, api_key_index)
            .await
            .map_err(|err| GatewayError::Transport(format!("load Lighter nonce: {err}")))?;
        let (events, _) = broadcast::channel(4_096);
        let state = Arc::new(Mutex::new(GatewayState::default()));
        let execution_ready = Arc::new(AtomicBool::new(false));
        let nonce = Arc::new(Mutex::new(NonceLane { next: nonce }));

        start_private_stream(
            PrivateStreamContext {
                api: Arc::clone(&api),
                signer: Arc::clone(&signer),
                host,
                account_index,
                api_key_index,
                market_index: market.market_index,
                events: events.clone(),
                state: Arc::clone(&state),
                execution_ready: Arc::clone(&execution_ready),
                nonce: Arc::clone(&nonce),
            },
            Duration::from_millis(config.reconnect_delay_ms),
            Duration::from_millis(config.request_timeout_ms),
        )
        .await?;

        Ok(Self {
            api,
            signer,
            account_index,
            api_key_index,
            market,
            order_expiry_ms: config.order_expiry_ms,
            nonce,
            events,
            state,
            execution_ready,
        })
    }

    pub fn market_index(&self) -> i32 {
        self.market.market_index
    }

    pub fn market_symbol(&self) -> &str {
        &self.market.symbol
    }

    fn scale_order(&self, order: &NewOrder) -> Result<(i64, i32), GatewayError> {
        if order.instrument.venue != Venue::Lighter {
            return Err(GatewayError::InvalidOrder(format!(
                "Lighter gateway received {} instrument",
                order.instrument.venue
            )));
        }
        if !lighter_symbol_matches(&order.instrument.venue_symbol, &self.market.symbol) {
            return Err(GatewayError::InvalidOrder(format!(
                "configured symbol {} resolved to {}, but order uses {}",
                self.market.symbol, self.market.market_index, order.instrument.venue_symbol
            )));
        }
        if order.client_order_id.0 == 0 || order.client_order_id.0 > MAX_CLIENT_ORDER_INDEX {
            return Err(GatewayError::InvalidOrder(format!(
                "client order id {} is outside Lighter's uint48 range",
                order.client_order_id
            )));
        }
        if order.quantity < self.market.min_base_amount {
            return Err(GatewayError::InvalidOrder(format!(
                "quantity {} is below Lighter minimum {}",
                order.quantity, self.market.min_base_amount
            )));
        }
        let notional = checked_order_notional(order.price, order.quantity)?;
        if notional < self.market.min_quote_amount {
            return Err(GatewayError::InvalidOrder(format!(
                "notional {} is below Lighter minimum {}",
                notional, self.market.min_quote_amount
            )));
        }
        let base_amount = scale_decimal_i64(order.quantity, self.market.size_decimals, "quantity")?;
        let price_i64 = scale_decimal_i64(order.price, self.market.price_decimals, "price")?;
        let price = i32::try_from(price_i64).map_err(|_| {
            GatewayError::InvalidOrder("scaled Lighter price exceeds signed 32-bit ABI".to_owned())
        })?;
        Ok((base_amount, price))
    }

    async fn refresh_nonce_exact(&self, lane: &mut NonceLane) -> Result<(), GatewayError> {
        lane.next = self
            .api
            .next_nonce(self.account_index, self.api_key_index)
            .await?;
        Ok(())
    }

    async fn refresh_nonce_monotonic(&self, lane: &mut NonceLane) -> Result<(), GatewayError> {
        let floor = lane.next;
        let next = self
            .api
            .next_nonce(self.account_index, self.api_key_index)
            .await?;
        lane.next = validate_lighter_nonce(floor.max(next))?;
        Ok(())
    }
}

impl OrderGateway for LighterGateway {
    fn venue(&self) -> Venue {
        Venue::Lighter
    }

    fn subscribe(&self) -> broadcast::Receiver<ExecutionEvent> {
        self.events.subscribe()
    }

    fn execution_ready(&self) -> bool {
        self.execution_ready.load(Ordering::Acquire)
    }

    async fn submit(&self, order: NewOrder) -> Result<OrderAck, GatewayError> {
        require_execution_ready(&self.execution_ready, Venue::Lighter)?;
        let (base_amount, price) = self.scale_order(&order)?;
        let client_order_id = order.client_order_id;
        let time_in_force = match order.time_in_force {
            TimeInForce::PostOnly => POST_ONLY,
            TimeInForce::GoodTilCanceled => GOOD_TILL_TIME,
            TimeInForce::ImmediateOrCancel => IMMEDIATE_OR_CANCEL,
        };
        // The signer protocol requires IOC orders to carry an expiry of zero.
        // GTT and post-only orders use an absolute Unix timestamp in milliseconds.
        let expiry = if time_in_force == IMMEDIATE_OR_CANCEL {
            0
        } else {
            let expiry = unix_timestamp_ms()
                .checked_add(self.order_expiry_ms)
                .ok_or_else(|| GatewayError::InvalidOrder("order expiry overflow".to_owned()))?;
            i64::try_from(expiry).map_err(|_| {
                GatewayError::InvalidOrder("order expiry exceeds the signer ABI".to_owned())
            })?
        };
        {
            let mut state = self.state.lock().await;
            if state.orders.contains_key(&client_order_id) {
                return Err(GatewayError::InvalidOrder(format!(
                    "duplicate client order id {client_order_id}"
                )));
            }
            if state.terminal_client_ids.contains(&client_order_id) {
                return Err(GatewayError::InvalidOrder(format!(
                    "client order id {client_order_id} was already terminal in this process"
                )));
            }
            state.orders.insert(
                client_order_id,
                TrackedOrder {
                    requested_quantity: order.quantity,
                    cumulative_quantity: Decimal::ZERO,
                    venue_order_id: None,
                    last_status: None,
                    pending_terminal: None,
                    lifecycle_wait: None,
                },
            );
        }

        let queue_started = Instant::now();
        let mut lane = self.nonce.lock().await;
        histogram!("gateway_nonce_queue_seconds", "venue" => "lighter")
            .record(queue_started.elapsed().as_secs_f64());
        let (nonce, next_nonce) = match nonce_with_successor(lane.next) {
            Ok(pair) => pair,
            Err(error) => {
                drop(lane);
                self.state.lock().await.orders.remove(&client_order_id);
                mark_lighter_stream_disconnected(
                    &self.execution_ready,
                    &self.events,
                    error.to_string(),
                );
                return Err(error);
            }
        };

        if let Err(error) = require_execution_ready(&self.execution_ready, Venue::Lighter) {
            drop(lane);
            self.state.lock().await.orders.remove(&client_order_id);
            return Err(error);
        }
        if let Some(tracked) = self.state.lock().await.orders.get_mut(&client_order_id) {
            tracked.lifecycle_wait = Some(LifecycleWait::new(LifecycleKind::PrivateAcceptance));
        }

        let sign_started = Instant::now();
        let signed = match self.signer.sign_create_order(LighterCreateOrder {
            market_index: self.market.market_index,
            client_order_index: client_order_id.0 as i64,
            base_amount,
            price,
            is_ask: i32::from(matches!(order.side, crate::types::Side::Sell)),
            order_type: LIMIT_ORDER,
            time_in_force,
            reduce_only: i32::from(order.reduce_only),
            trigger_price: 0,
            order_expiry: expiry,
            nonce,
            api_key_index: self.api_key_index,
            account_index: self.account_index,
        }) {
            Ok(signed) => signed,
            Err(error) => {
                // No transaction exists when local signing fails, so this ID is
                // safe to release for the caller to replace deterministically.
                drop(lane);
                self.state.lock().await.orders.remove(&client_order_id);
                return Err(error);
            }
        };
        histogram!("gateway_sign_seconds", "venue" => "lighter")
            .record(sign_started.elapsed().as_secs_f64());

        if let Err(error) = require_execution_ready(&self.execution_ready, Venue::Lighter) {
            drop(lane);
            self.state.lock().await.orders.remove(&client_order_id);
            return Err(error);
        }

        let http_started = Instant::now();
        let response = self.api.send_tx(&signed).await;
        histogram!("gateway_http_roundtrip_seconds", "venue" => "lighter", "operation" => "submit")
            .record(http_started.elapsed().as_secs_f64());
        match response {
            Ok(response) if response.code == 200 => {
                lane.next = next_nonce;
                drop(lane);
                counter!("gateway_orders_submitted_total", "venue" => "lighter").increment(1);
                Ok(OrderAck {
                    client_order_id,
                    // Lighter acknowledges transaction ingress first. The
                    // exchange order index arrives on the private stream.
                    venue_order_id: None,
                })
            }
            Ok(response) => {
                let nonce_refresh = self.refresh_nonce_exact(&mut lane).await;
                let rejection = GatewayError::Rejected {
                    code: response.code.to_string(),
                    message: response
                        .message
                        .unwrap_or_else(|| "Lighter rejected transaction".to_owned()),
                };
                let classified = {
                    let mut state = self.state.lock().await;
                    classify_submit_rejection(&mut state, client_order_id, rejection)
                };
                if let Err(error) = nonce_refresh {
                    mark_lighter_stream_disconnected(
                        &self.execution_ready,
                        &self.events,
                        error.to_string(),
                    );
                    return Err(error);
                }
                if matches!(classified, GatewayError::Protocol(_)) {
                    mark_lighter_stream_disconnected(
                        &self.execution_ready,
                        &self.events,
                        classified.to_string(),
                    );
                }
                Err(classified)
            }
            Err(err) => {
                // The request may have consumed this nonce even if HTTP failed
                // before we observed the response. Never move the lane back to
                // a possibly-consumed value while server visibility catches up.
                lane.next = lane.next.max(next_nonce);
                if let Err(refresh_error) = self.refresh_nonce_monotonic(&mut lane).await {
                    tracing::error!(error = %refresh_error, "failed to refresh Lighter nonce after indeterminate submit");
                    mark_lighter_stream_disconnected(
                        &self.execution_ready,
                        &self.events,
                        refresh_error.to_string(),
                    );
                }
                counter!("gateway_submit_errors_total", "venue" => "lighter", "kind" => "indeterminate")
                    .increment(1);
                Err(GatewayError::Indeterminate(format!(
                    "{} (tx_hash={})",
                    err, signed.tx_hash
                )))
            }
        }
    }

    async fn cancel(&self, cancel: CancelOrder) -> Result<CancelAck, GatewayError> {
        if cancel.instrument.venue != Venue::Lighter {
            return Err(GatewayError::InvalidOrder(
                "cancel instrument belongs to another venue".to_owned(),
            ));
        }
        if !lighter_symbol_matches(&cancel.instrument.venue_symbol, &self.market.symbol) {
            return Err(GatewayError::InvalidOrder(format!(
                "cancel symbol {} does not match configured Lighter market {}",
                cancel.instrument.venue_symbol, self.market.symbol
            )));
        }
        let order_index = i64::try_from(cancel.client_order_id.0).map_err(|_| {
            GatewayError::InvalidOrder("client order id exceeds Lighter range".to_owned())
        })?;

        let queue_started = Instant::now();
        let mut lane = self.nonce.lock().await;
        histogram!("gateway_nonce_queue_seconds", "venue" => "lighter")
            .record(queue_started.elapsed().as_secs_f64());
        let (nonce, next_nonce) = match nonce_with_successor(lane.next) {
            Ok(pair) => pair,
            Err(error) => {
                mark_lighter_stream_disconnected(
                    &self.execution_ready,
                    &self.events,
                    error.to_string(),
                );
                return Err(error);
            }
        };
        let sign_started = Instant::now();
        let signed = self.signer.sign_cancel_order(
            self.market.market_index,
            order_index,
            nonce,
            self.api_key_index,
            self.account_index,
        )?;
        histogram!("gateway_sign_seconds", "venue" => "lighter")
            .record(sign_started.elapsed().as_secs_f64());

        let previous_lifecycle = {
            let mut state = self.state.lock().await;
            state
                .orders
                .get_mut(&cancel.client_order_id)
                .and_then(|order| {
                    order
                        .lifecycle_wait
                        .replace(LifecycleWait::new(LifecycleKind::CancelTerminal))
                })
        };

        let http_started = Instant::now();
        let response = self.api.send_tx(&signed).await;
        histogram!("gateway_http_roundtrip_seconds", "venue" => "lighter", "operation" => "cancel")
            .record(http_started.elapsed().as_secs_f64());
        match response {
            Ok(response) if response.code == 200 => {
                lane.next = next_nonce;
                counter!("gateway_cancel_requests_total", "venue" => "lighter").increment(1);
                Ok(CancelAck {
                    client_order_id: cancel.client_order_id,
                })
            }
            Ok(response) => {
                if let Err(error) = self.refresh_nonce_exact(&mut lane).await {
                    mark_lighter_stream_disconnected(
                        &self.execution_ready,
                        &self.events,
                        error.to_string(),
                    );
                    return Err(error);
                }
                let mut state = self.state.lock().await;
                if let Some(order) = state.orders.get_mut(&cancel.client_order_id) {
                    if order
                        .lifecycle_wait
                        .as_ref()
                        .is_some_and(|wait| wait.kind == LifecycleKind::CancelTerminal)
                    {
                        order.lifecycle_wait = previous_lifecycle;
                    }
                }
                Err(GatewayError::Rejected {
                    code: response.code.to_string(),
                    message: response
                        .message
                        .unwrap_or_else(|| "Lighter rejected cancel".to_owned()),
                })
            }
            Err(err) => {
                lane.next = lane.next.max(next_nonce);
                if let Err(refresh_error) = self.refresh_nonce_monotonic(&mut lane).await {
                    tracing::error!(error = %refresh_error, "failed to refresh Lighter nonce after indeterminate cancel");
                    mark_lighter_stream_disconnected(
                        &self.execution_ready,
                        &self.events,
                        refresh_error.to_string(),
                    );
                }
                Err(GatewayError::Indeterminate(format!(
                    "{} (tx_hash={})",
                    err, signed.tx_hash
                )))
            }
        }
    }

    async fn cancel_all(&self, instrument: Instrument) -> Result<(), GatewayError> {
        if instrument.venue != Venue::Lighter {
            return Err(GatewayError::InvalidOrder(
                "cancel-all instrument belongs to another venue".to_owned(),
            ));
        }
        if !lighter_symbol_matches(&instrument.venue_symbol, &self.market.symbol) {
            return Err(GatewayError::InvalidOrder(format!(
                "cancel-all symbol {} does not match configured Lighter market {}",
                instrument.venue_symbol, self.market.symbol
            )));
        }
        let ids: Vec<ClientOrderId> = self.state.lock().await.orders.keys().copied().collect();
        let mut first_error = None;
        for client_order_id in ids {
            if let Err(error) = self
                .cancel(CancelOrder {
                    instrument: instrument.clone(),
                    client_order_id,
                    venue_order_id: None,
                })
                .await
            {
                tracing::error!(%client_order_id, %error, "Lighter cancel-all item failed");
                first_error.get_or_insert(error);
            }
        }
        first_error.map_or(Ok(()), Err)
    }
}

fn scale_decimal_i64(
    value: Decimal,
    decimals: u32,
    field: &'static str,
) -> Result<i64, GatewayError> {
    if value <= Decimal::ZERO {
        return Err(GatewayError::InvalidOrder(format!(
            "{field} must be positive"
        )));
    }
    if decimals > 18 {
        return Err(GatewayError::Protocol(format!(
            "unsupported Lighter {field} precision {decimals}"
        )));
    }
    let factor = Decimal::from(10u64.pow(decimals));
    let scaled = value.checked_mul(factor).ok_or_else(|| {
        GatewayError::InvalidOrder(format!(
            "scaled {field} exceeds decimal representation range"
        ))
    })?;
    if !scaled.fract().is_zero() {
        return Err(GatewayError::InvalidOrder(format!(
            "{field} {value} has more than {decimals} decimal places"
        )));
    }
    scaled.to_i64().ok_or_else(|| {
        GatewayError::InvalidOrder(format!("scaled {field} is outside the i64 range"))
    })
}

#[derive(Deserialize)]
struct OrderBooksResponse {
    code: i64,
    #[serde(default)]
    message: Option<String>,
    #[serde(default)]
    order_books: Vec<OrderBookMetadata>,
}

#[derive(Deserialize)]
struct OrderBookMetadata {
    symbol: String,
    market_id: i64,
    market_type: String,
    status: String,
    min_base_amount: String,
    min_quote_amount: String,
    supported_size_decimals: u32,
    supported_price_decimals: u32,
}

#[derive(Deserialize)]
struct NextNonceResponse {
    code: i64,
    #[serde(default)]
    message: Option<String>,
    nonce: i64,
}

#[derive(Deserialize)]
struct SendTxResponse {
    code: i64,
    #[serde(default)]
    message: Option<String>,
}

fn classify_send_tx_http_response(
    status: reqwest::StatusCode,
    body: SendTxResponse,
) -> Result<SendTxResponse, GatewayError> {
    if !status.is_success() {
        return Err(GatewayError::Indeterminate(format!(
            "Lighter sendTx returned HTTP {status} (API code {}, message={})",
            body.code,
            body.message.as_deref().unwrap_or("unavailable")
        )));
    }
    Ok(body)
}

struct LighterApi {
    client: reqwest::Client,
    base_url: String,
}

impl LighterApi {
    fn new(host: &str, timeout: Duration) -> Result<Self, GatewayError> {
        let base_url = if host.starts_with("https://") || host.starts_with("http://") {
            host.trim_end_matches('/').to_owned()
        } else {
            format!("https://{}", host.trim_end_matches('/'))
        };
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .pool_max_idle_per_host(16)
            .build()
            .map_err(|err| GatewayError::Transport(err.to_string()))?;
        Ok(Self { client, base_url })
    }

    fn base_url(&self) -> &str {
        &self.base_url
    }

    async fn discover_market(&self, symbol: &str) -> Result<MarketSpec, GatewayError> {
        let url = format!("{}/api/v1/orderBooks", self.base_url);
        let response = self
            .client
            .get(url)
            .query(&[("filter", "perp")])
            .send()
            .await
            .map_err(|err| GatewayError::Transport(err.to_string()))?;
        let status = response.status();
        let body: OrderBooksResponse = response
            .json()
            .await
            .map_err(|err| GatewayError::Protocol(err.to_string()))?;
        if !status.is_success() || body.code != 200 {
            return Err(GatewayError::Rejected {
                code: body.code.to_string(),
                message: body.message.unwrap_or_else(|| status.to_string()),
            });
        }
        let market = body
            .order_books
            .into_iter()
            .find(|market| {
                market.symbol.eq_ignore_ascii_case(symbol)
                    && market.market_type.eq_ignore_ascii_case("perp")
            })
            .ok_or_else(|| {
                GatewayError::Protocol(format!("Lighter perpetual {symbol} was not found"))
            })?;
        if !market.status.eq_ignore_ascii_case("active") {
            return Err(GatewayError::Protocol(format!(
                "Lighter perpetual {symbol} is {}",
                market.status
            )));
        }
        Ok(MarketSpec {
            market_index: validate_lighter_market_index(market.market_id)?,
            symbol: market.symbol,
            size_decimals: market.supported_size_decimals,
            price_decimals: market.supported_price_decimals,
            min_base_amount: Decimal::from_str(&market.min_base_amount)
                .map_err(|err| GatewayError::Protocol(err.to_string()))?,
            min_quote_amount: Decimal::from_str(&market.min_quote_amount)
                .map_err(|err| GatewayError::Protocol(err.to_string()))?,
        })
    }

    async fn next_nonce(
        &self,
        account_index: i64,
        api_key_index: i32,
    ) -> Result<i64, GatewayError> {
        let url = format!("{}/api/v1/nextNonce", self.base_url);
        let response = self
            .client
            .get(url)
            .query(&[
                ("account_index", account_index.to_string()),
                ("api_key_index", api_key_index.to_string()),
            ])
            .send()
            .await
            .map_err(|err| GatewayError::Transport(err.to_string()))?;
        let status = response.status();
        let body: NextNonceResponse = response.json().await.map_err(|err| {
            if status.is_success() {
                GatewayError::Protocol(err.to_string())
            } else {
                GatewayError::Transport(format!(
                    "Lighter nextNonce returned HTTP {status} with an invalid body: {err}"
                ))
            }
        })?;
        if !status.is_success() {
            return Err(GatewayError::Transport(format!(
                "Lighter nextNonce returned HTTP {status} (API code {}, message={})",
                body.code,
                body.message.as_deref().unwrap_or("unavailable")
            )));
        }
        if body.code != 200 {
            return Err(GatewayError::Rejected {
                code: body.code.to_string(),
                message: body
                    .message
                    .unwrap_or_else(|| "nextNonce failed".to_owned()),
            });
        }
        validate_lighter_nonce(body.nonce)
    }

    async fn send_tx(&self, tx: &SignedTransaction) -> Result<SendTxResponse, GatewayError> {
        let url = format!("{}/api/v1/sendTx", self.base_url);
        let response = self
            .client
            .post(url)
            .form(&[
                ("tx_type", tx.tx_type.to_string()),
                ("tx_info", tx.tx_info.clone()),
            ])
            .send()
            .await
            .map_err(|err| GatewayError::Transport(err.to_string()))?;
        let status = response.status();
        let body = response.json::<SendTxResponse>().await.map_err(|err| {
            if status.is_success() {
                GatewayError::Protocol(err.to_string())
            } else {
                GatewayError::Indeterminate(format!(
                    "Lighter sendTx returned HTTP {status} with an invalid body: {err}"
                ))
            }
        })?;
        classify_send_tx_http_response(status, body)
    }
}

#[repr(C)]
struct RawStrOrErr {
    value: *mut c_char,
    error: *mut c_char,
}

#[repr(C)]
struct RawSignedTransaction {
    tx_type: u8,
    tx_info: *mut c_char,
    tx_hash: *mut c_char,
    message_to_sign: *mut c_char,
    error: *mut c_char,
}

struct SignedTransaction {
    tx_type: u8,
    tx_info: String,
    tx_hash: String,
}

struct LighterCreateOrder {
    market_index: i32,
    client_order_index: i64,
    base_amount: i64,
    price: i32,
    is_ask: i32,
    order_type: i32,
    time_in_force: i32,
    reduce_only: i32,
    trigger_price: i32,
    order_expiry: i64,
    nonce: i64,
    api_key_index: i32,
    account_index: i64,
}

/// Narrow safe wrapper around layouts published in the official lighter-go
/// Rust FFI example. The Go library owns its scheduler and internal locking.
struct LighterSigner {
    library: Library,
    free: unsafe extern "C" fn(*mut c_void),
}

// SAFETY: lighter-go documents its exported functions as safe for concurrent
// calls and internally keys mutable clients by account/API-key pair.
unsafe impl Send for LighterSigner {}
unsafe impl Sync for LighterSigner {}

impl LighterSigner {
    fn load(path: PathBuf) -> Result<Self, GatewayError> {
        let library = unsafe { Library::new(&path) }.map_err(|err| {
            GatewayError::Authentication(format!(
                "failed to load official Lighter signer {}: {err}",
                path.display()
            ))
        })?;
        let free = unsafe {
            *library
                .get::<unsafe extern "C" fn(*mut c_void)>(b"Free\0")
                .map_err(|err| GatewayError::Authentication(err.to_string()))?
        };
        Ok(Self { library, free })
    }

    fn create_client(
        &self,
        url: &str,
        private_key: &str,
        chain_id: i32,
        api_key_index: i32,
        account_index: i64,
    ) -> Result<(), GatewayError> {
        let url = cstring(url)?;
        let private_key = cstring(private_key)?;
        type Function =
            unsafe extern "C" fn(*mut c_char, *mut c_char, i32, i32, i64) -> *mut c_char;
        let function = unsafe {
            *self
                .library
                .get::<Function>(b"CreateClient\0")
                .map_err(|err| GatewayError::Authentication(err.to_string()))?
        };
        let error = unsafe {
            self.take_string(function(
                url.as_ptr() as *mut c_char,
                private_key.as_ptr() as *mut c_char,
                chain_id,
                api_key_index,
                account_index,
            ))
        };
        match error {
            Some(error) => Err(GatewayError::Authentication(error)),
            None => Ok(()),
        }
    }

    fn check_client(&self, api_key_index: i32, account_index: i64) -> Result<(), GatewayError> {
        type Function = unsafe extern "C" fn(i32, i64) -> *mut c_char;
        let function = unsafe {
            *self
                .library
                .get::<Function>(b"CheckClient\0")
                .map_err(|err| GatewayError::Authentication(err.to_string()))?
        };
        match unsafe { self.take_string(function(api_key_index, account_index)) } {
            Some(error) => Err(GatewayError::Authentication(error)),
            None => Ok(()),
        }
    }

    fn create_auth_token(
        &self,
        api_key_index: i32,
        account_index: i64,
    ) -> Result<String, GatewayError> {
        type Function = unsafe extern "C" fn(i64, i32, i64) -> RawStrOrErr;
        let function = unsafe {
            *self
                .library
                .get::<Function>(b"CreateAuthToken\0")
                .map_err(|err| GatewayError::Authentication(err.to_string()))?
        };
        let raw = unsafe { function(0, api_key_index, account_index) };
        let value = unsafe { self.take_string(raw.value) };
        let error = unsafe { self.take_string(raw.error) };
        if let Some(error) = error {
            return Err(GatewayError::Authentication(error));
        }
        value.ok_or_else(|| GatewayError::Protocol("signer returned no auth token".to_owned()))
    }

    fn sign_create_order(
        &self,
        order: LighterCreateOrder,
    ) -> Result<SignedTransaction, GatewayError> {
        type Function = unsafe extern "C" fn(
            i32,
            i64,
            i64,
            i32,
            i32,
            i32,
            i32,
            i32,
            i32,
            i64,
            i64,
            i32,
            i32,
            u8,
            u8,
            u8,
            i64,
            i32,
            i64,
        ) -> RawSignedTransaction;
        let function = unsafe {
            *self
                .library
                .get::<Function>(b"SignCreateOrder\0")
                .map_err(|err| GatewayError::Authentication(err.to_string()))?
        };
        let raw = unsafe {
            function(
                order.market_index,
                order.client_order_index,
                order.base_amount,
                order.price,
                order.is_ask,
                order.order_type,
                order.time_in_force,
                order.reduce_only,
                order.trigger_price,
                order.order_expiry,
                0,
                0,
                0,
                0,
                0,
                0,
                order.nonce,
                order.api_key_index,
                order.account_index,
            )
        };
        self.convert_signed(raw)
    }

    fn sign_cancel_order(
        &self,
        market_index: i32,
        order_index: i64,
        nonce: i64,
        api_key_index: i32,
        account_index: i64,
    ) -> Result<SignedTransaction, GatewayError> {
        type Function = unsafe extern "C" fn(i32, i64, u8, i64, i32, i64) -> RawSignedTransaction;
        let function = unsafe {
            *self
                .library
                .get::<Function>(b"SignCancelOrder\0")
                .map_err(|err| GatewayError::Authentication(err.to_string()))?
        };
        let raw = unsafe {
            function(
                market_index,
                order_index,
                0,
                nonce,
                api_key_index,
                account_index,
            )
        };
        self.convert_signed(raw)
    }

    fn convert_signed(&self, raw: RawSignedTransaction) -> Result<SignedTransaction, GatewayError> {
        let tx_info = unsafe { self.take_string(raw.tx_info) };
        let tx_hash = unsafe { self.take_string(raw.tx_hash) };
        // L2 create/cancel do not require a second Ethereum signature, but the
        // pointer still belongs to the library if it is ever populated.
        let _message_to_sign = unsafe { self.take_string(raw.message_to_sign) };
        let error = unsafe { self.take_string(raw.error) };
        if let Some(error) = error {
            return Err(GatewayError::Authentication(error));
        }
        let tx_info = tx_info.filter(|value| !value.is_empty()).ok_or_else(|| {
            GatewayError::Protocol("official signer returned no tx_info".to_owned())
        })?;
        let tx_hash = tx_hash.filter(|value| !value.is_empty()).ok_or_else(|| {
            GatewayError::Protocol("official signer returned no tx_hash".to_owned())
        })?;
        Ok(SignedTransaction {
            tx_type: raw.tx_type,
            tx_info,
            tx_hash,
        })
    }

    unsafe fn take_string(&self, pointer: *mut c_char) -> Option<String> {
        if pointer.is_null() {
            return None;
        }
        let value = CStr::from_ptr(pointer).to_string_lossy().into_owned();
        (self.free)(pointer.cast());
        Some(value)
    }
}

fn cstring(value: &str) -> Result<CString, GatewayError> {
    CString::new(value).map_err(|_| {
        GatewayError::Authentication("signer input contains an interior NUL byte".to_owned())
    })
}

fn mark_lighter_stream_disconnected(
    execution_ready: &AtomicBool,
    events: &broadcast::Sender<ExecutionEvent>,
    reason: String,
) {
    let was_ready = execution_ready.swap(false, Ordering::AcqRel);
    gauge!("gateway_private_stream_up", "venue" => "lighter").set(0.0);
    if was_ready {
        let _ = events.send(ExecutionEvent {
            venue: Venue::Lighter,
            timestamp_ms: unix_timestamp_ms(),
            client_order_id: None,
            kind: ExecutionEventKind::Disconnected { reason },
        });
    }
}

async fn start_private_stream(
    context: PrivateStreamContext,
    reconnect_delay: Duration,
    startup_timeout: Duration,
) -> Result<(), GatewayError> {
    let (startup_sender, startup_receiver) = oneshot::channel();
    let execution_ready = Arc::clone(&context.execution_ready);
    let monitor_readiness = Arc::clone(&context.execution_ready);
    let monitor_events = context.events.clone();
    let supervisor = tokio::spawn(async move {
        let mut startup_sender = Some(startup_sender);
        loop {
            let result = run_private_stream_once(&context, &mut startup_sender).await;
            if let Some(sender) = startup_sender.take() {
                let error = result.as_ref().err().cloned().unwrap_or_else(|| {
                    GatewayError::Transport(
                        "Lighter private stream closed before subscriptions were ready".to_owned(),
                    )
                });
                let _ = sender.send(Err(error));
            }
            let reason = result
                .err()
                .map(|error| error.to_string())
                .unwrap_or_else(|| "stream closed".to_owned());
            mark_lighter_stream_disconnected(&context.execution_ready, &context.events, reason);
            counter!("gateway_private_stream_reconnects_total", "venue" => "lighter").increment(1);
            tokio::time::sleep(reconnect_delay).await;
        }
    });
    let abort_handle = supervisor.abort_handle();
    tokio::spawn(async move {
        let reason = match supervisor.await {
            Ok(()) => "Lighter private-stream supervisor exited unexpectedly".to_owned(),
            Err(error) if error.is_cancelled() => {
                "Lighter private-stream supervisor was aborted".to_owned()
            }
            Err(error) => format!("Lighter private-stream supervisor panicked: {error}"),
        };
        mark_lighter_stream_disconnected(&monitor_readiness, &monitor_events, reason);
    });

    match tokio::time::timeout(startup_timeout, startup_receiver).await {
        Ok(Ok(Ok(()))) => Ok(()),
        Ok(Ok(Err(error))) => {
            execution_ready.store(false, Ordering::Release);
            abort_handle.abort();
            Err(error)
        }
        Ok(Err(_)) => {
            execution_ready.store(false, Ordering::Release);
            abort_handle.abort();
            Err(GatewayError::Transport(
                "Lighter private stream task stopped during startup".to_owned(),
            ))
        }
        Err(_) => {
            execution_ready.store(false, Ordering::Release);
            abort_handle.abort();
            Err(GatewayError::Transport(format!(
                "Lighter private stream was not authenticated within {} ms",
                startup_timeout.as_millis()
            )))
        }
    }
}

async fn run_private_stream_once(
    context: &PrivateStreamContext,
    startup_sender: &mut Option<oneshot::Sender<Result<(), GatewayError>>>,
) -> Result<(), GatewayError> {
    let token = context
        .signer
        .create_auth_token(context.api_key_index, context.account_index)?;
    let normalized_host = context
        .host
        .trim_start_matches("https://")
        .trim_start_matches("http://")
        .trim_end_matches('/');
    let url = format!("wss://{normalized_host}/stream");
    let connect_started = Instant::now();
    let (stream, _) = connect_async(&url)
        .await
        .map_err(|err| GatewayError::Transport(err.to_string()))?;
    histogram!("gateway_ws_connect_seconds", "venue" => "lighter")
        .record(connect_started.elapsed().as_secs_f64());
    let (mut writer, mut reader) = stream.split();
    let mut keepalive = tokio::time::interval(PRIVATE_STREAM_KEEPALIVE_INTERVAL);
    keepalive.set_missed_tick_behavior(MissedTickBehavior::Delay);
    keepalive.tick().await;
    let mut lifecycle_check = tokio::time::interval(Duration::from_secs(1));
    lifecycle_check.set_missed_tick_behavior(MissedTickBehavior::Delay);
    lifecycle_check.tick().await;
    let read_deadline = tokio::time::sleep(PRIVATE_STREAM_READ_TIMEOUT);
    tokio::pin!(read_deadline);
    let auth_rotation = tokio::time::sleep(PRIVATE_STREAM_AUTH_ROTATION_INTERVAL);
    tokio::pin!(auth_rotation);
    let mut orders_subscribed = false;
    let mut trades_subscribed = false;
    let mut announced_ready = false;

    loop {
        tokio::select! {
            _ = lifecycle_check.tick() => {
                let locked = context.state.lock().await;
                let stalled = stalled_lifecycle(&locked, Instant::now());
                drop(locked);
                if let Some((client_order_id, kind)) = stalled {
                    counter!(
                        "gateway_private_lifecycle_timeouts_total",
                        "venue" => "lighter",
                        "kind" => kind.label()
                    )
                    .increment(1);
                    return Err(GatewayError::Transport(format!(
                        "Lighter order {client_order_id} received no {} within {} seconds",
                        kind.label(),
                        PRIVATE_LIFECYCLE_TIMEOUT.as_secs()
                    )));
                }
            }
            _ = &mut auth_rotation => {
                return Err(GatewayError::Transport(
                    "rotating Lighter private stream before its 7-hour auth token expires"
                        .to_owned(),
                ));
            }
            _ = &mut read_deadline => {
                return Err(GatewayError::Transport(format!(
                    "Lighter private stream received no data for {} seconds",
                    PRIVATE_STREAM_READ_TIMEOUT.as_secs()
                )));
            }
            _ = keepalive.tick() => {
                writer.send(lighter_keepalive_message())
                    .await
                    .map_err(|err| GatewayError::Transport(err.to_string()))?;
            }
            next = reader.next() => {
                let Some(message) = next else { return Ok(()); };
                read_deadline
                    .as_mut()
                    .reset(TokioInstant::now() + PRIVATE_STREAM_READ_TIMEOUT);
                match message.map_err(|err| GatewayError::Transport(err.to_string()))? {
                    Message::Text(text) => {
                        let parse_started = Instant::now();
                        let value: serde_json::Value = serde_json::from_str(&text)
                            .map_err(|err| GatewayError::Protocol(err.to_string()))?;
                        histogram!("gateway_ws_parse_seconds", "venue" => "lighter")
                            .record(parse_started.elapsed().as_secs_f64());
                        let message_type = value.get("type").and_then(|v| v.as_str()).unwrap_or("");
                        match message_type {
                            "connected" => {
                                for channel in [
                                    format!("account_all_orders/{}", context.account_index),
                                    format!("account_all_trades/{}", context.account_index),
                                ] {
                                    writer.send(Message::Text(serde_json::json!({
                                        "type": "subscribe",
                                        "channel": channel,
                                        "auth": token.clone(),
                                    }).to_string().into())).await
                                        .map_err(|err| GatewayError::Transport(err.to_string()))?;
                                }
                            }
                            "ping" => {
                                writer.send(Message::Text(serde_json::json!({"type":"pong"}).to_string().into()))
                                    .await
                                    .map_err(|err| GatewayError::Transport(err.to_string()))?;
                            }
                            kind if kind.starts_with("error") => {
                                return Err(GatewayError::Protocol(format!(
                                    "Lighter private stream rejected subscription: {}",
                                    lighter_ws_error(&value)
                                )));
                            }
                            kind if kind.contains("account_all_orders") => {
                                orders_subscribed = true;
                                process_lighter_orders(
                                    &value,
                                    context.market_index,
                                    &context.events,
                                    &context.state,
                                )
                                .await?;
                            }
                            kind if kind.contains("account_all_trades") => {
                                trades_subscribed = true;
                                process_lighter_trades(
                                    &value,
                                    context.account_index,
                                    context.market_index,
                                    &context.events,
                                    &context.state,
                                )
                                .await?;
                            }
                            _ => {}
                        }
                        if orders_subscribed && trades_subscribed && !announced_ready {
                            let server_nonce = context
                                .api
                                .next_nonce(context.account_index, context.api_key_index)
                                .await
                                .map_err(|error| {
                                    GatewayError::Transport(format!(
                                        "refresh Lighter nonce before private stream readiness: {error}"
                                    ))
                                })?;
                            let mut lane = context.nonce.lock().await;
                            lane.next = lane.next.max(server_nonce);
                            drop(lane);
                            announced_ready = true;
                            context.execution_ready.store(true, Ordering::Release);
                            gauge!("gateway_private_stream_up", "venue" => "lighter").set(1.0);
                            let _ = context.events.send(ExecutionEvent {
                                venue: Venue::Lighter,
                                timestamp_ms: unix_timestamp_ms(),
                                client_order_id: None,
                                kind: ExecutionEventKind::Reconnected,
                            });
                            if let Some(sender) = startup_sender.take() {
                                let _ = sender.send(Ok(()));
                            }
                        }
                    }
                    Message::Ping(payload) => writer.send(Message::Pong(payload)).await
                        .map_err(|err| GatewayError::Transport(err.to_string()))?,
                    Message::Close(_) => return Ok(()),
                    _ => {}
                }
            }
        }
    }
}

fn lighter_keepalive_message() -> Message {
    // Lighter answers an application-level `ping` with `pong`. Sending an
    // unsolicited `pong` would keep only the write side active and would not
    // prove that a quiet private stream can still deliver inbound frames.
    Message::Text(serde_json::json!({"type":"ping"}).to_string().into())
}

fn lighter_ws_error(value: &serde_json::Value) -> String {
    // Never reflect server-controlled strings or the raw payload: an
    // authentication error could echo the subscription bearer token.
    value
        .get("code")
        .and_then(serde_json::Value::as_i64)
        .map(|code| format!("error code {code}"))
        .unwrap_or_else(|| "error details redacted".to_owned())
}

#[derive(Debug, Deserialize)]
struct LighterOrderIdentity {
    client_order_index: i64,
    market_index: i64,
    status: String,
}

#[derive(Debug, Deserialize)]
struct LighterOrderUpdate {
    order_index: i64,
    client_order_index: i64,
    market_index: i64,
    status: String,
    #[serde(default)]
    price: String,
    #[serde(default)]
    filled_base_amount: String,
    #[serde(default)]
    filled_quote_amount: String,
    #[serde(default)]
    timestamp: i64,
    #[serde(default)]
    updated_at: i64,
    #[serde(default)]
    transaction_time: i64,
}

#[derive(Debug, Deserialize)]
struct LighterTradeIdentity {
    trade_id: i64,
    market_id: i64,
    #[serde(default)]
    ask_client_id: Option<i64>,
    #[serde(default)]
    bid_client_id: Option<i64>,
    ask_account_id: i64,
    bid_account_id: i64,
}

#[derive(Debug, Deserialize)]
struct LighterTradeUpdate {
    trade_id: i64,
    market_id: i64,
    size: String,
    price: String,
    #[serde(default)]
    ask_client_id: Option<i64>,
    #[serde(default)]
    bid_client_id: Option<i64>,
    ask_account_id: i64,
    bid_account_id: i64,
    timestamp: i64,
}

fn lighter_private_protocol(channel: &'static str, message: impl Into<String>) -> GatewayError {
    counter!(
        "gateway_ws_invalid_messages_total",
        "venue" => "lighter",
        "channel" => channel
    )
    .increment(1);
    GatewayError::Protocol(message.into())
}

fn collect_candidate_objects<'a>(
    value: &'a serde_json::Value,
    candidate_keys: &[&str],
    output: &mut Vec<&'a serde_json::Value>,
) {
    match value {
        serde_json::Value::Object(map) => {
            if candidate_keys.iter().any(|key| map.contains_key(*key)) {
                output.push(value);
                return;
            }
            for nested in map.values() {
                collect_candidate_objects(nested, candidate_keys, output);
            }
        }
        serde_json::Value::Array(values) => {
            for nested in values {
                collect_candidate_objects(nested, candidate_keys, output);
            }
        }
        _ => {}
    }
}

async fn process_lighter_orders(
    value: &serde_json::Value,
    market_index: i32,
    events: &broadcast::Sender<ExecutionEvent>,
    state: &Mutex<GatewayState>,
) -> Result<(), GatewayError> {
    let mut objects = Vec::new();
    collect_candidate_objects(
        value,
        &[
            "order_index",
            "client_order_index",
            "market_index",
            "status",
            "filled_base_amount",
        ],
        &mut objects,
    );
    for object in objects {
        let identity =
            serde_json::from_value::<LighterOrderIdentity>(object.clone()).map_err(|error| {
                lighter_private_protocol(
                    "orders",
                    format!("invalid Lighter order identity/status: {error}"),
                )
            })?;
        if identity.client_order_index <= 0 {
            continue;
        }
        if identity.market_index != i64::from(market_index) {
            counter!("gateway_ws_ignored_messages_total", "venue" => "lighter", "reason" => "wrong_market")
                .increment(1);
            continue;
        }
        let client_order_id = ClientOrderId(identity.client_order_index as u64);
        let identity_status = identity.status.trim().to_ascii_lowercase();
        let mut locked = state.lock().await;
        if locked.http_rejected_client_ids.contains(&client_order_id) {
            if identity_status.contains("reject") {
                continue;
            }
            return Err(lighter_private_protocol(
                "orders",
                format!(
                    "Lighter private status {:?} contradicted an HTTP rejection for {client_order_id}",
                    identity.status
                ),
            ));
        }
        if locked.terminal_client_ids.contains(&client_order_id) {
            return Err(lighter_private_protocol(
                "orders",
                format!(
                    "late Lighter status {:?} arrived after order {client_order_id} was terminal",
                    identity.status
                ),
            ));
        }
        if !locked.orders.contains_key(&client_order_id) {
            continue;
        }
        let order =
            serde_json::from_value::<LighterOrderUpdate>(object.clone()).map_err(|error| {
                lighter_private_protocol(
                    "orders",
                    format!("invalid Lighter update for tracked order {client_order_id}: {error}"),
                )
            })?;
        if order.client_order_index != identity.client_order_index
            || order.market_index != identity.market_index
            || order.status != identity.status
        {
            return Err(lighter_private_protocol(
                "orders",
                format!("inconsistent Lighter identity for tracked order {client_order_id}"),
            ));
        }
        if order.order_index < 0 {
            return Err(lighter_private_protocol(
                "orders",
                format!(
                    "negative Lighter venue order index {} for tracked order {client_order_id}",
                    order.order_index
                ),
            ));
        }
        let timestamp_ms =
            latest_timestamp_ms([order.updated_at, order.transaction_time, order.timestamp])
                .ok_or_else(|| {
                    lighter_private_protocol(
                "orders",
                format!("tracked Lighter order {client_order_id} has no valid positive timestamp"),
            )
                })?;
        let status = order.status.trim().to_ascii_lowercase();
        let is_active = matches!(
            status.as_str(),
            "open" | "new" | "in-progress" | "pending" | "partially_filled" | "partially-filled"
        );
        let mut event_kinds = Vec::with_capacity(2);
        let terminal = if is_active {
            let venue_order_id = VenueOrderId(order.order_index.to_string());
            let Some(tracked) = locked.orders.get_mut(&client_order_id) else {
                continue;
            };
            let reported_cumulative_quantity = Decimal::from_str(&order.filled_base_amount)
                .map_err(|error| {
                    lighter_private_protocol(
                        "orders",
                        format!(
                            "invalid active cumulative fill for tracked Lighter order {client_order_id}: {error}"
                        ),
                    )
                })?;
            if reported_cumulative_quantity < Decimal::ZERO
                || reported_cumulative_quantity > tracked.requested_quantity
            {
                return Err(lighter_private_protocol(
                    "orders",
                    format!(
                        "Lighter active cumulative fill {reported_cumulative_quantity} is outside [0, {}] for tracked order {client_order_id}",
                        tracked.requested_quantity
                    ),
                ));
            }
            if reported_cumulative_quantity > tracked.cumulative_quantity {
                return Err(lighter_private_protocol(
                    "orders",
                    format!(
                        "Lighter active update reports cumulative fill {reported_cumulative_quantity} ahead of authoritative private trades {} for tracked order {client_order_id}",
                        tracked.cumulative_quantity
                    ),
                ));
            }
            if let Some(existing_venue_order_id) = &tracked.venue_order_id {
                if existing_venue_order_id != &venue_order_id {
                    return Err(lighter_private_protocol(
                        "orders",
                        format!(
                            "Lighter tracked order {client_order_id} changed venue order index from {existing_venue_order_id} to {venue_order_id}"
                        ),
                    ));
                }
            }
            let duplicate_status = tracked.last_status.as_deref() == Some(status.as_str());
            tracked.last_status = Some(status.clone());
            tracked.venue_order_id = Some(venue_order_id.clone());
            if tracked
                .lifecycle_wait
                .as_ref()
                .is_some_and(|wait| wait.kind == LifecycleKind::PrivateAcceptance)
            {
                tracked.lifecycle_wait = None;
            }
            if !duplicate_status {
                event_kinds.push(ExecutionEventKind::Accepted { venue_order_id });
            }
            false
        } else if status == "filled" {
            let expected_cumulative_quantity = Decimal::from_str(&order.filled_base_amount)
                .map_err(|error| {
                    lighter_private_protocol(
                        "orders",
                        format!(
                            "invalid filled quantity for tracked Lighter order {client_order_id}: {error}"
                        ),
                    )
                })?;
            let Some(tracked) = locked.orders.get_mut(&client_order_id) else {
                continue;
            };
            if expected_cumulative_quantity <= Decimal::ZERO
                || expected_cumulative_quantity > tracked.requested_quantity
            {
                return Err(lighter_private_protocol(
                    "orders",
                    format!(
                        "Lighter terminal cumulative fill {expected_cumulative_quantity} is outside (0, {}] for tracked order {client_order_id}",
                        tracked.requested_quantity
                    ),
                ));
            }
            tracked.last_status = Some(status.clone());
            if tracked.cumulative_quantity >= expected_cumulative_quantity {
                // The trade stream has already delivered every economic fill.
                // This zero-quantity event carries terminality only.
                let price = terminal_fill_price(&order).map_err(|error| {
                    lighter_private_protocol("orders", format!("{client_order_id}: {error}"))
                })?;
                event_kinds.push(ExecutionEventKind::Fill {
                    trade_id: format!("order-final-{}-{timestamp_ms}", order.order_index),
                    price,
                    quantity: Decimal::ZERO,
                    cumulative_quantity: tracked.cumulative_quantity,
                    is_final: true,
                });
                true
            } else {
                tracked.pending_terminal = Some(PendingTerminal {
                    kind: PendingTerminalKind::Filled,
                    expected_cumulative_quantity,
                    timestamp_ms,
                });
                tracked.lifecycle_wait =
                    Some(LifecycleWait::new(LifecycleKind::FillReconciliation));
                false
            }
        } else if status == "canceled" {
            let expected_cumulative_quantity = Decimal::from_str(&order.filled_base_amount)
                .map_err(|error| {
                    lighter_private_protocol(
                        "orders",
                        format!(
                            "invalid canceled quantity for tracked Lighter order {client_order_id}: {error}"
                        ),
                    )
                })?;
            let Some(tracked) = locked.orders.get_mut(&client_order_id) else {
                continue;
            };
            if expected_cumulative_quantity < Decimal::ZERO
                || expected_cumulative_quantity > tracked.requested_quantity
            {
                return Err(lighter_private_protocol(
                    "orders",
                    format!(
                        "Lighter cancel cumulative fill {expected_cumulative_quantity} is outside [0, {}] for tracked order {client_order_id}",
                        tracked.requested_quantity
                    ),
                ));
            }
            tracked.last_status = Some(status.clone());
            if tracked.cumulative_quantity >= expected_cumulative_quantity {
                event_kinds.push(ExecutionEventKind::Canceled);
                true
            } else {
                // Wait for authoritative trade messages before allowing the
                // cancel to remove this order from gateway/OMS state.
                tracked.pending_terminal = Some(PendingTerminal {
                    kind: PendingTerminalKind::Canceled,
                    expected_cumulative_quantity,
                    timestamp_ms,
                });
                tracked.lifecycle_wait =
                    Some(LifecycleWait::new(LifecycleKind::FillReconciliation));
                false
            }
        } else if status == "expired" || status.contains("reject") || status.contains("cancel") {
            let expected_cumulative_quantity = Decimal::from_str(&order.filled_base_amount)
                .map_err(|error| {
                    lighter_private_protocol(
                        "orders",
                        format!(
                            "invalid rejected quantity for tracked Lighter order {client_order_id}: {error}"
                        ),
                    )
                })?;
            let Some(tracked) = locked.orders.get_mut(&client_order_id) else {
                continue;
            };
            if expected_cumulative_quantity < Decimal::ZERO
                || expected_cumulative_quantity > tracked.requested_quantity
            {
                return Err(lighter_private_protocol(
                    "orders",
                    format!(
                        "Lighter rejection cumulative fill {expected_cumulative_quantity} is outside [0, {}] for tracked order {client_order_id}",
                        tracked.requested_quantity
                    ),
                ));
            }
            tracked.last_status = Some(status.clone());
            if tracked.cumulative_quantity >= expected_cumulative_quantity {
                event_kinds.push(ExecutionEventKind::Rejected {
                    reason: order.status,
                });
                true
            } else {
                // Constraint-driven cancellations are rejections from the
                // strategy's point of view. Wait for authoritative trades,
                // then reject so the OMS cannot blindly resubmit the same
                // unchanged revision.
                tracked.pending_terminal = Some(PendingTerminal {
                    kind: PendingTerminalKind::Rejected(order.status),
                    expected_cumulative_quantity,
                    timestamp_ms,
                });
                tracked.lifecycle_wait =
                    Some(LifecycleWait::new(LifecycleKind::FillReconciliation));
                false
            }
        } else {
            return Err(lighter_private_protocol(
                "orders",
                format!(
                    "unknown Lighter status {:?} for tracked order {client_order_id}",
                    order.status
                ),
            ));
        };
        if terminal {
            locked.orders.remove(&client_order_id);
            locked.terminal_client_ids.insert(client_order_id);
        }
        drop(locked);
        for kind in event_kinds {
            crate::telemetry::record_event_lag(
                "gateway_exchange_event_lag_seconds",
                timestamp_ms,
                "lighter",
            );
            let _ = events.send(ExecutionEvent {
                venue: Venue::Lighter,
                timestamp_ms,
                client_order_id: Some(client_order_id),
                kind,
            });
        }
    }
    Ok(())
}

async fn process_lighter_trades(
    value: &serde_json::Value,
    account_index: i64,
    market_index: i32,
    events: &broadcast::Sender<ExecutionEvent>,
    state: &Mutex<GatewayState>,
) -> Result<(), GatewayError> {
    let mut objects = Vec::new();
    collect_candidate_objects(
        value,
        &[
            "trade_id",
            "market_id",
            "size",
            "ask_client_id",
            "bid_client_id",
            "ask_account_id",
            "bid_account_id",
        ],
        &mut objects,
    );
    for object in objects {
        let identity =
            serde_json::from_value::<LighterTradeIdentity>(object.clone()).map_err(|error| {
                lighter_private_protocol(
                    "trades",
                    format!("invalid Lighter trade identity: {error}"),
                )
            })?;
        if identity.market_id != i64::from(market_index) {
            counter!("gateway_ws_ignored_messages_total", "venue" => "lighter", "reason" => "wrong_market")
                .increment(1);
            continue;
        }
        let ask_owned = identity.ask_account_id == account_index;
        let bid_owned = identity.bid_account_id == account_index;
        let raw_client_id = match (ask_owned, bid_owned) {
            (false, false) => continue,
            (true, false) => identity.ask_client_id.filter(|id| *id > 0).ok_or_else(|| {
                lighter_private_protocol(
                    "trades",
                    format!(
                        "account-owned ask trade {} has no positive client order ID",
                        identity.trade_id
                    ),
                )
            })?,
            (false, true) => identity.bid_client_id.filter(|id| *id > 0).ok_or_else(|| {
                lighter_private_protocol(
                    "trades",
                    format!(
                        "account-owned bid trade {} has no positive client order ID",
                        identity.trade_id
                    ),
                )
            })?,
            (true, true) => {
                let ask_client_id =
                    identity.ask_client_id.filter(|id| *id > 0).ok_or_else(|| {
                        lighter_private_protocol(
                            "trades",
                            format!(
                                "self-trade {} has no positive ask client order ID",
                                identity.trade_id
                            ),
                        )
                    })?;
                let bid_client_id =
                    identity.bid_client_id.filter(|id| *id > 0).ok_or_else(|| {
                        lighter_private_protocol(
                            "trades",
                            format!(
                                "self-trade {} has no positive bid client order ID",
                                identity.trade_id
                            ),
                        )
                    })?;
                if ask_client_id != bid_client_id {
                    return Err(lighter_private_protocol(
                        "trades",
                        format!(
                            "self-trade {} references two client orders ({ask_client_id}, {bid_client_id}) and cannot be reconciled as one fill",
                            identity.trade_id
                        ),
                    ));
                }
                ask_client_id
            }
        };
        if raw_client_id as u64 > MAX_CLIENT_ORDER_INDEX {
            return Err(lighter_private_protocol(
                "trades",
                format!(
                    "account-owned trade {} has client order ID {raw_client_id} outside Lighter's uint48 range",
                    identity.trade_id
                ),
            ));
        }
        let client_order_id = ClientOrderId(raw_client_id as u64);
        let mut locked = state.lock().await;
        if locked.seen_trades.contains(&identity.trade_id) {
            continue;
        }
        if locked.terminal_client_ids.contains(&client_order_id) {
            return Err(lighter_private_protocol(
                "trades",
                format!(
                    "late Lighter trade {} arrived after order {client_order_id} was terminal",
                    identity.trade_id
                ),
            ));
        }
        if !locked.orders.contains_key(&client_order_id) {
            continue;
        }
        let trade =
            serde_json::from_value::<LighterTradeUpdate>(object.clone()).map_err(|error| {
                lighter_private_protocol(
                    "trades",
                    format!("invalid fill for tracked Lighter order {client_order_id}: {error}"),
                )
            })?;
        if trade.trade_id != identity.trade_id
            || trade.market_id != identity.market_id
            || trade.ask_client_id != identity.ask_client_id
            || trade.bid_client_id != identity.bid_client_id
            || trade.ask_account_id != identity.ask_account_id
            || trade.bid_account_id != identity.bid_account_id
        {
            return Err(lighter_private_protocol(
                "trades",
                format!("inconsistent Lighter trade identity for tracked order {client_order_id}"),
            ));
        }
        let price = Decimal::from_str(&trade.price).map_err(|error| {
            lighter_private_protocol(
                "trades",
                format!("invalid Lighter fill price for {client_order_id}: {error}"),
            )
        })?;
        let quantity = Decimal::from_str(&trade.size).map_err(|error| {
            lighter_private_protocol(
                "trades",
                format!("invalid Lighter fill quantity for {client_order_id}: {error}"),
            )
        })?;
        if price <= Decimal::ZERO || quantity <= Decimal::ZERO {
            return Err(lighter_private_protocol(
                "trades",
                format!(
                    "nonpositive Lighter fill price/quantity {price}/{quantity} for {client_order_id}"
                ),
            ));
        }
        let timestamp_ms = normalize_positive_timestamp_ms(trade.timestamp).ok_or_else(|| {
            lighter_private_protocol(
                "trades",
                format!("invalid Lighter fill timestamp {}", trade.timestamp),
            )
        })?;
        let (cumulative_quantity, is_final, terminal_after_fill) = {
            let Some(tracked) = locked.orders.get_mut(&client_order_id) else {
                continue;
            };
            let cumulative_quantity = tracked
                .cumulative_quantity
                .checked_add(quantity)
                .ok_or_else(|| {
                    lighter_private_protocol(
                        "trades",
                        format!("Lighter cumulative fill overflow for {client_order_id}"),
                    )
                })?;
            if cumulative_quantity > tracked.requested_quantity {
                return Err(lighter_private_protocol(
                    "trades",
                    format!(
                        "Lighter cumulative fill {cumulative_quantity} exceeds requested quantity {} for {client_order_id}",
                        tracked.requested_quantity
                    ),
                ));
            }
            let pending_terminal = tracked.pending_terminal.clone();
            let pending_reconciled = pending_terminal
                .as_ref()
                .is_some_and(|pending| cumulative_quantity >= pending.expected_cumulative_quantity);
            let is_final = cumulative_quantity >= tracked.requested_quantity
                || pending_terminal.as_ref().is_some_and(|pending| {
                    matches!(&pending.kind, PendingTerminalKind::Filled) && pending_reconciled
                });
            let terminal_after_fill = if !is_final && pending_reconciled {
                pending_terminal.clone().filter(|pending| {
                    matches!(
                        &pending.kind,
                        PendingTerminalKind::Canceled | PendingTerminalKind::Rejected(_)
                    )
                })
            } else {
                None
            };
            tracked.cumulative_quantity = cumulative_quantity;
            if !is_final && terminal_after_fill.is_none() {
                observe_private_fill(&mut tracked.lifecycle_wait);
            }
            (cumulative_quantity, is_final, terminal_after_fill)
        };
        locked.seen_trades.insert(trade.trade_id);
        if is_final || terminal_after_fill.is_some() {
            locked.orders.remove(&client_order_id);
            locked.terminal_client_ids.insert(client_order_id);
        }
        drop(locked);
        crate::telemetry::record_event_lag(
            "gateway_exchange_event_lag_seconds",
            timestamp_ms,
            "lighter",
        );
        let _ = events.send(ExecutionEvent {
            venue: Venue::Lighter,
            timestamp_ms,
            client_order_id: Some(client_order_id),
            kind: ExecutionEventKind::Fill {
                trade_id: format!("lighter-{}-{}", trade.market_id, trade.trade_id),
                price,
                quantity,
                cumulative_quantity,
                is_final,
            },
        });
        if let Some(terminal) = terminal_after_fill {
            let terminal_at = terminal.timestamp_ms.max(timestamp_ms);
            crate::telemetry::record_event_lag(
                "gateway_exchange_event_lag_seconds",
                terminal_at,
                "lighter",
            );
            let kind = match terminal.kind {
                PendingTerminalKind::Canceled => ExecutionEventKind::Canceled,
                PendingTerminalKind::Rejected(reason) => ExecutionEventKind::Rejected { reason },
                PendingTerminalKind::Filled => {
                    return Err(lighter_private_protocol(
                        "trades",
                        "invalid filled terminal reconciliation state",
                    ));
                }
            };
            let _ = events.send(ExecutionEvent {
                venue: Venue::Lighter,
                timestamp_ms: terminal_at,
                client_order_id: Some(client_order_id),
                kind,
            });
        }
    }
    Ok(())
}

#[cfg(test)]
fn collect_matching_objects<'a>(
    value: &'a serde_json::Value,
    required_key: &str,
    output: &mut Vec<&'a serde_json::Value>,
) {
    match value {
        serde_json::Value::Object(map) => {
            if map.contains_key(required_key) {
                output.push(value);
                return;
            }
            for nested in map.values() {
                collect_matching_objects(nested, required_key, output);
            }
        }
        serde_json::Value::Array(values) => {
            for nested in values {
                collect_matching_objects(nested, required_key, output);
            }
        }
        _ => {}
    }
}

fn terminal_fill_price(order: &LighterOrderUpdate) -> Result<Decimal, GatewayError> {
    let base = Decimal::from_str(&order.filled_base_amount)
        .map_err(|error| GatewayError::Protocol(format!("invalid filled base amount: {error}")))?;
    let quote = Decimal::from_str(&order.filled_quote_amount)
        .map_err(|error| GatewayError::Protocol(format!("invalid filled quote amount: {error}")))?;
    let price = if base > Decimal::ZERO && quote > Decimal::ZERO {
        quote.checked_div(base).ok_or_else(|| {
            GatewayError::Protocol("Lighter average fill price overflow".to_owned())
        })?
    } else {
        Decimal::from_str(&order.price)
            .map_err(|error| GatewayError::Protocol(format!("invalid order price: {error}")))?
    };
    if price <= Decimal::ZERO {
        return Err(GatewayError::Protocol(format!(
            "nonpositive Lighter terminal fill price {price}"
        )));
    }
    Ok(price)
}

#[cfg(test)]
fn normalize_timestamp_ms(timestamp: i64) -> u64 {
    normalize_positive_timestamp_ms(timestamp).unwrap_or_else(unix_timestamp_ms)
}

fn latest_timestamp_ms(timestamps: [i64; 3]) -> Option<u64> {
    timestamps
        .into_iter()
        .filter_map(normalize_positive_timestamp_ms)
        .max()
}

fn normalize_positive_timestamp_ms(timestamp: i64) -> Option<u64> {
    if timestamp <= 0 {
        return None;
    }
    let timestamp = timestamp as u64;
    Some(if timestamp >= 100_000_000_000_000_000 {
        // Nanoseconds since epoch.
        timestamp / 1_000_000
    } else if timestamp >= 100_000_000_000_000 {
        // Lighter's transaction_time field is microseconds since epoch.
        timestamp / 1_000
    } else if timestamp < 10_000_000_000 {
        // Seconds since epoch.
        timestamp * 1_000
    } else {
        // Milliseconds since epoch.
        timestamp
    })
}

fn read_env_parse<T>(name: &str) -> Result<T, GatewayError>
where
    T: FromStr,
{
    let value = read_required_env(name, false)?;
    value.parse::<T>().map_err(|_| {
        GatewayError::Authentication(format!("environment variable {name} has an invalid value"))
    })
}

fn read_required_env(name: &str, secret: bool) -> Result<String, GatewayError> {
    std::env::var(name).map_err(|_| {
        let kind = if secret { "secret" } else { "value" };
        GatewayError::Authentication(format!(
            "required {kind} environment variable {name} is not set"
        ))
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::str::FromStr;
    use std::sync::atomic::{AtomicBool, Ordering};

    use futures_util::{SinkExt, StreamExt};
    use rust_decimal::Decimal;
    use tokio::io::duplex;
    use tokio::sync::{broadcast, Mutex};
    use tokio::time::Instant as TokioInstant;
    use tokio_tungstenite::tungstenite::{protocol::Role, Message};
    use tokio_tungstenite::WebSocketStream;

    use super::{
        checked_order_notional, classify_send_tx_http_response, classify_submit_rejection,
        collect_matching_objects, latest_timestamp_ms, lighter_keepalive_message, lighter_ws_error,
        mark_lighter_stream_disconnected, nonce_with_successor, normalize_timestamp_ms,
        process_lighter_orders, process_lighter_trades, scale_decimal_i64,
        validate_lighter_account_index, validate_lighter_market_index, validate_lighter_nonce,
        validate_live_host, GatewayState, SendTxResponse, TrackedOrder,
        PRIVATE_STREAM_AUTH_ROTATION_INTERVAL, PRIVATE_STREAM_KEEPALIVE_INTERVAL,
        PRIVATE_STREAM_READ_TIMEOUT,
    };
    use crate::gateway::{GatewayError, LifecycleKind, LifecycleWait};
    use crate::types::{ClientOrderId, ExecutionEventKind};

    #[test]
    fn lighter_integer_scaling_is_exact() {
        assert_eq!(
            scale_decimal_i64(Decimal::new(1, 3), 5, "quantity").unwrap(),
            100
        );
        assert_eq!(
            scale_decimal_i64(Decimal::new(100_000, 0), 1, "price").unwrap(),
            1_000_000
        );
        assert!(scale_decimal_i64(Decimal::new(15, 6), 5, "quantity").is_err());
        assert!(scale_decimal_i64(Decimal::MAX, 1, "quantity").is_err());
        assert!(checked_order_notional(Decimal::MAX, Decimal::from(2)).is_err());
    }

    #[test]
    fn nonce_validation_never_wraps_or_installs_sentinels() {
        assert_eq!(validate_lighter_nonce(0).unwrap(), 0);
        assert!(validate_lighter_nonce(-1).is_err());
        assert!(validate_lighter_nonce(i64::MAX).is_err());
        assert_eq!(nonce_with_successor(7).unwrap(), (7, 8));
        assert!(nonce_with_successor(i64::MAX - 1).is_err());
    }

    #[test]
    fn non_success_send_tx_http_status_is_indeterminate_even_with_json_body() {
        for status in [
            reqwest::StatusCode::TOO_MANY_REQUESTS,
            reqwest::StatusCode::INTERNAL_SERVER_ERROR,
        ] {
            assert!(matches!(
                classify_send_tx_http_response(
                    status,
                    SendTxResponse {
                        code: status.as_u16().into(),
                        message: Some("retry later".to_owned()),
                    },
                ),
                Err(GatewayError::Indeterminate(_))
            ));
        }

        // A successful HTTP response is passed to the API-code classifier in
        // submit/cancel. Its explicit non-200 body code remains a definitive
        // venue business rejection.
        assert!(classify_send_tx_http_response(
            reqwest::StatusCode::OK,
            SendTxResponse {
                code: 400,
                message: Some("definitive API rejection".to_owned()),
            },
        )
        .is_ok());
    }

    #[test]
    fn account_and_market_indices_reject_negative_or_unencodable_values() {
        assert_eq!(validate_lighter_account_index(0).unwrap(), 0);
        assert!(validate_lighter_account_index(-1).is_err());
        assert_eq!(validate_lighter_market_index(0).unwrap(), 0);
        assert_eq!(
            validate_lighter_market_index(i64::from(i16::MAX)).unwrap(),
            i32::from(i16::MAX)
        );
        assert!(validate_lighter_market_index(-1).is_err());
        assert!(validate_lighter_market_index(i64::from(i16::MAX) + 1).is_err());
    }

    #[test]
    fn disconnect_transition_clears_readiness_before_broadcast() {
        let ready = AtomicBool::new(true);
        let (events, mut receiver) = broadcast::channel(4);
        mark_lighter_stream_disconnected(&ready, &events, "supervisor panic".to_owned());
        let event = receiver.try_recv().expect("disconnect event");
        assert!(!ready.load(Ordering::Acquire));
        assert!(matches!(
            event.kind,
            ExecutionEventKind::Disconnected { reason } if reason == "supervisor panic"
        ));
    }

    #[test]
    fn live_host_and_symbol_validation_is_consistent() {
        assert_eq!(
            validate_live_host("  mainnet.zklighter.elliot.ai ").unwrap(),
            "mainnet.zklighter.elliot.ai"
        );
        assert_eq!(
            validate_live_host("https://mainnet.zklighter.elliot.ai/").unwrap(),
            "mainnet.zklighter.elliot.ai"
        );
        assert!(validate_live_host("").is_err());
        assert!(validate_live_host("http://mainnet.zklighter.elliot.ai").is_err());
        assert!(validate_live_host("https://testnet.zklighter.elliot.ai").is_err());
        assert!(validate_live_host("https://example.com").is_err());
        assert!(super::lighter_symbol_matches("BTC", "btc"));
    }

    #[test]
    fn websocket_errors_never_reflect_server_strings_or_raw_auth_payloads() {
        let secret = "secret-bearer-token";
        let redacted = lighter_ws_error(&serde_json::json!({
            "type": "error/auth",
            "message": format!("invalid auth {secret}"),
            "auth": secret,
        }));
        assert_eq!(redacted, "error details redacted");
        assert!(!redacted.contains(secret));

        assert_eq!(
            lighter_ws_error(&serde_json::json!({
                "type": "error/subscription",
                "code": 401,
                "message": format!("invalid auth {secret}"),
            })),
            "error code 401"
        );
    }

    #[test]
    fn private_stream_liveness_windows_are_conservative() {
        assert!(PRIVATE_STREAM_READ_TIMEOUT >= PRIVATE_STREAM_KEEPALIVE_INTERVAL * 2);
        assert!(PRIVATE_STREAM_AUTH_ROTATION_INTERVAL.as_secs() <= 6 * 60 * 60);
    }

    #[tokio::test(start_paused = true)]
    async fn client_ping_keeps_a_quiet_private_socket_inside_the_read_watchdog() {
        let (client_io, server_io) = duplex(4_096);
        let (client_socket, mut server_socket) = tokio::join!(
            WebSocketStream::from_raw_socket(client_io, Role::Client, None),
            WebSocketStream::from_raw_socket(server_io, Role::Server, None),
        );
        let (mut writer, mut reader) = client_socket.split();

        let server = tokio::spawn(async move {
            for _ in 0..3 {
                let message = server_socket
                    .next()
                    .await
                    .expect("client heartbeat frame")
                    .expect("valid client heartbeat");
                let Message::Text(text) = message else {
                    panic!("heartbeat must be application JSON text");
                };
                let value: serde_json::Value =
                    serde_json::from_str(text.as_str()).expect("heartbeat JSON");
                assert_eq!(
                    value.get("type").and_then(|value| value.as_str()),
                    Some("ping")
                );
                server_socket
                    .send(Message::Text(
                        serde_json::json!({"type":"pong"}).to_string().into(),
                    ))
                    .await
                    .expect("server pong");
            }
            // Keep the peer open beyond the client's original 90-second read
            // deadline so passing cannot be caused by an early socket reset.
            tokio::time::sleep(std::time::Duration::from_secs(10)).await;
        });

        let mut keepalive = tokio::time::interval(PRIVATE_STREAM_KEEPALIVE_INTERVAL);
        keepalive.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        keepalive.tick().await;
        let read_deadline = tokio::time::sleep(PRIVATE_STREAM_READ_TIMEOUT);
        tokio::pin!(read_deadline);
        let beyond_original_watchdog = tokio::time::sleep_until(
            TokioInstant::now() + PRIVATE_STREAM_READ_TIMEOUT + std::time::Duration::from_secs(5),
        );
        tokio::pin!(beyond_original_watchdog);
        let mut pongs = 0_u8;

        loop {
            tokio::select! {
                _ = keepalive.tick() => {
                    writer
                        .send(lighter_keepalive_message())
                        .await
                        .expect("client ping");
                }
                message = reader.next() => {
                    let message = message
                        .expect("server response frame")
                        .expect("valid server response");
                    read_deadline
                        .as_mut()
                        .reset(TokioInstant::now() + PRIVATE_STREAM_READ_TIMEOUT);
                    if let Message::Text(text) = message {
                        let value: serde_json::Value =
                            serde_json::from_str(text.as_str()).expect("response JSON");
                        if value.get("type").and_then(|value| value.as_str()) == Some("pong") {
                            pongs += 1;
                        }
                    }
                }
                _ = &mut read_deadline => {
                    panic!("read watchdog fired despite responsive ping/pong heartbeat");
                }
                _ = &mut beyond_original_watchdog => break,
            }
        }

        assert_eq!(pongs, 3);
        server.await.expect("heartbeat server task");
    }

    #[test]
    fn finds_nested_ws_entities() {
        let value = serde_json::json!({
            "orders": {"1": [{"client_order_index": 42, "status": "open"}]}
        });
        let mut found = Vec::new();
        collect_matching_objects(&value, "client_order_index", &mut found);
        assert_eq!(found.len(), 1);
    }

    #[test]
    fn normalizes_lighter_timestamp_units() {
        assert_eq!(normalize_timestamp_ms(1_771_943_742), 1_771_943_742_000);
        assert_eq!(normalize_timestamp_ms(1_771_943_742_851), 1_771_943_742_851);
        assert_eq!(
            normalize_timestamp_ms(1_771_943_742_851_429),
            1_771_943_742_851
        );
        assert_eq!(
            normalize_timestamp_ms(1_771_943_742_851_429_000),
            1_771_943_742_851
        );
        assert_eq!(
            latest_timestamp_ms([1_771_943_742_900, 1_771_943_742_851_429, 0,]),
            Some(1_771_943_742_900)
        );
    }

    #[tokio::test]
    async fn terminal_cancel_waits_for_authoritative_trade() {
        let client_order_id = ClientOrderId(42);
        let state = Mutex::new(GatewayState {
            orders: HashMap::from([(
                client_order_id,
                TrackedOrder {
                    requested_quantity: Decimal::ONE,
                    cumulative_quantity: Decimal::ZERO,
                    venue_order_id: None,
                    last_status: None,
                    pending_terminal: None,
                    lifecycle_wait: None,
                },
            )]),
            seen_trades: Default::default(),
            terminal_client_ids: Default::default(),
            http_rejected_client_ids: Default::default(),
        });
        let (events, mut receiver) = broadcast::channel(8);

        process_lighter_orders(
            &serde_json::json!({
                "orders": {"0": [{
                    "order_index": 9001,
                    "client_order_index": 42,
                    "market_index": 0,
                    "status": "canceled",
                    "price": "100000",
                    "filled_base_amount": "0.4",
                    "filled_quote_amount": "40000",
                    "transaction_time": 1_771_943_742_851_429_i64
                }]}
            }),
            0,
            &events,
            &state,
        )
        .await
        .unwrap();
        assert!(receiver.try_recv().is_err());
        assert!(state.lock().await.orders.contains_key(&client_order_id));

        process_lighter_trades(
            &serde_json::json!({
                "trades": [{
                    "trade_id": 7,
                    "market_id": 0,
                    "size": "0.4",
                    "price": "100000",
                    "ask_client_id": 42,
                    "ask_account_id": 17,
                    "bid_account_id": 18,
                    "timestamp": 1_771_943_742_852_i64
                }]
            }),
            17,
            0,
            &events,
            &state,
        )
        .await
        .unwrap();

        let fill = receiver.recv().await.expect("partial fill");
        assert!(matches!(
            fill.kind,
            ExecutionEventKind::Fill {
                quantity,
                is_final: false,
                ..
            } if quantity == Decimal::from_str("0.4").unwrap()
        ));
        assert!(matches!(
            receiver.recv().await.expect("cancel after fill").kind,
            ExecutionEventKind::Canceled
        ));
        assert!(!state.lock().await.orders.contains_key(&client_order_id));
    }

    #[tokio::test]
    async fn constraint_cancel_is_rejected_instead_of_resubmittable_cancel() {
        let client_order_id = ClientOrderId(44);
        let state = Mutex::new(GatewayState {
            orders: HashMap::from([(
                client_order_id,
                TrackedOrder {
                    requested_quantity: Decimal::ONE,
                    cumulative_quantity: Decimal::ZERO,
                    venue_order_id: None,
                    last_status: None,
                    pending_terminal: None,
                    lifecycle_wait: None,
                },
            )]),
            seen_trades: Default::default(),
            terminal_client_ids: Default::default(),
            http_rejected_client_ids: Default::default(),
        });
        let (events, mut receiver) = broadcast::channel(8);

        process_lighter_orders(
            &serde_json::json!({
                "orders": [{
                    "order_index": 9003,
                    "client_order_index": 44,
                    "market_index": 0,
                    "status": "canceled-margin-not-allowed",
                    "price": "100000",
                    "filled_base_amount": "0",
                    "filled_quote_amount": "0",
                    "updated_at": 1_771_943_742_851_i64
                }]
            }),
            0,
            &events,
            &state,
        )
        .await
        .unwrap();

        assert!(matches!(
            receiver.recv().await.expect("constraint rejection").kind,
            ExecutionEventKind::Rejected { reason }
                if reason == "canceled-margin-not-allowed"
        ));
        assert!(!state.lock().await.orders.contains_key(&client_order_id));
    }

    #[tokio::test]
    async fn filled_order_update_does_not_duplicate_trade_quantity() {
        let client_order_id = ClientOrderId(43);
        let state = Mutex::new(GatewayState {
            orders: HashMap::from([(
                client_order_id,
                TrackedOrder {
                    requested_quantity: Decimal::ONE,
                    cumulative_quantity: Decimal::ZERO,
                    venue_order_id: None,
                    last_status: None,
                    pending_terminal: None,
                    lifecycle_wait: None,
                },
            )]),
            seen_trades: Default::default(),
            terminal_client_ids: Default::default(),
            http_rejected_client_ids: Default::default(),
        });
        let (events, mut receiver) = broadcast::channel(8);

        process_lighter_orders(
            &serde_json::json!({
                "orders": [{
                    "order_index": 9002,
                    "client_order_index": 43,
                    "market_index": 0,
                    "status": "filled",
                    "price": "100000",
                    "filled_base_amount": "1",
                    "filled_quote_amount": "100000",
                    "updated_at": 1_771_943_742_851_i64
                }]
            }),
            0,
            &events,
            &state,
        )
        .await
        .unwrap();
        assert!(receiver.try_recv().is_err());

        process_lighter_trades(
            &serde_json::json!({
                "trades": [{
                    "trade_id": 8,
                    "market_id": 0,
                    "size": "1",
                    "price": "100000",
                    "bid_client_id": 43,
                    "ask_account_id": 18,
                    "bid_account_id": 17,
                    "timestamp": 1_771_943_742_852_i64
                }]
            }),
            17,
            0,
            &events,
            &state,
        )
        .await
        .unwrap();

        let fill = receiver.recv().await.expect("final fill");
        assert!(matches!(
            fill.kind,
            ExecutionEventKind::Fill {
                quantity,
                cumulative_quantity,
                is_final: true,
                ..
            } if quantity == Decimal::ONE && cumulative_quantity == Decimal::ONE
        ));
        assert!(receiver.try_recv().is_err());
    }

    fn tracked_state(client_order_id: ClientOrderId) -> Mutex<GatewayState> {
        Mutex::new(GatewayState {
            orders: HashMap::from([(
                client_order_id,
                TrackedOrder {
                    requested_quantity: Decimal::ONE,
                    cumulative_quantity: Decimal::ZERO,
                    venue_order_id: None,
                    last_status: None,
                    pending_terminal: None,
                    lifecycle_wait: None,
                },
            )]),
            seen_trades: Default::default(),
            terminal_client_ids: Default::default(),
            http_rejected_client_ids: Default::default(),
        })
    }

    #[tokio::test]
    async fn http_rejection_tombstones_unprogressed_order_but_preserves_progressed_order() {
        let pending_id = ClientOrderId(51);
        let state = tracked_state(pending_id);
        state
            .lock()
            .await
            .orders
            .get_mut(&pending_id)
            .unwrap()
            .lifecycle_wait = Some(LifecycleWait::new(LifecycleKind::PrivateAcceptance));
        let rejection = GatewayError::Rejected {
            code: "400".to_owned(),
            message: "rejected".to_owned(),
        };
        {
            let mut locked = state.lock().await;
            assert!(matches!(
                classify_submit_rejection(&mut locked, pending_id, rejection.clone()),
                GatewayError::Rejected { .. }
            ));
            assert!(!locked.orders.contains_key(&pending_id));
            assert!(locked.terminal_client_ids.contains(&pending_id));
            assert!(locked.http_rejected_client_ids.contains(&pending_id));
        }

        let progressed_id = ClientOrderId(57);
        let progressed_state = tracked_state(progressed_id);
        progressed_state
            .lock()
            .await
            .orders
            .get_mut(&progressed_id)
            .unwrap()
            .venue_order_id = Some(crate::types::VenueOrderId("99".to_owned()));
        let mut locked = progressed_state.lock().await;
        assert!(matches!(
            classify_submit_rejection(&mut locked, progressed_id, rejection),
            GatewayError::Protocol(_)
        ));
        assert!(locked.orders.contains_key(&progressed_id));
    }

    #[tokio::test]
    async fn missing_or_unknown_tracked_order_status_fails_closed() {
        let client_order_id = ClientOrderId(52);
        let state = tracked_state(client_order_id);
        let (events, _) = broadcast::channel(4);

        let missing = process_lighter_orders(
            &serde_json::json!({"orders": [{
                "order_index": 90,
                "client_order_index": 52,
                "market_index": 0
            }]}),
            0,
            &events,
            &state,
        )
        .await;
        assert!(matches!(missing, Err(super::GatewayError::Protocol(_))));

        let unknown = process_lighter_orders(
            &serde_json::json!({"orders": [{
                "order_index": 90,
                "client_order_index": 52,
                "market_index": 0,
                "status": "mystery",
                "updated_at": 1_771_943_742_851_i64
            }]}),
            0,
            &events,
            &state,
        )
        .await;
        assert!(matches!(unknown, Err(super::GatewayError::Protocol(_))));
        assert!(state.lock().await.orders.contains_key(&client_order_id));
    }

    #[tokio::test]
    async fn active_order_fill_ahead_of_trade_stream_fails_closed_without_mutation() {
        let client_order_id = ClientOrderId(53);
        let state = tracked_state(client_order_id);
        let (events, mut receiver) = broadcast::channel(4);
        process_lighter_orders(
            &serde_json::json!({"orders": [{
                "order_index": 91,
                "client_order_index": 53,
                "market_index": 0,
                "status": "open",
                "filled_base_amount": "0",
                "updated_at": 1_771_943_742_851_i64
            }]}),
            0,
            &events,
            &state,
        )
        .await
        .unwrap();
        let _ = receiver.recv().await.expect("initial acceptance");
        let result = process_lighter_orders(
            &serde_json::json!({"orders": [{
                "order_index": 91,
                "client_order_index": 53,
                "market_index": 0,
                "status": "open",
                "filled_base_amount": "0.1",
                "updated_at": 1_771_943_742_852_i64
            }]}),
            0,
            &events,
            &state,
        )
        .await;
        assert!(matches!(result, Err(super::GatewayError::Protocol(_))));
        let locked = state.lock().await;
        let tracked = locked.orders.get(&client_order_id).unwrap();
        assert_eq!(tracked.cumulative_quantity, Decimal::ZERO);
        assert_eq!(tracked.venue_order_id.as_ref().unwrap().0, "91");
        assert_eq!(tracked.last_status.as_deref(), Some("open"));
    }

    #[tokio::test]
    async fn invalid_and_over_requested_trades_fail_without_dedupe_or_quantity_mutation() {
        let client_order_id = ClientOrderId(54);
        let state = tracked_state(client_order_id);
        let (events, _) = broadcast::channel(4);
        let trade = |trade_id: i64, size: &str, price: &str| {
            serde_json::json!({"trades": [{
                "trade_id": trade_id,
                "market_id": 0,
                "size": size,
                "price": price,
                "ask_client_id": 54,
                "ask_account_id": 17,
                "bid_account_id": 18,
                "timestamp": 1_771_943_742_852_i64
            }]})
        };

        let invalid =
            process_lighter_trades(&trade(20, "0.1", "bad"), 17, 0, &events, &state).await;
        assert!(matches!(invalid, Err(super::GatewayError::Protocol(_))));
        let over =
            process_lighter_trades(&trade(21, "1.1", "100000"), 17, 0, &events, &state).await;
        assert!(matches!(over, Err(super::GatewayError::Protocol(_))));

        let locked = state.lock().await;
        assert_eq!(
            locked.orders[&client_order_id].cumulative_quantity,
            Decimal::ZERO
        );
        assert!(!locked.seen_trades.contains(&20));
        assert!(!locked.seen_trades.contains(&21));
    }

    #[tokio::test]
    async fn wrong_market_and_valid_untracked_entities_remain_ignored() {
        let state = tracked_state(ClientOrderId(55));
        let (events, _) = broadcast::channel(4);
        process_lighter_orders(
            &serde_json::json!({"orders": [{
                "client_order_index": 55,
                "market_index": 99,
                "status": "open"
            }, {
                "client_order_index": 999,
                "market_index": 0,
                "status": "open"
            }]}),
            0,
            &events,
            &state,
        )
        .await
        .unwrap();
        process_lighter_trades(
            &serde_json::json!({"trades": [{
                "trade_id": 30,
                "market_id": 99,
                "ask_client_id": 55,
                "ask_account_id": 17,
                "bid_account_id": 18
            }]}),
            17,
            0,
            &events,
            &state,
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn trade_after_cancel_terminal_fails_closed_via_tombstone() {
        let client_order_id = ClientOrderId(56);
        let state = tracked_state(client_order_id);
        let (events, _) = broadcast::channel(4);
        process_lighter_orders(
            &serde_json::json!({"orders": [{
                "order_index": 92,
                "client_order_index": 56,
                "market_index": 0,
                "status": "canceled",
                "filled_base_amount": "0",
                "updated_at": 1_771_943_742_851_i64
            }]}),
            0,
            &events,
            &state,
        )
        .await
        .unwrap();
        assert!(!state.lock().await.orders.contains_key(&client_order_id));

        let late_trade = process_lighter_trades(
            &serde_json::json!({"trades": [{
                "trade_id": 31,
                "market_id": 0,
                "size": "0.1",
                "price": "100000",
                "ask_client_id": 56,
                "ask_account_id": 17,
                "bid_account_id": 18,
                "timestamp": 1_771_943_742_852_i64
            }]}),
            17,
            0,
            &events,
            &state,
        )
        .await;
        assert!(matches!(late_trade, Err(super::GatewayError::Protocol(_))));
    }

    #[tokio::test]
    async fn malformed_account_owned_and_ambiguous_self_trades_fail_closed() {
        let state = tracked_state(ClientOrderId(58));
        let (events, _) = broadcast::channel(4);
        let missing_client_id = process_lighter_trades(
            &serde_json::json!({"trades": [{
                "trade_id": 32,
                "market_id": 0,
                "ask_account_id": 17,
                "bid_account_id": 18
            }]}),
            17,
            0,
            &events,
            &state,
        )
        .await;
        assert!(matches!(
            missing_client_id,
            Err(super::GatewayError::Protocol(_))
        ));

        let self_trade = process_lighter_trades(
            &serde_json::json!({"trades": [{
                "trade_id": 33,
                "market_id": 0,
                "ask_client_id": 58,
                "bid_client_id": 59,
                "ask_account_id": 17,
                "bid_account_id": 17
            }]}),
            17,
            0,
            &events,
            &state,
        )
        .await;
        assert!(matches!(self_trade, Err(super::GatewayError::Protocol(_))));

        process_lighter_trades(
            &serde_json::json!({"trades": [{
                "trade_id": 34,
                "market_id": 0,
                "ask_account_id": 18,
                "bid_account_id": 19
            }]}),
            17,
            0,
            &events,
            &state,
        )
        .await
        .unwrap();
    }
}
