use std::collections::{HashMap, VecDeque};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use ethers::signers::{LocalWallet, Signer};
use ethers::types::H160;
use futures_util::{SinkExt, StreamExt};
use hyperliquid_rust_sdk::{
    BaseUrl, ClientCancelRequestCloid, ClientLimit, ClientOrder, ClientOrderRequest,
    ExchangeClient, ExchangeDataStatus, ExchangeResponseStatus, Message as HyperliquidMessage,
    OrderUpdate as HyperliquidOrderUpdate, Subscription, TradeInfo as HyperliquidTradeInfo,
    MAINNET_API_URL,
};
use metrics::{counter, gauge, histogram};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use serde::Deserialize;
use tokio::sync::{broadcast, oneshot, Mutex};
use tokio::time::{Instant as TokioInstant, MissedTickBehavior};
use tokio_tungstenite::{connect_async, tungstenite::Message as WebSocketMessage};
use uuid::Uuid;

use super::{
    observe_private_fill, require_execution_ready, BoundedDedupe, GatewayError, LifecycleKind,
    LifecycleWait, OrderGateway, PRIVATE_LIFECYCLE_TIMEOUT,
};
use crate::types::{
    decimal_is_step_aligned, unix_timestamp_ms, CancelAck, CancelOrder, ClientOrderId,
    ExecutionEvent, ExecutionEventKind, Instrument, NewOrder, OrderAck, TimeInForce, Venue,
    VenueOrderId,
};

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HyperliquidGatewayConfig {
    /// Environment variable containing the approved agent wallet private key.
    pub private_key_env: String,
    /// Environment variable containing the actual trading account or vault address.
    pub account_address_env: String,
    /// Optional environment variable containing the same vault address used in signatures.
    /// A mismatch with `account_address_env` is rejected so private execution
    /// updates can never subscribe to a different account than orders target.
    #[serde(default)]
    pub vault_address_env: Option<String>,
    #[serde(default = "default_request_timeout_ms")]
    pub request_timeout_ms: u64,
}

const fn default_request_timeout_ms() -> u64 {
    5_000
}

const PRIVATE_STREAM_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(30);
const PRIVATE_STREAM_READ_TIMEOUT: Duration = Duration::from_secs(90);
const PRIVATE_STREAM_RECONNECT_DELAY: Duration = Duration::from_secs(1);
const ORPHAN_FILL_CAPACITY: usize = 1_024;

fn resolve_execution_user(
    configured_account: H160,
    vault_address: Option<H160>,
) -> Result<H160, GatewayError> {
    match vault_address {
        Some(vault) if vault != configured_account => Err(GatewayError::Authentication(format!(
            "Hyperliquid account address {configured_account:?} does not match configured vault {vault:?}"
        ))),
        Some(vault) => Ok(vault),
        None => Ok(configured_account),
    }
}

#[derive(Debug, Clone)]
struct TrackedOrder {
    venue_symbol: String,
    requested_quantity: Decimal,
    cumulative_quantity: Decimal,
    venue_order_id: Option<VenueOrderId>,
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

struct FillReconciliation {
    cumulative_quantity: Decimal,
    is_final: bool,
    terminal_after_fill: Option<PendingTerminal>,
}

#[derive(Debug, Clone)]
struct BufferedFill {
    oid: u64,
    trade_id: String,
    price: Decimal,
    quantity: Decimal,
    timestamp_ms: u64,
}

#[derive(Default)]
struct GatewayState {
    orders: HashMap<ClientOrderId, TrackedOrder>,
    oid_to_client: HashMap<u64, ClientOrderId>,
    seen_fills: BoundedDedupe<String>,
    terminal_client_ids: BoundedDedupe<ClientOrderId>,
    terminal_oids: BoundedDedupe<u64>,
    http_rejected_client_ids: BoundedDedupe<ClientOrderId>,
    orphan_fills: VecDeque<BufferedFill>,
}

fn mark_terminal_order(state: &mut GatewayState, client_order_id: ClientOrderId, oid: Option<u64>) {
    state.terminal_client_ids.insert(client_order_id);
    if let Some(oid) = oid {
        state.terminal_oids.insert(oid);
    }
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
            || order
                .lifecycle_wait
                .as_ref()
                .is_none_or(|wait| wait.kind != LifecycleKind::PrivateAcceptance);
        if has_private_progress {
            return GatewayError::Protocol(format!(
                "Hyperliquid HTTP submit rejection contradicted private execution progress for {client_order_id}: {rejection}"
            ));
        }
        state.orders.remove(&client_order_id);
        mark_terminal_order(state, client_order_id, None);
        state.http_rejected_client_ids.insert(client_order_id);
        rejection
    } else {
        GatewayError::Protocol(format!(
            "Hyperliquid HTTP submit rejection arrived after private terminal progress for {client_order_id}: {rejection}"
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

fn cancel_all_client_ids(
    state: &GatewayState,
    instrument: &Instrument,
) -> Result<Vec<ClientOrderId>, GatewayError> {
    if instrument.venue != Venue::Hyperliquid {
        return Err(GatewayError::InvalidOrder(
            "cancel-all instrument belongs to another venue".to_owned(),
        ));
    }
    Ok(state
        .orders
        .iter()
        .filter_map(|(client_order_id, order)| {
            (order.venue_symbol == instrument.venue_symbol).then_some(*client_order_id)
        })
        .collect())
}

/// Mainnet Hyperliquid adapter. All Hyperliquid SDK types and CLOID encoding
/// remain inside this module.
pub struct HyperliquidGateway {
    exchange: Arc<ExchangeClient>,
    user_address: H160,
    events: broadcast::Sender<ExecutionEvent>,
    state: Arc<Mutex<GatewayState>>,
    execution_ready: Arc<AtomicBool>,
}

impl HyperliquidGateway {
    pub async fn connect(config: HyperliquidGatewayConfig) -> Result<Self, GatewayError> {
        crate::telemetry::install_tls_crypto_provider()
            .map_err(|error| GatewayError::Transport(error.to_string()))?;
        let secret = read_required_env(&config.private_key_env, true)?;
        let wallet = secret.parse::<LocalWallet>().map_err(|_| {
            GatewayError::Authentication(format!(
                "{} does not contain a valid Hyperliquid agent private key",
                config.private_key_env
            ))
        })?;

        let account = read_required_env(&config.account_address_env, false)?;
        let configured_account = H160::from_str(&account).map_err(|_| {
            GatewayError::Authentication(format!(
                "{} does not contain a valid account address",
                config.account_address_env
            ))
        })?;

        let vault_address = match config.vault_address_env.as_deref() {
            Some(env_name) => {
                let value = read_required_env(env_name, false)?;
                Some(H160::from_str(&value).map_err(|_| {
                    GatewayError::Authentication(format!(
                        "{env_name} does not contain a valid vault address"
                    ))
                })?)
            }
            None => None,
        };
        let user_address = resolve_execution_user(configured_account, vault_address)?;

        let http = reqwest::Client::builder()
            .timeout(Duration::from_millis(config.request_timeout_ms))
            .build()
            .map_err(|err| GatewayError::Transport(err.to_string()))?;

        let connect_started = Instant::now();
        let exchange = ExchangeClient::new(
            Some(http),
            wallet.clone(),
            Some(BaseUrl::Mainnet),
            None,
            vault_address,
        )
        .await
        .map_err(|err| GatewayError::Transport(err.to_string()))?;
        histogram!("gateway_connect_seconds", "venue" => "hyperliquid")
            .record(connect_started.elapsed().as_secs_f64());

        // The account used for subscriptions must be the trading account, not
        // necessarily the approved agent wallet that signs requests.
        if vault_address.is_none() && wallet.address() != user_address {
            tracing::info!(
                signer = ?wallet.address(),
                account = ?user_address,
                "using an approved Hyperliquid agent wallet"
            );
        }

        let (events, _) = broadcast::channel(4_096);
        let state = Arc::new(Mutex::new(GatewayState::default()));
        let execution_ready = Arc::new(AtomicBool::new(false));
        start_private_stream(
            user_address,
            events.clone(),
            Arc::clone(&state),
            Arc::clone(&execution_ready),
            Duration::from_millis(config.request_timeout_ms),
        )
        .await?;

        Ok(Self {
            exchange: Arc::new(exchange),
            user_address,
            events,
            state,
            execution_ready,
        })
    }

    pub fn user_address(&self) -> H160 {
        self.user_address
    }

    fn validate_order(&self, order: &NewOrder) -> Result<(), GatewayError> {
        if order.instrument.venue != Venue::Hyperliquid {
            return Err(GatewayError::InvalidOrder(format!(
                "Hyperliquid gateway received {} instrument",
                order.instrument.venue
            )));
        }
        if order.price <= Decimal::ZERO || order.quantity <= Decimal::ZERO {
            return Err(GatewayError::InvalidOrder(
                "price and quantity must be positive".to_owned(),
            ));
        }
        if !decimal_is_step_aligned(order.price, order.instrument.price_tick) {
            return Err(GatewayError::InvalidOrder(format!(
                "price {} is not aligned to tick {}",
                order.price, order.instrument.price_tick
            )));
        }
        if !decimal_is_step_aligned(order.quantity, order.instrument.quantity_step) {
            return Err(GatewayError::InvalidOrder(format!(
                "quantity {} is not aligned to step {}",
                order.quantity, order.instrument.quantity_step
            )));
        }
        let asset = self
            .exchange
            .meta
            .universe
            .iter()
            .find(|asset| asset.name == order.instrument.venue_symbol)
            .ok_or_else(|| {
                GatewayError::InvalidOrder(format!(
                    "Hyperliquid perpetual {} does not exist in live metadata",
                    order.instrument.venue_symbol
                ))
            })?;
        if order.quantity.normalize().scale() > asset.sz_decimals {
            return Err(GatewayError::InvalidOrder(format!(
                "quantity {} exceeds Hyperliquid szDecimals {}",
                order.quantity, asset.sz_decimals
            )));
        }
        if !hyperliquid_price_is_valid(order.price, asset.sz_decimals) {
            return Err(GatewayError::InvalidOrder(format!(
                "price {} violates Hyperliquid's 5-significant-figure/decimal-place rule",
                order.price
            )));
        }
        Ok(())
    }
}

fn register_pending_terminal(
    order: &mut TrackedOrder,
    kind: PendingTerminalKind,
    expected_cumulative_quantity: Decimal,
    timestamp_ms: u64,
) -> Result<bool, GatewayError> {
    if expected_cumulative_quantity < Decimal::ZERO
        || expected_cumulative_quantity > order.requested_quantity
        || (kind == PendingTerminalKind::Filled && expected_cumulative_quantity == Decimal::ZERO)
    {
        return Err(GatewayError::Protocol(format!(
            "terminal cumulative quantity {expected_cumulative_quantity} is outside 0..={}",
            order.requested_quantity
        )));
    }
    if order.cumulative_quantity >= expected_cumulative_quantity {
        order.pending_terminal = None;
        order.lifecycle_wait = None;
        Ok(true)
    } else {
        order.pending_terminal = Some(PendingTerminal {
            kind,
            expected_cumulative_quantity,
            timestamp_ms,
        });
        order.lifecycle_wait = Some(LifecycleWait::new(LifecycleKind::FillReconciliation));
        Ok(false)
    }
}

fn reconcile_fill(
    order: &mut TrackedOrder,
    quantity: Decimal,
) -> Result<FillReconciliation, GatewayError> {
    if quantity <= Decimal::ZERO {
        return Err(GatewayError::Protocol(format!(
            "Hyperliquid fill quantity {quantity} is not positive"
        )));
    }
    let cumulative_quantity = order
        .cumulative_quantity
        .checked_add(quantity)
        .ok_or_else(|| GatewayError::Protocol("Hyperliquid cumulative fill overflow".to_owned()))?;
    if cumulative_quantity > order.requested_quantity {
        return Err(GatewayError::Protocol(format!(
            "Hyperliquid cumulative fill {cumulative_quantity} exceeds requested quantity {}",
            order.requested_quantity
        )));
    }
    order.cumulative_quantity = cumulative_quantity;
    let pending = order.pending_terminal.clone();
    let pending_reconciled = pending
        .as_ref()
        .is_some_and(|terminal| cumulative_quantity >= terminal.expected_cumulative_quantity);
    let is_final = cumulative_quantity >= order.requested_quantity
        || pending.as_ref().is_some_and(|terminal| {
            matches!(terminal.kind, PendingTerminalKind::Filled) && pending_reconciled
        });
    let terminal_after_fill = if !is_final && pending_reconciled {
        pending.filter(|terminal| {
            matches!(
                terminal.kind,
                PendingTerminalKind::Canceled | PendingTerminalKind::Rejected(_)
            )
        })
    } else {
        None
    };
    if is_final || terminal_after_fill.is_some() {
        order.pending_terminal = None;
        order.lifecycle_wait = None;
    } else if order.pending_terminal.is_some() || order.lifecycle_wait.is_some() {
        observe_private_fill(&mut order.lifecycle_wait);
    }
    Ok(FillReconciliation {
        cumulative_quantity,
        is_final,
        terminal_after_fill,
    })
}

fn apply_fill_locked(
    state: &mut GatewayState,
    client_order_id: ClientOrderId,
    fill: BufferedFill,
) -> Result<Vec<ExecutionEvent>, GatewayError> {
    if !state.orders.contains_key(&client_order_id) {
        return Ok(Vec::new());
    }
    if state.seen_fills.contains(&fill.trade_id) {
        return Ok(Vec::new());
    }
    let Some(order) = state.orders.get_mut(&client_order_id) else {
        return Ok(Vec::new());
    };
    let reconciliation = reconcile_fill(order, fill.quantity)?;
    state.seen_fills.insert(fill.trade_id.clone());
    if reconciliation.is_final || reconciliation.terminal_after_fill.is_some() {
        state.orders.remove(&client_order_id);
        state.oid_to_client.remove(&fill.oid);
        mark_terminal_order(state, client_order_id, Some(fill.oid));
    }

    let mut events = vec![ExecutionEvent {
        venue: Venue::Hyperliquid,
        timestamp_ms: fill.timestamp_ms,
        client_order_id: Some(client_order_id),
        kind: ExecutionEventKind::Fill {
            trade_id: fill.trade_id,
            price: fill.price,
            quantity: fill.quantity,
            cumulative_quantity: reconciliation.cumulative_quantity,
            is_final: reconciliation.is_final,
        },
    }];
    if let Some(terminal) = reconciliation.terminal_after_fill {
        let timestamp_ms = terminal.timestamp_ms.max(fill.timestamp_ms);
        let kind = match terminal.kind {
            PendingTerminalKind::Canceled => ExecutionEventKind::Canceled,
            PendingTerminalKind::Rejected(reason) => ExecutionEventKind::Rejected { reason },
            PendingTerminalKind::Filled => {
                unreachable!("filled terminal is carried by the final fill event")
            }
        };
        events.push(ExecutionEvent {
            venue: Venue::Hyperliquid,
            timestamp_ms,
            client_order_id: Some(client_order_id),
            kind,
        });
    }
    Ok(events)
}

fn buffer_orphan_fill(state: &mut GatewayState, fill: BufferedFill) -> Result<(), GatewayError> {
    if state.seen_fills.contains(&fill.trade_id)
        || state
            .orphan_fills
            .iter()
            .any(|pending| pending.trade_id == fill.trade_id)
    {
        return Ok(());
    }
    if state.orphan_fills.len() == ORPHAN_FILL_CAPACITY {
        counter!("gateway_orphan_fills_rejected_total", "venue" => "hyperliquid").increment(1);
        return Err(GatewayError::Protocol(format!(
            "Hyperliquid orphan fill buffer reached its {ORPHAN_FILL_CAPACITY}-entry safety limit"
        )));
    }
    state.orphan_fills.push_back(fill);
    gauge!("gateway_orphan_fills_buffered", "venue" => "hyperliquid")
        .set(state.orphan_fills.len() as f64);
    Ok(())
}

fn apply_orphan_fills_locked(
    state: &mut GatewayState,
    oid: u64,
    client_order_id: ClientOrderId,
) -> Result<Vec<ExecutionEvent>, GatewayError> {
    let matching: Vec<_> = state
        .orphan_fills
        .iter()
        .filter(|fill| fill.oid == oid)
        .cloned()
        .collect();
    if let Some(order) = state.orders.get(&client_order_id) {
        let mut projected_cumulative = order.cumulative_quantity;
        for fill in matching
            .iter()
            .filter(|fill| !state.seen_fills.contains(&fill.trade_id))
        {
            if fill.quantity <= Decimal::ZERO {
                return Err(GatewayError::Protocol(format!(
                    "Hyperliquid buffered fill quantity {} is not positive",
                    fill.quantity
                )));
            }
            projected_cumulative =
                projected_cumulative
                    .checked_add(fill.quantity)
                    .ok_or_else(|| {
                        GatewayError::Protocol(
                            "Hyperliquid buffered cumulative fill overflow".to_owned(),
                        )
                    })?;
            if projected_cumulative > order.requested_quantity {
                return Err(GatewayError::Protocol(format!(
                    "Hyperliquid buffered cumulative fill {projected_cumulative} exceeds requested quantity {}",
                    order.requested_quantity
                )));
            }
        }
    }
    let mut events = Vec::new();
    for fill in matching {
        events.extend(apply_fill_locked(state, client_order_id, fill)?);
    }
    let previous_len = state.orphan_fills.len();
    state.orphan_fills.retain(|fill| fill.oid != oid);
    if state.orphan_fills.len() != previous_len {
        gauge!("gateway_orphan_fills_buffered", "venue" => "hyperliquid")
            .set(state.orphan_fills.len() as f64);
    }
    Ok(events)
}

fn emit_private_events(
    sender: &broadcast::Sender<ExecutionEvent>,
    events: impl IntoIterator<Item = ExecutionEvent>,
) {
    for event in events {
        crate::telemetry::record_event_lag(
            "gateway_exchange_event_lag_seconds",
            event.timestamp_ms,
            "hyperliquid",
        );
        send_event(sender, event);
    }
}

fn terminal_cumulative_from_order(
    original_quantity: &str,
    remaining_quantity: &str,
    requested_quantity: Decimal,
) -> Result<Decimal, GatewayError> {
    let original = Decimal::from_str(original_quantity)
        .map_err(|error| GatewayError::Protocol(error.to_string()))?;
    let remaining = Decimal::from_str(remaining_quantity)
        .map_err(|error| GatewayError::Protocol(error.to_string()))?;
    if original <= Decimal::ZERO || original != requested_quantity {
        return Err(GatewayError::Protocol(format!(
            "Hyperliquid original quantity {original} does not match requested quantity {requested_quantity}"
        )));
    }
    if remaining < Decimal::ZERO || remaining > original {
        return Err(GatewayError::Protocol(format!(
            "Hyperliquid remaining quantity {remaining} exceeds original quantity {original}"
        )));
    }
    Ok(original - remaining)
}

impl OrderGateway for HyperliquidGateway {
    fn venue(&self) -> Venue {
        Venue::Hyperliquid
    }

    fn subscribe(&self) -> broadcast::Receiver<ExecutionEvent> {
        self.events.subscribe()
    }

    fn execution_ready(&self) -> bool {
        self.execution_ready.load(Ordering::Acquire)
    }

    async fn submit(&self, order: NewOrder) -> Result<OrderAck, GatewayError> {
        require_execution_ready(&self.execution_ready, Venue::Hyperliquid)?;
        self.validate_order(&order)?;
        let client_order_id = order.client_order_id;
        let cloid = client_id_to_uuid(client_order_id);
        let price = order.price.to_f64().ok_or_else(|| {
            GatewayError::InvalidOrder("price cannot be represented for SDK".to_owned())
        })?;
        let size = order.quantity.to_f64().ok_or_else(|| {
            GatewayError::InvalidOrder("quantity cannot be represented for SDK".to_owned())
        })?;
        let tif = match order.time_in_force {
            TimeInForce::PostOnly => "Alo",
            TimeInForce::GoodTilCanceled => "Gtc",
            TimeInForce::ImmediateOrCancel => "Ioc",
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
                    venue_symbol: order.instrument.venue_symbol.clone(),
                    requested_quantity: order.quantity,
                    cumulative_quantity: Decimal::ZERO,
                    venue_order_id: None,
                    pending_terminal: None,
                    lifecycle_wait: None,
                },
            );
        }

        let request = ClientOrderRequest {
            asset: order.instrument.venue_symbol.clone(),
            is_buy: matches!(order.side, crate::types::Side::Buy),
            reduce_only: order.reduce_only,
            limit_px: price,
            sz: size,
            cloid: Some(cloid),
            order_type: ClientOrder::Limit(ClientLimit {
                tif: tif.to_owned(),
            }),
        };

        if let Err(error) = require_execution_ready(&self.execution_ready, Venue::Hyperliquid) {
            self.state.lock().await.orders.remove(&client_order_id);
            return Err(error);
        }
        if let Some(tracked) = self.state.lock().await.orders.get_mut(&client_order_id) {
            tracked.lifecycle_wait = Some(LifecycleWait::new(LifecycleKind::PrivateAcceptance));
        } else {
            return Err(GatewayError::Protocol(format!(
                "Hyperliquid local tracking disappeared before submit for {client_order_id}"
            )));
        }
        if let Err(error) = require_execution_ready(&self.execution_ready, Venue::Hyperliquid) {
            let mut state = self.state.lock().await;
            let unprogressed = state.orders.get(&client_order_id).is_some_and(|tracked| {
                tracked.venue_order_id.is_none()
                    && tracked.cumulative_quantity == Decimal::ZERO
                    && tracked.pending_terminal.is_none()
                    && tracked
                        .lifecycle_wait
                        .as_ref()
                        .is_some_and(|wait| wait.kind == LifecycleKind::PrivateAcceptance)
            });
            if unprogressed {
                state.orders.remove(&client_order_id);
                return Err(error);
            }
            return Err(GatewayError::Protocol(format!(
                "Hyperliquid private execution progressed for {client_order_id} before submit readiness dropped"
            )));
        }

        let started = Instant::now();
        let response = self.exchange.order(request, None).await.map_err(|err| {
            counter!("gateway_submit_errors_total", "venue" => "hyperliquid", "kind" => "transport")
                .increment(1);
            GatewayError::Indeterminate(err.to_string())
        })?;
        histogram!("gateway_sign_and_submit_seconds", "venue" => "hyperliquid")
            .record(started.elapsed().as_secs_f64());

        let result = match parse_submit_response(response) {
            Ok(result) => result,
            Err(error @ GatewayError::Rejected { .. }) => {
                let classified = {
                    let mut state = self.state.lock().await;
                    classify_submit_rejection(&mut state, client_order_id, error)
                };
                if matches!(classified, GatewayError::Protocol(_)) {
                    mark_hyperliquid_stream_disconnected(
                        &self.execution_ready,
                        &self.events,
                        classified.to_string(),
                    );
                }
                return Err(classified);
            }
            Err(error) => {
                // A malformed/unknown response cannot prove whether the order
                // reached the matching engine. Preserve reconciliation state.
                return Err(GatewayError::Indeterminate(format!(
                    "could not classify Hyperliquid submit response: {error}"
                )));
            }
        };
        match result {
            SubmitResult::Resting(oid) => {
                let venue_order_id = VenueOrderId(oid.to_string());
                let mut state = self.state.lock().await;
                let should_emit = if let Some(tracked) = state.orders.get_mut(&client_order_id) {
                    tracked.venue_order_id = Some(venue_order_id.clone());
                    true
                } else {
                    false
                };
                let buffered_events = if should_emit {
                    state.oid_to_client.insert(oid, client_order_id);
                    apply_orphan_fills_locked(&mut state, oid, client_order_id).map_err(|error| {
                        mark_hyperliquid_stream_disconnected(
                            &self.execution_ready,
                            &self.events,
                            error.to_string(),
                        );
                        GatewayError::Indeterminate(format!(
                            "invalid buffered Hyperliquid fill after resting acknowledgement: {error}"
                        ))
                    })?
                } else {
                    Vec::new()
                };
                drop(state);
                if should_emit {
                    // The HTTP acknowledgement is returned to the OMS below.
                    // Only the private order stream emits Accepted so
                    // submit-to-private-accept telemetry remains meaningful.
                    emit_private_events(&self.events, buffered_events);
                }
                counter!("gateway_orders_submitted_total", "venue" => "hyperliquid").increment(1);
                Ok(OrderAck {
                    client_order_id,
                    venue_order_id: Some(venue_order_id),
                })
            }
            SubmitResult::Filled {
                oid,
                price,
                quantity,
            } => {
                if quantity <= Decimal::ZERO || quantity > order.quantity {
                    return Err(GatewayError::Indeterminate(format!(
                        "Hyperliquid filled response quantity {quantity} is outside 0..={}",
                        order.quantity
                    )));
                }
                let venue_order_id = VenueOrderId(oid.to_string());
                let timestamp_ms = unix_timestamp_ms();
                let (buffered_events, terminal_marker) = {
                    let mut state = self.state.lock().await;
                    let mut buffered_events = Vec::new();
                    let reconciled = if state.orders.contains_key(&client_order_id) {
                        state.oid_to_client.insert(oid, client_order_id);
                        if let Some(tracked) = state.orders.get_mut(&client_order_id) {
                            tracked.venue_order_id = Some(venue_order_id.clone());
                        }
                        buffered_events = apply_orphan_fills_locked(
                            &mut state,
                            oid,
                            client_order_id,
                        )
                        .map_err(|error| {
                            mark_hyperliquid_stream_disconnected(
                                &self.execution_ready,
                                &self.events,
                                error.to_string(),
                            );
                            GatewayError::Indeterminate(format!(
                                "invalid buffered Hyperliquid fill after filled acknowledgement: {error}"
                            ))
                        })?;
                        if let Some(tracked) = state.orders.get_mut(&client_order_id) {
                            match register_pending_terminal(
                                tracked,
                                PendingTerminalKind::Filled,
                                quantity,
                                timestamp_ms,
                            ) {
                                Ok(reconciled) => reconciled,
                                Err(error) => {
                                    return Err(GatewayError::Indeterminate(format!(
                                        "invalid Hyperliquid filled response: {error}"
                                    )));
                                }
                            }
                        } else {
                            false
                        }
                    } else {
                        false
                    };
                    let terminal_marker = if reconciled {
                        state
                            .orders
                            .get(&client_order_id)
                            .map(|tracked| tracked.cumulative_quantity)
                    } else {
                        None
                    };
                    if terminal_marker.is_some() {
                        state.orders.remove(&client_order_id);
                        state.oid_to_client.remove(&oid);
                        mark_terminal_order(&mut state, client_order_id, Some(oid));
                    }
                    (buffered_events, terminal_marker)
                };
                emit_private_events(&self.events, buffered_events);
                if let Some(cumulative_quantity) = terminal_marker {
                    // UserFills already supplied all economic quantity before
                    // the HTTP response arrived. Carry terminality with zero
                    // incremental size so downstream accounting cannot double.
                    send_event(
                        &self.events,
                        ExecutionEvent {
                            venue: Venue::Hyperliquid,
                            timestamp_ms,
                            client_order_id: Some(client_order_id),
                            kind: ExecutionEventKind::Fill {
                                trade_id: format!("http-terminal-{oid}"),
                                price,
                                quantity: Decimal::ZERO,
                                cumulative_quantity,
                                is_final: true,
                            },
                        },
                    );
                }
                counter!("gateway_orders_submitted_total", "venue" => "hyperliquid").increment(1);
                Ok(OrderAck {
                    client_order_id,
                    venue_order_id: Some(venue_order_id),
                })
            }
        }
    }

    async fn cancel(&self, cancel: CancelOrder) -> Result<CancelAck, GatewayError> {
        if cancel.instrument.venue != Venue::Hyperliquid {
            return Err(GatewayError::InvalidOrder(
                "cancel instrument belongs to another venue".to_owned(),
            ));
        }
        let request = ClientCancelRequestCloid {
            asset: cancel.instrument.venue_symbol,
            cloid: client_id_to_uuid(cancel.client_order_id),
        };
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
        let started = Instant::now();
        let response = self
            .exchange
            .cancel_by_cloid(request, None)
            .await
            .map_err(|err| GatewayError::Indeterminate(err.to_string()))?;
        histogram!("gateway_cancel_roundtrip_seconds", "venue" => "hyperliquid")
            .record(started.elapsed().as_secs_f64());
        match parse_cancel_response(response) {
            Ok(()) => {}
            Err(error @ GatewayError::Rejected { .. }) => {
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
                return Err(error);
            }
            Err(error) => {
                return Err(GatewayError::Indeterminate(format!(
                    "could not classify Hyperliquid cancel response: {error}"
                )));
            }
        }

        // HTTP success acknowledges the cancel request only. Keep the order
        // tracked until the private order stream supplies terminal state (and
        // any fill that raced the cancel).
        counter!("gateway_cancel_requests_total", "venue" => "hyperliquid").increment(1);
        Ok(CancelAck {
            client_order_id: cancel.client_order_id,
        })
    }

    async fn cancel_all(&self, instrument: Instrument) -> Result<(), GatewayError> {
        let ids = {
            let state = self.state.lock().await;
            cancel_all_client_ids(&state, &instrument)?
        };
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
                tracing::error!(%client_order_id, %error, "Hyperliquid cancel-all item failed");
                first_error.get_or_insert(error);
            }
        }
        first_error.map_or(Ok(()), Err)
    }
}

enum SubmitResult {
    Resting(u64),
    Filled {
        oid: u64,
        price: Decimal,
        quantity: Decimal,
    },
}

fn parse_submit_response(response: ExchangeResponseStatus) -> Result<SubmitResult, GatewayError> {
    let response = match response {
        ExchangeResponseStatus::Ok(response) => response,
        ExchangeResponseStatus::Err(message) => {
            return Err(GatewayError::Rejected {
                code: "exchange".to_owned(),
                message,
            });
        }
    };
    let mut statuses = response
        .data
        .ok_or_else(|| GatewayError::Protocol("missing order response data".to_owned()))?
        .statuses;
    if statuses.len() != 1 {
        return Err(GatewayError::Protocol(format!(
            "expected one order status, received {}",
            statuses.len()
        )));
    }
    match statuses.remove(0) {
        ExchangeDataStatus::Resting(order) => Ok(SubmitResult::Resting(order.oid)),
        ExchangeDataStatus::Filled(order) => Ok(SubmitResult::Filled {
            oid: order.oid,
            price: Decimal::from_str(&order.avg_px)
                .map_err(|err| GatewayError::Protocol(err.to_string()))?,
            quantity: Decimal::from_str(&order.total_sz)
                .map_err(|err| GatewayError::Protocol(err.to_string()))?,
        }),
        ExchangeDataStatus::Error(message) => Err(GatewayError::Rejected {
            code: "order".to_owned(),
            message,
        }),
        other => Err(GatewayError::Protocol(format!(
            "unexpected order status: {other:?}"
        ))),
    }
}

fn parse_cancel_response(response: ExchangeResponseStatus) -> Result<(), GatewayError> {
    let response = match response {
        ExchangeResponseStatus::Ok(response) => response,
        ExchangeResponseStatus::Err(message) => {
            return Err(GatewayError::Rejected {
                code: "exchange".to_owned(),
                message,
            });
        }
    };
    let statuses = response
        .data
        .ok_or_else(|| GatewayError::Protocol("missing cancel response data".to_owned()))?
        .statuses;
    match statuses.as_slice() {
        [ExchangeDataStatus::Success] => Ok(()),
        [ExchangeDataStatus::Error(message)] => Err(GatewayError::Rejected {
            code: "cancel".to_owned(),
            message: message.clone(),
        }),
        other => Err(GatewayError::Protocol(format!(
            "unexpected cancel response: {other:?}"
        ))),
    }
}

fn mark_hyperliquid_stream_ready(
    execution_ready: &AtomicBool,
    events: &broadcast::Sender<ExecutionEvent>,
) {
    if !execution_ready.swap(true, Ordering::AcqRel) {
        gauge!("gateway_private_stream_up", "venue" => "hyperliquid").set(1.0);
        send_event(
            events,
            ExecutionEvent {
                venue: Venue::Hyperliquid,
                timestamp_ms: unix_timestamp_ms(),
                client_order_id: None,
                kind: ExecutionEventKind::Reconnected,
            },
        );
    }
}

fn mark_hyperliquid_stream_disconnected(
    execution_ready: &AtomicBool,
    events: &broadcast::Sender<ExecutionEvent>,
    reason: String,
) {
    let was_ready = execution_ready.swap(false, Ordering::AcqRel);
    gauge!("gateway_private_stream_up", "venue" => "hyperliquid").set(0.0);
    if was_ready {
        send_event(
            events,
            ExecutionEvent {
                venue: Venue::Hyperliquid,
                timestamp_ms: unix_timestamp_ms(),
                client_order_id: None,
                kind: ExecutionEventKind::Disconnected { reason },
            },
        );
    }
}

async fn start_private_stream(
    user: H160,
    events: broadcast::Sender<ExecutionEvent>,
    state: Arc<Mutex<GatewayState>>,
    execution_ready: Arc<AtomicBool>,
    startup_timeout: Duration,
) -> Result<(), GatewayError> {
    let (startup_sender, startup_receiver) = oneshot::channel();
    let readiness = Arc::clone(&execution_ready);
    let monitor_readiness = Arc::clone(&execution_ready);
    let monitor_events = events.clone();
    let supervisor = tokio::spawn(async move {
        let mut startup_sender = Some(startup_sender);
        loop {
            let result = run_hyperliquid_private_stream_once(
                user,
                &events,
                &state,
                &execution_ready,
                &mut startup_sender,
            )
            .await;
            if let Some(sender) = startup_sender.take() {
                let error = result.as_ref().err().cloned().unwrap_or_else(|| {
                    GatewayError::Transport(
                        "Hyperliquid private stream closed before subscriptions were confirmed"
                            .to_owned(),
                    )
                });
                let _ = sender.send(Err(error));
            }
            let reason = result
                .err()
                .map(|error| error.to_string())
                .unwrap_or_else(|| "stream closed".to_owned());
            mark_hyperliquid_stream_disconnected(&execution_ready, &events, reason);
            counter!("gateway_private_stream_reconnects_total", "venue" => "hyperliquid")
                .increment(1);
            tokio::time::sleep(PRIVATE_STREAM_RECONNECT_DELAY).await;
        }
    });
    let abort_handle = supervisor.abort_handle();
    tokio::spawn(async move {
        let reason = match supervisor.await {
            Ok(()) => "Hyperliquid private-stream supervisor exited unexpectedly".to_owned(),
            Err(error) if error.is_cancelled() => {
                "Hyperliquid private-stream supervisor was aborted".to_owned()
            }
            Err(error) => format!("Hyperliquid private-stream supervisor panicked: {error}"),
        };
        mark_hyperliquid_stream_disconnected(&monitor_readiness, &monitor_events, reason);
    });

    match tokio::time::timeout(startup_timeout, startup_receiver).await {
        Ok(Ok(Ok(()))) => Ok(()),
        Ok(Ok(Err(error))) => {
            readiness.store(false, Ordering::Release);
            abort_handle.abort();
            Err(error)
        }
        Ok(Err(_)) => {
            readiness.store(false, Ordering::Release);
            abort_handle.abort();
            Err(GatewayError::Transport(
                "Hyperliquid private stream task stopped during startup".to_owned(),
            ))
        }
        Err(_) => {
            readiness.store(false, Ordering::Release);
            abort_handle.abort();
            Err(GatewayError::Transport(format!(
                "Hyperliquid private subscriptions were not confirmed within {} ms",
                startup_timeout.as_millis()
            )))
        }
    }
}

fn hyperliquid_subscription_type(value: &serde_json::Value, user: H160) -> Option<&str> {
    if value.get("channel")?.as_str()? != "subscriptionResponse"
        || value.pointer("/data/method")?.as_str()? != "subscribe"
    {
        return None;
    }
    let acknowledged_user =
        H160::from_str(value.pointer("/data/subscription/user")?.as_str()?).ok()?;
    if acknowledged_user != user {
        return None;
    }
    value.pointer("/data/subscription/type")?.as_str()
}

fn hyperliquid_private_protocol(channel: &'static str, message: impl Into<String>) -> GatewayError {
    counter!(
        "gateway_ws_invalid_messages_total",
        "venue" => "hyperliquid",
        "channel" => channel
    )
    .increment(1);
    GatewayError::Protocol(message.into())
}

fn check_hyperliquid_oid_mapping(
    state: &GatewayState,
    oid: u64,
    client_order_id: ClientOrderId,
    channel: &'static str,
) -> Result<(), GatewayError> {
    if let Some(mapped_client_order_id) = state.oid_to_client.get(&oid) {
        if *mapped_client_order_id != client_order_id {
            return Err(hyperliquid_private_protocol(
                channel,
                format!(
                    "Hyperliquid OID {oid} maps to both {mapped_client_order_id} and {client_order_id}"
                ),
            ));
        }
    }
    Ok(())
}

async fn process_hyperliquid_order_update(
    update: HyperliquidOrderUpdate,
    events: &broadcast::Sender<ExecutionEvent>,
    state: &Mutex<GatewayState>,
) -> Result<(), GatewayError> {
    let Some(raw_client_order_id) = update.order.cloid.as_deref() else {
        return Ok(());
    };
    let client_order_id = client_id_from_cloid(raw_client_order_id).ok_or_else(|| {
        hyperliquid_private_protocol(
            "orders",
            format!("invalid Hyperliquid CLOID {raw_client_order_id:?}"),
        )
    })?;
    let status = update.status.trim().to_ascii_lowercase();

    let mut locked = state.lock().await;
    if locked.http_rejected_client_ids.contains(&client_order_id) {
        if status.contains("reject") {
            return Ok(());
        }
        return Err(hyperliquid_private_protocol(
            "orders",
            format!(
                "Hyperliquid private status {:?} contradicted an HTTP rejection for {client_order_id}",
                update.status
            ),
        ));
    }
    if locked.terminal_client_ids.contains(&client_order_id) {
        return Err(hyperliquid_private_protocol(
            "orders",
            format!(
                "late Hyperliquid status {:?} arrived after order {client_order_id} was terminal",
                update.status
            ),
        ));
    }
    let Some(tracked) = locked.orders.get(&client_order_id) else {
        return Ok(());
    };
    if update.order.coin != tracked.venue_symbol {
        return Err(hyperliquid_private_protocol(
            "orders",
            format!(
                "Hyperliquid tracked order {client_order_id} changed market from {} to {}",
                tracked.venue_symbol, update.order.coin
            ),
        ));
    }
    if update.order.oid == 0 || update.status_timestamp == 0 {
        return Err(hyperliquid_private_protocol(
            "orders",
            format!(
                "Hyperliquid tracked order {client_order_id} has invalid OID/timestamp ({}/{})",
                update.order.oid, update.status_timestamp
            ),
        ));
    }
    check_hyperliquid_oid_mapping(&locked, update.order.oid, client_order_id, "orders")?;

    let timestamp_ms = update.status_timestamp;
    if status == "open" || status == "triggered" {
        let reported_cumulative_quantity = terminal_cumulative_from_order(
            &update.order.orig_sz,
            &update.order.sz,
            tracked.requested_quantity,
        )
        .map_err(|error| hyperliquid_private_protocol("orders", error.to_string()))?;
        if reported_cumulative_quantity > tracked.cumulative_quantity {
            return Err(hyperliquid_private_protocol(
                "orders",
                format!(
                    "Hyperliquid active update reports cumulative fill {reported_cumulative_quantity} ahead of authoritative private fills {} for tracked order {client_order_id}",
                    tracked.cumulative_quantity
                ),
            ));
        }
        let venue_order_id = VenueOrderId(update.order.oid.to_string());
        let Some(tracked) = locked.orders.get_mut(&client_order_id) else {
            return Ok(());
        };
        tracked.venue_order_id = Some(venue_order_id.clone());
        if tracked
            .lifecycle_wait
            .as_ref()
            .is_some_and(|wait| wait.kind == LifecycleKind::PrivateAcceptance)
        {
            tracked.lifecycle_wait = None;
        }
        locked
            .oid_to_client
            .insert(update.order.oid, client_order_id);
        let buffered_events =
            apply_orphan_fills_locked(&mut locked, update.order.oid, client_order_id)
                .map_err(|error| hyperliquid_private_protocol("fills", error.to_string()))?;
        drop(locked);
        send_event(
            events,
            ExecutionEvent {
                venue: Venue::Hyperliquid,
                timestamp_ms,
                client_order_id: Some(client_order_id),
                kind: ExecutionEventKind::Accepted { venue_order_id },
            },
        );
        emit_private_events(events, buffered_events);
        return Ok(());
    }

    let terminal_kind = if status == "filled" {
        PendingTerminalKind::Filled
    } else if status == "canceled" {
        PendingTerminalKind::Canceled
    } else if status.contains("reject") || status.contains("cancel") {
        // Constraint-driven cancellations are rejections from the strategy's
        // perspective and must not cause an unchanged revision to resubmit.
        PendingTerminalKind::Rejected(update.status.clone())
    } else {
        return Err(hyperliquid_private_protocol(
            "orders",
            format!(
                "unknown Hyperliquid status {:?} for tracked order {client_order_id}",
                update.status
            ),
        ));
    };

    let requested_quantity = tracked.requested_quantity;
    let expected_cumulative_quantity =
        terminal_cumulative_from_order(&update.order.orig_sz, &update.order.sz, requested_quantity)
            .map_err(|error| hyperliquid_private_protocol("orders", error.to_string()))?;
    let terminal_price = if terminal_kind == PendingTerminalKind::Filled {
        let price = Decimal::from_str(&update.order.limit_px).map_err(|error| {
            hyperliquid_private_protocol(
                "orders",
                format!("invalid Hyperliquid terminal price: {error}"),
            )
        })?;
        if price <= Decimal::ZERO {
            return Err(hyperliquid_private_protocol(
                "orders",
                format!("nonpositive Hyperliquid terminal price {price}"),
            ));
        }
        price
    } else {
        Decimal::ZERO
    };

    locked
        .oid_to_client
        .insert(update.order.oid, client_order_id);
    let buffered_events = apply_orphan_fills_locked(&mut locked, update.order.oid, client_order_id)
        .map_err(|error| hyperliquid_private_protocol("fills", error.to_string()))?;
    let terminal_event = if let Some(tracked) = locked.orders.get_mut(&client_order_id) {
        let reconciled = register_pending_terminal(
            tracked,
            terminal_kind.clone(),
            expected_cumulative_quantity,
            timestamp_ms,
        )
        .map_err(|error| hyperliquid_private_protocol("orders", error.to_string()))?;
        let cumulative_quantity = tracked.cumulative_quantity;
        if reconciled {
            locked.orders.remove(&client_order_id);
            locked.oid_to_client.remove(&update.order.oid);
            mark_terminal_order(&mut locked, client_order_id, Some(update.order.oid));
            Some(match terminal_kind {
                PendingTerminalKind::Filled => ExecutionEventKind::Fill {
                    trade_id: format!("order-terminal-{}-{timestamp_ms}", update.order.oid),
                    price: terminal_price,
                    quantity: Decimal::ZERO,
                    cumulative_quantity,
                    is_final: true,
                },
                PendingTerminalKind::Canceled => ExecutionEventKind::Canceled,
                PendingTerminalKind::Rejected(reason) => ExecutionEventKind::Rejected { reason },
            })
        } else {
            None
        }
    } else {
        None
    };
    drop(locked);
    emit_private_events(events, buffered_events);
    if let Some(kind) = terminal_event {
        send_event(
            events,
            ExecutionEvent {
                venue: Venue::Hyperliquid,
                timestamp_ms,
                client_order_id: Some(client_order_id),
                kind,
            },
        );
    }
    Ok(())
}

async fn process_hyperliquid_fill(
    fill: HyperliquidTradeInfo,
    events: &broadcast::Sender<ExecutionEvent>,
    state: &Mutex<GatewayState>,
) -> Result<(), GatewayError> {
    let fill_key = format!("{}:{}:{}", fill.hash, fill.oid, fill.tid);
    let direct_client_order_id = match fill.cloid.as_deref() {
        Some(raw_client_order_id) => {
            Some(client_id_from_cloid(raw_client_order_id).ok_or_else(|| {
                hyperliquid_private_protocol(
                    "fills",
                    format!("invalid Hyperliquid fill CLOID {raw_client_order_id:?}"),
                )
            })?)
        }
        None => None,
    };

    let mut locked = state.lock().await;
    if locked.seen_fills.contains(&fill_key) {
        return Ok(());
    }
    if locked.terminal_oids.contains(&fill.oid)
        || direct_client_order_id
            .is_some_and(|client_order_id| locked.terminal_client_ids.contains(&client_order_id))
    {
        return Err(hyperliquid_private_protocol(
            "fills",
            format!(
                "late Hyperliquid fill {} arrived after its order was terminal",
                fill_key
            ),
        ));
    }
    let mapped_client_order_id = locked.oid_to_client.get(&fill.oid).copied();
    if let (Some(direct), Some(mapped)) = (direct_client_order_id, mapped_client_order_id) {
        if direct != mapped {
            return Err(hyperliquid_private_protocol(
                "fills",
                format!(
                    "Hyperliquid fill OID {} identifies both {direct} and {mapped}",
                    fill.oid
                ),
            ));
        }
    }
    let client_order_id = direct_client_order_id.or(mapped_client_order_id);
    let possible_orphan = client_order_id.is_none()
        && fill.cloid.is_none()
        && locked
            .orders
            .values()
            .any(|order| order.venue_order_id.is_none() && order.venue_symbol == fill.coin);
    if let Some(client_order_id) = client_order_id {
        let Some(tracked) = locked.orders.get(&client_order_id) else {
            return Ok(());
        };
        if tracked.venue_symbol != fill.coin {
            return Err(hyperliquid_private_protocol(
                "fills",
                format!(
                    "Hyperliquid fill for tracked order {client_order_id} changed market from {} to {}",
                    tracked.venue_symbol, fill.coin
                ),
            ));
        }
    } else if !possible_orphan {
        counter!(
            "gateway_ws_ignored_messages_total",
            "venue" => "hyperliquid",
            "reason" => "untracked_fill"
        )
        .increment(1);
        return Ok(());
    }

    if fill.oid == 0 || fill.time == 0 || fill.hash.trim().is_empty() {
        return Err(hyperliquid_private_protocol(
            "fills",
            format!(
                "Hyperliquid tracked fill has invalid OID/timestamp/hash ({}/{}/{:?})",
                fill.oid, fill.time, fill.hash
            ),
        ));
    }
    let price = Decimal::from_str(&fill.px).map_err(|error| {
        hyperliquid_private_protocol("fills", format!("invalid Hyperliquid fill price: {error}"))
    })?;
    let quantity = Decimal::from_str(&fill.sz).map_err(|error| {
        hyperliquid_private_protocol(
            "fills",
            format!("invalid Hyperliquid fill quantity: {error}"),
        )
    })?;
    if price <= Decimal::ZERO || quantity <= Decimal::ZERO {
        return Err(hyperliquid_private_protocol(
            "fills",
            format!("nonpositive Hyperliquid fill price/quantity {price}/{quantity}"),
        ));
    }
    let buffered_fill = BufferedFill {
        oid: fill.oid,
        trade_id: fill_key,
        price,
        quantity,
        timestamp_ms: fill.time,
    };
    let Some(client_order_id) = client_order_id else {
        buffer_orphan_fill(&mut locked, buffered_fill)
            .map_err(|error| hyperliquid_private_protocol("fills", error.to_string()))?;
        return Ok(());
    };
    let fill_events = apply_fill_locked(&mut locked, client_order_id, buffered_fill)
        .map_err(|error| hyperliquid_private_protocol("fills", error.to_string()))?;
    drop(locked);
    emit_private_events(events, fill_events);
    Ok(())
}

async fn run_hyperliquid_private_stream_once(
    user: H160,
    events: &broadcast::Sender<ExecutionEvent>,
    state: &Mutex<GatewayState>,
    execution_ready: &AtomicBool,
    startup_sender: &mut Option<oneshot::Sender<Result<(), GatewayError>>>,
) -> Result<(), GatewayError> {
    let websocket_url = format!("ws{}/ws", &MAINNET_API_URL[4..]);
    let connect_started = Instant::now();
    let (stream, _) = connect_async(&websocket_url)
        .await
        .map_err(|error| GatewayError::Transport(error.to_string()))?;
    histogram!("gateway_ws_connect_seconds", "venue" => "hyperliquid")
        .record(connect_started.elapsed().as_secs_f64());
    let (mut writer, mut reader) = stream.split();
    for subscription in [
        Subscription::OrderUpdates { user },
        Subscription::UserFills { user },
    ] {
        writer
            .send(WebSocketMessage::Text(
                serde_json::json!({
                    "method": "subscribe",
                    "subscription": subscription,
                })
                .to_string()
                .into(),
            ))
            .await
            .map_err(|error| GatewayError::Transport(error.to_string()))?;
    }

    let mut keepalive = tokio::time::interval(PRIVATE_STREAM_KEEPALIVE_INTERVAL);
    keepalive.set_missed_tick_behavior(MissedTickBehavior::Delay);
    keepalive.tick().await;
    let mut lifecycle_check = tokio::time::interval(Duration::from_secs(1));
    lifecycle_check.set_missed_tick_behavior(MissedTickBehavior::Delay);
    lifecycle_check.tick().await;
    let read_deadline = tokio::time::sleep(PRIVATE_STREAM_READ_TIMEOUT);
    tokio::pin!(read_deadline);
    let mut orders_subscribed = false;
    let mut fills_subscribed = false;

    loop {
        tokio::select! {
            _ = lifecycle_check.tick() => {
                let locked = state.lock().await;
                let stalled = stalled_lifecycle(&locked, Instant::now());
                drop(locked);
                if let Some((client_order_id, kind)) = stalled {
                    counter!(
                        "gateway_private_lifecycle_timeouts_total",
                        "venue" => "hyperliquid",
                        "kind" => kind.label()
                    )
                    .increment(1);
                    return Err(GatewayError::Transport(format!(
                        "Hyperliquid order {client_order_id} received no {} within {} seconds",
                        kind.label(),
                        PRIVATE_LIFECYCLE_TIMEOUT.as_secs()
                    )));
                }
            }
            _ = &mut read_deadline => {
                    return Err(GatewayError::Transport(format!(
                        "Hyperliquid private stream received no data for {} seconds",
                        PRIVATE_STREAM_READ_TIMEOUT.as_secs()
                    )));
                }
                _ = keepalive.tick() => {
                    writer
                        .send(WebSocketMessage::Text(
                            serde_json::json!({"method": "ping"}).to_string().into(),
                        ))
                        .await
                        .map_err(|error| GatewayError::Transport(error.to_string()))?;
                }
                next = reader.next() => {
                    let Some(message) = next else {
                        return Err(GatewayError::Transport(
                            "Hyperliquid private stream closed".to_owned(),
                        ));
                    };
                    read_deadline
                        .as_mut()
                        .reset(TokioInstant::now() + PRIVATE_STREAM_READ_TIMEOUT);
                    let message = message
                        .map_err(|error| GatewayError::Transport(error.to_string()))?;
                    let text = match message {
                        WebSocketMessage::Text(text) => text,
                        WebSocketMessage::Ping(payload) => {
                            writer
                                .send(WebSocketMessage::Pong(payload))
                                .await
                                .map_err(|error| GatewayError::Transport(error.to_string()))?;
                            continue;
                        }
                        WebSocketMessage::Close(frame) => {
                            return Err(GatewayError::Transport(format!(
                                "Hyperliquid private stream closed: {frame:?}"
                            )));
                        }
                        _ => continue,
                    };
                    let parse_started = Instant::now();
                    let value: serde_json::Value = serde_json::from_str(&text)
                        .map_err(|error| GatewayError::Protocol(error.to_string()))?;
                    histogram!("gateway_ws_parse_seconds", "venue" => "hyperliquid")
                        .record(parse_started.elapsed().as_secs_f64());

                if let Some(subscription_type) = hyperliquid_subscription_type(&value, user) {
                        match subscription_type {
                            "orderUpdates" => orders_subscribed = true,
                            "userFills" => fills_subscribed = true,
                            _ => {}
                        }
                        if orders_subscribed && fills_subscribed && !execution_ready.load(Ordering::Acquire) {
                            mark_hyperliquid_stream_ready(execution_ready, events);
                            if let Some(sender) = startup_sender.take() {
                                let _ = sender.send(Ok(()));
                            }
                        }
                        continue;
                    }
                    if value.get("channel").and_then(serde_json::Value::as_str) == Some("pong") {
                        continue;
                    }
                    if value.get("channel").and_then(serde_json::Value::as_str) == Some("error") {
                        return Err(GatewayError::Protocol(value.to_string()));
                    }
                    let message: HyperliquidMessage = serde_json::from_value(value)
                        .map_err(|error| GatewayError::Protocol(error.to_string()))?;
                match message {
                    HyperliquidMessage::OrderUpdates(update) => {
                        for update in update.data {
                            process_hyperliquid_order_update(update, events, state).await?;
                        }
                    }
                    HyperliquidMessage::UserFills(fills) => {
                        if fills.data.user != user {
                            return Err(hyperliquid_private_protocol(
                                "fills",
                                format!(
                                    "Hyperliquid user-fill payload for {:?} arrived on {:?} subscription",
                                    fills.data.user, user
                                ),
                            ));
                        }
                        for fill in fills.data.fills {
                            process_hyperliquid_fill(fill, events, state).await?;
                        }
                    }
                    HyperliquidMessage::NoData | HyperliquidMessage::HyperliquidError(_) => {
                        let reason = match message {
                            HyperliquidMessage::HyperliquidError(reason) => reason,
                            HyperliquidMessage::NoData => {
                                "Hyperliquid private stream returned no data".to_owned()
                            }
                            _ => unreachable!(),
                        };
                        return Err(GatewayError::Transport(reason));
                    }
                    _ => {}
                }
            }
        }
    }
}

fn client_id_to_uuid(id: ClientOrderId) -> Uuid {
    Uuid::from_u128(id.0 as u128)
}

fn hyperliquid_price_is_valid(price: Decimal, size_decimals: u32) -> bool {
    if price <= Decimal::ZERO {
        return false;
    }
    // Integer prices are explicitly allowed regardless of significant digits.
    if price.fract().is_zero() {
        return true;
    }
    let normalized = price.normalize();
    let maximum_decimal_places = 6u32.saturating_sub(size_decimals);
    if normalized.scale() > maximum_decimal_places {
        return false;
    }
    let mantissa = normalized.mantissa().unsigned_abs();
    let significant_digits = mantissa.to_string().trim_end_matches('0').len();
    significant_digits <= 5
}

fn client_id_from_cloid(cloid: &str) -> Option<ClientOrderId> {
    let uuid = Uuid::parse_str(cloid.strip_prefix("0x").unwrap_or(cloid)).ok()?;
    u64::try_from(uuid.as_u128()).ok().map(ClientOrderId)
}

fn send_event(sender: &broadcast::Sender<ExecutionEvent>, event: ExecutionEvent) {
    counter!("gateway_events_total", "venue" => "hyperliquid").increment(1);
    let _ = sender.send(event);
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
    use std::str::FromStr;
    use std::sync::atomic::{AtomicBool, Ordering};

    use ethers::types::H160;
    use rust_decimal::Decimal;
    use tokio::sync::{broadcast, Mutex};

    use super::{
        apply_fill_locked, apply_orphan_fills_locked, buffer_orphan_fill, cancel_all_client_ids,
        classify_submit_rejection, client_id_from_cloid, client_id_to_uuid,
        hyperliquid_price_is_valid, hyperliquid_subscription_type,
        mark_hyperliquid_stream_disconnected, mark_hyperliquid_stream_ready,
        process_hyperliquid_fill, process_hyperliquid_order_update, reconcile_fill,
        register_pending_terminal, resolve_execution_user, BufferedFill, GatewayState,
        HyperliquidOrderUpdate, HyperliquidTradeInfo, PendingTerminalKind, TrackedOrder,
        ORPHAN_FILL_CAPACITY, PRIVATE_STREAM_KEEPALIVE_INTERVAL, PRIVATE_STREAM_READ_TIMEOUT,
    };
    use crate::gateway::{GatewayError, LifecycleKind, LifecycleWait};
    use crate::types::{ClientOrderId, ExecutionEventKind, Instrument, Venue};

    #[test]
    fn client_id_cloid_round_trip() {
        let id = ClientOrderId(9_876_543_210);
        let encoded = format!("0x{}", client_id_to_uuid(id).simple());
        assert_eq!(client_id_from_cloid(&encoded), Some(id));
    }

    #[test]
    fn validates_hyperliquid_price_precision() {
        assert!(hyperliquid_price_is_valid(
            Decimal::from_str("99999").unwrap(),
            5
        ));
        assert!(hyperliquid_price_is_valid(
            Decimal::from_str("100000").unwrap(),
            5
        ));
        assert!(hyperliquid_price_is_valid(
            Decimal::from_str("9999.9").unwrap(),
            5
        ));
        assert!(!hyperliquid_price_is_valid(
            Decimal::from_str("99999.1").unwrap(),
            5
        ));
        assert!(!hyperliquid_price_is_valid(
            Decimal::from_str("9999.91").unwrap(),
            5
        ));
    }

    #[test]
    fn vault_and_private_subscription_target_cannot_diverge() {
        let account = H160::from_low_u64_be(1);
        let vault = H160::from_low_u64_be(2);
        assert_eq!(resolve_execution_user(account, None).unwrap(), account);
        assert_eq!(resolve_execution_user(vault, Some(vault)).unwrap(), vault);
        assert!(resolve_execution_user(account, Some(vault)).is_err());
    }

    #[test]
    fn recognizes_each_private_subscription_ack() {
        let user = H160::from_low_u64_be(7);
        let user_string = format!("{user:?}");
        let order_ack = serde_json::json!({
            "channel": "subscriptionResponse",
            "data": {
                "method": "subscribe",
                "subscription": {"type": "orderUpdates", "user": user_string}
            }
        });
        let fills_ack = serde_json::json!({
            "channel": "subscriptionResponse",
            "data": {
                "method": "subscribe",
                "subscription": {"type": "userFills", "user": format!("{user:?}")}
            }
        });
        assert_eq!(
            hyperliquid_subscription_type(&order_ack, user),
            Some("orderUpdates")
        );
        assert_eq!(
            hyperliquid_subscription_type(&fills_ack, user),
            Some("userFills")
        );
        assert_eq!(
            hyperliquid_subscription_type(&serde_json::json!({"channel": "pong"}), user),
            None
        );
    }

    #[test]
    fn readiness_changes_before_connectivity_broadcast() {
        let ready = AtomicBool::new(false);
        let (events, mut receiver) = broadcast::channel(4);
        mark_hyperliquid_stream_ready(&ready, &events);
        let reconnected = receiver.try_recv().expect("reconnected event");
        assert!(ready.load(Ordering::Acquire));
        assert!(matches!(reconnected.kind, ExecutionEventKind::Reconnected));

        mark_hyperliquid_stream_disconnected(&ready, &events, "watchdog".to_owned());
        let disconnected = receiver.try_recv().expect("disconnected event");
        assert!(!ready.load(Ordering::Acquire));
        assert!(matches!(
            disconnected.kind,
            ExecutionEventKind::Disconnected { reason } if reason == "watchdog"
        ));
    }

    #[test]
    fn private_stream_timeout_allows_multiple_heartbeats() {
        assert!(PRIVATE_STREAM_READ_TIMEOUT >= PRIVATE_STREAM_KEEPALIVE_INTERVAL * 2);
    }

    fn tracked_order(quantity: &str) -> TrackedOrder {
        TrackedOrder {
            venue_symbol: "BTC".to_owned(),
            requested_quantity: Decimal::from_str(quantity).unwrap(),
            cumulative_quantity: Decimal::ZERO,
            venue_order_id: None,
            pending_terminal: None,
            lifecycle_wait: None,
        }
    }

    #[test]
    fn http_filled_before_user_fill_emits_economic_quantity_once() {
        let mut order = tracked_order("1");
        assert!(!register_pending_terminal(
            &mut order,
            PendingTerminalKind::Filled,
            Decimal::from_str("0.4").unwrap(),
            1_000,
        )
        .unwrap());

        let fill = reconcile_fill(&mut order, Decimal::from_str("0.4").unwrap()).unwrap();
        assert_eq!(fill.cumulative_quantity, Decimal::from_str("0.4").unwrap());
        assert!(fill.is_final);
        assert!(fill.terminal_after_fill.is_none());
    }

    #[test]
    fn user_fill_before_http_filled_needs_only_zero_quantity_terminal_marker() {
        let mut order = tracked_order("1");
        let fill = reconcile_fill(&mut order, Decimal::from_str("0.4").unwrap()).unwrap();
        assert_eq!(fill.cumulative_quantity, Decimal::from_str("0.4").unwrap());
        assert!(!fill.is_final);

        assert!(register_pending_terminal(
            &mut order,
            PendingTerminalKind::Filled,
            Decimal::from_str("0.4").unwrap(),
            1_000,
        )
        .unwrap());
        assert_eq!(order.cumulative_quantity, Decimal::from_str("0.4").unwrap());
    }

    #[test]
    fn cumulative_fill_validation_is_checked_and_non_mutating_on_error() {
        let mut order = tracked_order("1");
        order.cumulative_quantity = Decimal::from_str("0.8").unwrap();
        assert!(reconcile_fill(&mut order, Decimal::ZERO).is_err());
        assert!(reconcile_fill(&mut order, Decimal::from_str("0.3").unwrap()).is_err());
        assert_eq!(order.cumulative_quantity, Decimal::from_str("0.8").unwrap());

        let mut overflow = tracked_order(&Decimal::MAX.to_string());
        overflow.cumulative_quantity = Decimal::MAX;
        assert!(reconcile_fill(&mut overflow, Decimal::ONE).is_err());
        assert_eq!(overflow.cumulative_quantity, Decimal::MAX);
    }

    fn order_update(
        client_order_id: ClientOrderId,
        status: &str,
        remaining: &str,
    ) -> HyperliquidOrderUpdate {
        serde_json::from_value(serde_json::json!({
            "order": {
                "coin": "BTC",
                "side": "B",
                "limitPx": "100000",
                "sz": remaining,
                "oid": 12345,
                "timestamp": 1_000,
                "origSz": "1",
                "cloid": format!("0x{}", client_id_to_uuid(client_order_id).simple()),
            },
            "status": status,
            "statusTimestamp": 1_001,
        }))
        .unwrap()
    }

    fn fill_update(
        client_order_id: ClientOrderId,
        oid: u64,
        quantity: &str,
    ) -> HyperliquidTradeInfo {
        serde_json::from_value(serde_json::json!({
            "coin": "BTC",
            "side": "B",
            "px": "100000",
            "sz": quantity,
            "time": 1_002,
            "hash": "0xabc",
            "startPosition": "0",
            "dir": "Open Long",
            "closedPnl": "0",
            "oid": oid,
            "cloid": format!("0x{}", client_id_to_uuid(client_order_id).simple()),
            "crossed": false,
            "fee": "0.1",
            "feeToken": "USDC",
            "tid": 7,
        }))
        .unwrap()
    }

    #[tokio::test]
    async fn tracked_unknown_status_and_active_fill_gap_fail_closed() {
        let client_order_id = ClientOrderId(71);
        let state = Mutex::new(GatewayState::default());
        state
            .lock()
            .await
            .orders
            .insert(client_order_id, tracked_order("1"));
        let (events, _) = broadcast::channel(4);

        let unknown = process_hyperliquid_order_update(
            order_update(client_order_id, "mystery", "1"),
            &events,
            &state,
        )
        .await;
        assert!(matches!(unknown, Err(super::GatewayError::Protocol(_))));

        let missed_fill = process_hyperliquid_order_update(
            order_update(client_order_id, "open", "0.9"),
            &events,
            &state,
        )
        .await;
        assert!(matches!(missed_fill, Err(super::GatewayError::Protocol(_))));
        let locked = state.lock().await;
        let tracked = locked.orders.get(&client_order_id).unwrap();
        assert_eq!(tracked.cumulative_quantity, Decimal::ZERO);
        assert!(tracked.venue_order_id.is_none());
    }

    #[tokio::test]
    async fn fill_after_cancel_terminal_fails_closed_via_tombstone() {
        let client_order_id = ClientOrderId(72);
        let state = Mutex::new(GatewayState::default());
        state
            .lock()
            .await
            .orders
            .insert(client_order_id, tracked_order("1"));
        let (events, _) = broadcast::channel(4);
        process_hyperliquid_order_update(
            order_update(client_order_id, "canceled", "1"),
            &events,
            &state,
        )
        .await
        .unwrap();
        assert!(!state.lock().await.orders.contains_key(&client_order_id));

        let late_fill =
            process_hyperliquid_fill(fill_update(client_order_id, 12_345, "0.1"), &events, &state)
                .await;
        assert!(matches!(late_fill, Err(super::GatewayError::Protocol(_))));
    }

    #[test]
    fn http_rejection_tombstones_unprogressed_order_but_preserves_progressed_order() {
        let client_order_id = ClientOrderId(73);
        let mut state = GatewayState::default();
        let mut pending = tracked_order("1");
        pending.lifecycle_wait = Some(LifecycleWait::new(LifecycleKind::PrivateAcceptance));
        state.orders.insert(client_order_id, pending);
        let rejection = GatewayError::Rejected {
            code: "rejected".to_owned(),
            message: "no order".to_owned(),
        };
        assert!(matches!(
            classify_submit_rejection(&mut state, client_order_id, rejection.clone()),
            GatewayError::Rejected { .. }
        ));
        assert!(!state.orders.contains_key(&client_order_id));
        assert!(state.terminal_client_ids.contains(&client_order_id));
        assert!(state.http_rejected_client_ids.contains(&client_order_id));

        let progressed_id = ClientOrderId(74);
        let mut progressed = tracked_order("1");
        progressed.venue_order_id = Some(crate::types::VenueOrderId("42".to_owned()));
        state.orders.insert(progressed_id, progressed);
        assert!(matches!(
            classify_submit_rejection(&mut state, progressed_id, rejection),
            GatewayError::Protocol(_)
        ));
        assert!(state.orders.contains_key(&progressed_id));
    }

    #[test]
    fn cancel_all_selection_is_market_scoped_and_venue_checked() {
        let mut state = GatewayState::default();
        state.orders.insert(ClientOrderId(1), tracked_order("1"));
        let mut eth = tracked_order("1");
        eth.venue_symbol = "ETH".to_owned();
        state.orders.insert(ClientOrderId(2), eth);
        let instrument = Instrument {
            venue: Venue::Hyperliquid,
            canonical_symbol: "BTC-USD-PERP".to_owned(),
            venue_symbol: "BTC".to_owned(),
            price_tick: Decimal::ONE,
            quantity_step: Decimal::from_str("0.001").unwrap(),
        };
        assert_eq!(
            cancel_all_client_ids(&state, &instrument).unwrap(),
            vec![ClientOrderId(1)]
        );
        let mut wrong_venue = instrument;
        wrong_venue.venue = Venue::Lighter;
        assert!(cancel_all_client_ids(&state, &wrong_venue).is_err());
    }

    #[test]
    fn fill_without_cloid_is_reconciled_after_oid_mapping_arrives() {
        let client_order_id = ClientOrderId(99);
        let oid = 12_345;
        let mut state = GatewayState::default();
        state.orders.insert(client_order_id, tracked_order("1"));
        let fill = BufferedFill {
            oid,
            trade_id: "hash:12345:7".to_owned(),
            price: Decimal::from_str("100000").unwrap(),
            quantity: Decimal::from_str("0.4").unwrap(),
            timestamp_ms: 1_000,
        };
        buffer_orphan_fill(&mut state, fill.clone()).unwrap();
        assert_eq!(state.orphan_fills.len(), 1);

        state.oid_to_client.insert(oid, client_order_id);
        let events = apply_orphan_fills_locked(&mut state, oid, client_order_id).unwrap();
        assert!(state.orphan_fills.is_empty());
        assert!(matches!(
            events.as_slice(),
            [crate::types::ExecutionEvent {
                kind: ExecutionEventKind::Fill {
                    quantity,
                    cumulative_quantity,
                    is_final: false,
                    ..
                },
                ..
            }] if *quantity == Decimal::from_str("0.4").unwrap()
                && *cumulative_quantity == Decimal::from_str("0.4").unwrap()
        ));

        assert!(apply_orphan_fills_locked(&mut state, oid, client_order_id)
            .unwrap()
            .is_empty());
        assert!(apply_fill_locked(&mut state, client_order_id, fill)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn orphan_fill_capacity_fails_closed_without_eviction() {
        let mut state = GatewayState::default();
        for index in 0..ORPHAN_FILL_CAPACITY {
            buffer_orphan_fill(
                &mut state,
                BufferedFill {
                    oid: index as u64 + 1,
                    trade_id: format!("fill-{index}"),
                    price: Decimal::ONE,
                    quantity: Decimal::ONE,
                    timestamp_ms: 1,
                },
            )
            .unwrap();
        }
        let duplicate = state.orphan_fills.front().unwrap().clone();
        buffer_orphan_fill(&mut state, duplicate).unwrap();
        let overflow = buffer_orphan_fill(
            &mut state,
            BufferedFill {
                oid: 99_999,
                trade_id: "overflow".to_owned(),
                price: Decimal::ONE,
                quantity: Decimal::ONE,
                timestamp_ms: 1,
            },
        );
        assert!(matches!(overflow, Err(super::GatewayError::Protocol(_))));
        assert_eq!(state.orphan_fills.len(), ORPHAN_FILL_CAPACITY);
        assert_eq!(state.orphan_fills.front().unwrap().trade_id, "fill-0");
    }
}
