use std::collections::{HashMap, HashSet, VecDeque};
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::stream::{FuturesUnordered, StreamExt};
use rust_decimal::Decimal;
use thiserror::Error;
use tokio::sync::{broadcast, mpsc, oneshot};
use uuid::Uuid;

use crate::gateway::{GatewayError, OrderGateway};
use crate::types::{
    decimal_is_step_aligned, unix_timestamp_ms, CancelAck, CancelOrder, ClientOrderId,
    ExecutionEvent, ExecutionEventKind, Instrument, NewOrder, OrderAck, Side, TimeInForce, Venue,
    VenueOrderId,
};

/// A strategy owns one logical order slot for each instrument and side.
///
/// The venue belongs to `instrument`, so two otherwise-identical strategy slots
/// on different venues remain independent.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OrderKey {
    pub strategy_id: String,
    pub instrument: Instrument,
    pub side: Side,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderIntent {
    pub price: Decimal,
    pub quantity: Decimal,
    pub time_in_force: TimeInForce,
    pub reduce_only: bool,
}

/// A complete desired-state update for one order slot.
///
/// `target: None` means that no order should be working. Revisions are compared
/// per [`OrderKey`] and must increase strictly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DesiredOrder {
    pub key: OrderKey,
    pub revision: u64,
    pub target: Option<OrderIntent>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DesiredOrderOutcome {
    Applied,
    IgnoredStale { current_revision: u64 },
}

/// Venue-neutral state exposed to strategies and operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NormalizedOrderState {
    Idle,
    PendingNew,
    Open,
    PartiallyFilled,
    PendingCancel,
    /// A request may have reached the venue. No new operation is issued for
    /// this slot until an execution event resolves the outcome.
    Uncertain,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagedOrderSnapshot {
    pub key: OrderKey,
    pub revision: u64,
    pub desired: Option<OrderIntent>,
    pub state: NormalizedOrderState,
    pub client_order_id: Option<ClientOrderId>,
    pub venue_order_id: Option<VenueOrderId>,
    pub working_price: Option<Decimal>,
    pub working_quantity: Option<Decimal>,
    pub cumulative_filled_quantity: Decimal,
    /// If set, this desired revision has reached a terminal/error condition.
    /// A strictly newer strategy revision is required before another operation.
    pub blocked_until_revision: Option<u64>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderManagerSnapshot {
    pub venue: Venue,
    pub execution_connected: bool,
    /// Latched when `halt_on_any_fill` is enabled and any owned fill arrives.
    /// The latch is process-local and intentionally cannot be reset.
    pub halted_after_fill: bool,
    /// Latched when fail-closed execution-fault handling is enabled and the
    /// private stream or an owned order enters an unsafe/error state.
    pub halted_after_execution_fault: bool,
    pub orders: Vec<ManagedOrderSnapshot>,
    pub seen_trade_ids: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GatewayOperation {
    Submit,
    Cancel,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrderManagerEvent {
    StateChanged(ManagedOrderSnapshot),
    Fill {
        key: OrderKey,
        trade_id: String,
        price: Decimal,
        quantity: Decimal,
        cumulative_quantity: Decimal,
        is_final: bool,
    },
    GatewayFailure {
        key: OrderKey,
        operation: GatewayOperation,
        error: GatewayError,
    },
    Connectivity {
        connected: bool,
        reason: Option<String>,
    },
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum OrderManagerError {
    #[error("order manager is closed")]
    Closed,
    #[error("order manager is halted after an owned fill")]
    HaltedAfterFill,
    #[error("order manager is halted after an execution fault")]
    HaltedAfterExecutionFault,
    #[error("invalid desired order: {0}")]
    InvalidDesired(String),
    #[error("order-manager shutdown timed out with {active_orders} active order(s)")]
    ShutdownTimedOut { active_orders: usize },
}

#[derive(Debug, Clone)]
pub struct OrderManagerConfig {
    pub command_queue_capacity: usize,
    pub event_capacity: usize,
    pub client_order_id_seed: u64,
    pub shutdown_timeout: Duration,
    /// Atomically revoke every desired order and reject future desired updates
    /// after any owned fill. Inventory-aware strategies can leave this false.
    pub halt_on_any_fill: bool,
    /// Permanently revoke desired orders and reject future desired updates after
    /// a private-stream disconnect or an owned-order execution fault. Resumable
    /// supervisors can leave this false and reconcile before issuing a new
    /// revision; the supplied live runtime enables it.
    pub fail_closed_on_execution_fault: bool,
}

/// Lighter's client order index is an unsigned 48-bit integer. Keeping the
/// venue-neutral allocator within the strictest supported range lets every
/// adapter preserve the ID exactly.
pub const MAX_CLIENT_ORDER_ID: u64 = (1_u64 << 48) - 1;

/// Duplicate suppression is intentionally in-memory and bounded. Once an
/// order is terminal its IDs are removed because the client-order mapping will
/// reject any later replay before fill handling.
const MAX_SEEN_TRADE_IDS: usize = 65_536;

impl Default for OrderManagerConfig {
    fn default() -> Self {
        Self {
            command_queue_capacity: 1_024,
            event_capacity: 1_024,
            // A fresh process chooses a random point in the uint48 space. This
            // reduces accidental reuse across clean restarts; it is not a
            // substitute for venue reconciliation or single-writer ownership.
            client_order_id_seed: random_client_order_id_seed(),
            shutdown_timeout: Duration::from_secs(5),
            halt_on_any_fill: false,
            fail_closed_on_execution_fault: false,
        }
    }
}

fn random_client_order_id_seed() -> u64 {
    let seed = Uuid::new_v4().as_u128() % u128::from(MAX_CLIENT_ORDER_ID);
    u64::try_from(seed)
        .expect("uint48 client-order-id seed fits u64")
        .max(1)
}

/// Cloneable handle to the serialized order-manager actor.
pub struct OrderManager<G: OrderGateway> {
    commands: mpsc::Sender<Command>,
    events: broadcast::Sender<OrderManagerEvent>,
    _gateway: PhantomData<fn() -> G>,
}

impl<G: OrderGateway> Clone for OrderManager<G> {
    fn clone(&self) -> Self {
        Self {
            commands: self.commands.clone(),
            events: self.events.clone(),
            _gateway: PhantomData,
        }
    }
}

impl<G: OrderGateway> OrderManager<G> {
    pub fn new(gateway: Arc<G>) -> Self {
        Self::with_config(gateway, OrderManagerConfig::default())
    }

    pub fn with_config(gateway: Arc<G>, config: OrderManagerConfig) -> Self {
        let queue_capacity = config.command_queue_capacity.max(1);
        let event_capacity = config.event_capacity.max(1);
        let (command_tx, command_rx) = mpsc::channel(queue_capacity);
        let (event_tx, _) = broadcast::channel(event_capacity);
        // Subscribe before reading the level-triggered readiness flag. This
        // ordering closes both sides of the startup broadcast race: a state
        // change is either reflected in the flag or queued for this receiver.
        let execution_events = gateway.subscribe();
        let execution_connected = gateway.execution_ready();
        let gateway_venue = gateway.venue();

        let actor = Actor {
            venue: gateway_venue,
            gateway,
            commands: command_rx,
            execution_events,
            events: event_tx.clone(),
            slots: HashMap::new(),
            key_by_client_id: HashMap::new(),
            seen_trade_ids: BoundedTradeIds::new(MAX_SEEN_TRADE_IDS),
            next_client_order_id: config.client_order_id_seed % MAX_CLIENT_ORDER_ID,
            execution_connected,
            shutdown_timeout: config.shutdown_timeout,
            halt_on_any_fill: config.halt_on_any_fill,
            halted_after_fill: false,
            fail_closed_on_execution_fault: config.fail_closed_on_execution_fault,
            halted_after_execution_fault: false,
            shutdown: None,
        };
        // Keep an independent monitor alive so a panic inside the serialized
        // actor cannot leave the public event channel open and live orders
        // silently unmanaged. The live runtime treats this connectivity event
        // as fatal and falls back to the gateway's owned-order cancellation.
        let actor_task = tokio::spawn(actor.run());
        let actor_failure_events = event_tx.clone();
        let actor_venue = gateway_venue;
        tokio::spawn(async move {
            if let Err(error) = actor_task.await {
                metrics::counter!(
                    "oms_actor_failures_total",
                    "venue" => actor_venue.to_string()
                )
                .increment(1);
                let _ = actor_failure_events.send(OrderManagerEvent::Connectivity {
                    connected: false,
                    reason: Some(format!("order-manager actor failed: {error}")),
                });
            }
        });

        Self {
            commands: command_tx,
            events: event_tx,
            _gateway: PhantomData,
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<OrderManagerEvent> {
        self.events.subscribe()
    }

    pub async fn set_desired(
        &self,
        desired: DesiredOrder,
    ) -> Result<DesiredOrderOutcome, OrderManagerError> {
        let queued_at = Instant::now();
        let (reply_tx, reply_rx) = oneshot::channel();
        self.commands
            .send(Command::SetDesired {
                queued_at,
                desired,
                reply: reply_tx,
            })
            .await
            .map_err(|_| OrderManagerError::Closed)?;
        reply_rx.await.map_err(|_| OrderManagerError::Closed)?
    }

    pub async fn snapshot(&self) -> Result<OrderManagerSnapshot, OrderManagerError> {
        let queued_at = Instant::now();
        let (reply_tx, reply_rx) = oneshot::channel();
        self.commands
            .send(Command::Snapshot {
                queued_at,
                reply: reply_tx,
            })
            .await
            .map_err(|_| OrderManagerError::Closed)?;
        reply_rx.await.map_err(|_| OrderManagerError::Closed)
    }

    pub async fn snapshot_order(
        &self,
        key: OrderKey,
    ) -> Result<Option<ManagedOrderSnapshot>, OrderManagerError> {
        let queued_at = Instant::now();
        let (reply_tx, reply_rx) = oneshot::channel();
        self.commands
            .send(Command::SnapshotOrder {
                queued_at,
                key,
                reply: reply_tx,
            })
            .await
            .map_err(|_| OrderManagerError::Closed)?;
        reply_rx.await.map_err(|_| OrderManagerError::Closed)
    }

    pub async fn shutdown(&self) -> Result<(), OrderManagerError> {
        let queued_at = Instant::now();
        let (reply_tx, reply_rx) = oneshot::channel();
        self.commands
            .send(Command::Shutdown {
                queued_at,
                reply: reply_tx,
            })
            .await
            .map_err(|_| OrderManagerError::Closed)?;
        reply_rx.await.map_err(|_| OrderManagerError::Closed)?
    }
}

enum Command {
    SetDesired {
        queued_at: Instant,
        desired: DesiredOrder,
        reply: oneshot::Sender<Result<DesiredOrderOutcome, OrderManagerError>>,
    },
    Snapshot {
        queued_at: Instant,
        reply: oneshot::Sender<OrderManagerSnapshot>,
    },
    SnapshotOrder {
        queued_at: Instant,
        key: OrderKey,
        reply: oneshot::Sender<Option<ManagedOrderSnapshot>>,
    },
    Shutdown {
        queued_at: Instant,
        reply: oneshot::Sender<Result<(), OrderManagerError>>,
    },
}

struct ShutdownState {
    deadline: tokio::time::Instant,
    timed_out: bool,
    replies: Vec<oneshot::Sender<Result<(), OrderManagerError>>>,
}

#[derive(Debug, Clone)]
struct WorkingOrder {
    order: NewOrder,
    venue_order_id: Option<VenueOrderId>,
    cumulative_filled_quantity: Decimal,
    /// True only after the public request future returned a successful ingress
    /// acknowledgement. A PendingNew order may then be canceled safely even
    /// before its private Accepted event arrives.
    submit_acknowledged: bool,
    submitted_at: Instant,
    private_accept_observed: bool,
    first_fill_observed: bool,
    cancel_requested_at: Option<Instant>,
}

#[derive(Debug)]
struct BoundedTradeIds {
    values: HashSet<(ClientOrderId, String)>,
    insertion_order: VecDeque<(ClientOrderId, String)>,
    capacity: usize,
}

impl BoundedTradeIds {
    fn new(capacity: usize) -> Self {
        Self {
            values: HashSet::with_capacity(capacity.min(1_024)),
            insertion_order: VecDeque::with_capacity(capacity.min(1_024)),
            capacity: capacity.max(1),
        }
    }

    fn insert(&mut self, value: (ClientOrderId, String)) -> bool {
        if !self.values.insert(value.clone()) {
            return false;
        }
        self.insertion_order.push_back(value);
        while self.insertion_order.len() > self.capacity {
            if let Some(oldest) = self.insertion_order.pop_front() {
                self.values.remove(&oldest);
            }
        }
        true
    }

    fn remove(&mut self, value: &(ClientOrderId, String)) {
        if self.values.remove(value) {
            self.insertion_order.retain(|entry| entry != value);
        }
    }

    fn remove_order(&mut self, client_order_id: ClientOrderId) {
        self.values
            .retain(|(candidate, _)| *candidate != client_order_id);
        self.insertion_order
            .retain(|(candidate, _)| *candidate != client_order_id);
    }

    fn len(&self) -> usize {
        self.values.len()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UncertainOperation {
    Submit,
    Cancel { cancel_revision: u64 },
}

#[derive(Debug, Clone)]
enum ActualOrder {
    Vacant,
    PendingNew(WorkingOrder),
    Working(WorkingOrder),
    PendingCancel {
        working: WorkingOrder,
        cancel_revision: u64,
    },
    Uncertain {
        working: WorkingOrder,
        operation: UncertainOperation,
    },
}

impl ActualOrder {
    fn working(&self) -> Option<&WorkingOrder> {
        match self {
            Self::Vacant => None,
            Self::PendingNew(working) | Self::Working(working) => Some(working),
            Self::PendingCancel { working, .. } | Self::Uncertain { working, .. } => Some(working),
        }
    }

    fn working_mut(&mut self) -> Option<&mut WorkingOrder> {
        match self {
            Self::Vacant => None,
            Self::PendingNew(working) | Self::Working(working) => Some(working),
            Self::PendingCancel { working, .. } | Self::Uncertain { working, .. } => Some(working),
        }
    }
}

#[derive(Debug, Clone)]
struct Slot {
    revision: u64,
    desired: Option<OrderIntent>,
    actual: ActualOrder,
    blocked_until_revision: Option<u64>,
    last_error: Option<String>,
}

type OperationFuture = Pin<Box<dyn Future<Output = OperationCompletion> + Send + 'static>>;

enum OperationCompletion {
    Submit {
        key: OrderKey,
        client_order_id: ClientOrderId,
        elapsed: Duration,
        result: Result<OrderAck, GatewayError>,
    },
    Cancel {
        key: OrderKey,
        client_order_id: ClientOrderId,
        elapsed: Duration,
        result: Result<CancelAck, GatewayError>,
    },
}

struct Actor<G: OrderGateway> {
    venue: Venue,
    gateway: Arc<G>,
    commands: mpsc::Receiver<Command>,
    execution_events: broadcast::Receiver<ExecutionEvent>,
    events: broadcast::Sender<OrderManagerEvent>,
    slots: HashMap<OrderKey, Slot>,
    key_by_client_id: HashMap<ClientOrderId, OrderKey>,
    seen_trade_ids: BoundedTradeIds,
    next_client_order_id: u64,
    execution_connected: bool,
    shutdown_timeout: Duration,
    halt_on_any_fill: bool,
    halted_after_fill: bool,
    fail_closed_on_execution_fault: bool,
    halted_after_execution_fault: bool,
    shutdown: Option<ShutdownState>,
}

impl<G: OrderGateway> Actor<G> {
    async fn run(mut self) {
        let mut operations = FuturesUnordered::<OperationFuture>::new();
        let mut execution_events_open = true;

        loop {
            let shutdown_deadline = self
                .shutdown
                .as_ref()
                .map(|shutdown| shutdown.deadline)
                .unwrap_or_else(|| tokio::time::Instant::now() + Duration::from_secs(86_400));
            let shutdown_timer_active = self
                .shutdown
                .as_ref()
                .is_some_and(|shutdown| !shutdown.timed_out);
            tokio::select! {
                command = self.commands.recv() => {
                    let Some(command) = command else {
                        if self.shutdown.is_some() {
                            self.finish_shutdown(Err(OrderManagerError::Closed));
                        }
                        break;
                    };
                    self.handle_command(command, &mut operations);
                }
                event = self.execution_events.recv(), if execution_events_open => {
                    match event {
                        Ok(event) => self.handle_execution_event(event, &mut operations),
                        Err(broadcast::error::RecvError::Lagged(skipped)) => {
                            metrics::counter!(
                                "oms_execution_event_lag_total",
                                "venue" => self.venue.to_string()
                            ).increment(skipped);
                            self.mark_execution_disconnected(
                                format!("execution event receiver lagged by {skipped} messages"),
                                &mut operations,
                            );
                        }
                        Err(broadcast::error::RecvError::Closed) => {
                            execution_events_open = false;
                            self.mark_execution_disconnected(
                                "execution event stream closed".to_owned(),
                                &mut operations,
                            );
                        }
                    }
                }
                Some(completion) = operations.next(), if !operations.is_empty() => {
                    self.handle_operation_completion(completion, &mut operations);
                }
                _ = tokio::time::sleep_until(shutdown_deadline), if shutdown_timer_active => {
                    if let Some(shutdown) = self.shutdown.as_mut() {
                        shutdown.timed_out = true;
                    }
                }
            }

            if self.shutdown.is_some() && self.active_order_count() == 0 {
                self.finish_shutdown(Ok(()));
                break;
            }
            if self
                .shutdown
                .as_ref()
                .is_some_and(|shutdown| shutdown.timed_out)
                && operations.is_empty()
            {
                let error = OrderManagerError::ShutdownTimedOut {
                    active_orders: self.active_order_count(),
                };
                self.finish_shutdown(Err(error));
                break;
            }
        }
    }

    fn handle_command(
        &mut self,
        command: Command,
        operations: &mut FuturesUnordered<OperationFuture>,
    ) {
        let queued_at = match &command {
            Command::SetDesired { queued_at, .. }
            | Command::Snapshot { queued_at, .. }
            | Command::SnapshotOrder { queued_at, .. }
            | Command::Shutdown { queued_at, .. } => *queued_at,
        };
        metrics::histogram!(
            "oms_queue_latency_seconds",
            "venue" => self.venue.to_string()
        )
        .record(queued_at.elapsed().as_secs_f64());

        match command {
            Command::SetDesired { desired, reply, .. } => {
                let result = if self.shutdown.is_some() {
                    Err(OrderManagerError::Closed)
                } else {
                    self.apply_desired(desired, operations)
                };
                let _ = reply.send(result);
            }
            Command::Snapshot { reply, .. } => {
                let _ = reply.send(self.snapshot());
            }
            Command::SnapshotOrder { key, reply, .. } => {
                let _ = reply.send(self.snapshot_for_key(&key));
            }
            Command::Shutdown { reply, .. } => {
                self.begin_shutdown(reply, operations);
            }
        }
    }

    fn begin_shutdown(
        &mut self,
        reply: oneshot::Sender<Result<(), OrderManagerError>>,
        operations: &mut FuturesUnordered<OperationFuture>,
    ) {
        if let Some(shutdown) = self.shutdown.as_mut() {
            shutdown.replies.push(reply);
            return;
        }

        self.shutdown = Some(ShutdownState {
            deadline: tokio::time::Instant::now() + self.shutdown_timeout,
            timed_out: false,
            replies: vec![reply],
        });

        let keys: Vec<_> = self.slots.keys().cloned().collect();
        for key in keys {
            if let Some(slot) = self.slots.get_mut(&key) {
                slot.desired = None;
                // Shutdown is an explicit safety action and must not be held
                // back by a failed strategy revision.
                slot.blocked_until_revision = None;
                slot.last_error = None;
            }
            self.reconcile_slot(&key, operations);
            self.publish_state(&key);
        }
    }

    fn active_order_count(&self) -> usize {
        self.slots
            .values()
            .filter(|slot| !matches!(slot.actual, ActualOrder::Vacant))
            .count()
    }

    fn finish_shutdown(&mut self, result: Result<(), OrderManagerError>) {
        if let Some(shutdown) = self.shutdown.take() {
            for reply in shutdown.replies {
                let _ = reply.send(result.clone());
            }
        }
    }

    fn apply_desired(
        &mut self,
        update: DesiredOrder,
        operations: &mut FuturesUnordered<OperationFuture>,
    ) -> Result<DesiredOrderOutcome, OrderManagerError> {
        if self.halted_after_fill {
            return Err(OrderManagerError::HaltedAfterFill);
        }
        if self.halted_after_execution_fault {
            return Err(OrderManagerError::HaltedAfterExecutionFault);
        }
        if let Some(slot) = self.slots.get(&update.key) {
            if update.revision <= slot.revision {
                return Ok(DesiredOrderOutcome::IgnoredStale {
                    current_revision: slot.revision,
                });
            }
        }
        self.validate_desired(&update)?;
        let key = update.key;

        match self.slots.get_mut(&key) {
            Some(slot) => {
                slot.revision = update.revision;
                slot.desired = update.target;
                slot.last_error = None;
                if slot
                    .blocked_until_revision
                    .is_some_and(|blocked| update.revision > blocked)
                {
                    slot.blocked_until_revision = None;
                }
            }
            None => {
                self.slots.insert(
                    key.clone(),
                    Slot {
                        revision: update.revision,
                        desired: update.target,
                        actual: ActualOrder::Vacant,
                        blocked_until_revision: None,
                        last_error: None,
                    },
                );
            }
        }

        self.reconcile_slot(&key, operations);
        self.publish_state(&key);
        Ok(DesiredOrderOutcome::Applied)
    }

    fn validate_desired(&self, update: &DesiredOrder) -> Result<(), OrderManagerError> {
        if update.key.strategy_id.trim().is_empty() {
            return Err(OrderManagerError::InvalidDesired(
                "strategy_id must not be empty".to_owned(),
            ));
        }
        if update.key.instrument.venue != self.venue {
            return Err(OrderManagerError::InvalidDesired(format!(
                "instrument venue {} does not match gateway venue {}",
                update.key.instrument.venue, self.venue
            )));
        }
        if let Some(target) = &update.target {
            if target.price <= Decimal::ZERO {
                return Err(OrderManagerError::InvalidDesired(
                    "price must be positive".to_owned(),
                ));
            }
            if target.quantity <= Decimal::ZERO {
                return Err(OrderManagerError::InvalidDesired(
                    "quantity must be positive".to_owned(),
                ));
            }
            if !decimal_is_step_aligned(target.price, update.key.instrument.price_tick) {
                return Err(OrderManagerError::InvalidDesired(format!(
                    "price {} is not aligned to tick {}",
                    target.price, update.key.instrument.price_tick
                )));
            }
            if !decimal_is_step_aligned(target.quantity, update.key.instrument.quantity_step) {
                return Err(OrderManagerError::InvalidDesired(format!(
                    "quantity {} is not aligned to step {}",
                    target.quantity, update.key.instrument.quantity_step
                )));
            }
        }
        Ok(())
    }

    fn reconcile_slot(
        &mut self,
        key: &OrderKey,
        operations: &mut FuturesUnordered<OperationFuture>,
    ) {
        enum Action {
            Submit {
                intent: OrderIntent,
            },
            Cancel {
                working: WorkingOrder,
                cancel_revision: u64,
            },
        }

        let action = {
            let Some(slot) = self.slots.get(key) else {
                return;
            };
            if slot
                .blocked_until_revision
                .is_some_and(|blocked| slot.revision <= blocked)
            {
                return;
            }

            match (&slot.actual, &slot.desired) {
                (ActualOrder::Vacant, Some(intent))
                    if self.execution_connected && self.gateway.execution_ready() =>
                {
                    Some(Action::Submit {
                        intent: intent.clone(),
                    })
                }
                (ActualOrder::Working(working), desired)
                    if !desired_matches_working(desired.as_ref(), working) =>
                {
                    Some(Action::Cancel {
                        working: working.clone(),
                        cancel_revision: slot.revision,
                    })
                }
                (ActualOrder::PendingNew(working), desired)
                    if working.submit_acknowledged
                        && !desired_matches_working(desired.as_ref(), working) =>
                {
                    Some(Action::Cancel {
                        working: working.clone(),
                        cancel_revision: slot.revision,
                    })
                }
                (
                    ActualOrder::Uncertain {
                        working,
                        operation: UncertainOperation::Submit,
                    },
                    desired,
                ) if !desired_matches_working(desired.as_ref(), working) => Some(Action::Cancel {
                    working: working.clone(),
                    cancel_revision: slot.revision,
                }),
                _ => None,
            }
        };

        match action {
            Some(Action::Submit { intent }) => {
                let client_order_id = self.allocate_client_order_id();
                let order = NewOrder {
                    client_order_id,
                    instrument: key.instrument.clone(),
                    side: key.side,
                    price: intent.price,
                    quantity: intent.quantity,
                    time_in_force: intent.time_in_force,
                    reduce_only: intent.reduce_only,
                };
                let working = WorkingOrder {
                    order: order.clone(),
                    venue_order_id: None,
                    cumulative_filled_quantity: Decimal::ZERO,
                    submit_acknowledged: false,
                    submitted_at: Instant::now(),
                    private_accept_observed: false,
                    first_fill_observed: false,
                    cancel_requested_at: None,
                };
                self.slots
                    .get_mut(key)
                    .expect("slot exists while reconciling")
                    .actual = ActualOrder::PendingNew(working);
                self.key_by_client_id.insert(client_order_id, key.clone());

                let gateway = Arc::clone(&self.gateway);
                let operation_key = key.clone();
                operations.push(Box::pin(async move {
                    let started = Instant::now();
                    let result = gateway.submit(order).await;
                    OperationCompletion::Submit {
                        key: operation_key,
                        client_order_id,
                        elapsed: started.elapsed(),
                        result,
                    }
                }));
            }
            Some(Action::Cancel {
                mut working,
                cancel_revision,
            }) => {
                let client_order_id = working.order.client_order_id;
                let cancel = CancelOrder {
                    instrument: working.order.instrument.clone(),
                    client_order_id,
                    venue_order_id: working.venue_order_id.clone(),
                };
                working.cancel_requested_at = Some(Instant::now());
                self.slots
                    .get_mut(key)
                    .expect("slot exists while reconciling")
                    .actual = ActualOrder::PendingCancel {
                    working,
                    cancel_revision,
                };

                let gateway = Arc::clone(&self.gateway);
                let operation_key = key.clone();
                operations.push(Box::pin(async move {
                    let started = Instant::now();
                    let result = gateway.cancel(cancel).await;
                    OperationCompletion::Cancel {
                        key: operation_key,
                        client_order_id,
                        elapsed: started.elapsed(),
                        result,
                    }
                }));
            }
            None => {}
        }
    }

    fn handle_operation_completion(
        &mut self,
        completion: OperationCompletion,
        operations: &mut FuturesUnordered<OperationFuture>,
    ) {
        match completion {
            OperationCompletion::Submit {
                key,
                client_order_id,
                elapsed,
                result,
            } => {
                metrics::histogram!(
                    "oms_submit_latency_seconds",
                    "venue" => self.venue.to_string()
                )
                .record(elapsed.as_secs_f64());
                self.complete_submit(key, client_order_id, result, operations);
            }
            OperationCompletion::Cancel {
                key,
                client_order_id,
                elapsed,
                result,
            } => {
                metrics::histogram!(
                    "oms_cancel_latency_seconds",
                    "venue" => self.venue.to_string()
                )
                .record(elapsed.as_secs_f64());
                self.complete_cancel(key, client_order_id, result, operations);
            }
        }
    }

    fn complete_submit(
        &mut self,
        key: OrderKey,
        client_order_id: ClientOrderId,
        result: Result<OrderAck, GatewayError>,
        operations: &mut FuturesUnordered<OperationFuture>,
    ) {
        let result = result.and_then(|ack| {
            if ack.client_order_id == client_order_id {
                Ok(ack)
            } else {
                Err(GatewayError::Protocol(format!(
                    "submit acknowledgement client ID {} did not match {}",
                    ack.client_order_id, client_order_id
                )))
            }
        });

        let mut remove_client_id = false;
        let mut failure = None;
        if let Some(slot) = self.slots.get_mut(&key) {
            match result {
                Ok(ack) => {
                    if slot
                        .actual
                        .working()
                        .is_some_and(|working| working.order.client_order_id == client_order_id)
                    {
                        let submit_is_terminally_accepted = ack.venue_order_id.is_some();
                        if let Some(working) = slot.actual.working_mut() {
                            working.submit_acknowledged = true;
                            if ack.venue_order_id.is_some() {
                                working.venue_order_id = ack.venue_order_id;
                            }
                        }
                        // Some venues (notably Lighter) return a successful
                        // ingress receipt without an exchange order ID. Keep
                        // PendingNew until the private stream establishes that
                        // the sequencer accepted the order.
                        if submit_is_terminally_accepted
                            && matches!(slot.actual, ActualOrder::PendingNew(_))
                        {
                            let ActualOrder::PendingNew(working) =
                                std::mem::replace(&mut slot.actual, ActualOrder::Vacant)
                            else {
                                unreachable!()
                            };
                            slot.actual = ActualOrder::Working(working);
                        }
                        slot.last_error = None;
                    }
                }
                Err(error) => {
                    let actual = std::mem::replace(&mut slot.actual, ActualOrder::Vacant);
                    match actual {
                        ActualOrder::PendingNew(working)
                            if working.order.client_order_id == client_order_id =>
                        {
                            failure = Some(error.clone());
                            if matches!(error, GatewayError::Indeterminate(_)) {
                                slot.actual = ActualOrder::Uncertain {
                                    working,
                                    operation: UncertainOperation::Submit,
                                };
                            } else {
                                // A newer desired revision may have arrived while
                                // this submit was in flight. Block through the
                                // current revision so reconciliation cannot launch
                                // another live order before the strategy observes
                                // this fatal failure.
                                slot.blocked_until_revision = Some(slot.revision);
                                remove_client_id = true;
                            }
                            slot.last_error = Some(error.to_string());
                        }
                        other => {
                            // A private-stream event may already have established a
                            // stronger state than the request future's result.
                            slot.actual = other;
                        }
                    }
                }
            }
        }

        if remove_client_id {
            self.key_by_client_id.remove(&client_order_id);
        }
        if let Some(error) = failure {
            self.halt_after_execution_fault("submit_failure", operations);
            let _ = self.events.send(OrderManagerEvent::GatewayFailure {
                key: key.clone(),
                operation: GatewayOperation::Submit,
                error,
            });
        }
        self.reconcile_slot(&key, operations);
        self.publish_state(&key);
    }

    fn complete_cancel(
        &mut self,
        key: OrderKey,
        client_order_id: ClientOrderId,
        result: Result<CancelAck, GatewayError>,
        operations: &mut FuturesUnordered<OperationFuture>,
    ) {
        let result = result.and_then(|ack| {
            if ack.client_order_id == client_order_id {
                Ok(ack)
            } else {
                Err(GatewayError::Protocol(format!(
                    "cancel acknowledgement client ID {} did not match {}",
                    ack.client_order_id, client_order_id
                )))
            }
        });

        let mut failure = None;
        if let Some(slot) = self.slots.get_mut(&key) {
            match result {
                Ok(_) => {
                    if matches!(
                        &slot.actual,
                        ActualOrder::PendingCancel { working, .. }
                            if working.order.client_order_id == client_order_id
                    ) {
                        // A gateway acknowledgement confirms only that the
                        // venue accepted the cancel request. The private order
                        // stream remains authoritative for terminal state.
                        slot.last_error = None;
                    }
                }
                Err(error) => {
                    let actual = std::mem::replace(&mut slot.actual, ActualOrder::Vacant);
                    match actual {
                        ActualOrder::PendingCancel {
                            working,
                            cancel_revision,
                        } if working.order.client_order_id == client_order_id => {
                            failure = Some(error.clone());
                            if matches!(error, GatewayError::Indeterminate(_)) {
                                slot.actual = ActualOrder::Uncertain {
                                    working,
                                    operation: UncertainOperation::Cancel { cancel_revision },
                                };
                            } else {
                                slot.actual = ActualOrder::Working(working);
                                slot.blocked_until_revision = Some(slot.revision);
                            }
                            slot.last_error = Some(error.to_string());
                        }
                        other => slot.actual = other,
                    }
                }
            }
        }

        if let Some(error) = failure {
            self.halt_after_execution_fault("cancel_failure", operations);
            let _ = self.events.send(OrderManagerEvent::GatewayFailure {
                key: key.clone(),
                operation: GatewayOperation::Cancel,
                error,
            });
        }
        self.reconcile_slot(&key, operations);
        self.publish_state(&key);
    }

    fn handle_execution_event(
        &mut self,
        event: ExecutionEvent,
        operations: &mut FuturesUnordered<OperationFuture>,
    ) {
        if event.venue != self.venue {
            metrics::counter!(
                "oms_ignored_execution_events_total",
                "venue" => self.venue.to_string(),
                "reason" => "wrong_venue"
            )
            .increment(1);
            return;
        }

        let event_kind = execution_event_kind_label(&event.kind);
        metrics::counter!(
            "oms_execution_events_total",
            "venue" => self.venue.to_string(),
            "kind" => event_kind
        )
        .increment(1);
        let now_ms = unix_timestamp_ms();
        if event.timestamp_ms <= now_ms {
            metrics::histogram!(
                "oms_execution_event_latency_seconds",
                "venue" => self.venue.to_string(),
                "kind" => event_kind
            )
            .record((now_ms - event.timestamp_ms) as f64 / 1_000.0);
        }

        match event.kind {
            ExecutionEventKind::Disconnected { reason } => {
                self.mark_execution_disconnected(reason, operations);
            }
            ExecutionEventKind::Reconnected => {
                if self.halted_after_execution_fault {
                    metrics::counter!(
                        "oms_ignored_execution_events_total",
                        "venue" => self.venue.to_string(),
                        "reason" => "fail_closed_latch"
                    )
                    .increment(1);
                    return;
                }
                self.execution_connected = true;
                let _ = self.events.send(OrderManagerEvent::Connectivity {
                    connected: true,
                    reason: None,
                });
                let keys: Vec<_> = self.slots.keys().cloned().collect();
                for key in keys {
                    self.reconcile_slot(&key, operations);
                    self.publish_state(&key);
                }
            }
            kind => {
                let Some(client_order_id) = event.client_order_id else {
                    metrics::counter!(
                        "oms_ignored_execution_events_total",
                        "venue" => self.venue.to_string(),
                        "reason" => "missing_client_order_id"
                    )
                    .increment(1);
                    return;
                };
                let Some(key) = self.key_by_client_id.get(&client_order_id).cloned() else {
                    metrics::counter!(
                        "oms_ignored_execution_events_total",
                        "venue" => self.venue.to_string(),
                        "reason" => "unknown_client_order_id"
                    )
                    .increment(1);
                    return;
                };

                match kind {
                    ExecutionEventKind::Accepted { venue_order_id } => {
                        self.apply_accepted(&key, client_order_id, venue_order_id);
                    }
                    ExecutionEventKind::Rejected { reason } => {
                        if self.apply_rejected(&key, client_order_id, reason) {
                            self.halt_after_execution_fault("private_rejection", operations);
                        }
                    }
                    ExecutionEventKind::Canceled => {
                        if self.apply_canceled(&key, client_order_id) {
                            self.halt_after_execution_fault("unexpected_cancel", operations);
                        }
                    }
                    ExecutionEventKind::CancelRejected { reason } => {
                        if self.apply_cancel_rejected(&key, client_order_id, reason) {
                            self.halt_after_execution_fault("cancel_rejection", operations);
                        }
                    }
                    ExecutionEventKind::Fill {
                        trade_id,
                        price,
                        quantity,
                        cumulative_quantity,
                        is_final,
                    } => {
                        let fill_event = self.apply_fill(
                            &key,
                            client_order_id,
                            trade_id,
                            price,
                            quantity,
                            cumulative_quantity,
                            is_final,
                        );
                        if let Some(fill_event) = fill_event {
                            if self.halt_on_any_fill {
                                self.halt_after_fill(key.side, operations);
                            }
                            let _ = self.events.send(fill_event);
                        }
                    }
                    ExecutionEventKind::Disconnected { .. } | ExecutionEventKind::Reconnected => {
                        unreachable!()
                    }
                }

                self.reconcile_slot(&key, operations);
                self.publish_state(&key);
            }
        }
    }

    fn halt_after_fill(&mut self, side: Side, operations: &mut FuturesUnordered<OperationFuture>) {
        if self.halted_after_fill {
            return;
        }
        self.halted_after_fill = true;
        metrics::counter!(
            "oms_fill_halts_total",
            "venue" => self.venue.to_string(),
            "side" => format!("{side:?}").to_lowercase()
        )
        .increment(1);
        self.withdraw_all_desired(operations);
    }

    fn halt_after_execution_fault(
        &mut self,
        reason: &'static str,
        operations: &mut FuturesUnordered<OperationFuture>,
    ) {
        if !self.fail_closed_on_execution_fault || self.halted_after_execution_fault {
            return;
        }
        self.halted_after_execution_fault = true;
        // This flag represents whether the OMS may safely execute, rather than
        // merely whether the underlying socket happens to be connected.
        self.execution_connected = false;
        metrics::counter!(
            "oms_execution_fault_halts_total",
            "venue" => self.venue.to_string(),
            "reason" => reason
        )
        .increment(1);
        self.withdraw_all_desired(operations);
    }

    fn withdraw_all_desired(&mut self, operations: &mut FuturesUnordered<OperationFuture>) {
        let keys: Vec<_> = self.slots.keys().cloned().collect();
        for key in keys {
            if let Some(slot) = self.slots.get_mut(&key) {
                slot.desired = None;
                // A global safety latch must not let an older per-revision
                // block prevent cancellation of another live order.
                slot.blocked_until_revision = None;
            }
            self.reconcile_slot(&key, operations);
            self.publish_state(&key);
        }
    }

    fn mark_execution_disconnected(
        &mut self,
        reason: String,
        operations: &mut FuturesUnordered<OperationFuture>,
    ) {
        self.execution_connected = false;
        if self.fail_closed_on_execution_fault {
            self.halt_after_execution_fault("private_disconnect", operations);
        } else {
            // Desired quotes created under an execution-event gap are no
            // longer valid. Resumable callers may provide a fresh revision
            // while disconnected and reconcile it after reconnecting.
            self.withdraw_all_desired(operations);
        }
        let _ = self.events.send(OrderManagerEvent::Connectivity {
            connected: false,
            reason: Some(reason),
        });
    }

    fn apply_accepted(
        &mut self,
        key: &OrderKey,
        client_order_id: ClientOrderId,
        venue_order_id: VenueOrderId,
    ) {
        let Some(slot) = self.slots.get_mut(key) else {
            return;
        };
        let mut actual = std::mem::replace(&mut slot.actual, ActualOrder::Vacant);
        if let Some(working) = actual
            .working_mut()
            .filter(|working| working.order.client_order_id == client_order_id)
        {
            record_submit_private_accept(self.venue, working);
        }
        slot.actual = match actual {
            ActualOrder::PendingNew(mut working)
                if working.order.client_order_id == client_order_id =>
            {
                working.venue_order_id = Some(venue_order_id);
                slot.last_error = None;
                ActualOrder::Working(working)
            }
            ActualOrder::Working(mut working)
                if working.order.client_order_id == client_order_id =>
            {
                working.venue_order_id = Some(venue_order_id);
                slot.last_error = None;
                ActualOrder::Working(working)
            }
            ActualOrder::PendingCancel {
                mut working,
                cancel_revision,
            } if working.order.client_order_id == client_order_id => {
                working.venue_order_id = Some(venue_order_id);
                ActualOrder::PendingCancel {
                    working,
                    cancel_revision,
                }
            }
            ActualOrder::Uncertain {
                mut working,
                operation: UncertainOperation::Submit,
            } if working.order.client_order_id == client_order_id => {
                working.venue_order_id = Some(venue_order_id);
                slot.last_error = None;
                ActualOrder::Working(working)
            }
            other => other,
        };
    }

    fn apply_rejected(
        &mut self,
        key: &OrderKey,
        client_order_id: ClientOrderId,
        reason: String,
    ) -> bool {
        let Some(slot) = self.slots.get_mut(key) else {
            return false;
        };
        let mut actual = std::mem::replace(&mut slot.actual, ActualOrder::Vacant);
        if actual
            .working()
            .is_some_and(|working| working.order.client_order_id == client_order_id)
        {
            let working = actual.working_mut().expect("matched working order");
            record_submit_private_reject(self.venue, working);
            record_cancel_terminal(self.venue, working, "rejected");
            slot.blocked_until_revision = Some(slot.revision);
            slot.last_error = Some(reason);
            self.key_by_client_id.remove(&client_order_id);
            self.seen_trade_ids.remove_order(client_order_id);
            slot.actual = ActualOrder::Vacant;
            true
        } else {
            slot.actual = actual;
            false
        }
    }

    /// Returns true when the venue canceled the order without an active OMS
    /// cancel request and the event therefore represents an execution fault.
    fn apply_canceled(&mut self, key: &OrderKey, client_order_id: ClientOrderId) -> bool {
        let Some(slot) = self.slots.get_mut(key) else {
            return false;
        };
        if slot
            .actual
            .working()
            .is_some_and(|working| working.order.client_order_id == client_order_id)
        {
            let working = slot.actual.working_mut().expect("matched working order");
            record_cancel_terminal(self.venue, working, "canceled");
            let cancellation_was_expected = matches!(
                &slot.actual,
                ActualOrder::PendingCancel { .. }
                    | ActualOrder::Uncertain {
                        operation: UncertainOperation::Cancel { .. },
                        ..
                    }
            );
            slot.actual = ActualOrder::Vacant;
            if cancellation_was_expected {
                slot.last_error = None;
            } else {
                slot.blocked_until_revision = Some(slot.revision);
                slot.last_error = Some(
                    "order was canceled by the venue without an active OMS cancel request"
                        .to_owned(),
                );
                metrics::counter!(
                    "oms_unexpected_cancellations_total",
                    "venue" => self.venue.to_string()
                )
                .increment(1);
            }
            self.key_by_client_id.remove(&client_order_id);
            self.seen_trade_ids.remove_order(client_order_id);
            !cancellation_was_expected
        } else {
            false
        }
    }

    fn apply_cancel_rejected(
        &mut self,
        key: &OrderKey,
        client_order_id: ClientOrderId,
        reason: String,
    ) -> bool {
        let Some(slot) = self.slots.get_mut(key) else {
            return false;
        };
        let mut actual = std::mem::replace(&mut slot.actual, ActualOrder::Vacant);
        let matching_order = actual
            .working()
            .is_some_and(|working| working.order.client_order_id == client_order_id);
        if !matching_order {
            slot.actual = actual;
            return false;
        }
        if let Some(working) = actual.working_mut() {
            record_cancel_terminal(self.venue, working, "cancel_rejected");
        }
        slot.actual = match actual {
            ActualOrder::PendingCancel {
                working,
                cancel_revision: _,
            } if working.order.client_order_id == client_order_id => {
                slot.blocked_until_revision = Some(slot.revision);
                slot.last_error = Some(reason);
                ActualOrder::Working(working)
            }
            ActualOrder::Uncertain {
                working,
                operation: UncertainOperation::Cancel { cancel_revision: _ },
            } if working.order.client_order_id == client_order_id => {
                slot.blocked_until_revision = Some(slot.revision);
                slot.last_error = Some(reason);
                ActualOrder::Working(working)
            }
            other => {
                slot.blocked_until_revision = Some(slot.revision);
                slot.last_error = Some(format!(
                    "cancel was rejected without an active OMS cancel request: {reason}"
                ));
                other
            }
        };
        true
    }

    #[allow(clippy::too_many_arguments)]
    fn apply_fill(
        &mut self,
        key: &OrderKey,
        client_order_id: ClientOrderId,
        trade_id: String,
        price: Decimal,
        quantity: Decimal,
        cumulative_quantity: Decimal,
        is_final: bool,
    ) -> Option<OrderManagerEvent> {
        if !self
            .seen_trade_ids
            .insert((client_order_id, trade_id.clone()))
        {
            metrics::counter!(
                "oms_duplicate_fills_total",
                "venue" => self.venue.to_string()
            )
            .increment(1);
            return None;
        }

        let Some(slot) = self.slots.get_mut(key) else {
            self.seen_trade_ids
                .remove(&(client_order_id, trade_id.clone()));
            return None;
        };
        let mut actual = std::mem::replace(&mut slot.actual, ActualOrder::Vacant);
        if let Some(working) = actual
            .working_mut()
            .filter(|working| working.order.client_order_id == client_order_id)
        {
            record_submit_first_fill(self.venue, working);
            if is_final
                || working.cumulative_filled_quantity.max(cumulative_quantity)
                    >= working.order.quantity
            {
                record_cancel_terminal(self.venue, working, "filled");
            }
        }
        let mut applied = false;
        let mut terminal = false;
        let mut normalized_cumulative_quantity = Decimal::ZERO;

        slot.actual = match actual {
            ActualOrder::PendingNew(mut working)
                if working.order.client_order_id == client_order_id =>
            {
                working.cumulative_filled_quantity =
                    working.cumulative_filled_quantity.max(cumulative_quantity);
                normalized_cumulative_quantity = working.cumulative_filled_quantity;
                terminal = is_final || working.cumulative_filled_quantity >= working.order.quantity;
                applied = true;
                if terminal {
                    ActualOrder::Vacant
                } else {
                    ActualOrder::Working(working)
                }
            }
            ActualOrder::Working(mut working)
                if working.order.client_order_id == client_order_id =>
            {
                working.cumulative_filled_quantity =
                    working.cumulative_filled_quantity.max(cumulative_quantity);
                normalized_cumulative_quantity = working.cumulative_filled_quantity;
                terminal = is_final || working.cumulative_filled_quantity >= working.order.quantity;
                applied = true;
                if terminal {
                    ActualOrder::Vacant
                } else {
                    ActualOrder::Working(working)
                }
            }
            ActualOrder::PendingCancel {
                mut working,
                cancel_revision,
            } if working.order.client_order_id == client_order_id => {
                working.cumulative_filled_quantity =
                    working.cumulative_filled_quantity.max(cumulative_quantity);
                normalized_cumulative_quantity = working.cumulative_filled_quantity;
                terminal = is_final || working.cumulative_filled_quantity >= working.order.quantity;
                applied = true;
                if terminal {
                    ActualOrder::Vacant
                } else {
                    ActualOrder::PendingCancel {
                        working,
                        cancel_revision,
                    }
                }
            }
            ActualOrder::Uncertain {
                mut working,
                operation: UncertainOperation::Submit,
            } if working.order.client_order_id == client_order_id => {
                working.cumulative_filled_quantity =
                    working.cumulative_filled_quantity.max(cumulative_quantity);
                normalized_cumulative_quantity = working.cumulative_filled_quantity;
                terminal = is_final || working.cumulative_filled_quantity >= working.order.quantity;
                applied = true;
                if terminal {
                    ActualOrder::Vacant
                } else {
                    // A fill is authoritative proof that the submit reached the
                    // venue, so a partial fill resolves submit uncertainty.
                    ActualOrder::Working(working)
                }
            }
            ActualOrder::Uncertain {
                mut working,
                operation,
            } if working.order.client_order_id == client_order_id => {
                working.cumulative_filled_quantity =
                    working.cumulative_filled_quantity.max(cumulative_quantity);
                normalized_cumulative_quantity = working.cumulative_filled_quantity;
                terminal = is_final || working.cumulative_filled_quantity >= working.order.quantity;
                applied = true;
                if terminal {
                    ActualOrder::Vacant
                } else {
                    ActualOrder::Uncertain { working, operation }
                }
            }
            other => other,
        };

        if !applied {
            self.seen_trade_ids.remove(&(client_order_id, trade_id));
            return None;
        }

        if terminal {
            self.key_by_client_id.remove(&client_order_id);
            self.seen_trade_ids.remove_order(client_order_id);
            // Even if the strategy updated the desired order while this order
            // was live, require a revision observed *after* the final fill.
            slot.blocked_until_revision = Some(slot.revision);
        }
        slot.last_error = None;
        Some(OrderManagerEvent::Fill {
            key: key.clone(),
            trade_id,
            price,
            quantity,
            cumulative_quantity: normalized_cumulative_quantity,
            is_final: terminal,
        })
    }

    fn allocate_client_order_id(&mut self) -> ClientOrderId {
        loop {
            self.next_client_order_id = if self.next_client_order_id >= MAX_CLIENT_ORDER_ID {
                1
            } else {
                self.next_client_order_id + 1
            };
            let candidate = ClientOrderId(self.next_client_order_id);
            if !self.key_by_client_id.contains_key(&candidate) {
                return candidate;
            }
        }
    }

    fn snapshot(&self) -> OrderManagerSnapshot {
        let mut orders: Vec<_> = self
            .slots
            .iter()
            .map(|(key, slot)| managed_snapshot(key, slot))
            .collect();
        orders.sort_by(|left, right| {
            left.key
                .strategy_id
                .cmp(&right.key.strategy_id)
                .then_with(|| {
                    left.key
                        .instrument
                        .canonical_symbol
                        .cmp(&right.key.instrument.canonical_symbol)
                })
                .then_with(|| side_sort_key(left.key.side).cmp(&side_sort_key(right.key.side)))
        });

        OrderManagerSnapshot {
            venue: self.venue,
            execution_connected: self.execution_connected,
            halted_after_fill: self.halted_after_fill,
            halted_after_execution_fault: self.halted_after_execution_fault,
            orders,
            seen_trade_ids: self.seen_trade_ids.len(),
        }
    }

    fn snapshot_for_key(&self, key: &OrderKey) -> Option<ManagedOrderSnapshot> {
        self.slots.get(key).map(|slot| managed_snapshot(key, slot))
    }

    fn publish_state(&self, key: &OrderKey) {
        if let Some(snapshot) = self.snapshot_for_key(key) {
            let _ = self.events.send(OrderManagerEvent::StateChanged(snapshot));
        }
    }
}

fn desired_matches_working(desired: Option<&OrderIntent>, working: &WorkingOrder) -> bool {
    desired.is_some_and(|desired| {
        desired.price == working.order.price
            && desired.quantity == working.order.quantity
            && desired.time_in_force == working.order.time_in_force
            && desired.reduce_only == working.order.reduce_only
    })
}

fn record_submit_private_accept(venue: Venue, working: &mut WorkingOrder) {
    if working.private_accept_observed {
        return;
    }
    working.private_accept_observed = true;
    metrics::histogram!(
        "oms_submit_to_private_accept_latency_seconds",
        "venue" => venue.to_string()
    )
    .record(working.submitted_at.elapsed().as_secs_f64());
}

fn record_submit_private_reject(venue: Venue, working: &WorkingOrder) {
    if working.private_accept_observed {
        return;
    }
    metrics::histogram!(
        "oms_submit_to_private_reject_latency_seconds",
        "venue" => venue.to_string()
    )
    .record(working.submitted_at.elapsed().as_secs_f64());
}

fn record_submit_first_fill(venue: Venue, working: &mut WorkingOrder) {
    if working.first_fill_observed {
        return;
    }
    working.first_fill_observed = true;
    metrics::histogram!(
        "oms_submit_to_first_fill_latency_seconds",
        "venue" => venue.to_string()
    )
    .record(working.submitted_at.elapsed().as_secs_f64());
}

fn record_cancel_terminal(venue: Venue, working: &mut WorkingOrder, outcome: &'static str) {
    let Some(requested_at) = working.cancel_requested_at.take() else {
        return;
    };
    metrics::histogram!(
        "oms_cancel_to_terminal_latency_seconds",
        "venue" => venue.to_string(),
        "outcome" => outcome
    )
    .record(requested_at.elapsed().as_secs_f64());
}

fn managed_snapshot(key: &OrderKey, slot: &Slot) -> ManagedOrderSnapshot {
    let working = slot.actual.working();
    let state = match &slot.actual {
        ActualOrder::Vacant => NormalizedOrderState::Idle,
        ActualOrder::PendingNew(_) => NormalizedOrderState::PendingNew,
        ActualOrder::Working(working) if working.cumulative_filled_quantity > Decimal::ZERO => {
            NormalizedOrderState::PartiallyFilled
        }
        ActualOrder::Working(_) => NormalizedOrderState::Open,
        ActualOrder::PendingCancel { .. } => NormalizedOrderState::PendingCancel,
        ActualOrder::Uncertain { .. } => NormalizedOrderState::Uncertain,
    };

    ManagedOrderSnapshot {
        key: key.clone(),
        revision: slot.revision,
        desired: slot.desired.clone(),
        state,
        client_order_id: working.map(|working| working.order.client_order_id),
        venue_order_id: working.and_then(|working| working.venue_order_id.clone()),
        working_price: working.map(|working| working.order.price),
        working_quantity: working.map(|working| working.order.quantity),
        cumulative_filled_quantity: working
            .map(|working| working.cumulative_filled_quantity)
            .unwrap_or(Decimal::ZERO),
        blocked_until_revision: slot.blocked_until_revision,
        last_error: slot.last_error.clone(),
    }
}

const fn side_sort_key(side: Side) -> u8 {
    match side {
        Side::Buy => 0,
        Side::Sell => 1,
    }
}

const fn execution_event_kind_label(kind: &ExecutionEventKind) -> &'static str {
    match kind {
        ExecutionEventKind::Accepted { .. } => "accepted",
        ExecutionEventKind::Rejected { .. } => "rejected",
        ExecutionEventKind::Canceled => "canceled",
        ExecutionEventKind::CancelRejected { .. } => "cancel_rejected",
        ExecutionEventKind::Fill { .. } => "fill",
        ExecutionEventKind::Disconnected { .. } => "disconnected",
        ExecutionEventKind::Reconnected => "reconnected",
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Mutex;

    use tokio::sync::Semaphore;

    use super::*;

    #[derive(Clone)]
    struct FakeGateway {
        events: broadcast::Sender<ExecutionEvent>,
        submitted: Arc<Mutex<Vec<NewOrder>>>,
        canceled: Arc<Mutex<Vec<CancelOrder>>>,
        execution_ready: Arc<AtomicBool>,
        submit_gate: Option<Arc<Semaphore>>,
        cancel_gate: Option<Arc<Semaphore>>,
        submit_venue_id: bool,
        indeterminate_submit: bool,
        indeterminate_cancel: bool,
        panic_submit: bool,
    }

    impl FakeGateway {
        fn new() -> Self {
            let (events, _) = broadcast::channel(64);
            Self {
                events,
                submitted: Arc::new(Mutex::new(Vec::new())),
                canceled: Arc::new(Mutex::new(Vec::new())),
                execution_ready: Arc::new(AtomicBool::new(true)),
                submit_gate: None,
                cancel_gate: None,
                submit_venue_id: true,
                indeterminate_submit: false,
                indeterminate_cancel: false,
                panic_submit: false,
            }
        }

        fn with_gated_cancel() -> (Self, Arc<Semaphore>) {
            let mut gateway = Self::new();
            let gate = Arc::new(Semaphore::new(0));
            gateway.cancel_gate = Some(Arc::clone(&gate));
            (gateway, gate)
        }

        fn with_gated_submit() -> (Self, Arc<Semaphore>) {
            let mut gateway = Self::new();
            let gate = Arc::new(Semaphore::new(0));
            gateway.submit_gate = Some(Arc::clone(&gate));
            (gateway, gate)
        }

        fn emit(&self, client_order_id: Option<ClientOrderId>, kind: ExecutionEventKind) {
            match &kind {
                ExecutionEventKind::Disconnected { .. } => {
                    self.execution_ready.store(false, Ordering::Release);
                }
                ExecutionEventKind::Reconnected => {
                    self.execution_ready.store(true, Ordering::Release);
                }
                _ => {}
            }
            let _ = self.events.send(ExecutionEvent {
                venue: Venue::Hyperliquid,
                timestamp_ms: unix_timestamp_ms(),
                client_order_id,
                kind,
            });
        }

        fn submissions(&self) -> Vec<NewOrder> {
            self.submitted.lock().expect("submit lock").clone()
        }

        fn cancellations(&self) -> Vec<CancelOrder> {
            self.canceled.lock().expect("cancel lock").clone()
        }
    }

    impl OrderGateway for FakeGateway {
        fn venue(&self) -> Venue {
            Venue::Hyperliquid
        }

        fn subscribe(&self) -> broadcast::Receiver<ExecutionEvent> {
            self.events.subscribe()
        }

        fn execution_ready(&self) -> bool {
            self.execution_ready.load(Ordering::Acquire)
        }

        fn submit(
            &self,
            order: NewOrder,
        ) -> impl Future<Output = Result<OrderAck, GatewayError>> + Send {
            let submitted = Arc::clone(&self.submitted);
            let include_venue_id = self.submit_venue_id;
            let indeterminate = self.indeterminate_submit;
            let panic_submit = self.panic_submit;
            let gate = self.submit_gate.clone();
            async move {
                submitted.lock().expect("submit lock").push(order.clone());
                assert!(!panic_submit, "simulated gateway future panic");
                if let Some(gate) = gate {
                    gate.acquire().await.expect("submit gate open").forget();
                }
                if indeterminate {
                    return Err(GatewayError::Indeterminate(
                        "test submit outcome".to_owned(),
                    ));
                }
                Ok(OrderAck {
                    client_order_id: order.client_order_id,
                    venue_order_id: include_venue_id
                        .then(|| VenueOrderId(format!("venue-{}", order.client_order_id.0))),
                })
            }
        }

        fn cancel(
            &self,
            cancel: CancelOrder,
        ) -> impl Future<Output = Result<CancelAck, GatewayError>> + Send {
            let canceled = Arc::clone(&self.canceled);
            let gate = self.cancel_gate.clone();
            let indeterminate = self.indeterminate_cancel;
            async move {
                canceled.lock().expect("cancel lock").push(cancel.clone());
                if let Some(gate) = gate {
                    gate.acquire().await.expect("cancel gate open").forget();
                }
                if indeterminate {
                    return Err(GatewayError::Indeterminate(
                        "test cancel outcome".to_owned(),
                    ));
                }
                Ok(CancelAck {
                    client_order_id: cancel.client_order_id,
                })
            }
        }

        async fn cancel_all(&self, _instrument: Instrument) -> Result<(), GatewayError> {
            Ok(())
        }
    }

    fn decimal(value: &str) -> Decimal {
        value.parse().expect("valid decimal")
    }

    fn instrument() -> Instrument {
        Instrument {
            venue: Venue::Hyperliquid,
            canonical_symbol: "BTC-USD-PERP".to_owned(),
            venue_symbol: "BTC".to_owned(),
            price_tick: decimal("0.1"),
            quantity_step: decimal("0.001"),
        }
    }

    fn key() -> OrderKey {
        OrderKey {
            strategy_id: "wide-btc".to_owned(),
            instrument: instrument(),
            side: Side::Buy,
        }
    }

    fn intent(price: &str) -> OrderIntent {
        OrderIntent {
            price: decimal(price),
            quantity: decimal("0.001"),
            time_in_force: TimeInForce::PostOnly,
            reduce_only: false,
        }
    }

    async fn wait_for_order<G, F>(
        manager: &OrderManager<G>,
        order_key: &OrderKey,
        predicate: F,
    ) -> ManagedOrderSnapshot
    where
        G: OrderGateway,
        F: Fn(&ManagedOrderSnapshot) -> bool,
    {
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if let Some(snapshot) = manager
                    .snapshot_order(order_key.clone())
                    .await
                    .expect("snapshot")
                {
                    if predicate(&snapshot) {
                        return snapshot;
                    }
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("order state reached before timeout")
    }

    #[tokio::test]
    async fn revisions_are_strict_and_decimal_alignment_is_exact() {
        let gateway = Arc::new(FakeGateway::new());
        let manager = OrderManager::with_config(
            Arc::clone(&gateway),
            OrderManagerConfig {
                client_order_id_seed: 100,
                ..OrderManagerConfig::default()
            },
        );
        let order_key = key();

        let result = manager
            .set_desired(DesiredOrder {
                key: order_key.clone(),
                revision: 7,
                target: Some(intent("99900.1")),
            })
            .await
            .expect("first desired order");
        assert_eq!(result, DesiredOrderOutcome::Applied);
        wait_for_order(&manager, &order_key, |snapshot| {
            snapshot.state == NormalizedOrderState::Open
        })
        .await;

        let stale = manager
            .set_desired(DesiredOrder {
                key: order_key.clone(),
                revision: 7,
                target: Some(intent("99800.1")),
            })
            .await
            .expect("stale revision is a normal outcome");
        assert_eq!(
            stale,
            DesiredOrderOutcome::IgnoredStale {
                current_revision: 7
            }
        );

        let invalid = manager
            .set_desired(DesiredOrder {
                key: order_key,
                revision: 8,
                target: Some(intent("99800.15")),
            })
            .await
            .expect_err("half tick must be rejected exactly");
        assert!(matches!(invalid, OrderManagerError::InvalidDesired(_)));
        assert_eq!(gateway.submissions().len(), 1);
    }

    #[tokio::test]
    async fn ingress_only_submit_ack_stays_pending_until_private_acceptance() {
        let mut fake = FakeGateway::new();
        fake.submit_venue_id = false;
        let gateway = Arc::new(fake);
        let manager = OrderManager::with_config(
            Arc::clone(&gateway),
            OrderManagerConfig {
                client_order_id_seed: 150,
                ..OrderManagerConfig::default()
            },
        );
        let order_key = key();

        manager
            .set_desired(DesiredOrder {
                key: order_key.clone(),
                revision: 1,
                target: Some(intent("99000.0")),
            })
            .await
            .expect("desired order");
        tokio::time::timeout(Duration::from_secs(1), async {
            while gateway.submissions().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("submit completed");
        tokio::task::yield_now().await;

        let pending = manager
            .snapshot_order(order_key.clone())
            .await
            .expect("snapshot")
            .expect("managed order");
        assert_eq!(pending.state, NormalizedOrderState::PendingNew);
        assert_eq!(pending.venue_order_id, None);

        let client_order_id = pending.client_order_id.expect("pending client order ID");
        gateway.emit(
            Some(client_order_id),
            ExecutionEventKind::Accepted {
                venue_order_id: VenueOrderId("lighter-42".to_owned()),
            },
        );
        let open = wait_for_order(&manager, &order_key, |snapshot| {
            snapshot.state == NormalizedOrderState::Open
        })
        .await;
        assert_eq!(
            open.venue_order_id,
            Some(VenueOrderId("lighter-42".to_owned()))
        );
    }

    #[tokio::test]
    async fn withdrawn_ingress_acknowledged_pending_order_is_canceled() {
        let mut fake = FakeGateway::new();
        fake.submit_venue_id = false;
        let gateway = Arc::new(fake);
        let manager = OrderManager::with_config(
            Arc::clone(&gateway),
            OrderManagerConfig {
                client_order_id_seed: 175,
                ..OrderManagerConfig::default()
            },
        );
        let order_key = key();

        manager
            .set_desired(DesiredOrder {
                key: order_key.clone(),
                revision: 1,
                target: Some(intent("99000.0")),
            })
            .await
            .expect("desired order");
        let pending = wait_for_order(&manager, &order_key, |snapshot| {
            snapshot.state == NormalizedOrderState::PendingNew
        })
        .await;
        let client_order_id = pending.client_order_id.expect("pending client order ID");

        manager
            .set_desired(DesiredOrder {
                key: order_key.clone(),
                revision: 2,
                target: None,
            })
            .await
            .expect("withdraw desired order");
        let canceling = wait_for_order(&manager, &order_key, |snapshot| {
            snapshot.state == NormalizedOrderState::PendingCancel
        })
        .await;
        assert_eq!(canceling.client_order_id, Some(client_order_id));
        let cancellations = gateway.cancellations();
        assert_eq!(cancellations.len(), 1);
        assert_eq!(cancellations[0].venue_order_id, None);

        gateway.emit(Some(client_order_id), ExecutionEventKind::Canceled);
        wait_for_order(&manager, &order_key, |snapshot| {
            snapshot.state == NormalizedOrderState::Idle
        })
        .await;
    }

    #[tokio::test]
    async fn withdrawn_indeterminate_submit_gets_one_conservative_cancel_attempt() {
        let mut fake = FakeGateway::new();
        fake.indeterminate_submit = true;
        fake.indeterminate_cancel = true;
        let gateway = Arc::new(fake);
        let manager = OrderManager::with_config(
            Arc::clone(&gateway),
            OrderManagerConfig {
                client_order_id_seed: 190,
                ..OrderManagerConfig::default()
            },
        );
        let order_key = key();

        manager
            .set_desired(DesiredOrder {
                key: order_key.clone(),
                revision: 1,
                target: Some(intent("99000.0")),
            })
            .await
            .expect("desired order");
        let uncertain = wait_for_order(&manager, &order_key, |snapshot| {
            snapshot.state == NormalizedOrderState::Uncertain
        })
        .await;
        let client_order_id = uncertain
            .client_order_id
            .expect("uncertain client order ID");

        manager
            .set_desired(DesiredOrder {
                key: order_key.clone(),
                revision: 2,
                target: None,
            })
            .await
            .expect("withdraw uncertain order");
        wait_for_order(&manager, &order_key, |snapshot| {
            snapshot.state == NormalizedOrderState::Uncertain
                && snapshot
                    .last_error
                    .as_deref()
                    .is_some_and(|error| error.contains("test cancel outcome"))
        })
        .await;
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        assert_eq!(gateway.cancellations().len(), 1);

        gateway.emit(Some(client_order_id), ExecutionEventKind::Canceled);
        wait_for_order(&manager, &order_key, |snapshot| {
            snapshot.state == NormalizedOrderState::Idle
        })
        .await;
    }

    #[tokio::test]
    async fn stale_submit_error_after_private_rejection_is_not_reported_as_a_new_failure() {
        let (mut fake, submit_gate) = FakeGateway::with_gated_submit();
        fake.indeterminate_submit = true;
        let gateway = Arc::new(fake);
        let manager = OrderManager::with_config(
            Arc::clone(&gateway),
            OrderManagerConfig {
                client_order_id_seed: 195,
                ..OrderManagerConfig::default()
            },
        );
        let mut events = manager.subscribe();
        let order_key = key();

        manager
            .set_desired(DesiredOrder {
                key: order_key.clone(),
                revision: 1,
                target: Some(intent("99000.0")),
            })
            .await
            .expect("desired order");
        tokio::time::timeout(Duration::from_secs(1), async {
            while gateway.submissions().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("submit started");
        let client_order_id = gateway.submissions()[0].client_order_id;
        gateway.emit(
            Some(client_order_id),
            ExecutionEventKind::Rejected {
                reason: "private rejection won the race".to_owned(),
            },
        );
        wait_for_order(&manager, &order_key, |snapshot| {
            snapshot.state == NormalizedOrderState::Idle
                && snapshot.blocked_until_revision == Some(1)
        })
        .await;

        while events.try_recv().is_ok() {}
        submit_gate.add_permits(1);
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                match events.recv().await.expect("manager event stream") {
                    OrderManagerEvent::GatewayFailure { .. } => {
                        panic!("stale HTTP completion must not override a private terminal event")
                    }
                    OrderManagerEvent::StateChanged(_) => break,
                    _ => {}
                }
            }
        })
        .await
        .expect("stale completion was consumed");
    }

    #[tokio::test]
    async fn private_rejection_blocks_a_newer_revision_before_reconciliation() {
        let (fake, submit_gate) = FakeGateway::with_gated_submit();
        let gateway = Arc::new(fake);
        let manager = OrderManager::with_config(
            Arc::clone(&gateway),
            OrderManagerConfig {
                client_order_id_seed: 197,
                ..OrderManagerConfig::default()
            },
        );
        let order_key = key();

        manager
            .set_desired(DesiredOrder {
                key: order_key.clone(),
                revision: 1,
                target: Some(intent("99000.0")),
            })
            .await
            .expect("initial desire");
        tokio::time::timeout(Duration::from_secs(1), async {
            while gateway.submissions().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("submit started");
        let client_order_id = gateway.submissions()[0].client_order_id;

        manager
            .set_desired(DesiredOrder {
                key: order_key.clone(),
                revision: 2,
                target: Some(intent("98900.0")),
            })
            .await
            .expect("newer desired revision");
        gateway.emit(
            Some(client_order_id),
            ExecutionEventKind::Rejected {
                reason: "venue rejected old request".to_owned(),
            },
        );
        let blocked = wait_for_order(&manager, &order_key, |snapshot| {
            snapshot.state == NormalizedOrderState::Idle
                && snapshot.revision == 2
                && snapshot.blocked_until_revision == Some(2)
        })
        .await;
        assert_eq!(blocked.desired, Some(intent("98900.0")));
        assert_eq!(gateway.submissions().len(), 1);

        submit_gate.add_permits(1);
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        assert_eq!(gateway.submissions().len(), 1);
    }

    #[tokio::test]
    async fn cancel_rejection_blocks_a_newer_revision_before_reconciliation() {
        let (fake, cancel_gate) = FakeGateway::with_gated_cancel();
        let gateway = Arc::new(fake);
        let manager = OrderManager::with_config(
            Arc::clone(&gateway),
            OrderManagerConfig {
                client_order_id_seed: 198,
                ..OrderManagerConfig::default()
            },
        );
        let order_key = key();

        manager
            .set_desired(DesiredOrder {
                key: order_key.clone(),
                revision: 1,
                target: Some(intent("99000.0")),
            })
            .await
            .expect("initial desire");
        let open = wait_for_order(&manager, &order_key, |snapshot| {
            snapshot.state == NormalizedOrderState::Open
        })
        .await;
        let client_order_id = open.client_order_id.expect("live client order ID");

        manager
            .set_desired(DesiredOrder {
                key: order_key.clone(),
                revision: 2,
                target: Some(intent("98900.0")),
            })
            .await
            .expect("replacement desire");
        tokio::time::timeout(Duration::from_secs(1), async {
            while gateway.cancellations().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("cancel started");
        manager
            .set_desired(DesiredOrder {
                key: order_key.clone(),
                revision: 3,
                target: Some(intent("98800.0")),
            })
            .await
            .expect("newer desired revision");

        gateway.emit(
            Some(client_order_id),
            ExecutionEventKind::CancelRejected {
                reason: "venue refused cancel".to_owned(),
            },
        );
        let blocked = wait_for_order(&manager, &order_key, |snapshot| {
            snapshot.state == NormalizedOrderState::Open
                && snapshot.revision == 3
                && snapshot.blocked_until_revision == Some(3)
        })
        .await;
        assert_eq!(blocked.desired, Some(intent("98800.0")));
        assert_eq!(gateway.cancellations().len(), 1);

        cancel_gate.add_permits(1);
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        assert_eq!(gateway.cancellations().len(), 1);
    }

    #[tokio::test]
    async fn changed_desire_cancels_before_submitting_replacement() {
        let (fake, cancel_gate) = FakeGateway::with_gated_cancel();
        let gateway = Arc::new(fake);
        let manager = OrderManager::with_config(
            Arc::clone(&gateway),
            OrderManagerConfig {
                client_order_id_seed: 200,
                ..OrderManagerConfig::default()
            },
        );
        let order_key = key();

        manager
            .set_desired(DesiredOrder {
                key: order_key.clone(),
                revision: 1,
                target: Some(intent("99000.0")),
            })
            .await
            .expect("initial desire");
        wait_for_order(&manager, &order_key, |snapshot| {
            snapshot.state == NormalizedOrderState::Open
        })
        .await;

        manager
            .set_desired(DesiredOrder {
                key: order_key.clone(),
                revision: 2,
                target: Some(intent("98900.0")),
            })
            .await
            .expect("replacement desire");
        wait_for_order(&manager, &order_key, |snapshot| {
            snapshot.state == NormalizedOrderState::PendingCancel
        })
        .await;
        while gateway.cancellations().is_empty() {
            tokio::task::yield_now().await;
        }
        assert_eq!(gateway.submissions().len(), 1);

        cancel_gate.add_permits(1);
        // The request acknowledgement is not a terminal order event. Lighter,
        // in particular, only acknowledges sequencer ingress at this point.
        tokio::time::sleep(Duration::from_millis(10)).await;
        let cancel_accepted = manager
            .snapshot_order(order_key.clone())
            .await
            .expect("snapshot")
            .expect("managed order");
        assert_eq!(cancel_accepted.state, NormalizedOrderState::PendingCancel);
        assert_eq!(gateway.submissions().len(), 1);

        let canceled_id = cancel_accepted
            .client_order_id
            .expect("canceling client order ID");
        gateway.emit(Some(canceled_id), ExecutionEventKind::Canceled);
        let replacement = wait_for_order(&manager, &order_key, |snapshot| {
            snapshot.state == NormalizedOrderState::Open
                && snapshot.working_price == Some(decimal("98900.0"))
        })
        .await;
        assert_eq!(replacement.revision, 2);
        assert_eq!(gateway.submissions().len(), 2);
    }

    #[tokio::test]
    async fn unexpected_venue_cancel_blocks_the_current_revision() {
        let gateway = Arc::new(FakeGateway::new());
        let manager = OrderManager::with_config(
            Arc::clone(&gateway),
            OrderManagerConfig {
                client_order_id_seed: 250,
                ..OrderManagerConfig::default()
            },
        );
        let order_key = key();

        manager
            .set_desired(DesiredOrder {
                key: order_key.clone(),
                revision: 1,
                target: Some(intent("99000.0")),
            })
            .await
            .expect("initial desire");
        let open = wait_for_order(&manager, &order_key, |snapshot| {
            snapshot.state == NormalizedOrderState::Open
        })
        .await;
        let client_order_id = open.client_order_id.expect("live client order ID");

        gateway.emit(Some(client_order_id), ExecutionEventKind::Canceled);
        let blocked = wait_for_order(&manager, &order_key, |snapshot| {
            snapshot.state == NormalizedOrderState::Idle
                && snapshot.blocked_until_revision == Some(1)
                && snapshot
                    .last_error
                    .as_deref()
                    .is_some_and(|error| error.contains("without an active OMS cancel request"))
        })
        .await;
        assert_eq!(blocked.desired, Some(intent("99000.0")));
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert_eq!(gateway.submissions().len(), 1);

        manager
            .set_desired(DesiredOrder {
                key: order_key.clone(),
                revision: 2,
                target: Some(intent("99000.0")),
            })
            .await
            .expect("fresh revision");
        wait_for_order(&manager, &order_key, |snapshot| {
            snapshot.state == NormalizedOrderState::Open
                && snapshot.revision == 2
                && snapshot.client_order_id != Some(client_order_id)
        })
        .await;
        assert_eq!(gateway.submissions().len(), 2);
    }

    #[tokio::test]
    async fn final_fill_needs_a_revision_observed_after_the_fill() {
        let gateway = Arc::new(FakeGateway::new());
        let manager = OrderManager::with_config(
            Arc::clone(&gateway),
            OrderManagerConfig {
                client_order_id_seed: 300,
                ..OrderManagerConfig::default()
            },
        );
        let order_key = key();

        manager
            .set_desired(DesiredOrder {
                key: order_key.clone(),
                revision: 1,
                target: Some(intent("99000.0")),
            })
            .await
            .expect("initial desire");
        let open = wait_for_order(&manager, &order_key, |snapshot| {
            snapshot.state == NormalizedOrderState::Open
        })
        .await;
        let client_order_id = open.client_order_id.expect("live client order ID");

        gateway.emit(
            Some(client_order_id),
            ExecutionEventKind::Fill {
                trade_id: "trade-final".to_owned(),
                price: decimal("99000.0"),
                quantity: decimal("0.001"),
                cumulative_quantity: decimal("0.001"),
                is_final: true,
            },
        );
        let filled = wait_for_order(&manager, &order_key, |snapshot| {
            snapshot.state == NormalizedOrderState::Idle
                && snapshot.blocked_until_revision == Some(1)
        })
        .await;
        assert_eq!(filled.desired, Some(intent("99000.0")));
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert_eq!(gateway.submissions().len(), 1);

        manager
            .set_desired(DesiredOrder {
                key: order_key.clone(),
                revision: 2,
                target: Some(intent("99000.0")),
            })
            .await
            .expect("fresh post-fill revision");
        wait_for_order(&manager, &order_key, |snapshot| {
            snapshot.state == NormalizedOrderState::Open
                && snapshot.revision == 2
                && snapshot.client_order_id != Some(client_order_id)
        })
        .await;
        assert_eq!(gateway.submissions().len(), 2);
    }

    #[tokio::test]
    async fn configured_fill_latch_revokes_both_sides_and_refuses_new_desire() {
        let gateway = Arc::new(FakeGateway::new());
        let manager = OrderManager::with_config(
            Arc::clone(&gateway),
            OrderManagerConfig {
                client_order_id_seed: 350,
                halt_on_any_fill: true,
                ..OrderManagerConfig::default()
            },
        );
        let buy_key = key();
        let mut sell_key = key();
        sell_key.side = Side::Sell;

        for order_key in [&buy_key, &sell_key] {
            manager
                .set_desired(DesiredOrder {
                    key: order_key.clone(),
                    revision: 1,
                    target: Some(intent("99000.0")),
                })
                .await
                .expect("initial two-sided desire");
        }
        let buy = wait_for_order(&manager, &buy_key, |snapshot| {
            snapshot.state == NormalizedOrderState::Open
        })
        .await;
        let sell = wait_for_order(&manager, &sell_key, |snapshot| {
            snapshot.state == NormalizedOrderState::Open
        })
        .await;
        let buy_id = buy.client_order_id.expect("buy client order ID");
        let sell_id = sell.client_order_id.expect("sell client order ID");

        gateway.emit(
            Some(buy_id),
            ExecutionEventKind::Fill {
                trade_id: "partial-fill-halts".to_owned(),
                price: decimal("99000.0"),
                quantity: decimal("0.0005"),
                cumulative_quantity: decimal("0.0005"),
                is_final: false,
            },
        );

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let snapshot = manager.snapshot().await.expect("manager snapshot");
                if snapshot.halted_after_fill
                    && snapshot.orders.len() == 2
                    && snapshot.orders.iter().all(|order| order.desired.is_none())
                    && gateway.cancellations().len() == 2
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("fill latch revoked both quote sides");

        assert_eq!(
            manager
                .set_desired(DesiredOrder {
                    key: buy_key.clone(),
                    revision: 2,
                    target: Some(intent("98900.0")),
                })
                .await,
            Err(OrderManagerError::HaltedAfterFill)
        );

        gateway.emit(Some(buy_id), ExecutionEventKind::Canceled);
        gateway.emit(Some(sell_id), ExecutionEventKind::Canceled);
        manager.shutdown().await.expect("clean shutdown");
        assert_eq!(gateway.submissions().len(), 2);
    }

    #[tokio::test]
    async fn fail_closed_private_rejection_revokes_other_side_and_refuses_newer_revision() {
        let gateway = Arc::new(FakeGateway::new());
        let manager = OrderManager::with_config(
            Arc::clone(&gateway),
            OrderManagerConfig {
                client_order_id_seed: 375,
                fail_closed_on_execution_fault: true,
                ..OrderManagerConfig::default()
            },
        );
        let buy_key = key();
        let mut sell_key = key();
        sell_key.side = Side::Sell;

        for order_key in [&buy_key, &sell_key] {
            manager
                .set_desired(DesiredOrder {
                    key: order_key.clone(),
                    revision: 1,
                    target: Some(intent("99000.0")),
                })
                .await
                .expect("initial two-sided desire");
        }
        let buy = wait_for_order(&manager, &buy_key, |snapshot| {
            snapshot.state == NormalizedOrderState::Open
        })
        .await;
        let sell = wait_for_order(&manager, &sell_key, |snapshot| {
            snapshot.state == NormalizedOrderState::Open
        })
        .await;
        let buy_id = buy.client_order_id.expect("buy client order ID");
        let sell_id = sell.client_order_id.expect("sell client order ID");

        // Deliberately do not consume the public manager event. The OMS latch,
        // rather than the outer runtime's scheduling, must enforce fail-stop.
        gateway.emit(
            Some(buy_id),
            ExecutionEventKind::Rejected {
                reason: "private rejection".to_owned(),
            },
        );
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let snapshot = manager.snapshot().await.expect("manager snapshot");
                if snapshot.halted_after_execution_fault
                    && snapshot.orders.iter().all(|order| order.desired.is_none())
                    && gateway.cancellations().len() == 1
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("private rejection latched and withdrew the other side");

        assert_eq!(
            manager
                .set_desired(DesiredOrder {
                    key: buy_key,
                    revision: 2,
                    target: Some(intent("98900.0")),
                })
                .await,
            Err(OrderManagerError::HaltedAfterExecutionFault)
        );
        gateway.emit(Some(sell_id), ExecutionEventKind::Canceled);
        manager.shutdown().await.expect("clean shutdown");
        assert_eq!(gateway.submissions().len(), 2);
    }

    #[tokio::test]
    async fn fail_closed_disconnect_cannot_be_resurrected_by_fast_reconnect() {
        let gateway = Arc::new(FakeGateway::new());
        let manager = OrderManager::with_config(
            Arc::clone(&gateway),
            OrderManagerConfig {
                client_order_id_seed: 390,
                fail_closed_on_execution_fault: true,
                ..OrderManagerConfig::default()
            },
        );
        let order_key = key();
        manager
            .set_desired(DesiredOrder {
                key: order_key.clone(),
                revision: 1,
                target: Some(intent("99000.0")),
            })
            .await
            .expect("initial desire");
        let open = wait_for_order(&manager, &order_key, |snapshot| {
            snapshot.state == NormalizedOrderState::Open
        })
        .await;
        let client_order_id = open.client_order_id.expect("working client order ID");

        gateway.emit(
            None,
            ExecutionEventKind::Disconnected {
                reason: "private stream gap".to_owned(),
            },
        );
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let snapshot = manager.snapshot().await.expect("manager snapshot");
                if snapshot.halted_after_execution_fault && gateway.cancellations().len() == 1 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("disconnect latched and started cancellation");

        // The gateway may reconnect before the outer runtime consumes its
        // queued Connectivity(false) event. That must not reopen execution.
        gateway.emit(None, ExecutionEventKind::Reconnected);
        assert_eq!(
            manager
                .set_desired(DesiredOrder {
                    key: order_key,
                    revision: 2,
                    target: Some(intent("98900.0")),
                })
                .await,
            Err(OrderManagerError::HaltedAfterExecutionFault)
        );
        assert!(
            !manager
                .snapshot()
                .await
                .expect("manager snapshot")
                .execution_connected
        );

        gateway.emit(Some(client_order_id), ExecutionEventKind::Canceled);
        manager.shutdown().await.expect("clean shutdown");
        assert_eq!(gateway.submissions().len(), 1);
    }

    #[tokio::test]
    async fn fail_closed_gateway_error_withdraws_and_blocks_future_desire() {
        let mut fake = FakeGateway::new();
        fake.indeterminate_submit = true;
        let gateway = Arc::new(fake);
        let manager = OrderManager::with_config(
            Arc::clone(&gateway),
            OrderManagerConfig {
                client_order_id_seed: 395,
                fail_closed_on_execution_fault: true,
                ..OrderManagerConfig::default()
            },
        );
        let order_key = key();
        manager
            .set_desired(DesiredOrder {
                key: order_key.clone(),
                revision: 1,
                target: Some(intent("99000.0")),
            })
            .await
            .expect("initial desire");

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let snapshot = manager.snapshot().await.expect("manager snapshot");
                if snapshot.halted_after_execution_fault
                    && snapshot.orders[0].desired.is_none()
                    && gateway.cancellations().len() == 1
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("gateway failure latched and initiated conservative cancel");

        assert_eq!(
            manager
                .set_desired(DesiredOrder {
                    key: order_key,
                    revision: 2,
                    target: Some(intent("98900.0")),
                })
                .await,
            Err(OrderManagerError::HaltedAfterExecutionFault)
        );
        let client_order_id = gateway.submissions()[0].client_order_id;
        gateway.emit(Some(client_order_id), ExecutionEventKind::Canceled);
        manager.shutdown().await.expect("clean shutdown");
        assert_eq!(gateway.submissions().len(), 1);
    }

    #[tokio::test]
    async fn duplicate_trade_ids_do_not_advance_normalized_fill_state() {
        let gateway = Arc::new(FakeGateway::new());
        let manager = OrderManager::with_config(
            Arc::clone(&gateway),
            OrderManagerConfig {
                client_order_id_seed: 400,
                ..OrderManagerConfig::default()
            },
        );
        let order_key = key();

        manager
            .set_desired(DesiredOrder {
                key: order_key.clone(),
                revision: 1,
                target: Some(OrderIntent {
                    quantity: decimal("0.003"),
                    ..intent("99000.0")
                }),
            })
            .await
            .expect("initial desire");
        let open = wait_for_order(&manager, &order_key, |snapshot| {
            snapshot.state == NormalizedOrderState::Open
        })
        .await;
        let client_order_id = open.client_order_id.expect("live client order ID");

        gateway.emit(
            Some(client_order_id),
            ExecutionEventKind::Fill {
                trade_id: "trade-1".to_owned(),
                price: decimal("99000.0"),
                quantity: decimal("0.001"),
                cumulative_quantity: decimal("0.001"),
                is_final: false,
            },
        );
        wait_for_order(&manager, &order_key, |snapshot| {
            snapshot.state == NormalizedOrderState::PartiallyFilled
                && snapshot.cumulative_filled_quantity == decimal("0.001")
        })
        .await;

        // A malformed duplicate carries a different cumulative quantity. Trade
        // ID deduplication must win and preserve the first normalized state.
        gateway.emit(
            Some(client_order_id),
            ExecutionEventKind::Fill {
                trade_id: "trade-1".to_owned(),
                price: decimal("99000.0"),
                quantity: decimal("0.001"),
                cumulative_quantity: decimal("0.002"),
                is_final: false,
            },
        );
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        let snapshot = manager.snapshot().await.expect("manager snapshot");
        assert_eq!(snapshot.seen_trade_ids, 1);
        assert_eq!(
            snapshot.orders[0].cumulative_filled_quantity,
            decimal("0.001")
        );
    }

    #[tokio::test]
    async fn desired_issued_while_disconnected_waits_for_reconnect() {
        let gateway = Arc::new(FakeGateway::new());
        let manager = OrderManager::with_config(
            Arc::clone(&gateway),
            OrderManagerConfig {
                client_order_id_seed: 500,
                ..OrderManagerConfig::default()
            },
        );
        let order_key = key();

        gateway.emit(
            None,
            ExecutionEventKind::Disconnected {
                reason: "test disconnect".to_owned(),
            },
        );
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if !manager
                    .snapshot()
                    .await
                    .expect("snapshot")
                    .execution_connected
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("disconnect observed");

        manager
            .set_desired(DesiredOrder {
                key: order_key.clone(),
                revision: 1,
                target: Some(intent("99000.0")),
            })
            .await
            .expect("desired state is retained while disconnected");
        let paused = manager
            .snapshot_order(order_key.clone())
            .await
            .expect("snapshot")
            .expect("slot");
        assert_eq!(paused.state, NormalizedOrderState::Idle);
        assert!(gateway.submissions().is_empty());

        gateway.emit(None, ExecutionEventKind::Reconnected);
        wait_for_order(&manager, &order_key, |snapshot| {
            snapshot.state == NormalizedOrderState::Open
        })
        .await;
        assert!(
            manager
                .snapshot()
                .await
                .expect("snapshot")
                .execution_connected
        );
        assert_eq!(gateway.submissions().len(), 1);
    }

    #[tokio::test]
    async fn disconnect_revokes_preexisting_desire_and_cancels_working_order() {
        let gateway = Arc::new(FakeGateway::new());
        let manager = OrderManager::with_config(
            Arc::clone(&gateway),
            OrderManagerConfig {
                client_order_id_seed: 525,
                ..OrderManagerConfig::default()
            },
        );
        let order_key = key();
        manager
            .set_desired(DesiredOrder {
                key: order_key.clone(),
                revision: 1,
                target: Some(intent("99000.0")),
            })
            .await
            .expect("initial desired order");
        let open = wait_for_order(&manager, &order_key, |snapshot| {
            snapshot.state == NormalizedOrderState::Open
        })
        .await;
        let client_order_id = open.client_order_id.expect("working client order ID");

        gateway.emit(
            None,
            ExecutionEventKind::Disconnected {
                reason: "execution gap".to_owned(),
            },
        );
        let canceling = wait_for_order(&manager, &order_key, |snapshot| {
            snapshot.state == NormalizedOrderState::PendingCancel
        })
        .await;
        assert_eq!(canceling.desired, None);
        assert_eq!(gateway.cancellations().len(), 1);

        gateway.emit(Some(client_order_id), ExecutionEventKind::Canceled);
        wait_for_order(&manager, &order_key, |snapshot| {
            snapshot.state == NormalizedOrderState::Idle
        })
        .await;
        gateway.emit(None, ExecutionEventKind::Reconnected);
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        assert_eq!(gateway.submissions().len(), 1);
    }

    #[tokio::test]
    async fn readiness_level_closes_the_pre_subscription_disconnect_race() {
        let gateway = Arc::new(FakeGateway::new());
        // There are no broadcast receivers yet, so only the level-triggered
        // readiness state can carry this disconnect into manager startup.
        gateway.emit(
            None,
            ExecutionEventKind::Disconnected {
                reason: "before manager startup".to_owned(),
            },
        );
        let manager = OrderManager::with_config(
            Arc::clone(&gateway),
            OrderManagerConfig {
                client_order_id_seed: 550,
                ..OrderManagerConfig::default()
            },
        );
        let order_key = key();
        assert!(
            !manager
                .snapshot()
                .await
                .expect("snapshot")
                .execution_connected
        );

        manager
            .set_desired(DesiredOrder {
                key: order_key.clone(),
                revision: 1,
                target: Some(intent("99000.0")),
            })
            .await
            .expect("desired state retained while disconnected");
        tokio::task::yield_now().await;
        assert!(gateway.submissions().is_empty());

        gateway.emit(None, ExecutionEventKind::Reconnected);
        wait_for_order(&manager, &order_key, |snapshot| {
            snapshot.state == NormalizedOrderState::Open
        })
        .await;
        assert_eq!(gateway.submissions().len(), 1);
    }

    #[test]
    fn trade_id_deduplication_is_bounded() {
        let mut seen = BoundedTradeIds::new(2);
        let first = (ClientOrderId(1), "first".to_owned());
        let second = (ClientOrderId(1), "second".to_owned());
        let third = (ClientOrderId(1), "third".to_owned());

        assert!(seen.insert(first.clone()));
        assert!(seen.insert(second.clone()));
        assert!(!seen.insert(second.clone()));
        assert!(seen.insert(third));
        assert_eq!(seen.len(), 2);
        assert!(seen.insert(first));
    }

    #[tokio::test]
    async fn shutdown_waits_for_terminal_cancel_event() {
        let gateway = Arc::new(FakeGateway::new());
        let manager = OrderManager::with_config(
            Arc::clone(&gateway),
            OrderManagerConfig {
                client_order_id_seed: 600,
                shutdown_timeout: Duration::from_secs(1),
                ..OrderManagerConfig::default()
            },
        );
        let order_key = key();

        manager
            .set_desired(DesiredOrder {
                key: order_key.clone(),
                revision: 1,
                target: Some(intent("99000.0")),
            })
            .await
            .expect("initial desire");
        let open = wait_for_order(&manager, &order_key, |snapshot| {
            snapshot.state == NormalizedOrderState::Open
        })
        .await;
        let client_order_id = open.client_order_id.expect("open client order ID");

        let shutdown_manager = manager.clone();
        let shutdown = tokio::spawn(async move { shutdown_manager.shutdown().await });
        tokio::time::timeout(Duration::from_secs(1), async {
            while gateway.cancellations().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("shutdown submitted a cancel");
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(
            !shutdown.is_finished(),
            "cancel request acknowledgement must not finish shutdown"
        );

        gateway.emit(Some(client_order_id), ExecutionEventKind::Canceled);
        assert_eq!(shutdown.await.expect("shutdown task"), Ok(()));
    }

    #[tokio::test]
    async fn shutdown_reports_orders_that_never_reach_terminal_state() {
        let gateway = Arc::new(FakeGateway::new());
        let manager = OrderManager::with_config(
            Arc::clone(&gateway),
            OrderManagerConfig {
                client_order_id_seed: 700,
                shutdown_timeout: Duration::from_millis(10),
                ..OrderManagerConfig::default()
            },
        );
        let order_key = key();

        manager
            .set_desired(DesiredOrder {
                key: order_key.clone(),
                revision: 1,
                target: Some(intent("99000.0")),
            })
            .await
            .expect("initial desire");
        wait_for_order(&manager, &order_key, |snapshot| {
            snapshot.state == NormalizedOrderState::Open
        })
        .await;

        assert_eq!(
            manager.shutdown().await,
            Err(OrderManagerError::ShutdownTimedOut { active_orders: 1 })
        );
        assert_eq!(gateway.cancellations().len(), 1);
    }

    #[tokio::test]
    async fn shutdown_does_not_drop_an_inflight_gateway_request_at_deadline() {
        let (fake, cancel_gate) = FakeGateway::with_gated_cancel();
        let gateway = Arc::new(fake);
        let manager = OrderManager::with_config(
            Arc::clone(&gateway),
            OrderManagerConfig {
                client_order_id_seed: 725,
                shutdown_timeout: Duration::from_millis(10),
                ..OrderManagerConfig::default()
            },
        );
        let order_key = key();
        manager
            .set_desired(DesiredOrder {
                key: order_key.clone(),
                revision: 1,
                target: Some(intent("99000.0")),
            })
            .await
            .expect("initial desire");
        wait_for_order(&manager, &order_key, |snapshot| {
            snapshot.state == NormalizedOrderState::Open
        })
        .await;

        let shutdown_manager = manager.clone();
        let shutdown = tokio::spawn(async move { shutdown_manager.shutdown().await });
        tokio::time::timeout(Duration::from_secs(1), async {
            while gateway.cancellations().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("cancel started");
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(
            !shutdown.is_finished(),
            "the OMS must let an already-dispatched request settle before fallback"
        );

        cancel_gate.add_permits(1);
        assert_eq!(
            shutdown.await.expect("shutdown task"),
            Err(OrderManagerError::ShutdownTimedOut { active_orders: 1 })
        );
    }

    #[tokio::test]
    async fn client_order_ids_wrap_without_leaving_uint48_range() {
        let gateway = Arc::new(FakeGateway::new());
        let manager = OrderManager::with_config(
            Arc::clone(&gateway),
            OrderManagerConfig {
                client_order_id_seed: MAX_CLIENT_ORDER_ID - 1,
                ..OrderManagerConfig::default()
            },
        );
        let buy_key = key();
        let mut sell_key = key();
        sell_key.side = Side::Sell;

        for (order_key, revision) in [(&buy_key, 1), (&sell_key, 1)] {
            manager
                .set_desired(DesiredOrder {
                    key: order_key.clone(),
                    revision,
                    target: Some(intent("99000.0")),
                })
                .await
                .expect("desired order");
        }
        wait_for_order(&manager, &buy_key, |snapshot| {
            snapshot.state == NormalizedOrderState::Open
        })
        .await;
        wait_for_order(&manager, &sell_key, |snapshot| {
            snapshot.state == NormalizedOrderState::Open
        })
        .await;

        let mut ids: Vec<_> = gateway
            .submissions()
            .into_iter()
            .map(|order| order.client_order_id.0)
            .collect();
        ids.sort_unstable();
        assert_eq!(ids, vec![1, MAX_CLIENT_ORDER_ID]);
    }

    #[tokio::test]
    async fn actor_panic_is_reported_as_fatal_connectivity() {
        let mut fake = FakeGateway::new();
        fake.panic_submit = true;
        let gateway = Arc::new(fake);
        let manager = OrderManager::new(gateway);
        let mut events = manager.subscribe();

        assert_eq!(
            manager
                .set_desired(DesiredOrder {
                    key: key(),
                    revision: 1,
                    target: Some(intent("99000.0")),
                })
                .await,
            Ok(DesiredOrderOutcome::Applied)
        );

        let reason = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if let OrderManagerEvent::Connectivity {
                    connected: false,
                    reason: Some(reason),
                } = events.recv().await.expect("manager event")
                {
                    break reason;
                }
            }
        })
        .await
        .expect("actor failure was surfaced");
        assert!(reason.contains("order-manager actor failed"));
        assert_eq!(manager.snapshot().await, Err(OrderManagerError::Closed));
    }

    #[test]
    fn default_client_order_id_seeds_stay_inside_uint48_range() {
        for _ in 0..128 {
            let seed = OrderManagerConfig::default().client_order_id_seed;
            assert!((1..MAX_CLIENT_ORDER_ID).contains(&seed));
        }
    }
}
