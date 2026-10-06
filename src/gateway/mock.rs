use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tokio::sync::{broadcast, Mutex};

use super::{require_execution_ready, GatewayError, OrderGateway};
use crate::types::{
    unix_timestamp_ms, CancelAck, CancelOrder, ClientOrderId, ExecutionEvent, ExecutionEventKind,
    Instrument, NewOrder, OrderAck, Venue, VenueOrderId,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GatewayCall {
    Submit(NewOrder),
    Cancel(CancelOrder),
    CancelAll(Instrument),
}

/// Deterministic test gateway. It never reaches a network and is not compiled
/// into production builds unless the `test-support` feature is explicit.
pub struct MockGateway {
    venue: Venue,
    events: broadcast::Sender<ExecutionEvent>,
    calls: Arc<Mutex<Vec<GatewayCall>>>,
    open: Arc<Mutex<HashMap<ClientOrderId, NewOrder>>>,
    execution_ready: Arc<AtomicBool>,
}

impl MockGateway {
    pub fn new(venue: Venue) -> Self {
        let (events, _) = broadcast::channel(256);
        Self {
            venue,
            events,
            calls: Arc::new(Mutex::new(Vec::new())),
            open: Arc::new(Mutex::new(HashMap::new())),
            // The in-memory gateway has no external stream to establish.
            execution_ready: Arc::new(AtomicBool::new(true)),
        }
    }

    pub async fn calls(&self) -> Vec<GatewayCall> {
        self.calls.lock().await.clone()
    }

    pub fn emit(&self, event: ExecutionEvent) {
        match &event.kind {
            ExecutionEventKind::Reconnected => {
                self.execution_ready.store(true, Ordering::Release);
            }
            ExecutionEventKind::Disconnected { .. } => {
                self.execution_ready.store(false, Ordering::Release);
            }
            _ => {}
        }
        let _ = self.events.send(event);
    }
}

impl OrderGateway for MockGateway {
    fn venue(&self) -> Venue {
        self.venue
    }

    fn subscribe(&self) -> broadcast::Receiver<ExecutionEvent> {
        self.events.subscribe()
    }

    fn execution_ready(&self) -> bool {
        self.execution_ready.load(Ordering::Acquire)
    }

    async fn submit(&self, order: NewOrder) -> Result<OrderAck, GatewayError> {
        require_execution_ready(&self.execution_ready, self.venue)?;
        self.calls
            .lock()
            .await
            .push(GatewayCall::Submit(order.clone()));
        self.open
            .lock()
            .await
            .insert(order.client_order_id, order.clone());
        let venue_order_id = VenueOrderId(format!("mock-{}", order.client_order_id));
        self.emit(ExecutionEvent {
            venue: self.venue,
            timestamp_ms: unix_timestamp_ms(),
            client_order_id: Some(order.client_order_id),
            kind: ExecutionEventKind::Accepted {
                venue_order_id: venue_order_id.clone(),
            },
        });
        Ok(OrderAck {
            client_order_id: order.client_order_id,
            venue_order_id: Some(venue_order_id),
        })
    }

    async fn cancel(&self, cancel: CancelOrder) -> Result<CancelAck, GatewayError> {
        self.calls
            .lock()
            .await
            .push(GatewayCall::Cancel(cancel.clone()));
        self.open.lock().await.remove(&cancel.client_order_id);
        self.emit(ExecutionEvent {
            venue: self.venue,
            timestamp_ms: unix_timestamp_ms(),
            client_order_id: Some(cancel.client_order_id),
            kind: ExecutionEventKind::Canceled,
        });
        Ok(CancelAck {
            client_order_id: cancel.client_order_id,
        })
    }

    async fn cancel_all(&self, instrument: Instrument) -> Result<(), GatewayError> {
        self.calls
            .lock()
            .await
            .push(GatewayCall::CancelAll(instrument.clone()));
        self.open
            .lock()
            .await
            .retain(|_, order| order.instrument != instrument);
        Ok(())
    }
}
