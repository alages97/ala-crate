mod hyperliquid;
mod lighter;

#[cfg(any(test, feature = "test-support"))]
pub mod mock;

use std::collections::{HashSet, VecDeque};
use std::future::Future;
use std::hash::Hash;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use thiserror::Error;
use tokio::sync::broadcast;

use crate::types::{CancelAck, CancelOrder, ExecutionEvent, Instrument, NewOrder, OrderAck, Venue};

pub use hyperliquid::{HyperliquidGateway, HyperliquidGatewayConfig};
pub use lighter::{LighterGateway, LighterGatewayConfig};

const GATEWAY_DEDUPE_CAPACITY: usize = 8_192;
const PRIVATE_LIFECYCLE_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LifecycleKind {
    PrivateAcceptance,
    CancelTerminal,
    FillReconciliation,
}

impl LifecycleKind {
    const fn label(self) -> &'static str {
        match self {
            Self::PrivateAcceptance => "private acceptance",
            Self::CancelTerminal => "cancel terminal update",
            Self::FillReconciliation => "authoritative fill reconciliation",
        }
    }
}

#[derive(Debug, Clone)]
struct LifecycleWait {
    kind: LifecycleKind,
    started: Instant,
}

impl LifecycleWait {
    fn new(kind: LifecycleKind) -> Self {
        Self {
            kind,
            started: Instant::now(),
        }
    }

    fn refresh(&mut self) {
        self.started = Instant::now();
    }

    fn is_stalled_at(&self, now: Instant) -> bool {
        now.saturating_duration_since(self.started) >= PRIVATE_LIFECYCLE_TIMEOUT
    }
}

/// A private fill is authoritative proof that submission succeeded. For other
/// lifecycle phases it is progress, but it does not prove cancel terminality or
/// complete a pending economic-fill reconciliation.
fn observe_private_fill(wait: &mut Option<LifecycleWait>) {
    match wait {
        Some(current) if current.kind == LifecycleKind::PrivateAcceptance => *wait = None,
        Some(current) => current.refresh(),
        None => {}
    }
}

/// Fixed-memory duplicate suppression for replayed private-stream events.
/// Terminal orders are removed independently; this protects long-running
/// processes from retaining every venue trade identifier forever.
struct BoundedDedupe<T> {
    capacity: usize,
    seen: HashSet<T>,
    insertion_order: VecDeque<T>,
}

impl<T> Default for BoundedDedupe<T>
where
    T: Clone + Eq + Hash,
{
    fn default() -> Self {
        Self {
            capacity: GATEWAY_DEDUPE_CAPACITY,
            seen: HashSet::with_capacity(GATEWAY_DEDUPE_CAPACITY),
            insertion_order: VecDeque::with_capacity(GATEWAY_DEDUPE_CAPACITY),
        }
    }
}

impl<T> BoundedDedupe<T>
where
    T: Clone + Eq + Hash,
{
    fn contains(&self, value: &T) -> bool {
        self.seen.contains(value)
    }

    fn insert(&mut self, value: T) -> bool {
        if self.seen.contains(&value) {
            return false;
        }
        if self.insertion_order.len() == self.capacity {
            if let Some(evicted) = self.insertion_order.pop_front() {
                self.seen.remove(&evicted);
            }
        }
        self.insertion_order.push_back(value.clone());
        self.seen.insert(value);
        true
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum GatewayError {
    #[error("invalid order: {0}")]
    InvalidOrder(String),
    #[error("venue rejected request ({code}): {message}")]
    Rejected { code: String, message: String },
    #[error("authentication failed: {0}")]
    Authentication(String),
    #[error("rate limited: {message}")]
    RateLimited {
        message: String,
        retry_after: Option<Duration>,
    },
    #[error("transport failed: {0}")]
    Transport(String),
    #[error("venue protocol error: {0}")]
    Protocol(String),
    /// The request may have reached the venue. The OMS must reconcile it by
    /// client order ID instead of retrying blindly.
    #[error("request outcome is indeterminate: {0}")]
    Indeterminate(String),
    #[error("operation is not supported: {0}")]
    Unsupported(&'static str),
}

impl GatewayError {
    pub const fn safe_to_retry(&self) -> bool {
        matches!(self, Self::RateLimited { .. } | Self::Transport(_))
    }
}

fn require_execution_ready(execution_ready: &AtomicBool, venue: Venue) -> Result<(), GatewayError> {
    if execution_ready.load(Ordering::Acquire) {
        Ok(())
    } else {
        Err(GatewayError::Transport(format!(
            "{venue} private execution stream is not ready; order was not sent"
        )))
    }
}

/// Venue-neutral execution boundary. Implementations own signing, nonces,
/// venue identifiers, REST payloads, private streams and reconciliation.
pub trait OrderGateway: Send + Sync + 'static {
    fn venue(&self) -> Venue;

    fn subscribe(&self) -> broadcast::Receiver<ExecutionEvent>;

    /// Whether authenticated private execution updates are currently live.
    /// Callers should subscribe first and then read this value so they cannot
    /// miss a connectivity transition between the two operations.
    fn execution_ready(&self) -> bool;

    fn submit(
        &self,
        order: NewOrder,
    ) -> impl Future<Output = Result<OrderAck, GatewayError>> + Send;

    fn cancel(
        &self,
        cancel: CancelOrder,
    ) -> impl Future<Output = Result<CancelAck, GatewayError>> + Send;

    /// Attempt cancellation of every order this gateway instance tracks for
    /// the supplied instrument. Implementations must continue after an
    /// individual failure and return the first error after all attempts.
    fn cancel_all(
        &self,
        instrument: Instrument,
    ) -> impl Future<Output = Result<(), GatewayError>> + Send;
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;

    use super::{
        observe_private_fill, require_execution_ready, BoundedDedupe, LifecycleKind, LifecycleWait,
        PRIVATE_LIFECYCLE_TIMEOUT,
    };
    use crate::types::Venue;

    #[test]
    fn bounded_dedupe_evicts_oldest_identifier() {
        let mut dedupe = BoundedDedupe {
            capacity: 2,
            ..BoundedDedupe::default()
        };
        assert!(dedupe.insert(1));
        assert!(dedupe.insert(2));
        assert!(!dedupe.insert(1));
        assert!(dedupe.insert(3));
        assert!(!dedupe.contains(&1));
        assert!(dedupe.insert(1));
    }

    #[test]
    fn execution_readiness_is_fail_closed() {
        let ready = AtomicBool::new(false);
        assert!(require_execution_ready(&ready, Venue::Hyperliquid).is_err());
        ready.store(true, std::sync::atomic::Ordering::Release);
        assert!(require_execution_ready(&ready, Venue::Hyperliquid).is_ok());
    }

    #[test]
    fn lifecycle_wait_detects_a_stalled_private_update() {
        let mut wait = LifecycleWait::new(LifecycleKind::PrivateAcceptance);
        let now = wait.started + PRIVATE_LIFECYCLE_TIMEOUT;
        assert!(wait.is_stalled_at(now));
        wait.refresh();
        assert!(!wait.is_stalled_at(wait.started));
    }

    #[test]
    fn a_private_fill_resolves_acceptance_but_preserves_other_lifecycle_waits() {
        let mut acceptance = Some(LifecycleWait::new(LifecycleKind::PrivateAcceptance));
        observe_private_fill(&mut acceptance);
        assert!(acceptance.is_none());

        let mut cancellation = Some(LifecycleWait::new(LifecycleKind::CancelTerminal));
        observe_private_fill(&mut cancellation);
        assert!(cancellation
            .as_ref()
            .is_some_and(|wait| wait.kind == LifecycleKind::CancelTerminal));
    }
}
