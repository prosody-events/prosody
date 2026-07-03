//! Shared test utilities for middleware tests.
//!
//! Provides a unified `MockEventContext` that replaces multiple duplicate
//! implementations across test modules. Supports configurable behavior for
//! testing different middleware scenarios.

use std::future::{self, Future};
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use educe::Educe;
use parking_lot::Mutex;
use thiserror::Error;
use tokio::sync::watch;

use crate::consumer::event_context::{EventContext, StateAccessError, TerminationSignals};
use crate::consumer::partition::ShutdownPhase;
use crate::error::{ClassifyError, ErrorCategory};
use crate::loader::MessageLoader;
use crate::state::descriptor::{Registered, StateDescriptor};
use crate::state::session::{CellSession, UnavailableState};
use crate::timers::TimerType;
use crate::timers::datetime::CompactDateTime;

/// Timer-operation error the mock injects on demand, carrying the category to
/// classify as. The backstop arm is must-succeed (invariant 8), so it retries
/// **every** category forever — a `with_timer_failures(k, category)` context
/// exercises the retry-forever self-heal for each, including `Terminal` (which
/// `retry_step` retries rather than abandons) and `Permanent` (which the arm's
/// own loop retries past `retry_step`'s `Skip`).
#[derive(Debug, Error)]
#[error("mock timer operation failed ({0:?})")]
pub struct MockTimerError(pub ErrorCategory);

impl ClassifyError for MockTimerError {
    fn classify_error(&self) -> ErrorCategory {
        self.0
    }
}

/// Records timer operations for verification in tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimerOperation {
    /// Timer scheduled at given time with given type.
    Schedule(CompactDateTime, TimerType),
    /// All timers cleared and new one scheduled.
    ClearAndSchedule(CompactDateTime, TimerType),
    /// Single timer unscheduled.
    Unschedule(CompactDateTime, TimerType),
    /// All timers of given type cleared.
    ClearScheduled(TimerType),
}

/// Unified mock context for middleware tests.
///
/// Uses `tokio::sync::watch` channels (matching production `TimerContext`)
/// to avoid race conditions between flag checks and async notifications.
///
/// # Examples
///
/// ```ignore
/// // Basic usage - no signals active
/// let ctx = MockEventContext::new();
///
/// // Start in shutdown state
/// let ctx = MockEventContext::new().with_shutdown();
///
/// // Track timer operations
/// let ctx = MockEventContext::new().with_timer_tracking();
/// // ... use context ...
/// let ops = ctx.timer_operations();
/// ```
#[derive(Educe)]
#[educe(Clone(bound(S: Clone)))]
pub struct MockEventContext<P = serde_json::Value, S = UnavailableState<P>> {
    /// Partition/consumer shutdown signal (sender for mutations).
    shutdown_tx: Arc<watch::Sender<ShutdownPhase>>,
    /// Partition/consumer shutdown signal (receiver for queries).
    shutdown_rx: watch::Receiver<ShutdownPhase>,

    /// Message-level cancellation signal (sender for mutations).
    cancel_tx: Arc<watch::Sender<bool>>,
    /// Message-level cancellation signal (receiver for queries).
    cancel_rx: watch::Receiver<bool>,

    /// Timer operation tracking (None = disabled).
    timer_operations: Option<Arc<Mutex<Vec<TimerOperation>>>>,

    /// Durable timer rows the mock's timer ops maintain, so `scheduled`
    /// answers from the same state `schedule`/`clear_and_schedule` mutate —
    /// like the real trigger store. Seed with
    /// [`with_durable_timer`](Self::with_durable_timer) to simulate a prior
    /// epoch's timer surviving a partition reacquisition; observe what an op
    /// left standing with [`durable_scheduled`](Self::durable_scheduled).
    durable_timers: Arc<Mutex<Vec<(CompactDateTime, TimerType)>>>,

    /// Number of leading timer schedules that fail (with
    /// [`Self::timer_fail_category`]) before schedules start succeeding —
    /// drives the arm's retry-forever self-heal. Shared so a clone observes
    /// the same countdown.
    timer_fail_count: Arc<AtomicUsize>,

    /// The category the leading failures classify as.
    timer_fail_category: ErrorCategory,

    /// Keyed-state session descriptor binds route to; defaults to the
    /// [`UnavailableState`] stub.
    session: S,

    /// Payload type pin ([`EventContext::Payload`]); defaults to
    /// `serde_json::Value` to match the default consumer codec.
    _payload: PhantomData<fn() -> P>,
}

impl<P> Default for MockEventContext<P>
where
    P: Clone + Send + Sync + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<P> MockEventContext<P>
where
    P: Clone + Send + Sync + 'static,
{
    /// Create a new mock context with default state (no signals active,
    /// keyed state unavailable).
    #[must_use]
    pub fn new() -> Self {
        let (shutdown_tx, shutdown_rx) = watch::channel(ShutdownPhase::default());
        let (cancel_tx, cancel_rx) = watch::channel(false);
        Self {
            shutdown_tx: Arc::new(shutdown_tx),
            shutdown_rx,
            cancel_tx: Arc::new(cancel_tx),
            cancel_rx,
            timer_operations: None,
            durable_timers: Arc::new(Mutex::new(Vec::new())),
            timer_fail_count: Arc::new(AtomicUsize::new(0)),
            timer_fail_category: ErrorCategory::Permanent,
            session: UnavailableState::new(),
            _payload: PhantomData,
        }
    }
}

impl<P, S> MockEventContext<P, S> {
    /// Replaces the keyed-state session descriptor binds route to.
    #[must_use]
    pub fn with_session<S2>(self, session: S2) -> MockEventContext<P, S2> {
        MockEventContext {
            shutdown_tx: self.shutdown_tx,
            shutdown_rx: self.shutdown_rx,
            cancel_tx: self.cancel_tx,
            cancel_rx: self.cancel_rx,
            timer_operations: self.timer_operations,
            durable_timers: self.durable_timers,
            timer_fail_count: self.timer_fail_count,
            timer_fail_category: self.timer_fail_category,
            session,
            _payload: PhantomData,
        }
    }

    /// Seeds a durable timer standing before the test runs — simulating a
    /// prior epoch's trigger surviving a partition reacquisition.
    #[must_use]
    pub fn with_durable_timer(self, time: CompactDateTime, timer_type: TimerType) -> Self {
        self.durable_timers.lock().push((time, timer_type));
        self
    }

    /// The durable scheduled times of `timer_type` (what `scheduled` answers).
    #[must_use]
    pub fn durable_scheduled(&self, timer_type: TimerType) -> Vec<CompactDateTime> {
        self.durable_timers
            .lock()
            .iter()
            .filter(|(_, t)| *t == timer_type)
            .map(|(time, _)| *time)
            .collect()
    }

    /// Make the first `count` timer schedules fail with `category`, then
    /// succeed — so the backstop arm's retry-forever loop self-heals after
    /// `count` retries. Run on a paused clock so the retry backoff advances
    /// instantly.
    #[must_use]
    pub fn with_timer_failures(self, count: usize, category: ErrorCategory) -> Self {
        Self {
            timer_fail_count: Arc::new(AtomicUsize::new(count)),
            timer_fail_category: category,
            ..self
        }
    }

    /// Start in shutdown state.
    ///
    /// Used for testing early-exit behavior in middleware.
    #[must_use]
    pub fn with_shutdown(self) -> Self {
        self.shutdown_tx.send_replace(ShutdownPhase::Cancelling);
        self
    }

    /// Enable timer operation tracking.
    ///
    /// Use `timer_operations()` to retrieve recorded operations.
    #[must_use]
    pub fn with_timer_tracking(self) -> Self {
        Self {
            timer_operations: Some(Arc::new(Mutex::new(Vec::new()))),
            ..self
        }
    }

    /// Trigger partition/consumer shutdown signal.
    pub fn request_shutdown(&self) {
        self.shutdown_tx.send_replace(ShutdownPhase::Cancelling);
    }

    /// Trigger message-level cancellation signal.
    pub fn request_cancellation(&self) {
        self.cancel_tx.send_replace(true);
    }

    /// Get all recorded timer operations.
    ///
    /// Returns empty vec if timer tracking is not enabled.
    #[must_use]
    pub fn timer_operations(&self) -> Vec<TimerOperation> {
        self.timer_operations
            .as_ref()
            .map(|ops| ops.lock().clone())
            .unwrap_or_default()
    }

    /// Check if any timer was scheduled.
    ///
    /// Returns false if timer tracking is not enabled.
    #[must_use]
    pub fn has_scheduled_timer(&self) -> bool {
        self.timer_operations.as_ref().is_some_and(|ops| {
            ops.lock().iter().any(|op| {
                matches!(
                    op,
                    TimerOperation::Schedule(_, _) | TimerOperation::ClearAndSchedule(_, _)
                )
            })
        })
    }

    /// Count scheduled timers of a specific type.
    ///
    /// Returns 0 if timer tracking is not enabled.
    #[must_use]
    pub fn count_scheduled(&self, timer_type: TimerType) -> usize {
        self.timer_operations.as_ref().map_or(0, |ops| {
            ops.lock()
                .iter()
                .filter(|op| match op {
                    TimerOperation::Schedule(_, t) | TimerOperation::ClearAndSchedule(_, t) => {
                        *t == timer_type
                    }
                    _ => false,
                })
                .count()
        })
    }

    /// Record a timer operation (internal helper).
    fn record(&self, op: TimerOperation) {
        if let Some(ops) = &self.timer_operations {
            ops.lock().push(op);
        }
    }

    /// The result a schedule returns: a `with_timer_failures` failure (of the
    /// configured category) while the countdown is still positive (decrementing
    /// it), otherwise success.
    fn timer_result(&self) -> Result<(), MockTimerError> {
        // fetch_update fails (leaving 0) once the countdown is exhausted; while
        // positive it decrements and we inject one more failure.
        match self
            .timer_fail_count
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
        {
            Ok(_) => Err(MockTimerError(self.timer_fail_category)),
            Err(_) => Ok(()),
        }
    }
}

impl<P, S> TerminationSignals for MockEventContext<P, S> {
    fn is_shutdown(&self) -> bool {
        *self.shutdown_rx.borrow() >= ShutdownPhase::Cancelling
    }

    fn is_message_cancelled(&self) -> bool {
        *self.cancel_rx.borrow()
    }

    fn on_shutdown(&self) -> impl Future<Output = ()> + Send + 'static {
        let mut rx = self.shutdown_rx.clone();
        async move {
            let _ = rx.wait_for(|v| *v >= ShutdownPhase::Cancelling).await;
        }
    }

    fn on_message_cancelled(&self) -> impl Future<Output = ()> + Send + 'static {
        let mut rx = self.cancel_rx.clone();
        async move {
            let _ = rx.wait_for(|&v| v).await;
        }
    }
}

impl<P, S> EventContext for MockEventContext<P, S>
where
    P: Send + Sync + 'static,
    S: CellSession<Loader: MessageLoader<Payload = P>>,
{
    type Error = MockTimerError;
    type Payload = P;
    type State = S;

    fn state<DESC>(&self, registered: Registered<DESC>) -> Result<DESC::Handle<S>, StateAccessError>
    where
        DESC: StateDescriptor,
    {
        registered.descriptor().bind(&self.session)
    }

    fn should_cancel(&self) -> bool {
        *self.shutdown_rx.borrow() >= ShutdownPhase::Cancelling || *self.cancel_rx.borrow()
    }

    fn on_cancel(&self) -> impl Future<Output = ()> + Send + 'static {
        let mut shutdown_rx = self.shutdown_rx.clone();
        let mut cancel_rx = self.cancel_rx.clone();
        async move {
            tokio::select! {
                _ = shutdown_rx.wait_for(|v| *v >= ShutdownPhase::Cancelling) => {}
                _ = cancel_rx.wait_for(|&v| v) => {}
            }
        }
    }

    fn cancel(&self) {
        self.cancel_tx.send_replace(true);
    }

    fn uncancel(&self) {
        self.cancel_tx.send_replace(false);
    }

    fn schedule(
        &self,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.record(TimerOperation::Schedule(time, timer_type));
        let result = self.timer_result();
        if result.is_ok() {
            self.durable_timers.lock().push((time, timer_type));
        }
        future::ready(result)
    }

    fn clear_and_schedule(
        &self,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.record(TimerOperation::ClearAndSchedule(time, timer_type));
        let result = self.timer_result();
        if result.is_ok() {
            // Type-scoped singleton overwrite, like the real store.
            let mut timers = self.durable_timers.lock();
            timers.retain(|(_, t)| *t != timer_type);
            timers.push((time, timer_type));
        }
        future::ready(result)
    }

    fn unschedule(
        &self,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.record(TimerOperation::Unschedule(time, timer_type));
        self.durable_timers
            .lock()
            .retain(|entry| *entry != (time, timer_type));
        future::ready(Ok(()))
    }

    fn clear_scheduled(
        &self,
        timer_type: TimerType,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.record(TimerOperation::ClearScheduled(timer_type));
        self.durable_timers.lock().retain(|(_, t)| *t != timer_type);
        future::ready(Ok(()))
    }

    fn invalidate(self) {
        self.cancel();
    }

    fn scheduled(
        &self,
        timer_type: TimerType,
    ) -> impl Future<Output = Result<Vec<CompactDateTime>, Self::Error>> + Send + 'static {
        future::ready(Ok(self.durable_scheduled(timer_type)))
    }
}
