//! Shared test utilities for middleware tests.
//!
//! Provides a unified `MockEventContext` that replaces multiple duplicate
//! implementations across test modules. Supports configurable behavior for
//! testing different middleware scenarios.

use std::convert::Infallible;
use std::future::{self, Future};
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::watch;

use crate::consumer::event_context::{EventContext, TerminationSignals};
use crate::timers::TimerType;
use crate::timers::datetime::CompactDateTime;

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
#[derive(Clone)]
pub struct MockEventContext {
    /// Partition/consumer shutdown signal (sender for mutations).
    shutdown_tx: Arc<watch::Sender<bool>>,
    /// Partition/consumer shutdown signal (receiver for queries).
    shutdown_rx: watch::Receiver<bool>,

    /// Message-level cancellation signal (sender for mutations).
    cancel_tx: Arc<watch::Sender<bool>>,
    /// Message-level cancellation signal (receiver for queries).
    cancel_rx: watch::Receiver<bool>,

    /// Timer operation tracking (None = disabled).
    timer_operations: Option<Arc<Mutex<Vec<TimerOperation>>>>,
}

impl Default for MockEventContext {
    fn default() -> Self {
        Self::new()
    }
}

impl MockEventContext {
    /// Create a new mock context with default state (no signals active).
    #[must_use]
    pub fn new() -> Self {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (cancel_tx, cancel_rx) = watch::channel(false);
        Self {
            shutdown_tx: Arc::new(shutdown_tx),
            shutdown_rx,
            cancel_tx: Arc::new(cancel_tx),
            cancel_rx,
            timer_operations: None,
        }
    }

    /// Start in shutdown state.
    ///
    /// Used for testing early-exit behavior in middleware.
    #[must_use]
    pub fn with_shutdown(self) -> Self {
        self.shutdown_tx.send_replace(true);
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
        self.shutdown_tx.send_replace(true);
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
}

impl TerminationSignals for MockEventContext {
    fn is_shutdown(&self) -> bool {
        *self.shutdown_rx.borrow()
    }

    fn is_message_cancelled(&self) -> bool {
        *self.cancel_rx.borrow()
    }

    fn on_shutdown(&self) -> impl Future<Output = ()> + Send + 'static {
        let mut rx = self.shutdown_rx.clone();
        async move {
            let _ = rx.wait_for(|&v| v).await;
        }
    }

    fn on_message_cancelled(&self) -> impl Future<Output = ()> + Send + 'static {
        let mut rx = self.cancel_rx.clone();
        async move {
            let _ = rx.wait_for(|&v| v).await;
        }
    }
}

impl EventContext for MockEventContext {
    type Error = Infallible;

    fn should_cancel(&self) -> bool {
        *self.shutdown_rx.borrow() || *self.cancel_rx.borrow()
    }

    fn on_cancel(&self) -> impl Future<Output = ()> + Send + 'static {
        let mut shutdown_rx = self.shutdown_rx.clone();
        let mut cancel_rx = self.cancel_rx.clone();
        async move {
            tokio::select! {
                _ = shutdown_rx.wait_for(|&v| v) => {}
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
        future::ready(Ok(()))
    }

    fn clear_and_schedule(
        &self,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.record(TimerOperation::ClearAndSchedule(time, timer_type));
        future::ready(Ok(()))
    }

    fn unschedule(
        &self,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.record(TimerOperation::Unschedule(time, timer_type));
        future::ready(Ok(()))
    }

    fn clear_scheduled(
        &self,
        timer_type: TimerType,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.record(TimerOperation::ClearScheduled(timer_type));
        future::ready(Ok(()))
    }

    fn invalidate(self) {
        self.cancel();
    }

    fn scheduled(
        &self,
        _timer_type: TimerType,
    ) -> impl Future<Output = Result<Vec<CompactDateTime>, Self::Error>> + Send + 'static {
        future::ready(Ok(Vec::new()))
    }
}
