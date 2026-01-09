//! Shared test utilities for middleware tests.
//!
//! Provides a unified `MockEventContext` that replaces multiple duplicate
//! implementations across test modules. Supports configurable behavior for
//! testing different middleware scenarios.

use std::convert::Infallible;
use std::future::{self, Future};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use futures::stream::{self, Stream};
use parking_lot::Mutex;
use tokio::sync::Notify;

use crate::consumer::event_context::{CancellationSignals, EventContext};
use crate::timers::TimerType;
use crate::timers::datetime::CompactDateTime;

/// Records timer operations for verification in tests.
#[derive(Debug, Clone, PartialEq, Eq)]
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
/// Supports:
/// - Separate shutdown and cancellation signals with async notification
/// - Optional timer operation tracking
/// - Builder pattern for test-specific configuration
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
    /// Partition/consumer shutdown signal.
    shutdown_requested: Arc<AtomicBool>,
    shutdown_notify: Arc<Notify>,

    /// Message-level cancellation signal (e.g., timeout).
    message_cancelled: Arc<AtomicBool>,
    message_cancel_notify: Arc<Notify>,

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
        Self {
            shutdown_requested: Arc::new(AtomicBool::new(false)),
            shutdown_notify: Arc::new(Notify::new()),
            message_cancelled: Arc::new(AtomicBool::new(false)),
            message_cancel_notify: Arc::new(Notify::new()),
            timer_operations: None,
        }
    }

    /// Start in shutdown state.
    ///
    /// Used for testing early-exit behavior in middleware.
    #[must_use]
    pub fn with_shutdown(self) -> Self {
        self.shutdown_requested.store(true, Ordering::Relaxed);
        self.shutdown_notify.notify_waiters();
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
        self.shutdown_requested.store(true, Ordering::Relaxed);
        self.shutdown_notify.notify_waiters();
    }

    /// Trigger message-level cancellation signal (e.g., timeout).
    pub fn request_cancellation(&self) {
        self.message_cancelled.store(true, Ordering::Relaxed);
        self.message_cancel_notify.notify_waiters();
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

    /// Clear all recorded timer operations.
    pub fn clear_timer_operations(&self) {
        if let Some(ops) = &self.timer_operations {
            ops.lock().clear();
        }
    }

    /// Record a timer operation (internal helper).
    fn record(&self, op: TimerOperation) {
        if let Some(ops) = &self.timer_operations {
            ops.lock().push(op);
        }
    }
}

impl CancellationSignals for MockEventContext {
    fn is_shutdown(&self) -> bool {
        self.shutdown_requested.load(Ordering::Relaxed)
    }

    fn is_message_cancelled(&self) -> bool {
        self.message_cancelled.load(Ordering::Relaxed)
    }

    fn on_shutdown(&self) -> impl Future<Output = ()> + Send + 'static {
        let notify = Arc::clone(&self.shutdown_notify);
        let already_shutdown = self.shutdown_requested.load(Ordering::Relaxed);
        async move {
            if !already_shutdown {
                notify.notified().await;
            }
        }
    }

    fn on_message_cancelled(&self) -> impl Future<Output = ()> + Send + 'static {
        let notify = Arc::clone(&self.message_cancel_notify);
        let already_cancelled = self.message_cancelled.load(Ordering::Relaxed);
        async move {
            if !already_cancelled {
                notify.notified().await;
            }
        }
    }
}

impl EventContext for MockEventContext {
    type Error = Infallible;

    fn should_cancel(&self) -> bool {
        self.shutdown_requested.load(Ordering::Relaxed)
            || self.message_cancelled.load(Ordering::Relaxed)
    }

    fn on_cancel(&self) -> impl Future<Output = ()> + Send + 'static {
        let shutdown_notify = Arc::clone(&self.shutdown_notify);
        let cancel_notify = Arc::clone(&self.message_cancel_notify);
        let already_cancelled = self.should_cancel();
        async move {
            if !already_cancelled {
                tokio::select! {
                    () = shutdown_notify.notified() => {}
                    () = cancel_notify.notified() => {}
                }
            }
        }
    }

    fn cancel(&self) {
        self.message_cancelled.store(true, Ordering::Relaxed);
        self.message_cancel_notify.notify_waiters();
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
    ) -> impl Stream<Item = Result<CompactDateTime, Self::Error>> + Send + 'static {
        stream::empty()
    }
}
