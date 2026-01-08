use crate::consumer::event_context::EventContext;
use crate::timers::TimerType;
use crate::timers::datetime::CompactDateTime;
use futures::{Stream, stream};
use parking_lot::Mutex;
use std::convert::Infallible;
use std::future;
use std::sync::{Arc, LazyLock};
use tokio::runtime::{Builder, Runtime};

mod context;
mod generator;
mod handler;
mod harness;
mod integration;
mod loader;
mod properties;
mod types;

pub use loader::{FailableLoader, FailableLoaderError, LoaderFailureType};

/// Base backoff delay in seconds for test handler config.
pub const TEST_BASE_BACKOFF_SECS: u32 = 1;

/// Maximum backoff delay in seconds for test handler config.
pub const TEST_MAX_BACKOFF_SECS: u32 = 3600;

/// Shared multi-threaded runtime for all defer property tests.
///
/// # Rationale for `expect`
///
/// `LazyLock` requires a non-fallible closure. Runtime creation failure is
/// unrecoverable in test infrastructure - tests cannot run without a runtime.
#[allow(
    clippy::expect_used,
    reason = "LazyLock requires non-fallible closure; test infra"
)]
pub(crate) static TEST_RUNTIME: LazyLock<Runtime> = LazyLock::new(|| {
    Builder::new_multi_thread()
        .enable_time()
        .build()
        .expect("Failed to create tokio runtime")
});

/// Timer operation recorded by `MockContext`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimerOperation {
    /// Timer was scheduled.
    Schedule(CompactDateTime, TimerType),
    /// Timer was cleared and rescheduled.
    ClearAndSchedule(CompactDateTime, TimerType),
    /// Timer was unscheduled.
    Unschedule(CompactDateTime, TimerType),
    /// All timers of a type were cleared.
    ClearScheduled(TimerType),
}

/// Minimal mock context for property tests.
///
/// Tracks all timer operations without executing them or interacting with
/// actual timer infrastructure. This is sufficient for verifying the
/// middleware maintains timer coverage invariants.
#[derive(Clone)]
pub struct MockContext {
    /// Records all timer operations in order.
    operations: Arc<Mutex<Vec<TimerOperation>>>,
}

impl Default for MockContext {
    fn default() -> Self {
        Self::new()
    }
}

impl MockContext {
    /// Create a new mock context for the given topic and partition.
    #[must_use]
    pub fn new() -> Self {
        Self {
            operations: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Get all recorded timer operations.
    #[must_use]
    pub fn operations(&self) -> Vec<TimerOperation> {
        self.operations.lock().clone()
    }

    /// Check if any timer was scheduled.
    #[must_use]
    pub fn has_scheduled_timer(&self) -> bool {
        self.operations.lock().iter().any(|op| {
            matches!(
                op,
                TimerOperation::Schedule(_, _) | TimerOperation::ClearAndSchedule(_, _)
            )
        })
    }

    /// Count scheduled timers of a specific type.
    #[must_use]
    pub fn count_scheduled(&self, timer_type: TimerType) -> usize {
        self.operations
            .lock()
            .iter()
            .filter(|op| match op {
                TimerOperation::Schedule(_, t) | TimerOperation::ClearAndSchedule(_, t) => {
                    *t == timer_type
                }
                _ => false,
            })
            .count()
    }

    /// Clear all recorded operations.
    pub fn clear_operations(&self) {
        self.operations.lock().clear();
    }
}

impl EventContext for MockContext {
    type Error = Infallible;

    fn should_cancel(&self) -> bool {
        false
    }

    fn on_cancel(&self) -> impl Future<Output = ()> + Send + 'static {
        future::pending::<()>()
    }

    fn schedule(
        &self,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.operations
            .lock()
            .push(TimerOperation::Schedule(time, timer_type));
        future::ready(Ok(()))
    }

    fn clear_and_schedule(
        &self,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.operations
            .lock()
            .push(TimerOperation::ClearAndSchedule(time, timer_type));
        future::ready(Ok(()))
    }

    fn unschedule(
        &self,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.operations
            .lock()
            .push(TimerOperation::Unschedule(time, timer_type));
        future::ready(Ok(()))
    }

    fn clear_scheduled(
        &self,
        timer_type: TimerType,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.operations
            .lock()
            .push(TimerOperation::ClearScheduled(timer_type));

        future::ready(Ok(()))
    }

    fn cancel(&self) {
        // No-op for testing
    }

    fn invalidate(self) {
        // No-op for testing - just consume self
    }

    fn scheduled(
        &self,
        _timer_type: TimerType,
    ) -> impl Stream<Item = Result<CompactDateTime, Self::Error>> + Send + 'static {
        stream::empty()
    }
}
