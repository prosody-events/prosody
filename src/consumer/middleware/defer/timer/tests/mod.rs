//! Test module for timer defer handler.
//!
//! Contains integration tests, property-based tests, and test utilities for
//! verifying [`TimerDeferHandler`](super::TimerDeferHandler) behavior.

use crate::consumer::DemandType;
use crate::consumer::event_context::{EventContext, TerminationSignals};
use crate::consumer::message::ConsumerMessage;
use crate::consumer::middleware::FallibleHandler;
use crate::consumer::middleware::defer::config::DeferConfiguration;
use crate::consumer::middleware::defer::decider::TraceBasedDecider;
use crate::consumer::middleware::defer::timer::handler::TimerDeferHandler;
use crate::consumer::middleware::defer::timer::store::TimerDeferStore;
use crate::consumer::middleware::defer::timer::store::memory::MemoryTimerDeferStore;
use crate::error::{ClassifyError, ErrorCategory};
use crate::otel::SpanRelation;
use crate::telemetry::Telemetry;
use crate::test_util::TEST_RUNTIME;
use crate::timers::datetime::CompactDateTime;
use crate::timers::{TimerType, Trigger};
use crate::{Key, Partition, Topic};
use color_eyre::eyre::eyre;
use parking_lot::Mutex;
use std::convert::Infallible;
use std::error::Error;
use std::fmt::{self, Debug, Display};
use std::future::{Future, pending, ready};
use std::sync::Arc;
use std::time::Duration;

mod context;
mod integration;
mod properties;
pub mod types;

// ============================================================================
// MockContext - Minimal context for tests
// ============================================================================

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

/// Minimal mock context for tests.
#[derive(Clone)]
pub struct MockContext {
    operations: Arc<Mutex<Vec<TimerOperation>>>,
}

impl Default for MockContext {
    fn default() -> Self {
        Self::new()
    }
}

impl MockContext {
    #[must_use]
    pub fn new() -> Self {
        Self {
            operations: Arc::new(Mutex::new(Vec::new())),
        }
    }

    #[must_use]
    pub fn has_scheduled_timer(&self, timer_type: TimerType) -> bool {
        self.operations.lock().iter().any(|op| {
            matches!(
                op,
                TimerOperation::Schedule(_, t) | TimerOperation::ClearAndSchedule(_, t)
                if *t == timer_type
            )
        })
    }

    pub fn clear_operations(&self) {
        self.operations.lock().clear();
    }
}

impl TerminationSignals for MockContext {
    fn is_shutdown(&self) -> bool {
        false
    }

    fn is_message_cancelled(&self) -> bool {
        false
    }

    fn on_shutdown(&self) -> impl Future<Output = ()> + Send + 'static {
        pending::<()>()
    }

    fn on_message_cancelled(&self) -> impl Future<Output = ()> + Send + 'static {
        pending::<()>()
    }
}

impl EventContext for MockContext {
    type Error = Infallible;

    fn should_cancel(&self) -> bool {
        false
    }

    fn on_cancel(&self) -> impl Future<Output = ()> + Send + 'static {
        pending::<()>()
    }

    fn schedule(
        &self,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.operations
            .lock()
            .push(TimerOperation::Schedule(time, timer_type));
        ready(Ok(()))
    }

    fn clear_and_schedule(
        &self,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.operations
            .lock()
            .push(TimerOperation::ClearAndSchedule(time, timer_type));
        ready(Ok(()))
    }

    fn unschedule(
        &self,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.operations
            .lock()
            .push(TimerOperation::Unschedule(time, timer_type));
        ready(Ok(()))
    }

    fn clear_scheduled(
        &self,
        timer_type: TimerType,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.operations
            .lock()
            .push(TimerOperation::ClearScheduled(timer_type));
        ready(Ok(()))
    }

    fn cancel(&self) {
        // No-op for testing
    }

    fn uncancel(&self) {
        // No-op for testing
    }

    fn invalidate(self) {
        // No-op for testing
    }

    fn scheduled(
        &self,
        _timer_type: TimerType,
    ) -> impl Future<Output = Result<Vec<CompactDateTime>, Self::Error>> + Send + 'static {
        ready(Ok(Vec::new()))
    }
}

// ============================================================================
// OutcomeHandler - Mock handler for tests
// ============================================================================

/// Outcome that the handler should return.
#[derive(Clone, Debug)]
pub enum HandlerOutcome {
    /// Handler succeeds.
    Success,
    /// Handler fails with a permanent error.
    Permanent,
    /// Handler fails with a transient error.
    Transient,
}

/// Error returned by [`OutcomeHandler`].
#[derive(Clone)]
pub struct OutcomeError {
    category: ErrorCategory,
}

impl OutcomeError {
    #[must_use]
    pub fn permanent() -> Self {
        Self {
            category: ErrorCategory::Permanent,
        }
    }

    #[must_use]
    pub fn transient() -> Self {
        Self {
            category: ErrorCategory::Transient,
        }
    }
}

impl Debug for OutcomeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OutcomeError")
            .field("category", &self.category)
            .finish()
    }
}

impl Display for OutcomeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.category {
            ErrorCategory::Permanent => write!(f, "permanent test error"),
            ErrorCategory::Transient => write!(f, "transient test error"),
            ErrorCategory::Terminal => write!(f, "terminal test error"),
        }
    }
}

impl Error for OutcomeError {}

impl ClassifyError for OutcomeError {
    fn classify_error(&self) -> ErrorCategory {
        self.category
    }
}

/// Handler that returns predetermined outcomes.
#[derive(Clone)]
pub struct OutcomeHandler {
    next_outcome: Arc<Mutex<Option<HandlerOutcome>>>,
    timer_calls: Arc<Mutex<Vec<Key>>>,
}

impl OutcomeHandler {
    #[must_use]
    pub fn new() -> Self {
        Self {
            next_outcome: Arc::new(Mutex::new(None)),
            timer_calls: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn set_outcome(&self, outcome: HandlerOutcome) {
        *self.next_outcome.lock() = Some(outcome);
    }

    #[must_use]
    pub fn timer_calls(&self) -> Vec<Key> {
        self.timer_calls.lock().clone()
    }

    fn take_outcome(&self) -> HandlerOutcome {
        self.next_outcome
            .lock()
            .take()
            .unwrap_or(HandlerOutcome::Success)
    }
}

impl Default for OutcomeHandler {
    fn default() -> Self {
        Self::new()
    }
}

impl Debug for OutcomeHandler {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OutcomeHandler")
            .field("next_outcome", &self.next_outcome.lock())
            .finish_non_exhaustive()
    }
}

impl FallibleHandler for OutcomeHandler {
    type Error = OutcomeError;

    async fn on_message<C>(
        &self,
        _context: C,
        _message: ConsumerMessage,
        _demand_type: DemandType,
    ) -> Result<(), Self::Error>
    where
        C: EventContext,
    {
        Ok(())
    }

    async fn on_timer<C>(
        &self,
        _context: C,
        trigger: Trigger,
        _demand_type: DemandType,
    ) -> Result<(), Self::Error>
    where
        C: EventContext,
    {
        self.timer_calls.lock().push(trigger.key.clone());

        match self.take_outcome() {
            HandlerOutcome::Success => Ok(()),
            HandlerOutcome::Permanent => Err(OutcomeError::permanent()),
            HandlerOutcome::Transient => Err(OutcomeError::transient()),
        }
    }

    async fn shutdown(self) {}
}

// ============================================================================
// TestHarness - Test harness for timer defer handler
// ============================================================================

/// Test harness for executing timer defer tests.
pub struct TestHarness {
    /// The timer defer handler under test.
    pub handler: TimerDeferHandler<OutcomeHandler, MemoryTimerDeferStore, TraceBasedDecider>,
    /// Inner handler for setting outcomes (shared via Arc).
    pub inner_handler: OutcomeHandler,
    /// Decider for setting defer decisions (shared via Arc).
    pub decider: TraceBasedDecider,
    /// Store for verification (shared via Arc).
    store: MemoryTimerDeferStore,
    /// Context for timer operations.
    context: MockContext,
}

impl TestHarness {
    /// Creates a new test harness with default (enabled) configuration.
    pub fn new() -> color_eyre::Result<Self> {
        Self::with_enabled(true)
    }

    /// Creates a new test harness with specified enabled state.
    pub fn with_enabled(enabled: bool) -> color_eyre::Result<Self> {
        let topic = Topic::from("test-topic");
        let partition = Partition::from(0_i32);

        let inner_handler = OutcomeHandler::new();
        let decider = TraceBasedDecider::new();
        let store = MemoryTimerDeferStore::new(SpanRelation::default());
        let context = MockContext::new();

        let config = DeferConfiguration::builder()
            .enabled(enabled)
            .base(Duration::from_secs(1))
            .max_delay(Duration::from_hours(1))
            .failure_threshold(0.9_f64)
            .build()
            .map_err(|e| eyre!("config error: {e}"))?;

        let telemetry = Telemetry::new();
        let sender = telemetry.partition_sender(topic, partition);

        let handler = TimerDeferHandler {
            handler: inner_handler.clone(),
            store: store.clone(),
            decider: decider.clone(),
            config,
            topic,
            partition,
            sender,
            source: Arc::from("test"),
        };

        Ok(Self {
            handler,
            inner_handler,
            decider,
            store,
            context,
        })
    }

    #[must_use]
    pub fn context(&self) -> &MockContext {
        &self.context
    }

    #[must_use]
    pub fn create_trigger(key: &str, time_secs: u32) -> Trigger {
        let key: Key = Arc::from(key);
        let time = CompactDateTime::from(time_secs);
        Trigger::new(key, time, TimerType::Application, tracing::Span::current())
    }

    #[must_use]
    pub fn create_deferred_timer_trigger(key: &str, time_secs: u32) -> Trigger {
        let key: Key = Arc::from(key);
        let time = CompactDateTime::from(time_secs);
        Trigger::new(
            key,
            time,
            TimerType::DeferredTimer,
            tracing::Span::current(),
        )
    }

    pub async fn get_retry_count(&self, key: &str) -> color_eyre::Result<Option<u32>> {
        let key: Key = Arc::from(key);
        self.store
            .is_deferred(&key)
            .await
            .map_err(|e| eyre!("store error: {e}"))
    }

    #[must_use]
    pub fn has_deferred_timer(&self) -> bool {
        self.context.has_scheduled_timer(TimerType::DeferredTimer)
    }
}
