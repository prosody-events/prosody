//! Test module for timer defer handler.
//!
//! Contains integration tests, property-based tests, and test utilities for
//! verifying [`TimerDeferHandler`](super::TimerDeferHandler) behavior.

use crate::consumer::DemandType;
use crate::consumer::event_context::StateAccessError;
use crate::consumer::event_context::{EventContext, TerminationSignals};
use crate::consumer::message::ConsumerMessage;
use crate::consumer::middleware::defer::config::DeferConfiguration;
use crate::consumer::middleware::defer::decider::TraceBasedDecider;
use crate::consumer::middleware::defer::timer::handler::TimerDeferHandler;
use crate::consumer::middleware::defer::timer::store::TimerDeferStore;
use crate::consumer::middleware::defer::timer::store::memory::MemoryTimerDeferStore;
use crate::consumer::middleware::tests::test_support::{HandlerOutcome, OutcomeError, OutcomeSlot};
use crate::consumer::middleware::{FallibleHandler, RepinProof};
use crate::otel::SpanRelation;
use crate::state::descriptor::{Registered, StateDescriptor};
use crate::state::tests::support::UnavailableState;
use crate::telemetry::Telemetry;
use crate::test_util::TEST_RUNTIME;
use crate::timers::datetime::CompactDateTime;
use crate::timers::{TimerType, Trigger};
use crate::{Key, Partition, Topic};
use color_eyre::eyre::eyre;
use parking_lot::Mutex;
use std::convert::Infallible;
use std::fmt::{self, Debug};
use std::future::{Future, pending, ready};
use std::sync::Arc;
use std::time::Duration;
use tracing::span::Id;

mod context;
mod integration;
mod properties;
mod types;

// ============================================================================
// MockContext - Minimal context for tests
// ============================================================================

/// Timer operation recorded by `MockContext`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TimerOperation {
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
struct MockContext {
    operations: Arc<Mutex<Vec<TimerOperation>>>,
}

impl Default for MockContext {
    fn default() -> Self {
        Self::new()
    }
}

impl MockContext {
    #[must_use]
    fn new() -> Self {
        Self {
            operations: Arc::new(Mutex::new(Vec::new())),
        }
    }

    #[must_use]
    fn has_scheduled_timer(&self, timer_type: TimerType) -> bool {
        self.operations.lock().iter().any(|op| {
            matches!(
                op,
                TimerOperation::Schedule(_, t) | TimerOperation::ClearAndSchedule(_, t)
                if *t == timer_type
            )
        })
    }

    fn clear_operations(&self) {
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
    type Payload = serde_json::Value;
    type State = UnavailableState<serde_json::Value>;

    fn state<DESC>(
        &self,
        registered: Registered<DESC>,
    ) -> Result<DESC::Handle<Self::State>, StateAccessError>
    where
        DESC: StateDescriptor,
    {
        registered.descriptor().bind(&UnavailableState::new())
    }

    fn redispatch(&self, _proof: RepinProof) -> Self {
        // Leaf mock over stateless keyed state: nothing to re-pin.
        self.clone()
    }

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

/// `(ambient span id, event span id)` recorded inside one handler call.
type AmbientPair = (Option<Id>, Option<Id>);

/// Handler that returns predetermined outcomes.
#[derive(Clone)]
struct OutcomeHandler {
    outcome: OutcomeSlot,
    timer_calls: Arc<Mutex<Vec<Key>>>,
    /// Pairs observed inside each `on_timer` call — pins that dispatch
    /// entered the trigger's span.
    ambient_pairs: Arc<Mutex<Vec<AmbientPair>>>,
}

impl OutcomeHandler {
    #[must_use]
    fn new() -> Self {
        Self {
            outcome: OutcomeSlot::default(),
            timer_calls: Arc::new(Mutex::new(Vec::new())),
            ambient_pairs: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn set_outcome(&self, outcome: HandlerOutcome) {
        self.outcome.set(outcome);
    }

    #[must_use]
    fn timer_calls(&self) -> Vec<Key> {
        self.timer_calls.lock().clone()
    }

    /// Returns the `(ambient, trigger-span)` id pairs recorded per call.
    #[must_use]
    fn ambient_pairs(&self) -> Vec<AmbientPair> {
        self.ambient_pairs.lock().clone()
    }

    fn take_outcome(&self) -> HandlerOutcome {
        self.outcome.take()
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
            .field("outcome", &self.outcome)
            .finish_non_exhaustive()
    }
}

impl FallibleHandler for OutcomeHandler {
    type Error = OutcomeError;
    type Output = ();
    type Payload = serde_json::Value;

    async fn on_excise<C>(
        &self,
        context: C,
        message: ConsumerMessage<Self::Payload>,
        demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        FallibleHandler::on_message(self, context, message, demand_type).await
    }

    async fn on_message<C>(
        &self,
        _context: C,
        _message: ConsumerMessage<serde_json::Value>,
        _demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        Ok(())
    }

    async fn on_timer<C>(
        &self,
        _context: C,
        trigger: Trigger,
        _demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        self.timer_calls.lock().push(trigger.key.clone());
        self.ambient_pairs
            .lock()
            .push((tracing::Span::current().id(), trigger.span().id()));
        self.take_outcome().into_result()
    }

    async fn shutdown(self) {}
}

// ============================================================================
// TestHarness - Test harness for timer defer handler
// ============================================================================

/// Test harness for executing timer defer tests.
struct TestHarness {
    /// The timer defer handler under test.
    handler: TimerDeferHandler<OutcomeHandler, MemoryTimerDeferStore, TraceBasedDecider>,
    /// Inner handler for setting outcomes (shared via Arc).
    inner_handler: OutcomeHandler,
    /// Decider for setting defer decisions (shared via Arc).
    decider: TraceBasedDecider,
    /// Store for verification (shared via Arc).
    store: MemoryTimerDeferStore,
    /// Context for timer operations.
    context: MockContext,
}

impl TestHarness {
    /// Creates a new test harness with default (enabled) configuration.
    fn new() -> color_eyre::Result<Self> {
        Self::with_enabled(true)
    }

    /// Creates a new test harness with specified enabled state.
    fn with_enabled(enabled: bool) -> color_eyre::Result<Self> {
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
    fn context(&self) -> &MockContext {
        &self.context
    }

    fn trigger_of_type(key: &str, time_secs: u32, timer_type: TimerType) -> Trigger {
        let key: Key = Arc::from(key);
        let time = CompactDateTime::from(time_secs);
        Trigger::new(key, time, timer_type, tracing::Span::current())
    }

    #[must_use]
    fn create_trigger(key: &str, time_secs: u32) -> Trigger {
        Self::trigger_of_type(key, time_secs, TimerType::Application)
    }

    #[must_use]
    fn create_deferred_timer_trigger(key: &str, time_secs: u32) -> Trigger {
        Self::trigger_of_type(key, time_secs, TimerType::DeferredTimer)
    }

    async fn get_retry_count(&self, key: &str) -> color_eyre::Result<Option<u32>> {
        let key: Key = Arc::from(key);
        self.store
            .is_deferred(&key)
            .await
            .map_err(|e| eyre!("store error: {e}"))
    }

    #[must_use]
    fn has_deferred_timer(&self) -> bool {
        self.context.has_scheduled_timer(TimerType::DeferredTimer)
    }
}
/// The timer-defer swallow through the settle boundary: the inner attempt
/// buffers a `cart` write and fails Transient; `defer_first_timer` swallows
/// that error into `Ok(Deferred)` — classified `Bypassed`, so the trigger
/// commits while nothing stages, no marker records, and **no `StateRecovery`
/// backstop arms** (the empty-finalize `Clean`-never-arms parity: arming is
/// possession-driven, and a bypassed dispatch never mints a receipt). A
/// clean success that staged nothing arms nothing either.
mod defer_swallow;
