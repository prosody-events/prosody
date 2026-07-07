//! Test module for timer defer handler.
//!
//! Contains integration tests, property-based tests, and test utilities for
//! verifying [`TimerDeferHandler`](super::TimerDeferHandler) behavior.

use crate::consumer::DemandType;
use crate::consumer::event_context::StateAccessError;
use crate::consumer::event_context::{EventContext, TerminationSignals};
use crate::consumer::message::ConsumerMessage;
use crate::consumer::middleware::FallibleHandler;
use crate::consumer::middleware::defer::config::DeferConfiguration;
use crate::consumer::middleware::defer::decider::TraceBasedDecider;
use crate::consumer::middleware::defer::timer::handler::TimerDeferHandler;
use crate::consumer::middleware::defer::timer::store::TimerDeferStore;
use crate::consumer::middleware::defer::timer::store::memory::MemoryTimerDeferStore;
use crate::consumer::middleware::tests::test_support::{HandlerOutcome, OutcomeError, OutcomeSlot};
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

/// Handler that returns predetermined outcomes.
#[derive(Clone)]
struct OutcomeHandler {
    outcome: OutcomeSlot,
    timer_calls: Arc<Mutex<Vec<Key>>>,
}

impl OutcomeHandler {
    #[must_use]
    fn new() -> Self {
        Self {
            outcome: OutcomeSlot::default(),
            timer_calls: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn set_outcome(&self, outcome: HandlerOutcome) {
        self.outcome.set(outcome);
    }

    #[must_use]
    fn timer_calls(&self) -> Vec<Key> {
        self.timer_calls.lock().clone()
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

/// The first-defer swallow's session reset, end to end through the settle
/// boundary: the inner attempt buffers a `cart` write and registers a dedup
/// marker before failing Transient; `defer_first_timer` swallows that error
/// into `Ok(Deferred)` — so the trigger commits — but only after
/// `reset_state_session` discards the failed attempt's dirty ops and marker.
/// Dropping that reset is silent data loss: the failed attempt's write would
/// commit and its marker would dedup-filter the deferred retry.
mod defer_swallow {
    use super::*;
    use crate::consumer::middleware::FallibleEventHandler;
    use crate::consumer::middleware::defer::decider::AlwaysDefer;
    use crate::consumer::middleware::tests::test_support::{
        MockEventContext, StagingHook, StagingTransientHandler, committed_value, recording_session,
    };
    use crate::consumer::{EventHandler, Keyed, Uncommitted};
    use crate::state::StateKey;
    use crate::state::registry::{CollectionDef, CollectionDefRegistry};
    use crate::timers::UncommittedTimer;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use uuid::Uuid;

    impl FallibleEventHandler
        for TimerDeferHandler<StagingTransientHandler, MemoryTimerDeferStore, AlwaysDefer>
    {
    }

    /// Guard recording whether the trigger committed or aborted.
    struct RecordingGuard {
        committed: Arc<AtomicUsize>,
        aborted: Arc<AtomicUsize>,
    }

    impl Uncommitted for RecordingGuard {
        async fn commit(self) {
            self.committed.fetch_add(1, Ordering::SeqCst);
        }

        async fn abort(self) {
            self.aborted.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// Minimal [`UncommittedTimer`] over a fixed trigger, so the dispatch can
    /// run through `EventHandler::on_timer` and its settle sequence.
    struct RecordingTimer {
        trigger: Trigger,
        committed: Arc<AtomicUsize>,
        aborted: Arc<AtomicUsize>,
    }

    impl Keyed for RecordingTimer {
        type Key = Key;

        fn key(&self) -> &Self::Key {
            &self.trigger.key
        }
    }

    impl Uncommitted for RecordingTimer {
        async fn commit(self) {
            self.committed.fetch_add(1, Ordering::SeqCst);
        }

        async fn abort(self) {
            self.aborted.fetch_add(1, Ordering::SeqCst);
        }
    }

    impl UncommittedTimer for RecordingTimer {
        type CommitGuard = RecordingGuard;

        fn time(&self) -> CompactDateTime {
            self.trigger.time
        }

        fn timer_type(&self) -> TimerType {
            self.trigger.timer_type
        }

        fn span(&self) -> tracing::Span {
            tracing::Span::current()
        }

        fn into_inner(self) -> (Trigger, Self::CommitGuard) {
            let guard = RecordingGuard {
                committed: self.committed.clone(),
                aborted: self.aborted.clone(),
            };
            (self.trigger, guard)
        }
    }

    #[tokio::test]
    async fn first_defer_swallow_resets_the_state_session() -> color_eyre::Result<()> {
        const MARKER: Uuid = Uuid::from_u128(0xDEF2);
        let topic = Topic::from("test-topic");
        let partition = Partition::from(0_i32);

        let mut registry = CollectionDefRegistry::default();
        registry.register(
            &StagingTransientHandler::collection(),
            CollectionDef::new(None),
        )?;
        let state_key = StateKey::new(Uuid::from_u128(0xD2), Arc::from("user-1"));
        let (session, cell_store, dirty, recorded) = recording_session(registry, state_key.clone());

        let inner = StagingTransientHandler::new(MARKER);
        let store = MemoryTimerDeferStore::new(SpanRelation::default());
        let telemetry = Telemetry::new();
        let handler = TimerDeferHandler {
            handler: inner.clone(),
            store: store.clone(),
            decider: AlwaysDefer,
            config: DeferConfiguration::builder()
                .enabled(true)
                .base(Duration::from_secs(1))
                .max_delay(Duration::from_hours(1))
                .failure_threshold(0.9_f64)
                .build()
                .map_err(|e| eyre!("config error: {e}"))?,
            topic,
            partition,
            sender: telemetry.partition_sender(topic, partition),
            source: Arc::from("test"),
        };

        let context = MockEventContext::new().with_session(session);
        let key: Key = Arc::from("user-1");
        let committed = Arc::new(AtomicUsize::new(0));
        let aborted = Arc::new(AtomicUsize::new(0));
        let timer = RecordingTimer {
            trigger: Trigger::new(
                key.clone(),
                CompactDateTime::from(1000_u32),
                TimerType::Application,
                tracing::Span::current(),
            ),
            committed: committed.clone(),
            aborted: aborted.clone(),
        };

        EventHandler::on_timer(&handler, context, timer, DemandType::Normal).await;

        // Positive control: the swallow path ran — the inner attempt was
        // rolled back into a deferred retry, not surfaced as a final error.
        assert_eq!(inner.hooks(), vec![StagingHook::Abort]);
        assert_eq!(
            store.is_deferred(&key).await?,
            Some(0),
            "the timer must be deferred for timer-based retry",
        );
        // The reset's contract: nothing from the failed attempt survives.
        assert_eq!(
            committed_value(&cell_store, state_key, "cart").await?,
            None,
            "the failed attempt's buffered write must not commit",
        );
        assert!(
            dirty.touched(&key).is_empty(),
            "the session's dirty buffer must be empty after the swallow",
        );
        assert!(
            recorded.lock().is_empty(),
            "the failed attempt's marker must not flush — the deferred retry must not be \
             dedup-filtered",
        );
        assert_eq!(committed.load(Ordering::SeqCst), 1, "the trigger commits");
        assert_eq!(
            aborted.load(Ordering::SeqCst),
            0,
            "the trigger never aborts"
        );
        Ok(())
    }
}
