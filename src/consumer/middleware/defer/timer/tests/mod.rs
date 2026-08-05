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
mod defer_swallow {
    use super::*;
    use crate::consumer::EventHandler;
    use crate::consumer::middleware::defer::decider::AlwaysDefer;
    use crate::consumer::middleware::defer::error::DeferError;
    use crate::consumer::middleware::defer::timer::handler::TimerDeferOutput;
    use crate::consumer::middleware::tests::test_support::{
        BypassedHandler, MockEventContext, RecordingTimer, ScriptedHandler, StagingHook,
        StagingTransientHandler, TestError, committed_json_value, recording_session,
    };
    use crate::consumer::middleware::{FallibleEventHandler, Settlement, SettlementHandler};
    use crate::error::ErrorCategory;
    use crate::loader::KafkaLoaderError;
    use crate::state::registry::{CollectionDef, CollectionDefRegistry};
    use crate::state::{EventRef, StateKey, TimerEventRef};
    use std::sync::atomic::Ordering;
    use uuid::Uuid;

    impl FallibleEventHandler
        for TimerDeferHandler<StagingTransientHandler, MemoryTimerDeferStore, AlwaysDefer>
    {
    }

    impl FallibleEventHandler
        for TimerDeferHandler<ScriptedHandler, MemoryTimerDeferStore, AlwaysDefer>
    {
    }

    fn timer_event() -> EventRef {
        EventRef::Timer(TimerEventRef::new(
            TimerType::Application,
            CompactDateTime::from(1000_u32),
            0,
        ))
    }

    fn defer_handler<T>(
        inner: T,
        store: MemoryTimerDeferStore,
        topic: Topic,
        partition: Partition,
    ) -> color_eyre::Result<TimerDeferHandler<T, MemoryTimerDeferStore, AlwaysDefer>> {
        let telemetry = Telemetry::new();
        Ok(TimerDeferHandler {
            handler: inner,
            store,
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
        })
    }

    /// Part A — the swallow: a staged-then-transient inner attempt swallowed
    /// into `Ok(Deferred)` arms NO backstop, stages nothing, records nothing,
    /// and commits the trigger; the dirty residue dies with the scope drop.
    /// Part B — the parity control: a clean success that staged nothing arms
    /// nothing either (`Finalized::Clean` never arms).
    #[tokio::test]
    async fn defer_swallow_arms_no_backstop_and_stages_nothing() -> color_eyre::Result<()> {
        use crate::state::manager::EventStateScope;

        let topic = Topic::from("test-topic");
        let partition = Partition::from(0_i32);
        let key: Key = Arc::from("user-1");

        // Part A: the swallow.
        let mut registry = CollectionDefRegistry::default();
        registry.register(
            &StagingTransientHandler::collection(),
            CollectionDef::new(None),
        )?;
        let state_key = StateKey::new(Uuid::from_u128(0xD2), Arc::from("user-1"));
        let (session, cell_store, dirty, recorded) =
            recording_session(registry, state_key.clone(), timer_event());
        let scope = EventStateScope::new(session);

        let inner = StagingTransientHandler::new();
        let store = MemoryTimerDeferStore::new(SpanRelation::default());
        let handler = defer_handler(inner.clone(), store.clone(), topic, partition)?;

        let context = MockEventContext::new()
            .with_session(scope.handle())
            .with_timer_tracking();
        let (timer, committed, aborted) = RecordingTimer::new(Trigger::new(
            key.clone(),
            CompactDateTime::from(1000_u32),
            TimerType::Application,
            tracing::Span::current(),
        ));

        EventHandler::on_timer(&handler, context.clone(), timer, DemandType::Normal).await;

        // Positive control: the swallow path ran — the inner attempt was
        // rolled back into a deferred retry, not surfaced as a final error.
        assert_eq!(inner.hooks(), vec![StagingHook::Abort]);
        assert_eq!(
            store.is_deferred(&key).await?,
            Some(0),
            "the timer must be deferred for timer-based retry",
        );
        // The Bypassed contract: nothing from the failed attempt settles.
        assert_eq!(
            context.count_scheduled(TimerType::StateRecovery),
            0,
            "a bypassed dispatch must arm NO StateRecovery backstop (Clean-never-arms parity)",
        );
        assert_eq!(
            committed_json_value(&cell_store, state_key, "cart").await?,
            None,
            "the failed attempt's buffered write must not commit",
        );
        assert!(
            recorded.lock().is_empty(),
            "the swallowed attempt must record no marker",
        );
        assert_eq!(committed.load(Ordering::SeqCst), 1, "the trigger commits");
        assert_eq!(
            aborted.load(Ordering::SeqCst),
            0,
            "the trigger never aborts"
        );
        drop(scope);
        assert!(
            dirty.touched(&key).is_empty(),
            "the scope drop sweeps the swallowed attempt's dirty residue",
        );

        // Part B: a clean success that stages nothing arms nothing.
        let (clean_session, ..) = recording_session(
            CollectionDefRegistry::default(),
            StateKey::new(Uuid::from_u128(0xD3), Arc::from("user-2")),
            timer_event(),
        );
        let clean_scope = EventStateScope::new(clean_session);
        let clean_context = MockEventContext::new()
            .with_session(clean_scope.handle())
            .with_timer_tracking();
        let clean_handler = defer_handler(
            ScriptedHandler::success(),
            MemoryTimerDeferStore::new(SpanRelation::default()),
            topic,
            partition,
        )?;
        let (clean_timer, clean_committed, _) = RecordingTimer::new(Trigger::new(
            Arc::from("user-2"),
            CompactDateTime::from(1000_u32),
            TimerType::Application,
            tracing::Span::current(),
        ));
        EventHandler::on_timer(
            &clean_handler,
            clean_context.clone(),
            clean_timer,
            DemandType::Normal,
        )
        .await;
        assert_eq!(
            clean_context.count_scheduled(TimerType::StateRecovery),
            0,
            "a clean (nothing-staged) success arms no backstop",
        );
        assert_eq!(clean_committed.load(Ordering::SeqCst), 1);
        Ok(())
    }

    /// Store double whose `Error` is constructible (the memory store's is
    /// `Infallible`), so the `DeferError::Store` row exists. `settlement()`
    /// never runs a store method.
    #[derive(Clone)]
    struct TableStore;

    impl TimerDeferStore for TableStore {
        type Error = TestError;

        async fn defer_first_timer(&self, _trigger: &Trigger) -> Result<(), TestError> {
            Ok(())
        }

        async fn get_next_deferred_timer(
            &self,
            _key: &Key,
        ) -> Result<Option<(Trigger, u32)>, TestError> {
            Ok(None)
        }

        async fn append_deferred_timer(&self, _trigger: &Trigger) -> Result<(), TestError> {
            Ok(())
        }

        fn deferred_times(
            &self,
            _key: &Key,
        ) -> impl Future<Output = Result<Vec<CompactDateTime>, TestError>> + Send + 'static
        {
            ready(Ok(Vec::new()))
        }

        async fn remove_deferred_timer(
            &self,
            _key: &Key,
            _time: CompactDateTime,
        ) -> Result<(), TestError> {
            Ok(())
        }

        async fn set_retry_count(&self, _key: &Key, _retry_count: u32) -> Result<(), TestError> {
            Ok(())
        }

        async fn delete_key(&self, _key: &Key) -> Result<(), TestError> {
            Ok(())
        }
    }

    type TableOut = TimerDeferOutput<(), TestError>;
    type TableErr = DeferError<TestError, TestError>;

    /// The settlement classification table for the timer-defer wrapper:
    /// every Output and error variant, over a `Final` leaf so delegation is
    /// observable against the `Bypassed` rows. The `Inner`/`Handler`
    /// delegation is proven separately in [`settlement_table_delegates`].
    #[test]
    fn settlement_classification_table() {
        use crate::timers::datetime::CompactDateTimeError;

        type Subject = TimerDeferHandler<ScriptedHandler, TableStore, AlwaysDefer>;
        type Out = TableOut;

        let rows: Vec<(&str, Result<Out, TableErr>, Settlement)> = vec![
            (
                "Inner delegates to the leaf's Final",
                Ok(TimerDeferOutput::Inner(())),
                Settlement::Final,
            ),
            (
                "Deferred is Bypassed (parked for retry)",
                Ok(TimerDeferOutput::Deferred(TestError(
                    ErrorCategory::Transient,
                ))),
                Settlement::Bypassed,
            ),
            (
                "NoInner is Bypassed (queued behind / orphan cleanup)",
                Ok(TimerDeferOutput::NoInner),
                Settlement::Bypassed,
            ),
            (
                "Handler delegates to the leaf's Final",
                Err(DeferError::Handler(TestError(ErrorCategory::Permanent))),
                Settlement::Final,
            ),
            (
                "Store rescue failure is Bypassed",
                Err(DeferError::Store(TestError(ErrorCategory::Transient))),
                Settlement::Bypassed,
            ),
            (
                "Timer rescue failure is Bypassed",
                Err(DeferError::Timer(Box::new(TestError(
                    ErrorCategory::Transient,
                )))),
                Settlement::Bypassed,
            ),
            (
                "Loader rescue failure is Bypassed",
                Err(DeferError::Loader(KafkaLoaderError::LoaderShutdown)),
                Settlement::Bypassed,
            ),
            (
                "CompactTime (backoff computation, Permanent) is Bypassed",
                Err(DeferError::CompactTime(CompactDateTimeError::OutOfRange)),
                Settlement::Bypassed,
            ),
        ];
        for (label, result, expected) in rows {
            assert_eq!(Subject::settlement(result.as_ref()), expected, "{label}");
        }
    }

    /// Delegation proof for the timer-defer wrapper: over a
    /// `Bypassed`-classifying probe leaf, the delegating rows (`Inner`,
    /// `Handler`) stay `Bypassed` — a wrapper hardcoding `Final` on them
    /// fails this test.
    #[test]
    fn settlement_table_delegates() {
        type Probe = TimerDeferHandler<BypassedHandler, TableStore, AlwaysDefer>;

        let inner: Result<TableOut, TableErr> = Ok(TimerDeferOutput::Inner(()));
        let handler: Result<TableOut, TableErr> =
            Err(DeferError::Handler(TestError(ErrorCategory::Permanent)));
        assert_eq!(Probe::settlement(inner.as_ref()), Settlement::Bypassed);
        assert_eq!(Probe::settlement(handler.as_ref()), Settlement::Bypassed);
    }
}
