//! Mock handler for trace-based property testing.
//!
//! Provides [`OutcomeHandler`] that implements [`FallibleHandler`] and returns
//! outcomes specified by the test trace. This allows property tests to control
//! exactly how the inner handler behaves for each event.
//!
//! The apply-hook invariant (exactly one `after_commit`/`after_abort` per
//! dispatch) is proven at the real `settle` boundary by
//! `consumer::middleware::tests`, not from this mock.

use crate::consumer::DemandType;
use crate::consumer::event_context::EventContext;
use crate::consumer::message::ConsumerMessage;
use crate::consumer::middleware::FallibleHandler;
use crate::consumer::middleware::tests::test_support::{MockEventContext, OutcomeSlot};
use crate::timers::Trigger;
use crate::{Key, Offset};
use parking_lot::Mutex;
use std::fmt::{self, Debug};
use std::sync::Arc;
use tracing::span::Id;

pub use crate::consumer::middleware::tests::test_support::{HandlerOutcome, OutcomeError};

/// A processed message record: (key, offset).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProcessedMessage {
    /// The key of the processed message.
    pub key: Key,
    /// The offset of the processed message.
    pub offset: Offset,
}

// ============================================================================
// OutcomeHandler - Mock handler returning trace-specified outcomes
// ============================================================================

/// `(ambient span id, event span id)` recorded inside one handler call.
type AmbientPair = (Option<Id>, Option<Id>);

/// Handler that returns predetermined outcomes from the trace.
///
/// The test harness sets the next outcome before each event using
/// [`OutcomeHandler::set_outcome`]. When the defer middleware calls
/// `on_message()` or `on_timer()`, this handler returns the preset outcome.
#[derive(Clone)]
pub struct OutcomeHandler {
    /// Next outcome to return (set by harness before each event).
    outcome: OutcomeSlot,
    /// Record of all messages processed by this handler (in order).
    processed: Arc<scc::Queue<ProcessedMessage>>,
    /// When set, triggers partition shutdown before returning the outcome.
    ///
    /// Used to simulate shutdown occurring mid-handler-execution, exercising
    /// post-call cancellation promotion paths.
    shutdown_trigger: Arc<Mutex<Option<MockEventContext>>>,
    /// Pairs observed inside each `on_message` call — pins that dispatch
    /// entered the message's span.
    ambient_pairs: Arc<Mutex<Vec<AmbientPair>>>,
}

impl OutcomeHandler {
    /// Creates a new handler with no preset outcome.
    #[must_use]
    pub fn new() -> Self {
        Self {
            outcome: OutcomeSlot::default(),
            processed: Arc::new(scc::Queue::default()),
            shutdown_trigger: Arc::new(Mutex::new(None)),
            ambient_pairs: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Sets the outcome for the next handler call.
    ///
    /// Must be called before each `on_message()` or `on_timer()` invocation.
    pub fn set_outcome(&self, outcome: HandlerOutcome) {
        self.outcome.set(outcome);
    }

    /// Configures shutdown to be signaled when this handler is next called.
    ///
    /// The provided context shares its shutdown channel with any clones, so
    /// contexts passed to outer middleware layers will also observe shutdown
    /// after the handler fires.
    pub fn set_shutdown_trigger(&self, ctx: MockEventContext) {
        *self.shutdown_trigger.lock() = Some(ctx);
    }

    /// Fires the shutdown trigger if one is configured.
    fn maybe_trigger_shutdown(&self) {
        if let Some(ctx) = self.shutdown_trigger.lock().as_ref() {
            ctx.request_shutdown();
        }
    }

    /// Returns all processed messages in order (drains the queue).
    #[must_use]
    pub fn processed(&self) -> Vec<ProcessedMessage> {
        let mut result = Vec::with_capacity(self.processed.len());
        while let Some(entry) = self.processed.pop() {
            result.push((**entry).clone());
        }
        result
    }

    /// Records a processed message.
    fn record_processed(&self, key: Key, offset: Offset) {
        self.processed.push(ProcessedMessage { key, offset });
    }

    /// Returns the `(ambient, message-span)` id pairs recorded per call.
    #[must_use]
    pub fn ambient_pairs(&self) -> Vec<AmbientPair> {
        self.ambient_pairs.lock().clone()
    }

    /// Takes the next outcome, returning Success if none was set.
    ///
    /// This is called internally by `on_message()` and `on_timer()`.
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
            .field("processed_count", &self.processed.len())
            .field("shutdown_trigger", &self.shutdown_trigger.lock().is_some())
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
        message: ConsumerMessage<serde_json::Value>,
        _demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        use crate::consumer::Keyed;
        let key = message.key().clone();
        let offset = message.offset();
        let outcome = self.take_outcome();
        tracing::info!(
            "OutcomeHandler.on_message: key={:?}, offset={}, outcome={:?}",
            key,
            offset,
            outcome
        );

        // Record this message as processed (for order verification)
        self.record_processed(key, offset);
        self.ambient_pairs
            .lock()
            .push((tracing::Span::current().id(), message.span().id()));

        self.maybe_trigger_shutdown();
        outcome.into_result()
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
        let outcome = self.take_outcome();
        tracing::debug!(
            "OutcomeHandler.on_timer: key={:?}, outcome={:?}",
            trigger.key,
            outcome
        );
        self.maybe_trigger_shutdown();
        outcome.into_result()
    }

    async fn shutdown(self) {
        // No-op for test handler
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Key;
    use crate::consumer::middleware::defer::message::handler::tests::{
        MockEventContext, TEST_RUNTIME,
    };
    use crate::error::{ClassifyError, ErrorCategory};
    use crate::timers::TimerType;
    use crate::timers::datetime::CompactDateTime;
    use crate::tracing::init_test_logging;

    fn make_test_trigger(key_name: &str) -> Trigger {
        let key: Key = Arc::from(key_name);
        let time = CompactDateTime::from(1000_u32);
        Trigger::for_testing(key, time, TimerType::DeferredMessage)
    }

    #[test]
    fn outcome_handler_returns_configured_outcome() {
        init_test_logging();

        TEST_RUNTIME.block_on(async {
            for (outcome, expected_category) in [
                (HandlerOutcome::Success, None),
                (HandlerOutcome::Permanent, Some(ErrorCategory::Permanent)),
                (HandlerOutcome::Transient, Some(ErrorCategory::Transient)),
            ] {
                let handler = OutcomeHandler::new();
                let context = MockEventContext::new();
                let trigger = make_test_trigger("test-key");

                handler.set_outcome(outcome);
                let result = handler.on_timer(context, trigger, DemandType::Normal).await;
                assert_eq!(
                    result.as_ref().err().map(OutcomeError::classify_error),
                    expected_category
                );
            }
        });
    }

    #[test]
    fn outcome_handler_is_clone_safe() {
        init_test_logging();

        let handler1 = OutcomeHandler::new();
        let handler2 = handler1.clone();

        // Both should share state
        handler1.set_outcome(HandlerOutcome::Permanent);

        // handler2 should see the outcome set by handler1
        // (they share Arc<Mutex>)
        let outcome = handler2.take_outcome();
        assert!(matches!(outcome, HandlerOutcome::Permanent));
    }

    #[test]
    fn outcome_handler_defaults_to_success_when_not_set() {
        init_test_logging();

        let handler = OutcomeHandler::new();

        // When no outcome is set, take_outcome should return Success
        let outcome = handler.take_outcome();
        assert!(matches!(outcome, HandlerOutcome::Success));
    }
}

/// The settlement pins for the message-defer seams, each driven end to end
/// through the real settle boundary (the blanket `EventHandler` impl, or
/// retry's for the last-wins pin):
///
/// - a defer swallow (`Ok(Deferred)`) records **no** marker and stages
///   **nothing**, so the deferred reload re-runs unfiltered;
/// - a deferred reload records the **reloaded message's** marker while staging
///   under the timer's `EventRef`, and a redelivery of that message filters;
/// - a reload that fails permanently records the reloaded id;
/// - a retry re-dispatch that loads a **different** queue head records under
///   the new head's id (the last-wins override).
#[cfg(test)]
mod settlement_pins {
    use super::*;
    use crate::consumer::EventHandler;
    use crate::consumer::message::{ConsumerMessageValue, UncommittedMessage};
    use crate::consumer::middleware::deduplication::{
        DeduplicationHandler, DeduplicationStore, MemoryDeduplicationStore, dedup_uuid,
    };
    use crate::consumer::middleware::defer::config::DeferConfiguration;
    use crate::consumer::middleware::defer::decider::AlwaysDefer;
    use crate::consumer::middleware::defer::error::DeferError;
    use crate::consumer::middleware::defer::message::handler::{
        MessageDeferHandler, MessageDeferOutput,
    };
    use crate::consumer::middleware::defer::message::store::MessageDeferStore;
    use crate::consumer::middleware::defer::message::store::memory::MemoryMessageDeferStore;
    use crate::consumer::middleware::providers::FallibleCloneProvider;
    use crate::consumer::middleware::retry::{RetryConfiguration, RetryMiddleware};
    use crate::consumer::middleware::tests::test_support::{
        RecordingOracle, RecordingSession, RecordingTimer, StagingError, committed_json_value,
        recording_session_with_loader,
    };
    use crate::consumer::middleware::{
        ErrorCategory, FallibleEventHandler, FallibleHandlerProvider, HandlerMiddleware,
        Settlement, SettlementHandler,
    };
    use crate::consumer::partition::offsets::OffsetTracker;
    use crate::loader::{MemoryLoader, MemoryLoaderError};
    use crate::state::descriptor::{Registered, ValueDescriptor, value_state};
    use crate::state::dirty::DirtyStore;
    use crate::state::manager::EventStateScope;
    use crate::state::memory::MemoryCellStore;
    use crate::state::registry::{CollectionDef, CollectionDefRegistry};
    use crate::state::{EventRef, StateKey, TimerEventRef};
    use crate::telemetry::Telemetry;
    use crate::timers::TimerType;
    use crate::timers::datetime::CompactDateTime;
    use crate::{Offset, Partition, Topic};
    use color_eyre::Result;
    use crossbeam_utils::CachePadded;
    use parking_lot::Mutex;
    use serde_json::{Value, json};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use tokio::sync::Semaphore;
    use uuid::Uuid;

    /// What [`Fixture::session`] hands back: the session, its durable cell
    /// store, the shared dirty store, and the oracle's recorded-marker log.
    type SessionParts = (
        RecordingSession,
        MemoryCellStore<RecordingOracle>,
        Arc<DirtyStore>,
        Arc<Mutex<Vec<Uuid>>>,
    );

    const TOPIC: &str = "test-topic";
    const PARTITION: i32 = 0;
    const GROUP: &str = "test";
    const VERSION: &str = "1";
    const KEY: &str = "user-1";

    /// Leaf for the pins: on every `on_message` it buffers one `cart` write,
    /// records the message's offset, and returns the next scripted outcome
    /// (default success). `Final` settlement, like the production leaf
    /// adapter.
    #[derive(Clone, Default)]
    struct StagingLeaf {
        outcomes: Arc<Mutex<Vec<ErrorCategory>>>,
        processed: Arc<Mutex<Vec<Offset>>>,
    }

    impl StagingLeaf {
        fn collection() -> ValueDescriptor {
            value_state("cart")
        }

        /// Queue a failure for the next dispatch (front-first).
        fn fail_next(&self, category: ErrorCategory) {
            self.outcomes.lock().push(category);
        }

        fn processed(&self) -> Vec<Offset> {
            self.processed.lock().clone()
        }
    }

    impl FallibleHandler for StagingLeaf {
        type Error = StagingError;
        type Output = ();
        type Payload = Value;

        async fn on_message<C>(
            &self,
            context: C,
            message: ConsumerMessage<Self::Payload>,
            _demand_type: DemandType,
        ) -> Result<Self::Output, Self::Error>
        where
            C: EventContext<Payload = Self::Payload>,
        {
            self.processed.lock().push(message.offset());
            let handle = context
                .state(Registered::new(Self::collection()))
                .map_err(|_| StagingError(ErrorCategory::Terminal))?;
            handle
                .set(json!({ "offset": message.offset() }))
                .await
                .map_err(|_| StagingError(ErrorCategory::Terminal))?;
            let mut outcomes = self.outcomes.lock();
            if outcomes.is_empty() {
                Ok(())
            } else {
                Err(StagingError(outcomes.remove(0)))
            }
        }

        async fn on_timer<C>(
            &self,
            _context: C,
            _trigger: Trigger,
            _demand_type: DemandType,
        ) -> Result<Self::Output, Self::Error>
        where
            C: EventContext<Payload = Self::Payload>,
        {
            Ok(())
        }

        async fn shutdown(self) {}
    }

    impl SettlementHandler for StagingLeaf {
        fn settlement(_result: Result<&Self::Output, &Self::Error>) -> Settlement {
            Settlement::Final
        }
    }

    type PinStack = MessageDeferHandler<
        DeduplicationHandler<StagingLeaf, MemoryDeduplicationStore>,
        MemoryMessageDeferStore,
        MemoryLoader<Value>,
        AlwaysDefer,
    >;

    impl FallibleEventHandler for PinStack {}

    /// The pin fixture: the composed defer→dedup→leaf stack over a shared
    /// loader and defer store, plus the leaf and dedup store handles.
    struct Fixture {
        handler: PinStack,
        leaf: StagingLeaf,
        dedup_store: MemoryDeduplicationStore,
        loader: MemoryLoader<Value>,
        defer_store: MemoryMessageDeferStore,
        registry_key: StateKey,
    }

    impl Fixture {
        fn new() -> Result<Self> {
            let topic = Topic::from(TOPIC);
            let partition = Partition::from(PARTITION);
            let leaf = StagingLeaf::default();
            let dedup_store = MemoryDeduplicationStore::new();
            let loader = MemoryLoader::new();
            let defer_store = MemoryMessageDeferStore::new();
            let telemetry = Telemetry::new();
            let handler = MessageDeferHandler {
                handler: DeduplicationHandler {
                    inner: leaf.clone(),
                    store: dedup_store.clone(),
                },
                loader: loader.clone(),
                store: defer_store.clone(),
                decider: AlwaysDefer,
                config: DeferConfiguration::builder()
                    .enabled(true)
                    .base(Duration::from_secs(1))
                    .max_delay(Duration::from_hours(1))
                    .failure_threshold(0.9_f64)
                    .build()?,
                topic,
                partition,
                sender: telemetry.partition_sender(topic, partition),
                source: Arc::from(GROUP),
                dedup_version: Arc::from(VERSION),
            };
            Ok(Self {
                handler,
                leaf,
                dedup_store,
                loader,
                defer_store,
                registry_key: StateKey::new(Uuid::from_u128(0xD1), Arc::from(KEY)),
            })
        }

        /// The registry with the leaf's `cart` collection.
        fn registry() -> Result<CollectionDefRegistry> {
            let mut registry = CollectionDefRegistry::default();
            registry.register(&StagingLeaf::collection(), CollectionDef::new(None))?;
            Ok(registry)
        }

        /// Seeds message at `offset` (key [`KEY`], no event id) into the
        /// loader and returns its canonical dedup id.
        fn seed_message(&self, offset: Offset) -> Uuid {
            self.loader.store_message(
                Topic::from(TOPIC),
                Partition::from(PARTITION),
                offset,
                Key::from(KEY),
                json!({}),
            );
            message_id(offset)
        }

        /// A recording session (and its assert surfaces) for `event`,
        /// sharing this fixture's loader so reloads resolve.
        fn session(&self, event: EventRef) -> Result<SessionParts> {
            Ok(recording_session_with_loader(
                Self::registry()?,
                self.registry_key.clone(),
                event,
                self.loader.clone(),
            ))
        }
    }

    /// The canonical dedup id of the fixture message at `offset` (offset
    /// hash branch: the payloads carry no event id).
    fn message_id(offset: Offset) -> Uuid {
        dedup_uuid(
            VERSION,
            GROUP,
            TOPIC,
            PARTITION,
            KEY.as_bytes(),
            None,
            offset,
        )
    }

    /// A `DeferredMessage` trigger for the fixture key.
    fn defer_trigger() -> Trigger {
        Trigger::for_testing(
            Arc::from(KEY),
            CompactDateTime::from(1000_u32),
            TimerType::DeferredMessage,
        )
    }

    /// The timer `EventRef` the reload sessions stage under.
    fn timer_event() -> EventRef {
        EventRef::Timer(TimerEventRef::new(
            TimerType::DeferredMessage,
            CompactDateTime::from(1000_u32),
            0,
        ))
    }

    /// An uncommitted message for `offset` plus its offset tracker.
    async fn uncommitted_message(
        offset: Offset,
    ) -> Result<(UncommittedMessage<Value>, OffsetTracker)> {
        let version = Arc::new(CachePadded::new(AtomicUsize::new(0)));
        let tracker =
            OffsetTracker::new(TOPIC.into(), PARTITION, 10, Duration::from_secs(5), version);
        let uncommitted_offset = tracker.take(offset).await?;
        let semaphore = Arc::new(Semaphore::new(1));
        let permit = semaphore.try_acquire_owned()?;
        let message = ConsumerMessage::new(
            ConsumerMessageValue {
                key: KEY.into(),
                offset,
                topic: Topic::from(TOPIC),
                partition: Partition::from(PARTITION),
                ..Default::default()
            },
            tracing::Span::current(),
            permit,
        )
        .into_uncommitted(uncommitted_offset);
        Ok((message, tracker))
    }

    /// `Ok(Deferred)` records NOTHING and stages NOTHING, so the deferred
    /// reload re-runs (the fatal case a result-blind boundary would lose):
    /// the swallowed attempt's dirty write dies with the scope drop, no
    /// marker records, the offset commits — and firing the defer timer
    /// re-runs the leaf unfiltered.
    #[tokio::test]
    async fn defer_swallow_records_nothing_and_reload_reruns() -> Result<()> {
        let fx = Fixture::new()?;
        let (session, cell_store, dirty, recorded) = fx.session(EventRef::Message {
            dedup_id: message_id(0),
        })?;
        let scope = EventStateScope::new(session);
        let context = MockEventContext::new().with_session(scope.handle());

        fx.leaf.fail_next(ErrorCategory::Transient);
        let (message, tracker) = uncommitted_message(0).await?;
        EventHandler::on_message(&fx.handler, context, message, DemandType::Normal).await;

        assert_eq!(
            fx.defer_store.is_deferred(&Key::from(KEY)).await?,
            Some(0),
            "the message must be deferred for timer-based retry",
        );
        assert!(
            recorded.lock().is_empty(),
            "the swallowed attempt must record NO marker — the reload must not be filtered",
        );
        assert_eq!(
            committed_json_value(&cell_store, fx.registry_key.clone(), "cart").await?,
            None,
            "the swallowed attempt must stage nothing",
        );
        assert_eq!(
            tracker.shutdown().await,
            Some(0),
            "the swallowed dispatch commits the offset",
        );
        drop(scope);
        assert!(
            dirty.touched(&Key::from(KEY)).is_empty(),
            "the scope drop sweeps the swallowed attempt's dirty residue",
        );

        // The reload re-runs: seed the loader, fire the defer timer with the
        // leaf now succeeding, and the leaf sees a second dispatch (it was
        // not dedup-filtered).
        fx.seed_message(0);
        let (timer_session, ..) = fx.session(timer_event())?;
        let timer_scope = EventStateScope::new(timer_session);
        let timer_context = MockEventContext::new()
            .with_session(timer_scope.handle())
            .with_timer_tracking();
        let (timer, committed, _aborted) = RecordingTimer::new(defer_trigger());
        EventHandler::on_timer(&fx.handler, timer_context, timer, DemandType::Normal).await;
        assert_eq!(
            fx.leaf.processed(),
            vec![0, 0],
            "the reload re-ran the leaf on the same offset",
        );
        assert_eq!(committed.load(Ordering::SeqCst), 1, "the trigger commits");
        Ok(())
    }

    /// A deferred reload records the RELOADED message's marker while staging
    /// under the timer's `EventRef`, and a redelivery of that message
    /// filters against exactly the recorded id.
    #[tokio::test]
    async fn reload_records_the_reloaded_marker_and_redelivery_filters() -> Result<()> {
        let fx = Fixture::new()?;
        let expected = fx.seed_message(0);
        fx.defer_store
            .defer_first_message(&Key::from(KEY), 0)
            .await?;

        let (session, cell_store, _dirty, recorded) = fx.session(timer_event())?;
        let scope = EventStateScope::new(session);
        let context = MockEventContext::new()
            .with_session(scope.handle())
            .with_timer_tracking();
        let (timer, committed, _aborted) = RecordingTimer::new(defer_trigger());

        EventHandler::on_timer(&fx.handler, context, timer, DemandType::Normal).await;

        assert_eq!(
            recorded.lock().clone(),
            vec![expected],
            "the boundary records the RELOADED message's id, read from the override",
        );
        assert_eq!(
            committed_json_value(&cell_store, fx.registry_key.clone(), "cart").await?,
            Some(json!({ "offset": 0_i64 })),
            "the reload's write staged under the timer session and promoted",
        );
        assert_eq!(committed.load(Ordering::SeqCst), 1, "the trigger commits");
        assert_eq!(fx.leaf.processed(), vec![0], "the leaf ran once");

        // Redelivery of the original message: the settle-recorded id reaches
        // the dedup store (in production the oracle IS the dedup store), and
        // the filter — reading the message session's EventRef id — skips the
        // leaf without recording a second marker.
        let redelivered_id = recorded.lock()[0];
        fx.dedup_store.insert(redelivered_id).await?;
        let (msg_session, _, _, msg_recorded) = fx.session(EventRef::Message {
            dedup_id: message_id(0),
        })?;
        let msg_scope = EventStateScope::new(msg_session);
        let msg_context = MockEventContext::new().with_session(msg_scope.handle());
        let (message, tracker) = uncommitted_message(0).await?;
        EventHandler::on_message(&fx.handler, msg_context, message, DemandType::Normal).await;
        assert_eq!(
            fx.leaf.processed(),
            vec![0],
            "the redelivery is dedup-filtered — the leaf did not re-run",
        );
        assert!(
            msg_recorded.lock().is_empty(),
            "a filtered redelivery records no second marker",
        );
        assert_eq!(
            tracker.shutdown().await,
            Some(0),
            "the filtered redelivery commits its offset",
        );
        Ok(())
    }

    /// A reload that fails permanently records the reloaded message's id
    /// (with no stage) and advances the queue — the case a marker threaded
    /// through Ok-payloads could not express.
    #[tokio::test]
    async fn reload_permanent_failure_records_the_reloaded_id() -> Result<()> {
        let fx = Fixture::new()?;
        let expected = fx.seed_message(0);
        fx.defer_store
            .defer_first_message(&Key::from(KEY), 0)
            .await?;

        let (session, cell_store, _dirty, recorded) = fx.session(timer_event())?;
        let scope = EventStateScope::new(session);
        let context = MockEventContext::new()
            .with_session(scope.handle())
            .with_timer_tracking();
        let (timer, committed, _aborted) = RecordingTimer::new(defer_trigger());

        fx.leaf.fail_next(ErrorCategory::Permanent);
        EventHandler::on_timer(&fx.handler, context, timer, DemandType::Normal).await;

        assert_eq!(
            recorded.lock().clone(),
            vec![expected],
            "a permanently-failed reload records the reloaded message's id",
        );
        assert_eq!(
            committed_json_value(&cell_store, fx.registry_key.clone(), "cart").await?,
            None,
            "permanent reload failure stages nothing",
        );
        assert_eq!(
            fx.defer_store.is_deferred(&Key::from(KEY)).await?,
            None,
            "the permanent failure advances (empties) the queue",
        );
        assert_eq!(committed.load(Ordering::SeqCst), 1, "the trigger commits");
        Ok(())
    }

    /// Last-wins override: a retry re-dispatch of the same defer timer after
    /// a durable queue advance loads a DIFFERENT queue head and records
    /// under the NEW head's id — a set-once override would record message
    /// B's dispatch under message A's identity.
    #[tokio::test(start_paused = true)]
    async fn retry_redispatch_records_under_the_new_head_id() -> Result<()> {
        let fx = Fixture::new()?;
        let id_m1 = fx.seed_message(0);
        let id_m2 = fx.seed_message(1);
        fx.defer_store
            .defer_first_message(&Key::from(KEY), 0)
            .await?;
        fx.defer_store
            .defer_additional_message(&Key::from(KEY), 1)
            .await?;

        let (session, _cell_store, _dirty, recorded) = fx.session(timer_event())?;
        let scope = EventStateScope::new(session);
        // Poison the FIRST timer op: attempt 1 reloads M1, the leaf
        // succeeds, the queue durably advances to M2, then the
        // reschedule fails Transient — the outer retry re-dispatches and
        // attempt 2 reloads M2.
        let context = MockEventContext::new()
            .with_session(scope.handle())
            .with_timer_tracking()
            .with_timer_failures(1, ErrorCategory::Transient);
        let (timer, committed, _aborted) = RecordingTimer::new(defer_trigger());

        let retry_provider = RetryMiddleware::new(RetryConfiguration::builder().build()?)?
            .with_provider(FallibleCloneProvider::new(fx.handler.clone()));
        let retry_handler = FallibleHandlerProvider::handler_for_partition(
            &retry_provider,
            Topic::from(TOPIC),
            Partition::from(PARTITION),
        );

        EventHandler::on_timer(&retry_handler, context, timer, DemandType::Normal).await;

        assert_eq!(
            fx.leaf.processed(),
            vec![0, 1],
            "attempt 1 reloaded M1; attempt 2 reloaded the advanced head M2",
        );
        assert_eq!(
            recorded.lock().clone(),
            vec![id_m2],
            "the marker records under the NEW head's id — last-wins, never M1's",
        );
        assert_ne!(id_m1, id_m2, "distinct offsets hash to distinct ids");
        assert_eq!(committed.load(Ordering::SeqCst), 1, "the trigger commits");
        Ok(())
    }

    /// Store double for the classification table only: its `Error` is
    /// constructible (unlike the memory store's `Infallible`), so the
    /// `DeferError::Store` row can be exercised. `settlement()` is a pure
    /// function of the result value, so no store method ever runs.
    #[derive(Clone)]
    struct TableStore;

    impl MessageDeferStore for TableStore {
        type Error = StagingError;

        async fn defer_first_message(
            &self,
            _key: &Key,
            _offset: Offset,
        ) -> Result<(), StagingError> {
            Ok(())
        }

        async fn get_next_deferred_message(
            &self,
            _key: &Key,
        ) -> Result<Option<(Offset, u32)>, StagingError> {
            Ok(None)
        }

        async fn append_deferred_message(
            &self,
            _key: &Key,
            _offset: Offset,
        ) -> Result<(), StagingError> {
            Ok(())
        }

        async fn remove_deferred_message(
            &self,
            _key: &Key,
            _offset: Offset,
        ) -> Result<(), StagingError> {
            Ok(())
        }

        async fn set_retry_count(&self, _key: &Key, _retry_count: u32) -> Result<(), StagingError> {
            Ok(())
        }

        async fn delete_key(&self, _key: &Key) -> Result<(), StagingError> {
            Ok(())
        }
    }

    /// The settlement classification table for the message-defer wrapper:
    /// every Output and error variant. The delegating rows are proven to
    /// delegate by routing through the dedup wrapper's own table (`Inner
    /// None` reaches dedup's `Bypassed`, never a hardcoded `Final`).
    #[test]
    fn settlement_classification_table() {
        use crate::consumer::middleware::deduplication::DeduplicationError;
        use crate::timers::datetime::CompactDateTimeError;

        type Subject = MessageDeferHandler<
            DeduplicationHandler<StagingLeaf, MemoryDeduplicationStore>,
            TableStore,
            MemoryLoader<Value>,
            AlwaysDefer,
        >;
        type Out = MessageDeferOutput<Option<()>, DeduplicationError<StagingError>>;
        type TableErr =
            DeferError<StagingError, DeduplicationError<StagingError>, MemoryLoaderError>;

        let rows: Vec<(&str, Result<Out, TableErr>, Settlement)> = vec![
            (
                "Inner(Some) delegates through dedup to the leaf's Final",
                Ok(MessageDeferOutput::Inner(Some(()))),
                Settlement::Final,
            ),
            (
                "Inner(None) delegates to dedup's Bypassed (dedup hit)",
                Ok(MessageDeferOutput::Inner(None)),
                Settlement::Bypassed,
            ),
            (
                "Deferred is Bypassed (parked for retry)",
                Ok(MessageDeferOutput::Deferred(DeduplicationError::Inner(
                    StagingError(ErrorCategory::Transient),
                ))),
                Settlement::Bypassed,
            ),
            (
                "NoInner is Bypassed (queued behind / load handled)",
                Ok(MessageDeferOutput::NoInner),
                Settlement::Bypassed,
            ),
            (
                "Handler(Inner leaf error) delegates to Final",
                Err(DeferError::Handler(DeduplicationError::Inner(
                    StagingError(ErrorCategory::Permanent),
                ))),
                Settlement::Final,
            ),
            (
                "Handler(dedup Store) delegates to dedup's Bypassed",
                Err(DeferError::Handler(DeduplicationError::Store(Box::new(
                    StagingError(ErrorCategory::Transient),
                )))),
                Settlement::Bypassed,
            ),
            (
                "Store rescue failure is Bypassed",
                Err(DeferError::Store(StagingError(ErrorCategory::Transient))),
                Settlement::Bypassed,
            ),
            (
                "Timer rescue failure is Bypassed",
                Err(DeferError::Timer(Box::new(StagingError(
                    ErrorCategory::Transient,
                )))),
                Settlement::Bypassed,
            ),
            (
                "Loader rescue failure is Bypassed",
                Err(DeferError::Loader(MemoryLoaderError::LoaderShutdown)),
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
}
