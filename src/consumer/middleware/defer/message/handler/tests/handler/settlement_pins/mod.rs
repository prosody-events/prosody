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
    ErrorCategory, FallibleEventHandler, FallibleHandlerProvider, HandlerMiddleware, Settlement,
    SettlementHandler,
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
use std::future::ready;
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
    async fn handle_message<C, P>(
        &self,
        context: C,
        message: ConsumerMessage<P>,
    ) -> Result<(), StagingError>
    where
        C: EventContext<Payload = Value>,
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

    async fn on_excise<C>(
        &self,
        context: C,
        message: ConsumerMessage<()>,
        _demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        self.handle_message(context, message).await
    }

    async fn on_message<C>(
        &self,
        context: C,
        message: ConsumerMessage<Self::Payload>,
        _demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        self.handle_message(context, message).await
    }

    fn on_timer<C>(
        &self,
        _context: C,
        _trigger: Trigger,
        _demand_type: DemandType,
    ) -> impl Future<Output = Result<Self::Output, Self::Error>>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        ready(Ok(()))
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
async fn uncommitted_message(offset: Offset) -> Result<(UncommittedMessage<Value>, OffsetTracker)> {
    let version = Arc::new(CachePadded::new(AtomicUsize::new(0)));
    let tracker = OffsetTracker::new(TOPIC.into(), PARTITION, 10, Duration::from_secs(5), version);
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
mod reload_failures;
