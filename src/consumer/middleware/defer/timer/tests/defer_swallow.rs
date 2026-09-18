use super::*;
use crate::consumer::EventHandler;
use crate::consumer::middleware::defer::DeferOutput;
use crate::consumer::middleware::defer::decider::AlwaysDefer;
use crate::consumer::middleware::defer::error::DeferError;
use crate::consumer::middleware::providers::LeafHandler;
use crate::consumer::middleware::tests::test_support::settlement_name;
use crate::consumer::middleware::tests::test_support::{
    BypassedHandler, MockEventContext, RecordingTimer, ScriptedHandler, StagingHook,
    StagingTransientHandler, TestError, committed_json_value, recording_session,
};
use crate::consumer::middleware::{FallibleEventHandler, SettlementHandler};
use crate::error::ErrorCategory;
use crate::loader::KafkaLoaderError;
use crate::state::registry::{CollectionDef, CollectionDefRegistry};
use crate::state::{EventRef, StateKey, TimerEventRef};
use std::future::ready;
use std::sync::atomic::Ordering;
use uuid::Uuid;

impl FallibleEventHandler
    for TimerDeferHandler<LeafHandler<StagingTransientHandler>, MemoryTimerDeferStore, AlwaysDefer>
{
}

impl FallibleEventHandler
    for TimerDeferHandler<LeafHandler<ScriptedHandler>, MemoryTimerDeferStore, AlwaysDefer>
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
) -> color_eyre::Result<TimerDeferHandler<LeafHandler<T>, MemoryTimerDeferStore, AlwaysDefer>> {
    let telemetry = Telemetry::new();
    Ok(TimerDeferHandler {
        handler: LeafHandler::new(inner),
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
/// into `Ok(Deferred)` arms NO recovery timer, stages nothing, records nothing,
/// and commits the trigger; the dirty residue dies with the scope drop.
/// Part B — the parity control: a clean success that staged nothing arms
/// nothing either (`Finalized::Clean` never arms).
#[tokio::test]
async fn defer_swallow_stages_nothing() -> color_eyre::Result<()> {
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
        "a bypassed dispatch must arm NO StateRecovery recovery timer",
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
        "a clean (nothing-staged) success arms no recovery timer",
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

    fn defer_first_timer(&self, _trigger: &Trigger) -> impl Future<Output = Result<(), TestError>> {
        ready(Ok(()))
    }

    fn get_next_deferred_timer(
        &self,
        _key: &Key,
    ) -> impl Future<Output = Result<Option<(Trigger, u32)>, TestError>> {
        ready(Ok(None))
    }

    fn append_deferred_timer(
        &self,
        _trigger: &Trigger,
    ) -> impl Future<Output = Result<(), TestError>> {
        ready(Ok(()))
    }

    fn deferred_times(
        &self,
        _key: &Key,
    ) -> impl Future<Output = Result<Vec<CompactDateTime>, TestError>> + Send + 'static {
        ready(Ok(Vec::new()))
    }

    fn remove_deferred_timer(
        &self,
        _key: &Key,
        _time: CompactDateTime,
    ) -> impl Future<Output = Result<(), TestError>> {
        ready(Ok(()))
    }

    fn set_retry_count(
        &self,
        _key: &Key,
        _retry_count: u32,
    ) -> impl Future<Output = Result<(), TestError>> {
        ready(Ok(()))
    }

    fn delete_key(&self, _key: &Key) -> impl Future<Output = Result<(), TestError>> {
        ready(Ok(()))
    }
}

type TableOut = DeferOutput<(), TestError>;
type TableErr = DeferError<TestError, TestError>;

/// The settlement classification table for the timer-defer wrapper:
/// every Output and error variant, over a `Final` leaf so delegation is
/// observable against the `Bypassed` rows. The `Inner`/`Handler`
/// delegation is proven separately in [`settlement_table_delegates`].
#[test]
fn settlement_classification_table() {
    use crate::timers::datetime::CompactDateTimeError;

    type Subject = TimerDeferHandler<LeafHandler<ScriptedHandler>, TableStore, AlwaysDefer>;
    type Out = TableOut;

    let rows: Vec<(&str, Result<Out, TableErr>, &str)> = vec![
        (
            "Inner delegates to the leaf's Final",
            Ok(DeferOutput::Inner(())),
            "Final",
        ),
        (
            "Deferred is Bypassed (parked for retry)",
            Ok(DeferOutput::Deferred(TestError(ErrorCategory::Transient))),
            "Bypassed",
        ),
        (
            "NoInner is Bypassed (queued behind / orphan cleanup)",
            Ok(DeferOutput::NoInner),
            "Bypassed",
        ),
        (
            "Handler delegates to the leaf's Rejected",
            Err(DeferError::Handler(TestError(ErrorCategory::Permanent))),
            "Rejected",
        ),
        (
            "Store bookkeeping failure is Abandoned",
            Err(DeferError::Store(TestError(ErrorCategory::Transient))),
            "Abandoned",
        ),
        (
            "Timer bookkeeping failure is Abandoned",
            Err(DeferError::Timer(Box::new(TestError(
                ErrorCategory::Transient,
            )))),
            "Abandoned",
        ),
        (
            "Loader bookkeeping failure is Abandoned",
            Err(DeferError::Loader(KafkaLoaderError::LoaderShutdown)),
            "Abandoned",
        ),
        (
            "CompactTime (backoff computation, Permanent) is Abandoned",
            Err(DeferError::CompactTime(CompactDateTimeError::OutOfRange)),
            "Abandoned",
        ),
    ];
    for (label, result, expected) in rows {
        assert_eq!(
            settlement_name(Subject::settlement(result.as_ref())),
            expected,
            "{label}"
        );
    }
}

/// Delegation proof for the timer-defer wrapper: over a
/// `Bypassed`-classifying probe leaf, the delegating rows (`Inner`,
/// `Handler`) stay `Bypassed` — a wrapper hardcoding `Final` on them
/// fails this test.
#[test]
fn settlement_table_delegates() {
    type Probe = TimerDeferHandler<BypassedHandler, TableStore, AlwaysDefer>;

    let inner: Result<TableOut, TableErr> = Ok(DeferOutput::Inner(()));
    let handler: Result<TableOut, TableErr> =
        Err(DeferError::Handler(TestError(ErrorCategory::Permanent)));
    assert_eq!(
        settlement_name(Probe::settlement(inner.as_ref())),
        "Bypassed"
    );
    assert_eq!(
        settlement_name(Probe::settlement(handler.as_ref())),
        "Bypassed"
    );
}
