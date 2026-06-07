//! Directed tests for [`StateManager`], [`StateManagerProvider`], and
//! [`NoState`].
//!
//! These exercise eager identity acquisition, session minting (event-ref
//! derivation parity with the deduplication writer), the recovery sweep
//! against a real in-memory `TimerManager`, and the stateless `NoState`
//! arm. All tests are broker-free.

use super::*;
use crate::consumer::event_context::StateAccessError;
use crate::consumer::middleware::defer::message::loader::MemoryLoader;
use crate::consumer::partition::ShutdownPhase;
use crate::heartbeat::HeartbeatRegistry;
use crate::state::SharedStateBackend;
use crate::state::descriptor::{ValueDescriptor, value_state};
use crate::state::memory::{MemoryDirtyValueStoreProvider, MemoryDurableValueStore};
use crate::state::registry::CollectionDef;
use crate::state::session::sealed::{FinalizeOutcome, StateLifecycle};
use crate::state::tests::value_suite::{FixedOracle, FixedOracleError, bytes};
use crate::state::value::ValueOp;
use crate::state::{CommitDecision, DurableState, StateName, StateType};
use crate::telemetry::Telemetry;
use crate::timers::datetime::CompactDateTime;
use crate::timers::store::adapter::TableAdapter;
use crate::timers::store::memory::{InMemoryTriggerStore, memory_store};
use crate::timers::store::{Segment, SegmentVersion};
use crate::timers::{PendingTimer, TimerManagerConfig, TimerRequest, TimerSemaphores};
use color_eyre::eyre::{Result, eyre};
use futures::Stream;
use std::array::from_fn;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::{Semaphore, watch};
use tracing::Span;
use uuid::Uuid;

const CART: ValueDescriptor = value_state("cart");

/// Oracle that counts `resolve` calls so a test can assert it was never
/// consulted (the stale-pending sweep arm must not touch the oracle).
#[derive(Clone)]
struct CountingOracle {
    decision: CommitDecision,
    calls: Arc<AtomicUsize>,
}

impl CountingOracle {
    fn new(decision: CommitDecision) -> Self {
        Self {
            decision,
            calls: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn call_count(&self) -> usize {
        self.calls.load(Ordering::Relaxed)
    }
}

impl CommitOracle for CountingOracle {
    type Error = FixedOracleError;

    async fn resolve<'a>(
        &'a self,
        _collection: &'a CollectionId<ValueKind>,
        _event: EventRef,
    ) -> Result<CommitDecision, Self::Error> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        Ok(self.decision)
    }
}

type TestBackend<O> = SharedStateBackend<MemoryDurableValueStore, O, MemoryDirtyValueStoreProvider>;
type TestProvider<O> =
    StateManagerProvider<TestBackend<O>, MemoryDurableValueStore, MemoryLoader<serde_json::Value>>;
type TestTimerManager = TimerManager<TableAdapter<InMemoryTriggerStore>>;

fn registry_with_cart() -> Result<Arc<CollectionDefRegistry>> {
    let mut registry = CollectionDefRegistry::new(Some(CompactDuration::new(3_600)));
    registry.register(&CART, CollectionDef::new(None))?;
    Ok(Arc::new(registry))
}

fn provider<O>(
    durable: MemoryDurableValueStore,
    oracle: O,
    registry: Arc<CollectionDefRegistry>,
) -> TestProvider<O>
where
    O: CommitOracle,
{
    StateManagerProvider::new(
        SharedStateBackend::new(durable.clone(), oracle, MemoryDirtyValueStoreProvider),
        durable,
        MemoryLoader::new(),
        registry,
        Arc::from("test-group"),
        Arc::from("1"),
        CompactDuration::new(30),
    )
}

fn termination() -> TerminationWatch {
    let (_shutdown_tx, shutdown_rx) = watch::channel(ShutdownPhase::default());
    let (_cancel_tx, cancel_rx) = watch::channel(false);
    // Dropped senders are fine: watch receivers keep reporting the last
    // value, which stays "live" for these tests.
    TerminationWatch::new(shutdown_rx, cancel_rx)
}

fn test_message(offset: crate::Offset) -> Result<ConsumerMessage<serde_json::Value>> {
    ConsumerMessage::for_testing(
        Topic::from("t"),
        0,
        offset,
        Arc::from("k"),
        serde_json::Value::Null,
    )
    .map_err(|e| eyre!("for_testing: {e}"))
}

/// Builds a real in-memory `TimerManager`; the pending stream is returned
/// so it stays alive for the manager's lifetime.
async fn timer_manager() -> Result<(
    impl Stream<Item = PendingTimer<TableAdapter<InMemoryTriggerStore>>>,
    TestTimerManager,
    watch::Sender<ShutdownPhase>,
)> {
    let segment = Segment {
        id: Uuid::new_v4(),
        name: "test".to_owned(),
        slab_size: CompactDuration::new(300),
        version: SegmentVersion::V3,
    };
    let store = memory_store(segment);
    let (shutdown_tx, shutdown_rx) = watch::channel(ShutdownPhase::default());
    let telemetry = Telemetry::new();
    let config = TimerManagerConfig {
        name: "test".to_owned(),
        store,
        telemetry: telemetry.partition_sender(Topic::from("test"), 0),
        source: Arc::from(""),
    };
    let semaphores: Arc<TimerSemaphores> = Arc::new(from_fn(|_| Arc::new(Semaphore::new(64))));
    let (stream, manager) =
        TimerManager::new(config, HeartbeatRegistry::test(), shutdown_rx, semaphores)
            .await
            .map_err(|e| eyre!("{e}"))?;
    Ok((stream, manager, shutdown_tx))
}

async fn acquire<O>(
    provider: &TestProvider<O>,
) -> Result<
    StateManager<
        MemoryDurableValueStore,
        O,
        MemoryDirtyValueStoreProvider,
        MemoryDurableValueStore,
        MemoryLoader<serde_json::Value>,
    >,
>
where
    O: CommitOracle,
{
    provider
        .acquire(Topic::from("t"), 0)
        .await
        .map_err(|e| eyre!("acquire failed: {e}"))
}

/// Read a partition expected to be `Idle`, returning its applied payload.
async fn read_idle_applied(
    durable: &MemoryDurableValueStore,
    id: &CollectionId<ValueKind>,
) -> Result<Option<bytes::Bytes>> {
    match DurableWalStore::read_partition(durable, id).await? {
        DurableState::Idle { applied } => Ok(applied),
        other @ DurableState::Sealed { .. } => Err(eyre!("expected Idle, got {other:?}")),
    }
}

/// The `"cart"` collection identity for `key` in the fixed test segment.
///
/// Deterministic by design — recovery tests re-derive the same identity
/// across `recover` calls, so this must build the segment from
/// [`compute_segment_id`] rather than reuse the random-UUID
/// `value_suite::collection_id` fixture.
fn cart_id(key: &Key) -> Result<CollectionId<ValueKind>> {
    Ok(CollectionId::new(
        StateKey::new(
            compute_segment_id(Topic::from("t"), 0, "test-group"),
            key.clone(),
        ),
        StateType::Application,
        StateName::try_new("cart")?,
    ))
}

/// Acquisition eagerly writes first-seen identity rows and fails Permanent
/// when a later deployment asserts a different identity for the same name.
#[tokio::test]
async fn acquire_validates_identities_eagerly() -> Result<()> {
    let durable = MemoryDurableValueStore::for_tests();
    let registry = registry_with_cart()?;
    let p = provider(durable.clone(), FixedOracle::committed(), registry);
    let manager = acquire(&p).await?;

    // The rows were written during acquisition.
    let segment_id = compute_segment_id(Topic::from("t"), 0, "test-group");
    let rows = durable.read_descriptor_identities(segment_id).await?;
    assert_eq!(rows.len(), 1, "first acquisition writes the identity row");
    assert!(manager.intercepts_recovery());

    // Re-acquiring with the same registry is idempotent.
    acquire(&p).await?;

    // A conflicting identity for the same name fails Permanent.
    let mut conflicting = CollectionDefRegistry::new(None);
    conflicting.register(&CART.with_schema_label("v2"), CollectionDef::new(None))?;
    let conflicted = provider(durable, FixedOracle::committed(), Arc::new(conflicting));
    let err = conflicted
        .acquire(Topic::from("t"), 0)
        .await
        .err()
        .ok_or_else(|| eyre!("conflicting identity must fail acquisition"))?;
    assert!(matches!(err, StateAcquireError::Identity(_)));
    assert!(matches!(err.classify_error(), ErrorCategory::Permanent));
    Ok(())
}

/// T1 (reader side): the manager derives a message's dedup id through the
/// canonical [`dedup_uuid_for_message`] the deduplication *writer* uses,
/// threading its configured `version` and `consumer_group`. Covered for
/// payloads with and without an `event_id` so both hash branches are
/// exercised.
#[tokio::test]
async fn session_for_message_derives_canonical_dedup_id() -> Result<()> {
    let durable = MemoryDurableValueStore::for_tests();
    let manager = acquire(&provider(
        durable,
        FixedOracle::committed(),
        registry_with_cart()?,
    ))
    .await?;

    for payload in [serde_json::json!({ "id": "evt-1" }), serde_json::json!({})] {
        let msg = ConsumerMessage::for_testing(Topic::from("t"), 0, 7, Arc::from("k"), payload)
            .map_err(|e| eyre!("for_testing: {e}"))?;
        let session = manager.session_for_message(&msg, termination());
        let expected = dedup_uuid_for_message("1", "test-group", "t", 0, &msg);
        assert_eq!(
            session.event(),
            EventRef::Message { dedup_id: expected },
            "session event must match the canonical writer derivation"
        );
    }
    Ok(())
}

/// Timer sessions carry the trigger's durable timer coordinates.
#[tokio::test]
async fn session_for_timer_uses_timer_event_ref() -> Result<()> {
    let durable = MemoryDurableValueStore::for_tests();
    let manager = acquire(&provider(
        durable,
        FixedOracle::committed(),
        registry_with_cart()?,
    ))
    .await?;

    let trigger = Trigger::for_testing(
        Arc::from("k"),
        CompactDateTime::from(99_u32),
        TimerType::Application,
    );
    let session = manager.session_for_timer(&trigger, termination());
    assert_eq!(
        session.event(),
        EventRef::Timer(TimerEventRef::new(
            trigger.timer_type,
            trigger.time,
            trigger.tag
        ))
    );
    Ok(())
}

/// The full crash-window lifecycle at the manager level: a session seals,
/// the apply hook never runs (simulated crash), and `recover` resolves the
/// WAL through the oracle and clears the `StateRecovery` timer through a
/// real `TimerManager`.
#[tokio::test]
async fn recover_applies_sealed_and_clears_timer() -> Result<()> {
    let durable = MemoryDurableValueStore::for_tests();
    let manager = acquire(&provider(
        durable.clone(),
        FixedOracle::committed(),
        registry_with_cart()?,
    ))
    .await?;
    let (_stream, timers, _shutdown_tx) = timer_manager().await?;
    let key: Key = Arc::from("k");

    // A session seals but its apply hook never runs (crash window).
    let msg = test_message(0)?;
    let session = manager.session_for_message(&msg, termination());
    session
        .set_state_cell(&StateName::try_new("cart")?, bytes(7))
        .await?;
    assert_eq!(session.finalize().await?, FinalizeOutcome::Sealed);

    // The backstop timer the lifecycle middleware would have armed.
    let fire = CompactDateTime::now()?.add_duration(CompactDuration::new(60))?;
    timers
        .schedule(TimerRequest::new(
            key.clone(),
            fire,
            TimerType::StateRecovery,
            Span::current(),
        ))
        .await?;

    manager
        .recover(key.clone(), &timers)
        .await
        .map_err(|e| eyre!("recover failed: {e}"))?;

    // The WAL resolved as committed and the timer is gone.
    let id = cart_id(&key)?;
    assert_eq!(read_idle_applied(&durable, &id).await?, Some(bytes(7)));
    let remaining = timers
        .scheduled_times(&key, TimerType::StateRecovery)
        .await?;
    assert!(
        remaining.is_empty(),
        "recover must clear the backstop timer"
    );
    Ok(())
}

/// `recover` rolls an uncommitted seal back when the oracle says the event
/// never committed.
#[tokio::test]
async fn recover_rolls_back_when_not_committed() -> Result<()> {
    let durable = MemoryDurableValueStore::for_tests();
    let manager = acquire(&provider(
        durable.clone(),
        FixedOracle::not_committed(),
        registry_with_cart()?,
    ))
    .await?;
    let (_stream, timers, _shutdown_tx) = timer_manager().await?;
    let key: Key = Arc::from("k");

    let msg = test_message(1)?;
    let session = manager.session_for_message(&msg, termination());
    session
        .set_state_cell(&StateName::try_new("cart")?, bytes(99))
        .await?;
    assert_eq!(session.finalize().await?, FinalizeOutcome::Sealed);

    manager
        .recover(key.clone(), &timers)
        .await
        .map_err(|e| eyre!("recover failed: {e}"))?;

    let id = cart_id(&key)?;
    assert_eq!(
        read_idle_applied(&durable, &id).await?,
        None,
        "rollback restored pre-seal state"
    );
    Ok(())
}

/// F4: a crash between `insert_pending` and the WAL write leaves a pending
/// row over an *Idle* partition. Recovery must delete that row and never
/// consult the oracle.
#[tokio::test]
async fn recover_deletes_stale_pending_without_consulting_oracle() -> Result<()> {
    let durable = MemoryDurableValueStore::for_tests();
    let oracle = CountingOracle::new(CommitDecision::Committed);
    let manager = acquire(&provider(
        durable.clone(),
        oracle.clone(),
        registry_with_cart()?,
    ))
    .await?;
    let (_stream, timers, _shutdown_tx) = timer_manager().await?;
    let key: Key = Arc::from("k");
    let id = cart_id(&key)?;

    PendingIndexStore::insert_pending::<ValueKind>(&durable, &id).await?;

    manager
        .recover(key, &timers)
        .await
        .map_err(|e| eyre!("recover failed: {e}"))?;

    let pending: Vec<_> = durable.scan_pending(id.state_key()).collect().await;
    assert!(pending.is_empty(), "stale pending row must be deleted");
    assert_eq!(
        oracle.call_count(),
        0,
        "oracle must not be consulted for a stale pending row"
    );
    Ok(())
}

/// `NoState` yields unavailable sessions, no-op recovery, and no
/// `StateRecovery` interception.
#[tokio::test]
async fn no_state_provider_yields_unavailable_sessions() -> Result<()> {
    let provider: NoState<serde_json::Value> = NoState::new();
    let manager = provider
        .acquire(Topic::from("t"), 0)
        .await
        .map_err(|e| eyre!("{e}"))?;
    assert!(!manager.intercepts_recovery());

    let msg = test_message(0)?;
    let session = manager.session_for_message(&msg, termination());
    assert!(matches!(
        session.state_cell(&StateName::try_new("anything")?).await,
        Err(StateAccessError::Unavailable)
    ));

    let (_stream, timers, _shutdown_tx) = timer_manager().await?;
    manager.recover(Arc::from("k"), &timers).await?;
    Ok(())
}

/// Deferred-message reload, commit arm: a `DeferredMessage` trigger fires
/// (the message-defer reload signal), the loop mints the session from the
/// **firing trigger** — so the reloaded message's state ops seal under the
/// timer [`EventRef`] — the marker commits, and the apply hook never runs
/// (crash window). Recovery must resolve **Committed** through the real
/// [`CommitManager`] timer oracle (row absent → fired-and-removed →
/// committed) and apply the seal.
///
/// This is the deferred-reload path the 2026-06-04 review found had zero
/// coverage; the oracle's three-state tag logic is timer-type-agnostic by
/// design, which is what makes sealing under a `DeferredMessage` ref
/// correct.
///
/// [`CommitManager`]: crate::commit_manager::CommitManager
#[tokio::test]
async fn deferred_message_seal_commit_resolves_through_timer_oracle() -> Result<()> {
    let (applied, timers, key) = run_deferred_message_crash(CrashMarker::Commit).await?;
    assert_eq!(
        applied,
        Some(bytes(7)),
        "committed reload: recovery must apply the seal"
    );
    let remaining = timers
        .scheduled_times(&key, TimerType::DeferredMessage)
        .await?;
    assert!(remaining.is_empty(), "the committed trigger is gone");
    Ok(())
}

/// Deferred-message reload, abort arm: the marker aborts, leaving the
/// trigger scheduled under its original tag — the oracle reads
/// `current_tag == wal_tag` → **`NotCommitted`** → recovery rolls the seal
/// back, and the redelivered trigger re-runs the reload from clean state.
#[tokio::test]
async fn deferred_message_seal_abort_rolls_back_through_timer_oracle() -> Result<()> {
    let (applied, _timers, _key) = run_deferred_message_crash(CrashMarker::Abort).await?;
    assert_eq!(
        applied, None,
        "aborted reload: recovery must roll the seal back"
    );
    Ok(())
}

/// How the crashed dispatch's durability marker resolved.
enum CrashMarker {
    Commit,
    Abort,
}

/// Shared body of the deferred-message crash-window tests: schedules a
/// `DeferredMessage` trigger on a real `TimerManager`, fires it, seals a
/// state op under the firing trigger's event ref, resolves the marker,
/// and recovers through a `CommitManager` backed by the same timers.
/// Returns the recovered applied cell.
async fn run_deferred_message_crash(
    marker: CrashMarker,
) -> Result<(Option<bytes::Bytes>, TestTimerManager, Key)> {
    use crate::commit_manager::CommitManager;
    use crate::consumer::Uncommitted;
    use crate::consumer::middleware::deduplication::memory::MemoryDeduplicationStore;
    use crate::timers::UncommittedTimer;
    use std::time::Duration;
    use tokio::task::yield_now;
    use tokio::time::{self, advance};

    time::pause();
    let (stream, timers, _shutdown_tx) = timer_manager().await?;
    futures::pin_mut!(stream);

    let durable = MemoryDurableValueStore::for_tests();
    let oracle = CommitManager::new(MemoryDeduplicationStore::new(), timers.clone());
    let manager = acquire(&provider(durable.clone(), oracle, registry_with_cart()?)).await?;
    let key: Key = Arc::from("k");

    // The message-defer middleware's reload signal.
    let fire_at = CompactDateTime::now()?.add_duration(CompactDuration::new(5))?;
    timers
        .schedule(TimerRequest::new(
            key.clone(),
            fire_at,
            TimerType::DeferredMessage,
            Span::current(),
        ))
        .await?;

    advance(Duration::from_secs(30)).await;
    yield_now().await;
    let pending = stream
        .next()
        .await
        .ok_or_else(|| eyre!("no pending timer fired"))?;
    let firing = pending
        .fire()
        .await
        .ok_or_else(|| eyre!("pending timer not active"))?;

    // The loop mints the session from the firing trigger (canonical tag);
    // the reloaded message's handler writes state, which seals under the
    // timer event ref on finalize.
    let session = manager.session_for_timer(firing.trigger(), termination());
    session
        .set_state_cell(&StateName::try_new("cart")?, bytes(7))
        .await?;
    assert_eq!(session.finalize().await?, FinalizeOutcome::Sealed);

    // Crash window: the marker resolves but the apply hook never runs.
    let (_trigger, guard) = firing.into_inner();
    match marker {
        CrashMarker::Commit => guard.commit().await,
        CrashMarker::Abort => guard.abort().await,
    }

    manager
        .recover(key.clone(), &timers)
        .await
        .map_err(|e| eyre!("recover failed: {e}"))?;

    let id = cart_id(&key)?;
    let applied = read_idle_applied(&durable, &id).await?;
    Ok((applied, timers, key))
}

/// `sweep_pending` resolves a WAL sealed out-of-band (no session at all) —
/// the path the durable seal API exposes to recovery without any manager
/// involvement.
#[tokio::test]
async fn sweep_pending_resolves_out_of_band_seal() -> Result<()> {
    let durable = MemoryDurableValueStore::for_tests();
    let state_key = StateKey::new(Uuid::new_v4(), Arc::from("k"));
    let id = CollectionId::<ValueKind>::new(
        state_key.clone(),
        StateType::Application,
        StateName::try_new("snapshot")?,
    );
    let collection_ref = CollectionRef::new(id.clone(), None);
    let event = EventRef::Message {
        dedup_id: Uuid::from_u128(42),
    };
    durable
        .seal(
            &collection_ref,
            event,
            vec![ValueOp::Set { payload: bytes(11) }],
        )
        .await?;

    let registry = CollectionDefRegistry::new(None);
    sweep_pending(
        &durable,
        &durable,
        &FixedOracle::committed(),
        &registry,
        state_key,
    )
    .await
    .map_err(|e| eyre!("sweep failed: {e}"))?;

    assert_eq!(read_idle_applied(&durable, &id).await?, Some(bytes(11)));
    Ok(())
}
