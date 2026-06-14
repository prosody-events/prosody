//! Directed tests for [`StateManager::recover`] — the recovery-sweep
//! orchestration the partition loop drives when a `StateRecovery` backstop
//! fires.
//!
//! The per-collection resolution (`resolve_cell`), the no-strand aggregation,
//! the timer-tag oracle's three-state logic, and backstop arm amortization are
//! each covered elsewhere (the cell suite, `partition_store::tests`,
//! `commit_manager::tests`, and the middleware tests). These tests pin the glue
//! `recover` adds on top: it resolves a provisional cell staged under a real
//! [`EventRef`] through the oracle (commit **and** abort arms), clears the
//! per-key armed flag so the next commit re-arms, and unschedules the backstop
//! through a real [`TimerManager`] only when every cell ended resolved. The
//! manager is Kafka-agnostic, so these mint a session from a key and an
//! `EventRef` directly; all are broker-free.

use super::*;
use crate::consumer::partition::ShutdownPhase;
use crate::heartbeat::HeartbeatRegistry;
use crate::loader::MemoryLoader;
use crate::state::cell::{Cell, Committed, ProvisionalCell, ProvisionalWrite};
use crate::state::descriptor::{ValueDescriptor, value_state};
use crate::state::descriptor_identity::{DescriptorIdentityStore, DurableDescriptorIdentity};
use crate::state::memory::{MemoryCellStore, MemoryCommittedCache, MemoryDirtyValueStoreProvider};
use crate::state::registry::CollectionDef;
use crate::state::session::StateSession;
use crate::state::session::sealed::{ApplyOutcome, FinalizeOutcome, StateLifecycle};
use crate::state::tests::value_suite::bytes;
use crate::state::{
    CommitDecision, CommitMode, EventRef, SharedStateBackend, StateName, StateType, TimerEventRef,
};
use crate::telemetry::Telemetry;
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::timers::store::adapter::TableAdapter;
use crate::timers::store::memory::{InMemoryTriggerStore, memory_store};
use crate::timers::store::{Segment, SegmentVersion};
use crate::timers::{
    PendingTimer, TimerManagerConfig, TimerRequest, TimerSemaphores, TimerType, Trigger,
};
use bytes::Bytes;
use color_eyre::eyre::{Result, eyre};
use futures::{Stream, StreamExt};
use std::array::from_fn;
use std::convert::Infallible;
use std::sync::Arc;
use tokio::sync::{Semaphore, watch};
use tracing::Span;
use uuid::Uuid;

/// Oracle that returns a fixed decision for every event.
#[derive(Clone)]
struct FixedOracle(CommitDecision);

impl FixedOracle {
    fn committed() -> Self {
        Self(CommitDecision::Committed)
    }

    fn not_committed() -> Self {
        Self(CommitDecision::NotCommitted)
    }
}

impl CommitOracle for FixedOracle {
    type Error = Infallible;

    async fn record_message(&self, _dedup_id: Uuid) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn resolve<'a>(
        &'a self,
        _state_key: &'a StateKey,
        _event: EventRef,
    ) -> Result<CommitDecision, Self::Error> {
        Ok(self.0)
    }
}

type TestBackend = SharedStateBackend<
    MemoryCellStore,
    FixedOracle,
    MemoryCommittedCache,
    MemoryDirtyValueStoreProvider,
>;
type TestProvider = StateManagerProvider<TestBackend, MemoryLoader<serde_json::Value>>;
type TestManager = StateManager<
    MemoryCellStore,
    FixedOracle,
    MemoryCommittedCache,
    MemoryDirtyValueStoreProvider,
    MemoryLoader<serde_json::Value>,
>;

/// Per-event session type the test manager mints.
type TestSession = <TestManager as PartitionStateManager>::Session;

fn cart() -> ValueDescriptor {
    value_state("cart")
}

fn wishlist() -> ValueDescriptor {
    value_state("wishlist")
}

fn last_seen() -> ValueDescriptor {
    value_state("last_seen")
}

fn registry_with_cart() -> Result<Arc<CollectionDefRegistry>> {
    let mut registry = CollectionDefRegistry::new(Some(CompactDuration::new(3_600)));
    registry.register(&cart(), CollectionDef::new(None))?;
    Ok(Arc::new(registry))
}

/// A registry with three collections exercising both commit modes: two
/// `ReadCommitted` (`cart`, `wishlist`) that stage provisional cells and one
/// `ReadUncommitted` (`last_seen`) that writes resolved at finalize.
fn registry_with_mixed() -> Result<Arc<CollectionDefRegistry>> {
    let mut registry = CollectionDefRegistry::new(Some(CompactDuration::new(3_600)));
    registry.register(&cart(), CollectionDef::new(None))?;
    registry.register(&wishlist(), CollectionDef::new(None))?;
    registry.register(
        &last_seen(),
        CollectionDef::new(None).with_commit_mode(CommitMode::ReadUncommitted),
    )?;
    Ok(Arc::new(registry))
}

/// Builds a provider sharing `cell` (so a test can read the durable cell back
/// after `recover`) with the given oracle and registry.
fn provider_with(
    cell: MemoryCellStore,
    oracle: FixedOracle,
    registry: Arc<CollectionDefRegistry>,
) -> TestProvider {
    StateManagerProvider::new(
        SharedStateBackend::new(
            cell,
            oracle,
            MemoryCommittedCache::new(),
            MemoryDirtyValueStoreProvider,
        ),
        MemoryLoader::new(),
        registry,
        Arc::from("test-group"),
        CompactDuration::new(30),
    )
}

/// A provider over the single-`cart` registry the recovery tests use.
fn provider(cell: MemoryCellStore, oracle: FixedOracle) -> Result<TestProvider> {
    Ok(provider_with(cell, oracle, registry_with_cart()?))
}

async fn acquire(provider: &TestProvider) -> Result<TestManager> {
    provider
        .acquire(Topic::from("t"), 0)
        .await
        .map_err(|e| eyre!("acquire failed: {e}"))
}

fn termination() -> TerminationWatch {
    let (_shutdown_tx, shutdown_rx) = watch::channel(ShutdownPhase::default());
    let (_cancel_tx, cancel_rx) = watch::channel(false);
    TerminationWatch::new(shutdown_rx, cancel_rx)
}

/// The `name` collection identity for `key` in the fixed test segment —
/// re-derived through [`compute_segment_id`] so it matches what `acquire`
/// wrote, exactly as recovery does.
fn id_for(key: &Key, name: &str) -> Result<CollectionId<ValueKind>> {
    Ok(CollectionId::new(
        StateKey::new(
            compute_segment_id(Topic::from("t"), 0, "test-group"),
            key.clone(),
        ),
        StateType::Application,
        StateName::try_new(name)?,
    ))
}

/// The `cart` collection identity for `key`.
fn cart_id(key: &Key) -> Result<CollectionId<ValueKind>> {
    id_for(key, "cart")
}

/// Builds a real in-memory `TimerManager`; the pending stream is returned so it
/// stays alive for the manager's lifetime.
async fn timer_manager() -> Result<(
    impl Stream<Item = PendingTimer<TableAdapter<InMemoryTriggerStore>>>,
    TimerManager<TableAdapter<InMemoryTriggerStore>>,
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

/// Stages a `cart` provisional cell under a timer `EventRef` through a real
/// session (the crash window: the cell is staged, its promote never ran), and
/// marks the key armed as the durability boundary would have.
async fn stage_under_timer(manager: &TestManager, key: &Key, value: u8) -> Result<()> {
    let trigger = Trigger::for_testing(
        key.clone(),
        CompactDateTime::from(99_u32),
        TimerType::DeferredMessage,
    );
    let event = EventRef::Timer(TimerEventRef::new(
        trigger.timer_type,
        trigger.time,
        trigger.tag,
    ));
    let session = manager.session(key.clone(), event, termination());
    session
        .set_state_cell(&StateName::try_new("cart")?, bytes(value))
        .await?;
    assert_eq!(session.finalize().await?, FinalizeOutcome::Staged);
    manager.inner.armed.insert_async(key.clone()).await.ok();
    Ok(())
}

/// `recover` resolves a committed provisional cell to its `data`, clears the
/// per-key armed flag, and unschedules the backstop timer.
#[tokio::test]
async fn recover_promotes_committed_cell_clears_armed_and_timer() -> Result<()> {
    let cell = MemoryCellStore::new();
    let manager = acquire(&provider(cell.clone(), FixedOracle::committed())?).await?;
    let (_stream, timers, _shutdown_tx) = timer_manager().await?;
    let key: Key = Arc::from("k");

    stage_under_timer(&manager, &key, 7).await?;
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

    let id = cart_id(&key)?;
    assert_eq!(
        cell.read_cell(&id, &()).await?,
        Cell::Resolved(Committed::new(Some(bytes(7)))),
        "a committed cell promotes to its staged data",
    );
    assert!(
        !manager.inner.armed.contains_async(&key).await,
        "recover clears the per-key armed flag on fire",
    );
    assert!(
        timers
            .scheduled_times(&key, TimerType::StateRecovery)
            .await?
            .is_empty(),
        "an all-resolved sweep unschedules the backstop",
    );
    Ok(())
}

/// `recover` rolls an uncommitted provisional cell back to its committed base
/// (`prev`, here absent) when the oracle says the event never committed.
#[tokio::test]
async fn recover_rolls_back_uncommitted_cell() -> Result<()> {
    let cell = MemoryCellStore::new();
    let manager = acquire(&provider(cell.clone(), FixedOracle::not_committed())?).await?;
    let (_stream, timers, _shutdown_tx) = timer_manager().await?;
    let key: Key = Arc::from("k");

    stage_under_timer(&manager, &key, 99).await?;

    manager
        .recover(key.clone(), &timers)
        .await
        .map_err(|e| eyre!("recover failed: {e}"))?;

    let id = cart_id(&key)?;
    assert_eq!(
        cell.read_cell(&id, &()).await?,
        Cell::Resolved(Committed::new(None)),
        "an uncommitted cell rolls back to its (absent) committed base",
    );
    Ok(())
}

/// The timer `EventRef` the multi-collection lifecycle tests stage under.
fn timer_event(key: &Key) -> EventRef {
    let trigger = Trigger::for_testing(
        key.clone(),
        CompactDateTime::from(99_u32),
        TimerType::DeferredMessage,
    );
    EventRef::Timer(TimerEventRef::new(
        trigger.timer_type,
        trigger.time,
        trigger.tag,
    ))
}

/// Buffers writes to two `ReadCommitted` collections (`cart`, `wishlist`) and
/// one `ReadUncommitted` collection (`last_seen`) through one session, before
/// `finalize`.
async fn write_mixed(manager: &TestManager, key: &Key) -> Result<(TestSession, EventRef)> {
    let event = timer_event(key);
    let session = manager.session(key.clone(), event, termination());
    session
        .set_state_cell(&StateName::try_new("cart")?, bytes(7))
        .await?;
    session
        .set_state_cell(&StateName::try_new("wishlist")?, bytes(13))
        .await?;
    session
        .set_state_cell(&StateName::try_new("last_seen")?, bytes(42))
        .await?;
    Ok((session, event))
}

/// `finalize` over a mix of commit modes stages every `ReadCommitted`
/// collection as a provisional cell and writes every `ReadUncommitted`
/// collection resolved. Assertions are set-keyed by collection id, never by
/// staged-set iteration order (the concurrent fan-out makes order
/// nondeterministic).
#[tokio::test]
async fn finalize_stages_mixed_collections_by_mode() -> Result<()> {
    let cell = MemoryCellStore::new();
    let manager = acquire(&provider_with(
        cell.clone(),
        FixedOracle::committed(),
        registry_with_mixed()?,
    ))
    .await?;
    let key: Key = Arc::from("k");

    let (session, event) = write_mixed(&manager, &key).await?;
    assert_eq!(session.finalize().await?, FinalizeOutcome::Staged);

    assert_eq!(
        cell.read_cell(&id_for(&key, "cart")?, &()).await?,
        Cell::Provisional(ProvisionalCell::new(Some(bytes(7)), None, event)),
        "a ReadCommitted collection stages a provisional cell over its absent base",
    );
    assert_eq!(
        cell.read_cell(&id_for(&key, "wishlist")?, &()).await?,
        Cell::Provisional(ProvisionalCell::new(Some(bytes(13)), None, event)),
        "the second ReadCommitted collection stages its own provisional cell",
    );
    assert_eq!(
        cell.read_cell(&id_for(&key, "last_seen")?, &()).await?,
        Cell::Resolved(Committed::new(Some(bytes(42)))),
        "a ReadUncommitted collection writes a resolved value at stage time",
    );
    Ok(())
}

/// After `finalize`, `commit_apply` promotes every staged `ReadCommitted` cell
/// to its committed data; the `ReadUncommitted` cell was already resolved.
#[tokio::test]
async fn commit_apply_promotes_all_staged_cells() -> Result<()> {
    let cell = MemoryCellStore::new();
    let manager = acquire(&provider_with(
        cell.clone(),
        FixedOracle::committed(),
        registry_with_mixed()?,
    ))
    .await?;
    let key: Key = Arc::from("k");

    let (session, _event) = write_mixed(&manager, &key).await?;
    assert_eq!(session.finalize().await?, FinalizeOutcome::Staged);
    assert_eq!(session.commit_apply().await, ApplyOutcome::Resolved);

    assert_eq!(
        cell.read_cell(&id_for(&key, "cart")?, &()).await?,
        Cell::Resolved(Committed::new(Some(bytes(7)))),
        "the first ReadCommitted cell promotes to its staged data",
    );
    assert_eq!(
        cell.read_cell(&id_for(&key, "wishlist")?, &()).await?,
        Cell::Resolved(Committed::new(Some(bytes(13)))),
        "the second ReadCommitted cell promotes to its staged data",
    );
    assert_eq!(
        cell.read_cell(&id_for(&key, "last_seen")?, &()).await?,
        Cell::Resolved(Committed::new(Some(bytes(42)))),
        "the ReadUncommitted cell stays resolved",
    );
    Ok(())
}

/// After `finalize`, `rollback_aborted` rolls every staged `ReadCommitted` cell
/// back to its committed base (here absent).
#[tokio::test]
async fn rollback_aborted_rolls_back_all_staged_cells() -> Result<()> {
    let cell = MemoryCellStore::new();
    let manager = acquire(&provider_with(
        cell.clone(),
        FixedOracle::committed(),
        registry_with_mixed()?,
    ))
    .await?;
    let key: Key = Arc::from("k");

    let (session, _event) = write_mixed(&manager, &key).await?;
    assert_eq!(session.finalize().await?, FinalizeOutcome::Staged);
    assert_eq!(session.rollback_aborted().await, ApplyOutcome::Resolved);

    assert_eq!(
        cell.read_cell(&id_for(&key, "cart")?, &()).await?,
        Cell::Resolved(Committed::new(None)),
        "the first ReadCommitted cell rolls back to its absent base",
    );
    assert_eq!(
        cell.read_cell(&id_for(&key, "wishlist")?, &()).await?,
        Cell::Resolved(Committed::new(None)),
        "the second ReadCommitted cell rolls back to its absent base",
    );
    Ok(())
}

/// A cell store that fails the promote (`mark_resolved`) arm with a *permanent*
/// error for one named collection and delegates everything else (including the
/// descriptor-identity rows acquisition reads) to an inner memory store. Drives
/// the session-level best-effort `commit_apply` path: one poisoned cell must be
/// left provisional for the sweep without cancelling its siblings' promotes.
#[derive(Clone)]
struct PoisonPromoteCell {
    inner: MemoryCellStore,
    poison: StateName,
}

#[derive(Debug, Error)]
#[error("permanent promote poison")]
struct PromotePoison;

impl ClassifyError for PromotePoison {
    fn classify_error(&self) -> ErrorCategory {
        ErrorCategory::Permanent
    }
}

impl CellStore<ValueKind> for PoisonPromoteCell {
    type Error = PromotePoison;

    async fn read_cell<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
        addr: &'a (),
    ) -> Result<Cell, Self::Error> {
        self.inner.read_cell(collection, addr).await.map_err(never)
    }

    fn provisional_cells<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
    ) -> impl Stream<Item = Result<((), ProvisionalCell), Self::Error>> + Send + 'a {
        self.inner
            .provisional_cells(collection)
            .map(|item| item.map_err(never))
    }

    async fn write_provisional<'a>(
        &'a self,
        collection: &'a CollectionRef<ValueKind>,
        addr: &'a (),
        write: &'a ProvisionalWrite,
    ) -> Result<(), Self::Error> {
        self.inner
            .write_provisional(collection, addr, write)
            .await
            .map_err(never)
    }

    async fn write_resolved<'a>(
        &'a self,
        collection: &'a CollectionRef<ValueKind>,
        addr: &'a (),
        data: Option<&'a Bytes>,
    ) -> Result<(), Self::Error> {
        self.inner
            .write_resolved(collection, addr, data)
            .await
            .map_err(never)
    }

    async fn mark_resolved<'a>(
        &'a self,
        collection: &'a CollectionRef<ValueKind>,
        addr: &'a (),
    ) -> Result<(), Self::Error> {
        if *collection.id().name() == self.poison {
            return Err(PromotePoison);
        }
        self.inner
            .mark_resolved(collection, addr)
            .await
            .map_err(never)
    }
}

impl DescriptorIdentityStore for PoisonPromoteCell {
    type Error = PromotePoison;

    async fn read_descriptor_identities(
        &self,
        segment_id: SegmentId,
    ) -> Result<Vec<DurableDescriptorIdentity>, Self::Error> {
        self.inner
            .read_descriptor_identities(segment_id)
            .await
            .map_err(never)
    }

    async fn write_descriptor_identities(
        &self,
        segment_id: SegmentId,
        rows: Vec<DurableDescriptorIdentity>,
    ) -> Result<(), Self::Error> {
        self.inner
            .write_descriptor_identities(segment_id, rows)
            .await
            .map_err(never)
    }
}

/// Lifts the inner store's [`Infallible`] error into [`PromotePoison`]; never
/// called, because [`Infallible`] is uninhabited.
fn never(error: Infallible) -> PromotePoison {
    match error {}
}

type PoisonProvider = StateManagerProvider<
    SharedStateBackend<
        PoisonPromoteCell,
        FixedOracle,
        MemoryCommittedCache,
        MemoryDirtyValueStoreProvider,
    >,
    MemoryLoader<serde_json::Value>,
>;

/// A provider over a [`PoisonPromoteCell`] sharing `cell` so a test can read
/// the durable cells back after `commit_apply`.
fn poison_provider(
    cell: PoisonPromoteCell,
    registry: Arc<CollectionDefRegistry>,
) -> PoisonProvider {
    StateManagerProvider::new(
        SharedStateBackend::new(
            cell,
            FixedOracle::committed(),
            MemoryCommittedCache::new(),
            MemoryDirtyValueStoreProvider,
        ),
        MemoryLoader::new(),
        registry,
        Arc::from("test-group"),
        CompactDuration::new(30),
    )
}

/// `commit_apply` is best-effort under a per-cell promote failure: the poisoned
/// `ReadCommitted` cell is left provisional (for the sweep) and the outcome is
/// `Incomplete`, but every healthy sibling still promotes. This pins the
/// `fold`-not-`try_fold` reduction — one failure must never cancel the rest of
/// the concurrent fan-out.
#[tokio::test]
async fn commit_apply_is_best_effort_when_one_promote_fails() -> Result<()> {
    let cell = PoisonPromoteCell {
        inner: MemoryCellStore::new(),
        poison: StateName::try_new("wishlist")?,
    };
    let manager = poison_provider(cell.clone(), registry_with_mixed()?)
        .acquire(Topic::from("t"), 0)
        .await
        .map_err(|e| eyre!("acquire failed: {e}"))?;
    let key: Key = Arc::from("k");

    let event = timer_event(&key);
    let session = manager.session(key.clone(), event, termination());
    session
        .set_state_cell(&StateName::try_new("cart")?, bytes(7))
        .await?;
    session
        .set_state_cell(&StateName::try_new("wishlist")?, bytes(13))
        .await?;
    assert_eq!(session.finalize().await?, FinalizeOutcome::Staged);

    assert_eq!(
        session.commit_apply().await,
        ApplyOutcome::Incomplete,
        "a failed promote yields Incomplete so the boundary leaves the backstop armed",
    );
    assert_eq!(
        cell.read_cell(&id_for(&key, "cart")?, &()).await?,
        Cell::Resolved(Committed::new(Some(bytes(7)))),
        "the healthy sibling still promotes despite the poisoned cell failing",
    );
    assert_eq!(
        cell.read_cell(&id_for(&key, "wishlist")?, &()).await?,
        Cell::Provisional(ProvisionalCell::new(Some(bytes(13)), None, event)),
        "the poisoned cell stays provisional for the recovery sweep",
    );
    Ok(())
}
