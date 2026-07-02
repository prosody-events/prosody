//! Directed tests for [`StateManager::recover`] and the multi-collection
//! lifecycle the partition loop drives.
//!
//! The per-collection resolution (`resolve_cell`), sweep idempotence, and the
//! commit oracle are covered in the cell suite and `commit_manager::tests`.
//! These pin the glue `recover` and the real session add on top: resolving a
//! provisional cell staged under a real [`EventRef`] through the oracle (commit
//! **and** abort arms), clearing the per-key armed flag, unscheduling the
//! backstop **only** when every cell resolved (no-strand, inv 6), and the
//! mixed-mode `finalize`/`commit_apply`/`rollback_aborted` over the real
//! session. The manager is Kafka-agnostic, so these mint a session from a key
//! and an `EventRef` directly; all are broker-free.
//!
//! A still-provisional cell is observed through `provisional_cells` (the
//! resolving `get` would mutate it); a known-resolved cell through `get`.

use super::*;
use crate::consumer::partition::ShutdownPhase;
use crate::heartbeat::HeartbeatRegistry;
use crate::loader::MemoryLoader;
use crate::state::cell::{Committed, ProvisionalCell};
use crate::state::cell_key::{CellKey, Coordinate, Section};
use crate::state::descriptor::{ValueDescriptor, value_state};
use crate::state::memory::{MemoryCellStore, MemoryCells, MemoryDescriptorIdentityStore};
use crate::state::registry::CollectionDef;
use crate::state::session::CellSession;
use crate::state::session::sealed::{ApplyOutcome, FinalizeOutcome, StateLifecycle};
use crate::state::tests::cell_suite::{FailingCellStore, bytes};
use crate::state::{CommitDecision, CommitMode, EventRef, SharedStateBackend, TimerEventRef};
use crate::telemetry::Telemetry;
use crate::timers::datetime::CompactDateTime;
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

type MemCell = MemoryCellStore<FixedOracle>;
type TestBackend = SharedStateBackend<MemCell, MemoryDescriptorIdentityStore, FixedOracle>;
type TestProvider = StateManagerProvider<TestBackend, MemoryLoader<serde_json::Value>>;
type TestManager =
    StateManager<<TestBackend as StateBackendFactory>::Backend, MemoryLoader<serde_json::Value>>;
type TestSession = <TestManager as PartitionStateManager>::Session;

/// A `FailingCellStore` over a memory store, poisoning one collection's
/// promote.
type PoisonCell = FailingCellStore<MemCell>;
type PoisonBackend = SharedStateBackend<PoisonCell, MemoryDescriptorIdentityStore, FixedOracle>;
type PoisonProvider = StateManagerProvider<PoisonBackend, MemoryLoader<serde_json::Value>>;

fn cart() -> ValueDescriptor {
    value_state("cart")
}

fn wishlist() -> ValueDescriptor {
    value_state("wishlist")
}

fn last_seen() -> ValueDescriptor {
    value_state("last_seen")
}

/// The single Value cell (`ValueNs::Entries`, empty coordinate).
fn value_cell() -> CellKey {
    CellKey {
        section: Section::new(0),
        coordinate: Coordinate::empty(),
    }
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

/// A memory cell store resolving through `oracle`, binding `registry`'s TTLs.
fn cell_store(oracle: FixedOracle, registry: &Arc<CollectionDefRegistry>) -> MemCell {
    MemoryCellStore::new(MemoryCells::new(), oracle, registry.clone())
}

/// A provider sharing `cell` (so a test can read the durable cell back after
/// `recover`) with the given oracle and registry.
fn provider_with(
    cell: MemCell,
    oracle: FixedOracle,
    registry: Arc<CollectionDefRegistry>,
) -> TestProvider {
    StateManagerProvider::new(
        SharedStateBackend::new(cell, MemoryDescriptorIdentityStore::new(), oracle),
        MemoryLoader::new(),
        registry,
        Arc::from("test-group"),
        CompactDuration::new(30),
    )
}

/// A provider over a `FailingCellStore` sharing `cell` so a test can read the
/// durable cells back.
fn poison_provider(cell: PoisonCell, registry: Arc<CollectionDefRegistry>) -> PoisonProvider {
    StateManagerProvider::new(
        SharedStateBackend::new(
            cell,
            MemoryDescriptorIdentityStore::new(),
            FixedOracle::committed(),
        ),
        MemoryLoader::new(),
        registry,
        Arc::from("test-group"),
        CompactDuration::new(30),
    )
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
/// re-derived through [`partition_segment_id`] so it matches what `acquire`
/// wrote, exactly as recovery does.
fn id_for(key: &Key, name: &str) -> Result<CollectionId> {
    Ok(CollectionId::new(
        StateKey::new(
            partition_segment_id(Topic::from("t"), 0, "test-group"),
            key.clone(),
        ),
        StateType::Application,
        StateName::try_new(name)?,
    ))
}

/// The still-provisional Value cell of a collection, if any (the non-resolving
/// way to observe staged state — `get` would resolve and mutate it).
async fn staged_cell<S>(store: &S, id: &CollectionId) -> Result<Option<ProvisionalCell>>
where
    S: CellStore,
{
    let stream = store.provisional_cells(id);
    futures::pin_mut!(stream);
    let mut found = None;
    while let Some(item) = stream.next().await {
        let (_, prov) = item.map_err(|e| eyre!("{e}"))?;
        found = Some(prov);
    }
    Ok(found)
}

/// The resolved committed value of a collection (call only on a known-resolved
/// cell — `get` resolves a provisional one).
async fn committed<S>(store: &S, id: &CollectionId) -> Result<Option<Bytes>>
where
    S: CellStore,
{
    let probe = EventRef::Message {
        dedup_id: Uuid::from_u128(u128::MAX),
    };
    store
        .get(id, &value_cell(), probe)
        .await
        .map(Committed::into_inner)
        .map_err(|e| eyre!("{e}"))
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

/// The timer `EventRef` the lifecycle tests stage under.
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

/// Stages a `cart` provisional cell under a timer `EventRef` through a real
/// session (the crash window: the cell is staged, its promote never ran), and
/// marks the key armed as the durability boundary would have.
async fn stage_under_timer(
    session: TestSession,
    manager: &TestManager,
    key: &Key,
    value: u8,
) -> Result<()> {
    session
        .set(
            StateType::Application,
            &StateName::try_new("cart")?,
            &value_cell(),
            &bytes(value),
        )
        .await?;
    assert_eq!(session.finalize().await?, FinalizeOutcome::Staged);
    // `insert_async` returns `Err(...)` if already present — harmless; the entry
    // is idempotent. The stored fire is immaterial to `recover`, which only
    // removes the key.
    let _ = manager
        .inner
        .armed
        .insert_async(key.clone(), CompactDateTime::now()?)
        .await;
    Ok(())
}

/// `recover` resolves a committed provisional cell to its `data`, clears the
/// per-key armed flag, and unschedules the backstop timer.
#[tokio::test]
async fn recover_promotes_committed_cell_clears_armed_and_timer() -> Result<()> {
    let oracle = FixedOracle::committed();
    let registry = registry_with_cart()?;
    let cell = cell_store(oracle.clone(), &registry);
    let manager = acquire(&provider_with(cell.clone(), oracle, registry)).await?;
    let (_stream, timers, _shutdown_tx) = timer_manager().await?;
    let key: Key = Arc::from("k");

    let session = manager
        .session(key.clone(), timer_event(&key), termination())
        .handle();
    stage_under_timer(session, &manager, &key, 7).await?;
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

    assert_eq!(
        committed(&cell, &id_for(&key, "cart")?).await?,
        Some(bytes(7)),
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
    let oracle = FixedOracle::not_committed();
    let registry = registry_with_cart()?;
    let cell = cell_store(oracle.clone(), &registry);
    let manager = acquire(&provider_with(cell.clone(), oracle, registry)).await?;
    let (_stream, timers, _shutdown_tx) = timer_manager().await?;
    let key: Key = Arc::from("k");

    let session = manager
        .session(key.clone(), timer_event(&key), termination())
        .handle();
    stage_under_timer(session, &manager, &key, 99).await?;

    manager
        .recover(key.clone(), &timers)
        .await
        .map_err(|e| eyre!("recover failed: {e}"))?;

    assert_eq!(
        committed(&cell, &id_for(&key, "cart")?).await?,
        None,
        "an uncommitted cell rolls back to its (absent) committed base",
    );
    Ok(())
}

/// No-strand (inv 6): when a collection's resolution fails permanently, the
/// sweep returns "not all resolved", so `recover` leaves the backstop timer
/// scheduled for a later sweep / first-touch rather than stranding the
/// unresolved cell.
#[tokio::test]
async fn recover_leaves_backstop_when_resolution_fails() -> Result<()> {
    let registry = registry_with_cart()?;
    let inner = cell_store(FixedOracle::committed(), &registry);
    let cell = FailingCellStore::new(inner, StateName::try_new("cart")?);
    let manager = poison_provider(cell, registry)
        .acquire(Topic::from("t"), 0)
        .await
        .map_err(|e| eyre!("acquire failed: {e}"))?;
    let (_stream, timers, _shutdown_tx) = timer_manager().await?;
    let key: Key = Arc::from("k");

    let session = manager
        .session(key.clone(), timer_event(&key), termination())
        .handle();
    session
        .set(
            StateType::Application,
            &StateName::try_new("cart")?,
            &value_cell(),
            &bytes(7),
        )
        .await?;
    assert_eq!(session.finalize().await?, FinalizeOutcome::Staged);
    let _ = manager
        .inner
        .armed
        .insert_async(key.clone(), CompactDateTime::now()?)
        .await;

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

    assert!(
        !timers
            .scheduled_times(&key, TimerType::StateRecovery)
            .await?
            .is_empty(),
        "a failed resolution must leave the backstop armed (no-strand)",
    );
    Ok(())
}

/// Buffers writes to two `ReadCommitted` collections (`cart`, `wishlist`) and
/// one `ReadUncommitted` collection (`last_seen`) through one session.
async fn write_mixed(manager: &TestManager, key: &Key) -> Result<(TestSession, EventRef)> {
    let event = timer_event(key);
    let session = manager.session(key.clone(), event, termination()).handle();
    session
        .set(
            StateType::Application,
            &StateName::try_new("cart")?,
            &value_cell(),
            &bytes(7),
        )
        .await?;
    session
        .set(
            StateType::Application,
            &StateName::try_new("wishlist")?,
            &value_cell(),
            &bytes(13),
        )
        .await?;
    session
        .set(
            StateType::Application,
            &StateName::try_new("last_seen")?,
            &value_cell(),
            &bytes(42),
        )
        .await?;
    Ok((session, event))
}

/// `finalize` over a mix of commit modes stages every `ReadCommitted`
/// collection as a provisional cell and writes every `ReadUncommitted`
/// collection resolved.
#[tokio::test]
async fn finalize_stages_mixed_collections_by_mode() -> Result<()> {
    let oracle = FixedOracle::committed();
    let registry = registry_with_mixed()?;
    let cell = cell_store(oracle.clone(), &registry);
    let manager = acquire(&provider_with(cell.clone(), oracle, registry)).await?;
    let key: Key = Arc::from("k");

    let (session, event) = write_mixed(&manager, &key).await?;
    assert_eq!(session.finalize().await?, FinalizeOutcome::Staged);

    assert_eq!(
        staged_cell(&cell, &id_for(&key, "cart")?).await?,
        Some(ProvisionalCell::new(Some(bytes(7)), None, event)),
        "a ReadCommitted collection stages a provisional cell over its absent base",
    );
    assert_eq!(
        staged_cell(&cell, &id_for(&key, "wishlist")?).await?,
        Some(ProvisionalCell::new(Some(bytes(13)), None, event)),
        "the second ReadCommitted collection stages its own provisional cell",
    );
    assert!(
        staged_cell(&cell, &id_for(&key, "last_seen")?)
            .await?
            .is_none(),
        "a ReadUncommitted collection writes resolved, leaving no provisional cell",
    );
    assert_eq!(
        committed(&cell, &id_for(&key, "last_seen")?).await?,
        Some(bytes(42)),
        "a ReadUncommitted collection writes its resolved value at stage time",
    );
    Ok(())
}

/// After `finalize`, `commit_apply` promotes every staged `ReadCommitted` cell
/// to its committed data; the `ReadUncommitted` cell was already resolved.
#[tokio::test]
async fn commit_apply_promotes_all_staged_cells() -> Result<()> {
    let oracle = FixedOracle::committed();
    let registry = registry_with_mixed()?;
    let cell = cell_store(oracle.clone(), &registry);
    let manager = acquire(&provider_with(cell.clone(), oracle, registry)).await?;
    let key: Key = Arc::from("k");

    let (session, _event) = write_mixed(&manager, &key).await?;
    assert_eq!(session.finalize().await?, FinalizeOutcome::Staged);
    assert_eq!(session.commit_apply().await, ApplyOutcome::Resolved);

    assert_eq!(
        committed(&cell, &id_for(&key, "cart")?).await?,
        Some(bytes(7))
    );
    assert_eq!(
        committed(&cell, &id_for(&key, "wishlist")?).await?,
        Some(bytes(13))
    );
    assert_eq!(
        committed(&cell, &id_for(&key, "last_seen")?).await?,
        Some(bytes(42))
    );
    Ok(())
}

/// After `finalize`, `rollback_aborted` rolls every staged `ReadCommitted` cell
/// back to its committed base (here absent).
#[tokio::test]
async fn rollback_aborted_rolls_back_all_staged_cells() -> Result<()> {
    let oracle = FixedOracle::committed();
    let registry = registry_with_mixed()?;
    let cell = cell_store(oracle.clone(), &registry);
    let manager = acquire(&provider_with(cell.clone(), oracle, registry)).await?;
    let key: Key = Arc::from("k");

    let (session, _event) = write_mixed(&manager, &key).await?;
    assert_eq!(session.finalize().await?, FinalizeOutcome::Staged);
    assert_eq!(session.rollback_aborted().await, ApplyOutcome::Resolved);

    assert_eq!(committed(&cell, &id_for(&key, "cart")?).await?, None);
    assert_eq!(committed(&cell, &id_for(&key, "wishlist")?).await?, None);
    Ok(())
}

/// `commit_apply` is best-effort under a per-cell promote failure: the poisoned
/// `ReadCommitted` cell is left provisional (for the sweep) and the outcome is
/// `Incomplete`, but every healthy sibling still promotes. Pins the
/// `fold`-not-`try_fold` reduction — one failure must never cancel the rest.
#[tokio::test]
async fn commit_apply_is_best_effort_when_one_promote_fails() -> Result<()> {
    let registry = registry_with_mixed()?;
    let inner = cell_store(FixedOracle::committed(), &registry);
    let cell = FailingCellStore::new(inner, StateName::try_new("wishlist")?);
    let manager = poison_provider(cell.clone(), registry)
        .acquire(Topic::from("t"), 0)
        .await
        .map_err(|e| eyre!("acquire failed: {e}"))?;
    let key: Key = Arc::from("k");

    let event = timer_event(&key);
    let session = manager.session(key.clone(), event, termination()).handle();
    session
        .set(
            StateType::Application,
            &StateName::try_new("cart")?,
            &value_cell(),
            &bytes(7),
        )
        .await?;
    session
        .set(
            StateType::Application,
            &StateName::try_new("wishlist")?,
            &value_cell(),
            &bytes(13),
        )
        .await?;
    assert_eq!(session.finalize().await?, FinalizeOutcome::Staged);

    assert_eq!(
        session.commit_apply().await,
        ApplyOutcome::Incomplete,
        "a failed promote yields Incomplete so the boundary leaves the backstop armed",
    );
    assert_eq!(
        committed(&cell, &id_for(&key, "cart")?).await?,
        Some(bytes(7)),
        "the healthy sibling still promotes despite the poisoned cell failing",
    );
    assert_eq!(
        staged_cell(&cell, &id_for(&key, "wishlist")?).await?,
        Some(ProvisionalCell::new(Some(bytes(13)), None, event)),
        "the poisoned cell stays provisional for the recovery sweep",
    );
    Ok(())
}
