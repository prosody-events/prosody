//! Directed tests for [`StateManager::recover`] and the multi-collection
//! lifecycle the partition loop drives.
//!
//! The per-collection resolution (`resolve_cell`), sweep idempotence, and the
//! commit oracle are proven by the cell suite and `state::commit::tests`.
//! These pin the glue `recover` and the real session add on top: resolving a
//! provisional cell staged under a real [`EventRef`] through the oracle (commit
//! **and** abort arms), clearing the per-key armed flag, unscheduling the
//! backstop **only** when every cell is resolved (no strand-back), and the
//! mixed-mode `finalize` + receipt settle (`promote`/`rollback`) over the real
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
use crate::state::descriptor::{ValueDescriptor, value_state};
use crate::state::memory::{MemoryCellStore, MemoryCells, MemoryDescriptorIdentityStore};
use crate::state::registry::CollectionDef;
use crate::state::session::sealed::{ApplyOutcome, StateLifecycle};
use crate::state::session::{Finalized, KeyedStateSession};
use crate::state::tests::cell_suite::{FailingCellStore, bytes, value_cell};
use crate::state::tests::support::FixedOracle;
use crate::state::{
    CommitMode, EventRef, PartitionBackend, SharedStateBackend, StateBackend, TimerEventRef,
};
use crate::telemetry::Telemetry;
use crate::timers::datetime::CompactDateTime;
use crate::timers::store::memory::{InMemoryTriggerStore, memory_store};
use crate::timers::store::{Segment, SegmentVersion};
use crate::timers::{
    PendingTimer, TimerManagerConfig, TimerRequest, TimerSemaphores, TimerType, Trigger,
};
use bytes::Bytes;
use color_eyre::eyre::{Result, bail, eyre};
use futures::{Stream, StreamExt};
use quickcheck::{QuickCheck, TestResult};
use std::array::from_fn;
use std::sync::Arc;
use tokio::runtime::Builder;
use tokio::sync::{Semaphore, watch};
use tracing::Span;
use uuid::Uuid;

type MemCell = MemoryCellStore<FixedOracle>;
type TestBackend = SharedStateBackend<MemCell, MemoryDescriptorIdentityStore, FixedOracle>;
type TestProvider = StateManagerProvider<TestBackend, MemoryLoader<serde_json::Value>>;
type TestManager = StateManager<
    PartitionBackend<FixedOracle, MemoryDescriptorIdentityStore, MemCell>,
    MemoryLoader<serde_json::Value>,
>;
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

fn registry_with_cart() -> Result<Arc<CollectionDefRegistry>> {
    registry_with_cart_within(None)
}

/// Like [`registry_with_cart`] but binds a `recovery_within` bound to `cart`,
/// so a test can assert the reschedule-after-failed-sweep tightening.
fn registry_with_cart_within(
    within: Option<CompactDuration>,
) -> Result<Arc<CollectionDefRegistry>> {
    let mut registry = CollectionDefRegistry::default();
    registry.register(
        &cart(),
        CollectionDef {
            recovery_within: within,
            ..CollectionDef::new(None)
        },
    )?;
    Ok(Arc::new(registry))
}

/// A registry with three collections exercising both commit modes: two
/// `ReadCommitted` (`cart`, `wishlist`) that stage provisional cells and one
/// `ReadUncommitted` (`last_seen`) that writes resolved at finalize.
fn registry_with_mixed() -> Result<Arc<CollectionDefRegistry>> {
    let mut registry = CollectionDefRegistry::default();
    registry.register(&cart(), CollectionDef::new(None))?;
    registry.register(&wishlist(), CollectionDef::new(None))?;
    registry.register(
        &last_seen(),
        CollectionDef {
            commit_mode: CommitMode::ReadUncommitted,
            ..CollectionDef::new(None)
        },
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
        NoPublisher,
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
        NoPublisher,
        registry,
        Arc::from("test-group"),
        CompactDuration::new(30),
    )
}

/// A fresh in-memory trigger store standing in for the partition's store
/// handle; the [`SharedStateBackend`] providers here carry a pre-built
/// oracle, so the handle is accepted and ignored.
fn test_triggers() -> InMemoryTriggerStore {
    memory_store(Segment {
        id: Uuid::new_v4(),
        name: "test".to_owned(),
        slab_size: CompactDuration::new(300),
        version: SegmentVersion::V3,
    })
}

async fn acquire(provider: &TestProvider) -> Result<TestManager> {
    provider
        .acquire(Topic::from("t"), 0, test_triggers())
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
    impl Stream<Item = PendingTimer<InMemoryTriggerStore>>,
    TimerManager<InMemoryTriggerStore>,
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
        store,
        telemetry: telemetry.partition_sender(Topic::from("test"), 0),
        source: Arc::from(""),
    };
    let semaphores: Arc<TimerSemaphores> = Arc::new(from_fn(|_| Arc::new(Semaphore::new(64))));
    let (stream, manager) =
        TimerManager::new(config, HeartbeatRegistry::test(), shutdown_rx, semaphores)
            .await
            .map_err(|e| eyre!("{e}"))?;
    Ok((stream::iter(stream.await).flatten(), manager, shutdown_tx))
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

/// Stages one Value cell into `session`'s dirty overlay — the seed every
/// staging test starts from.
async fn seed_value<B: StateBackend, L>(
    session: &KeyedStateSession<B, L>,
    name: &StateName,
    value: u8,
) {
    let value = bytes(value);
    session
        .seed(StateType::Application, name, &value_cell(), Some(&value))
        .await;
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
    seed_value(&session, &StateName::try_new("cart")?, value).await;
    // The receipt is deliberately dropped: the crash window under test is
    // "staged, its promote never ran".
    assert!(matches!(session.finalize().await?, Finalized::Staged(_)));
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
/// per-key armed flag, and — on a fully resolved sweep — commits without ever
/// unscheduling the backstop (the fired trigger is a per-key singleton the arm
/// commits; recover never touches the timer store on success).
#[tokio::test]
async fn recover_promotes_committed_cell_clears_armed_and_leaves_backstop() -> Result<()> {
    let oracle = FixedOracle::committed();
    let registry = registry_with_cart()?;
    let cell = cell_store(oracle.clone(), &registry);
    let manager = acquire(&provider_with(cell.clone(), oracle, registry)).await?;
    let (_stream, timers, _shutdown_tx) = timer_manager().await?;
    let (_no_shutdown_tx, no_shutdown) = watch::channel(ShutdownPhase::default());
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

    assert!(
        matches!(
            manager.recover(key.clone(), &timers, &no_shutdown).await,
            SweepResolution::Commit(_)
        ),
        "a fully resolved sweep commits the fired trigger"
    );

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
        !timers
            .scheduled_times(&key, TimerType::StateRecovery)
            .await?
            .is_empty(),
        "recover never unschedules the backstop; the arm commits the fired trigger",
    );
    Ok(())
}

/// A redelivery sweep resolves committed state without consuming a standing
/// backstop.
#[tokio::test]
async fn resolve_redelivered_promotes_and_preserves_armed_key() -> Result<()> {
    let oracle = FixedOracle::committed();
    let registry = registry_with_cart()?;
    let cell = cell_store(oracle.clone(), &registry);
    let manager = acquire(&provider_with(cell.clone(), oracle, registry)).await?;
    let (_stream, timers, _shutdown_tx) = timer_manager().await?;
    let (_no_shutdown_tx, no_shutdown) = watch::channel(ShutdownPhase::default());
    let key: Key = Arc::from("redelivered");

    let session = manager
        .session(key.clone(), timer_event(&key), termination())
        .handle();
    stage_under_timer(session, &manager, &key, 11).await?;

    assert!(matches!(
        manager
            .resolve_redelivered(key.clone(), &timers, &no_shutdown)
            .await,
        SweepResolution::Commit(_)
    ));
    assert_eq!(
        committed(&cell, &id_for(&key, "cart")?).await?,
        Some(bytes(11)),
    );
    assert!(manager.inner.armed.contains_async(&key).await);
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

    let (_no_shutdown_tx, no_shutdown) = watch::channel(ShutdownPhase::default());
    let session = manager
        .session(key.clone(), timer_event(&key), termination())
        .handle();
    stage_under_timer(session, &manager, &key, 99).await?;

    assert!(
        matches!(
            manager.recover(key.clone(), &timers, &no_shutdown).await,
            SweepResolution::Commit(_)
        ),
        "a fully resolved rollback commits the fired trigger"
    );

    assert_eq!(
        committed(&cell, &id_for(&key, "cart")?).await?,
        None,
        "an uncommitted cell rolls back to its (absent) committed base",
    );
    Ok(())
}

/// Whether a rescheduled backstop's `fire` honors the tightened retry delay:
/// the 30s test floor tightened by `within`, floored at the 1s retry cadence,
/// bracketed by the `now` reads around `recover`.
fn fire_in_tightened_window(
    fire: CompactDateTime,
    before: CompactDateTime,
    after: CompactDateTime,
    within: Option<u32>,
) -> bool {
    let expected = within.map_or(30, |w| w.min(30)).max(1);
    (before.epoch_seconds() + expected..=after.epoch_seconds() + expected)
        .contains(&fire.epoch_seconds())
}

/// A failed sweep keeps an earlier timer or arms a timer with the tightened
/// delay. Shutdown alone can abort a sweep. Both recovery entry points follow
/// this rule.
#[test]
fn prop_recover_commits_unless_shutdown_interrupts_reschedule() {
    fn property(
        category_sel: u8,
        shutdown: bool,
        within_sel: Option<u16>,
        redelivered: bool,
        standing_sel: u8,
    ) -> TestResult {
        let category = match category_sel % 3 {
            0 => ErrorCategory::Permanent,
            1 => ErrorCategory::Transient,
            _ => ErrorCategory::Terminal,
        };
        // Fold the raw bound into 0..60s so it lands on both sides of the 30s
        // floor (and on the 0 → 1s cadence floor) with real probability.
        let within = within_sel.map(|w| u32::from(w % 60));
        let Ok(runtime) = Builder::new_current_thread().enable_all().build() else {
            return TestResult::error("failed to build runtime");
        };
        runtime.block_on(async move {
            match run_recovery_case(
                category,
                shutdown,
                within,
                redelivered,
                if standing_sel % 61 == 60 {
                    600
                } else {
                    u32::from(standing_sel % 61)
                },
            )
            .await
            {
                Ok(result) => result,
                Err(e) => TestResult::error(format!("setup failed: {e}")),
            }
        })
    }

    QuickCheck::new().quickcheck(property as fn(u8, bool, Option<u16>, bool, u8) -> TestResult);
}

async fn run_recovery_case(
    category: ErrorCategory,
    shutdown: bool,
    within: Option<u32>,
    redelivered: bool,
    standing_offset: u32,
) -> Result<TestResult> {
    let cart = StateName::try_new("cart")?;
    let registry = registry_with_cart_within(within.map(CompactDuration::new))?;
    let inner = cell_store(FixedOracle::committed(), &registry);
    let cell = FailingCellStore::new_with_category(inner, cart.clone(), category);
    let manager = poison_provider(cell, registry)
        .acquire(Topic::from("t"), 0, test_triggers())
        .await
        .map_err(|e| eyre!("acquire failed: {e}"))?;
    let (_stream, timers, _shutdown_tx) = timer_manager().await?;
    let (shutdown_tx, shutdown_rx) = watch::channel(ShutdownPhase::default());
    if shutdown {
        shutdown_tx.send_replace(ShutdownPhase::Cancelling);
    }
    let key: Key = Arc::from("k");

    // Stage a committed cell whose sweep-time promote hits the poison.
    let session = manager
        .session(key.clone(), timer_event(&key), termination())
        .handle();
    seed_value(&session, &cart, 7).await;
    if !matches!(session.finalize().await?, Finalized::Staged(_)) {
        return Ok(TestResult::error("expected the cell to stage"));
    }

    // Seed a standing timer on either side of the recovery delay.
    let standing = CompactDateTime::now()?.add_duration(CompactDuration::new(standing_offset))?;
    timers
        .schedule(TimerRequest::new(
            key.clone(),
            standing,
            TimerType::StateRecovery,
            Span::current(),
        ))
        .await?;
    let _ = manager
        .inner
        .armed
        .insert_async(key.clone(), standing)
        .await;

    let before = CompactDateTime::now()?;
    let resolution = if redelivered {
        manager
            .resolve_redelivered(key.clone(), &timers, &shutdown_rx)
            .await
    } else {
        manager.recover(key.clone(), &timers, &shutdown_rx).await
    };
    let after = CompactDateTime::now()?;
    let scheduled = timers
        .scheduled_times(&key, TimerType::StateRecovery)
        .await?;
    let armed = manager.inner.armed.contains_async(&key).await;

    Ok(check_recovery(RecoveryObservation {
        category,
        shutdown,
        within,
        redelivered,
        resolution,
        scheduled,
        armed,
        standing,
        before,
        after,
    }))
}

struct RecoveryObservation {
    category: ErrorCategory,
    shutdown: bool,
    within: Option<u32>,
    redelivered: bool,
    resolution: SweepResolution,
    scheduled: Vec<CompactDateTime>,
    armed: bool,
    standing: CompactDateTime,
    before: CompactDateTime,
    after: CompactDateTime,
}

fn check_recovery(observed: RecoveryObservation) -> TestResult {
    let RecoveryObservation {
        category,
        shutdown,
        within,
        redelivered,
        resolution,
        scheduled,
        armed,
        standing,
        before,
        after,
    } = observed;
    if scheduled.len() != 1 {
        return TestResult::error(format!(
            "exactly one StateRecovery trigger must exist, got {}",
            scheduled.len()
        ));
    }
    let rescheduled = scheduled[0] != standing;
    let delay = within.map_or(30, |within| within.min(30)).max(1);
    let earlier = if rescheduled {
        scheduled[0] < standing && fire_in_tightened_window(scheduled[0], before, after, within)
    } else {
        standing.epoch_seconds() <= after.epoch_seconds() + delay
    };
    let retry = matches!(category, ErrorCategory::Transient | ErrorCategory::Terminal);
    let ok = match (redelivered, retry, shutdown) {
        (true, true, true) => resolution == SweepResolution::Abort && !rescheduled && armed,
        (_, true, false) => matches!(resolution, SweepResolution::Commit(_)) && earlier && armed,
        (true, false, _) => {
            matches!(resolution, SweepResolution::Commit(_)) && !rescheduled && armed
        }
        (false, true, true) => resolution == SweepResolution::Abort && !rescheduled && !armed,
        (false, false, _) => {
            matches!(resolution, SweepResolution::Commit(_)) && !rescheduled && !armed
        }
    };
    if ok {
        TestResult::passed()
    } else {
        TestResult::error(format!(
            "category={category:?} shutdown={shutdown}: resolution={resolution:?} \
             rescheduled={rescheduled} armed={armed} standing={standing:?} \
             scheduled={scheduled:?} within={within:?}"
        ))
    }
}

/// Buffers writes to two `ReadCommitted` collections (`cart`, `wishlist`) and
/// one `ReadUncommitted` collection (`last_seen`) through one session.
async fn write_mixed(manager: &TestManager, key: &Key) -> Result<(TestSession, EventRef)> {
    let event = timer_event(key);
    let session = manager.session(key.clone(), event, termination()).handle();
    seed_value(&session, &StateName::try_new("cart")?, 7).await;
    seed_value(&session, &StateName::try_new("wishlist")?, 13).await;
    seed_value(&session, &StateName::try_new("last_seen")?, 42).await;
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
    // The receipt is dropped: the probes below observe the stage itself.
    assert!(matches!(session.finalize().await?, Finalized::Staged(_)));

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

/// After `finalize`, the receipt's `promote` promotes every staged
/// `ReadCommitted` cell to its committed data; the `ReadUncommitted` cell was
/// already resolved.
#[tokio::test]
async fn promote_promotes_all_staged_cells() -> Result<()> {
    let oracle = FixedOracle::committed();
    let registry = registry_with_mixed()?;
    let cell = cell_store(oracle.clone(), &registry);
    let manager = acquire(&provider_with(cell.clone(), oracle, registry)).await?;
    let key: Key = Arc::from("k");

    let (session, _event) = write_mixed(&manager, &key).await?;
    let Finalized::Staged(staged) = session.finalize().await? else {
        bail!("expected a staged receipt");
    };
    assert_eq!(staged.certify().promote().await, ApplyOutcome::Resolved);

    // Non-resolving probes: `committed()` reads through a resolving `get`, which
    // heals a still-provisional cell to its "as if committed" value — so a
    // promote that skipped `commit_provisional` would read back identically.
    // Assert the provisional cell is actually gone before trusting the value.
    assert!(
        staged_cell(&cell, &id_for(&key, "cart")?).await?.is_none(),
        "promote commits the cell — a skipped commit would leave it provisional",
    );
    assert!(
        staged_cell(&cell, &id_for(&key, "wishlist")?)
            .await?
            .is_none(),
        "promote commits the second cell too, leaving no provisional marker",
    );
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

/// After `finalize`, the receipt's `rollback` rolls every staged
/// `ReadCommitted` cell back to its committed base (here absent) — asserted
/// through `committed()` reads, since `rollback` returns nothing.
#[tokio::test]
async fn rollback_rolls_back_all_staged_cells() -> Result<()> {
    let oracle = FixedOracle::committed();
    let registry = registry_with_mixed()?;
    let cell = cell_store(oracle.clone(), &registry);
    let manager = acquire(&provider_with(cell.clone(), oracle, registry)).await?;
    let key: Key = Arc::from("k");

    let (session, _event) = write_mixed(&manager, &key).await?;
    let Finalized::Staged(staged) = session.finalize().await? else {
        bail!("expected a staged receipt");
    };
    staged.rollback().await;

    assert_eq!(committed(&cell, &id_for(&key, "cart")?).await?, None);
    assert_eq!(committed(&cell, &id_for(&key, "wishlist")?).await?, None);
    Ok(())
}

/// The receipt's `promote` is best-effort under a per-cell promote failure:
/// the poisoned `ReadCommitted` cell is left provisional (for the sweep) and
/// the outcome is `Incomplete`, but every healthy sibling still promotes. Pins
/// the `fold`-not-`try_fold` reduction — one failure must never cancel the
/// rest.
#[tokio::test]
async fn promote_is_best_effort_when_one_fails() -> Result<()> {
    let registry = registry_with_mixed()?;
    let inner = cell_store(FixedOracle::committed(), &registry);
    let cell = FailingCellStore::new(inner, StateName::try_new("wishlist")?);
    let manager = poison_provider(cell.clone(), registry)
        .acquire(Topic::from("t"), 0, test_triggers())
        .await
        .map_err(|e| eyre!("acquire failed: {e}"))?;
    let key: Key = Arc::from("k");

    let event = timer_event(&key);
    let session = manager.session(key.clone(), event, termination()).handle();
    seed_value(&session, &StateName::try_new("cart")?, 7).await;
    seed_value(&session, &StateName::try_new("wishlist")?, 13).await;
    let Finalized::Staged(staged) = session.finalize().await? else {
        bail!("expected a staged receipt");
    };

    assert_eq!(
        staged.certify().promote().await,
        ApplyOutcome::Incomplete,
        "a failed promote yields Incomplete so the boundary leaves the backstop armed",
    );
    assert!(
        staged_cell(&cell, &id_for(&key, "cart")?).await?.is_none(),
        "promote commits the healthy cell — a skipped commit would leave it provisional",
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
