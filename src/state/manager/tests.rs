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
use crate::state::cell::{Cell, Committed};
use crate::state::descriptor::{ValueDescriptor, value_state};
use crate::state::memory::{MemoryCellStore, MemoryCommittedCache, MemoryDirtyValueStoreProvider};
use crate::state::registry::CollectionDef;
use crate::state::session::StateSession;
use crate::state::session::sealed::{FinalizeOutcome, StateLifecycle};
use crate::state::tests::value_suite::bytes;
use crate::state::{
    CommitDecision, EventRef, SharedStateBackend, StateName, StateType, TimerEventRef,
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
use color_eyre::eyre::{Result, eyre};
use futures::Stream;
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

fn cart() -> ValueDescriptor {
    value_state("cart")
}

fn registry_with_cart() -> Result<Arc<CollectionDefRegistry>> {
    let mut registry = CollectionDefRegistry::new(Some(CompactDuration::new(3_600)));
    registry.register(&cart(), CollectionDef::new(None))?;
    Ok(Arc::new(registry))
}

/// Builds a provider sharing `cell` (so a test can read the durable cell back
/// after `recover`) with the given oracle.
fn provider(cell: MemoryCellStore, oracle: FixedOracle) -> Result<TestProvider> {
    Ok(StateManagerProvider::new(
        SharedStateBackend::new(
            cell,
            oracle,
            MemoryCommittedCache::new(),
            MemoryDirtyValueStoreProvider,
        ),
        MemoryLoader::new(),
        registry_with_cart()?,
        Arc::from("test-group"),
        CompactDuration::new(30),
    ))
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

/// The `cart` collection identity for `key` in the fixed test segment —
/// re-derived through [`compute_segment_id`] so it matches what `acquire`
/// wrote, exactly as recovery does.
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
