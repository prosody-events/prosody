use std::array::from_fn;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use color_eyre::eyre::{Result, eyre};
use futures::{Stream, StreamExt, pin_mut};
use quickcheck::{Arbitrary, Gen, QuickCheck, TestResult};
use tokio::runtime::Builder;
use tokio::sync::{Semaphore, watch};
use tokio::task;
use tokio::time::{self, advance};
use tracing::Span;
use uuid::Uuid;

use crate::Key;
use crate::Topic;
use crate::consumer::Uncommitted;
use crate::consumer::middleware::deduplication::DeduplicationStore;
use crate::consumer::middleware::deduplication::memory::MemoryDeduplicationStore;
use crate::consumer::partition::ShutdownPhase;
use crate::heartbeat::HeartbeatRegistry;
use crate::telemetry::Telemetry;
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::timers::store::Segment;
use crate::timers::store::SegmentVersion;
use crate::timers::store::TriggerStore;
use crate::timers::store::adapter::TableAdapter;
use crate::timers::store::memory::{InMemoryTriggerStore, memory_store};
use crate::timers::{
    FiringTimer, PendingTimer, TimerManager, TimerManagerConfig, TimerRequest, TimerSemaphores,
    TimerType, Trigger,
};

use super::{CommitManager, StoreTagSource};

type TestManager = TimerManager<TableAdapter<InMemoryTriggerStore>>;
type TestCommitManager = CommitManager<MemoryDeduplicationStore, TestManager>;

fn test_semaphores() -> Arc<TimerSemaphores> {
    Arc::new(from_fn(|_| Arc::new(Semaphore::new(64))))
}

async fn setup() -> Result<(
    impl Stream<Item = PendingTimer<TableAdapter<InMemoryTriggerStore>>>,
    TestManager,
    watch::Sender<ShutdownPhase>,
)> {
    let (stream, manager, shutdown_tx, _store) = setup_with_store().await?;
    Ok((stream, manager, shutdown_tx))
}

async fn setup_with_store() -> Result<(
    impl Stream<Item = PendingTimer<TableAdapter<InMemoryTriggerStore>>>,
    TestManager,
    watch::Sender<ShutdownPhase>,
    TableAdapter<InMemoryTriggerStore>,
)> {
    let segment = Segment {
        id: Uuid::new_v4(),
        name: "test".to_owned(),
        slab_size: CompactDuration::new(300),
        version: SegmentVersion::V3,
    };
    let store = memory_store(segment);
    let (stream, manager, shutdown_tx) = manager_for_store(store.clone()).await?;
    Ok((stream, manager, shutdown_tx, store))
}

/// Builds a `TimerManager` over an existing store with the standard test
/// telemetry/heartbeat wiring. Shared by `setup_with_store` (fresh store) and
/// `fresh_commit_manager` (reopening a store to assert store-only oracle
/// state).
async fn manager_for_store(
    store: TableAdapter<InMemoryTriggerStore>,
) -> Result<(
    impl Stream<Item = PendingTimer<TableAdapter<InMemoryTriggerStore>>>,
    TestManager,
    watch::Sender<ShutdownPhase>,
)> {
    let (shutdown_tx, shutdown_rx) = watch::channel(ShutdownPhase::default());
    let telemetry = Telemetry::new();
    let config = TimerManagerConfig {
        name: "test".to_owned(),
        store,
        telemetry: telemetry.partition_sender(Topic::from("test"), 0),
        source: Arc::from(""),
    };
    let (stream, manager) = TimerManager::new(
        config,
        HeartbeatRegistry::test(),
        shutdown_rx,
        test_semaphores(),
    )
    .await
    .map_err(|e| eyre!("{e}"))?;
    Ok((stream, manager, shutdown_tx))
}

fn test_trigger(key: &str, offset: u32) -> Result<Trigger> {
    let time = CompactDateTime::now()?.add_duration(CompactDuration::new(offset))?;
    Ok(Trigger::new(
        Key::from(key),
        time,
        TimerType::Application,
        Span::current(),
    ))
}

fn commit_manager(timers: TestManager) -> TestCommitManager {
    CommitManager::new(MemoryDeduplicationStore::new(), timers)
}

async fn fresh_commit_manager(
    store: TableAdapter<InMemoryTriggerStore>,
) -> Result<TestCommitManager> {
    let (_stream, manager, _shutdown_tx) = manager_for_store(store).await?;
    Ok(commit_manager(manager))
}

/// Drives the next pending timer to its firing state: advances paused time,
/// lets the scheduler hand off the trigger, then claims it via `fire`.
/// Returns the active [`FiringTimer`] so the caller can capture its WAL tag
/// and choose the terminal step (commit / abort / reschedule).
async fn fire_next<S>(
    mut stream: Pin<&mut S>,
    advance_by: Duration,
) -> Result<FiringTimer<TableAdapter<InMemoryTriggerStore>>>
where
    S: Stream<Item = PendingTimer<TableAdapter<InMemoryTriggerStore>>>,
{
    advance(advance_by).await;
    task::yield_now().await;
    let pending = stream
        .next()
        .await
        .ok_or_else(|| eyre!("no pending timer fired"))?;
    pending
        .fire()
        .await
        .ok_or_else(|| eyre!("pending timer not active"))
}

/// Oracle: message not inserted → not committed.
#[tokio::test]
async fn message_not_committed_when_absent() -> Result<()> {
    time::pause();
    let (_stream, manager, _tx) = setup().await?;
    let oracle = commit_manager(manager);
    assert!(
        !oracle.is_message_committed(Uuid::new_v4()).await?,
        "absent UUID must not be committed"
    );
    Ok(())
}

/// Oracle: message inserted → committed.
#[tokio::test]
async fn message_committed_after_insert() -> Result<()> {
    time::pause();
    let (_stream, manager, _tx) = setup().await?;
    let dedup = MemoryDeduplicationStore::new();
    let id = Uuid::new_v4();
    dedup.insert(id).await.map_err(|e| eyre!("{e}"))?;
    let oracle = CommitManager::new(dedup, manager);
    assert!(
        oracle.is_message_committed(id).await?,
        "inserted UUID must be committed"
    );
    Ok(())
}

/// Oracle: timer row absent → committed (fired-and-removed path).
#[tokio::test]
async fn timer_committed_when_row_absent() -> Result<()> {
    time::pause();
    let (stream, manager, _tx) = setup().await?;
    pin_mut!(stream);
    let trigger = test_trigger("k", 1)?;
    manager.schedule_trigger(trigger.clone()).await?;

    let firing = fire_next(stream.as_mut(), Duration::from_secs(2)).await?;
    let wal_tag = firing.trigger().tag;
    firing.commit().await;

    let oracle = commit_manager(manager);
    assert!(
        oracle
            .is_timer_committed(&trigger.key, trigger.timer_type, trigger.time, wal_tag)
            .await?,
        "row absent after commit → committed"
    );
    Ok(())
}

/// Oracle: tag matches WAL → not committed.
#[tokio::test]
async fn timer_not_committed_when_tag_matches() -> Result<()> {
    time::pause();
    let (_stream, manager, _tx) = setup().await?;
    let trigger = test_trigger("k", 10)?;
    manager.schedule_trigger(trigger.clone()).await?;

    let current_tag = manager
        .current_tag(&trigger.key, trigger.time, trigger.timer_type)
        .await?
        .ok_or_else(|| eyre!("no tag"))?;

    let oracle = commit_manager(manager);
    assert!(
        !oracle
            .is_timer_committed(&trigger.key, trigger.timer_type, trigger.time, current_tag)
            .await?,
        "matching tag means not committed"
    );
    Ok(())
}

/// Oracle: tag differs from WAL → committed-and-rescheduled.
#[tokio::test]
async fn timer_committed_when_tag_differs() -> Result<()> {
    time::pause();
    let (stream, manager, _tx) = setup().await?;
    pin_mut!(stream);
    let trigger = test_trigger("k", 1)?;
    manager.schedule_trigger(trigger.clone()).await?;

    let firing = fire_next(stream.as_mut(), Duration::from_secs(2)).await?;
    let wal_tag = firing.trigger().tag;

    manager.schedule_trigger(trigger.clone()).await?; // → FiringRescheduled
    firing.commit().await; // → rotates tag

    let oracle = commit_manager(manager);
    assert!(
        oracle
            .is_timer_committed(&trigger.key, trigger.timer_type, trigger.time, wal_tag)
            .await?,
        "mismatching tag → committed-and-rescheduled"
    );
    Ok(())
}

/// B.4: production wires the timer half of the oracle with
/// [`StoreTagSource`] (a bare `TriggerStore` read), not the scheduler-first
/// [`TimerManager`] every other oracle test drives. Build a `CommitManager`
/// over `StoreTagSource(store)` and assert `is_timer_committed`'s
/// three-state contract resolves against the store tag directly:
/// tag-matches → not committed; tag-differs → committed; row-absent →
/// committed. The row is created through the manager (the known-good write
/// path); only the tag *source* is the bare store, which is the production
/// gap.
#[tokio::test]
async fn store_tag_source_resolves_three_states() -> Result<()> {
    time::pause();
    let (_stream, manager, _tx, store) = setup_with_store().await?;
    let trigger = test_trigger("k", 5)?;
    manager.schedule_trigger(trigger.clone()).await?;

    let oracle = CommitManager::new(
        MemoryDeduplicationStore::new(),
        StoreTagSource(store.clone()),
    );
    let live_tag = store
        .current_tag(&trigger.key, trigger.time, trigger.timer_type)
        .await?
        .ok_or_else(|| eyre!("scheduled timer must have a live store tag"))?;

    // tag-matches → not committed.
    assert!(
        !oracle
            .is_timer_committed(&trigger.key, trigger.timer_type, trigger.time, live_tag)
            .await?,
        "store tag matches the WAL tag → not committed"
    );

    // tag-differs → committed-and-rescheduled.
    assert!(
        oracle
            .is_timer_committed(
                &trigger.key,
                trigger.timer_type,
                trigger.time,
                live_tag.wrapping_add(1)
            )
            .await?,
        "store tag differs from the WAL tag → committed"
    );

    // row-absent → committed (fired-and-removed).
    store
        .remove_trigger(&trigger.key, trigger.time, trigger.timer_type)
        .await?;
    assert!(
        oracle
            .is_timer_committed(&trigger.key, trigger.timer_type, trigger.time, live_tag)
            .await?,
        "store row absent → committed"
    );
    Ok(())
}

#[tokio::test]
async fn timer_same_coordinate_clear_not_committed_until_commit() -> Result<()> {
    time::pause();
    let (stream, manager, _tx, store) = setup_with_store().await?;
    pin_mut!(stream);
    let trigger = test_trigger("k", 1)?;
    manager.schedule_trigger(trigger.clone()).await?;

    let firing = fire_next(stream.as_mut(), Duration::from_secs(2)).await?;
    let wal_tag = firing.trigger().tag;

    manager
        .clear_and_schedule(TimerRequest::new(
            trigger.key.clone(),
            trigger.time,
            trigger.timer_type,
            Span::current(),
        ))
        .await?;

    let before_commit = fresh_commit_manager(store.clone()).await?;
    assert!(
        !before_commit
            .is_timer_committed(&trigger.key, trigger.timer_type, trigger.time, wal_tag)
            .await?,
        "store-only oracle must stay false after same-coordinate clear before commit"
    );

    firing.commit().await;

    let after_commit = fresh_commit_manager(store).await?;
    assert!(
        after_commit
            .is_timer_committed(&trigger.key, trigger.timer_type, trigger.time, wal_tag)
            .await?,
        "store-only oracle must become true after commit rotates the tag"
    );
    Ok(())
}

/// Smoke test that `impl CommitOracle for CommitManager` plugs into
/// `RecoveringValueStore::get` end-to-end on the message arm. We seal
/// a Value WAL under a message event, insert the dedup row so the
/// oracle answers `Committed`, then call `get` and assert recovery
/// applied the WAL.
#[tokio::test]
async fn commit_manager_drives_recovering_value_store_get() -> Result<()> {
    use std::sync::Arc as StdArc;

    use crate::state::memory::MemoryDurableValueStore;
    use crate::state::recovering::RecoveringValueStore;
    use crate::state::value::{DurableWalStore, ValueOp, ValueStore};
    use crate::state::{
        CollectionId, CollectionRef, DurableState, EventRef, Read, StateKey, StateName, StateType,
    };
    use bytes::Bytes;

    time::pause();
    let (_stream, manager, _tx) = setup().await?;
    let dedup = MemoryDeduplicationStore::new();
    let dedup_id = Uuid::new_v4();
    dedup.insert(dedup_id).await.map_err(|e| eyre!("{e}"))?;
    let oracle = CommitManager::new(dedup, manager);

    let inner = MemoryDurableValueStore::for_tests();
    let collection = CollectionRef::new(
        CollectionId::new(
            StateKey::new(Uuid::new_v4(), StdArc::from("commit-manager-test-key")),
            StateType::Application,
            StateName::try_new("commit-manager-smoke").map_err(|e| eyre!("{e}"))?,
        ),
        None,
    );
    let id = collection.id().clone();
    let payload = Bytes::from_static(b"recovered");
    inner
        .seal(
            &collection,
            EventRef::Message { dedup_id },
            vec![ValueOp::Set {
                payload: payload.clone(),
            }],
        )
        .await
        .map_err(|e| eyre!("{e}"))?;

    let store = RecoveringValueStore::with_default_ttl(inner.clone(), oracle, None);
    let visible = store.get(&id).await.map_err(|e| eyre!("{e}"))?;
    assert_eq!(visible, Read::Present(payload));

    // Partition transitions to Idle after first-touch recovery.
    match inner.read_partition(&id).await.map_err(|e| eyre!("{e}"))? {
        DurableState::Idle { .. } => Ok(()),
        DurableState::Sealed { .. } => Err(eyre!("expected Idle post-recovery")),
    }
}

/// Sibling of [`commit_manager_drives_recovering_value_store_get`] for the
/// timer arm: a Value WAL sealed under a *timer* `EventRef` recovers through
/// `RecoveringValueStore::get`, exercising `is_timer_committed`. The timer is
/// fired and committed so its store row is removed (→ committed), and the
/// recovered collection is keyed by the same app key the oracle resolves the
/// timer coordinate under.
#[tokio::test]
async fn commit_manager_drives_recovering_value_store_get_timer_arm() -> Result<()> {
    use std::sync::Arc as StdArc;

    use crate::state::memory::MemoryDurableValueStore;
    use crate::state::recovering::RecoveringValueStore;
    use crate::state::value::{DurableWalStore, ValueOp, ValueStore};
    use crate::state::{
        CollectionId, CollectionRef, DurableState, EventRef, Read, StateKey, StateName, StateType,
        TimerEventRef,
    };
    use bytes::Bytes;

    time::pause();
    let (stream, manager, _tx) = setup().await?;
    pin_mut!(stream);
    let app_key = "timer-recovery-key";
    let trigger = test_trigger(app_key, 1)?;
    manager.schedule_trigger(trigger.clone()).await?;

    // Fire and commit: the timer row is removed, so the oracle answers
    // `Committed` for this coordinate regardless of the WAL tag.
    let firing = fire_next(stream.as_mut(), Duration::from_secs(2)).await?;
    let wal_tag = firing.trigger().tag;
    firing.commit().await;
    let oracle = CommitManager::new(MemoryDeduplicationStore::new(), manager);

    // Seal a Value WAL under the timer EventRef on a collection keyed by the
    // SAME app key the oracle uses to resolve the timer coordinate.
    let inner = MemoryDurableValueStore::for_tests();
    let collection = CollectionRef::new(
        CollectionId::new(
            StateKey::new(Uuid::new_v4(), StdArc::from(app_key)),
            StateType::Application,
            StateName::try_new("timer-arm-smoke").map_err(|e| eyre!("{e}"))?,
        ),
        None,
    );
    let id = collection.id().clone();
    let payload = Bytes::from_static(b"timer-recovered");
    let event = EventRef::Timer(TimerEventRef::new(
        trigger.timer_type,
        trigger.time,
        wal_tag,
    ));
    inner
        .seal(
            &collection,
            event,
            vec![ValueOp::Set {
                payload: payload.clone(),
            }],
        )
        .await
        .map_err(|e| eyre!("{e}"))?;

    let store = RecoveringValueStore::with_default_ttl(inner.clone(), oracle, None);
    let visible = store.get(&id).await.map_err(|e| eyre!("{e}"))?;
    assert_eq!(visible, Read::Present(payload));

    match inner.read_partition(&id).await.map_err(|e| eyre!("{e}"))? {
        DurableState::Idle { .. } => Ok(()),
        DurableState::Sealed { .. } => Err(eyre!("expected Idle post-recovery")),
    }
}

#[tokio::test]
async fn timer_same_coordinate_clear_abort_preserves_wal_tag() -> Result<()> {
    time::pause();
    let (stream, manager, _tx, store) = setup_with_store().await?;
    pin_mut!(stream);
    let trigger = test_trigger("k", 1)?;
    manager.schedule_trigger(trigger.clone()).await?;

    let firing = fire_next(stream.as_mut(), Duration::from_secs(2)).await?;
    let wal_tag = firing.trigger().tag;

    manager
        .clear_and_schedule(TimerRequest::new(
            trigger.key.clone(),
            trigger.time,
            trigger.timer_type,
            Span::current(),
        ))
        .await?;
    firing.abort().await;

    let oracle = fresh_commit_manager(store).await?;
    assert!(
        !oracle
            .is_timer_committed(&trigger.key, trigger.timer_type, trigger.time, wal_tag)
            .await?,
        "abort after same-coordinate clear must keep the WAL tag current"
    );
    Ok(())
}

/// The terminal lifecycle state a timer reaches before recovery asks the
/// oracle about its WAL tag.
#[derive(Clone, Copy, Debug)]
enum TimerScenario {
    /// Scheduled, never fired: `current_tag == wal_tag` → not committed.
    ScheduledTagMatches,
    /// Fired and committed without rescheduling: row removed → committed.
    FiredAndRemoved,
    /// Fired, rescheduled, committed: tag rotated, so the captured WAL
    /// tag differs from the live tag → committed.
    FiredAndRescheduled,
}

impl Arbitrary for TimerScenario {
    fn arbitrary(g: &mut Gen) -> Self {
        match u8::arbitrary(g) % 3 {
            0 => Self::ScheduledTagMatches,
            1 => Self::FiredAndRemoved,
            _ => Self::FiredAndRescheduled,
        }
    }
}

/// A randomized timer coordinate plus the terminal state to drive it to.
#[derive(Clone, Debug)]
struct TimerCase {
    scenario: TimerScenario,
    key: String,
    offset: u32,
}

impl Arbitrary for TimerCase {
    fn arbitrary(g: &mut Gen) -> Self {
        // A non-empty key and a small, non-zero schedule offset (the
        // store rejects a zero `CompactDuration`; paused-time `advance`
        // makes the magnitude free).
        let key = match g.choose(&["k", "user-1", "abc", "z"]) {
            Some(k) => (*k).to_owned(),
            None => "k".to_owned(),
        };
        let offset = 1 + (u32::arbitrary(g) % 8);
        Self {
            scenario: TimerScenario::arbitrary(g),
            key,
            offset,
        }
    }
}

/// T3: property over the timer three-state tag logic in
/// [`CommitManager::is_timer_committed`]. For a random coordinate driven
/// to each terminal lifecycle state through the **real** `TimerManager`
/// (schedule / fire / commit / reschedule), the oracle's verdict must
/// match the three-state contract: still-scheduled → not committed;
/// fired-and-removed → committed; fired-and-rescheduled (tag rotated) →
/// committed. Generalizes the three example tests over random
/// keys/offsets/scenarios. Iteration count comes from `QUICKCHECK_TESTS`.
#[test]
fn prop_timer_three_state_tag_logic() {
    fn property(case: TimerCase) -> TestResult {
        // A fresh paused-time current-thread runtime per case keeps the
        // timer firing deterministic, mirroring the `#[tokio::test]`
        // firing idiom used by the example tests above.
        let runtime = match Builder::new_current_thread().enable_all().build() {
            Ok(rt) => rt,
            Err(e) => return TestResult::error(format!("runtime build failed: {e}")),
        };
        let case_dbg = format!("{case:?}");
        match runtime.block_on(run_timer_case(case)) {
            Ok(true) => TestResult::passed(),
            Ok(false) => TestResult::error(format!("oracle verdict wrong for {case_dbg}")),
            Err(e) => TestResult::error(format!("case {case_dbg} errored: {e}")),
        }
    }
    QuickCheck::new().quickcheck(property as fn(TimerCase) -> TestResult);
}

/// Drives one [`TimerCase`] to its terminal state and returns whether the
/// oracle's verdict matches the three-state contract.
async fn run_timer_case(case: TimerCase) -> Result<bool> {
    time::pause();
    let (stream, manager, _tx) = setup().await?;
    pin_mut!(stream);
    let trigger = test_trigger(&case.key, case.offset)?;
    manager.schedule_trigger(trigger.clone()).await?;

    match case.scenario {
        TimerScenario::ScheduledTagMatches => {
            let tag = manager
                .current_tag(&trigger.key, trigger.time, trigger.timer_type)
                .await?
                .ok_or_else(|| eyre!("scheduled timer has no current tag"))?;
            let oracle = commit_manager(manager);
            // Correct ⇒ NOT committed (live tag matches the WAL tag).
            Ok(!oracle
                .is_timer_committed(&trigger.key, trigger.timer_type, trigger.time, tag)
                .await?)
        }
        TimerScenario::FiredAndRemoved => {
            let firing = fire_next(
                stream.as_mut(),
                Duration::from_secs(u64::from(case.offset) + 1),
            )
            .await?;
            let wal_tag = firing.trigger().tag;
            firing.commit().await; // committed without reschedule → row removed
            let oracle = commit_manager(manager);
            // Correct ⇒ committed (row absent).
            oracle
                .is_timer_committed(&trigger.key, trigger.timer_type, trigger.time, wal_tag)
                .await
                .map_err(Into::into)
        }
        TimerScenario::FiredAndRescheduled => {
            let firing = fire_next(
                stream.as_mut(),
                Duration::from_secs(u64::from(case.offset) + 1),
            )
            .await?;
            let wal_tag = firing.trigger().tag;
            // Re-arm the same coordinate before committing: commit rotates
            // the tag, so the captured WAL tag is no longer live.
            manager.schedule_trigger(trigger.clone()).await?;
            firing.commit().await;
            let oracle = commit_manager(manager);
            // Correct ⇒ committed (WAL tag differs from the rotated tag).
            oracle
                .is_timer_committed(&trigger.key, trigger.timer_type, trigger.time, wal_tag)
                .await
                .map_err(Into::into)
        }
    }
}
