use crate::timers::store::adapter::TableAdapter;
use color_eyre::eyre::{Result, eyre};
use uuid::Uuid;

use crate::consumer::middleware::deduplication::DeduplicationStore;
use crate::consumer::middleware::deduplication::memory::MemoryDeduplicationStore;
use crate::timers::TimerType;
use crate::timers::store::TriggerStore;
use crate::timers::store::TriggerStoreProvider;
use crate::timers::store::memory::{InMemoryTriggerStoreProvider, memory_store};
use crate::timers::test_support::{create_test_trigger, test_segment};

use super::{CommitManager, StoreTagSource};

/// Oracle: message not inserted → not committed.
#[tokio::test]
async fn message_not_committed_when_absent() -> Result<()> {
    let oracle = CommitManager::new(
        MemoryDeduplicationStore::new(),
        StoreTagSource(memory_store(test_segment("test", 300_u32))),
    );
    assert!(
        !oracle.is_message_committed(Uuid::new_v4()).await?,
        "absent UUID must not be committed"
    );
    Ok(())
}

/// Oracle: message inserted → committed.
#[tokio::test]
async fn message_committed_after_insert() -> Result<()> {
    let dedup = MemoryDeduplicationStore::new();
    let id = Uuid::new_v4();
    dedup.insert(id).await.map_err(|e| eyre!("{e}"))?;
    let oracle = CommitManager::new(
        dedup,
        StoreTagSource(memory_store(test_segment("test", 300_u32))),
    );
    assert!(
        oracle.is_message_committed(id).await?,
        "inserted UUID must be committed"
    );
    Ok(())
}

/// Production wires the timer half of the oracle with [`StoreTagSource`] —
/// a bare `TriggerStore` read over the partition's own store. Assert
/// [`CommitManager::is_timer_committed`] resolves its three-state contract
/// against the store tag directly: tag-matches → not committed;
/// tag-differs → committed; row-absent → committed. (The tag lifecycle
/// that produces these states — clear/commit/abort rotation — is proven in
/// `timers::manager::tests`.)
#[tokio::test]
async fn store_tag_source_resolves_three_states() -> Result<()> {
    let store = memory_store(test_segment("test", 300_u32));
    let trigger = create_test_trigger("k", 5, TimerType::Application)?;
    TableAdapter::new(store.clone())
        .add_trigger(trigger.clone())
        .await?;

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
    TableAdapter::new(store.clone())
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

/// Production hands the commit oracle a **clone of the partition's own
/// trigger store** (handle passing at partition acquisition). Over a
/// disjoint fresh store the oracle would read a permanently empty store and
/// answer "committed" for an abandoned, uncommitted timer event (a phantom
/// commit recovery would promote); over the shared handle it must answer
/// `NotCommitted` while the trigger row stands and flip only once the
/// partition's store commits the fire. The partition store is minted from
/// the provider exactly as production does — the provider's shared maps are
/// memory mode's durable substrate.
#[tokio::test]
async fn oracle_reads_through_the_partitions_store() -> Result<()> {
    let provider = InMemoryTriggerStoreProvider::new();
    let partition_store = provider.create_store(test_segment("oracle-shared", 300_u32));
    let oracle = CommitManager::new(
        MemoryDeduplicationStore::new(),
        StoreTagSource(partition_store.clone()),
    );

    // The partition schedules a timer; the event is then abandoned before its
    // trigger commit (shutdown abandon), so the row and its WAL tag stand.
    let trigger = create_test_trigger("k", 5, TimerType::Application)?;
    let (key, time, timer_type, wal_tag) = (
        trigger.key.clone(),
        trigger.time,
        trigger.timer_type,
        trigger.tag,
    );
    TableAdapter::new(partition_store.clone())
        .add_trigger(trigger)
        .await?;

    assert!(
        !oracle
            .is_timer_committed(&key, timer_type, time, wal_tag)
            .await?,
        "abandoned pre-commit: the standing trigger row must read NotCommitted"
    );

    // Committing the fire through the partition's store removes the row; the
    // oracle must observe it through its own view of the shared store.
    TableAdapter::new(partition_store.clone())
        .remove_trigger(&key, time, timer_type)
        .await?;
    assert!(
        oracle
            .is_timer_committed(&key, timer_type, time, wal_tag)
            .await?,
        "fired-and-removed must read committed through the shared store"
    );
    Ok(())
}
