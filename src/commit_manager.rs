//! Commit oracle for keyed-state recovery.
//!
//! [`CommitManager`] answers "did this event commit?" for both message and
//! timer events. It is the read-only interface consumed by the state
//! middleware during WAL-based recovery; no write operations are exposed.
//!
//! # Message side
//!
//! `is_message_committed(dedup_id)` delegates to a [`DeduplicationStore`]:
//! row present ⇔ committed.
//!
//! # Timer side
//!
//! `is_timer_committed(key, type, time, wal_tag)` compares the WAL-recorded
//! tag against the current tag in storage:
//! - row absent → committed (fired-and-removed)
//! - `Some(cur) == wal_tag` → not committed
//! - `Some(cur) != wal_tag` → committed-and-rescheduled (returns `true`)
//!
//! **Encapsulation note**: no accessor returns the inner store or manager;
//! the state middleware physically cannot reach write operations through this
//! type.

use crate::Key;
use crate::consumer::middleware::deduplication::DeduplicationStore;
use crate::error::{ClassifyError, ErrorCategory};
use crate::state::oracle::CommitOracle;
use crate::state::value::ValueKind;
use crate::state::{CollectionId, CommitDecision, EventRef, TimerEventRef};
use crate::timers::TimerManager;
use crate::timers::TimerType;
use crate::timers::datetime::CompactDateTime;
use crate::timers::error::TimerManagerError;
use crate::timers::store::TriggerStore;
use std::error::Error;
use thiserror::Error;
use uuid::Uuid;

/// Read-only oracle for commit state of message and timer events.
#[derive(Clone, Debug)]
pub struct CommitManager<D, T>
where
    D: DeduplicationStore,
    T: TriggerStore,
{
    dedup: D,
    timers: TimerManager<T>,
}

impl<D, T> CommitManager<D, T>
where
    D: DeduplicationStore,
    T: TriggerStore,
    T::Error: ClassifyError,
{
    /// Creates a new commit manager.
    pub fn new(dedup: D, timers: TimerManager<T>) -> Self {
        Self { dedup, timers }
    }

    /// Returns `true` if the message identified by `dedup_id` has been
    /// committed to the deduplication store.
    ///
    /// # Errors
    ///
    /// Returns `CommitManagerError::Dedup` if the store read fails.
    pub async fn is_message_committed(
        &self,
        dedup_id: Uuid,
    ) -> Result<bool, CommitManagerError<D::Error, T::Error>> {
        self.dedup
            .exists(dedup_id)
            .await
            .map_err(CommitManagerError::Dedup)
    }

    /// Returns `true` if the timer event identified by `(key, timer_type,
    /// time, wal_tag)` has been committed.
    ///
    /// Three-state decision:
    /// - row absent → `true` (fired-and-removed → committed)
    /// - `current_tag == wal_tag` → `false` (still scheduled, not committed)
    /// - `current_tag != wal_tag` → `true` (committed-and-rescheduled)
    ///
    /// # Errors
    ///
    /// Returns `CommitManagerError::Timer` if the timer store read fails.
    pub async fn is_timer_committed(
        &self,
        key: &Key,
        timer_type: TimerType,
        time: CompactDateTime,
        wal_tag: i32,
    ) -> Result<bool, CommitManagerError<D::Error, T::Error>> {
        match self
            .timers
            .current_tag(key, time, timer_type)
            .await
            .map_err(CommitManagerError::Timer)?
        {
            None => Ok(true),
            Some(cur) => Ok(cur != wal_tag),
        }
    }
}

impl<D, T> CommitOracle for CommitManager<D, T>
where
    D: DeduplicationStore,
    T: TriggerStore,
    T::Error: ClassifyError,
{
    type Error = CommitManagerError<D::Error, T::Error>;

    async fn resolve<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
        event: EventRef,
    ) -> Result<CommitDecision, Self::Error> {
        let committed = match event {
            EventRef::Message { dedup_id } => self.is_message_committed(dedup_id).await?,
            EventRef::Timer(TimerEventRef {
                timer_type,
                time,
                tag,
            }) => {
                self.is_timer_committed(&collection.state_key().key, timer_type, time, tag)
                    .await?
            }
        };
        Ok(if committed {
            CommitDecision::Committed
        } else {
            CommitDecision::NotCommitted
        })
    }
}

/// Error type for [`CommitManager`] operations.
#[derive(Debug, Error)]
pub enum CommitManagerError<DE, TE>
where
    DE: Error + Send + Sync + 'static,
    TE: ClassifyError + Error + Send + Sync + 'static,
{
    /// Deduplication store read failed.
    #[error("deduplication store error")]
    Dedup(#[source] DE),
    /// Timer store read failed.
    #[error("timer store error")]
    Timer(#[source] TimerManagerError<TE>),
}

impl<DE, TE> ClassifyError for CommitManagerError<DE, TE>
where
    DE: Error + Send + Sync + 'static,
    TE: ClassifyError + Error + Send + Sync + 'static,
{
    fn classify_error(&self) -> ErrorCategory {
        // Both halves are storage reads — transient.
        ErrorCategory::Transient
    }
}

#[cfg(test)]
mod tests {
    use std::array::from_fn;
    use std::sync::Arc;
    use std::time::Duration;

    use color_eyre::eyre::{Result, eyre};
    use futures::{StreamExt, pin_mut};
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
    use crate::timers::store::adapter::TableAdapter;
    use crate::timers::store::memory::{InMemoryTriggerStore, memory_store};
    use crate::timers::{
        PendingTimer, TimerManager, TimerManagerConfig, TimerRequest, TimerSemaphores, TimerType,
        Trigger,
    };

    use super::CommitManager;

    type TestManager = TimerManager<TableAdapter<InMemoryTriggerStore>>;
    type TestCommitManager =
        CommitManager<MemoryDeduplicationStore, TableAdapter<InMemoryTriggerStore>>;

    fn test_semaphores() -> Arc<TimerSemaphores> {
        Arc::new(from_fn(|_| Arc::new(Semaphore::new(64))))
    }

    async fn setup() -> Result<(
        impl futures::Stream<Item = PendingTimer<TableAdapter<InMemoryTriggerStore>>>,
        TestManager,
        watch::Sender<ShutdownPhase>,
    )> {
        let (stream, manager, shutdown_tx, _store) = setup_with_store().await?;
        Ok((stream, manager, shutdown_tx))
    }

    async fn setup_with_store() -> Result<(
        impl futures::Stream<Item = PendingTimer<TableAdapter<InMemoryTriggerStore>>>,
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
        let (shutdown_tx, shutdown_rx) = watch::channel(ShutdownPhase::default());
        let telemetry = Telemetry::new();
        let config = TimerManagerConfig {
            name: "test".to_owned(),
            store: store.clone(),
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
        Ok((stream, manager, shutdown_tx, store))
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
        let (_shutdown_tx, shutdown_rx) = watch::channel(ShutdownPhase::default());
        let telemetry = Telemetry::new();
        let config = TimerManagerConfig {
            name: "test".to_owned(),
            store,
            telemetry: telemetry.partition_sender(Topic::from("test"), 0),
            source: Arc::from(""),
        };
        let (_stream, manager) = TimerManager::new(
            config,
            HeartbeatRegistry::test(),
            shutdown_rx,
            test_semaphores(),
        )
        .await
        .map_err(|e| eyre!("{e}"))?;
        Ok(commit_manager(manager))
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

        advance(Duration::from_secs(2)).await;
        task::yield_now().await;
        let pending = stream.next().await.ok_or_else(|| eyre!("no pending"))?;
        let firing = pending.fire().await.ok_or_else(|| eyre!("not active"))?;
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

        advance(Duration::from_secs(2)).await;
        task::yield_now().await;
        let pending = stream.next().await.ok_or_else(|| eyre!("no pending"))?;
        let firing = pending.fire().await.ok_or_else(|| eyre!("not active"))?;
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

    #[tokio::test]
    async fn timer_same_coordinate_clear_not_committed_until_commit() -> Result<()> {
        time::pause();
        let (stream, manager, _tx, store) = setup_with_store().await?;
        pin_mut!(stream);
        let trigger = test_trigger("k", 1)?;
        manager.schedule_trigger(trigger.clone()).await?;

        advance(Duration::from_secs(2)).await;
        task::yield_now().await;
        let pending = stream.next().await.ok_or_else(|| eyre!("no pending"))?;
        let firing = pending.fire().await.ok_or_else(|| eyre!("not active"))?;
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
        use crate::state::value::{DurableWalStore, StoredPayload, ValueOp, ValueStore};
        use crate::state::{
            CollectionId, CollectionRef, DurableState, EventRef, Read, StateKey, StateName,
            StateType,
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
        let payload = StoredPayload::Inline(Bytes::from_static(b"recovered"));
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

        let store = RecoveringValueStore::new(inner.clone(), oracle, None);
        let visible = store.get(&id).await.map_err(|e| eyre!("{e}"))?;
        assert_eq!(visible, Read::Present(payload));

        // Partition transitions to Idle after first-touch recovery.
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

        advance(Duration::from_secs(2)).await;
        task::yield_now().await;
        let pending = stream.next().await.ok_or_else(|| eyre!("no pending"))?;
        let firing = pending.fire().await.ok_or_else(|| eyre!("not active"))?;
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
}
