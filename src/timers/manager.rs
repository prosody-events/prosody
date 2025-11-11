//! Timer management and coordination for scheduled events.
//!
//! The [`TimerManager`] serves as the primary interface for scheduling,
//! querying, and canceling timers within a specific segment. It coordinates
//! between:
//! - **Persistent Storage**: Durable [`TriggerStore`] for timer metadata.
//! - **Background Slab Loader**: Preloads upcoming timer slabs.
//! - **In-Memory Scheduler**: Precise, delay-queue based timer dispatch.
//! - **Application**: Delivers timers as an async stream of
//!   [`UncommittedTimer`].
//!
//! The manager ensures timers survive restarts, supports distributed ownership,
//! and provides at-least-once delivery semantics for timer events.

use crate::Key;
use crate::heartbeat::HeartbeatRegistry;
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;

pub use crate::timers::error::TimerManagerError;
use crate::timers::loader::{State, get_or_create_segment, slab_loader};
use crate::timers::scheduler::TriggerScheduler;
use crate::timers::slab::Slab;
use crate::timers::slab_lock::SlabLock;
use crate::timers::store::{Segment, SegmentId, TriggerStore};
use crate::timers::{DELETE_CONCURRENCY, PendingTimer, TimerType, Trigger};
use async_stream::try_stream;
use educe::Educe;
use futures::{Stream, StreamExt, TryStreamExt, pin_mut};
use std::sync::Arc;
use tokio::spawn;
use tokio::task::coop::cooperative;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{Instrument, Span};

/// Manages timer scheduling, storage, and delivery for a specific segment.
///
/// Partitions timers into time-based slabs, persists them in a
/// [`TriggerStore`], schedules them in memory, and delivers them as an async
/// stream of [`PendingTimer`]. Supports concurrent operations and
/// automatically cleans up resources when dropped.
///
/// # Type Parameters
///
/// * `T`: The [`TriggerStore`] backend for persistent timer data.
#[derive(Educe)]
#[educe(Debug(bound = ""), Clone(bound()))]
pub struct TimerManager<T>(#[educe(Debug(ignore))] Arc<TimerManagerInner<T>>);

/// Internal shared state for the [`TimerManager`].
pub struct TimerManagerInner<T> {
    /// Segment configuration (ID, name, slab size).
    segment: Segment,
    /// Shared state for loader and scheduler (wrapped in a read–write lock).
    state: SlabLock<State<T>>,
}

impl<T> TimerManager<T>
where
    T: TriggerStore,
{
    /// Creates a new timer manager for the specified segment.
    ///
    /// Initializes:
    /// 1. A persistent segment record (creating or retrieving it).
    /// 2. An in-memory scheduler and its command processing task.
    /// 3. A background slab loader task for preloading upcoming timers.
    ///
    /// # Arguments
    ///
    /// * `segment_id` - Unique identifier for the timer segment.
    /// * `slab_size` - Duration of each time-based slab.
    /// * `name` - Human-readable name for the segment.
    /// * `store` - Persistent [`TriggerStore`] implementation.
    ///
    /// # Returns
    ///
    /// On success, returns a tuple:
    /// - A [`Stream`] of [`PendingTimer<T>`] delivering timer events.
    /// - The [`TimerManager<T>`] instance for scheduling and management.
    ///
    /// # Errors
    ///
    /// Returns [`TimerManagerError`] if:
    /// - The segment metadata cannot be created or retrieved.
    /// - The scheduler fails to initialize.
    /// - The slab loader task cannot be spawned.
    pub async fn new(
        segment_id: SegmentId,
        slab_size: CompactDuration,
        name: &str,
        store: T,
        heartbeats: HeartbeatRegistry,
    ) -> Result<(impl Stream<Item = PendingTimer<T>>, Self), TimerManagerError<T::Error>> {
        // Ensure the segment exists in persistent storage.
        let segment = get_or_create_segment(&store, segment_id, slab_size, name).await?;

        // Initialize the in-memory scheduler.
        let (trigger_rx, scheduler) = TriggerScheduler::new(&heartbeats);

        // Share state between loader and API.
        let state = SlabLock::new(State::new(store, scheduler.clone()));

        // Spawn the background task to preload and clean slab data.
        spawn(slab_loader(
            segment.clone(),
            state.clone(),
            heartbeats.register("timer loader"),
        ));

        // Build the manager wrapper.
        let manager = Self(Arc::new(TimerManagerInner { segment, state }));
        let cloned_manager = manager.clone();

        // Wrap the scheduler receiver into an UncommittedTimer stream.
        let stream = ReceiverStream::new(trigger_rx)
            .map(move |trigger| PendingTimer::new(trigger, cloned_manager.clone()));

        Ok((stream, manager))
    }

    /// Retrieves all scheduled execution times for a given key.
    ///
    /// # Arguments
    ///
    /// * `key` - The entity key whose timers to list.
    ///
    /// # Returns
    ///
    /// A [`Stream`] of all scheduled times for `key`.
    ///
    /// # Errors
    ///
    /// Returns [`TimerManagerError::Store`] if the underlying storage query
    /// fails.
    pub fn scheduled_times(
        &self,
        key: &Key,
        timer_type: TimerType,
    ) -> impl Stream<Item = Result<CompactDateTime, TimerManagerError<T::Error>>> + Send + 'static
    {
        let slab_lock = self.0.state.clone();
        let key = key.clone();
        let segment_id = self.0.segment.id;

        try_stream! {
            let state = slab_lock.trigger_lock().await;

            let stream = state
                .store
                .get_key_times(&segment_id, timer_type, &key)
                .map_err(TimerManagerError::Store);

            pin_mut!(stream);
            while let Some(item) = cooperative(stream.try_next()).await? {
                yield item;
            }
        }
    }

    /// Retrieves all scheduled triggers for a given key.
    ///
    /// # Arguments
    ///
    /// * `key` - The entity key whose full [`Trigger`] records to list.
    ///
    /// # Errors
    ///
    /// Returns [`TimerManagerError::Store`] if the underlying storage query
    /// fails.
    pub async fn scheduled_triggers(
        &self,
        key: &Key,
        timer_type: TimerType,
    ) -> Result<Vec<Trigger>, TimerManagerError<T::Error>> {
        self.0
            .state
            .trigger_lock()
            .await
            .store
            .get_key_triggers(&self.0.segment.id, timer_type, key)
            .map_err(TimerManagerError::Store)
            .try_collect()
            .await
    }

    /// Schedules a new timer for future execution.
    ///
    /// Inserts the timer into persistent storage and, if its slab is currently
    /// owned, enqueues it in the in-memory scheduler.
    ///
    /// # Arguments
    ///
    /// * `trigger` - The [`Trigger`] to schedule (key, time, span).
    ///
    /// # Errors
    ///
    /// Returns [`TimerManagerError`] if:
    /// - The time is in the past.
    /// - The storage insert fails.
    /// - The scheduler enqueue fails.
    pub async fn schedule(&self, trigger: Trigger) -> Result<(), TimerManagerError<T::Error>> {
        // Determine the slab for this trigger time.
        let slab = Slab::from_time(self.0.segment.id, self.0.segment.slab_size, trigger.time);
        let slab_id = slab.id();
        let state = self.0.state.trigger_lock().await;

        // Persist the trigger in both slab and key indices.
        state
            .store
            .add_trigger(&self.0.segment, slab, trigger.clone())
            .await
            .map_err(TimerManagerError::Store)?;

        // If we own the slab, enqueue in the in-memory scheduler.
        if state.is_owned(slab_id) {
            state.scheduler.schedule(trigger).await?;
        }

        Ok(())
    }

    /// Cancels a specific scheduled timer.
    ///
    /// Removes the timer from persistent storage and, if owned, from the
    /// in-memory scheduler. If already delivered, the delivery is not reversed.
    ///
    /// # Arguments
    ///
    /// * `key` - The entity key of the timer.
    /// * `time` - The scheduled execution time to cancel.
    ///
    /// # Errors
    ///
    /// Returns [`TimerManagerError`] if:
    /// - The scheduler removal fails.
    /// - The storage removal fails.
    pub async fn unschedule(
        &self,
        key: &Key,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> Result<(), TimerManagerError<T::Error>> {
        // Identify the slab containing this time.
        let slab = Slab::from_time(self.0.segment.id, self.0.segment.slab_size, time);
        let slab_id = slab.id();
        let state = self.0.state.trigger_lock().await;

        // Remove from in-memory scheduler if owned.
        if state.is_owned(slab_id) {
            let trigger = Trigger::new(key.clone(), time, timer_type, Span::current());
            state.scheduler.unschedule(trigger).await?;
        }

        // Remove from persistent storage.
        state
            .store
            .remove_trigger(&self.0.segment, &slab, key, time, timer_type)
            .await
            .map_err(TimerManagerError::Store)
    }

    /// Cancels all timers for a specific key concurrently.
    ///
    /// Queries all scheduled times for `key` and issues
    /// [`unschedule`](Self::unschedule) for each in parallel, controlled by
    /// [`DELETE_CONCURRENCY`].
    ///
    /// # Arguments
    ///
    /// * `key` - The entity key whose timers to cancel.
    ///
    /// # Errors
    ///
    /// Returns [`TimerManagerError::Store`] or scheduler errors if any cancel
    /// operation fails.
    pub async fn unschedule_all(
        &self,
        key: &Key,
        timer_type: TimerType,
    ) -> Result<(), TimerManagerError<T::Error>> {
        let span = Span::current();

        self.scheduled_times(key, timer_type)
            .map_ok(|time| {
                self.unschedule(key, time, timer_type)
                    .instrument(span.clone())
            })
            .try_buffer_unordered(DELETE_CONCURRENCY)
            .try_collect::<()>()
            .await
    }

    /// Checks if a timer is currently loaded in the in-memory scheduler.
    ///
    /// Does not query persistent storage; a `false` return means the timer is
    /// either not scheduled yet or not owned/loaded.
    ///
    /// # Arguments
    ///
    /// * `key` - The entity key of the timer.
    /// * `time` - The scheduled execution time to check.
    ///
    /// # Returns
    ///
    /// `true` if the timer is active in the scheduler.
    pub async fn is_active(&self, key: &Key, time: CompactDateTime, timer_type: TimerType) -> bool {
        self.0
            .state
            .trigger_lock()
            .await
            .scheduler
            .is_active(key, time, timer_type)
            .await
    }

    /// Marks a timer as completed and removes it permanently.
    ///
    /// Deactivates the timer if owned, then deletes it from persistent storage.
    /// Typically invoked by `UncommittedTimer::commit()`.
    ///
    /// # Arguments
    ///
    /// * `key` - The entity key of the completed timer.
    /// * `time` - The execution time of the completed timer.
    ///
    /// # Errors
    ///
    /// Returns [`TimerManagerError::Store`] if the storage removal fails.
    pub async fn complete(
        &self,
        key: &Key,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> Result<(), TimerManagerError<T::Error>> {
        // Derive the slab and lock state.
        let slab = Slab::from_time(self.0.segment.id, self.0.segment.slab_size, time);
        let slab_id = slab.id();
        let state = self.0.state.trigger_lock().await;

        // Deactivate in-memory if owned.
        if state.is_owned(slab_id) {
            state.scheduler.deactivate(key, time, timer_type).await;
        }

        // Remove from storage.
        state
            .store
            .remove_trigger(&self.0.segment, &slab, key, time, timer_type)
            .await
            .map_err(TimerManagerError::Store)?;

        Ok(())
    }

    /// Aborts a timer delivery, deactivating it but preserving storage state.
    ///
    /// Does not delete the timer from persistent storage; it can be reloaded
    /// and retried later by the slab loader.
    ///
    /// # Arguments
    ///
    /// * `key` - The entity key of the timer.
    /// * `time` - The scheduled execution time to abort.
    pub async fn abort(&self, key: &Key, time: CompactDateTime, timer_type: TimerType) {
        let slab = Slab::from_time(self.0.segment.id, self.0.segment.slab_size, time);
        let slab_id = slab.id();
        let state = self.0.state.trigger_lock().await;

        if state.is_owned(slab_id) {
            state.scheduler.deactivate(key, time, timer_type).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consumer::Uncommitted;
    use crate::timers::UncommittedTimer;
    use crate::timers::store::adapter::TableAdapter;
    use crate::timers::store::memory::{InMemoryTriggerStore, memory_store};
    use color_eyre::eyre::{Result, eyre};
    use futures::StreamExt;
    use std::time::Duration;
    use tokio::task;
    use tokio::time::{self, advance, timeout};
    use tracing::Span;
    use uuid::Uuid;

    /// Helper function to create a test trigger
    fn create_test_trigger(
        key: &str,
        seconds_offset: u32,
        timer_type: TimerType,
    ) -> Result<Trigger> {
        let time = CompactDateTime::now()?.add_duration(CompactDuration::new(seconds_offset))?;

        Ok(Trigger::new(
            Key::from(key),
            time,
            timer_type,
            Span::current(),
        ))
    }

    /// Helper function to set up a timer manager for testing
    async fn setup_timer_manager() -> Result<(
        impl Stream<Item = PendingTimer<TableAdapter<InMemoryTriggerStore>>>,
        TimerManager<TableAdapter<InMemoryTriggerStore>>,
    )> {
        let store = memory_store();
        let segment_id = Uuid::new_v4();
        let slab_size = CompactDuration::new(300);

        TimerManager::new(
            segment_id,
            slab_size,
            "test-manager",
            store,
            HeartbeatRegistry::test(),
        )
        .await
        .map_err(|e| eyre!("Failed to create timer manager: {}", e))
    }

    #[tokio::test]
    async fn test_new_timer_manager_creation() -> Result<()> {
        time::pause();

        let store = memory_store();
        let segment_id = Uuid::new_v4();
        let slab_size = CompactDuration::new(300);

        let result = TimerManager::new(
            segment_id,
            slab_size,
            "test-creation",
            store,
            HeartbeatRegistry::test(),
        )
        .await;

        assert!(result.is_ok(), "Timer manager creation should succeed");

        let (_stream, manager) = result?;
        assert_eq!(manager.0.segment.id, segment_id);
        assert_eq!(manager.0.segment.slab_size, slab_size);
        assert_eq!(manager.0.segment.name, "test-creation");
        Ok(())
    }

    #[tokio::test]
    async fn test_schedule_timer_basic() -> Result<()> {
        time::pause();

        let (_stream, manager) = setup_timer_manager().await?;
        let trigger = create_test_trigger("test-key", 60, TimerType::Application)?;

        let result = manager.schedule(trigger.clone()).await;
        assert!(result.is_ok(), "Scheduling should succeed");

        // Verify the timer is stored
        let scheduled_times = manager
            .scheduled_times(&trigger.key, TimerType::Application)
            .try_collect::<Vec<_>>()
            .await?;
        assert_eq!(scheduled_times.len(), 1);
        assert!(scheduled_times.contains(&trigger.time));
        Ok(())
    }

    #[tokio::test]
    async fn test_schedule_multiple_timers_same_key() -> Result<()> {
        time::pause();

        let (_stream, manager) = setup_timer_manager().await?;
        let key = Key::from("multi-timer-key");

        // Schedule multiple timers for the same key
        let triggers = vec![
            create_test_trigger("multi-timer-key", 60, TimerType::Application)?,
            create_test_trigger("multi-timer-key", 120, TimerType::Application)?,
            create_test_trigger("multi-timer-key", 180, TimerType::Application)?,
        ];

        for trigger in &triggers {
            manager.schedule(trigger.clone()).await?;
        }

        let scheduled_times = manager
            .scheduled_times(&key, TimerType::Application)
            .try_collect::<Vec<_>>()
            .await?;
        assert_eq!(scheduled_times.len(), 3);

        for trigger in &triggers {
            assert!(scheduled_times.contains(&trigger.time));
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_schedule_multiple_keys() -> Result<()> {
        time::pause();

        let (_stream, manager) = setup_timer_manager().await?;

        let trigger1 = create_test_trigger("key-1", 60, TimerType::Application)?;
        let trigger2 = create_test_trigger("key-2", 120, TimerType::Application)?;

        manager.schedule(trigger1.clone()).await?;
        manager.schedule(trigger2.clone()).await?;

        // Verify each key has its timer
        let times1 = manager
            .scheduled_times(&trigger1.key, TimerType::Application)
            .try_collect::<Vec<_>>()
            .await?;
        let times2 = manager
            .scheduled_times(&trigger2.key, TimerType::Application)
            .try_collect::<Vec<_>>()
            .await?;

        assert_eq!(times1.len(), 1);
        assert_eq!(times2.len(), 1);
        assert!(times1.contains(&trigger1.time));
        assert!(times2.contains(&trigger2.time));
        Ok(())
    }

    #[tokio::test]
    async fn test_scheduled_times_empty_key() -> Result<()> {
        time::pause();

        let (_stream, manager) = setup_timer_manager().await?;
        let nonexistent_key = Key::from("nonexistent");

        let scheduled_times = manager
            .scheduled_times(&nonexistent_key, TimerType::Application)
            .try_collect::<Vec<_>>()
            .await?;
        assert!(scheduled_times.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn test_unschedule_timer() -> Result<()> {
        time::pause();

        let (_stream, manager) = setup_timer_manager().await?;
        let trigger = create_test_trigger("unschedule-key", 60, TimerType::Application)?;

        // Schedule then unschedule
        manager.schedule(trigger.clone()).await?;
        let result = manager
            .unschedule(&trigger.key, trigger.time, TimerType::Application)
            .await;
        assert!(result.is_ok(), "Unscheduling should succeed");

        // Verify timer is removed
        let scheduled_times = manager
            .scheduled_times(&trigger.key, TimerType::Application)
            .try_collect::<Vec<_>>()
            .await?;
        assert!(scheduled_times.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn test_unschedule_nonexistent_timer() -> Result<()> {
        time::pause();

        let (_stream, manager) = setup_timer_manager().await?;
        let key = Key::from("nonexistent-key");
        let time = CompactDateTime::now()?;

        // Unscheduling non-existent timer should succeed (idempotent)
        let result = manager.unschedule(&key, time, TimerType::Application).await;
        assert!(
            result.is_ok(),
            "Unscheduling nonexistent timer should succeed"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_unschedule_all_timers() -> Result<()> {
        time::pause();

        let (_stream, manager) = setup_timer_manager().await?;
        let key = Key::from("unschedule-all-key");

        // Schedule multiple timers
        let triggers = vec![
            create_test_trigger("unschedule-all-key", 60, TimerType::Application)?,
            create_test_trigger("unschedule-all-key", 120, TimerType::Application)?,
            create_test_trigger("unschedule-all-key", 180, TimerType::Application)?,
        ];

        for trigger in &triggers {
            manager.schedule(trigger.clone()).await?;
        }

        // Verify all are scheduled
        let scheduled_times = manager
            .scheduled_times(&key, TimerType::Application)
            .try_collect::<Vec<_>>()
            .await?;
        assert_eq!(scheduled_times.len(), 3);

        // Unschedule all
        let result = manager.unschedule_all(&key, TimerType::Application).await;
        assert!(result.is_ok(), "Unschedule all should succeed");

        // Verify all are removed
        let scheduled_times = manager
            .scheduled_times(&key, TimerType::Application)
            .try_collect::<Vec<_>>()
            .await?;
        assert!(scheduled_times.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn test_unschedule_all_empty_key() -> Result<()> {
        time::pause();

        let (_stream, manager) = setup_timer_manager().await?;
        let empty_key = Key::from("empty-key");

        // Unschedule all on empty key should succeed
        let result = manager
            .unschedule_all(&empty_key, TimerType::Application)
            .await;
        assert!(result.is_ok(), "Unschedule all on empty key should succeed");
        Ok(())
    }

    #[tokio::test]
    async fn test_is_active_timer() -> Result<()> {
        time::pause();

        let (_stream, manager) = setup_timer_manager().await?;

        // Create a timer that should be immediately active (very soon)
        let now = CompactDateTime::now()?;
        let near_future = now.add_duration(CompactDuration::new(10))?;
        let trigger = Trigger::new(
            Key::from("active-test"),
            near_future,
            TimerType::Application,
            Span::current(),
        );

        manager.schedule(trigger.clone()).await?;

        // Allow some time for the scheduler to process
        time::advance(Duration::from_millis(100)).await;
        task::yield_now().await;

        // Check if timer is active
        let is_active = manager
            .is_active(&trigger.key, trigger.time, TimerType::Application)
            .await;
        assert!(is_active);

        Ok(())
    }

    #[tokio::test]
    async fn test_is_active_nonexistent_timer() -> Result<()> {
        time::pause();

        let (_stream, manager) = setup_timer_manager().await?;
        let key = Key::from("nonexistent");
        let time = CompactDateTime::now()?;

        let is_active = manager.is_active(&key, time, TimerType::Application).await;
        assert!(!is_active, "Nonexistent timer should not be active");
        Ok(())
    }

    #[tokio::test]
    async fn test_complete_timer() -> Result<()> {
        time::pause();

        let (_stream, manager) = setup_timer_manager().await?;
        let trigger = create_test_trigger("complete-key", 60, TimerType::Application)?;

        // Schedule timer
        manager.schedule(trigger.clone()).await?;

        // Complete timer
        let result = manager
            .complete(&trigger.key, trigger.time, TimerType::Application)
            .await;
        assert!(result.is_ok(), "Complete should succeed");

        // Verify timer is removed from storage
        let scheduled_times = manager
            .scheduled_times(&trigger.key, TimerType::Application)
            .try_collect::<Vec<_>>()
            .await?;
        assert!(scheduled_times.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn test_complete_nonexistent_timer() -> Result<()> {
        time::pause();

        let (_stream, manager) = setup_timer_manager().await?;
        let key = Key::from("nonexistent");
        let time = CompactDateTime::now()?;

        // Completing nonexistent timer should succeed (idempotent)
        let result = manager.complete(&key, time, TimerType::Application).await;
        assert!(result.is_ok(), "Complete nonexistent timer should succeed");
        Ok(())
    }

    #[tokio::test]
    async fn test_abort_timer() -> Result<()> {
        time::pause();

        let (_stream, manager) = setup_timer_manager().await?;
        let trigger = create_test_trigger("abort-key", 60, TimerType::Application)?;

        // Schedule timer
        manager.schedule(trigger.clone()).await?;

        // Abort timer (should deactivate but leave in storage)
        manager
            .abort(&trigger.key, trigger.time, TimerType::Application)
            .await;

        // Timer should still be in storage after abort
        let scheduled_times = manager
            .scheduled_times(&trigger.key, TimerType::Application)
            .try_collect::<Vec<_>>()
            .await?;
        assert_eq!(scheduled_times.len(), 1);
        assert!(scheduled_times.contains(&trigger.time));
        Ok(())
    }

    #[tokio::test]
    async fn test_abort_nonexistent_timer() -> Result<()> {
        time::pause();

        let (_stream, manager) = setup_timer_manager().await?;
        let key = Key::from("nonexistent");
        let time = CompactDateTime::now()?;

        // Aborting nonexistent timer should succeed without error
        manager.abort(&key, time, TimerType::Application).await;
        Ok(())
    }

    #[tokio::test]
    async fn test_timer_stream_delivery() -> Result<()> {
        time::pause();

        let (mut stream, manager) = setup_timer_manager().await?;

        // Schedule a timer for immediate execution
        let now = CompactDateTime::now()?;
        let immediate_time = now.add_duration(CompactDuration::new(1))?;
        let trigger = Trigger::new(
            Key::from("stream-test"),
            immediate_time,
            TimerType::Application,
            Span::current(),
        );

        manager.schedule(trigger.clone()).await?;

        // Advance time past the trigger time
        time::advance(Duration::from_secs(2)).await;
        task::yield_now().await;

        if let Some(uncommitted_timer) = stream.next().await {
            let (trigger_data, _) = uncommitted_timer.into_inner();
            assert_eq!(trigger_data.key, trigger.key);
            assert_eq!(trigger_data.time, trigger.time);
        }

        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::unwrap_used)]
    async fn test_concurrent_operations() -> Result<()> {
        time::pause();

        let (_stream, manager) = setup_timer_manager().await?;
        let manager = Arc::new(manager);

        // Spawn multiple concurrent operations
        let mut handles = vec![];

        // Schedule timers concurrently
        for i in 0..10 {
            let manager_clone = manager.clone();
            let handle = spawn(async move {
                let trigger =
                    create_test_trigger(&format!("concurrent-{i}"), 60 + i, TimerType::Application)
                        .unwrap();
                manager_clone.schedule(trigger).await
            });
            handles.push(handle);
        }

        // Wait for all operations to complete
        for handle in handles {
            handle
                .await
                .map_err(|e| eyre!("Task join error: {}", e))??;
        }

        // Verify all timers were scheduled
        for i in 0..10_u8 {
            let key = Key::from(format!("concurrent-{i}"));
            let times = manager
                .scheduled_times(&key, TimerType::Application)
                .try_collect::<Vec<_>>()
                .await?;
            assert_eq!(times.len(), 1, "Timer {i} should be scheduled");
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_timer_lifecycle() -> Result<()> {
        time::pause();

        let (_stream, manager) = setup_timer_manager().await?;
        let trigger = create_test_trigger("lifecycle-key", 60, TimerType::Application)?;

        // 1. Schedule timer
        manager.schedule(trigger.clone()).await?;
        let times = manager
            .scheduled_times(&trigger.key, TimerType::Application)
            .try_collect::<Vec<_>>()
            .await?;
        assert_eq!(times.len(), 1);

        // 2. Verify timer exists
        assert!(times.contains(&trigger.time));

        // 3. Complete timer
        manager
            .complete(&trigger.key, trigger.time, TimerType::Application)
            .await?;
        let times = manager
            .scheduled_times(&trigger.key, TimerType::Application)
            .try_collect::<Vec<_>>()
            .await?;
        assert!(times.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn test_edge_case_same_time_different_keys() -> Result<()> {
        time::pause();

        let (_stream, manager) = setup_timer_manager().await?;
        let base_time = CompactDateTime::now()?.add_duration(CompactDuration::new(60))?;

        // Schedule multiple timers for the same time but different keys
        let triggers = vec![
            Trigger::new(
                Key::from("key-1"),
                base_time,
                TimerType::Application,
                Span::current(),
            ),
            Trigger::new(
                Key::from("key-2"),
                base_time,
                TimerType::Application,
                Span::current(),
            ),
            Trigger::new(
                Key::from("key-3"),
                base_time,
                TimerType::Application,
                Span::current(),
            ),
        ];

        for trigger in &triggers {
            manager.schedule(trigger.clone()).await?;
        }

        // Verify each key has exactly one timer at the same time
        for trigger in &triggers {
            let times = manager
                .scheduled_times(&trigger.key, TimerType::Application)
                .try_collect::<Vec<_>>()
                .await?;
            assert_eq!(times.len(), 1);
            assert!(times.contains(&base_time));
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_time_boundary_conditions() -> Result<()> {
        time::pause();

        let (_stream, manager) = setup_timer_manager().await?;

        // Test with minimum time (current time)
        let now = CompactDateTime::now()?;
        let trigger_now = Trigger::new(
            Key::from("boundary-now"),
            now,
            TimerType::Application,
            Span::current(),
        );

        let result = manager.schedule(trigger_now.clone()).await;
        assert!(result.is_ok(), "Scheduling at current time should succeed");

        // Test with far future time
        let far_future = now.add_duration(CompactDuration::new(86400 * 365))?; // 1 year
        let trigger_future = Trigger::new(
            Key::from("boundary-future"),
            far_future,
            TimerType::Application,
            Span::current(),
        );

        let result = manager.schedule(trigger_future.clone()).await;
        assert!(result.is_ok(), "Scheduling far in future should succeed");

        // Verify both timers are stored
        let times_now = manager
            .scheduled_times(&trigger_now.key, TimerType::Application)
            .try_collect::<Vec<_>>()
            .await?;

        let times_future = manager
            .scheduled_times(&trigger_future.key, TimerType::Application)
            .try_collect::<Vec<_>>()
            .await?;

        assert_eq!(times_now.len(), 1);
        assert_eq!(times_future.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_timer_type_isolation_end_to_end() -> Result<()> {
        time::pause();

        let (mut stream, manager) = setup_timer_manager().await?;
        let key = Key::from("isolation-key");
        let time = CompactDateTime::now()?.add_duration(CompactDuration::new(1))?;

        // Schedule BOTH types at same (key, time)
        let app_trigger = Trigger::new(key.clone(), time, TimerType::Application, Span::current());
        let retry_trigger = Trigger::new(key.clone(), time, TimerType::DeferRetry, Span::current());

        manager.schedule(app_trigger.clone()).await?;
        manager.schedule(retry_trigger.clone()).await?;

        // Allow time for the scheduler to process
        time::advance(Duration::from_millis(100)).await;
        task::yield_now().await;

        // Verify both are scheduled independently via scheduled_times
        let app_times = manager
            .scheduled_times(&key, TimerType::Application)
            .try_collect::<Vec<_>>()
            .await?;
        let retry_times = manager
            .scheduled_times(&key, TimerType::DeferRetry)
            .try_collect::<Vec<_>>()
            .await?;

        assert_eq!(
            app_times.len(),
            1,
            "Should have exactly 1 Application timer"
        );
        assert_eq!(
            retry_times.len(),
            1,
            "Should have exactly 1 DeferRetry timer"
        );
        assert_eq!(app_times[0], time, "Application timer at correct time");
        assert_eq!(retry_times[0], time, "DeferRetry timer at correct time");

        // Verify both are active via is_active
        assert!(
            manager.is_active(&key, time, TimerType::Application).await,
            "Application timer should be active"
        );
        assert!(
            manager.is_active(&key, time, TimerType::DeferRetry).await,
            "DeferRetry timer should be active"
        );

        // Advance time to trigger BOTH timers
        advance(Duration::from_secs(2)).await;
        task::yield_now().await;

        // First timer fires - could be either type
        let first_timer = stream
            .next()
            .await
            .ok_or_else(|| eyre!("First timer should fire"))?;
        let (first_trigger, first_guard) = first_timer.into_inner();

        // Second timer fires - should be the other type
        let second_timer = stream
            .next()
            .await
            .ok_or_else(|| eyre!("Second timer should fire"))?;
        let (second_trigger, second_guard) = second_timer.into_inner();

        // Verify we got both types (order may vary)
        let fired_types = [first_trigger.timer_type, second_trigger.timer_type];
        assert!(
            fired_types.contains(&TimerType::Application),
            "Application timer should fire"
        );
        assert!(
            fired_types.contains(&TimerType::DeferRetry),
            "DeferRetry timer should fire"
        );

        // Both should have same key and time
        assert_eq!(first_trigger.key, key, "First timer has correct key");
        assert_eq!(first_trigger.time, time, "First timer has correct time");
        assert_eq!(second_trigger.key, key, "Second timer has correct key");
        assert_eq!(second_trigger.time, time, "Second timer has correct time");

        // Separate the guards by type
        let (app_guard, retry_guard) = if first_trigger.timer_type == TimerType::Application {
            (first_guard, second_guard)
        } else {
            (second_guard, first_guard)
        };

        // Commit only the Application timer
        app_guard.commit().await;

        // Verify only Application is gone from storage (isolation test!)
        let app_times = manager
            .scheduled_times(&key, TimerType::Application)
            .try_collect::<Vec<_>>()
            .await?;
        let retry_times = manager
            .scheduled_times(&key, TimerType::DeferRetry)
            .try_collect::<Vec<_>>()
            .await?;

        assert!(
            app_times.is_empty(),
            "Application timer should be removed after commit"
        );
        assert_eq!(
            retry_times.len(),
            1,
            "DeferRetry timer should still exist (isolation!)"
        );

        // Commit the DeferRetry timer
        retry_guard.commit().await;

        // Verify both types are now gone
        let app_times = manager
            .scheduled_times(&key, TimerType::Application)
            .try_collect::<Vec<_>>()
            .await?;
        let retry_times = manager
            .scheduled_times(&key, TimerType::DeferRetry)
            .try_collect::<Vec<_>>()
            .await?;

        assert!(app_times.is_empty(), "No Application timers remain");
        assert!(retry_times.is_empty(), "No DeferRetry timers remain");

        Ok(())
    }

    #[tokio::test]
    async fn test_timer_type_unschedule_isolation() -> Result<()> {
        time::pause();

        let (mut stream, manager) = setup_timer_manager().await?;
        let key = Key::from("unschedule-isolation-key");
        let time = CompactDateTime::now()?.add_duration(CompactDuration::new(1))?;

        // Schedule BOTH types at same (key, time)
        let app_trigger = Trigger::new(key.clone(), time, TimerType::Application, Span::current());
        let retry_trigger = Trigger::new(key.clone(), time, TimerType::DeferRetry, Span::current());

        manager.schedule(app_trigger.clone()).await?;
        manager.schedule(retry_trigger.clone()).await?;

        // Allow time for the scheduler to process
        time::advance(Duration::from_millis(100)).await;
        task::yield_now().await;

        // Verify both are scheduled
        let app_times = manager
            .scheduled_times(&key, TimerType::Application)
            .try_collect::<Vec<_>>()
            .await?;
        let retry_times = manager
            .scheduled_times(&key, TimerType::DeferRetry)
            .try_collect::<Vec<_>>()
            .await?;

        assert_eq!(app_times.len(), 1, "Should have 1 Application timer");
        assert_eq!(retry_times.len(), 1, "Should have 1 DeferRetry timer");

        // Unschedule ONLY the Application timer
        manager
            .unschedule(&key, time, TimerType::Application)
            .await?;

        // Allow scheduler to process the unschedule
        task::yield_now().await;

        // Verify Application is gone but DeferRetry remains (isolation!)
        let app_times = manager
            .scheduled_times(&key, TimerType::Application)
            .try_collect::<Vec<_>>()
            .await?;
        let retry_times = manager
            .scheduled_times(&key, TimerType::DeferRetry)
            .try_collect::<Vec<_>>()
            .await?;

        assert!(
            app_times.is_empty(),
            "Application timer should be unscheduled"
        );
        assert_eq!(
            retry_times.len(),
            1,
            "DeferRetry timer should still exist (isolation!)"
        );

        // Verify Application is not active but DeferRetry is
        assert!(
            !manager.is_active(&key, time, TimerType::Application).await,
            "Application timer should not be active after unschedule"
        );
        assert!(
            manager.is_active(&key, time, TimerType::DeferRetry).await,
            "DeferRetry timer should still be active"
        );

        // Advance time to trigger timers
        advance(Duration::from_secs(2)).await;
        task::yield_now().await;

        // Only DeferRetry should fire
        let fired_timer = stream
            .next()
            .await
            .ok_or_else(|| eyre!("DeferRetry timer should fire"))?;
        let (fired_trigger, guard) = fired_timer.into_inner();

        assert_eq!(fired_trigger.key, key, "Fired timer has correct key");
        assert_eq!(fired_trigger.time, time, "Fired timer has correct time");
        assert_eq!(
            fired_trigger.timer_type,
            TimerType::DeferRetry,
            "Only DeferRetry should fire"
        );

        // Commit the DeferRetry timer
        guard.commit().await;

        // Verify no more timers fire (Application was unscheduled)
        advance(Duration::from_secs(1)).await;
        task::yield_now().await;

        let no_more_timers = timeout(Duration::from_millis(100), stream.next()).await;

        assert!(
            no_more_timers.is_err(),
            "No more timers should fire (Application was unscheduled)"
        );

        Ok(())
    }
}
