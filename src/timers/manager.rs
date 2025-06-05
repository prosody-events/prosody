//! Timer management and coordination for scheduled events.
//!
//! This module provides the [`TimerManager`], which serves as the primary interface
//! for scheduling, managing, and delivering timer events. The manager coordinates
//! between persistent storage, in-memory scheduling, and event delivery to ensure
//! reliable timer processing across distributed systems.
//!
//! ## Architecture Overview
//!
//! The [`TimerManager`] implements a multi-layered architecture:
//!
//! ```text
//! ┌─────────────────┐
//! │   Application   │ ← Timer events delivered here
//! └─────────────────┘
//!          ↑
//! ┌─────────────────┐
//! │  TimerManager   │ ← Primary coordination layer
//! └─────────────────┘
//!          ↑
//! ┌─────────────────┐
//! │   Scheduler     │ ← In-memory event scheduling
//! └─────────────────┘
//!          ↑
//! ┌─────────────────┐
//! │  Slab Loader    │ ← Preload upcoming timers
//! └─────────────────┘
//!          ↑
//! ┌─────────────────┐
//! │ Trigger Store   │ ← Persistent storage
//! └─────────────────┘
//! ```
//!
//! ## Key Features
//!
//! - **Persistent Storage**: Timers survive application restarts
//! - **Distributed Coordination**: Multiple instances can share timer processing
//! - **Preloading**: Upcoming timers are loaded before their execution time
//! - **Efficient Scheduling**: In-memory queues for precise timing
//! - **Key-based Operations**: Schedule and query timers by entity keys
//!
//! ## Usage Example
//!
//! ```rust,no_run
//! use prosody::timers::TimerManager;
//! use prosody::timers::store::memory::InMemoryTriggerStore;
//! use prosody::timers::duration::CompactDuration;
//! use uuid::Uuid;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let store = InMemoryTriggerStore::new();
//! let segment_id = Uuid::new_v4();
//! let slab_size = CompactDuration::new(300); // 5 minutes
//!
//! let (timer_stream, manager) = TimerManager::new(
//!     segment_id,
//!     slab_size,
//!     "my-application",
//!     store,
//! ).await?;
//!
//! // Manager is now ready for scheduling timers
//! # Ok(())
//! # }
//! ```

use crate::Key;
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;

use crate::timers::Trigger;
pub use crate::timers::error::TimerManagerError;
use crate::timers::loader::{State, get_or_create_segment, slab_loader};
use crate::timers::scheduler::TriggerScheduler;
use crate::timers::slab::Slab;
use crate::timers::slab_lock::SlabLock;
use crate::timers::store::{Segment, SegmentId, TriggerStore};
use crate::timers::uncommitted::UncommittedTimer;
use educe::Educe;
use futures::stream::iter;
use futures::{Stream, StreamExt, TryStreamExt};
use std::sync::Arc;

use tokio::spawn;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{Instrument, Span};

/// Maximum number of concurrent delete operations for timer cleanup.
///
/// This constant controls the parallelism level when performing bulk
/// cleanup operations such as removing all timers for a specific key.
pub const DELETE_CONCURRENCY: usize = 16;

/// Manages timer scheduling, storage, and delivery for a specific segment.
///
/// [`TimerManager`] serves as the primary interface for timer operations within
/// a specific segment (typically corresponding to a consumer group). It coordinates
/// between persistent storage, in-memory scheduling, and event delivery to provide
/// reliable timer functionality.
///
/// ## Responsibilities
///
/// - **Persistent Storage**: Ensures timers survive application restarts
/// - **Scheduling**: Maintains in-memory queues for precise timer delivery
/// - **Preloading**: Loads upcoming timers before their execution time
/// - **Coordination**: Manages distributed timer processing across multiple instances
/// - **Lifecycle**: Handles timer creation, scheduling, execution, and cleanup
///
/// ## Thread Safety
///
/// [`TimerManager`] is designed for concurrent access and can be safely shared
/// across multiple threads. All operations are internally synchronized.
///
/// ## Resource Management
///
/// The manager automatically spawns background tasks for:
/// - Loading upcoming timer slabs
/// - Maintaining in-memory timer queues
/// - Coordinating with the persistent store
///
/// These tasks are automatically cleaned up when the manager is dropped.
///
/// ## Generic Parameters
///
/// * `T` - The [`TriggerStore`] implementation used for persistent storage
#[derive(Educe)]
#[educe(Debug(bound = ""), Clone(bound()))]
pub struct TimerManager<T>(#[educe(Debug(ignore))] Arc<TimerManagerInner<T>>);

/// Internal state for the timer manager.
///
/// This structure contains the segment configuration and shared state
/// needed for timer coordination between the storage layer and scheduler.
pub struct TimerManagerInner<T> {
    /// The segment configuration for this timer manager instance.
    segment: Segment,
    
    /// Shared state including the trigger store and scheduler.
    state: SlabLock<State<T>>,
}

impl<T> TimerManager<T>
where
    T: TriggerStore,
{
    /// Creates a new timer manager for the specified segment.
    ///
    /// This operation initializes the timer management system for a specific
    /// segment, including setting up the persistent storage connection,
    /// in-memory scheduler, and background slab loading tasks.
    ///
    /// # Arguments
    ///
    /// * `segment_id` - Unique identifier for the timer segment
    /// * `slab_size` - Time span for each slab (determines timer partitioning)
    /// * `name` - Human-readable name for the segment (used for monitoring)
    /// * `store` - The persistent storage implementation to use
    ///
    /// # Returns
    ///
    /// A tuple containing:
    /// - A [`Stream`] of [`UncommittedTimer`] events ready for processing
    /// - The [`TimerManager`] instance for scheduling new timers
    ///
    /// # Errors
    ///
    /// Returns [`TimerManagerError`] if:
    /// - The segment cannot be created or retrieved from storage
    /// - The scheduler initialization fails
    /// - Background tasks cannot be spawned
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use prosody::timers::TimerManager;
    /// use prosody::timers::store::memory::InMemoryTriggerStore;
    /// use prosody::timers::duration::CompactDuration;
    /// use uuid::Uuid;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let store = InMemoryTriggerStore::new();
    /// let (timer_stream, manager) = TimerManager::new(
    ///     Uuid::new_v4(),
    ///     CompactDuration::new(300), // 5-minute slabs
    ///     "order-processor",
    ///     store,
    /// ).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn new(
        segment_id: SegmentId,
        slab_size: CompactDuration,
        name: &str,
        store: T,
    ) -> Result<(impl Stream<Item = UncommittedTimer<T>>, Self), TimerManagerError<T::Error>> {
        let segment = get_or_create_segment(&store, segment_id, slab_size, name).await?;
        let (trigger_rx, scheduler) = TriggerScheduler::new();
        let state = SlabLock::new(State::new(store, scheduler));

        spawn(slab_loader(segment.clone(), state.clone()));

        let manager = Self(Arc::new(TimerManagerInner { segment, state }));
        let cloned_manager = manager.clone();
        let stream = ReceiverStream::new(trigger_rx)
            .map(move |trigger| UncommittedTimer::new(trigger, cloned_manager.clone()));

        Ok((stream, manager))
    }

    /// Retrieves all scheduled times for a specific key.
    ///
    /// This method queries the persistent storage to find all times when
    /// timers are scheduled to execute for the specified key. This is useful
    /// for displaying upcoming events or validating timer schedules.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to query for scheduled times
    ///
    /// # Returns
    ///
    /// A [`Vec`] of [`CompactDateTime`] values representing all scheduled
    /// execution times for the specified key, sorted in chronological order.
    ///
    /// # Errors
    ///
    /// Returns [`TimerManagerError`] if:
    /// - The storage system is unavailable
    /// - The query operation fails
    /// - Data corruption is detected
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use prosody::Key;
    /// # use prosody::timers::TimerManager;
    /// # use prosody::timers::store::memory::InMemoryTriggerStore;
    /// # async fn example(manager: &TimerManager<InMemoryTriggerStore>) -> Result<(), Box<dyn std::error::Error>> {
    /// let key = Key::from("user-123");
    /// let times = manager.scheduled_times(&key).await?;
    /// 
    /// println!("User 123 has {} scheduled timers", times.len());
    /// for time in times {
    ///     println!("Timer at: {}", time);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn scheduled_times(
        &self,
        key: &Key,
    ) -> Result<Vec<CompactDateTime>, TimerManagerError<T::Error>> {
        self.0
            .state
            .trigger_lock()
            .await
            .store
            .get_key_triggers(&self.0.segment.id, key)
            .map_err(TimerManagerError::Store)
            .try_collect()
            .await
    }

    /// Schedules a new timer for execution.
    ///
    /// This method adds a new timer to both the persistent storage and the
    /// in-memory scheduler (if the timer's slab is currently owned by this
    /// manager instance). The timer will be delivered as an [`UncommittedTimer`]
    /// when its scheduled time arrives.
    ///
    /// # Arguments
    ///
    /// * `trigger` - The timer event to schedule, containing the key, time, and tracing span
    ///
    /// # Returns
    ///
    /// `Ok(())` if the timer was successfully scheduled, or a [`TimerManagerError`]
    /// if the operation failed.
    ///
    /// # Errors
    ///
    /// Returns [`TimerManagerError`] if:
    /// - The timer time is in the past
    /// - The storage operation fails
    /// - The scheduler is unavailable
    /// - The trigger data is invalid
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use prosody::Key;
    /// use prosody::timers::{Trigger, datetime::CompactDateTime, duration::CompactDuration};
    /// use tracing::Span;
    /// # use prosody::timers::TimerManager;
    /// # use prosody::timers::store::memory::InMemoryTriggerStore;
    /// # async fn example(manager: &TimerManager<InMemoryTriggerStore>) -> Result<(), Box<dyn std::error::Error>> {
    /// 
    /// let future_time = CompactDateTime::now()?
    ///     .add_duration(CompactDuration::new(3600))?; // 1 hour from now
    /// 
    /// let trigger = Trigger {
    ///     key: Key::from("order-456"),
    ///     time: future_time,
    ///     span: Span::current(),
    /// };
    /// 
    /// manager.schedule(trigger).await?;
    /// println!("Timer scheduled successfully");
    /// # Ok(())
    /// # }
    /// ```
    pub async fn schedule(&self, trigger: Trigger) -> Result<(), TimerManagerError<T::Error>> {
        let slab = Slab::from_time(self.0.segment.id, self.0.segment.slab_size, trigger.time);
        let slab_id = slab.id();
        let state = self.0.state.trigger_lock().await;

        state
            .store
            .add_trigger(&self.0.segment, slab, trigger.clone())
            .await
            .map_err(TimerManagerError::Store)?;

        if state.is_owned(slab_id) {
            state.scheduler.schedule(trigger).await?;
        }

        Ok(())
    }

    /// Cancels a specific scheduled timer.
    ///
    /// This method removes a timer from both the persistent storage and the
    /// in-memory scheduler. If the timer has already been delivered but not
    /// yet committed, this operation will not affect the delivered timer.
    ///
    /// # Arguments
    ///
    /// * `key` - The key of the timer to cancel
    /// * `time` - The scheduled execution time of the timer to cancel
    ///
    /// # Returns
    ///
    /// `Ok(())` if the timer was successfully canceled (or did not exist),
    /// or a [`TimerManagerError`] if the operation failed.
    ///
    /// # Errors
    ///
    /// Returns [`TimerManagerError`] if:
    /// - The storage operation fails
    /// - The scheduler is unavailable
    /// - The timer data is corrupted
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use prosody::Key;
    /// use prosody::timers::datetime::CompactDateTime;
    /// # use prosody::timers::TimerManager;
    /// # use prosody::timers::store::memory::InMemoryTriggerStore;
    /// # async fn example(manager: &TimerManager<InMemoryTriggerStore>) -> Result<(), Box<dyn std::error::Error>> {
    /// 
    /// let key = Key::from("session-789");
    /// let time = CompactDateTime::from(1234567890_u32);
    /// 
    /// manager.unschedule(&key, time).await?;
    /// println!("Timer canceled successfully");
    /// # Ok(())
    /// # }
    /// ```
    pub async fn unschedule(
        &self,
        key: &Key,
        time: CompactDateTime,
    ) -> Result<(), TimerManagerError<T::Error>> {
        let slab = Slab::from_time(self.0.segment.id, self.0.segment.slab_size, time);
        let slab_id = slab.id();
        let state = self.0.state.trigger_lock().await;

        if state.is_owned(slab_id) {
            let trigger = Trigger {
                key: key.clone(),
                time,
                span: Span::current(),
            };

            state.scheduler.unschedule(trigger).await?;
        }

        state
            .store
            .remove_trigger(&self.0.segment, &slab, key, time)
            .await
            .map_err(TimerManagerError::Store)
    }

    /// Cancels all scheduled timers for a specific key.
    ///
    /// This method removes all timers associated with the specified key from
    /// both persistent storage and in-memory scheduling. This is useful when
    /// an entity is deleted or when all related timers should be canceled.
    ///
    /// # Arguments
    ///
    /// * `key` - The key for which to cancel all timers
    ///
    /// # Returns
    ///
    /// `Ok(())` if all timers were successfully canceled, or a [`TimerManagerError`]
    /// if the operation failed. Partial failures may occur where some timers
    /// are canceled before an error occurs.
    ///
    /// # Errors
    ///
    /// Returns [`TimerManagerError`] if:
    /// - The storage query or delete operations fail
    /// - The scheduler is unavailable
    /// - Individual timer operations fail
    ///
    /// # Performance
    ///
    /// This operation may be expensive for keys with many scheduled timers,
    /// as it requires querying all scheduled times and then removing each
    /// timer individually. Consider the performance implications for keys
    /// with hundreds or thousands of scheduled timers.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use prosody::Key;
    /// # use prosody::timers::TimerManager;
    /// # use prosody::timers::store::memory::InMemoryTriggerStore;
    /// # async fn example(manager: &TimerManager<InMemoryTriggerStore>) -> Result<(), Box<dyn std::error::Error>> {
    /// 
    /// let key = Key::from("user-account-123");
    /// 
    /// // Cancel all timers when user deletes their account
    /// manager.unschedule_all(&key).await?;
    /// println!("All timers for user account canceled");
    /// # Ok(())
    /// # }
    /// ```
    pub async fn unschedule_all(&self, key: &Key) -> Result<(), TimerManagerError<T::Error>> {
        let span = Span::current();
        let times = self.scheduled_times(key).await?;

        let futures = times
            .into_iter()
            .map(|time| self.unschedule(key, time).instrument(span.clone()));

        iter(futures)
            .buffer_unordered(DELETE_CONCURRENCY)
            .try_collect::<()>()
            .await
    }

    /// Checks if a timer is currently active in the in-memory scheduler.
    ///
    /// This method queries the in-memory scheduler to determine if a specific
    /// timer is currently loaded and active. A timer is considered active if
    /// it has been loaded from storage and is awaiting execution or has been
    /// delivered but not yet committed.
    ///
    /// # Arguments
    ///
    /// * `key` - The key of the timer to check
    /// * `time` - The scheduled execution time of the timer to check
    ///
    /// # Returns
    ///
    /// `true` if the timer is active in the scheduler, `false` otherwise.
    /// Note that `false` does not necessarily mean the timer doesn't exist;
    /// it may be stored persistently but not currently loaded.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use prosody::Key;
    /// use prosody::timers::datetime::CompactDateTime;
    /// # use prosody::timers::TimerManager;
    /// # use prosody::timers::store::memory::InMemoryTriggerStore;
    /// # async fn example(manager: &TimerManager<InMemoryTriggerStore>) {
    /// 
    /// let key = Key::from("task-456");
    /// let time = CompactDateTime::from(1234567890_u32);
    /// 
    /// if manager.is_active(&key, time).await {
    ///     println!("Timer is currently active in scheduler");
    /// } else {
    ///     println!("Timer is not currently active (may be in storage)");
    /// }
    /// # }
    /// ```
    pub async fn is_active(&self, key: &Key, time: CompactDateTime) -> bool {
        self.0
            .state
            .trigger_lock()
            .await
            .scheduler
            .is_active(key, time)
            .await
    }

    /// Marks a timer as completed and removes it from all storage.
    ///
    /// This method is called when a timer has been successfully processed
    /// and should be permanently removed. It removes the timer from both
    /// the in-memory scheduler and persistent storage.
    ///
    /// # Arguments
    ///
    /// * `key` - The key of the completed timer
    /// * `time` - The execution time of the completed timer
    ///
    /// # Returns
    ///
    /// `Ok(())` if the timer was successfully marked as complete, or a
    /// [`TimerManagerError`] if the operation failed.
    ///
    /// # Errors
    ///
    /// Returns [`TimerManagerError`] if:
    /// - The storage removal operation fails
    /// - The timer was not found in storage
    /// - The scheduler deactivation fails
    ///
    /// # Usage
    ///
    /// This method is typically called automatically when an [`UncommittedTimer`]
    /// is committed. Direct usage is rare but may be needed for cleanup operations.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use prosody::Key;
    /// use prosody::timers::datetime::CompactDateTime;
    /// # use prosody::timers::TimerManager;
    /// # use prosody::timers::store::memory::InMemoryTriggerStore;
    /// # async fn example(manager: &TimerManager<InMemoryTriggerStore>) -> Result<(), Box<dyn std::error::Error>> {
    /// 
    /// let key = Key::from("completed-task");
    /// let time = CompactDateTime::from(1234567890_u32);
    /// 
    /// // Mark timer as completed (usually done automatically)
    /// manager.complete(&key, time).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn complete(
        &self,
        key: &Key,
        time: CompactDateTime,
    ) -> Result<(), TimerManagerError<T::Error>> {
        let slab = Slab::from_time(self.0.segment.id, self.0.segment.slab_size, time);
        let slab_id = slab.id();
        let state = self.0.state.trigger_lock().await;

        if state.is_owned(slab_id) {
            state.scheduler.deactivate(key, time).await;
        }

        state
            .store
            .remove_trigger(&self.0.segment, &slab, key, time)
            .await
            .map_err(TimerManagerError::Store)?;

        Ok(())
    }

    /// Aborts a timer without removing it from persistent storage.
    ///
    /// This method deactivates a timer in the in-memory scheduler but leaves
    /// it in persistent storage. This is typically used when a timer delivery
    /// fails and should not be retried immediately, but may be reloaded later.
    ///
    /// # Arguments
    ///
    /// * `key` - The key of the timer to abort
    /// * `time` - The execution time of the timer to abort
    ///
    /// # Usage
    ///
    /// This method is typically called automatically when an [`UncommittedTimer`]
    /// is aborted due to processing failure or shutdown conditions. It ensures
    /// the timer is not retried immediately while preserving it for potential
    /// future execution.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use prosody::Key;
    /// use prosody::timers::datetime::CompactDateTime;
    /// # use prosody::timers::TimerManager;
    /// # use prosody::timers::store::memory::InMemoryTriggerStore;
    /// # async fn example(manager: &TimerManager<InMemoryTriggerStore>) {
    /// 
    /// let key = Key::from("failed-task");
    /// let time = CompactDateTime::from(1234567890_u32);
    /// 
    /// // Abort timer due to processing failure
    /// manager.abort(&key, time).await;
    /// # }
    /// ```
    pub async fn abort(&self, key: &Key, time: CompactDateTime) {
        let slab = Slab::from_time(self.0.segment.id, self.0.segment.slab_size, time);
        let slab_id = slab.id();
        let state = self.0.state.trigger_lock().await;

        if state.is_owned(slab_id) {
            state.scheduler.deactivate(key, time).await;
        }
    }
}
