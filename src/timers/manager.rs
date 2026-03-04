//! Timer management and coordination for scheduled events.
//!
//! The [`TimerManager`] serves as the primary interface for scheduling,
//! querying, and canceling timers within a specific segment. It coordinates
//! between:
//! - **Persistent Storage**: Durable [`TriggerStore`] for timer metadata.
//! - **Background Slab Loader**: Preloads upcoming timer slabs.
//! - **In-Memory Scheduler**: Precise, delay-queue based timer dispatch.
//! - **Application**: Delivers timers as an async stream of [`PendingTimer`].
//!
//! The manager ensures timers survive restarts, supports distributed ownership,
//! and provides at-least-once delivery semantics for timer events.

use crate::Key;
use crate::heartbeat::HeartbeatRegistry;
use crate::timers::active::TimerState;
use crate::timers::datetime::CompactDateTime;

pub use crate::timers::error::TimerManagerError;
use crate::timers::loader::{State, get_or_create_segment, slab_loader};
use crate::timers::scheduler::TriggerScheduler;
use crate::timers::slab::Slab;
use crate::timers::slab_lock::SlabLock;
use crate::timers::store::{Segment, TriggerStore};
use crate::timers::{DELETE_CONCURRENCY, PendingTimer, TimerType, Trigger};
use educe::Educe;
use futures::{Stream, StreamExt, TryStreamExt, stream};
use std::sync::Arc;
use tokio::spawn;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{Instrument, Span, debug};

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
        name: &str,
        store: T,
        heartbeats: HeartbeatRegistry,
    ) -> Result<(impl Stream<Item = PendingTimer<T>>, Self), TimerManagerError<T::Error>> {
        // Ensure the segment exists in persistent storage.
        let segment = get_or_create_segment(&store, name).await?;

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
    /// **State-aware filtering:**
    /// - Excludes timers in `Firing` state (being processed, not scheduled to
    ///   fire again).
    /// - Includes timers in `Scheduled` state (waiting to fire).
    /// - Includes timers in `FiringRescheduled` state (will fire again after
    ///   current handler completes).
    ///
    /// # Arguments
    ///
    /// * `key` - The entity key whose timers to list.
    /// * `timer_type` - The [`TimerType`] classification to filter by.
    ///
    /// # Returns
    ///
    /// A `Vec` of scheduled times for `key` that will fire in the future.
    ///
    /// # Errors
    ///
    /// Returns [`TimerManagerError::Store`] if the underlying storage query
    /// fails.
    pub async fn scheduled_times(
        &self,
        key: &Key,
        timer_type: TimerType,
    ) -> Result<Vec<CompactDateTime>, TimerManagerError<T::Error>> {
        let state = self.0.state.trigger_lock().await;

        let active_triggers = state.scheduler.active_triggers();

        // Stream from storage and filter in a single pass — no intermediate Vec.
        //
        // Include if:
        // - Not in ActiveTriggers (timer not loaded yet, will fire when slab loads)
        // - In Scheduled state (waiting to fire)
        // - In FiringRescheduled state (will fire again after commit)
        // Exclude if:
        // - In Firing state (currently being processed, won't fire again unless
        //   rescheduled)
        let stream = state
            .store
            .get_key_times(timer_type, key)
            .map_err(TimerManagerError::Store);

        stream
            .try_filter(|&time| {
                let is_scheduled = active_triggers.is_scheduled(key, time, timer_type);
                let not_active = active_triggers.contains(key, time, timer_type);
                async move {
                    let is_scheduled = is_scheduled.await;
                    let not_active = !not_active.await;
                    is_scheduled || not_active
                }
            })
            .try_collect()
            .await
    }

    /// Schedules a new timer for future execution.
    ///
    /// Inserts the timer into persistent storage and, if its slab is currently
    /// owned, enqueues it in the in-memory scheduler.
    ///
    /// **State-aware behavior:**
    /// - If the timer is in `Firing` state (same key, time, type), transitions
    ///   to `FiringRescheduled` and adds to `DelayQueue` without DB write.
    /// - If the timer is in `FiringRescheduled` state, this is idempotent
    ///   (no-op).
    /// - Otherwise, performs normal scheduling with DB write.
    ///
    /// **Singleton vs Overflow routing:**
    /// - First timer for a key/type → written to singleton slot (via store
    ///   layer)
    /// - Second+ timer → promotes to overflow (clustering columns)
    /// - Use [`clear_and_schedule`](Self::clear_and_schedule) for
    ///   tombstone-free singleton overwrites
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
        let slab = Slab::from_time(self.0.segment.slab_size, trigger.time);
        let slab_id = slab.id();
        let state = self.0.state.trigger_lock().await;

        // Check current state for state-aware transitions.
        let current_state = state
            .scheduler
            .active_triggers()
            .get_state(&trigger.key, trigger.time, trigger.timer_type)
            .await;

        match current_state {
            // FIRING → FIRING_RESCHEDULED: transition state and add to queue
            Some(TimerState::Firing) => {
                // Transition to FiringRescheduled
                state
                    .scheduler
                    .active_triggers()
                    .set_state(
                        &trigger.key,
                        trigger.time,
                        trigger.timer_type,
                        TimerState::FiringRescheduled,
                    )
                    .await;

                // Add to DelayQueue (no DB write - row already exists)
                state.scheduler.add_to_queue(trigger).await?;

                Ok(())
            }

            // Already FIRING_RESCHEDULED: idempotent no-op
            Some(TimerState::FiringRescheduled) => Ok(()),

            // SCHEDULED or UNSCHEDULED: normal scheduling path
            _ => {
                // Singleton detection: Check if this key/type already has timers.
                // If no existing timers, this is a singleton case and the store
                // layer will write to the static slot. If existing timers, this
                // triggers singleton→overflow promotion in the store layer.
                //
                // NOTE: The actual singleton vs overflow routing is handled by
                // the store's add_trigger implementation which checks slot state.
                // This comment documents the expected behavior for clarity.

                // Persist the trigger in both slab and key indices.
                // The store layer handles singleton/overflow routing internally.
                state
                    .store
                    .add_trigger(slab, trigger.clone())
                    .await
                    .map_err(TimerManagerError::Store)?;

                // If we own the slab, enqueue in the in-memory scheduler.
                if state.is_owned(slab_id) {
                    state.scheduler.schedule(trigger).await?;
                }

                Ok(())
            }
        }
    }

    /// Cancels a specific scheduled timer.
    ///
    /// Removes the timer from persistent storage and, if owned, from the
    /// in-memory scheduler. If already delivered, the delivery is not reversed.
    ///
    /// **State-aware behavior:**
    /// - If the timer is in `Firing` state (being processed), this is a no-op.
    ///   The handler is already processing it; unschedule has no effect.
    /// - If the timer is in `FiringRescheduled` state, transitions back to
    ///   `Firing` and removes from `DelayQueue`. The timer will complete
    ///   normally without firing again.
    /// - Otherwise, performs normal unscheduling with DB and scheduler removal.
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
        let slab = Slab::from_time(self.0.segment.slab_size, time);
        let slab_id = slab.id();
        let state = self.0.state.trigger_lock().await;

        // Check current state for state-aware transitions.
        let current_state = state
            .scheduler
            .active_triggers()
            .get_state(key, time, timer_type)
            .await;

        match current_state {
            // FIRING: no-op - timer is being processed, unschedule has no effect
            Some(TimerState::Firing) => Ok(()),

            // FIRING_RESCHEDULED → FIRING: cancel the reschedule
            Some(TimerState::FiringRescheduled) => {
                // Transition back to Firing
                state
                    .scheduler
                    .active_triggers()
                    .set_state(key, time, timer_type, TimerState::Firing)
                    .await;

                // Remove from DelayQueue (no DB change - row still needed for this
                // firing)
                let trigger = Trigger::new(key.clone(), time, timer_type, Span::current());
                state.scheduler.remove_from_queue(trigger).await?;

                Ok(())
            }

            // SCHEDULED or UNSCHEDULED: normal unscheduling path
            _ => {
                // Remove from in-memory scheduler if owned.
                if state.is_owned(slab_id) {
                    let trigger = Trigger::new(key.clone(), time, timer_type, Span::current());
                    state.scheduler.unschedule(trigger).await?;
                }

                // Remove from persistent storage.
                state
                    .store
                    .remove_trigger(&slab, key, time, timer_type)
                    .await
                    .map_err(TimerManagerError::Store)
            }
        }
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
        let times = self.scheduled_times(key, timer_type).await?;

        stream::iter(times)
            .map(|time| {
                self.unschedule(key, time, timer_type)
                    .instrument(span.clone())
            })
            .buffer_unordered(DELETE_CONCURRENCY)
            .try_collect::<()>()
            .await
    }

    /// Atomically clears existing timers and schedules a new one.
    ///
    /// This is the optimized path for singleton timer overwrites. It:
    /// 1. Reads existing triggers to determine which slabs need cleanup
    /// 2. Updates the in-memory scheduler state (unschedule old, schedule new)
    /// 3. Calls the store's `clear_and_schedule` for atomic persistence
    ///
    /// # State-aware behavior
    ///
    /// For each existing timer at a different time:
    /// - `Firing` → no-op (timer is being processed)
    /// - `FiringRescheduled` → transitions to `Firing` (cancels reschedule)
    /// - `Scheduled` → unscheduled from `DelayQueue`
    ///
    /// For the new timer:
    /// - If same time as existing `Firing` → transitions to `FiringRescheduled`
    /// - Otherwise → schedules in `DelayQueue`
    ///
    /// # Arguments
    ///
    /// * `trigger` - The new timer to schedule (replaces all existing)
    ///
    /// # Errors
    ///
    /// Returns [`TimerManagerError`] if:
    /// - Storage operations fail
    /// - Scheduler operations fail
    pub async fn clear_and_schedule(
        &self,
        trigger: Trigger,
    ) -> Result<(), TimerManagerError<T::Error>> {
        let new_slab = Slab::from_time(self.0.segment.slab_size, trigger.time);
        let new_slab_id = new_slab.id();
        let state = self.0.state.trigger_lock().await;

        // Get all existing triggers for this key/type
        let existing_triggers: Vec<Trigger> = state
            .store
            .get_key_triggers(trigger.timer_type, &trigger.key)
            .map_err(TimerManagerError::Store)
            .try_collect()
            .await?;

        // Collect slabs that contain old triggers (for cleanup)
        let old_slabs: Vec<Slab> = existing_triggers
            .iter()
            .filter(|t| t.time != trigger.time)
            .map(|t| Slab::from_time(self.0.segment.slab_size, t.time))
            .collect();

        debug!(
            key = %trigger.key,
            timer_type = ?trigger.timer_type,
            new_time = ?trigger.time,
            existing_count = existing_triggers.len(),
            old_slab_count = old_slabs.len(),
            "clear_and_schedule: read existing triggers, preparing state transitions"
        );

        // Step 1: Handle scheduler state for the new timer.
        // STATE MACHINE PRESERVATION: Reuses the same Firing/FiringRescheduled/
        // Scheduled transitions as schedule() and unschedule(). No new states
        // are introduced; clear_and_schedule composes existing transitions.
        let new_timer_state = state
            .scheduler
            .active_triggers()
            .get_state(&trigger.key, trigger.time, trigger.timer_type)
            .await;

        match new_timer_state {
            Some(TimerState::Firing) => {
                // Reuses schedule()'s Firing → FiringRescheduled transition
                debug!(
                    key = %trigger.key,
                    timer_type = ?trigger.timer_type,
                    time = ?trigger.time,
                    "clear_and_schedule: new timer is Firing, transitioning to FiringRescheduled"
                );
                state
                    .scheduler
                    .active_triggers()
                    .set_state(
                        &trigger.key,
                        trigger.time,
                        trigger.timer_type,
                        TimerState::FiringRescheduled,
                    )
                    .await;
                // Add to DelayQueue for re-firing
                state.scheduler.add_to_queue(trigger.clone()).await?;
            }
            Some(TimerState::FiringRescheduled) => {
                // Reuses schedule()'s idempotent FiringRescheduled no-op
                debug!(
                    key = %trigger.key,
                    timer_type = ?trigger.timer_type,
                    time = ?trigger.time,
                    "clear_and_schedule: new timer already FiringRescheduled, no-op"
                );
            }
            _ => {
                // Reuses schedule()'s normal scheduling path
                if state.is_owned(new_slab_id) {
                    debug!(
                        key = %trigger.key,
                        timer_type = ?trigger.timer_type,
                        time = ?trigger.time,
                        state = ?new_timer_state,
                        "clear_and_schedule: scheduling new timer in DelayQueue"
                    );
                    state.scheduler.schedule(trigger.clone()).await?;
                }
            }
        }

        // Step 2: Unschedule old timers from in-memory scheduler.
        // STATE MACHINE PRESERVATION: Reuses the same state-aware transitions
        // as unschedule(). Each old timer is handled according to its current
        // state without introducing new transition paths.
        unschedule_replaced_timers(&state, &self.0.segment, &trigger, &existing_triggers).await?;

        // Step 3: Persist via store's clear_and_schedule
        debug!(
            key = %trigger.key,
            timer_type = ?trigger.timer_type,
            new_time = ?trigger.time,
            old_slab_count = old_slabs.len(),
            "clear_and_schedule: persisting to store"
        );
        state
            .store
            .clear_and_schedule(new_slab, trigger, old_slabs)
            .await
            .map_err(TimerManagerError::Store)?;

        Ok(())
    }

    /// Transitions a timer from `Scheduled` to `Firing` state.
    ///
    /// Called when a timer is delivered from the queue and about to be
    /// processed by a handler. Returns `true` if the transition succeeded.
    ///
    /// # Arguments
    ///
    /// * `key` - The entity key of the timer.
    /// * `time` - The scheduled execution time.
    /// * `timer_type` - The timer type classification.
    ///
    /// # Returns
    ///
    /// `true` if the timer was successfully transitioned to `Firing`.
    pub(crate) async fn fire(
        &self,
        key: &Key,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> bool {
        self.0
            .state
            .trigger_lock()
            .await
            .scheduler
            .fire(key, time, timer_type)
            .await
    }

    /// Marks a timer as completed.
    ///
    /// **State-aware behavior:**
    /// - From `Firing`: deletes from DB and removes from `ActiveTriggers`.
    /// - From `FiringRescheduled`: transitions to `Scheduled` (keeps DB row,
    ///   timer will fire again).
    ///
    /// Typically invoked by `FiringTimer::commit()`.
    ///
    /// # Arguments
    ///
    /// * `key` - The entity key of the completed timer.
    /// * `time` - The execution time of the completed timer.
    /// * `timer_type` - The timer type classification.
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
        let slab = Slab::from_time(self.0.segment.slab_size, time);
        let slab_id = slab.id();
        let state = self.0.state.trigger_lock().await;

        // Check current state for state-aware transitions.
        let current_state = state
            .scheduler
            .active_triggers()
            .get_state(key, time, timer_type)
            .await;

        // FIRING_RESCHEDULED → SCHEDULED: keep DB row, timer fires again
        if current_state == Some(TimerState::FiringRescheduled) {
            state
                .scheduler
                .active_triggers()
                .set_state(key, time, timer_type, TimerState::Scheduled)
                .await;
            return Ok(());
        }

        // FIRING or anything else: delete from DB and remove from ActiveTriggers
        // Deactivate in-memory if owned.
        if state.is_owned(slab_id) {
            state.scheduler.deactivate(key, time, timer_type).await;
        }

        // Remove from storage.
        state
            .store
            .remove_trigger(&slab, key, time, timer_type)
            .await
            .map_err(TimerManagerError::Store)?;

        Ok(())
    }

    /// Aborts a timer delivery.
    ///
    /// **State-aware behavior:**
    /// - From `Firing`: removes from `ActiveTriggers` (DB row preserved for
    ///   recovery via slab loader).
    /// - From `FiringRescheduled`: transitions to `Scheduled` (timer already in
    ///   `DelayQueue`, will fire again without restart).
    ///
    /// Does not delete the timer from persistent storage; it can be reloaded
    /// and retried later by the slab loader (from `Firing`) or fires again
    /// immediately (from `FiringRescheduled`).
    ///
    /// # Arguments
    ///
    /// * `key` - The entity key of the timer.
    /// * `time` - The scheduled execution time to abort.
    /// * `timer_type` - The timer type classification.
    pub async fn abort(&self, key: &Key, time: CompactDateTime, timer_type: TimerType) {
        let slab = Slab::from_time(self.0.segment.slab_size, time);
        let slab_id = slab.id();
        let state = self.0.state.trigger_lock().await;

        // Check current state for state-aware transitions.
        let current_state = state
            .scheduler
            .active_triggers()
            .get_state(key, time, timer_type)
            .await;

        match current_state {
            // FIRING_RESCHEDULED → SCHEDULED: timer already in DelayQueue, fires again
            Some(TimerState::FiringRescheduled) => {
                state
                    .scheduler
                    .active_triggers()
                    .set_state(key, time, timer_type, TimerState::Scheduled)
                    .await;
            }

            // FIRING or anything else: remove from ActiveTriggers, DB preserved
            _ => {
                if state.is_owned(slab_id) {
                    state.scheduler.deactivate(key, time, timer_type).await;
                }
            }
        }
    }
}

/// Unschedules old timers from the in-memory scheduler during a
/// `clear_and_schedule` operation.
///
/// STATE MACHINE PRESERVATION: Reuses the same state-aware transitions as
/// `TimerManager::unschedule()`. Each old timer is handled according to its
/// current state without introducing new transition paths:
/// - `Firing` → no-op (being processed)
/// - `FiringRescheduled` → `Firing` (cancels reschedule)
/// - `Scheduled`/absent → removed from `DelayQueue`
async fn unschedule_replaced_timers<T: TriggerStore>(
    state: &State<T>,
    segment: &Segment,
    new_trigger: &Trigger,
    existing_triggers: &[Trigger],
) -> Result<(), TimerManagerError<T::Error>> {
    for old_trigger in existing_triggers {
        if old_trigger.time == new_trigger.time {
            continue; // Same time as new - already handled by caller
        }

        let old_slab_id = Slab::from_time(segment.slab_size, old_trigger.time).id();

        let old_state = state
            .scheduler
            .active_triggers()
            .get_state(&old_trigger.key, old_trigger.time, old_trigger.timer_type)
            .await;

        match old_state {
            Some(TimerState::Firing) => {
                debug!(
                    key = %old_trigger.key,
                    timer_type = ?old_trigger.timer_type,
                    old_time = ?old_trigger.time,
                    "clear_and_schedule: old timer is Firing, skipping (no-op)"
                );
            }
            Some(TimerState::FiringRescheduled) => {
                debug!(
                    key = %old_trigger.key,
                    timer_type = ?old_trigger.timer_type,
                    old_time = ?old_trigger.time,
                    "clear_and_schedule: old timer FiringRescheduled, cancelling reschedule"
                );
                state
                    .scheduler
                    .active_triggers()
                    .set_state(
                        &old_trigger.key,
                        old_trigger.time,
                        old_trigger.timer_type,
                        TimerState::Firing,
                    )
                    .await;
                state
                    .scheduler
                    .remove_from_queue(old_trigger.clone())
                    .await?;
            }
            _ => {
                if state.is_owned(old_slab_id) {
                    debug!(
                        key = %old_trigger.key,
                        timer_type = ?old_trigger.timer_type,
                        old_time = ?old_trigger.time,
                        state = ?old_state,
                        "clear_and_schedule: unscheduling old timer from DelayQueue"
                    );
                    state.scheduler.unschedule(old_trigger.clone()).await?;
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consumer::{Keyed, Uncommitted};
    use crate::timers::UncommittedTimer;
    use crate::timers::duration::CompactDuration;
    use crate::timers::store::SegmentVersion;
    use crate::timers::store::adapter::TableAdapter;
    use crate::timers::store::memory::{InMemoryTriggerStore, memory_store};
    use crate::timers::uncommitted::UncommittedTriggerGuard;
    use color_eyre::eyre::{Result, eyre};
    use futures::StreamExt;
    use std::time::Duration;
    use tokio::task;
    use tokio::time::{self, advance, timeout};
    use tracing::Span;
    use uuid::Uuid;

    fn test_segment() -> Segment {
        Segment {
            id: Uuid::new_v4(),
            name: "test-segment".to_owned(),
            slab_size: CompactDuration::new(300),
            version: SegmentVersion::V3,
        }
    }

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
        let store = memory_store(test_segment());
        TimerManager::new("test-manager", store, HeartbeatRegistry::test())
            .await
            .map_err(|e| eyre!("Failed to create timer manager: {}", e))
    }

    /// Helper: count scheduled times for a key and timer type
    async fn count_scheduled<T: TriggerStore>(
        manager: &TimerManager<T>,
        key: &Key,
        timer_type: TimerType,
    ) -> Result<usize> {
        Ok(manager.scheduled_times(key, timer_type).await?.len())
    }

    /// Helper: wait for timer and fire it
    async fn wait_and_fire<S, T>(
        stream: &mut S,
        msg: &str,
    ) -> Result<(Trigger, UncommittedTriggerGuard<T>)>
    where
        S: Stream<Item = PendingTimer<T>> + Unpin,
        T: TriggerStore,
    {
        let pending = stream.next().await.ok_or_else(|| eyre!("{msg}"))?;
        let firing = pending
            .fire()
            .await
            .ok_or_else(|| eyre!("{msg} - not active"))?;
        Ok(firing.into_inner())
    }

    #[tokio::test]
    async fn test_new_timer_manager_creation() -> Result<()> {
        time::pause();

        let segment = test_segment();
        let store = memory_store(segment.clone());

        let result = TimerManager::new("test-creation", store, HeartbeatRegistry::test()).await;

        assert!(result.is_ok(), "Timer manager creation should succeed");

        let (_stream, manager) = result?;
        assert_eq!(manager.0.segment.id, segment.id);
        assert_eq!(manager.0.segment.name, segment.name);
        assert_eq!(manager.0.segment.slab_size, segment.slab_size);
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
            .await?;
        let times2 = manager
            .scheduled_times(&trigger2.key, TimerType::Application)
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
            .await?;
        assert_eq!(scheduled_times.len(), 3);

        // Unschedule all
        let result = manager.unschedule_all(&key, TimerType::Application).await;
        assert!(result.is_ok(), "Unschedule all should succeed");

        // Verify all are removed
        let scheduled_times = manager
            .scheduled_times(&key, TimerType::Application)
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

        if let Some(pending_timer) = stream.next().await {
            let firing_timer = pending_timer
                .fire()
                .await
                .ok_or_else(|| eyre!("Timer should be active"))?;
            let (trigger_data, _) = firing_timer.into_inner();
            assert_eq!(trigger_data.key, trigger.key);
            assert_eq!(trigger_data.time, trigger.time);
        }

        Ok(())
    }

    #[tokio::test]
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
                let trigger = create_test_trigger(
                    &format!("concurrent-{i}"),
                    60 + i,
                    TimerType::Application,
                )?;
                manager_clone.schedule(trigger).await?;
                Ok::<_, color_eyre::Report>(())
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
            .await?;

        let times_future = manager
            .scheduled_times(&trigger_future.key, TimerType::Application)
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
        let app = Trigger::new(key.clone(), time, TimerType::Application, Span::current());
        let retry = Trigger::new(
            key.clone(),
            time,
            TimerType::DeferredMessage,
            Span::current(),
        );
        manager.schedule(app).await?;
        manager.schedule(retry).await?;

        // Allow scheduler to process and verify both types are scheduled
        time::advance(Duration::from_millis(100)).await;
        task::yield_now().await;
        assert_eq!(
            count_scheduled(&manager, &key, TimerType::Application).await?,
            1
        );
        assert_eq!(
            count_scheduled(&manager, &key, TimerType::DeferredMessage).await?,
            1
        );

        // Advance time to trigger BOTH timers
        advance(Duration::from_secs(2)).await;
        task::yield_now().await;

        // Fire both timers (order may vary)
        let (t1, g1) = wait_and_fire(&mut stream, "First timer").await?;
        let (t2, g2) = wait_and_fire(&mut stream, "Second timer").await?;

        // Verify we got both types with correct key/time
        let types = [t1.timer_type, t2.timer_type];
        assert!(
            types.contains(&TimerType::Application),
            "Application should fire"
        );
        assert!(
            types.contains(&TimerType::DeferredMessage),
            "DeferredMessage should fire"
        );
        assert_eq!((t1.key.clone(), t1.time), (key.clone(), time));
        assert_eq!((t2.key.clone(), t2.time), (key.clone(), time));

        // Separate guards by type and commit Application only
        let (app_guard, retry_guard) = if t1.timer_type == TimerType::Application {
            (g1, g2)
        } else {
            (g2, g1)
        };
        app_guard.commit().await;

        // Verify isolation: Application is removed from DB
        // Note: DeferredMessage is still in Firing state, so it's excluded from
        // scheduled_times() (Firing state is excluded by design).
        // The important isolation property is that committing Application
        // doesn't affect DeferredMessage's ability to commit separately.
        assert_eq!(
            count_scheduled(&manager, &key, TimerType::Application).await?,
            0,
            "Application should be removed after commit"
        );
        // DeferredMessage in Firing state - excluded from scheduled_times by design
        assert_eq!(
            count_scheduled(&manager, &key, TimerType::DeferredMessage).await?,
            0,
            "DeferredMessage in Firing state is excluded from scheduled_times"
        );

        // Commit DeferredMessage and verify both gone from DB
        retry_guard.commit().await;
        assert_eq!(
            count_scheduled(&manager, &key, TimerType::Application).await?,
            0,
            "Application should remain removed"
        );
        assert_eq!(
            count_scheduled(&manager, &key, TimerType::DeferredMessage).await?,
            0,
            "DeferredMessage should be removed after commit"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_timer_type_unschedule_isolation() -> Result<()> {
        time::pause();

        let (mut stream, manager) = setup_timer_manager().await?;
        let key = Key::from("unschedule-isolation-key");
        let time = CompactDateTime::now()?.add_duration(CompactDuration::new(1))?;

        // Schedule BOTH types at same (key, time)
        let app = Trigger::new(key.clone(), time, TimerType::Application, Span::current());
        let retry = Trigger::new(
            key.clone(),
            time,
            TimerType::DeferredMessage,
            Span::current(),
        );
        manager.schedule(app).await?;
        manager.schedule(retry).await?;

        // Allow scheduler to process and verify both scheduled
        time::advance(Duration::from_millis(100)).await;
        task::yield_now().await;
        assert_eq!(
            count_scheduled(&manager, &key, TimerType::Application).await?,
            1
        );
        assert_eq!(
            count_scheduled(&manager, &key, TimerType::DeferredMessage).await?,
            1
        );

        // Unschedule ONLY Application and verify isolation
        manager
            .unschedule(&key, time, TimerType::Application)
            .await?;
        task::yield_now().await;
        assert_eq!(
            count_scheduled(&manager, &key, TimerType::Application).await?,
            0
        );
        assert_eq!(
            count_scheduled(&manager, &key, TimerType::DeferredMessage).await?,
            1
        );

        // Advance time - only DeferredMessage should fire
        advance(Duration::from_secs(2)).await;
        task::yield_now().await;
        let (fired, guard) = wait_and_fire(&mut stream, "DeferredMessage timer").await?;
        assert_eq!(
            fired.timer_type,
            TimerType::DeferredMessage,
            "Only DeferredMessage fires"
        );
        assert_eq!((fired.key, fired.time), (key.clone(), time));

        // Commit and verify no more timers
        guard.commit().await;
        advance(Duration::from_secs(1)).await;
        task::yield_now().await;
        assert!(
            timeout(Duration::from_millis(100), stream.next())
                .await
                .is_err()
        );

        Ok(())
    }

    // =========================================================================
    // Reschedule Firing Timer Tests
    // =========================================================================

    #[tokio::test]
    async fn test_reschedule_firing_timer() -> Result<()> {
        // T049: Schedule same timer while firing transitions to FiringRescheduled
        time::pause();

        let (mut stream, manager) = setup_timer_manager().await?;
        let trigger = create_test_trigger("reschedule-key", 1, TimerType::Application)?;

        // Schedule and wait for timer to fire
        manager.schedule(trigger.clone()).await?;
        time::advance(Duration::from_secs(2)).await;
        task::yield_now().await;

        // Fire the timer (transition to FIRING state)
        let pending = stream.next().await.ok_or_else(|| eyre!("No timer"))?;
        let firing = pending.fire().await.ok_or_else(|| eyre!("Not active"))?;

        // Reschedule same timer while firing - should succeed (FIRING →
        // FIRING_RESCHEDULED)
        let reschedule_result = manager.schedule(trigger.clone()).await;
        assert!(reschedule_result.is_ok(), "Reschedule should succeed");

        // Verify state is FiringRescheduled via is_scheduled
        let is_scheduled = manager
            .0
            .state
            .trigger_lock()
            .await
            .scheduler
            .active_triggers()
            .is_scheduled(&trigger.key, trigger.time, trigger.timer_type)
            .await;
        assert!(is_scheduled, "Timer should be scheduled after reschedule");

        // Commit and verify timer is still scheduled (FiringRescheduled → Scheduled)
        let (_, guard) = firing.into_inner();
        guard.commit().await;

        let times = manager
            .scheduled_times(&trigger.key, TimerType::Application)
            .await?;
        assert_eq!(
            times.len(),
            1,
            "Timer should still be scheduled after commit"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_reschedule_idempotent() -> Result<()> {
        // T050: Multiple reschedules while firing are no-op (idempotent)
        time::pause();

        let (mut stream, manager) = setup_timer_manager().await?;
        let trigger = create_test_trigger("idempotent-key", 1, TimerType::Application)?;

        // Schedule and fire
        manager.schedule(trigger.clone()).await?;
        time::advance(Duration::from_secs(2)).await;
        task::yield_now().await;

        let pending = stream.next().await.ok_or_else(|| eyre!("No timer"))?;
        let firing = pending.fire().await.ok_or_else(|| eyre!("Not active"))?;

        // Reschedule multiple times - all should succeed as no-ops
        manager.schedule(trigger.clone()).await?;
        manager.schedule(trigger.clone()).await?;
        manager.schedule(trigger.clone()).await?;

        // Commit and verify only fires once more (not 3 times)
        let (_, guard) = firing.into_inner();
        guard.commit().await;

        // Advance time and verify exactly one more fire
        time::advance(Duration::from_secs(2)).await;
        task::yield_now().await;

        let pending2 = timeout(Duration::from_millis(100), stream.next())
            .await
            .map_err(|_| eyre!("Timer should fire again"))?
            .ok_or_else(|| eyre!("No second timer"))?;

        let firing2 = pending2
            .fire()
            .await
            .ok_or_else(|| eyre!("Second fire not active"))?;
        let (_, guard2) = firing2.into_inner();
        guard2.commit().await;

        // No more timers should fire
        time::advance(Duration::from_secs(2)).await;
        task::yield_now().await;
        assert!(
            timeout(Duration::from_millis(100), stream.next())
                .await
                .is_err(),
            "No more timers should fire"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_commit_deletes_when_not_rescheduled() -> Result<()> {
        // T051: Commit from FIRING state deletes DB row
        time::pause();

        let (mut stream, manager) = setup_timer_manager().await?;
        let trigger = create_test_trigger("delete-key", 1, TimerType::Application)?;

        // Schedule and fire
        manager.schedule(trigger.clone()).await?;
        time::advance(Duration::from_secs(2)).await;
        task::yield_now().await;

        let pending = stream.next().await.ok_or_else(|| eyre!("No timer"))?;
        let firing = pending.fire().await.ok_or_else(|| eyre!("Not active"))?;

        // Commit without rescheduling (FIRING → UNSCHEDULED)
        let (_, guard) = firing.into_inner();
        guard.commit().await;

        // Verify timer is completely removed
        let times = manager
            .scheduled_times(&trigger.key, TimerType::Application)
            .await?;
        assert!(times.is_empty(), "Timer should be deleted from DB");

        // Verify no more fires
        time::advance(Duration::from_secs(2)).await;
        task::yield_now().await;
        assert!(
            timeout(Duration::from_millis(100), stream.next())
                .await
                .is_err(),
            "Timer should not fire again"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_commit_keeps_when_rescheduled() -> Result<()> {
        // T052: Commit from FIRING_RESCHEDULED state keeps DB row
        time::pause();

        let (mut stream, manager) = setup_timer_manager().await?;
        let trigger = create_test_trigger("keep-key", 1, TimerType::Application)?;

        // Schedule, fire, and reschedule
        manager.schedule(trigger.clone()).await?;
        time::advance(Duration::from_secs(2)).await;
        task::yield_now().await;

        let pending = stream.next().await.ok_or_else(|| eyre!("No timer"))?;
        let firing = pending.fire().await.ok_or_else(|| eyre!("Not active"))?;
        manager.schedule(trigger.clone()).await?;

        // Commit with reschedule (FIRING_RESCHEDULED → SCHEDULED)
        let (_, guard) = firing.into_inner();
        guard.commit().await;

        // Verify timer is still scheduled
        let times = manager
            .scheduled_times(&trigger.key, TimerType::Application)
            .await?;
        assert_eq!(times.len(), 1, "Timer should remain in DB");
        assert!(times.contains(&trigger.time));

        Ok(())
    }

    #[tokio::test]
    async fn test_abort_rescheduled_stays_scheduled() -> Result<()> {
        // T053: Abort from FIRING_RESCHEDULED transitions to SCHEDULED
        time::pause();

        let (mut stream, manager) = setup_timer_manager().await?;
        let trigger = create_test_trigger("abort-reschedule-key", 1, TimerType::Application)?;

        // Schedule, fire, and reschedule
        manager.schedule(trigger.clone()).await?;
        time::advance(Duration::from_secs(2)).await;
        task::yield_now().await;

        let pending = stream.next().await.ok_or_else(|| eyre!("No timer"))?;
        let firing = pending.fire().await.ok_or_else(|| eyre!("Not active"))?;
        manager.schedule(trigger.clone()).await?;

        // Abort with reschedule (FIRING_RESCHEDULED → SCHEDULED)
        let (_, guard) = firing.into_inner();
        guard.abort().await;

        // Verify timer fires again
        time::advance(Duration::from_secs(2)).await;
        task::yield_now().await;

        let pending2 = timeout(Duration::from_millis(100), stream.next())
            .await
            .map_err(|_| eyre!("Timer should fire again after abort"))?
            .ok_or_else(|| eyre!("No second timer"))?;
        assert!(pending2.fire().await.is_some(), "Second fire should work");

        Ok(())
    }

    #[tokio::test]
    async fn test_reschedule_same_time_fires_again() -> Result<()> {
        // T054: End-to-end integration test: schedule, fire, reschedule, commit, fires
        // again
        time::pause();

        let (mut stream, manager) = setup_timer_manager().await?;
        let trigger = create_test_trigger("e2e-key", 1, TimerType::Application)?;

        // 1. Schedule timer
        manager.schedule(trigger.clone()).await?;

        // 2. Timer fires
        time::advance(Duration::from_secs(2)).await;
        task::yield_now().await;

        let pending1 = stream.next().await.ok_or_else(|| eyre!("First timer"))?;
        let firing1 = pending1.fire().await.ok_or_else(|| eyre!("First fire"))?;

        // 3. Reschedule during handler
        manager.schedule(trigger.clone()).await?;

        // 4. Commit
        let (_, guard1) = firing1.into_inner();
        guard1.commit().await;

        // 5. Timer fires again
        time::advance(Duration::from_secs(2)).await;
        task::yield_now().await;

        let pending2 = timeout(Duration::from_millis(100), stream.next())
            .await
            .map_err(|_| eyre!("Second timer should fire"))?
            .ok_or_else(|| eyre!("No second timer"))?;

        let firing2 = pending2
            .fire()
            .await
            .ok_or_else(|| eyre!("Second fire not active"))?;

        // Verify it's the same timer
        let (trigger2, guard2) = firing2.into_inner();
        assert_eq!(trigger2.key, trigger.key);
        assert_eq!(trigger2.time, trigger.time);
        assert_eq!(trigger2.timer_type, trigger.timer_type);

        // Commit without reschedule - timer should be done
        guard2.commit().await;

        time::advance(Duration::from_secs(2)).await;
        task::yield_now().await;
        assert!(
            timeout(Duration::from_millis(100), stream.next())
                .await
                .is_err(),
            "Timer should not fire a third time"
        );

        Ok(())
    }

    // =========================================================================
    // Cancel Reschedule Tests
    // =========================================================================

    #[tokio::test]
    async fn test_unschedule_firing_noop() -> Result<()> {
        // T058: Verify unschedule when firing (not rescheduled) is a no-op
        time::pause();

        let (mut stream, manager) = setup_timer_manager().await?;
        let trigger = create_test_trigger("unschedule-firing-key", 1, TimerType::Application)?;

        // Schedule and wait for timer to fire
        manager.schedule(trigger.clone()).await?;
        time::advance(Duration::from_secs(2)).await;
        task::yield_now().await;

        // Fire the timer (transition to FIRING state)
        let pending = stream.next().await.ok_or_else(|| eyre!("No timer"))?;
        let firing = pending.fire().await.ok_or_else(|| eyre!("Not active"))?;

        // Unschedule while firing - should be a no-op (FIRING state)
        let unschedule_result = manager
            .unschedule(&trigger.key, trigger.time, trigger.timer_type)
            .await;
        assert!(unschedule_result.is_ok(), "Unschedule should succeed");

        // Verify timer is still in FIRING state (not removed)
        let current_state = manager
            .0
            .state
            .trigger_lock()
            .await
            .scheduler
            .active_triggers()
            .get_state(&trigger.key, trigger.time, trigger.timer_type)
            .await;
        assert_eq!(
            current_state,
            Some(TimerState::Firing),
            "Timer should still be in Firing state"
        );

        // Commit normally - timer should be deleted since not rescheduled
        let (_, guard) = firing.into_inner();
        guard.commit().await;

        // Verify timer is completely removed
        let times = manager
            .scheduled_times(&trigger.key, TimerType::Application)
            .await?;
        assert!(times.is_empty(), "Timer should be deleted after commit");

        // No more fires
        time::advance(Duration::from_secs(2)).await;
        task::yield_now().await;
        assert!(
            timeout(Duration::from_millis(100), stream.next())
                .await
                .is_err(),
            "Timer should not fire again"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_unschedule_cancels_reschedule() -> Result<()> {
        // T059: Verify unschedule when firing+rescheduled cancels the reschedule
        time::pause();

        let (mut stream, manager) = setup_timer_manager().await?;
        let trigger = create_test_trigger("cancel-reschedule-key", 1, TimerType::Application)?;

        // Schedule and wait for timer to fire
        manager.schedule(trigger.clone()).await?;
        time::advance(Duration::from_secs(2)).await;
        task::yield_now().await;

        // Fire the timer (transition to FIRING state)
        let pending = stream.next().await.ok_or_else(|| eyre!("No timer"))?;
        let firing = pending.fire().await.ok_or_else(|| eyre!("Not active"))?;

        // Reschedule while firing (FIRING → FIRING_RESCHEDULED)
        manager.schedule(trigger.clone()).await?;

        // Verify state is FiringRescheduled
        let state_after_reschedule = manager
            .0
            .state
            .trigger_lock()
            .await
            .scheduler
            .active_triggers()
            .get_state(&trigger.key, trigger.time, trigger.timer_type)
            .await;
        assert_eq!(
            state_after_reschedule,
            Some(TimerState::FiringRescheduled),
            "Timer should be in FiringRescheduled state"
        );

        // Unschedule to cancel the reschedule (FIRING_RESCHEDULED → FIRING)
        let unschedule_result = manager
            .unschedule(&trigger.key, trigger.time, trigger.timer_type)
            .await;
        assert!(unschedule_result.is_ok(), "Unschedule should succeed");

        // Verify state is back to Firing
        let state_after_unschedule = manager
            .0
            .state
            .trigger_lock()
            .await
            .scheduler
            .active_triggers()
            .get_state(&trigger.key, trigger.time, trigger.timer_type)
            .await;
        assert_eq!(
            state_after_unschedule,
            Some(TimerState::Firing),
            "Timer should be back in Firing state"
        );

        // Commit - timer should be deleted since reschedule was cancelled
        let (_, guard) = firing.into_inner();
        guard.commit().await;

        // Verify timer is completely removed
        let times = manager
            .scheduled_times(&trigger.key, TimerType::Application)
            .await?;
        assert!(times.is_empty(), "Timer should be deleted after commit");

        // Timer should NOT fire again (reschedule was cancelled)
        time::advance(Duration::from_secs(2)).await;
        task::yield_now().await;
        assert!(
            timeout(Duration::from_millis(100), stream.next())
                .await
                .is_err(),
            "Timer should NOT fire again after reschedule was cancelled"
        );

        Ok(())
    }

    // =========================================================================
    // State-Aware Query Tests: scheduled_times() filtering
    // =========================================================================

    #[tokio::test]
    async fn test_scheduled_times_excludes_firing() -> Result<()> {
        // T061: Verify firing timers are excluded from scheduled_times()
        time::pause();

        let (mut stream, manager) = setup_timer_manager().await?;
        let trigger = create_test_trigger("exclude-firing-key", 1, TimerType::Application)?;

        // Schedule timer
        manager.schedule(trigger.clone()).await?;

        // Verify timer is in scheduled_times before firing
        let times_before = manager
            .scheduled_times(&trigger.key, TimerType::Application)
            .await?;
        assert_eq!(
            times_before.len(),
            1,
            "Timer should be in scheduled_times before firing"
        );
        assert!(times_before.contains(&trigger.time));

        // Advance time and fire the timer
        time::advance(Duration::from_secs(2)).await;
        task::yield_now().await;

        let pending = stream.next().await.ok_or_else(|| eyre!("No timer"))?;
        let firing = pending.fire().await.ok_or_else(|| eyre!("Not active"))?;

        // Verify timer is NOT in scheduled_times while firing
        let times_during = manager
            .scheduled_times(&trigger.key, TimerType::Application)
            .await?;
        assert!(
            times_during.is_empty(),
            "Timer in Firing state should NOT be in scheduled_times"
        );

        // Commit and verify timer is removed
        let (_, guard) = firing.into_inner();
        guard.commit().await;

        let times_after = manager
            .scheduled_times(&trigger.key, TimerType::Application)
            .await?;
        assert!(times_after.is_empty(), "Timer should be gone after commit");

        Ok(())
    }

    #[tokio::test]
    async fn test_scheduled_times_includes_rescheduled() -> Result<()> {
        // Verify FiringRescheduled timers are included in scheduled_times()
        time::pause();

        let (mut stream, manager) = setup_timer_manager().await?;
        let trigger = create_test_trigger("include-rescheduled-key", 1, TimerType::Application)?;

        // Schedule timer
        manager.schedule(trigger.clone()).await?;

        // Advance time and fire the timer
        time::advance(Duration::from_secs(2)).await;
        task::yield_now().await;

        let pending = stream.next().await.ok_or_else(|| eyre!("No timer"))?;
        let firing = pending.fire().await.ok_or_else(|| eyre!("Not active"))?;

        // While firing, timer should NOT be in scheduled_times
        let times_firing = manager
            .scheduled_times(&trigger.key, TimerType::Application)
            .await?;
        assert!(
            times_firing.is_empty(),
            "Timer in Firing state should NOT be in scheduled_times"
        );

        // Reschedule the timer (FIRING → FIRING_RESCHEDULED)
        manager.schedule(trigger.clone()).await?;

        // Now timer SHOULD be in scheduled_times (FiringRescheduled includes it)
        let times_rescheduled = manager
            .scheduled_times(&trigger.key, TimerType::Application)
            .await?;
        assert_eq!(
            times_rescheduled.len(),
            1,
            "Timer in FiringRescheduled state SHOULD be in scheduled_times"
        );
        assert!(times_rescheduled.contains(&trigger.time));

        // Commit and verify timer is still scheduled (transitions to Scheduled)
        let (_, guard) = firing.into_inner();
        guard.commit().await;

        let times_after_commit = manager
            .scheduled_times(&trigger.key, TimerType::Application)
            .await?;
        assert_eq!(
            times_after_commit.len(),
            1,
            "Timer should still be scheduled after commit from FiringRescheduled"
        );
        assert!(times_after_commit.contains(&trigger.time));

        Ok(())
    }

    // =========================================================================
    // Type-Safe Timer Lifecycle Tests
    // =========================================================================

    #[tokio::test]
    async fn test_fire_scheduled_timer() -> Result<()> {
        // Verify fire() returns Some for a scheduled timer
        time::pause();

        let (mut stream, manager) = setup_timer_manager().await?;
        let trigger = create_test_trigger("fire-scheduled-key", 1, TimerType::Application)?;

        // Schedule timer
        manager.schedule(trigger.clone()).await?;

        // Advance time to trigger emission
        time::advance(Duration::from_secs(2)).await;
        task::yield_now().await;

        // Get the pending timer from stream
        let pending = stream
            .next()
            .await
            .ok_or_else(|| eyre!("Expected a pending timer"))?;

        // Verify fire() returns Some for scheduled timer
        let firing = pending
            .fire()
            .await
            .ok_or_else(|| eyre!("fire() should return Some for scheduled timer"))?;

        // Verify the FiringTimer has correct metadata
        assert_eq!(firing.time(), trigger.time);
        assert_eq!(firing.timer_type(), TimerType::Application);
        assert_eq!(firing.key(), &trigger.key);

        // Clean up
        let (_, guard) = firing.into_inner();
        guard.commit().await;

        Ok(())
    }

    #[tokio::test]
    async fn test_fire_cancelled_timer() -> Result<()> {
        // Verify fire() returns None if timer was unscheduled after delivery but before
        // fire()
        time::pause();

        let (mut stream, manager) = setup_timer_manager().await?;
        let trigger = create_test_trigger("fire-cancelled-key", 1, TimerType::Application)?;

        // Schedule timer
        manager.schedule(trigger.clone()).await?;

        // Advance time to trigger emission into queue
        time::advance(Duration::from_secs(2)).await;
        task::yield_now().await;

        // Get the pending timer from stream (timer has been delivered)
        let pending = stream
            .next()
            .await
            .ok_or_else(|| eyre!("Expected a pending timer"))?;

        // Unschedule the timer AFTER delivery but BEFORE calling fire()
        // This is the race window where cancellation should still work
        manager
            .unschedule(&trigger.key, trigger.time, trigger.timer_type)
            .await?;

        // Verify fire() returns None since timer was cancelled
        let result = pending.fire().await;
        assert!(
            result.is_none(),
            "fire() should return None for cancelled timer"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_reschedule_abort_fires_again() -> Result<()> {
        // T069: End-to-end integration test: reschedule then abort, timer fires again
        time::pause();

        let (mut stream, manager) = setup_timer_manager().await?;
        let trigger = create_test_trigger("reschedule-abort-key", 1, TimerType::Application)?;

        // 1. Schedule timer
        manager.schedule(trigger.clone()).await?;

        // 2. Timer fires
        time::advance(Duration::from_secs(2)).await;
        task::yield_now().await;

        let pending1 = stream
            .next()
            .await
            .ok_or_else(|| eyre!("First timer should fire"))?;
        let firing1 = pending1
            .fire()
            .await
            .ok_or_else(|| eyre!("First fire should succeed"))?;

        // 3. Reschedule during handler (FIRING → FIRING_RESCHEDULED)
        manager.schedule(trigger.clone()).await?;

        // 4. Abort (FIRING_RESCHEDULED → SCHEDULED, timer remains in DelayQueue)
        let (_, guard1) = firing1.into_inner();
        guard1.abort().await;

        // 5. Timer should fire again (already in DelayQueue from reschedule)
        time::advance(Duration::from_secs(2)).await;
        task::yield_now().await;

        let pending2 = timeout(Duration::from_millis(100), stream.next())
            .await
            .map_err(|_| eyre!("Second timer should fire after abort"))?
            .ok_or_else(|| eyre!("No second timer"))?;

        let firing2 = pending2
            .fire()
            .await
            .ok_or_else(|| eyre!("Second fire should succeed"))?;

        // 6. Verify it's the same timer
        let (trigger2, guard2) = firing2.into_inner();
        assert_eq!(trigger2.key, trigger.key);
        assert_eq!(trigger2.time, trigger.time);
        assert_eq!(trigger2.timer_type, trigger.timer_type);

        // 7. Commit without reschedule - timer should be done
        guard2.commit().await;

        time::advance(Duration::from_secs(2)).await;
        task::yield_now().await;
        assert!(
            timeout(Duration::from_millis(100), stream.next())
                .await
                .is_err(),
            "Timer should not fire a third time"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_abort_firing_preserves_db() -> Result<()> {
        // Verify abort from Firing state keeps DB row but removes from ActiveTriggers
        time::pause();

        let (mut stream, manager) = setup_timer_manager().await?;
        let trigger = create_test_trigger("abort-firing-key", 1, TimerType::Application)?;

        // Schedule timer
        manager.schedule(trigger.clone()).await?;

        // Advance time and fire the timer
        time::advance(Duration::from_secs(2)).await;
        task::yield_now().await;

        let pending = stream
            .next()
            .await
            .ok_or_else(|| eyre!("Expected a pending timer"))?;
        let firing = pending.fire().await.ok_or_else(|| eyre!("Not active"))?;

        // Verify timer is in Firing state - scheduled_times() excludes Firing timers
        let times_while_firing = manager
            .scheduled_times(&trigger.key, TimerType::Application)
            .await?;
        assert!(
            times_while_firing.is_empty(),
            "Timer in Firing state should be excluded from scheduled_times()"
        );

        // Abort the timer (removes from ActiveTriggers, DB preserved for recovery)
        let (_, guard) = firing.into_inner();
        guard.abort().await;

        // Verify timer is removed from ActiveTriggers but still in DB.
        // After abort, timer is no longer in ActiveTriggers, so scheduled_times()
        // returns it (DB-only timers are included).
        let times_after_abort = manager
            .scheduled_times(&trigger.key, TimerType::Application)
            .await?;
        assert_eq!(
            times_after_abort.len(),
            1,
            "Timer should still be in DB after abort (preserved for recovery)"
        );
        assert!(times_after_abort.contains(&trigger.time));

        Ok(())
    }

    #[tokio::test]
    async fn test_clear_and_schedule_firing_same_time() -> Result<()> {
        // Issue #7: clear_and_schedule with Firing state at same time as new timer.
        // Schedule T at time X → fire → clear_and_schedule at same time X →
        // verify FiringRescheduled → commit → verify timer fires again.
        time::pause();

        let (mut stream, manager) = setup_timer_manager().await?;
        let trigger = create_test_trigger("cas-firing-key", 1, TimerType::Application)?;

        // Step 1: Schedule timer T at time X
        manager.schedule(trigger.clone()).await?;
        time::advance(Duration::from_secs(2)).await;
        task::yield_now().await;

        // Step 2: Timer fires, enters Firing state
        let pending = stream
            .next()
            .await
            .ok_or_else(|| eyre!("Expected pending timer"))?;
        let firing = pending
            .fire()
            .await
            .ok_or_else(|| eyre!("Expected active timer"))?;

        // Step 3: clear_and_schedule with a new timer at the SAME time X.
        // This exercises the Firing → FiringRescheduled path in clear_and_schedule
        // (manager.rs line 507) and the skip in unschedule_replaced_timers (line 731).
        manager.clear_and_schedule(trigger.clone()).await?;

        // Step 4: Verify transition to FiringRescheduled
        let is_scheduled = manager
            .0
            .state
            .trigger_lock()
            .await
            .scheduler
            .active_triggers()
            .is_scheduled(&trigger.key, trigger.time, trigger.timer_type)
            .await;
        assert!(
            is_scheduled,
            "Timer should be scheduled (FiringRescheduled) after clear_and_schedule"
        );

        // Step 5: Commit the first firing. FiringRescheduled → re-queued.
        let (_, guard) = firing.into_inner();
        guard.commit().await;

        // The timer should still be scheduled after commit
        let times = manager
            .scheduled_times(&trigger.key, TimerType::Application)
            .await?;
        assert_eq!(
            times.len(),
            1,
            "Timer should be scheduled for re-firing after commit"
        );
        assert!(times.contains(&trigger.time));

        // Advance time again and verify the timer fires a second time
        time::advance(Duration::from_secs(2)).await;
        task::yield_now().await;

        let pending2 = timeout(Duration::from_secs(5), stream.next())
            .await?
            .ok_or_else(|| eyre!("Expected timer to fire again after FiringRescheduled commit"))?;
        let firing2 = pending2
            .fire()
            .await
            .ok_or_else(|| eyre!("Second firing not active"))?;

        let (refired_trigger, guard2) = firing2.into_inner();
        assert_eq!(refired_trigger.key, trigger.key);
        assert_eq!(refired_trigger.time, trigger.time);
        guard2.commit().await;

        Ok(())
    }
}
