//! Timer management and coordination for scheduled events.
//!
//! The [`TimerManager`] serves as the primary interface for scheduling,
//! querying, and canceling timers within a specific segment. It coordinates
//! between:
//! - **Persistent Storage**: Durable [`TriggerStore`] for timer metadata.
//! - **Background Slab Loader**: Preloads upcoming timer slabs.
//! - **In-Memory Scheduler**: Precise, delay-queue based timer dispatch.
//! - **Application**: Delivers timers as an async stream of [`PendingTimer`].
//! - **Per-Type Backpressure**: A [`TimerSemaphores`] array provides one
//!   [`tokio::sync::Semaphore`] per [`TimerType`] variant, bounding in-flight
//!   timer events independently so retry timers cannot starve application
//!   timers; the stream blocks when all permits for a type are held and
//!   terminates if the semaphore is closed.
//!
//! The manager ensures timers survive restarts, supports distributed ownership,
//! and provides at-least-once delivery semantics for timer events.

use crate::Key;
use crate::consumer::partition::ShutdownPhase;
use crate::error::ClassifyError;
use crate::heartbeat::HeartbeatRegistry;
use crate::telemetry::partition::TelemetryPartitionSender;
use crate::timers::active::{TimerSnapshot, TimerState};
use crate::timers::datetime::CompactDateTime;
use rand::RngExt;
use std::error::Error;
use std::fmt::Debug;

pub use crate::timers::error::TimerManagerError;
use crate::timers::scheduler::TriggerScheduler;
use crate::timers::segment::get_or_create_segment;
use crate::timers::store::TriggerStore;
use crate::timers::{
    DELETE_CONCURRENCY, PendingTimer, TimerRequest, TimerSemaphores, TimerType, Trigger,
};
use async_stream::stream;
use educe::Educe;
use futures::{Stream, StreamExt, TryStreamExt, stream};
use std::sync::Arc;
use tokio::sync::watch;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{Instrument, Span, debug};

/// Configuration for a [`TimerManager`] instance.
///
/// Bundles all stable configuration parameters — segment identity, storage
/// backend, and telemetry context — so they can be passed as a single value to
/// [`TimerManager::new`].
///
/// # Type Parameters
///
/// * `T`: The [`TriggerStore`] backend for persistent timer data.
#[derive(Clone)]
pub struct TimerManagerConfig<T> {
    /// Human-readable name for the segment.
    pub name: String,
    /// Persistent storage backend for timer triggers.
    pub store: T,
    /// Partition-scoped telemetry sender for timer lifecycle events.
    pub telemetry: TelemetryPartitionSender,
    /// Consumer group ID used as the `source` field in telemetry events.
    pub source: Arc<str>,
}

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
pub struct TimerManager<T: TriggerStore>(#[educe(Debug(ignore))] Arc<TimerManagerInner<T>>);

/// Internal shared state for the [`TimerManager`].
///
/// The bound `T: TriggerStore` is required because `scheduler` is typed on
/// the store's error variant.
pub struct TimerManagerInner<T: TriggerStore> {
    /// Persistent trigger store. Cloned across the manager and the scheduler
    /// actor so trigger-row writes can race in parallel with slab metadata
    /// writes inside the actor.
    store: T,
    /// In-memory scheduler actor handle. Owns slab metadata writes, slab
    /// loading, slab cleanup, and the trigger queue.
    scheduler: TriggerScheduler<T::Error>,
    /// Partition-scoped telemetry sender for timer lifecycle events.
    telemetry: TelemetryPartitionSender,
    /// Consumer group ID used as the `source` field in telemetry events.
    source: Arc<str>,
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
    /// 3. A background scheduler actor for preloading upcoming timers.
    ///
    /// # Arguments
    ///
    /// * `config` - Stable configuration: segment identity, store, and
    ///   telemetry context.
    /// * `heartbeats` - Registry for monitoring timer scheduler liveness.
    /// * `shutdown_rx` - Watch channel signaling partition shutdown; the
    ///   scheduler actor exits at `>= ShutdownPhase::Draining`.
    /// * `semaphores` - Per-type semaphores bounding in-flight timer events
    ///   across all partitions; the timer stream blocks when all permits for
    ///   the trigger's type are held and terminates if the semaphore is closed.
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
    pub async fn new(
        config: TimerManagerConfig<T>,
        heartbeats: HeartbeatRegistry,
        shutdown_rx: watch::Receiver<ShutdownPhase>,
        semaphores: Arc<TimerSemaphores>,
    ) -> Result<(impl Stream<Item = PendingTimer<T>>, Self), TimerManagerError<T::Error>> {
        // Ensure the segment exists in persistent storage.
        let segment = get_or_create_segment(&config.store, &config.name).await?;

        // Initialize the unified scheduler actor — it owns slab metadata,
        // loading, cleanup, and the trigger queue. The manager keeps its
        // own `store` clone for trigger-row writes that race in parallel.
        let (trigger_rx, scheduler) = TriggerScheduler::new(
            config.store.clone(),
            segment.clone(),
            &heartbeats,
            shutdown_rx,
        );

        // Build the manager wrapper. The segment is consumed when the
        // scheduler actor spawns; no copy is retained here.
        let manager = Self(Arc::new(TimerManagerInner {
            store: config.store,
            scheduler,
            telemetry: config.telemetry,
            source: config.source,
        }));
        let cloned_manager = manager.clone();

        // Wrap the scheduler receiver into a PendingTimer stream, acquiring a
        // per-type semaphore permit per timer to bound in-flight timer events.
        // If the semaphore is closed the stream terminates rather than
        // silently dropping timers.
        let timer_stream = stream! {
            let mut receiver = ReceiverStream::new(trigger_rx);
            while let Some(trigger) = receiver.next().await {
                let semaphore = semaphores[trigger.timer_type as usize].clone();
                let Ok(permit) = semaphore.acquire_owned().await else {
                    break;
                };
                yield PendingTimer::new(trigger, cloned_manager.clone(), permit);
            }
        };

        Ok((timer_stream, manager))
    }

    /// Retrieves all scheduled execution times for a given key.
    ///
    /// **State-aware filtering:**
    /// - Excludes timers in `Firing` state (being processed, not scheduled to
    ///   fire again).
    /// - Includes timers in `Scheduled` state (waiting to fire).
    /// - Includes timers in `FiringRescheduled` state (will fire again after
    ///   current handler completes).
    /// - Includes timers in `Aborted` state (persisted for recovery, but not
    ///   currently queued in this process).
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
        let active_triggers = self.0.scheduler.active_triggers();

        // Stream from storage and filter in a single pass — no intermediate Vec.
        //
        // Include if:
        // - Not in ActiveTriggers (timer not loaded yet, will fire when slab loads)
        // - In Scheduled state (waiting to fire)
        // - In FiringRescheduled state (will fire again after commit)
        // - In Aborted state (persisted for recovery/requeue)
        // Exclude if:
        // - In Firing state (currently being processed, won't fire again unless
        //   rescheduled)
        let stream = self
            .0
            .store
            .get_key_times(timer_type, key)
            .map_err(TimerManagerError::Store);

        stream
            .try_filter(|&time| {
                let state = active_triggers.get_state(key, time, timer_type);
                async move {
                    match state.await {
                        Some(TimerState::Firing) => false,
                        Some(
                            TimerState::Scheduled
                            | TimerState::FiringRescheduled
                            | TimerState::Aborted,
                        )
                        | None => true,
                    }
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
    /// - If the timer is in `Aborted` state, transitions to `Scheduled` and
    ///   re-adds only to the in-memory queue, preserving the stored tag.
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
    /// * `request` - The timer identity and span to schedule.
    ///
    /// # Errors
    ///
    /// Returns [`TimerManagerError`] if:
    /// - The time is in the past.
    /// - The storage insert fails.
    /// - The scheduler enqueue fails.
    pub async fn schedule(&self, request: TimerRequest) -> Result<(), TimerManagerError<T::Error>> {
        self.schedule_trigger(request.into_trigger()).await
    }

    /// Schedules an already-tagged internal trigger.
    pub(crate) async fn schedule_trigger(
        &self,
        trigger: Trigger,
    ) -> Result<(), TimerManagerError<T::Error>> {
        // Check current state for state-aware transitions.
        let current_state = self
            .0
            .scheduler
            .active_triggers()
            .get_state(&trigger.key, trigger.time, trigger.timer_type)
            .await;

        match current_state {
            // FIRING → FIRING_RESCHEDULED: transition state and add to queue
            Some(TimerState::Firing) => {
                self.0
                    .scheduler
                    .active_triggers()
                    .set_state(
                        &trigger.key,
                        trigger.time,
                        trigger.timer_type,
                        TimerState::FiringRescheduled,
                    )
                    .await;
                self.0.scheduler.add_to_queue(trigger).await?;
                Ok(())
            }

            // Already FIRING_RESCHEDULED: idempotent no-op
            Some(TimerState::FiringRescheduled) => Ok(()),

            // ABORTED → SCHEDULED: DB row and active tag are already present;
            // only requeue the timer.
            Some(TimerState::Aborted) => {
                self.0
                    .scheduler
                    .active_triggers()
                    .set_state(
                        &trigger.key,
                        trigger.time,
                        trigger.timer_type,
                        TimerState::Scheduled,
                    )
                    .await;
                self.0.scheduler.add_to_queue(trigger.clone()).await?;
                self.0.telemetry.timer_scheduled(
                    trigger.key.clone(),
                    trigger.time,
                    trigger.timer_type,
                    self.0.source.clone(),
                );
                Ok(())
            }

            // SCHEDULED or UNSCHEDULED: normal scheduling path.
            //
            // The trigger row writes happen before the scheduler is told
            // about the slab. If a concurrent `load_step` scans the slab
            // between the two awaits, the trigger row is already in storage
            // and the scan picks it up; the scheduler's `Add` then sees the
            // slab is owned and just inserts the in-memory entry.
            _ => {
                self.0
                    .store
                    .add_trigger(trigger.clone())
                    .await
                    .map_err(TimerManagerError::Store)?;

                self.0.scheduler.schedule(trigger.clone()).await?;

                self.0.telemetry.timer_scheduled(
                    trigger.key.clone(),
                    trigger.time,
                    trigger.timer_type,
                    self.0.source.clone(),
                );

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
    /// - If the timer is in `Aborted` state, removes from `ActiveTriggers` and
    ///   persistent storage through the normal unschedule path.
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
        // Check current state for state-aware transitions.
        let current_state = self
            .0
            .scheduler
            .active_triggers()
            .get_state(key, time, timer_type)
            .await;

        match current_state {
            // FIRING: no-op - timer is being processed, unschedule has no effect
            Some(TimerState::Firing) => Ok(()),

            // FIRING_RESCHEDULED → FIRING: cancel the reschedule
            Some(TimerState::FiringRescheduled) => {
                self.0
                    .scheduler
                    .active_triggers()
                    .set_state(key, time, timer_type, TimerState::Firing)
                    .await;

                let trigger = Trigger::new(key.clone(), time, timer_type, Span::current());
                self.0.scheduler.remove_from_queue(trigger).await?;
                Ok(())
            }

            // SCHEDULED, ABORTED, or UNSCHEDULED: normal unscheduling path.
            //
            // The scheduler's `unschedule` is idempotent: if the slab is not
            // owned in-memory, the actor has nothing to remove and returns
            // success.
            _ => {
                let trigger = Trigger::new(key.clone(), time, timer_type, Span::current());
                self.0.scheduler.unschedule(trigger).await?;

                self.0
                    .store
                    .remove_trigger(key, time, timer_type)
                    .await
                    .map_err(TimerManagerError::Store)?;

                self.0.telemetry.timer_cancelled(
                    key.clone(),
                    time,
                    timer_type,
                    self.0.source.clone(),
                );

                Ok(())
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
    /// - `Scheduled`/`Aborted` → unscheduled from active state/`DelayQueue`
    ///
    /// For the new timer:
    /// - If same time as existing `Firing` → transitions to `FiringRescheduled`
    /// - Otherwise → schedules in `DelayQueue`
    ///
    /// # Arguments
    ///
    /// * `request` - The new timer identity and span.
    ///
    /// # Errors
    ///
    /// Returns [`TimerManagerError`] if:
    /// - Storage operations fail
    /// - Scheduler operations fail
    pub async fn clear_and_schedule(
        &self,
        request: TimerRequest,
    ) -> Result<(), TimerManagerError<T::Error>> {
        let tag = self
            .current_tag(&request.key, request.time, request.timer_type)
            .await?;
        let trigger = match tag {
            Some(tag) => request.into_trigger_with_tag(tag),
            None => request.into_trigger(),
        };
        self.clear_and_schedule_trigger(trigger).await
    }

    /// Clears and schedules an already-tagged internal trigger.
    pub(crate) async fn clear_and_schedule_trigger(
        &self,
        trigger: Trigger,
    ) -> Result<(), TimerManagerError<T::Error>> {
        let existing_times: Vec<CompactDateTime> = self
            .0
            .store
            .get_key_times(trigger.timer_type, &trigger.key)
            .map_err(TimerManagerError::Store)
            .try_collect()
            .await?;

        debug!(
            key = %trigger.key,
            timer_type = ?trigger.timer_type,
            new_time = ?trigger.time,
            existing_count = existing_times.len(),
            "clear_and_schedule: read existing times, preparing state transitions"
        );

        let prior_state = self
            .0
            .scheduler
            .active_triggers()
            .get_state(&trigger.key, trigger.time, trigger.timer_type)
            .await;

        // In-memory transitions that don't depend on the new store row being
        // visible: the new timer's `ActiveTriggers` state and the
        // unscheduling of old timers from the queue.
        self.apply_clear_pre_persist(&trigger, prior_state).await?;
        unschedule_replaced_timers(&self.0.scheduler, &trigger, &existing_times).await?;

        debug!(
            key = %trigger.key,
            timer_type = ?trigger.timer_type,
            new_time = ?trigger.time,
            "clear_and_schedule: persisting to store"
        );
        self.0
            .store
            .clear_and_schedule(trigger.clone())
            .await
            .map_err(TimerManagerError::Store)?;

        // Post-persistence transitions that require the new store row to
        // be visible — tag rotation for Aborted→Scheduled and the scheduler
        // `Add` for states that deferred it.
        self.apply_clear_post_persist(&trigger, prior_state).await?;
        self.emit_clear_telemetry(&trigger, &existing_times);

        Ok(())
    }

    /// In-memory state transitions to run before the atomic store write.
    ///
    /// Each prior state has its own forward move:
    /// - `Firing` → `FiringRescheduled` and re-queue (the store row already
    ///   exists; we only need the queue update).
    /// - `Aborted` → `Scheduled` (the new schedule revives a previously aborted
    ///   slot; the actor's `Add` call will run post-persistence).
    /// - `FiringRescheduled` is idempotent.
    /// - `Scheduled` and absent both defer to the post-persistence `Add`.
    async fn apply_clear_pre_persist(
        &self,
        trigger: &Trigger,
        prior_state: Option<TimerState>,
    ) -> Result<(), TimerManagerError<T::Error>> {
        let active = self.0.scheduler.active_triggers();
        match prior_state {
            Some(TimerState::Firing) => {
                debug!(
                    key = %trigger.key,
                    timer_type = ?trigger.timer_type,
                    time = ?trigger.time,
                    "clear_and_schedule: new timer is Firing, transitioning to FiringRescheduled"
                );
                active
                    .set_state(
                        &trigger.key,
                        trigger.time,
                        trigger.timer_type,
                        TimerState::FiringRescheduled,
                    )
                    .await;
                self.0.scheduler.add_to_queue(trigger.clone()).await?;
            }
            Some(TimerState::FiringRescheduled) => {
                debug!(
                    key = %trigger.key,
                    timer_type = ?trigger.timer_type,
                    time = ?trigger.time,
                    "clear_and_schedule: new timer already FiringRescheduled, no-op"
                );
            }
            Some(TimerState::Aborted) => {
                debug!(
                    key = %trigger.key,
                    timer_type = ?trigger.timer_type,
                    time = ?trigger.time,
                    "clear_and_schedule: new timer is Aborted, transitioning to Scheduled"
                );
                active
                    .set_state(
                        &trigger.key,
                        trigger.time,
                        trigger.timer_type,
                        TimerState::Scheduled,
                    )
                    .await;
            }
            Some(TimerState::Scheduled) | None => {}
        }
        Ok(())
    }

    /// In-memory state transitions to run after the store row is written.
    ///
    /// - `Aborted`: revive the cached tag (so the oracle's reload check is
    ///   coherent with the new trigger) and run the deferred `Add`.
    /// - `Scheduled` / absent: just the deferred `Add`.
    /// - `Firing` / `FiringRescheduled`: pre-persistence already handled the
    ///   in-memory side; nothing left to do here.
    async fn apply_clear_post_persist(
        &self,
        trigger: &Trigger,
        prior_state: Option<TimerState>,
    ) -> Result<(), TimerManagerError<T::Error>> {
        match prior_state {
            Some(TimerState::Aborted) => {
                self.0
                    .scheduler
                    .active_triggers()
                    .set_tag(&trigger.key, trigger.time, trigger.timer_type, trigger.tag)
                    .await;
                self.schedule_after_clear(trigger, prior_state).await
            }
            Some(TimerState::Scheduled) | None => {
                self.schedule_after_clear(trigger, prior_state).await
            }
            Some(TimerState::Firing | TimerState::FiringRescheduled) => Ok(()),
        }
    }

    /// Runs the scheduler `Add` deferred from `apply_clear_pre_persist` and
    /// logs the dispatch reason for diagnostic traces.
    async fn schedule_after_clear(
        &self,
        trigger: &Trigger,
        prior_state: Option<TimerState>,
    ) -> Result<(), TimerManagerError<T::Error>> {
        debug!(
            key = %trigger.key,
            timer_type = ?trigger.timer_type,
            time = ?trigger.time,
            state = ?prior_state,
            "clear_and_schedule: scheduling new timer via actor"
        );
        self.0.scheduler.schedule(trigger.clone()).await?;
        Ok(())
    }

    /// Emits one `timer_cancelled` event per replaced time (excluding the
    /// new time) and one `timer_scheduled` event for the new trigger.
    fn emit_clear_telemetry(&self, trigger: &Trigger, existing_times: &[CompactDateTime]) {
        for &old_time in existing_times {
            if old_time != trigger.time {
                self.0.telemetry.timer_cancelled(
                    trigger.key.clone(),
                    old_time,
                    trigger.timer_type,
                    self.0.source.clone(),
                );
            }
        }
        self.0.telemetry.timer_scheduled(
            trigger.key.clone(),
            trigger.time,
            trigger.timer_type,
            self.0.source.clone(),
        );
    }

    /// Transitions a timer from `Scheduled` to `Firing` state, returning the
    /// canonical tag at the moment of transition.
    ///
    /// Returns `None` if the transition failed (timer absent or not Scheduled).
    /// Reading the tag under the same trigger-lock as the state transition
    /// guarantees the tag is coherent with the Scheduled→Firing transition.
    pub(crate) async fn fire_with_tag(
        &self,
        key: &Key,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> Option<i32> {
        if !self.0.scheduler.fire(key, time, timer_type).await {
            return None;
        }
        // KeyManager linearises events for this key, so the tag read here is
        // coherent with the just-completed Scheduled → Firing transition.
        self.0
            .scheduler
            .active_triggers()
            .get_tag(key, time, timer_type)
            .await
    }

    /// Marks a timer as completed.
    ///
    /// **State-aware behavior:**
    /// - From `Firing`: deletes from DB and removes from `ActiveTriggers`.
    /// - From `FiringRescheduled`: transitions to `Scheduled` (keeps DB row,
    ///   timer will fire again).
    /// - From `Aborted`: deletes from DB and removes from `ActiveTriggers`,
    ///   matching existing non-firing idempotent completion behavior.
    ///
    /// Typically invoked by [`crate::timers::uncommitted::FiringTimer`]'s
    /// [`crate::consumer::Uncommitted::commit()`] impl.
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
        let active = self.0.scheduler.active_triggers();
        let current_state = active.get_state(key, time, timer_type).await;

        // FIRING_RESCHEDULED → SCHEDULED: keep DB row, rotate tag, timer fires
        // again. The read-modify-write here is per-key linearised by
        // KeyManager — no TOCTOU window.
        if current_state == Some(TimerState::FiringRescheduled) {
            let current_tag = active.get_tag(key, time, timer_type).await.unwrap_or(0_i32);
            let new_tag = fresh_tag_distinct_from(current_tag);

            self.0
                .store
                .update_tag(key, time, timer_type, new_tag)
                .await
                .map_err(TimerManagerError::Store)?;

            active
                .set_state(key, time, timer_type, TimerState::Scheduled)
                .await;
            active.set_tag(key, time, timer_type, new_tag).await;
            return Ok(());
        }

        // FIRING or anything else: delete from DB and remove from ActiveTriggers.
        self.0.scheduler.deactivate(key, time, timer_type).await;

        self.0
            .store
            .remove_trigger(key, time, timer_type)
            .await
            .map_err(TimerManagerError::Store)?;

        Ok(())
    }

    /// Returns a point-in-time [`TimerSnapshot`] of the in-memory scheduler.
    ///
    /// Returns `None` if the system clock is outside the 1970–2106 range
    /// (extreme edge case); callers should skip that tick.
    pub async fn snapshot(&self) -> Option<TimerSnapshot> {
        let now = CompactDateTime::now().ok()?;
        Some(self.0.scheduler.active_triggers().snapshot(now).await)
    }

    /// Aborts a timer delivery.
    ///
    /// **State-aware behavior:**
    /// - From `Firing`: transitions to `Aborted` (DB row preserved; not
    ///   queued).
    /// - From `FiringRescheduled`: transitions to `Scheduled` (timer already in
    ///   `DelayQueue`, will fire again without restart).
    /// - From `Scheduled`: transitions to `Aborted` and removes from
    ///   `DelayQueue` (DB row preserved).
    ///
    /// Does not delete the timer from persistent storage; aborted timers can be
    /// requeued explicitly or recovered as `Scheduled` after scheduler restart.
    ///
    /// # Arguments
    ///
    /// * `key` - The entity key of the timer.
    /// * `time` - The scheduled execution time to abort.
    /// * `timer_type` - The timer type classification.
    pub async fn abort(&self, key: &Key, time: CompactDateTime, timer_type: TimerType) {
        let active = self.0.scheduler.active_triggers();
        let current_state = active.get_state(key, time, timer_type).await;

        match current_state {
            // FIRING_RESCHEDULED → SCHEDULED: timer already in DelayQueue, fires again
            Some(TimerState::FiringRescheduled) => {
                active
                    .set_state(key, time, timer_type, TimerState::Scheduled)
                    .await;
            }

            // SCHEDULED → ABORTED: DB row preserved, queue entry removed.
            Some(TimerState::Scheduled) => {
                active
                    .set_state(key, time, timer_type, TimerState::Aborted)
                    .await;
                let trigger = Trigger::new(key.clone(), time, timer_type, Span::current());
                let _ = self.0.scheduler.remove_from_queue(trigger).await;
            }

            // FIRING → ABORTED: delivery was already removed from DelayQueue.
            Some(TimerState::Firing) => {
                active
                    .set_state(key, time, timer_type, TimerState::Aborted)
                    .await;
            }

            // Already aborted, or absent: idempotent no-op. DB is preserved.
            Some(TimerState::Aborted) | None => {}
        }
    }

    /// Returns the current `tag` for a timer, consulting `ActiveTriggers` first
    /// and falling back to the store.
    ///
    /// Returns `None` if the timer is absent from both in-memory state and the
    /// store (oracle interpretation: "committed"). Returns `Some(0)` for legacy
    /// rows without a stored tag.
    ///
    /// # Errors
    ///
    /// Returns [`TimerManagerError::Store`] if the store read fails.
    pub async fn current_tag(
        &self,
        key: &Key,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> Result<Option<i32>, TimerManagerError<T::Error>> {
        if let Some(tag) = self
            .0
            .scheduler
            .active_triggers()
            .get_tag(key, time, timer_type)
            .await
        {
            return Ok(Some(tag));
        }
        self.0
            .store
            .current_tag(key, time, timer_type)
            .await
            .map_err(TimerManagerError::Store)
    }
}

/// Generates a fresh tag guaranteed != `current`.
///
/// Required by `complete()`-from-`FiringRescheduled`: a same-value re-roll
/// would conflate "not yet committed" with "rotation happened but landed on
/// the same value," which the oracle must not confuse.
pub(crate) fn fresh_tag_distinct_from(current: i32) -> i32 {
    loop {
        let t = rand::rng().random::<i32>();
        if t != current {
            return t;
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
/// - `Scheduled`/`Aborted`/absent → removed from active state/`DelayQueue`
async fn unschedule_replaced_timers<E>(
    scheduler: &TriggerScheduler<E>,
    new_trigger: &Trigger,
    existing_times: &[CompactDateTime],
) -> Result<(), TimerManagerError<E>>
where
    E: ClassifyError + Error + Debug + Send + Sync + 'static,
{
    for &old_time in existing_times {
        if old_time == new_trigger.time {
            continue; // Same time as new - already handled by caller
        }

        let active = scheduler.active_triggers();
        let old_state = active
            .get_state(&new_trigger.key, old_time, new_trigger.timer_type)
            .await;

        match old_state {
            Some(TimerState::Firing) => {
                debug!(
                    key = %new_trigger.key,
                    timer_type = ?new_trigger.timer_type,
                    old_time = ?old_time,
                    "clear_and_schedule: old timer is Firing, skipping (no-op)"
                );
            }
            Some(TimerState::FiringRescheduled) => {
                debug!(
                    key = %new_trigger.key,
                    timer_type = ?new_trigger.timer_type,
                    old_time = ?old_time,
                    "clear_and_schedule: old timer FiringRescheduled, cancelling reschedule"
                );
                active
                    .set_state(
                        &new_trigger.key,
                        old_time,
                        new_trigger.timer_type,
                        TimerState::Firing,
                    )
                    .await;
                let trigger = Trigger::new(
                    new_trigger.key.clone(),
                    old_time,
                    new_trigger.timer_type,
                    Span::current(),
                );
                scheduler.remove_from_queue(trigger).await?;
            }
            _ => {
                // Always issue the unschedule — actor finds nothing if slab
                // isn't loaded, equivalent to the old `is_owned` no-op gate.
                debug!(
                    key = %new_trigger.key,
                    timer_type = ?new_trigger.timer_type,
                    old_time = ?old_time,
                    state = ?old_state,
                    "clear_and_schedule: unscheduling old timer from DelayQueue"
                );
                let trigger = Trigger::new(
                    new_trigger.key.clone(),
                    old_time,
                    new_trigger.timer_type,
                    Span::current(),
                );
                scheduler.unschedule(trigger).await?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
