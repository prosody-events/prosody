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
use crate::timers::{DELETE_CONCURRENCY, PendingTimer, TimerSemaphores, TimerType, Trigger};
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
    /// * `trigger` - The [`Trigger`] to schedule (key, time, span).
    ///
    /// # Errors
    ///
    /// Returns [`TimerManagerError`] if:
    /// - The time is in the past.
    /// - The storage insert fails.
    /// - The scheduler enqueue fails.
    pub async fn schedule(&self, trigger: Trigger) -> Result<(), TimerManagerError<T::Error>> {
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
    /// - `Aborted` → `Scheduled` (the new schedule revives a previously
    ///   aborted slot; the actor's `Add` call will run post-persistence).
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
    /// - `Firing` / `FiringRescheduled`: pre-persistence already handled
    ///   the in-memory side; nothing left to do here.
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
    /// - From `Firing`: transitions to `Aborted` (DB row preserved; not queued).
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
mod tests {
    use super::*;
    use crate::Topic;
    use crate::consumer::{Keyed, Uncommitted};
    use crate::telemetry::Telemetry;
    use crate::timers::TimerSemaphores;
    use crate::timers::UncommittedTimer;
    use crate::timers::duration::CompactDuration;
    use crate::timers::store::adapter::TableAdapter;
    use crate::timers::store::memory::{InMemoryTriggerStore, memory_store};
    use crate::timers::store::{Segment, SegmentVersion};
    use crate::timers::uncommitted::UncommittedTriggerGuard;
    use color_eyre::eyre::{Result, eyre};
    use futures::{StreamExt, pin_mut};
    use std::array::from_fn;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::{Semaphore, watch};

    const TEST_TIMER_SEMAPHORE_SIZE: usize = 64;

    fn test_semaphores() -> Arc<TimerSemaphores> {
        Arc::new(from_fn(|_| {
            Arc::new(Semaphore::new(TEST_TIMER_SEMAPHORE_SIZE))
        }))
    }

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

    /// Helper function to set up a timer manager for testing.
    ///
    /// Returns `(stream, manager, shutdown_tx)`. The caller holds
    /// `shutdown_tx` and can send `ShutdownPhase::Draining` to stop the
    /// background scheduler actor.
    async fn setup_timer_manager() -> Result<(
        impl Stream<Item = PendingTimer<TableAdapter<InMemoryTriggerStore>>>,
        TimerManager<TableAdapter<InMemoryTriggerStore>>,
        watch::Sender<ShutdownPhase>,
    )> {
        let store = memory_store(test_segment());
        let (shutdown_tx, shutdown_rx) = watch::channel(ShutdownPhase::default());
        let telemetry = Telemetry::new();

        let config = TimerManagerConfig {
            name: "test-manager".to_owned(),
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
        .map_err(|e| eyre!("Failed to create timer manager: {}", e))?;
        Ok((stream, manager, shutdown_tx))
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
        let telemetry = Telemetry::new();

        let (_shutdown_tx, shutdown_rx) = watch::channel(ShutdownPhase::default());
        let config = TimerManagerConfig {
            name: "test-creation".to_owned(),
            store,
            telemetry: telemetry.partition_sender(Topic::from("test"), 0),
            source: Arc::from(""),
        };
        let result = TimerManager::new(
            config,
            HeartbeatRegistry::test(),
            shutdown_rx,
            test_semaphores(),
        )
        .await;

        assert!(result.is_ok(), "Timer manager creation should succeed");

        let (_stream, manager) = result?;
        // Manager construction succeeded; the segment was bootstrapped into
        // the store and is now owned by the scheduler actor.
        let stored = manager
            .0
            .store
            .get_segment()
            .await?
            .ok_or_else(|| eyre!("segment should be persisted after manager init"))?;
        assert_eq!(stored.id, segment.id);
        assert_eq!(stored.name, segment.name);
        assert_eq!(stored.slab_size, segment.slab_size);
        Ok(())
    }

    #[tokio::test]
    async fn test_schedule_timer_basic() -> Result<()> {
        time::pause();

        let (_stream, manager, _shutdown_tx) = setup_timer_manager().await?;
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

        let (_stream, manager, _shutdown_tx) = setup_timer_manager().await?;
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

        let (_stream, manager, _shutdown_tx) = setup_timer_manager().await?;

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

        let (_stream, manager, _shutdown_tx) = setup_timer_manager().await?;
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

        let (_stream, manager, _shutdown_tx) = setup_timer_manager().await?;
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

        let (_stream, manager, _shutdown_tx) = setup_timer_manager().await?;
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

        let (_stream, manager, _shutdown_tx) = setup_timer_manager().await?;
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

        let (_stream, manager, _shutdown_tx) = setup_timer_manager().await?;
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

        let (_stream, manager, _shutdown_tx) = setup_timer_manager().await?;
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

        let (_stream, manager, _shutdown_tx) = setup_timer_manager().await?;
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

        let (_stream, manager, _shutdown_tx) = setup_timer_manager().await?;
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

        let (_stream, manager, _shutdown_tx) = setup_timer_manager().await?;
        let key = Key::from("nonexistent");
        let time = CompactDateTime::now()?;

        // Aborting nonexistent timer should succeed without error
        manager.abort(&key, time, TimerType::Application).await;
        Ok(())
    }

    #[tokio::test]
    async fn test_timer_stream_delivery() -> Result<()> {
        time::pause();

        let (stream, manager, _shutdown_tx) = setup_timer_manager().await?;
        pin_mut!(stream);

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

        let (_stream, manager, _shutdown_tx) = setup_timer_manager().await?;
        let manager = Arc::new(manager);

        // Spawn multiple concurrent operations
        let mut handles = vec![];

        // Schedule timers concurrently
        for i in 0..10 {
            let manager_clone = manager.clone();
            let handle = task::spawn(async move {
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

        let (_stream, manager, _shutdown_tx) = setup_timer_manager().await?;
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

        let (_stream, manager, _shutdown_tx) = setup_timer_manager().await?;
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

        let (_stream, manager, _shutdown_tx) = setup_timer_manager().await?;

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

        let (stream, manager, _shutdown_tx) = setup_timer_manager().await?;
        pin_mut!(stream);
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

        let (stream, manager, _shutdown_tx) = setup_timer_manager().await?;
        pin_mut!(stream);
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

        let (stream, manager, _shutdown_tx) = setup_timer_manager().await?;
        pin_mut!(stream);
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

        let (stream, manager, _shutdown_tx) = setup_timer_manager().await?;
        pin_mut!(stream);
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

        let (stream, manager, _shutdown_tx) = setup_timer_manager().await?;
        pin_mut!(stream);
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

        let (stream, manager, _shutdown_tx) = setup_timer_manager().await?;
        pin_mut!(stream);
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

        let (stream, manager, _shutdown_tx) = setup_timer_manager().await?;
        pin_mut!(stream);
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

        let (stream, manager, _shutdown_tx) = setup_timer_manager().await?;
        pin_mut!(stream);
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

        let (stream, manager, _shutdown_tx) = setup_timer_manager().await?;
        pin_mut!(stream);
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

        let (stream, manager, _shutdown_tx) = setup_timer_manager().await?;
        pin_mut!(stream);
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

        let (stream, manager, _shutdown_tx) = setup_timer_manager().await?;
        pin_mut!(stream);
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

        let (stream, manager, _shutdown_tx) = setup_timer_manager().await?;
        pin_mut!(stream);
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

        let (stream, manager, _shutdown_tx) = setup_timer_manager().await?;
        pin_mut!(stream);
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

        let (stream, manager, _shutdown_tx) = setup_timer_manager().await?;
        pin_mut!(stream);
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

        let (stream, manager, _shutdown_tx) = setup_timer_manager().await?;
        pin_mut!(stream);
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
        // Verify abort from Firing state keeps DB row and protects active slab state.
        time::pause();

        let (stream, manager, _shutdown_tx) = setup_timer_manager().await?;
        pin_mut!(stream);
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

        // Abort the timer (transitions to Aborted, DB preserved for recovery)
        let (_, guard) = firing.into_inner();
        guard.abort().await;

        let state_after_abort = manager
            .0
            .scheduler
            .active_triggers()
            .get_state(&trigger.key, trigger.time, trigger.timer_type)
            .await;
        assert_eq!(
            state_after_abort,
            Some(TimerState::Aborted),
            "Timer should remain active as Aborted after abort"
        );

        // Verify timer is still visible through scheduled_times because its DB
        // row is preserved for recovery/requeue.
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

        let (stream, manager, _shutdown_tx) = setup_timer_manager().await?;
        pin_mut!(stream);
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

    // =========================================================================
    // prop_tag_rotation: timer tag invariants
    // =========================================================================

    /// Inv #6: after `schedule`, `current_tag` returns `Some(_)`.
    #[tokio::test]
    async fn tag_inv6_schedule_gives_tag() -> Result<()> {
        time::pause();
        let (_stream, manager, _shutdown_tx) = setup_timer_manager().await?;
        let trigger = create_test_trigger("k", 10, TimerType::Application)?;
        manager.schedule(trigger.clone()).await?;
        let tag = manager
            .current_tag(&trigger.key, trigger.time, trigger.timer_type)
            .await?;
        assert!(tag.is_some(), "expected Some tag after schedule");
        Ok(())
    }

    /// Inv #7: Scheduled→Firing does NOT rotate the tag.
    #[tokio::test]
    async fn tag_inv7_fire_does_not_rotate() -> Result<()> {
        time::pause();
        let (stream, manager, _shutdown_tx) = setup_timer_manager().await?;
        pin_mut!(stream);
        let trigger = create_test_trigger("k", 1, TimerType::Application)?;
        manager.schedule(trigger.clone()).await?;
        let tag_before = manager
            .current_tag(&trigger.key, trigger.time, trigger.timer_type)
            .await?
            .ok_or_else(|| eyre!("tag missing before fire"))?;

        advance(Duration::from_secs(2)).await;
        task::yield_now().await;
        let pending = stream.next().await.ok_or_else(|| eyre!("no pending"))?;
        let firing = pending.fire().await.ok_or_else(|| eyre!("not active"))?;

        // Tag must be identical at dispatch.
        assert_eq!(
            firing.trigger().tag,
            tag_before,
            "inv #7: Scheduled→Firing must not rotate tag"
        );
        firing.commit().await;
        Ok(())
    }

    /// Inv #2: complete()-from-FiringRescheduled rotates tag; new != old.
    #[tokio::test]
    async fn tag_inv2_firing_rescheduled_commit_rotates() -> Result<()> {
        time::pause();
        let (stream, manager, _shutdown_tx) = setup_timer_manager().await?;
        pin_mut!(stream);
        let trigger = create_test_trigger("k", 1, TimerType::Application)?;
        manager.schedule(trigger.clone()).await?;
        let tag_initial = manager
            .current_tag(&trigger.key, trigger.time, trigger.timer_type)
            .await?
            .ok_or_else(|| eyre!("no tag before fire"))?;

        advance(Duration::from_secs(2)).await;
        task::yield_now().await;
        let pending = stream.next().await.ok_or_else(|| eyre!("no pending"))?;
        let firing = pending.fire().await.ok_or_else(|| eyre!("not active"))?;

        // Re-schedule same trigger (Firing → FiringRescheduled).
        manager.schedule(trigger.clone()).await?;

        // Commit while FiringRescheduled → must rotate tag.
        firing.commit().await;

        let tag_after = manager
            .current_tag(&trigger.key, trigger.time, trigger.timer_type)
            .await?
            .ok_or_else(|| {
                eyre!("tag absent after FiringRescheduled commit (row should remain)")
            })?;
        assert_ne!(
            tag_after, tag_initial,
            "inv #2: complete()-from-FiringRescheduled must rotate tag"
        );
        Ok(())
    }

    /// Inv #3: `complete()`-from-`Firing` removes the row; `current_tag` →
    /// `None`.
    #[tokio::test]
    async fn tag_inv3_firing_commit_removes_row() -> Result<()> {
        time::pause();
        let (stream, manager, _shutdown_tx) = setup_timer_manager().await?;
        pin_mut!(stream);
        let trigger = create_test_trigger("k", 1, TimerType::Application)?;
        manager.schedule(trigger.clone()).await?;

        advance(Duration::from_secs(2)).await;
        task::yield_now().await;
        let pending = stream.next().await.ok_or_else(|| eyre!("no pending"))?;
        let firing = pending.fire().await.ok_or_else(|| eyre!("not active"))?;
        firing.commit().await;

        let tag_after = manager
            .current_tag(&trigger.key, trigger.time, trigger.timer_type)
            .await?;
        assert!(
            tag_after.is_none(),
            "inv #3: complete()-from-Firing must leave no row (current_tag → None)"
        );
        Ok(())
    }

    /// Inv #10: FiringTimer.trigger().tag equals the canonical tag at dispatch,
    /// even if a complete()-from-FiringRescheduled rotation ran while the entry
    /// sat in the delay queue.
    #[tokio::test]
    async fn tag_inv10_firing_timer_tag_frozen_at_dispatch() -> Result<()> {
        time::pause();
        let (stream, manager, _shutdown_tx) = setup_timer_manager().await?;
        pin_mut!(stream);
        let trigger = create_test_trigger("k", 1, TimerType::Application)?;
        manager.schedule(trigger.clone()).await?;
        let dispatch_tag = manager
            .current_tag(&trigger.key, trigger.time, trigger.timer_type)
            .await?
            .ok_or_else(|| eyre!("no tag"))?;

        advance(Duration::from_secs(2)).await;
        task::yield_now().await;
        let pending = stream.next().await.ok_or_else(|| eyre!("no pending"))?;
        let firing = pending.fire().await.ok_or_else(|| eyre!("not active"))?;

        // Tag on FiringTimer must equal the pre-dispatch canonical tag.
        assert_eq!(
            firing.trigger().tag,
            dispatch_tag,
            "inv #10: FiringTimer.trigger().tag must be the canonical tag at dispatch"
        );
        firing.commit().await;
        Ok(())
    }

    /// Inv #8 (reload parity): after `complete()`-from-`FiringRescheduled`,
    /// the tag persisted in the store equals the tag held in
    /// `ActiveTriggers`, and both equal the rotated post-commit value.
    ///
    /// `TimerManager::current_tag` consults `ActiveTriggers` first and only
    /// falls through to the store on miss, so this test bypasses the manager
    /// helper and queries the store directly via the held trigger-lock —
    /// otherwise both reads would hit the in-memory path and a store/memory
    /// divergence would go undetected.
    #[tokio::test]
    async fn tag_inv8_reload_parity() -> Result<()> {
        time::pause();
        let (stream, manager, _shutdown_tx) = setup_timer_manager().await?;
        pin_mut!(stream);
        let trigger = create_test_trigger("k", 1, TimerType::Application)?;
        manager.schedule(trigger.clone()).await?;
        let tag_initial = manager
            .current_tag(&trigger.key, trigger.time, trigger.timer_type)
            .await?
            .ok_or_else(|| eyre!("no tag before fire"))?;

        advance(Duration::from_secs(2)).await;
        task::yield_now().await;
        let pending = stream.next().await.ok_or_else(|| eyre!("no pending"))?;
        let firing = pending.fire().await.ok_or_else(|| eyre!("not active"))?;
        manager.schedule(trigger.clone()).await?; // → FiringRescheduled
        firing.commit().await; // → rotates tag

        // In-memory path: ActiveTriggers has the new tag.
        let tag_active = manager
            .current_tag(&trigger.key, trigger.time, trigger.timer_type)
            .await?
            .ok_or_else(|| eyre!("in-memory tag absent"))?;

        // Store path: query the persistent store directly, skipping the
        // ActiveTriggers cache that `manager.current_tag` consults first.
        let tag_store = manager
            .0
            .store
            .current_tag(&trigger.key, trigger.time, trigger.timer_type)
            .await?
            .ok_or_else(|| eyre!("store tag absent"))?;

        assert_eq!(
            tag_active, tag_store,
            "inv #8: reload parity — in-memory tag must equal store tag after rotation"
        );
        assert_ne!(
            tag_store, tag_initial,
            "inv #8: store tag must reflect the post-commit rotation, not the pre-fire value"
        );
        Ok(())
    }
}
