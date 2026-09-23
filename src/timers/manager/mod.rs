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
use crate::heartbeat::HeartbeatRegistry;
use crate::state::TimerEventRef;
#[cfg(test)]
use crate::telemetry::Telemetry;
use crate::telemetry::partition::TelemetryPartitionSender;
use crate::timers::active::{TimerOp, TimerSnapshot, TimerState, transition};
use crate::timers::datetime::CompactDateTime;

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

mod apply;

use apply::{apply_memory, unschedule_replaced_timers};

/// Configuration for a [`TimerManager`] instance.
///
/// Bundles all stable configuration parameters — storage backend and telemetry
/// context — so they can be passed as a single value to [`TimerManager::new`].
///
/// # Type Parameters
///
/// * `T`: The [`TriggerStore`] backend for persistent timer data.
#[derive(Clone)]
pub struct TimerManagerConfig<T> {
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
    /// `shutdown_rx` signals partition shutdown; the scheduler actor serves
    /// commands through `Draining` and exits at `>= ShutdownPhase::Cancelling`.
    /// `semaphores` bounds in-flight timer
    /// events per type (see module docs). Returns a [`Stream`] of
    /// [`PendingTimer<T>`] alongside the [`TimerManager<T>`] used to
    /// schedule and manage timers.
    ///
    /// # Errors
    ///
    /// Returns [`TimerManagerError`] if the segment metadata cannot be
    /// created or retrieved, or if the scheduler fails to initialize.
    pub async fn new(
        config: TimerManagerConfig<T>,
        heartbeats: HeartbeatRegistry,
        shutdown_rx: watch::Receiver<ShutdownPhase>,
        semaphores: Arc<TimerSemaphores>,
    ) -> Result<(impl Stream<Item = PendingTimer<T>> + use<T>, Self), TimerManagerError<T::Error>>
    {
        // Ensure the segment exists in persistent storage.
        let segment = get_or_create_segment(&config.store).await?;

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

    /// Retrieves all scheduled execution times for a given key and timer
    /// type: every persisted time except those currently `Firing` (being
    /// processed and not scheduled to fire again).
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

        // Stream from storage and filter in a single pass — no intermediate
        // Vec. Firing is the only excluded state; times absent from
        // ActiveTriggers (slab not yet loaded) still count as scheduled.
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
    /// owned, enqueues it in the in-memory scheduler. Prior-state handling
    /// (re-fire after commit for a firing timer, requeue for an aborted one)
    /// is resolved by the state machine in `timers::active::transition`.
    ///
    /// **Singleton vs Overflow routing:**
    /// - First timer for a key/type → written to singleton slot (via store
    ///   layer)
    /// - Second+ timer → promotes to overflow (clustering columns)
    /// - Use [`clear_and_schedule`](Self::clear_and_schedule) for
    ///   tombstone-free singleton overwrites
    ///
    /// # Errors
    ///
    /// Returns [`TimerManagerError`] if the storage insert or the scheduler
    /// enqueue fails.
    pub async fn schedule(&self, request: TimerRequest) -> Result<(), TimerManagerError<T::Error>> {
        self.0
            .drive(&request.into_trigger(), TimerOp::Schedule)
            .await
    }

    /// Seeds a known attempt identity for tests.
    #[cfg(test)]
    pub(crate) async fn schedule_trigger(
        &self,
        trigger: Trigger,
    ) -> Result<(), TimerManagerError<T::Error>> {
        self.0.drive(&trigger, TimerOp::Schedule).await
    }

    /// Cancels a specific scheduled timer.
    ///
    /// Removes the timer from persistent storage and, if owned, from the
    /// in-memory scheduler. If already delivered, the delivery is not
    /// reversed; prior-state handling (no-op mid-fire, cancelling a pending
    /// reschedule) is resolved by the state machine in
    /// `timers::active::transition`. The scheduler removal is idempotent: if
    /// the slab is not owned in-memory, the actor has nothing to remove and
    /// returns success.
    ///
    /// # Errors
    ///
    /// Returns [`TimerManagerError`] if the scheduler or storage removal
    /// fails.
    pub async fn unschedule(
        &self,
        key: &Key,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> Result<(), TimerManagerError<T::Error>> {
        let trigger = Trigger::new(key.clone(), time, timer_type, Span::current());
        self.0.drive(&trigger, TimerOp::Unschedule).await
    }

    /// Cancels all timers for a specific key concurrently.
    ///
    /// Queries all scheduled times for `key` and issues
    /// [`unschedule`](Self::unschedule) for each in parallel, controlled by
    /// [`DELETE_CONCURRENCY`].
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
    /// This is the optimized path for singleton timer overwrites: existing
    /// triggers are read to determine which slabs need cleanup, the
    /// in-memory scheduler is updated (unschedule old, schedule new), and
    /// the store's `clear_and_schedule` persists everything in one atomic
    /// write. Prior-state handling for the new and replaced timers is
    /// resolved by the state machine in `timers::active::transition`.
    ///
    /// # Errors
    ///
    /// Returns [`TimerManagerError`] if storage or scheduler operations
    /// fail.
    pub async fn clear_and_schedule(
        &self,
        request: TimerRequest,
    ) -> Result<(), TimerManagerError<T::Error>> {
        self.clear_and_schedule_trigger(request.into_trigger())
            .await
    }

    /// Clears and schedules an already-tagged internal trigger.
    async fn clear_and_schedule_trigger(
        &self,
        mut trigger: Trigger,
    ) -> Result<(), TimerManagerError<T::Error>> {
        let existing_times: Vec<CompactDateTime> = self
            .0
            .store
            .get_key_times(trigger.timer_type, &trigger.key)
            .map_err(TimerManagerError::Store)
            .try_collect()
            .await?;

        let queued = self
            .0
            .scheduler
            .active_triggers()
            .get(&trigger.key, trigger.time, trigger.timer_type)
            .await;
        let prior = queued.map(|entry| entry.state);
        if let Some(entry) = queued.filter(|entry| {
            matches!(
                entry.state,
                TimerState::Scheduled | TimerState::FiringRescheduled
            )
        }) {
            trigger.tag = entry.tag;
        }
        let (pre, post) = transition(prior, TimerOp::ClearSchedule).phases();

        debug!(
            key = %trigger.key,
            timer_type = ?trigger.timer_type,
            new_time = ?trigger.time,
            existing_count = existing_times.len(),
            prior_state = ?prior,
            "clear_and_schedule: resolved transition, applying"
        );

        // In-memory effects that must precede the atomic write: the new
        // timer's pre-persist half, then the removal of every replaced time.
        apply_memory(&self.0.scheduler, &trigger, pre).await?;
        unschedule_replaced_timers(&self.0.scheduler, &trigger, &existing_times).await?;

        // The single durable write: atomically inserts the new row and
        // clears the replaced ones (`ClearSchedule` transitions carry no
        // store effect of their own).
        self.0
            .store
            .clear_and_schedule(trigger.clone())
            .await
            .map_err(TimerManagerError::Store)?;

        apply_memory(&self.0.scheduler, &trigger, post).await?;
        self.0.emit_clear_telemetry(&trigger, &existing_times);

        Ok(())
    }

    /// Starts the pending trigger only if its queued attempt is still
    /// scheduled.
    pub(crate) async fn fire(&self, trigger: &Trigger) -> bool {
        self.0.scheduler.fire(trigger).await
    }

    /// Marks a timer as completed.
    ///
    /// A completion from `FiringRescheduled` preserves the queued replacement.
    /// Other states delete the row. See `timers::active::transition`.
    ///
    /// Typically invoked by [`crate::timers::uncommitted::FiringTimer`]'s
    /// [`crate::consumer::Uncommitted::commit()`] impl.
    ///
    /// # Errors
    ///
    /// Returns [`TimerManagerError::Store`] if the storage write fails.
    pub async fn complete(
        &self,
        key: &Key,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> Result<(), TimerManagerError<T::Error>> {
        let trigger = Trigger::with_tag(key.clone(), time, timer_type, 0, Span::current());
        self.0.drive(&trigger, TimerOp::Complete).await
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
    /// Never deletes the timer from persistent storage: aborted timers can
    /// be requeued explicitly or recover as `Scheduled` after scheduler
    /// restart. Resolved by the state machine in
    /// `timers::active::transition`; abort transitions carry no store
    /// effect, so the only fallible step is the queue removal, which is
    /// deliberately best-effort — abort has no error path.
    pub async fn abort(&self, key: &Key, time: CompactDateTime, timer_type: TimerType) {
        let trigger = Trigger::new(key.clone(), time, timer_type, Span::current());
        let _ = self.0.drive(&trigger, TimerOp::Abort).await;
    }

    #[cfg(test)]
    pub(crate) fn with_test_scheduler(store: T, scheduler: TriggerScheduler<T::Error>) -> Self {
        Self(Arc::new(TimerManagerInner {
            store,
            scheduler,
            telemetry: Telemetry::new().partition_sender(crate::Topic::from("admission"), 0),
            source: Arc::from("admission"),
        }))
    }

    /// Reads the durable tag for legacy residue and source retirement.
    pub(crate) async fn current_timer_tag(
        &self,
        key: &Key,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> Result<Option<i32>, TimerManagerError<T::Error>> {
        self.0
            .store
            .current_trigger(key, time, timer_type)
            .await
            .map(|trigger| trigger.map(|trigger| trigger.tag))
            .map_err(TimerManagerError::Store)
    }

    /// Retires a committed attempt and preserves the key row's replacement.
    /// Write both deletes or the slab repair before the scheduler acknowledges
    /// the command. The actor's serial loop corrects earlier loads; later
    /// loads read the repaired state.
    pub(crate) async fn retire_committed(
        &self,
        key: &Key,
        timer: TimerEventRef,
    ) -> Result<(), TimerManagerError<T::Error>> {
        let current = self
            .0
            .store
            .current_trigger(key, timer.time, timer.timer_type)
            .await
            .map_err(TimerManagerError::Store)?;
        if let Some(replacement) = current.filter(|trigger| trigger.tag != timer.tag) {
            self.0
                .store
                .insert_slab_trigger(replacement.clone())
                .await
                .map_err(TimerManagerError::Store)?;
            self.0.scheduler.schedule(replacement).await?;
        } else {
            self.0
                .store
                .remove_trigger(key, timer.time, timer.timer_type)
                .await
                .map_err(TimerManagerError::Store)?;
            let trigger = Trigger::with_tag(
                key.clone(),
                timer.time,
                timer.timer_type,
                timer.tag,
                Span::current(),
            );
            self.0.scheduler.retire_committed(trigger).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
