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
use crate::timers::active::{
    Announce, MemoryEffects, QueueEffect, StoreEffect, TimerOp, TimerSnapshot, TimerState,
    Transition, transition,
};
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
        self.schedule_trigger(request.into_trigger()).await
    }

    /// Schedules an already-tagged internal trigger.
    pub(crate) async fn schedule_trigger(
        &self,
        trigger: Trigger,
    ) -> Result<(), TimerManagerError<T::Error>> {
        self.drive(&trigger, TimerOp::Schedule).await
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
        self.drive(&trigger, TimerOp::Unschedule).await
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

        let prior = self
            .0
            .scheduler
            .active_triggers()
            .get_state(&trigger.key, trigger.time, trigger.timer_type)
            .await;
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
        self.emit_clear_telemetry(&trigger, &existing_times);

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
    /// A completion from `FiringRescheduled` keeps the DB row (the timer
    /// fires again) and rotates the oracle tag; from any other state it
    /// deletes the row. Resolved by the state machine in
    /// `timers::active::transition`.
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
        let active = self.0.scheduler.active_triggers();
        let prior = active.get_state(key, time, timer_type).await;
        let t = transition(prior, TimerOp::Complete);

        // A tag-rotating completion writes a tag provably distinct from the
        // current one; baking it into the trigger keeps the store write and
        // the registry adoption agreeing on one value. The read-modify-write
        // is per-key linearised by KeyManager — no TOCTOU window.
        let tag = if matches!(t.store(), StoreEffect::UpdateTag) {
            let current = active.get_tag(key, time, timer_type).await.unwrap_or(0_i32);
            fresh_tag_distinct_from(current)
        } else {
            0_i32
        };
        let trigger = Trigger::with_tag(key.clone(), time, timer_type, tag, Span::current());
        self.apply(&trigger, t).await
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
        let _ = self.drive(&trigger, TimerOp::Abort).await;
    }

    /// Resolves the state-machine transition for `trigger` and applies it.
    async fn drive(
        &self,
        trigger: &Trigger,
        op: TimerOp,
    ) -> Result<(), TimerManagerError<T::Error>> {
        let prior = self
            .0
            .scheduler
            .active_triggers()
            .get_state(&trigger.key, trigger.time, trigger.timer_type)
            .await;
        self.apply(trigger, transition(prior, op)).await
    }

    /// Applies a resolved [`Transition`]: pre-persist in-memory effects, the
    /// durable write, post-persist in-memory effects, then telemetry.
    async fn apply(
        &self,
        trigger: &Trigger,
        t: Transition,
    ) -> Result<(), TimerManagerError<T::Error>> {
        let (pre, post) = t.phases();
        apply_memory(&self.0.scheduler, trigger, pre).await?;

        match t.store() {
            StoreEffect::None => {}
            StoreEffect::Insert => self
                .0
                .store
                .add_trigger(trigger.clone())
                .await
                .map_err(TimerManagerError::Store)?,
            StoreEffect::Delete => self
                .0
                .store
                .remove_trigger(&trigger.key, trigger.time, trigger.timer_type)
                .await
                .map_err(TimerManagerError::Store)?,
            StoreEffect::UpdateTag => self
                .0
                .store
                .update_tag(&trigger.key, trigger.time, trigger.timer_type, trigger.tag)
                .await
                .map_err(TimerManagerError::Store)?,
        }

        apply_memory(&self.0.scheduler, trigger, post).await?;

        match t.announce() {
            Some(Announce::Scheduled) => self.0.telemetry.timer_scheduled(
                trigger.key.clone(),
                trigger.time,
                trigger.timer_type,
                self.0.source.clone(),
            ),
            Some(Announce::Cancelled) => self.0.telemetry.timer_cancelled(
                trigger.key.clone(),
                trigger.time,
                trigger.timer_type,
                self.0.source.clone(),
            ),
            None => {}
        }
        Ok(())
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

/// Applies one side of a transition's in-memory effects: the registry state
/// flip, the tag adoption, then the scheduler queue effect, in that order.
async fn apply_memory<E>(
    scheduler: &TriggerScheduler<E>,
    trigger: &Trigger,
    effects: MemoryEffects,
) -> Result<(), TimerManagerError<E>>
where
    E: ClassifyError + Error + Debug + Send + Sync + 'static,
{
    let active = scheduler.active_triggers();
    if let Some(state) = effects.next_state {
        active
            .set_state(&trigger.key, trigger.time, trigger.timer_type, state)
            .await;
    }
    if effects.adopt_tag {
        active
            .set_tag(&trigger.key, trigger.time, trigger.timer_type, trigger.tag)
            .await;
    }
    match effects.queue {
        QueueEffect::None => {}
        QueueEffect::Enqueue => scheduler.add_to_queue(trigger.clone()).await?,
        QueueEffect::Dequeue => scheduler.remove_from_queue(trigger.clone()).await?,
        QueueEffect::Insert => scheduler.schedule(trigger.clone()).await?,
        QueueEffect::Remove => scheduler.unschedule(trigger.clone()).await?,
        QueueEffect::Deactivate => {
            scheduler
                .deactivate(&trigger.key, trigger.time, trigger.timer_type)
                .await;
        }
    }
    Ok(())
}

/// Unschedules every replaced (old-time) timer during a `clear_and_schedule`
/// operation, resolving each through the state machine's `ClearReplaced` op.
///
/// All effects are in-memory: the caller's atomic store write subsumes the
/// per-row deletes. The scheduler removal is idempotent — the actor finds
/// nothing when the slab isn't loaded.
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
            continue; // Same time as new — resolved by the caller's ClearSchedule op.
        }

        let old = Trigger::new(
            new_trigger.key.clone(),
            old_time,
            new_trigger.timer_type,
            Span::current(),
        );
        let prior = scheduler
            .active_triggers()
            .get_state(&old.key, old.time, old.timer_type)
            .await;
        let (pre, post) = transition(prior, TimerOp::ClearReplaced).phases();
        debug!(
            key = %old.key,
            timer_type = ?old.timer_type,
            old_time = ?old.time,
            prior_state = ?prior,
            "clear_and_schedule: unscheduling replaced timer"
        );
        apply_memory(scheduler, &old, pre).await?;
        apply_memory(scheduler, &old, post).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests;
