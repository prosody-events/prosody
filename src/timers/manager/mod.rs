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
use crate::timers::queue::RemoveOutcome;
use rand::RngExt;
use smallvec::SmallVec;
use std::borrow::Cow;
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
    #[cfg(test)]
    pub(crate) fn test_store(&self) -> &T {
        &self.0.store
    }

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
    /// events per type. Returns the manager and a future for its timer stream.
    /// The future yields the stream after the first load, or `None` if the
    /// actor stops first.
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
    ) -> Result<
        (
            impl Future<Output = Option<impl Stream<Item = PendingTimer<T>>>>,
            Self,
        ),
        TimerManagerError<T::Error>,
    > {
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
        let timer_stream = async move {
            let Ok(receiver) = trigger_rx.await else {
                return None;
            };
            Some(stream! {
            let mut receiver = ReceiverStream::new(receiver);
            while let Some(trigger) = receiver.next().await {
                let semaphore = semaphores[trigger.timer_type as usize].clone();
                let Ok(permit) = semaphore.acquire_owned().await else {
                    break;
                };
                yield PendingTimer::new(trigger, cloned_manager.clone(), permit);
            }
            })
        };

        Ok((timer_stream, manager))
    }

    /// Returns persisted times for a key and timer type, except those in
    /// `Firing` or `FiringReplaced` state.
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

        // Filter the store stream in one pass without an intermediate buffer.
        // Times absent from the registry (slab not yet loaded) still count as
        // scheduled.
        let stream = self
            .0
            .store
            .get_key_times(timer_type, key)
            .map_ok(|(time, _)| time)
            .map_err(TimerManagerError::Store);

        stream
            .try_filter(|&time| {
                let state = active_triggers.get_state(key, time, timer_type);
                async move {
                    match state.await {
                        Some(TimerState::Firing | TimerState::FiringReplaced) => false,
                        Some(
                            TimerState::Scheduled
                            | TimerState::FiringRescheduled
                            | TimerState::Parked,
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
    /// (re-fire after commit for a firing timer, requeue for a parked one)
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
        let trigger = self.mint(request).await?;
        self.schedule_trigger(trigger).await
    }

    /// Uses the key-row tag for `request`, or a fresh tag when no row exists.
    /// The key row supplies the live tag. The registry holds no tag.
    /// Keep this read: a fresh tag on a repeated write would make an aborted
    /// attempt's cells read as committed.
    async fn mint(&self, request: TimerRequest) -> Result<Trigger, TimerManagerError<T::Error>> {
        let tag = self
            .0
            .store
            .current_tag(&request.key, request.time, request.timer_type)
            .await
            .map_err(TimerManagerError::Store)?;
        Ok(request.into_trigger_with_tag(tag.unwrap_or_else(|| rand::rng().random())))
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

    /// Replace the key rows and update the scheduler.
    /// Keep each firing source until its attempt finishes.
    ///
    /// # Errors
    ///
    /// Returns [`TimerManagerError`] if storage or scheduler operations
    /// fail.
    pub async fn clear_and_schedule(
        &self,
        request: TimerRequest,
    ) -> Result<(), TimerManagerError<T::Error>> {
        let trigger = self.mint(request).await?;
        self.clear_and_schedule_trigger(trigger).await
    }

    /// Clears and schedules an already-tagged internal trigger.
    pub(crate) async fn clear_and_schedule_trigger(
        &self,
        trigger: Trigger,
    ) -> Result<(), TimerManagerError<T::Error>> {
        let existing_times: SmallVec<[(CompactDateTime, i32); 2]> = self
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
        apply_memory(&self.0.scheduler, &trigger, pre, None).await?;
        let mut keep =
            unschedule_replaced_timers(&self.0.scheduler, &trigger, &existing_times).await?;
        if prior.is_some() {
            keep.push(trigger.time);
        }

        // The single durable write: atomically inserts the new row and
        // clears the replaced ones (`ClearSchedule` transitions carry no
        // store effect of their own).
        self.0
            .store
            .clear_and_schedule(trigger.clone(), &keep)
            .await
            .map_err(TimerManagerError::Store)?;

        apply_memory(&self.0.scheduler, &trigger, post, None).await?;
        self.emit_clear_telemetry(&trigger, &existing_times);

        Ok(())
    }

    /// Emits one `timer_cancelled` event per replaced time (excluding the
    /// new time) and one `timer_scheduled` event for the new trigger.
    fn emit_clear_telemetry(&self, trigger: &Trigger, existing_times: &[(CompactDateTime, i32)]) {
        for &(old_time, _) in existing_times {
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

    /// Classifies a fired timer from its key row, then moves it from
    /// `Scheduled` to `Firing`.
    ///
    /// The store read runs first. A failed read leaves the timer `Scheduled`,
    /// so the caller can retry this call. Every schedule for a key runs
    /// inside that key's handler, so no schedule lands between the read and
    /// the flip.
    ///
    /// Returns `None` if the timer is absent or not `Scheduled`.
    pub(crate) async fn fire(
        &self,
        trigger: &Trigger,
    ) -> Result<Option<Fire>, TimerManagerError<T::Error>> {
        let tag = self
            .0
            .store
            .current_tag(&trigger.key, trigger.time, trigger.timer_type)
            .await
            .map_err(TimerManagerError::Store)?;
        let fired = self
            .0
            .scheduler
            .fire(&trigger.key, trigger.time, trigger.timer_type)
            .await;
        Ok(fired.then_some(match tag {
            Some(tag) if tag == trigger.tag => Fire::Live(tag),
            Some(tag) => Fire::Unswept(tag),
            None => Fire::Committed,
        }))
    }

    /// Marks a timer as completed.
    ///
    /// `FiringRescheduled` keeps the rows and rotates the tag for the next
    /// fire. `FiringReplaced` deletes its slab row. Other states delete both
    /// rows.
    ///
    /// Typically invoked by [`crate::timers::uncommitted::FiringTimer`]'s
    /// [`crate::consumer::Uncommitted::commit()`] impl.
    ///
    /// # Errors
    ///
    /// Returns [`TimerManagerError::Store`] if the storage write fails.
    pub async fn complete(&self, trigger: &Trigger) -> Result<(), TimerManagerError<T::Error>> {
        self.drive(trigger, TimerOp::Complete).await
    }

    /// Records a timer receipt without retiring its redelivery source.
    ///
    /// # Errors
    ///
    /// Returns [`TimerManagerError::Store`] if the receipt write fails.
    pub(crate) async fn receipt(
        &self,
        trigger: &Trigger,
    ) -> Result<(), TimerManagerError<T::Error>> {
        self.drive(trigger, TimerOp::Receipt).await
    }

    /// Retires a timer redelivery source after state promotion.
    ///
    /// # Errors
    ///
    /// Returns [`TimerManagerError::Store`] if the retirement write fails.
    pub(crate) async fn retire(
        &self,
        trigger: &Trigger,
    ) -> Result<(), TimerManagerError<T::Error>> {
        self.drive(trigger, TimerOp::Retire).await
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
    pub async fn abort(&self, trigger: &Trigger) {
        let _ = self.drive(trigger, TimerOp::Abort).await;
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
        let key_tag = if pre.queue == QueueEffect::Remove {
            self.0
                .store
                .current_tag(&trigger.key, trigger.time, trigger.timer_type)
                .await
                .map_err(TimerManagerError::Store)?
        } else {
            None
        };
        let removed = apply_memory(&self.0.scheduler, trigger, pre, key_tag).await?;
        let store = match (t.store(), removed) {
            (StoreEffect::Delete, RemoveOutcome::KeptSource) => StoreEffect::DeleteKeyRow,
            (store, _) => store,
        };

        let mut next = Cow::Borrowed(trigger);
        match store {
            StoreEffect::None => {}
            StoreEffect::Insert => self
                .0
                .store
                .add_trigger(trigger.clone())
                .await
                .map_err(TimerManagerError::Store)?,
            StoreEffect::InsertKeyRow => self
                .0
                .store
                .add_key_row(trigger.clone())
                .await
                .map_err(TimerManagerError::Store)?,
            StoreEffect::Delete => self
                .0
                .store
                .remove_trigger(&trigger.key, trigger.time, trigger.timer_type)
                .await
                .map_err(TimerManagerError::Store)?,
            StoreEffect::DeleteKeyRow => self
                .0
                .store
                .remove_key_row(&trigger.key, trigger.time, trigger.timer_type)
                .await
                .map_err(TimerManagerError::Store)?,
            StoreEffect::DeleteSlabRow => self
                .0
                .store
                .remove_slab_row(&trigger.key, trigger.time, trigger.timer_type)
                .await
                .map_err(TimerManagerError::Store)?,
            StoreEffect::UpdateTag => {
                next.to_mut().tag = fresh_tag_distinct_from(trigger.tag);
                self.0
                    .store
                    .update_tag(&trigger.key, trigger.time, trigger.timer_type, next.tag)
                    .await
                    .map_err(TimerManagerError::Store)?;
            }
        }

        apply_memory(&self.0.scheduler, &next, post, None).await?;

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

    pub(crate) async fn timer_state(
        &self,
        key: &Key,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> Option<TimerState> {
        self.0
            .scheduler
            .active_triggers()
            .get_state(key, time, timer_type)
            .await
    }
}

/// Classifies the store key row for one fired timer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Fire {
    /// The key tag equals the queued tag: one live attempt.
    Live(i32),
    /// The key tag differs from the queued tag: sweep before the handler.
    Unswept(i32),
    /// The key row is absent: the attempt committed.
    Committed,
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
/// flip, then the scheduler queue effect, in that order.
async fn apply_memory<E>(
    scheduler: &TriggerScheduler<E>,
    trigger: &Trigger,
    effects: MemoryEffects,
    key_tag: Option<i32>,
) -> Result<RemoveOutcome, TimerManagerError<E>>
where
    E: ClassifyError + Error + Debug + Send + Sync + 'static,
{
    let active = scheduler.active_triggers();
    if let Some(state) = effects.next_state {
        active
            .set_state(&trigger.key, trigger.time, trigger.timer_type, state)
            .await;
    }
    match effects.queue {
        QueueEffect::None => {}
        QueueEffect::Enqueue => scheduler.add_to_queue(trigger.clone()).await?,
        QueueEffect::Dequeue => scheduler.remove_from_queue(trigger.clone()).await?,
        QueueEffect::Insert => scheduler.schedule(trigger.clone()).await?,
        QueueEffect::Retag => scheduler.retag(trigger.clone()).await?,
        QueueEffect::Remove => return Ok(scheduler.unschedule(trigger.clone(), key_tag).await?),
        QueueEffect::Deactivate => {
            scheduler
                .deactivate(&trigger.key, trigger.time, trigger.timer_type)
                .await;
        }
    }
    Ok(RemoveOutcome::Removed)
}

/// Remove replaced queue entries and collect the slab rows that must stay.
/// The caller applies the slab deletes in the clear write.
async fn unschedule_replaced_timers<E>(
    scheduler: &TriggerScheduler<E>,
    new_trigger: &Trigger,
    existing_times: &[(CompactDateTime, i32)],
) -> Result<SmallVec<[CompactDateTime; 2]>, TimerManagerError<E>>
where
    E: ClassifyError + Error + Debug + Send + Sync + 'static,
{
    let mut keep = SmallVec::new();
    for &(old_time, key_tag) in existing_times {
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
        let transition = transition(prior, TimerOp::ClearReplaced);
        let (pre, post) = transition.phases();
        debug!(
            key = %old.key,
            timer_type = ?old.timer_type,
            old_time = ?old.time,
            prior_state = ?prior,
            "clear_and_schedule: unscheduling replaced timer"
        );
        let removed = apply_memory(scheduler, &old, pre, Some(key_tag)).await?;
        apply_memory(scheduler, &old, post, Some(key_tag)).await?;
        if transition.store() == StoreEffect::None || removed == RemoveOutcome::KeptSource {
            keep.push(old_time);
        }
    }
    Ok(keep)
}

#[cfg(test)]
mod tests;
