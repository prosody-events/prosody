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
use crate::telemetry::partition::TelemetryPartitionSender;
use crate::timers::active::{Insert, Listed, TimerSnapshot};
use crate::timers::datetime::CompactDateTime;

pub use crate::timers::error::TimerManagerError;
use crate::timers::scheduler::TriggerScheduler;
use crate::timers::segment::get_or_create_segment;
use crate::timers::store::TriggerStore;
use crate::timers::{
    DELETE_CONCURRENCY, PendingTimer, TimerRequest, TimerSemaphores, TimerType, Trigger, TriggerId,
};
use async_stream::stream;
use educe::Educe;
use futures::{Stream, StreamExt, TryFutureExt, TryStreamExt, stream, try_join};
use std::sync::Arc;
use tokio::sync::watch;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{Instrument, Span};

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

        // The scheduler actor owns timer memory and slab metadata. The manager
        // keeps its own `store` clone for row writes.
        let (trigger_rx, scheduler) =
            TriggerScheduler::new(config.store.clone(), segment, &heartbeats, shutdown_rx);
        let manager = Self::assemble(config, scheduler);
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

    /// Builds a manager over a scheduler handle.
    pub(in crate::timers) fn assemble(
        config: TimerManagerConfig<T>,
        scheduler: TriggerScheduler<T::Error>,
    ) -> Self {
        Self(Arc::new(TimerManagerInner {
            store: config.store,
            scheduler,
            telemetry: config.telemetry,
            source: config.source,
        }))
    }

    /// Retrieves the scheduled execution times for a given key and timer
    /// type: every time with a row or a waiting attempt in memory, except the
    /// times whose delivery runs with no attempt waiting.
    ///
    /// # Errors
    ///
    /// Returns [`TimerManagerError`] if the storage query fails or the
    /// scheduler has shut down.
    pub async fn scheduled_times(
        &self,
        key: &Key,
        timer_type: TimerType,
    ) -> Result<Vec<CompactDateTime>, TimerManagerError<T::Error>> {
        let stored = self
            .0
            .store
            .get_key_times(timer_type, key)
            .map_err(TimerManagerError::Store)
            .try_collect::<Vec<_>>();
        let listed = self.0.scheduler.list(key.clone(), timer_type).err_into();
        let (mut times, listed) = try_join!(stored, listed)?;

        times.extend(listed.iter().map(|&(time, _)| time));
        times.retain(|&time| !listed.contains(&(time, Listed::Firing { again: false })));
        times.sort_unstable();
        times.dedup();
        Ok(times)
    }

    /// Schedules a new timer for future execution.
    ///
    /// Writes the row of the timer and, if its slab is owned, queues it in
    /// memory. A timer that already waits keeps its attempt tag. During its
    /// own delivery, the timer fires again after the delivery commits.
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
    /// fails.
    pub async fn schedule(&self, request: TimerRequest) -> Result<(), TimerManagerError<T::Error>> {
        self.schedule_trigger(request.into_trigger()).await
    }

    /// Schedules `trigger`, with its tag unless an attempt waits.
    pub(crate) async fn schedule_trigger(
        &self,
        trigger: Trigger,
    ) -> Result<(), TimerManagerError<T::Error>> {
        let inner = &self.0;
        let (trigger, ticket) = inner.scheduler.insert(trigger, Insert::Schedule).await?;
        ticket
            .run(inner.store.add_trigger(trigger.clone()))
            .await
            .map_err(TimerManagerError::Store)?;
        inner.scheduled(&trigger);
        Ok(())
    }

    /// Cancels a specific scheduled timer.
    ///
    /// Deletes the row of the timer and removes it from memory. A delivery in
    /// progress is not reversed: its row stays for its commit, and only a
    /// waiting attempt of the same timer goes.
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
        if self.0.remove(key, time, timer_type).await? {
            self.0.cancelled(key, time, timer_type);
        }
        Ok(())
    }

    /// Cancels all timers for a specific key and timer type.
    ///
    /// Deletes the rows concurrently, up to [`DELETE_CONCURRENCY`] at once. A
    /// delivery in progress keeps its row, as in
    /// [`unschedule`](Self::unschedule).
    ///
    /// # Errors
    ///
    /// Returns [`TimerManagerError`] if a storage operation fails or the
    /// scheduler has shut down.
    pub async fn unschedule_all(
        &self,
        key: &Key,
        timer_type: TimerType,
    ) -> Result<(), TimerManagerError<T::Error>> {
        let inner = &self.0;
        let stored = inner
            .store
            .get_key_times(timer_type, key)
            .map_err(TimerManagerError::Store)
            .try_collect::<Vec<_>>();
        let started = inner
            .scheduler
            .unschedule_all(key.clone(), timer_type)
            .err_into();
        let (mut times, (ticket, firing)) = try_join!(stored, started)?;
        times.retain(|time| !firing.contains(time));

        let span = Span::current();
        let deletes = stream::iter(times.iter().copied())
            .map(|time| {
                inner
                    .store
                    .remove_trigger(key, time, timer_type)
                    .instrument(span.clone())
            })
            .buffer_unordered(DELETE_CONCURRENCY)
            .try_collect::<()>();
        ticket
            .run(deletes)
            .await
            .map_err(TimerManagerError::Store)?;
        for time in times {
            inner.cancelled(key, time, timer_type);
        }
        Ok(())
    }

    /// Atomically clears existing timers and schedules a new one.
    ///
    /// This is the path for singleton timer overwrites: one atomic store write
    /// inserts the new row and deletes the rows of the key's other timers of
    /// the type. Memory follows the same change.
    ///
    /// # Errors
    ///
    /// Returns [`TimerManagerError`] if storage or scheduler operations
    /// fail.
    pub async fn clear_and_schedule(
        &self,
        request: TimerRequest,
    ) -> Result<(), TimerManagerError<T::Error>> {
        let inner = &self.0;
        let (trigger, ticket) = inner
            .scheduler
            .insert(request.into_trigger(), Insert::Clear)
            .await?;
        let replaced = ticket
            .run(inner.store.clear_and_schedule(trigger.clone()))
            .await
            .map_err(TimerManagerError::Store)?;
        for time in replaced {
            inner.cancelled(&trigger.key, time, trigger.timer_type);
        }
        inner.scheduled(&trigger);
        Ok(())
    }

    /// Starts the pending trigger only if its attempt is still the one that
    /// fires next.
    pub(crate) async fn fire(&self, trigger: &Trigger) -> bool {
        // A scheduler that shut down fires nothing.
        matches!(self.0.scheduler.fire(trigger.clone()).await, Ok(true))
    }

    /// Marks a timer as completed.
    ///
    /// The row goes unless the timer was scheduled again during its delivery:
    /// then the waiting attempt keeps the row.
    ///
    /// Typically invoked by [`crate::timers::uncommitted::FiringTimer`]'s
    /// [`crate::consumer::Uncommitted::commit()`] impl.
    ///
    /// # Errors
    ///
    /// Returns [`TimerManagerError`] if the storage write fails or the
    /// scheduler has shut down.
    pub async fn complete(
        &self,
        key: &Key,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> Result<(), TimerManagerError<T::Error>> {
        let id = TriggerId {
            key: key.clone(),
            time,
            timer_type,
        };
        if let Some(ticket) = self.0.scheduler.complete(id).await? {
            ticket
                .run(self.0.store.remove_trigger(key, time, timer_type))
                .await
                .map_err(TimerManagerError::Store)?;
        }
        Ok(())
    }

    /// Returns a point-in-time [`TimerSnapshot`] of the in-memory scheduler.
    ///
    /// # Errors
    ///
    /// Returns [`TimerManagerError`] if the system clock is outside the
    /// 1970–2106 range or the scheduler has shut down; callers should skip
    /// that tick.
    pub async fn snapshot(&self) -> Result<TimerSnapshot, TimerManagerError<T::Error>> {
        Ok(self.0.scheduler.snapshot(CompactDateTime::now()?).await?)
    }

    /// Aborts a timer delivery, or its delivered attempt with `tag` that
    /// never fired.
    ///
    /// Never deletes the timer from persistent storage: an aborted timer
    /// fires again after a restart.
    pub fn abort(&self, key: &Key, time: CompactDateTime, timer_type: TimerType, tag: i32) {
        let id = TriggerId {
            key: key.clone(),
            time,
            timer_type,
        };
        self.0.scheduler.abort(id, tag);
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

    /// Retires a committed attempt and keeps the key row's replacement.
    ///
    /// A replacement in the key row gets its slab row back and waits to fire.
    /// Otherwise the committed attempt's rows and memory go.
    pub(crate) async fn retire_committed(
        &self,
        key: &Key,
        timer: TimerEventRef,
    ) -> Result<(), TimerManagerError<T::Error>> {
        let inner = &self.0;
        let current = inner
            .store
            .current_trigger(key, timer.time, timer.timer_type)
            .await
            .map_err(TimerManagerError::Store)?;
        if let Some(replacement) = current.filter(|trigger| trigger.tag != timer.tag) {
            let (trigger, ticket) = inner.scheduler.insert(replacement, Insert::Repair).await?;
            ticket
                .run(inner.store.insert_slab_trigger(trigger))
                .await
                .map_err(TimerManagerError::Store)?;
            return Ok(());
        }
        inner
            .remove(key, timer.time, timer.timer_type)
            .await
            .map(drop)
    }
}

impl<T: TriggerStore> TimerManagerInner<T> {
    /// Deletes the row of a timer and removes it from memory. Returns whether
    /// it deleted the row: a delivery in progress keeps its row.
    async fn remove(
        &self,
        key: &Key,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> Result<bool, TimerManagerError<T::Error>> {
        let id = TriggerId {
            key: key.clone(),
            time,
            timer_type,
        };
        let Some(ticket) = self.scheduler.unschedule(id).await? else {
            return Ok(false);
        };
        ticket
            .run(self.store.remove_trigger(key, time, timer_type))
            .await
            .map_err(TimerManagerError::Store)?;
        Ok(true)
    }

    /// Reports a scheduled timer to telemetry.
    fn scheduled(&self, trigger: &Trigger) {
        self.telemetry.timer_scheduled(
            trigger.key.clone(),
            trigger.time,
            trigger.timer_type,
            self.source.clone(),
        );
    }

    /// Reports a cancelled timer to telemetry.
    fn cancelled(&self, key: &Key, time: CompactDateTime, timer_type: TimerType) {
        self.telemetry
            .timer_cancelled(key.clone(), time, timer_type, self.source.clone());
    }
}

#[cfg(test)]
mod tests;
