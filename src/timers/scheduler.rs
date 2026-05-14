//! Asynchronous in-memory scheduler for timer triggers.
//!
//! Provides [`TriggerScheduler`] backed by a single actor task that owns:
//!
//! - **Slab metadata** writes: when a command schedules a trigger for a slab
//!   the actor hasn't seen, it writes the slab clustering row to persistent
//!   storage. Past-time slabs (`slab.id <= last_persisted_watermark`) route
//!   through an atomic UNLOGGED BATCH that also lowers the watermark, so
//!   invariant I1 holds across crashes.
//! - **Slab preloading**: future slabs are loaded from the store and their
//!   triggers inserted into the in-memory queue.
//! - **Slab cleanup**: completed slabs are deleted from the store; the
//!   watermark is raised opportunistically so the next restart skips the
//!   tombstones.
//!
//! Load and cleanup process one slab per `select!` arm invocation, so
//! Cassandra round-trips yield back to command processing and trigger
//! emission between slabs.

use crate::consumer::partition::ShutdownPhase;
use crate::error::{ClassifyError, ErrorCategory};
use crate::heartbeat::{Heartbeat, HeartbeatRegistry};
use crate::timers::active::ActiveTriggers;
use crate::timers::datetime::{CompactDateTime, CompactDateTimeError};
use crate::timers::duration::CompactDuration;
use crate::timers::queue::TriggerQueue;
use crate::timers::slab::{Slab, SlabId};
use crate::timers::store::{Segment, TriggerStore};
use crate::timers::{TimerType, Trigger};
use futures::TryFutureExt;
use futures::TryStreamExt;
use rand::RngExt;
use std::collections::BTreeSet;
use std::error::Error as StdError;
use std::fmt::Debug;
use std::ops::RangeInclusive;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::coop::cooperative;
use tokio::time::Instant;
use tokio::time::sleep_until;
use tokio::{select, spawn};
use tracing::{debug, warn};

/// Size of the internal command and trigger channels.
const BUFFER_SIZE: usize = 64;

/// Minimum jittered preload window: never start loading less than this much
/// time before a slab begins.
const MIN_PRELOAD: CompactDuration = CompactDuration::new(60);

/// Backoff between cleanup ticks when the previous tick found nothing to do.
const CLEANUP_INTERVAL: Duration = Duration::from_secs(30);

/// Backoff after a Cassandra-side failure inside the actor's load/cleanup
/// arms — these errors are typically transient.
const RETRY_DELAY: Duration = Duration::from_secs(1);

/// Asynchronous scheduler for timer [`Trigger`]s.
///
/// Cloning shares the same underlying actor task and active-triggers
/// registry.
#[derive(Debug)]
pub struct TriggerScheduler<E> {
    command_tx: mpsc::Sender<Command<E>>,
    active_triggers: ActiveTriggers,
}

impl<E> Clone for TriggerScheduler<E> {
    fn clone(&self) -> Self {
        Self {
            command_tx: self.command_tx.clone(),
            active_triggers: self.active_triggers.clone(),
        }
    }
}

/// Command sent from `TriggerScheduler` handles to the actor task.
///
/// Carries an `oneshot` reply so the caller can observe success, a store
/// error (currently only the Schedule operation can produce one), or a
/// shutdown signal via the channel being closed.
#[derive(Debug)]
struct Command<E> {
    operation: CommandOperation,
    trigger: Trigger,
    result_tx: oneshot::Sender<Result<(), E>>,
}

/// Operation variants for [`Command`].
#[derive(Copy, Clone, Debug)]
enum CommandOperation {
    /// Insert a trigger: persist slab metadata + add to `TriggerQueue`/
    /// `ActiveTriggers`.
    Add,
    /// Remove a trigger from both `DelayQueue` and `ActiveTriggers`.
    Remove,
    /// Add a trigger to the `DelayQueue` only (used when the caller has
    /// already transitioned `ActiveTriggers` to `FiringRescheduled`).
    AddToQueue,
    /// Remove a trigger from the `DelayQueue` only (cancel an earlier
    /// `AddToQueue`).
    RemoveFromQueue,
}

impl<E> TriggerScheduler<E>
where
    E: ClassifyError + StdError + Send + Sync + 'static,
{
    /// Creates a new scheduler, spawning the unified actor task.
    ///
    /// The actor owns the trigger queue, slab metadata writes, slab loads,
    /// and slab cleanup. Returns the receiver end of the expired-trigger
    /// channel along with the scheduler handle.
    ///
    /// # Arguments
    ///
    /// * `store` - Persistent trigger store; owned by the actor for slab
    ///   metadata writes, slab loads, and slab cleanup. Must be `Clone` so the
    ///   manager can keep its own handle for trigger-row writes.
    /// * `segment` - Segment metadata (id, slab size).
    /// * `heartbeats` - Registry for the actor's heartbeat.
    /// * `shutdown_rx` - Watch channel; the actor exits when phase reaches
    ///   `Draining`.
    pub fn new<T>(
        store: T,
        segment: Segment,
        heartbeats: &HeartbeatRegistry,
        shutdown_rx: watch::Receiver<ShutdownPhase>,
    ) -> (mpsc::Receiver<Trigger>, Self)
    where
        T: TriggerStore<Error = E>,
    {
        let (command_tx, commands_rx) = mpsc::channel(BUFFER_SIZE);
        let (trigger_tx, triggers_rx) = mpsc::channel(BUFFER_SIZE);
        let triggers = TriggerQueue::new();
        let active_triggers = triggers.active_triggers().clone();
        let heartbeat = heartbeats.register("timer scheduler");

        spawn(run_actor(
            store,
            segment,
            triggers,
            commands_rx,
            trigger_tx,
            heartbeat,
            shutdown_rx,
        ));

        (
            triggers_rx,
            Self {
                command_tx,
                active_triggers,
            },
        )
    }

    /// Returns a reference to the set of active triggers.
    pub fn active_triggers(&self) -> &ActiveTriggers {
        &self.active_triggers
    }

    /// Schedule a new [`Trigger`] for future emission.
    ///
    /// The actor persists slab metadata (if not already known) and inserts
    /// the trigger into the in-memory queue. If the slab is at or below the
    /// current watermark, the slab insert and watermark update are written
    /// in one UNLOGGED BATCH.
    pub async fn schedule(&self, trigger: Trigger) -> Result<(), TimerSchedulerError<E>> {
        self.send_command(CommandOperation::Add, trigger).await
    }

    /// Unschedule a previously scheduled [`Trigger`].
    pub async fn unschedule(&self, trigger: Trigger) -> Result<(), TimerSchedulerError<E>> {
        self.send_command(CommandOperation::Remove, trigger).await
    }

    /// Add a trigger to the `DelayQueue` without modifying `ActiveTriggers`.
    pub(crate) async fn add_to_queue(
        &self,
        trigger: Trigger,
    ) -> Result<(), TimerSchedulerError<E>> {
        self.send_command(CommandOperation::AddToQueue, trigger)
            .await
    }

    /// Remove a trigger from the `DelayQueue` without modifying
    /// `ActiveTriggers`.
    pub(crate) async fn remove_from_queue(
        &self,
        trigger: Trigger,
    ) -> Result<(), TimerSchedulerError<E>> {
        self.send_command(CommandOperation::RemoveFromQueue, trigger)
            .await
    }

    /// Transitions a timer from `Scheduled` to `Firing` state.
    ///
    /// Returns `true` if the transition succeeded.
    pub(crate) async fn fire(
        &self,
        key: &crate::Key,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> bool {
        use crate::timers::active::TimerState;

        if let Some(TimerState::Scheduled) =
            self.active_triggers.get_state(key, time, timer_type).await
        {
            self.active_triggers
                .set_state(key, time, timer_type, TimerState::Firing)
                .await
        } else {
            false
        }
    }

    /// Deactivate a trigger without removing it from the persistent queue.
    pub async fn deactivate(&self, key: &crate::Key, time: CompactDateTime, timer_type: TimerType) {
        self.active_triggers.remove(key, time, timer_type).await;
    }

    async fn send_command(
        &self,
        operation: CommandOperation,
        trigger: Trigger,
    ) -> Result<(), TimerSchedulerError<E>> {
        let (result_tx, result_rx) = oneshot::channel();
        self.command_tx
            .send(Command {
                operation,
                trigger,
                result_tx,
            })
            .map_err(|_| TimerSchedulerError::Shutdown)
            .await?;
        result_rx
            .await
            .map_err(|_| TimerSchedulerError::Shutdown)?
            .map_err(TimerSchedulerError::Store)
    }
}

/// Errors returned by [`TriggerScheduler`] methods.
#[derive(Debug, Error)]
pub enum TimerSchedulerError<E>
where
    E: ClassifyError + StdError + Send + Sync + 'static,
{
    /// A datetime conversion error occurred when scheduling a trigger.
    #[error(transparent)]
    DateTime(#[from] CompactDateTimeError),

    /// The scheduler has been shut down and cannot accept commands.
    #[error("Timer has been shutdown")]
    Shutdown,

    /// A store-layer write performed by the actor failed.
    #[error("Timer store error: {0:#}")]
    Store(E),
}

impl<E> ClassifyError for TimerSchedulerError<E>
where
    E: ClassifyError + StdError + Send + Sync + 'static,
{
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::DateTime(e) => e.classify_error(),
            // Scheduler is shutting down — partition rebalance / draining.
            Self::Shutdown => ErrorCategory::Transient,
            Self::Store(e) => e.classify_error(),
        }
    }
}

/// Mutable actor state — everything except the trigger queue and the
/// transient `trigger_to_send` slot. The queue is held as a local in
/// [`run_actor`] so handlers can borrow it independently of the rest of
/// the state.
struct ActorState<T> {
    store: T,
    segment: Segment,
    /// Slab IDs whose metadata row is known to be persisted (and whose
    /// triggers are loaded in `triggers`). Mirrors the loader's previous
    /// `HashSet<SlabId>` but stored as a `BTreeSet` so `first()` is O(log n).
    loaded_slab_ids: BTreeSet<SlabId>,
    /// Last persisted value of `slab_watermark` for this segment.
    ///
    /// Invariant I1: when `Some(w)`, every clustering row in `timer_segments`
    /// for this segment has `slab_id > w`. `None` means pre-migration / not
    /// yet written; treated as 0 by the loader.
    last_persisted_watermark: Option<SlabId>,
    /// Highest slab ID we've issued a load for. Tracks loader progress so we
    /// know where to resume next tick.
    highest_loaded_slab_id: Option<SlabId>,
    /// Jittered preload window — re-rolled after each load cycle.
    preload_window: CompactDuration,
    /// Wall-clock deadline for the next load tick.
    next_load_at: Instant,
    /// Wall-clock deadline for the next cleanup tick.
    next_cleanup_at: Instant,
}

/// Background task: drives the scheduler's command loop, expired trigger
/// emission, slab loading, and slab cleanup.
async fn run_actor<T>(
    store: T,
    segment: Segment,
    mut triggers: TriggerQueue,
    mut commands: mpsc::Receiver<Command<T::Error>>,
    trigger_tx: mpsc::Sender<Trigger>,
    heartbeat: Heartbeat,
    mut shutdown_rx: watch::Receiver<ShutdownPhase>,
) where
    T: TriggerStore,
{
    let preload_window = calculate_preload(segment.slab_size);
    let now = Instant::now();
    let mut state: ActorState<T> = ActorState {
        store,
        segment,
        loaded_slab_ids: BTreeSet::new(),
        last_persisted_watermark: None,
        highest_loaded_slab_id: None,
        preload_window,
        next_load_at: now,
        next_cleanup_at: now + CLEANUP_INTERVAL,
    };

    // Seed `last_persisted_watermark` from the store. A read failure
    // degrades to "scan from 0", matching the NULL-watermark case.
    match state.store.get_slab_watermark().await {
        Ok(w) => state.last_persisted_watermark = w,
        Err(e) => warn!("Failed to read slab_watermark on startup: {e:#}"),
    }

    let mut trigger_to_send: Option<Trigger> = None;

    loop {
        heartbeat.beat();

        if *shutdown_rx.borrow() >= ShutdownPhase::Draining {
            debug!("Scheduler actor shutting down");
            return;
        }

        let cleanup_deadline = state.next_cleanup_at;
        let load_deadline = state.next_load_at;

        match trigger_to_send.clone() {
            Some(trigger) => {
                select! {
                    result = commands.recv() => {
                        let Some(command) = result else { break; };
                        process_command(&mut state, &mut triggers, command).await;
                    }
                    result = trigger_tx.send(trigger) => {
                        trigger_to_send = None;
                        if result.is_err() { break; }
                    }
                    () = sleep_until(cleanup_deadline) => {
                        cleanup_step(&mut state, &triggers).await;
                    }
                    () = sleep_until(load_deadline) => {
                        load_step(&mut state, &mut triggers).await;
                    }
                    () = heartbeat.next() => {},
                    _ = shutdown_rx.changed() => {
                        debug!("Scheduler actor shutting down");
                        return;
                    }
                }
            }
            None => {
                select! {
                    result = commands.recv() => {
                        let Some(command) = result else { break; };
                        process_command(&mut state, &mut triggers, command).await;
                    }
                    Some(trigger) = triggers.next() => {
                        if let Err(err) = trigger_tx.try_send(trigger) {
                            match err {
                                TrySendError::Full(t) => trigger_to_send = Some(t),
                                TrySendError::Closed(_) => break,
                            }
                        }
                    }
                    () = sleep_until(cleanup_deadline) => {
                        cleanup_step(&mut state, &triggers).await;
                    }
                    () = sleep_until(load_deadline) => {
                        load_step(&mut state, &mut triggers).await;
                    }
                    () = heartbeat.next() => {},
                    _ = shutdown_rx.changed() => {
                        debug!("Scheduler actor shutting down");
                        return;
                    }
                }
            }
        }
    }
}

/// Processes a single command from the command channel.
async fn process_command<T>(
    state: &mut ActorState<T>,
    triggers: &mut TriggerQueue,
    Command {
        operation,
        trigger,
        result_tx,
    }: Command<T::Error>,
) where
    T: TriggerStore,
{
    let result: Result<(), T::Error> = match operation {
        CommandOperation::Add => handle_add(state, triggers, trigger).await,
        CommandOperation::Remove => {
            triggers.remove(&trigger).await;
            Ok(())
        }
        CommandOperation::AddToQueue => {
            triggers.insert_queue_only(trigger);
            Ok(())
        }
        CommandOperation::RemoveFromQueue => {
            triggers.remove_queue_only(&trigger);
            Ok(())
        }
    };

    // Ignore send errors: the caller will observe a closed reply channel as
    // a shutdown signal.
    let _ = result_tx.send(result);
}

/// Handler for [`CommandOperation::Add`].
///
/// Classifies the trigger's slab against the actor's load high-water and
/// the persisted watermark, then dispatches to one of three paths:
///
/// - **Past-time** (`slab_id <= watermark`): write the slab clustering row and
///   lower the watermark in one UNLOGGED BATCH (I6), register the slab as
///   loaded, and insert into the in-memory queue. The watermark route is the
///   only way the actor learns about slabs below its existing watermark, so
///   this also forces the trigger in-memory (`load_step` never scans backward).
/// - **Owned**: `slab_id` is at or below `highest_loaded_slab_id` — the
///   `load_step` has already scanned this far. Insert the trigger in-memory and
///   add `slab_id` to `loaded_slab_ids` so cleanup can sweep the slab when its
///   triggers complete. Slab metadata is written only on the first sighting (we
///   skip the round-trip when the slab is already known).
/// - **Pending** (`slab_id` above the load high-water): write the slab
///   clustering row only. The trigger row (written first by the manager via
///   [`TriggerStore::add_trigger`]) waits in storage; `load_step` will pick it
///   up when its slab enters the preload window. Keeps the in-memory queue
///   bounded to the preload window.
async fn handle_add<T>(
    state: &mut ActorState<T>,
    triggers: &mut TriggerQueue,
    trigger: Trigger,
) -> Result<(), T::Error>
where
    T: TriggerStore,
{
    let slab = Slab::from_time(state.segment.slab_size, trigger.time);
    let slab_id = slab.id();

    let is_past_time = state.last_persisted_watermark.is_some_and(|w| slab_id <= w);
    let is_in_window = state.highest_loaded_slab_id.is_some_and(|h| slab_id <= h);
    let is_tracked = state.loaded_slab_ids.contains(&slab_id);

    if is_past_time {
        // I6: write the slab row and lower the watermark atomically.
        let new_watermark = slab_id.checked_sub(1);
        state
            .store
            .batch_insert_slab_with_watermark(slab, new_watermark)
            .await?;
        state.last_persisted_watermark = new_watermark;
        state.loaded_slab_ids.insert(slab_id);
        triggers.insert(trigger).await;
    } else if is_in_window {
        if !is_tracked {
            state.store.insert_slab(slab).await?;
            state.loaded_slab_ids.insert(slab_id);
        }
        triggers.insert(trigger).await;
    } else {
        // Pending: register slab metadata so load_step finds it later.
        // Skip the round-trip if a prior tick already wrote it.
        if !is_tracked {
            state.store.insert_slab(slab).await?;
        }
    }

    Ok(())
}

/// Cleanup tick: try to delete one completed slab per invocation.
///
/// Snapshots active slab ids once, picks the first loaded slab that is
/// neither active nor the current `now_slab`, and deletes it. If no
/// candidate exists, tries to raise the watermark and reschedules.
async fn cleanup_step<T>(state: &mut ActorState<T>, triggers: &TriggerQueue)
where
    T: TriggerStore,
{
    let now_slab_id = match CompactDateTime::now() {
        Ok(now) => Slab::from_time(state.segment.slab_size, now).id(),
        Err(e) => {
            warn!("cleanup_step: failed to compute now: {e:#}");
            state.next_cleanup_at = Instant::now() + RETRY_DELAY;
            return;
        }
    };

    let active_slab_ids = collect_active_slab_ids(state.segment.slab_size, triggers).await;

    let candidate = state
        .loaded_slab_ids
        .iter()
        .copied()
        .find(|slab_id| *slab_id < now_slab_id && !active_slab_ids.contains(slab_id));

    if let Some(slab_id) = candidate {
        if let Err(e) = state.store.delete_slab(slab_id).await {
            warn!(
                slab_id,
                "cleanup_step: failed to delete slab: {e:#}; will retry"
            );
            state.next_cleanup_at = Instant::now() + RETRY_DELAY;
            return;
        }
        state.loaded_slab_ids.remove(&slab_id);
        // Reschedule immediately — we may have more slabs ready to delete.
        state.next_cleanup_at = Instant::now();
        return;
    }

    // No deletion candidate; opportunistically raise the watermark.
    if let Err(e) = maybe_advance_watermark(state, now_slab_id).await {
        warn!("cleanup_step: failed to advance watermark: {e:#}");
    }
    state.next_cleanup_at = Instant::now() + CLEANUP_INTERVAL;
}

/// Raises `slab_watermark` to one below the lowest in-progress slab, when
/// the new value would be a meaningful increase over what we previously
/// persisted.
///
/// - When `loaded_slab_ids` is non-empty, candidate = `first - 1`.
/// - When `loaded_slab_ids` is empty, candidate = `now_slab_id - 1` (all
///   future-DB slabs sit above `now_slab_id`, so this is safe).
///
/// Skips the write entirely if the candidate is no higher than what we
/// already persisted.
async fn maybe_advance_watermark<T>(
    state: &mut ActorState<T>,
    now_slab_id: SlabId,
) -> Result<(), T::Error>
where
    T: TriggerStore,
{
    let candidate = match state.loaded_slab_ids.iter().next() {
        Some(&first) => first.checked_sub(1),
        None => now_slab_id.checked_sub(1),
    };

    let Some(candidate) = candidate else {
        return Ok(());
    };

    let should_persist = match state.last_persisted_watermark {
        Some(current) => candidate > current,
        None => true,
    };
    if !should_persist {
        return Ok(());
    }

    state.store.set_slab_watermark(Some(candidate)).await?;
    state.last_persisted_watermark = Some(candidate);
    debug!(watermark = candidate, "advanced slab_watermark");
    Ok(())
}

/// Load tick: drain triggers for every slab from the current load high-water
/// up to the slab containing `now + preload_window`, then push the high-water
/// forward.
///
/// The store fans the per-slab scans out concurrently via
/// [`TriggerStore::get_slab_triggers_in_range`], so startup catch-up runs at
/// up to `LOAD_CONCURRENCY` slabs in flight.
async fn load_step<T>(state: &mut ActorState<T>, triggers: &mut TriggerQueue)
where
    T: TriggerStore,
{
    let now = match CompactDateTime::now() {
        Ok(t) => t,
        Err(e) => {
            warn!("load_step: failed to compute now: {e:#}");
            state.next_load_at = Instant::now() + RETRY_DELAY;
            return;
        }
    };
    let target_time = match now.add_duration(state.preload_window) {
        Ok(t) => t,
        Err(e) => {
            warn!("load_step: failed to compute target_time: {e:#}");
            state.next_load_at = Instant::now() + RETRY_DELAY;
            return;
        }
    };
    let target_slab = Slab::from_time(state.segment.slab_size, target_time);
    let target_slab_id = target_slab.id();

    let Some(start_slab_id) = next_unloaded_slab_id(state) else {
        // SlabId space exhausted (`u32::MAX` reached). Nothing more to load.
        state.next_load_at = Instant::now() + CLEANUP_INTERVAL;
        return;
    };

    if start_slab_id > target_slab_id {
        // High-water already past the preload horizon. Schedule the next
        // tick for when the following slab enters the window.
        schedule_next_load(state, &target_slab);
        return;
    }

    let load_result =
        drain_slab_range(&state.store, start_slab_id..=target_slab_id, triggers).await;

    match load_result {
        Ok(LoadOutcome { seen, count }) => {
            for slab_id in seen {
                state.loaded_slab_ids.insert(slab_id);
            }
            state.highest_loaded_slab_id = Some(target_slab_id);
            debug!(
                start_slab_id,
                target_slab_id,
                triggers = count,
                "load_step: loaded range"
            );
            schedule_next_load(state, &target_slab);
        }
        Err(e) => {
            warn!(
                start_slab_id,
                target_slab_id, "load_step: stream error: {e:#}; will retry"
            );
            state.next_load_at = Instant::now() + RETRY_DELAY;
        }
    }
}

/// Result of draining one range scan: which slabs yielded at least one
/// trigger, and the total trigger count for logging.
struct LoadOutcome {
    seen: BTreeSet<SlabId>,
    count: usize,
}

/// Drains a [`TriggerStore::get_slab_triggers_in_range`] stream into the
/// trigger queue. Isolated as a helper so the caller can let the stream's
/// borrow on `store` end before mutating other actor state.
async fn drain_slab_range<T>(
    store: &T,
    range: RangeInclusive<SlabId>,
    triggers: &mut TriggerQueue,
) -> Result<LoadOutcome, T::Error>
where
    T: TriggerStore,
{
    let mut stream = std::pin::pin!(store.get_slab_triggers_in_range(range));
    let mut seen = BTreeSet::<SlabId>::new();
    let mut count = 0_usize;
    while let Some((slab_id, trigger)) = cooperative(stream.try_next()).await? {
        triggers.insert(trigger).await;
        seen.insert(slab_id);
        count = count.saturating_add(1);
    }
    Ok(LoadOutcome { seen, count })
}

/// Returns the next slab id the load loop should scan from, or `None` when
/// the slab-id space is exhausted.
fn next_unloaded_slab_id<T>(state: &ActorState<T>) -> Option<SlabId> {
    match state.highest_loaded_slab_id {
        Some(h) => h.checked_add(1),
        None => Some(
            state
                .last_persisted_watermark
                .map_or(0, |w| w.saturating_add(1)),
        ),
    }
}

/// Sets `next_load_at` to when the slab after `target_slab` should be
/// preloaded, re-rolling the jittered window for the next cycle.
fn schedule_next_load<T>(state: &mut ActorState<T>, target_slab: &Slab) {
    let Some(next_slab) = target_slab.next() else {
        state.next_load_at = Instant::now() + CLEANUP_INTERVAL;
        return;
    };
    let wait = calculate_wait_time(next_slab.range().start, state.preload_window);
    state.next_load_at = Instant::now() + wait.into();
    state.preload_window = calculate_preload(state.segment.slab_size);
}

/// Builds the set of slab IDs currently holding active triggers — i.e. the
/// slabs cleanup must NOT delete.
async fn collect_active_slab_ids(
    slab_size: CompactDuration,
    triggers: &TriggerQueue,
) -> BTreeSet<SlabId> {
    let mut active = BTreeSet::new();
    triggers
        .active_triggers()
        .scan_active_times(|time, _timer_type| {
            active.insert(Slab::from_time(slab_size, time).id());
        })
        .await;
    active
}

/// Computes the wait time until a slab beginning at `load_time` should be
/// preloaded. Returns zero for past or imminent slabs.
fn calculate_wait_time(
    load_time: CompactDateTime,
    preload_window: CompactDuration,
) -> CompactDuration {
    load_time
        .compact_duration_from_now()
        .unwrap_or(CompactDuration::MIN)
        .saturating_sub(preload_window)
}

/// Generates a jittered preload window between [`MIN_PRELOAD`, `slab_size`].
fn calculate_preload(slab_size: CompactDuration) -> CompactDuration {
    let max_jitter = slab_size.saturating_sub(MIN_PRELOAD);
    CompactDuration::from(rand::rng().random_range(0..=max_jitter.seconds()))
        .saturating_add(MIN_PRELOAD)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Key;
    use crate::timers::datetime::CompactDateTime;
    use crate::timers::duration::CompactDuration;
    use crate::timers::store::adapter::TableAdapter;
    use crate::timers::store::memory::{InMemoryTriggerStore, memory_store};
    use crate::timers::store::operations::TriggerOperations;
    use crate::timers::store::{Segment, SegmentVersion, TriggerStore};
    use crate::timers::{TimerType, Trigger};
    use color_eyre::eyre::Result;
    use std::time::Duration as StdDuration;
    use tokio::time::{Instant, advance};
    use tracing::Span;
    use uuid::Uuid;

    /// Tests use a 300s slab and a deterministic preload window so slab
    /// math is predictable across runs.
    const SLAB_SIZE_SECS: u32 = 300;
    const PRELOAD_SECS: u32 = 120;

    fn test_segment() -> Segment {
        Segment {
            id: Uuid::new_v4(),
            name: "scheduler-test".to_owned(),
            slab_size: CompactDuration::new(SLAB_SIZE_SECS),
            version: SegmentVersion::V3,
        }
    }

    type TestStore = TableAdapter<InMemoryTriggerStore>;

    fn fresh_state(store: TestStore, segment: Segment) -> ActorState<TestStore> {
        let now = Instant::now();
        ActorState {
            store,
            segment,
            loaded_slab_ids: BTreeSet::new(),
            last_persisted_watermark: None,
            highest_loaded_slab_id: None,
            preload_window: CompactDuration::new(PRELOAD_SECS),
            next_load_at: now,
            next_cleanup_at: now,
        }
    }

    fn slab_time(slab_id: SlabId) -> CompactDateTime {
        CompactDateTime::from(slab_id.saturating_mul(SLAB_SIZE_SECS))
    }

    fn make_trigger(key: &str, slab_id: SlabId, ty: TimerType) -> Trigger {
        Trigger::new(Key::from(key), slab_time(slab_id), ty, Span::current())
    }

    // ===================================================================
    // Pure helper tests
    // ===================================================================

    #[tokio::test(start_paused = true)]
    async fn test_calculate_wait_time_future() -> Result<()> {
        let now = CompactDateTime::now()?;
        let future = now.add_duration(CompactDuration::new(120))?;
        let preload = CompactDuration::new(30);
        // 120s out minus 30s preload = 90s wait.
        assert_eq!(
            calculate_wait_time(future, preload),
            CompactDuration::new(90)
        );
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn test_calculate_wait_time_within_preload_window() -> Result<()> {
        let now = CompactDateTime::now()?;
        let near = now.add_duration(CompactDuration::new(15))?;
        // 15s out minus 30s preload saturates at zero — load immediately.
        assert!(calculate_wait_time(near, CompactDuration::new(30)).is_zero());
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn test_calculate_wait_time_past_time() -> Result<()> {
        let load_time = CompactDateTime::now()?;
        advance(StdDuration::from_mins(1)).await;
        // Load time is now in the past — return zero.
        assert!(calculate_wait_time(load_time, CompactDuration::new(30)).is_zero());
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn test_calculate_wait_time_exact_preload_boundary() -> Result<()> {
        let now = CompactDateTime::now()?;
        let boundary = now.add_duration(CompactDuration::new(30))?;
        // Exactly at the preload boundary — load immediately.
        assert!(calculate_wait_time(boundary, CompactDuration::new(30)).is_zero());
        Ok(())
    }

    #[test]
    fn test_calculate_preload_within_bounds() {
        let slab_size = CompactDuration::new(SLAB_SIZE_SECS);
        for _ in 0_i32..32_i32 {
            let window = calculate_preload(slab_size);
            assert!(
                window >= MIN_PRELOAD && window <= slab_size,
                "preload {window} outside [{MIN_PRELOAD}, {slab_size}]"
            );
        }
    }

    #[test]
    fn test_next_unloaded_slab_id_from_fresh_state() {
        let segment = test_segment();
        let store = memory_store(segment.clone());
        let mut state = fresh_state(store, segment);
        // No watermark, no high-water: scan starts at 0.
        assert_eq!(next_unloaded_slab_id(&state), Some(0));

        state.last_persisted_watermark = Some(7);
        assert_eq!(next_unloaded_slab_id(&state), Some(8));

        state.highest_loaded_slab_id = Some(12);
        assert_eq!(next_unloaded_slab_id(&state), Some(13));

        state.highest_loaded_slab_id = Some(SlabId::MAX);
        // Slab-id space exhausted.
        assert_eq!(next_unloaded_slab_id(&state), None);
    }

    // ===================================================================
    // handle_add ownership gate
    // ===================================================================

    #[tokio::test(start_paused = true)]
    async fn test_handle_add_past_time_routes_through_batch() -> Result<()> {
        let segment = test_segment();
        let store = memory_store(segment.clone());
        store.set_slab_watermark(Some(5)).await?;

        let mut state = fresh_state(store.clone(), segment);
        state.last_persisted_watermark = Some(5);

        let mut queue = TriggerQueue::new();
        let active = queue.active_triggers().clone();

        // Trigger for slab 3 — below watermark — must route through the
        // BATCH and force the trigger in-memory.
        let trigger = make_trigger("past", 3, TimerType::Application);
        handle_add(&mut state, &mut queue, trigger.clone()).await?;

        assert!(state.loaded_slab_ids.contains(&3));
        assert_eq!(state.last_persisted_watermark, Some(2));
        assert_eq!(store.get_slab_watermark().await?, Some(2));
        assert!(
            active
                .contains(&trigger.key, trigger.time, trigger.timer_type)
                .await,
            "past-time trigger must land in ActiveTriggers"
        );
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn test_handle_add_past_time_at_slab_zero_clears_watermark() -> Result<()> {
        let segment = test_segment();
        let store = memory_store(segment.clone());
        store.set_slab_watermark(Some(0)).await?;

        let mut state = fresh_state(store.clone(), segment);
        state.last_persisted_watermark = Some(0);

        let mut queue = TriggerQueue::new();
        let trigger = make_trigger("zero", 0, TimerType::Application);
        handle_add(&mut state, &mut queue, trigger).await?;

        // Lowering 0 saturates to None.
        assert_eq!(state.last_persisted_watermark, None);
        assert_eq!(store.get_slab_watermark().await?, None);
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn test_handle_add_owned_writes_metadata_and_inserts() -> Result<()> {
        let segment = test_segment();
        let store = memory_store(segment.clone());
        let mut state = fresh_state(store.clone(), segment);
        // Pretend load_step has already scanned through slab 2.
        state.highest_loaded_slab_id = Some(2);

        let mut queue = TriggerQueue::new();
        let active = queue.active_triggers().clone();

        let trigger = make_trigger("owned", 1, TimerType::Application);
        handle_add(&mut state, &mut queue, trigger.clone()).await?;

        assert!(state.loaded_slab_ids.contains(&1));
        assert!(
            active
                .contains(&trigger.key, trigger.time, trigger.timer_type)
                .await,
            "owned trigger must land in ActiveTriggers"
        );
        let slab_ids: Vec<SlabId> = store
            .get_slab_range(1..=1)
            .map_ok(|s| s)
            .try_collect()
            .await?;
        assert_eq!(
            slab_ids,
            vec![1],
            "slab metadata persisted on first sighting"
        );
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn test_handle_add_owned_skips_redundant_metadata_write() -> Result<()> {
        let segment = test_segment();
        let store = memory_store(segment.clone());
        let mut state = fresh_state(store.clone(), segment);
        state.highest_loaded_slab_id = Some(5);
        state.loaded_slab_ids.insert(3);

        let mut queue = TriggerQueue::new();
        let active = queue.active_triggers().clone();

        let trigger = make_trigger("owned-tracked", 3, TimerType::Application);
        handle_add(&mut state, &mut queue, trigger.clone()).await?;

        // No metadata write needed (already tracked) — verify by deleting
        // from store BEFORE the call would have been a no-op anyway, but we
        // can at least confirm the in-memory side fired.
        assert!(
            active
                .contains(&trigger.key, trigger.time, trigger.timer_type)
                .await
        );
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn test_handle_add_pending_persists_only() -> Result<()> {
        let segment = test_segment();
        let store = memory_store(segment.clone());
        let mut state = fresh_state(store.clone(), segment);
        // High-water at slab 1: anything ≥ slab 2 is pending.
        state.highest_loaded_slab_id = Some(1);

        let mut queue = TriggerQueue::new();
        let active = queue.active_triggers().clone();

        let trigger = make_trigger("pending", 10, TimerType::Application);
        handle_add(&mut state, &mut queue, trigger.clone()).await?;

        assert!(!state.loaded_slab_ids.contains(&10));
        assert!(
            !active
                .contains(&trigger.key, trigger.time, trigger.timer_type)
                .await,
            "pending trigger must NOT be in ActiveTriggers"
        );
        let slab_ids: Vec<SlabId> = store.get_slab_range(10..=10).try_collect().await?;
        assert_eq!(
            slab_ids,
            vec![10],
            "pending path still writes slab metadata so load_step can find it"
        );
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn test_handle_add_pending_at_initial_state() -> Result<()> {
        // Initial actor state: no high-water, no watermark. Every slab is
        // pending until load_step runs at least once.
        let segment = test_segment();
        let store = memory_store(segment.clone());
        let mut state = fresh_state(store.clone(), segment);

        let mut queue = TriggerQueue::new();
        let active = queue.active_triggers().clone();

        let trigger = make_trigger("initial-pending", 0, TimerType::Application);
        handle_add(&mut state, &mut queue, trigger.clone()).await?;

        assert!(state.last_persisted_watermark.is_none());
        assert!(state.highest_loaded_slab_id.is_none());
        assert!(state.loaded_slab_ids.is_empty());
        assert!(
            !active
                .contains(&trigger.key, trigger.time, trigger.timer_type)
                .await
        );
        Ok(())
    }

    // ===================================================================
    // load_step
    // ===================================================================

    #[tokio::test(start_paused = true)]
    async fn test_load_step_picks_up_store_triggers() -> Result<()> {
        let segment = test_segment();
        let store = memory_store(segment.clone());
        // Seed a trigger directly in storage at slab 0 (i.e. time 0 — now).
        let trigger = make_trigger("preloaded", 0, TimerType::Application);
        store.add_trigger(trigger.clone()).await?;
        store.insert_slab(Slab::new(0, segment.slab_size)).await?;

        let mut state = fresh_state(store.clone(), segment.clone());
        let mut queue = TriggerQueue::new();
        let active = queue.active_triggers().clone();

        load_step(&mut state, &mut queue).await;

        assert!(
            state.highest_loaded_slab_id.is_some(),
            "load_step must advance the high-water"
        );
        assert!(state.loaded_slab_ids.contains(&0));
        assert!(
            active
                .contains(&trigger.key, trigger.time, trigger.timer_type)
                .await,
            "load_step must enqueue the preloaded trigger"
        );
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn test_load_step_advances_high_water_on_empty_range() -> Result<()> {
        let segment = test_segment();
        let store = memory_store(segment.clone());
        let mut state = fresh_state(store, segment);
        let mut queue = TriggerQueue::new();

        load_step(&mut state, &mut queue).await;

        // Even with no slabs in the store, the high-water advances to the
        // preload-window target slab so subsequent triggers land owned.
        assert!(state.highest_loaded_slab_id.is_some());
        assert!(state.loaded_slab_ids.is_empty());
        Ok(())
    }

    // ===================================================================
    // cleanup_step + maybe_advance_watermark
    // ===================================================================

    /// Returns the slab id containing `CompactDateTime::now()`. Useful for
    /// tests that position past / now / future slabs relative to wall-clock
    /// time (which `tokio::time::pause` does not override).
    fn now_slab_id() -> Result<SlabId> {
        Ok(Slab::from_time(
            CompactDuration::new(SLAB_SIZE_SECS),
            CompactDateTime::now()?,
        )
        .id())
    }

    #[tokio::test(start_paused = true)]
    async fn test_cleanup_step_deletes_completed_slab() -> Result<()> {
        let segment = test_segment();
        let store = memory_store(segment.clone());
        let past = now_slab_id()?.saturating_sub(5);
        store
            .insert_slab(Slab::new(past, segment.slab_size))
            .await?;

        let mut state = fresh_state(store.clone(), segment);
        state.loaded_slab_ids.insert(past);

        let queue = TriggerQueue::new();
        cleanup_step(&mut state, &queue).await;

        assert!(!state.loaded_slab_ids.contains(&past));
        let remaining: Vec<SlabId> = store.get_slab_range(past..=past).try_collect().await?;
        assert!(
            remaining.is_empty(),
            "past slab should be deleted from store"
        );
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn test_cleanup_step_preserves_active_slab() -> Result<()> {
        let segment = test_segment();
        let store = memory_store(segment.clone());
        let past = now_slab_id()?.saturating_sub(5);
        store
            .insert_slab(Slab::new(past, segment.slab_size))
            .await?;

        let mut state = fresh_state(store.clone(), segment);
        state.loaded_slab_ids.insert(past);

        let mut queue = TriggerQueue::new();
        let trigger = make_trigger("active", past, TimerType::Application);
        queue.insert(trigger).await;

        cleanup_step(&mut state, &queue).await;

        assert!(
            state.loaded_slab_ids.contains(&past),
            "active slab must stay"
        );
        let remaining: Vec<SlabId> = store.get_slab_range(past..=past).try_collect().await?;
        assert_eq!(remaining, vec![past], "store still has the slab");
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn test_cleanup_step_preserves_now_slab() -> Result<()> {
        let segment = test_segment();
        let store = memory_store(segment.clone());
        let now = now_slab_id()?;
        store.insert_slab(Slab::new(now, segment.slab_size)).await?;

        let mut state = fresh_state(store.clone(), segment);
        state.loaded_slab_ids.insert(now);

        let queue = TriggerQueue::new();
        cleanup_step(&mut state, &queue).await;

        // The current slab is never a deletion candidate.
        assert!(state.loaded_slab_ids.contains(&now));
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn test_cleanup_step_advances_watermark_when_idle() -> Result<()> {
        let segment = test_segment();
        let store = memory_store(segment.clone());
        let mut state = fresh_state(store.clone(), segment);

        let now = now_slab_id()?;
        let queue = TriggerQueue::new();
        cleanup_step(&mut state, &queue).await;

        // With no loaded slabs, the candidate is `now_slab_id - 1`.
        let expected = now.checked_sub(1);
        assert_eq!(state.last_persisted_watermark, expected);
        assert_eq!(store.get_slab_watermark().await?, expected);
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn test_maybe_advance_watermark_uses_lowest_loaded_minus_one() -> Result<()> {
        let segment = test_segment();
        let store = memory_store(segment.clone());
        let mut state = fresh_state(store.clone(), segment);
        state.loaded_slab_ids.insert(10);
        state.loaded_slab_ids.insert(12);

        maybe_advance_watermark(&mut state, 20).await?;
        assert_eq!(state.last_persisted_watermark, Some(9));
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn test_maybe_advance_watermark_skips_no_progress() -> Result<()> {
        let segment = test_segment();
        let store = memory_store(segment.clone());
        store.set_slab_watermark(Some(20)).await?;

        let mut state = fresh_state(store.clone(), segment);
        state.last_persisted_watermark = Some(20);
        state.loaded_slab_ids.insert(15);

        maybe_advance_watermark(&mut state, 30).await?;
        // Candidate 14 is below current 20 — no write.
        assert_eq!(state.last_persisted_watermark, Some(20));
        assert_eq!(store.get_slab_watermark().await?, Some(20));
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn test_maybe_advance_watermark_empty_loaded() -> Result<()> {
        let segment = test_segment();
        let store = memory_store(segment.clone());
        let mut state = fresh_state(store.clone(), segment);
        // No loaded slabs — candidate falls back to now_slab - 1.
        maybe_advance_watermark(&mut state, 7).await?;
        assert_eq!(state.last_persisted_watermark, Some(6));
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn test_collect_active_slab_ids_empty() {
        let queue = TriggerQueue::new();
        let ids = collect_active_slab_ids(CompactDuration::new(SLAB_SIZE_SECS), &queue).await;
        assert!(ids.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn test_collect_active_slab_ids_dedups_across_keys() -> Result<()> {
        let mut queue = TriggerQueue::new();
        queue
            .insert(make_trigger("a", 0, TimerType::Application))
            .await;
        queue
            .insert(make_trigger("b", 0, TimerType::Application))
            .await;
        queue
            .insert(make_trigger("c", 7, TimerType::Application))
            .await;

        let ids = collect_active_slab_ids(CompactDuration::new(SLAB_SIZE_SECS), &queue).await;
        let expected: BTreeSet<SlabId> = [0_u32, 7].into_iter().collect();
        assert_eq!(ids, expected);
        Ok(())
    }

    // ===================================================================
    // End-to-end through TriggerScheduler::new
    // ===================================================================

    #[tokio::test(start_paused = true)]
    async fn test_scheduler_seeds_watermark_from_store() -> Result<()> {
        let segment = test_segment();
        let store = memory_store(segment.clone());
        store.operations().set_slab_watermark(Some(42)).await?;

        let (_shutdown_tx, shutdown_rx) = watch::channel(ShutdownPhase::default());
        let (_rx, scheduler) =
            TriggerScheduler::new(store, segment, &HeartbeatRegistry::test(), shutdown_rx);

        // Schedule a trigger below the seeded watermark — the past-time
        // path must lower the watermark, proving the seed happened.
        let trigger = make_trigger("seeded", 10, TimerType::Application);
        scheduler.schedule(trigger.clone()).await?;
        assert!(
            scheduler
                .active_triggers()
                .contains(&trigger.key, trigger.time, trigger.timer_type)
                .await,
            "past-time trigger must be in ActiveTriggers"
        );
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn test_deactivate_removes_from_active() -> Result<()> {
        let segment = test_segment();
        let store = memory_store(segment.clone());
        // Seed a high watermark so the trigger lands via the past-time
        // path (deterministic) and skips the load_step race.
        store.operations().set_slab_watermark(Some(100)).await?;

        let (_shutdown_tx, shutdown_rx) = watch::channel(ShutdownPhase::default());
        let (_rx, scheduler) =
            TriggerScheduler::new(store, segment, &HeartbeatRegistry::test(), shutdown_rx);

        let trigger = make_trigger("deactivate", 5, TimerType::Application);
        scheduler.schedule(trigger.clone()).await?;
        assert!(
            scheduler
                .active_triggers()
                .contains(&trigger.key, trigger.time, trigger.timer_type)
                .await
        );

        scheduler
            .deactivate(&trigger.key, trigger.time, trigger.timer_type)
            .await;
        assert!(
            !scheduler
                .active_triggers()
                .contains(&trigger.key, trigger.time, trigger.timer_type)
                .await
        );
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn test_pending_trigger_stays_out_of_memory() -> Result<()> {
        let segment = test_segment();
        let store = memory_store(segment.clone());
        let (_shutdown_tx, shutdown_rx) = watch::channel(ShutdownPhase::default());
        let (_rx, scheduler) = TriggerScheduler::new(
            store.clone(),
            segment.clone(),
            &HeartbeatRegistry::test(),
            shutdown_rx,
        );

        // Choose a slab well past `now_slab_id + preload_window` so the
        // actor classifies it as Pending regardless of whether its first
        // `load_step` has fired yet.
        let far_future = now_slab_id()?.saturating_add(1_000);
        let trigger = make_trigger("far-future", far_future, TimerType::Application);
        scheduler.schedule(trigger.clone()).await?;

        assert!(
            !scheduler
                .active_triggers()
                .contains(&trigger.key, trigger.time, trigger.timer_type)
                .await,
            "far-future trigger must NOT be in ActiveTriggers"
        );

        // Slab metadata must still be persisted so a later load_step finds it.
        let slabs: Vec<SlabId> = store
            .get_slab_range(far_future..=far_future)
            .try_collect()
            .await?;
        assert_eq!(slabs, vec![far_future]);
        Ok(())
    }
}
