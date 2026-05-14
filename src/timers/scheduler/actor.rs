//! Scheduler actor: state, run loop, and the four step handlers.
//!
//! The actor owns slab metadata writes, slab preloading, slab cleanup, and
//! the trigger queue. The public [`TriggerScheduler`] handle in the parent
//! module communicates with it through a command channel; [`run_actor`] is
//! spawned by `TriggerScheduler::new`.
//!
//! [`TriggerScheduler`]: super::TriggerScheduler

use super::{Command, CommandOperation};
use crate::consumer::partition::ShutdownPhase;
use crate::heartbeat::Heartbeat;
use crate::timers::Trigger;
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::timers::queue::TriggerQueue;
use crate::timers::slab::{Slab, SlabId};
use crate::timers::store::{Segment, TriggerStore};
use ahash::HashSet;
use futures::TryStreamExt;
use rand::RngExt;
use std::ops::RangeInclusive;
use std::pin::pin;
use std::time::Duration;
use tokio::select;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, watch};
use tokio::task::coop::cooperative;
use tokio::time::{Instant, sleep_until};
use tracing::{debug, warn};

/// Minimum jittered preload window: never start loading less than this much
/// time before a slab begins.
pub(super) const MIN_PRELOAD: CompactDuration = CompactDuration::new(60);

/// Backoff used when no further load work can currently be scheduled.
const LOAD_IDLE_INTERVAL: Duration = Duration::from_secs(30);

/// Backoff after a Cassandra-side failure inside the actor's load/cleanup
/// arms — these errors are typically transient.
const RETRY_DELAY: Duration = Duration::from_secs(1);

/// Mutable actor state — everything except the trigger queue and the
/// transient `trigger_to_send` slot. The queue is held as a local in
/// [`run_actor`] so handlers can borrow it independently of the rest of
/// the state.
pub(super) struct ActorState<T> {
    pub(super) store: T,
    pub(super) segment: Segment,
    /// Slab metadata rows this actor knows it has written or observed.
    ///
    /// This is only a write-deduplication memo. It is not ownership state:
    /// ownership is the compact numeric range represented by the persisted
    /// watermark and `highest_loaded_slab_id`.
    pub(super) known_slab_ids: HashSet<SlabId>,
    /// Last persisted value of `slab_watermark` for this segment.
    ///
    /// Invariant I1: when `Some(w)`, every clustering row in `timer_segments`
    /// for this segment has `slab_id > w`. `None` means the column was never
    /// written; treated as "scan from slab 0" by the scheduler.
    pub(super) last_persisted_watermark: Option<SlabId>,
    /// Highest slab ID `load_step` has scanned to. Tracks loading progress
    /// so we know where to resume next tick.
    pub(super) highest_loaded_slab_id: Option<SlabId>,
    /// Jittered preload window — re-rolled after each load cycle.
    pub(super) preload_window: CompactDuration,
    /// Wall-clock deadline for the next load tick.
    pub(super) next_load_at: Instant,
}

/// Background task: drives the scheduler's command loop, expired trigger
/// emission, slab loading, and slab cleanup.
pub(super) async fn run_actor<T>(
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
        known_slab_ids: HashSet::default(),
        last_persisted_watermark: None,
        highest_loaded_slab_id: None,
        preload_window,
        next_load_at: now,
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

        let load_deadline = state.next_load_at;

        if trigger_to_send.is_some() {
            select! {
                result = commands.recv() => {
                    let Some(command) = result else { break; };
                    process_command(&mut state, &mut triggers, command).await;
                }
                result = trigger_tx.reserve() => {
                    let Ok(permit) = result else { break; };
                    let Some(trigger) = trigger_to_send.take() else {
                        continue;
                    };
                    permit.send(trigger);
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
        } else {
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
/// Classifies the trigger's slab as **owned** or **pending**:
///
/// - A slab is **owned** when it sits at or below the persisted watermark
///   (past-time) or is at/below `highest_loaded_slab_id`. Owned-slab triggers
///   go into the in-memory queue; their slab metadata is persisted on first
///   sighting. Past-time slabs route through an atomic UNLOGGED BATCH that also
///   lowers the watermark (invariant I6) — the only way the actor can pull a
///   slab back below its own high-water without violating I1.
/// - A **pending** slab sits above the load high-water and has never been
///   touched by the actor. The trigger row (written first by the manager via
///   [`TriggerStore::add_trigger`]) waits in storage; the actor just persists
///   slab metadata so `load_step` can find it later. Keeps the in-memory queue
///   bounded to the preload window.
pub(super) async fn handle_add<T>(
    state: &mut ActorState<T>,
    triggers: &mut TriggerQueue,
    trigger: Trigger,
) -> Result<(), T::Error>
where
    T: TriggerStore,
{
    let slab = Slab::from_time(state.segment.slab_size, trigger.time);
    let slab_id = slab.id();

    let is_registered = state.known_slab_ids.contains(&slab_id);
    // Once the actor commits to owning a slab's triggers in memory, it must
    // keep accepting new triggers for that slab in memory. Otherwise the
    // slab's active set drifts below its DB content and P1 breaks.
    let is_owned = owns_slab(state, slab_id);

    if !is_registered {
        if let Some(old_watermark) = state.last_persisted_watermark
            && slab_id <= old_watermark
        {
            // I6: atomically write the slab row and lower the watermark.
            let new_watermark = slab_id.checked_sub(1);
            state
                .store
                .batch_insert_slab_with_watermark(slab, new_watermark)
                .await?;
            state.last_persisted_watermark = new_watermark;
            // Before lowering, the old watermark proved there were no
            // persisted slab rows <= old_watermark. After atomically inserting
            // this slab and lowering to slab_id - 1, the actor owns exactly the
            // compact range slab_id..=old_watermark; no per-slab scan or
            // materialized loaded set is needed.
            state.highest_loaded_slab_id = Some(
                state
                    .highest_loaded_slab_id
                    .map_or(old_watermark, |h| h.max(old_watermark)),
            );
            state.known_slab_ids.insert(slab_id);
        } else {
            // Plain INSERT for in-window or pending. Pending writes are
            // what let `load_step` discover the slab when it enters the
            // preload window.
            state.store.insert_slab(slab).await?;
            state.known_slab_ids.insert(slab_id);
        }
    }

    if is_owned {
        triggers.insert(trigger).await;
    }

    Ok(())
}

/// Deletes completed persisted slabs below the current slab and then advances
/// the watermark.
///
/// Cleanup is load-driven: a successful `load_step` calls this after moving
/// the load high-water. If a delete fails, in-memory bookkeeping is left
/// untouched and a later load will retry.
pub(super) async fn cleanup_step<T>(state: &mut ActorState<T>, triggers: &TriggerQueue)
where
    T: TriggerStore,
{
    let now_slab_id = match CompactDateTime::now() {
        Ok(now) => Slab::from_time(state.segment.slab_size, now).id(),
        Err(e) => {
            warn!("cleanup_step: failed to compute now: {e:#}");
            return;
        }
    };

    let active_slab_ids = collect_active_slab_ids(state.segment.slab_size, triggers).await;

    loop {
        let candidate = match cleanup_candidate(state, now_slab_id, &active_slab_ids).await {
            Ok(candidate) => candidate,
            Err(e) => {
                warn!("cleanup_step: failed to find cleanup candidate: {e:#}");
                return;
            }
        };

        if let Some(slab_id) = candidate {
            if let Err(e) = state.store.delete_slab(slab_id).await {
                warn!(
                    slab_id,
                    "cleanup_step: failed to delete slab: {e:#}; will retry"
                );
                return;
            }
            state.known_slab_ids.remove(&slab_id);
        } else {
            if let Err(e) = maybe_advance_watermark(state, now_slab_id).await {
                warn!("cleanup_step: failed to advance watermark: {e:#}");
            }
            return;
        }
    }
}

async fn cleanup_candidate<T>(
    state: &ActorState<T>,
    now_slab_id: SlabId,
    active_slab_ids: &HashSet<SlabId>,
) -> Result<Option<SlabId>, T::Error>
where
    T: TriggerStore,
{
    let Some(highest_loaded) = state.highest_loaded_slab_id else {
        return Ok(None);
    };
    let Some(end) = now_slab_id.checked_sub(1).map(|s| s.min(highest_loaded)) else {
        return Ok(None);
    };
    let start = state
        .last_persisted_watermark
        .map_or(0, |w| w.saturating_add(1));
    if start > end {
        return Ok(None);
    }

    let mut slabs = pin!(state.store.get_slab_range(start..=end));
    while let Some(slab_id) = cooperative(slabs.try_next()).await? {
        if !active_slab_ids.contains(&slab_id) {
            return Ok(Some(slab_id));
        }
    }
    Ok(None)
}

/// Raises `slab_watermark` to one below the lowest slab still represented
/// in the store, when the new value would be a meaningful increase over
/// what we already persisted.
///
/// When the store has no slabs at all, fall back to `now_slab_id - 1` —
/// genuinely safe because nothing's persisted to step on.
pub(super) async fn maybe_advance_watermark<T>(
    state: &mut ActorState<T>,
    now_slab_id: SlabId,
) -> Result<(), T::Error>
where
    T: TriggerStore,
{
    let lowest_in_store = pin!(state.store.get_slab_range(0..=SlabId::MAX))
        .try_next()
        .await?;

    let candidate = match lowest_in_store {
        Some(first_slab) => first_slab.checked_sub(1),
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

/// Load tick: record and drain every persisted slab from the current load
/// high-water up to the slab containing `now + preload_window`, then push the
/// high-water forward.
///
/// The store fans the per-slab scans out concurrently via
/// [`TriggerStore::get_slab_triggers_in_range`], so startup catch-up runs at
/// up to `LOAD_CONCURRENCY` slabs in flight.
pub(super) async fn load_step<T>(state: &mut ActorState<T>, triggers: &mut TriggerQueue)
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
        state.next_load_at = Instant::now() + LOAD_IDLE_INTERVAL;
        return;
    };

    if start_slab_id > target_slab_id {
        // High-water already past the preload horizon. Schedule the next
        // tick for when the following slab enters the window.
        schedule_next_load(state, &target_slab);
        return;
    }

    let slab_range = start_slab_id..=target_slab_id;
    let load_result = drain_slab_range(&state.store, slab_range.clone(), triggers).await;

    match load_result {
        Ok((registered, count)) => {
            state.known_slab_ids.extend(registered);
            state.highest_loaded_slab_id = Some(target_slab_id);
            debug!(
                start_slab_id,
                target_slab_id,
                triggers = count,
                "load_step: loaded range"
            );
            schedule_next_load(state, &target_slab);
            cleanup_step(state, triggers).await;
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

/// Drains a [`TriggerStore::get_slab_triggers_in_range`] stream into the
/// trigger queue. Isolated as a helper so the caller can let the stream's
/// borrow on `store` end before mutating other actor state.
async fn drain_slab_range<T>(
    store: &T,
    range: RangeInclusive<SlabId>,
    triggers: &mut TriggerQueue,
) -> Result<(HashSet<SlabId>, usize), T::Error>
where
    T: TriggerStore,
{
    let mut stream = pin!(store.get_slab_triggers_in_range(range));
    let mut registered = HashSet::default();
    let mut count = 0_usize;
    while let Some(trigger) = cooperative(stream.try_next()).await? {
        registered.insert(Slab::from_time(store.slab_size(), trigger.time).id());
        triggers.insert(trigger).await;
        count = count.saturating_add(1);
    }
    Ok((registered, count))
}

pub(super) fn owns_slab<T>(state: &ActorState<T>, slab_id: SlabId) -> bool {
    state.last_persisted_watermark.is_some_and(|w| slab_id <= w)
        || state.highest_loaded_slab_id.is_some_and(|h| slab_id <= h)
}

/// Returns the next slab id the load loop should scan from, or `None` when
/// the slab-id space is exhausted.
pub(super) fn next_unloaded_slab_id<T>(state: &ActorState<T>) -> Option<SlabId> {
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
        state.next_load_at = Instant::now() + LOAD_IDLE_INTERVAL;
        return;
    };
    let wait = calculate_wait_time(next_slab.range().start, state.preload_window);
    state.next_load_at = Instant::now() + wait.into();
    state.preload_window = calculate_preload(state.segment.slab_size);
}

/// Builds the set of slab IDs currently holding active triggers — i.e. the
/// slabs cleanup must NOT delete.
pub(super) async fn collect_active_slab_ids(
    slab_size: CompactDuration,
    triggers: &TriggerQueue,
) -> HashSet<SlabId> {
    let mut active = HashSet::default();
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
pub(super) fn calculate_wait_time(
    load_time: CompactDateTime,
    preload_window: CompactDuration,
) -> CompactDuration {
    load_time
        .compact_duration_from_now()
        .unwrap_or(CompactDuration::MIN)
        .saturating_sub(preload_window)
}

/// Generates a jittered preload window between [`MIN_PRELOAD`, `slab_size`].
pub(super) fn calculate_preload(slab_size: CompactDuration) -> CompactDuration {
    let max_jitter = slab_size.saturating_sub(MIN_PRELOAD);
    CompactDuration::from(rand::rng().random_range(0..=max_jitter.seconds()))
        .saturating_add(MIN_PRELOAD)
}
