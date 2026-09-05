//! Scheduler actor: state, run loop, and the four step handlers.
//!
//! The actor owns slab metadata writes, slab preloading, slab cleanup, and
//! the trigger queue. The public [`TriggerScheduler`] handle in the parent
//! module communicates with it through a command channel; [`run_actor`] is
//! spawned by `TriggerScheduler::new`.
//!
//! [`TriggerScheduler`]: super::TriggerScheduler

use super::BUFFER_SIZE;
use super::{Command, CommandOperation};
use crate::consumer::partition::ShutdownPhase;
use crate::heartbeat::Heartbeat;
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::timers::queue::TriggerQueue;
use crate::timers::slab::{Slab, SlabId};
use crate::timers::store::{Segment, TriggerStore};
use crate::timers::{DELETE_CONCURRENCY, Trigger};
use futures::{StreamExt, TryStreamExt, stream};
use rand::RngExt;
use std::collections::BTreeSet;
use std::ops::RangeInclusive;
use std::pin::pin;
use std::time::Duration;
use tokio::select;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::coop::cooperative;
use tokio::time::{Instant, sleep_until};
use tracing::{debug, warn};

/// Minimum jittered preload window: never start loading less than this much
/// time before a slab begins.
pub(super) const MIN_PRELOAD: CompactDuration = CompactDuration::new(60);

/// Backoff used when no further load work can currently be scheduled.
const LOAD_IDLE_INTERVAL: Duration = Duration::from_secs(30);

/// Maximum concurrent per-slab trigger scans during a load tick.
const LOAD_CONCURRENCY: usize = 16;

/// Backoff after a Cassandra-side failure inside the actor's load/cleanup
/// arms — these errors are typically transient.
const RETRY_DELAY: Duration = Duration::from_secs(1);

/// Mutable actor state — everything except the trigger queue and the
/// transient `trigger_to_send` slot. The queue is held as a local in
/// [`run_actor`] so handlers can borrow it independently of the rest of
/// the state.
pub(super) struct ActorState<T> {
    pub(super) store: T,
    /// The trigger receiver and its hand-off. The first successful load sends
    /// the receiver to the partition.
    pub(super) ready: Option<(
        oneshot::Sender<mpsc::Receiver<Trigger>>,
        mpsc::Receiver<Trigger>,
    )>,
    pub(super) segment: Segment,
    /// Slab metadata rows this actor has written or observed in the loaded
    /// prefix.
    ///
    /// Within `..=highest_loaded_slab_id`, this set is the actor's source of
    /// truth for which persisted slab rows still need cleanup. The load loop
    /// is the only code path that discovers preexisting rows from storage.
    pub(super) known_slab_ids: BTreeSet<SlabId>,
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
    ready: oneshot::Sender<mpsc::Receiver<Trigger>>,
    heartbeat: Heartbeat,
    mut shutdown_rx: watch::Receiver<ShutdownPhase>,
) where
    T: TriggerStore,
{
    let preload_window = calculate_preload(segment.slab_size);
    let now = Instant::now();
    let (trigger_tx, receiver) = mpsc::channel(BUFFER_SIZE);
    let mut state: ActorState<T> = ActorState {
        store,
        ready: Some((ready, receiver)),
        segment,
        known_slab_ids: BTreeSet::new(),
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

        // Serve commands through `Draining` — in-flight handlers still arm
        // recovery backstops as they settle, and their contexts permit timer
        // ops until `Cancelling`. Exit only once `Cancelling` is visible. This
        // gate owns the exit decision; the `changed()` arms below merely wake
        // a parked actor and route back here. It also bounds the transition
        // race: `select!` is unbiased, so with commands continuously ready the
        // wake arm only eventually wins — this loop-boundary check caps that at
        // one extra command served after `Cancelling`.
        if *shutdown_rx.borrow() >= ShutdownPhase::Cancelling {
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
                // Wake a parked actor on any phase change: an `Ok` falls
                // through to the loop-top gate, which exits iff the phase is
                // now `>= Cancelling` (and re-parks otherwise, without
                // spinning — `changed()` consumes the notification). An `Err`
                // means every sender dropped below `Cancelling`, so exit.
                // (`changed()` yields `Result<(), _>`, not a `!Send` `Ref`
                // like `wait_for`, keeping this spawned future `Send`.)
                result = shutdown_rx.changed() => {
                    if result.is_err() {
                        debug!("Scheduler actor shutting down");
                        return;
                    }
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
                // Wake-and-route-to-the-gate; see the trigger_to_send branch.
                result = shutdown_rx.changed() => {
                    if result.is_err() {
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
    command: Command<T::Error>,
) where
    T: TriggerStore,
{
    let (operation, trigger, result_tx) = match command {
        Command::Apply {
            operation,
            trigger,
            result_tx,
        } => (operation, trigger, result_tx),
        Command::Remove {
            trigger,
            key_tag,
            result_tx,
        } => {
            let outcome = triggers.remove_if_live(&trigger, key_tag).await;
            let _ = result_tx.send(outcome);
            return;
        }
        Command::Retag(trigger) => {
            triggers.retag(&trigger);
            return;
        }
    };
    let result: Result<Option<Trigger>, T::Error> = match operation {
        CommandOperation::Add => handle_add(state, triggers, trigger).await.map(|()| None),
        CommandOperation::AddToQueue => {
            triggers.insert_queue_only(trigger);
            Ok(None)
        }
        CommandOperation::RemoveFromQueue => Ok(triggers.remove_queue_only(&trigger)),
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
    let completed_slabs = completed_slab_ids(state, now_slab_id, &active_slab_ids);

    match delete_completed_slabs(&state.store, &completed_slabs).await {
        Ok(()) => {
            for slab_id in completed_slabs {
                state.known_slab_ids.remove(&slab_id);
            }
        }
        Err(e) => {
            warn!("cleanup_step: failed to delete slab: {e:#}; will retry");
            return;
        }
    }

    if let Err(e) = maybe_advance_watermark(state, now_slab_id, &active_slab_ids).await {
        warn!("cleanup_step: failed to advance watermark: {e:#}");
    }
}

fn cleanable_slab_end<T>(state: &ActorState<T>, now_slab_id: SlabId) -> Option<SlabId> {
    Some(
        now_slab_id
            .checked_sub(1)?
            .min(state.highest_loaded_slab_id?),
    )
}

fn completed_slab_ids<T>(
    state: &ActorState<T>,
    now_slab_id: SlabId,
    active_slab_ids: &BTreeSet<SlabId>,
) -> Vec<SlabId> {
    let Some(end) = cleanable_slab_end(state, now_slab_id) else {
        return Vec::new();
    };

    state
        .known_slab_ids
        .range(..=end)
        .filter(|slab_id| !active_slab_ids.contains(slab_id))
        .copied()
        .collect()
}

async fn delete_completed_slabs<T>(store: &T, slab_ids: &[SlabId]) -> Result<(), T::Error>
where
    T: TriggerStore,
{
    stream::iter(slab_ids.iter().copied())
        .map(|slab_id| async move {
            store.delete_slab(slab_id).await?;
            Ok(())
        })
        .buffer_unordered(DELETE_CONCURRENCY)
        .try_collect::<()>()
        .await
}

/// Raises `slab_watermark` using only actor-owned state.
///
/// After [`cleanup_step`] has found no more deletable slabs, every persisted
/// slab in the cleanable owned prefix is active. The actor can therefore
/// advance to one below the lowest active slab in that prefix, or to the end
/// of the prefix when it is empty, without asking storage to rediscover the
/// partition's low end.
pub(super) async fn maybe_advance_watermark<T>(
    state: &mut ActorState<T>,
    now_slab_id: SlabId,
    active_slab_ids: &BTreeSet<SlabId>,
) -> Result<(), T::Error>
where
    T: TriggerStore,
{
    let Some(end) = cleanable_slab_end(state, now_slab_id) else {
        return Ok(());
    };

    let lowest_active = active_slab_ids.range(..=end).next().copied();

    let Some(candidate) = lowest_active.map_or(Some(end), |slab_id| slab_id.checked_sub(1)) else {
        return Ok(());
    };

    if state
        .last_persisted_watermark
        .is_some_and(|current| candidate <= current)
    {
        return Ok(());
    }

    state.store.set_slab_watermark(Some(candidate)).await?;
    state.last_persisted_watermark = Some(candidate);
    debug!(watermark = candidate, "advanced slab_watermark");
    Ok(())
}

/// Load tick: read slab metadata from the current load high-water up to the
/// slab containing `now + preload_window`, drain each registered slab's
/// triggers, then push the high-water forward.
///
/// This is the actor's only slab-table read path after startup. Cleanup and
/// watermark advancement consume the slab IDs learned here rather than
/// re-scanning metadata.
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

    let load_result = drain_slab_range(
        &state.store,
        start_slab_id..=target_slab_id,
        triggers,
        &mut state.known_slab_ids,
    )
    .await;

    match load_result {
        Ok(count) => {
            if let Some((sender, receiver)) = state.ready.take() {
                let _ = sender.send(receiver);
            }
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

/// Drains registered slabs and their triggers into the trigger queue.
///
/// The slab metadata scan happens here, in the load loop. Empty slab rows are
/// still recorded in `known_slab_ids` so cleanup can delete them later without
/// reading the slab table again.
async fn drain_slab_range<T>(
    store: &T,
    range: RangeInclusive<SlabId>,
    triggers: &mut TriggerQueue,
    known_slab_ids: &mut BTreeSet<SlabId>,
) -> Result<usize, T::Error>
where
    T: TriggerStore,
{
    let store_for_tasks = store.clone();
    let mut stream = pin!(
        store
            .get_slab_range(range)
            .map_ok(move |slab_id| {
                let store = store_for_tasks.clone();
                async move {
                    let triggers = pin!(store.get_slab_triggers_all_types(slab_id));
                    let slab_triggers = cooperative(triggers.try_collect::<Vec<_>>()).await?;
                    Ok::<_, T::Error>((slab_id, slab_triggers))
                }
            })
            .try_buffer_unordered(LOAD_CONCURRENCY)
    );

    let mut count = 0_usize;
    while let Some((slab_id, slab_triggers)) = cooperative(stream.try_next()).await? {
        known_slab_ids.insert(slab_id);
        for trigger in slab_triggers {
            triggers.insert(trigger).await;
            count = count.saturating_add(1);
        }
    }
    Ok(count)
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
