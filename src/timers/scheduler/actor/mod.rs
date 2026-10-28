//! Scheduler actor: state, run loop, and command handlers.
//!
//! The actor owns slab metadata writes, slab preloading, slab cleanup, and
//! the trigger queue. The public [`TriggerScheduler`] handle in the parent
//! module communicates with it through a command channel; [`run_actor`] is
//! spawned by `TriggerScheduler::new`. The [`load`] module holds the preload
//! and cleanup steps.
//!
//! [`TriggerScheduler`]: super::TriggerScheduler

use super::{Command, CommandOperation};
use crate::consumer::partition::ShutdownPhase;
use crate::heartbeat::Heartbeat;
use crate::timers::Trigger;
use crate::timers::active::{Queued, Row};
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::timers::queue::TriggerQueue;
use crate::timers::slab::{Slab, SlabId};
use crate::timers::store::{Segment, StoredSegment, TriggerStore};
use std::collections::BTreeSet;
use tokio::select;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, watch};
use tokio::time::{Instant, sleep_until};
use tracing::debug;

use load::{calculate_preload, load_step};

pub(super) mod load;

/// Mutable actor state — everything except the trigger queue and the
/// transient `trigger_to_send` slot. The queue is held as a local in
/// [`run_actor`] so handlers can borrow it independently of the rest of
/// the state.
pub(super) struct ActorState<T> {
    pub(super) store: T,
    pub(super) segment: Segment,
    /// Slab metadata rows this actor has written or observed in the loaded
    /// prefix.
    ///
    /// Within `..=highest_loaded_slab_id`, this set is the actor's source of
    /// truth for which persisted slab rows still need cleanup. The load loop
    /// is the only code path that discovers preexisting rows from storage.
    pub(super) known_slab_ids: BTreeSet<SlabId>,
    /// Last persisted value of `slab_watermark` for this segment. It keeps
    /// invariant I1 of [`StoredSegment`]. `None` makes the scheduler scan
    /// from slab 0.
    pub(super) last_persisted_watermark: Option<SlabId>,
    /// Highest slab ID `load_step` has scanned to. Tracks loading progress
    /// so we know where to resume next tick.
    pub(super) highest_loaded_slab_id: Option<SlabId>,
    /// Jittered preload window — re-rolled after each load cycle.
    pub(super) preload_window: CompactDuration,
    /// Wall-clock deadline for the next load tick.
    pub(super) next_load_at: Instant,
}

impl<T> ActorState<T> {
    /// Builds the state of an actor that starts at `now`.
    ///
    /// It keeps the stored watermark only if it is below the slab of `now`.
    /// An older version did not convert the watermark on a slab-size change,
    /// so a higher one is stale.
    pub(super) fn start(store: T, segment: StoredSegment, now: CompactDateTime) -> Self {
        let (segment, watermark) = segment.into_parts();
        let current_slab_id = Slab::from_time(segment.slab_size, now).id();

        Self {
            store,
            known_slab_ids: BTreeSet::new(),
            last_persisted_watermark: watermark.filter(|&watermark| watermark < current_slab_id),
            highest_loaded_slab_id: None,
            preload_window: calculate_preload(segment.slab_size),
            next_load_at: Instant::now(),
            segment,
        }
    }
}

/// Background task: drives the scheduler's command loop, expired trigger
/// emission, slab loading, and slab cleanup.
pub(super) async fn run_actor<T>(
    store: T,
    segment: StoredSegment,
    mut triggers: TriggerQueue,
    mut commands: mpsc::UnboundedReceiver<Command<T::Error>>,
    trigger_tx: mpsc::Sender<Trigger>,
    heartbeat: Heartbeat,
    mut shutdown_rx: watch::Receiver<ShutdownPhase>,
) where
    T: TriggerStore,
{
    // A clock error discards the watermark: no slab is below slab 0.
    let now = CompactDateTime::now().unwrap_or(CompactDateTime::MIN);
    let mut state = ActorState::start(store, segment, now);

    let mut trigger_to_send: Option<Trigger> = None;

    loop {
        heartbeat.beat();

        // Serve commands through Draining. Handler contexts permit timer
        // operations until Cancelling. Exit when Cancelling is visible. This
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
pub(super) async fn process_command<T>(
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
        CommandOperation::Hold => handle_hold(state, triggers, trigger).await,
        CommandOperation::Release => {
            release(triggers, &trigger).await;
            Ok(())
        }
        CommandOperation::Add(queued, row) => {
            handle_add(state, triggers, trigger, queued, row).await;
            Ok(())
        }
        CommandOperation::Remove => {
            triggers.remove(&trigger).await;
            Ok(())
        }
        CommandOperation::RetireCommitted => {
            let tag = triggers
                .active_triggers()
                .get(&trigger.key, trigger.time, trigger.timer_type)
                .await
                .map(|entry| entry.tag);
            if tag.is_none_or(|tag| tag == trigger.tag) {
                triggers.remove(&trigger).await;
            }
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

/// Handler for [`CommandOperation::Hold`].
///
/// Registers the trigger's slab and holds the trigger before the manager
/// writes its row. This keeps the durable row invariant of
/// [`crate::timers::active`] if the write is dropped or fails.
///
/// A slab at or below the persisted watermark routes through an atomic
/// UNLOGGED BATCH that also lowers the watermark (invariant I6). This is the
/// only way the actor can pull a slab back below its own high-water without
/// violating I1.
pub(super) async fn handle_hold<T>(
    state: &mut ActorState<T>,
    triggers: &TriggerQueue,
    trigger: Trigger,
) -> Result<(), T::Error>
where
    T: TriggerStore,
{
    let slab = Slab::from_time(state.segment.slab_size, trigger.time);
    let slab_id = slab.id();

    if !state.known_slab_ids.contains(&slab_id) {
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
        } else {
            // Plain INSERT for in-window or pending. Pending writes are
            // what let `load_step` discover the slab when it enters the
            // preload window.
            state.store.insert_slab(slab).await?;
        }
        state.known_slab_ids.insert(slab_id);
    }

    triggers.active_triggers().hold(trigger).await;
    Ok(())
}

/// Handler for [`CommandOperation::Add`].
///
/// Queues the trigger in the `queued` state with its `row` certainty if the
/// actor keeps the coordinate in memory: it owns the slab, or a partial drain
/// or a delivery holds the coordinate. The slab's active set then never drifts
/// below its DB content (P1). Otherwise the actor releases the hold, and
/// `load_step` reads the row later. This keeps the in-memory queue bounded to
/// the preload window.
pub(super) async fn handle_add<T>(
    state: &ActorState<T>,
    triggers: &mut TriggerQueue,
    trigger: Trigger,
    queued: Queued,
    row: Row,
) {
    let entry = triggers
        .active_triggers()
        .get(&trigger.key, trigger.time, trigger.timer_type)
        .await;
    let slab_id = Slab::from_time(state.segment.slab_size, trigger.time).id();
    if !owns_slab(state, slab_id) && entry.is_none_or(|entry| entry.holds(&trigger)) {
        triggers.remove(&trigger).await;
        return;
    }

    // The key row supplies the tag, even when a partial drain has not advanced
    // ownership.
    if entry.is_some_and(|entry| entry.tag != trigger.tag) {
        triggers.remove(&trigger).await;
    }
    let (key, time, timer_type) = (trigger.key.clone(), trigger.time, trigger.timer_type);
    triggers.insert(trigger).await;
    triggers
        .active_triggers()
        .queue(&key, time, timer_type, queued, row)
        .await;
}

/// Handler for [`CommandOperation::Release`]: removes the trigger's own hold.
pub(super) async fn release(triggers: &TriggerQueue, trigger: &Trigger) {
    let active = triggers.active_triggers();
    let (key, time, timer_type) = (&trigger.key, trigger.time, trigger.timer_type);
    if active
        .get(key, time, timer_type)
        .await
        .is_some_and(|entry| entry.holds(trigger))
    {
        active.remove(key, time, timer_type).await;
    }
}

pub(super) fn owns_slab<T>(state: &ActorState<T>, slab_id: SlabId) -> bool {
    state.last_persisted_watermark.is_some_and(|w| slab_id <= w)
        || state.highest_loaded_slab_id.is_some_and(|h| slab_id <= h)
}
