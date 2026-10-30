//! Scheduler actor: state, run loop, and command handlers.
//!
//! The public [`TriggerScheduler`] handle in the parent module sends commands
//! to the actor; [`run_actor`] is spawned by `TriggerScheduler::new`. The
//! [`load`] module holds the preload and cleanup steps.
//!
//! [`TriggerScheduler`]: super::TriggerScheduler

use super::{Command, Start, Ticket};
use crate::Key;
use crate::consumer::partition::ShutdownPhase;
use crate::heartbeat::Heartbeat;
use crate::timers::active::{Change, Outcome, Timers};
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::timers::slab::{Slab, SlabId};
use crate::timers::store::{Segment, StoredSegment, TriggerStore};
use crate::timers::{TimerType, Trigger};
use ahash::HashMap;
use std::collections::hash_map::Entry;
use std::collections::{BTreeSet, VecDeque};
use tokio::select;
use tokio::sync::mpsc::WeakUnboundedSender;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, watch};
use tokio::time::{Instant, sleep_until};
use tracing::debug;

use load::{calculate_preload, load_step};

pub(super) mod load;

/// The starts that wait for the operation that runs on their key and timer
/// type. A lane exists while an operation on it runs. The end of its last
/// operation removes it.
pub(super) type Lanes<E> = HashMap<(Key, TimerType), VecDeque<Start<E>>>;

/// Slab state of the actor. [`run_actor`] holds the [`Timers`] apart, so
/// handlers can borrow both.
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
    tickets: WeakUnboundedSender<Command<T::Error>>,
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
    let mut timers = Timers::default();
    let mut lanes = Lanes::default();

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

        // A trigger that the full channel refused waits for space before the
        // queue releases the next one.
        let waiting = trigger_to_send.is_some();
        select! {
            result = commands.recv() => {
                let Some(command) = result else { break; };
                process_command(&mut state, &mut timers, &mut lanes, &tickets, command).await;
            }
            result = trigger_tx.reserve(), if waiting => {
                let Ok(permit) = result else { break; };
                if let Some(trigger) = trigger_to_send.take() {
                    permit.send(trigger);
                }
            }
            Some(trigger) = timers.next(), if !waiting => {
                if let Err(err) = trigger_tx.try_send(trigger) {
                    match err {
                        TrySendError::Full(t) => trigger_to_send = Some(t),
                        TrySendError::Closed(_) => break,
                    }
                }
            }
            () = sleep_until(load_deadline) => {
                load_step(&mut state, &mut timers).await;
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
    }
}

/// Processes a single command from the command channel.
pub(super) async fn process_command<T>(
    state: &mut ActorState<T>,
    timers: &mut Timers,
    lanes: &mut Lanes<T::Error>,
    tickets: &WeakUnboundedSender<Command<T::Error>>,
    command: Command<T::Error>,
) where
    T: TriggerStore,
{
    match command {
        Command::Start(start) => match lanes.entry(start.lane()) {
            Entry::Occupied(mut waiting) => waiting.get_mut().push_back(start),
            Entry::Vacant(lane) => {
                if begin(state, timers, tickets, start).await {
                    lane.insert(VecDeque::new());
                }
            }
        },
        Command::End(change, outcome) => {
            let lane = change.lane();
            end(state, timers, change, outcome);
            let Entry::Occupied(mut waiting) = lanes.entry(lane) else {
                return;
            };
            while let Some(start) = waiting.get_mut().pop_front() {
                if begin(state, timers, tickets, start).await {
                    return;
                }
            }
            waiting.remove();
        }
        Command::Abort { id, tag } => timers.abort(&id, tag),
        Command::Fire { id, tag, reply } => {
            let _ = reply.send(timers.fire(&id, tag));
        }
        Command::List {
            key,
            timer_type,
            reply,
        } => {
            let _ = reply.send(timers.list(&key, timer_type));
        }
        Command::Snapshot { now, reply } => {
            let _ = reply.send(timers.snapshot(now));
        }
    }
}

/// Starts an operation. Returns whether it runs: its caller holds its ticket.
async fn begin<T>(
    state: &mut ActorState<T>,
    timers: &mut Timers,
    tickets: &WeakUnboundedSender<Command<T::Error>>,
    start: Start<T::Error>,
) -> bool
where
    T: TriggerStore,
{
    let mut runs = false;
    let mut ticket = |change| {
        runs = true;
        Ticket::new(change, tickets.clone())
    };
    // A caller that left before its reply never ran the row change.
    let returned = match start {
        Start::Insert {
            trigger,
            insert,
            reply,
        } => {
            let started = register_slab(state, trigger.time)
                .await
                .map(|()| timers.start_insert(trigger, insert))
                .map(|(trigger, change)| (trigger, ticket(change)));
            match reply.send(started) {
                Err(Ok((_, ticket))) => Some(ticket),
                _ => None,
            }
        }
        Start::Unschedule { id, reply } => {
            let change = timers.start_unschedule(id.key, id.time, id.timer_type);
            reply.send(change.map(ticket)).err().flatten()
        }
        Start::UnscheduleAll {
            key,
            timer_type,
            reply,
        } => {
            let (change, firing) = timers.start_unschedule_all(key, timer_type);
            reply.send((ticket(change), firing)).err().map(|(t, _)| t)
        }
        Start::Complete { id, reply } => {
            let change = timers.start_complete(id.key, id.time, id.timer_type);
            reply.send(change.map(ticket)).err().flatten()
        }
    };
    if let Some(change) = returned.and_then(Ticket::into_change) {
        end(state, timers, change, Outcome::Unchanged);
        return false;
    }
    runs
}

/// Ends an operation in `timers`.
fn end<T>(state: &ActorState<T>, timers: &mut Timers, change: Change, outcome: Outcome) {
    let slab_size = state.segment.slab_size;
    timers.end(change, outcome, |time| {
        owns_slab(state, Slab::from_time(slab_size, time).id())
    });
}

/// Registers the slab that holds `time` before an insert writes its row.
///
/// A slab at or below the persisted watermark routes through an atomic
/// UNLOGGED BATCH that also lowers the watermark (invariant I6). This is the
/// only way the actor can pull a slab back below its own high-water without
/// violating I1.
pub(super) async fn register_slab<T>(
    state: &mut ActorState<T>,
    time: CompactDateTime,
) -> Result<(), T::Error>
where
    T: TriggerStore,
{
    let slab = Slab::from_time(state.segment.slab_size, time);
    let slab_id = slab.id();
    if state.known_slab_ids.contains(&slab_id) {
        return Ok(());
    }

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
        // Before lowering, the old watermark proved there were no persisted
        // slab rows <= old_watermark. After atomically inserting this slab and
        // lowering to slab_id - 1, the actor owns exactly the compact range
        // slab_id..=old_watermark; no per-slab scan or materialized loaded set
        // is needed.
        state.highest_loaded_slab_id = Some(
            state
                .highest_loaded_slab_id
                .map_or(old_watermark, |h| h.max(old_watermark)),
        );
    } else {
        // Plain INSERT for in-window or pending. Pending writes are what let
        // `load_step` discover the slab when it enters the preload window.
        state.store.insert_slab(slab).await?;
    }
    state.known_slab_ids.insert(slab_id);
    Ok(())
}

pub(super) fn owns_slab<T>(state: &ActorState<T>, slab_id: SlabId) -> bool {
    state.last_persisted_watermark.is_some_and(|w| slab_id <= w)
        || state.highest_loaded_slab_id.is_some_and(|h| slab_id <= h)
}
