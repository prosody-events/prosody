//! Active trigger registry with explicit timer lifecycle states.
//!
//! Provides [`ActiveTriggers`], a thread-safe registry tracking timer
//! triggers currently loaded in the scheduler. Each trigger is identified
//! by a [`Key`], scheduled [`CompactDateTime`], and [`TimerType`].
//!
//! # Timer State Machine
//!
//! Each timer is in exactly one [`TimerState`], or absent (unscheduled).
//! Every lifecycle operation is resolved by [`transition`] — the single
//! authority for the state machine — which maps `(prior state, op)` to the
//! exact registry, queue, and store effects the manager applies.
//!
//! # State Definitions
//!
//! | State                | `ActiveTriggers`    | `DelayQueue` | `Database` |
//! |----------------------|---------------------|--------------|------------|
//! | `UNSCHEDULED`        | absent              | -            | -          |
//! | `SCHEDULED`          | `Scheduled`         | ✓            | ✓          |
//! | `UNCONFIRMED`        | `Unconfirmed`       | ✓            | maybe      |
//! | `FIRING`             | `Firing`            | -            | ✓          |
//! | `FIRING_RESCHEDULED` | `FiringRescheduled` | ✓            | ✓          |
//! | `ABORTED`            | `Aborted`           | -            | maybe      |
//!
//! # Durable Row Invariant
//!
//! A written row is in a registered slab. In an owned slab, it also has a
//! registry entry, so cleanup keeps the slab and a restart loads the row.
//! [`MemoryEffect::Insert`] keeps this invariant when a schedule is dropped
//! or its write fails. Removals run before the row delete, so a dropped
//! removal can leave a cancelled row that never loads.

use crate::Key;
use crate::timers::datetime::CompactDateTime;
use crate::timers::{TimerType, Trigger};
use ahash::HashMap;
use scc::hash_map::Entry;
use std::sync::Arc;

/// The lifecycle state and immutable identity of one queued attempt.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct ActiveTriggerEntry {
    /// Lifecycle state of the timer.
    pub(crate) state: TimerState,
    /// Random 32-bit identity; `0` for legacy/inline timers without a stored
    /// tag.
    pub(crate) tag: i32,
}

impl ActiveTriggerEntry {
    /// Whether this entry is the hold of `trigger`'s schedule.
    pub(crate) fn holds(self, trigger: &Trigger) -> bool {
        self.state == TimerState::Aborted && self.tag == trigger.tag
    }
}

/// Point-in-time counts of active timers for metrics reporting.
#[derive(Clone, Copy, Debug, Default)]
pub struct TimerSnapshot {
    /// Timers in any state in the loaded scheduling window.
    pub active: u32,
    /// Timers in `Firing` or `FiringRescheduled` state.
    pub in_flight: u32,
    /// Timers whose `fire_time <= now` (any state).
    pub overdue: u32,
    /// Age in seconds of the oldest overdue timer; 0 when none.
    pub oldest_overdue_secs: u32,
}

/// Maps (time, type) tuples to their lifecycle entry for a single key.
type TriggerStateMap = HashMap<(CompactDateTime, TimerType), ActiveTriggerEntry>;

/// Lifecycle state of a timer in the in-memory scheduler.
///
/// Each timer in [`ActiveTriggers`] is in exactly one of these states; how
/// every operation moves a timer between them is resolved by [`transition`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum TimerState {
    /// Timer is in the `DelayQueue`, waiting to fire.
    ///
    /// This is the default state for newly scheduled timers.
    #[default]
    Scheduled,

    /// Handler is processing this timer; it has not been rescheduled.
    Firing,

    /// Handler is processing this timer; the same (key, time, type) was
    /// re-scheduled during dispatch and will fire again after commit.
    FiringRescheduled,

    /// Delivery was aborted. The row stays for recovery, but the timer is not
    /// in the `DelayQueue`. A schedule also holds its timer in this state
    /// while it writes the row.
    Aborted,

    /// The timer is in the `DelayQueue`, but its row write did not confirm.
    /// It fires once, and the next schedule writes the row again.
    Unconfirmed,
}

/// A lifecycle operation submitted to the timer state machine.
///
/// Ops name caller intent; [`transition`] resolves `(prior state, op)` to
/// the effects to apply. Firing is not an op: delivering a timer performs
/// its own `Scheduled` → `Firing` flip inside the scheduler after the
/// `DelayQueue` pop.
#[derive(Clone, Copy, Debug)]
pub(crate) enum TimerOp {
    /// Request a future fire at this (key, time, type).
    Schedule,
    /// Cancel a pending fire.
    Unschedule,
    /// The delivery handler committed.
    Complete,
    /// The delivery handler abandoned the attempt.
    Abort,
    /// The new timer of a `clear_and_schedule`. Rows for this op and
    /// [`ClearReplaced`](Self::ClearReplaced) carry [`StoreEffect::None`]:
    /// the caller owns the single atomic `clear_and_schedule` store write,
    /// which subsumes every per-row insert and delete.
    ClearSchedule,
    /// A replaced (old-time) timer of a `clear_and_schedule`. As
    /// [`Unschedule`](Self::Unschedule), except the atomic store write
    /// subsumes the row delete and the caller emits telemetry wholesale.
    ClearReplaced,
}

/// The scheduler effect of a [`MemoryEffect::Update`].
///
/// `Dequeue` changes only the `DelayQueue`. `Remove` and `Deactivate` also
/// delete the [`ActiveTriggers`] entry.
#[derive(Clone, Copy, Debug)]
pub(crate) enum QueueEffect {
    /// No queue change.
    None,
    /// Queue-only removal; the registry entry stays.
    Dequeue,
    /// Full scheduler `Remove`: queue removal plus registry removal.
    Remove,
    /// Registry-only removal; the queue entry was already popped by firing.
    Deactivate,
}

/// Durable-store effect of a [`Transition`].
#[derive(Clone, Copy, Debug)]
pub(crate) enum StoreEffect {
    /// No store write.
    None,
    /// Insert the timer row (`add_trigger`).
    Insert,
    /// Delete the timer row (`remove_trigger`).
    Delete,
}

/// Telemetry event a [`Transition`] emits once fully applied.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Announce {
    /// Emit a `timer_scheduled` lifecycle event.
    Scheduled,
    /// Emit a `timer_cancelled` lifecycle event.
    Cancelled,
}

/// The in-memory effect of a [`Transition`].
#[derive(Clone, Copy, Debug)]
pub(crate) enum MemoryEffect {
    /// Runs before the durable write: the queue effect, then the new state.
    Update {
        /// New registry state; `None` leaves the entry untouched.
        next_state: Option<TimerState>,
        /// Scheduler effect to run before the state change.
        queue: QueueEffect,
    },
    /// Queues the timer in this state around the durable write, through
    /// `TriggerScheduler::insert`.
    Insert(TimerState),
}

/// One resolved step of the timer state machine.
///
/// Only [`transition`] constructs values of this type, so the table's
/// exhaustive `(op, prior state)` match is the only place a transition can
/// be defined — an illegal transition is unwritable outside it.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Transition {
    /// In-memory effect.
    memory: MemoryEffect,
    /// Durable-store effect.
    store: StoreEffect,
    /// Telemetry event emitted once the transition is fully applied.
    announce: Option<Announce>,
}

impl Transition {
    /// The in-memory effect.
    pub(crate) fn memory(self) -> MemoryEffect {
        self.memory
    }

    /// The durable-store effect, if any.
    pub(crate) fn store(self) -> StoreEffect {
        self.store
    }

    /// The telemetry event to emit once the transition has been applied.
    pub(crate) fn announce(self) -> Option<Announce> {
        self.announce
    }
}

/// A concurrent registry of active timer triggers.
///
/// Maps each [`Key`] to a [`TriggerStateMap`] of `(CompactDateTime, TimerType)`
/// to [`TimerState`]. This allows timers of different types to coexist at the
/// same (key, time). Cloning shares the same underlying registry.
#[derive(Clone, Debug, Default)]
pub struct ActiveTriggers(Arc<scc::HashMap<Key, TriggerStateMap>>);

impl ActiveTriggers {
    /// Inserts a trigger into the active registry with
    /// [`TimerState::Scheduled`] state and the trigger's `tag`.
    ///
    /// Creates a new map of (time, type) to entry if no entry exists for the
    /// trigger's key. The trigger replaces an `Aborted` entry and leaves any
    /// other entry unchanged.
    pub async fn insert(&self, trigger: Trigger) {
        self.insert_as(trigger, TimerState::Scheduled).await;
    }

    /// Holds a trigger as [`TimerState::Aborted`] while its row is written.
    /// The hold never replaces an entry.
    pub(crate) async fn hold(&self, trigger: Trigger) {
        self.insert_as(trigger, TimerState::Aborted).await;
    }

    /// Inserts an entry in `state`. Only a live state replaces an `Aborted`
    /// entry.
    async fn insert_as(&self, trigger: Trigger, state: TimerState) {
        let entry = ActiveTriggerEntry {
            state,
            tag: trigger.tag,
        };
        self.0
            .entry_async(trigger.key)
            .await
            .or_default()
            .get_mut()
            .entry((trigger.time, trigger.timer_type))
            .and_modify(|current| {
                if current.state == TimerState::Aborted && state != TimerState::Aborted {
                    *current = entry;
                }
            })
            .or_insert(entry);
    }

    /// Removes a trigger time for a specific key and timer type.
    ///
    /// Removes the key from the registry if removing the (time, type) entry
    /// leaves the map empty. Removing non-existent keys or entries has no
    /// effect.
    pub async fn remove(&self, key: &Key, time: CompactDateTime, timer_type: TimerType) {
        // Look up the entry; if it exists, remove the (time, type) entry and clean up.
        if let Entry::Occupied(mut occupied) = self.0.entry_async(key.clone()).await {
            let states = occupied.get_mut();
            states.remove(&(time, timer_type));

            if states.is_empty() {
                let _ = occupied.remove();
            }
        }
    }

    /// Returns the state of a given trigger time and type for a key, or
    /// `None` if the registry contains no such entry.
    pub async fn get_state(
        &self,
        key: &Key,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> Option<TimerState> {
        self.0
            .read_async(key, |_, states| {
                states.get(&(time, timer_type)).map(|e| e.state)
            })
            .await
            .flatten()
    }

    /// Returns the queued attempt, or `None` if absent.
    pub(crate) async fn get(
        &self,
        key: &Key,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> Option<ActiveTriggerEntry> {
        self.0
            .read_async(key, |_, states| states.get(&(time, timer_type)).copied())
            .await
            .flatten()
    }

    /// Starts only the queued attempt that the pending trigger names.
    pub(crate) async fn fire(&self, trigger: &Trigger) -> bool {
        if let Entry::Occupied(mut occupied) = self.0.entry_async(trigger.key.clone()).await
            && let Some(entry) = occupied
                .get_mut()
                .get_mut(&(trigger.time, trigger.timer_type))
            && matches!(entry.state, TimerState::Scheduled | TimerState::Unconfirmed)
            && entry.tag == trigger.tag
        {
            entry.state = TimerState::Firing;
            return true;
        }
        false
    }

    /// Checks whether a given trigger time and type is active for a key.
    #[cfg(test)]
    pub async fn contains(&self, key: &Key, time: CompactDateTime, timer_type: TimerType) -> bool {
        self.get_state(key, time, timer_type).await.is_some()
    }

    /// Checks whether a trigger is currently scheduled to fire.
    #[cfg(test)]
    pub async fn is_scheduled(
        &self,
        key: &Key,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> bool {
        self.get_state(key, time, timer_type)
            .await
            .is_some_and(|state| {
                matches!(
                    state,
                    TimerState::Scheduled | TimerState::Unconfirmed | TimerState::FiringRescheduled
                )
            })
    }

    /// Atomically sets the state of a timer (preserves tag). Returns `true`
    /// if the state was set (timer exists), `false` otherwise.
    pub async fn set_state(
        &self,
        key: &Key,
        time: CompactDateTime,
        timer_type: TimerType,
        state: TimerState,
    ) -> bool {
        if let Entry::Occupied(mut occupied) = self.0.entry_async(key.clone()).await {
            let states = occupied.get_mut();
            if let Some(entry) = states.get_mut(&(time, timer_type)) {
                entry.state = state;
                return true;
            }
        }
        false
    }

    /// Invokes a closure for every active trigger time and type in the
    /// registry.
    ///
    /// The closure is called once for each stored `(CompactDateTime,
    /// TimerType)` tuple across all keys. Iteration order depends on
    /// internal hash map ordering.
    pub async fn scan_active_times<F>(&self, mut f: F)
    where
        F: FnMut(CompactDateTime, TimerType),
    {
        // For each key and its map of (time, type) → state, apply the callback.
        self.0
            .iter_async(|_, states| {
                for &(time, timer_type) in states.keys() {
                    f(time, timer_type);
                }
                true // Continue iteration over all entries
            })
            .await;
    }

    /// Computes a point-in-time [`TimerSnapshot`] of this registry.
    ///
    /// Scans all active timers once and accumulates counts and ages relative to
    /// `now`. Saturating arithmetic prevents wrapping under pathological loads.
    pub async fn snapshot(&self, now: CompactDateTime) -> TimerSnapshot {
        let mut s = TimerSnapshot::default();
        self.0
            .iter_async(|_, states| {
                for (&(time, _), &entry) in states {
                    s.active = s.active.saturating_add(1);
                    if matches!(
                        entry.state,
                        TimerState::Firing | TimerState::FiringRescheduled
                    ) {
                        s.in_flight = s.in_flight.saturating_add(1);
                    }
                    if time <= now {
                        s.overdue = s.overdue.saturating_add(1);
                        let age = now.epoch_seconds().saturating_sub(time.epoch_seconds());
                        if age > s.oldest_overdue_secs {
                            s.oldest_overdue_secs = age;
                        }
                    }
                }
                true
            })
            .await;
        s
    }
}

/// The timer state machine: resolves a prior state and a [`TimerOp`] to the
/// exact effects to apply.
///
/// This match is the single authority for timer lifecycle behavior — every
/// `(op, prior)` pair resolves to exactly one row here, and rows that must
/// do nothing resolve to the explicit no-op group at the bottom. A `None`
/// prior state means the timer is absent from [`ActiveTriggers`]: never
/// scheduled, already removed, or its slab not yet loaded (absent timers
/// still accept `Schedule`/`Unschedule` so durable rows stay authoritative).
pub(crate) fn transition(prior: Option<TimerState>, op: TimerOp) -> Transition {
    use TimerState::{Aborted, Firing, FiringRescheduled, Scheduled, Unconfirmed};

    /// A row with every effect at its no-op value.
    const NOOP: Transition = Transition {
        memory: update(None, QueueEffect::None),
        store: StoreEffect::None,
        announce: None,
    };

    const fn update(next_state: Option<TimerState>, queue: QueueEffect) -> MemoryEffect {
        MemoryEffect::Update { next_state, queue }
    }

    match (op, prior) {
        // Each new queued attempt has one tag, held before its row is written.
        (TimerOp::Schedule, Some(Firing)) => Transition {
            memory: MemoryEffect::Insert(FiringRescheduled),
            store: StoreEffect::Insert,
            ..NOOP
        },
        (TimerOp::ClearSchedule, Some(Firing)) => Transition {
            memory: MemoryEffect::Insert(FiringRescheduled),
            ..NOOP
        },
        (TimerOp::Schedule, Some(Aborted | Unconfirmed) | None) => Transition {
            memory: MemoryEffect::Insert(Scheduled),
            store: StoreEffect::Insert,
            announce: Some(Announce::Scheduled),
        },

        // Cancel a reschedule: back to `Firing`, drop the queued re-fire.
        // The in-flight delivery completes normally without firing again.
        (TimerOp::Unschedule | TimerOp::ClearReplaced, Some(FiringRescheduled)) => Transition {
            memory: update(Some(Firing), QueueEffect::Dequeue),
            ..NOOP
        },

        // Plain cancel: remove from memory first, then delete the row.
        (TimerOp::Unschedule, Some(Scheduled | Unconfirmed | Aborted) | None) => Transition {
            memory: update(None, QueueEffect::Remove),
            store: StoreEffect::Delete,
            announce: Some(Announce::Cancelled),
        },

        // A timer replaced by `clear_and_schedule`: as `Unschedule`, minus
        // the row delete and telemetry the caller owns.
        (TimerOp::ClearReplaced, Some(Scheduled | Unconfirmed | Aborted) | None) => Transition {
            memory: update(None, QueueEffect::Remove),
            ..NOOP
        },

        // The replacement already has durable rows and a queue entry.
        (TimerOp::Complete | TimerOp::Abort, Some(FiringRescheduled)) => Transition {
            memory: update(Some(Scheduled), QueueEffect::None),
            ..NOOP
        },

        // Commit anything else: drop the registry entry (firing already
        // popped the queue entry), then delete the row.
        (TimerOp::Complete, Some(Firing | Scheduled | Unconfirmed | Aborted) | None) => {
            Transition {
                memory: update(None, QueueEffect::Deactivate),
                store: StoreEffect::Delete,
                ..NOOP
            }
        }

        // Abort a queued timer: keep the row for recovery, drop the queue
        // entry.
        (TimerOp::Abort, Some(Scheduled | Unconfirmed)) => Transition {
            memory: update(Some(Aborted), QueueEffect::Dequeue),
            ..NOOP
        },

        // Abort a firing timer: keep the row; firing already dequeued it.
        (TimerOp::Abort, Some(Firing)) => Transition {
            memory: update(Some(Aborted), QueueEffect::None),
            ..NOOP
        },

        // The caller owns the atomic write of the replacement.
        (TimerOp::ClearSchedule, Some(Aborted | Unconfirmed) | None) => Transition {
            memory: MemoryEffect::Insert(Scheduled),
            ..NOOP
        },

        // Inert pairs: re-scheduling an already-rescheduled timer,
        // cancelling or replacing a timer mid-fire, and aborting an
        // already-aborted or absent timer.
        (TimerOp::Schedule | TimerOp::ClearSchedule, Some(Scheduled | FiringRescheduled))
        | (TimerOp::Unschedule | TimerOp::ClearReplaced, Some(Firing))
        | (TimerOp::Abort, Some(Aborted) | None) => NOOP,
    }
}

#[cfg(test)]
mod tests;
