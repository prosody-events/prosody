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
//! | State                  | `ActiveTriggers`      | `DelayQueue`   | `Database`                             |
//! | ---------------------- | --------------------- | -------------- | -------------------------------------- |
//! | `UNSCHEDULED`          | absent                | -              | -                                      |
//! | `SCHEDULED`            | `Scheduled`           | ✓              | ✓                                      |
//! | `FIRING`               | `Firing`              | -              | ✓                                      |
//! | `FIRING_REPLACED`      | `FiringReplaced`      | -              | slab row; no key row                   |
//! | `FIRING_RESCHEDULED`   | `FiringRescheduled`   | ✓              | ✓                                      |
//! | `PARKED`               | `Parked`              | -              | ✓ (slab row; key row unless receipted) |
//!
//! Persisted rows load as `Scheduled` after restart.

use crate::Key;
use crate::timers::datetime::CompactDateTime;
use crate::timers::{TimerType, Trigger};
use ahash::HashMap;
use scc::hash_map::Entry;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct ActiveTriggerEntry {
    /// Lifecycle state of the timer.
    pub(crate) state: TimerState,
}

/// Point-in-time counts of active timers for metrics reporting.
#[derive(Clone, Copy, Debug, Default)]
pub struct TimerSnapshot {
    /// Timers in any state in the loaded scheduling window.
    pub active: u32,
    /// Timers in `Firing`, `FiringReplaced`, or `FiringRescheduled` state.
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

    /// The handler still processes this attempt, but `clear_and_schedule`
    /// replaced its key row. Its slab row remains until complete or retire.
    FiringReplaced,

    /// Handler is processing this timer; the same (key, time, type) was
    /// re-scheduled during dispatch and will fire again after commit.
    FiringRescheduled,

    /// The timer has no queue entry. Its slab row waits for reload, schedule,
    /// or retire. An aborted attempt keeps its key row. A receipted attempt
    /// has no key row.
    Parked,
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
    /// Record the commit receipt but keep its redelivery source.
    Receipt,
    /// Retire the redelivery source after state promotion.
    Retire,
    /// The delivery handler abandoned the attempt.
    Abort,
    /// Schedule the new coordinate. The clear write applies the store effects
    /// for this coordinate and each [`ClearReplaced`](Self::ClearReplaced) row.
    ClearSchedule,
    /// Replace an old coordinate. The clear write applies its slab delete.
    ClearReplaced,
}

/// Scheduler-side effect of a [`Transition`].
///
/// `Enqueue`/`Dequeue` touch only the `DelayQueue`; `Insert`/`Remove` are
/// the full scheduler commands, which also create/delete the
/// [`ActiveTriggers`] entry (and, for `Insert`, persist slab metadata).
/// These four and `Deactivate` are the only effects that create or delete
/// registry entries — [`Transition`]s never invent state any other way.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum QueueEffect {
    /// No queue change.
    #[default]
    None,
    /// Queue-only insert; the registry entry already exists.
    Enqueue,
    /// Queue-only removal; the registry entry stays.
    Dequeue,
    /// Full scheduler `Add`: slab metadata, queue insert, registry insert.
    Insert,
    /// Set the queued tag after the store rotates it.
    Retag,
    /// Full scheduler `Remove`: queue removal plus registry removal.
    Remove,
    /// Registry-only removal; the queue entry was already popped by firing.
    Deactivate,
}

/// Durable-store effect of a [`Transition`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StoreEffect {
    /// No store write.
    None,
    /// Insert the timer row (`add_trigger`).
    Insert,
    /// Insert only the key row (`add_key_row`).
    InsertKeyRow,
    /// Delete both rows. If the queue keeps an older source, delete only the
    /// key row.
    Delete,
    /// Delete only the key-index row (`remove_key_row`).
    DeleteKeyRow,
    /// Delete only the slab row (`remove_slab_row`).
    DeleteSlabRow,
    /// Rotate the persisted oracle tag to the trigger's tag (`update_tag`).
    UpdateTag,
}

/// Which side of the durable write each in-memory effect of a
/// [`Transition`] runs on.
///
/// Store-first orderings exist exactly where an in-memory effect must not
/// be observable before the row it describes is durable; memory-first
/// orderings run queue removals before the row disappears.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum EffectOrder {
    /// All in-memory effects run before the durable write.
    MemoryThenStore,
    /// The durable write runs first; all in-memory effects after.
    StoreThenMemory,
}

/// Telemetry event a [`Transition`] emits once fully applied.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Announce {
    /// Emit a `timer_scheduled` lifecycle event.
    Scheduled,
    /// Emit a `timer_cancelled` lifecycle event.
    Cancelled,
}

/// The in-memory effects on one side of a [`Transition`]'s durable write.
///
/// Applied in field order: state flip, then queue effect.
#[derive(Clone, Copy, Debug)]
pub(crate) struct MemoryEffects {
    /// New registry state for the timer; `None` leaves the entry untouched.
    pub(crate) next_state: Option<TimerState>,
    /// Scheduler effect to run after the state and tag updates.
    pub(crate) queue: QueueEffect,
}

impl MemoryEffects {
    /// No in-memory effects.
    const NONE: Self = Self {
        next_state: None,
        queue: QueueEffect::None,
    };
}

/// The effects of one timer operation.
///
/// [`transition`] selects the effects for every operation and prior state.
/// [`phases`](Self::phases) separates memory effects around the durable write.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Transition {
    /// New registry state for the timer; `None` leaves the entry untouched.
    next_state: Option<TimerState>,
    /// Scheduler queue effect.
    queue: QueueEffect,
    /// Durable-store effect.
    store: StoreEffect,
    /// Where the in-memory effects sit relative to the durable write.
    ordering: EffectOrder,
    /// Telemetry event emitted once the transition is fully applied.
    announce: Option<Announce>,
}

impl Transition {
    const DEACTIVATE: Self = Self {
        queue: QueueEffect::Deactivate,
        ..Self::NONE
    };
    /// A row with every effect at its no-op value.
    const NONE: Self = Self {
        next_state: None,
        queue: QueueEffect::None,
        store: StoreEffect::None,
        ordering: EffectOrder::MemoryThenStore,
        announce: None,
    };
    const PARK: Self = Self {
        next_state: Some(TimerState::Parked),
        ..Self::NONE
    };
    const SCHEDULE_KEY: Self = Self {
        next_state: Some(TimerState::Scheduled),
        store: StoreEffect::InsertKeyRow,
        ordering: EffectOrder::StoreThenMemory,
        announce: Some(Announce::Scheduled),
        ..Self::NONE
    };

    /// The durable-store effect, if any.
    pub(crate) fn store(self) -> StoreEffect {
        self.store
    }

    /// The telemetry event to emit once the transition has been applied.
    pub(crate) fn announce(self) -> Option<Announce> {
        self.announce
    }

    /// Splits the in-memory effects into their (pre-persist, post-persist)
    /// halves according to the row's [`EffectOrder`].
    pub(crate) fn phases(self) -> (MemoryEffects, MemoryEffects) {
        let all = MemoryEffects {
            next_state: self.next_state,
            queue: self.queue,
        };
        match self.ordering {
            EffectOrder::MemoryThenStore => (all, MemoryEffects::NONE),
            EffectOrder::StoreThenMemory => (MemoryEffects::NONE, all),
        }
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
    /// [`TimerState::Scheduled`] state.
    ///
    /// Creates a new map of (time, type) to entry if no entry exists for the
    /// trigger's key. Duplicate insertions are ignored if the trigger already
    /// exists.
    pub async fn insert(&self, trigger: Trigger) {
        let entry = ActiveTriggerEntry {
            state: TimerState::Scheduled,
        };
        self.0
            .entry_async(trigger.key)
            .await
            .or_default()
            .get_mut()
            .entry((trigger.time, trigger.timer_type))
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
                matches!(state, TimerState::Scheduled | TimerState::FiringRescheduled)
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
                        TimerState::Firing
                            | TimerState::FiringReplaced
                            | TimerState::FiringRescheduled
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
    use EffectOrder::StoreThenMemory;
    use TimerOp as Op;
    use TimerState::{Firing, FiringReplaced, FiringRescheduled, Parked, Scheduled};

    match (op, prior) {
        // Queue the next fire. The current attempt still has durable rows.
        (Op::Schedule | Op::ClearSchedule, Some(Firing)) => Transition {
            next_state: Some(FiringRescheduled),
            queue: QueueEffect::Enqueue,
            ..Transition::NONE
        },

        // Restore the key row before the next fire enters the queue.
        (Op::Schedule, Some(FiringReplaced)) => Transition {
            next_state: Some(FiringRescheduled),
            queue: QueueEffect::Enqueue,
            store: StoreEffect::InsertKeyRow,
            ordering: StoreThenMemory,
            ..Transition::NONE
        },

        // Queue the next fire after the caller's atomic write restores the rows.
        (Op::ClearSchedule, Some(FiringReplaced)) => Transition {
            next_state: Some(FiringRescheduled),
            queue: QueueEffect::Enqueue,
            ordering: StoreThenMemory,
            ..Transition::NONE
        },

        // Keep the attempt active while the caller replaces its rows.
        (Op::ClearReplaced, Some(Firing)) => Transition {
            next_state: Some(FiringReplaced),
            ..Transition::NONE
        },

        // Cancel the next fire because the caller replaces its rows.
        (Op::ClearReplaced, Some(FiringRescheduled)) => Transition {
            next_state: Some(FiringReplaced),
            queue: QueueEffect::Dequeue,
            ..Transition::NONE
        },

        // Write both rows before the scheduler can load them or queue the timer.
        (Op::Schedule, None) => Transition {
            queue: QueueEffect::Insert,
            store: StoreEffect::Insert,
            ..Transition::SCHEDULE_KEY
        },

        // Keep the queued trigger and its slab tag on a repeat schedule.
        (Op::Schedule, Some(Scheduled)) => Transition::SCHEDULE_KEY,

        // Queue the parked coordinate after its key row is ready.
        (Op::Schedule, Some(Parked)) => Transition {
            queue: QueueEffect::Insert,
            ..Transition::SCHEDULE_KEY
        },

        // Cancel the next fire. The current attempt keeps its rows until commit.
        (Op::Unschedule, Some(FiringRescheduled)) => Transition {
            next_state: Some(Firing),
            queue: QueueEffect::Dequeue,
            ..Transition::NONE
        },

        // Cancel the timer before its durable rows disappear.
        (Op::Unschedule, Some(Scheduled | Parked) | None) => Transition {
            queue: QueueEffect::Remove,
            store: StoreEffect::Delete,
            announce: Some(Announce::Cancelled),
            ..Transition::NONE
        },

        // Remove the replaced timer. The caller deletes its rows and emits telemetry.
        (Op::ClearReplaced, Some(Scheduled | Parked) | None) => Transition {
            queue: QueueEffect::Remove,
            store: StoreEffect::DeleteSlabRow,
            ..Transition::NONE
        },

        // Rotate the tag before the queued timer can fire as a new attempt.
        (Op::Complete | Op::Receipt | Op::Retire, Some(FiringRescheduled)) => Transition {
            next_state: Some(Scheduled),
            store: StoreEffect::UpdateTag,
            queue: QueueEffect::Retag,
            ordering: StoreThenMemory,
            ..Transition::NONE
        },

        // Remove the entry and rows because the attempt committed without a next fire.
        (Op::Complete, Some(Firing | Scheduled | Parked) | None) => Transition {
            store: StoreEffect::Delete,
            ..Transition::DEACTIVATE
        },

        // Park after the key-row delete. A failed delete leaves Firing, so the retry deletes the
        // row again.
        (Op::Receipt, Some(Firing)) => Transition {
            store: StoreEffect::DeleteKeyRow,
            ordering: StoreThenMemory,
            ..Transition::PARK
        },

        // Remove the slab row after promotion. The timer has no queue entry.
        (Op::Retire, Some(Firing | Parked | FiringReplaced) | None)
        | (Op::Complete, Some(FiringReplaced)) => Transition {
            store: StoreEffect::DeleteSlabRow,
            ..Transition::DEACTIVATE
        },

        // Keep the queued timer so another attempt can fire after abort.
        (Op::Abort, Some(FiringRescheduled)) => Transition {
            next_state: Some(Scheduled),
            ..Transition::NONE
        },

        // Park a queued timer. Its rows remain for reload or another schedule.
        (Op::Abort, Some(Scheduled)) => Transition {
            queue: QueueEffect::Dequeue,
            ..Transition::PARK
        },

        // Park the failed attempt. Its rows remain, and fire already removed its queue entry.
        (Op::Abort, Some(Firing | FiringReplaced)) => Transition::PARK,

        // Queue the timer after the caller's atomic write restores its rows.
        (Op::ClearSchedule, Some(Scheduled | Parked) | None) => Transition {
            next_state: Some(Scheduled),
            queue: QueueEffect::Insert,
            ordering: StoreThenMemory,
            ..Transition::NONE
        },

        // Keep the current state when the operation has no work to do.
        (Op::Schedule | Op::ClearSchedule, Some(FiringRescheduled))
        | (Op::Unschedule, Some(Firing))
        | (Op::Unschedule | Op::ClearReplaced, Some(FiringReplaced))
        | (Op::Receipt, Some(FiringReplaced | Scheduled | Parked) | None)
        | (Op::Abort, Some(Parked) | None)
        | (Op::Retire, Some(Scheduled)) => Transition::NONE,
    }
}

#[cfg(test)]
mod tests;
