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
//! | `FIRING`             | `Firing`            | -            | ✓          |
//! | `FIRING_RESCHEDULED` | `FiringRescheduled` | ✓            | ✓          |
//! | `ABORTED`            | `Aborted`           | -            | maybe      |
//!
//! A queued timer whose schedule was dropped or failed during its row write
//! has a [`Row::Unconfirmed`] row. The next schedule writes it again.
//!
//! # Durable Row Invariant
//!
//! A written row is in a registered slab. In an owned slab, it also has a
//! registry entry, so cleanup keeps the slab and a restart loads the row. The
//! registry gains an entry before its row is written and loses it after its
//! row is deleted, so a dropped or failed call cannot break the invariant.

use crate::Key;
use crate::timers::datetime::CompactDateTime;
use crate::timers::{TimerType, Trigger};
use ahash::HashMap;
use scc::hash_map::Entry;
use std::sync::Arc;

pub(crate) use machine::{
    Announce, MemoryEffect, QueueEffect, StoreEffect, TimerOp, Transition, transition,
};

mod machine;

/// The lifecycle state and immutable identity of one queued attempt.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct ActiveTriggerEntry {
    /// Lifecycle state of the timer.
    pub(crate) state: TimerState,
    /// Random 32-bit identity; `0` for legacy/inline timers without a stored
    /// tag.
    pub(crate) tag: i32,
    /// Whether the attempt's row write is known to have landed.
    pub(crate) row: Row,
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
}

/// A state that a schedule queues its timer in.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Queued {
    /// The timer waits to fire.
    Scheduled,
    /// The timer fires again after the current delivery commits.
    FiringRescheduled,
}

impl From<Queued> for TimerState {
    fn from(queued: Queued) -> Self {
        match queued {
            Queued::Scheduled => Self::Scheduled,
            Queued::FiringRescheduled => Self::FiringRescheduled,
        }
    }
}

/// Whether a timer's row write is known to have landed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum Row {
    /// The write succeeded, or a load read the row.
    #[default]
    Written,
    /// The schedule was dropped or failed during the write, so the row may be
    /// absent. The timer still fires once from memory.
    Unconfirmed,
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
            row: Row::Written,
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
        // Look up the entry; if it exists, remove the (time, type) entry and
        // clean up.
        if let Entry::Occupied(mut occupied) = self.0.entry_async(key.clone()).await {
            let states = occupied.get_mut();
            states.remove(&(time, timer_type));

            if states.is_empty() {
                let _ = occupied.remove();
            }
        }
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
            && entry.state == TimerState::Scheduled
            && entry.tag == trigger.tag
        {
            entry.state = TimerState::Firing;
            return true;
        }
        false
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
        self.update(key, time, timer_type, |entry| entry.state = state)
            .await
    }

    /// Sets the queued state of a timer and the certainty of its row.
    pub(crate) async fn queue(
        &self,
        key: &Key,
        time: CompactDateTime,
        timer_type: TimerType,
        queued: Queued,
        row: Row,
    ) {
        self.update(key, time, timer_type, |entry| {
            entry.state = queued.into();
            entry.row = row;
        })
        .await;
    }

    /// Applies `change` to an entry. Returns `false` if there is no entry.
    async fn update(
        &self,
        key: &Key,
        time: CompactDateTime,
        timer_type: TimerType,
        change: impl FnOnce(&mut ActiveTriggerEntry),
    ) -> bool {
        if let Entry::Occupied(mut occupied) = self.0.entry_async(key.clone()).await
            && let Some(entry) = occupied.get_mut().get_mut(&(time, timer_type))
        {
            change(entry);
            return true;
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

#[cfg(test)]
mod tests;
