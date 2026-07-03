//! Active trigger registry with explicit timer lifecycle states.
//!
//! Provides [`ActiveTriggers`], a thread-safe registry tracking timer
//! triggers currently loaded in the scheduler. Each trigger is identified
//! by a [`Key`], scheduled [`CompactDateTime`], and [`TimerType`].
//!
//! # Timer State Machine
//!
//! Each timer transitions through these states:
//!
//! ```text
//! UNSCHEDULED ──schedule()──► SCHEDULED ──fire()──► FIRING
//!      ▲                          │                   │ │
//!      │                   unschedule()          commit() schedule(same)
//!      │                          │                   │      │
//!      │                          ▼                   ▼      ▼
//!      └──────────────────── UNSCHEDULED      UNSCHEDULED  FIRING_RESCHEDULED
//!                                                    ▲            │ │
//!                                               commit()    abort() unschedule(same)
//!                                                    │            │ │
//!                                                    └─SCHEDULED◄─┘ │
//!                                                         ▲         │
//!                                                         └─────────┘
//! ```
//!
//! # State Definitions
//!
//! | State                | `ActiveTriggers`    | `DelayQueue` | `Database` |
//! |----------------------|---------------------|--------------|------------|
//! | `UNSCHEDULED`        | absent              | -            | -          |
//! | `SCHEDULED`          | `Scheduled`         | ✓            | ✓          |
//! | `FIRING`             | `Firing`            | -            | ✓          |
//! | `FIRING_RESCHEDULED` | `FiringRescheduled` | ✓            | ✓          |
//! | `ABORTED`            | `Aborted`           | -            | ✓          |
//!
//! # Transition Effects
//!
//! | From                 | Operation          | To                   | `ActiveTriggers`      | `DelayQueue`   | `Database` |
//! |----------------------|--------------------|----------------------|-----------------------|----------------|------------|
//! | `UNSCHEDULED`        | `schedule()`       | `SCHEDULED`          | insert `Scheduled`    | insert         | insert     |
//! | `SCHEDULED`          | `unschedule()`     | `UNSCHEDULED`        | remove                | remove         | delete     |
//! | `SCHEDULED`          | `fire()`           | `FIRING`             | → `Firing`            | (auto-removed) | -          |
//! | `FIRING`             | `schedule(same)`   | `FIRING_RESCHEDULED` | → `FiringRescheduled` | insert         | -          |
//! | `FIRING`             | `unschedule(same)` | `FIRING`             | -                     | -              | -          |
//! | `FIRING`             | `commit()`         | `UNSCHEDULED`        | remove                | -              | delete     |
//! | `FIRING`             | `abort()`          | `ABORTED`            | → `Aborted`           | -              | -          |
//! | `FIRING_RESCHEDULED` | `unschedule(same)` | `FIRING`             | → `Firing`            | remove         | -          |
//! | `FIRING_RESCHEDULED` | `commit()`         | `SCHEDULED`          | → `Scheduled`         | -              | -          |
//! | `FIRING_RESCHEDULED` | `abort()`          | `SCHEDULED`          | → `Scheduled`         | -              | -          |
//! | `ABORTED`            | `schedule(same)`   | `SCHEDULED`          | → `Scheduled`         | insert         | -          |
//! | `ABORTED`            | `unschedule(same)` | `UNSCHEDULED`        | remove                | -              | delete     |
//! | `ABORTED`            | `commit()`         | `UNSCHEDULED`        | remove                | -              | delete     |
//!
//! Aborted state is in-memory only; persisted rows load as `Scheduled` after
//! restart.
//!
//! # API Behavior by State
//!
//! | Operation            | `SCHEDULED` | `FIRING`   | `FIRING_RESCHEDULED` | `ABORTED` |
//! |----------------------|-------------|------------|----------------------|-----------|
//! | `schedule(T)`        | no-op       | reschedule | no-op (idempotent)   | requeue   |
//! | `unschedule(T)`      | remove      | no-op      | cancel reschedule    | remove    |
//! | `scheduled_times()`  | include     | exclude    | include              | include   |
//! | `commit(T)`          | N/A         | delete DB  | keep DB              | delete DB |

use crate::Key;
use crate::timers::datetime::CompactDateTime;
use crate::timers::{TimerType, Trigger};
use ahash::HashMap;
use scc::hash_map::Entry;
use std::sync::Arc;

/// Per-timer entry combining lifecycle state with the oracle tag.
///
/// `state` and `tag` must be mutated together under the trigger-lock.
/// Per-key linearization (`KeyManager`) makes this coherent: no two
/// `EventContext` operations for the same key run concurrently.
#[derive(Clone, Copy, Debug, Default)]
pub struct ActiveTriggerEntry {
    /// Lifecycle state of the timer.
    pub state: TimerState,
    /// Random 32-bit identity; `0` for legacy/inline timers without a stored
    /// tag.
    pub tag: i32,
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
/// Each timer in [`ActiveTriggers`] is in exactly one of these states.
/// The state determines how operations like `schedule()`, `unschedule()`,
/// `commit()`, and `abort()` behave. See `data-model.md` for the full
/// state machine diagram.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum TimerState {
    /// Timer is in the `DelayQueue`, waiting to fire.
    ///
    /// This is the default state for newly scheduled timers.
    #[default]
    Scheduled,

    /// Handler is processing this timer; timer has not been rescheduled.
    ///
    /// In this state:
    /// - `schedule(same)` transitions to `FiringRescheduled`
    /// - `unschedule(same)` is a no-op
    /// - `commit()` deletes the database row and removes from `ActiveTriggers`
    /// - `abort()` keeps the database row and transitions to `Aborted`
    Firing,

    /// Handler is processing this timer; timer will fire again after commit.
    ///
    /// In this state:
    /// - `schedule(same)` is idempotent (no-op)
    /// - `unschedule(same)` transitions back to `Firing`
    /// - `commit()` keeps the database row and transitions to `Scheduled`
    /// - `abort()` keeps the database row and transitions to `Scheduled`
    FiringRescheduled,

    /// Delivery was aborted and the persisted timer row is retained.
    ///
    /// In this state:
    /// - the timer remains in `ActiveTriggers` and persistent storage
    /// - the timer is not in the `DelayQueue`
    /// - `schedule(same)` transitions to `Scheduled` and requeues it
    /// - `unschedule(same)` deletes the database row and removes it from
    ///   `ActiveTriggers`
    /// - `complete()` preserves the current idempotent delete behavior for
    ///   non-firing timers
    Aborted,
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
    /// trigger's key. Duplicate insertions are ignored if the trigger already
    /// exists.
    ///
    /// # Arguments
    ///
    /// * `trigger` - The [`Trigger`] containing the key, time, type, and tag.
    pub async fn insert(&self, trigger: Trigger) {
        let entry = ActiveTriggerEntry {
            state: TimerState::Scheduled,
            tag: trigger.tag,
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
    ///
    /// # Arguments
    ///
    /// * `key` - The [`Key`] from which to remove the trigger.
    /// * `time` - The [`CompactDateTime`] to remove.
    /// * `timer_type` - The [`TimerType`] to remove.
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

    /// Returns the state of a given trigger time and type for a key.
    ///
    /// # Returns
    ///
    /// `Some(TimerState)` if the registry contains the entry, `None` otherwise.
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

    /// Returns the tag of a given trigger time and type for a key.
    ///
    /// # Returns
    ///
    /// `Some(tag)` if the registry contains the entry, `None` if absent.
    pub async fn get_tag(
        &self,
        key: &Key,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> Option<i32> {
        self.0
            .read_async(key, |_, states| {
                states.get(&(time, timer_type)).map(|e| e.tag)
            })
            .await
            .flatten()
    }

    /// Atomically updates the tag for a given trigger entry.
    ///
    /// Returns `true` if the entry existed and was updated.
    pub async fn set_tag(
        &self,
        key: &Key,
        time: CompactDateTime,
        timer_type: TimerType,
        tag: i32,
    ) -> bool {
        if let Entry::Occupied(mut occupied) = self.0.entry_async(key.clone()).await {
            let states = occupied.get_mut();
            if let Some(entry) = states.get_mut(&(time, timer_type)) {
                entry.tag = tag;
                return true;
            }
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
                matches!(state, TimerState::Scheduled | TimerState::FiringRescheduled)
            })
    }

    /// Atomically sets the state of a timer (preserves tag).
    ///
    /// # Returns
    ///
    /// `true` if the state was set (timer exists), `false` otherwise.
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
    ///
    /// # Arguments
    ///
    /// * `f` - A closure that takes a [`CompactDateTime`] and [`TimerType`].
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
