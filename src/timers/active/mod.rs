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
//! | `ABORTED`            | `Aborted`           | -            | ✓          |
//!
//! Aborted state is in-memory only; persisted rows load as `Scheduled` after
//! restart.

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

    /// Delivery was aborted; the persisted row is retained (for recovery or
    /// an explicit requeue) but the timer is not in the `DelayQueue`.
    Aborted,
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

/// Scheduler-side effect of a [`Transition`].
///
/// `Enqueue`/`Dequeue` touch only the `DelayQueue`; `Insert`/`Remove` are
/// the full scheduler commands, which also create/delete the
/// [`ActiveTriggers`] entry (and, for `Insert`, persist slab metadata).
/// These four and `Deactivate` are the only effects that create or delete
/// registry entries — [`Transition`]s never invent state any other way.
#[derive(Clone, Copy, Debug, Default)]
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
    /// Rotate the persisted oracle tag to the trigger's tag (`update_tag`).
    UpdateTag,
}

/// Which side of the durable write each in-memory effect of a
/// [`Transition`] runs on.
///
/// Store-first orderings exist exactly where an in-memory effect must not
/// be observable before the row it describes is durable; memory-first
/// orderings run queue removals before the row disappears.
#[derive(Clone, Copy, Debug)]
pub(crate) enum EffectOrder {
    /// All in-memory effects run before the durable write.
    MemoryThenStore,
    /// The durable write runs first; all in-memory effects after.
    StoreThenMemory,
    /// The state flip runs before the durable write; the tag adoption and
    /// queue effect after it. Sole user: reviving an `Aborted` slot via
    /// `clear_and_schedule`, whose requeue must only happen once the fresh
    /// row is durable.
    StateThenStoreThenQueue,
}

/// Telemetry event a [`Transition`] emits once fully applied.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Announce {
    /// Emit a `timer_scheduled` lifecycle event.
    Scheduled,
    /// Emit a `timer_cancelled` lifecycle event.
    Cancelled,
}

/// The in-memory effects on one side of a [`Transition`]'s durable write.
///
/// Applied in field order: state flip, tag adoption, then queue effect.
#[derive(Clone, Copy, Debug)]
pub(crate) struct MemoryEffects {
    /// New registry state for the timer; `None` leaves the entry untouched.
    pub(crate) next_state: Option<TimerState>,
    /// Adopt the driving trigger's tag into the registry entry.
    pub(crate) adopt_tag: bool,
    /// Scheduler effect to run after the state and tag updates.
    pub(crate) queue: QueueEffect,
}

impl MemoryEffects {
    /// No in-memory effects.
    const NONE: Self = Self {
        next_state: None,
        adopt_tag: false,
        queue: QueueEffect::None,
    };
}

/// One resolved step of the timer state machine.
///
/// Only [`transition`] constructs values of this type, so the table's
/// exhaustive `(op, prior state)` match is the only place a transition can
/// be defined — an illegal transition is unwritable outside it. Consumers
/// read the in-memory effects through [`phases`](Self::phases), which
/// splits them around the durable write according to the row's
/// [`EffectOrder`].
#[derive(Clone, Copy, Debug)]
pub(crate) struct Transition {
    /// New registry state for the timer; `None` leaves the entry untouched.
    next_state: Option<TimerState>,
    /// Adopt the driving trigger's tag into the registry entry.
    adopt_tag: bool,
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
            adopt_tag: self.adopt_tag,
            queue: self.queue,
        };
        match self.ordering {
            EffectOrder::MemoryThenStore => (all, MemoryEffects::NONE),
            EffectOrder::StoreThenMemory => (MemoryEffects::NONE, all),
            EffectOrder::StateThenStoreThenQueue => (
                MemoryEffects {
                    next_state: self.next_state,
                    ..MemoryEffects::NONE
                },
                MemoryEffects {
                    adopt_tag: self.adopt_tag,
                    queue: self.queue,
                    ..MemoryEffects::NONE
                },
            ),
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
    /// [`TimerState::Scheduled`] state and the trigger's `tag`.
    ///
    /// Creates a new map of (time, type) to entry if no entry exists for the
    /// trigger's key. Duplicate insertions are ignored if the trigger already
    /// exists.
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
    use EffectOrder::{StateThenStoreThenQueue, StoreThenMemory};
    use TimerState::{Aborted, Firing, FiringRescheduled, Scheduled};

    /// A row with every effect at its no-op value.
    const NOOP: Transition = Transition {
        next_state: None,
        adopt_tag: false,
        queue: QueueEffect::None,
        store: StoreEffect::None,
        ordering: EffectOrder::MemoryThenStore,
        announce: None,
    };

    match (op, prior) {
        // Scheduling a firing timer marks it for a re-fire after commit; the
        // durable row already exists, only the queue needs the entry back.
        (TimerOp::Schedule | TimerOp::ClearSchedule, Some(Firing)) => Transition {
            next_state: Some(FiringRescheduled),
            queue: QueueEffect::Enqueue,
            ..NOOP
        },

        // Requeue an aborted timer: its row and tag are already durable.
        (TimerOp::Schedule, Some(Aborted)) => Transition {
            next_state: Some(Scheduled),
            queue: QueueEffect::Enqueue,
            announce: Some(Announce::Scheduled),
            ..NOOP
        },

        // Fresh schedule. Persisting the row before the scheduler `Add`
        // means a concurrent slab-load scan between the two writes already
        // sees the row; the actor's `Add` then finds the slab owned and just
        // inserts the in-memory entry.
        (TimerOp::Schedule, Some(Scheduled) | None) => Transition {
            queue: QueueEffect::Insert,
            store: StoreEffect::Insert,
            ordering: StoreThenMemory,
            announce: Some(Announce::Scheduled),
            ..NOOP
        },

        // Cancel a reschedule: back to `Firing`, drop the queued re-fire.
        // The in-flight delivery completes normally without firing again.
        (TimerOp::Unschedule | TimerOp::ClearReplaced, Some(FiringRescheduled)) => Transition {
            next_state: Some(Firing),
            queue: QueueEffect::Dequeue,
            ..NOOP
        },

        // Plain cancel: remove from memory first, then delete the row.
        (TimerOp::Unschedule, Some(Scheduled | Aborted) | None) => Transition {
            queue: QueueEffect::Remove,
            store: StoreEffect::Delete,
            announce: Some(Announce::Cancelled),
            ..NOOP
        },

        // A timer replaced by `clear_and_schedule`: as `Unschedule`, minus
        // the row delete and telemetry the caller owns.
        (TimerOp::ClearReplaced, Some(Scheduled | Aborted) | None) => Transition {
            queue: QueueEffect::Remove,
            ..NOOP
        },

        // Commit a rescheduled timer: the row stays (the timer fires again)
        // and the oracle tag rotates — durably first, so memory never shows
        // a rotation the store could lose.
        (TimerOp::Complete, Some(FiringRescheduled)) => Transition {
            next_state: Some(Scheduled),
            adopt_tag: true,
            store: StoreEffect::UpdateTag,
            ordering: StoreThenMemory,
            ..NOOP
        },

        // Commit anything else: drop the registry entry (firing already
        // popped the queue entry), then delete the row.
        (TimerOp::Complete, Some(Firing | Scheduled | Aborted) | None) => Transition {
            queue: QueueEffect::Deactivate,
            store: StoreEffect::Delete,
            ..NOOP
        },

        // Abort a reschedule: the timer is still queued; it fires again.
        (TimerOp::Abort, Some(FiringRescheduled)) => Transition {
            next_state: Some(Scheduled),
            ..NOOP
        },

        // Abort a queued timer: keep the row for recovery, drop the queue
        // entry.
        (TimerOp::Abort, Some(Scheduled)) => Transition {
            next_state: Some(Aborted),
            queue: QueueEffect::Dequeue,
            ..NOOP
        },

        // Abort a firing timer: keep the row; firing already dequeued it.
        (TimerOp::Abort, Some(Firing)) => Transition {
            next_state: Some(Aborted),
            ..NOOP
        },

        // Revive an aborted slot via `clear_and_schedule`: flip to
        // `Scheduled` before the atomic write; adopt the trigger's tag and
        // requeue only once the fresh row is durable.
        (TimerOp::ClearSchedule, Some(Aborted)) => Transition {
            next_state: Some(Scheduled),
            adopt_tag: true,
            queue: QueueEffect::Insert,
            ordering: StateThenStoreThenQueue,
            ..NOOP
        },

        // The new timer of a `clear_and_schedule`: queue it only after the
        // atomic write lands.
        (TimerOp::ClearSchedule, Some(Scheduled) | None) => Transition {
            queue: QueueEffect::Insert,
            ordering: StoreThenMemory,
            ..NOOP
        },

        // Inert pairs: re-scheduling an already-rescheduled timer,
        // cancelling or replacing a timer mid-fire, and aborting an
        // already-aborted or absent timer.
        (TimerOp::Schedule | TimerOp::ClearSchedule, Some(FiringRescheduled))
        | (TimerOp::Unschedule | TimerOp::ClearReplaced, Some(Firing))
        | (TimerOp::Abort, Some(Aborted) | None) => NOOP,
    }
}

#[cfg(test)]
mod tests;
