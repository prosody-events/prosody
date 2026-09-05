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
//! A coordinate owns at most one item. Its state records the item's location.
//! Persisted rows load as `Scheduled(Item::Queued)` after restart.

use crate::Key;
use crate::timers::datetime::CompactDateTime;
use crate::timers::store::RetainOldSlab;
use crate::timers::{TimerType, Trigger};
use ahash::HashMap;
use scc::hash_map::Entry;
use std::sync::Arc;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
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

/// Where a coordinate's one queue item is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Item {
    /// In the delay queue. The queue holds its tag.
    Queued,
    /// Emitted to the partition and not yet fired. The registry holds its tag.
    Delivered { tag: i32 },
}

/// Lifecycle state of a timer in the in-memory scheduler.
///
/// Only `Scheduled` and `FiringRescheduled` own an [`Item`].
/// [`transition`] defines the effects of each lifecycle operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum TimerState {
    /// The timer has an item for its next attempt.
    Scheduled(Item),
    /// The handler processes this timer without a next fire.
    Firing,
    /// The replacement waits for this attempt's receipt or completion.
    /// Both old rows stay until then. Abort drops the replacement.
    FiringReplaced(Trigger),
    /// The handler processes this timer. Its next attempt has an item.
    FiringRescheduled(Item),
    /// The timer has no item. Its slab row waits for reload, schedule, or
    /// retire. An aborted attempt keeps its key row. A receipted attempt
    /// has no key row.
    Parked,
}

impl Default for TimerState {
    fn default() -> Self {
        Self::Scheduled(Item::Queued)
    }
}

/// A lifecycle operation submitted to the timer state machine.
///
/// Ops name caller intent; [`transition`] resolves `(prior state, op)` to
/// the effects to apply. The registry starts an attempt through
/// [`ActiveTriggers::fire`].
#[derive(Clone, Debug)]
pub(crate) enum TimerOp {
    /// Request a future fire at this (key, time, type).
    Schedule,
    /// Cancel a pending fire.
    Unschedule,
    /// Cancel the pending replacement.
    DropReplacement,
    /// The delivery handler committed.
    Complete,
    /// Record the commit receipt but keep its redelivery source.
    Receipt,
    /// Retire the redelivery source after state promotion.
    Retire,
    /// The delivery handler abandoned the attempt.
    Abort,
    /// Schedule the clear's target coordinate.
    ClearSchedule,
    /// Replace an old coordinate with this request.
    ClearReplaced(Trigger),
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
    /// Set the item tag at its current location after the store rotates it.
    Retag,
    /// Full scheduler `Remove`: queue removal plus registry removal.
    Remove,
    /// Registry-only removal; the queue entry was already popped by firing.
    Deactivate,
}

/// Durable-store effect of a [`Transition`].
#[derive(Clone, Debug, Eq, PartialEq)]
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
    /// Replace the key entry and write the next timer.
    Replace(Trigger, RetainOldSlab),
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

/// What the registry does to the coordinate's state.
#[derive(Clone, Debug)]
pub(crate) enum Next {
    /// Keep the current state.
    Keep,
    /// End a rescheduled attempt and keep its current item. Keep any other
    /// state.
    Idle,
    /// Replace the state.
    To(TimerState),
}

/// The in-memory effects on one side of a [`Transition`]'s durable write.
///
/// Applied in field order: state flip, then queue effect.
#[derive(Clone, Debug)]
pub(crate) struct MemoryEffects {
    /// The action to apply to the current registry state.
    pub(crate) next: Next,
    /// Scheduler effect to run after the state and tag updates.
    pub(crate) queue: QueueEffect,
}

impl MemoryEffects {
    /// No in-memory effects.
    const NONE: Self = Self {
        next: Next::Keep,
        queue: QueueEffect::None,
    };
}

/// The effects of one timer operation.
///
/// [`transition`] selects the effects for every operation and prior state.
/// [`phases`](Self::phases) separates memory effects around the durable write.
#[derive(Clone, Debug)]
pub(crate) struct Transition {
    /// The action to apply to the current registry state.
    next: Next,
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
    const CANCEL: Self = Self {
        queue: QueueEffect::Remove,
        store: StoreEffect::Delete,
        announce: Some(Announce::Cancelled),
        ..Self::NONE
    };
    const DEACTIVATE: Self = Self {
        queue: QueueEffect::Deactivate,
        ..Self::NONE
    };
    const DROP_REPLACEMENT: Self = Self {
        next: Next::To(TimerState::Firing),
        announce: Some(Announce::Cancelled),
        ..Self::NONE
    };
    /// A row with every effect at its no-op value.
    const NONE: Self = Self {
        next: Next::Keep,
        queue: QueueEffect::None,
        store: StoreEffect::None,
        ordering: EffectOrder::MemoryThenStore,
        announce: None,
    };
    const PARK: Self = Self {
        next: Next::To(TimerState::Parked),
        ..Self::NONE
    };
    const RESCHEDULE: Self = Self {
        next: Next::To(TimerState::FiringRescheduled(Item::Queued)),
        queue: QueueEffect::Enqueue,
        ..Self::NONE
    };
    const SCHEDULE_KEY: Self = Self {
        next: Next::To(TimerState::Scheduled(Item::Queued)),
        store: StoreEffect::InsertKeyRow,
        ordering: EffectOrder::StoreThenMemory,
        announce: Some(Announce::Scheduled),
        ..Self::NONE
    };

    /// The durable-store effect, if any.
    pub(crate) fn store(&self) -> StoreEffect {
        self.store.clone()
    }

    /// The telemetry event to emit once the transition has been applied.
    pub(crate) fn announce(&self) -> Option<Announce> {
        self.announce
    }

    /// Splits the in-memory effects into their (pre-persist, post-persist)
    /// halves according to the row's [`EffectOrder`].
    pub(crate) fn phases(&self) -> (MemoryEffects, MemoryEffects) {
        let all = MemoryEffects {
            next: self.next.clone(),
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
/// to [`TimerState`]. Timers of different types can share a key and time.
/// Item moves hold one entry guard. Transitions select actions and never copy
/// an existing item's location. Cloning shares the registry.
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
            state: TimerState::Scheduled(Item::Queued),
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
    pub(crate) async fn get_state(
        &self,
        key: &Key,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> Option<TimerState> {
        self.0
            .read_async(key, |_, states| {
                states.get(&(time, timer_type)).map(|e| e.state.clone())
            })
            .await
            .flatten()
    }

    /// Returns this key's active attempt for the timer type.
    /// Per-key dispatch permits at most one such attempt.
    pub(crate) async fn attempt(
        &self,
        key: &Key,
        timer_type: TimerType,
    ) -> Option<(CompactDateTime, TimerState)> {
        self.0
            .read_async(key, |_, states| {
                states.iter().find_map(|(&(time, kind), entry)| {
                    (kind == timer_type
                        && matches!(
                            entry.state,
                            TimerState::Firing
                                | TimerState::FiringRescheduled(_)
                                | TimerState::FiringReplaced(_)
                        ))
                    .then(|| (time, entry.state.clone()))
                })
            })
            .await
            .flatten()
    }

    /// Returns the attempt coordinate and its pending replacement.
    pub(crate) async fn pending_replacement(
        &self,
        key: &Key,
        timer_type: TimerType,
    ) -> Option<(CompactDateTime, Trigger)> {
        match self.attempt(key, timer_type).await {
            Some((time, TimerState::FiringReplaced(next))) => Some((time, next)),
            _ => None,
        }
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
                    TimerState::Scheduled(_) | TimerState::FiringRescheduled(_)
                )
            })
    }

    /// Applies [`Next::To`]. Returns `true` if the timer exists.
    pub(crate) async fn set_state(
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

    /// Ends a rescheduled attempt and keeps its current item.
    pub(crate) async fn idle(&self, key: &Key, time: CompactDateTime, timer_type: TimerType) {
        if let Entry::Occupied(mut occupied) = self.0.entry_async(key.clone()).await
            && let Some(entry) = occupied.get_mut().get_mut(&(time, timer_type))
            && let TimerState::FiringRescheduled(item) = &entry.state
        {
            entry.state = TimerState::Scheduled(*item);
        }
    }

    /// Moves a queued item into the registry before delivery.
    pub(crate) async fn deliver(&self, trigger: &Trigger) {
        if let Entry::Occupied(mut occupied) = self.0.entry_async(trigger.key.clone()).await
            && let Some(ActiveTriggerEntry {
                state:
                    TimerState::Scheduled(item @ Item::Queued)
                    | TimerState::FiringRescheduled(item @ Item::Queued),
            }) = occupied
                .get_mut()
                .get_mut(&(trigger.time, trigger.timer_type))
        {
            *item = Item::Delivered { tag: trigger.tag };
        }
    }

    /// Starts a delivered item's attempt and returns its tag.
    pub(crate) async fn fire(
        &self,
        key: &Key,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> Option<i32> {
        if let Entry::Occupied(mut occupied) = self.0.entry_async(key.clone()).await
            && let Some(entry) = occupied.get_mut().get_mut(&(time, timer_type))
            && let TimerState::Scheduled(Item::Delivered { tag }) = entry.state
        {
            entry.state = TimerState::Firing;
            Some(tag)
        } else {
            None
        }
    }

    /// Sets the tag of a delivered item after its state change.
    pub(crate) async fn retag_delivered(&self, trigger: &Trigger) {
        if let Some(mut entry) = self.0.get_async(&trigger.key).await
            && let Some(ActiveTriggerEntry {
                state: TimerState::Scheduled(Item::Delivered { tag }),
            }) = entry.get_mut().get_mut(&(trigger.time, trigger.timer_type))
        {
            *tag = trigger.tag;
        }
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
                for (&(time, _), entry) in states {
                    s.active = s.active.saturating_add(1);
                    if matches!(
                        entry.state,
                        TimerState::Firing
                            | TimerState::FiringReplaced(_)
                            | TimerState::FiringRescheduled(_)
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
pub(crate) fn transition(prior: Option<&TimerState>, op: TimerOp) -> Transition {
    use EffectOrder::StoreThenMemory;
    use TimerOp as Op;
    use TimerState::{Firing, FiringReplaced, FiringRescheduled, Parked, Scheduled};

    match (op, prior) {
        // Queue the next fire. The attempt keeps its rows. A pending
        // replacement is dropped; `TimerManager::schedule` wrote it first.
        (Op::Schedule | Op::ClearSchedule, Some(Firing | FiringReplaced(_))) => {
            Transition::RESCHEDULE
        }

        // Cancel the replacement before it reaches the store.
        (Op::DropReplacement, Some(FiringReplaced(_))) => Transition::DROP_REPLACEMENT,

        // Keep the newest replacement until the attempt commits.
        (Op::ClearReplaced(next), Some(Firing | FiringReplaced(_))) => Transition {
            next: Next::To(FiringReplaced(next)),
            ..Transition::NONE
        },

        // Cancel the next item and keep the replacement.
        (Op::ClearReplaced(next), Some(FiringRescheduled(_))) => Transition {
            next: Next::To(FiringReplaced(next)),
            queue: QueueEffect::Dequeue,
            ..Transition::NONE
        },

        // Write both rows before the scheduler can load them or queue the timer.
        (Op::Schedule, None) => Transition {
            queue: QueueEffect::Insert,
            store: StoreEffect::Insert,
            ..Transition::SCHEDULE_KEY
        },

        // Keep the queued or delivered tag until fire reads it.
        (Op::Schedule, Some(Scheduled(_))) => Transition {
            next: Next::Keep,
            ..Transition::SCHEDULE_KEY
        },

        // Queue the parked coordinate after its key row is ready.
        (Op::Schedule, Some(Parked)) => Transition {
            queue: QueueEffect::Insert,
            ..Transition::SCHEDULE_KEY
        },

        // Cancel the next fire. The current attempt keeps its rows until commit.
        (Op::Unschedule, Some(FiringRescheduled(_))) => Transition {
            next: Next::To(Firing),
            queue: QueueEffect::Dequeue,
            ..Transition::NONE
        },

        // Cancel the timer before its durable rows disappear.
        (Op::Unschedule, Some(Scheduled(_) | Parked) | None) => Transition::CANCEL,

        // Remove the replaced timer. The caller deletes its rows and emits telemetry.
        (Op::ClearReplaced(_), Some(Scheduled(_) | Parked) | None) => Transition {
            queue: QueueEffect::Remove,
            store: StoreEffect::DeleteSlabRow,
            ..Transition::NONE
        },

        // Rotate the item tag before the next attempt.
        (Op::Complete | Op::Receipt | Op::Retire, Some(FiringRescheduled(_))) => Transition {
            next: Next::Idle,
            store: StoreEffect::UpdateTag,
            queue: QueueEffect::Retag,
            ordering: StoreThenMemory,
            ..Transition::NONE
        },

        // Remove the entry and rows because the attempt committed without a next fire.
        (Op::Complete, Some(Firing | Scheduled(_) | Parked) | None) => Transition {
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

        // The receipt writes the replacement and keeps the old recovery source.
        (Op::Receipt, Some(FiringReplaced(next))) => Transition {
            store: StoreEffect::Replace(next.clone(), RetainOldSlab::Yes),
            ordering: StoreThenMemory,
            announce: Some(Announce::Scheduled),
            ..Transition::PARK
        },

        // Completion writes the replacement and removes the old source.
        (Op::Complete, Some(FiringReplaced(next))) => Transition {
            store: StoreEffect::Replace(next.clone(), RetainOldSlab::No),
            ordering: StoreThenMemory,
            announce: Some(Announce::Scheduled),
            ..Transition::DEACTIVATE
        },

        // Remove the slab row after promotion. The timer has no queue entry.
        (Op::Retire, Some(Firing | Parked) | None) => Transition {
            store: StoreEffect::DeleteSlabRow,
            ..Transition::DEACTIVATE
        },

        // Keep the item so another attempt can fire after abort.
        (Op::Abort, Some(FiringRescheduled(_))) => Transition {
            next: Next::Idle,
            ..Transition::NONE
        },

        // Park the timer. Its rows remain for reload or another schedule.
        (Op::Abort, Some(Scheduled(_))) => Transition {
            queue: QueueEffect::Dequeue,
            ..Transition::PARK
        },

        // Park the failed attempt. Its rows remain, and fire already removed its queue entry.
        (Op::Abort, Some(Firing | FiringReplaced(_))) => Transition::PARK,

        // Queue the timer after the caller's atomic write restores its rows.
        (Op::ClearSchedule, Some(Parked) | None) => Transition {
            next: Next::To(Scheduled(Item::Queued)),
            queue: QueueEffect::Insert,
            ordering: StoreThenMemory,
            ..Transition::NONE
        },

        // A clear on a scheduled timer keeps its item. The caller rewrites the key row.
        // These operations keep the current state and have no other work.
        (Op::ClearSchedule | Op::Retire, Some(Scheduled(_)))
        | (Op::Schedule | Op::ClearSchedule, Some(FiringRescheduled(_)))
        | (Op::Unschedule, Some(Firing))
        | (Op::Unschedule | Op::Retire, Some(FiringReplaced(_)))
        | (Op::Receipt, Some(Scheduled(_) | Parked) | None)
        | (Op::Abort, Some(Parked) | None)
        | (
            Op::DropReplacement,
            Some(Scheduled(_) | Firing | FiringRescheduled(_) | Parked) | None,
        ) => Transition::NONE,
    }
}

#[cfg(test)]
mod tests;
