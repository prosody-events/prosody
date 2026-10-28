//! The timer state machine. [`transition`] is its single authority.

use super::{ActiveTriggerEntry, Queued, Row, TimerState};

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
///
/// [`ActiveTriggers`]: super::ActiveTriggers
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
    /// Runs after the durable write: the queue effect, then the new state.
    Update {
        /// New registry state; `None` leaves the entry untouched.
        next_state: Option<TimerState>,
        /// Scheduler effect to run before the state change.
        queue: QueueEffect,
    },
    /// Queues the timer in this state around the durable write, through
    /// `TriggerScheduler::insert`.
    Insert(Queued),
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

/// The timer state machine: resolves a prior entry and a [`TimerOp`] to the
/// exact effects to apply.
///
/// This match is the single authority for timer lifecycle behavior — every
/// `(op, prior)` pair resolves to exactly one row here, and rows that must
/// do nothing resolve to the explicit no-op group at the bottom. A `None`
/// prior means the timer is absent from [`ActiveTriggers`]: never scheduled,
/// already removed, or its slab not yet loaded (absent timers still accept
/// `Schedule`/`Unschedule` so durable rows stay authoritative).
///
/// [`ActiveTriggers`]: super::ActiveTriggers
pub(crate) fn transition(prior: Option<ActiveTriggerEntry>, op: TimerOp) -> Transition {
    use TimerState::{Aborted, Firing, FiringRescheduled, Scheduled};

    /// A row with every effect at its no-op value.
    const NOOP: Transition = Transition {
        memory: update(None, QueueEffect::None),
        store: StoreEffect::None,
        announce: None,
    };
    /// A schedule of a timer that is not queued.
    const SCHEDULE: Transition = Transition {
        memory: MemoryEffect::Insert(Queued::Scheduled),
        store: StoreEffect::Insert,
        announce: Some(Announce::Scheduled),
    };
    /// A schedule during the timer's own delivery.
    const RESCHEDULE: Transition = Transition {
        memory: MemoryEffect::Insert(Queued::FiringRescheduled),
        store: StoreEffect::Insert,
        ..NOOP
    };

    const fn update(next_state: Option<TimerState>, queue: QueueEffect) -> MemoryEffect {
        MemoryEffect::Update { next_state, queue }
    }

    let unconfirmed = prior.is_some_and(|entry| entry.row == Row::Unconfirmed);
    match (op, prior.map(|entry| entry.state)) {
        // Each new queued attempt has one tag, held before its row is written.
        // A queued timer with an unconfirmed row writes the row again.
        (TimerOp::Schedule, Some(Firing)) => RESCHEDULE,
        (TimerOp::Schedule, Some(FiringRescheduled)) if unconfirmed => RESCHEDULE,
        (TimerOp::Schedule, Some(Aborted) | None) => SCHEDULE,
        (TimerOp::Schedule, Some(Scheduled)) if unconfirmed => SCHEDULE,
        (TimerOp::ClearSchedule, Some(Firing)) => Transition {
            memory: MemoryEffect::Insert(Queued::FiringRescheduled),
            ..NOOP
        },

        // Cancel a reschedule: back to `Firing`, drop the queued re-fire.
        // The in-flight delivery completes normally without firing again.
        (TimerOp::Unschedule | TimerOp::ClearReplaced, Some(FiringRescheduled)) => Transition {
            memory: update(Some(Firing), QueueEffect::Dequeue),
            ..NOOP
        },

        // Plain cancel: delete the row, then remove the timer from memory.
        (TimerOp::Unschedule, Some(Scheduled | Aborted) | None) => Transition {
            memory: update(None, QueueEffect::Remove),
            store: StoreEffect::Delete,
            announce: Some(Announce::Cancelled),
        },

        // A timer replaced by `clear_and_schedule`: as `Unschedule`, minus
        // the row delete and telemetry the caller owns.
        (TimerOp::ClearReplaced, Some(Scheduled | Aborted) | None) => Transition {
            memory: update(None, QueueEffect::Remove),
            ..NOOP
        },

        // The replacement already has durable rows and a queue entry.
        (TimerOp::Complete | TimerOp::Abort, Some(FiringRescheduled)) => Transition {
            memory: update(Some(Scheduled), QueueEffect::None),
            ..NOOP
        },

        // Commit anything else: delete the row, then drop the registry entry
        // (firing already popped the queue entry).
        (TimerOp::Complete, Some(Firing | Scheduled | Aborted) | None) => Transition {
            memory: update(None, QueueEffect::Deactivate),
            store: StoreEffect::Delete,
            ..NOOP
        },

        // Abort a queued timer: keep the row for recovery, drop the queue
        // entry.
        (TimerOp::Abort, Some(Scheduled)) => Transition {
            memory: update(Some(Aborted), QueueEffect::Dequeue),
            ..NOOP
        },

        // Abort a firing timer: keep the row; firing already dequeued it.
        (TimerOp::Abort, Some(Firing)) => Transition {
            memory: update(Some(Aborted), QueueEffect::None),
            ..NOOP
        },

        // The caller owns the atomic write of the replacement.
        (TimerOp::ClearSchedule, Some(Aborted) | None) => Transition {
            memory: MemoryEffect::Insert(Queued::Scheduled),
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
