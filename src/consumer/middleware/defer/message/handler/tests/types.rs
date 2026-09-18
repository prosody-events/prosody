//! The harness interprets each step against the real store.
//! No event is predicted. Every check reads real state.

use super::context::TimerOp;
use super::{FaultKind, StoreOp};
use crate::timers::datetime::CompactDateTime;
use crate::{Key, Offset};

/// A timer operation that the context recorded.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OutputEvent {
    /// The context scheduled a timer.
    Scheduled {
        /// The timer key.
        key: Key,
        /// The scheduled time.
        time: CompactDateTime,
    },
    /// The context cleared timers.
    Cleared {
        /// The timer key.
        key: Key,
    },
}

/// A fault before one dispatch.
#[derive(Clone, Copy, Debug)]
pub enum Fault {
    /// Fails one store operation.
    Store(StoreOp, FaultKind),
    /// Fails one timer operation.
    Timer(TimerOp, FaultKind),
    /// The timer vanished outside the handler.
    LostTimer,
    /// A loader error precedes a timer error in one dispatch.
    LoaderThenTimer(FaultKind, TimerOp, FaultKind),
}

/// Selects the handler result and initial deferral decision.
#[derive(Clone, Debug)]
pub enum MessageOutcome {
    /// The handler succeeds.
    Success,
    /// The handler fails permanently.
    Permanent,
    /// The handler fails transiently.
    Transient {
        /// Enables initial deferral.
        defer: bool,
    },
    /// The message joins an existing queue.
    Queued,
}

/// A message arrival with a selected outcome.
#[derive(Clone, Debug)]
pub struct MessageEvent {
    /// A fault for the first dispatch.
    pub fault: Option<Fault>,
    /// Index into the key pool.
    pub key_idx: usize,
    /// The message offset.
    pub offset: Offset,
    /// The selected outcome.
    pub outcome: MessageOutcome,
}

/// Selects the result of a deferred reload.
#[derive(Clone, Debug)]
pub enum TimerOutcome {
    /// The handler succeeds.
    Success,
    /// The handler fails permanently.
    Permanent,
    /// The handler fails transiently.
    Transient,
    /// The loader fails permanently.
    LoaderPermanent,
    /// The loader fails transiently.
    LoaderTransient,
}

/// A timer fire for the real queue head.
#[derive(Clone, Debug)]
pub struct TimerEvent {
    /// A fault for the first dispatch.
    pub fault: Option<Fault>,
    /// Index into the key pool.
    pub key_idx: usize,
    /// The selected outcome.
    pub outcome: TimerOutcome,
}

/// An event that the harness interprets from a step.
#[derive(Clone, Debug)]
pub enum TraceEvent {
    /// A message arrives.
    Message(MessageEvent),
    /// A retry timer fires.
    Timer(TimerEvent),
}

/// One random choice. The harness reads the real store and creates a
/// [`TraceEvent`].
#[derive(Clone, Copy, Debug)]
pub struct Step {
    /// Selects a key modulo the key count.
    pub key_idx: u8,
    /// Selects the outcome.
    pub roll: u8,
    /// A fault for the first dispatch.
    pub fault: Option<Fault>,
}

/// A list of steps over a fixed key pool.
#[derive(Clone, Debug)]
pub struct Trace {
    /// Choices that the harness interprets in order.
    pub steps: Vec<Step>,
    /// The number of keys.
    pub key_count: usize,
}
