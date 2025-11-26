//! Core types for trace-based property testing of defer middleware.
//!
//! Traces describe expected behavior - test inputs become sequences of
//! [`MessageEvent`] and [`TimerEvent`] with explicit outcomes. Verification
//! happens against real store state and [`OutputEvent`] records.

use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::{Key, Offset};

// ============================================================================
// Output Events (recorded by CapturingContext)
// ============================================================================

/// Timer operation recorded by [`CapturingContext`] for verification.
///
/// The middleware schedules and clears timers through [`EventContext`].
/// We capture these operations to verify timer coverage and cleanup invariants.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OutputEvent {
    /// Timer scheduled for key at specified time.
    Scheduled {
        /// Message key that was deferred.
        key: Key,
        /// Scheduled retry time.
        time: CompactDateTime,
    },
    /// Timer cleared for key (queue empty).
    Cleared {
        /// Message key whose timer was cleared.
        key: Key,
    },
}

// ============================================================================
// Message Event Types
// ============================================================================

/// Specifies how the handler should behave and whether to defer.
///
/// This is set by the trace before each message arrives, controlling
/// both handler outcome and deferral decision.
#[derive(Clone, Debug)]
pub enum MessageOutcome {
    /// Handler succeeds - message complete.
    Success,

    /// Handler fails permanently - message complete, no deferral.
    Permanent,

    /// Handler fails transiently.
    Transient {
        /// Maximum backoff for verification (actual: 0..=max due to jitter).
        max_backoff: CompactDuration,
        /// Whether deferral is enabled (controls [`TraceBasedDecider`]).
        defer: bool,
    },

    /// Key already deferred - message queued without handler call.
    Queued,
}

/// Describes a message arrival and expected outcome.
///
/// The test harness executes this by:
/// 1. Setting [`TraceBasedDecider`] state (if `Transient`)
/// 2. Constructing and processing the message
/// 3. Verifying the outcome matches expectations
#[derive(Clone, Debug)]
pub struct MessageEvent {
    /// Index into test key pool (0..N).
    pub key_idx: usize,
    /// Kafka message offset.
    pub offset: Offset,
    /// Expected handler result and deferral decision.
    pub outcome: MessageOutcome,
}

// ============================================================================
// Timer Event Types
// ============================================================================

/// Specifies how the retry should behave.
///
/// No `defer` flag needed - key is already deferred when timer fires.
#[derive(Clone, Debug)]
pub enum TimerOutcome {
    /// Retry succeeds - offset complete, next message or clear.
    Success,

    /// Retry fails permanently - offset complete, next message or clear.
    Permanent,

    /// Retry fails transiently - reschedule with backoff.
    Transient {
        /// Maximum backoff for verification.
        max_backoff: CompactDuration,
    },
}

/// Describes a timer fire and expected retry outcome.
///
/// The test harness executes this by:
/// 1. Verifying FIFO order (offset at queue head)
/// 2. Firing the timer
/// 3. Verifying retry count and timer state
#[derive(Clone, Debug)]
pub struct TimerEvent {
    /// Index into test key pool.
    pub key_idx: usize,
    /// Expected offset at queue head (FIFO verification).
    pub offset: Offset,
    /// Expected retry result.
    pub outcome: TimerOutcome,
}

// ============================================================================
// Trace Types
// ============================================================================

/// A single event in a test trace.
#[derive(Clone, Debug)]
pub enum TraceEvent {
    /// Message arrives for processing.
    Message(MessageEvent),
    /// Retry timer fires.
    Timer(TimerEvent),
}

/// Complete test input - sequence of events with expected outcomes.
///
/// # Validity Invariants
///
/// These are enforced by [`TraceBuilder`] during generation:
///
/// 1. `MessageEvent.key_idx < key_count`
/// 2. `TimerEvent.key_idx < key_count`
/// 3. `TimerEvent` only when key is deferred with matching offset at head
/// 4. `MessageOutcome::Queued` only when key is already deferred
/// 5. `MessageOutcome::{Success,Permanent,Transient}` only when key NOT
///    deferred
///
/// # `QuickCheck` Integration
///
/// Implements `Arbitrary` for random generation with shrinking that preserves
/// validity. See [`generator`] module.
#[derive(Clone, Debug)]
pub struct Trace {
    /// Ordered sequence of events.
    pub events: Vec<TraceEvent>,
    /// Number of distinct keys (1..N).
    pub key_count: usize,
}
