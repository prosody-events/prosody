//! Core types for trace-based property testing of timer defer middleware.
//!
//! Traces describe expected behavior - test inputs become sequences of
//! [`ApplicationTimerEvent`] and [`DeferredTimerEvent`] with explicit outcomes.
//!
//! # Differences from Message Defer Types
//!
//! Timer defer differs from message defer in key ways:
//! - No Kafka loader: Timers are stored directly (with span context), not
//!   reloaded from Kafka
//! - Original time preservation: Handler receives `original_time`, not retry
//!   time
//! - Span restoration: Each retry creates a fresh span linked to stored context

use crate::timers::datetime::CompactDateTime;

// ============================================================================
// Application Timer Event Types
// ============================================================================

/// Specifies how the handler should behave for an application timer.
///
/// This is set by the trace before each timer fires, controlling
/// both handler outcome and deferral decision.
#[derive(Clone, Debug)]
pub enum ApplicationTimerOutcome {
    /// Handler succeeds - timer complete.
    Success,

    /// Handler fails permanently - timer complete, no deferral.
    Permanent,

    /// Handler fails transiently, deferral decision specified.
    Transient {
        /// Whether deferral is enabled (controls [`TraceBasedDecider`]).
        defer: bool,
    },

    /// Key already deferred - timer queued without handler call.
    Queued,
}

/// Describes an application timer fire and expected outcome.
///
/// The test harness executes this by:
/// 1. Setting [`TraceBasedDecider`] state (if `Transient`)
/// 2. Constructing and firing the timer
/// 3. Verifying the outcome matches expectations
#[derive(Clone, Debug)]
pub struct ApplicationTimerEvent {
    /// Index into test key pool (0..N).
    pub key_idx: usize,
    /// Original fire time of the timer.
    pub time: CompactDateTime,
    /// Expected handler result and deferral decision.
    pub outcome: ApplicationTimerOutcome,
}

// ============================================================================
// Deferred Timer Retry Event Types
// ============================================================================

/// Specifies how the retry should behave when a `DeferredTimer` fires.
///
/// No `defer` flag needed - key is already deferred when timer fires.
/// No loader variants - timers are stored directly, not reloaded from Kafka.
#[derive(Clone, Debug)]
pub enum DeferredTimerOutcome {
    /// Retry succeeds - timer complete, next timer or clear.
    Success,

    /// Retry fails permanently - timer complete, next timer or clear.
    /// Error is propagated but queue advances.
    Permanent,

    /// Retry fails transiently - reschedule with backoff.
    Transient,
}

/// Describes a `DeferredTimer` retry fire and expected outcome.
///
/// The test harness executes this by:
/// 1. Verifying FIFO order (timer with earliest `original_time` at queue head)
/// 2. Firing the `DeferredTimer`
/// 3. Verifying handler receives correct `original_time` (not retry time)
/// 4. Verifying retry count and timer state
#[derive(Clone, Debug)]
pub struct DeferredTimerEvent {
    /// Index into test key pool.
    pub key_idx: usize,
    /// Expected `original_time` at queue head (FIFO verification).
    pub expected_time: CompactDateTime,
    /// Expected retry result.
    pub outcome: DeferredTimerOutcome,
}

// ============================================================================
// Trace Types
// ============================================================================

/// A single event in a timer defer test trace.
#[derive(Clone, Debug)]
pub enum TimerTraceEvent {
    /// Application timer fires for processing.
    ApplicationTimer(ApplicationTimerEvent),
    /// `DeferredTimer` retry fires.
    DeferredTimer(DeferredTimerEvent),
}

/// Complete test input - sequence of timer events with expected outcomes.
///
/// # Validity Invariants
///
/// These must be enforced during generation:
///
/// 1. `ApplicationTimerEvent.key_idx < key_count`
/// 2. `DeferredTimerEvent.key_idx < key_count`
/// 3. `DeferredTimerEvent` only when key is deferred with matching time at head
/// 4. `ApplicationTimerOutcome::Queued` only when key is already deferred
/// 5. `ApplicationTimerOutcome::{Success,Permanent,Transient}` only when key
///    NOT deferred
///
/// # `QuickCheck` Integration
///
/// Implements `Arbitrary` for random generation with shrinking that preserves
/// validity. See [`properties`] module.
#[derive(Clone, Debug)]
pub struct TimerTrace {
    /// Ordered sequence of events.
    pub events: Vec<TimerTraceEvent>,
    /// Number of distinct keys (1..N).
    pub key_count: usize,
}
