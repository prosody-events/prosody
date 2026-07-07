//! Deferral decision abstraction for defer middleware.
//!
//! Provides [`DeferralDecider`] trait for controlling when transient failures
//! should be deferred for retry. Production code uses [`FailureTracker`] which
//! tracks failure rates. Tests use the `cfg(test)` doubles `AlwaysDefer` and
//! `TraceBasedDecider` for deterministic control.

use crate::heartbeat::{Heartbeat, HeartbeatRegistry};
use crate::telemetry::Telemetry;
use crate::telemetry::event::{Data, KeyEvent, KeyState, TelemetryEvent};
use portable_atomic::{AtomicF64, Ordering};
use quanta::Instant;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;
use tokio::spawn;
use tokio::sync::broadcast;
use tracing::warn;

/// Controls whether transient failures should be deferred for retry.
///
/// # Implementors
///
/// - [`FailureTracker`]: Production implementation - defers based on failure
///   rate
/// - `AlwaysDefer` and `TraceBasedDecider`: `cfg(test)` doubles - fixed or
///   harness-controlled decisions
pub trait DeferralDecider: Clone + Send + Sync + 'static {
    /// Returns `true` if messages should be deferred on transient failure.
    ///
    /// # Production Behavior ([`FailureTracker`])
    ///
    /// Returns `failure_rate < threshold` where:
    /// - `failure_rate` = failures / (failures + successes) in sliding window
    /// - `threshold` = configured threshold (e.g., 0.9)
    ///
    /// # Test Behavior (`TraceBasedDecider`)
    ///
    /// Returns value set by `TraceBasedDecider::set_next` before each message
    /// event.
    fn should_defer(&self) -> bool;
}

// ============================================================================
// Test Doubles
// ============================================================================

#[cfg(test)]
use portable_atomic::AtomicBool;

/// Always enables deferral.
///
/// Use for testing paths where all transient failures should be deferred.
#[cfg(test)]
#[derive(Clone, Copy, Debug, Default)]
pub struct AlwaysDefer;

#[cfg(test)]
impl DeferralDecider for AlwaysDefer {
    fn should_defer(&self) -> bool {
        true
    }
}

/// Trace-controlled deferral decisions.
///
/// Test harness sets `next_decision` before each `MessageEvent`,
/// based on the trace's `Transient { defer: bool }` field.
///
/// # Example
///
/// ```ignore
/// let decider = TraceBasedDecider::new();
///
/// // Before processing a message that should be deferred
/// decider.set_next(true);
/// harness.process_message(event);
///
/// // Before processing a message that should NOT be deferred
/// decider.set_next(false);
/// harness.process_message(event);
/// ```
#[cfg(test)]
#[derive(Clone, Debug, Default)]
pub struct TraceBasedDecider {
    /// Atomic bool set by test, read by middleware.
    next_decision: Arc<AtomicBool>,
}

#[cfg(test)]
impl TraceBasedDecider {
    /// Creates a new decider (defaults to `true`).
    #[must_use]
    pub fn new() -> Self {
        Self {
            next_decision: Arc::new(AtomicBool::new(true)),
        }
    }

    /// Sets the return value for the next `should_defer()` call.
    pub fn set_next(&self, value: bool) {
        self.next_decision.store(value, Ordering::Relaxed);
    }
}

#[cfg(test)]
impl DeferralDecider for TraceBasedDecider {
    fn should_defer(&self) -> bool {
        self.next_decision.load(Ordering::Relaxed)
    }
}

// ============================================================================
// FailureTracker - Production Implementation
// ============================================================================

/// Tracks failure rates within a sliding time window by subscribing to
/// telemetry events.
///
/// Uses a lock-free atomic for reading the current failure rate, with a
/// background task processing telemetry events. This eliminates lock
/// contention on reads and removes await points from the hot path.
///
/// # Algorithm
///
/// - Background task subscribes to telemetry broadcast channel
/// - Filters for `HandlerSucceeded` and `HandlerFailed` events
/// - Maintains two queues: one for successes, one for failures
/// - Periodically removes events older than the time window
/// - Calculates failure rate as: failures / (failures + successes)
/// - Stores rate in atomic for lock-free reads
///
/// # Thread Safety
///
/// Safe to clone and use from multiple threads. Reads are lock-free via
/// atomic operations. State updates happen in a background task.
#[derive(Clone, Debug)]
pub struct FailureTracker {
    /// Current failure rate (0.0 to 1.0).
    failure_rate: Arc<AtomicF64>,
    /// Failure rate threshold (0.0 to 1.0).
    threshold: f64,
}

impl FailureTracker {
    /// Creates a new failure tracker.
    ///
    /// Spawns a background task to process telemetry events and registers
    /// a heartbeat with the provided registry for stall detection.
    ///
    /// # Arguments
    ///
    /// * `window` - Time window for tracking events
    /// * `threshold` - Failure rate threshold (0.0 to 1.0) above which deferral
    ///   is disabled
    /// * `telemetry` - Telemetry system for subscribing to handler events
    /// * `heartbeats` - Registry to register the heartbeat for monitoring
    #[must_use]
    pub fn new(
        window: Duration,
        threshold: f64,
        telemetry: &Telemetry,
        heartbeats: &HeartbeatRegistry,
    ) -> Self {
        let failure_rate = Arc::new(AtomicF64::new(0.0_f64));
        let heartbeat = heartbeats.register("failure tracker");
        let telemetry_rx = telemetry.subscribe();

        spawn(run_event_loop(
            Arc::clone(&failure_rate),
            window,
            heartbeat,
            telemetry_rx,
        ));

        Self {
            failure_rate,
            threshold,
        }
    }

    /// Gets current failure rate for monitoring/metrics.
    ///
    /// Lock-free read from atomic.
    ///
    /// # Returns
    ///
    /// Failure rate as a value between 0.0 (no failures) and 1.0 (all
    /// failures). Returns 0.0 if there are no events in the window.
    #[must_use]
    pub fn failure_rate(&self) -> f64 {
        self.failure_rate.load(Ordering::Relaxed)
    }
}

impl DeferralDecider for FailureTracker {
    fn should_defer(&self) -> bool {
        self.failure_rate() < self.threshold
    }
}

/// Runs the telemetry event processing loop.
///
/// Subscribes to telemetry events and updates failure rate based on
/// handler success/failure events. Wakes periodically via heartbeat to
/// prune expired events even when no new events arrive.
async fn run_event_loop(
    failure_rate: Arc<AtomicF64>,
    window: Duration,
    heartbeat: Heartbeat,
    mut telemetry_rx: broadcast::Receiver<TelemetryEvent>,
) {
    let mut successes: VecDeque<Instant> = VecDeque::new();
    let mut failures: VecDeque<Instant> = VecDeque::new();

    loop {
        heartbeat.beat();

        tokio::select! {
            result = telemetry_rx.recv() => match result {
                Ok(event) => match &*event.data {
                    Data::Key(KeyEvent { state: KeyState::HandlerSucceeded, .. }) => {
                        successes.push_back(event.timestamp);
                    }
                    Data::Key(KeyEvent { state: KeyState::HandlerFailed, .. }) => {
                        failures.push_back(event.timestamp);
                    }
                    _ => {}
                },
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    warn!("failure tracker telemetry lagged by {skipped} events");
                }
                Err(broadcast::error::RecvError::Closed) => break,
            },
            () = heartbeat.next() => {} // Periodic wake-up to prune old events
        }

        // Prune expired events and update failure rate after each wake-up
        prune_events(&mut successes, &mut failures, window);
        update_failure_rate(&failure_rate, &successes, &failures);
    }
}

/// Removes events older than the time window.
fn prune_events(
    successes: &mut VecDeque<Instant>,
    failures: &mut VecDeque<Instant>,
    window: Duration,
) {
    let now = Instant::now();

    // If window extends before epoch (underflow), keep everything
    let Some(cutoff) = now.checked_sub(window) else {
        return;
    };

    let is_old = |&ts: &Instant| ts < cutoff;

    // Remove old successes
    while successes.front().is_some_and(is_old) {
        successes.pop_front();
    }

    // Remove old failures
    while failures.front().is_some_and(is_old) {
        failures.pop_front();
    }
}

/// Calculates current failure rate and updates the atomic.
fn update_failure_rate(
    failure_rate: &Arc<AtomicF64>,
    successes: &VecDeque<Instant>,
    failures: &VecDeque<Instant>,
) {
    // Window sizes are bounded and small, so len() fits in u32 without loss.
    // Using u32 allows lossless f64::from() conversion.
    let failure_count = failures.len() as u32;
    let total = failure_count + successes.len() as u32;

    let rate = if total == 0 {
        0.0_f64
    } else {
        f64::from(failure_count) / f64::from(total)
    };

    failure_rate.store(rate, Ordering::Relaxed);
}

#[cfg(test)]
mod tests;
