//! Deferral decision abstraction for defer middleware.
//!
//! Provides [`DeferralDecider`] trait for controlling when transient failures
//! should be deferred for retry. Production code uses [`FailureTracker`] which
//! tracks failure rates. Tests use [`AlwaysDefer`], [`NeverDefer`], or
//! [`TraceBasedDecider`] for deterministic control.

use crate::heartbeat::{Heartbeat, HeartbeatRegistry};
use crate::telemetry::Telemetry;
use crate::telemetry::event::{Data, KeyEvent, KeyState, TelemetryEvent};
use portable_atomic::{AtomicBool, AtomicF64, Ordering};
use quanta::Instant;
use std::collections::VecDeque;
use std::fmt::{Debug, Formatter, Result as FmtResult};
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
/// - [`AlwaysDefer`]: Test double - always enables deferral
/// - [`NeverDefer`]: Test double - always disables deferral
/// - [`TraceBasedDecider`]: Test double - returns value set by test harness
///
/// # Thread Safety
///
/// All implementations must be `Clone + Send + Sync + 'static`.
///
/// # Example
///
/// ```ignore
/// // Production usage - unchanged API
/// let middleware = MessageDeferMiddleware::new(config, consumer_config, ...)?;
///
/// // Test usage - inject test double
/// let decider = TraceBasedDecider::new();
/// let middleware = MessageDeferMiddleware::with_decider(config, ..., decider)?;
/// ```
pub trait DeferralDecider: Clone + Send + Sync + 'static {
    /// Returns `true` if messages should be deferred on transient failure.
    ///
    /// # Production Behavior ([`FailureTracker`])
    ///
    /// Returns `failure_rate < threshold` where:
    /// - `failure_rate` = failures / (failures + successes) in sliding window
    /// - `threshold` = configured threshold (e.g., 0.9)
    ///
    /// # Test Behavior ([`TraceBasedDecider`])
    ///
    /// Returns value set by [`TraceBasedDecider::set_next`] before each message
    /// event.
    fn should_defer(&self) -> bool;
}

// ============================================================================
// Test Doubles
// ============================================================================

/// Always enables deferral.
///
/// Use for testing paths where all transient failures should be deferred.
#[derive(Clone, Copy, Debug, Default)]
pub struct AlwaysDefer;

impl DeferralDecider for AlwaysDefer {
    fn should_defer(&self) -> bool {
        true
    }
}

/// Never enables deferral.
///
/// Use for testing paths where transient failures should propagate.
#[derive(Clone, Copy, Debug, Default)]
pub struct NeverDefer;

impl DeferralDecider for NeverDefer {
    fn should_defer(&self) -> bool {
        false
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
#[derive(Clone, Debug, Default)]
pub struct TraceBasedDecider {
    /// Atomic bool set by test, read by middleware.
    next_decision: Arc<AtomicBool>,
}

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
#[derive(Clone)]
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

impl Debug for FailureTracker {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("FailureTracker")
            .field("failure_rate", &self.failure_rate())
            .field("threshold", &self.threshold)
            .finish_non_exhaustive()
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
mod tests {
    use super::*;
    use crate::consumer::DemandType;
    use crate::telemetry::Telemetry;
    use crate::{Partition, Topic};
    use tokio::task::yield_now;
    use tokio::time::sleep;

    #[test]
    fn always_defer_returns_true() {
        let decider = AlwaysDefer;
        assert!(decider.should_defer());
        assert!(decider.should_defer()); // multiple calls
    }

    #[test]
    fn never_defer_returns_false() {
        let decider = NeverDefer;
        assert!(!decider.should_defer());
        assert!(!decider.should_defer()); // multiple calls
    }

    #[test]
    fn trace_based_decider_defaults_to_true() {
        let decider = TraceBasedDecider::new();
        assert!(decider.should_defer());
    }

    #[test]
    fn trace_based_decider_returns_configured_value() {
        let decider = TraceBasedDecider::new();

        decider.set_next(false);
        assert!(!decider.should_defer());

        decider.set_next(true);
        assert!(decider.should_defer());

        decider.set_next(false);
        assert!(!decider.should_defer());
    }

    #[test]
    fn trace_based_decider_is_clone_safe() {
        let decider1 = TraceBasedDecider::new();
        let decider2 = decider1.clone();

        decider1.set_next(false);
        assert!(!decider2.should_defer()); // Clones share state
    }

    // ========================================================================
    // FailureTracker Tests
    // ========================================================================

    /// Helper to emit success telemetry events
    fn emit_success(telemetry: &Telemetry) {
        let sender = telemetry.partition_sender(Topic::from("test"), Partition::from(0_i32));
        sender.handler_succeeded(Arc::from("test-key"), DemandType::Normal);
    }

    /// Helper to emit failure telemetry events
    fn emit_failure(telemetry: &Telemetry) {
        let sender = telemetry.partition_sender(Topic::from("test"), Partition::from(0_i32));
        sender.handler_failed(Arc::from("test-key"), DemandType::Normal);
    }

    #[tokio::test]
    async fn test_new_tracker() {
        let telemetry = Telemetry::new();
        let tracker = FailureTracker::new(
            Duration::from_secs(60),
            0.9_f64,
            &telemetry,
            &HeartbeatRegistry::test(),
        );
        // Give actor time to initialize
        yield_now().await;
        assert!(tracker.should_defer());
        assert!((tracker.failure_rate() - 0.0_f64).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn test_only_successes() {
        let telemetry = Telemetry::new();
        let tracker = FailureTracker::new(
            Duration::from_secs(60),
            0.9_f64,
            &telemetry,
            &HeartbeatRegistry::test(),
        );

        emit_success(&telemetry);
        emit_success(&telemetry);
        emit_success(&telemetry);

        // Give actor time to process
        yield_now().await;

        assert!(tracker.should_defer());
        assert!((tracker.failure_rate() - 0.0_f64).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn test_only_failures() {
        let telemetry = Telemetry::new();
        let tracker = FailureTracker::new(
            Duration::from_secs(60),
            0.9_f64,
            &telemetry,
            &HeartbeatRegistry::test(),
        );

        emit_failure(&telemetry);
        emit_failure(&telemetry);
        emit_failure(&telemetry);

        // Give actor time to process
        yield_now().await;

        assert!(!tracker.should_defer());
        assert!((tracker.failure_rate() - 1.0_f64).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn test_below_threshold() {
        let telemetry = Telemetry::new();
        let tracker = FailureTracker::new(
            Duration::from_secs(60),
            0.5_f64,
            &telemetry,
            &HeartbeatRegistry::test(),
        );

        emit_success(&telemetry);
        emit_success(&telemetry);
        emit_failure(&telemetry);

        // Give actor time to process
        yield_now().await;

        // 1/3 = 0.333... < 0.5
        assert!(tracker.should_defer());
        assert!((tracker.failure_rate() - 0.333_f64).abs() < 0.01_f64);
    }

    #[tokio::test]
    async fn test_above_threshold() {
        let telemetry = Telemetry::new();
        let tracker = FailureTracker::new(
            Duration::from_secs(60),
            0.5_f64,
            &telemetry,
            &HeartbeatRegistry::test(),
        );

        emit_success(&telemetry);
        emit_failure(&telemetry);
        emit_failure(&telemetry);

        // Give actor time to process
        yield_now().await;

        // 2/3 = 0.666... > 0.5
        assert!(!tracker.should_defer());
        assert!((tracker.failure_rate() - 0.666_f64).abs() < 0.01_f64);
    }

    #[tokio::test]
    async fn test_at_threshold() {
        let telemetry = Telemetry::new();
        let tracker = FailureTracker::new(
            Duration::from_secs(60),
            0.5_f64,
            &telemetry,
            &HeartbeatRegistry::test(),
        );

        emit_success(&telemetry);
        emit_failure(&telemetry);

        // Give actor time to process
        yield_now().await;

        // 1/2 = 0.5 == 0.5 (not less than, so should_defer = false)
        assert!(!tracker.should_defer());
        assert!((tracker.failure_rate() - 0.5_f64).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn test_window_expiration() {
        let telemetry = Telemetry::new();
        let tracker = FailureTracker::new(
            Duration::from_millis(100),
            0.5_f64,
            &telemetry,
            &HeartbeatRegistry::test(),
        );

        // Record failures that will expire
        emit_failure(&telemetry);
        emit_failure(&telemetry);

        // Give actor time to process
        yield_now().await;

        // Wait for events to expire
        sleep(Duration::from_millis(150)).await;

        // Record new success
        emit_success(&telemetry);

        // Give actor time to process
        yield_now().await;

        // Old failures should be pruned, only success remains
        assert!(tracker.should_defer());
        assert!((tracker.failure_rate() - 0.0_f64).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn test_concurrent_access() {
        let telemetry = Telemetry::new();
        let tracker = FailureTracker::new(
            Duration::from_secs(60),
            0.5_f64,
            &telemetry,
            &HeartbeatRegistry::test(),
        );

        let telemetry_clone = telemetry.clone();
        let handle1 = spawn(async move {
            for _ in 0_i32..10_i32 {
                emit_success(&telemetry_clone);
            }
        });

        let telemetry_clone = telemetry.clone();
        let handle2 = spawn(async move {
            for _ in 0_i32..5_i32 {
                emit_failure(&telemetry_clone);
            }
        });

        assert!(handle1.await.is_ok());
        assert!(handle2.await.is_ok());

        // Give actor time to process all events
        yield_now().await;

        // 5 failures / 15 total = 0.333... < 0.5
        assert!(tracker.should_defer());
        assert!((tracker.failure_rate() - 0.333_f64).abs() < 0.01_f64);
    }
}
