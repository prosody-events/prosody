//! Failure rate tracking for defer middleware.
//!
//! Provides [`FailureTracker`] which monitors message processing failure rates
//! within a sliding time window by subscribing to telemetry events. When the
//! failure rate exceeds the configured threshold, deferral is disabled to
//! prevent cascading failures.
//!
//! Uses telemetry subscription with lock-free atomic reads for high
//! performance.

use super::decider::DeferralDecider;
use crate::heartbeat::{Heartbeat, HeartbeatRegistry};
use crate::telemetry::Telemetry;
use crate::telemetry::event::{Data, KeyEvent, KeyState, TelemetryEvent};
use portable_atomic::{AtomicF64, Ordering};
use quanta::Instant;
use std::collections::VecDeque;
use std::fmt::{Debug, Formatter, Result as FmtResult};
use std::sync::Arc;
use std::time::Duration;
use tokio::spawn;
use tokio::sync::broadcast;
use tracing::warn;

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

    /// Checks if deferral should be enabled based on current failure rate.
    ///
    /// Lock-free read from atomic. Returns `true` if the failure rate is
    /// below the threshold (deferral should be enabled), `false` otherwise.
    ///
    /// # Returns
    ///
    /// - `true`: Failure rate is acceptable, deferral enabled
    /// - `false`: Failure rate exceeds threshold, deferral disabled
    ///
    /// # Note
    ///
    /// This method also exists on [`DeferralDecider`] trait. Both are kept
    /// for backward compatibility - existing code calls this inherent method,
    /// while generic code uses the trait method.
    #[must_use]
    #[allow(clippy::same_name_method)]
    pub fn should_defer(&self) -> bool {
        self.failure_rate() < self.threshold
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
                Ok(event) => match event.data {
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
#[allow(clippy::cast_precision_loss)]
fn update_failure_rate(
    failure_rate: &Arc<AtomicF64>,
    successes: &VecDeque<Instant>,
    failures: &VecDeque<Instant>,
) {
    let failure_count = failures.len();
    let total = failure_count + successes.len();

    let rate = if total == 0 {
        0.0
    } else {
        failure_count as f64 / total as f64
    };

    failure_rate.store(rate, Ordering::Relaxed);
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
    #[allow(clippy::same_name_method)]
    fn should_defer(&self) -> bool {
        // Delegate to the existing inherent method for backward compatibility
        self.failure_rate() < self.threshold
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consumer::DemandType;
    use crate::telemetry::Telemetry;
    use crate::{Partition, Topic};
    use std::sync::Arc;
    use tokio::spawn;
    use tokio::task::yield_now;
    use tokio::time::sleep;

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
