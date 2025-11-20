//! Failure rate tracking for defer middleware.
//!
//! Provides [`FailureTracker`] which monitors message processing failure rates
//! within a sliding time window. When the failure rate exceeds the configured
//! threshold, deferral is disabled to prevent cascading failures.
//!
//! Uses an actor pattern with lock-free atomic reads for high performance.

use crate::heartbeat::{Heartbeat, HeartbeatRegistry};
use portable_atomic::{AtomicF64, Ordering};
use quanta::Instant;
use std::collections::VecDeque;
use std::fmt::{Debug, Formatter, Result as FmtResult};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::{select, spawn};

/// Events sent to the failure tracker actor.
#[derive(Debug, Clone, Copy)]
enum Event {
    Success(Instant),
    Failure(Instant),
}

/// Tracks failure rates within a sliding time window using an actor pattern.
///
/// Uses a lock-free atomic for reading the current failure rate, with a
/// background actor task handling state updates. This eliminates lock
/// contention on reads and provides better performance under high load.
///
/// # Algorithm
///
/// - Actor maintains two queues: one for successes, one for failures
/// - On each event, removes events older than the time window
/// - Calculates failure rate as: failures / (failures + successes)
/// - Stores rate in atomic for lock-free reads
///
/// # Thread Safety
///
/// Safe to clone and use from multiple threads. Reads are lock-free via
/// atomic operations. Writes send messages to a background actor.
#[derive(Clone)]
pub struct FailureTracker {
    /// Current failure rate (0.0 to 1.0).
    failure_rate: Arc<AtomicF64>,
    /// Sender for recording events.
    tx: mpsc::UnboundedSender<Event>,
    /// Failure rate threshold (0.0 to 1.0).
    threshold: f64,
}

/// Background processor for tracking failure rates.
struct Processor {
    /// Timestamps of recent successful message processing.
    successes: VecDeque<Instant>,
    /// Timestamps of recent failed message processing.
    failures: VecDeque<Instant>,
    /// Time window for failure rate calculation.
    window: Duration,
    /// Shared failure rate atomic.
    failure_rate: Arc<AtomicF64>,
    /// Receiver for events.
    rx: mpsc::UnboundedReceiver<Event>,
}

impl FailureTracker {
    /// Creates a new failure tracker.
    ///
    /// Spawns a background actor task to process events and registers
    /// a heartbeat with the provided registry for stall detection.
    ///
    /// # Arguments
    ///
    /// * `window` - Time window for tracking events
    /// * `threshold` - Failure rate threshold (0.0 to 1.0) above which deferral
    ///   is disabled
    /// * `heartbeats` - Registry to register the heartbeat for monitoring
    ///
    /// # Panics
    ///
    /// Panics if threshold is not in the range \[0.0, 1.0\].
    #[must_use]
    pub fn new(window: Duration, threshold: f64, heartbeats: &HeartbeatRegistry) -> Self {
        assert!(
            (0.0_f64..=1.0_f64).contains(&threshold),
            "threshold must be between 0.0 and 1.0"
        );

        let (tx, rx) = mpsc::unbounded_channel();
        let failure_rate = Arc::new(AtomicF64::new(0.0_f64));

        let heartbeat = heartbeats.register("failure tracker");

        let processor = Processor {
            successes: VecDeque::new(),
            failures: VecDeque::new(),
            window,
            failure_rate: Arc::clone(&failure_rate),
            rx,
        };

        spawn(processor.run(heartbeat));

        Self {
            failure_rate,
            tx,
            threshold,
        }
    }

    /// Records a successful message processing.
    ///
    /// Sends a success event to the actor. Non-blocking.
    pub fn record_success(&self) {
        // Silently drop if actor is dead - safe default
        let _ = self.tx.send(Event::Success(Instant::now()));
    }

    /// Records a failed message processing.
    ///
    /// Sends a failure event to the actor. Non-blocking.
    pub fn record_failure(&self) {
        // Silently drop if actor is dead - safe default
        let _ = self.tx.send(Event::Failure(Instant::now()));
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
    #[must_use]
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

impl Processor {
    /// Runs the actor event loop.
    ///
    /// Processes events until the channel is closed. Batches multiple events
    /// to amortize prune and update operations. Wakes periodically via
    /// heartbeat to prune expired events even when no new events arrive.
    async fn run(mut self, heartbeat: Heartbeat) {
        loop {
            heartbeat.beat();

            select! {
                result = self.rx.recv() => {
                    let Some(event) = result else {
                        break;  // Channel closed, exit loop
                    };

                    self.record_event(event);

                    // Batch process any additional pending events
                    while let Ok(event) = self.rx.try_recv() {
                        self.record_event(event);
                    }
                }

                () = heartbeat.next() => {
                    // Periodic wake-up to prune old events even when idle
                }
            }

            // Prune expired events and update failure rate after each wake-up
            self.prune(Instant::now());
            self.update_failure_rate();
        }
    }

    /// Records an event to the appropriate queue.
    fn record_event(&mut self, event: Event) {
        match event {
            Event::Success(ts) => self.successes.push_back(ts),
            Event::Failure(ts) => self.failures.push_back(ts),
        }
    }

    /// Removes events older than the time window.
    fn prune(&mut self, now: Instant) {
        // If window extends before epoch (underflow), keep everything
        let Some(cutoff) = now.checked_sub(self.window) else {
            return;
        };

        let is_old = |&ts: &Instant| ts < cutoff;

        // Remove old successes
        while self.successes.front().is_some_and(is_old) {
            self.successes.pop_front();
        }

        // Remove old failures
        while self.failures.front().is_some_and(is_old) {
            self.failures.pop_front();
        }
    }

    /// Calculates current failure rate and updates the atomic.
    #[allow(clippy::cast_precision_loss)]
    fn update_failure_rate(&self) {
        let failures = self.failures.len();
        let total = failures + self.successes.len();

        let rate = match total {
            0 => 0.0_f64,
            _ => failures as f64 / total as f64,
        };

        self.failure_rate.store(rate, Ordering::Relaxed);
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

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::spawn;
    use tokio::task::yield_now;
    use tokio::time::sleep;

    #[tokio::test]
    async fn test_new_tracker() {
        let tracker =
            FailureTracker::new(Duration::from_secs(60), 0.9_f64, &HeartbeatRegistry::test());
        // Give actor time to initialize
        yield_now().await;
        assert!(tracker.should_defer());
        assert!((tracker.failure_rate() - 0.0_f64).abs() < f64::EPSILON);
    }

    #[test]
    #[should_panic(expected = "threshold must be between 0.0 and 1.0")]
    fn test_invalid_threshold() {
        let _tracker =
            FailureTracker::new(Duration::from_secs(60), 1.5_f64, &HeartbeatRegistry::test());
    }

    #[tokio::test]
    async fn test_only_successes() {
        let tracker =
            FailureTracker::new(Duration::from_secs(60), 0.9_f64, &HeartbeatRegistry::test());

        tracker.record_success();
        tracker.record_success();
        tracker.record_success();

        // Give actor time to process
        yield_now().await;

        assert!(tracker.should_defer());
        assert!((tracker.failure_rate() - 0.0_f64).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn test_only_failures() {
        let tracker =
            FailureTracker::new(Duration::from_secs(60), 0.9_f64, &HeartbeatRegistry::test());

        tracker.record_failure();
        tracker.record_failure();
        tracker.record_failure();

        // Give actor time to process
        yield_now().await;

        assert!(!tracker.should_defer());
        assert!((tracker.failure_rate() - 1.0_f64).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn test_below_threshold() {
        let tracker =
            FailureTracker::new(Duration::from_secs(60), 0.5_f64, &HeartbeatRegistry::test());

        tracker.record_success();
        tracker.record_success();
        tracker.record_failure();

        // Give actor time to process
        yield_now().await;

        // 1/3 = 0.333... < 0.5
        assert!(tracker.should_defer());
        assert!((tracker.failure_rate() - 0.333_f64).abs() < 0.01_f64);
    }

    #[tokio::test]
    async fn test_above_threshold() {
        let tracker =
            FailureTracker::new(Duration::from_secs(60), 0.5_f64, &HeartbeatRegistry::test());

        tracker.record_success();
        tracker.record_failure();
        tracker.record_failure();

        // Give actor time to process
        yield_now().await;

        // 2/3 = 0.666... > 0.5
        assert!(!tracker.should_defer());
        assert!((tracker.failure_rate() - 0.666_f64).abs() < 0.01_f64);
    }

    #[tokio::test]
    async fn test_at_threshold() {
        let tracker =
            FailureTracker::new(Duration::from_secs(60), 0.5_f64, &HeartbeatRegistry::test());

        tracker.record_success();
        tracker.record_failure();

        // Give actor time to process
        yield_now().await;

        // 1/2 = 0.5 == 0.5 (not less than, so should_defer = false)
        assert!(!tracker.should_defer());
        assert!((tracker.failure_rate() - 0.5_f64).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn test_window_expiration() {
        let tracker = FailureTracker::new(
            Duration::from_millis(100),
            0.5_f64,
            &HeartbeatRegistry::test(),
        );

        // Record failures that will expire
        tracker.record_failure();
        tracker.record_failure();

        // Give actor time to process
        yield_now().await;

        // Wait for events to expire
        sleep(Duration::from_millis(150)).await;

        // Record new success
        tracker.record_success();

        // Give actor time to process
        yield_now().await;

        // Old failures should be pruned, only success remains
        assert!(tracker.should_defer());
        assert!((tracker.failure_rate() - 0.0_f64).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn test_concurrent_access() {
        let tracker =
            FailureTracker::new(Duration::from_secs(60), 0.5_f64, &HeartbeatRegistry::test());

        let tracker_clone = tracker.clone();
        let handle1 = spawn(async move {
            for _ in 0_i32..10_i32 {
                tracker_clone.record_success();
            }
        });

        let tracker_clone = tracker.clone();
        let handle2 = spawn(async move {
            for _ in 0_i32..5_i32 {
                tracker_clone.record_failure();
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
