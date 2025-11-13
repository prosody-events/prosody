//! Failure rate tracking for defer middleware.
//!
//! Provides [`FailureTracker`] which monitors message processing failure rates
//! within a sliding time window. When the failure rate exceeds the configured
//! threshold, deferral is disabled to prevent cascading failures.

use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

/// Tracks failure rates within a sliding time window.
///
/// Thread-safe tracker that maintains timestamps of successes and failures
/// to calculate failure rate. Used by defer middleware to detect high failure
/// scenarios and disable deferral to prevent cascading failures.
///
/// # Algorithm
///
/// - Maintains two queues: one for successes, one for failures
/// - On each record, removes events older than the time window
/// - Calculates failure rate as: failures / (failures + successes)
/// - Compares against threshold to determine if deferral should be disabled
///
/// # Thread Safety
///
/// Safe to clone and use from multiple threads. All operations are
/// protected by an internal mutex.
#[derive(Clone, Debug)]
pub struct FailureTracker(Arc<Mutex<Inner>>);

/// Internal state for [`FailureTracker`].
#[derive(Debug)]
struct Inner {
    /// Timestamps of recent successful message processing.
    successes: VecDeque<Instant>,
    /// Timestamps of recent failed message processing.
    failures: VecDeque<Instant>,
    /// Time window for failure rate calculation.
    window: Duration,
    /// Failure rate threshold (0.0 to 1.0).
    threshold: f64,
}

impl FailureTracker {
    /// Creates a new failure tracker.
    ///
    /// # Arguments
    ///
    /// * `window` - Time window for tracking events
    /// * `threshold` - Failure rate threshold (0.0 to 1.0) above which deferral
    ///   is disabled
    ///
    /// # Panics
    ///
    /// Panics if threshold is not in the range \[0.0, 1.0\].
    #[must_use]
    pub fn new(window: Duration, threshold: f64) -> Self {
        assert!(
            (0.0_f64..=1.0_f64).contains(&threshold),
            "threshold must be between 0.0 and 1.0"
        );

        Self(Arc::new(Mutex::new(Inner {
            successes: VecDeque::new(),
            failures: VecDeque::new(),
            window,
            threshold,
        })))
    }

    /// Records a successful message processing.
    ///
    /// Adds a timestamp to the success queue and removes events outside
    /// the time window.
    pub fn record_success(&self) {
        let mut inner = self.0.lock();
        let now = Instant::now();

        inner.successes.push_back(now);
        inner.prune(now);
    }

    /// Records a failed message processing.
    ///
    /// Adds a timestamp to the failure queue and removes events outside
    /// the time window.
    pub fn record_failure(&self) {
        let mut inner = self.0.lock();
        let now = Instant::now();

        inner.failures.push_back(now);
        inner.prune(now);
    }

    /// Checks if deferral should be enabled based on current failure rate.
    ///
    /// Returns `true` if the failure rate is below the threshold (deferral
    /// should be enabled), `false` otherwise.
    ///
    /// # Returns
    ///
    /// - `true`: Failure rate is acceptable, deferral enabled
    /// - `false`: Failure rate exceeds threshold, deferral disabled
    #[must_use]
    pub fn should_defer(&self) -> bool {
        let inner = self.0.lock();
        inner.should_defer()
    }

    /// Gets current failure rate for monitoring/metrics.
    ///
    /// # Returns
    ///
    /// Failure rate as a value between 0.0 (no failures) and 1.0 (all
    /// failures). Returns 0.0 if there are no events in the window.
    #[must_use]
    pub fn failure_rate(&self) -> f64 {
        let inner = self.0.lock();
        inner.failure_rate()
    }
}

impl Inner {
    /// Removes events older than the time window.
    fn prune(&mut self, now: Instant) {
        let cutoff = now.checked_sub(self.window).unwrap_or(now);

        // Remove old successes
        while let Some(&ts) = self.successes.front() {
            if ts < cutoff {
                self.successes.pop_front();
            } else {
                break;
            }
        }

        // Remove old failures
        while let Some(&ts) = self.failures.front() {
            if ts < cutoff {
                self.failures.pop_front();
            } else {
                break;
            }
        }
    }

    /// Calculates current failure rate.
    fn failure_rate(&self) -> f64 {
        let total = self.successes.len() + self.failures.len();

        if total == 0 {
            return 0.0;
        }

        #[allow(clippy::cast_precision_loss)]
        let rate = self.failures.len() as f64 / total as f64;
        rate
    }

    /// Checks if deferral should be enabled.
    fn should_defer(&self) -> bool {
        self.failure_rate() < self.threshold
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{Duration, sleep};

    #[test]
    fn test_new_tracker() {
        let tracker = FailureTracker::new(Duration::from_secs(60), 0.9_f64);
        assert!(tracker.should_defer());
        assert!((tracker.failure_rate() - 0.0_f64).abs() < f64::EPSILON);
    }

    #[test]
    #[should_panic(expected = "threshold must be between 0.0 and 1.0")]
    fn test_invalid_threshold() {
        let _tracker = FailureTracker::new(Duration::from_secs(60), 1.5_f64);
    }

    #[test]
    fn test_only_successes() {
        let tracker = FailureTracker::new(Duration::from_secs(60), 0.9_f64);

        tracker.record_success();
        tracker.record_success();
        tracker.record_success();

        assert!(tracker.should_defer());
        assert!((tracker.failure_rate() - 0.0_f64).abs() < f64::EPSILON);
    }

    #[test]
    fn test_only_failures() {
        let tracker = FailureTracker::new(Duration::from_secs(60), 0.9_f64);

        tracker.record_failure();
        tracker.record_failure();
        tracker.record_failure();

        assert!(!tracker.should_defer());
        assert!((tracker.failure_rate() - 1.0_f64).abs() < f64::EPSILON);
    }

    #[test]
    fn test_below_threshold() {
        let tracker = FailureTracker::new(Duration::from_secs(60), 0.5_f64);

        tracker.record_success();
        tracker.record_success();
        tracker.record_failure();

        // 1/3 = 0.333... < 0.5
        assert!(tracker.should_defer());
        assert!((tracker.failure_rate() - 0.333_f64).abs() < 0.01_f64);
    }

    #[test]
    fn test_above_threshold() {
        let tracker = FailureTracker::new(Duration::from_secs(60), 0.5_f64);

        tracker.record_success();
        tracker.record_failure();
        tracker.record_failure();

        // 2/3 = 0.666... > 0.5
        assert!(!tracker.should_defer());
        assert!((tracker.failure_rate() - 0.666_f64).abs() < 0.01_f64);
    }

    #[test]
    fn test_at_threshold() {
        let tracker = FailureTracker::new(Duration::from_secs(60), 0.5_f64);

        tracker.record_success();
        tracker.record_failure();

        // 1/2 = 0.5 == 0.5 (not less than, so should_defer = false)
        assert!(!tracker.should_defer());
        assert!((tracker.failure_rate() - 0.5_f64).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn test_window_expiration() {
        let tracker = FailureTracker::new(Duration::from_millis(100), 0.5_f64);

        // Record failures that will expire
        tracker.record_failure();
        tracker.record_failure();

        // Wait for events to expire
        sleep(Duration::from_millis(150)).await;

        // Record new success
        tracker.record_success();

        // Old failures should be pruned, only success remains
        assert!(tracker.should_defer());
        assert!((tracker.failure_rate() - 0.0_f64).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn test_concurrent_access() {
        let tracker = FailureTracker::new(Duration::from_secs(60), 0.5_f64);

        let tracker_clone = tracker.clone();
        let handle1 = tokio::spawn(async move {
            for _ in 0_i32..10_i32 {
                tracker_clone.record_success();
            }
        });

        let tracker_clone = tracker.clone();
        let handle2 = tokio::spawn(async move {
            for _ in 0_i32..5_i32 {
                tracker_clone.record_failure();
            }
        });

        assert!(handle1.await.is_ok());
        assert!(handle2.await.is_ok());

        // 5 failures / 15 total = 0.333... < 0.5
        assert!(tracker.should_defer());
        assert!((tracker.failure_rate() - 0.333_f64).abs() < 0.01_f64);
    }
}
