//! Heartbeat monitoring system for detecting stalled processing components.
//!
//! This module provides heartbeat monitoring for both consumer partition
//! processing and timer system operations. The heartbeat system enables
//! detection of stalled or unresponsive components by tracking regular activity
//! signals.
//!
//! # Core Components
//!
//! - [`Heartbeat`]: Individual monitor for a single component
//! - [`HeartbeatRegistry`]: Collection of heartbeats with shared stall
//!   detection
//!
//! # Integration Points
//!
//! - **Partition processing**: Monitors message processing loops for stalls
//! - **Timer system**: Monitors slab loading and trigger scheduling operations
//! - **Health probes**: Used by readiness/liveness endpoints for service health

use crossbeam_utils::CachePadded;
use educe::Educe;
use humantime::format_duration;
use parking_lot::Mutex;
use std::borrow::Cow;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::time::sleep;
use tracing::{error, info};

/// Additional safety margin applied to heartbeat check intervals.
///
/// The heartbeat check interval is calculated as the stall threshold divided by
/// this value, resulting in more frequent checks than the threshold itself.
const HEARTBEAT_MARGIN: u32 = 5;

/// Registry for managing multiple heartbeat monitors with a shared stall
/// threshold.
///
/// Maintains a collection of heartbeats that all use the same stall detection
/// threshold, typically representing components within the same processing
/// context (e.g., partition, segment).
#[derive(Educe, Clone)]
#[educe(Debug)]
pub struct HeartbeatRegistry {
    base_name: String,
    stall_threshold: Duration,

    #[educe(Debug(ignore))]
    heartbeats: Arc<Mutex<Vec<Heartbeat>>>,
}

impl HeartbeatRegistry {
    /// Creates a new heartbeat registry with the specified base name and stall
    /// threshold.
    ///
    /// # Arguments
    ///
    /// * `base_name` - Base name prefix for all heartbeats registered with this
    ///   registry
    /// * `stall_threshold` - Duration of inactivity before considering
    ///   components stalled
    #[must_use]
    pub fn new(base_name: String, stall_threshold: Duration) -> Self {
        Self {
            base_name,
            stall_threshold,
            heartbeats: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Registers a new heartbeat with the configured stall threshold.
    ///
    /// # Arguments
    ///
    /// * `name` - Name suffix for this specific heartbeat
    #[must_use]
    pub fn register(&self, name: &str) -> Heartbeat {
        let mut registry = self.heartbeats.lock();
        let heartbeat = Heartbeat::new(format!("{} {name}", &self.base_name), self.stall_threshold);
        registry.push(heartbeat.clone());
        heartbeat
    }

    /// Checks if any registered heartbeat is currently stalled.
    ///
    /// # Returns
    ///
    /// `true` if at least one heartbeat exceeds its stall threshold, `false`
    /// otherwise
    pub fn any_stalled(&self) -> bool {
        self.heartbeats.lock().iter().any(Heartbeat::is_stalled)
    }

    /// Creates a test heartbeat registry with default settings.
    ///
    /// Only intended for use in tests and examples.
    #[cfg(test)]
    #[must_use]
    pub fn test() -> Self {
        Self::new("test".to_owned(), Duration::from_secs(30))
    }
}

/// A mechanism for monitoring activity and detecting stalled processes.
///
/// `Heartbeat` provides a way to track the liveness of a component by regularly
/// recording activity timestamps and checking if the component has become
/// inactive for longer than a configured threshold.
#[derive(Clone, Educe)]
#[educe(Debug)]
pub struct Heartbeat {
    #[educe(Debug(ignore))]
    inner: Arc<Inner>,
}

/// Internal state for the `Heartbeat` type.
///
/// Contains the shared state that is used to track heartbeat updates
/// and determine if a component has stalled.
#[derive(Educe)]
#[educe(Debug)]
struct Inner {
    /// Heartbeat name
    name: Cow<'static, str>,

    /// Duration of inactivity allowed before considering a partition stalled
    stall_threshold: Duration,

    /// The reference instant when the partition started, used for liveness
    /// checks
    #[educe(Debug(ignore))]
    epoch: Instant,

    /// The duration in milliseconds between the epoch and the last event loop
    /// iteration, used for liveness checks.
    #[educe(Debug(ignore))]
    last_heartbeat: CachePadded<AtomicU64>,

    /// Last stall state used to prevent spamming the logs with stall errors
    last_state: CachePadded<AtomicBool>,
}

impl Heartbeat {
    /// Creates a new heartbeat monitor with the specified name and stall
    /// threshold.
    ///
    /// # Arguments
    ///
    /// * `name` - A name identifier for this heartbeat, used in logging
    /// * `stall_threshold` - The maximum allowed duration of inactivity before
    ///   considering the component stalled
    pub fn new<T>(name: T, stall_threshold: Duration) -> Self
    where
        T: Into<Cow<'static, str>>,
    {
        Self {
            inner: Arc::new(Inner {
                name: name.into(),
                stall_threshold,
                epoch: Instant::now(),
                last_heartbeat: CachePadded::default(),
                last_state: CachePadded::default(),
            }),
        }
    }

    /// Records a heartbeat, indicating that the component is active.
    ///
    /// This should be called regularly by the monitored component to
    /// indicate it's functioning normally.
    pub fn beat(&self) {
        self.inner.last_heartbeat.store(
            Instant::now().duration_since(self.inner.epoch).as_millis() as u64,
            Ordering::Release,
        );
    }

    /// Waits until the next heartbeat check should be performed.
    ///
    /// This method sleeps for a duration calculated as the stall threshold
    /// divided by `HEARTBEAT_MARGIN`.
    pub async fn next(&self) {
        sleep(self.inner.stall_threshold / HEARTBEAT_MARGIN).await;
    }

    /// Checks if the monitored component has stalled.
    ///
    /// Returns `true` if the time since the last heartbeat exceeds the
    /// stall threshold. This method also logs state transitions between
    /// stalled and active states.
    ///
    /// # Returns
    ///
    /// `true` if the component is considered stalled, `false` otherwise
    pub fn is_stalled(&self) -> bool {
        let heartbeat_millis = self.inner.last_heartbeat.load(Ordering::Acquire);
        let heartbeat_duration = Duration::from_millis(heartbeat_millis);
        let last_heartbeat = self.inner.epoch + heartbeat_duration;
        let inactivity = last_heartbeat.elapsed();
        let is_stalled = inactivity > self.inner.stall_threshold;
        let last_state = self.inner.last_state.load(Ordering::Acquire);

        match (last_state, is_stalled) {
            (false, true) => {
                self.inner.last_state.store(is_stalled, Ordering::Release);
                error!(
                    "{} has been inactive for {}, which exceeds the stall threshold of {}",
                    self.inner.name,
                    format_duration(inactivity),
                    format_duration(self.inner.stall_threshold)
                );
            }
            (true, false) => {
                self.inner.last_state.store(is_stalled, Ordering::Release);
                info!("{} is now active", self.inner.name);
            }
            _ => {}
        }

        is_stalled
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{Duration, Instant as TokioInstant, sleep};

    #[tokio::test]
    async fn test_heartbeat_initially_active() {
        let threshold = Duration::from_millis(100);
        let heartbeat = Heartbeat::new("test_initial", threshold);
        heartbeat.beat();
        assert!(
            !heartbeat.is_stalled(),
            "Heartbeat should be active immediately after a beat"
        );
    }

    #[tokio::test]
    async fn test_heartbeat_becomes_stalled() {
        let threshold = Duration::from_millis(100);
        let heartbeat = Heartbeat::new("test_stall", threshold);
        heartbeat.beat();
        sleep(Duration::from_millis(50)).await;
        assert!(
            !heartbeat.is_stalled(),
            "Heartbeat should be active before the stall threshold is exceeded"
        );
        sleep(Duration::from_millis(60)).await;
        assert!(
            heartbeat.is_stalled(),
            "Heartbeat should be stalled after inactivity exceeds the threshold"
        );
    }

    #[tokio::test]
    async fn test_next_sleep_duration() {
        let threshold = Duration::from_millis(100);
        let heartbeat = Heartbeat::new("test_next", threshold);
        let expected_sleep = threshold / HEARTBEAT_MARGIN;
        let start = TokioInstant::now();
        heartbeat.next().await;
        let elapsed = TokioInstant::now().duration_since(start);
        assert!(
            elapsed >= expected_sleep,
            "next() did not sleep for the expected duration (expected at least \
             {expected_sleep:?}, got {elapsed:?})",
        );
    }
}
