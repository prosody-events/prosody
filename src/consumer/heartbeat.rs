//! Provides a mechanism to monitor and detect stalled consumer processes.
//!
//! This module implements a heartbeat system to track activity within consumer
//! processes, allowing detection of stalled or unresponsive components.

use crossbeam_utils::CachePadded;
use educe::Educe;
use humantime::format_duration;
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
