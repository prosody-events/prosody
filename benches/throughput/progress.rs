//! Counts handled messages and turns the count into a measurement.

use color_eyre::eyre::{Result, eyre};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::Notify;
use tokio::time::timeout;

/// The handled-message count of one layer run.
///
/// Redelivery can count a message twice. The run then ends early by the
/// duplicate count, which is small next to the topic size.
pub(crate) struct Progress {
    target: u32,
    handled: AtomicU32,
    first: OnceLock<Instant>,
    done: Notify,
}

/// The timing of one layer run.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Measurement {
    /// From consumer construction to the first handled message: connection,
    /// group join, and assignment.
    pub(crate) startup: Duration,
    /// From the first handled message to the last.
    pub(crate) drain: Duration,
    /// The handled-message count.
    pub(crate) messages: u32,
}

impl Progress {
    pub(crate) fn new(target: u32) -> Self {
        Self {
            target,
            handled: AtomicU32::new(0),
            first: OnceLock::new(),
            done: Notify::new(),
        }
    }

    /// Records one handled message.
    pub(crate) fn record(&self) {
        self.first.get_or_init(Instant::now);
        if self.handled.fetch_add(1, Ordering::AcqRel) + 1 == self.target {
            // `notify_one` stores a permit, so a later wait still completes.
            self.done.notify_one();
        }
    }

    /// Whether every message was handled.
    pub(crate) fn is_done(&self) -> bool {
        self.handled.load(Ordering::Acquire) >= self.target
    }

    /// Measures the run now. `started` marks consumer construction.
    pub(crate) fn measure(&self, started: Instant) -> Measurement {
        let end = Instant::now();
        let first = self.first.get().copied().unwrap_or(end);
        Measurement {
            startup: first.saturating_duration_since(started),
            drain: end.saturating_duration_since(first),
            messages: self.handled.load(Ordering::Acquire),
        }
    }

    /// Waits until every message is handled, then measures the run.
    ///
    /// # Errors
    ///
    /// Returns an error if the run does not finish within `deadline`.
    pub(crate) async fn finished(
        &self,
        started: Instant,
        deadline: Duration,
    ) -> Result<Measurement> {
        timeout(deadline, self.done.notified()).await.map_err(|_| {
            let handled = self.handled.load(Ordering::Acquire);
            eyre!("timed out after {handled} of {} messages", self.target)
        })?;
        Ok(self.measure(started))
    }
}

impl Measurement {
    /// Handled messages per second over the drain.
    pub(crate) fn rate(&self) -> f64 {
        f64::from(self.messages) / self.drain.as_secs_f64().max(f64::EPSILON)
    }
}
