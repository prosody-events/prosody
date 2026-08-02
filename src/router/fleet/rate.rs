//! How fast one destination may be sent to.

use parking_lot::Mutex;
use std::time::Duration;
use tokio::time::Instant;

/// One destination's send pacing.
///
/// Slots bound the work queued against a destination; this bounds how often
/// that work leaves. Pacing is strict with a burst of one: the first send goes
/// at once, and each later send goes one period after the one before it. A
/// destination left idle for many periods gets no burst — the next claim starts
/// from the present, not from the schedule it stopped at. Times are
/// [`tokio::time::Instant`], so a paused-time test observes the pacing exactly.
pub(crate) struct RateLimit {
    period: Duration,
    next: Mutex<Option<Instant>>,
}

impl RateLimit {
    /// A limit of `sends_per_second` sends to one destination.
    ///
    /// A rate of zero yields no pacing at all.
    /// [`DestinationFleet::new`](crate::router::fleet::DestinationFleet::new)
    /// refuses that value before it builds a fleet, so the fallback is
    /// unreachable rather than a silently degraded mode.
    pub(crate) fn new(sends_per_second: u32) -> Self {
        Self {
            period: Duration::from_secs(1)
                .checked_div(sends_per_second)
                .unwrap_or(Duration::ZERO),
            next: Mutex::new(None),
        }
    }

    /// Claims the instant the next send may go at.
    ///
    /// A claimed turn is spent whether or not the send happens, so a
    /// destination receives less than the limit and never more.
    ///
    /// Synchronous by design: the caller sleeps until the instant it gets, so
    /// the lock is never held across an await.
    pub(crate) fn claim(&self) -> Instant {
        let now = Instant::now();
        let mut next = self.next.lock();
        let at = next.map_or(now, |at| at.max(now));
        *next = Some(at + self.period);
        at
    }
}
