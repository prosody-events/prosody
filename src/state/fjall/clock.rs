//! The cache clock that stamps and checks frame expiries.

use educe::Educe;
#[cfg(test)]
use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// The cache's `now` source for TTL co-expiry, in milliseconds since the Unix
/// epoch.
///
/// A non-`dyn` seam: production reads the [`Wall`](Self::Wall) clock; a test
/// can pin time with `Fixed` and advance the shared counter past
/// a stamped expiry **without sleeping**, so the TTL-expiry property is
/// deterministic. The cache stamps expiries with the same source it reads them
/// against, so the two never disagree.
#[derive(Clone, Educe)]
#[educe(Debug)]
pub(crate) enum Clock {
    /// The system wall clock.
    Wall,
    /// A test-controlled clock over a shared millisecond counter.
    #[cfg(test)]
    Fixed(#[educe(Debug(ignore))] Arc<AtomicU64>),
}

impl Clock {
    /// The current time in milliseconds since the Unix epoch. The wall arm
    /// saturates a pre-epoch clock to 0 (a misconfigured host only expires
    /// fjall entries early, which self-heals via fall-through).
    #[must_use]
    pub fn now_ms(&self) -> u64 {
        match self {
            Self::Wall => SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX)),
            #[cfg(test)]
            Self::Fixed(now) => now.load(Ordering::Relaxed),
        }
    }
}
