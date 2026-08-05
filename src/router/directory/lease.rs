//! The lease a registration lives on.

use std::time::Duration;
use thiserror::Error;

/// How long a registration survives without a refresh.
///
/// The range is checked once, in the only constructor, and this module holds
/// no other way to build one. A write site outside it therefore cannot forge a
/// lease of zero, a negative one, or one above Cassandra's maximum TTL: the
/// value is always a positive number of seconds, and every statement binds
/// [`RegistrationTtl::seconds`] directly. This is a fixed lease rather than a
/// retention window anchored on a natural end time, so a write site needs no
/// lease arithmetic and no overflow check of its own.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RegistrationTtl(i32);

impl RegistrationTtl {
    /// The lease a process publishes when an operator asks for none. Long
    /// enough that a refresher paces itself well inside it, short enough that a
    /// dead process's row expires within half a minute.
    pub(crate) const DEFAULT: Self = Self(30);
    /// Longest lease a caller can ask for. A dead process stays resolvable for
    /// at most this long, and each stale resolution costs one dropped response.
    pub(crate) const MAX: Duration = Duration::from_hours(1);
    /// Shortest lease a caller can ask for. Below this, a refresh falls due
    /// less than a second after the one before it, and each write's own round
    /// trip then takes a large part of the margin the jitter leaves.
    pub(crate) const MIN: Duration = Duration::from_secs(5);

    /// The lease in seconds, ready to bind to a `USING TTL` placeholder.
    pub(crate) const fn seconds(self) -> i32 {
        self.0
    }

    /// The lease as a duration, for callers that pace themselves against it.
    pub(crate) fn duration(self) -> Duration {
        Duration::from_secs(u64::from(self.0.unsigned_abs()))
    }
}

impl TryFrom<Duration> for RegistrationTtl {
    type Error = RegistrationTtlError;

    fn try_from(lease: Duration) -> Result<Self, Self::Error> {
        if lease < Self::MIN || lease > Self::MAX {
            return Err(RegistrationTtlError {
                min: Self::MIN,
                max: Self::MAX,
                actual: lease,
            });
        }
        // The check above caps the value at 3600, so the cast cannot truncate.
        Ok(Self(lease.as_secs() as i32))
    }
}

/// A lease outside the range [`RegistrationTtl`] accepts.
#[derive(Debug, Error)]
#[error("a registration lease must be between {min:?} and {max:?}, not {actual:?}")]
pub(crate) struct RegistrationTtlError {
    min: Duration,
    max: Duration,
    actual: Duration,
}
