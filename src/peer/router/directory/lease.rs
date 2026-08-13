//! The lease a registration lives on.

use std::num::NonZeroU32;
use std::time::Duration;
use thiserror::Error;

use crate::cassandra::MAX_CASSANDRA_TTL_SECS;

/// How long a registration survives without a refresh.
///
/// The range is checked once, in the only constructor. A write site outside it
/// cannot forge a lease that refreshes too frequently or exceeds Cassandra's
/// TTL. Every statement binds
/// [`RegistrationTtl::seconds`] directly. This is a fixed lease rather than a
/// retention window anchored on a natural end time, so a write site needs no
/// lease arithmetic and no overflow check of its own.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RegistrationTtl(NonZeroU32);

impl RegistrationTtl {
    /// The lease a process publishes when an operator asks for none. Long
    /// enough that a refresher paces itself well inside it, short enough that a
    /// dead process's entry expires within half a minute.
    pub(crate) const DEFAULT: Self = Self(NonZeroU32::MIN.saturating_add(29));
    /// Shortest lease a caller can ask for. Below this, a refresh falls due
    /// less than a second after the one before it, and each write's own round
    /// trip then takes a large part of the margin the jitter leaves.
    pub(crate) const MIN: Duration = Duration::from_secs(5);

    /// The lease in seconds, ready to bind to a `USING TTL` placeholder.
    pub(crate) fn seconds(self) -> i32 {
        self.0.get() as i32
    }

    /// The lease as a duration, for callers that pace themselves against it.
    pub(crate) fn duration(self) -> Duration {
        Duration::from_secs(u64::from(self.0.get()))
    }
}

impl TryFrom<Duration> for RegistrationTtl {
    type Error = RegistrationTtlError;

    fn try_from(lease: Duration) -> Result<Self, Self::Error> {
        let seconds = lease.as_secs() + u64::from(lease.subsec_nanos() != 0);
        if lease < Self::MIN || seconds > MAX_CASSANDRA_TTL_SECS as u64 {
            return Err(RegistrationTtlError {
                min: Self::MIN,
                actual: lease,
            });
        }
        // Cassandra accepts whole seconds. Round up so the stored lease never
        // expires before the duration the caller requested.
        Ok(Self(NonZeroU32::new(seconds as u32).ok_or(
            RegistrationTtlError {
                min: Self::MIN,
                actual: lease,
            },
        )?))
    }
}

/// A lease outside the range [`RegistrationTtl`] accepts.
#[derive(Debug, Error)]
#[error("a registration lease must be at least {min:?} and fit Cassandra's TTL, not {actual:?}")]
pub(crate) struct RegistrationTtlError {
    min: Duration,
    actual: Duration,
}
