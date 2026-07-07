//! Exponentially decaying durations for virtual time accounting.
//!
//! Prevents starvation by allowing older execution times to gradually lose
//! influence in scheduling decisions through exponential decay.

#![allow(
    clippy::cast_precision_loss,
    reason = "Nanosecond values stay well below f64's exact integer range (2^53)"
)]

use quanta::Instant;
use std::ops::{Add, AddAssign};
use std::time::Duration;

/// A duration that exponentially decays over time using base-2 exponential
/// decay.
///
/// Tracks when a value was measured and automatically applies decay when
/// queried, preventing unbounded growth of virtual time while maintaining
/// fairness over recent execution history.
#[derive(Clone, Copy, Debug)]
pub struct DecayingDuration<const HALF_LIFE_SECS: u64> {
    value_nanos: u64,
    measured_at: Instant,
}

impl<const HALF_LIFE_SECS: u64> DecayingDuration<HALF_LIFE_SECS> {
    pub const HALF_LIFE: Duration = {
        assert!(HALF_LIFE_SECS > 0, "half-life must be positive");
        Duration::from_secs(HALF_LIFE_SECS)
    };

    const fn new(value: Duration, measured_at: Instant) -> Self {
        Self {
            value_nanos: value.as_nanos() as u64,
            measured_at,
        }
    }

    fn decay_factor_at(&self, instant: Instant) -> f64 {
        let elapsed = instant.saturating_duration_since(self.measured_at);
        let elapsed_secs = elapsed.as_secs_f64();
        let half_life_secs = Self::HALF_LIFE.as_secs_f64();
        (-elapsed_secs / half_life_secs).exp2()
    }

    fn decayed_nanos_at(&self, instant: Instant) -> u64 {
        (self.value_nanos as f64 * self.decay_factor_at(instant)) as u64
    }

    /// Returns the decayed value as of `instant`.
    pub fn at(&self, instant: Instant) -> Duration {
        Duration::from_nanos(self.decayed_nanos_at(instant))
    }
}

impl<const HALF_LIFE_SECS: u64> Add<Duration> for DecayingDuration<HALF_LIFE_SECS> {
    type Output = Self;

    /// Adds a duration to the decayed value at the current instant.
    ///
    /// Decay is applied to `self` before adding `rhs`, modeling exponential
    /// decay where accumulated time naturally decreases and new time is added
    /// at full value.
    fn add(self, rhs: Duration) -> Self {
        let now = Instant::now();
        Self {
            value_nanos: self
                .decayed_nanos_at(now)
                .saturating_add(rhs.as_nanos() as u64),
            measured_at: now,
        }
    }
}

impl<const HALF_LIFE_SECS: u64> AddAssign<Duration> for DecayingDuration<HALF_LIFE_SECS> {
    fn add_assign(&mut self, rhs: Duration) {
        *self = *self + rhs;
    }
}

impl<const HALF_LIFE_SECS: u64> From<Duration> for DecayingDuration<HALF_LIFE_SECS> {
    fn from(duration: Duration) -> Self {
        Self::new(duration, Instant::now())
    }
}

#[cfg(test)]
mod tests;
