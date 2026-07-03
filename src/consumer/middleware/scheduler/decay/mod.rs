//! Exponentially decaying durations for virtual time accounting.
//!
//! Prevents starvation by allowing older execution times to gradually lose
//! influence in scheduling decisions through exponential decay.

#![allow(
    clippy::cast_precision_loss,
    reason = "Nanosecond values stay well below f64's exact integer range (2^53)"
)]

use quanta::Instant;
use std::cmp::Ordering;
use std::iter::Sum;
use std::ops::{Add, AddAssign, Div, DivAssign, Mul, MulAssign, Sub, SubAssign};
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

    pub const fn new(value: Duration, measured_at: Instant) -> Self {
        Self {
            value_nanos: value.as_nanos() as u64,
            measured_at,
        }
    }

    pub fn is_zero(&self, instant: Instant) -> bool {
        self.decayed_nanos_at(instant) == 0
    }

    pub const fn from_nanos(nanos: u64, measured_at: Instant) -> Self {
        Self {
            value_nanos: nanos,
            measured_at,
        }
    }

    pub const fn from_micros(micros: u64, measured_at: Instant) -> Self {
        Self::from_nanos(micros.saturating_mul(1_000), measured_at)
    }

    pub const fn from_millis(millis: u64, measured_at: Instant) -> Self {
        Self::from_nanos(millis.saturating_mul(1_000_000), measured_at)
    }

    pub const fn from_secs(secs: u64, measured_at: Instant) -> Self {
        Self::from_nanos(secs.saturating_mul(1_000_000_000), measured_at)
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

    pub fn at(&self, instant: Instant) -> Duration {
        Duration::from_nanos(self.decayed_nanos_at(instant))
    }
}

impl<const HALF_LIFE_SECS: u64> Add for DecayingDuration<HALF_LIFE_SECS> {
    type Output = Self;

    fn add(self, rhs: Self) -> Self {
        let instant = self.measured_at.max(rhs.measured_at);
        let self_nanos = (self.value_nanos as f64 * self.decay_factor_at(instant)) as u64;
        let rhs_nanos = (rhs.value_nanos as f64 * rhs.decay_factor_at(instant)) as u64;
        Self {
            value_nanos: self_nanos.saturating_add(rhs_nanos),
            measured_at: instant,
        }
    }
}

impl<const HALF_LIFE_SECS: u64> Sub for DecayingDuration<HALF_LIFE_SECS> {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self {
        let instant = self.measured_at.max(rhs.measured_at);
        let self_nanos = (self.value_nanos as f64 * self.decay_factor_at(instant)) as u64;
        let rhs_nanos = (rhs.value_nanos as f64 * rhs.decay_factor_at(instant)) as u64;
        Self {
            value_nanos: self_nanos.saturating_sub(rhs_nanos),
            measured_at: instant,
        }
    }
}

impl<const HALF_LIFE_SECS: u64> Mul<u32> for DecayingDuration<HALF_LIFE_SECS> {
    type Output = Self;

    fn mul(self, rhs: u32) -> Self {
        Self {
            value_nanos: self.value_nanos.saturating_mul(u64::from(rhs)),
            measured_at: self.measured_at,
        }
    }
}

impl<const HALF_LIFE_SECS: u64> Div<u32> for DecayingDuration<HALF_LIFE_SECS> {
    type Output = Self;

    fn div(self, rhs: u32) -> Self {
        Self {
            value_nanos: self.value_nanos / u64::from(rhs),
            measured_at: self.measured_at,
        }
    }
}

impl<const HALF_LIFE_SECS: u64> PartialEq for DecayingDuration<HALF_LIFE_SECS> {
    fn eq(&self, other: &Self) -> bool {
        let instant = self.measured_at.max(other.measured_at);
        self.at(instant) == other.at(instant)
    }
}

impl<const HALF_LIFE_SECS: u64> Eq for DecayingDuration<HALF_LIFE_SECS> {}

impl<const HALF_LIFE_SECS: u64> PartialOrd for DecayingDuration<HALF_LIFE_SECS> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<const HALF_LIFE_SECS: u64> Ord for DecayingDuration<HALF_LIFE_SECS> {
    fn cmp(&self, other: &Self) -> Ordering {
        let instant = self.measured_at.max(other.measured_at);
        self.at(instant).cmp(&other.at(instant))
    }
}

impl<const HALF_LIFE_SECS: u64> Default for DecayingDuration<HALF_LIFE_SECS> {
    fn default() -> Self {
        Self {
            value_nanos: 0,
            measured_at: Instant::now(),
        }
    }
}

impl<const HALF_LIFE_SECS: u64> Add<Duration> for DecayingDuration<HALF_LIFE_SECS> {
    type Output = Self;

    /// Adds a duration to the decayed value at the current instant.
    ///
    /// Decay is applied to `self` before adding `rhs`, modeling exponential
    /// decay where accumulated time naturally decreases and new time is added
    /// at full value.
    #[expect(
        clippy::suspicious_arithmetic_impl,
        reason = "Intentional: decay applied before add for exponential decay semantics"
    )]
    fn add(self, rhs: Duration) -> Self {
        let now = Instant::now();
        let decayed_nanos = (self.value_nanos as f64 * self.decay_factor_at(now)) as u64;
        Self {
            value_nanos: decayed_nanos.saturating_add(rhs.as_nanos() as u64),
            measured_at: now,
        }
    }
}

impl<const HALF_LIFE_SECS: u64> Sub<Duration> for DecayingDuration<HALF_LIFE_SECS> {
    type Output = Self;

    /// Subtracts a duration from the decayed value at the current instant.
    ///
    /// Decay is applied to `self` before subtracting `rhs`, modeling
    /// exponential decay where accumulated time naturally decreases and
    /// subtraction happens against the current decayed value.
    #[expect(
        clippy::suspicious_arithmetic_impl,
        reason = "Intentional: decay applied before sub for exponential decay semantics"
    )]
    fn sub(self, rhs: Duration) -> Self {
        let now = Instant::now();
        let decayed_nanos = (self.value_nanos as f64 * self.decay_factor_at(now)) as u64;
        Self {
            value_nanos: decayed_nanos.saturating_sub(rhs.as_nanos() as u64),
            measured_at: now,
        }
    }
}

impl<const HALF_LIFE_SECS: u64> AddAssign for DecayingDuration<HALF_LIFE_SECS> {
    fn add_assign(&mut self, rhs: Self) {
        *self = *self + rhs;
    }
}

impl<const HALF_LIFE_SECS: u64> AddAssign<Duration> for DecayingDuration<HALF_LIFE_SECS> {
    fn add_assign(&mut self, rhs: Duration) {
        *self = *self + rhs;
    }
}

impl<const HALF_LIFE_SECS: u64> SubAssign for DecayingDuration<HALF_LIFE_SECS> {
    fn sub_assign(&mut self, rhs: Self) {
        *self = *self - rhs;
    }
}

impl<const HALF_LIFE_SECS: u64> SubAssign<Duration> for DecayingDuration<HALF_LIFE_SECS> {
    fn sub_assign(&mut self, rhs: Duration) {
        *self = *self - rhs;
    }
}

impl<const HALF_LIFE_SECS: u64> MulAssign<u32> for DecayingDuration<HALF_LIFE_SECS> {
    fn mul_assign(&mut self, rhs: u32) {
        *self = *self * rhs;
    }
}

impl<const HALF_LIFE_SECS: u64> DivAssign<u32> for DecayingDuration<HALF_LIFE_SECS> {
    fn div_assign(&mut self, rhs: u32) {
        *self = *self / rhs;
    }
}

impl<const HALF_LIFE_SECS: u64> Sum for DecayingDuration<HALF_LIFE_SECS> {
    fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
        iter.fold(Self::default(), |acc, x| acc + x)
    }
}

impl<'a, const HALF_LIFE_SECS: u64> Sum<&'a DecayingDuration<HALF_LIFE_SECS>>
    for DecayingDuration<HALF_LIFE_SECS>
{
    fn sum<I: Iterator<Item = &'a Self>>(iter: I) -> Self {
        iter.copied().sum()
    }
}

impl<const HALF_LIFE_SECS: u64> From<Duration> for DecayingDuration<HALF_LIFE_SECS> {
    fn from(duration: Duration) -> Self {
        Self::new(duration, Instant::now())
    }
}

impl<const HALF_LIFE_SECS: u64> From<DecayingDuration<HALF_LIFE_SECS>> for Duration {
    fn from(decaying: DecayingDuration<HALF_LIFE_SECS>) -> Self {
        decaying.at(Instant::now())
    }
}

#[cfg(test)]
mod tests;
