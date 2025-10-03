#![allow(clippy::cast_precision_loss)]

use quanta::Instant;
use std::cmp::Ordering;
use std::iter::Sum;
use std::ops::{Add, AddAssign, Div, DivAssign, Mul, MulAssign, Sub, SubAssign};
use std::time::Duration;

#[derive(Clone, Copy, Debug)]
pub struct DecayingDuration<const HALF_LIFE_SECS: u64> {
    value_nanos: u64,
    measured_at: Instant,
}

impl<const HALF_LIFE_SECS: u64> DecayingDuration<HALF_LIFE_SECS> {
    pub const HALF_LIFE: Duration = Duration::from_secs(HALF_LIFE_SECS);

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

    #[allow(clippy::suspicious_arithmetic_impl)]
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

    #[allow(clippy::suspicious_arithmetic_impl)]
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
mod tests {
    use super::*;
    use std::mem::size_of;

    type TestDecayingDuration = DecayingDuration<60>;

    #[test]
    fn test_size_of_decaying_duration() {
        assert_eq!(size_of::<DecayingDuration<60>>(), 16);
    }

    #[test]
    fn test_new_and_at() {
        let now = Instant::now();
        let duration = Duration::from_secs(100);
        let decaying = TestDecayingDuration::new(duration, now);

        assert_eq!(decaying.at(now), duration);
    }

    #[test]
    fn test_decay_after_half_life() {
        let now = Instant::now();
        let duration = Duration::from_secs(100);
        let decaying = TestDecayingDuration::new(duration, now);

        let after_half_life = now + Duration::from_secs(60);
        let decayed = decaying.at(after_half_life);

        assert!((decayed.as_secs_f64() - 50.0_f64).abs() < 0.1_f64);
    }

    #[test]
    fn test_decay_after_two_half_lives() {
        let now = Instant::now();
        let duration = Duration::from_secs(100);
        let decaying = TestDecayingDuration::new(duration, now);

        let after_two_half_lives = now + Duration::from_secs(120);
        let decayed = decaying.at(after_two_half_lives);

        assert!((decayed.as_secs_f64() - 25.0_f64).abs() < 0.1_f64);
    }

    #[test]
    fn test_is_zero() {
        let now = Instant::now();
        let zero = TestDecayingDuration::from_nanos(0, now);
        let non_zero = TestDecayingDuration::from_secs(1, now);

        assert!(zero.is_zero(now));
        assert!(!non_zero.is_zero(now));
    }

    #[test]
    fn test_from_time_units() {
        let now = Instant::now();
        let from_nanos = TestDecayingDuration::from_nanos(1_000_000_000, now);
        let from_micros = TestDecayingDuration::from_micros(1_000_000, now);
        let from_millis = TestDecayingDuration::from_millis(1_000, now);
        let from_secs = TestDecayingDuration::from_secs(1, now);

        assert_eq!(from_nanos.at(now), Duration::from_secs(1));
        assert_eq!(from_micros.at(now), Duration::from_secs(1));
        assert_eq!(from_millis.at(now), Duration::from_secs(1));
        assert_eq!(from_secs.at(now), Duration::from_secs(1));
    }

    #[test]
    fn test_add_decaying_durations() {
        let now = Instant::now();
        let d1 = TestDecayingDuration::from_secs(10, now);
        let d2 = TestDecayingDuration::from_secs(20, now);

        let sum = d1 + d2;
        assert_eq!(sum.at(now), Duration::from_secs(30));
    }

    #[test]
    fn test_sub_decaying_durations() {
        let now = Instant::now();
        let d1 = TestDecayingDuration::from_secs(30, now);
        let d2 = TestDecayingDuration::from_secs(10, now);

        let diff = d1 - d2;
        assert_eq!(diff.at(now), Duration::from_secs(20));
    }

    #[test]
    fn test_sub_saturating() {
        let now = Instant::now();
        let d1 = TestDecayingDuration::from_secs(10, now);
        let d2 = TestDecayingDuration::from_secs(30, now);

        let diff = d1 - d2;
        assert_eq!(diff.at(now), Duration::ZERO);
    }

    #[test]
    fn test_mul_u32() {
        let now = Instant::now();
        let d = TestDecayingDuration::from_secs(10, now);

        let result = d * 3;
        let measured = result.at(Instant::now());
        assert!((measured.as_secs_f64() - 30.0_f64).abs() < 0.1_f64);
    }

    #[test]
    fn test_div_u32() {
        let now = Instant::now();
        let d = TestDecayingDuration::from_secs(30, now);

        let result = d / 3;
        let measured = result.at(Instant::now());
        assert!((measured.as_secs_f64() - 10.0_f64).abs() < 0.1_f64);
    }

    #[test]
    fn test_add_duration() {
        let now = Instant::now();
        let d = TestDecayingDuration::from_secs(10, now);

        let result = d + Duration::from_secs(5);
        let measured = result.at(Instant::now());
        assert!((measured.as_secs_f64() - 15.0_f64).abs() < 0.1_f64);
    }

    #[test]
    fn test_sub_duration() {
        let now = Instant::now();
        let d = TestDecayingDuration::from_secs(10, now);

        let result = d - Duration::from_secs(3);
        let measured = result.at(Instant::now());
        assert!((measured.as_secs_f64() - 7.0_f64).abs() < 0.1_f64);
    }

    #[test]
    fn test_equality() {
        let now = Instant::now();
        let d1 = TestDecayingDuration::from_secs(10, now);
        let d2 = TestDecayingDuration::from_secs(10, now);
        let d3 = TestDecayingDuration::from_secs(20, now);

        assert_eq!(d1, d2);
        assert_ne!(d1, d3);
    }

    #[test]
    fn test_ordering() {
        let now = Instant::now();
        let d1 = TestDecayingDuration::from_secs(10, now);
        let d2 = TestDecayingDuration::from_secs(20, now);

        assert!(d1 < d2);
        assert!(d2 > d1);
        assert!(d1 <= d2);
        assert!(d2 >= d1);
    }

    #[test]
    fn test_default() {
        let d = TestDecayingDuration::default();
        assert!(d.is_zero(Instant::now()));
    }

    #[test]
    fn test_sum() {
        let now = Instant::now();
        let durations = [
            TestDecayingDuration::from_secs(10, now),
            TestDecayingDuration::from_secs(20, now),
            TestDecayingDuration::from_secs(30, now),
        ];

        let sum: TestDecayingDuration = durations.into_iter().sum();
        let measured = sum.at(Instant::now());
        assert!((measured.as_secs_f64() - 60.0_f64).abs() < 0.1_f64);
    }

    #[test]
    fn test_sum_refs() {
        let now = Instant::now();
        let durations = [
            TestDecayingDuration::from_secs(10, now),
            TestDecayingDuration::from_secs(20, now),
            TestDecayingDuration::from_secs(30, now),
        ];

        let sum: TestDecayingDuration = durations.iter().sum();
        let measured = sum.at(Instant::now());
        assert!((measured.as_secs_f64() - 60.0_f64).abs() < 0.1_f64);
    }

    #[test]
    fn test_from_duration() {
        let duration = Duration::from_secs(42);
        let decaying: TestDecayingDuration = duration.into();

        let recovered = decaying.at(Instant::now());
        assert!((recovered.as_secs_f64() - 42.0_f64).abs() < 0.1_f64);
    }

    #[test]
    fn test_into_duration() {
        let now = Instant::now();
        let decaying = TestDecayingDuration::from_secs(42, now);

        let duration: Duration = decaying.into();
        assert!((duration.as_secs_f64() - 42.0_f64).abs() < 0.1_f64);
    }

    #[test]
    fn test_add_assign() {
        let now = Instant::now();
        let mut d1 = TestDecayingDuration::from_secs(10, now);
        let d2 = TestDecayingDuration::from_secs(5, now);

        d1 += d2;
        assert_eq!(d1.at(now), Duration::from_secs(15));
    }

    #[test]
    fn test_sub_assign() {
        let now = Instant::now();
        let mut d1 = TestDecayingDuration::from_secs(10, now);
        let d2 = TestDecayingDuration::from_secs(3, now);

        d1 -= d2;
        assert_eq!(d1.at(now), Duration::from_secs(7));
    }

    #[test]
    fn test_mul_assign() {
        let now = Instant::now();
        let mut d = TestDecayingDuration::from_secs(10, now);

        d *= 3;
        let measured = d.at(Instant::now());
        assert!((measured.as_secs_f64() - 30.0_f64).abs() < 0.1_f64);
    }

    #[test]
    fn test_div_assign() {
        let now = Instant::now();
        let mut d = TestDecayingDuration::from_secs(30, now);

        d /= 3;
        let measured = d.at(Instant::now());
        assert!((measured.as_secs_f64() - 10.0_f64).abs() < 0.1_f64);
    }

    #[test]
    fn test_half_life_constant() {
        assert_eq!(TestDecayingDuration::HALF_LIFE, Duration::from_secs(60));
    }

    #[test]
    fn test_decay_approaches_zero() {
        let now = Instant::now();
        let duration = Duration::from_secs(1000);
        let decaying = TestDecayingDuration::new(duration, now);

        let far_future = now + Duration::from_secs(60 * 20);
        let decayed = decaying.at(far_future);

        assert!(decayed.as_nanos() < 1_000_000);
    }

    #[test]
    fn test_different_measurement_times() {
        let t1 = Instant::now();
        let t2 = t1 + Duration::from_secs(30);

        let d1 = TestDecayingDuration::from_secs(100, t1);
        let d2 = TestDecayingDuration::from_secs(100, t2);

        let t3 = t2 + Duration::from_secs(30);

        assert!(d1.at(t3).as_secs_f64() < d2.at(t3).as_secs_f64());
    }

    #[test]
    fn test_add_with_different_measured_at_times() {
        let t0 = Instant::now();
        let t1 = t0 + Duration::from_secs(60);

        let d1 = TestDecayingDuration::from_secs(100, t0);
        let d2 = TestDecayingDuration::from_secs(100, t1);

        let sum = d1 + d2;

        assert_eq!(sum.measured_at, t1, "Result should use max measured_at");

        let expected = d1.at(t1).as_secs_f64() + d2.at(t1).as_secs_f64();
        let actual = sum.at(t1).as_secs_f64();
        assert!(
            (actual - expected).abs() < 0.1_f64,
            "Sum should decay d1 to t1 before adding"
        );
    }

    #[test]
    fn test_sub_with_different_measured_at_times() {
        let t0 = Instant::now();
        let t1 = t0 + Duration::from_secs(60);

        let d1 = TestDecayingDuration::from_secs(100, t0);
        let d2 = TestDecayingDuration::from_secs(30, t1);

        let diff = d1 - d2;

        assert_eq!(diff.measured_at, t1, "Result should use max measured_at");

        let expected = d1.at(t1).as_secs_f64() - d2.at(t1).as_secs_f64();
        let actual = diff.at(t1).as_secs_f64();
        assert!(
            (actual - expected).abs() < 0.1_f64,
            "Difference should decay d1 to t1 before subtracting"
        );
    }

    #[test]
    fn test_equality_with_different_measured_at() {
        let t0 = Instant::now();
        let t1 = t0 + Duration::from_secs(60);

        let d1 = TestDecayingDuration::from_secs(100, t0);
        let d2 = TestDecayingDuration::from_secs(50, t1);

        assert_eq!(d1, d2, "Equal decayed values should be equal");
    }

    #[test]
    fn test_ordering_with_different_measured_at() {
        let t0 = Instant::now();
        let t1 = t0 + Duration::from_secs(60);

        let d1 = TestDecayingDuration::from_secs(100, t0);
        let d2 = TestDecayingDuration::from_secs(60, t1);

        assert!(d1 < d2, "d1 decays to 50, d2 is 60");
        assert!(d2 > d1);
    }

    #[test]
    fn test_at_with_past_instant() {
        let now = Instant::now();
        let future = now + Duration::from_secs(10);
        let d = TestDecayingDuration::from_secs(100, future);

        let result = d.at(now);

        assert_eq!(
            result,
            Duration::from_secs(100),
            "Querying before measurement time should not decay"
        );
    }

    #[test]
    fn test_zero_decay_at_measurement_time() {
        let now = Instant::now();
        let d = TestDecayingDuration::from_secs(42, now);

        let result = d.at(now);

        assert_eq!(
            result,
            Duration::from_secs(42),
            "No decay should occur at measurement time"
        );
    }

    #[test]
    fn test_from_micros_overflow() {
        let now = Instant::now();
        let d = TestDecayingDuration::from_micros(u64::MAX, now);

        assert!(
            d.at(now).as_nanos() > 0,
            "Should saturate instead of overflow"
        );
    }

    #[test]
    fn test_from_millis_overflow() {
        let now = Instant::now();
        let d = TestDecayingDuration::from_millis(u64::MAX, now);

        assert!(
            d.at(now).as_nanos() > 0,
            "Should saturate instead of overflow"
        );
    }

    #[test]
    fn test_from_secs_overflow() {
        let now = Instant::now();
        let d = TestDecayingDuration::from_secs(u64::MAX, now);

        assert!(
            d.at(now).as_nanos() > 0,
            "Should saturate instead of overflow"
        );
    }

    #[test]
    fn test_add_saturating_overflow() {
        let now = Instant::now();
        let d1 = TestDecayingDuration::from_nanos(u64::MAX - 100, now);
        let d2 = TestDecayingDuration::from_nanos(1000, now);

        let sum = d1 + d2;

        assert_eq!(
            sum.at(now).as_nanos(),
            u128::from(u64::MAX),
            "Addition should saturate at u64::MAX"
        );
    }

    #[test]
    fn test_add_assign_duration() {
        let now = Instant::now();
        let mut d = TestDecayingDuration::from_secs(10, now);

        d += Duration::from_secs(5);

        let measured = d.at(Instant::now());
        assert!((measured.as_secs_f64() - 15.0_f64).abs() < 0.1_f64);
    }

    #[test]
    fn test_sub_assign_duration() {
        let now = Instant::now();
        let mut d = TestDecayingDuration::from_secs(10, now);

        d -= Duration::from_secs(3);

        let measured = d.at(Instant::now());
        assert!((measured.as_secs_f64() - 7.0_f64).abs() < 0.1_f64);
    }
}
