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

    let after_half_life = now + Duration::from_mins(1);
    let decayed = decaying.at(after_half_life);

    assert!((decayed.as_secs_f64() - 50.0_f64).abs() < 0.1_f64);
}

#[test]
fn test_decay_after_two_half_lives() {
    let now = Instant::now();
    let duration = Duration::from_secs(100);
    let decaying = TestDecayingDuration::new(duration, now);

    let after_two_half_lives = now + Duration::from_mins(2);
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
    assert_eq!(TestDecayingDuration::HALF_LIFE, Duration::from_mins(1));
}

#[test]
fn test_decay_approaches_zero() {
    let now = Instant::now();
    let duration = Duration::from_secs(1000);
    let decaying = TestDecayingDuration::new(duration, now);

    let far_future = now + Duration::from_mins(20);
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
    let t1 = t0 + Duration::from_mins(1);

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
    let t1 = t0 + Duration::from_mins(1);

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
    let t1 = t0 + Duration::from_mins(1);

    let d1 = TestDecayingDuration::from_secs(100, t0);
    let d2 = TestDecayingDuration::from_secs(50, t1);

    assert_eq!(d1, d2, "Equal decayed values should be equal");
}

#[test]
fn test_ordering_with_different_measured_at() {
    let t0 = Instant::now();
    let t1 = t0 + Duration::from_mins(1);

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
