use super::*;
use quickcheck_macros::quickcheck;
use std::mem::size_of;

type TestDecayingDuration = DecayingDuration<60>;

#[test]
fn test_size_of_decaying_duration() {
    assert_eq!(size_of::<DecayingDuration<60>>(), 16);
}

/// Decay law: `at(t)` matches the closed form
/// `value · 2^(−elapsed / HALF_LIFE)` across the whole domain — random
/// values and elapsed times, not just the hand-picked half-life points.
/// The only permitted divergence is the final `as u64` truncation.
#[quickcheck]
fn decay_matches_closed_form(value_nanos: u64, elapsed_secs: u32) -> bool {
    let elapsed_secs = elapsed_secs % 3601; // up to 60 half-lives
    let now = Instant::now();
    let decaying = TestDecayingDuration::new(Duration::from_nanos(value_nanos), now);

    let query = now + Duration::from_secs(u64::from(elapsed_secs));
    let expected = value_nanos as f64 * (-f64::from(elapsed_secs) / 60.0_f64).exp2();
    let actual = decaying.at(query).as_nanos() as f64;

    (actual - expected).abs() <= expected.mul_add(1e-6_f64, 1.0_f64)
}

/// Frozen smoke for the decay law: one half-life halves the value.
#[test]
fn test_decay_after_half_life() {
    let now = Instant::now();
    let duration = Duration::from_secs(100);
    let decaying = TestDecayingDuration::new(duration, now);

    let after_half_life = now + Duration::from_mins(1);
    let decayed = decaying.at(after_half_life);

    assert!((decayed.as_secs_f64() - 50.0_f64).abs() < 0.1_f64);
}

/// `Add<Duration>` decays the accumulator to the add instant, then adds the
/// new time at full (undecayed) value, saturating at `u64::MAX` nanos.
#[quickcheck]
fn add_duration_adds_undecayed(base_millis: u32, added_millis: u32) -> bool {
    let now = Instant::now();
    let base = TestDecayingDuration::new(Duration::from_millis(u64::from(base_millis)), now);
    let added = Duration::from_millis(u64::from(added_millis));

    let sum = base + added;
    let expected = base
        .decayed_nanos_at(sum.measured_at)
        .saturating_add(added.as_nanos() as u64);

    sum.value_nanos == expected
}

/// `+=` is the same operation as `+`, performed in place.
#[test]
fn test_add_assign_matches_add() {
    let now = Instant::now();
    let base = TestDecayingDuration::new(Duration::from_secs(10), now);
    let added = Duration::from_secs(5);

    let sum = base + added;
    let mut assigned = base;
    assigned += added;

    let at = Instant::now();
    let diff = (sum.at(at).as_secs_f64() - assigned.at(at).as_secs_f64()).abs();
    assert!(diff < 0.1_f64);
    assert!((sum.at(at).as_secs_f64() - 15.0_f64).abs() < 0.1_f64);
}

/// Saturation boundary: adding onto a near-`u64::MAX` accumulator pins at
/// `u64::MAX` nanos instead of wrapping.
#[test]
fn test_add_duration_saturates_at_u64_max_nanos() {
    let now = Instant::now();
    let near_max = TestDecayingDuration::new(Duration::from_nanos(u64::MAX - 1), now);

    let sum = near_max + Duration::from_nanos(u64::MAX);

    assert_eq!(sum.value_nanos, u64::MAX);
}

/// `From<Duration>` measures at the conversion instant, so an immediate
/// read returns (approximately) the input.
#[test]
fn test_from_duration() {
    let decaying: TestDecayingDuration = Duration::from_secs(10).into();
    let measured = decaying.at(Instant::now());
    assert!((measured.as_secs_f64() - 10.0_f64).abs() < 0.1_f64);
}
