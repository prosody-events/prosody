//! Unit tests for consumer-build configuration cross-checks.

use super::{
    MIN_RECOVERY_EVIDENCE_TTL_SECONDS, RECOVERY_TTL_DELAY_MULTIPLIER, validate_recovery_ttl_margin,
};
use crate::timers::duration::CompactDuration;
use quickcheck::TestResult;
use quickcheck_macros::quickcheck;
use std::time::Duration;

/// The deduplication-TTL floor is `max(48 × recovery_delay, 1h)` (see
/// [`validate_recovery_ttl_margin`]). Pins both arms of the `max` and the `≥`
/// boundary over arbitrary recovery delays: a TTL one second below the
/// required floor is always rejected with a `required` that matches the
/// floor, and a TTL exactly at the floor is always allowed.
#[quickcheck]
fn prop_dedup_ttl_must_clear_the_recovery_margin(delay_secs: u32, below: bool) -> TestResult {
    let delay = CompactDuration::new(delay_secs);
    let required = u64::from(delay_secs)
        .saturating_mul(RECOVERY_TTL_DELAY_MULTIPLIER)
        .max(MIN_RECOVERY_EVIDENCE_TTL_SECONDS);
    let dedup_ttl_secs = if below { required - 1 } else { required };

    let result = validate_recovery_ttl_margin(Duration::from_secs(dedup_ttl_secs), delay);
    match (below, result) {
        (true, Err(err)) => TestResult::from_bool(
            err.dedup_ttl == dedup_ttl_secs
                && err.recovery_delay == u64::from(delay_secs)
                && err.required == required,
        ),
        (false, Ok(())) => TestResult::passed(),
        _ => TestResult::failed(),
    }
}

/// The default dedup TTL (7 days) clears the default recovery delay (30s)
/// comfortably — the common case must not fail the build.
#[test]
fn the_defaults_clear_the_margin() {
    let delay = CompactDuration::new(30);
    assert!(validate_recovery_ttl_margin(Duration::from_hours(7 * 24), delay).is_ok());
}
