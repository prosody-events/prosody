//! Unit tests for consumer-build configuration cross-checks.

use super::{
    ConsumerConfiguration, DEFAULT_STATISTICS_INTERVAL, MAX_STATISTICS_INTERVAL,
    MIN_RECOVERY_EVIDENCE_TTL_SECONDS, RECOVERY_TTL_DELAY_MULTIPLIER, validate_recovery_ttl_margin,
    validate_statistics_interval,
};
use crate::timers::duration::CompactDuration;
use quickcheck::TestResult;
use quickcheck_macros::quickcheck;
use std::time::Duration;

/// A reader needs Kafka coordinates, but it does not need a topic subscription.
///
/// Falsify: require one subscribed topic in [`ConsumerConfiguration`].
#[test]
fn consumer_configuration_allows_no_topic_subscription() {
    let mut builder = ConsumerConfiguration::builder();
    builder
        .bootstrap_servers(vec!["kafka:9092".to_owned()])
        .group_id("state-reader");

    let config = builder.build();
    assert!(matches!(config, Ok(config) if config.subscribed_topics.is_empty()));
}

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

/// A statistics interval is accepted exactly when librdkafka can carry it: at
/// least one whole millisecond, at most 24 hours. Anything shorter truncates to
/// zero milliseconds, which would silently stop reporting.
///
/// Falsify: relax either bound in [`validate_statistics_interval`]. The
/// sub-millisecond and over-24-hour intervals then pass.
#[quickcheck]
fn prop_statistics_interval_accepts_only_what_librdkafka_carries(
    exponent: u8,
    offset: u32,
) -> TestResult {
    // One nanosecond to roughly 292 years, so both bounds stay reachable from
    // quickcheck's small integer generator.
    let nanos = (1_u64 << (exponent % 63)).saturating_add(u64::from(offset));
    let interval = Duration::from_nanos(nanos);
    let accepted = validate_statistics_interval(&interval).is_ok();
    TestResult::from_bool(
        accepted == (interval.as_millis() >= 1 && interval <= MAX_STATISTICS_INTERVAL),
    )
}

/// The default interval survives its own validation, so a consumer that
/// configures nothing builds.
#[test]
fn the_default_statistics_interval_validates() {
    assert!(validate_statistics_interval(&DEFAULT_STATISTICS_INTERVAL).is_ok());
}
