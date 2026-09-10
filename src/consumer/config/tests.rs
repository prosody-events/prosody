//! Unit tests for consumer-build configuration cross-checks.

use super::{
    ConsumerConfiguration, DEFAULT_STATISTICS_INTERVAL, MAX_STATISTICS_INTERVAL,
    validate_statistics_interval,
};
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
