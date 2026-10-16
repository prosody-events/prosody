//! Unit tests for consumer-build configuration cross-checks.

use super::{
    ConsumerConfiguration, ConsumerConfigurationBuilder, DEFAULT_STATISTICS_INTERVAL,
    MAX_STATISTICS_INTERVAL, validate_statistics_interval,
};
use crate::ByteSize;
use color_eyre::Result;
use quickcheck::TestResult;
use quickcheck_macros::quickcheck;
use std::num::NonZeroU64;
use std::time::Duration;
use validator::Validate;

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

/// A byte count from one byte to about 2^63, so the boundaries of every
/// librdkafka byte range stay reachable from quickcheck's small integers.
fn byte_count(exponent: u8, offset: u32) -> ByteSize {
    let bytes = (1_u64 << (exponent % 63)).saturating_add(u64::from(offset));
    ByteSize::new(NonZeroU64::MIN.saturating_add(bytes - 1))
}

/// A consumer configuration with only its required fields set.
fn required() -> ConsumerConfigurationBuilder {
    let mut builder = ConsumerConfiguration::builder();
    builder
        .bootstrap_servers(vec!["kafka:9092".to_owned()])
        .group_id("prefetch");
    builder
}

/// A partition prefetch is accepted exactly when librdkafka can carry it as a
/// queue limit in whole KiB and as a partition fetch size.
///
/// Falsify: widen the `partition_prefetch` range in [`ConsumerConfiguration`].
#[quickcheck]
fn prop_partition_prefetch_accepts_only_what_librdkafka_carries(
    exponent: u8,
    offset: u32,
) -> Result<bool> {
    let prefetch = byte_count(exponent, offset);
    let config = required().partition_prefetch(prefetch).build()?;
    let carried = (1 << 10_u32..=1_000_000_000).contains(&prefetch.get());
    Ok(config.validate().is_ok() == carried)
}

/// A fetch response limit is accepted exactly when librdkafka can carry it:
/// at least its default record limit, and room for 512 bytes of framing in a
/// 32-bit size.
///
/// Falsify: widen the `fetch_max_bytes` range in [`ConsumerConfiguration`].
#[quickcheck]
fn prop_fetch_max_bytes_accepts_only_what_librdkafka_carries(
    exponent: u8,
    offset: u32,
) -> Result<bool> {
    let limit = byte_count(exponent, offset);
    let config = required().fetch_max_bytes(limit).build()?;
    let carried = (1_000_000..=u64::from(i32::MAX.unsigned_abs()) - 512).contains(&limit.get());
    Ok(config.validate().is_ok() == carried)
}

/// The prefetch defaults survive their own validation, so a consumer that
/// configures nothing builds.
#[test]
fn the_prefetch_defaults_validate() -> Result<()> {
    required().build()?.validate()?;
    Ok(())
}
