//! Unit tests for consumer-build configuration cross-checks.

use super::{ConsumerConfiguration, ConsumerConfigurationBuilder};
use crate::ByteSize;
use color_eyre::Result;
use quickcheck_macros::quickcheck;
use std::num::NonZeroU64;
use std::time::Duration;
use validator::Validate;

/// A reader needs Kafka coordinates, but it does not need a topic subscription.
///
/// Falsify: require one subscribed topic in [`ConsumerConfiguration`].
#[test]
fn consumer_configuration_allows_no_topic_subscription() {
    let config = required().build();
    assert!(matches!(config, Ok(config) if config.subscribed_topics.is_empty()));
}

/// A number from one to about 2^63, so the boundaries of every librdkafka
/// range stay reachable from quickcheck's small integers.
fn magnitude(exponent: u8, offset: u32) -> NonZeroU64 {
    let above_one = (1_u64 << (exponent % 63)) - 1;
    NonZeroU64::MIN.saturating_add(above_one.saturating_add(u64::from(offset)))
}

/// A consumer configuration with only its required fields set.
fn required() -> ConsumerConfigurationBuilder {
    let mut builder = ConsumerConfiguration::builder();
    builder
        .bootstrap_servers(vec!["kafka:9092".to_owned()])
        .group_id("configuration");
    builder
}

/// A duration near one bound of a millisecond setting: zero, one millisecond,
/// or `max`. It lies a power of two nanoseconds, up to about one second, below
/// or above that bound.
fn near_bound(max: Duration, anchor: u8, below: bool, exponent: u8) -> Duration {
    let bound = match anchor % 3 {
        0 => Duration::ZERO,
        1 => Duration::from_millis(1),
        _ => max,
    };
    let offset = Duration::from_nanos(1 << (exponent % 31));
    if below {
        bound.saturating_sub(offset)
    } else {
        bound + offset
    }
}

/// Whether librdkafka can carry `value` for a setting whose maximum is `max`.
fn carried(value: Duration, max: Duration) -> bool {
    value.as_millis() >= 1 && value <= max
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
    let prefetch = ByteSize::new(magnitude(exponent, offset));
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
    let limit = ByteSize::new(magnitude(exponent, offset));
    let config = required().fetch_max_bytes(limit).build()?;
    let carried = (1_000_000..=u64::from(i32::MAX.unsigned_abs()) - 512).contains(&limit.get());
    Ok(config.validate().is_ok() == carried)
}

/// The validator accepts a millisecond setting exactly when librdkafka can
/// carry it: at least one whole millisecond, and at most the setting's maximum.
/// A shorter duration truncates to zero milliseconds, which has a special
/// meaning.
///
/// Falsify: move either bound in `validate_millis`, raise a maximum, or drop
/// the validator of either field.
#[quickcheck]
fn prop_millisecond_settings_accept_only_what_librdkafka_carries(
    anchor: u8,
    below: bool,
    exponent: u8,
) -> Result<bool> {
    let interval_max = Duration::from_hours(24);
    let interval = near_bound(interval_max, anchor, below, exponent);
    let statistics = required().statistics_interval(interval).build()?;

    let wait_max = Duration::from_secs(59);
    let wait = near_bound(wait_max, anchor, below, exponent);
    let fetch = required().fetch_wait(wait).build()?;

    Ok(
        statistics.validate().is_ok() == carried(interval, interval_max)
            && fetch.validate().is_ok() == carried(wait, wait_max),
    )
}

/// The defaults survive their own validation, so a consumer that configures
/// nothing builds.
#[test]
fn the_defaults_validate() -> Result<()> {
    required().build()?.validate()?;
    Ok(())
}
