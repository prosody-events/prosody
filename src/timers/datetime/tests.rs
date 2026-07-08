use super::*;
use chrono::{TimeZone, Utc};
use quickcheck_macros::quickcheck;
use std::time::Duration;

fn compact_datetime_from_epoch(epoch_seconds: u32) -> CompactDateTime {
    CompactDateTime { epoch_seconds }
}

#[test]
fn test_now() {
    let now = CompactDateTime::now();
    assert!(now.is_ok(), "Failed to get current time");

    if let Ok(now) = now {
        let system_now = Utc::now().timestamp() as u32;
        assert!((i64::from(now.epoch_seconds()) - i64::from(system_now)).abs() <= 1);
    }
}

#[test]
fn test_duration_since() {
    let earlier = compact_datetime_from_epoch(1000_u32);
    let later = compact_datetime_from_epoch(2000_u32);

    let duration = later.duration_since(earlier);
    assert!(duration.is_ok(), "Failed to calculate duration");

    if let Ok(duration) = duration {
        assert_eq!(duration, Duration::from_secs(1000));
    }

    let result = earlier.duration_since(later);
    assert!(matches!(result, Err(CompactDateTimeError::PastDateTime)));
}

#[test]
fn test_duration_from_now() {
    let now = CompactDateTime::now();
    assert!(now.is_ok(), "Failed to get current time");

    if let Ok(now) = now {
        let future = compact_datetime_from_epoch(now.epoch_seconds() + 10);

        let duration = future.duration_from_now();
        assert!(duration.is_ok(), "Failed to calculate duration from now");

        if let Ok(duration) = duration {
            assert!(duration.as_secs() <= 10);
        }

        let past = compact_datetime_from_epoch(now.epoch_seconds() - 10);
        let result = past.duration_from_now();
        assert!(matches!(result, Err(CompactDateTimeError::PastDateTime)));
    }
}

#[test]
fn test_add_duration() {
    let dt = compact_datetime_from_epoch(1000_u32);
    let duration = CompactDuration::new(500_u32);

    let new_dt = dt.add_duration(duration);
    assert!(new_dt.is_ok(), "Failed to add duration");

    if let Ok(new_dt) = new_dt {
        assert_eq!(new_dt.epoch_seconds(), 1500_u32);
    }
}

#[test]
fn test_try_from_datetime() {
    let datetime = Utc.timestamp_opt(12345, 0).single();
    assert!(datetime.is_some(), "Failed to create datetime");

    if let Some(datetime) = datetime {
        let compact_dt = CompactDateTime::try_from(datetime);
        assert!(compact_dt.is_ok(), "Failed to convert from DateTime");

        if let Ok(compact_dt) = compact_dt {
            assert_eq!(compact_dt.epoch_seconds(), 12345_u32);
        }
    }

    if let chrono::LocalResult::Single(out_of_range_datetime) = Utc.timestamp_opt(i64::MAX, 0) {
        let result = CompactDateTime::try_from(out_of_range_datetime);
        assert!(matches!(result, Err(CompactDateTimeError::OutOfRange)));
    }
}

#[test]
fn test_from_compact_datetime_to_datetime() {
    let compact_dt = compact_datetime_from_epoch(12345_u32);
    let datetime: DateTime<Utc> = compact_dt.into();
    assert_eq!(datetime.timestamp(), 12345);
}

#[test]
fn test_try_from_system_time() {
    let system_time = SystemTime::UNIX_EPOCH + Duration::from_secs(12345);
    let compact_dt = CompactDateTime::try_from(system_time);
    assert!(compact_dt.is_ok(), "Failed to convert from SystemTime");

    if let Ok(compact_dt) = compact_dt {
        assert_eq!(compact_dt.epoch_seconds(), 12345_u32);
    }

    // Test with sub-second rounding down
    let system_time_round_down =
        SystemTime::UNIX_EPOCH + Duration::from_secs(12345) + Duration::from_nanos(499_999_999);
    let compact_dt = CompactDateTime::try_from(system_time_round_down);
    assert!(compact_dt.is_ok());
    if let Ok(compact_dt) = compact_dt {
        assert_eq!(compact_dt.epoch_seconds(), 12345_u32);
    }

    // Test with sub-second rounding up
    let system_time_round_up =
        SystemTime::UNIX_EPOCH + Duration::from_secs(12345) + Duration::from_millis(500);
    let compact_dt = CompactDateTime::try_from(system_time_round_up);
    assert!(compact_dt.is_ok());
    if let Ok(compact_dt) = compact_dt {
        assert_eq!(compact_dt.epoch_seconds(), 12346_u32);
    }
}

#[test]
fn test_try_from_system_time_error_cases() {
    // Test timestamp beyond u32::MAX
    let large_system_time = SystemTime::UNIX_EPOCH + Duration::from_secs(u64::from(u32::MAX) + 1);
    let result = CompactDateTime::try_from(large_system_time);
    assert!(matches!(result, Err(CompactDateTimeError::OutOfRange)));

    // Test rounding at maximum value should fail
    let max_with_rounding = SystemTime::UNIX_EPOCH
        + Duration::from_secs(u64::from(u32::MAX))
        + Duration::from_millis(500);
    let result = CompactDateTime::try_from(max_with_rounding);
    assert!(matches!(result, Err(CompactDateTimeError::OutOfRange)));
}

#[test]
fn test_from_compact_datetime_to_system_time() {
    let compact_dt = compact_datetime_from_epoch(12345_u32);
    let system_time: SystemTime = compact_dt.into();
    let duration = system_time.duration_since(SystemTime::UNIX_EPOCH);
    assert!(duration.is_ok());
    if let Ok(duration) = duration {
        assert_eq!(duration.as_secs(), 12345);
    }
}

#[test]
fn test_system_time_roundtrip() {
    let original = SystemTime::UNIX_EPOCH + Duration::from_secs(12345);
    let compact_dt = CompactDateTime::try_from(original);
    assert!(compact_dt.is_ok());
    if let Ok(compact_dt) = compact_dt {
        let roundtrip: SystemTime = compact_dt.into();
        assert_eq!(roundtrip, original);
    }

    // Test MIN boundary
    let min_compact = CompactDateTime::MIN;
    let min_system_time: SystemTime = min_compact.into();
    assert_eq!(min_system_time, SystemTime::UNIX_EPOCH);

    // Test MAX boundary
    let max_compact = CompactDateTime::MAX;
    let max_system_time: SystemTime = max_compact.into();
    let expected = SystemTime::UNIX_EPOCH + Duration::from_secs(u64::from(u32::MAX));
    assert_eq!(max_system_time, expected);
}

#[test]
fn test_from_u32() {
    let compact_dt = CompactDateTime::from(12345_u32);
    assert_eq!(compact_dt.epoch_seconds(), 12345_u32);
}

#[test]
fn test_from_i32_negative_wraps_to_max_u32() {
    // Illustrates the little-endian byte-reinterpretation: a negative i32
    // becomes a large u32, not a saturated/clamped value.
    let compact_dt = CompactDateTime::from(-1_i32);
    assert_eq!(compact_dt.epoch_seconds(), u32::MAX);
}

#[test]
fn test_display() {
    let compact_dt = compact_datetime_from_epoch(12345_u32);
    let display = format!("{compact_dt}");
    assert_eq!(display, "1970-01-01 03:25:45 UTC");
}

#[test]
fn test_debug() {
    let compact_dt = compact_datetime_from_epoch(12345_u32);
    let debug = format!("{compact_dt:?}");
    assert_eq!(debug, "1970-01-01T03:25:45Z");
}

#[test]
fn test_constants() {
    // Test MIN constant
    assert_eq!(CompactDateTime::MIN.epoch_seconds(), 0_u32);
    let min_datetime: DateTime<Utc> = CompactDateTime::MIN.into();
    assert_eq!(min_datetime.timestamp(), 0);

    // Test MAX constant
    assert_eq!(CompactDateTime::MAX.epoch_seconds(), u32::MAX);
    let max_datetime: DateTime<Utc> = CompactDateTime::MAX.into();
    assert_eq!(max_datetime.timestamp(), i64::from(u32::MAX));
}

#[test]
fn test_try_from_datetime_rounding() {
    // Test rounding down (< 500ms nanoseconds)
    let datetime_round_down = Utc.timestamp_opt(12345, 499_999_999).single();
    assert!(datetime_round_down.is_some());
    if let Some(datetime) = datetime_round_down {
        let compact_dt = CompactDateTime::try_from(datetime);
        assert!(compact_dt.is_ok());
        if let Ok(compact_dt) = compact_dt {
            assert_eq!(compact_dt.epoch_seconds(), 12345_u32);
        }
    }

    // Test rounding up (>= 500ms nanoseconds)
    let datetime_round_up = Utc.timestamp_opt(12345, 500_000_000).single();
    assert!(datetime_round_up.is_some());
    if let Some(datetime) = datetime_round_up {
        let compact_dt = CompactDateTime::try_from(datetime);
        assert!(compact_dt.is_ok());
        if let Ok(compact_dt) = compact_dt {
            assert_eq!(compact_dt.epoch_seconds(), 12346_u32);
        }
    }

    // Test edge case: rounding at maximum value should fail
    let max_datetime = Utc.timestamp_opt(i64::from(u32::MAX), 500_000_000).single();
    if let Some(datetime) = max_datetime {
        let result = CompactDateTime::try_from(datetime);
        assert!(matches!(result, Err(CompactDateTimeError::OutOfRange)));
    }
}

#[test]
fn test_try_from_datetime_error_cases() {
    // Test negative timestamp (before Unix epoch)
    let negative_datetime = Utc.timestamp_opt(-1, 0).single();
    if let Some(datetime) = negative_datetime {
        let result = CompactDateTime::try_from(datetime);
        assert!(matches!(result, Err(CompactDateTimeError::OutOfRange)));
    }

    // Test timestamp beyond u32::MAX
    let large_datetime = Utc.timestamp_opt(i64::from(u32::MAX) + 1, 0).single();
    if let Some(datetime) = large_datetime {
        let result = CompactDateTime::try_from(datetime);
        assert!(matches!(result, Err(CompactDateTimeError::OutOfRange)));
    }
}

#[test]
fn test_duration_edge_cases() {
    // Test duration between MIN and MAX
    let duration = CompactDateTime::MAX.duration_since(CompactDateTime::MIN);
    assert!(duration.is_ok());
    if let Ok(duration) = duration {
        assert_eq!(duration.as_secs(), u64::from(u32::MAX));
    }

    // Test duration with same times
    let dt = compact_datetime_from_epoch(1000_u32);
    let duration = dt.duration_since(dt);
    assert!(duration.is_ok());
    if let Ok(duration) = duration {
        assert_eq!(duration.as_secs(), 0);
    }
}

#[test]
fn test_add_duration_edge_cases() {
    // Test adding to MAX should fail
    let max_dt = CompactDateTime::MAX;
    let one_sec = CompactDuration::new(1_u32);
    let result = max_dt.add_duration(one_sec);
    assert!(matches!(result, Err(CompactDateTimeError::OutOfRange)));

    // Test adding zero duration
    let dt = compact_datetime_from_epoch(1000_u32);
    let zero_duration = CompactDuration::new(0_u32);
    let result = dt.add_duration(zero_duration);
    assert!(result.is_ok());
    if let Ok(result_dt) = result {
        assert_eq!(result_dt.epoch_seconds(), 1000_u32);
    }

    // Test adding maximum possible duration to zero
    let min_dt = CompactDateTime::MIN;
    let max_duration = CompactDuration::new(u32::MAX);
    let result = min_dt.add_duration(max_duration);
    assert!(result.is_ok());
    if let Ok(result_dt) = result {
        assert_eq!(result_dt.epoch_seconds(), u32::MAX);
    }
}

#[quickcheck]
fn prop_compact_datetime_roundtrip(epoch_seconds: u32) -> bool {
    let compact_dt = compact_datetime_from_epoch(epoch_seconds);
    let datetime: DateTime<Utc> = compact_dt.into();
    let roundtrip = CompactDateTime::try_from(datetime);
    roundtrip.is_ok_and(|dt| dt == compact_dt)
}

#[quickcheck]
fn prop_i32_roundtrip(value: i32) -> bool {
    i32::from(CompactDateTime::from(value)) == value
}

#[quickcheck]
fn prop_add_duration_increases_time(epoch_seconds: u32, duration_seconds: u32) -> bool {
    let compact_dt = CompactDateTime::from(epoch_seconds);
    let duration = CompactDuration::new(duration_seconds);

    // Simulate the expected behavior of CompactDateTime::add_duration
    if let Some(expected_sum) = epoch_seconds.checked_add(duration_seconds) {
        // If addition does not overflow, ensure the result matches
        match compact_dt.add_duration(duration) {
            Ok(new_dt) => new_dt.epoch_seconds() == expected_sum,
            Err(_) => false, // Unexpected error
        }
    } else {
        // If addition would overflow, ensure an error is returned
        matches!(
            compact_dt.add_duration(duration),
            Err(CompactDateTimeError::OutOfRange)
        )
    }
}
