//! Compact datetime representation for efficient timer storage and processing.
//!
//! This module provides [`CompactDateTime`], a space-efficient representation
//! of datetime values using 32-bit epoch seconds. This compact format is
//! optimized for timer systems where storage efficiency and fast comparisons
//! are important.
//!
//! ## Design Rationale
//!
//! Traditional datetime representations often use 64-bit timestamps or complex
//! structures. For timer systems processing large volumes of scheduled events,
//! this can lead to significant memory overhead. [`CompactDateTime`] addresses
//! this by:
//!
//! - Using only 32 bits for storage (4 bytes vs 8+ bytes)
//! - Providing fast arithmetic and comparison operations
//! - Supporting a useful range from 1970 to 2106
//! - Maintaining second-level precision which is sufficient for most timer use
//!   cases
//!
//! ## Time Range and Precision
//!
//! The 32-bit epoch seconds representation provides:
//!
//! - **Range**: January 1, 1970 to February 7, 2106
//! - **Precision**: 1 second
//! - **Rounding**: Sub-second values are rounded to the nearest second
//!
//! ## Usage Examples
//!
//! ```rust,no_run
//! use chrono::{DateTime, Utc};
//! use prosody::timers::datetime::CompactDateTime;
//! use prosody::timers::duration::CompactDuration;
//!
//! // Create from current time
//! let now = CompactDateTime::now().unwrap();
//!
//! // Create from epoch seconds
//! let specific_time = CompactDateTime::from(1234567890_u32);
//!
//! // Convert from/to chrono DateTime
//! let chrono_time = Utc::now();
//! let compact = CompactDateTime::try_from(chrono_time).unwrap();
//! let back_to_chrono: DateTime<Utc> = compact.into();
//!
//! // Time arithmetic
//! let later = now.add_duration(CompactDuration::new(3600)).unwrap(); // +1 hour
//! let duration_between = later.duration_since(now).unwrap();
//! ```

use crate::timers::duration::CompactDuration;
use chrono::{DateTime, Utc};
use std::fmt::{Debug, Display, Formatter};
use std::time::Duration;
use thiserror::Error;

/// A compact datetime representation using 32-bit epoch seconds.
///
/// [`CompactDateTime`] provides an efficient way to represent datetime values
/// for timer systems where memory usage and fast operations are critical.
/// It stores time as seconds since the Unix epoch (January 1, 1970 UTC).
///
/// ## Storage Efficiency
///
/// - **Size**: 4 bytes (vs 8+ for standard datetime types)
/// - **Alignment**: Optimal for CPU cache usage
/// - **Comparison**: Fast integer comparison operations
///
/// ## Precision and Range
///
/// - **Precision**: 1 second (sub-second values are rounded)
/// - **Range**: 1970-01-01 00:00:00 UTC to 2106-02-07 06:28:15 UTC
/// - **Overflow**: Operations that would exceed the range return errors
///
/// ## Thread Safety
///
/// [`CompactDateTime`] is [`Copy`] and all operations are thread-safe.
#[derive(Copy, Clone, Hash, PartialEq, Eq, Ord, PartialOrd)]
pub struct CompactDateTime {
    epoch_seconds: u32,
}

impl CompactDateTime {
    /// The maximum representable datetime (2106-02-07 06:28:15 UTC).
    pub const MAX: Self = Self {
        epoch_seconds: u32::MAX,
    };
    /// The minimum representable datetime (1970-01-01 00:00:00 UTC).
    pub const MIN: Self = Self {
        epoch_seconds: u32::MIN,
    };

    /// Creates a [`CompactDateTime`] representing the current time.
    ///
    /// # Returns
    ///
    /// `Ok(CompactDateTime)` if the system clock is within the representable
    /// range, otherwise an error.
    ///
    /// # Errors
    ///
    /// Returns [`CompactDateTimeError::OutOfRange`] if the system time is
    /// before 1970 or after 2106.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use prosody::timers::datetime::CompactDateTime;
    ///
    /// let now = CompactDateTime::now().unwrap();
    /// println!("Current time: {}", now);
    /// ```
    pub fn now() -> Result<Self, CompactDateTimeError> {
        Self::try_from(Utc::now())
    }

    /// Returns the number of seconds since the Unix epoch.
    ///
    /// # Returns
    ///
    /// A `u32` value representing seconds since January 1, 1970 00:00:00 UTC.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use prosody::timers::datetime::CompactDateTime;
    ///
    /// let time = CompactDateTime::from(1234567890_u32);
    /// assert_eq!(time.epoch_seconds(), 1234567890);
    /// ```
    #[must_use]
    pub fn epoch_seconds(self) -> u32 {
        self.epoch_seconds
    }

    /// Calculates the duration between this time and an earlier time.
    ///
    /// # Arguments
    ///
    /// * `other` - An earlier [`CompactDateTime`] to measure from.
    ///
    /// # Returns
    ///
    /// `Ok(Duration)` if `other <= self`, otherwise an error.
    ///
    /// # Errors
    ///
    /// Returns [`CompactDateTimeError::PastDateTime`] if `other` is later
    /// than `self`.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use prosody::timers::datetime::CompactDateTime;
    /// use std::time::Duration;
    ///
    /// let earlier = CompactDateTime::from(1000_u32);
    /// let later = CompactDateTime::from(2000_u32);
    ///
    /// assert_eq!(
    ///     later.duration_since(earlier).unwrap(),
    ///     Duration::from_secs(1000)
    /// );
    /// ```
    pub fn duration_since(self, other: Self) -> Result<Duration, CompactDateTimeError> {
        let seconds = self
            .epoch_seconds
            .checked_sub(other.epoch_seconds)
            .ok_or(CompactDateTimeError::PastDateTime)?;

        Ok(Duration::from_secs(u64::from(seconds)))
    }

    /// Calculates the duration from now until this datetime.
    ///
    /// # Returns
    ///
    /// `Ok(Duration)` if `self >= now()`, otherwise an error.
    ///
    /// # Errors
    ///
    /// - [`CompactDateTimeError::OutOfRange`] if current time is out of range.
    /// - [`CompactDateTimeError::PastDateTime`] if `self` is in the past.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use prosody::timers::datetime::CompactDateTime;
    /// use prosody::timers::duration::CompactDuration;
    ///
    /// let future = CompactDateTime::now()
    ///     .unwrap()
    ///     .add_duration(CompactDuration::new(60))
    ///     .unwrap();
    ///
    /// let until = future.duration_from_now().unwrap();
    /// assert!(until.as_secs() <= 60);
    /// ```
    pub fn duration_from_now(self) -> Result<Duration, CompactDateTimeError> {
        self.duration_since(Self::now()?)
    }

    /// Adds a [`CompactDuration`] to this datetime.
    ///
    /// # Arguments
    ///
    /// * `duration` - The number of seconds to add.
    ///
    /// # Returns
    ///
    /// `Ok(CompactDateTime)` for the new time, or an error if it overflows.
    ///
    /// # Errors
    ///
    /// Returns [`CompactDateTimeError::OutOfRange`] if the result exceeds the
    /// maximum representable time.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use prosody::timers::datetime::CompactDateTime;
    /// use prosody::timers::duration::CompactDuration;
    ///
    /// let base = CompactDateTime::from(1000_u32);
    /// let later = base.add_duration(CompactDuration::new(500)).unwrap();
    /// assert_eq!(later.epoch_seconds(), 1500);
    /// ```
    pub fn add_duration(self, duration: CompactDuration) -> Result<Self, CompactDateTimeError> {
        let epoch_seconds = self
            .epoch_seconds
            .checked_add(duration.seconds())
            .ok_or(CompactDateTimeError::OutOfRange)?;

        Ok(Self { epoch_seconds })
    }

    /// Subtracts a [`CompactDuration`] from this datetime.
    ///
    /// # Arguments
    ///
    /// * `duration` - The number of seconds to subtract.
    ///
    /// # Returns
    ///
    /// `Ok(CompactDateTime)` for the new time, or an error if it underflows.
    ///
    /// # Errors
    ///
    /// Returns [`CompactDateTimeError::OutOfRange`] if the result goes before
    /// the Unix epoch.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use prosody::timers::datetime::CompactDateTime;
    /// use prosody::timers::duration::CompactDuration;
    ///
    /// let base = CompactDateTime::from(2000_u32);
    /// let earlier = base.subtract_duration(CompactDuration::new(500)).unwrap();
    /// assert_eq!(earlier.epoch_seconds(), 1500);
    /// ```
    pub fn subtract_duration(
        self,
        duration: CompactDuration,
    ) -> Result<Self, CompactDateTimeError> {
        let epoch_seconds = self
            .epoch_seconds
            .checked_sub(duration.seconds())
            .ok_or(CompactDateTimeError::OutOfRange)?;

        Ok(Self { epoch_seconds })
    }
}

impl TryFrom<DateTime<Utc>> for CompactDateTime {
    type Error = CompactDateTimeError;

    /// Converts a [`DateTime<Utc>`] to a [`CompactDateTime`], rounding
    /// sub-second precision to the nearest second.
    ///
    /// # Arguments
    ///
    /// * `value` - The `DateTime<Utc>` to convert.
    ///
    /// # Returns
    ///
    /// `Ok(CompactDateTime)` if the timestamp fits in `u32`, otherwise an
    /// error.
    ///
    /// # Errors
    ///
    /// Returns [`CompactDateTimeError::OutOfRange`] if the datetime is before
    /// 1970-01-01 or after 2106-02-07.
    fn try_from(value: DateTime<Utc>) -> Result<Self, Self::Error> {
        let seconds = value.timestamp();
        let nanos = value.timestamp_subsec_nanos();

        let seconds = if nanos >= 500_000_000 {
            seconds
                .checked_add(1)
                .ok_or(CompactDateTimeError::OutOfRange)?
        } else {
            seconds
        };

        let epoch_seconds = u32::try_from(seconds).map_err(|_| CompactDateTimeError::OutOfRange)?;

        Ok(CompactDateTime { epoch_seconds })
    }
}

impl Display for CompactDateTime {
    /// Formats the datetime using RFC 3339 (ISO 8601) in UTC.
    ///
    /// # Examples
    ///
    /// ```
    /// use prosody::timers::datetime::CompactDateTime;
    /// let dt = CompactDateTime::from(0_u32);
    /// assert_eq!(dt.to_string(), "1970-01-01 00:00:00 UTC");
    /// ```
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let time: DateTime<Utc> = (*self).into();
        write!(f, "{time}")
    }
}

impl Debug for CompactDateTime {
    /// Formats the datetime using the `{:?}` representation of `DateTime<Utc>`.
    ///
    /// # Examples
    ///
    /// ```
    /// use prosody::timers::datetime::CompactDateTime;
    /// let dt = CompactDateTime::from(0_u32);
    /// assert_eq!(format!("{dt:?}"), "1970-01-01T00:00:00Z");
    /// ```
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let time: DateTime<Utc> = (*self).into();
        write!(f, "{time:?}")
    }
}

impl From<CompactDateTime> for DateTime<Utc> {
    /// Converts a [`CompactDateTime`] into a `DateTime<Utc>`.
    ///
    /// # Returns
    ///
    /// A `DateTime<Utc>` corresponding to the same epoch second.
    fn from(value: CompactDateTime) -> Self {
        DateTime::UNIX_EPOCH + Duration::from_secs(u64::from(value.epoch_seconds))
    }
}

impl From<u32> for CompactDateTime {
    /// Creates a [`CompactDateTime`] from raw epoch seconds.
    ///
    /// # Arguments
    ///
    /// * `value` - Seconds since the Unix epoch.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use prosody::timers::datetime::CompactDateTime;
    /// let dt = CompactDateTime::from(12345_u32);
    /// assert_eq!(dt.epoch_seconds(), 12345);
    /// ```
    fn from(value: u32) -> Self {
        Self {
            epoch_seconds: value,
        }
    }
}

impl From<i32> for CompactDateTime {
    /// Creates a [`CompactDateTime`] from a signed epoch seconds by
    /// interpreting its bytes as little-endian.
    ///
    /// # Arguments
    ///
    /// * `value` - A signed 32-bit epoch seconds value.
    fn from(value: i32) -> Self {
        Self {
            epoch_seconds: u32::from_le_bytes(value.to_le_bytes()),
        }
    }
}

impl From<CompactDateTime> for i32 {
    /// Converts a [`CompactDateTime`] to a signed 32-bit epoch seconds by
    /// using little-endian representation.
    fn from(value: CompactDateTime) -> Self {
        i32::from_le_bytes(value.epoch_seconds.to_le_bytes())
    }
}

/// Errors that can occur when working with [`CompactDateTime`].
#[derive(Clone, Debug, Error)]
pub enum CompactDateTimeError {
    /// Indicates an attempt to create or calculate a time outside the
    /// representable range (before 1970-01-01 or after 2106-02-07).
    #[error("Time is out of range")]
    OutOfRange,

    /// Indicates that a time subtraction or duration calculation resulted
    /// in a negative interval.
    #[error("Time is in the past")]
    PastDateTime,
}

#[cfg(test)]
mod tests {
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
    fn test_epoch_seconds() {
        let dt = compact_datetime_from_epoch(12345_u32);
        assert_eq!(dt.epoch_seconds(), 12345_u32);
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

        let max_dt = CompactDateTime::MAX;
        let result = max_dt.add_duration(duration);
        assert!(matches!(result, Err(CompactDateTimeError::OutOfRange)));
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

        let datetime_with_nanos = Utc.timestamp_opt(12345, 500_000_000).single();
        assert!(datetime_with_nanos.is_some(), "Failed to create datetime");

        if let Some(datetime_with_nanos) = datetime_with_nanos {
            let compact_dt = CompactDateTime::try_from(datetime_with_nanos);
            assert!(compact_dt.is_ok(), "Failed to convert from DateTime");

            if let Ok(compact_dt) = compact_dt {
                assert_eq!(compact_dt.epoch_seconds(), 12346_u32);
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
    fn test_from_u32() {
        let compact_dt = CompactDateTime::from(12345_u32);
        assert_eq!(compact_dt.epoch_seconds(), 12345_u32);
    }

    #[test]
    fn test_from_i32() {
        let compact_dt = CompactDateTime::from(12345_i32);
        assert_eq!(compact_dt.epoch_seconds(), 12345_u32);
    }

    #[test]
    fn test_from_compact_datetime_to_i32() {
        let compact_dt = compact_datetime_from_epoch(12345_u32);
        let value: i32 = compact_dt.into();
        assert_eq!(value, 12345_i32);
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
    fn test_subtract_duration() {
        let dt = compact_datetime_from_epoch(2000_u32);
        let duration = CompactDuration::new(500_u32);

        let new_dt = dt.subtract_duration(duration);
        assert!(new_dt.is_ok(), "Failed to subtract duration");

        if let Ok(new_dt) = new_dt {
            assert_eq!(new_dt.epoch_seconds(), 1500_u32);
        }

        // Test underflow
        let min_dt = CompactDateTime::MIN;
        let result = min_dt.subtract_duration(duration);
        assert!(matches!(result, Err(CompactDateTimeError::OutOfRange)));

        // Test edge case: subtract from exact minimum
        let one_sec_dt = compact_datetime_from_epoch(1_u32);
        let one_sec_duration = CompactDuration::new(1_u32);
        let result = one_sec_dt.subtract_duration(one_sec_duration);
        assert!(
            result.is_ok(),
            "Should be able to subtract 1 second from epoch + 1"
        );
        if let Ok(result_dt) = result {
            assert_eq!(result_dt.epoch_seconds(), 0_u32);
        }
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
    fn test_from_i32_edge_cases() {
        // Test positive i32 value
        let positive_compact_dt = CompactDateTime::from(12345_i32);
        assert_eq!(positive_compact_dt.epoch_seconds(), 12345_u32);

        // Test negative i32 value (should be interpreted as large u32 due to byte
        // conversion)
        let negative_compact_dt = CompactDateTime::from(-1_i32);
        assert_eq!(negative_compact_dt.epoch_seconds(), u32::MAX);

        // Test max positive i32
        let max_positive_i32 = CompactDateTime::from(i32::MAX);
        assert_eq!(max_positive_i32.epoch_seconds(), i32::MAX as u32);

        // Test min negative i32 (most negative value)
        let min_negative_i32 = CompactDateTime::from(i32::MIN);
        assert_eq!(min_negative_i32.epoch_seconds(), 2_147_483_648_u32); // 2^31
    }

    #[test]
    fn test_from_compact_datetime_to_i32_edge_cases() {
        // Test conversion of large u32 values to i32
        let large_compact_dt = compact_datetime_from_epoch(u32::MAX);
        let converted: i32 = large_compact_dt.into();
        assert_eq!(converted, -1_i32);

        // Test roundtrip conversion
        let original_i32 = -12345_i32;
        let compact_dt = CompactDateTime::from(original_i32);
        let converted_back: i32 = compact_dt.into();
        assert_eq!(converted_back, original_i32);
    }

    #[test]
    fn test_boundary_dates() {
        // Test Unix epoch (1970-01-01 00:00:00 UTC)
        let epoch = compact_datetime_from_epoch(0_u32);
        let epoch_datetime: DateTime<Utc> = epoch.into();
        assert_eq!(epoch_datetime.timestamp(), 0);

        // Test year 2038 boundary (famous 32-bit timestamp limit for signed integers)
        let y2038_timestamp = 2_147_483_647_u32; // 2038-01-19 03:14:07 UTC
        let y2038 = compact_datetime_from_epoch(y2038_timestamp);
        let y2038_datetime: DateTime<Utc> = y2038.into();
        assert_eq!(y2038_datetime.timestamp(), i64::from(y2038_timestamp));

        // Test maximum representable date (2106-02-07 06:28:15 UTC)
        let max = CompactDateTime::MAX;
        let max_datetime: DateTime<Utc> = max.into();
        assert_eq!(max_datetime.timestamp(), i64::from(u32::MAX));
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
    fn prop_add_duration_increases_time(epoch_seconds: u32, duration_seconds: u32) -> bool {
        let compact_dt = CompactDateTime::from(epoch_seconds);
        let duration = CompactDuration::new(duration_seconds);

        // Simulate the expected behavior of CompactDateTime::add_duration
        if let Some(expected_sum) = epoch_seconds.checked_add(duration_seconds) {
            // If addition does not overflow, ensure the result matches
            match compact_dt.add_duration(duration) {
                Ok(new_dt) => {
                    new_dt.epoch_seconds() == expected_sum
                        && (duration_seconds == 0 || new_dt.epoch_seconds() >= epoch_seconds)
                }
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

    #[quickcheck]
    fn prop_subtract_duration_decreases_time(epoch_seconds: u32, duration_seconds: u32) -> bool {
        let compact_dt = CompactDateTime::from(epoch_seconds);
        let duration = CompactDuration::new(duration_seconds);

        // Simulate the expected behavior of CompactDateTime::subtract_duration
        if let Some(expected_diff) = epoch_seconds.checked_sub(duration_seconds) {
            // If subtraction does not underflow, ensure the result matches
            match compact_dt.subtract_duration(duration) {
                Ok(new_dt) => {
                    new_dt.epoch_seconds() == expected_diff
                        && (duration_seconds == 0 || new_dt.epoch_seconds() <= epoch_seconds)
                }
                Err(_) => false, // Unexpected error
            }
        } else {
            // If subtraction would underflow, ensure an error is returned
            matches!(
                compact_dt.subtract_duration(duration),
                Err(CompactDateTimeError::OutOfRange)
            )
        }
    }
}
