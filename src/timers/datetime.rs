//! Compact datetime representation for efficient timer storage and processing.
//!
//! This module provides [`CompactDateTime`], a space-efficient representation of
//! datetime values using 32-bit epoch seconds. This compact format is optimized
//! for timer systems where storage efficiency and fast comparisons are important.
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
//! - Maintaining second-level precision which is sufficient for most timer use cases
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
//! use prosody::timers::datetime::CompactDateTime;
//! use prosody::timers::duration::CompactDuration;
//! use chrono::{DateTime, Utc};
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
    /// A [`Result`] containing the current time as a [`CompactDateTime`] if successful,
    /// or a [`CompactDateTimeError`] if the current time is outside the representable range.
    ///
    /// # Errors
    ///
    /// Returns [`CompactDateTimeError::OutOfRange`] if the current system time
    /// is before 1970 or after 2106.
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
    /// The number of seconds since January 1, 1970 00:00:00 UTC as a [`u32`].
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
    /// * `other` - The earlier [`CompactDateTime`] to calculate duration from
    ///
    /// # Returns
    ///
    /// A [`Result`] containing the [`Duration`] between the two times if successful,
    /// or a [`CompactDateTimeError`] if the other time is in the future relative to this time.
    ///
    /// # Errors
    ///
    /// Returns [`CompactDateTimeError::PastDateTime`] if `other` is later than `self`.
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
    /// let duration = later.duration_since(earlier).unwrap();
    /// assert_eq!(duration, Duration::from_secs(1000));
    /// ```
    pub fn duration_since(self, other: Self) -> Result<Duration, CompactDateTimeError> {
        let seconds = self
            .epoch_seconds
            .checked_sub(other.epoch_seconds)
            .ok_or(CompactDateTimeError::PastDateTime)?;

        Ok(Duration::from_secs(u64::from(seconds)))
    }

    /// Calculates the duration from the current time to this datetime.
    ///
    /// This is a convenience method equivalent to `self.duration_since(CompactDateTime::now()?)`.
    ///
    /// # Returns
    ///
    /// A [`Result`] containing the [`Duration`] from now to this time if successful,
    /// or a [`CompactDateTimeError`] if this time is in the past or if the current
    /// time cannot be determined.
    ///
    /// # Errors
    ///
    /// - [`CompactDateTimeError::PastDateTime`] if this time is in the past
    /// - [`CompactDateTimeError::OutOfRange`] if the current time is outside the representable range
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use prosody::timers::datetime::CompactDateTime;
    /// use prosody::timers::duration::CompactDuration;
    ///
    /// let future_time = CompactDateTime::now().unwrap()
    ///     .add_duration(CompactDuration::new(3600)).unwrap();
    ///
    /// let time_until = future_time.duration_from_now().unwrap();
    /// assert!(time_until.as_secs() <= 3600);
    /// ```
    pub fn duration_from_now(self) -> Result<Duration, CompactDateTimeError> {
        self.duration_since(Self::now()?)
    }

    /// Adds a duration to this datetime.
    ///
    /// # Arguments
    ///
    /// * `duration` - The [`CompactDuration`] to add
    ///
    /// # Returns
    ///
    /// A [`Result`] containing the new [`CompactDateTime`] if successful,
    /// or a [`CompactDateTimeError`] if the result would be outside the representable range.
    ///
    /// # Errors
    ///
    /// Returns [`CompactDateTimeError::OutOfRange`] if adding the duration would
    /// result in a time after 2106-02-07 06:28:15 UTC.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use prosody::timers::datetime::CompactDateTime;
    /// use prosody::timers::duration::CompactDuration;
    ///
    /// let base_time = CompactDateTime::from(1000_u32);
    /// let later = base_time.add_duration(CompactDuration::new(500)).unwrap();
    /// assert_eq!(later.epoch_seconds(), 1500);
    /// ```
    pub fn add_duration(self, duration: CompactDuration) -> Result<Self, CompactDateTimeError> {
        let epoch_seconds = self
            .epoch_seconds
            .checked_add(duration.seconds())
            .ok_or(CompactDateTimeError::OutOfRange)?;

        Ok(Self { epoch_seconds })
    }

    /// Subtracts a duration from this datetime.
    ///
    /// # Arguments
    ///
    /// * `duration` - The [`CompactDuration`] to subtract
    ///
    /// # Returns
    ///
    /// A [`Result`] containing the new [`CompactDateTime`] if successful,
    /// or a [`CompactDateTimeError`] if the result would be outside the representable range.
    ///
    /// # Errors
    ///
    /// Returns [`CompactDateTimeError::OutOfRange`] if subtracting the duration would
    /// result in a time before 1970-01-01 00:00:00 UTC.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use prosody::timers::datetime::CompactDateTime;
    /// use prosody::timers::duration::CompactDuration;
    ///
    /// let base_time = CompactDateTime::from(2000_u32);
    /// let earlier = base_time.subtract_duration(CompactDuration::new(500)).unwrap();
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

    /// Converts a [`DateTime<Utc>`] to a [`CompactDateTime`].
    ///
    /// Sub-second precision is handled by rounding to the nearest second:
    /// - Nanoseconds >= 500,000,000 round up to the next second
    /// - Nanoseconds < 500,000,000 round down to the current second
    ///
    /// # Arguments
    ///
    /// * `value` - The [`DateTime<Utc>`] to convert
    ///
    /// # Returns
    ///
    /// A [`Result`] containing the [`CompactDateTime`] if the conversion succeeds,
    /// or a [`CompactDateTimeError`] if the datetime is outside the representable range.
    ///
    /// # Errors
    ///
    /// Returns [`CompactDateTimeError::OutOfRange`] if the datetime is before
    /// 1970-01-01 00:00:00 UTC or after 2106-02-07 06:28:15 UTC.
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
    /// Formats the datetime for display using RFC 3339 format.
    ///
    /// The output format is compatible with ISO 8601 and RFC 3339 standards,
    /// showing the datetime in UTC timezone.
    ///
    /// # Examples
    ///
    /// Output format: `1970-01-01 03:25:45 UTC`
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let time: DateTime<Utc> = (*self).into();
        write!(f, "{time}")
    }
}

impl Debug for CompactDateTime {
    /// Formats the datetime for debugging using ISO 8601 format.
    ///
    /// The debug output uses the standard ISO 8601 format for precise
    /// debugging and logging purposes.
    ///
    /// # Examples
    ///
    /// Output format: `1970-01-01T03:25:45Z`
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let time: DateTime<Utc> = (*self).into();
        write!(f, "{time:?}")
    }
}

impl From<CompactDateTime> for DateTime<Utc> {
    /// Converts a [`CompactDateTime`] to a [`DateTime<Utc>`].
    ///
    /// This conversion is infallible as all [`CompactDateTime`] values
    /// represent valid times within the range supported by [`DateTime<Utc>`].
    ///
    /// # Arguments
    ///
    /// * `value` - The [`CompactDateTime`] to convert
    ///
    /// # Returns
    ///
    /// A [`DateTime<Utc>`] representing the same instant in time.
    fn from(value: CompactDateTime) -> Self {
        DateTime::UNIX_EPOCH + Duration::from_secs(u64::from(value.epoch_seconds))
    }
}

impl From<u32> for CompactDateTime {
    /// Creates a [`CompactDateTime`] from epoch seconds.
    ///
    /// # Arguments
    ///
    /// * `value` - The number of seconds since the Unix epoch
    ///
    /// # Returns
    ///
    /// A [`CompactDateTime`] representing the specified time.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use prosody::timers::datetime::CompactDateTime;
    ///
    /// let time = CompactDateTime::from(1234567890_u32);
    /// assert_eq!(time.epoch_seconds(), 1234567890);
    /// ```
    fn from(value: u32) -> Self {
        Self {
            epoch_seconds: value,
        }
    }
}

impl From<i32> for CompactDateTime {
    /// Creates a [`CompactDateTime`] from a signed 32-bit epoch seconds value.
    ///
    /// The conversion treats the `i32` value as an unsigned value using
    /// little-endian byte representation. This allows handling of values
    /// that might be represented as negative in signed arithmetic but
    /// represent valid epoch times.
    ///
    /// # Arguments
    ///
    /// * `value` - The signed epoch seconds value
    ///
    /// # Returns
    ///
    /// A [`CompactDateTime`] representing the specified time.
    fn from(value: i32) -> Self {
        Self {
            epoch_seconds: u32::from_le_bytes(value.to_le_bytes()),
        }
    }
}

impl From<CompactDateTime> for i32 {
    /// Converts a [`CompactDateTime`] to a signed 32-bit epoch seconds value.
    ///
    /// The conversion uses little-endian byte representation to maintain
    /// bijection with the [`From<i32>`] implementation.
    ///
    /// # Arguments
    ///
    /// * `value` - The [`CompactDateTime`] to convert
    ///
    /// # Returns
    ///
    /// A signed 32-bit representation of the epoch seconds.
    fn from(value: CompactDateTime) -> Self {
        i32::from_le_bytes(value.epoch_seconds.to_le_bytes())
    }
}

/// Errors that can occur when working with [`CompactDateTime`].
#[derive(Clone, Debug, Error)]
pub enum CompactDateTimeError {
    /// The time value is outside the representable range.
    ///
    /// [`CompactDateTime`] can only represent times between 1970-01-01 00:00:00 UTC
    /// and 2106-02-07 06:28:15 UTC. This error occurs when attempting to create
    /// or calculate a time outside this range.
    #[error("Time is out of range")]
    OutOfRange,

    /// The specified time is in the past relative to another time.
    ///
    /// This error occurs when attempting to calculate a duration where the
    /// end time is earlier than the start time.
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

        match Utc.timestamp_opt(i64::MAX, 0) {
            chrono::LocalResult::Single(out_of_range_datetime) => {
                let result = CompactDateTime::try_from(out_of_range_datetime);
                assert!(matches!(result, Err(CompactDateTimeError::OutOfRange)));
            }
            _ => {} // No need for `assert!(true)` here
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
}
