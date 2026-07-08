//! Memory-efficient datetime using 32-bit epoch seconds.
//!
//! Provides [`CompactDateTime`] which stores timestamps as 32-bit epoch
//! seconds, reducing memory usage for timer systems that process large volumes
//! of events. Supports the range 1970-2106 with second-level precision.

use crate::error::{ClassifyError, ErrorCategory};
use crate::timers::duration::CompactDuration;
use chrono::{DateTime, Utc};
use std::fmt::{Debug, Display, Formatter, Result as FmtResult};
use std::time::{Duration, SystemTime};
use thiserror::Error;

/// Nanosecond threshold for rounding up to the next second (500ms).
const ROUND_UP_NANOS: u32 = 500_000_000;

/// 32-bit datetime stored as epoch seconds.
///
/// Stores time as seconds since Unix epoch (1970-01-01 UTC). Uses 4 bytes
/// instead of 8+ bytes for standard datetime types. Supports range 1970-2106
/// with second precision.
///
/// # Rounding Behavior
///
/// Conversions from higher-precision types round to the nearest second:
/// sub-second values ≥500ms round up, <500ms round down.
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

    /// Creates a `CompactDateTime` from the current system time.
    ///
    /// # Errors
    ///
    /// Returns `CompactDateTimeError::OutOfRange` if the system time is outside
    /// the 1970-2106 range.
    pub fn now() -> Result<Self, CompactDateTimeError> {
        Self::try_from(Utc::now())
    }

    /// Returns the stored epoch seconds value.
    #[must_use]
    pub fn epoch_seconds(self) -> u32 {
        self.epoch_seconds
    }

    /// Calculates duration from `other` to `self`.
    ///
    /// # Errors
    ///
    /// Returns `CompactDateTimeError::PastDateTime` if `other` is later than
    /// `self`.
    pub fn duration_since(self, other: Self) -> Result<Duration, CompactDateTimeError> {
        let seconds = self
            .epoch_seconds
            .checked_sub(other.epoch_seconds)
            .ok_or(CompactDateTimeError::PastDateTime)?;

        Ok(Duration::from_secs(u64::from(seconds)))
    }

    /// Calculates the compact duration between this time and an earlier time.
    ///
    /// This is a more efficient version of
    /// [`duration_since`](Self::duration_since) that returns a
    /// [`CompactDuration`] instead of a [`std::time::Duration`].
    ///
    /// # Errors
    ///
    /// Returns [`CompactDateTimeError::PastDateTime`] if `other` is later
    /// than `self`.
    pub(crate) fn compact_duration_since(
        self,
        other: Self,
    ) -> Result<CompactDuration, CompactDateTimeError> {
        Ok(CompactDuration::new(
            self.epoch_seconds
                .checked_sub(other.epoch_seconds)
                .ok_or(CompactDateTimeError::PastDateTime)?,
        ))
    }

    /// Calculates the duration from now until this datetime.
    ///
    /// # Errors
    ///
    /// - [`CompactDateTimeError::OutOfRange`] if current time is out of range.
    /// - [`CompactDateTimeError::PastDateTime`] if `self` is in the past.
    pub fn duration_from_now(self) -> Result<Duration, CompactDateTimeError> {
        self.duration_since(Self::now()?)
    }

    /// Calculates the compact duration from now until this datetime.
    ///
    /// More efficient version of [`duration_from_now`](Self::duration_from_now)
    /// that returns a [`CompactDuration`] instead of a [`std::time::Duration`].
    ///
    /// # Errors
    ///
    /// - [`CompactDateTimeError::OutOfRange`] if current time is out of range.
    /// - [`CompactDateTimeError::PastDateTime`] if `self` is in the past.
    pub fn compact_duration_from_now(self) -> Result<CompactDuration, CompactDateTimeError> {
        self.compact_duration_since(Self::now()?)
    }

    /// Adds a [`CompactDuration`] to this datetime.
    ///
    /// # Errors
    ///
    /// Returns [`CompactDateTimeError::OutOfRange`] if the result exceeds the
    /// maximum representable time.
    pub fn add_duration(self, duration: CompactDuration) -> Result<Self, CompactDateTimeError> {
        let epoch_seconds = self
            .epoch_seconds
            .checked_add(duration.seconds())
            .ok_or(CompactDateTimeError::OutOfRange)?;

        Ok(Self { epoch_seconds })
    }
}

/// Converts epoch seconds and nanoseconds to `CompactDateTime`, rounding to
/// the nearest second.
///
/// Rounding happens in the wide `i64` seconds before narrowing to `u32`: a
/// pre-epoch instant like -0.5s (`seconds = -1`, `nanos = 500_000_000`) rounds
/// up to `0` here, which is valid, but would spuriously report out-of-range if
/// narrowed to `u32` first.
fn from_seconds_nanos(seconds: i64, nanos: u32) -> Result<CompactDateTime, CompactDateTimeError> {
    let seconds = if nanos >= ROUND_UP_NANOS {
        seconds
            .checked_add(1)
            .ok_or(CompactDateTimeError::OutOfRange)?
    } else {
        seconds
    };
    let epoch_seconds = u32::try_from(seconds).map_err(|_| CompactDateTimeError::OutOfRange)?;
    Ok(CompactDateTime { epoch_seconds })
}

impl TryFrom<DateTime<Utc>> for CompactDateTime {
    type Error = CompactDateTimeError;

    /// Converts a [`DateTime<Utc>`] to a [`CompactDateTime`].
    ///
    /// Rounds sub-second precision to the nearest second (≥500ms rounds up).
    ///
    /// # Errors
    ///
    /// Returns [`CompactDateTimeError::OutOfRange`] if the datetime is before
    /// 1970-01-01 or after 2106-02-07.
    fn try_from(value: DateTime<Utc>) -> Result<Self, Self::Error> {
        let seconds = value.timestamp();
        let nanos = value.timestamp_subsec_nanos();
        from_seconds_nanos(seconds, nanos)
    }
}

impl TryFrom<SystemTime> for CompactDateTime {
    type Error = CompactDateTimeError;

    /// Converts a [`SystemTime`] to a [`CompactDateTime`].
    ///
    /// Rounds sub-second precision to the nearest second (≥500ms rounds up).
    ///
    /// # Errors
    ///
    /// Returns [`CompactDateTimeError::OutOfRange`] if the time is before
    /// 1970-01-01 or after 2106-02-07.
    fn try_from(value: SystemTime) -> Result<Self, Self::Error> {
        let duration = value
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_err(|_| CompactDateTimeError::OutOfRange)?;
        let seconds =
            i64::try_from(duration.as_secs()).map_err(|_| CompactDateTimeError::OutOfRange)?;
        from_seconds_nanos(seconds, duration.subsec_nanos())
    }
}

impl From<CompactDateTime> for SystemTime {
    /// Converts a [`CompactDateTime`] to a [`SystemTime`] at the same epoch
    /// second.
    fn from(value: CompactDateTime) -> Self {
        SystemTime::UNIX_EPOCH + Duration::from_secs(u64::from(value.epoch_seconds))
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
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
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
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        let time: DateTime<Utc> = (*self).into();
        write!(f, "{time:?}")
    }
}

impl From<CompactDateTime> for DateTime<Utc> {
    /// Converts a [`CompactDateTime`] into a `DateTime<Utc>` at the same
    /// epoch second.
    fn from(value: CompactDateTime) -> Self {
        DateTime::UNIX_EPOCH + Duration::from_secs(u64::from(value.epoch_seconds))
    }
}

impl From<u32> for CompactDateTime {
    /// Creates a [`CompactDateTime`] from raw epoch seconds.
    fn from(value: u32) -> Self {
        Self {
            epoch_seconds: value,
        }
    }
}

impl From<i32> for CompactDateTime {
    /// Creates a [`CompactDateTime`] from a signed epoch seconds value by
    /// interpreting its bytes as little-endian.
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

impl ClassifyError for CompactDateTimeError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            // Data-dependent bad time value or ordering; dropping beats retrying forever.
            Self::OutOfRange | Self::PastDateTime => ErrorCategory::Permanent,
        }
    }
}

#[cfg(test)]
mod tests;
