use crate::timers::duration::CompactDuration;
use chrono::{DateTime, Utc};
use std::fmt::{Debug, Display, Formatter};
use std::time::Duration;
use thiserror::Error;

#[derive(Copy, Clone, Hash, PartialEq, Eq, Ord, PartialOrd)]
pub struct CompactDateTime {
    epoch_seconds: u32,
}

impl CompactDateTime {
    pub const MAX: Self = Self {
        epoch_seconds: u32::MAX,
    };
    pub const MIN: Self = Self {
        epoch_seconds: u32::MIN,
    };

    pub fn now() -> Result<Self, CompactDateTimeError> {
        Self::try_from(Utc::now())
    }

    pub fn epoch_seconds(self) -> u32 {
        self.epoch_seconds
    }

    pub fn duration_since(self, other: Self) -> Result<Duration, CompactDateTimeError> {
        let seconds = self
            .epoch_seconds
            .checked_sub(other.epoch_seconds)
            .ok_or(CompactDateTimeError::PastDateTime)?;

        Ok(Duration::from_secs(u64::from(seconds)))
    }

    pub fn duration_from_now(self) -> Result<Duration, CompactDateTimeError> {
        self.duration_since(Self::now()?)
    }

    pub fn add_duration(self, duration: CompactDuration) -> Result<Self, CompactDateTimeError> {
        let epoch_seconds = self
            .epoch_seconds
            .checked_add(duration.seconds())
            .ok_or(CompactDateTimeError::OutOfRange)?;

        Ok(Self { epoch_seconds })
    }
}

impl TryFrom<DateTime<Utc>> for CompactDateTime {
    type Error = CompactDateTimeError;

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
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let time: DateTime<Utc> = (*self).into();
        write!(f, "{time}")
    }
}

impl Debug for CompactDateTime {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let time: DateTime<Utc> = (*self).into();
        write!(f, "{time:?}")
    }
}

impl From<CompactDateTime> for DateTime<Utc> {
    fn from(value: CompactDateTime) -> Self {
        DateTime::UNIX_EPOCH + Duration::from_secs(u64::from(value.epoch_seconds))
    }
}

impl From<u32> for CompactDateTime {
    fn from(value: u32) -> Self {
        Self {
            epoch_seconds: value,
        }
    }
}

impl From<i32> for CompactDateTime {
    fn from(value: i32) -> Self {
        Self {
            epoch_seconds: u32::from_le_bytes(value.to_le_bytes()),
        }
    }
}

impl From<CompactDateTime> for i32 {
    fn from(value: CompactDateTime) -> Self {
        i32::from_le_bytes(value.epoch_seconds.to_le_bytes())
    }
}

#[derive(Clone, Debug, Error)]
pub enum CompactDateTimeError {
    #[error("Time is out of range")]
    OutOfRange,

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
