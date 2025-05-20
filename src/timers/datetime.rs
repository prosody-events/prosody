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
