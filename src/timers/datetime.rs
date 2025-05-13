use chrono::{DateTime, Utc};
use std::time::Duration;
use thiserror::Error;

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq, Ord, PartialOrd)]
pub struct CompactDateTime {
    epoch_seconds: u32,
}

impl CompactDateTime {
    pub fn now() -> Result<Self, CompactDateTimeError> {
        Self::try_from(Utc::now())
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
}

impl TryFrom<DateTime<Utc>> for CompactDateTime {
    type Error = CompactDateTimeError;

    fn try_from(value: DateTime<Utc>) -> Result<Self, Self::Error> {
        let secs = value.timestamp();
        let nanos = value.timestamp_subsec_nanos();

        let rounded_secs = if nanos >= 500_000_000 {
            secs.checked_add(1)
                .ok_or(CompactDateTimeError::OutOfRange)?
        } else {
            secs
        };

        let epoch_seconds =
            u32::try_from(rounded_secs).map_err(|_| CompactDateTimeError::OutOfRange)?;

        Ok(CompactDateTime { epoch_seconds })
    }
}

impl From<CompactDateTime> for DateTime<Utc> {
    fn from(value: CompactDateTime) -> Self {
        DateTime::UNIX_EPOCH + Duration::from_secs(u64::from(value.epoch_seconds))
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
    #[error("DateTime is out of range")]
    OutOfRange,

    #[error("Date time is in the past")]
    PastDateTime,
}
