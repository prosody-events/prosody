use std::fmt::{Debug, Display, Formatter};
use std::time::Duration;
use thiserror::Error;

#[derive(Copy, Clone, Hash, PartialEq, Eq, Ord, PartialOrd)]
pub struct CompactDuration {
    seconds: u32,
}

impl CompactDuration {
    pub const MAX: Self = Self { seconds: u32::MAX };
    pub const MIN: Self = Self { seconds: u32::MIN };

    pub fn new(seconds: u32) -> Self {
        Self { seconds }
    }

    pub fn seconds(self) -> u32 {
        self.seconds
    }

    pub fn add(self, other: Self) -> Result<Self, CompactDurationError> {
        Ok(Self {
            seconds: self
                .seconds
                .checked_add(other.seconds)
                .ok_or(CompactDurationError::OutOfRange)?,
        })
    }
}

impl From<CompactDuration> for Duration {
    fn from(value: CompactDuration) -> Self {
        Duration::from_secs(u64::from(value.seconds))
    }
}

impl TryFrom<Duration> for CompactDuration {
    type Error = CompactDurationError;

    fn try_from(value: Duration) -> Result<Self, Self::Error> {
        let seconds = value.as_secs();
        let nanos = value.subsec_nanos();

        let seconds = if nanos >= 500_000_000 {
            seconds
                .checked_add(1)
                .ok_or(CompactDurationError::OutOfRange)?
        } else {
            seconds
        };

        Ok(Self {
            seconds: u32::try_from(seconds).map_err(|_| CompactDurationError::OutOfRange)?,
        })
    }
}

impl Display for CompactDuration {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let duration: Duration = (*self).into();
        let duration: humantime::Duration = duration.into();
        write!(f, "{duration}")
    }
}

impl Debug for CompactDuration {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let duration: Duration = (*self).into();
        let duration: humantime::Duration = duration.into();
        write!(f, "{duration:?}")
    }
}

#[derive(Clone, Debug, Error)]
pub enum CompactDurationError {
    #[error("Duration is out of range")]
    OutOfRange,
}
