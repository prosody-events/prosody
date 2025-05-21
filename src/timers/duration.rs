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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_new() {
        let duration = CompactDuration::new(12345);
        assert_eq!(duration.seconds(), 12345);
    }

    #[test]
    fn test_add() {
        let duration1 = CompactDuration::new(1000);
        let duration2 = CompactDuration::new(2000);

        // Adding two durations within range
        let result = duration1.add(duration2);
        assert!(result.is_ok(), "Addition failed unexpectedly");
        assert_eq!(
            result.unwrap_or_else(|_| CompactDuration::new(0)).seconds(),
            3000
        );

        // Adding durations that exceed the maximum range
        let max_duration = CompactDuration::MAX;
        let result = max_duration.add(CompactDuration::new(1));
        assert!(
            matches!(result, Err(CompactDurationError::OutOfRange)),
            "Expected OutOfRange error"
        );
    }

    #[test]
    fn test_from_compact_duration_to_duration() {
        let compact_duration = CompactDuration::new(12345);
        let duration: Duration = compact_duration.into();
        assert_eq!(duration.as_secs(), 12345);
    }

    #[test]
    fn test_try_from_duration_to_compact_duration() {
        let duration = Duration::from_secs(12345);
        let compact_duration = CompactDuration::try_from(duration);
        assert!(compact_duration.is_ok(), "Conversion failed unexpectedly");
        assert_eq!(
            compact_duration
                .unwrap_or_else(|_| CompactDuration::new(0))
                .seconds(),
            12345
        );

        // Test with a duration that exceeds the maximum range
        let large_duration = Duration::from_secs(u64::from(u32::MAX) + 1);
        let result = CompactDuration::try_from(large_duration);
        assert!(
            matches!(result, Err(CompactDurationError::OutOfRange)),
            "Expected OutOfRange error"
        );

        // Test rounding up when nanoseconds are >= 500_000_000
        let duration_with_nanos = Duration::new(12345, 500_000_000);
        let compact_duration = CompactDuration::try_from(duration_with_nanos);
        assert!(compact_duration.is_ok(), "Rounding failed unexpectedly");
        assert_eq!(
            compact_duration
                .unwrap_or_else(|_| CompactDuration::new(0))
                .seconds(),
            12346
        );
    }

    #[test]
    fn test_display() {
        let compact_duration = CompactDuration::new(3661); // 1 hour, 1 minute, 1 second
        let display = format!("{compact_duration}");
        assert_eq!(display, "1h 1m 1s");
    }

    #[test]
    fn test_debug() {
        let compact_duration = CompactDuration::new(3661); // 1 hour, 1 minute, 1 second
        let debug = format!("{compact_duration:?}");
        assert_eq!(debug, "Duration(3661s)");
    }

    #[test]
    fn test_constants() {
        assert_eq!(CompactDuration::MAX.seconds(), u32::MAX);
        assert_eq!(CompactDuration::MIN.seconds(), u32::MIN);
    }

    #[test]
    fn test_edge_cases() {
        // Test with zero duration
        let zero_duration = CompactDuration::new(0);
        assert_eq!(zero_duration.seconds(), 0);

        // Test with maximum duration
        let max_duration = CompactDuration::MAX;
        assert_eq!(max_duration.seconds(), u32::MAX);
    }
}
