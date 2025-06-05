//! Compact duration representation for efficient timer calculations.
//!
//! This module provides [`CompactDuration`], a space-efficient representation of
//! time durations using 32-bit seconds. This compact format is optimized for
//! timer systems where storage efficiency and fast arithmetic operations are important.
//!
//! ## Design Rationale
//!
//! Standard duration types often use 64-bit representations or complex structures
//! with nanosecond precision. For timer systems that only need second-level precision,
//! this creates unnecessary overhead. [`CompactDuration`] addresses this by:
//!
//! - Using only 32 bits for storage (4 bytes vs 8+ bytes)
//! - Providing fast arithmetic operations
//! - Supporting durations up to ~136 years
//! - Maintaining second-level precision sufficient for most timer use cases
//!
//! ## Range and Precision
//!
//! The 32-bit seconds representation provides:
//!
//! - **Range**: 0 seconds to 4,294,967,295 seconds (~136 years)
//! - **Precision**: 1 second
//! - **Rounding**: Sub-second values are rounded to the nearest second
//!
//! ## Usage Examples
//!
//! ```rust,no_run
//! use prosody::timers::duration::CompactDuration;
//! use std::time::Duration;
//!
//! // Create from seconds
//! let duration = CompactDuration::new(3600); // 1 hour
//!
//! // Convert from/to std::time::Duration
//! let std_duration = Duration::from_secs(7200);
//! let compact = CompactDuration::try_from(std_duration).unwrap();
//! let back_to_std: Duration = compact.into();
//!
//! // Duration arithmetic
//! let total = duration.checked_add(compact).unwrap();
//! ```

use std::fmt::{Debug, Display, Formatter};
use std::time::Duration;
use thiserror::Error;

/// A compact duration representation using 32-bit seconds.
///
/// [`CompactDuration`] provides an efficient way to represent time durations
/// for timer systems where memory usage and fast operations are critical.
/// It stores duration as seconds, providing sufficient precision for most
/// timer-based applications while using minimal memory.
///
/// ## Storage Efficiency
///
/// - **Size**: 4 bytes (vs 8+ for standard duration types)
/// - **Alignment**: Optimal for CPU cache usage
/// - **Arithmetic**: Fast integer operations
///
/// ## Precision and Range
///
/// - **Precision**: 1 second (sub-second values are rounded)
/// - **Range**: 0 seconds to 4,294,967,295 seconds (~136 years)
/// - **Overflow**: Operations that would exceed the range return errors
///
/// ## Thread Safety
///
/// [`CompactDuration`] is [`Copy`] and all operations are thread-safe.
#[derive(Copy, Clone, Hash, PartialEq, Eq, Ord, PartialOrd)]
pub struct CompactDuration {
    seconds: u32,
}

impl CompactDuration {
    /// The maximum representable duration (~136 years).
    pub const MAX: Self = Self { seconds: u32::MAX };

    /// The minimum representable duration (0 seconds).
    pub const MIN: Self = Self { seconds: u32::MIN };

    /// Creates a new [`CompactDuration`] from the specified number of seconds.
    ///
    /// # Arguments
    ///
    /// * `seconds` - The number of seconds for this duration
    ///
    /// # Returns
    ///
    /// A new [`CompactDuration`] representing the specified time span.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use prosody::timers::duration::CompactDuration;
    ///
    /// let one_hour = CompactDuration::new(3600);
    /// let one_day = CompactDuration::new(86400);
    /// ```
    #[must_use]
    pub fn new(seconds: u32) -> Self {
        Self { seconds }
    }

    /// Returns the number of seconds in this duration.
    ///
    /// # Returns
    ///
    /// The duration in seconds as a [`u32`].
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use prosody::timers::duration::CompactDuration;
    ///
    /// let duration = CompactDuration::new(3600);
    /// assert_eq!(duration.seconds(), 3600);
    /// ```
    #[must_use]
    pub fn seconds(self) -> u32 {
        self.seconds
    }

    /// Adds two durations together with overflow checking.
    ///
    /// # Arguments
    ///
    /// * `other` - The [`CompactDuration`] to add to this one
    ///
    /// # Returns
    ///
    /// A [`Result`] containing the sum of the durations if successful,
    /// or a [`CompactDurationError`] if the result would overflow.
    ///
    /// # Errors
    ///
    /// Returns [`CompactDurationError::OutOfRange`] if adding the durations
    /// would result in a value greater than [`u32::MAX`] seconds.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use prosody::timers::duration::CompactDuration;
    ///
    /// let one_hour = CompactDuration::new(3600);
    /// let two_hours = CompactDuration::new(7200);
    /// let three_hours = one_hour.checked_add(two_hours).unwrap();
    /// assert_eq!(three_hours.seconds(), 10800);
    /// ```
    pub fn checked_add(self, other: Self) -> Result<Self, CompactDurationError> {
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

/// Errors that can occur when working with [`CompactDuration`].
#[derive(Clone, Debug, Error)]
pub enum CompactDurationError {
    /// The duration value is outside the representable range.
    ///
    /// [`CompactDuration`] can only represent durations from 0 to 4,294,967,295 seconds
    /// (approximately 136 years). This error occurs when attempting to create or
    /// calculate a duration outside this range.
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
        let result = duration1.checked_add(duration2);
        assert!(result.is_ok(), "Addition failed unexpectedly");
        assert_eq!(
            result.unwrap_or_else(|_| CompactDuration::new(0)).seconds(),
            3000
        );

        // Adding durations that exceed the maximum range
        let max_duration = CompactDuration::MAX;
        let result = max_duration.checked_add(CompactDuration::new(1));
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
