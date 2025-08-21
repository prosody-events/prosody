//! Compact duration representation for efficient timer calculations.
//!
//! Provides [`CompactDuration`], a space-efficient duration type using 32-bit
//! seconds. Reduces memory usage and enables fast arithmetic for timer systems
//! requiring only second-level precision.

use std::fmt::{Debug, Display, Formatter, Result as FmtResult};
use std::ops::{Add, Sub};
use std::time::Duration;
use thiserror::Error;

/// A compact duration using 32-bit seconds.
///
/// Stores duration as seconds in a [`u32`], supporting 0 to [`u32::MAX`]
/// seconds (~136 years). Enables efficient arithmetic with minimal memory
/// footprint for timer systems.
#[derive(Copy, Clone, Hash, PartialEq, Eq, Ord, PartialOrd)]
pub struct CompactDuration {
    seconds: u32,
}

impl CompactDuration {
    /// The maximum representable duration (~136 years).
    pub const MAX: Self = Self { seconds: u32::MAX };
    /// The minimum representable duration (0 seconds).
    pub const MIN: Self = Self { seconds: u32::MIN };

    /// Creates a new `CompactDuration` from the specified number of seconds.
    ///
    /// # Arguments
    ///
    /// * `seconds` - The number of seconds for this duration.
    ///
    /// # Examples
    ///
    /// ```
    /// use prosody::timers::duration::CompactDuration;
    ///
    /// let one_hour = CompactDuration::new(3600);
    /// assert_eq!(one_hour.seconds(), 3600);
    /// ```
    #[must_use]
    pub fn new(seconds: u32) -> Self {
        Self { seconds }
    }

    /// Returns the number of seconds in this duration.
    #[must_use]
    pub fn seconds(self) -> u32 {
        self.seconds
    }

    /// Adds two durations with overflow checking.
    ///
    /// # Arguments
    ///
    /// * `other` - The [`CompactDuration`] to add.
    ///
    /// # Errors
    ///
    /// Returns [`CompactDurationError::OutOfRange`] if the result exceeds
    /// [`u32::MAX`] seconds.
    pub fn checked_add(self, other: Self) -> Result<Self, CompactDurationError> {
        Ok(Self {
            seconds: self
                .seconds
                .checked_add(other.seconds)
                .ok_or(CompactDurationError::OutOfRange)?,
        })
    }

    /// Adds two durations with saturation at [`Self::MAX`].
    ///
    /// # Arguments
    ///
    /// * `other` - The [`CompactDuration`] to add.
    ///
    /// # Examples
    ///
    /// ```
    /// use prosody::timers::duration::CompactDuration;
    ///
    /// let a = CompactDuration::new(1000);
    /// let b = CompactDuration::new(2000);
    /// assert_eq!(a.saturating_add(b).seconds(), 3000);
    ///
    /// let max = CompactDuration::MAX;
    /// let one = CompactDuration::new(1);
    /// assert_eq!(max.saturating_add(one), CompactDuration::MAX);
    /// ```
    #[must_use]
    pub fn saturating_add(self, other: Self) -> Self {
        Self {
            seconds: self.seconds.saturating_add(other.seconds),
        }
    }

    /// Subtracts two durations with overflow checking.
    ///
    /// # Arguments
    ///
    /// * `other` - The [`CompactDuration`] to subtract.
    ///
    /// # Errors
    ///
    /// Returns [`CompactDurationError::OutOfRange`] if `other` is greater than
    /// `self`, which would result in a negative duration.
    pub fn checked_sub(self, other: Self) -> Result<Self, CompactDurationError> {
        Ok(Self {
            seconds: self
                .seconds
                .checked_sub(other.seconds)
                .ok_or(CompactDurationError::OutOfRange)?,
        })
    }

    /// Subtracts two durations with saturation at [`Self::MIN`].
    ///
    /// # Arguments
    ///
    /// * `other` - The [`CompactDuration`] to subtract.
    ///
    /// # Examples
    ///
    /// ```
    /// use prosody::timers::duration::CompactDuration;
    ///
    /// let a = CompactDuration::new(3000);
    /// let b = CompactDuration::new(1000);
    /// assert_eq!(a.saturating_sub(b).seconds(), 2000);
    ///
    /// let min = CompactDuration::new(100);
    /// let large = CompactDuration::new(1000);
    /// assert_eq!(min.saturating_sub(large), CompactDuration::MIN);
    /// ```
    #[must_use]
    pub fn saturating_sub(self, other: Self) -> Self {
        Self {
            seconds: self.seconds.saturating_sub(other.seconds),
        }
    }
}

impl Add for CompactDuration {
    type Output = Self;

    /// Adds two durations using saturating arithmetic.
    ///
    /// If the result would overflow, returns [`Self::MAX`].
    fn add(self, rhs: Self) -> Self::Output {
        self.saturating_add(rhs)
    }
}

impl Sub for CompactDuration {
    type Output = Self;

    /// Subtracts two durations using saturating arithmetic.
    ///
    /// If the result would underflow, returns [`Self::MIN`].
    fn sub(self, rhs: Self) -> Self::Output {
        self.saturating_sub(rhs)
    }
}

impl From<CompactDuration> for Duration {
    /// Converts [`CompactDuration`] into a standard [`Duration`].
    fn from(value: CompactDuration) -> Self {
        Duration::from_secs(u64::from(value.seconds))
    }
}

impl From<CompactDuration> for i32 {
    fn from(value: CompactDuration) -> Self {
        i32::from_le_bytes(value.seconds.to_le_bytes())
    }
}

impl From<u32> for CompactDuration {
    fn from(seconds: u32) -> Self {
        CompactDuration::new(seconds)
    }
}

impl From<i32> for CompactDuration {
    fn from(value: i32) -> Self {
        CompactDuration::new(u32::from_le_bytes(value.to_le_bytes()))
    }
}

impl TryFrom<Duration> for CompactDuration {
    type Error = CompactDurationError;

    /// Converts a standard [`Duration`] into [`CompactDuration`].
    ///
    /// Rounds sub-second nanoseconds to the nearest whole second.
    /// Nanoseconds >= 500,000,000 round up to the next second.
    ///
    /// # Arguments
    ///
    /// * `value` - The [`Duration`] to convert.
    ///
    /// # Errors
    ///
    /// Returns [`CompactDurationError::OutOfRange`] if the computed seconds
    /// exceed [`u32::MAX`] or if rounding causes overflow.
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
    /// Formats the duration in a human-readable form.
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        let duration: Duration = (*self).into();
        let duration: humantime::Duration = duration.into();
        write!(f, "{duration}")
    }
}

impl Debug for CompactDuration {
    /// Displays the debug representation.
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        let duration: Duration = (*self).into();
        let duration: humantime::Duration = duration.into();
        write!(f, "{duration:?}")
    }
}

/// Errors that can occur when working with [`CompactDuration`].
#[derive(Clone, Debug, Error)]
pub enum CompactDurationError {
    /// The duration is outside the representable range of `0..=u32::MAX`
    /// seconds.
    #[error("Duration is out of range")]
    OutOfRange,
}

#[cfg(test)]
mod tests {
    use super::*;
    use color_eyre::eyre::Result;
    use quickcheck_macros::quickcheck;
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

    #[test]
    fn test_rounding_behavior() -> Result<()> {
        // Test rounding down when nanoseconds are < 500_000_000
        let duration_round_down = Duration::new(1234, 499_999_999);
        let compact = CompactDuration::try_from(duration_round_down)?;
        assert_eq!(compact.seconds(), 1234);

        // Test rounding up when nanoseconds are > 500_000_000
        let duration_round_up = Duration::new(1234, 500_000_001);
        let compact = CompactDuration::try_from(duration_round_up)?;
        assert_eq!(compact.seconds(), 1235);

        // Test exact boundary case (500_000_000 nanoseconds)
        let duration_exact_boundary = Duration::new(1234, 500_000_000);
        let compact = CompactDuration::try_from(duration_exact_boundary)?;
        assert_eq!(compact.seconds(), 1235);

        // Test zero nanoseconds (no rounding)
        let duration_no_nanos = Duration::new(1234, 0);
        let compact = CompactDuration::try_from(duration_no_nanos)?;
        assert_eq!(compact.seconds(), 1234);
        Ok(())
    }

    #[test]
    fn test_overflow_during_rounding() {
        // Test overflow when rounding up from u32::MAX seconds
        let duration_overflow = Duration::new(u64::from(u32::MAX), 500_000_000);
        let result = CompactDuration::try_from(duration_overflow);
        assert!(matches!(result, Err(CompactDurationError::OutOfRange)));
    }

    #[test]
    fn test_comparison_traits() {
        let duration1 = CompactDuration::new(1000);
        let duration2 = CompactDuration::new(2000);
        let duration1_copy = CompactDuration::new(1000);

        // Test equality
        assert_eq!(duration1, duration1_copy);
        assert_ne!(duration1, duration2);

        // Test ordering
        assert!(duration1 < duration2);
        assert!(duration2 > duration1);
        assert!(duration1 <= duration1_copy);
        assert!(duration1 >= duration1_copy);
        assert!(duration1 <= duration2);
        assert!(duration2 >= duration1);
    }

    #[test]
    fn test_hash_trait() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let duration1 = CompactDuration::new(1000);
        let duration2 = CompactDuration::new(1000);
        let duration3 = CompactDuration::new(2000);

        let mut hasher1 = DefaultHasher::new();
        let mut hasher2 = DefaultHasher::new();
        let mut hasher3 = DefaultHasher::new();

        duration1.hash(&mut hasher1);
        duration2.hash(&mut hasher2);
        duration3.hash(&mut hasher3);

        // Equal durations should have equal hashes
        assert_eq!(hasher1.finish(), hasher2.finish());
        // Different durations should likely have different hashes
        assert_ne!(hasher1.finish(), hasher3.finish());
    }

    #[test]
    fn test_copy_clone_behavior() {
        let original = CompactDuration::new(1234);

        // Test Copy trait (implicit)
        let copied = original;
        assert_eq!(copied.seconds(), 1234);
        assert_eq!(original.seconds(), 1234); // Original should still be usable

        // Test Clone trait (explicit)
        let cloned = original;
        assert_eq!(cloned.seconds(), 1234);
        assert_eq!(original.seconds(), 1234);
    }

    #[test]
    fn test_checked_add_edge_cases() -> Result<()> {
        let zero = CompactDuration::new(0);
        let some_duration = CompactDuration::new(1000);
        let max_duration = CompactDuration::MAX;

        // Adding zero should not change the value
        assert_eq!(some_duration.checked_add(zero)?.seconds(), 1000);
        assert_eq!(zero.checked_add(some_duration)?.seconds(), 1000);

        // Adding zero to max should still be max
        assert_eq!(max_duration.checked_add(zero)?.seconds(), u32::MAX);

        // Adding any non-zero value to max should overflow
        let one = CompactDuration::new(1);
        assert!(matches!(
            max_duration.checked_add(one),
            Err(CompactDurationError::OutOfRange)
        ));

        // Test near-overflow cases
        let near_max = CompactDuration::new(u32::MAX - 5);
        let five = CompactDuration::new(5);
        let six = CompactDuration::new(6);

        assert_eq!(near_max.checked_add(five)?.seconds(), u32::MAX);
        assert!(matches!(
            near_max.checked_add(six),
            Err(CompactDurationError::OutOfRange)
        ));
        Ok(())
    }

    #[test]
    fn test_precision_loss_documentation() -> Result<()> {
        // Verify that subsecond precision is indeed lost
        let precise_duration = Duration::new(5, 123_456_789);
        let compact = CompactDuration::try_from(precise_duration)?;
        let back_to_duration: Duration = compact.into();

        // Should lose nanosecond precision
        assert_eq!(back_to_duration.as_secs(), 5);
        assert_eq!(back_to_duration.subsec_nanos(), 0);

        // Original had subsecond precision, converted back should not
        assert_ne!(
            precise_duration.subsec_nanos(),
            back_to_duration.subsec_nanos()
        );
        Ok(())
    }

    #[test]
    fn test_range_boundaries() -> Result<()> {
        // Test exactly at the boundaries
        let min_duration = Duration::from_secs(0);
        let max_valid_duration = Duration::from_secs(u64::from(u32::MAX));
        let just_over_max = Duration::from_secs(u64::from(u32::MAX) + 1);

        // Min should work
        let compact_min = CompactDuration::try_from(min_duration)?;
        assert_eq!(compact_min.seconds(), 0);

        // Max should work
        let compact_max = CompactDuration::try_from(max_valid_duration)?;
        assert_eq!(compact_max.seconds(), u32::MAX);

        // Just over max should fail
        assert!(matches!(
            CompactDuration::try_from(just_over_max),
            Err(CompactDurationError::OutOfRange)
        ));
        Ok(())
    }

    #[test]
    fn test_add_operator() {
        let a = CompactDuration::new(1000);
        let b = CompactDuration::new(2000);
        let result = a + b;
        assert_eq!(result.seconds(), 3000);

        // Test overflow saturation
        let max = CompactDuration::MAX;
        let one = CompactDuration::new(1);
        let result = max + one;
        assert_eq!(result, CompactDuration::MAX);
    }

    #[test]
    fn test_sub_operator() {
        let a = CompactDuration::new(3000);
        let b = CompactDuration::new(1000);
        let result = a - b;
        assert_eq!(result.seconds(), 2000);

        // Test underflow saturation
        let small = CompactDuration::new(100);
        let large = CompactDuration::new(1000);
        let result = small - large;
        assert_eq!(result, CompactDuration::MIN);
    }

    #[test]
    fn test_saturating_add() {
        let a = CompactDuration::new(1000);
        let b = CompactDuration::new(2000);
        assert_eq!(a.saturating_add(b).seconds(), 3000);

        // Test saturation at maximum
        let max = CompactDuration::MAX;
        let one = CompactDuration::new(1);
        assert_eq!(max.saturating_add(one), CompactDuration::MAX);

        // Test adding zero
        let some_duration = CompactDuration::new(1234);
        let zero = CompactDuration::new(0);
        assert_eq!(some_duration.saturating_add(zero), some_duration);
    }

    #[test]
    fn test_saturating_sub() {
        let a = CompactDuration::new(3000);
        let b = CompactDuration::new(1000);
        assert_eq!(a.saturating_sub(b).seconds(), 2000);

        // Test saturation at minimum
        let small = CompactDuration::new(100);
        let large = CompactDuration::new(1000);
        assert_eq!(small.saturating_sub(large), CompactDuration::MIN);

        // Test subtracting zero
        let some_duration = CompactDuration::new(1234);
        let zero = CompactDuration::new(0);
        assert_eq!(some_duration.saturating_sub(zero), some_duration);

        // Test same values
        let duration = CompactDuration::new(1234);
        assert_eq!(duration.saturating_sub(duration), CompactDuration::MIN);
    }

    #[test]
    fn test_checked_sub() -> Result<()> {
        let a = CompactDuration::new(3000);
        let b = CompactDuration::new(1000);
        assert_eq!(a.checked_sub(b)?.seconds(), 2000);

        // Test underflow error
        let small = CompactDuration::new(100);
        let large = CompactDuration::new(1000);
        assert!(matches!(
            small.checked_sub(large),
            Err(CompactDurationError::OutOfRange)
        ));

        // Test same values
        let duration = CompactDuration::new(1234);
        assert_eq!(duration.checked_sub(duration)?.seconds(), 0);

        // Test subtracting zero
        let some_duration = CompactDuration::new(1234);
        let zero = CompactDuration::new(0);
        assert_eq!(some_duration.checked_sub(zero)?.seconds(), 1234);
        Ok(())
    }

    #[quickcheck]
    fn prop_add_commutative(a: CompactDuration, b: CompactDuration) -> bool {
        a + b == b + a
    }

    #[quickcheck]
    fn prop_add_associative(a: CompactDuration, b: CompactDuration, c: CompactDuration) -> bool {
        (a + b) + c == a + (b + c)
    }

    #[quickcheck]
    fn prop_add_identity(a: CompactDuration) -> bool {
        let zero = CompactDuration::new(0);
        a + zero == a
    }

    #[quickcheck]
    fn prop_sub_identity(a: CompactDuration) -> bool {
        let zero = CompactDuration::new(0);
        a - zero == a
    }

    #[quickcheck]
    fn prop_sub_self_is_zero(a: CompactDuration) -> bool {
        a - a == CompactDuration::new(0)
    }

    #[quickcheck]
    fn prop_saturating_add_monotonic(a: CompactDuration, b: CompactDuration) -> bool {
        a.saturating_add(b) >= a && a.saturating_add(b) >= b
    }

    #[quickcheck]
    fn prop_saturating_sub_monotonic(a: CompactDuration, b: CompactDuration) -> bool {
        a.saturating_sub(b) <= a
    }

    #[quickcheck]
    fn prop_checked_add_consistent_with_saturating(a: CompactDuration, b: CompactDuration) -> bool {
        match a.checked_add(b) {
            Ok(result) => result == a.saturating_add(b),
            Err(_) => a.saturating_add(b) == CompactDuration::MAX,
        }
    }

    #[quickcheck]
    fn prop_checked_sub_consistent_with_saturating(a: CompactDuration, b: CompactDuration) -> bool {
        match a.checked_sub(b) {
            Ok(result) => result == a.saturating_sub(b),
            Err(_) => a.saturating_sub(b) == CompactDuration::MIN,
        }
    }

    #[quickcheck]
    fn prop_add_sub_inverse_when_possible(a: CompactDuration, b: CompactDuration) -> bool {
        // If we can add b to a and subtract b from the result, we should get back to a
        // This only holds when there's no overflow/underflow
        if let (Ok(_sum), Ok(diff)) = (a.checked_add(b), a.saturating_add(b).checked_sub(b)) {
            diff == a
        } else {
            true // Skip cases with overflow
        }
    }

    #[test]
    fn test_edge_case_operations() {
        // Test operations with MIN and MAX
        let min = CompactDuration::MIN;
        let max = CompactDuration::MAX;
        let one = CompactDuration::new(1);

        // MIN operations
        assert_eq!(min + min, min);
        assert_eq!(min - min, min);
        assert_eq!(min + one, one);

        // MAX operations
        assert_eq!(max + one, max); // Saturates
        assert_eq!(max - one, CompactDuration::new(u32::MAX - 1));
        assert_eq!(max - max, min);

        // Mixed operations
        assert_eq!(max + min, max);
        assert_eq!(max - min, max);
        assert_eq!(min + max, max);
    }

    #[test]
    fn test_operator_equivalence() {
        let a = CompactDuration::new(1000);
        let b = CompactDuration::new(500);

        // Add operator should be equivalent to saturating_add
        assert_eq!(a + b, a.saturating_add(b));

        // Sub operator should be equivalent to saturating_sub
        assert_eq!(a - b, a.saturating_sub(b));
    }
}
