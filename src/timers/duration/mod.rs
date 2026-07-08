//! Compact duration representation for efficient timer calculations.
//!
//! Provides [`CompactDuration`], a space-efficient duration type using 32-bit
//! seconds. Reduces memory usage and enables fast arithmetic for timer systems
//! requiring only second-level precision.

use crate::error::{ClassifyError, ErrorCategory};
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
    /// # Examples
    ///
    /// ```
    /// use prosody::timers::duration::CompactDuration;
    ///
    /// let one_hour = CompactDuration::new(3600);
    /// assert_eq!(one_hour.seconds(), 3600);
    /// ```
    #[must_use]
    pub const fn new(seconds: u32) -> Self {
        Self { seconds }
    }

    /// Returns the number of seconds in this duration.
    #[must_use]
    pub const fn seconds(self) -> u32 {
        self.seconds
    }

    /// Adds two durations with overflow checking.
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

    /// Returns `true` if this duration is zero seconds.
    ///
    /// # Examples
    ///
    /// ```
    /// use prosody::timers::duration::CompactDuration;
    ///
    /// assert!(CompactDuration::new(0).is_zero());
    /// assert!(!CompactDuration::new(1).is_zero());
    /// ```
    #[must_use]
    pub const fn is_zero(self) -> bool {
        self.seconds == 0
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

impl ClassifyError for CompactDurationError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            // OutOfRange: Arithmetic overflow (add/sub exceeding u32 bounds) or
            // conversion failure (Duration > u32::MAX seconds). Invalid data or
            // calculation - not recoverable by retry.
            Self::OutOfRange => ErrorCategory::Permanent,
        }
    }
}

#[cfg(test)]
mod tests;
