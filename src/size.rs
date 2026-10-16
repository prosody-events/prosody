//! Human-readable byte budgets used at configuration boundaries.

use serde::Serialize;
use std::num::NonZeroU64;
use std::str::FromStr;
use thiserror::Error;
use validator::ValidateRange;

/// A positive byte count parsed from a human-readable size.
///
/// It serializes as the byte count, so a validation error can report it.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct ByteSize(NonZeroU64);

impl ByteSize {
    /// Creates a byte size from an already validated byte count.
    #[must_use]
    pub const fn new(bytes: NonZeroU64) -> Self {
        Self(bytes)
    }

    /// Creates a byte size of `count` mebibytes, for a constant default. A
    /// zero count gives one byte.
    pub(crate) const fn mebibytes(count: u64) -> Self {
        match NonZeroU64::new(count.saturating_mul(1 << 20_u32)) {
            Some(bytes) => Self(bytes),
            None => Self(NonZeroU64::MIN),
        }
    }

    /// Returns the byte count.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }

    pub(crate) const fn nonzero(self) -> NonZeroU64 {
        self.0
    }
}

impl FromStr for ByteSize {
    type Err = ByteSizeError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let bytes = parse_size::parse_size(value).map_err(ByteSizeError::Parse)?;
        NonZeroU64::new(bytes).map(Self).ok_or(ByteSizeError::Zero)
    }
}

/// Lets a configuration field bound a byte size with `#[validate(range)]`, in
/// bytes.
impl ValidateRange<u64> for ByteSize {
    fn greater_than(&self, max: u64) -> Option<bool> {
        Some(self.get() > max)
    }

    fn less_than(&self, min: u64) -> Option<bool> {
        Some(self.get() < min)
    }
}

/// A human-readable byte size was malformed or resolved to zero.
#[derive(Debug, Error)]
pub enum ByteSizeError {
    /// The value did not contain a supported number and unit.
    #[error("invalid byte size: {0}")]
    Parse(#[source] parse_size::Error),

    /// Cache budgets must reserve at least one byte.
    #[error("byte size must be greater than zero")]
    Zero,
}
