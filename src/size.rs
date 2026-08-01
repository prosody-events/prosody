//! Human-readable byte budgets used at configuration boundaries.

use std::num::NonZeroU64;
use std::str::FromStr;
use thiserror::Error;

/// A positive byte count parsed from a human-readable size.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ByteSize(NonZeroU64);

impl ByteSize {
    /// Creates a byte size from an already validated byte count.
    #[must_use]
    pub const fn new(bytes: NonZeroU64) -> Self {
        Self(bytes)
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
