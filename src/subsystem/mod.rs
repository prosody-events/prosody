//! Subsystem naming.
//!
//! A [`SubsystemName`] identifies a keyed-state subsystem. That is the logical
//! unit a consumer group publishes state under and a reader addresses. The name
//! carries no durable state, so it lives at the crate root rather than under
//! `state`. Request/reply responders that need no Cassandra state will reuse
//! it.

use crate::error::{ClassifyError, ErrorCategory};
use std::borrow::Borrow;
use std::str::FromStr;
use std::sync::Arc;
use thiserror::Error;

/// Human-readable subsystem name.
#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct SubsystemName(Arc<str>);

impl SubsystemName {
    /// Longest name this crate accepts, in bytes, measured after the trim.
    ///
    /// The bound belongs to the name rather than to any one reader of it,
    /// because a name that exceeds it is unusable everywhere it travels: a peer
    /// response frame carries the answering subsystem in a field of this width,
    /// and a name also becomes a metric label and a Cassandra value. A consumer
    /// configured with a longer name could never be addressed, so the name is
    /// refused where it is made instead.
    pub const MAX_BYTES: usize = 64;

    /// Creates a subsystem name, trimmed of surrounding whitespace.
    ///
    /// # Errors
    ///
    /// Returns [`SubsystemNameError`] when the trimmed name is empty or is
    /// longer than [`MAX_BYTES`](Self::MAX_BYTES).
    pub fn try_new<N>(name: N) -> Result<Self, SubsystemNameError>
    where
        N: AsRef<str>,
    {
        let name = name.as_ref().trim();
        if name.is_empty() {
            return Err(SubsystemNameError::Blank);
        }
        if name.len() > Self::MAX_BYTES {
            return Err(SubsystemNameError::TooLong { bytes: name.len() });
        }

        Ok(Self(Arc::from(name)))
    }

    /// Returns the subsystem name as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for SubsystemName {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl FromStr for SubsystemName {
    type Err = SubsystemNameError;

    fn from_str(name: &str) -> Result<Self, Self::Err> {
        Self::try_new(name)
    }
}

/// Shares the name's backing allocation, so an error or log field that needs an
/// owned name never copies the bytes.
impl From<&SubsystemName> for Arc<str> {
    fn from(name: &SubsystemName) -> Self {
        Arc::clone(&name.0)
    }
}

/// Lets maps keyed by [`SubsystemName`] resolve `&str` lookups without
/// allocating, as [`StateName`](crate::state::StateName) does.
///
/// The `Borrow` contract requires `Hash` and `Eq` to agree between the owned
/// and borrowed forms. The derived `Hash`/`Eq` delegate to the inner `str`, so
/// they match `str`'s own implementations.
impl Borrow<str> for SubsystemName {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

/// Why a subsystem name was refused.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum SubsystemNameError {
    /// The name is empty, or is whitespace alone.
    #[error("subsystem name must not be empty")]
    Blank,
    /// The name is longer than [`SubsystemName::MAX_BYTES`].
    #[error(
        "subsystem name is {bytes} bytes; the limit is {}",
        SubsystemName::MAX_BYTES
    )]
    TooLong {
        /// Length of the trimmed name.
        bytes: usize,
    },
}

impl ClassifyError for SubsystemNameError {
    fn classify_error(&self) -> ErrorCategory {
        ErrorCategory::Permanent
    }
}

#[cfg(test)]
mod tests;
