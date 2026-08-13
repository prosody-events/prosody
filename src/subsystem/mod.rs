//! Subsystem naming.
//!
//! A [`SubsystemName`] identifies a keyed-state subsystem. That is the logical
//! unit a consumer group publishes state under and a reader addresses. The name
//! carries no durable state, so it lives at the crate root rather than under
//! `state`. Request/reply responders that need no Cassandra state will reuse
//! it.

use crate::error::{ClassifyError, ErrorCategory};
use fixedstr::Flexstr;
use std::borrow::Borrow;
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::str::FromStr;
use thiserror::Error;

/// Inline capacity for common subsystem names.
///
/// `Flexstr` uses one byte for the length. Names of 24 bytes or fewer stay
/// inline. Longer names remain valid and use heap storage.
const CAPACITY: usize = 25;

/// Human-readable subsystem name.
///
/// Every value is trimmed and is not blank. Common names stay inline. Longer
/// names remain valid and use heap storage.
#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct SubsystemName(Flexstr<CAPACITY>);

impl SubsystemName {
    /// Creates a subsystem name, trimmed of surrounding whitespace.
    ///
    /// # Errors
    ///
    /// Returns [`SubsystemNameError`] when the trimmed name is empty.
    pub fn try_new<N>(name: N) -> Result<Self, SubsystemNameError>
    where
        N: AsRef<str>,
    {
        Ok(Self(Flexstr::make(Self::checked(name.as_ref())?)))
    }

    /// Trims `name` and holds it to the rule [`try_new`](Self::try_new)
    /// applies, without building a name.
    ///
    /// This is the one place the rule is written. A caller that only compares a
    /// borrowed name — a reserved Kafka header, a decoded wire field — checks
    /// it here and copies nothing.
    ///
    /// # Errors
    ///
    /// Returns [`SubsystemNameError`] when the trimmed name is empty.
    pub(crate) fn checked(name: &str) -> Result<&str, SubsystemNameError> {
        let name = name.trim();
        if name.is_empty() {
            return Err(SubsystemNameError);
        }
        Ok(name)
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

impl Display for SubsystemName {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.write_str(self.as_str())
    }
}

impl FromStr for SubsystemName {
    type Err = SubsystemNameError;

    fn from_str(name: &str) -> Result<Self, Self::Err> {
        Self::try_new(name)
    }
}

/// Lets maps keyed by [`SubsystemName`] resolve `&str` lookups without
/// allocating, as [`StateName`](crate::state::StateName) does.
///
/// The `Borrow` contract requires `Hash` and `Eq` to agree between the owned
/// and borrowed forms. The derived `Hash`/`Eq` delegate to the inner string, so
/// they match `str`'s own implementations.
impl Borrow<str> for SubsystemName {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

/// A subsystem name is empty or contains only whitespace.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
#[error("subsystem name must not be empty")]
pub struct SubsystemNameError;

impl ClassifyError for SubsystemNameError {
    fn classify_error(&self) -> ErrorCategory {
        ErrorCategory::Permanent
    }
}

#[cfg(test)]
mod tests;
