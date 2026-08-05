//! Error classification for retry and failure handling.
//!
//! This module provides a unified error classification system used across
//! Kafka, Cassandra, and other components to determine retry behavior.
//!
//! # Classification Rubric
//!
//! - **Terminal**: Fatal errors where the client is unusable and must shutdown.
//!   The client cannot recover; a new instance must be created.
//! - **Permanent**: Message-level issues (corruption, serialization failures,
//!   invalid data) where retrying forever won't help. Data loss is inevitable
//!   for this specific message, but the client can continue processing others.
//! - **Transient**: Everything else - errors that could be fixed by retry,
//!   waiting, configuration changes, or code changes.

use serde::Serialize;
use thiserror::Error;

pub mod kafka;

/// Categorizes errors in message processing.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize)]
#[cfg_attr(test, derive(strum::VariantArray))]
#[serde(rename_all = "camelCase")]
pub enum ErrorCategory {
    /// Error is temporary and recovery is possible.
    Transient,
    /// Error is permanent and irrecoverable for this message.
    Permanent,
    /// Error requires partition/client shutdown.
    Terminal,
}

/// Defines methods for classifying errors.
pub trait ClassifyError {
    /// Classifies the error into a specific `ErrorCategory`.
    fn classify_error(&self) -> ErrorCategory;

    /// Returns `true` if the error is classified as `ErrorCategory::Transient`.
    fn is_recoverable(&self) -> bool {
        matches!(self.classify_error(), ErrorCategory::Transient)
    }
}

/// The wire discriminants, for formats that carry a category between processes.
///
/// **Zero is reserved and no category claims it.** Protobuf decodes a missing
/// `int32` field as `0`, so an omitted category reads as malformed rather than
/// as a silent [`Transient`](ErrorCategory::Transient).
///
/// **Four is reserved too.** A peer response frame gives `4` to a successful
/// result, and reads that discriminant before it consults this mapping. So a
/// category numbered `4` here would decode as a success. Give the next category
/// `5`.
impl From<ErrorCategory> for i32 {
    fn from(category: ErrorCategory) -> Self {
        match category {
            ErrorCategory::Transient => 1,
            ErrorCategory::Permanent => 2,
            ErrorCategory::Terminal => 3,
        }
    }
}

impl TryFrom<i32> for ErrorCategory {
    type Error = UnknownErrorCategory;

    fn try_from(value: i32) -> Result<Self, UnknownErrorCategory> {
        match value {
            1 => Ok(Self::Transient),
            2 => Ok(Self::Permanent),
            3 => Ok(Self::Terminal),
            other => Err(UnknownErrorCategory(other)),
        }
    }
}

/// A wire discriminant naming no [`ErrorCategory`].
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error("unknown error category discriminant: {0}")]
pub struct UnknownErrorCategory(pub i32);

#[cfg(test)]
mod tests;
