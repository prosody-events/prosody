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

pub mod kafka;

/// Categorizes errors in message processing.
#[derive(Copy, Clone, Debug, Serialize)]
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
    ///
    /// # Returns
    ///
    /// An `ErrorCategory` indicating the nature of the error.
    fn classify_error(&self) -> ErrorCategory;

    /// Determines if the error is recoverable.
    ///
    /// # Returns
    ///
    /// `true` if the error is classified as `ErrorCategory::Transient`, `false`
    /// otherwise.
    fn is_recoverable(&self) -> bool {
        matches!(self.classify_error(), ErrorCategory::Transient)
    }
}
