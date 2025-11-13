//! Error types for defer middleware.

use crate::consumer::event_context::BoxEventContextError;
use crate::consumer::middleware::defer::loader::KafkaLoaderError;
use crate::consumer::middleware::{ClassifyError, ErrorCategory};
use crate::timers::datetime::CompactDateTimeError;
use std::error::Error as StdError;
use std::fmt::Debug;
use thiserror::Error;

/// Errors that can occur in defer middleware operations.
///
/// Generic over store error type. Timer errors use type erasure.
///
/// # Type Parameters
///
/// * `S` - Store error type (from `DeferStore::Error`)
#[derive(Debug, Error)]
pub enum DeferError<S>
where
    S: StdError + ClassifyError + Send + Sync + 'static,
{
    /// Error from the underlying store implementation.
    #[error("store error: {0:#}")]
    Store(S),

    /// Error from the timer system.
    #[error("event context operation failed: {0:#}")]
    EventContext(BoxEventContextError),

    /// Error loading message from Kafka.
    #[error("kafka loader error: {0:#}")]
    KafkaLoader(#[from] KafkaLoaderError),

    /// Configuration validation error.
    #[error("configuration error: {0:#}")]
    Configuration(#[from] ConfigurationError),

    /// Error encoding/decoding compact time values.
    #[error("compact time error: {0:#}")]
    CompactTime(#[from] CompactDateTimeError),
}

/// Configuration validation errors.
#[derive(Debug, Error, Clone)]
pub enum ConfigurationError {
    /// Invalid configuration value.
    #[error("invalid configuration: {0:#}")]
    Invalid(String),

    /// Configuration builder error.
    #[error("failed to build configuration: {0:#}")]
    BuildError(String),
}

impl<T> ClassifyError for DeferError<T>
where
    T: StdError + ClassifyError + Send + Sync + 'static,
{
    fn classify_error(&self) -> ErrorCategory {
        match self {
            // Configuration errors are terminal - system cannot operate
            Self::Configuration(_) => ErrorCategory::Terminal,

            // Delegate to inner error classifications
            Self::Store(error) => error.classify_error(),
            Self::EventContext(error) => error.classify_error(),
            Self::KafkaLoader(error) => error.classify_error(),
            Self::CompactTime(error) => error.classify_error(),
        }
    }
}
