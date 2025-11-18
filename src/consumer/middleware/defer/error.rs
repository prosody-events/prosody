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
/// Generic over store and handler error types. Timer errors use type erasure.
///
/// # Type Parameters
///
/// * `S` - Store error type (from `DeferStore::Error`)
/// * `H` - Handler error type (from inner `FallibleHandler::Error`)
#[derive(Debug, Error)]
pub enum DeferError<S, H>
where
    S: StdError + ClassifyError + Send + Sync + 'static,
    H: StdError + ClassifyError + Send,
{
    /// Error from the underlying store implementation.
    #[error("store error: {0:#}")]
    Store(S),

    /// Error from the inner handler.
    #[error("handler error: {0:#}")]
    Handler(H),

    /// Error from the timer system.
    #[error("timer error: {0:#}")]
    Timer(BoxEventContextError),

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

impl<S, H> ClassifyError for DeferError<S, H>
where
    S: StdError + ClassifyError + Send + Sync + 'static,
    H: StdError + ClassifyError + Send,
{
    fn classify_error(&self) -> ErrorCategory {
        match self {
            // Configuration errors are terminal - system cannot operate
            Self::Configuration(_) => ErrorCategory::Terminal,

            // Delegate to inner error classifications
            Self::Store(error) => error.classify_error(),
            Self::Handler(error) => error.classify_error(),
            Self::Timer(error) => error.classify_error(),
            Self::KafkaLoader(error) => error.classify_error(),
            Self::CompactTime(error) => error.classify_error(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test error type for transient store errors.
    #[derive(Debug, thiserror::Error, Clone)]
    #[error("test transient error")]
    struct TestTransientError;

    impl ClassifyError for TestTransientError {
        fn classify_error(&self) -> ErrorCategory {
            ErrorCategory::Transient
        }
    }

    /// Test error type for permanent store errors.
    #[derive(Debug, thiserror::Error, Clone)]
    #[error("test permanent error")]
    struct TestPermanentError;

    impl ClassifyError for TestPermanentError {
        fn classify_error(&self) -> ErrorCategory {
            ErrorCategory::Permanent
        }
    }

    #[test]
    fn test_configuration_error_is_terminal() {
        let error = DeferError::<TestTransientError, TestTransientError>::Configuration(
            ConfigurationError::Invalid("test".to_owned()),
        );
        assert!(matches!(error.classify_error(), ErrorCategory::Terminal));
    }

    #[test]
    fn test_store_error_delegates_transient() {
        let error = DeferError::<TestTransientError, TestTransientError>::Store(TestTransientError);
        assert!(matches!(error.classify_error(), ErrorCategory::Transient));
    }

    #[test]
    fn test_store_error_delegates_permanent() {
        let error = DeferError::<TestPermanentError, TestTransientError>::Store(TestPermanentError);
        assert!(matches!(error.classify_error(), ErrorCategory::Permanent));
    }

    #[test]
    fn test_handler_error_delegates_transient() {
        let error =
            DeferError::<TestTransientError, TestTransientError>::Handler(TestTransientError);
        assert!(matches!(error.classify_error(), ErrorCategory::Transient));
    }

    #[test]
    fn test_handler_error_delegates_permanent() {
        let error =
            DeferError::<TestTransientError, TestPermanentError>::Handler(TestPermanentError);
        assert!(matches!(error.classify_error(), ErrorCategory::Permanent));
    }
}
