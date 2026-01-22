//! Error types for defer middleware.

use crate::cassandra::errors::CassandraStoreError;
use crate::consumer::event_context::BoxEventContextError;
use crate::consumer::middleware::defer::message::loader::KafkaLoaderError;
use crate::error::{ClassifyError, ErrorCategory};
use crate::timers::datetime::CompactDateTimeError;
use std::error::Error as StdError;
use std::fmt::Debug;
use thiserror::Error;
use validator::ValidationErrors;

/// Result type alias for defer operations.
///
/// Wraps the standard `Result` with [`DeferError`] as the error type.
///
/// # Type Parameters
///
/// * `T` - Success value type
/// * `S` - Store error type (from `MessageDeferStore::Error`)
/// * `H` - Handler error type (from inner `FallibleHandler::Error`)
/// * `L` - Loader error type (from `MessageLoader::Error`)
pub type DeferResult<T, S, H, L> = Result<T, DeferError<S, H, L>>;

/// Errors that can occur in defer middleware operations.
///
/// Generic over store, handler, and loader error types. Timer errors use type
/// erasure.
///
/// # Type Parameters
///
/// * `S` - Store error type (from `MessageDeferStore::Error`)
/// * `H` - Handler error type (from inner `FallibleHandler::Error`)
/// * `L` - Loader error type (from `MessageLoader::Error`)
#[derive(Debug, Error)]
pub enum DeferError<S, H, L = KafkaLoaderError>
where
    S: StdError + ClassifyError + Send + Sync + 'static,
    H: StdError + ClassifyError + Send,
    L: StdError + ClassifyError + Send + Sync + 'static,
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

    /// Error loading message.
    #[error("loader error: {0:#}")]
    Loader(L),

    /// Configuration validation error.
    #[error("configuration error: {0:#}")]
    Configuration(ConfigurationError),

    /// Error encoding/decoding compact time values.
    #[error("compact time error: {0:#}")]
    CompactTime(CompactDateTimeError),
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

/// Errors that can occur during defer middleware initialization.
#[derive(Debug, Error)]
pub enum DeferInitError {
    /// Configuration validation failed.
    #[error("invalid configuration: {0:#}")]
    Validation(#[from] ValidationErrors),

    /// `KafkaLoader` construction failed.
    #[error("failed to create kafka loader: {0:#}")]
    KafkaLoader(#[from] KafkaLoaderError),

    /// Cassandra store initialization failed.
    #[error("failed to initialize cassandra store: {0:#}")]
    CassandraStore(#[from] CassandraStoreError),
}

/// Unified error type for Cassandra-backed defer stores.
///
/// Used by both [`CassandraMessageDeferStore`] and [`CassandraTimerDeferStore`]
/// to provide a common error type when message and timer stores share the same
/// segment store type.
///
/// [`CassandraMessageDeferStore`]: crate::consumer::middleware::defer::message::store::cassandra::CassandraMessageDeferStore
/// [`CassandraTimerDeferStore`]: crate::consumer::middleware::defer::timer::store::cassandra::CassandraTimerDeferStore
#[derive(Debug, Error)]
pub enum CassandraDeferStoreError {
    /// Error from Cassandra operations.
    #[error("cassandra error: {0:#}")]
    Cassandra(#[from] CassandraStoreError),
}

impl ClassifyError for CassandraDeferStoreError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            // Delegate Cassandra errors to their classification
            Self::Cassandra(error) => error.classify_error(),
        }
    }
}

impl From<super::segment::CassandraSegmentStoreError> for CassandraDeferStoreError {
    fn from(err: super::segment::CassandraSegmentStoreError) -> Self {
        match err {
            super::segment::CassandraSegmentStoreError::Cassandra(e) => Self::Cassandra(e),
        }
    }
}

impl<S, H, L> ClassifyError for DeferError<S, H, L>
where
    S: StdError + ClassifyError + Send + Sync + 'static,
    H: StdError + ClassifyError + Send,
    L: StdError + ClassifyError + Send + Sync + 'static,
{
    fn classify_error(&self) -> ErrorCategory {
        match self {
            // Configuration errors are terminal - system cannot operate
            Self::Configuration(_) => ErrorCategory::Terminal,

            // Delegate to inner error classifications
            Self::Store(error) => error.classify_error(),
            Self::Handler(error) => error.classify_error(),
            Self::Timer(error) => error.classify_error(),
            Self::Loader(error) => error.classify_error(),
            Self::CompactTime(error) => error.classify_error(),
        }
    }
}

impl<S, H, L> From<CompactDateTimeError> for DeferError<S, H, L>
where
    S: StdError + ClassifyError + Send + Sync + 'static,
    H: StdError + ClassifyError + Send,
    L: StdError + ClassifyError + Send + Sync + 'static,
{
    fn from(error: CompactDateTimeError) -> Self {
        Self::CompactTime(error)
    }
}

impl<S, H, L> From<ConfigurationError> for DeferError<S, H, L>
where
    S: StdError + ClassifyError + Send + Sync + 'static,
    H: StdError + ClassifyError + Send,
    L: StdError + ClassifyError + Send + Sync + 'static,
{
    fn from(error: ConfigurationError) -> Self {
        Self::Configuration(error)
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
        let error =
            DeferError::<TestTransientError, TestTransientError, TestTransientError>::Configuration(
                ConfigurationError::Invalid("test".to_owned()),
            );
        assert!(matches!(error.classify_error(), ErrorCategory::Terminal));
    }

    #[test]
    fn test_store_error_delegates_transient() {
        let error = DeferError::<TestTransientError, TestTransientError, TestTransientError>::Store(
            TestTransientError,
        );
        assert!(matches!(error.classify_error(), ErrorCategory::Transient));
    }

    #[test]
    fn test_handler_error_delegates_transient() {
        let error =
            DeferError::<TestTransientError, TestTransientError, TestTransientError>::Handler(
                TestTransientError,
            );
        assert!(matches!(error.classify_error(), ErrorCategory::Transient));
    }

    #[test]
    fn test_handler_error_delegates_permanent() {
        let error =
            DeferError::<TestTransientError, TestPermanentError, TestTransientError>::Handler(
                TestPermanentError,
            );
        assert!(matches!(error.classify_error(), ErrorCategory::Permanent));
    }
}
