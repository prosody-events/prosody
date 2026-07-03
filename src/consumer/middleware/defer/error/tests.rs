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
    let error = DeferError::<TestTransientError, TestTransientError, TestTransientError>::Handler(
        TestTransientError,
    );
    assert!(matches!(error.classify_error(), ErrorCategory::Transient));
}

#[test]
fn test_handler_error_delegates_permanent() {
    let error = DeferError::<TestTransientError, TestPermanentError, TestTransientError>::Handler(
        TestPermanentError,
    );
    assert!(matches!(error.classify_error(), ErrorCategory::Permanent));
}
