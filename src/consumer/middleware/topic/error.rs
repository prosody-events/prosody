use thiserror::Error;

use super::{ClassifyError, ErrorCategory, ProducerError};

/// An error from failure topic handling.
#[derive(Debug, Error)]
pub enum FailureTopicError<E, P> {
    /// The wrapped handler returned an error that the middleware did not
    /// rescue.
    #[error(transparent)]
    Handler(E),

    /// The producer did not accept the failure topic record.
    ///
    /// Classification uses `producer`. The inner error remains available for
    /// the matching apply hook.
    #[error("failure-topic send failed: {producer}")]
    DlqSendFailed {
        /// The original handler error.
        inner: E,
        /// The failure topic producer error.
        #[source]
        producer: ProducerError<P>,
    },
}

impl<E, P> ClassifyError for FailureTopicError<E, P>
where
    E: ClassifyError,
{
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::Handler(error) => error.classify_error(),
            // Outer retry reacts to the producer failure. The inner error is
            // retained only for its apply hook.
            Self::DlqSendFailed { producer, .. } => producer.classify_error(),
        }
    }
}
