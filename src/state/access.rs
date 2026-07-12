//! The keyed-state capability error.

use crate::error::{ClassifyError, ErrorCategory};
use crate::state::descriptor::StructuralIdentity;
use std::error::Error;
use thiserror::Error;

/// Error raised by keyed-state descriptor binds and the typed collection
/// handles, and re-exported from the consumer's `EventContext` capability
/// surface.
///
/// One concrete enum for the whole capability surface: store and loader
/// errors are type-erased at the boundary (message + captured
/// [`ErrorCategory`]) so the error type needs no generics.
#[derive(Debug, Error)]
pub enum StateAccessError {
    /// This context does not provide keyed state (the default for every
    /// context outside the keyed-state middleware).
    #[error("keyed state is unavailable on this context")]
    Unavailable,

    /// The collection name was never registered with the consumer.
    #[error("state collection {name:?} is not registered")]
    Unregistered {
        /// The descriptor's collection name.
        name: &'static str,
    },

    /// The registry holds a different identity for this name.
    #[error(
        "state collection identity mismatch: registered {stored:?}, descriptor asserts \
         {asserted:?}"
    )]
    IdentityMismatch {
        /// Identity held by the registry.
        stored: StructuralIdentity,

        /// Identity the binding descriptor asserts.
        asserted: StructuralIdentity,
    },

    /// A state handle was used while the context is shutting down or the
    /// message is cancelled.
    #[error("state access attempted on a terminated context")]
    Terminated,

    /// A state mutation was attempted after the event settled: the settlement
    /// boundary closed the session, so it no longer accepts writes (reads
    /// still answer, serving the post-settle apply hooks).
    #[error("state mutation attempted on a settled session")]
    SessionClosed,

    /// The underlying state store failed (type-erased).
    #[error("keyed-state store failed: {message}")]
    Store {
        /// Rendered store error.
        message: String,

        /// The store error's captured classification.
        category: ErrorCategory,
    },

    /// The message loader failed (type-erased).
    #[error("keyed-state message loader failed: {message}")]
    Load {
        /// Rendered loader error.
        message: String,

        /// The loader error's captured classification.
        category: ErrorCategory,
    },
}

impl StateAccessError {
    /// Type-erases a store error into [`Self::Store`], capturing its
    /// classification.
    pub(crate) fn store<E>(error: &E) -> Self
    where
        E: ClassifyError + Error,
    {
        Self::Store {
            message: error.to_string(),
            category: error.classify_error(),
        }
    }

    /// Type-erases a loader error into [`Self::Load`], capturing its
    /// classification.
    pub(crate) fn load<E>(error: &E) -> Self
    where
        E: ClassifyError + Error,
    {
        Self::Load {
            message: error.to_string(),
            category: error.classify_error(),
        }
    }
}

impl ClassifyError for StateAccessError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::Unavailable
            | Self::Unregistered { .. }
            | Self::IdentityMismatch { .. }
            // The event settled; retrying the same op on the same session can
            // never succeed.
            | Self::SessionClosed => ErrorCategory::Permanent,
            // Aligned with the cancellation middleware: a terminated
            // context is a transient condition (retry decides).
            Self::Terminated => ErrorCategory::Transient,
            Self::Store { category, .. } | Self::Load { category, .. } => *category,
        }
    }
}
