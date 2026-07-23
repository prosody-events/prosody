//! The standalone reader's capability error.

use crate::error::{ClassifyError, ErrorCategory};
use crate::state::access::StateAccessError;
use std::error::Error;
use thiserror::Error;

/// Error raised by [`StateReader`](super::StateReader) construction and its
/// read operations.
///
/// One concrete, non-generic enum for the whole reader surface: store, loader,
/// and codec failures are type-erased at the boundary (rendered message plus a
/// captured [`ErrorCategory`]) so the public shape stays flat and
/// FFI-exposable — owned fields, a C-like classification, no generics or
/// borrows in return position.
#[derive(Debug, Error)]
pub enum StateReaderError {
    /// No publication rows exist for the collection yet. **Transient in every
    /// position**: under first-write publication, zero rows is ambiguous
    /// between a misconfigured name and a publisher that has not written yet,
    /// and a withdrawal that empties the snapshot may be re-admitted later —
    /// only a retry tells them apart.
    #[error("no publication rows for {subsystem}/{name}")]
    UnknownPublication {
        /// The subsystem the reader routed under.
        subsystem: String,
        /// The collection name.
        name: String,
    },

    /// A source's frozen descriptor identity disagrees with the reader's
    /// descriptor — a genuine byte-layout incompatibility. Permanent: the
    /// deployed descriptor must match the group's frozen identity.
    #[error("descriptor identity mismatch for source group {group}")]
    IdentityMismatch {
        /// The publishing group whose frozen identity disagreed.
        group: String,
    },

    /// Publication rows exist but no source has a frozen identity yet.
    /// Transient: identity registration structurally precedes any state write,
    /// so a persistently dangling row is administrative residue for the
    /// runbook, but a transient read may still recover.
    #[error("no source has a frozen identity yet for {name}")]
    IdentityUnavailable {
        /// The collection name.
        name: String,
    },

    /// The collection advertises more publication sources than the reader
    /// admits. Permanent (the bound is liftable in a later release, never at
    /// runtime).
    #[error("too many publication sources ({found} > {max})")]
    TooManySources {
        /// The number of distinct sources advertised.
        found: usize,
        /// The admitted maximum number of publication sources.
        max: usize,
    },

    /// The read key was empty. Permanent: librdkafka randomizes empty/NULL
    /// keys, so no deterministic partition exists to route to.
    #[error("empty read key")]
    EmptyKey,

    /// The descriptor's read-cache policy is degenerate (a zero or
    /// sub-resolution TTL). Permanent: validated once at construction.
    #[error("invalid read-cache policy: {reason}")]
    InvalidReadCache {
        /// Why the policy is rejected.
        reason: &'static str,
    },

    /// The reader cannot serve this descriptor's collection. Permanent:
    /// determined once at construction — currently a collection whose name is
    /// empty.
    #[error("unsupported collection: {reason}")]
    Unsupported {
        /// Why the collection is unsupported.
        reason: &'static str,
    },

    /// A publication/identity/cell store read failed (type-erased). Carries
    /// the store error's classification so retry posture is preserved.
    #[error("reader store failed: {message}")]
    Store {
        /// Rendered store error.
        message: String,
        /// The store error's captured classification.
        category: ErrorCategory,
    },

    /// A descriptor bind failed, or a bound handle surfaced a keyed-state
    /// access error.
    #[error(transparent)]
    Access(#[from] StateAccessError),
}

impl StateReaderError {
    /// Type-erases a store/publication/identity error into [`Self::Store`],
    /// capturing its classification.
    pub(crate) fn store<E>(error: &E) -> Self
    where
        E: ClassifyError + Error,
    {
        Self::Store {
            message: error.to_string(),
            category: error.classify_error(),
        }
    }
}

impl ClassifyError for StateReaderError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::UnknownPublication { .. } | Self::IdentityUnavailable { .. } => {
                ErrorCategory::Transient
            }
            Self::IdentityMismatch { .. }
            | Self::TooManySources { .. }
            | Self::EmptyKey
            | Self::InvalidReadCache { .. }
            | Self::Unsupported { .. } => ErrorCategory::Permanent,
            Self::Store { category, .. } => *category,
            Self::Access(error) => error.classify_error(),
        }
    }
}
