//! The standalone reader's capability error.

use crate::error::{ClassifyError, ErrorCategory};
use crate::state::access::StateAccessError;
use std::error::Error;
use thiserror::Error;

/// Error raised by [`StateReader`](super::StateReader) construction and its
/// read operations.
///
/// One concrete, non-generic enum covers every reader failure. Store, loader,
/// and codec errors are type-erased at the boundary into a rendered message
/// plus a captured [`ErrorCategory`]. This keeps the public type flat and
/// FFI-exposable: owned fields, a C-like classification, no generics or borrows
/// in return position.
#[derive(Debug, Error)]
pub enum StateReaderError {
    /// No publication rows exist for the collection yet. Always transient.
    /// Under first-write publication, zero rows cannot distinguish a
    /// misconfigured name from a publisher that has not written yet. A
    /// withdrawal that empties the snapshot may also be re-admitted later. Only
    /// a retry tells these apart.
    #[error("no publication rows for {subsystem}/{name}")]
    UnknownPublication {
        /// The subsystem the reader routed under.
        subsystem: String,
        /// The collection name.
        name: String,
    },

    /// A source's frozen descriptor identity disagrees with the reader's
    /// descriptor. This is a genuine byte-layout incompatibility. Permanent:
    /// the deployed descriptor must match the group's frozen identity.
    #[error("descriptor identity mismatch for source group {group}")]
    IdentityMismatch {
        /// The publishing group whose frozen identity disagreed.
        group: String,
    },

    /// Publication rows exist but no source has a frozen identity yet.
    /// Transient. Identity registration always precedes any state write, so a
    /// row that stays dangling is leftover state for an operator to clean up. A
    /// transient read may still recover.
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
    /// determined once at construction. Currently this means a collection whose
    /// name is empty.
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
    /// capturing its classification clamped to the client posture.
    pub(crate) fn store<E>(error: &E) -> Self
    where
        E: ClassifyError + Error,
    {
        Self::Store {
            message: error.to_string(),
            category: client_category(error.classify_error()),
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
            Self::Store { category, .. } => client_category(*category),
            Self::Access(error) => client_category(error.classify_error()),
        }
    }
}

/// Clamps an upstream classification to the reader's client posture.
/// `Terminal` folds to `Transient`; everything else passes through. The reader
/// is a leaf, FFI-exposable client with no middleware above it to consume a
/// `Terminal`. Shutting the client down is meaningless for an ownerless
/// cross-group reader, and a faulted loader or driver may recover on a retry.
/// Mirrors the owner-side fold in
/// [`ErasedStateError::from_classified`](crate::consumer::event_context::ErasedStateError).
fn client_category(category: ErrorCategory) -> ErrorCategory {
    match category {
        ErrorCategory::Terminal => ErrorCategory::Transient,
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A synthetic upstream error that classifies `Terminal`. It stands in for
    /// a shut-down Kafka loader or a scylla driver fault reaching the reader.
    #[derive(Debug, Error)]
    #[error("synthetic terminal")]
    struct SyntheticTerminal;

    impl ClassifyError for SyntheticTerminal {
        fn classify_error(&self) -> ErrorCategory {
            ErrorCategory::Terminal
        }
    }

    /// No constructible [`StateReaderError`] classifies `Terminal`: the reader
    /// is a leaf client with no middleware to consume one. The two type-erasing
    /// arms carry an upstream classification, so both are driven with a
    /// `Terminal`-emitting fault through the production `store`/`load`
    /// boundaries.
    ///
    /// The clamp must also hold for a `Store` value constructed directly with a
    /// `Terminal` category, bypassing [`StateReaderError::store`]. The variant
    /// and its fields are public, so `classify_error` must fold on read, not
    /// only `store(...)` on capture.
    ///
    /// FALSIFICATION: drop the `client_category` clamp from either the
    /// `store(...)` capture or the `classify_error` `Store`/`Access` arms. A
    /// `Terminal` then reaches classification and the assert fires.
    #[test]
    fn no_variant_leaks_terminal_to_the_client() {
        let cases = [
            StateReaderError::UnknownPublication {
                subsystem: "orders".into(),
                name: "cart".into(),
            },
            StateReaderError::IdentityMismatch {
                group: "group-a".into(),
            },
            StateReaderError::IdentityUnavailable {
                name: "cart".into(),
            },
            StateReaderError::TooManySources { found: 9, max: 4 },
            StateReaderError::EmptyKey,
            StateReaderError::InvalidReadCache { reason: "zero ttl" },
            StateReaderError::Unsupported {
                reason: "empty name",
            },
            StateReaderError::store(&SyntheticTerminal),
            StateReaderError::Access(StateAccessError::load(&SyntheticTerminal)),
            // Directly constructed (not via `store`), so only a fold on read
            // keeps this from leaking Terminal.
            StateReaderError::Store {
                message: "directly constructed terminal".into(),
                category: ErrorCategory::Terminal,
            },
        ];
        for case in cases {
            assert_ne!(
                case.classify_error(),
                ErrorCategory::Terminal,
                "variant leaked Terminal: {case}"
            );
        }
    }
}
