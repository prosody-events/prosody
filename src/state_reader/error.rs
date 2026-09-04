//! The standalone reader's capability error.

use crate::error::{ClassifyError, ErrorCategory};
use crate::state::access::StateAccessError;
use crate::subsystem::SubsystemName;
use std::error::Error;
use std::sync::Arc;
use thiserror::Error;

/// Error raised by [`StateReader`](super::StateReader) construction and its
/// read operations.
///
/// One concrete, non-generic enum covers every reader failure. Store, loader,
/// and codec errors are type-erased at the boundary into a rendered message
/// plus a captured [`ErrorCategory`]. This keeps the public type flat and
/// FFI-exposable: owned fields, a C-like classification, no generics or borrows
/// in return position.
/// Tests derive a fieldless mirror of this enum so
/// `no_variant_leaks_terminal_to_the_client` can enumerate every variant. A new
/// variant then fails to compile until it is covered there.
#[derive(Debug, Error)]
#[cfg_attr(test, derive(strum::EnumDiscriminants))]
#[cfg_attr(test, strum_discriminants(derive(strum::VariantArray)))]
#[non_exhaustive]
pub enum StateReaderError {
    /// No publication rows exist for the collection yet. Always transient.
    /// Zero rows cannot distinguish a misconfigured name from a publisher that
    /// does not own its leader assignment yet. A withdrawal that empties the
    /// snapshot can also be admitted later. Only a retry tells these apart.
    #[error("no publication rows for {subsystem}/{name}")]
    UnknownPublication {
        /// The subsystem the reader routed under.
        subsystem: SubsystemName,
        /// The collection name.
        name: Arc<str>,
    },

    /// A source's frozen descriptor identity disagrees with the reader's
    /// descriptor. This is a genuine byte-layout incompatibility. Permanent:
    /// the deployed descriptor must match the group's frozen identity.
    #[error("descriptor identity mismatch for source group {group}")]
    IdentityMismatch {
        /// The publishing group whose frozen identity disagreed.
        group: Arc<str>,
    },

    /// The last refresh of the collection's publication sources failed, and the
    /// reader is pacing its retry with no cached outcome to serve. Transient:
    /// the first read past the pacing window re-reads the routing table. The
    /// failure that established the pacing is logged with its cause.
    #[error("publication sources unavailable for {name}, pacing retries after a failed refresh")]
    RefreshUnavailable {
        /// The collection name.
        name: Arc<str>,
    },

    /// Publication rows exist but no source has a frozen identity yet.
    /// Transient. Identity registration always precedes any state write, so a
    /// row that stays dangling is leftover state for an operator to clean up. A
    /// transient read may still recover.
    #[error("no source has a frozen identity yet for {name}")]
    IdentityUnavailable {
        /// The collection name.
        name: Arc<str>,
    },

    /// The collection advertises more publication sources than the reader
    /// admits. Permanent (the bound is liftable in a later release, never at
    /// runtime).
    #[error("too many publication sources (at least {found}; maximum {max})")]
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
    #[error("{message}")]
    Store {
        /// Rendered store error.
        message: String,
        /// The store error's captured classification.
        category: ErrorCategory,
    },
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
            Self::UnknownPublication { .. }
            | Self::IdentityUnavailable { .. }
            | Self::RefreshUnavailable { .. } => ErrorCategory::Transient,
            Self::IdentityMismatch { .. }
            | Self::TooManySources { .. }
            | Self::EmptyKey
            | Self::InvalidReadCache { .. }
            | Self::Unsupported { .. } => ErrorCategory::Permanent,
            Self::Store { category, .. } => client_category(*category),
        }
    }
}

impl From<StateAccessError> for StateReaderError {
    fn from(error: StateAccessError) -> Self {
        Self::store(&error)
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
    use color_eyre::Result;
    use strum::VariantArray;

    /// The derived fieldless mirror of [`StateReaderError`], aliased for
    /// readability.
    use super::StateReaderErrorDiscriminants as Variant;

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

    /// One value of the named variant, carrying `Terminal` wherever the variant
    /// can carry a classification at all.
    ///
    /// The match is exhaustive over the derived discriminants and has no
    /// wildcard, so adding a [`StateReaderError`] variant stops this compiling
    /// until the new variant returns a sample.
    fn sample(variant: Variant, subsystem: &SubsystemName) -> StateReaderError {
        match variant {
            Variant::UnknownPublication => StateReaderError::UnknownPublication {
                subsystem: subsystem.clone(),
                name: "cart".into(),
            },
            Variant::IdentityMismatch => StateReaderError::IdentityMismatch {
                group: "group-a".into(),
            },
            Variant::IdentityUnavailable => StateReaderError::IdentityUnavailable {
                name: "cart".into(),
            },
            Variant::RefreshUnavailable => StateReaderError::RefreshUnavailable {
                name: "cart".into(),
            },
            Variant::TooManySources => StateReaderError::TooManySources { found: 9, max: 4 },
            Variant::EmptyKey => StateReaderError::EmptyKey,
            Variant::InvalidReadCache => StateReaderError::InvalidReadCache { reason: "zero ttl" },
            Variant::Unsupported => StateReaderError::Unsupported {
                reason: "empty name",
            },
            // Constructed directly rather than through `store`, so only a fold
            // on read keeps this from leaking Terminal.
            Variant::Store => StateReaderError::Store {
                message: "directly constructed terminal".into(),
                category: ErrorCategory::Terminal,
            },
        }
    }

    /// No [`StateReaderError`] classifies `Terminal`: the reader is a leaf
    /// client with no middleware to consume one. Every variant is covered,
    /// and the type-erasing capture boundary is driven separately with a
    /// `Terminal`-emitting fault.
    ///
    /// FALSIFICATION: drop the `client_category` clamp from either the
    /// `store(...)` capture or the `classify_error` `Store` arm. A `Terminal`
    /// then reaches classification and the assert fires.
    #[test]
    fn no_variant_leaks_terminal_to_the_client() -> Result<()> {
        let subsystem = SubsystemName::try_new("orders")?;
        let captured = StateReaderError::store(&SyntheticTerminal);
        let cases = Variant::VARIANTS
            .iter()
            .copied()
            .map(|variant| sample(variant, &subsystem))
            .chain([captured]);
        for case in cases {
            assert_ne!(
                case.classify_error(),
                ErrorCategory::Terminal,
                "variant leaked Terminal: {case}"
            );
        }
        Ok(())
    }
}
