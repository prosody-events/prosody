//! Errors for the Cassandra Value store.
//!
//! Most failures are wrapped Cassandra driver errors (network, timeout,
//! schema mismatch) whose `ClassifyError` impl already returns the right
//! retry category. The keyed-state-specific errors —
//! [`CassandraValueStoreError::Encoding`],
//! [`CassandraValueStoreError::CorruptWal`],
//! [`CassandraValueStoreError::CorruptUdt`],
//! [`CassandraValueStoreError::EventMismatch`],
//! [`CassandraValueStoreError::IdentityVersionMismatch`] — are all
//! permanent per-message data errors: retrying them indefinitely will not
//! change the outcome.

use super::decode::CorruptReason;
use crate::cassandra::errors::CassandraStoreError;
use crate::error::{ClassifyError, ErrorCategory};
use crate::state::EventRef;
use crate::state::encoding::EncodingError;
use thiserror::Error;

/// Errors that can occur during Cassandra Value store operations.
#[derive(Debug, Error)]
pub enum CassandraValueStoreError {
    /// Wrapped Cassandra driver error.
    #[error("database error: {0:#}")]
    Database(#[from] CassandraStoreError),

    /// WAL encode/decode failure.
    #[error(transparent)]
    Encoding(#[from] EncodingError),

    /// The value partition columns formed a shape the decoder rejects.
    #[error("Cassandra value row is corrupt: {reason}")]
    CorruptWal {
        /// Specific corruption shape; also the `source()` of this error.
        #[from]
        reason: CorruptReason,
    },

    /// The `event_ref` UDT was not in a shape this build understands.
    #[error("Cassandra event_ref UDT is corrupt: {0}")]
    CorruptUdt(#[from] CorruptUdtError),

    /// The sealed WAL referenced a different event than the caller asked
    /// to resolve.
    #[error("sealed event mismatch: expected {expected:?}, actual {actual:?}")]
    EventMismatch {
        /// Event the caller asked to apply or roll back.
        expected: EventRef,

        /// Event sealed on the durable row.
        actual: EventRef,
    },

    /// The value row's `identity_version` stamp is not the version this
    /// build writes. Unreachable until identity migration ships; rejected
    /// defensively so a future-version cell is never misread.
    #[error("identity version mismatch: stored {stored}, expected {expected}")]
    IdentityVersionMismatch {
        /// Version stamped on the durable row.
        stored: i32,

        /// The only version this build accepts.
        expected: i32,
    },
}

impl ClassifyError for CassandraValueStoreError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::Database(e) => e.classify_error(),
            Self::Encoding(_)
            | Self::CorruptWal { .. }
            | Self::CorruptUdt(_)
            | Self::EventMismatch { .. }
            | Self::IdentityVersionMismatch { .. } => ErrorCategory::Permanent,
        }
    }
}

/// The `event_ref` UDT carried a shape the deserializer does not accept.
#[derive(Debug, Error)]
pub enum CorruptUdtError {
    /// `kind` did not match a known event variant.
    #[error("unknown event_ref kind discriminator: {0}")]
    UnknownKind(i8),

    /// `kind == 0` (Message) but `msg_dedup_id` was NULL.
    #[error("event_ref Message variant is missing msg_dedup_id")]
    MessageMissingDedupId,

    /// `kind == 0` (Message) but timer fields were populated.
    #[error("event_ref Message variant has unexpected timer fields populated")]
    MessageHasTimerFields,

    /// `kind == 1` (Timer) but one of the timer fields was NULL.
    #[error("event_ref Timer variant is missing a required field")]
    TimerMissingField,

    /// `kind == 1` (Timer) but `msg_dedup_id` was populated.
    #[error("event_ref Timer variant has unexpected msg_dedup_id populated")]
    TimerHasDedupId,
}
