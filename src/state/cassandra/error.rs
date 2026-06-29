//! Errors for the Cassandra cell store.
//!
//! Most failures are wrapped Cassandra driver errors (network, timeout,
//! schema mismatch) whose `ClassifyError` impl already returns the right
//! retry category. The keyed-state-specific errors —
//! [`CassandraCellStoreError::Encoding`],
//! [`CassandraCellStoreError::CorruptCell`],
//! [`CassandraCellStoreError::CorruptUdt`],
//! [`CassandraCellStoreError::VersionMismatch`] — are all
//! permanent per-message data errors: retrying them indefinitely will not
//! change the outcome.

use super::cell::CellCorruptReason;
use crate::cassandra::errors::CassandraStoreError;
use crate::error::{ClassifyError, ErrorCategory};
use crate::state::encoding::EncodingError;
use thiserror::Error;

/// Errors that can occur during Cassandra cell store operations.
#[derive(Debug, Error)]
pub enum CassandraCellStoreError {
    /// Wrapped Cassandra driver error.
    #[error("database error: {0:#}")]
    Database(#[from] CassandraStoreError),

    /// Payload encode/decode failure.
    #[error(transparent)]
    Encoding(#[from] EncodingError),

    /// The cell row columns formed a shape the cell decoder rejects.
    #[error("Cassandra cell row is corrupt: {reason}")]
    CorruptCell {
        /// Specific corruption shape; also the `source()` of this error.
        #[from]
        reason: CellCorruptReason,
    },

    /// The `event_ref` UDT was not in a shape this build understands.
    #[error("Cassandra event_ref UDT is corrupt: {0}")]
    CorruptUdt(#[from] CorruptUdtError),

    /// The value row's `version` stamp is not the version this
    /// build writes. Unreachable until identity migration ships; rejected
    /// defensively so a future-version cell is never misread.
    #[error("identity version mismatch: stored {stored}, expected {expected}")]
    VersionMismatch {
        /// Version stamped on the durable row.
        stored: i32,

        /// The only version this build accepts.
        expected: i32,
    },
}

impl ClassifyError for CassandraCellStoreError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::Database(e) => e.classify_error(),
            Self::Encoding(_)
            | Self::CorruptCell { .. }
            | Self::CorruptUdt(_)
            | Self::VersionMismatch { .. } => ErrorCategory::Permanent,
        }
    }
}

/// The `event_ref` UDT carried a shape the deserializer does not accept.
#[derive(Debug, Error)]
pub enum CorruptUdtError {
    /// `kind` did not match a known event variant.
    #[error("unknown event_ref kind discriminator: {0}")]
    UnknownKind(i8),

    /// `kind == 1` (Timer) but `timer_type` was not a known discriminant.
    #[error("unknown event_ref timer_type discriminator: {0}")]
    UnknownTimerType(i8),

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
