//! Errors for the Cassandra trigger store.
//!
//! Most failures are network or timeout errors that should be retried; a few
//! are programming or configuration mistakes (slab-size mismatch, serialising
//! `Absent` as a UDT) that indicate a bug and must not be retried. The
//! [`ClassifyError`] impl on [`CassandraTriggerStoreError`] makes that
//! distinction explicit so the retry layer doesn't need to inspect error
//! messages.
//!
//! [`ClassifyError`]: crate::error::ClassifyError

use crate::cassandra::errors::CassandraStoreError;
use crate::error::{ClassifyError, ErrorCategory};
use crate::timers::duration::CompactDuration;
use crate::timers::error::ParseError;
use crate::timers::store::{InvalidSegmentVersionError, SegmentId};
use thiserror::Error;

/// Errors that can occur during Cassandra trigger store operations.
#[derive(Debug, Error)]
pub enum CassandraTriggerStoreError {
    /// Database error
    #[error("database error: {0:#}")]
    Database(#[from] CassandraStoreError),

    /// Invalid segment version value.
    #[error("Invalid segment version: {0:#}")]
    InvalidSegmentVersion(#[from] InvalidSegmentVersionError),

    /// Slab size mismatch during segment insertion.
    #[error(
        "Cannot insert segment {segment_id} with slab_size {segment_slab_size} that differs from \
         configured slab_size {configured_slab_size}"
    )]
    SlabSizeMismatch {
        /// The ID of the segment being inserted.
        segment_id: SegmentId,
        /// The slab size of the segment being inserted.
        segment_slab_size: CompactDuration,
        /// The configured slab size for this store.
        configured_slab_size: CompactDuration,
    },

    /// Segment disappeared during data migration.
    #[error("Segment {segment_id} disappeared during {operation}")]
    SegmentDisappeared {
        /// The ID of the segment that disappeared.
        segment_id: SegmentId,
        /// The operation that was being performed when the segment disappeared.
        operation: &'static str,
    },

    /// Invalid timer type value in database.
    #[error("Invalid timer type: {0:#}")]
    Parse(#[from] ParseError),

    /// `TimerState::Absent` cannot be serialized as a Cassandra UDT value.
    ///
    /// `Absent` is represented as a missing map entry, never as a UDT value.
    #[error("TimerState::Absent cannot be serialized as a UDT value")]
    AbsentStateNotSerializable,
}

impl ClassifyError for CassandraTriggerStoreError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::Database(e) => e.classify_error(),
            Self::InvalidSegmentVersion(e) => e.classify_error(),

            // Attempting to insert segment with slab_size different from store's configured
            // slab_size, or serializing Absent as a UDT — both are programming/configuration
            // errors that are not retryable.
            Self::SlabSizeMismatch { .. } | Self::AbsentStateNotSerializable => {
                ErrorCategory::Terminal
            }

            // Segment disappeared during data migration, likely due to concurrent deletion or
            // race condition. Retrying might succeed if segment reappears or operation completes.
            Self::SegmentDisappeared { .. } => ErrorCategory::Transient,

            Self::Parse(e) => e.classify_error(),
        }
    }
}
