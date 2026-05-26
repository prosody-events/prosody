//! Errors for the fjall-backed Value cache.

use crate::error::{ClassifyError, ErrorCategory};
use crate::state::encoding::EncodingError;
use fjall::Error as FjallEngineError;
use thiserror::Error;
use tokio::task::JoinError;

/// Errors that can occur while using the fjall Value cache.
#[derive(Debug, Error)]
pub enum FjallValueStoreError {
    /// Underlying fjall engine error.
    #[error("fjall engine error: {0}")]
    Engine(#[source] FjallEngineError),

    /// Cache codec error (payload encode/decode failed).
    #[error(transparent)]
    Encoding(#[from] EncodingError),

    /// A cached cell carried a tag byte the decoder does not recognize.
    #[error("unknown cache tag byte: 0x{0:02X}")]
    UnknownCacheTag(u8),

    /// A cached cell was empty (no tag byte).
    #[error("cached cell was empty")]
    EmptyCacheCell,

    /// A cached `Present` cell carried zero payload bytes; a stored
    /// payload must always have a non-empty `MsgPack` encoding.
    #[error("cached Present cell carried zero payload bytes")]
    EmptyPresentPayload,

    /// The blocking task that ran a fjall call failed (panic or cancel).
    #[error("fjall blocking task failed: {0}")]
    BlockingTaskJoin(#[from] JoinError),
}

impl From<FjallEngineError> for FjallValueStoreError {
    fn from(value: FjallEngineError) -> Self {
        Self::Engine(value)
    }
}

impl ClassifyError for FjallValueStoreError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            // Engine errors include I/O on local disk: treat as transient
            // so callers can retry once before falling through to backing.
            Self::Engine(_) | Self::BlockingTaskJoin(_) => ErrorCategory::Transient,
            // Corrupt cache cells and codec failures are permanent for that
            // cell; the combinator will invalidate and re-populate.
            Self::Encoding(_)
            | Self::UnknownCacheTag(_)
            | Self::EmptyCacheCell
            | Self::EmptyPresentPayload => ErrorCategory::Permanent,
        }
    }
}
