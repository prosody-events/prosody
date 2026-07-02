//! Errors for the fjall-backed cell cache.

use crate::error::{ClassifyError, ErrorCategory};
use fjall::Error as FjallEngineError;
use thiserror::Error;
use tokio::task::JoinError;

/// Errors that can occur while using the fjall cell cache.
#[derive(Debug, Error)]
pub enum FjallCellCacheError {
    /// Underlying fjall engine error.
    #[error("fjall engine error: {0}")]
    Engine(#[from] FjallEngineError),

    /// A stored cell carried a tag byte the decoder does not recognize.
    #[error("unknown cache tag byte: 0x{0:02X}")]
    UnknownCacheTag(u8),

    /// A stored cell was empty (no tag byte).
    #[error("cached cell was empty")]
    EmptyCacheCell,

    /// A coverage bound frame carried a tag byte the decoder does not recognize
    /// (an own-write corruption bug).
    #[error("unknown coverage bound tag byte: 0x{0:02X}")]
    UnknownBoundTag(u8),

    /// The blocking task that ran a fjall call failed (panic or cancel).
    #[error("fjall blocking task failed: {0}")]
    BlockingTaskJoin(#[from] JoinError),

    /// Test-only injected fault, for the publish-failure (uncover-on-put-error)
    /// regression.
    #[cfg(test)]
    #[error("injected cache fault")]
    Injected,
}

impl ClassifyError for FjallCellCacheError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            // Engine errors include I/O on local disk: treat as transient
            // so callers can retry once before falling through to backing.
            Self::Engine(_) | Self::BlockingTaskJoin(_) => ErrorCategory::Transient,
            #[cfg(test)]
            Self::Injected => ErrorCategory::Transient,
            // A corrupt stored cell is permanent for that cell: re-reading the
            // same bytes cannot succeed. The covered serve falls through to the
            // authoritative cell store and re-publishes a fresh entry.
            Self::UnknownCacheTag(_) | Self::EmptyCacheCell | Self::UnknownBoundTag(_) => {
                ErrorCategory::Permanent
            }
        }
    }
}
