//! Failable loader wrapper for property testing.
//!
//! Wraps [`MemoryLoader`] and allows single-shot failure injection
//! based on trace specifications.

use crate::consumer::message::ConsumerMessage;
use crate::consumer::middleware::defer::loader::{MemoryLoader, MemoryLoaderError, MessageLoader};
use crate::consumer::middleware::{ClassifyError, ErrorCategory};
use crate::{Key, Offset, Partition, Topic};
use parking_lot::Mutex;
use serde_json::Value;
use std::sync::Arc;
use thiserror::Error;

// ============================================================================
// Types
// ============================================================================

/// Failure mode for injection.
#[derive(Clone, Copy, Debug)]
pub enum LoaderFailureType {
    /// Injected permanent failure (message unrecoverable).
    Permanent,
    /// Injected transient failure (retry may succeed).
    Transient,
}

// ============================================================================
// FailableLoader
// ============================================================================

/// Wrapper that injects failures into loader operations.
///
/// Wraps a [`MemoryLoader`] and allows single-shot failure injection.
/// After a failure is returned, the failure mode is cleared (single-shot
/// semantics to prevent state leakage between test events).
#[derive(Clone)]
pub struct FailableLoader {
    inner: MemoryLoader,
    next_failure: Arc<Mutex<Option<LoaderFailureType>>>,
}

impl FailableLoader {
    /// Creates a new failable loader wrapping the given memory loader.
    #[must_use]
    pub fn new(inner: MemoryLoader) -> Self {
        Self {
            inner,
            next_failure: Arc::new(Mutex::new(None)),
        }
    }

    /// Configure next load to fail with given type.
    ///
    /// Cleared after one use (single-shot semantics).
    pub fn set_next_failure(&self, failure: Option<LoaderFailureType>) {
        *self.next_failure.lock() = failure;
    }

    /// Delegate `store_message` to inner loader.
    pub fn store_message(
        &self,
        topic: Topic,
        partition: Partition,
        offset: Offset,
        key: Key,
        payload: Value,
    ) {
        self.inner
            .store_message(topic, partition, offset, key, payload);
    }
}

impl MessageLoader for FailableLoader {
    type Error = FailableLoaderError;

    async fn load_message(
        &self,
        topic: Topic,
        partition: Partition,
        offset: Offset,
    ) -> Result<ConsumerMessage, Self::Error> {
        // Check for injected failure (single-shot: take clears it)
        if let Some(failure) = self.next_failure.lock().take() {
            return Err(match failure {
                LoaderFailureType::Permanent => FailableLoaderError::Permanent,
                LoaderFailureType::Transient => FailableLoaderError::Transient,
            });
        }
        // Delegate to inner loader
        self.inner
            .load_message(topic, partition, offset)
            .await
            .map_err(FailableLoaderError::Inner)
    }
}

// ============================================================================
// Errors
// ============================================================================

/// Errors from failable loader.
#[derive(Clone, Debug, Error)]
pub enum FailableLoaderError {
    /// Injected permanent failure (message unrecoverable).
    #[error("injected permanent failure")]
    Permanent,

    /// Injected transient failure (retry may succeed).
    #[error("injected transient failure")]
    Transient,

    /// Error from inner loader.
    #[error("inner loader error: {0:#}")]
    Inner(#[from] MemoryLoaderError),
}

impl ClassifyError for FailableLoaderError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::Permanent => ErrorCategory::Permanent,
            Self::Transient => ErrorCategory::Transient,
            Self::Inner(e) => e.classify_error(),
        }
    }
}
