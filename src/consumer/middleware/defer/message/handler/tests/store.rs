//! Store faults for deferred-message traces.

use super::super::super::store::{MessageDeferStore, MessageRetryCompletionResult};
use crate::error::{ClassifyError, ErrorCategory};
use crate::{Key, Offset};
use parking_lot::Mutex;
use std::sync::Arc;
use thiserror::Error;

/// Selects the store call that receives a fault.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StoreOp {
    /// Checks whether the key has a queue.
    IsDeferred,
    /// Creates a deferred queue.
    DeferFirst,
    /// Appends an offset.
    DeferAdditional,
    /// Completes a retry and advances the queue.
    CompleteRetrySuccess,
    /// Increases the retry count.
    IncrementRetryCount,
    /// Reads the queue head.
    GetNext,
    /// Deletes the key.
    DeleteKey,
}

/// Selects an injected error category.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FaultKind {
    /// Reports a transient error.
    Transient,
    /// Reports a permanent error.
    Permanent,
}

/// Injects one error before the selected store call changes state.
#[derive(Clone)]
pub struct FailableStore<S> {
    inner: S,
    next_fault: Arc<Mutex<Option<(StoreOp, FaultKind)>>>,
}

impl<S> FailableStore<S> {
    /// Wraps the store with an empty fault slot.
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            next_fault: Arc::default(),
        }
    }

    /// Arms one fault for the selected call.
    pub fn set_fault(&self, fault: Option<(StoreOp, FaultKind)>) {
        *self.next_fault.lock() = fault;
    }

    /// Reports whether a fault still awaits its selected call.
    pub fn fault_pending(&self) -> bool {
        self.next_fault.lock().is_some()
    }

    fn check<E>(&self, op: StoreOp) -> Result<(), FailableStoreError<E>> {
        let mut slot = self.next_fault.lock();
        if let Some((target, kind)) = *slot
            && target == op
        {
            *slot = None;
            return Err(match kind {
                FaultKind::Transient => FailableStoreError::Transient,
                FaultKind::Permanent => FailableStoreError::Permanent,
            });
        }
        Ok(())
    }
}

impl<S: MessageDeferStore> MessageDeferStore for FailableStore<S> {
    type Error = FailableStoreError<S::Error>;

    async fn defer_first_message(&self, key: &Key, offset: Offset) -> Result<(), Self::Error> {
        self.check(StoreOp::DeferFirst)?;
        self.inner
            .defer_first_message(key, offset)
            .await
            .map_err(FailableStoreError::Inner)
    }

    async fn defer_additional_message(&self, key: &Key, offset: Offset) -> Result<(), Self::Error> {
        self.check(StoreOp::DeferAdditional)?;
        self.inner
            .defer_additional_message(key, offset)
            .await
            .map_err(FailableStoreError::Inner)
    }

    async fn complete_retry_success(
        &self,
        key: &Key,
        offset: Offset,
    ) -> Result<MessageRetryCompletionResult, Self::Error> {
        self.check(StoreOp::CompleteRetrySuccess)?;
        self.inner
            .complete_retry_success(key, offset)
            .await
            .map_err(FailableStoreError::Inner)
    }

    async fn increment_retry_count(
        &self,
        key: &Key,
        current_retry_count: u32,
    ) -> Result<u32, Self::Error> {
        self.check(StoreOp::IncrementRetryCount)?;
        self.inner
            .increment_retry_count(key, current_retry_count)
            .await
            .map_err(FailableStoreError::Inner)
    }

    async fn get_next_deferred_message(
        &self,
        key: &Key,
    ) -> Result<Option<(Offset, u32)>, Self::Error> {
        self.check(StoreOp::GetNext)?;
        self.inner
            .get_next_deferred_message(key)
            .await
            .map_err(FailableStoreError::Inner)
    }

    async fn is_deferred(&self, key: &Key) -> Result<Option<u32>, Self::Error> {
        self.check(StoreOp::IsDeferred)?;
        self.inner
            .is_deferred(key)
            .await
            .map_err(FailableStoreError::Inner)
    }

    async fn append_deferred_message(&self, key: &Key, offset: Offset) -> Result<(), Self::Error> {
        self.check(StoreOp::DeferAdditional)?;
        self.inner
            .append_deferred_message(key, offset)
            .await
            .map_err(FailableStoreError::Inner)
    }

    async fn remove_deferred_message(&self, key: &Key, offset: Offset) -> Result<(), Self::Error> {
        self.check(StoreOp::CompleteRetrySuccess)?;
        self.inner
            .remove_deferred_message(key, offset)
            .await
            .map_err(FailableStoreError::Inner)
    }

    async fn set_retry_count(&self, key: &Key, retry_count: u32) -> Result<(), Self::Error> {
        self.check(StoreOp::IncrementRetryCount)?;
        self.inner
            .set_retry_count(key, retry_count)
            .await
            .map_err(FailableStoreError::Inner)
    }

    async fn delete_key(&self, key: &Key) -> Result<(), Self::Error> {
        self.check(StoreOp::DeleteKey)?;
        self.inner
            .delete_key(key)
            .await
            .map_err(FailableStoreError::Inner)
    }
}

/// Reports an injected error or the inner store's error.
#[derive(Debug, Error)]
pub enum FailableStoreError<E> {
    /// Reports an injected transient store error.
    #[error("injected transient store failure")]
    Transient,
    /// Reports an injected permanent store error.
    #[error("injected permanent store failure")]
    Permanent,
    /// Retains the inner store error.
    #[error("inner store error: {0}")]
    Inner(E),
}

impl<E: ClassifyError> ClassifyError for FailableStoreError<E> {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::Transient => ErrorCategory::Transient,
            Self::Permanent => ErrorCategory::Permanent,
            Self::Inner(error) => error.classify_error(),
        }
    }
}
