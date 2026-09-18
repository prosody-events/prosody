//! Store faults for deferred-timer traces.

use super::super::store::{TimerDeferStore, TimerRetryCompletionResult};
use crate::Key;
use crate::consumer::middleware::defer::message::handler::tests::store::{
    FailableStoreError, FaultKind,
};
use crate::timers::{Trigger, datetime::CompactDateTime};
use parking_lot::Mutex;
use std::future::Future;
use std::sync::Arc;

/// Selects the store call that receives a fault.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StoreOp {
    /// Checks whether the key has a queue.
    IsDeferred,
    /// Creates a deferred queue.
    DeferFirst,
    /// Appends a timer.
    DeferAdditional,
    /// Completes a retry and advances the queue.
    CompleteRetrySuccess,
    /// Increases the retry count.
    IncrementRetryCount,
    /// Reads the queue head.
    GetNext,
    /// Deletes the key.
    DeleteKey,
    /// Reads all deferred times.
    DeferredTimes,
    /// Appends a timer without a retry count change.
    Append,
    /// Removes one timer.
    Remove,
    /// Sets the retry count.
    SetRetryCount,
}

/// Injects one error before the selected store call changes state.
#[derive(Clone)]
pub struct FailableTimerStore<S> {
    inner: S,
    next_fault: Arc<Mutex<Option<(StoreOp, FaultKind)>>>,
}

impl<S> FailableTimerStore<S> {
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

impl<S: TimerDeferStore> TimerDeferStore for FailableTimerStore<S> {
    type Error = FailableStoreError<S::Error>;

    async fn defer_first_timer(&self, trigger: &Trigger) -> Result<(), Self::Error> {
        self.check(StoreOp::DeferFirst)?;
        self.inner
            .defer_first_timer(trigger)
            .await
            .map_err(FailableStoreError::Inner)
    }

    async fn defer_additional_timer(&self, trigger: &Trigger) -> Result<(), Self::Error> {
        self.check(StoreOp::DeferAdditional)?;
        self.inner
            .defer_additional_timer(trigger)
            .await
            .map_err(FailableStoreError::Inner)
    }

    async fn complete_retry_success(
        &self,
        key: &Key,
        time: CompactDateTime,
    ) -> Result<TimerRetryCompletionResult, Self::Error> {
        self.check(StoreOp::CompleteRetrySuccess)?;
        self.inner
            .complete_retry_success(key, time)
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

    async fn get_next_deferred_timer(
        &self,
        key: &Key,
    ) -> Result<Option<(Trigger, u32)>, Self::Error> {
        self.check(StoreOp::GetNext)?;
        self.inner
            .get_next_deferred_timer(key)
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

    async fn append_deferred_timer(&self, trigger: &Trigger) -> Result<(), Self::Error> {
        self.check(StoreOp::Append)?;
        self.inner
            .append_deferred_timer(trigger)
            .await
            .map_err(FailableStoreError::Inner)
    }

    async fn remove_deferred_timer(
        &self,
        key: &Key,
        time: CompactDateTime,
    ) -> Result<(), Self::Error> {
        self.check(StoreOp::Remove)?;
        self.inner
            .remove_deferred_timer(key, time)
            .await
            .map_err(FailableStoreError::Inner)
    }

    async fn set_retry_count(&self, key: &Key, retry_count: u32) -> Result<(), Self::Error> {
        self.check(StoreOp::SetRetryCount)?;
        self.inner
            .set_retry_count(key, retry_count)
            .await
            .map_err(FailableStoreError::Inner)
    }

    fn deferred_times(
        &self,
        key: &Key,
    ) -> impl Future<Output = Result<Vec<CompactDateTime>, Self::Error>> + Send + 'static {
        let checked = self.check(StoreOp::DeferredTimes);
        let inner = self.inner.deferred_times(key);
        async move {
            checked?;
            inner.await.map_err(FailableStoreError::Inner)
        }
    }

    async fn delete_key(&self, key: &Key) -> Result<(), Self::Error> {
        self.check(StoreOp::DeleteKey)?;
        self.inner
            .delete_key(key)
            .await
            .map_err(FailableStoreError::Inner)
    }
}
