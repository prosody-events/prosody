//! Store faults for deferred-timer traces.

use super::super::store::TimerDeferStore;
use super::context::TimerCapture;
use crate::Key;
use crate::timers::{Trigger, datetime::CompactDateTime};
use std::future::Future;

use crate::consumer::middleware::tests::test_support::faults::{
    FailableStoreError, FaultKind, FaultSlot,
};
use quickcheck::{Arbitrary, Gen};

/// Selects the store call that receives a fault.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StoreOp {
    /// Checks whether the key has a queue.
    IsDeferred,
    /// Creates a deferred queue.
    DeferFirst,
    /// Appends a timer.
    DeferAdditional,
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

impl Arbitrary for StoreOp {
    fn arbitrary(g: &mut Gen) -> Self {
        let ops = [
            Self::IsDeferred,
            Self::DeferFirst,
            Self::DeferAdditional,
            Self::IncrementRetryCount,
            Self::GetNext,
            Self::DeleteKey,
            Self::DeferredTimes,
            Self::Append,
            Self::Remove,
            Self::SetRetryCount,
        ];
        ops[usize::arbitrary(g) % ops.len()]
    }
}

/// Injects one error before the selected store call changes state, and checks
/// the timer rule after each queue write.
#[derive(Clone)]
pub struct FailableTimerStore<S> {
    inner: S,
    capture: TimerCapture,
    pub(super) next_fault: FaultSlot<StoreOp>,
}

impl<S> FailableTimerStore<S> {
    /// Wraps the store with an empty fault slot. The slot shares the
    /// capture's phase, so the store sees the trigger under dispatch.
    pub fn new(inner: S, capture: TimerCapture) -> Self {
        Self {
            next_fault: FaultSlot::sharing(capture.phase()),
            inner,
            capture,
        }
    }

    /// Runs one queue write, then checks the rule. A queue that stays
    /// non-empty keeps a retry timer that fires again. The trigger under
    /// dispatch has already fired, so it counts only when the dispatch
    /// re-armed its time. A violation is recorded, not raised: the handler
    /// must see the write succeed. The property reads it through
    /// `take_uncovered`.
    async fn write<T>(
        &self,
        op: StoreOp,
        key: &Key,
        write: impl Future<Output = Result<T, S::Error>>,
    ) -> Result<T, FailableStoreError<S::Error>>
    where
        S: TimerDeferStore,
    {
        self.check(op)?;
        let value = write.await.map_err(FailableStoreError::Inner)?;

        let deferred = self
            .inner
            .is_deferred(key)
            .await
            .map_err(FailableStoreError::Inner)?;
        if deferred.is_some()
            && !self
                .capture
                .fires_again(key, self.next_fault.phase().fired())
        {
            self.next_fault.record_uncovered(op);
        }

        Ok(value)
    }

    fn check<E>(&self, op: StoreOp) -> Result<(), FailableStoreError<E>> {
        if let Some(kind) = self.next_fault.check(op) {
            return Err(match kind {
                FaultKind::Transient => FailableStoreError::Transient,
                FaultKind::Permanent => FailableStoreError::Permanent,
                FaultKind::Terminal => FailableStoreError::Terminal,
            });
        }
        Ok(())
    }
}

impl<S: TimerDeferStore> TimerDeferStore for FailableTimerStore<S> {
    type Error = FailableStoreError<S::Error>;

    async fn defer_first_timer(&self, trigger: &Trigger) -> Result<(), Self::Error> {
        self.write(
            StoreOp::DeferFirst,
            &trigger.key,
            self.inner.defer_first_timer(trigger),
        )
        .await
    }

    async fn defer_additional_timer(&self, trigger: &Trigger) -> Result<(), Self::Error> {
        self.write(
            StoreOp::DeferAdditional,
            &trigger.key,
            self.inner.defer_additional_timer(trigger),
        )
        .await
    }

    async fn increment_retry_count(
        &self,
        key: &Key,
        current_retry_count: u32,
    ) -> Result<u32, Self::Error> {
        self.write(
            StoreOp::IncrementRetryCount,
            key,
            self.inner.increment_retry_count(key, current_retry_count),
        )
        .await
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
        self.write(
            StoreOp::Append,
            &trigger.key,
            self.inner.append_deferred_timer(trigger),
        )
        .await
    }

    async fn remove_deferred_timer(
        &self,
        key: &Key,
        time: CompactDateTime,
    ) -> Result<(), Self::Error> {
        self.write(
            StoreOp::Remove,
            key,
            self.inner.remove_deferred_timer(key, time),
        )
        .await
    }

    async fn set_retry_count(&self, key: &Key, retry_count: u32) -> Result<(), Self::Error> {
        self.write(
            StoreOp::SetRetryCount,
            key,
            self.inner.set_retry_count(key, retry_count),
        )
        .await
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
