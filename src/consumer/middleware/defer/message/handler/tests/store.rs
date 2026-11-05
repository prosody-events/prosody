//! Store faults for deferred-message traces.

use super::super::super::store::MessageDeferStore;
use super::context::TimerCapture;
use crate::{Key, Offset};
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
    /// Appends an offset.
    DeferAdditional,
    /// Increases the retry count.
    IncrementRetryCount,
    /// Reads the queue head.
    GetNext,
    /// Removes one message.
    Remove,
    /// Deletes the key.
    DeleteKey,
}

impl Arbitrary for StoreOp {
    fn arbitrary(g: &mut Gen) -> Self {
        let ops = [
            Self::IsDeferred,
            Self::DeferFirst,
            Self::DeferAdditional,
            Self::IncrementRetryCount,
            Self::GetNext,
            Self::Remove,
            Self::DeleteKey,
        ];
        ops[usize::arbitrary(g) % ops.len()]
    }
}

/// Injects one error before the selected store call changes state, and checks
/// the timer rule after each queue write.
#[derive(Clone)]
pub struct FailableStore<S> {
    inner: S,
    capture: TimerCapture,
    pub(super) next_fault: FaultSlot<StoreOp>,
}

impl<S> FailableStore<S> {
    /// Wraps the store with an empty fault slot. The slot shares the
    /// capture's phase, so the store sees the trigger under dispatch.
    pub fn new(inner: S, capture: TimerCapture) -> Self {
        Self {
            next_fault: FaultSlot::sharing(capture.next_fault.phase()),
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
        S: MessageDeferStore,
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

impl<S: MessageDeferStore> MessageDeferStore for FailableStore<S> {
    type Error = FailableStoreError<S::Error>;

    async fn defer_first_message(&self, key: &Key, offset: Offset) -> Result<(), Self::Error> {
        self.write(
            StoreOp::DeferFirst,
            key,
            self.inner.defer_first_message(key, offset),
        )
        .await
    }

    async fn defer_additional_message(&self, key: &Key, offset: Offset) -> Result<(), Self::Error> {
        self.write(
            StoreOp::DeferAdditional,
            key,
            self.inner.defer_additional_message(key, offset),
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
        self.write(
            StoreOp::DeferAdditional,
            key,
            self.inner.append_deferred_message(key, offset),
        )
        .await
    }

    async fn remove_deferred_message(&self, key: &Key, offset: Offset) -> Result<(), Self::Error> {
        self.write(
            StoreOp::Remove,
            key,
            self.inner.remove_deferred_message(key, offset),
        )
        .await
    }

    async fn set_retry_count(&self, key: &Key, retry_count: u32) -> Result<(), Self::Error> {
        self.write(
            StoreOp::IncrementRetryCount,
            key,
            self.inner.set_retry_count(key, retry_count),
        )
        .await
    }

    async fn delete_key(&self, key: &Key) -> Result<(), Self::Error> {
        self.check(StoreOp::DeleteKey)?;
        self.inner
            .delete_key(key)
            .await
            .map_err(FailableStoreError::Inner)
    }
}
