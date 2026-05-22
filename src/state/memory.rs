//! In-memory keyed-state stores.

use super::encoding::{EncodingError, decode_wal};
use super::value::{
    DirectApplyStore, DurableWalStore, PendingOpSource, StoredPayload, ValueKind, ValueOp,
    ValueStore, fold_value_ops,
};
use super::{
    CollectionId, CollectionRef, DurableState, EventRef, PayloadEncoding, PendingOps, Read,
    SealedCollection, SealedWal, StoreOutcome, WalFormat,
};
use crate::error::{ClassifyError, ErrorCategory};
use ahash::RandomState;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::num::NonZeroU64;
use std::option::IntoIter as OptionIntoIter;
use std::sync::Arc;
use thiserror::Error;

/// In-memory dirty Value store.
///
/// The store keeps only the compact final Value operation. Collections absent
/// from this store are untouched, so reads return [`Read::Unknown`].
#[derive(Clone, Debug)]
pub struct MemoryDirtyValueStore {
    inner: Arc<Mutex<DirtyInner>>,
}

impl MemoryDirtyValueStore {
    /// Creates an empty dirty Value store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl Default for MemoryDirtyValueStore {
    fn default() -> Self {
        Self {
            inner: Arc::new(Mutex::new(DirtyInner::default())),
        }
    }
}

impl ValueStore for MemoryDirtyValueStore {
    type Error = MemoryStateError;

    async fn get<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
    ) -> Result<Read<StoredPayload>, Self::Error> {
        Ok(self
            .inner
            .lock()
            .entries
            .get(collection)
            .and_then(|entry| entry.op.clone())
            .map_or(Read::Unknown, |op| match op {
                ValueOp::Set { payload } => Read::Present(payload),
                ValueOp::Clear => Read::Absent,
            }))
    }

    async fn set<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
        payload: StoredPayload,
    ) -> Result<(), Self::Error> {
        let mut inner = self.inner.lock();
        let entry = inner.entries.entry(collection.clone()).or_default();
        entry.op = Some(ValueOp::Set { payload });
        Ok(())
    }

    async fn clear<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
    ) -> Result<(), Self::Error> {
        let mut inner = self.inner.lock();
        let entry = inner.entries.entry(collection.clone()).or_default();
        entry.op = Some(ValueOp::Clear);
        Ok(())
    }
}

impl PendingOpSource<ValueKind> for MemoryDirtyValueStore {
    type Error = MemoryStateError;
    type Ops<'a> = OptionIntoIter<ValueOp>;

    fn pending_ops<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
    ) -> Result<Option<PendingOps<Self::Ops<'a>>>, Self::Error> {
        let op = self
            .inner
            .lock()
            .entries
            .get(collection)
            .and_then(|entry| entry.op.clone());
        Ok(op.map(|op| PendingOps {
            count: NonZeroU64::MIN,
            ops: Some(op).into_iter(),
        }))
    }

    fn clear_pending_ops(&self, collection: &CollectionId<ValueKind>) -> Result<(), Self::Error> {
        self.inner.lock().entries.remove(collection);
        Ok(())
    }
}

/// In-memory durable Value store.
///
/// Each collection has authoritative applied state and at most one sealed WAL
/// record. Sealing never mutates applied state; applying or rolling back a
/// matching sealed event resolves the WAL record.
#[derive(Clone, Debug)]
pub struct MemoryDurableValueStore {
    inner: Arc<Mutex<DurableInner>>,
}

impl MemoryDurableValueStore {
    /// Creates an empty durable Value store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl Default for MemoryDurableValueStore {
    fn default() -> Self {
        Self {
            inner: Arc::new(Mutex::new(DurableInner::default())),
        }
    }
}

impl DurableWalStore<ValueKind> for MemoryDurableValueStore {
    type Error = MemoryStateError;

    async fn read_partition<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
    ) -> Result<DurableState<ValueKind>, Self::Error> {
        let inner = self.inner.lock();
        let entry = inner.entries.get(collection);
        let applied = entry.and_then(|entry| entry.applied.clone());
        if let Some(wal) = entry.and_then(|entry| entry.wal.clone()) {
            return Ok(DurableState::Sealed { applied, wal });
        }

        Ok(DurableState::Idle { applied })
    }

    async fn seal<'a, I>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
        event: EventRef,
        ops: I,
    ) -> Result<SealedCollection<ValueKind>, Self::Error>
    where
        I: IntoIterator<Item = ValueOp> + Send + 'a,
    {
        let collected: Vec<ValueOp> = ops.into_iter().collect();
        let wal = SealedWal::try_new(
            event,
            collected,
            WalFormat::MsgpackStreamV1,
            PayloadEncoding::MsgpackV1,
        )
        .map_err(MemoryStateError::Encoding)?;

        let mut inner = self.inner.lock();
        let entry = inner.entries.entry(collection.clone()).or_default();
        if entry.wal.is_some() {
            return Err(MemoryStateError::AlreadySealed);
        }

        entry.wal = Some(wal);
        Ok(SealedCollection::new(
            CollectionRef::new(collection.clone()),
            event,
        ))
    }

    async fn apply_sealed<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
        expected_event: EventRef,
    ) -> Result<StoreOutcome, Self::Error> {
        let mut inner = self.inner.lock();
        let entry = inner.entries.entry(collection.clone()).or_default();
        let Some(sealed) = entry.wal.clone() else {
            return Ok(StoreOutcome::NoOp);
        };
        if sealed.event() != expected_event {
            return Err(MemoryStateError::EventMismatch {
                expected: expected_event,
                actual: sealed.event(),
            });
        }

        let envelope = decode_wal::<ValueKind>(sealed.wal()).map_err(MemoryStateError::Encoding)?;
        let ops = envelope.into_ops();
        entry.applied = fold_value_ops(entry.applied.clone(), &ops);
        entry.wal = None;
        Ok(StoreOutcome::Applied)
    }

    async fn rollback_sealed<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
        expected_event: EventRef,
    ) -> Result<StoreOutcome, Self::Error> {
        let mut inner = self.inner.lock();
        let entry = inner.entries.entry(collection.clone()).or_default();
        let Some(sealed) = entry.wal.clone() else {
            return Ok(StoreOutcome::NoOp);
        };
        if sealed.event() != expected_event {
            return Err(MemoryStateError::EventMismatch {
                expected: expected_event,
                actual: sealed.event(),
            });
        }

        entry.wal = None;
        Ok(StoreOutcome::Applied)
    }
}

impl DirectApplyStore<ValueKind> for MemoryDurableValueStore {
    type Error = MemoryStateError;

    async fn direct_apply<'a, I>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
        ops: I,
    ) -> Result<StoreOutcome, Self::Error>
    where
        I: IntoIterator<Item = ValueOp> + Send + 'a,
    {
        let ops = ops.into_iter().collect::<Vec<_>>();
        if ops.is_empty() {
            return Ok(StoreOutcome::NoOp);
        }

        let mut inner = self.inner.lock();
        let entry = inner.entries.entry(collection.clone()).or_default();
        entry.applied = fold_value_ops(entry.applied.clone(), &ops);
        Ok(StoreOutcome::Applied)
    }
}

#[derive(Debug, Default)]
struct DirtyInner {
    entries: HashMap<CollectionId<ValueKind>, DirtyValueEntry, RandomState>,
}

#[derive(Debug, Default)]
struct DirtyValueEntry {
    op: Option<ValueOp>,
}

#[derive(Debug, Default)]
struct DurableInner {
    entries: HashMap<CollectionId<ValueKind>, DurableValueEntry, RandomState>,
}

/// Authoritative state for one durable Value collection.
///
/// Invariant: `applied` always reflects the pre-seal authoritative state.
/// `seal` writes only `wal`; `applied` is mutated solely by `apply_sealed`
/// and `direct_apply`. This is what lets `rollback_sealed` resolve a
/// sealed event with `entry.wal = None` and no `applied` restoration.
#[derive(Debug, Default)]
struct DurableValueEntry {
    applied: Option<StoredPayload>,
    wal: Option<SealedWal<ValueKind>>,
}

/// Error returned by memory keyed-state stores.
#[derive(Debug, Error)]
pub enum MemoryStateError {
    /// A seal was requested while another event is already sealed.
    #[error("state collection is already sealed")]
    AlreadySealed,

    /// WAL encoding or decoding failed.
    #[error(transparent)]
    Encoding(#[from] EncodingError),

    /// The sealed event did not match the event being resolved.
    #[error("sealed event mismatch: expected {expected:?}, actual {actual:?}")]
    EventMismatch {
        /// Event requested by the caller.
        expected: EventRef,

        /// Event stored in durable sealed state.
        actual: EventRef,
    },
}

impl ClassifyError for MemoryStateError {
    fn classify_error(&self) -> ErrorCategory {
        ErrorCategory::Permanent
    }
}
