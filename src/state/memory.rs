//! In-memory keyed-state stores.

use super::encoding::{EncodingError, decode_wal};
use super::pending::{PendingEntry, PendingIndexScanner, PendingIndexStore};
use super::value::{
    DirectApplyStore, DurableWalStore, PendingOpSource, StoredPayload, ValueKind, ValueOp,
    ValueStore, fold_value_ops,
};
use super::{
    CollectionId, CollectionKind, CollectionKindId, CollectionRef, DirtyStoreFactory,
    DirtyStoreProvider, DurableState, EventRef, EventScopeId, PayloadEncoding, PendingOps, Read,
    SealedCollection, SealedWal, StateKey, StateName, StateType, StoreOutcome, WalFormat,
};
use crate::error::{ClassifyError, ErrorCategory};
use crate::timers::duration::CompactDuration;
use crate::{Partition, Topic};
use ahash::RandomState;
use futures::{Stream, stream};
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
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
            .cloned()
            .flatten()
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
        self.inner
            .lock()
            .entries
            .insert(collection.clone(), Some(ValueOp::Set { payload }));
        Ok(())
    }

    async fn clear<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
    ) -> Result<(), Self::Error> {
        self.inner
            .lock()
            .entries
            .insert(collection.clone(), Some(ValueOp::Clear));
        Ok(())
    }
}

/// Per-partition provider for [`MemoryDirtyValueStore`].
///
/// In-memory dirty stores have no per-partition state, so the provider
/// is unit-shaped and `for_scope` returns a fresh
/// [`MemoryDirtyValueStore`] each call.
#[derive(Clone, Debug, Default)]
pub struct MemoryDirtyValueStoreProvider;

impl DirtyStoreProvider<ValueKind> for MemoryDirtyValueStoreProvider {
    type Store = MemoryDirtyValueStore;

    fn for_scope(&self, _scope: EventScopeId) -> Self::Store {
        MemoryDirtyValueStore::new()
    }
}

/// Process-wide factory that hands out
/// [`MemoryDirtyValueStoreProvider`]s.
#[derive(Clone, Debug, Default)]
pub struct MemoryDirtyValueStoreFactory;

impl DirtyStoreFactory<ValueKind> for MemoryDirtyValueStoreFactory {
    type Error = Infallible;
    type Provider = MemoryDirtyValueStoreProvider;

    fn for_partition(
        &self,
        _topic: Topic,
        _partition: Partition,
    ) -> Result<Self::Provider, Self::Error> {
        Ok(MemoryDirtyValueStoreProvider)
    }
}

impl PendingOpSource<ValueKind> for MemoryDirtyValueStore {
    type Error = MemoryStateError;
    type Ops<'a> = OptionIntoIter<ValueOp>;

    fn pending_ops<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
    ) -> Result<Option<PendingOps<Self::Ops<'a>>>, Self::Error> {
        let op = self.inner.lock().entries.get(collection).cloned().flatten();
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
///
/// The store ignores TTL at the write layer (memory has no expiry), but it
/// still threads its constructor-supplied `default_ttl` through every
/// [`CollectionRef`] it builds for [`ValueStore::set`] / [`ValueStore::clear`].
/// This keeps the production pattern symmetric with [`super::cassandra`] so a
/// careless copy-paste cannot regress to hardcoded `None`.
#[derive(Clone, Debug)]
pub struct MemoryDurableValueStore {
    inner: Arc<Mutex<DurableInner>>,
    default_ttl: Option<CompactDuration>,
}

impl MemoryDurableValueStore {
    /// Creates an empty durable Value store with a constructor-supplied
    /// default TTL. The TTL is propagated through every
    /// [`CollectionRef`] built by [`ValueStore::set`] / [`ValueStore::clear`].
    /// Pass `None` for indefinite retention (or the Cassandra >20-year
    /// overflow fallback at the wiring layer).
    #[must_use]
    pub fn new(default_ttl: Option<CompactDuration>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(DurableInner::default())),
            default_ttl,
        }
    }

    /// Test-only convenience: one-hour default TTL.
    #[cfg(test)]
    #[must_use]
    pub fn for_tests() -> Self {
        Self::new(Some(CompactDuration::new(3_600)))
    }

    /// Applies a single op via [`DirectApplyStore`], threading the
    /// constructor-supplied `default_ttl` through the built [`CollectionRef`].
    /// Backs the [`ValueStore::set`] / [`ValueStore::clear`] impls.
    async fn apply_one(
        &self,
        collection: &CollectionId<ValueKind>,
        op: ValueOp,
    ) -> Result<(), MemoryStateError> {
        let collection_ref = CollectionRef::new(collection.clone(), self.default_ttl);
        self.direct_apply(&collection_ref, vec![op]).await?;
        Ok(())
    }

    /// Resolves a sealed event, running `resolve` over the matched WAL before
    /// clearing it. Shared by `apply_sealed` (folds the decoded ops into
    /// `applied`) and `rollback_sealed` (a no-op resolution). The whole body is
    /// synchronous, so the `parking_lot` guard is never held across an await.
    ///
    /// `resolve` runs *before* the WAL is cleared, so a failing resolution
    /// (e.g. a WAL decode error) leaves both `applied` and `wal` intact.
    fn resolve_sealed(
        &self,
        collection: &CollectionRef<ValueKind>,
        expected_event: EventRef,
        resolve: impl FnOnce(
            &mut DurableValueEntry,
            &SealedWal<ValueKind>,
        ) -> Result<(), MemoryStateError>,
    ) -> Result<StoreOutcome, MemoryStateError> {
        let pending_key = PendingKey::from_id(collection.id());
        let mut inner = self.inner.lock();
        let entry = inner.entries.entry(collection.id().clone()).or_default();
        let Some(sealed) = entry.wal.clone() else {
            return Ok(StoreOutcome::NoOp);
        };
        if sealed.event() != expected_event {
            return Err(MemoryStateError::EventMismatch {
                expected: expected_event,
                actual: sealed.event(),
            });
        }

        resolve(entry, &sealed)?;
        entry.wal = None;
        // WAL cleared; drop the pending row last (Cassandra ordering). The
        // `entry` borrow has ended, so re-borrowing `inner` is sound.
        inner.pending.remove(&pending_key);
        Ok(StoreOutcome::Applied)
    }
}

impl ValueStore for MemoryDurableValueStore {
    type Error = MemoryStateError;

    async fn get<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
    ) -> Result<Read<StoredPayload>, Self::Error> {
        let state = self.read_partition(collection).await?;
        Ok(match state {
            DurableState::Idle { applied } | DurableState::Sealed { applied, .. } => {
                applied.map_or(Read::Absent, Read::Present)
            }
        })
    }

    async fn set<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
        payload: StoredPayload,
    ) -> Result<(), Self::Error> {
        self.apply_one(collection, ValueOp::Set { payload }).await
    }

    async fn clear<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
    ) -> Result<(), Self::Error> {
        self.apply_one(collection, ValueOp::Clear).await
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
        collection: &'a CollectionRef<ValueKind>,
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

        // Mirror Cassandra's ordering: record the pending row first, then
        // write the WAL. Mutate `pending` before borrowing `entries` so the
        // two `&mut inner` reaches do not overlap (the trait methods are not
        // called here — the mutex is non-reentrant). See [`PendingKey`].
        let pending_key = PendingKey::from_id(collection.id());
        let mut inner = self.inner.lock();
        inner.pending.insert(pending_key);
        let entry = inner.entries.entry(collection.id().clone()).or_default();
        entry.wal = Some(wal);
        Ok(SealedCollection::new(collection.clone(), event))
    }

    async fn apply_sealed<'a>(
        &'a self,
        collection: &'a CollectionRef<ValueKind>,
        expected_event: EventRef,
    ) -> Result<StoreOutcome, Self::Error> {
        // Sync resolution: nothing awaits under the lock, and `decode_wal`
        // running before the WAL is cleared keeps a decode failure
        // non-destructive (state survives intact on `Err`).
        self.resolve_sealed(collection, expected_event, |entry, sealed| {
            let envelope =
                decode_wal::<ValueKind>(sealed.wal()).map_err(MemoryStateError::Encoding)?;
            entry.applied = fold_value_ops(entry.applied.clone(), &envelope.into_ops());
            Ok(())
        })
    }

    async fn rollback_sealed<'a>(
        &'a self,
        collection: &'a CollectionRef<ValueKind>,
        expected_event: EventRef,
    ) -> Result<StoreOutcome, Self::Error> {
        self.resolve_sealed(collection, expected_event, |_, _| Ok(()))
    }
}

impl DirectApplyStore<ValueKind> for MemoryDurableValueStore {
    type Error = MemoryStateError;

    async fn direct_apply<'a, I>(
        &'a self,
        collection: &'a CollectionRef<ValueKind>,
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
        let entry = inner.entries.entry(collection.id().clone()).or_default();
        entry.applied = fold_value_ops(entry.applied.clone(), &ops);
        Ok(StoreOutcome::Applied)
    }
}

impl PendingIndexStore for MemoryDurableValueStore {
    type Error = MemoryStateError;

    async fn insert_pending<'a, K>(&'a self, id: &'a CollectionId<K>) -> Result<(), Self::Error>
    where
        K: CollectionKind,
    {
        self.inner.lock().pending.insert(PendingKey::from_id(id));
        Ok(())
    }

    async fn delete_pending<'a, K>(&'a self, id: &'a CollectionId<K>) -> Result<(), Self::Error>
    where
        K: CollectionKind,
    {
        self.inner.lock().pending.remove(&PendingKey::from_id(id));
        Ok(())
    }
}

impl PendingIndexScanner for MemoryDurableValueStore {
    type Error = MemoryStateError;

    /// Streams the pending Value entries on `(segment, key)` from the
    /// dedicated pending index, yielding one entry per recorded
    /// `PendingKey` whose segment-qualified key matches `state_key`.
    ///
    /// Reading from the real index (rather than deriving from
    /// `entry.wal.is_some()`) lets the memory backend represent the
    /// crash state *pending-written / WAL-not-written* — the
    /// `Idle ⇒ delete_pending` recovery sweep arm that derived presence
    /// could never reproduce. The snapshot is taken under the inner lock;
    /// the stream itself is materialized at call time (the memory backend
    /// has no I/O to incrementalize), which honors the "Collection scans
    /// are streaming" contract by shape even though the underlying data is
    /// fully resident.
    fn scan_pending(
        &self,
        state_key: &StateKey,
    ) -> impl Stream<Item = Result<PendingEntry, Self::Error>> + Send {
        let snapshot: Vec<PendingEntry> = {
            let inner = self.inner.lock();
            inner
                .pending
                .iter()
                .filter(|key| &key.state_key == state_key)
                .map(|key| PendingEntry::new(key.state_type, key.kind, key.name.clone()))
                .collect()
        };

        stream::iter(snapshot.into_iter().map(Ok))
    }
}

#[derive(Debug, Default)]
struct DirtyInner {
    entries: HashMap<CollectionId<ValueKind>, Option<ValueOp>, RandomState>,
}

#[derive(Debug, Default)]
struct DurableInner {
    entries: HashMap<CollectionId<ValueKind>, DurableValueEntry, RandomState>,
    pending: HashSet<PendingKey, RandomState>,
}

/// Untyped key into the in-memory pending-WAL index.
///
/// [`PendingIndexStore::insert_pending`] / `delete_pending` are generic over
/// the collection kind `K`, so the key erases the type-level kind into the
/// runtime [`CollectionKindId`] discriminator. Storing `kind` lets the
/// kind-agnostic [`PendingIndexScanner::scan_pending`] mirror Cassandra's
/// cross-kind scan and keeps the middleware's WARN-and-skip path for
/// non-Value kinds reachable from memory-backed tests.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct PendingKey {
    state_key: StateKey,
    state_type: StateType,
    kind: CollectionKindId,
    name: StateName,
}

impl PendingKey {
    fn from_id<K>(id: &CollectionId<K>) -> Self
    where
        K: CollectionKind,
    {
        Self {
            state_key: id.state_key().clone(),
            state_type: id.state_type(),
            kind: id.kind(),
            name: id.name().clone(),
        }
    }
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
