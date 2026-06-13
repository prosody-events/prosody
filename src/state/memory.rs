//! In-memory keyed-state stores.

use super::cell::{Cell, Committed, ProvisionalCell, ProvisionalWrite};
use super::descriptor_identity::{DescriptorIdentityStore, DurableDescriptorIdentity};
use super::partition_store::CommittedCache;
use super::store::CellStore;
use super::value::{PendingOpSource, ValueKind, ValueOp, ValueStore};
use super::{
    CollectionId, CollectionRef, DirtyStoreProvider, EventRef, EventScopeId, PendingOps, Read,
};
use crate::timers::store::SegmentId;
use ahash::RandomState;
use bytes::Bytes;
use futures::{Stream, stream};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::convert::Infallible;
use std::option::IntoIter as OptionIntoIter;
use std::sync::Arc;

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
    type Error = Infallible;

    async fn get<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
    ) -> Result<Read<Bytes>, Self::Error> {
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
        payload: Bytes,
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

impl PendingOpSource<ValueKind> for MemoryDirtyValueStore {
    type Error = Infallible;
    type Ops<'a> = OptionIntoIter<ValueOp>;

    fn pending_ops<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
    ) -> Result<Option<PendingOps<Self::Ops<'a>>>, Self::Error> {
        let op = self.inner.lock().entries.get(collection).cloned().flatten();
        Ok(op.map(PendingOps::single))
    }

    fn clear_pending_ops(&self, collection: &CollectionId<ValueKind>) -> Result<(), Self::Error> {
        self.inner.lock().entries.remove(collection);
        Ok(())
    }
}

/// In-memory [`CellStore`] for the Value kind.
///
/// The provisional-cell durable backend: one cell per collection (Value's
/// `CellAddr` is `()`), each either resolved or provisional, plus the
/// per-segment descriptor-identity rows. One instance is shared
/// process-wide across partition reassignments (`Clone` shares the `Arc`),
/// so committed state and identities survive a rebalance within the process.
#[derive(Clone, Debug, Default)]
pub struct MemoryCellStore {
    inner: Arc<Mutex<CellInner>>,
}

impl MemoryCellStore {
    /// Creates an empty cell store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl CellStore<ValueKind> for MemoryCellStore {
    type Error = Infallible;

    async fn read_cell<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
        (): &'a (),
    ) -> Result<Cell, Self::Error> {
        Ok(self
            .inner
            .lock()
            .cells
            .get(collection)
            .map_or_else(|| Cell::Resolved(Committed::new(None)), StoredCell::to_cell))
    }

    fn provisional_cells<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
    ) -> impl Stream<Item = Result<((), ProvisionalCell), Self::Error>> + Send + 'a {
        let provisional = match self.inner.lock().cells.get(collection) {
            Some(StoredCell::Provisional { data, prev, event }) => {
                Some(ProvisionalCell::new(data.clone(), prev.clone(), *event))
            }
            _ => None,
        };
        stream::iter(provisional.map(|cell| Ok(((), cell))))
    }

    async fn write_provisional<'a>(
        &'a self,
        collection: &'a CollectionRef<ValueKind>,
        (): &'a (),
        write: &'a ProvisionalWrite,
    ) -> Result<(), Self::Error> {
        self.inner.lock().cells.insert(
            collection.id().clone(),
            StoredCell::Provisional {
                data: write.data().cloned(),
                prev: write.prev().cloned(),
                event: write.event(),
            },
        );
        Ok(())
    }

    async fn write_resolved<'a>(
        &'a self,
        collection: &'a CollectionRef<ValueKind>,
        (): &'a (),
        data: Option<&'a Bytes>,
    ) -> Result<(), Self::Error> {
        self.inner
            .lock()
            .cells
            .insert(collection.id().clone(), StoredCell::Resolved(data.cloned()));
        Ok(())
    }

    async fn mark_resolved<'a>(
        &'a self,
        collection: &'a CollectionRef<ValueKind>,
        (): &'a (),
    ) -> Result<(), Self::Error> {
        let mut inner = self.inner.lock();
        if let Some(StoredCell::Provisional { data, .. }) = inner.cells.get(collection.id()) {
            let data = data.clone();
            inner
                .cells
                .insert(collection.id().clone(), StoredCell::Resolved(data));
        }
        Ok(())
    }
}

impl DescriptorIdentityStore for MemoryCellStore {
    type Error = Infallible;

    async fn read_descriptor_identities(
        &self,
        segment_id: SegmentId,
    ) -> Result<Vec<DurableDescriptorIdentity>, Self::Error> {
        Ok(self
            .inner
            .lock()
            .identities
            .get(&segment_id)
            .cloned()
            .unwrap_or_default())
    }

    async fn write_descriptor_identities(
        &self,
        segment_id: SegmentId,
        rows: Vec<DurableDescriptorIdentity>,
    ) -> Result<(), Self::Error> {
        // Upsert by name, mirroring Cassandra's INSERT-overwrites-row
        // semantics so the shared acquisition tests observe one row per
        // collection on both backends.
        let mut inner = self.inner.lock();
        let segment = inner.identities.entry(segment_id).or_default();
        for row in rows {
            if let Some(existing) = segment.iter_mut().find(|r| r.name == row.name) {
                *existing = row;
            } else {
                segment.push(row);
            }
        }
        Ok(())
    }
}

/// In-memory [`CommittedCache`] for the Value kind.
///
/// A plain map of committed cell values, used by memory-backed tests and the
/// memory production wiring. Infallible — its error is [`Infallible`].
#[derive(Clone, Debug, Default)]
pub struct MemoryCommittedCache {
    inner: Arc<Mutex<HashMap<CollectionId<ValueKind>, Committed, RandomState>>>,
}

impl MemoryCommittedCache {
    /// Creates an empty committed-value cache.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl CommittedCache<ValueKind> for MemoryCommittedCache {
    type Error = Infallible;

    async fn get<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
        (): &'a (),
    ) -> Result<Option<Committed>, Self::Error> {
        Ok(self.inner.lock().get(collection).cloned())
    }

    async fn put<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
        (): &'a (),
        value: &'a Committed,
    ) -> Result<(), Self::Error> {
        self.inner.lock().insert(collection.clone(), value.clone());
        Ok(())
    }

    async fn invalidate<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
        (): &'a (),
    ) -> Result<(), Self::Error> {
        self.inner.lock().remove(collection);
        Ok(())
    }
}

#[derive(Debug, Default)]
struct DirtyInner {
    entries: HashMap<CollectionId<ValueKind>, Option<ValueOp>, RandomState>,
}

#[derive(Debug, Default)]
struct CellInner {
    cells: HashMap<CollectionId<ValueKind>, StoredCell, RandomState>,
    identities: HashMap<SegmentId, Vec<DurableDescriptorIdentity>, RandomState>,
}

/// One stored cell in [`MemoryCellStore`].
#[derive(Clone, Debug)]
enum StoredCell {
    Resolved(Option<Bytes>),
    Provisional {
        data: Option<Bytes>,
        prev: Option<Bytes>,
        event: EventRef,
    },
}

impl StoredCell {
    fn to_cell(&self) -> Cell {
        match self {
            Self::Resolved(data) => Cell::Resolved(Committed::new(data.clone())),
            Self::Provisional { data, prev, event } => {
                Cell::Provisional(ProvisionalCell::new(data.clone(), prev.clone(), *event))
            }
        }
    }
}
