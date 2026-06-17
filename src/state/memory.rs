//! In-memory keyed-state stores.

use super::cell::{Cell, Committed, ProvisionalCell, ProvisionalWrite};
use super::descriptor_identity::{DescriptorIdentityStore, DurableDescriptorIdentity};
use super::partition_store::CommittedCache;
use super::store::CellStore;
use super::value::ValueKind;
use super::{CollectionId, CollectionRef, EventRef};
use crate::SegmentId;
use ahash::RandomState;
use bytes::Bytes;
use futures::{Stream, stream};
use scc::hash_map::Entry;
use std::convert::Infallible;
use std::sync::Arc;

/// In-memory [`CellStore`] for the Value kind.
///
/// The provisional-cell durable backend: one cell per collection (Value's
/// `CellAddr` is `()`), each either resolved or provisional, plus the
/// per-segment descriptor-identity rows. One instance is shared
/// process-wide across partition reassignments (`Clone` shares the `Arc`),
/// so committed state and identities survive a rebalance within the process.
#[derive(Clone, Debug, Default)]
pub struct MemoryCellStore {
    inner: Arc<CellInner>,
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
            .cells
            .read_async(collection, |_, cell| cell.to_cell())
            .await
            .unwrap_or_else(|| Cell::Resolved(Committed::new(None))))
    }

    fn provisional_cells<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
    ) -> impl Stream<Item = Result<((), ProvisionalCell), Self::Error>> + Send + 'a {
        let provisional = self
            .inner
            .cells
            .read_sync(collection, |_, cell| match cell {
                StoredCell::Provisional { data, prev, event } => {
                    Some(ProvisionalCell::new(data.clone(), prev.clone(), *event))
                }
                StoredCell::Resolved(_) => None,
            });
        stream::iter(provisional.flatten().map(|cell| Ok(((), cell))))
    }

    async fn write_provisional<'a>(
        &'a self,
        collection: &'a CollectionRef<ValueKind>,
        writes: &'a [((), ProvisionalWrite)],
    ) -> Result<(), Self::Error> {
        // Value is single-cell, so the slice is size-1; the loop is the
        // collection-grain batch a multi-cell kind would issue as one mutation.
        for ((), write) in writes {
            self.inner
                .cells
                .upsert_async(
                    collection.id().clone(),
                    StoredCell::Provisional {
                        data: write.data().cloned(),
                        prev: write.prev().cloned(),
                        event: write.event(),
                    },
                )
                .await;
        }
        Ok(())
    }

    async fn write_resolved<'a>(
        &'a self,
        collection: &'a CollectionRef<ValueKind>,
        cells: &'a [((), Option<Bytes>)],
    ) -> Result<(), Self::Error> {
        for ((), data) in cells {
            self.inner
                .cells
                .upsert_async(collection.id().clone(), StoredCell::Resolved(data.clone()))
                .await;
        }
        Ok(())
    }

    async fn mark_resolved<'a>(
        &'a self,
        collection: &'a CollectionRef<ValueKind>,
        addrs: &'a [()],
    ) -> Result<(), Self::Error> {
        for () in addrs {
            if let Entry::Occupied(mut entry) =
                self.inner.cells.entry_async(collection.id().clone()).await
                && let StoredCell::Provisional { data, .. } = entry.get()
            {
                let data = data.clone();
                *entry.get_mut() = StoredCell::Resolved(data);
            }
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
            .identities
            .read_async(&segment_id, |_, rows| rows.clone())
            .await
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
        let mut entry = self
            .inner
            .identities
            .entry_async(segment_id)
            .await
            .or_default();
        let segment = entry.get_mut();
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
    inner: Arc<scc::HashMap<CollectionId<ValueKind>, Committed, RandomState>>,
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
        Ok(self.inner.read_async(collection, |_, v| v.clone()).await)
    }

    async fn put<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
        (): &'a (),
        value: &'a Committed,
    ) -> Result<(), Self::Error> {
        self.inner
            .upsert_async(collection.clone(), value.clone())
            .await;
        Ok(())
    }

    async fn invalidate<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
        (): &'a (),
    ) -> Result<(), Self::Error> {
        self.inner.remove_async(collection).await;
        Ok(())
    }
}

#[derive(Debug, Default)]
struct CellInner {
    cells: scc::HashMap<CollectionId<ValueKind>, StoredCell, RandomState>,
    identities: scc::HashMap<SegmentId, Vec<DurableDescriptorIdentity>, RandomState>,
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
