//! In-memory keyed-state stores.

use super::cell::{Cell, Committed, ProvisionalCell, ProvisionalWrite};
use super::descriptor_identity::{
    DescriptorIdentityStore, DurableDescriptorIdentity, RegisterOutcome,
};
use super::partition_store::CommittedCache;
use super::store::CellStore;
use super::value::ValueKind;
use super::{CollectionId, CollectionRef, EventRef, StateType};
use ahash::RandomState;
use bytes::Bytes;
use futures::{Stream, stream};
use scc::hash_map::Entry;
use std::convert::Infallible;
use std::sync::Arc;

/// Group-global identity key: `(group_id, state_type discriminator, name)`.
type IdentityKey = (String, i8, String);

/// In-memory [`CellStore`] for the Value kind.
///
/// The provisional-cell durable backend: one cell per collection (Value's
/// `CellAddr` is `()`), each either resolved or provisional. One instance is
/// shared process-wide across partition reassignments (`Clone` shares the
/// `Arc`), so committed state survives a rebalance within the process.
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

/// In-memory group-global [`DescriptorIdentityStore`].
///
/// The control-plane half of in-memory keyed state, decoupled from any kind's
/// cell data. One instance is shared process-wide across partition
/// reassignments (`Clone` shares the `Arc`), so registered identities survive a
/// rebalance within the process — the property `acquire_descriptor_identities`
/// relies on to coalesce cross-partition first-acquires.
#[derive(Clone, Debug, Default)]
pub struct MemoryDescriptorIdentityStore {
    inner: Arc<scc::HashMap<IdentityKey, DurableDescriptorIdentity, RandomState>>,
}

impl MemoryDescriptorIdentityStore {
    /// Creates an empty identity store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl DescriptorIdentityStore for MemoryDescriptorIdentityStore {
    type Error = Infallible;

    async fn read_identity(
        &self,
        group_id: &str,
        state_type: StateType,
        name: &str,
    ) -> Result<Option<DurableDescriptorIdentity>, Self::Error> {
        let key = (group_id.to_owned(), state_type.into(), name.to_owned());
        Ok(self.inner.read_async(&key, |_, row| row.clone()).await)
    }

    async fn register_identity(
        &self,
        group_id: &str,
        row: &DurableDescriptorIdentity,
    ) -> Result<RegisterOutcome, Self::Error> {
        // Atomic insert-if-absent, mirroring Cassandra's `INSERT … IF NOT
        // EXISTS`: a present key yields `Conflict(existing)` so the caller
        // validates without a re-read.
        let key = (group_id.to_owned(), row.state_type, row.name.clone());
        match self.inner.entry_async(key).await {
            Entry::Vacant(slot) => {
                slot.insert_entry(row.clone());
                Ok(RegisterOutcome::Applied)
            }
            Entry::Occupied(existing) => Ok(RegisterOutcome::Conflict(existing.get().clone())),
        }
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
