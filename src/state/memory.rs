//! In-memory keyed-state stores.

use super::cell::{Cell, Committed, ProvisionalCell, ProvisionalWrite};
use super::cell_key::{CellKey, Direction, Scan};
use super::descriptor_identity::{
    DescriptorIdentityStore, DurableDescriptorIdentity, RegisterOutcome,
};
use super::oracle::CommitOracle;
use super::registry::CollectionDefRegistry;
use super::resolve::{ResolveCellError, Resolver, flatten_resolve, resolve_read};
use super::store::CellStore;
use super::{CollectionId, CollectionRef, EventRef, StateType};
use ahash::RandomState;
use async_stream::try_stream;
use bytes::Bytes;
use futures::Stream;
use scc::hash_map::Entry;
use std::convert::Infallible;
use std::sync::Arc;

/// Group-global identity key: `(group_id, state_type discriminator, name)`.
type IdentityKey = (String, i8, String);

/// A process-wide shareable in-memory cell map.
///
/// The oracle-independent half of [`MemoryCellStore`]: the cells themselves,
/// keyed by `(CollectionId, CellKey)`. One instance is shared across partition
/// assignments (`Clone` shares the `Arc`), so committed state survives a
/// rebalance within the process — each partition wraps it in a fresh
/// [`MemoryCellStore`] carrying that partition's oracle.
#[derive(Clone, Debug, Default)]
pub struct MemoryCells {
    inner: Arc<CellInner>,
}

impl MemoryCells {
    /// Creates an empty shared cell map.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

/// In-memory, uniform [`CellStore`] — the mock/test bottom store.
///
/// The provisional-cell durable backend keyed by `(CollectionId, CellKey)`:
/// each cell is either resolved or provisional. Resolution of in-flight
/// provisional cells funnels through the composed `Resolver` — the same
/// oracle/registry the production store uses — so `get`/`scan_cells` return
/// resolved [`Committed`] cells exactly as Cassandra does.
#[derive(Clone, Debug)]
pub struct MemoryCellStore<O> {
    cells: MemoryCells,
    resolver: Resolver<O>,
}

impl<O> MemoryCellStore<O> {
    /// Wraps a shared cell map, resolving through `oracle` and binding
    /// per-collection TTLs from `registry` on resolution write-backs.
    #[must_use]
    pub fn new(cells: MemoryCells, oracle: O, registry: Arc<CollectionDefRegistry>) -> Self {
        Self {
            cells,
            resolver: Resolver::new(oracle, registry),
        }
    }
}

impl<O> MemoryCellStore<O>
where
    O: CommitOracle,
{
    /// The raw stored cell at `(collection, cell)`, defaulting a missing row to
    /// `Resolved(Committed(None))`.
    fn read_raw(&self, collection: &CollectionId, cell: &CellKey) -> Cell {
        self.map()
            .read_sync(&(collection.clone(), cell.clone()), |_, stored| {
                stored.to_cell()
            })
            .unwrap_or_else(|| Cell::Resolved(Committed::new(None)))
    }

    /// The shared cell map.
    fn map(&self) -> &scc::HashMap<(CollectionId, CellKey), StoredCell, RandomState> {
        &self.cells.inner.cells
    }
}

impl<O> CellStore for MemoryCellStore<O>
where
    O: CommitOracle,
{
    type Error = ResolveCellError<Infallible, O::Error>;

    async fn get<'a>(
        &'a self,
        collection: &'a CollectionId,
        cell: &'a CellKey,
        own: EventRef,
    ) -> Result<Committed, Self::Error> {
        let raw = self.read_raw(collection, cell);
        let collection_ref = self.resolver.collection_ref(collection);
        resolve_read(
            self,
            self.resolver.oracle(),
            &collection_ref,
            cell,
            own,
            raw,
        )
        .await
        .map_err(flatten_resolve)
    }

    fn scan_cells<'a>(
        &'a self,
        collection: &'a CollectionId,
        scan: Scan<'a>,
        own: EventRef,
    ) -> impl Stream<Item = Result<(CellKey, Bytes), Self::Error>> + Send + 'a {
        // Snapshot the matching raw cells synchronously (scc holds no borrowing
        // iterator across an await), then resolve each lazily.
        let mut raw: Vec<(CellKey, Cell)> = Vec::new();
        self.map().iter_sync(|(id, cell), stored| {
            if id == collection && cell.section == scan.section && in_range(&scan, &cell.coordinate)
            {
                raw.push((cell.clone(), stored.to_cell()));
            }
            true
        });
        raw.sort_by(|(a, _), (b, _)| a.coordinate.cmp(&b.coordinate));
        if scan.dir == Direction::Backward {
            raw.reverse();
        }
        let limit = scan.limit;
        let collection_ref = self.resolver.collection_ref(collection);
        try_stream! {
            // The limit bounds *yielded* (present) cells, not raw rows: a cleared
            // or rolled-back-to-absent cell in range is skipped without consuming
            // a limit slot (matching the Cassandra scan's `yielded` counter).
            let mut yielded = 0usize;
            for (cell, stored) in raw {
                if limit.is_some_and(|n| yielded >= n) {
                    break;
                }
                let committed =
                    resolve_read(self, self.resolver.oracle(), &collection_ref, &cell, own, stored)
                        .await
                        .map_err(flatten_resolve)?;
                if let Some(bytes) = committed.into_inner() {
                    yield (cell, bytes);
                    yielded += 1;
                }
            }
        }
    }

    fn provisional_cells<'a>(
        &'a self,
        collection: &'a CollectionId,
    ) -> impl Stream<Item = Result<(CellKey, ProvisionalCell), Self::Error>> + Send + 'a {
        let mut provisional: Vec<(CellKey, ProvisionalCell)> = Vec::new();
        self.map().iter_sync(|(id, cell), stored| {
            if id == collection
                && let StoredCell::Provisional { data, prev, event } = stored
            {
                provisional.push((
                    cell.clone(),
                    ProvisionalCell::new(data.clone(), prev.clone(), *event),
                ));
            }
            true
        });
        try_stream! {
            for item in provisional {
                yield item;
            }
        }
    }

    async fn write_provisional<'a>(
        &'a self,
        collection: &'a CollectionRef,
        writes: &'a [(CellKey, ProvisionalWrite)],
    ) -> Result<(), Self::Error> {
        for (cell, write) in writes {
            self.map()
                .upsert_async(
                    (collection.id().clone(), cell.clone()),
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
        collection: &'a CollectionRef,
        cells: &'a [(CellKey, Option<Bytes>)],
    ) -> Result<(), Self::Error> {
        for (cell, data) in cells {
            self.map()
                .upsert_async(
                    (collection.id().clone(), cell.clone()),
                    StoredCell::Resolved(data.clone()),
                )
                .await;
        }
        Ok(())
    }

    async fn mark_resolved<'a>(
        &'a self,
        collection: &'a CollectionRef,
        cells: &'a [CellKey],
    ) -> Result<(), Self::Error> {
        for cell in cells {
            if let Entry::Occupied(mut entry) = self
                .map()
                .entry_async((collection.id().clone(), cell.clone()))
                .await
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
/// The control-plane half of in-memory keyed state, decoupled from any cell
/// data. One instance is shared process-wide across partition reassignments
/// (`Clone` shares the `Arc`), so registered identities survive a rebalance
/// within the process — the property `acquire_descriptor_identities` relies on
/// to coalesce cross-partition first-acquires.
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

#[derive(Debug, Default)]
struct CellInner {
    cells: scc::HashMap<(CollectionId, CellKey), StoredCell, RandomState>,
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

/// Whether `coordinate` lies in `scan`'s inclusive range, accounting for
/// direction: forward walks `>= start` (`<= end`), backward `<= start`
/// (`>= end`).
fn in_range(scan: &Scan<'_>, coordinate: &super::cell_key::Coordinate) -> bool {
    match scan.dir {
        Direction::Forward => {
            coordinate >= scan.start && scan.end.is_none_or(|end| coordinate <= end)
        }
        Direction::Backward => {
            coordinate <= scan.start && scan.end.is_none_or(|end| coordinate >= end)
        }
    }
}
