//! In-memory keyed-state stores.

use super::cell::{Cell, Committed, ProvisionalCell, ProvisionalWrite};
use super::cell_key::{CellKey, Coordinate, Direction, Scan, Section};
use super::marker::{EventMarker, SectionClear};
use super::oracle::CommitOracle;
use super::registry::CollectionDefRegistry;
use super::resolve::{
    ResolveCellError, Resolver, flatten_resolve, peek_read, resolve_event_marker,
    resolve_prior_clear_before_read, resolve_read, resolve_unsettled_clear_before_write,
};
use super::store::{CellBuffer, CellStore, CoordinateBatch, provisional_point_loop};
use super::{CollectionId, CollectionRef, EventRef};
use async_stream::try_stream;
use bytes::Bytes;
use futures::Stream;
use scc::hash_map::Entry;
use smallvec::SmallVec;
use std::convert::Infallible;
use std::future::{Future, ready};
use std::sync::Arc;
use tokio::task::coop::cooperative;

mod cells;
mod identity;
mod index;
mod publication;

pub use cells::MemoryCells;
use cells::{CellMap, StoredCell};
pub use identity::MemoryDescriptorIdentityStore;
use index::WarmIndex;
pub use publication::MemoryPublicationStore;

/// In-memory, uniform `CellStore` — the in-memory (and mock-mode) backend.
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
    /// The in-RAM provisional-coordinate index gating the recovery sweep — see
    /// [`WarmIndex`] for its seed/re-seed semantics. `Arc` so the per-event
    /// store clones share one instance, minted fresh per store so a re-acquired
    /// partition re-seeds from the process-shared `cells`.
    warm: Arc<WarmIndex>,
}

impl<O> MemoryCellStore<O> {
    /// Wraps a shared cell map, resolving through `oracle` and binding
    /// per-collection TTLs from `registry` on resolution write-backs.
    #[must_use]
    pub(crate) fn new(cells: MemoryCells, oracle: O, registry: Arc<CollectionDefRegistry>) -> Self {
        Self {
            cells,
            resolver: Resolver::new(oracle, registry),
            warm: Arc::default(),
        }
    }
}

impl<O> MemoryCellStore<O>
where
    O: CommitOracle,
{
    /// The raw stored cell at `(collection, cell)`, defaulting a missing row to
    /// `Resolved(Committed(None))`. Delegates to
    /// [`MemoryCells::read_committed_cell`], the oracle-free body shared with
    /// the reader.
    fn read_raw(&self, collection: &CollectionId, cell: &CellKey) -> Cell {
        self.cells.read_committed_cell(collection, cell)
    }

    /// The shared cell map.
    fn map(&self) -> &CellMap {
        &self.cells.inner
    }

    /// Resolves a prior event's section clear before a read.
    ///
    /// Both point reads and scans use this function.
    /// They cannot return data that the clear removed.
    async fn read_help(
        &self,
        collection_ref: &CollectionRef,
        own: EventRef,
    ) -> Result<(), ResolveCellError<Infallible, O::Error>> {
        let marker = self.unsettled_marker(collection_ref.id()).await?;
        // The read starts after this resolution, so a durable change needs no re-read.
        let _ = resolve_prior_clear_before_read(
            self,
            self.resolver.oracle(),
            collection_ref,
            marker.as_ref(),
            own,
        )
        .await
        .map_err(flatten_resolve)?;
        Ok(())
    }

    /// Applies one frozen section clear: removes every stored cell of the
    /// cleared section whose coordinate is not a survivor (positional
    /// exclusion — the frozen list is sorted, so a binary search decides), and
    /// clears each removed coordinate from the [`WarmIndex`]. Erasing a
    /// still-provisional prior event entry is correct (the erasure argument on
    /// [`CellStore::commit_provisional`]); removal is idempotent.
    async fn erase_clear(&self, collection: &CollectionId, clear: &SectionClear) {
        let mut removed: Vec<CellKey> = Vec::new();
        self.map().iter_sync(|(id, cell), _stored| {
            if id == collection
                && cell.section == clear.section()
                && clear.survivors().binary_search(&cell.coordinate).is_err()
            {
                removed.push(cell.clone());
            }
            true
        });
        for cell in removed {
            self.map()
                .remove_async(&(collection.clone(), cell.clone()))
                .await;
            self.warm.clear(collection, &cell).await;
        }
    }

    /// Applies resolved values without marker resolution.
    ///
    /// Callers must resolve required section clears before this function.
    async fn apply_resolved(
        &self,
        collection: &CollectionId,
        cells: &[(CellKey, Option<Bytes>)],
        clears: &[SectionClear],
    ) {
        // Erase the cleared sections before upserting `cells` (belt-and-braces
        // — survivors are excluded positionally anyway).
        for clear in clears {
            self.erase_clear(collection, clear).await;
        }
        for (cell, data) in cells {
            match data {
                // Present value: upsert the resolved cell.
                Some(_) => {
                    self.map()
                        .upsert_async(
                            (collection.clone(), cell.clone()),
                            StoredCell::Resolved(data.clone()),
                        )
                        .await;
                }
                // Absent value: remove the entry (the row-absence invariant —
                // a missing entry already reads `Resolved(Committed(None))` via
                // `read_raw`'s default). Removing an absent key is a no-op.
                None => {
                    self.map()
                        .remove_async(&(collection.clone(), cell.clone()))
                        .await;
                }
            }
            // Rollback/committed-write resolves the cell; drop its provisional
            // coordinate (a no-op for a never-staged direct write).
            self.warm.clear(collection, cell).await;
        }
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
        let collection_ref = self.resolver.collection_ref(collection);
        self.read_help(&collection_ref, own).await?;
        let raw = self.read_raw(collection, cell);
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
        let collection_ref = self.resolver.collection_ref(collection);
        try_stream! {
            // Read-help before the snapshot (see `read_help`), so the snapshot
            // below reads post-clear truth.
            self.read_help(&collection_ref, own).await?;
            // Snapshot the matching raw cells synchronously (scc holds no
            // borrowing iterator across an await), then resolve each lazily.
            let mut raw: Vec<(CellKey, Cell)> = Vec::new();
            self.map().iter_sync(|(id, cell), stored| {
                if id == collection && cell.section == scan.section && scan.contains(&cell.coordinate) {
                    raw.push((cell.clone(), stored.to_cell()));
                }
                true
            });
            raw.sort_by(|(a, _), (b, _)| a.coordinate.cmp(&b.coordinate));
            if scan.dir == Direction::Backward {
                raw.reverse();
            }
            let limit = scan.limit;
            // The limit bounds *yielded* (present) cells, not raw rows: a cleared
            // or rolled-back-to-absent cell in range is skipped without consuming
            // a limit slot (matching the Cassandra scan's `yielded` counter).
            //
            // The resolved fast path touches no tokio leaf, so a large in-memory
            // scan would drain in one poll; a per-item `cooperative` yield point
            // fires every ~128 items.
            let mut yielded = 0usize;
            for (cell, stored) in raw {
                if limit.is_some_and(|n| yielded >= n) {
                    break;
                }
                let committed =
                    cooperative(peek_read(self.resolver.oracle(), &collection_ref, own, stored))
                        .await
                        .map_err(ResolveCellError::Oracle)?;
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
        try_stream! {
            // The coordinates to visit. Warm (seeded): the in-RAM index — an
            // empty snapshot yields nothing and never scans the map. Cold: the
            // one-time full-map seed scan that populates the index and marks the
            // collection seeded, mirroring the Cassandra cold marker seed.
            let coords = if self.warm.is_seeded(collection).await {
                self.warm.snapshot(collection)
            } else {
                let mut coords: Vec<CellKey> = Vec::new();
                self.map().iter_sync(|(id, cell), stored| {
                    if id == collection && matches!(stored, StoredCell::Provisional { .. }) {
                        coords.push(cell.clone());
                    }
                    true
                });
                for cell in &coords {
                    self.warm.record(collection, cell).await;
                }
                self.warm.mark_seeded(collection).await;
                coords
            };
            // Point-read each coordinate; a concurrently-resolved coordinate
            // decodes `Resolved` and is dropped (over-report-safe). The reads
            // touch no tokio leaf, so a per-item `cooperative` yield point
            // releases a large recovery drain to the runtime every ~128 items.
            for cell in coords {
                if let Cell::Provisional(provisional) = self.read_raw(collection, &cell) {
                    yield cooperative(async move { (cell, provisional) }).await;
                }
            }
        }
    }

    fn provisional_cell_at<'a>(
        &'a self,
        collection: &'a CollectionId,
        cell: &'a CellKey,
    ) -> impl Future<Output = Result<Option<ProvisionalCell>, Self::Error>> + Send + 'a {
        ready(Ok(match self.read_raw(collection, cell) {
            Cell::Provisional(provisional) => Some(provisional),
            Cell::Resolved(_) => None,
        }))
    }

    fn provisional_many<'a>(
        &'a self,
        collection: &'a CollectionId,
        section: Section,
        batch: &'a CoordinateBatch,
    ) -> impl Future<Output = Result<CellBuffer<(Coordinate, ProvisionalCell)>, Self::Error>> + Send + 'a
    {
        // No batch query of its own — the raw point-loop reference, reading each
        // distinct coordinate through `provisional_cell_at` in ascending order.
        provisional_point_loop(self, collection, section, batch)
    }

    async fn write_provisional<'a>(
        &'a self,
        collection: &'a CollectionRef,
        writes: &'a [(CellKey, ProvisionalWrite)],
        marker: Option<&'a EventMarker>,
    ) -> Result<(), Self::Error> {
        // `None` ⇒ the explicit empty-stage no-op: no marker and no boundary
        // check (nothing to strand). A clears-only stage passes a marker with
        // empty `staged()` and runs the boundary like any stage.
        debug_assert!(
            marker.is_some() || writes.is_empty(),
            "a markerless stage must write nothing"
        );
        if let Some(marker) = marker {
            debug_assert!(
                writes
                    .iter()
                    .all(|(cell, _)| marker.staged().binary_search(cell).is_ok()),
                "every staged write must be listed by the event marker"
            );
            // Stage boundary: resolve any unsettled prior event marker (a different
            // event) before overwriting it, establishing marker uniqueness per
            // collection. A resolution failure fails the stage.
            if let Some(unsettled) = self
                .cells
                .markers
                .read_async(collection.id(), |_, marker| marker.clone())
                .await
                && unsettled.event() != marker.event()
            {
                resolve_event_marker(self, self.resolver.oracle(), collection, &unsettled)
                    .await
                    .map_err(flatten_resolve)?;
            }
            // Marker-first: order-irrelevant in memory (no mid-call crash), but
            // mirrors the documented stage ordering.
            self.cells
                .markers
                .upsert_async(collection.id().clone(), marker.clone())
                .await;
        }
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
            // Record after the write lands (an in-memory upsert never fails).
            self.warm.record(collection.id(), cell).await;
        }
        Ok(())
    }

    async fn write_resolved<'a>(
        &'a self,
        collection: &'a CollectionRef,
        cells: &'a [(CellKey, Option<Bytes>)],
        clears: &'a [SectionClear],
    ) -> Result<(), Self::Error> {
        // Resolve an unsettled section clear before this write.
        // The clear cannot remove a value that this write adds.
        let marker = self.unsettled_marker(collection.id()).await?;
        resolve_unsettled_clear_before_write(
            self,
            self.resolver.oracle(),
            collection,
            marker.as_ref(),
        )
        .await
        .map_err(flatten_resolve)?;
        self.apply_resolved(collection.id(), cells, clears).await;
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
            // Promote resolves the cell; drop its provisional coordinate
            // (idempotent — clearing an absent coordinate is a no-op).
            self.warm.clear(collection.id(), cell).await;
        }
        Ok(())
    }

    async fn unsettled_marker<'a>(
        &'a self,
        collection: &'a CollectionId,
    ) -> Result<Option<EventMarker>, Self::Error> {
        Ok(self
            .cells
            .markers
            .read_async(collection, |_, marker| marker.clone())
            .await)
    }

    async fn commit_provisional<'a>(
        &'a self,
        collection: &'a CollectionRef,
        writes: &'a [(CellKey, ProvisionalWrite)],
        clears: &'a [SectionClear],
    ) -> Result<(), Self::Error> {
        // Route present-data cells to a promote (`mark_resolved`) and
        // absent-data cells to a row-deleting raw apply (the row-absence
        // invariant), then erase the clears and delete the marker. The raw
        // `apply_resolved` never re-enters the clear resolution boundary — the marker
        // being settled here remains unsettled, so a re-entry would recurse on it.
        // All steps idempotent, and memory has no mid-call crash, so the
        // ordering carries no correctness weight (survivors are excluded from
        // the erase positionally either way).
        let mut keeps: CellBuffer<CellKey> = SmallVec::with_capacity(writes.len());
        let mut absents: CellBuffer<(CellKey, Option<Bytes>)> =
            SmallVec::with_capacity(writes.len());
        for (cell, write) in writes {
            if write.data().is_some() {
                keeps.push(cell.clone());
            } else {
                absents.push((cell.clone(), None));
            }
        }
        if !keeps.is_empty() {
            self.mark_resolved(collection, &keeps).await?;
        }
        if !absents.is_empty() {
            self.apply_resolved(collection.id(), &absents, &[]).await;
        }
        for clear in clears {
            self.erase_clear(collection.id(), clear).await;
        }
        // Settle owns the marker delete; removing an absent marker is a no-op
        // (idempotent settle).
        self.cells.markers.remove_async(collection.id()).await;
        Ok(())
    }

    async fn abort_provisional<'a>(
        &'a self,
        collection: &'a CollectionRef,
        writes: &'a [(CellKey, ProvisionalWrite)],
    ) -> Result<(), Self::Error> {
        // Write each staged cell's committed base `prev` back as resolved
        // (`prev = None` restores exact absence) via the raw apply — which,
        // unlike the trait `write_resolved`, must not re-enter the clear resolution
        // boundary on the marker this abort is deleting — then delete the
        // marker.
        let cells: CellBuffer<(CellKey, Option<Bytes>)> = writes
            .iter()
            .map(|(cell, write)| (cell.clone(), write.prev().cloned()))
            .collect();
        self.apply_resolved(collection.id(), &cells, &[]).await;
        self.cells.markers.remove_async(collection.id()).await;
        Ok(())
    }
}
