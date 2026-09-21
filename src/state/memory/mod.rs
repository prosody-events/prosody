//! In-memory keyed-state stores.

use super::cell::{Cell, Committed, Projection, ProvisionalCell, ProvisionalWrite};
use super::cell_key::{CellKey, Coordinate, Direction, Scan, Section};
use super::marker::{EventMarker, MarkerState, SectionClear};
use super::resolve::{EvidenceLookup, ResolveCellError};
use super::store::{
    CacheBatch, CellBackend, CellBuffer, CellRead, CellStore, CoordinateBatch, Durable, dedupe,
    expand_to_input_order, provisional_point_loop,
};
use super::{CollectionId, CollectionRef};
use async_stream::try_stream;
use bytes::Bytes;
use futures::Stream;
use scc::hash_map::Entry;
use smallvec::SmallVec;
use std::convert::Infallible;
use std::future::{Future, ready};
use tokio::task::coop::cooperative;

mod cells;
mod identity;
mod publication;

pub use cells::MemoryCells;
use cells::{CellMap, StoredCell};
pub use identity::MemoryDescriptorIdentityStore;
pub use publication::MemoryPublicationStore;

/// The in-memory cell store. Admission resolves residue before owner reads.
#[derive(Clone, Debug)]
pub struct MemoryCellStore {
    cells: MemoryCells,
}

impl MemoryCellStore {
    /// Wraps the shared durable cells.
    #[must_use]
    pub(crate) fn new(cells: MemoryCells) -> Self {
        Self { cells }
    }

    /// Returns the raw cell through [`MemoryCells::read_committed_cell`].
    /// A missing row represents committed absence.
    fn read_raw(&self, collection: &CollectionId, cell: &CellKey) -> Cell {
        self.cells.read_committed_cell(collection, cell)
    }

    /// The shared cell map.
    fn map(&self) -> &CellMap {
        &self.cells.inner
    }

    /// Deletes cells outside the frozen survivors of a section clear.
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
        }
    }
}

impl CellBackend for MemoryCellStore {
    type Error = ResolveCellError<Infallible>;
}

impl<P: Projection> CellRead<P> for MemoryCellStore {
    async fn read<'a>(
        &'a self,
        collection: &'a CollectionId,
        cell: &'a CellKey,
    ) -> Result<Durable<P>, Self::Error> {
        let committed = EvidenceLookup::new(self, collection)
            .resolve(self.read_raw(collection, cell))
            .await?;
        Ok((
            Committed::new(committed.into_inner().map(P::from_value)),
            None,
        ))
    }

    async fn read_many<'a>(
        &'a self,
        collection: &'a CollectionId,
        section: Section,
        batch: &'a CoordinateBatch,
    ) -> Result<CacheBatch<P>, Self::Error> {
        let (coordinates, indices) = dedupe(batch);
        let mut answers = CacheBatch::<P>::with_capacity(coordinates.len());
        let mut lookup = EvidenceLookup::new(self, collection);
        for coordinate in coordinates {
            let cell = CellKey {
                section,
                coordinate: coordinate.clone(),
            };
            let committed = cooperative(lookup.resolve(self.read_raw(collection, &cell))).await?;
            answers.push((
                Committed::new(committed.into_inner().map(P::from_value)),
                None,
            ));
        }
        Ok(expand_to_input_order(&indices, &answers))
    }

    fn scan<'a>(
        &'a self,
        collection: &'a CollectionId,
        scan: Scan<'a>,
    ) -> impl Stream<Item = Result<(CellKey, P::Payload), Self::Error>> + Send + use<'a, P> {
        try_stream! {
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
            // The resolved fast path touches no tokio leaf, so a large in-memory
            // scan would drain in one poll; a per-item `cooperative` yield point
            // fires every ~128 items.
            let mut lookup = EvidenceLookup::new(self, collection);
            for (cell, stored) in raw {
                let committed =
                    cooperative(lookup.resolve(stored)).await?;
                if let Some(bytes) = committed.into_inner() {
                    yield (cell, P::from_value(bytes));
                }
            }
        }
    }
}

impl CellStore for MemoryCellStore {
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
            // Marker-first: order-irrelevant in memory (no mid-call crash), but
            // mirrors the documented stage ordering.
            self.cells
                .markers
                .entry_async(collection.id().clone())
                .await
                .or_default()
                .get_mut()
                .staged = Some(marker.clone());
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
        }
        Ok(())
    }

    async fn write_resolved<'a>(
        &'a self,
        collection: &'a CollectionRef,
        cells: &'a [(CellKey, Option<Bytes>)],
        clears: &'a [SectionClear],
    ) -> Result<(), Self::Error> {
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
        }
        Ok(())
    }

    async fn marker_state<'a>(
        &'a self,
        collection: &'a CollectionId,
    ) -> Result<MarkerState, Self::Error> {
        Ok(self
            .cells
            .markers
            .read_async(collection, |_, state| state.clone())
            .await
            .unwrap_or_default())
    }

    async fn commit_provisional<'a>(
        &'a self,
        collection: &'a CollectionRef,
        marker: &'a EventMarker,
        writes: &'a [(CellKey, ProvisionalWrite)],
    ) -> Result<(), Self::Error> {
        self.cells
            .markers
            .entry_async(collection.id().clone())
            .await
            .or_default()
            .get_mut()
            .committed = Some(marker.into());
        let clears = marker.clears();
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
        self.cells
            .markers
            .entry_async(collection.id().clone())
            .await
            .or_default()
            .get_mut()
            .staged = None;
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
        if let Some(mut entry) = self.cells.markers.get_async(collection.id()).await {
            entry.get_mut().staged = None;
            if entry.get().committed.is_none() {
                let _ = entry.remove();
            }
        }
        Ok(())
    }
}
