//! Process-shared in-memory cells and committed reader projections.

use crate::state::cell::{Cell, Committed, ProvisionalCell};
use crate::state::cell_key::{CellKey, Direction, Scan, Section};
use crate::state::marker::EventMarker;
use crate::state::store::{CellBuffer, CoordinateBatch};
use crate::state::{CollectionId, EventRef};
use ahash::RandomState;
use async_stream::try_stream;
use bytes::Bytes;
use futures::Stream;
use std::convert::Infallible;
use std::sync::Arc;
use tokio::task::coop::cooperative;

pub(super) type CellMap = scc::HashMap<(CollectionId, CellKey), StoredCell, RandomState>;
type MarkerMap = scc::HashMap<CollectionId, EventMarker, RandomState>;

/// A process-shared in-memory cell map.
///
/// Cells and standing event markers survive partition reassignment within the
/// process because clones share these maps.
#[derive(Clone, Debug, Default)]
pub struct MemoryCells {
    pub(super) inner: Arc<CellMap>,
    pub(super) markers: Arc<MarkerMap>,
}

impl MemoryCells {
    /// Creates an empty shared cell map.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub(super) fn read_committed_cell(&self, collection: &CollectionId, cell: &CellKey) -> Cell {
        self.inner
            .read_sync(&(collection.clone(), cell.clone()), |_, stored| {
                stored.to_cell()
            })
            .unwrap_or_else(|| Cell::Resolved(Committed::new(None)))
    }

    pub(crate) fn read_committed(
        &self,
        collection: &CollectionId,
        cell: &CellKey,
    ) -> Option<Bytes> {
        self.read_committed_cell(collection, cell)
            .project_committed()
            .cloned()
    }

    pub(crate) fn read_committed_many(
        &self,
        collection: &CollectionId,
        section: Section,
        batch: &CoordinateBatch,
    ) -> CellBuffer<Option<Bytes>> {
        batch
            .iter()
            .map(|coordinate| {
                self.read_committed(
                    collection,
                    &CellKey {
                        section,
                        coordinate: coordinate.clone(),
                    },
                )
            })
            .collect()
    }

    pub(crate) fn scan_committed<'a>(
        &'a self,
        collection: &'a CollectionId,
        scan: Scan<'a>,
    ) -> impl Stream<Item = Result<(CellKey, Bytes), Infallible>> + Send + 'a {
        try_stream! {
            let mut raw: Vec<(CellKey, Cell)> = Vec::new();
            self.inner.iter_sync(|(id, cell), stored| {
                if id == collection
                    && cell.section == scan.section
                    && scan.contains(&cell.coordinate)
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
            let mut yielded = 0usize;
            for (cell, stored) in raw {
                if limit.is_some_and(|n| yielded >= n) {
                    break;
                }
                if let Some(bytes) =
                    cooperative(async move { stored.project_committed().cloned() }).await
                {
                    yield (cell, bytes);
                    yielded += 1;
                }
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn stored_coordinates(&self, collection: &CollectionId) -> Vec<CellKey> {
        let mut out = Vec::new();
        self.inner.iter_sync(|(id, cell), _stored| {
            if id == collection {
                out.push(cell.clone());
            }
            true
        });
        out
    }

    #[cfg(test)]
    pub(crate) fn provisional_coordinates(&self, collection: &CollectionId) -> Vec<CellKey> {
        let mut out = Vec::new();
        self.inner.iter_sync(|(id, cell), stored| {
            if id == collection && matches!(stored, StoredCell::Provisional { .. }) {
                out.push(cell.clone());
            }
            true
        });
        out
    }

    #[cfg(test)]
    pub(crate) fn standing_marker_of(&self, collection: &CollectionId) -> Option<EventMarker> {
        self.markers
            .read_sync(collection, |_, marker| marker.clone())
    }
}

#[derive(Clone, Debug)]
pub(super) enum StoredCell {
    Resolved(Option<Bytes>),
    Provisional {
        data: Option<Bytes>,
        prev: Option<Bytes>,
        event: EventRef,
    },
}

impl StoredCell {
    pub(super) fn to_cell(&self) -> Cell {
        match self {
            Self::Resolved(data) => Cell::Resolved(Committed::new(data.clone())),
            Self::Provisional { data, prev, event } => {
                Cell::Provisional(ProvisionalCell::new(data.clone(), prev.clone(), *event))
            }
        }
    }
}
