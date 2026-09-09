//! Process-shared in-memory cells and committed reader projections.

use crate::state::cell::{Cell, Committed, ProvisionalCell, resolve_for_reader};
use crate::state::cell_key::{CellKey, Direction, Scan, Section};
#[cfg(test)]
use crate::state::marker::EventMarker;
use crate::state::marker::{MarkerState, ReaderEvidence};
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
/// This map is the memory store itself, not a memo beside a durable store.
/// The memory backend never expires cells or marker entries.
/// Abort removes entries without evidence; other entries live until the store
/// drops.
type MarkerMap = scc::HashMap<CollectionId, MarkerState, RandomState>;

/// A process-shared in-memory cell map.
///
/// Cells and unsettled event markers survive partition reassignment within the
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

    pub(crate) fn marker_state(&self, collection: &CollectionId) -> MarkerState {
        self.markers
            .read_sync(collection, |_, state| state.clone())
            .unwrap_or_default()
    }

    fn reader_evidence(&self, collection: &CollectionId) -> ReaderEvidence {
        let state = self.marker_state(collection);
        let staged_committed = state.staged.as_ref().is_some_and(|marker| {
            marker.touched().iter().any(|(state_type, name)| {
                if *state_type == collection.state_type() && name == collection.name() {
                    return false;
                }
                let id =
                    CollectionId::new(collection.state_key().clone(), *state_type, name.clone());
                self.marker_state(&id)
                    .committed
                    .as_ref()
                    .is_some_and(|evidence| evidence.certifies(marker))
            })
        });
        ReaderEvidence {
            state,
            staged_committed,
        }
    }

    pub(crate) fn read_committed(
        &self,
        collection: &CollectionId,
        cell: &CellKey,
    ) -> Option<Bytes> {
        let cell = self.read_committed_cell(collection, cell);
        let evidence = if matches!(cell, Cell::Provisional(_)) {
            self.reader_evidence(collection)
        } else {
            ReaderEvidence::default()
        };
        resolve_for_reader(&cell, &evidence).cloned()
    }

    pub(crate) fn read_committed_many(
        &self,
        collection: &CollectionId,
        section: Section,
        batch: &CoordinateBatch,
    ) -> CellBuffer<Option<Bytes>> {
        let cells: CellBuffer<Cell> = batch
            .iter()
            .map(|coordinate| {
                self.read_committed_cell(
                    collection,
                    &CellKey {
                        section,
                        coordinate: coordinate.clone(),
                    },
                )
            })
            .collect();
        let evidence = if cells
            .iter()
            .any(|cell| matches!(cell, Cell::Provisional(_)))
        {
            self.reader_evidence(collection)
        } else {
            ReaderEvidence::default()
        };
        cells
            .iter()
            .map(|cell| resolve_for_reader(cell, &evidence).cloned())
            .collect()
    }

    pub(crate) fn scan_committed<'a>(
        &'a self,
        collection: &'a CollectionId,
        scan: Scan<'a>,
    ) -> impl Stream<Item = Result<(CellKey, Bytes), Infallible>> + Send + 'a {
        try_stream! {
            let evidence = self.reader_evidence(collection);
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
                if !evidence.survives(&cell) { continue; }
                if let Some(bytes) =
                    cooperative(async { resolve_for_reader(&stored, &evidence).cloned() }).await
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
    pub(crate) fn unsettled_marker_of(&self, collection: &CollectionId) -> Option<EventMarker> {
        self.markers
            .read_sync(collection, |_, marker| marker.staged.clone())
            .flatten()
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
