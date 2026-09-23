//! Process-shared in-memory cells and committed reader projections.

use crate::state::cell::{Cell, Committed, Projection, ProvisionalCell, resolve_for_reader};
use crate::state::cell_key::{CellKey, CellRef, Direction, Scan, Section};
#[cfg(test)]
use crate::state::marker::EventMarker;
use crate::state::marker::{MarkerState, ReaderEvidence};
use crate::state::store::{CellBuffer, ReadBatch};
use crate::state::{CollectionId, EventRef};
use ahash::RandomState;
use async_stream::try_stream;
use bytes::Bytes;
use futures::Stream;
use std::convert::Infallible;
use std::sync::Arc;
use tokio::task::coop::cooperative;

pub(super) type CellMap = scc::HashMap<(CollectionId, CellKey), StoredCell, RandomState>;
/// A borrowed key for [`CellMap`] lookups.
///
/// Its derived `Hash` must equal the hash of the owned
/// `(CollectionId, CellKey)` key. It does, because `CellKey` hashes through
/// its `CellRef`.
#[derive(Hash)]
struct CellLookup<'a>(&'a CollectionId, CellRef<'a>);

impl scc::Equivalent<(CollectionId, CellKey)> for CellLookup<'_> {
    fn equivalent(&self, key: &(CollectionId, CellKey)) -> bool {
        self.0 == &key.0 && self.1 == key.1.as_ref()
    }
}

/// This map is the memory store itself.
/// The test and mock store has no clock. It retains Committed evidence for its
/// lifetime. This is the only retention that a clockless store can express.
/// Abort removes entries without evidence.
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

    pub(super) fn read_committed_cell(&self, collection: &CollectionId, cell: CellRef<'_>) -> Cell {
        self.inner
            .read_sync(&CellLookup(collection, cell), |_, stored| stored.to_cell())
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
        cell: CellRef<'_>,
    ) -> Option<Bytes> {
        let evidence = self.reader_evidence(collection);
        if !evidence.survives(cell) {
            return None;
        }
        resolve_for_reader(&self.read_committed_cell(collection, cell), &evidence).cloned()
    }

    pub(crate) fn read_committed_many<P: Projection>(
        &self,
        collection: &CollectionId,
        section: Section,
        batch: &ReadBatch<'_>,
    ) -> CellBuffer<Option<P::Payload>> {
        let evidence = self.reader_evidence(collection);
        batch
            .iter()
            .map(|coordinate| {
                let key = CellRef {
                    section,
                    coordinate,
                };
                if !evidence.survives(key) {
                    return None;
                }
                resolve_for_reader(&self.read_committed_cell(collection, key), &evidence)
                    .cloned()
                    .map(P::from_value)
            })
            .collect()
    }

    pub(crate) fn scan_committed<'a>(
        &'a self,
        collection: &'a CollectionId,
        scan: Scan<'a>,
    ) -> impl Stream<Item = Result<(CellKey, Bytes), Infallible>> + Send + use<'a> {
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
            for (cell, stored) in raw {
                if !evidence.survives(cell.as_ref()) { continue; }
                if let Some(bytes) =
                    cooperative(async { resolve_for_reader(&stored, &evidence).cloned() }).await
                {
                    yield (cell, bytes);
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
