//! Provisional writes paired with the event marker that lists them.

use super::{EventEvidence, EventMarker, SectionClear};
use crate::state::cell::ProvisionalWrite;
use crate::state::cell_key::{CellKey, Section};
use crate::state::event_ref::EventRef;

/// One collection's provisional writes and the event marker frozen from them.
///
/// The marker's staged list holds exactly the write coordinates, and each
/// clear's survivors are exactly the present-data writes in its section.
/// Admission finds provisional cells only through the staged list.
pub(crate) struct FrozenStage {
    writes: Vec<(CellKey, ProvisionalWrite)>,
    marker: EventMarker,
}

impl FrozenStage {
    /// Freezes the marker for `event` from `writes` and the `cleared`
    /// sections. Each clear's survivors are the present-data writes in its
    /// section.
    pub(crate) fn new(
        event: EventRef,
        writes: Vec<(CellKey, ProvisionalWrite)>,
        cleared: &[Section],
        evidence: &EventEvidence,
    ) -> Self {
        let clears = cleared
            .iter()
            .map(|&section| SectionClear::frozen(section, &writes))
            .collect();
        let marker = EventMarker::frozen(event, &writes, clears, evidence);
        Self { writes, marker }
    }

    /// The staged writes.
    pub(crate) fn writes(&self) -> &[(CellKey, ProvisionalWrite)] {
        &self.writes
    }

    /// The marker frozen from the writes.
    pub(crate) fn marker(&self) -> &EventMarker {
        &self.marker
    }

    /// Borrows all writes as one stage request.
    pub(crate) fn request(&self) -> ProvisionalStage<'_> {
        ProvisionalStage {
            marker: &self.marker,
            writes: &self.writes,
        }
    }
}

/// A stage request: provisional writes and an event marker that lists every
/// write.
///
/// [`CellStore::write_provisional`](crate::state::store::CellStore::write_provisional)
/// takes this type, so a stage cannot write a cell that admission cannot find.
/// A split stage sends each chunk of writes with the full marker.
#[derive(Clone, Copy, Debug)]
pub struct ProvisionalStage<'a> {
    marker: &'a EventMarker,
    writes: &'a [(CellKey, ProvisionalWrite)],
}

impl<'a> ProvisionalStage<'a> {
    /// Pairs `writes` with `marker`. Returns `None` when the marker does not
    /// list a write.
    #[cfg(test)]
    pub(crate) fn listed(
        marker: &'a EventMarker,
        writes: &'a [(CellKey, ProvisionalWrite)],
    ) -> Option<Self> {
        writes
            .iter()
            .all(|(cell, _)| marker.staged().binary_search(cell).is_ok())
            .then_some(Self { marker, writes })
    }

    /// The event marker. It lists every write.
    pub(crate) fn marker(self) -> &'a EventMarker {
        self.marker
    }

    /// The writes of this request.
    pub(crate) fn writes(self) -> &'a [(CellKey, ProvisionalWrite)] {
        self.writes
    }
}
