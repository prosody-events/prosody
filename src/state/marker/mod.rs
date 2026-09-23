//! A collection stores two marker rows. `Staged` lists the residue: the
//! provisional cells that an interrupted settle leaves behind. `Committed`
//! carries positive commit evidence. Only a promote writes evidence.
//!
//! The frozen payload lists staged coordinates, clear survivors, and touched
//! collections. Version 2 adds touched collections, the shared evidence TTL,
//! the dedup id, and the stage id.
//! The row's version column selects the format; the payload has no version
//! byte.
//!
//! Staged uses the collection TTL. Committed uses the finite dedup TTL.
//!
//! The stage write captures the staged list and survivor lists; admission never
//! derives them again. These lists contain coordinates, never values.
//! The event marker is distinct from the dedup commit marker and the in-RAM
//! dirty clear marker. Always use the qualified name.

use super::cell::ProvisionalWrite;
use super::cell_key::{CellKey, Coordinate, Section};
use super::event_ref::EventRef;
use super::identity::{StateName, StateType};
use crate::state::cell_key::CellRef;
use crate::timers::duration::CompactDuration;
use bytes::Bytes;
use std::sync::Arc;
use uuid::Uuid;

mod payload;
mod stage;
#[cfg(test)]
mod tests;

pub use payload::MarkerPayloadError;
pub(in crate::state) use payload::{
    decode_marker_payload, encode_committed_payload, encode_marker_payload,
};
pub(crate) use stage::FrozenStage;
pub use stage::ProvisionalStage;

/// Identifies one stage across all collections of one settle.
/// A Committed row certifies a Staged row exactly when their stage ids match.
/// The event and touched list do not select this decision.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct StageId(Uuid);

impl StageId {
    pub(crate) fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

/// The two addresses in a collection's marker slice.
///
/// A Committed row certifies residue through its [`StageId`].
/// Only a promote writes this row, before any destructive promote chunk.
#[derive(Clone, Copy, Debug)]
pub(crate) enum MarkerRow {
    Staged,
    Committed,
}

impl MarkerRow {
    /// The Staged address is the marker address that prosody 0.6.0 writes, so
    /// its residue decodes in place. Committed takes the next coordinate.
    pub(crate) fn coordinate(self) -> &'static [u8] {
        match self {
            Self::Staged => &[],
            Self::Committed => &[1],
        }
    }
}

/// One durable read of a collection's marker slice.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MarkerState {
    pub(crate) staged: Option<EventMarker>,
    pub(crate) committed: Option<CommittedMarker>,
}

/// Commit evidence retains the discovery path after Staged disappears.
/// This value cannot carry staged cells or section clears.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CommittedMarker {
    pub(crate) stage: StageId,
    pub(crate) event: EventRef,
    pub(crate) dedup: Option<Uuid>,
    pub(crate) touched: Arc<[(StateType, StateName)]>,
}

impl CommittedMarker {
    pub(crate) fn certifies(&self, marker: &EventMarker) -> bool {
        self.stage == marker.stage()
    }
}

impl From<&EventMarker> for CommittedMarker {
    fn from(marker: &EventMarker) -> Self {
        Self {
            stage: marker.stage(),
            event: marker.event(),
            dedup: marker.dedup(),
            touched: marker.inner.touched.clone(),
        }
    }
}

/// Evidence retained for one external read or scan.
/// The local committed event and evidence for the staged event can both exist,
/// so the flag does not select an alternative state.
#[derive(Default)]
pub(crate) struct ReaderEvidence {
    /// The collection's own Staged and Committed rows.
    pub(crate) state: MarkerState,
    /// A sibling collection holds Committed for the same stage.
    pub(crate) staged_committed: bool,
}

impl ReaderEvidence {
    pub(crate) fn committed(&self, event: EventRef) -> bool {
        self.state.staged.as_ref().is_some_and(|staged| {
            staged.event() == event
                && (self.staged_committed
                    || self
                        .state
                        .committed
                        .as_ref()
                        .is_some_and(|marker| marker.certifies(staged)))
        })
    }

    pub(crate) fn survives(&self, cell: CellRef<'_>) -> bool {
        self.state.staged.as_ref().is_none_or(|marker| {
            !self.committed(marker.event())
                || marker
                    .clears()
                    .iter()
                    .filter(|clear| clear.section() == cell.section)
                    .all(|clear| {
                        clear
                            .survivors()
                            .binary_search_by(|coordinate| {
                                coordinate.as_bytes().cmp(cell.coordinate)
                            })
                            .is_ok()
                    })
        })
    }
}

/// The format selected by the staged row's version column.
/// A future payload version must ship with a new `SegmentVersion`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MarkerVersion {
    V1,
    V2,
}

impl From<MarkerVersion> for i32 {
    fn from(version: MarkerVersion) -> Self {
        match version {
            MarkerVersion::V1 => 1,
            MarkerVersion::V2 => 2,
        }
    }
}

impl TryFrom<i32> for MarkerVersion {
    type Error = MarkerPayloadError;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::V1),
            2 => Ok(Self::V2),
            _ => Err(MarkerPayloadError::Version(value)),
        }
    }
}

/// One cleared section paired with its **frozen survivor list**: the
/// coordinates that outlive the clear (the section's post-clear `Set` cells).
///
/// The survivor list is derived once, at stage time, by
/// [`SectionClear::frozen`] and thereafter replayed verbatim — never recomputed
/// from whatever cells are still provisional at resolve time. A repeated
/// promote therefore applies the same clear.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SectionClear {
    section: Section,
    survivors: Vec<Coordinate>,
}

impl SectionClear {
    /// Freezes the ascending coordinates of `section`'s staged cells with
    /// present data. The session's `finalize` derives survivors from its
    /// staged record. The stage stores them unchanged; settle and admission
    /// replay them unchanged.
    #[must_use]
    pub(in crate::state) fn frozen(
        section: Section,
        staged: &[(CellKey, ProvisionalWrite)],
    ) -> Self {
        Self::from_survivors(
            section,
            staged
                .iter()
                .filter(|(cell, write)| cell.section == section && write.data().is_some())
                .map(|(cell, _)| cell.coordinate.clone())
                .collect(),
        )
    }

    /// [`Self::frozen`]'s resolved-shape twin for the direct-apply paths
    /// (`ReadUncommitted` finalize, the mid-handler `commit()`): survivors are
    /// the section's present-data resolved cells. Shares the survivor
    /// definition with `frozen` — only the input shape differs.
    #[must_use]
    pub(in crate::state) fn frozen_resolved(
        section: Section,
        cells: &[(CellKey, Option<Bytes>)],
    ) -> Self {
        Self::from_survivors(
            section,
            cells
                .iter()
                .filter(|(cell, data)| cell.section == section && data.is_some())
                .map(|(cell, _)| cell.coordinate.clone())
                .collect(),
        )
    }

    /// The shared survivor-definition tail: ascending, deduped.
    fn from_survivors(section: Section, mut survivors: Vec<Coordinate>) -> Self {
        sort_distinct(&mut survivors);
        Self { section, survivors }
    }

    /// The cleared section.
    #[must_use]
    pub fn section(&self) -> Section {
        self.section
    }

    /// The frozen survivor coordinates, ascending.
    #[must_use]
    pub fn survivors(&self) -> &[Coordinate] {
        &self.survivors
    }
}

/// The unsettled event marker for one collection: the owning event, its full
/// staged coordinate set, and each cleared section's frozen survivors.
///
/// See the module docs for the invariants it carries. Only the state module
/// constructs one: the stage path freezes it, and admission decodes it from
/// its Staged row. Clones share one immutable payload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EventMarker {
    inner: Arc<EventMarkerData>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct EventMarkerData {
    version: MarkerVersion,
    stage: StageId,
    event: EventRef,
    staged: Vec<CellKey>,
    clears: Vec<SectionClear>,
    /// The event's dirty `ReadCommitted` collections at stage time, sorted and
    /// unique. A collection that stages nothing stays listed; a reader
    /// finds no Committed row there and moves on.
    touched: Arc<[(StateType, StateName)]>,
    evidence_ttl: CompactDuration,
    dedup: Option<Uuid>,
}

impl EventMarker {
    /// Freezes the marker for `event` from its staged cells and cleared
    /// sections. The staged list is sorted by `(section, coordinate)` and
    /// distinct, as the decoder makes it, so a payload round trip is exact.
    #[must_use]
    pub(in crate::state) fn frozen(
        event: EventRef,
        staged: &[(CellKey, ProvisionalWrite)],
        clears: Vec<SectionClear>,
        evidence: &EventEvidence,
    ) -> Self {
        let mut coordinates: Vec<CellKey> = staged.iter().map(|(cell, _)| cell.clone()).collect();
        sort_distinct(&mut coordinates);
        Self::from_parts(EventMarkerData {
            version: MarkerVersion::V2,
            stage: evidence.stage,
            event,
            staged: coordinates,
            clears,
            touched: Arc::clone(&evidence.touched),
            evidence_ttl: evidence.evidence_ttl,
            dedup: evidence.dedup,
        })
    }

    fn from_parts(data: EventMarkerData) -> Self {
        Self {
            inner: Arc::new(data),
        }
    }

    pub(crate) fn stage(&self) -> StageId {
        self.inner.stage
    }

    /// The durable format selects the commit rule for old residue.
    pub(crate) fn version(&self) -> MarkerVersion {
        self.inner.version
    }

    /// Supplies current retention before admission promotes a legacy stage.
    pub(crate) fn for_admission(&self, dedup_ttl: CompactDuration) -> Self {
        if self.version() == MarkerVersion::V1 {
            Self::from_parts(EventMarkerData {
                evidence_ttl: dedup_ttl,
                ..(*self.inner).clone()
            })
        } else {
            self.clone()
        }
    }

    /// The dedup row to record after the promote.
    pub(crate) fn dedup(&self) -> Option<Uuid> {
        self.inner.dedup
    }

    /// The owning event.
    #[must_use]
    pub fn event(&self) -> EventRef {
        self.inner.event
    }

    /// The event's full staged coordinate set, ascending.
    #[must_use]
    pub fn staged(&self) -> &[CellKey] {
        &self.inner.staged
    }

    /// Each cleared section with its frozen survivors.
    #[must_use]
    pub fn clears(&self) -> &[SectionClear] {
        &self.inner.clears
    }

    /// The event's touched `ReadCommitted` collections, sorted and unique.
    pub(crate) fn touched(&self) -> &[(StateType, StateName)] {
        &self.inner.touched
    }

    /// The finite retention for committed evidence.
    pub(crate) fn evidence_ttl(&self) -> CompactDuration {
        self.inner.evidence_ttl
    }
}

/// Fields shared by all collection stages of one event.
/// Committed evidence uses the finite dedup TTL, regardless of collection TTLs.
/// Interrupted settlement leaves the source uncommitted, so redelivery runs
/// admission. The dedup TTL bounds that redelivery window.
/// Evidence precedes the dedup row and expires no later than that row.
/// Thus retained evidence without a dedup row requires recovery of the dedup
/// record.
pub(crate) struct EventEvidence {
    pub(crate) stage: StageId,
    pub(crate) touched: Arc<[(StateType, StateName)]>,
    pub(crate) evidence_ttl: CompactDuration,
    pub(crate) dedup: Option<Uuid>,
}

/// Sorts `items` ascending and removes duplicates. A sorted input costs one
/// linear pass.
fn sort_distinct<T: Ord>(items: &mut Vec<T>) {
    items.sort_unstable();
    items.dedup();
}
