//! Shared property suites for memory, cached memory, and Cassandra cells.
//! Crash traces preserve durable rows and rebuild the store with a cold cache.
//! Admission resolves residue through collection evidence before the next
//! event. Physical probes check marker rows, provisional cells, and committed
//! absence.

use crate::state::cell::{Presence, Projection, Values};
use crate::state::cell_key::CellRef;
use crate::state::marker::ProvisionalStage;
use crate::state::store::{CellBackend, CellRead, Durable};
use crate::state::tests::support::listed;

use super::super::cell::{Committed, ProvisionalCell, ProvisionalWrite};
use super::super::cell_key::{CellKey, Coordinate, Direction, Scan, Section};
use super::super::dirty::DirtyStore;
use super::super::identity::{CollectionId, CollectionRef};
use super::super::marker::{EventMarker, SectionClear};
use super::super::memory::MemoryCells;
use super::super::overlay::Overlay;
use super::super::resolve::{EvidenceLookup, resolve_event_marker};
use super::super::store::{
    CELL_BATCH, CellBuffer, CellStore, CommittedBatch, CoordinateBatch, provisional_point_loop,
};
use super::super::{CommitDecision, EventRef, StateKey, StateName, StateType};
pub(crate) use super::support::MemoryDeduplicationStore;
use super::support::{CountingCellStore, batch_of};
use crate::consumer::middleware::deduplication::DeduplicationStore;
use crate::error::{ClassifyError, ErrorCategory};
use crate::state::cell::Cell::Provisional;
use crate::state::marker::{AttemptId, EventEvidence, MarkerState};
use crate::state::tests::support::{admit_collection, evidence, seed_commit_evidence};
use crate::timers::duration::CompactDuration;
use bytes::Bytes;
use color_eyre::eyre::eyre;
use color_eyre::eyre::{Result, ensure};
use futures::{Stream, StreamExt};
use quickcheck::{Arbitrary, Gen};
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::future::{Future, ready};
use std::iter;
use std::num::NonZeroUsize;
use std::ops::Bound;
use std::slice;
use std::sync::Arc;
use uuid::Uuid;

mod apply;
mod batch;
mod crash;
mod cuts;
mod events;
mod failing;
mod overlay;
mod overwrite;
mod raw;
mod scan;
pub(crate) use apply::{ApplyTrace, run_apply_idempotence};
use batch::{BatchCellState, seed_batch};
pub(crate) use batch::{
    BatchReadTrace, run_batch_alignment, run_batch_duplicate_co_observation,
    run_batch_read_parity_trace,
};
use crash::{collapse_cells, pooled_collections};
pub(crate) use crash::{run_blind_write_leaves_clears_free_marker, run_crash_equivalence_trace};
use cuts::{assert_crash_state, promote_prefix, stage_clock_crash};
pub(crate) use events::Trace;
use events::{Mutation, TraceEvent};
pub(crate) use failing::{FailingCellStore, Poison, PoisonHandle};
use overlay::CellModel;
pub(crate) use overlay::{OverlayTrace, run_overlay_precedence_pin, run_overlay_trace};
pub(crate) use overwrite::{OverwriteTrace, run_overwrite_trace};
use raw::collapse_population;
pub(crate) use raw::{
    RawBatchTrace, run_raw_batch_ascending_output, run_raw_batch_no_side_effects,
    run_raw_batch_parity_trace,
};
use scan::{
    ScanReq, SeedClear, collect_scan, collect_scan_coordinates, scan_oracle, seed_section_clear,
};
pub(crate) use scan::{ScanTrace, run_bottom_scan_trace};

/// Distinct collections a crash/overwrite trace cycles through. Small so events
/// collide on the same cell and exercise overwrite + resolution-on-read.
const POOL: u8 = 3;

/// Cells per collection a crash/overwrite event may stage in **one**
/// `write_provisional` call — the multi-cell same-partition batch. Small so
/// events collide on the same `(collection, cell)`.
const CRASH_CELLS: u8 = 3;

/// Coordinate pool for the multi-cell overlay/scan suites — wide enough for
/// real intervals (a Map entry set / Deque index window).
const CELLS: u8 = 12;

/// Value's single section (`ValueNs::Entries = 0`) — the default section for
/// modules that need only one (`cached_suite`, `fjall`, `session` via
/// [`value_cell`]/[`cell_at`]).
pub(crate) const SECTION: Section = Section::new(0);

/// The sampled section pool the suite generators draw from. Clear markers and
/// durable section clears are **section-scoped**, so every trace samples a
/// small pool of sections rather than hardwiring one — a marker consulted at
/// the wrong section (hiding a live sibling, or serving a cleared section's
/// stale rows) is visible to every property.
pub(crate) const SECTIONS: [Section; 2] = [Section::new(0), Section::new(1)];

/// Upper bound on generated trace lengths, keeping property runs bounded.
pub(crate) const MAX_TRACE_OPS: usize = 40;

/// Canonical single-byte payload (the cell content is opaque to the LWW state
/// machine). Shared by every keyed-state test module.
pub(crate) fn bytes(value: u8) -> Bytes {
    Bytes::from(vec![value])
}

/// Generates an [`Arbitrary`] vector capped at `max` elements, keeping trace
/// lengths bounded.
pub(crate) fn capped_vec<T: Arbitrary>(g: &mut Gen, max: usize) -> Vec<T> {
    Vec::<T>::arbitrary(g).into_iter().take(max).collect()
}

/// The single Value cell (`ValueNs::Entries`, empty coordinate). Shared by
/// every keyed-state test module.
pub(crate) fn value_cell() -> CellKey {
    CellKey {
        section: SECTION,
        coordinate: Coordinate::empty(),
    }
}

/// The cell at coordinate `c` in the shared default section (single byte, so
/// byte order == numeric order — the in-memory oracle keys on `u8`). Shared
/// with `cached_suite`.
pub(crate) fn cell_at(c: u8) -> CellKey {
    CellKey {
        section: SECTION,
        coordinate: Coordinate::from_bytes(vec![c]),
    }
}

/// The cell at `(section index s, coordinate byte c)` over the sampled
/// [`SECTIONS`] pool — how every section-aware generator addresses cells.
pub(crate) fn cell_in(s: u8, c: u8) -> CellKey {
    CellKey {
        section: SECTIONS[s as usize % SECTIONS.len()],
        coordinate: Coordinate::from_bytes(vec![c]),
    }
}

/// Folds a generated byte into a [`SECTIONS`] pool index.
fn section_idx(s: u8) -> u8 {
    s % SECTIONS.len() as u8
}

/// The first coordinate byte of a scanned cell (the suites use single-byte
/// coordinates).
fn coord_of(key: &CellKey) -> u8 {
    key.coordinate.as_bytes()[0]
}

/// The physical `(section, coordinate byte)` row key of a stored cell — the
/// probes' and models' shared comparison currency.
pub(crate) fn row_key(key: &CellKey) -> (i8, u8) {
    (i8::from(key.section), coord_of(key))
}

/// A set of physical `(section, coordinate byte)` row keys ([`row_key`]).
pub(crate) type RowKeys = BTreeSet<(i8, u8)>;

/// A marker payload's clear half: each cleared section mapped to its frozen
/// survivor coordinate bytes.
pub(crate) type ClearMap = BTreeMap<i8, BTreeSet<u8>>;

/// The physically observed unsettled **event marker**: its owning event, its
/// frozen staged row keys, and each cleared section's frozen survivors —
/// exactly what the durable payload carries, in probe-comparable shape.
pub(crate) type ProbedMarker = (EventRef, RowKeys, ClearMap);

/// Converts a decoded [`EventMarker`] into [`ProbedMarker`] form (minus the
/// event, which each probe reads alongside) — shared by the memory and
/// Cassandra probes so both assert the payload's clear half identically.
pub(crate) fn probed_parts(marker: &EventMarker) -> (RowKeys, ClearMap) {
    (
        marker.staged().iter().map(row_key).collect(),
        marker
            .clears()
            .iter()
            .map(|clear| {
                (
                    i8::from(clear.section()),
                    clear
                        .survivors()
                        .iter()
                        .map(|coordinate| coordinate.as_bytes()[0])
                        .collect(),
                )
            })
            .collect(),
    )
}

/// The physical-row-shape oracle for the row-absence invariant.
///
/// Enumerates the physically stored `kind=Cell` rows of a collection over the
/// sampled [`SECTIONS`] pool, as `(section, first coordinate byte)` row keys.
/// At every settled point the stored-row set must equal the model's
/// *present* set — a residue row (an absent value left with live
/// columns/entry) shows up as an extra member, a lost row as a missing one, so
/// exact equality catches both. Probe errors are environment errors
/// (propagated with `?`), never property failures.
///
/// Only [`run_crash_equivalence_trace`], [`run_bottom_scan_trace`], and
/// [`run_apply_idempotence`] take a probe: they drive every physical settle
/// primitive (clean promote, clean abort, crash followed by admission, the
/// direct `write_resolved(None)` clear, and the section-clear gap erase).
/// `run_overlay_trace`/`run_overwrite_trace` add no new physical path — their
/// committed mutations and committed reads go through those same
/// primitives — so hooking them would only add live round-trips.
pub(crate) trait ShapeProbe {
    async fn cell_rows(&self, id: &CollectionId) -> Result<RowKeys>;

    /// The collection's unsettled **event marker** as physically observed —
    /// including the payload's clear half — or `None` when no marker stands.
    /// Read raw, never through the resolving store.
    async fn unsettled_marker(&self, id: &CollectionId) -> Result<Option<ProbedMarker>>;

    /// The row keys whose stored rows are physically **provisional**, read
    /// raw. Together with [`Self::unsettled_marker`] this feeds the
    /// marker-completeness postcondition: a provisional row unlisted by the
    /// unsettled marker is stranded from recovery.
    async fn provisional_rows(&self, id: &CollectionId) -> Result<RowKeys>;
}

/// [`ShapeProbe`] over the memory backend: the store map itself, every entry
/// regardless of variant (so a lingering `Resolved(None)` residue is visible),
/// and the durable marker map (so a leaked or missing marker is visible).
pub(crate) struct MemoryShapeProbe(pub(crate) MemoryCells);

impl ShapeProbe for MemoryShapeProbe {
    fn cell_rows<'a, 'b>(
        &'a self,
        id: &'b CollectionId,
    ) -> impl Future<Output = Result<RowKeys>> + use<'a, 'b> {
        ready(Ok(self
            .0
            .stored_coordinates(id)
            .into_iter()
            .map(|cell| row_key(&cell))
            .collect()))
    }

    fn unsettled_marker<'a, 'b>(
        &'a self,
        id: &'b CollectionId,
    ) -> impl Future<Output = Result<Option<ProbedMarker>>> + use<'a, 'b> {
        ready(Ok(self.0.unsettled_marker_of(id).map(|marker| {
            let (staged, clears) = probed_parts(&marker);
            (marker.event(), staged, clears)
        })))
    }

    fn provisional_rows<'a, 'b>(
        &'a self,
        id: &'b CollectionId,
    ) -> impl Future<Output = Result<RowKeys>> + use<'a, 'b> {
        ready(Ok(self
            .0
            .provisional_coordinates(id)
            .iter()
            .map(row_key)
            .collect()))
    }
}
