//! Backend-generic property suite for the uniform cell store.
//!
//! Every runner is generic over the [`CellStore`] backend and takes a
//! `make_store` closure (or a pre-built lower store) so memory
//! ([`Overlay<MemoryCellStore>`]) and Cassandra
//! ([`Overlay<Cached<CassandraStore>>`]) prove the *same* invariants from one
//! body — backend parity by transitivity through a deliberately simple model.
//!
//! The flagship is **crash-recovery equivalence**: a generated
//! trace stages provisional writes and resolves them one of five ways — clean
//! promote, clean inline rollback, or a crash at one of three points followed
//! by recovery through the sweep *or* first-touch. After every event the
//! committed projection must equal the model, whichever path ran. Companions
//! pin implicit overwrite (resolution-on-read); the **unified overlay view**
//! (`run_overlay_trace`), where point `get`s, range `scan`s, dirty buffering,
//! and committed writes are **intermixed** in one trace so their interaction is
//! exercised — dirty-wins, clear-hides, scan bounds / direction / limit /
//! early-stop (unified-view soundness and oracle-correctness); the bottom-store
//! scan primitive; and sweep idempotence.
//!
//! Faithfulness to production:
//!
//! * **A "crash" is `make_store()` again over the same warm backing** — the
//!   durable rows and the oracle's committed set survive; a fresh store starts
//!   with a cold in-process cache, exactly as after a restart. Nothing is ever
//!   leaked or forgotten to fake a crash (the CLAUDE.md memory rule).
//! * **`get`/`scan` resolve in the backend, so they MUTATE a provisional
//!   cell.** Provisional state is therefore observed only through
//!   `provisional_cells`; committed seeds for the overlay/scan suites are
//!   written **resolved** (`write_resolved`, no event) so reads stay pure
//!   dirty-over-committed.
//! * **`prev` always comes from `store.get`**, never minted — the staged `prev`
//!   is the resolved committed base, the same path `finalize` uses.

use super::super::cell::{Committed, ProvisionalCell, ProvisionalWrite};
use super::super::cell_key::{CellKey, Coordinate, Direction, Scan, ScanEdge, Section};
use super::super::dirty::DirtyStore;
use super::super::identity::{CollectionId, CollectionRef};
use super::super::marker::{EventMarker, SectionClear};
use super::super::memory::MemoryCells;
use super::super::oracle::CommitOracle;
use super::super::overlay::Overlay;
use super::super::resolve::sweep_provisional;
use super::super::store::CellStore;
use super::super::{CommitDecision, EventRef, StateKey, StateName, StateType};
use crate::error::{ClassifyError, ErrorCategory};
use ahash::RandomState;
use bytes::Bytes;
use color_eyre::eyre::Result;
use futures::{Stream, StreamExt};
use quickcheck::{Arbitrary, Gen};
use std::collections::{BTreeMap, BTreeSet};
use std::convert::Infallible;
use std::error::Error;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use uuid::Uuid;

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

/// Value's single section (`ValueNs::Entries = 0`); the overlay/scan suites
/// place every cell here and address by coordinate, mirroring a Map's entry
/// section. Shared with `cached_suite`.
pub(crate) const SECTION: Section = Section::new(0);

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

/// The cell at coordinate `c` in the shared section (single byte, so byte order
/// == numeric order — the in-memory oracle keys on `u8`). Shared with
/// `cached_suite`.
pub(crate) fn cell_at(c: u8) -> CellKey {
    CellKey {
        section: SECTION,
        coordinate: Coordinate::from_bytes(vec![c]),
    }
}

/// The first coordinate byte of a scanned cell (the suites use single-byte
/// coordinates).
fn coord_of(key: &CellKey) -> u8 {
    key.coordinate.as_bytes()[0]
}

/// A committed-marker oracle backed by an in-memory set of dedup ids.
///
/// Models the durable deduplication store: `record_message` writes the marker,
/// and `resolve` answers `Committed` iff the staged event's marker is present.
/// The set is shared across clones, so it survives the simulated crash exactly
/// as the durable dedup store does.
#[derive(Clone, Default)]
pub(crate) struct ScriptedOracle {
    committed: Arc<scc::HashSet<Uuid, RandomState>>,
}

impl ScriptedOracle {
    /// Whether `record_message` has durably recorded this dedup id.
    pub(crate) async fn is_recorded(&self, dedup_id: Uuid) -> bool {
        self.committed.contains_async(&dedup_id).await
    }

    /// The number of markers recorded — pins "flushed exactly once".
    pub(crate) fn recorded_count(&self) -> usize {
        self.committed.len()
    }
}

impl CommitOracle for ScriptedOracle {
    type Error = Infallible;

    async fn record_message(&self, dedup_id: Uuid) -> Result<(), Self::Error> {
        // `insert_async` returns `Err(key)` if already present — harmless; the
        // marker is idempotent.
        let _ = self.committed.insert_async(dedup_id).await;
        Ok(())
    }

    async fn resolve<'a>(
        &'a self,
        _state_key: &'a StateKey,
        event: EventRef,
    ) -> Result<CommitDecision, Self::Error> {
        let committed = match event {
            EventRef::Message { dedup_id } => self.committed.contains_async(&dedup_id).await,
            EventRef::Timer(_) => false,
        };
        Ok(if committed {
            CommitDecision::Committed
        } else {
            CommitDecision::NotCommitted
        })
    }
}

/// The physical-row-shape oracle for the row-absence invariant.
///
/// Enumerates the physically stored `kind=Cell` rows of a collection's shared
/// test section, as their first coordinate byte (the suites use single-byte
/// coordinates, matching [`cell_at`]). At any settled checkpoint the stored-row
/// set must equal the model's *present* set — a residue row (an absent value
/// left with live columns/entry) shows up as an extra member, a lost row as a
/// missing one, so exact equality catches both. Probe errors are environment
/// errors (propagated with `?`), never property failures.
///
/// Only [`run_crash_equivalence_trace`] and [`run_bottom_scan_trace`] take a
/// probe: they drive every physical settle primitive (clean promote, clean
/// abort, crash→sweep, crash→first-touch, and the direct `write_resolved(None)`
/// clear). `run_overlay_trace`/`run_overwrite_trace` add no new physical path —
/// their committed mutations and first-touch resolutions go through those same
/// primitives — so hooking them would only add live round-trips.
pub(crate) trait ShapeProbe {
    async fn cell_rows(&self, id: &CollectionId) -> Result<BTreeSet<u8>>;

    /// The collection's standing **event marker** as physically observed, or
    /// [`MarkerObservation::Untracked`] on a backend that cannot yet answer
    /// (the Cassandra interim, which writes no marker rows). An `Untracked`
    /// answer makes the runner skip the marker check explicitly, never
    /// vacuously pass a wrong `None`.
    async fn standing_marker(&self, id: &CollectionId) -> Result<MarkerObservation>;
}

/// What a [`ShapeProbe`] physically sees for a collection's event marker.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MarkerObservation {
    /// The backend cannot answer (the Cassandra interim) — skip the check.
    Untracked,

    /// The observed marker's owning event, or `None` when no marker stands.
    Marker(Option<EventRef>),
}

/// [`ShapeProbe`] over the memory backend: the store map itself, every entry
/// regardless of variant (so a lingering `Resolved(None)` residue is visible),
/// and the durable marker map (so a leaked or missing marker is visible).
pub(crate) struct MemoryShapeProbe(pub(crate) MemoryCells);

impl ShapeProbe for MemoryShapeProbe {
    async fn cell_rows(&self, id: &CollectionId) -> Result<BTreeSet<u8>> {
        Ok(self
            .0
            .stored_coordinates(id)
            .into_iter()
            .map(|cell| coord_of(&cell))
            .collect())
    }

    async fn standing_marker(&self, id: &CollectionId) -> Result<MarkerObservation> {
        Ok(MarkerObservation::Marker(
            self.0.standing_marker_of(id).map(|marker| marker.event()),
        ))
    }
}

// ─────────────────────────── crash-recovery equivalence
// ───────────────────────

/// One Value mutation staged by an event.
#[derive(Clone, Copy, Debug)]
enum Mutation {
    Set(u8),
    Clear,
}

impl Mutation {
    /// The staged outcome (`Set` → present bytes, `Clear` → known-absent).
    fn value(self) -> Option<Bytes> {
        match self {
            Self::Set(b) => Some(bytes(b)),
            Self::Clear => None,
        }
    }
}

impl Arbitrary for Mutation {
    fn arbitrary(g: &mut Gen) -> Self {
        if bool::arbitrary(g) {
            Self::Set(u8::arbitrary(g))
        } else {
            Self::Clear
        }
    }
}

/// How an event resolved — the five distinct cell outcomes.
#[derive(Clone, Copy, Debug)]
enum Outcome {
    /// Committed and promoted inline (the hot path).
    CleanCommitted,
    /// Staged then rolled back inline before any marker flush (abandon).
    CleanRolledBack,
    /// All cells staged, marker never flushed, crash → recover (rolls back).
    CrashAfterStage,
    /// All cells staged, marker flushed, crash → recover (promotes).
    CrashAfterMarker,
    /// Only a prefix staged, marker never flushed, crash → recover.
    CrashMidFanOut,
}

impl Outcome {
    fn marker_flushed(self) -> bool {
        matches!(self, Self::CleanCommitted | Self::CrashAfterMarker)
    }

    fn mid_fan_out(self) -> bool {
        matches!(self, Self::CrashMidFanOut)
    }

    fn is_crash(self) -> bool {
        matches!(
            self,
            Self::CrashAfterStage | Self::CrashAfterMarker | Self::CrashMidFanOut
        )
    }
}

impl Arbitrary for Outcome {
    fn arbitrary(g: &mut Gen) -> Self {
        g.choose(&[
            Self::CleanCommitted,
            Self::CleanRolledBack,
            Self::CrashAfterStage,
            Self::CrashAfterMarker,
            Self::CrashMidFanOut,
        ])
        .copied()
        .unwrap_or(Self::CleanCommitted)
    }
}

/// One event: a flat list of `(collection, cell, mutation)` writes, an outcome,
/// and the recovery path to use when the outcome is a crash. The flat list is
/// grouped by collection ([`collapse_writes`]) so each touched collection
/// stages **all** its cells in one `write_provisional` call (the multi-cell
/// batch).
#[derive(Clone, Debug)]
struct TraceEvent {
    writes: Vec<(u8, u8, Mutation)>,
    outcome: Outcome,
    recover_by_sweep: bool,
}

impl Arbitrary for TraceEvent {
    fn arbitrary(g: &mut Gen) -> Self {
        let writes = capped_vec::<(u8, u8, Mutation)>(g, (POOL * CRASH_CELLS) as usize)
            .into_iter()
            .map(|(coll, cell, m)| (coll % POOL, cell % CRASH_CELLS, m))
            .collect();
        Self {
            writes,
            outcome: Outcome::arbitrary(g),
            recover_by_sweep: bool::arbitrary(g),
        }
    }
}

/// A shrinkable trace of events over the key pool.
#[derive(Clone, Debug)]
pub(crate) struct Trace {
    events: Vec<TraceEvent>,
}

impl Arbitrary for Trace {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            events: capped_vec(g, MAX_TRACE_OPS),
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        // Shortening the trace is the highest-value reduction.
        Box::new(self.events.shrink().map(|events| Self { events }))
    }
}

/// Groups an event's flat `(collection, cell, mutation)` writes by collection,
/// preserving first-seen collection order, and within each collection collapses
/// repeats of a cell to the last mutation (last-writer-wins). The result is the
/// per-collection cell set each `write_provisional` call stages atomically.
fn collapse_writes(writes: Vec<(u8, u8, Mutation)>) -> Vec<(u8, Vec<(u8, Mutation)>)> {
    let mut out: Vec<(u8, Vec<(u8, Mutation)>)> = Vec::new();
    for (coll, cell, mutation) in writes {
        if let Some((_, cells)) = out.iter_mut().find(|(c, _)| *c == coll) {
            collapse_cell_into(cells, cell, mutation);
        } else {
            out.push((coll, vec![(cell, mutation)]));
        }
    }
    out
}

/// Inserts `(cell, mutation)` into a collection's cell set, overwriting any
/// existing mutation for that cell (last-writer-wins) and preserving order.
fn collapse_cell_into(cells: &mut Vec<(u8, Mutation)>, cell: u8, mutation: Mutation) {
    match cells.iter_mut().find(|(c, _)| *c == cell) {
        Some(slot) => slot.1 = mutation,
        None => cells.push((cell, mutation)),
    }
}

/// Collapses one collection's cell writes to last-writer-wins per cell,
/// preserving first-seen order (an event stages each cell at most once).
fn collapse_cells(cells: Vec<(u8, Mutation)>) -> Vec<(u8, Mutation)> {
    let mut out: Vec<(u8, Mutation)> = Vec::new();
    for (cell, mutation) in cells {
        collapse_cell_into(&mut out, cell, mutation);
    }
    out
}

/// The collection ids / refs / pool for a crash or overwrite trace.
fn pooled_collections() -> Result<(Vec<CollectionId>, Vec<CollectionRef>)> {
    let state_key = StateKey::new(Uuid::new_v4(), Arc::from("key"));
    let ids: Vec<CollectionId> = (0..POOL)
        .map(|c| {
            Ok(CollectionId::new(
                state_key.clone(),
                StateType::Application,
                StateName::try_new(format!("c{c}"))?,
            ))
        })
        .collect::<Result<_>>()?;
    let refs = ids
        .iter()
        .map(|id| CollectionRef::new(id.clone(), None))
        .collect();
    Ok((ids, refs))
}

/// Asserts every pooled collection is fully resolved (no provisional lingers)
/// and each modelled cell projects its committed value.
async fn assert_converged<S>(
    store: &S,
    ids: &[CollectionId],
    model: &[BTreeMap<u8, Option<Bytes>>],
) -> Result<bool>
where
    S: CellStore,
{
    // A probe event distinct from every trace event so own-event never
    // short-circuits.
    let probe = EventRef::Message {
        dedup_id: Uuid::from_u128(u128::MAX / 2),
    };
    for (id, expected) in ids.iter().zip(model) {
        // Check raw provisional state FIRST — `get` would resolve a lingering
        // provisional cell and mask a non-convergence.
        if provisional_count(store, id).await? != 0 {
            return Ok(false);
        }
        for (&coord, value) in expected {
            let committed = store.get(id, &cell_at(coord), probe).await?;
            if committed.into_inner() != *value {
                return Ok(false);
            }
        }
    }
    Ok(true)
}

/// The number of still-provisional cells in a collection (the public,
/// non-resolving way to observe staged state).
async fn provisional_count<S>(store: &S, id: &CollectionId) -> Result<usize>
where
    S: CellStore,
{
    let stream = store.provisional_cells(id);
    futures::pin_mut!(stream);
    let mut count = 0usize;
    while let Some(item) = stream.next().await {
        item?;
        count += 1;
    }
    Ok(count)
}

/// Drives `trace` through stores built by `make_store`, asserting the committed
/// projection equals the model after every event regardless of the resolution
/// path. A crash rebuilds the store over the same warm backing the closure
/// captures.
pub(crate) async fn run_crash_equivalence_trace<S, F, P>(
    make_store: F,
    oracle: ScriptedOracle,
    trace: Trace,
    probe: &P,
) -> Result<bool>
where
    S: CellStore,
    F: Fn() -> Result<S>,
    P: ShapeProbe,
{
    let (ids, refs) = pooled_collections()?;
    let mut store = make_store()?;
    let mut model: Vec<BTreeMap<u8, Option<Bytes>>> = vec![BTreeMap::new(); POOL as usize];
    // The standing event marker per collection, as the owning event's dedup
    // payload (`None` ⇒ no marker stands). Set on stage; cleared by a clean
    // settle or a crash→sweep; **kept** by a crash→first-touch (first-touch is
    // marker-free — the over-report the model pins); replaced by the next stage
    // (boundary overwrite).
    let mut marker_model: Vec<Option<u128>> = vec![None; POOL as usize];

    for (index, ev) in trace.events.into_iter().enumerate() {
        let dedup_id = Uuid::from_u128(index as u128);
        let event = EventRef::Message { dedup_id };

        let writes = collapse_writes(ev.writes);
        // Mid-fan-out tears at CELL granularity: a cell is atomic — its
        // `kind=Cell` row and its `kind=Index` marker land in one batch or not
        // at all — so a crash mid-stage leaves a whole-cell prefix, never a torn
        // cell and never one row without the other. Model it by dropping the
        // last touched collection's final cell (the cell whose batch never
        // committed), then dropping any collection left empty.
        let mut staged = writes;
        if ev.outcome.mid_fan_out() {
            if let Some((_, cells)) = staged.last_mut() {
                cells.pop();
            }
            staged.retain(|(_, cells)| !cells.is_empty());
        }
        let staged = staged.as_slice();

        // Stage each collection's whole cell set in ONE `write_provisional` call
        // over its committed base (prev-is-committed at stage time: no
        // provisional cell lingers between events). Keep the staged
        // `(cell, ProvisionalWrite)` set per collection so the clean arms settle
        // through `commit_provisional` / `abort_provisional` in one call too
        // (carrying the projection the write-through cache publishes).
        let mut staged_writes: Vec<(u8, Vec<(CellKey, ProvisionalWrite)>)> =
            Vec::with_capacity(staged.len());
        for (coll, cells) in staged {
            let mut cell_writes: Vec<(CellKey, ProvisionalWrite)> = Vec::with_capacity(cells.len());
            for &(coord, mutation) in cells {
                let key = cell_at(coord);
                let prev = store.get(&ids[*coll as usize], &key, event).await?;
                let prev_value = prev.get().cloned();
                if prev_value != model[*coll as usize].get(&coord).cloned().flatten() {
                    return Ok(false);
                }
                cell_writes.push((key, ProvisionalWrite::new(mutation.value(), prev, event)));
            }
            store
                .write_provisional(&refs[*coll as usize], &cell_writes, &[], &cell_writes)
                .await?;
            // The stage writes (overwriting any lingering foreign marker) this
            // event's marker for the collection.
            marker_model[*coll as usize] = Some(index as u128);
            staged_writes.push((*coll, cell_writes));
        }

        // Marker strictly after staging; advance the model for committed cells.
        if ev.outcome.marker_flushed() {
            oracle.record_message(dedup_id).await?;
            for (coll, cells) in staged {
                for &(coord, mutation) in cells {
                    model[*coll as usize].insert(coord, mutation.value());
                }
            }
        }

        // Resolve along the outcome's path.
        if ev.outcome.is_crash() {
            // Crash = a cold store over the same warm backing.
            store = make_store()?;
            if ev.recover_by_sweep {
                for r in &refs {
                    if !sweep_provisional(&store, &oracle, r).await? {
                        return Ok(false);
                    }
                }
                // The sweep's marker leg deletes every collection's standing
                // marker (including any lingering from an earlier first-touch).
                for marker in &mut marker_model {
                    *marker = None;
                }
            } else {
                // First-touch under a fresh event so own-event never
                // short-circuits; the read resolves each staged provisional cell.
                // First-touch is marker-free, so each staged collection's marker
                // (set above) lingers — the over-report the model keeps.
                let recovery_event = EventRef::Message {
                    dedup_id: Uuid::from_u128(u128::MAX - index as u128),
                };
                for (coll, cells) in staged {
                    for &(coord, _) in cells {
                        store
                            .get(&ids[*coll as usize], &cell_at(coord), recovery_event)
                            .await?;
                    }
                }
            }
        } else if matches!(ev.outcome, Outcome::CleanCommitted) {
            // Promote through the lifecycle settle path (publishes `data`); the
            // settle deletes the collection's marker.
            for (coll, cell_writes) in &staged_writes {
                store
                    .commit_provisional(&refs[*coll as usize], cell_writes, &[])
                    .await?;
                marker_model[*coll as usize] = None;
            }
        } else {
            // The only remaining non-crash outcome: clean inline rollback
            // through the settle path (publishes `prev`); the settle deletes the
            // marker.
            for (coll, cell_writes) in &staged_writes {
                store
                    .abort_provisional(&refs[*coll as usize], cell_writes)
                    .await?;
                marker_model[*coll as usize] = None;
            }
        }

        if !assert_converged(&store, &ids, &model).await? {
            return Ok(false);
        }

        // Physical-shape oracle: the stored `kind=Cell` rows must equal the
        // model's present set exactly — a committed-absent cell is row absence
        // (no residue row), a committed value is one row. Rides every settle path
        // the trace generates.
        for (id, expected) in ids.iter().zip(&model) {
            let present: BTreeSet<u8> = expected
                .iter()
                .filter_map(|(&coord, value)| value.is_some().then_some(coord))
                .collect();
            if probe.cell_rows(id).await? != present {
                return Ok(false);
            }
        }

        // Marker-shape oracle: the standing event marker must match the model
        // exactly — absent when settled or swept, the owning event when the
        // stage lingers (first-touch recovery). `Untracked` (Cassandra interim)
        // skips the check rather than passing a wrong `None`.
        for (slot, id) in ids.iter().enumerate() {
            if let MarkerObservation::Marker(observed) = probe.standing_marker(id).await? {
                let expected = marker_model[slot].map(|dedup_id| EventRef::Message {
                    dedup_id: Uuid::from_u128(dedup_id),
                });
                if observed != expected {
                    return Ok(false);
                }
            }
        }
    }

    Ok(true)
}

// ─────────────────────────── implicit overwrite
// ───────────────────────────────

/// One overwrite-trace step: a multi-cell write to a pooled collection (all
/// cells staged in one `write_provisional` call), and whether the event
/// commits. An empty cell set exercises the empty-batch no-op boundary.
#[derive(Clone, Debug)]
struct OverwriteOp {
    coll: u8,
    cells: Vec<(u8, Mutation)>,
    commit: bool,
}

impl Arbitrary for OverwriteOp {
    fn arbitrary(g: &mut Gen) -> Self {
        let cells = capped_vec::<(u8, Mutation)>(g, CRASH_CELLS as usize)
            .into_iter()
            .map(|(cell, m)| (cell % CRASH_CELLS, m))
            .collect();
        Self {
            coll: u8::arbitrary(g) % POOL,
            cells,
            commit: bool::arbitrary(g),
        }
    }
}

/// A shrinkable overwrite trace.
#[derive(Clone, Debug)]
pub(crate) struct OverwriteTrace {
    ops: Vec<OverwriteOp>,
}

impl Arbitrary for OverwriteTrace {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            ops: capped_vec(g, MAX_TRACE_OPS),
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        Box::new(self.ops.shrink().map(|ops| Self { ops }))
    }
}

/// Drives events that NEVER promote or roll back explicitly: each event reads
/// its committed base through a **cold** store (`make_store`), so a
/// predecessor's still-provisional cell is resolved through the oracle on read
/// — the implicit-overwrite / first-touch path. The staged `prev` must equal
/// the model committed base, and at the end every cell, resolved only by the
/// next read, must equal the model. Both oracle arms run: a committing
/// predecessor promotes to its `data`, a non-committing one rolls back to its
/// `prev`.
pub(crate) async fn run_overwrite_trace<S, F>(
    make_store: F,
    oracle: ScriptedOracle,
    trace: OverwriteTrace,
) -> Result<bool>
where
    S: CellStore,
    F: Fn() -> Result<S>,
{
    let (ids, refs) = pooled_collections()?;
    let mut model: Vec<BTreeMap<u8, Option<Bytes>>> = vec![BTreeMap::new(); POOL as usize];

    for (index, op) in trace.ops.into_iter().enumerate() {
        let slot = op.coll as usize;
        let dedup_id = Uuid::from_u128(index as u128);
        let event = EventRef::Message { dedup_id };
        // A fresh cold store: reads never hit a warm in-process cache, so every
        // overwrite resolves its predecessor's provisional cell durably.
        let store = make_store()?;

        // The whole cell set stages in one `write_provisional`; each cell's
        // staged `prev` must equal its committed base.
        let cells = collapse_cells(op.cells);
        let mut cell_writes: Vec<(CellKey, ProvisionalWrite)> = Vec::with_capacity(cells.len());
        for &(coord, mutation) in &cells {
            let key = cell_at(coord);
            let prev = store.get(&ids[slot], &key, event).await?;
            if prev.get().cloned() != model[slot].get(&coord).cloned().flatten() {
                return Ok(false);
            }
            cell_writes.push((key, ProvisionalWrite::new(mutation.value(), prev, event)));
        }
        store
            .write_provisional(&refs[slot], &cell_writes, &[], &cell_writes)
            .await?;
        if op.commit {
            oracle.record_message(dedup_id).await?;
            for &(coord, mutation) in &cells {
                model[slot].insert(coord, mutation.value());
            }
        }
    }

    // Every last provisional cell, resolved only by this final read, converges.
    let store = make_store()?;
    for (i, id) in ids.iter().enumerate() {
        let final_event = EventRef::Message {
            dedup_id: Uuid::from_u128(u128::MAX - i as u128),
        };
        for (&coord, value) in &model[i] {
            if store
                .get(id, &cell_at(coord), final_event)
                .await?
                .into_inner()
                != *value
            {
                return Ok(false);
            }
        }
    }
    Ok(true)
}

// ─────────────────────────── overlay point merge
// ──────────────────────────────

/// One op on a multi-cell collection view, intermixing point reads, range
/// scans, dirty buffering, and committed writes so the property exercises their
/// interaction — a `scan` between a `buffer_set` and a `clear`, a `get` after a
/// dropped scan, and so on (TESTING.md "interleaved operations").
#[derive(Clone, Copy, Debug)]
enum OverlayOp {
    /// Buffer a set into the dirty leg.
    BufferSet(u8, u8),
    /// Buffer a clear into the dirty leg.
    BufferClear(u8),
    /// Commit a present value to the committed lower store (resolved).
    CommitSet(u8, u8),
    /// Commit a known-absent value to the lower store (resolved).
    CommitClear(u8),
    /// Run a range scan and assert it against the oracle (the range leg,
    /// intermixed with the point reads asserted after every op).
    Scan(ScanReq),
}

impl Arbitrary for OverlayOp {
    fn arbitrary(g: &mut Gen) -> Self {
        let c = u8::arbitrary(g) % CELLS;
        match u8::arbitrary(g) % 5 {
            0 => Self::BufferSet(c, u8::arbitrary(g)),
            1 => Self::BufferClear(c),
            2 => Self::CommitSet(c, u8::arbitrary(g)),
            3 => Self::CommitClear(c),
            _ => Self::Scan(ScanReq::arbitrary(g)),
        }
    }
}

/// A shrinkable overlay trace.
#[derive(Clone, Debug)]
pub(crate) struct OverlayTrace {
    ops: Vec<OverlayOp>,
}

impl Arbitrary for OverlayTrace {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            ops: capped_vec(g, MAX_TRACE_OPS),
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        Box::new(self.ops.shrink().map(|ops| Self { ops }))
    }
}

/// The visible-value model: dirty wins (`Set`→present, `Cleared`→absent), else
/// the committed value (present or absent).
#[derive(Default)]
struct CellModel {
    committed: BTreeMap<u8, Option<Bytes>>,
    dirty: BTreeMap<u8, Option<Bytes>>,
}

impl CellModel {
    /// The visible committed bytes for coordinate `c`.
    fn visible(&self, c: u8) -> Option<Bytes> {
        match self.dirty.get(&c) {
            Some(value) => value.clone(),
            None => self.committed.get(&c).cloned().flatten(),
        }
    }

    /// The visible cells in coordinate order (only present values).
    fn visible_ordered(&self) -> Vec<(u8, Bytes)> {
        let mut coords: Vec<u8> = self
            .committed
            .keys()
            .chain(self.dirty.keys())
            .copied()
            .collect();
        coords.sort_unstable();
        coords.dedup();
        coords
            .into_iter()
            .filter_map(|c| self.visible(c).map(|b| (c, b)))
            .collect()
    }
}

/// Drives random ops over an [`Overlay`] of a multi-cell collection — dirty
/// buffering, committed writes, and range scans **intermixed** — asserting both
/// the range leg (each `Scan` op vs the sorted-map oracle, incl. early-stop)
/// and the point leg (`get` per cell vs the dirty-over-committed oracle, after
/// **every** op). This is the unified view property: point reads, range reads,
/// and writes interleave so their interaction is exercised, not just each in
/// isolation (dirty-wins, clear-hides, bounds, direction, limit —
/// unified-view soundness and oracle-correctness properties).
pub(crate) async fn run_overlay_trace<S>(lower: S, trace: OverlayTrace) -> Result<bool>
where
    S: CellStore,
{
    let state_key = StateKey::new(Uuid::new_v4(), Arc::from("key"));
    let id = CollectionId::new(
        state_key,
        StateType::Application,
        StateName::try_new("entries")?,
    );
    let collection_ref = CollectionRef::new(id.clone(), None);
    let own = EventRef::Message {
        dedup_id: Uuid::from_u128(1),
    };
    let overlay = Overlay::new(Arc::new(DirtyStore::new()), lower);
    let mut model = CellModel::default();

    for op in trace.ops {
        match op {
            OverlayOp::BufferSet(c, b) => {
                overlay.dirty().set(&id, &cell_at(c), &bytes(b));
                model.dirty.insert(c, Some(bytes(b)));
            }
            OverlayOp::BufferClear(c) => {
                overlay.dirty().clear(&id, &cell_at(c));
                model.dirty.insert(c, None);
            }
            OverlayOp::CommitSet(c, b) => {
                // Committed seeds are written resolved (no event) so reads stay
                // pure dirty-over-committed.
                overlay
                    .lower()
                    .write_resolved(&collection_ref, &[(cell_at(c), Some(bytes(b)))], &[])
                    .await?;
                model.committed.insert(c, Some(bytes(b)));
            }
            OverlayOp::CommitClear(c) => {
                overlay
                    .lower()
                    .write_resolved(&collection_ref, &[(cell_at(c), None)], &[])
                    .await?;
                model.committed.insert(c, None);
            }
            OverlayOp::Scan(req) => {
                let expected = scan_oracle(&model, req);
                if let Some(k) = req.partial {
                    // Early stop: the k-prefix matches the oracle prefix, then a
                    // follow-up full scan still yields the complete result
                    // (dropping a scan mid-stream corrupts nothing).
                    let k = (k as usize).min(expected.len());
                    if collect_scan(&overlay, &id, &req, own, Some(k)).await? != expected[..k] {
                        return Ok(false);
                    }
                }
                if collect_scan(&overlay, &id, &req, own, None).await? != expected {
                    return Ok(false);
                }
            }
        }

        // Point leg: after every op (mutation OR scan), every cell's `get`
        // matches the model — so point reads interleave with the scans above.
        for c in 0..CELLS {
            if overlay.get(&id, &cell_at(c), own).await?.into_inner() != model.visible(c) {
                return Ok(false);
            }
        }
    }
    Ok(true)
}

// ─────────────────────────── scan merge
// ───────────────────────────────────────

/// One scan-trace step: seed the model/store, or run a scan and assert it.
#[derive(Clone, Copy, Debug)]
enum ScanStep {
    Seed(SeedOp),
    Scan(ScanReq),
}

impl Arbitrary for ScanStep {
    fn arbitrary(g: &mut Gen) -> Self {
        if bool::arbitrary(g) {
            Self::Seed(SeedOp::arbitrary(g))
        } else {
            Self::Scan(ScanReq::arbitrary(g))
        }
    }
}

/// A seed mutation interleaving committed and dirty cells at overlapping and
/// disjoint coordinates.
#[derive(Clone, Copy, Debug)]
enum SeedOp {
    CommitSet(u8, u8),
    CommitClear(u8),
    DirtySet(u8, u8),
    DirtyClear(u8),
}

impl Arbitrary for SeedOp {
    fn arbitrary(g: &mut Gen) -> Self {
        let c = u8::arbitrary(g) % CELLS;
        match u8::arbitrary(g) % 4 {
            0 => Self::CommitSet(c, u8::arbitrary(g)),
            1 => Self::CommitClear(c),
            2 => Self::DirtySet(c, u8::arbitrary(g)),
            _ => Self::DirtyClear(c),
        }
    }
}

/// A scan request with random anchor, direction, bound exclusivity, optional
/// end and limit, and an optional early-stop prefix length.
#[derive(Clone, Copy, Debug)]
struct ScanReq {
    start: u8,
    forward: bool,
    start_excl: bool,
    end_excl: bool,
    end: u8,
    limit: Option<u8>,
    partial: Option<u8>,
}

impl Arbitrary for ScanReq {
    fn arbitrary(g: &mut Gen) -> Self {
        // Anchors range over `0..=CELLS` so they fall between cells and below /
        // above every cell.
        Self {
            start: u8::arbitrary(g) % (CELLS + 1),
            forward: bool::arbitrary(g),
            // Exclusive bounds exercise the gap-fall-through statement variants
            // and the open `(p, q)` intervals coverage stitching produces.
            start_excl: bool::arbitrary(g),
            end_excl: bool::arbitrary(g),
            end: u8::arbitrary(g) % (CELLS + 1),
            // Includes 0 and values > the cell count.
            limit: bool::arbitrary(g).then(|| u8::arbitrary(g) % (CELLS + 4)),
            partial: bool::arbitrary(g).then(|| u8::arbitrary(g) % (CELLS + 1)),
        }
    }
}

/// A shrinkable scan trace.
#[derive(Clone, Debug)]
pub(crate) struct ScanTrace {
    steps: Vec<ScanStep>,
}

impl Arbitrary for ScanTrace {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            steps: capped_vec(g, MAX_TRACE_OPS),
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        Box::new(self.steps.shrink().map(|steps| Self { steps }))
    }
}

/// The oracle scan result: `model`'s visible cells filtered to the request's
/// range, ordered in the scan direction, truncated to the limit.
fn scan_oracle(model: &CellModel, req: ScanReq) -> Vec<(u8, Bytes)> {
    let mut cells: Vec<(u8, Bytes)> = model
        .visible_ordered()
        .into_iter()
        .filter(|(c, _)| in_scan_range(req, *c))
        .collect();
    if !req.forward {
        cells.reverse();
    }
    if let Some(limit) = req.limit {
        cells.truncate(limit as usize);
    }
    cells
}

/// Whether coordinate `c` lies in the scan request's range, mirroring
/// [`Scan::contains`]: `start`/`end` are direction-relative (forward: `start`
/// low, `end` high; backward inverted) and each bound's exclusivity drops its
/// endpoint.
fn in_scan_range(req: ScanReq, c: u8) -> bool {
    // `start` is the low side forward, the high side backward; `end` inverts.
    let on_start_side = match (req.forward, req.start_excl) {
        (true, false) => c >= req.start,
        (true, true) => c > req.start,
        (false, false) => c <= req.start,
        (false, true) => c < req.start,
    };
    let within_end = match (req.forward, req.end_excl) {
        (true, false) => c <= req.end,
        (true, true) => c < req.end,
        (false, false) => c >= req.end,
        (false, true) => c > req.end,
    };
    on_start_side && within_end
}

/// Builds the [`Scan`] request from a [`ScanReq`] and owned anchor coordinates.
/// `req.start_excl`/`req.end_excl` choose the edge exclusivity.
fn scan_of<'a>(req: ScanReq, start: &'a Coordinate, end: &'a Coordinate) -> Scan<'a> {
    let start = if req.start_excl {
        ScanEdge::Excluded(start)
    } else {
        ScanEdge::Included(start)
    };
    let end = if req.end_excl {
        ScanEdge::Excluded(end)
    } else {
        ScanEdge::Included(end)
    };
    Scan {
        section: SECTION,
        start,
        dir: if req.forward {
            Direction::Forward
        } else {
            Direction::Backward
        },
        end,
        limit: req.limit.map(usize::from),
    }
}

/// Collects an overlay scan, mapping each cell to `(coordinate byte, bytes)`.
/// `take` caps how many items are drained before the stream is dropped (`None`
/// drains to exhaustion); an early `Some(k)` drop must corrupt nothing.
async fn collect_scan<S>(
    overlay: &Overlay<S>,
    id: &CollectionId,
    req: &ScanReq,
    own: EventRef,
    take: Option<usize>,
) -> Result<Vec<(u8, Bytes)>>
where
    S: CellStore,
{
    let start = Coordinate::from_bytes(vec![req.start]);
    let end = Coordinate::from_bytes(vec![req.end]);
    let stream = overlay.scan_cells(id, scan_of(*req, &start, &end), own);
    futures::pin_mut!(stream);
    let mut out = Vec::new();
    while take.is_none_or(|k| out.len() < k)
        && let Some(item) = stream.next().await
    {
        let (key, value) = item?;
        out.push((coord_of(&key), value));
    }
    Ok(out)
}

/// Drives interleaved committed seeds and scans **directly over a bottom
/// store's `scan_cells`** (no overlay), pinning the backend's own ordering,
/// clustering-range bounds, and limit handling — the Cassandra `ORDER BY
/// ASC/DESC` + `coordinate` range the overlay merge delegates to and the
/// limit/end the overlay strips before delegating. Every seed is committed
/// (`write_resolved`), so the oracle is committed-only.
pub(crate) async fn run_bottom_scan_trace<S, P>(
    store: S,
    trace: ScanTrace,
    probe: &P,
) -> Result<bool>
where
    S: CellStore,
    P: ShapeProbe,
{
    let state_key = StateKey::new(Uuid::new_v4(), Arc::from("key"));
    let id = CollectionId::new(
        state_key,
        StateType::Application,
        StateName::try_new("entries")?,
    );
    let collection_ref = CollectionRef::new(id.clone(), None);
    let own = EventRef::Message {
        dedup_id: Uuid::from_u128(1),
    };
    let mut model = CellModel::default();

    for step in trace.steps {
        match step {
            // Every seed lands in committed state; a "dirty" seed becomes a
            // committed set/clear so the bottom store alone holds the data.
            ScanStep::Seed(SeedOp::CommitSet(c, b) | SeedOp::DirtySet(c, b)) => {
                store
                    .write_resolved(&collection_ref, &[(cell_at(c), Some(bytes(b)))], &[])
                    .await?;
                model.committed.insert(c, Some(bytes(b)));
            }
            ScanStep::Seed(SeedOp::CommitClear(c) | SeedOp::DirtyClear(c)) => {
                store
                    .write_resolved(&collection_ref, &[(cell_at(c), None)], &[])
                    .await?;
                model.committed.insert(c, None);
            }
            ScanStep::Scan(req) => {
                let expected = scan_oracle(&model, req);
                let start = Coordinate::from_bytes(vec![req.start]);
                let end = Coordinate::from_bytes(vec![req.end]);
                let stream = store.scan_cells(&id, scan_of(req, &start, &end), own);
                futures::pin_mut!(stream);
                let mut got = Vec::new();
                while let Some(item) = stream.next().await {
                    let (key, value) = item?;
                    got.push((coord_of(&key), value));
                }
                if got != expected {
                    return Ok(false);
                }
            }
        }

        // After each seed (the direct `write_resolved` path, `None` ⇒ row
        // delete): the stored `kind=Cell` rows equal the committed-present set,
        // pinning the ReadUncommitted-clear row-absence path with no oracle in
        // the loop.
        if matches!(step, ScanStep::Seed(_)) {
            let present: BTreeSet<u8> = model
                .committed
                .iter()
                .filter_map(|(&coord, value)| value.is_some().then_some(coord))
                .collect();
            if probe.cell_rows(&id).await? != present {
                return Ok(false);
            }
        }
    }
    Ok(true)
}

// ─────────────────────────── sweep idempotence
// ────────────────────────────────

/// A [`CellStore`] decorator counting every durable mutation, for the
/// sweep-idempotence pin. Delegates to `inner`; the counters ride an `Arc` so
/// `Clone` shares them.
#[derive(Clone)]
pub(crate) struct CountingCellStore<S> {
    inner: S,
    counts: Arc<OpCounts>,
}

#[derive(Default)]
struct OpCounts {
    write_provisional: AtomicUsize,
    write_resolved: AtomicUsize,
    mark_resolved: AtomicUsize,
    commit_provisional: AtomicUsize,
    abort_provisional: AtomicUsize,
    standing_marker: AtomicUsize,
    get: AtomicUsize,
    scan_cells: AtomicUsize,
    provisional_cells: AtomicUsize,
    provisional_cell_at: AtomicUsize,
}

impl<S> CountingCellStore<S> {
    pub(crate) fn new(inner: S) -> Self {
        Self {
            inner,
            counts: Arc::new(OpCounts::default()),
        }
    }

    /// Total durable mutations (excludes reads). Folds the two settle verbs in:
    /// a forwarded `commit_provisional` / `abort_provisional` routes through
    /// the *inner* store's `mark_resolved` / `write_resolved`, invisible to
    /// this wrapper's per-primitive counters, so the settle itself is
    /// counted here.
    pub(crate) fn durable_writes(&self) -> usize {
        self.counts.write_provisional.load(Ordering::Relaxed)
            + self.counts.write_resolved.load(Ordering::Relaxed)
            + self.counts.mark_resolved.load(Ordering::Relaxed)
            + self.counts.commit_provisional.load(Ordering::Relaxed)
            + self.counts.abort_provisional.load(Ordering::Relaxed)
    }

    /// Standing-marker point reads — the quiescence counter: one per sweep
    /// marker leg. A read, so excluded from [`Self::durable_writes`].
    pub(crate) fn marker_reads(&self) -> usize {
        self.counts.standing_marker.load(Ordering::Relaxed)
    }

    /// Point reads issued to the lower store — zero on a covered-negative
    /// `Cached::get`.
    pub(crate) fn lower_reads(&self) -> usize {
        self.counts.get.load(Ordering::Relaxed)
    }

    /// Range scans issued to the lower store — one per coverage gap query, so
    /// the op-budget property can pin "N covered ranges ⇒ exactly the gap
    /// queries, never a section-wide re-scan".
    pub(crate) fn lower_scans(&self) -> usize {
        self.counts.scan_cells.load(Ordering::Relaxed)
    }

    /// Recovery-sweep entries — one per `provisional_cells` (cold seed) call,
    /// so a test can pin how many times the sweep hit the durable cold
    /// source.
    pub(crate) fn recovery_sweeps(&self) -> usize {
        self.counts.provisional_cells.load(Ordering::Relaxed)
    }

    /// Warm-sweep point reads — one per `provisional_cell_at`, the reads a warm
    /// (seeded) sweep issues (bounded by #provisional).
    pub(crate) fn warm_point_reads(&self) -> usize {
        self.counts.provisional_cell_at.load(Ordering::Relaxed)
    }

    pub(crate) fn reset(&self) {
        self.counts.write_provisional.store(0, Ordering::Relaxed);
        self.counts.write_resolved.store(0, Ordering::Relaxed);
        self.counts.mark_resolved.store(0, Ordering::Relaxed);
        self.counts.commit_provisional.store(0, Ordering::Relaxed);
        self.counts.abort_provisional.store(0, Ordering::Relaxed);
        self.counts.standing_marker.store(0, Ordering::Relaxed);
        self.counts.get.store(0, Ordering::Relaxed);
        self.counts.scan_cells.store(0, Ordering::Relaxed);
        self.counts.provisional_cells.store(0, Ordering::Relaxed);
        self.counts.provisional_cell_at.store(0, Ordering::Relaxed);
    }
}

impl<S> CellStore for CountingCellStore<S>
where
    S: CellStore,
{
    type Error = S::Error;

    fn get<'a>(
        &'a self,
        collection: &'a CollectionId,
        cell: &'a CellKey,
        own: EventRef,
    ) -> impl Future<Output = Result<Committed, Self::Error>> + Send + 'a {
        self.counts.get.fetch_add(1, Ordering::Relaxed);
        self.inner.get(collection, cell, own)
    }

    fn scan_cells<'a>(
        &'a self,
        collection: &'a CollectionId,
        scan: Scan<'a>,
        own: EventRef,
    ) -> impl Stream<Item = Result<(CellKey, Bytes), Self::Error>> + Send + 'a {
        // One increment per scan *request* (a gap query), not per yielded cell.
        self.counts.scan_cells.fetch_add(1, Ordering::Relaxed);
        self.inner.scan_cells(collection, scan, own)
    }

    fn provisional_cells<'a>(
        &'a self,
        collection: &'a CollectionId,
    ) -> impl Stream<Item = Result<(CellKey, ProvisionalCell), Self::Error>> + Send + 'a {
        // One increment per sweep entry, not per yielded provisional cell.
        self.counts
            .provisional_cells
            .fetch_add(1, Ordering::Relaxed);
        self.inner.provisional_cells(collection)
    }

    async fn provisional_cell_at<'a>(
        &'a self,
        collection: &'a CollectionId,
        cell: &'a CellKey,
    ) -> Result<Option<ProvisionalCell>, Self::Error> {
        // One increment per warm-sweep point read.
        self.counts
            .provisional_cell_at
            .fetch_add(1, Ordering::Relaxed);
        self.inner.provisional_cell_at(collection, cell).await
    }

    async fn write_provisional<'a>(
        &'a self,
        collection: &'a CollectionRef,
        writes: &'a [(CellKey, ProvisionalWrite)],
        clears: &'a [SectionClear],
        staged: &'a [(CellKey, ProvisionalWrite)],
    ) -> Result<(), Self::Error> {
        // One increment per collection-grain batch call (not one per cell).
        self.counts
            .write_provisional
            .fetch_add(1, Ordering::Relaxed);
        self.inner
            .write_provisional(collection, writes, clears, staged)
            .await
    }

    async fn write_resolved<'a>(
        &'a self,
        collection: &'a CollectionRef,
        cells: &'a [(CellKey, Option<Bytes>)],
        clears: &'a [SectionClear],
    ) -> Result<(), Self::Error> {
        self.counts.write_resolved.fetch_add(1, Ordering::Relaxed);
        self.inner.write_resolved(collection, cells, clears).await
    }

    async fn mark_resolved<'a>(
        &'a self,
        collection: &'a CollectionRef,
        cells: &'a [CellKey],
    ) -> Result<(), Self::Error> {
        self.counts.mark_resolved.fetch_add(1, Ordering::Relaxed);
        self.inner.mark_resolved(collection, cells).await
    }

    async fn standing_marker<'a>(
        &'a self,
        collection: &'a CollectionId,
    ) -> Result<Option<EventMarker>, Self::Error> {
        self.counts.standing_marker.fetch_add(1, Ordering::Relaxed);
        self.inner.standing_marker(collection).await
    }

    async fn commit_provisional<'a>(
        &'a self,
        collection: &'a CollectionRef,
        writes: &'a [(CellKey, ProvisionalWrite)],
        clears: &'a [SectionClear],
    ) -> Result<(), Self::Error> {
        // Count the settle itself: the inner store routes to `mark_resolved` /
        // `write_resolved` on *itself*, so those never reach this wrapper's
        // per-primitive counters.
        self.counts
            .commit_provisional
            .fetch_add(1, Ordering::Relaxed);
        self.inner
            .commit_provisional(collection, writes, clears)
            .await
    }

    async fn abort_provisional<'a>(
        &'a self,
        collection: &'a CollectionRef,
        writes: &'a [(CellKey, ProvisionalWrite)],
    ) -> Result<(), Self::Error> {
        self.counts
            .abort_provisional
            .fetch_add(1, Ordering::Relaxed);
        self.inner.abort_provisional(collection, writes).await
    }
}

/// Which surface a [`FailingCellStore`] poisons, and for what target.
#[derive(Clone)]
enum Poison {
    /// Promote path: `mark_resolved` fails with the given category for every
    /// cell of one named collection — the session's best-effort `commit_apply`
    /// and the manager's recovery sweep.
    Collection(StateName, ErrorCategory),
    /// Promote path: `mark_resolved` fails for the chosen single-byte
    /// coordinates, each with its mapped category — a mixed per-cell sweep
    /// where unpoisoned siblings must still resolve.
    Cells(BTreeMap<u8, ErrorCategory>),
    /// Stage path: `write_provisional` fails with the given category for one
    /// named collection — drives the settle boundary's permanent-finalize-skip
    /// arm (offset still commits, marker skipped, backstop armed).
    WriteProvisional(StateName, ErrorCategory),
}

/// A [`CellStore`] wrapper whose `mark_resolved` (promote path) or
/// `write_provisional` (stage path) fails for a chosen target, delegating
/// everything else to `inner`. Drives the session's best-effort
/// `commit_apply` (one poisoned cell yields `Incomplete` without cancelling
/// siblings), the manager's no-strand recovery (a failed resolution leaves the
/// backstop armed), the sweep's per-cell `try_fold` failure arm, and the
/// settle boundary's finalize-`Skip` arm.
#[derive(Clone)]
pub(crate) struct FailingCellStore<S> {
    inner: S,
    poison: Poison,
}

impl<S> FailingCellStore<S> {
    /// Wraps `inner`, poisoning `mark_resolved` `Permanent` for every cell of
    /// the `poison` collection.
    pub(crate) fn new(inner: S, poison: StateName) -> Self {
        Self::new_with_category(inner, poison, ErrorCategory::Permanent)
    }

    /// Wraps `inner`, poisoning `mark_resolved` with `category` for every cell
    /// of the `poison` collection — e.g. `Transient` to drive the recovery
    /// sweep's reschedule path.
    pub(crate) fn new_with_category(inner: S, poison: StateName, category: ErrorCategory) -> Self {
        Self {
            inner,
            poison: Poison::Collection(poison, category),
        }
    }

    /// Wraps `inner`, poisoning `mark_resolved` for each single-byte coordinate
    /// in `cells` with its mapped category (others resolve normally).
    pub(crate) fn with_cells(inner: S, cells: BTreeMap<u8, ErrorCategory>) -> Self {
        Self {
            inner,
            poison: Poison::Cells(cells),
        }
    }

    /// Wraps `inner`, poisoning `write_provisional` with `category` for the
    /// `poison` collection — the stage path (`mark_resolved` stays healthy).
    pub(crate) fn failing_write_provisional(
        inner: S,
        poison: StateName,
        category: ErrorCategory,
    ) -> Self {
        Self {
            inner,
            poison: Poison::WriteProvisional(poison, category),
        }
    }

    /// The category to inject when `mark_resolved` touches `cells`, or `None`.
    fn injected(&self, collection: &CollectionRef, cells: &[CellKey]) -> Option<ErrorCategory> {
        match &self.poison {
            Poison::Collection(name, category) => {
                (*collection.id().name() == *name).then_some(*category)
            }
            Poison::Cells(targets) => cells
                .iter()
                .find_map(|c| targets.get(&coord_of(c)).copied()),
            Poison::WriteProvisional(..) => None,
        }
    }

    /// The category to inject when `write_provisional` touches `collection`,
    /// or `None`.
    fn injected_stage(&self, collection: &CollectionRef) -> Option<ErrorCategory> {
        match &self.poison {
            Poison::WriteProvisional(name, category) => {
                (*collection.id().name() == *name).then_some(*category)
            }
            Poison::Collection(..) | Poison::Cells(..) => None,
        }
    }
}

/// Error of a [`FailingCellStore`]: the injected poison (with its category), or
/// a delegated inner error.
#[derive(Debug, thiserror::Error)]
pub(crate) enum FailCellError<E>
where
    E: Error + 'static,
{
    /// `mark_resolved` or `write_provisional` touched a poisoned target; the
    /// category is what the wrapper was asked to inject.
    #[error("cell-store poison ({0:?})")]
    Poison(ErrorCategory),

    /// A delegated inner-store error.
    #[error(transparent)]
    Inner(#[from] E),
}

impl<E> ClassifyError for FailCellError<E>
where
    E: ClassifyError + Error + 'static,
{
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::Poison(category) => *category,
            Self::Inner(e) => e.classify_error(),
        }
    }
}

impl<S> CellStore for FailingCellStore<S>
where
    S: CellStore,
{
    type Error = FailCellError<S::Error>;

    async fn get<'a>(
        &'a self,
        collection: &'a CollectionId,
        cell: &'a CellKey,
        own: EventRef,
    ) -> Result<Committed, Self::Error> {
        self.inner
            .get(collection, cell, own)
            .await
            .map_err(FailCellError::Inner)
    }

    fn scan_cells<'a>(
        &'a self,
        collection: &'a CollectionId,
        scan: Scan<'a>,
        own: EventRef,
    ) -> impl Stream<Item = Result<(CellKey, Bytes), Self::Error>> + Send + 'a {
        self.inner
            .scan_cells(collection, scan, own)
            .map(|item| item.map_err(FailCellError::Inner))
    }

    fn provisional_cells<'a>(
        &'a self,
        collection: &'a CollectionId,
    ) -> impl Stream<Item = Result<(CellKey, ProvisionalCell), Self::Error>> + Send + 'a {
        self.inner
            .provisional_cells(collection)
            .map(|item| item.map_err(FailCellError::Inner))
    }

    async fn provisional_cell_at<'a>(
        &'a self,
        collection: &'a CollectionId,
        cell: &'a CellKey,
    ) -> Result<Option<ProvisionalCell>, Self::Error> {
        self.inner
            .provisional_cell_at(collection, cell)
            .await
            .map_err(FailCellError::Inner)
    }

    async fn write_provisional<'a>(
        &'a self,
        collection: &'a CollectionRef,
        writes: &'a [(CellKey, ProvisionalWrite)],
        clears: &'a [SectionClear],
        staged: &'a [(CellKey, ProvisionalWrite)],
    ) -> Result<(), Self::Error> {
        if let Some(category) = self.injected_stage(collection) {
            return Err(FailCellError::Poison(category));
        }
        self.inner
            .write_provisional(collection, writes, clears, staged)
            .await
            .map_err(FailCellError::Inner)
    }

    async fn write_resolved<'a>(
        &'a self,
        collection: &'a CollectionRef,
        cells: &'a [(CellKey, Option<Bytes>)],
        clears: &'a [SectionClear],
    ) -> Result<(), Self::Error> {
        self.inner
            .write_resolved(collection, cells, clears)
            .await
            .map_err(FailCellError::Inner)
    }

    async fn mark_resolved<'a>(
        &'a self,
        collection: &'a CollectionRef,
        cells: &'a [CellKey],
    ) -> Result<(), Self::Error> {
        if let Some(category) = self.injected(collection, cells) {
            return Err(FailCellError::Poison(category));
        }
        self.inner
            .mark_resolved(collection, cells)
            .await
            .map_err(FailCellError::Inner)
    }

    async fn standing_marker<'a>(
        &'a self,
        collection: &'a CollectionId,
    ) -> Result<Option<EventMarker>, Self::Error> {
        self.inner
            .standing_marker(collection)
            .await
            .map_err(FailCellError::Inner)
    }

    async fn commit_provisional<'a>(
        &'a self,
        collection: &'a CollectionRef,
        writes: &'a [(CellKey, ProvisionalWrite)],
        clears: &'a [SectionClear],
    ) -> Result<(), Self::Error> {
        // A settle arriving via the sweep's marker leg routes through the inner
        // store's `mark_resolved` on the *inner* store, bypassing the poison on
        // this wrapper's `mark_resolved`. Re-check the poison here against the
        // promoted (present-data) cells so a per-cell promote poison still fires
        // on the batch settle.
        let keeps: Vec<CellKey> = writes
            .iter()
            .filter(|(_, write)| write.data().is_some())
            .map(|(cell, _)| cell.clone())
            .collect();
        if let Some(category) = self.injected(collection, &keeps) {
            return Err(FailCellError::Poison(category));
        }
        self.inner
            .commit_provisional(collection, writes, clears)
            .await
            .map_err(FailCellError::Inner)
    }

    async fn abort_provisional<'a>(
        &'a self,
        collection: &'a CollectionRef,
        writes: &'a [(CellKey, ProvisionalWrite)],
    ) -> Result<(), Self::Error> {
        self.inner
            .abort_provisional(collection, writes)
            .await
            .map_err(FailCellError::Inner)
    }
}

#[cfg(test)]
mod sweep {
    use super::super::super::memory::{MemoryCellStore, MemoryCells};
    use super::super::super::registry::CollectionDefRegistry;
    use super::*;
    use futures::executor;
    use quickcheck::QuickCheck;
    use std::collections::BTreeSet;
    use std::slice;

    /// A staged cell's promote outcome, assigned at random by the sweep
    /// failure-arm property: resolve cleanly, fail `Permanent` (skipped by the
    /// sweep), or fail transiently (aborts the sweep).
    #[derive(Clone, Copy, Debug)]
    enum Promote {
        Resolve,
        SkipPermanent,
        FailTransient,
    }

    impl Promote {
        /// The failure category injected for this cell's `mark_resolved`, or
        /// `None` to let it resolve.
        fn category(self) -> Option<ErrorCategory> {
            match self {
                Self::Resolve => None,
                Self::SkipPermanent => Some(ErrorCategory::Permanent),
                Self::FailTransient => Some(ErrorCategory::Transient),
            }
        }
    }

    impl Arbitrary for Promote {
        fn arbitrary(g: &mut Gen) -> Self {
            match u8::arbitrary(g) % 3 {
                0 => Self::SkipPermanent,
                1 => Self::FailTransient,
                _ => Self::Resolve,
            }
        }
    }

    /// A random per-cell outcome assignment, capped so coordinates stay
    /// distinct single bytes and traces stay small.
    #[derive(Clone, Debug)]
    struct SweepOutcomes(Vec<Promote>);

    impl Arbitrary for SweepOutcomes {
        fn arbitrary(g: &mut Gen) -> Self {
            Self(capped_vec(g, 16))
        }

        fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
            Box::new(self.0.shrink().map(Self))
        }
    }

    /// Drives one random outcome assignment through `sweep_provisional` and
    /// checks its contract. Stages **all** the outcomes' cells under **one**
    /// committed event in a single `write_provisional` (the stage-boundary rule
    /// makes multiple events' provisional cells coexisting on one collection
    /// unreachable — a later stage would boundary-resolve its predecessor), so
    /// the sweep resolves them as one marker leg followed by the per-cell
    /// mop-up. Poisons the chosen cells, then asserts the sweep's return
    /// matches the assignment. Converted from the prior one-event-per-cell
    /// shape: same failure-arm invariant, now a reachable staging shape.
    async fn run_sweep_failure(SweepOutcomes(outcomes): SweepOutcomes) -> Result<bool> {
        let oracle = ScriptedOracle::default();
        let registry = Arc::new(CollectionDefRegistry::default());
        let inner = MemoryCellStore::new(MemoryCells::new(), oracle.clone(), registry);
        let collection = CollectionRef::new(
            CollectionId::new(
                StateKey::new(Uuid::new_v4(), Arc::from("k")),
                StateType::Application,
                StateName::try_new("sweep-fail")?,
            ),
            None,
        );

        // All cells staged under one committed event, at distinct coordinates.
        let dedup_id = Uuid::from_u128(0);
        let event = EventRef::Message { dedup_id };
        let mut writes: Vec<(CellKey, ProvisionalWrite)> = Vec::with_capacity(outcomes.len());
        for c in 0..outcomes.len() as u8 {
            let cell = cell_at(c);
            let prev = inner.get(collection.id(), &cell, event).await?;
            writes.push((cell, ProvisionalWrite::new(Some(bytes(c)), prev, event)));
        }
        inner
            .write_provisional(&collection, &writes, &[], &writes)
            .await?;
        oracle.record_message(dedup_id).await?;

        let poison: BTreeMap<u8, ErrorCategory> = outcomes
            .iter()
            .enumerate()
            .filter_map(|(i, outcome)| outcome.category().map(|category| (i as u8, category)))
            .collect();
        let permanent = poison
            .values()
            .filter(|&&category| category == ErrorCategory::Permanent)
            .count();
        let has_transient = poison.values().any(|&c| c == ErrorCategory::Transient);

        let store = FailingCellStore::with_cells(inner, poison);
        match sweep_provisional(&store, &oracle, &collection).await {
            // A transient failure must surface as an error; nothing else may.
            Err(_) => Ok(has_transient),
            // No transient: the sweep reports all-resolved iff nothing was
            // skipped, and exactly the Permanent-skipped cells linger — every
            // sibling resolved regardless of submission order.
            Ok(all_resolved) => {
                let lingering = provisional_count(&store, collection.id()).await?;
                Ok(!has_transient && all_resolved == (permanent == 0) && lingering == permanent)
            }
        }
    }

    /// `sweep_provisional`'s failure-arm contract, the invariant the `try_fold`
    /// rewrite must preserve over the old sequential loop: a `Permanent` cell
    /// is skipped (the sweep still resolves its siblings and reports
    /// `false`), while any transient cell aborts the whole sweep.
    /// Order-independent, so the concurrent `buffer_unordered` pipeline
    /// answers exactly as the loop did.
    #[test]
    fn prop_sweep_failure_arm() {
        fn property(outcomes: SweepOutcomes) -> Result<bool> {
            executor::block_on(run_sweep_failure(outcomes))
        }
        QuickCheck::new().quickcheck(property as fn(SweepOutcomes) -> Result<bool>);
    }

    /// The first sweep over staged provisional cells resolves them; a second
    /// sweep performs **zero durable writes** — `provisional_cells` yields
    /// nothing once every cell is resolved, so `sweep_provisional`
    /// short-circuits before touching the backend (sweep idempotence).
    #[tokio::test]
    async fn second_sweep_is_a_no_op() -> Result<()> {
        let oracle = ScriptedOracle::default();
        let registry = Arc::new(CollectionDefRegistry::default());
        let store = CountingCellStore::new(MemoryCellStore::new(
            MemoryCells::new(),
            oracle.clone(),
            registry,
        ));
        let (ids, refs) = pooled_collections()?;
        let cell = value_cell();

        // Stage a committed provisional cell on every collection.
        for (i, (id, r)) in ids.iter().zip(&refs).enumerate() {
            let dedup_id = Uuid::from_u128(i as u128);
            let event = EventRef::Message { dedup_id };
            let prev = store.get(id, &cell, event).await?;
            let writes = [(
                cell.clone(),
                ProvisionalWrite::new(Some(bytes(7)), prev, event),
            )];
            store.write_provisional(r, &writes, &[], &writes).await?;
            oracle.record_message(dedup_id).await?;
        }

        // First sweep resolves every cell — via the marker leg's one settle per
        // collection, so `durable_writes` counts the folded `commit_provisional`.
        for r in &refs {
            assert!(sweep_provisional(&store, &oracle, r).await?);
        }
        assert!(
            store.durable_writes() > 0,
            "first sweep must resolve provisional cells"
        );
        // Non-vacuous: the sweep read each collection's standing marker once.
        assert_eq!(
            store.marker_reads(),
            refs.len(),
            "first sweep reads each collection's standing marker exactly once"
        );

        // Second sweep is a no-op: no provisional cell remains to resolve and no
        // marker stands (the first sweep's settle deleted it), so the marker leg
        // and the per-cell leg both issue zero durable writes.
        store.reset();
        for r in &refs {
            assert!(sweep_provisional(&store, &oracle, r).await?);
        }
        assert_eq!(store.durable_writes(), 0);
        // The sweep still *entered* both legs once per collection — the counters
        // that make the durable-write assertion non-vacuous.
        assert_eq!(store.recovery_sweeps(), refs.len());
        assert_eq!(store.marker_reads(), refs.len());
        Ok(())
    }

    /// One provisional-set maintenance op the agreement property drives.
    #[derive(Clone, Copy, Debug)]
    enum SetOp {
        /// Stage a provisional cell and leave it unresolved (it must linger in
        /// the set).
        StageOnly(u8),
        /// Stage then promote (the set entry must clear).
        Promote(u8),
        /// Stage then roll back to `prev` (the set entry must clear).
        Rollback(u8),
    }

    impl SetOp {
        fn coord(self) -> u8 {
            match self {
                Self::StageOnly(c) | Self::Promote(c) | Self::Rollback(c) => c % CELLS,
            }
        }
    }

    impl Arbitrary for SetOp {
        fn arbitrary(g: &mut Gen) -> Self {
            let c = u8::arbitrary(g);
            match u8::arbitrary(g) % 3 {
                0 => Self::Promote(c),
                1 => Self::Rollback(c),
                _ => Self::StageOnly(c),
            }
        }
    }

    /// A capped random op sequence over a single collection.
    #[derive(Clone, Debug)]
    struct SetOps(Vec<SetOp>);

    impl Arbitrary for SetOps {
        fn arbitrary(g: &mut Gen) -> Self {
            Self(capped_vec(g, 24))
        }

        fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
            Box::new(self.0.shrink().map(Self))
        }
    }

    /// The provisional cell coordinates a store reports, as an
    /// order-independent set — the warm store reads its in-memory set, a
    /// cold store full-scans.
    async fn provisional_key_set<S>(store: &S, id: &CollectionId) -> Result<BTreeSet<CellKey>>
    where
        S: CellStore,
    {
        let stream = store.provisional_cells(id);
        futures::pin_mut!(stream);
        let mut set = BTreeSet::new();
        while let Some(item) = stream.next().await {
            let (key, _) = item?;
            set.insert(key);
        }
        Ok(set)
    }

    /// Runs one op sequence against a **seeded** (warm) store, then checks its
    /// incrementally-maintained provisional set against ground truth: a fresh
    /// **cold** store over the same backing that enumerates the durable
    /// provisional cells from scratch. Equality proves the set neither
    /// under-reports (a provisional cell it missed → a strand) nor over-reports
    /// beyond what the point-read filter drops (the incremental set must stay
    /// ⟺ the durable provisional cells). Minting the cold store fresh over the
    /// shared cells is the memory cold-window.
    async fn run_set_agreement(SetOps(ops): SetOps) -> Result<bool> {
        let oracle = ScriptedOracle::default();
        let cells = MemoryCells::new();
        let registry = Arc::new(CollectionDefRegistry::default());
        let warm = MemoryCellStore::new(cells.clone(), oracle.clone(), registry.clone());
        let collection = CollectionRef::new(
            CollectionId::new(
                StateKey::new(Uuid::new_v4(), Arc::from("k")),
                StateType::Application,
                StateName::try_new("agree")?,
            ),
            None,
        );

        // Seed the warm store's latch (empty), so subsequent ops exercise the
        // incrementally-maintained set, not another cold scan.
        let _ = provisional_key_set(&warm, collection.id()).await?;

        for (i, op) in ops.into_iter().enumerate() {
            let event = EventRef::Message {
                dedup_id: Uuid::from_u128(i as u128),
            };
            let cell = cell_at(op.coord());
            let prev = warm.get(collection.id(), &cell, event).await?;
            let prev_value = prev.get().cloned();
            let writes = [(
                cell.clone(),
                ProvisionalWrite::new(Some(bytes(i as u8)), prev, event),
            )];
            warm.write_provisional(&collection, &writes, &[], &writes)
                .await?;
            match op {
                SetOp::StageOnly(_) => {}
                SetOp::Promote(_) => {
                    warm.mark_resolved(&collection, slice::from_ref(&cell))
                        .await?;
                }
                SetOp::Rollback(_) => {
                    warm.write_resolved(&collection, &[(cell.clone(), prev_value)], &[])
                        .await?;
                }
            }
        }

        let warm_set = provisional_key_set(&warm, collection.id()).await?;
        let cold = MemoryCellStore::new(cells, oracle, registry);
        let cold_set = provisional_key_set(&cold, collection.id()).await?;
        Ok(warm_set == cold_set)
    }

    /// The seeded in-memory provisional set agrees with the durable provisional
    /// cells after any stage/promote/rollback sequence.
    #[test]
    fn prop_provisional_set_agreement() {
        fn property(ops: SetOps) -> Result<bool> {
            executor::block_on(run_set_agreement(ops))
        }
        QuickCheck::new().quickcheck(property as fn(SetOps) -> Result<bool>);
    }
}
