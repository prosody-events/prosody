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
use super::super::resolve::{resolve_cell, resolve_marker, sweep_provisional};
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
use std::slice;
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

/// The [`SECTIONS`] pool index of a sampled section — the inverse of
/// [`cell_in`]'s folding. Every trace section is drawn from the pool, so the
/// lookup always hits; the `unwrap_or_default` merely keeps it total.
fn section_slot(section: Section) -> u8 {
    SECTIONS
        .iter()
        .position(|&s| s == section)
        .unwrap_or_default() as u8
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

/// A set of physical `(section, coordinate byte)` row keys ([`row_key`]).
pub(crate) type RowKeys = BTreeSet<(i8, u8)>;

/// A marker payload's clear half: each cleared section mapped to its frozen
/// survivor coordinate bytes.
pub(crate) type ClearMap = BTreeMap<i8, BTreeSet<u8>>;

/// The physically observed standing **event marker**: its owning event, its
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
/// primitive (clean promote, clean abort, crash→sweep, crash→first-touch, the
/// direct `write_resolved(None)` clear, and the section-clear gap erase).
/// `run_overlay_trace`/`run_overwrite_trace` add no new physical path — their
/// committed mutations and first-touch resolutions go through those same
/// primitives — so hooking them would only add live round-trips.
pub(crate) trait ShapeProbe {
    async fn cell_rows(&self, id: &CollectionId) -> Result<RowKeys>;

    /// The collection's standing **event marker** as physically observed —
    /// including the payload's clear half — or `None` when no marker stands.
    /// Read raw, never through the resolving store.
    async fn standing_marker(&self, id: &CollectionId) -> Result<Option<ProbedMarker>>;

    /// The row keys whose stored rows are physically **provisional**, read
    /// raw. Together with [`Self::standing_marker`] this feeds the
    /// staged-coverage postcondition: a provisional row unlisted by the
    /// standing marker is stranded from recovery.
    async fn provisional_rows(&self, id: &CollectionId) -> Result<RowKeys>;
}

/// [`ShapeProbe`] over the memory backend: the store map itself, every entry
/// regardless of variant (so a lingering `Resolved(None)` residue is visible),
/// and the durable marker map (so a leaked or missing marker is visible).
pub(crate) struct MemoryShapeProbe(pub(crate) MemoryCells);

impl ShapeProbe for MemoryShapeProbe {
    async fn cell_rows(&self, id: &CollectionId) -> Result<RowKeys> {
        Ok(self
            .0
            .stored_coordinates(id)
            .into_iter()
            .map(|cell| row_key(&cell))
            .collect())
    }

    async fn standing_marker(&self, id: &CollectionId) -> Result<Option<ProbedMarker>> {
        Ok(self.0.standing_marker_of(id).map(|marker| {
            let (staged, clears) = probed_parts(&marker);
            (marker.event(), staged, clears)
        }))
    }

    async fn provisional_rows(&self, id: &CollectionId) -> Result<RowKeys> {
        Ok(self
            .0
            .provisional_coordinates(id)
            .iter()
            .map(row_key)
            .collect())
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

/// Where an injected settle failure fires: at the **wrapper** (outside the
/// store under test — the settle provably never reaches it, so durable state
/// and any warm coverage stay exactly as staged), or at the **lower** store
/// (beneath any cache in the composition — a `Cached` instantiation runs its
/// punch-first legs before the lower store rejects). For a bare store the two
/// depths coincide.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FaultDepth {
    Wrapper,
    Lower,
}

impl Arbitrary for FaultDepth {
    fn arbitrary(g: &mut Gen) -> Self {
        if bool::arbitrary(g) {
            Self::Wrapper
        } else {
            Self::Lower
        }
    }
}

/// How an event resolved — the six distinct outcomes.
#[derive(Clone, Copy, Debug)]
enum Outcome {
    /// Committed and promoted inline (the hot path).
    CleanCommitted,
    /// Staged then rolled back inline before any commit-marker flush
    /// (abandon).
    CleanRolledBack,
    /// All cells staged, commit marker never flushed, crash → the generated
    /// recovery (rolls back).
    CrashAfterStage,
    /// All cells staged, commit marker flushed, crash → the generated
    /// recovery (promotes).
    CrashAfterMarker,
    /// Only a prefix staged, commit marker never flushed, crash → recovery.
    CrashMidFanOut,
    /// Commit marker flushed (committed), then the settle attempt fails under
    /// a poison armed at the generated [`FaultDepth`]: the stage lingers over
    /// the **warm** in-process store — the committed-unapplied window (on a
    /// `Cached` instantiation, with warm coverage still holding the
    /// stage-time `prev`s at `Wrapper` depth, and Cov-Clr-punched cleared
    /// sections at `Lower` depth).
    SettleFailure(FaultDepth),
}

impl Outcome {
    fn marker_flushed(self) -> bool {
        matches!(
            self,
            Self::CleanCommitted | Self::CrashAfterMarker | Self::SettleFailure(_)
        )
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
        match u8::arbitrary(g) % 6 {
            0 => Self::CleanCommitted,
            1 => Self::CleanRolledBack,
            2 => Self::CrashAfterStage,
            3 => Self::CrashAfterMarker,
            4 => Self::CrashMidFanOut,
            _ => Self::SettleFailure(FaultDepth::arbitrary(g)),
        }
    }
}

/// How a crash event recovers — or deliberately doesn't.
#[derive(Clone, Copy, Debug)]
enum Recovery {
    /// The backstop sweep resolves every pooled collection.
    Sweep,
    /// Reads of the crashed event's staged cells first-touch-resolve them;
    /// the standing event marker lingers (first-touch is marker-free — the
    /// over-report the model keeps).
    FirstTouch,
    /// No immediate recovery: the standing event marker and provisional cells
    /// linger into subsequent events, resolved at the next stage boundary or
    /// a later sweep — the fresh-assignee shape (the crash rebuild models the
    /// cold new assignee).
    Defer,
}

impl Arbitrary for Recovery {
    fn arbitrary(g: &mut Gen) -> Self {
        g.choose(&[Self::Sweep, Self::FirstTouch, Self::Defer])
            .copied()
            .unwrap_or(Self::Sweep)
    }
}

/// One event: a flat list of `(collection, section idx, coord, mutation)`
/// writes, the sections it durably clears, an outcome, and the recovery path
/// to use when the outcome is a crash. The flat list is grouped by collection
/// ([`event_plan`]) so each touched collection stages **all** its cells (and
/// clears) in one `write_provisional` call — unless `split` is set, which
/// stages a ≥2-cell collection in two sequential same-event calls carrying the
/// same union marker, exercising the same-event marker overwrite at the stage
/// boundary (the second stage's standing marker is the event's OWN and must
/// not be resolved).
#[derive(Clone, Debug)]
struct TraceEvent {
    writes: Vec<(u8, u8, u8, Mutation)>,
    /// Durable section clears as `(collection, section idx)`. Deduped; the
    /// clear's collection is drawn independently of the writes', so
    /// clears-only stages (a clear on a collection the event never writes)
    /// arise organically.
    clears: Vec<(u8, u8)>,
    outcome: Outcome,
    recovery: Recovery,
    split: bool,
    /// When set, the event's FIRST planned stage is rejected at the lower
    /// store (a transient `write_provisional` fault) and the event is never
    /// dispatched: the model, marker model, and deferrals stay untouched —
    /// the rejected stage's boundary resolve never reached the bottom store,
    /// so a lingering foreign stage still lingers. Weighted low; shrinks to
    /// `false`.
    stage_fault: bool,
}

impl Arbitrary for TraceEvent {
    fn arbitrary(g: &mut Gen) -> Self {
        let writes = capped_vec::<(u8, u8, u8, Mutation)>(g, (POOL * CRASH_CELLS) as usize)
            .into_iter()
            .map(|(coll, s, c, m)| (coll % POOL, section_idx(s), c % CRASH_CELLS, m))
            .collect();
        let mut clears: Vec<(u8, u8)> = capped_vec::<(u8, u8)>(g, 2)
            .into_iter()
            .map(|(coll, s)| (coll % POOL, section_idx(s)))
            .collect();
        clears.sort_unstable();
        clears.dedup();
        Self {
            writes,
            clears,
            outcome: Outcome::arbitrary(g),
            recovery: Recovery::arbitrary(g),
            split: bool::arbitrary(g),
            stage_fault: u8::arbitrary(g) % 8 == 0,
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        let base = self.clone();
        let unfaulted = self.stage_fault.then(|| Self {
            stage_fault: false,
            ..base.clone()
        });
        let writes = self.writes.shrink().map({
            let base = base.clone();
            move |writes| Self {
                writes,
                ..base.clone()
            }
        });
        let clears = self.clears.shrink().map(move |clears| Self {
            clears,
            ..base.clone()
        });
        Box::new(unfaulted.into_iter().chain(writes).chain(clears))
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

/// One collection's stage-plan entry: the pool slot, the collapsed
/// `((section idx, coord), mutation)` cell set staged atomically, and the
/// section indices durably cleared.
type PlannedStage = (u8, Vec<((u8, u8), Mutation)>, Vec<u8>);

/// One event's stage plan, grouped by collection.
type StagePlan = Vec<PlannedStage>;

/// Groups an event's flat writes by collection (first-seen order, repeats of a
/// cell collapsed last-writer-wins) and merges in its durable clears — a
/// clear-touched collection with no writes becomes a clears-only entry.
fn event_plan(event: &TraceEvent) -> StagePlan {
    let mut plan: StagePlan = Vec::new();
    for &(coll, s, c, mutation) in &event.writes {
        match plan.iter_mut().find(|(p, ..)| *p == coll) {
            Some((_, cells, _)) => collapse_cell_into(cells, (s, c), mutation),
            None => plan.push((coll, vec![((s, c), mutation)], Vec::new())),
        }
    }
    for &(coll, s) in &event.clears {
        match plan.iter_mut().find(|(p, ..)| *p == coll) {
            Some((_, _, cleared)) => {
                if !cleared.contains(&s) {
                    cleared.push(s);
                }
            }
            None => plan.push((coll, Vec::new(), vec![s])),
        }
    }
    plan
}

/// Inserts `(cell, mutation)` into a collection's cell set, overwriting any
/// existing mutation for that cell (last-writer-wins) and preserving order.
fn collapse_cell_into<K: PartialEq>(cells: &mut Vec<(K, Mutation)>, cell: K, mutation: Mutation) {
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

/// Asserts every non-skipped pooled collection is fully resolved (no
/// provisional lingers) and each modelled cell projects its committed value.
/// `skip[i]` marks a collection with a deliberately lingering (deferred)
/// stage: reading it would first-touch-resolve the cells and destroy the very
/// shape the deferral exists to exercise, so its checks wait for resolution.
async fn assert_converged<S>(
    store: &S,
    ids: &[CollectionId],
    model: &[BTreeMap<(u8, u8), Option<Bytes>>],
    skip: &[bool],
) -> Result<bool>
where
    S: CellStore,
{
    // A probe event distinct from every trace event so own-event never
    // short-circuits.
    let probe = EventRef::Message {
        dedup_id: Uuid::from_u128(u128::MAX / 2),
    };
    for (slot, (id, expected)) in ids.iter().zip(model).enumerate() {
        if skip[slot] {
            continue;
        }
        // Check raw provisional state FIRST — `get` would resolve a lingering
        // provisional cell and mask a non-convergence.
        if provisional_count(store, id).await? != 0 {
            return Ok(false);
        }
        for (&(s, c), value) in expected {
            let committed = store.get(id, &cell_in(s, c), probe).await?;
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

/// The per-collection staged state an event produces: for each touched
/// collection, its pool index, the `(cell, write)` set staged atomically, and
/// the frozen [`SectionClear`]s its marker carries.
type StagedWrites = Vec<(u8, Vec<(CellKey, ProvisionalWrite)>, Vec<SectionClear>)>;

/// Stages every collection in the plan under `event`, checking each cell's
/// committed base against the model first (returning `None` on a mismatch — a
/// property violation the caller surfaces) and returning the per-collection
/// staged writes + frozen clears for the settle. Every stage passes the
/// event's frozen union marker; when `split` is set, a ≥2-cell collection is
/// staged in two sequential same-event `write_provisional` calls (prefix, then
/// the rest), both carrying that union marker. The second call's standing
/// marker is the event's OWN; the stage boundary must overwrite it, never
/// resolve it. This is the only path that exercises the same-event marker
/// overwrite, so treating an own marker as foreign strands the prefix and
/// fails convergence. A clears-only collection stages `writes = []` with a
/// marker whose `staged()` is empty and `clears()` non-empty.
///
/// `stale_prev_ok[i]` accepts a stale prev-read on collection `i`: a restage
/// over a **warm** committed-unapplied stage (a settle failure with the
/// in-process cache intact) may legitimately read the covered pre-settle value
/// — the accepted bounded window. The staged prev still feeds the write, so a
/// later rollback restores exactly what was read (mirrored in the model by the
/// rollback-restores-staged-prev rule).
async fn stage_event<S>(
    store: &S,
    refs: &[CollectionRef],
    staged: &StagePlan,
    model: &[BTreeMap<(u8, u8), Option<Bytes>>],
    event: EventRef,
    split: bool,
    stale_prev_ok: &[bool],
) -> Result<Option<StagedWrites>>
where
    S: CellStore,
{
    let mut staged_writes = Vec::with_capacity(staged.len());
    for (coll, cells, cleared) in staged {
        let mut cell_writes: Vec<(CellKey, ProvisionalWrite)> = Vec::with_capacity(cells.len());
        for &((s, c), mutation) in cells {
            let key = cell_in(s, c);
            let prev = store.get(refs[*coll as usize].id(), &key, event).await?;
            if !stale_prev_ok[*coll as usize]
                && prev.get().cloned() != model[*coll as usize].get(&(s, c)).cloned().flatten()
            {
                return Ok(None);
            }
            cell_writes.push((key, ProvisionalWrite::new(mutation.value(), prev, event)));
        }
        // Survivors frozen from the staged set (the single survivor
        // definition: the cleared section's present-data staged cells).
        let clears: Vec<SectionClear> = cleared
            .iter()
            .map(|&s| SectionClear::frozen(SECTIONS[s as usize], &cell_writes))
            .collect();
        let marker = EventMarker::frozen(event, &cell_writes, &clears);
        let collection = &refs[*coll as usize];
        if split && cell_writes.len() >= 2 {
            let mid = cell_writes.len() / 2;
            store
                .write_provisional(collection, &cell_writes[..mid], Some(&marker))
                .await?;
            store
                .write_provisional(collection, &cell_writes[mid..], Some(&marker))
                .await?;
        } else {
            store
                .write_provisional(collection, &cell_writes, Some(&marker))
                .await?;
        }
        staged_writes.push((*coll, cell_writes, clears));
    }
    Ok(Some(staged_writes))
}

/// One collection's lingering (deferred) stage: its staged writes (whose
/// `prev`s a rollback restores), whether the owning event committed, and
/// whether the stage lingers over the **warm** in-process store (a settle
/// failure) rather than a crash rebuild — warm deferral is what makes a stale
/// covered prev-read reachable on the next restage.
struct Deferred {
    writes: Vec<(CellKey, ProvisionalWrite)>,
    committed: bool,
    warm: bool,
}

/// The crash-equivalence runner's tracked state: the committed-projection
/// model, the standing-event-marker model (owning event's dedup payload, its
/// staged row keys, and its clear half; `None` ⇒ no marker stands), and the
/// per-slot lingering (deferred) stages whose convergence/row-shape checks
/// wait for resolution. The marker model is set on stage; cleared by a clean
/// settle, a sweep, or — for a clears-bearing marker — the first foreign read
/// (read-help, [`Self::note_read_help`]); **kept** by clears-free
/// crash→first-touch (first-touch is marker-free — the over-report the model
/// pins), a deferral, and a settle failure; replaced by the next stage
/// (boundary overwrite).
struct TraceState {
    model: Vec<BTreeMap<(u8, u8), Option<Bytes>>>,
    marker_model: Vec<Option<(u128, RowKeys, ClearMap)>>,
    deferred: Vec<Option<Deferred>>,
}

impl TraceState {
    fn new() -> Self {
        Self {
            model: vec![BTreeMap::new(); POOL as usize],
            marker_model: vec![None; POOL as usize],
            deferred: (0..POOL).map(|_| None).collect(),
        }
    }

    /// Applies the rollback outcome to the model: each staged cell's
    /// coordinate becomes the **staged prev** — exactly what
    /// `abort_provisional` / first-touch rollback durably restores. Normally
    /// a no-op (the staged prev was checked equal to the model); in the
    /// accepted warm committed-unapplied window it faithfully captures the
    /// restage's stale-read rollback.
    fn apply_rollback(&mut self, slot: usize, writes: &[(CellKey, ProvisionalWrite)]) {
        for (cell, write) in writes {
            let key = (i8::from(cell.section).cast_unsigned(), coord_of(cell));
            self.model[slot].insert(key, write.prev().cloned());
        }
    }

    /// Advances the model for a committed event's clears + staged mutations:
    /// every modelled cell of a cleared section reads absent (the gap erase —
    /// kept as an explicit `None` so convergence verifies erasure via `get`),
    /// then the staged mutations land (survivors return).
    fn apply_committed(&mut self, slot: usize, cells: &[((u8, u8), Mutation)], cleared: &[u8]) {
        for &s in cleared {
            for ((sect, _), value) in &mut self.model[slot] {
                if *sect == s {
                    *value = None;
                }
            }
        }
        for &((s, c), mutation) in cells {
            self.model[slot].insert((s, c), mutation.value());
        }
    }

    /// Models the read-help side effect of the convergence pass: its `get`s on
    /// a non-deferred collection resolve any standing foreign clears-bearing
    /// marker (the committed-unapplied read window), so the marker model drops
    /// it. A clears-free marker is never resolved by reads (first-touch stays
    /// marker-free), and an unread collection (empty model, nothing to `get`)
    /// keeps its marker standing.
    fn note_read_help(&mut self) {
        for slot in 0..self.deferred.len() {
            if self.deferred[slot].is_none()
                && !self.model[slot].is_empty()
                && self.marker_model[slot]
                    .as_ref()
                    .is_some_and(|(_, _, clears)| !clears.is_empty())
            {
                self.marker_model[slot] = None;
            }
        }
    }

    /// Resolves every lingering deferral in the model (a sweep settled them
    /// all): an uncommitted deferral rolls back to its staged prevs, a
    /// committed one already advanced the model at its flush.
    fn resolve_deferrals(&mut self) {
        for slot in 0..self.deferred.len() {
            if let Some(d) = self.deferred[slot].take()
                && !d.committed
            {
                self.apply_rollback(slot, &d.writes);
            }
        }
    }

    /// The crash arm: recovers the freshly rebuilt `store` along the event's
    /// generated recovery path, keeping the models in step.
    async fn recover_crash<S>(
        &mut self,
        store: &FailingCellStore<S>,
        oracle: &ScriptedOracle,
        refs: &[CollectionRef],
        staged_writes: &StagedWrites,
        ev: &TraceEvent,
        index: usize,
    ) -> Result<bool>
    where
        S: CellStore,
    {
        // Any warm deferral is now cold: the rebuild dropped the in-process
        // cache, so subsequent reads resolve durably.
        for d in self.deferred.iter_mut().flatten() {
            d.warm = false;
        }
        match ev.recovery {
            Recovery::Sweep => {
                for r in refs {
                    if !sweep_provisional(store, oracle, r).await? {
                        return Ok(false);
                    }
                }
                // The sweep's marker leg resolves every standing marker —
                // this event's stage AND any earlier deferral. A committed
                // marker's clear half applies its gaps here (the model
                // already advanced at flush).
                self.resolve_deferrals();
                if !ev.outcome.marker_flushed() {
                    for (coll, cell_writes, _) in staged_writes {
                        self.apply_rollback(*coll as usize, cell_writes);
                    }
                }
                self.marker_model.fill(None);
            }
            Recovery::FirstTouch => {
                // First-touch under a fresh event so own-event never
                // short-circuits; the read resolves each staged provisional
                // cell. A clears-free marker lingers (first-touch is
                // marker-free — the over-report the model keeps); a
                // clears-bearing marker is resolved WHOLE by the first get
                // (read-help), which `note_read_help` accounts for after the
                // convergence pass.
                let recovery_event = EventRef::Message {
                    dedup_id: Uuid::from_u128(u128::MAX - index as u128),
                };
                for (coll, cell_writes, _) in staged_writes {
                    for (cell, _) in cell_writes {
                        store
                            .get(refs[*coll as usize].id(), cell, recovery_event)
                            .await?;
                    }
                }
                if !ev.outcome.marker_flushed() {
                    for (coll, cell_writes, _) in staged_writes {
                        self.apply_rollback(*coll as usize, cell_writes);
                    }
                }
            }
            Recovery::Defer => {
                // No recovery: the stage lingers into subsequent events
                // (cold — the rebuild models the fresh assignee). The
                // standing marker's clear half is asserted by the probe while
                // it lingers.
                for (coll, cell_writes, _) in staged_writes {
                    self.deferred[*coll as usize] = Some(Deferred {
                        writes: cell_writes.clone(),
                        committed: ev.outcome.marker_flushed(),
                        warm: false,
                    });
                }
            }
        }
        Ok(true)
    }

    /// The non-crash settle arms: clean promote, clean rollback, or the
    /// poisoned settle failure at its generated [`FaultDepth`].
    async fn settle<S>(
        &mut self,
        store: &FailingCellStore<S>,
        lower: &PoisonHandle,
        refs: &[CollectionRef],
        staged_writes: &StagedWrites,
        ev: &TraceEvent,
    ) -> Result<bool>
    where
        S: CellStore,
    {
        for (coll, cell_writes, clears) in staged_writes {
            let slot = *coll as usize;
            match ev.outcome {
                Outcome::SettleFailure(depth) => {
                    // Committed, then the settle fails under the armed poison
                    // — the committed-unapplied window over the WARM store.
                    // `Wrapper` fires outside the store under test (the settle
                    // provably never reaches it: durable state and any warm
                    // coverage stay exactly as staged); `Lower` fires beneath
                    // any cache (a `Cached` instantiation runs its Cov-Clr
                    // punch before the lower store rejects). Injected faults
                    // are not runtime errors: the armed settle MUST return
                    // `Err` — an `Ok` is a red property.
                    let poison = Poison::Collection(
                        refs[slot].id().name().clone(),
                        ErrorCategory::Transient,
                    );
                    match depth {
                        FaultDepth::Wrapper => store.set_poison(Some(poison)),
                        FaultDepth::Lower => *lower.lock() = Some(poison),
                    }
                    let settled = store
                        .commit_provisional(&refs[slot], cell_writes, clears)
                        .await;
                    store.set_poison(None);
                    *lower.lock() = None;
                    if settled.is_ok() {
                        return Ok(false);
                    }
                    if depth == FaultDepth::Lower && !clears.is_empty() {
                        // Directed post-failure reads: the punch-first
                        // contract means a Lower-depth apply failure left the
                        // cleared sections uncovered, so reading every cell of
                        // each cleared section must serve marker-resolved
                        // truth (the model advanced at flush — committed ⇒
                        // post-clear values); a wrong, missing, or late punch
                        // serves covered pre-clear values ⇒ divergence. These
                        // reads read-help-resolve the standing marker WHOLE
                        // (writes included), so the event is now settled: the
                        // marker model drops and no deferral is recorded.
                        let probe = EventRef::Message {
                            dedup_id: Uuid::from_u128(u128::MAX / 2),
                        };
                        for clear in clears {
                            let s = section_slot(clear.section());
                            for c in 0..CRASH_CELLS {
                                let got = store
                                    .get(refs[slot].id(), &cell_in(s, c), probe)
                                    .await?
                                    .into_inner();
                                let want = self.model[slot].get(&(s, c)).cloned().flatten();
                                if got != want {
                                    return Ok(false);
                                }
                            }
                        }
                        self.marker_model[slot] = None;
                    } else {
                        self.deferred[slot] = Some(Deferred {
                            writes: cell_writes.clone(),
                            committed: true,
                            warm: true,
                        });
                    }
                }
                Outcome::CleanCommitted => {
                    // Promote through the settle path (publishes `data` and
                    // applies the clears' gap erase); the settle deletes the
                    // collection's marker.
                    store
                        .commit_provisional(&refs[slot], cell_writes, clears)
                        .await?;
                    self.marker_model[slot] = None;
                }
                _ => {
                    // Clean inline rollback through the settle path (restores
                    // each staged prev; clears staged nothing destructive, so
                    // rollback needs no clear leg); the settle deletes the
                    // marker.
                    store.abort_provisional(&refs[slot], cell_writes).await?;
                    self.marker_model[slot] = None;
                    self.apply_rollback(slot, cell_writes);
                }
            }
        }
        Ok(true)
    }
}

/// The per-event physical-shape assertions, all raw probe reads:
///
/// * **Row absence** (skipped for deferred collections — their rows are
///   legitimately provisional): the stored `kind=Cell` rows equal the model's
///   present set exactly, so a residue row or a lost row both surface — and a
///   committed clear whose gaps never landed shows up as extra rows.
/// * **Marker shape** (always): the standing event marker matches the model —
///   owning event, frozen staged row keys, AND the payload's clear half — or is
///   absent.
/// * **Staged coverage** (always — the universal marker-first invariant): every
///   physically provisional row is listed by the standing marker; with no
///   marker standing, no provisional row exists. A violation is a strand.
async fn assert_physical<P>(probe: &P, ids: &[CollectionId], state: &TraceState) -> Result<bool>
where
    P: ShapeProbe,
{
    for (slot, id) in ids.iter().enumerate() {
        if state.deferred[slot].is_none() {
            let present: RowKeys = state.model[slot]
                .iter()
                .filter(|(_, value)| value.is_some())
                .map(|(&(s, c), _)| row_key(&cell_in(s, c)))
                .collect();
            if probe.cell_rows(id).await? != present {
                return Ok(false);
            }
        }
        let observed = probe.standing_marker(id).await?;
        let expected = state.marker_model[slot]
            .as_ref()
            .map(|(index, staged, clears)| {
                (
                    EventRef::Message {
                        dedup_id: Uuid::from_u128(*index),
                    },
                    staged.clone(),
                    clears.clone(),
                )
            });
        if observed != expected {
            return Ok(false);
        }
        let provisional = probe.provisional_rows(id).await?;
        match &observed {
            Some((_, listed, _)) => {
                if !provisional.is_subset(listed) {
                    return Ok(false);
                }
            }
            None => {
                if !provisional.is_empty() {
                    return Ok(false);
                }
            }
        }
    }
    Ok(true)
}

/// The per-event closing pass: convergence + row shape wait for deferred
/// collections; marker shape and staged coverage never wait (read-only
/// probes). The convergence pass's own reads resolve foreign clears-bearing
/// markers (read-help), which `note_read_help` folds into the marker model
/// before the physical probe.
async fn assert_event_end<S, P>(
    store: &S,
    ids: &[CollectionId],
    probe: &P,
    state: &mut TraceState,
) -> Result<bool>
where
    S: CellStore,
    P: ShapeProbe,
{
    let skip: Vec<bool> = state.deferred.iter().map(Option::is_some).collect();
    if !assert_converged(store, ids, &state.model, &skip).await? {
        return Ok(false);
    }
    state.note_read_help();
    assert_physical(probe, ids, state).await
}

/// The stage-fault arm: attempts one collection's stage with the lower fault
/// seam armed (`Poison::WriteProvisional`, transient), requiring the stage to
/// be rejected — the injected fault is not a runtime error: an `Ok` is a red
/// property. Prevs come from the model, never from store reads — a prev-read
/// could first-touch-resolve a lingering foreign stage the fault must leave
/// lingering; the rejected write lands nothing, so the staged prevs are never
/// observed.
async fn reject_stage<S>(
    store: &FailingCellStore<S>,
    lower: &PoisonHandle,
    refs: &[CollectionRef],
    state: &TraceState,
    event: EventRef,
    (coll, cells, cleared): &PlannedStage,
) -> Result<bool>
where
    S: CellStore,
{
    let slot = *coll as usize;
    let cell_writes: Vec<(CellKey, ProvisionalWrite)> = cells
        .iter()
        .map(|&((s, c), mutation)| {
            let prev = Committed::new(state.model[slot].get(&(s, c)).cloned().flatten());
            (
                cell_in(s, c),
                ProvisionalWrite::new(mutation.value(), prev, event),
            )
        })
        .collect();
    let clears: Vec<SectionClear> = cleared
        .iter()
        .map(|&s| SectionClear::frozen(SECTIONS[s as usize], &cell_writes))
        .collect();
    let marker = EventMarker::frozen(event, &cell_writes, &clears);
    *lower.lock() = Some(Poison::WriteProvisional(
        refs[slot].id().name().clone(),
        ErrorCategory::Transient,
    ));
    let attempted = store
        .write_provisional(&refs[slot], &cell_writes, Some(&marker))
        .await;
    *lower.lock() = None;
    Ok(attempted.is_err())
}

/// Drives `trace` through stores built by `make_store` (each wrapped in a
/// poison-armable [`FailingCellStore`]; the [`PoisonHandle`] argument is the
/// **lower** fault seam each instantiation buries beneath any cache in its
/// composition), asserting the committed projection equals the model after
/// every event regardless of the resolution path. A crash rebuilds the store
/// over the same warm backing the closure captures. Alongside the committed
/// projection it tracks a standing-event-marker model (owner + staged set +
/// clear half) checked with the staged-coverage postcondition after every
/// event ([`assert_physical`]); [`stage_event`]'s split dimension exercises
/// the same-event marker overwrite; the durable `clears` dimension (including
/// clears-only stages) exercises the gap erase and the committed-unapplied
/// read window (read-help); the `Defer` recovery, the depth-generated
/// `SettleFailure` outcome (with directed post-failure reads pinning the
/// Cov-Clr punch-first ordering), and the `stage_fault` dimension make
/// stage-over-a-standing-foreign-marker, committed-unapplied windows,
/// rejected stages, and fresh-assignment recovery arise organically. A final
/// sweep + full assertion pass closes every trace, so no trace ends
/// unchecked.
pub(crate) async fn run_crash_equivalence_trace<S, F, P>(
    make_store: F,
    oracle: ScriptedOracle,
    trace: Trace,
    probe: &P,
) -> Result<bool>
where
    S: CellStore,
    F: Fn(&PoisonHandle) -> Result<S>,
    P: ShapeProbe,
{
    let (ids, refs) = pooled_collections()?;
    let poison: PoisonHandle = Arc::default();
    let lower: PoisonHandle = Arc::default();
    let mut store = FailingCellStore::with_handle(make_store(&lower)?, poison.clone());
    let mut state = TraceState::new();

    for (index, ev) in trace.events.into_iter().enumerate() {
        let dedup_id = Uuid::from_u128(index as u128);
        let event = EventRef::Message { dedup_id };

        // Mid-fan-out tears at CELL granularity: a whole-cell prefix of the
        // stage landed, never a torn cell. Model it by dropping the last
        // touched collection's final cell (the cell whose batch never
        // committed), then dropping any collection left with neither cells
        // nor clears. The event marker of a torn stage lists exactly what it
        // staged, so the marker model uses the same truncated set.
        let mut staged = event_plan(&ev);
        if ev.outcome.mid_fan_out() {
            if let Some((_, cells, _)) = staged.last_mut() {
                cells.pop();
            }
            staged.retain(|(_, cells, cleared)| !cells.is_empty() || !cleared.is_empty());
        }

        // Stage fault: the event's first planned stage is rejected at the
        // lower store and the event is never dispatched — every model stays
        // untouched. Decided BEFORE the deferral-take below (a rejected
        // stage's boundary resolve never reached the bottom store, so a
        // lingering foreign stage still lingers). The convergence and
        // physical passes still run: this is where a phantom publish or a
        // lost standing marker would surface.
        if ev.stage_fault
            && let Some(plan) = staged.first()
        {
            if !reject_stage(&store, &lower, &refs, &state, event, plan).await?
                || !assert_event_end(&store, &ids, probe, &mut state).await?
            {
                return Ok(false);
            }
            continue;
        }

        // Restaging a deferred collection resolves its lingering stage — the
        // prev-reads first-touch-resolve overlapping cells (read-help resolves
        // a clears-bearing marker whole) and the stage boundary mops up the
        // rest, so apply the deferred verdict to the model BEFORE the prev
        // check. A warm deferral additionally permits a stale prev-read (see
        // `stage_event`).
        let mut stale_prev_ok = vec![false; POOL as usize];
        for (coll, ..) in &staged {
            if let Some(d) = state.deferred[*coll as usize].take() {
                stale_prev_ok[*coll as usize] = d.warm;
                if !d.committed {
                    state.apply_rollback(*coll as usize, &d.writes);
                }
            }
        }

        // Stage each collection's cell set over its committed base
        // (prev-is-committed at stage time). Keep the staged
        // `(cell, ProvisionalWrite)` set + frozen clears per collection so the
        // settle arms run through `commit_provisional` / `abort_provisional`
        // in one call too (carrying the projection the write-through cache
        // publishes).
        let Some(staged_writes) = stage_event(
            &store,
            &refs,
            &staged,
            &state.model,
            event,
            ev.split,
            &stale_prev_ok,
        )
        .await?
        else {
            return Ok(false);
        };
        // Each stage overwrites any lingering foreign marker with this event's
        // marker listing its full staged set and clear half.
        for (coll, cell_writes, clears) in &staged_writes {
            let (staged_set, clear_map) =
                probed_parts(&EventMarker::frozen(event, cell_writes, clears));
            state.marker_model[*coll as usize] = Some((index as u128, staged_set, clear_map));
        }

        // Commit marker strictly after staging; advance the model for
        // committed clears + cells (a committed clear's section reads absent
        // even while its gaps are still unapplied — reads are marker-resolved
        // truth).
        if ev.outcome.marker_flushed() {
            oracle.record_message(dedup_id).await?;
            for (coll, cells, cleared) in &staged {
                state.apply_committed(*coll as usize, cells, cleared);
            }
        }

        // Resolve along the outcome's path.
        if ev.outcome.is_crash() {
            // Crash = a cold store over the same warm backing.
            store = FailingCellStore::with_handle(make_store(&lower)?, poison.clone());
            if !state
                .recover_crash(&store, &oracle, &refs, &staged_writes, &ev, index)
                .await?
            {
                return Ok(false);
            }
        } else if !state
            .settle(&store, &lower, &refs, &staged_writes, &ev)
            .await?
        {
            return Ok(false);
        }

        if !assert_event_end(&store, &ids, probe, &mut state).await? {
            return Ok(false);
        }
    }

    // Final settle-everything pass: sweep every collection, resolve remaining
    // deferrals in the model, then run the full convergence + shape
    // assertions — no trace ends unchecked.
    for r in &refs {
        if !sweep_provisional(&store, &oracle, r).await? {
            return Ok(false);
        }
    }
    state.resolve_deferrals();
    state.marker_model.fill(None);
    let none = vec![false; POOL as usize];
    if !assert_converged(&store, &ids, &state.model, &none).await? {
        return Ok(false);
    }
    assert_physical(probe, &ids, &state).await
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
/// predecessor's still-provisional cell is resolved through the oracle — the
/// implicit-overwrite / first-touch path. A predecessor is resolved either at
/// the **next stage of its collection** (the stage-boundary resolve of the
/// standing foreign marker) or, when its collection is never re-staged, by the
/// final read. The staged `prev` must equal the model committed base, and at
/// the end every cell must equal the model. Both oracle arms run: a committing
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
        let marker = EventMarker::frozen(event, &cell_writes, &[]);
        store
            .write_provisional(
                &refs[slot],
                &cell_writes,
                (!cell_writes.is_empty()).then_some(&marker),
            )
            .await?;
        if op.commit {
            oracle.record_message(dedup_id).await?;
            for &(coord, mutation) in &cells {
                model[slot].insert(coord, mutation.value());
            }
        }
    }

    // Every cell still provisional (a collection never re-staged, so no stage
    // boundary resolved it), resolved by this final read, converges.
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
/// scans, dirty buffering, section clears, and committed writes so the
/// property exercises their interaction — a `scan` between a `buffer_set` and
/// a `clear`, a `get` after a dropped scan, a `clear_section` between commits
/// and buffers, and so on (TESTING.md "interleaved operations").
#[derive(Clone, Debug)]
enum OverlayOp {
    /// Buffer a set into the dirty leg at `(section idx, coord)`.
    BufferSet(u8, u8, u8),
    /// Buffer a clear into the dirty leg.
    BufferClear(u8, u8),
    /// Commit a present value to the committed lower store (resolved).
    CommitSet(u8, u8, u8),
    /// Commit a known-absent value to the lower store (resolved).
    CommitClear(u8, u8),
    /// Buffer a dirty clear marker over one sampled section: that section's
    /// committed cells vanish behind it (even ones committed later) while its
    /// siblings stay visible; post-clear `BufferSet`s are survivors.
    ClearSection(u8),
    /// Commit a durable section clear with survivors to the lower store
    /// (`write_resolved` with a frozen [`SectionClear`]) — the generated
    /// producer for the direct-apply clear leg beneath the overlay. On a
    /// `Cached` lower store this is the op that kills a missing
    /// `write_resolved` Cov-Clr punch: a covered pre-clear cell of the
    /// cleared section would serve stale and diverge on the next point leg.
    CommitClearSection(SeedClear),
    /// Run a range scan and assert it against the oracle (the range leg,
    /// intermixed with the point reads asserted after every op).
    Scan(ScanReq),
}

impl Arbitrary for OverlayOp {
    fn arbitrary(g: &mut Gen) -> Self {
        let s = section_idx(u8::arbitrary(g));
        let c = u8::arbitrary(g) % CELLS;
        match u8::arbitrary(g) % 7 {
            0 => Self::BufferSet(s, c, u8::arbitrary(g)),
            1 => Self::BufferClear(s, c),
            2 => Self::CommitSet(s, c, u8::arbitrary(g)),
            3 => Self::CommitClear(s, c),
            4 => Self::ClearSection(s),
            5 => Self::CommitClearSection(SeedClear::arbitrary(g)),
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

/// The visible-value model over the sampled section pool: dirty wins
/// (`Set`→present, `Cleared`→absent), a standing dirty clear marker hides the
/// committed leg **of its section only**, else the committed value (present or
/// absent). Keys are `(section idx, coord)`.
#[derive(Default)]
struct CellModel {
    committed: BTreeMap<(u8, u8), Option<Bytes>>,
    dirty: BTreeMap<(u8, u8), Option<Bytes>>,
    /// The section indices over which a dirty clear marker stands — each
    /// hides its own section's committed leg, and only that section's.
    cleared: BTreeSet<u8>,
}

impl CellModel {
    /// The visible committed bytes at `(section idx s, coordinate c)`.
    fn visible(&self, s: u8, c: u8) -> Option<Bytes> {
        match self.dirty.get(&(s, c)) {
            Some(value) => value.clone(),
            None if self.cleared.contains(&s) => None,
            None => self.committed.get(&(s, c)).cloned().flatten(),
        }
    }

    /// Section `s`'s visible cells in coordinate order (only present values)
    /// — a scan reads ONE section.
    fn visible_ordered(&self, s: u8) -> Vec<(u8, Bytes)> {
        let mut coords: Vec<u8> = self
            .committed
            .keys()
            .chain(self.dirty.keys())
            .filter(|&&(sect, _)| sect == s)
            .map(|&(_, c)| c)
            .collect();
        coords.sort_unstable();
        coords.dedup();
        coords
            .into_iter()
            .filter_map(|c| self.visible(s, c).map(|b| (c, b)))
            .collect()
    }
}

/// Drives random ops over an [`Overlay`] of a multi-cell collection — dirty
/// buffering, section clears, committed writes, and range scans
/// **intermixed** — asserting both the range leg (each `Scan` op vs the
/// sorted-map oracle, incl. early-stop) and the point leg (`get` per cell of
/// every sampled section vs the dirty-over-committed oracle, after **every**
/// op). This is the unified view property: point reads, range reads, and
/// writes interleave so their interaction is exercised, not just each in
/// isolation (dirty-wins, clear-hides, the dirty clear marker hiding the
/// lower leg **of exactly its section** — the [`SECTIONS`] sampling makes a
/// marker consulted at the wrong section visible to both legs — bounds,
/// direction, limit; unified-view soundness and oracle-correctness
/// properties).
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
            OverlayOp::BufferSet(s, c, b) => {
                overlay.dirty().set(&id, &cell_in(s, c), &bytes(b));
                model.dirty.insert((s, c), Some(bytes(b)));
            }
            OverlayOp::BufferClear(s, c) => {
                overlay.dirty().clear(&id, &cell_in(s, c));
                model.dirty.insert((s, c), None);
            }
            OverlayOp::CommitSet(s, c, b) => {
                // Committed seeds are written resolved (no event) so reads stay
                // pure dirty-over-committed.
                overlay
                    .lower()
                    .write_resolved(&collection_ref, &[(cell_in(s, c), Some(bytes(b)))], &[])
                    .await?;
                model.committed.insert((s, c), Some(bytes(b)));
            }
            OverlayOp::CommitClear(s, c) => {
                overlay
                    .lower()
                    .write_resolved(&collection_ref, &[(cell_in(s, c), None)], &[])
                    .await?;
                model.committed.insert((s, c), None);
            }
            OverlayOp::ClearSection(s) => {
                overlay
                    .dirty()
                    .clear_section(&id, SECTIONS[s as usize % SECTIONS.len()]);
                // The marker wipes ITS section's buffered cells and hides
                // that section's committed leg; siblings stay untouched;
                // later `BufferSet`s repopulate as survivors.
                model.dirty.retain(|&(sect, _), _| sect != s);
                model.cleared.insert(s);
            }
            OverlayOp::CommitClearSection(clear) => {
                // A committed write beneath the overlay: the cleared section's
                // committed leg collapses to exactly its survivors; the dirty
                // leg and its markers are untouched.
                seed_section_clear(overlay.lower(), &collection_ref, &mut model, clear).await?;
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

        // Point leg: after every op (mutation OR scan), every cell of every
        // sampled section `get`s the model's answer — so point reads
        // interleave with the scans above and the section scoping of every
        // dirty/committed interaction is checked cell-by-cell.
        for s in 0..SECTIONS.len() as u8 {
            for c in 0..CELLS {
                if overlay.get(&id, &cell_in(s, c), own).await?.into_inner() != model.visible(s, c)
                {
                    return Ok(false);
                }
            }
        }
    }
    Ok(true)
}

// ─────────────────────────── scan merge
// ───────────────────────────────────────

/// One scan-trace step: seed the model/store, durably clear a section, or run
/// a scan and assert it.
#[derive(Clone, Debug)]
enum ScanStep {
    Seed(SeedOp),
    SeedClear(SeedClear),
    Scan(ScanReq),
}

impl Arbitrary for ScanStep {
    fn arbitrary(g: &mut Gen) -> Self {
        match u8::arbitrary(g) % 6 {
            0..=2 => Self::Seed(SeedOp::arbitrary(g)),
            3 => Self::SeedClear(SeedClear::arbitrary(g)),
            _ => Self::Scan(ScanReq::arbitrary(g)),
        }
    }
}

/// A seed mutation interleaving committed and dirty cells at overlapping and
/// disjoint `(section idx, coord)`s.
#[derive(Clone, Copy, Debug)]
enum SeedOp {
    CommitSet(u8, u8, u8),
    CommitClear(u8, u8),
    DirtySet(u8, u8, u8),
    DirtyClear(u8, u8),
}

impl Arbitrary for SeedOp {
    fn arbitrary(g: &mut Gen) -> Self {
        let s = section_idx(u8::arbitrary(g));
        let c = u8::arbitrary(g) % CELLS;
        match u8::arbitrary(g) % 4 {
            0 => Self::CommitSet(s, c, u8::arbitrary(g)),
            1 => Self::CommitClear(s, c),
            2 => Self::DirtySet(s, c, u8::arbitrary(g)),
            _ => Self::DirtyClear(s, c),
        }
    }
}

/// A direct durable section clear (`write_resolved` with a frozen
/// [`SectionClear`]): the cleared section collapses to exactly the survivor
/// cells written alongside. Drives all four gap statements live — whole
/// section (no survivors), below the first, between adjacent, and above the
/// last — so the existing three-way scan parity + row-shape probe then cover
/// gap-tombstoned sections.
#[derive(Clone, Debug)]
struct SeedClear {
    sect: u8,
    /// Survivor `(coord, value)`s (deduped last-writer-wins in the runner).
    survivors: Vec<(u8, u8)>,
}

impl Arbitrary for SeedClear {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            sect: section_idx(u8::arbitrary(g)),
            survivors: capped_vec::<(u8, u8)>(g, 3)
                .into_iter()
                .map(|(c, v)| (c % CELLS, v))
                .collect(),
        }
    }
}

/// Apply a durable section clear to `store` and mirror it into `model`'s
/// committed leg. Survivors are deduped last-writer-wins (no batch timestamp
/// ties — a repeated coordinate would put two writes of one row in one batch),
/// written alongside the frozen [`SectionClear`], and the cleared section's
/// committed leg collapses to exactly those survivors. Shared by the overlay
/// trace's `CommitClearSection` and the bottom-scan trace's `SeedClear`, which
/// stage the identical durable clear beneath their respective readers.
async fn seed_section_clear<S: CellStore>(
    store: &S,
    collection: &CollectionRef,
    model: &mut CellModel,
    clear: SeedClear,
) -> Result<()> {
    let s = clear.sect;
    let mut survivors: BTreeMap<u8, u8> = BTreeMap::new();
    for (c, v) in clear.survivors {
        survivors.insert(c, v);
    }
    let cells: Vec<(CellKey, Option<Bytes>)> = survivors
        .iter()
        .map(|(&c, &v)| (cell_in(s, c), Some(bytes(v))))
        .collect();
    let section_clear =
        SectionClear::frozen_resolved(SECTIONS[s as usize % SECTIONS.len()], &cells);
    store
        .write_resolved(collection, &cells, slice::from_ref(&section_clear))
        .await?;
    model.committed.retain(|&(sect, _), _| sect != s);
    for (&c, &v) in &survivors {
        model.committed.insert((s, c), Some(bytes(v)));
    }
    Ok(())
}

/// A scan request over one sampled section with random anchor, direction,
/// bound exclusivity, optional end and limit, and an optional early-stop
/// prefix length.
#[derive(Clone, Copy, Debug)]
struct ScanReq {
    sect: u8,
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
            sect: section_idx(u8::arbitrary(g)),
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

/// The oracle scan result: `model`'s visible cells of the request's section
/// filtered to its range, ordered in the scan direction, truncated to the
/// limit.
fn scan_oracle(model: &CellModel, req: ScanReq) -> Vec<(u8, Bytes)> {
    let mut cells: Vec<(u8, Bytes)> = model
        .visible_ordered(req.sect)
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
        section: SECTIONS[req.sect as usize % SECTIONS.len()],
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

/// Drives interleaved committed seeds, durable section clears, and scans
/// **directly over a bottom store's `scan_cells`** (no overlay), pinning the
/// backend's own ordering, clustering-range bounds, and limit handling — the
/// Cassandra `ORDER BY ASC/DESC` + `coordinate` range the overlay merge
/// delegates to and the limit/end the overlay strips before delegating — plus
/// post-clear (gap-tombstoned) section states across the full Direction ×
/// exclusivity × limit space. Every seed is committed (`write_resolved`), so
/// the oracle is committed-only.
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
        let seeded = matches!(&step, ScanStep::Seed(_) | ScanStep::SeedClear(_));
        match step {
            // Every seed lands in committed state; a "dirty" seed becomes a
            // committed set/clear so the bottom store alone holds the data.
            ScanStep::Seed(SeedOp::CommitSet(s, c, b) | SeedOp::DirtySet(s, c, b)) => {
                store
                    .write_resolved(&collection_ref, &[(cell_in(s, c), Some(bytes(b)))], &[])
                    .await?;
                model.committed.insert((s, c), Some(bytes(b)));
            }
            ScanStep::Seed(SeedOp::CommitClear(s, c) | SeedOp::DirtyClear(s, c)) => {
                store
                    .write_resolved(&collection_ref, &[(cell_in(s, c), None)], &[])
                    .await?;
                model.committed.insert((s, c), None);
            }
            ScanStep::SeedClear(clear) => {
                seed_section_clear(&store, &collection_ref, &mut model, clear).await?;
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

        // After each seed (the direct `write_resolved` path: `None` ⇒ row
        // delete, a clear ⇒ gap erase): the stored `kind=Cell` rows across
        // both sampled sections equal the committed-present set, pinning the
        // ReadUncommitted-clear row-absence and gap-erase paths with no
        // oracle in the loop.
        if seeded {
            let present: RowKeys = model
                .committed
                .iter()
                .filter(|(_, value)| value.is_some())
                .map(|(&(s, c), _)| row_key(&cell_in(s, c)))
                .collect();
            if probe.cell_rows(&id).await? != present {
                return Ok(false);
            }
        }
    }
    Ok(true)
}

// ─────────────────────────── apply idempotence
// ────────────────────────────────

/// One step of the generated apply interleaving.
#[derive(Clone, Copy, Debug)]
enum ApplyOp {
    /// Resolve the standing marker as a unit (the sweep's marker leg) — a
    /// re-resolve after settlement exercises the exhausted-marker path.
    ResolveMarker,
    /// Re-apply the verdict-matching settle over the full staged set.
    Settle,
    /// First-touch one staged cell through `resolve_cell` (skipped when the
    /// cell is already resolved — the over-report-safe drop).
    FirstTouch(u8),
}

impl Arbitrary for ApplyOp {
    fn arbitrary(g: &mut Gen) -> Self {
        match u8::arbitrary(g) % 3 {
            0 => Self::ResolveMarker,
            1 => Self::Settle,
            _ => Self::FirstTouch(u8::arbitrary(g)),
        }
    }
}

/// Generated input for the apply-idempotence property: a committed pre-clear
/// base, one event's staged set + cleared sections (survivors frozen from the
/// staged set), a verdict, and a shuffled interleaving of re-applies and
/// first-touches.
#[derive(Clone, Debug)]
pub(crate) struct ApplyTrace {
    /// Committed base rows as `(section idx, coord, value)`.
    base: Vec<(u8, u8, u8)>,
    /// The staged set as `(section idx, coord, mutation)`.
    staged: Vec<(u8, u8, Mutation)>,
    /// Section indices the stage durably clears (deduped).
    cleared: Vec<u8>,
    committed: bool,
    ops: Vec<ApplyOp>,
}

impl Arbitrary for ApplyTrace {
    fn arbitrary(g: &mut Gen) -> Self {
        let base = capped_vec::<(u8, u8, u8)>(g, 8)
            .into_iter()
            .map(|(s, c, v)| (section_idx(s), c % CELLS, v))
            .collect();
        let staged = capped_vec::<(u8, u8, Mutation)>(g, 6)
            .into_iter()
            .map(|(s, c, m)| (section_idx(s), c % CELLS, m))
            .collect();
        let mut cleared: Vec<u8> = capped_vec::<u8>(g, 2)
            .into_iter()
            .map(section_idx)
            .collect();
        cleared.sort_unstable();
        cleared.dedup();
        Self {
            base,
            staged,
            cleared,
            committed: bool::arbitrary(g),
            ops: capped_vec(g, 12),
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        let this = self.clone();
        let ops = self.ops.shrink().map({
            let this = this.clone();
            move |ops| Self {
                ops,
                ..this.clone()
            }
        });
        let base = self.base.shrink().map({
            let this = this.clone();
            move |base| Self {
                base,
                ..this.clone()
            }
        });
        let staged = self.staged.shrink().map(move |staged| Self {
            staged,
            ..this.clone()
        });
        Box::new(ops.chain(base).chain(staged))
    }
}

/// Apply idempotence: settle re-application is a pure idempotent function of
/// the frozen staged data. Seeds a committed base, stages one event's writes
/// and section clears, records the verdict, then drives the generated
/// interleaving of `resolve_marker`, verdict-matching settle re-applies, and
/// per-cell `resolve_cell` first-touches — closing with one final settle so
/// every schedule ends fully settled. Postconditions: the committed
/// projection equals the verdict state (committed ⇒ cleared sections hold
/// exactly the survivors, staged data promoted, other sections keep the base
/// plus staged mutations; aborted ⇒ exactly the pre-stage base), no marker
/// stands, nothing is provisional, and the physical row shape matches.
pub(crate) async fn run_apply_idempotence<S, P>(
    store: S,
    oracle: ScriptedOracle,
    input: ApplyTrace,
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
        StateName::try_new("apply")?,
    );
    let collection = CollectionRef::new(id.clone(), None);
    let event = EventRef::Message {
        dedup_id: Uuid::from_u128(1),
    };

    // Committed pre-clear base (last-writer-wins per row).
    let mut base: BTreeMap<(u8, u8), Bytes> = BTreeMap::new();
    for &(s, c, v) in &input.base {
        base.insert((s, c), bytes(v));
    }
    let seed: Vec<(CellKey, Option<Bytes>)> = base
        .iter()
        .map(|(&(s, c), value)| (cell_in(s, c), Some(value.clone())))
        .collect();
    if !seed.is_empty() {
        store.write_resolved(&collection, &seed, &[]).await?;
    }

    // Stage the event's writes (prev = the committed base) + frozen clears.
    let mut staged_map: BTreeMap<(u8, u8), Mutation> = BTreeMap::new();
    for &(s, c, mutation) in &input.staged {
        staged_map.insert((s, c), mutation);
    }
    let writes: Vec<(CellKey, ProvisionalWrite)> = staged_map
        .iter()
        .map(|(&(s, c), mutation)| {
            let prev = Committed::new(base.get(&(s, c)).cloned());
            (
                cell_in(s, c),
                ProvisionalWrite::new(mutation.value(), prev, event),
            )
        })
        .collect();
    let clears: Vec<SectionClear> = input
        .cleared
        .iter()
        .map(|&s| SectionClear::frozen(SECTIONS[s as usize], &writes))
        .collect();
    if writes.is_empty() && clears.is_empty() {
        // Nothing staged and nothing cleared — no marker is written, so the
        // apply machinery is a no-op. Verify rather than skip: the committed
        // base must survive untouched (no marker, no provisional residue, and
        // every base row still reads its seeded value).
        let keys: BTreeSet<(u8, u8)> = base.keys().copied().collect();
        return assert_apply_settled(&store, probe, &id, &base, &keys).await;
    }
    let marker = EventMarker::frozen(event, &writes, &clears);
    store
        .write_provisional(&collection, &writes, Some(&marker))
        .await?;
    if input.committed {
        oracle.record_message(Uuid::from_u128(1)).await?;
    }

    // The generated interleaving, then one final verdict-matching settle so
    // every schedule ends fully settled.
    for op in &input.ops {
        match op {
            ApplyOp::ResolveMarker => {
                resolve_marker(&store, &oracle, &collection, &marker).await?;
            }
            ApplyOp::Settle => {
                reapply_settle(&store, &collection, input.committed, &writes, &clears).await?;
            }
            ApplyOp::FirstTouch(i) => {
                let Some((cell, _)) = writes.get(*i as usize % writes.len().max(1)) else {
                    continue;
                };
                if let Some(provisional) = store.provisional_cell_at(&id, cell).await? {
                    resolve_cell(&store, &oracle, &collection, cell, provisional).await?;
                }
            }
        }
    }
    reapply_settle(&store, &collection, input.committed, &writes, &clears).await?;

    // The verdict state: committed ⇒ cleared sections collapse to survivors
    // and staged mutations land; aborted ⇒ exactly the pre-stage base.
    let keys: BTreeSet<(u8, u8)> = base.keys().chain(staged_map.keys()).copied().collect();
    let mut expected = base;
    if input.committed {
        for &s in &input.cleared {
            expected.retain(|&(sect, _), _| sect != s);
        }
        for (&(s, c), mutation) in &staged_map {
            match mutation.value() {
                Some(value) => {
                    expected.insert((s, c), value);
                }
                None => {
                    expected.remove(&(s, c));
                }
            }
        }
    }
    assert_apply_settled(&store, probe, &id, &expected, &keys).await
}

/// The verdict-matching settle re-apply ([`run_apply_idempotence`]'s
/// idempotent subject): commit with the frozen clears, or abort.
async fn reapply_settle<S>(
    store: &S,
    collection: &CollectionRef,
    committed: bool,
    writes: &[(CellKey, ProvisionalWrite)],
    clears: &[SectionClear],
) -> Result<(), S::Error>
where
    S: CellStore,
{
    if committed {
        store.commit_provisional(collection, writes, clears).await
    } else {
        store.abort_provisional(collection, writes).await
    }
}

/// [`run_apply_idempotence`]'s postconditions: the physical row shape equals
/// the verdict state exactly, no marker stands, nothing is provisional, and
/// every touched key's committed projection matches.
async fn assert_apply_settled<S, P>(
    store: &S,
    probe: &P,
    id: &CollectionId,
    expected: &BTreeMap<(u8, u8), Bytes>,
    keys: &BTreeSet<(u8, u8)>,
) -> Result<bool>
where
    S: CellStore,
    P: ShapeProbe,
{
    let present: RowKeys = expected
        .keys()
        .map(|&(s, c)| row_key(&cell_in(s, c)))
        .collect();
    if probe.cell_rows(id).await? != present {
        return Ok(false);
    }
    if probe.standing_marker(id).await?.is_some() {
        return Ok(false);
    }
    if !probe.provisional_rows(id).await?.is_empty() {
        return Ok(false);
    }
    let probe_event = EventRef::Message {
        dedup_id: Uuid::from_u128(u128::MAX / 2),
    };
    for &(s, c) in keys {
        let committed = store.get(id, &cell_in(s, c), probe_event).await?;
        if committed.into_inner() != expected.get(&(s, c)).cloned() {
            return Ok(false);
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
        marker: Option<&'a EventMarker>,
    ) -> Result<(), Self::Error> {
        // One increment per collection-grain batch call (not one per cell).
        self.counts
            .write_provisional
            .fetch_add(1, Ordering::Relaxed);
        self.inner
            .write_provisional(collection, writes, marker)
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
pub(crate) enum Poison {
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
    /// arm (offset still commits, marker skipped, backstop armed) and the
    /// crash trace's stage-fault dimension.
    WriteProvisional(StateName, ErrorCategory),
    /// Direct-apply path: `write_resolved` fails with the given category for
    /// one named collection — the Cov3 pin's lower-write fault (a failed
    /// lower write must leave coverage untouched).
    WriteResolved(StateName, ErrorCategory),
}

/// A runtime-armable poison slot shared by a [`FailingCellStore`], its
/// clones, and the trace runner (which arms a settle failure for exactly one
/// settle and disarms after): `None` delegates cleanly.
pub(crate) type PoisonHandle = Arc<parking_lot::Mutex<Option<Poison>>>;

/// A [`CellStore`] wrapper whose `mark_resolved`/`commit_provisional`
/// (promote path) or `write_provisional` (stage path) fails for a chosen
/// target, delegating everything else to `inner`. Drives the session's
/// best-effort `commit_apply` (one poisoned cell yields `Incomplete` without
/// cancelling siblings), the manager's no-strand recovery (a failed
/// resolution leaves the backstop armed), the sweep's per-cell `try_fold`
/// failure arm, the settle boundary's finalize-`Skip` arm, and the generated
/// trace alphabet's settle-failure outcome (via the runtime [`PoisonHandle`]).
#[derive(Clone)]
pub(crate) struct FailingCellStore<S> {
    inner: S,
    poison: PoisonHandle,
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
        Self::armed(inner, Poison::Collection(poison, category))
    }

    /// Wraps `inner`, poisoning `mark_resolved` for each single-byte coordinate
    /// in `cells` with its mapped category (others resolve normally).
    pub(crate) fn with_cells(inner: S, cells: BTreeMap<u8, ErrorCategory>) -> Self {
        Self::armed(inner, Poison::Cells(cells))
    }

    /// Wraps `inner`, poisoning `write_provisional` with `category` for the
    /// `poison` collection — the stage path (`mark_resolved` stays healthy).
    pub(crate) fn failing_write_provisional(
        inner: S,
        poison: StateName,
        category: ErrorCategory,
    ) -> Self {
        Self::armed(inner, Poison::WriteProvisional(poison, category))
    }

    /// Wraps `inner` around a shared runtime `poison` slot — the trace
    /// runner's constructor, re-wrapping each crash-rebuilt store around one
    /// handle.
    pub(crate) fn with_handle(inner: S, poison: PoisonHandle) -> Self {
        Self { inner, poison }
    }

    fn armed(inner: S, poison: Poison) -> Self {
        Self::with_handle(inner, Arc::new(parking_lot::Mutex::new(Some(poison))))
    }

    /// Arms (`Some`) or disarms (`None`) the shared poison slot for
    /// subsequent ops on every clone sharing the handle.
    pub(crate) fn set_poison(&self, poison: Option<Poison>) {
        *self.poison.lock() = poison;
    }

    /// The category to inject when `mark_resolved` touches `cells`, or `None`.
    fn injected(&self, collection: &CollectionRef, cells: &[CellKey]) -> Option<ErrorCategory> {
        match &*self.poison.lock() {
            Some(Poison::Collection(name, category)) => {
                (*collection.id().name() == *name).then_some(*category)
            }
            Some(Poison::Cells(targets)) => cells
                .iter()
                .find_map(|c| targets.get(&coord_of(c)).copied()),
            Some(Poison::WriteProvisional(..) | Poison::WriteResolved(..)) | None => None,
        }
    }

    /// The category to inject when `write_provisional` touches `collection`,
    /// or `None`.
    fn injected_stage(&self, collection: &CollectionRef) -> Option<ErrorCategory> {
        match &*self.poison.lock() {
            Some(Poison::WriteProvisional(name, category)) => {
                (*collection.id().name() == *name).then_some(*category)
            }
            Some(Poison::Collection(..) | Poison::Cells(..) | Poison::WriteResolved(..)) | None => {
                None
            }
        }
    }

    /// The category to inject when `write_resolved` touches `collection`,
    /// or `None`.
    fn injected_resolved(&self, collection: &CollectionRef) -> Option<ErrorCategory> {
        match &*self.poison.lock() {
            Some(Poison::WriteResolved(name, category)) => {
                (*collection.id().name() == *name).then_some(*category)
            }
            Some(Poison::Collection(..) | Poison::Cells(..) | Poison::WriteProvisional(..))
            | None => None,
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
        marker: Option<&'a EventMarker>,
    ) -> Result<(), Self::Error> {
        if let Some(category) = self.injected_stage(collection) {
            return Err(FailCellError::Poison(category));
        }
        self.inner
            .write_provisional(collection, writes, marker)
            .await
            .map_err(FailCellError::Inner)
    }

    async fn write_resolved<'a>(
        &'a self,
        collection: &'a CollectionRef,
        cells: &'a [(CellKey, Option<Bytes>)],
        clears: &'a [SectionClear],
    ) -> Result<(), Self::Error> {
        if let Some(category) = self.injected_resolved(collection) {
            return Err(FailCellError::Poison(category));
        }
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
        let marker = EventMarker::frozen(event, &writes, &[]);
        inner
            .write_provisional(&collection, &writes, Some(&marker))
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
            let marker = EventMarker::frozen(event, &writes, &[]);
            store.write_provisional(r, &writes, Some(&marker)).await?;
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
    ///
    /// Because each stage's boundary resolves the predecessor's standing
    /// marker, **only the final op's cell can still be provisional**: a
    /// trailing `StageOnly` lingers, any trailing settle clears it. So the
    /// property also asserts both sets against that exact expected set,
    /// keeping teeth even when the intermediate sets collapse to trivial.
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

        // At most the final op's cell survives the stage-boundary resolves.
        let expected: BTreeSet<CellKey> = match ops.last() {
            Some(op @ SetOp::StageOnly(_)) => BTreeSet::from([cell_at(op.coord())]),
            _ => BTreeSet::new(),
        };

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
            let marker = EventMarker::frozen(event, &writes, &[]);
            warm.write_provisional(&collection, &writes, Some(&marker))
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
        Ok(warm_set == expected && cold_set == expected)
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
