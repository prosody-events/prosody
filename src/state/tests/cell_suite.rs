//! Backend-generic property suite for the uniform cell store.
//!
//! Every runner is generic over the [`CellStore`] backend and takes a
//! `make_store` closure (or a pre-built lower store) so memory
//! ([`Overlay<MemoryCellStore>`]) and Cassandra
//! ([`Overlay<Cached<CassandraStore>>`]) prove the *same* invariants from one
//! body — backend parity by transitivity through a deliberately simple model.
//!
//! The flagship is **crash-recovery equivalence** (invariant 1): a generated
//! trace stages provisional writes and resolves them one of five ways — clean
//! promote, clean inline rollback, or a crash at one of three points followed
//! by recovery through the sweep *or* first-touch. After every event the
//! committed projection must equal the model, whichever path ran. Companions
//! pin implicit overwrite (resolution-on-read); the **unified overlay view**
//! (`run_overlay_trace`), where point `get`s, range `scan`s, dirty buffering,
//! and committed writes are **intermixed** in one trace so their interaction is
//! exercised — dirty-wins, clear-hides, scan bounds / direction / limit /
//! early-stop (invariants 3, 5); the bottom-store scan primitive; and sweep
//! idempotence.
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
use super::super::cell_key::{CellKey, Coordinate, Direction, Scan, Section};
use super::super::dirty::DirtyStore;
use super::super::identity::{CollectionId, CollectionRef};
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
use std::collections::BTreeMap;
use std::convert::Infallible;
use std::error::Error;
use std::future::Future;
use std::ops::Bound;
use std::slice;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use uuid::Uuid;

/// Distinct collections a crash/overwrite trace cycles through. Small so events
/// collide on the same cell and exercise overwrite + resolution-on-read.
const POOL: u8 = 3;

/// Coordinate pool for the multi-cell overlay/scan suites — wide enough for
/// real intervals (a Map entry set / Deque index window).
const CELLS: u8 = 12;

/// Value's single section (`ValueNs::Entries = 0`); the overlay/scan suites
/// place every cell here and address by coordinate, mirroring a Map's entry
/// section.
const SECTION: Section = Section::new(0);

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

/// The single Value cell (`ValueNs::Entries`, empty coordinate).
fn value_cell() -> CellKey {
    CellKey {
        section: SECTION,
        coordinate: Coordinate::empty(),
    }
}

/// The cell at coordinate `c` in the shared section (single byte, so byte order
/// == numeric order — the in-memory oracle keys on `u8`).
fn cell_at(c: u8) -> CellKey {
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

/// One event: per-collection mutations, an outcome, and the recovery path to
/// use when the outcome is a crash.
#[derive(Clone, Debug)]
struct TraceEvent {
    writes: Vec<(u8, Mutation)>,
    outcome: Outcome,
    recover_by_sweep: bool,
}

impl Arbitrary for TraceEvent {
    fn arbitrary(g: &mut Gen) -> Self {
        let writes = capped_vec::<(u8, Mutation)>(g, POOL as usize)
            .into_iter()
            .map(|(coll, m)| (coll % POOL, m))
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

/// Collapses an event's writes to one per collection (an event stages a cell at
/// most once), keeping the last mutation — last-writer-wins.
fn collapse_writes(writes: Vec<(u8, Mutation)>) -> Vec<(u8, Mutation)> {
    let mut out: Vec<(u8, Mutation)> = Vec::new();
    for (coll, mutation) in writes {
        match out.iter_mut().find(|(c, _)| *c == coll) {
            Some(slot) => slot.1 = mutation,
            None => out.push((coll, mutation)),
        }
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

/// Asserts every pooled cell is fully resolved (no provisional lingers) and
/// projects its model committed value.
async fn assert_converged<S>(
    store: &S,
    ids: &[CollectionId],
    cell: &CellKey,
    model: &[Option<Bytes>],
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
        let committed = store.get(id, cell, probe).await?;
        if committed.into_inner() != *expected {
            return Ok(false);
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
///
/// # Errors
///
/// Propagates backend / oracle errors raised during the run.
pub(crate) async fn run_crash_equivalence_trace<S, F>(
    make_store: F,
    oracle: ScriptedOracle,
    trace: Trace,
) -> Result<bool>
where
    S: CellStore,
    F: Fn() -> Result<S>,
{
    let (ids, refs) = pooled_collections()?;
    let cell = value_cell();
    let mut store = make_store()?;
    let mut model: Vec<Option<Bytes>> = vec![None; POOL as usize];

    for (index, ev) in trace.events.into_iter().enumerate() {
        let dedup_id = Uuid::from_u128(index as u128);
        let event = EventRef::Message { dedup_id };

        let writes = collapse_writes(ev.writes);
        let staged_count = if ev.outcome.mid_fan_out() {
            writes.len().saturating_sub(1)
        } else {
            writes.len()
        };
        let staged = &writes[..staged_count];

        // Stage each cell over its committed base (prev-is-committed at stage
        // time: no provisional cell lingers between events). Keep the staged
        // `ProvisionalWrite` per collection so the clean arms can settle through
        // `commit_provisional` / `abort_provisional` (carrying the projection
        // the write-through cache publishes).
        let mut writes: Vec<(u8, ProvisionalWrite)> = Vec::with_capacity(staged.len());
        for &(coll, mutation) in staged {
            let prev = store.get(&ids[coll as usize], &cell, event).await?;
            let prev_value = prev.get().cloned();
            if prev_value != model[coll as usize] {
                return Ok(false);
            }
            let write = ProvisionalWrite::new(mutation.value(), prev, event);
            store
                .write_provisional(&refs[coll as usize], &[(cell.clone(), write.clone())])
                .await?;
            writes.push((coll, write));
        }

        // Marker strictly after staging; advance the model for committed cells.
        if ev.outcome.marker_flushed() {
            oracle.record_message(dedup_id).await?;
            for &(coll, mutation) in staged {
                model[coll as usize] = mutation.value();
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
            } else {
                // First-touch under a fresh event so own-event never
                // short-circuits; the read resolves the provisional cell.
                let recovery_event = EventRef::Message {
                    dedup_id: Uuid::from_u128(u128::MAX - index as u128),
                };
                for id in &ids {
                    store.get(id, &cell, recovery_event).await?;
                }
            }
        } else if matches!(ev.outcome, Outcome::CleanCommitted) {
            // Promote through the lifecycle settle path (publishes `data`).
            for (coll, write) in &writes {
                store
                    .commit_provisional(
                        &refs[*coll as usize],
                        slice::from_ref(&(cell.clone(), write.clone())),
                    )
                    .await?;
            }
        } else {
            // The only remaining non-crash outcome: clean inline rollback
            // through the settle path (publishes `prev`).
            for (coll, write) in &writes {
                store
                    .abort_provisional(
                        &refs[*coll as usize],
                        slice::from_ref(&(cell.clone(), write.clone())),
                    )
                    .await?;
            }
        }

        if !assert_converged(&store, &ids, &cell, &model).await? {
            return Ok(false);
        }
    }

    Ok(true)
}

// ─────────────────────────── implicit overwrite
// ───────────────────────────────

/// One overwrite-trace step: a mutation on a pooled collection, and whether the
/// event commits.
#[derive(Clone, Copy, Debug)]
struct OverwriteOp {
    coll: u8,
    mutation: Mutation,
    commit: bool,
}

impl Arbitrary for OverwriteOp {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            coll: u8::arbitrary(g) % POOL,
            mutation: Mutation::arbitrary(g),
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
///
/// # Errors
///
/// Propagates backend / oracle errors raised during the run.
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
    let cell = value_cell();
    let mut model: Vec<Option<Bytes>> = vec![None; POOL as usize];

    for (index, op) in trace.ops.into_iter().enumerate() {
        let slot = op.coll as usize;
        let dedup_id = Uuid::from_u128(index as u128);
        let event = EventRef::Message { dedup_id };
        // A fresh cold store: reads never hit a warm in-process cache, so every
        // overwrite resolves its predecessor's provisional cell durably.
        let store = make_store()?;

        let prev = store.get(&ids[slot], &cell, event).await?;
        if prev.get().cloned() != model[slot] {
            return Ok(false);
        }
        store
            .write_provisional(
                &refs[slot],
                &[(
                    cell.clone(),
                    ProvisionalWrite::new(op.mutation.value(), prev, event),
                )],
            )
            .await?;
        if op.commit {
            oracle.record_message(dedup_id).await?;
            model[slot] = op.mutation.value();
        }
    }

    // Every last provisional cell, resolved only by this final read, converges.
    let store = make_store()?;
    for (i, id) in ids.iter().enumerate() {
        let final_event = EventRef::Message {
            dedup_id: Uuid::from_u128(u128::MAX - i as u128),
        };
        if store.get(id, &cell, final_event).await?.into_inner() != model[i] {
            return Ok(false);
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
/// isolation (dirty-wins, clear-hides, bounds, direction, limit — invariants 3,
/// 5; DT7).
///
/// # Errors
///
/// Propagates backend errors raised during the run.
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
    let overlay = Overlay::new(Arc::new(DirtyStore::new()), lower, own);
    let mut model = CellModel::default();

    for op in trace.ops {
        match op {
            OverlayOp::BufferSet(c, b) => {
                overlay.buffer_set(&id, &cell_at(c), &bytes(b));
                model.dirty.insert(c, Some(bytes(b)));
            }
            OverlayOp::BufferClear(c) => {
                overlay.buffer_clear(&id, &cell_at(c));
                model.dirty.insert(c, None);
            }
            OverlayOp::CommitSet(c, b) => {
                // Committed seeds are written resolved (no event) so reads stay
                // pure dirty-over-committed.
                overlay
                    .lower()
                    .write_resolved(&collection_ref, &[(cell_at(c), Some(bytes(b)))])
                    .await?;
                model.committed.insert(c, Some(bytes(b)));
            }
            OverlayOp::CommitClear(c) => {
                overlay
                    .lower()
                    .write_resolved(&collection_ref, &[(cell_at(c), None)])
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
                    if collect_scan_prefix(&overlay, &id, &req, own, k).await? != expected[..k] {
                        return Ok(false);
                    }
                }
                if collect_scan(&overlay, &id, &req, own).await? != expected {
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
    end: Option<u8>,
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
            end: bool::arbitrary(g).then(|| u8::arbitrary(g) % (CELLS + 1)),
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
    let within_end = req.end.is_none_or(|end| match (req.forward, req.end_excl) {
        (true, false) => c <= end,
        (true, true) => c < end,
        (false, false) => c >= end,
        (false, true) => c > end,
    });
    on_start_side && within_end
}

/// Builds the [`Scan`] request from a [`ScanReq`] and owned anchor coordinates.
/// `req.start_excl`/`req.end_excl` choose the bound exclusivity; an absent
/// `end` is `Unbounded`.
fn scan_of<'a>(req: ScanReq, start: &'a Coordinate, end: Option<&'a Coordinate>) -> Scan<'a> {
    let start = if req.start_excl {
        Bound::Excluded(start)
    } else {
        Bound::Included(start)
    };
    let end = match end {
        Some(end) if req.end_excl => Bound::Excluded(end),
        Some(end) => Bound::Included(end),
        None => Bound::Unbounded,
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
async fn collect_scan<S>(
    overlay: &Overlay<S>,
    id: &CollectionId,
    req: &ScanReq,
    own: EventRef,
) -> Result<Vec<(u8, Bytes)>>
where
    S: CellStore,
{
    let start = Coordinate::from_bytes(vec![req.start]);
    let end = req.end.map(|e| Coordinate::from_bytes(vec![e]));
    let stream = overlay.scan_cells(id, scan_of(*req, &start, end.as_ref()), own);
    futures::pin_mut!(stream);
    let mut out = Vec::new();
    while let Some(item) = stream.next().await {
        let (key, value) = item?;
        out.push((coord_of(&key), value));
    }
    Ok(out)
}

/// Collects only the first `k` items of an overlay scan, then drops the stream.
async fn collect_scan_prefix<S>(
    overlay: &Overlay<S>,
    id: &CollectionId,
    req: &ScanReq,
    own: EventRef,
    k: usize,
) -> Result<Vec<(u8, Bytes)>>
where
    S: CellStore,
{
    let start = Coordinate::from_bytes(vec![req.start]);
    let end = req.end.map(|e| Coordinate::from_bytes(vec![e]));
    let stream = overlay.scan_cells(id, scan_of(*req, &start, end.as_ref()), own);
    futures::pin_mut!(stream);
    let mut out = Vec::new();
    while out.len() < k {
        let Some(item) = stream.next().await else {
            break;
        };
        let (key, value) = item?;
        out.push((coord_of(&key), value));
    }
    // Stream dropped here — dropping mid-scan must corrupt nothing.
    Ok(out)
}

/// Drives interleaved committed seeds and scans **directly over a bottom
/// store's `scan_cells`** (no overlay), pinning the backend's own ordering,
/// clustering-range bounds, and limit handling — the Cassandra `ORDER BY
/// ASC/DESC` + `coordinate` range the overlay merge delegates to and the
/// limit/end the overlay strips before delegating. Every seed is committed
/// (`write_resolved`), so the oracle is committed-only.
///
/// # Errors
///
/// Propagates backend errors raised during the run.
pub(crate) async fn run_bottom_scan_trace<S>(store: S, trace: ScanTrace) -> Result<bool>
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
    let mut model = CellModel::default();

    for step in trace.steps {
        match step {
            // Every seed lands in committed state; a "dirty" seed becomes a
            // committed set/clear so the bottom store alone holds the data.
            ScanStep::Seed(SeedOp::CommitSet(c, b) | SeedOp::DirtySet(c, b)) => {
                store
                    .write_resolved(&collection_ref, &[(cell_at(c), Some(bytes(b)))])
                    .await?;
                model.committed.insert(c, Some(bytes(b)));
            }
            ScanStep::Seed(SeedOp::CommitClear(c) | SeedOp::DirtyClear(c)) => {
                store
                    .write_resolved(&collection_ref, &[(cell_at(c), None)])
                    .await?;
                model.committed.insert(c, None);
            }
            ScanStep::Scan(req) => {
                let expected = scan_oracle(&model, req);
                let start = Coordinate::from_bytes(vec![req.start]);
                let end = req.end.map(|e| Coordinate::from_bytes(vec![e]));
                let stream = store.scan_cells(&id, scan_of(req, &start, end.as_ref()), own);
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
    get: AtomicUsize,
    scan_cells: AtomicUsize,
}

impl<S> CountingCellStore<S> {
    pub(crate) fn new(inner: S) -> Self {
        Self {
            inner,
            counts: Arc::new(OpCounts::default()),
        }
    }

    /// Total durable mutations (excludes reads).
    pub(crate) fn durable_writes(&self) -> usize {
        self.counts.write_provisional.load(Ordering::Relaxed)
            + self.counts.write_resolved.load(Ordering::Relaxed)
            + self.counts.mark_resolved.load(Ordering::Relaxed)
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

    pub(crate) fn reset(&self) {
        self.counts.write_provisional.store(0, Ordering::Relaxed);
        self.counts.write_resolved.store(0, Ordering::Relaxed);
        self.counts.mark_resolved.store(0, Ordering::Relaxed);
        self.counts.get.store(0, Ordering::Relaxed);
        self.counts.scan_cells.store(0, Ordering::Relaxed);
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
        self.inner.provisional_cells(collection)
    }

    async fn write_provisional<'a>(
        &'a self,
        collection: &'a CollectionRef,
        writes: &'a [(CellKey, ProvisionalWrite)],
    ) -> Result<(), Self::Error> {
        // One increment per collection-grain batch call (not one per cell).
        self.counts
            .write_provisional
            .fetch_add(1, Ordering::Relaxed);
        self.inner.write_provisional(collection, writes).await
    }

    async fn write_resolved<'a>(
        &'a self,
        collection: &'a CollectionRef,
        cells: &'a [(CellKey, Option<Bytes>)],
    ) -> Result<(), Self::Error> {
        self.counts.write_resolved.fetch_add(1, Ordering::Relaxed);
        self.inner.write_resolved(collection, cells).await
    }

    async fn mark_resolved<'a>(
        &'a self,
        collection: &'a CollectionRef,
        cells: &'a [CellKey],
    ) -> Result<(), Self::Error> {
        self.counts.mark_resolved.fetch_add(1, Ordering::Relaxed);
        self.inner.mark_resolved(collection, cells).await
    }
}

/// A [`CellStore`] wrapper whose `mark_resolved` fails **permanently** for one
/// named collection, delegating everything else to `inner`. Drives the
/// session's best-effort `commit_apply` (one poisoned cell yields `Incomplete`
/// without cancelling siblings) and the manager's no-strand recovery (a failed
/// resolution leaves the backstop armed).
#[derive(Clone)]
pub(crate) struct FailingCellStore<S> {
    inner: S,
    poison: StateName,
}

impl<S> FailingCellStore<S> {
    /// Wraps `inner`, poisoning `mark_resolved` for the `poison` collection.
    pub(crate) fn new(inner: S, poison: StateName) -> Self {
        Self { inner, poison }
    }
}

/// Error of a [`FailingCellStore`]: the poison, or a delegated inner error.
#[derive(Debug, thiserror::Error)]
pub(crate) enum FailCellError<E>
where
    E: Error + 'static,
{
    /// The poisoned collection's `mark_resolved` was called.
    #[error("permanent promote poison")]
    Poison,

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
            Self::Poison => ErrorCategory::Permanent,
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

    async fn write_provisional<'a>(
        &'a self,
        collection: &'a CollectionRef,
        writes: &'a [(CellKey, ProvisionalWrite)],
    ) -> Result<(), Self::Error> {
        self.inner
            .write_provisional(collection, writes)
            .await
            .map_err(FailCellError::Inner)
    }

    async fn write_resolved<'a>(
        &'a self,
        collection: &'a CollectionRef,
        cells: &'a [(CellKey, Option<Bytes>)],
    ) -> Result<(), Self::Error> {
        self.inner
            .write_resolved(collection, cells)
            .await
            .map_err(FailCellError::Inner)
    }

    async fn mark_resolved<'a>(
        &'a self,
        collection: &'a CollectionRef,
        cells: &'a [CellKey],
    ) -> Result<(), Self::Error> {
        if *collection.id().name() == self.poison {
            return Err(FailCellError::Poison);
        }
        self.inner
            .mark_resolved(collection, cells)
            .await
            .map_err(FailCellError::Inner)
    }
}

#[cfg(test)]
mod sweep {
    use super::super::super::memory::{MemoryCellStore, MemoryCells};
    use super::super::super::registry::CollectionDefRegistry;
    use super::*;

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
            store
                .write_provisional(
                    r,
                    &[(
                        cell.clone(),
                        ProvisionalWrite::new(Some(bytes(7)), prev, event),
                    )],
                )
                .await?;
            oracle.record_message(dedup_id).await?;
        }

        // First sweep resolves every cell.
        for r in &refs {
            assert!(sweep_provisional(&store, &oracle, r).await?);
        }
        assert!(
            store.durable_writes() > 0,
            "first sweep must resolve provisional cells"
        );

        // Second sweep is a no-op: no provisional cell remains to resolve.
        store.reset();
        for r in &refs {
            assert!(sweep_provisional(&store, &oracle, r).await?);
        }
        assert_eq!(store.durable_writes(), 0);
        Ok(())
    }
}
