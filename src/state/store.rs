//! The durable cell-store backend trait.
//!
//! [`CellStore`] is the single, uniform, **untyped** durable backend interface
//! for keyed state. It names no collection family: cells are addressed by
//! [`CellKey`] (a [`Section`] + ordered
//! [`Coordinate`]), so Value/Map/Deque are
//! collection-layer handles over this one trait and the durability layer is
//! written exactly once.
//!
//! Its currency is the resolved [`Committed`] cell: `get` and `scan_cells`
//! oracle-resolve any in-flight provisional cell **inside the backend** before
//! yielding, so callers above it (the `Overlay`
//! dirty overlay, the [`Cached`](super::cached::Cached) write-through cache)
//! are oracle-free and merely delegate down. The `own: EventRef` argument lets
//! the bottom store short-circuit to `prev` for the running handler's own
//! provisional cell without an oracle consult (the own-event-base-is-prev
//! invariant); the per-event session injects it, so collections never pass it.
//!
//! # Collection-grain atomicity invariant
//!
//! The three mutators work at **collection grain**: each takes the touched
//! cells of one collection as a slice. The invariant every backend upholds is
//! **atomic multi-cell commit** — a single `write_provisional` /
//! `write_resolved` / `mark_resolved` call (and `commit_provisional` /
//! `abort_provisional` — each verb's doc states its routing)
//! applies *all* its cells
//! together, so no reader and no crash-recovery ever observes a torn subset
//! (some cells written, others not), and on the Cassandra backend every cell
//! shares one write timestamp and one TTL anchor (keyset and entries
//! co-expire).
//!
//! * **Cassandra** packs the cells into **one same-partition `UNLOGGED
//!   BATCH`**: a collection's cells share its row key, so the batch is a single
//!   atomic replica mutation — one round-trip, not one per cell. The **sole**
//!   split is the over-budget fallback: a collection whose cells exceed the
//!   backend batch budget is divided into the *fewest* atomic batches that fit.
//!   The accepted consequence is narrow — an **over-budget** `ReadUncommitted`
//!   multi-cell *resolved* write (which arms no recovery backstop) can crash
//!   between chunks, leaving a torn committed write recovery cannot
//!   reconstruct; within budget that window does not exist, and a staged
//!   (`write_provisional`) write is always recoverable regardless of chunking.
//! * **Memory** loops its writes cell by cell. It needs no batch: one handler
//!   per key system-wide means no observer can witness a partial multi-cell
//!   write, and an in-memory loop never crashes mid-write — atomicity holds by
//!   serialization, not by a transaction.
use super::cell::{Committed, ProvisionalCell, ProvisionalWrite};
use super::cell_key::{CellKey, Coordinate, Scan, Section};
use super::event_ref::EventRef;
use super::identity::{CollectionId, CollectionRef};
use super::marker::{EventMarker, SectionClear};
use crate::error::ClassifyError;
use crate::timers::duration::CompactDuration;
use bytes::Bytes;
use futures::Stream;
use std::error::Error;
use std::future::Future;

pub(crate) use super::store_helpers::{
    dedupe, expand_to_input_order, provisional_point_loop, section_batches,
    sorted_unique_coordinates,
};
pub(crate) use super::store_types::CELL_BATCH;
pub use super::store_types::{CacheBatch, CellBuffer, CommittedBatch, CoordinateBatch};

/// Uniform durable storage for the cells of one collection partition.
///
/// `get` is a resolving point read and `scan_cells` a resolving single-section
/// range stream; `provisional_cells` is the whole-partition recovery scan. The
/// three mutators take a collection's touched cells as a batch and map onto the
/// durability sequence:
///
/// * [`Self::write_provisional`] — *stage*: writes `data | prev | event` for
///   each cell (the `ReadCommitted` outcome path).
/// * [`Self::write_resolved`] — writes committed values with `event` and `prev`
///   null (the `ReadUncommitted` direct write, the mid-handler `commit()`, and
///   abort resolution, where the committed value written back is the staged
///   `prev`).
/// * [`Self::mark_resolved`] — *promote*: nulls `event` and `prev`, keeping
///   `data`. O(1) regardless of value size.
///
/// # Committed absence is row absence
///
/// **Invariant:** a committed-absent cell is stored as *no row*. Every path
/// that resolves a cell to absent deletes the row rather than leaving a
/// null-blob residue. This is why [`Self::write_resolved`] partitions on data
/// presence (a `None` value deletes) and why [`Self::commit_provisional`]
/// routes an absent-data promote to `write_resolved(cell, None)` instead of the
/// value-preserving [`Self::mark_resolved`] verb.
pub trait CellStore: Clone + Send + Sync + 'static {
    /// Error type for cell-store operations.
    type Error: ClassifyError + Error + Send + Sync + 'static;

    /// Reads one cell's visible committed value, resolving an in-flight
    /// provisional cell through the oracle (or short-circuiting to its `prev`
    /// when `own` owns it). A missing row resolves to `Committed(None)`.
    ///
    /// # The committed-unapplied read window
    ///
    /// Reads return **marker-resolved truth**: a standing **foreign** event
    /// marker that carries section clears is resolved through the sweep path
    /// (`resolve_prior_clear_before_read` in `resolve`) before the read is
    /// served, so a committed-but-unapplied clear can never serve pre-clear
    /// rows. Markers without clears are left standing — first-touch
    /// resolution stays cell-grained and marker-free. [`Self::scan_cells`]
    /// and the cache-fill twin [`Self::get_for_cache`] share the same
    /// implementation, so the contract holds across all three reads.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] on a store failure, a corrupt row shape, or an
    /// oracle failure.
    fn get<'a>(
        &'a self,
        collection: &'a CollectionId,
        cell: &'a CellKey,
        own: EventRef,
    ) -> impl Future<Output = Result<Committed, Self::Error>> + Send + 'a;

    /// Complete, ordered, single-section scan (start positional, section
    /// required). Provisional cells in range are oracle-resolved here; cleared
    /// or absent cells are skipped, so the stream yields only present committed
    /// bytes in `coordinate` byte order. Returns marker-resolved truth exactly
    /// as [`Self::get`] does (the committed-unapplied read window, stated
    /// there).
    fn scan_cells<'a>(
        &'a self,
        collection: &'a CollectionId,
        scan: Scan<'a>,
        own: EventRef,
    ) -> impl Stream<Item = Result<(CellKey, Bytes), Self::Error>> + Send + 'a;

    /// Cache-fill point read: the committed value **plus** the durable cell's
    /// remaining TTL, for the [`Cached`](super::cached::Cached) write-through
    /// cache to mirror with a co-expiring fjall entry. `None` TTL means the
    /// durable row has no expiry (the fjall entry is stamped "never expires").
    ///
    /// Backends with no per-write TTL inherit the default: the committed value
    /// from [`Self::get`] with a `None` TTL. Only the Cassandra store overrides
    /// it (selecting the TTL of whichever blob resolution returns); the TTL is
    /// a best-effort hint, so a wrong or missing value only makes the cache
    /// fall through early, never stale.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] on any failure [`Self::get`] would.
    fn get_for_cache<'a>(
        &'a self,
        collection: &'a CollectionId,
        cell: &'a CellKey,
        own: EventRef,
    ) -> impl Future<Output = Result<(Committed, Option<CompactDuration>), Self::Error>> + Send + 'a
    {
        async move { Ok((self.get(collection, cell, own).await?, None)) }
    }

    /// Batch twin of [`Self::get`]: resolves one section's coordinates in one
    /// backend hop, answering each input position by index (`result[i]` answers
    /// `batch[i]`; a missing row is `Committed(None)`).
    ///
    /// # Read contract (the observation rules)
    ///
    /// This is **not** naive point-sequence equivalence — it is the weaker,
    /// backend-uniform contract every implementation upholds:
    ///
    /// * **Input positions** — the result has exactly `batch.len()` entries.
    ///   `result[i]` answers `batch[i]`.
    /// * **Dedup + co-observation** — a repeated coordinate is read once; all
    ///   its positions answer identically.
    /// * **First-occurrence ordering** — unique coordinates are resolved in the
    ///   order of their first appearance in `batch`, so among the **per-row
    ///   semantic failures** (a corrupt row shape, an oracle consult) the one
    ///   surfaced is the earliest input position's. A backend may additionally
    ///   fail the batch as a whole *before* any row resolves (the Cassandra
    ///   override's `IN` query, its standing-marker read, or the read-window
    ///   re-issue); such a whole-collection failure carries **no** input
    ///   position, exactly as the point [`Self::get`] surfaces the same failure
    ///   with no cell attribution.
    ///
    /// The default reads each unique coordinate through [`Self::get`] in
    /// first-occurrence order and expands the answer to every duplicate
    /// position; a failing coordinate fails the whole batch at its earliest
    /// occurrence (the memory default has no whole-collection phase). The
    /// Cassandra backend overrides it with one `IN` query. Every internal
    /// scratch buffers use [`CellBuffer`], so small calls stay inline while a
    /// full batch spills rather than inflating this future.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] on the first per-row failure [`Self::get`] would
    /// (at the earliest input position) or a whole-collection read failure (no
    /// position).
    fn get_many<'a>(
        &'a self,
        collection: &'a CollectionId,
        section: Section,
        batch: &'a CoordinateBatch,
        own: EventRef,
    ) -> impl Future<Output = Result<CommittedBatch, Self::Error>> + Send + 'a {
        async move {
            let (unique_coordinates, input_indices) = dedupe(batch);
            let mut unique_answers = CommittedBatch::with_capacity(unique_coordinates.len());
            for &coordinate in &unique_coordinates {
                let cell = CellKey {
                    section,
                    coordinate: Coordinate::clone(coordinate),
                };
                unique_answers.push(self.get(collection, &cell, own).await?);
            }
            Ok(expand_to_input_order(&input_indices, &unique_answers))
        }
    }

    /// Cache-fill batch twin of [`Self::get_for_cache`]: the batch read plus
    /// each position's remaining TTL, for the
    /// [`Cached`](super::cached::Cached) write-through cache to mirror.
    /// Same input-position contract as [`Self::get_many`].
    ///
    /// The default reads each unique coordinate through [`Self::get_for_cache`]
    /// (**not** [`Self::get_many`] with `None` TTLs — that would silently drop
    /// a backend's TTL metadata, the `commit_provisional`-wrapper bug
    /// class) in first-occurrence order and expands the `(value, ttl)`
    /// pair to every duplicate position. Only the Cassandra store overrides
    /// it, sharing one prepared statement with [`Self::get_many`].
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] on the first failure [`Self::get_for_cache`]
    /// would, at the earliest input position.
    fn get_many_for_cache<'a>(
        &'a self,
        collection: &'a CollectionId,
        section: Section,
        batch: &'a CoordinateBatch,
        own: EventRef,
    ) -> impl Future<Output = Result<CacheBatch, Self::Error>> + Send + 'a {
        async move {
            let (unique_coordinates, input_indices) = dedupe(batch);
            let mut unique_answers = CacheBatch::with_capacity(unique_coordinates.len());
            for &coordinate in &unique_coordinates {
                let cell = CellKey {
                    section,
                    coordinate: Coordinate::clone(coordinate),
                };
                unique_answers.push(self.get_for_cache(collection, &cell, own).await?);
            }
            Ok(expand_to_input_order(&input_indices, &unique_answers))
        }
    }

    /// Streams the whole partition's provisional cells (all sections) for the
    /// recovery sweep, filtering resolved rows in code.
    ///
    /// This is the **cold** recovery source: on the Cassandra store, the
    /// event-marker point read (memoized per assignment) followed by one raw
    /// `IN` batch read per `<=CELL_BATCH` section chunk
    /// ([`provisional_many`](Self::provisional_many)) — cost ∝ #provisional,
    /// never partition size. The warm short-circuit that skips it on a
    /// quiescent sweep lives on [`Cached`](super::cached::Cached).
    fn provisional_cells<'a>(
        &'a self,
        collection: &'a CollectionId,
    ) -> impl Stream<Item = Result<(CellKey, ProvisionalCell), Self::Error>> + Send + 'a;

    /// Point-reads one coordinate's provisional cell, or `None` when it is
    /// absent or resolved (over-report-safe). The single-coordinate primitive
    /// that [`provisional_point_loop`] fans out over to
    /// supply [`provisional_many`](Self::provisional_many) for backends with no
    /// native batch read (the memory store, test doubles); a backend with a
    /// native `IN` read implements [`provisional_many`](Self::provisional_many)
    /// directly and never routes through this.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] on a store failure.
    fn provisional_cell_at<'a>(
        &'a self,
        collection: &'a CollectionId,
        cell: &'a CellKey,
    ) -> impl Future<Output = Result<Option<ProvisionalCell>, Self::Error>> + Send + 'a;

    /// Batch twin of [`Self::provisional_cell_at`]: the provisional cells among
    /// the batch's **distinct** coordinates in one section, each tagged with
    /// its coordinate, in **ascending coordinate order**.
    ///
    /// Order is defined on the OUTPUT, so a duplicate coordinate is
    /// indistinguishable from a single mention and there is no sorted-unique
    /// input precondition; [`CoordinateBatch`] is non-empty, so there is no
    /// empty-input case. Absent and already-resolved coordinates are omitted
    /// (over-report-safe), with exact [`Self::provisional_cell_at`] parity for
    /// malformed / partially-expired rows — the raw decoder, never a visible
    /// resolve. Surviving rows retain `data`/`prev`/[`EventRef`] **without
    /// consulting the oracle, writing durable state, or publishing into the
    /// committed-value cache** — this is the raw recovery-reconstruction read,
    /// not a resolving one.
    ///
    /// A whole-batch failure carries no input position (as
    /// [`Self::get_many`]'s whole-collection failures). A per-row decode
    /// failure fails the batch with the **lowest failing coordinate's**
    /// error — the backends read in ascending coordinate order so this is
    /// deterministic. One collection partition and one section per call.
    ///
    /// Required with no default: a default inherited by a wrapper (notably
    /// [`Cached`](super::cached::Cached), whose committed-value cache cannot
    /// answer a raw provisional read) would loop [`Self::provisional_cell_at`]
    /// — N uncached point reads that silently defeat the batch. Making it
    /// required turns a forgotten forward into a compile error, as the
    /// settle verbs do.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] on a store failure or a per-row decode failure
    /// (at the lowest failing coordinate).
    fn provisional_many<'a>(
        &'a self,
        collection: &'a CollectionId,
        section: Section,
        batch: &'a CoordinateBatch,
    ) -> impl Future<Output = Result<CellBuffer<(Coordinate, ProvisionalCell)>, Self::Error>> + Send + 'a;

    /// Stages each `(cell, write)`'s provisional cell (`data | prev | event`)
    /// in one same-partition batch, binding `collection`'s TTL, and
    /// creates/overwrites the collection's **event marker** from the
    /// pre-frozen `marker` — the durable recovery handle naming the staging
    /// event, the coordinates it staged, and the sections it cleared.
    ///
    /// # The event-marker lifecycle (stated once, here)
    ///
    /// Exactly one verb family owns the marker: this verb (create/overwrite),
    /// and [`commit_provisional`](Self::commit_provisional) /
    /// [`abort_provisional`](Self::abort_provisional) / the recovery sweep
    /// (delete). [`write_resolved`](Self::write_resolved) and
    /// [`mark_resolved`](Self::mark_resolved) never touch it — there is nothing
    /// provisional to recover — so a fresh-commit no-op marker write is
    /// unrepresentable.
    ///
    /// `marker` carries the event's **whole** per-collection staged set
    /// (`writes ⊆ marker.staged()`), so a stage that splits over the byte
    /// budget passes the same union marker with every chunk rather than
    /// stranding a coordinate (an unlisted durable row is invisible to the
    /// recovery sweep). The session freezes it once per collection at
    /// `finalize`; only a retry attempt re-running `finalize` re-stages,
    /// replacing the same-event marker idempotently — handlers are assumed
    /// deterministic across retries. `None` is the explicit empty-stage no-op
    /// (`writes` must be empty): no marker, no boundary check — nothing to
    /// strand. A **clears-only** stage is representable as `writes = []` with
    /// a marker whose `staged()` is empty and `clears()` non-empty; it writes
    /// the marker and runs the boundary check like any stage, because a
    /// committed-unapplied clear is recoverable state.
    ///
    /// Before staging, the backend resolves any standing marker naming a
    /// **different** event — the stage-boundary rule that keeps marker
    /// uniqueness per collection an invariant; a resolution failure fails the
    /// stage (the retry middleware handles it).
    ///
    /// The marker's `clears` are frozen here and **applied at settle** (see
    /// [`commit_provisional`](Self::commit_provisional)), so re-apply during
    /// recovery stays a pure function of durable staged data. The session's
    /// `finalize` is the live producer: it freezes each cleared section's
    /// survivors from the collection's staged writes.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] on a store failure.
    fn write_provisional<'a>(
        &'a self,
        collection: &'a CollectionRef,
        writes: &'a [(CellKey, ProvisionalWrite)],
        marker: Option<&'a EventMarker>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;

    /// Writes each `(cell, data)` as a resolved cell in one same-partition
    /// batch, binding `collection`'s TTL, partitioning internally on data
    /// presence: `Some(data)` writes the committed value (`event`/`prev` null);
    /// `None` **deletes the row** (the row-absence invariant). Handles the
    /// `ReadUncommitted` direct clear, the mid-handler `commit()` of a clear,
    /// and rollback-to-absent. Never touches the event marker (see
    /// [`write_provisional`](Self::write_provisional)).
    ///
    /// `clears` names sections to erase before `cells` land — the direct-apply
    /// twin of a staged clear: every non-survivor row of each cleared section
    /// is deleted (on Cassandra as gap range tombstones between the sorted
    /// survivors), with survivors excluded **positionally** via the frozen
    /// list. The caller derives each clear's survivors from `cells`' own
    /// present-data coordinates — the single survivor definition — so no
    /// written row can overlap a gap range (batches stay row-disjoint).
    ///
    /// # The committed-unapplied write window
    ///
    /// Before writing, each bottom store resolves any standing clears-bearing
    /// event marker (`resolve_unsettled_clear_before_write` in `resolve`, the
    /// write-side twin of the read window on [`Self::get`]), ordering this
    /// write after that resolution so a stale clear's positional replay
    /// cannot erase it — subject to the concurrent-resolver residual stated
    /// on `resolve_unsettled_clear_before_write`. The settle verbs bypass
    /// this boundary (they delete the very marker they settle); the callers
    /// that can reach it *with a standing clears-bearing marker* are the
    /// blind writes — the mid-handler `commit()` and the `ReadUncommitted`
    /// direct apply. (The `resolve_cell` write-backs also reach
    /// `write_resolved`, but only ever clears-free or marker-free — see the
    /// repair-provenance invariant on `resolve_cell`.)
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] on a store failure.
    fn write_resolved<'a>(
        &'a self,
        collection: &'a CollectionRef,
        cells: &'a [(CellKey, Option<Bytes>)],
        clears: &'a [SectionClear],
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;

    /// Point-reads the collection's standing **event marker**, or `None` when
    /// none stands (≈ always). Feeds the recovery sweep's marker leg and the
    /// stage-boundary rule. Required with no default: a defaulted `Ok(None)` on
    /// a marker-bearing backend would be a silent recovery hole, so every impl
    /// answers truthfully.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] on a store failure.
    fn unsettled_marker<'a>(
        &'a self,
        collection: &'a CollectionId,
    ) -> impl Future<Output = Result<Option<EventMarker>, Self::Error>> + Send + 'a;

    /// Promotes each `cell`'s provisional cell to resolved: nulls `event` and
    /// `prev`, keeping `data`. O(1) bytes per cell. Idempotent — promoting a
    /// resolved cell is a harmless no-op write.
    ///
    /// **Precondition:** callers route an absent-data promote to
    /// [`write_resolved`](Self::write_resolved)`(cell, None)` (the row-absence
    /// invariant), so this verb only ever promotes present data. Promoting an
    /// absent-data cell through it instead is not corruption — the row still
    /// decodes `Committed(None)` — but it leaves a resolved-null residue row,
    /// which is why the absent-data route exists.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] on a store failure.
    fn mark_resolved<'a>(
        &'a self,
        collection: &'a CollectionRef,
        cells: &'a [CellKey],
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;

    /// Settles a staged set as **committed** AND deletes the collection's event
    /// marker: each cell's provisional `data` becomes the committed value.
    ///
    /// Required with no default (present data promotes via
    /// [`Self::mark_resolved`], absent data deletes its row via the raw
    /// resolved apply, and the marker is deleted last — the memory backend
    /// routes to its own raw apply so the settle never re-enters the
    /// write-help boundary on the marker it is deleting; the Cassandra backend
    /// implements the identical routing natively with same-partition batches):
    /// with markers, a
    /// defaulted override behind a trait default is a landmine — a wrapper
    /// store that forgot to forward the verb would fall into a default routing
    /// through the *wrapper's* verbs and bypass the inner store's marker
    /// delete, a leaked marker with no compile error. Making both settle verbs
    /// required makes that bug class uncompilable.
    ///
    /// `clears` (frozen at stage time) are **applied here**: each cleared
    /// section's non-survivor rows are erased — on Cassandra as the n+1 gap
    /// range deletes between sorted survivors. The marker delete is issued
    /// only after every cell resolution and gap delete has completed, unless
    /// one same-partition batch carries all of them atomically. Erasing a
    /// still-provisional **foreign** row is correct: single-writer ordering
    /// puts the committed clear after every pre-existing row, so a
    /// non-survivor's post-clear state is absent regardless of its unresolved
    /// history (the erasure argument). Survivors are protected positionally by
    /// the frozen list, never temporally. Idempotent; the sweep retries it.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] on a store failure.
    fn commit_provisional<'a>(
        &'a self,
        collection: &'a CollectionRef,
        writes: &'a [(CellKey, ProvisionalWrite)],
        clears: &'a [SectionClear],
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;

    /// Settles a staged set as **aborted** AND deletes the collection's event
    /// marker: each cell's committed base `prev` is written back as the
    /// resolved value. Required with no default, for the reason on
    /// [`commit_provisional`](Self::commit_provisional). The base was never
    /// touched by the stage, so the rollback is exact and needs no per-section
    /// discard. Idempotent; the sweep retries it.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] on a store failure.
    fn abort_provisional<'a>(
        &'a self,
        collection: &'a CollectionRef,
        writes: &'a [(CellKey, ProvisionalWrite)],
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;
}
