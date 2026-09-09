//! Durable cells and collection commit evidence.
//!
//! Collection handles address cells through this uniform store interface.
//! Admission resolves residue before the handler runs. Reads project
//! provisional cells through collection evidence without durable writes.
//!
//! Cassandra uses atomic batches within each collection partition.
//! Oversized writes use multiple chunks. Each stage chunk includes its
//! discovery row. Promote writes evidence before destructive chunks and deletes
//! Staged last. An oversized resolved write can remain partial after a crash
//! because it has no provisional state to reconstruct.
use super::cell::{Committed, ProvisionalCell, ProvisionalWrite};
use super::cell_key::{CellKey, Coordinate, Scan, Section};
use super::event_ref::EventRef;
use super::identity::{CollectionId, CollectionRef};
use super::marker::{EventMarker, MarkerState, SectionClear};
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
/// range stream. The
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

    /// Reads the committed projection without a durable write.
    /// A missing row returns `Committed(None)`. The current event reads its
    /// staged `prev` value.
    ///
    /// # Errors
    ///
    /// Returns the store error for failed reads or corrupt rows.
    fn get<'a>(
        &'a self,
        collection: &'a CollectionId,
        cell: &'a CellKey,
        own: EventRef,
    ) -> impl Future<Output = Result<Committed, Self::Error>> + Send + 'a;

    /// Scans one section in coordinate order and yields present committed
    /// values. Provisional cells use the same projection as [`Self::get`].
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
    ///   semantic failures** (a corrupt row shape, an evidence read) the one
    ///   surfaced is the earliest input position's. A backend may additionally
    ///   fail the batch as a whole *before* any row resolves (the Cassandra
    ///   override's `IN` query or its marker read); such a whole-collection
    ///   failure carries **no** input position, exactly as the point
    ///   [`Self::get`] surfaces the same failure with no cell attribution.
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
    /// writing durable state or publishing into the
    /// committed-value cache** — this is the raw residue read,
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

    /// Stages provisional cells and writes the collection's Staged row from
    /// the frozen `marker`. Cells bind the collection TTL; Staged binds the
    /// evidence TTL. The Staged row names the event, coordinates, and clears.
    ///
    /// # Staged row lifecycle
    ///
    /// This verb creates or replaces Staged. Settle and admission delete it
    /// through [`Self::commit_provisional`] or [`Self::abort_provisional`].
    /// [`Self::write_resolved`] and [`Self::mark_resolved`] never write Staged.
    ///
    /// Every write must occur in the frozen staged list
    /// (`writes ⊆ marker.staged()`). Split stages use that full list for every
    /// chunk, so admission can find every provisional coordinate.
    /// The session freezes the payload once per collection at `finalize`.
    /// A retry can replace the same event's Staged row; handlers must produce
    /// the same result across retries.
    ///
    /// `None` requires empty `writes`: it writes no Staged row and skips the
    /// boundary check. A clears-only stage supplies a payload with empty
    /// `staged()` and non-empty `clears()`. It writes Staged and checks the
    ///
    /// Before the write, the backend resolves a Staged row for a different
    /// event. A resolution failure fails the stage; retry middleware handles
    /// it. Thus each collection has at most one unresolved stage.
    ///
    /// Staged carries frozen clear survivors that [`Self::commit_provisional`]
    /// applies at settle. Recovery uses only that durable payload.
    /// The session derives the survivors from each collection's staged writes.
    /// After an operator shortens the TTL, older cells can outlive a committed,
    /// unapplied clear and then expire independently.
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
    /// and rollback-to-absent. Never touches the Staged row (see
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
    /// # Unsettled section clears
    ///
    /// Resolves an unsettled section clear before it writes cells.
    /// The clear cannot remove a value that this write adds.
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

    /// Reads both marker rows from the store, without a memo.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] on a store or decode failure.
    fn marker_state<'a>(
        &'a self,
        collection: &'a CollectionId,
    ) -> impl Future<Output = Result<MarkerState, Self::Error>> + Send + 'a;

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

    /// Writes Committed evidence, promotes the staged values and frozen clears,
    /// then deletes Staged. The Staged payload supplies the event and evidence
    /// TTL.
    ///
    /// Required with no default (present data promotes via
    /// [`Self::mark_resolved`], absent data deletes its row via the raw
    /// resolved apply, and Staged is deleted last — the memory backend
    /// routes to its own raw apply so the settle never re-enters the
    /// clear resolution boundary on the Staged row it deletes; the Cassandra
    /// backend implements the identical routing natively with
    /// same-partition batches): with markers, a
    /// defaulted override behind a trait default is a landmine — a wrapper
    /// store that forgot to forward the verb would fall into a default routing
    /// through the *wrapper's* verbs and bypass the inner store's Staged
    /// delete, a leaked Staged row with no compile error. Making both settle
    /// verbs required makes that bug class uncompilable.
    ///
    /// The Staged payload's frozen clears are **applied here**: each cleared
    /// section's non-survivor rows are erased — on Cassandra as the n+1 gap
    /// range deletes between sorted survivors. The Staged delete is issued
    /// only after every cell resolution and gap delete has completed, unless
    /// one same-partition batch carries all of them atomically. Erasing a
    /// still-provisional **prior event** row is correct: single-writer ordering
    /// puts the committed clear after every pre-existing row, so a
    /// non-survivor's post-clear state is absent regardless of its unresolved
    /// history (the erasure argument). Survivors are protected positionally by
    /// the frozen list, never temporally. Admission can retry this operation.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] on a store failure.
    fn commit_provisional<'a>(
        &'a self,
        collection: &'a CollectionRef,
        marker: &'a EventMarker,
        writes: &'a [(CellKey, ProvisionalWrite)],
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;

    /// Settles a staged set as **aborted** AND deletes the collection's
    /// Staged row: each cell's committed base `prev` is written back as the
    /// resolved value. Required with no default, for the reason on
    /// [`commit_provisional`](Self::commit_provisional). The base was never
    /// touched by the stage, so the rollback is exact and needs no per-section
    /// discard. Admission can retry this operation.
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
