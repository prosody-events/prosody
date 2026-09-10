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
/// `get` resolves one cell. `scan_cells` resolves a section range.
/// Three mutators implement the durability sequence:
///
/// * [`Self::write_provisional`] stages `data`, `prev`, and `event` for
///   `ReadCommitted`.
/// * [`Self::write_resolved`] writes committed values with null `event` and
///   `prev`. `ReadUncommitted`, mid-handler `commit()`, and abort use this
///   operation. Abort restores the staged `prev`.
/// * [`Self::mark_resolved`] clears `event` and `prev` but preserves `data`.
///   Its cost is O(1), regardless of value size.
///
/// # Committed absence is row absence
///
/// A committed-absent cell has no row. Every operation that resolves a cell to
/// absence deletes its row.
/// Thus, [`Self::write_resolved`] deletes cells with `None` data.
/// [`Self::commit_provisional`] uses that delete for absent data and
/// [`Self::mark_resolved`] for present data.
pub trait CellStore: Clone + Send + Sync + 'static {
    /// Error type for cell-store operations.
    type Error: ClassifyError + Error + Send + Sync + 'static;

    /// Reads the committed projection without a durable write.
    /// A missing row returns `Committed(None)`.
    /// The current event reads `prev` for a provisional cell.
    /// Admission resolves registered residue before dispatch. The event stages
    /// only at settle.
    ///
    /// # Errors
    ///
    /// Returns the store error for failed reads or corrupt rows.
    fn get<'a>(
        &'a self,
        collection: &'a CollectionId,
        cell: &'a CellKey,
    ) -> impl Future<Output = Result<Committed, Self::Error>> + Send + 'a;

    /// Scans one section in coordinate order and yields present committed
    /// values. Provisional cells use the same projection as [`Self::get`].
    fn scan_cells<'a>(
        &'a self,
        collection: &'a CollectionId,
        scan: Scan<'a>,
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
    ) -> impl Future<Output = Result<(Committed, Option<CompactDuration>), Self::Error>> + Send + 'a
    {
        async move { Ok((self.get(collection, cell).await?, None)) }
    }

    /// Reads one section's coordinates and returns one committed value per
    /// input position.
    /// A missing row returns `Committed(None)`.
    ///
    /// # Read contract
    ///
    /// * The result has exactly `batch.len()` entries. `result[i]` answers
    ///   `batch[i]`.
    /// * Each repeated coordinate requires one read. All its positions return
    ///   the same value.
    /// * Unique coordinates resolve in first-occurrence order. Row corruption
    ///   or evidence errors fail the batch at the earliest affected input
    ///   position. A backend can fail the whole batch before row resolution.
    ///   Cassandra query and marker failures carry no input position, as with
    ///   [`Self::get`].
    ///
    /// The default calls [`Self::get`] once per unique coordinate in
    /// first-occurrence order and copies each answer to duplicate
    /// positions. The memory backend shares evidence across raw reads.
    /// Cassandra uses one `IN` query.
    /// [`CellBuffer`] keeps small scratch buffers inline; a full batch uses
    /// heap space to limit the future's size.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] for the earliest affected input position, or a
    /// whole-collection failure without a position.
    fn get_many<'a>(
        &'a self,
        collection: &'a CollectionId,
        section: Section,
        batch: &'a CoordinateBatch,
    ) -> impl Future<Output = Result<CommittedBatch, Self::Error>> + Send + 'a {
        async move {
            let (unique_coordinates, input_indices) = dedupe(batch);
            let mut unique_answers = CommittedBatch::with_capacity(unique_coordinates.len());
            for &coordinate in &unique_coordinates {
                let cell = CellKey {
                    section,
                    coordinate: Coordinate::clone(coordinate),
                };
                unique_answers.push(self.get(collection, &cell).await?);
            }
            Ok(expand_to_input_order(&input_indices, &unique_answers))
        }
    }

    /// Reads committed values and remaining TTLs for
    /// [`Cached`](super::cached::Cached). Uses the input-position contract
    /// of [`Self::get_many`].
    ///
    /// The default calls [`Self::get_for_cache`] in first-occurrence order and
    /// copies each `(value, ttl)` pair to duplicate positions.
    /// It preserves backend TTL metadata; a call to [`Self::get_many`] with
    /// added `None` TTLs would lose that metadata.
    /// Cassandra shares a prepared statement with [`Self::get_many`]. Memory
    /// uses its batch read with no TTL.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] on the first failure from
    /// [`Self::get_for_cache`], at the earliest input position.
    fn get_many_for_cache<'a>(
        &'a self,
        collection: &'a CollectionId,
        section: Section,
        batch: &'a CoordinateBatch,
    ) -> impl Future<Output = Result<CacheBatch, Self::Error>> + Send + 'a {
        async move {
            let (unique_coordinates, input_indices) = dedupe(batch);
            let mut unique_answers = CacheBatch::with_capacity(unique_coordinates.len());
            for &coordinate in &unique_coordinates {
                let cell = CellKey {
                    section,
                    coordinate: Coordinate::clone(coordinate),
                };
                unique_answers.push(self.get_for_cache(collection, &cell).await?);
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

    /// Reads distinct provisional cells from one collection section in
    /// ascending coordinate order.
    /// Each result includes its coordinate. Input coordinates can repeat or
    /// arrive out of order. [`CoordinateBatch`] requires non-empty input.
    ///
    /// The raw decoder omits absent and resolved cells, with exact
    /// [`Self::provisional_cell_at`] parity for malformed or partially expired
    /// rows. Results retain `data`, `prev`, and
    /// [`EventRef`](super::event_ref::EventRef). This operation changes
    /// neither durable state nor the committed-value cache.
    ///
    /// This method requires an implementation. A wrapper must forward it to
    /// preserve native batch reads.
    /// A default point loop would cause N uncached reads through
    /// [`Cached`](super::cached::Cached), whose cache cannot answer raw
    /// provisional reads.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] on a store or row decode failure.
    /// A whole-batch failure has no input position. A row failure reports the
    /// lowest affected coordinate, because backends decode in ascending
    /// order.
    fn provisional_many<'a>(
        &'a self,
        collection: &'a CollectionId,
        section: Section,
        batch: &'a CoordinateBatch,
    ) -> impl Future<Output = Result<CellBuffer<(Coordinate, ProvisionalCell)>, Self::Error>> + Send + 'a;

    /// Stages provisional cells and writes the collection's Staged row from
    /// the frozen `marker`. Cells and Staged bind the collection TTL.
    /// The Staged row names the event, coordinates, and clears.
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
    /// boundary. Admission resolves prior residue before dispatch.
    ///
    /// Staged carries frozen clear survivors that [`Self::commit_provisional`]
    /// applies at settle. Admission uses only that durable payload.
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

    /// Writes resolved cells in a same-partition batch with the collection TTL.
    /// `Some(data)` writes a committed value with null `event` and `prev`.
    /// `None` deletes the row.
    /// This operation supports `ReadUncommitted` clears, mid-handler `commit()`
    /// clears, and rollback to absence. It never changes Staged.
    ///
    /// `clears` removes each section's non-survivor rows before cell writes.
    /// Cassandra deletes gaps between sorted survivors. The caller freezes
    /// survivors from the present-data coordinates in `cells`.
    /// These positions exclude every written row from the gap ranges, so batch
    /// rows remain disjoint.
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

    /// Writes Committed evidence, promotes staged values and frozen clears,
    /// then deletes Staged.
    /// The Staged payload supplies the event and evidence TTL. Promotion
    /// preserves each cell's TTL; deletes bind no TTL.
    /// Admission can thus promote unregistered collections without a registry
    /// TTL.
    ///
    /// Both settle methods require implementations. A wrapper must forward
    /// them; a default could bypass the inner store's Staged delete and
    /// leak residue. Memory promotes present data through
    /// [`Self::mark_resolved`] and deletes absent data through its raw
    /// apply operation. The raw apply avoids recursive clear resolution on
    /// Staged. Cassandra uses the same sequence in same-partition batches.
    ///
    /// The frozen clears remove each section's non-survivor rows. Cassandra
    /// uses n+1 gap deletes between n sorted survivors.
    /// Staged disappears after all cell changes and gap deletes, unless one
    /// atomic batch contains the entire operation.
    /// A committed clear follows every prior row under the single-writer rule.
    /// It therefore removes prior provisional non-survivors regardless of
    /// their unresolved history.
    ///
    /// The frozen positions protect survivors without timestamp comparisons.
    /// Admission can retry this operation.
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

    /// Restores each staged cell's committed base `prev` as its resolved value,
    /// then deletes Staged.
    /// The stage preserves the base, so rollback needs no section discard.
    /// Admission can retry this operation.
    /// This method requires an implementation for the reason in
    /// [`Self::commit_provisional`].
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
