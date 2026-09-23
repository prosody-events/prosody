//! Durable reads, cell writes, and collection commit evidence.
//!
//! [`CellBackend`] supplies one error type for all operations.
//! [`CellRead`] selects a projection for point, batch, and range reads.
//! Point and batch answers carry the remaining durable TTL.
//! [`CellStore`] supports both projections and supplies the durability
//! operations. Collection operations select projections through their engine.
//!
//! Admission resolves residue before the handler runs.
//! Reads use collection evidence to resolve provisional cells without durable
//! writes.
//!
//! Cassandra uses atomic batches within each collection partition.
//! Each stage chunk includes its discovery row.
//! Promotion writes evidence before destructive chunks and deletes Staged last.
//! An oversized resolved write can remain partial after a crash because it has
//! no provisional state to reconstruct.

use super::cell::{Presence, Projection, ProvisionalCell, ProvisionalWrite, Values};
use super::cell_key::{CellKey, Coordinate, Scan, Section};
use super::identity::{CollectionId, CollectionRef};
use super::marker::{EventMarker, MarkerState, ProvisionalStage, SectionClear};
use crate::error::ClassifyError;
use crate::state::cell_key::CellRef;
use bytes::Bytes;
use futures::Stream;
use std::error::Error;
use std::future::Future;
use std::num::NonZeroUsize;

pub(crate) use super::store_helpers::{
    distinct, provisional_point_loop, repeated, section_batches, sorted_unique_coordinates,
};
pub use super::store_types::MisalignedBatch;
pub(crate) use super::store_types::{CELL_BATCH, ensure_aligned};
pub use super::store_types::{
    CacheBatch, CellBuffer, CommittedBatch, CoordinateBatch, Durable, ReadBatch,
};

/// Sizes the fetches of one read. The first fetch equals the caller's
/// expectation, capped at the transport maximum. Each later fetch doubles,
/// up to that maximum. Rows a filter hides therefore cost O(log n) extra
/// round trips, never one round trip per hidden row.
///
/// The first fetch has no floor of its own. `Projection::FETCH_FLOOR` sets
/// the floor per projection, so a value fetch stays as small as its limit.
#[derive(Clone, Copy, Debug)]
pub(crate) struct FetchSchedule {
    next: NonZeroUsize,
    max: NonZeroUsize,
}

impl FetchSchedule {
    /// `first` is the expected result count. `None` means no expectation, so
    /// every fetch is `max`.
    pub(crate) fn new(first: Option<NonZeroUsize>, max: NonZeroUsize) -> Self {
        Self {
            next: first.unwrap_or(max).min(max),
            max,
        }
    }

    /// Returns the size of the next fetch and advances the schedule.
    pub(crate) fn next(&mut self) -> NonZeroUsize {
        let next = self.next;
        self.next = next.saturating_add(next.get()).min(self.max);
        next
    }
}

/// One error type for every cell read and mutation.
pub trait CellBackend: Clone + Send + Sync + 'static {
    /// The backend error.
    type Error: ClassifyError + Error + Send + Sync + 'static;
}

/// Reads committed cells under one projection with their remaining durable TTL.
pub trait CellRead<P: Projection>: CellBackend {
    /// Reads one cell. A missing row returns committed absence without a TTL.
    fn read<'a>(
        &'a self,
        collection: &'a CollectionId,
        cell: CellRef<'a>,
    ) -> impl Future<Output = Result<Durable<P>, Self::Error>> + Send + use<'a, Self, P>;

    /// Reads one section's coordinates in input order.
    ///
    /// Each result answers the input at the same position. Duplicate
    /// coordinates share one read. Distinct coordinates resolve in
    /// first-occurrence order. The earliest affected position supplies the
    /// error. A backend can fail the whole batch before row resolution.
    fn read_many<'buf, 'a>(
        &'a self,
        collection: &'a CollectionId,
        section: Section,
        batch: &'a ReadBatch<'buf>,
    ) -> impl Future<Output = Result<CacheBatch<P>, Self::Error>> + Send + use<'buf, 'a, Self, P>
    {
        async move {
            let mut answers = CacheBatch::<P>::with_capacity(batch.len());
            for &coordinate in batch.iter() {
                let answer = if let Some(answer) = repeated(batch, &answers, coordinate) {
                    answer
                } else {
                    let cell = CellRef {
                        section,
                        coordinate,
                    };
                    self.read(collection, cell).await?
                };
                answers.push(answer);
            }
            Ok(answers)
        }
    }

    /// Scans present committed cells in coordinate order within one section.
    fn scan<'a>(
        &'a self,
        collection: &'a CollectionId,
        scan: Scan<'a>,
    ) -> impl Stream<Item = Result<(CellKey, P::Payload), Self::Error>> + Send + use<'a, Self, P>;
}

/// Stores durable cells and collection commit evidence.
///
/// A committed-absent cell has no row. Each operation that resolves a cell to
/// absence deletes its row.
pub trait CellStore: CellRead<Values> + CellRead<Presence> {
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
    ) -> impl Future<Output = Result<Option<ProvisionalCell>, Self::Error>> + Send + use<'a, Self>;

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
    ) -> impl Future<Output = Result<CellBuffer<(Coordinate, ProvisionalCell)>, Self::Error>>
    + Send
    + use<'a, Self>;

    /// Stages provisional cells and writes the collection's Staged row from
    /// the stage's frozen marker. Cells and Staged bind the collection TTL.
    /// The Staged row names the event, coordinates, and clears.
    ///
    /// # Staged row lifecycle
    ///
    /// This verb creates or replaces Staged. Settle and admission delete it
    /// through [`Self::commit_provisional`] or [`Self::abort_provisional`].
    /// [`Self::write_resolved`] and [`Self::mark_resolved`] never write Staged.
    ///
    /// [`ProvisionalStage`] guarantees that the marker lists every write.
    /// Split stages send the full marker with every chunk, so admission can
    /// find every provisional coordinate.
    /// The session freezes the payload once per collection at `finalize`.
    /// A retry can replace the same event's Staged row; handlers must produce
    /// the same result across retries.
    ///
    /// A clears-only stage supplies a payload with empty `staged()` and
    /// non-empty `clears()`. It writes Staged and checks the boundary.
    /// Admission resolves prior residue before dispatch.
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
        stage: ProvisionalStage<'a>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + use<'a, Self>;

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
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + use<'a, Self>;

    /// Reads both marker rows from the store, without a memo.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] on a store or decode failure.
    fn marker_state<'a>(
        &'a self,
        collection: &'a CollectionId,
    ) -> impl Future<Output = Result<MarkerState, Self::Error>> + Send + use<'a, Self>;

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
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + use<'a, Self>;

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
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + use<'a, Self>;

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
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + use<'a, Self>;
}

#[cfg(test)]
mod tests;
