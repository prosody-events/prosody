use super::CELLS_INLINE;
use super::cell::Committed;
use super::cell_key::Coordinate;
use crate::timers::duration::CompactDuration;
use smallvec::SmallVec;
use std::iter::from_fn;
use std::slice;

/// The maximum number of coordinates a batch read carries in one hop.
pub(crate) const CELL_BATCH: usize = 128;

const _: () = assert!(
    CELL_BATCH > 0,
    "CELL_BATCH must be positive or every stream-unfold chunk source stalls on empty chunks"
);

/// A non-empty, bounded (`1..=CELL_BATCH`) run of coordinates for one batch
/// read.
///
/// The sole constructor splits input into maximal `CELL_BATCH`-sized batches.
/// It yields no empty batch. Callers cannot create an invalid batch.
///
/// Duplicates and unknown coordinates are valid. The read contract on
/// [`super::store::CellStore::get_many`] defines each result position.
pub struct CoordinateBatch(CellBuffer<Coordinate>);

impl CoordinateBatch {
    /// Splits `coords` into maximal `1..=CELL_BATCH` batches in input order.
    pub fn chunks<I: IntoIterator<Item = Coordinate>>(
        coords: I,
    ) -> impl Iterator<Item = CoordinateBatch> {
        let mut it = coords.into_iter();
        from_fn(move || {
            let batch: CellBuffer<Coordinate> = it.by_ref().take(CELL_BATCH).collect();
            (!batch.is_empty()).then_some(CoordinateBatch(batch))
        })
    }

    /// Returns the number of coordinates in this batch.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns the coordinates in input order.
    pub fn as_slice(&self) -> &[Coordinate] {
        &self.0
    }

    /// Iterates over coordinates in input order.
    pub fn iter(&self) -> slice::Iter<'_, Coordinate> {
        self.0.iter()
    }
}

/// A keyed-state work buffer. Small operations stay inline.
pub type CellBuffer<T> = SmallVec<[T; CELLS_INLINE]>;

/// The index-aligned result of a committed batch read.
pub type CommittedBatch = CellBuffer<Committed>;

/// One presence bit per input position.
pub type PresenceBatch = CellBuffer<bool>;

/// The index-aligned result of a cache-fill batch read.
pub type CacheBatch = CellBuffer<(Committed, Option<CompactDuration>)>;
