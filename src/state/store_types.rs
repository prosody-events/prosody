use super::CELLS_INLINE;
use super::cell::{Committed, Values};
use super::cell_key::Coordinate;
use crate::timers::duration::CompactDuration;
use smallvec::SmallVec;
use std::iter::from_fn;
use std::num::NonZeroUsize;
use std::slice;

/// The maximum number of coordinates a batch read carries in one hop.
pub(crate) const CELL_BATCH: NonZeroUsize = NonZeroUsize::MIN.saturating_add(127);

// Batch positions fit in one byte.
const _: () = assert!(
    CELL_BATCH.get() <= u8::MAX as usize + 1,
    "batch positions must fit in one byte"
);

/// A non-empty, bounded (`1..=CELL_BATCH`) run of coordinates for one batch
/// read.
///
/// Constructors enforce the length bound. `N` selects inline storage capacity.
/// Owned coordinates use the small buffer capacity. Borrowed coordinates
/// keep a full batch inline.
///
/// Duplicates and unknown coordinates are valid. The read contract on
/// [`super::store::CellRead::read_many`] defines each result position.
pub struct Batch<C, const N: usize>(SmallVec<[C; N]>);

/// A bounded batch of owned coordinates.
pub type CoordinateBatch = Batch<Coordinate, CELLS_INLINE>;

/// A bounded read request with borrowed coordinate bytes.
pub type ReadBatch<'a> = Batch<&'a [u8], { CELL_BATCH.get() }>;

impl<C, const N: usize> Batch<C, N> {
    /// Splits `coords` into maximal `1..=CELL_BATCH` batches in input order.
    pub fn chunks<I: IntoIterator<Item = C>>(
        coords: I,
    ) -> impl Iterator<Item = Self> + use<C, I, N> {
        let mut it = coords.into_iter();
        from_fn(move || {
            let batch: SmallVec<[C; N]> = it.by_ref().take(CELL_BATCH.get()).collect();
            Self::from_buffer(batch)
        })
    }

    pub(crate) fn from_buffer(batch: SmallVec<[C; N]>) -> Option<Self> {
        assert!(
            batch.len() <= CELL_BATCH.get(),
            "batch exceeds the coordinate limit"
        );
        (!batch.is_empty()).then_some(Self(batch))
    }

    /// Returns the number of coordinates in this batch.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns the coordinates in input order.
    pub fn as_slice(&self) -> &[C] {
        &self.0
    }

    /// Iterates over coordinates in input order.
    pub fn iter(&self) -> slice::Iter<'_, C> {
        self.0.iter()
    }
}

impl<C: AsRef<[u8]>, const N: usize> Batch<C, N> {
    /// Borrows every coordinate. The bounded address buffer stays on the stack.
    #[must_use]
    pub fn as_ref(&self) -> ReadBatch<'_> {
        Batch(self.iter().map(AsRef::as_ref).collect())
    }
}

/// A keyed-state work buffer. Small operations stay inline.
pub type CellBuffer<T> = SmallVec<[T; CELLS_INLINE]>;

/// The index-aligned result of a committed batch read.
pub type CommittedBatch<P = Values> = CellBuffer<Committed<P>>;

/// The index-aligned result of a cache-fill batch read.
pub type CacheBatch<P = Values> = CellBuffer<Durable<P>>;

/// One committed cell with the remaining TTL of its durable row.
pub type Durable<P = Values> = (Committed<P>, Option<CompactDuration>);
