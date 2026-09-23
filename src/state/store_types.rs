use super::CELLS_INLINE;
use super::cell::{Committed, Values};
use super::cell_key::Coordinate;
use crate::timers::duration::CompactDuration;
use smallvec::SmallVec;
use std::iter::from_fn;
use std::num::NonZeroUsize;
use std::slice;
use thiserror::Error;

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
/// Only [`Self::chunks`] and subsets of a batch create one, so no batch can
/// exceed the bound. `N` selects inline storage capacity.
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
            (!batch.is_empty()).then_some(Self(batch))
        })
    }

    /// Keeps the coordinates that `keep` accepts, in input order. `keep` runs
    /// once per coordinate, in input order. An empty result returns `None`.
    /// A subset stays within the batch bound.
    pub(crate) fn filter(&self, mut keep: impl FnMut(&C) -> bool) -> Option<Self>
    where
        C: Clone,
    {
        let kept: SmallVec<[C; N]> = self.iter().filter(|c| keep(c)).cloned().collect();
        (!kept.is_empty()).then_some(Self(kept))
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

/// Checks that a batch read returned one answer per requested coordinate.
/// Callers pair answers with coordinates by position.
///
/// # Errors
///
/// Returns [`MisalignedBatch`] when the counts differ.
pub(crate) fn ensure_aligned(returned: usize, requested: usize) -> Result<(), MisalignedBatch> {
    if returned == requested {
        Ok(())
    } else {
        Err(MisalignedBatch {
            returned,
            requested,
        })
    }
}

/// A batch read that returned a different number of answers than
/// coordinates. Only a store defect produces one; stored data cannot.
///
/// Stores answer a batch from a fetch, and a downstream trait can implement
/// the read. The answer count is therefore checked when the answers arrive.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
#[error("batch read returned {returned} answers for {requested} coordinates")]
pub struct MisalignedBatch {
    /// The number of answers the read returned.
    pub returned: usize,

    /// The number of coordinates the batch requested.
    pub requested: usize,
}
