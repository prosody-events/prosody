//! Bounded batch reads: the batch type, its answers, and its helpers.

use super::CellStore;
use crate::state::CELLS_INLINE;
use crate::state::cell::{Committed, ProvisionalCell, Values};
use crate::state::cell_key::{CellKey, Coordinate, Section};
use crate::state::identity::CollectionId;
use crate::timers::duration::CompactDuration;
use smallvec::SmallVec;
use std::future::Future;
use std::iter::from_fn;
use std::num::NonZeroUsize;
use std::ops::{Deref, DerefMut};
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
/// Only [`Self::chunks`], [`Self::as_ref`], and the pending subset of
/// [`ReadBatch::merge`] create one. Each keeps or shrinks a bounded length, so
/// no batch can exceed the bound.
/// `N` selects inline storage capacity. Owned coordinates use the small buffer
/// capacity. Borrowed coordinates keep a full batch inline.
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

    /// Borrows every coordinate. The bounded address buffer stays on the stack.
    #[must_use]
    pub fn as_ref(&self) -> ReadBatch<'_>
    where
        C: AsRef<[u8]>,
    {
        Batch(self.iter().map(AsRef::as_ref).collect())
    }

    /// Answers each position with `answer`.
    pub(crate) fn map<T>(&self, answer: impl FnMut(&C) -> T) -> Answers<T> {
        Answers(self.iter().map(answer).collect())
    }

    /// Answers each position with `answer`, or returns its first error.
    pub(crate) fn try_map<T, E>(
        &self,
        answer: impl FnMut(&C) -> Result<T, E>,
    ) -> Result<Answers<T>, E> {
        try_answers(&self.0, answer)
    }
}

impl<'a> ReadBatch<'a> {
    /// Reads each distinct coordinate once, in first-occurrence order.
    /// A repeated coordinate copies its first answer. The first error in input
    /// order ends the read.
    pub(crate) async fn read<T: Clone, E, F>(
        &self,
        mut read: impl FnMut(&'a [u8]) -> F,
    ) -> Result<Answers<T>, E>
    where
        F: Future<Output = Result<T, E>>,
    {
        let mut answers: CellBuffer<T> = CellBuffer::with_capacity(self.len());
        for (position, &coordinate) in self.iter().enumerate() {
            let earlier = self.0[..position].iter().position(|&e| e == coordinate);
            let answer = match earlier {
                Some(earlier) => answers[earlier].clone(),
                None => read(coordinate).await?,
            };
            answers.push(answer);
        }
        Ok(Answers(answers))
    }

    /// Answers each position from `local`, which returns `None` for a position
    /// it cannot answer. One `lower` read of the pending subset answers the
    /// rest, and it runs only when a position remains. `answer` converts each
    /// lower answer.
    pub(crate) async fn merge<T: Default, U, E, F>(
        &self,
        local: impl FnMut(&'a [u8]) -> Option<T>,
        lower: impl FnOnce(ReadBatch<'a>) -> F,
        answer: impl FnMut(U) -> T,
    ) -> Result<Answers<T>, E>
    where
        F: Future<Output = Result<Answers<U>, E>>,
    {
        let mut answers = CellBuffer::new();
        self.merge_into(&mut answers, local, lower, answer).await?;
        Ok(Answers(answers))
    }

    /// Appends the [`Self::merge`] answers to `answers`, so a read of many
    /// batches fills one buffer. After an error, `answers` holds placeholders.
    pub(crate) async fn merge_into<T: Default, U, E, F>(
        &self,
        answers: &mut CellBuffer<T>,
        mut local: impl FnMut(&'a [u8]) -> Option<T>,
        lower: impl FnOnce(ReadBatch<'a>) -> F,
        mut answer: impl FnMut(U) -> T,
    ) -> Result<(), E>
    where
        F: Future<Output = Result<Answers<U>, E>>,
    {
        let start = answers.len();
        let mut positions: SmallVec<[u8; CELL_BATCH.get()]> = SmallVec::new();
        let mut pending = SmallVec::new();
        answers.reserve(self.len());
        for (position, &coordinate) in self.iter().enumerate() {
            answers.push(local(coordinate).unwrap_or_else(|| {
                positions.push(position as u8);
                pending.push(coordinate);
                T::default()
            }));
        }
        if !pending.is_empty() {
            // `pending` is a non-empty subset of this batch.
            for (position, value) in positions.into_iter().zip(lower(Batch(pending)).await?) {
                answers[start + usize::from(position)] = answer(value);
            }
        }
        Ok(())
    }
}

/// One answer for each position of a batch, in input order.
///
/// Only a batch builds answers, and no method changes their length. So every
/// answer list has the length of the batch it answers, and callers pair
/// answers with coordinates by position. A reader must answer the batch it
/// receives. The type cannot stop crate code that answers a different batch.
#[derive(Debug)]
pub struct Answers<T>(CellBuffer<T>);

impl<T> Answers<T> {
    /// Maps each borrowed answer, or returns the first error.
    pub(crate) fn try_map<U, E>(
        &self,
        answer: impl FnMut(&T) -> Result<U, E>,
    ) -> Result<Answers<U>, E> {
        try_answers(&self.0, answer)
    }
}

impl<T> Deref for Answers<T> {
    type Target = [T];

    fn deref(&self) -> &[T] {
        &self.0
    }
}

impl<T> DerefMut for Answers<T> {
    fn deref_mut(&mut self) -> &mut [T] {
        &mut self.0
    }
}

impl<T> IntoIterator for Answers<T> {
    type IntoIter = smallvec::IntoIter<[T; CELLS_INLINE]>;
    type Item = T;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<T> From<Answers<T>> for CellBuffer<T> {
    fn from(answers: Answers<T>) -> Self {
        answers.0
    }
}

/// Maps each position into an answer buffer sized once.
fn try_answers<X, T, E>(
    items: &[X],
    mut answer: impl FnMut(&X) -> Result<T, E>,
) -> Result<Answers<T>, E> {
    let mut answers = CellBuffer::with_capacity(items.len());
    for item in items {
        answers.push(answer(item)?);
    }
    Ok(Answers(answers))
}

/// A keyed-state work buffer. Small operations stay inline.
pub type CellBuffer<T> = SmallVec<[T; CELLS_INLINE]>;

/// The answers of a cache-fill batch read.
pub type CacheBatch<P = Values> = Answers<Durable<P>>;

/// One committed cell with the remaining TTL of its durable row.
pub type Durable<P = Values> = (Committed<P>, Option<CompactDuration>);

/// Returns the distinct coordinates of `batch` in first-occurrence order.
pub(crate) fn distinct<'a>(batch: &ReadBatch<'a>) -> SmallVec<[&'a [u8]; CELL_BATCH.get()]> {
    let mut coordinates: SmallVec<[&[u8]; CELL_BATCH.get()]> = SmallVec::new();
    for &coordinate in batch.iter() {
        if !coordinates.contains(&coordinate) {
            coordinates.push(coordinate);
        }
    }
    coordinates
}

/// Returns the sorted, distinct coordinates for one bounded batch.
pub(crate) fn sorted_unique_coordinates(batch: &CoordinateBatch) -> CellBuffer<&Coordinate> {
    let mut coordinates: CellBuffer<&Coordinate> = SmallVec::with_capacity(batch.len());
    coordinates.extend(batch.iter());
    coordinates.sort_unstable();
    coordinates.dedup();
    coordinates
}

/// Groups sorted cell keys into bounded batches for each section.
pub(crate) fn section_batches(keys: &[CellKey]) -> Vec<(Section, CoordinateBatch)> {
    keys.chunk_by(|a, b| a.section == b.section)
        .flat_map(|run| {
            let section = run[0].section;
            CoordinateBatch::chunks(run.iter().map(|key| key.coordinate.clone()))
                .map(move |batch| (section, batch))
        })
        .collect()
}

/// Reads distinct provisional cells in ascending coordinate order.
pub(crate) async fn provisional_point_loop<S: CellStore>(
    store: &S,
    collection: &CollectionId,
    section: Section,
    batch: &CoordinateBatch,
) -> Result<CellBuffer<(Coordinate, ProvisionalCell)>, S::Error> {
    let unique_coordinates = sorted_unique_coordinates(batch);
    let mut out: CellBuffer<(Coordinate, ProvisionalCell)> =
        SmallVec::with_capacity(unique_coordinates.len());
    for coordinate in unique_coordinates {
        let cell = CellKey {
            section,
            coordinate: coordinate.clone(),
        };
        if let Some(provisional) = store.provisional_cell_at(collection, &cell).await? {
            out.push((coordinate.clone(), provisional));
        }
    }
    Ok(out)
}
