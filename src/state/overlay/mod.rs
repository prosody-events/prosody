//! Reads projected cells through the event's dirty overlay.
//!
//! Dirty values take precedence over committed values. Dirty clears hide cells.
//! A section clear hides every lower cell until the event writes it again.
//! Point and batch reads send only untouched positions to the lower store.
//! Scans merge an owned dirty snapshot with the lazy lower stream in coordinate
//! order.
//!
//! The overlay exposes no durable writes. Cache fills and promotion use the
//! lower store directly. The owned snapshot releases its tree guard before the
//! stream suspends. Its size cannot exceed the event's buffered writes.

use super::cell::{Committed, Projection};
use super::cell_key::{CellKey, Coordinate, Direction, Scan, Section};
use super::dirty::{DirtyStore, DirtyVal};
use super::identity::CollectionId;
use super::store::{CellBuffer, CommittedBatch, CoordinateBatch};
use crate::state::store::CellRead;
use async_stream::try_stream;
use futures::{Stream, StreamExt};
use smallvec::{SmallVec, smallvec};
use std::cmp::Ordering;
use std::sync::Arc;

/// A dirty overlay over a lower committed
/// [`CellStore`](super::store::CellStore).
#[derive(Clone)]
pub struct Overlay<L> {
    dirty: Arc<DirtyStore>,
    lower: L,
}

impl<L> Overlay<L> {
    /// Composes an overlay over `lower` with `dirty` as this event's write
    /// buffer.
    #[must_use]
    pub fn new(dirty: Arc<DirtyStore>, lower: L) -> Self {
        Self { dirty, lower }
    }

    /// The shared dirty workspace (the session enumerates it at `finalize`).
    #[must_use]
    pub fn dirty(&self) -> &Arc<DirtyStore> {
        &self.dirty
    }

    /// The lower committed store (the session reads the committed base and
    /// stages through it at `finalize`; the receipt holds a clone of it for
    /// promote/rollback).
    #[must_use]
    pub fn lower(&self) -> &L {
        &self.lower
    }

    /// Reads one projected cell through the dirty overlay.
    ///
    /// # Errors
    ///
    /// Returns the lower store error when the dirty overlay has no answer.
    pub async fn get<'a, P: Projection>(
        &'a self,
        collection: &'a CollectionId,
        cell: &'a CellKey,
    ) -> Result<Committed<P>, L::Error>
    where
        L: CellRead<P>,
    {
        match self.dirty.lookup(collection, cell) {
            Some(DirtyVal::Set(bytes)) => Ok(Committed::new(Some(P::from_value(bytes)))),
            Some(DirtyVal::Cleared) => Ok(Committed::new(None)),
            // A standing dirty clear marker answers known-absence for the
            // whole section: the cell was erased at the clear and has not
            // been repopulated (a repopulating `set` would have hit above).
            None if self.dirty.section_cleared(collection, cell.section) => {
                Ok(Committed::new(None))
            }
            None => CellRead::<P>::read(&self.lower, collection, cell)
                .await
                .map(|(cell, _)| cell),
        }
    }

    /// Reads one projected answer for each coordinate in input order.
    /// Dirty answers take precedence. One lower batch reads the untouched
    /// positions.
    ///
    /// # Errors
    ///
    /// Returns the lower store error when the dirty overlay has no answer.
    pub async fn get_many<'a, P: Projection>(
        &'a self,
        collection: &'a CollectionId,
        section: Section,
        batch: &'a CoordinateBatch,
    ) -> Result<CommittedBatch<P>, L::Error>
    where
        L: CellRead<P>,
    {
        let (dirty_answers, untouched, untouched_pos) =
            self.classify_batch(collection, section, batch);
        let mut answers: CellBuffer<Option<Committed<P>>> = dirty_answers
            .into_iter()
            .map(|answer| {
                answer.map(|value| match value {
                    DirtyVal::Set(bytes) => Committed::new(Some(P::from_value(bytes))),
                    DirtyVal::Cleared => Committed::new(None),
                })
            })
            .collect();
        // `untouched.len() ≤ batch.len() ≤ CELL_BATCH`, so this yields zero or
        // one lower batch; `untouched_pos` aligns 1:1 with its answers.
        for lower_batch in CoordinateBatch::chunks(untouched) {
            let lower =
                CellRead::<P>::read_many(&self.lower, collection, section, &lower_batch).await?;
            for ((committed, _), &pos) in lower.into_iter().zip(untouched_pos.iter()) {
                answers[pos] = Some(committed);
            }
        }
        // Every position is filled (dirty-answered or lower-scattered), so
        // `flatten` drops nothing; a short lower result — a lower-store
        // alignment violation, the same bug the store layer's own `get_many`
        // default guards — would leave a hole the debug assert catches.
        let out: CommittedBatch<P> = answers.into_iter().flatten().collect();
        debug_assert_eq!(
            out.len(),
            batch.len(),
            "batch read must answer every input position"
        );
        Ok(out)
    }

    /// A dirty `Set` answers its value. A dirty `Cleared` answers absence.
    /// A section clear answers absence for an untouched cell because a later
    /// `Set` matches first. The lower sub-batch keeps untouched input
    /// positions.
    fn classify_batch(
        &self,
        collection: &CollectionId,
        section: Section,
        batch: &CoordinateBatch,
    ) -> (
        CellBuffer<Option<DirtyVal>>,
        CellBuffer<Coordinate>,
        CellBuffer<usize>,
    ) {
        let section_cleared = self.dirty.section_cleared(collection, section);
        let mut answers = smallvec![None; batch.len()];
        let mut untouched = SmallVec::new();
        let mut untouched_pos = SmallVec::new();
        for (i, coordinate) in batch.iter().enumerate() {
            let cell = CellKey {
                section,
                coordinate: Coordinate::clone(coordinate),
            };
            match self.dirty.lookup(collection, &cell) {
                Some(value) => answers[i] = Some(value),
                None if section_cleared => answers[i] = Some(DirtyVal::Cleared),
                None => {
                    untouched.push(Coordinate::clone(coordinate));
                    untouched_pos.push(i);
                }
            }
        }
        (answers, untouched, untouched_pos)
    }

    /// Merges the dirty snapshot with the lower scan in coordinate order.
    /// A dirty value replaces the lower value. A dirty clear hides the lower
    /// cell. A standing dirty clear marker hides the whole lower section.
    /// The merge drops the lower stream unpolled, so no lower query runs.
    /// The stream then contains only the dirty snapshot filtered to the range.
    pub fn scan<'a, P: Projection>(
        &'a self,
        collection: &'a CollectionId,
        scan: Scan<'a>,
    ) -> impl Stream<Item = Result<(CellKey, P::Payload), L::Error>> + Send + 'a
    where
        L: CellRead<P>,
    {
        let bottom = CellRead::<P>::scan(&self.lower, collection, scan);
        let cleared = self.dirty.section_cleared(collection, scan.section);
        let mut top = self.dirty.section_snapshot(collection, scan.section);
        // Bound the dirty leg to the scan's range in `dir` before merging:
        // `section_snapshot` yields the whole section, so without this a dirty
        // cell outside the range (or on the wrong side of `start`) would leak
        // into a bounded scan. The lower leg is already range-bounded.
        top.retain(|(key, _)| scan.contains(&key.coordinate));
        // The snapshot is ascending; the lower leg is in `dir` order, so align.
        if scan.dir == Direction::Backward {
            top.reverse();
        }
        try_stream! {
            if cleared {
                // The dirty clear marker hides the lower section: yield only
                // the post-clear dirty `Set`s.
                for (key, value) in &top {
                    if let DirtyVal::Set(bytes) = value {
                        yield (key.clone(), P::from_value(bytes.clone()));
                    }
                }
                return;
            }
            // `top` is an owned, pre-sorted snapshot (the guard was dropped when
            // it was built), walked by index; `bottom` stays a lazy stream.
            let mut ti = 0usize;
            let mut bottom = std::pin::pin!(bottom.peekable());
            loop {
                let order = match (top.get(ti), bottom.as_mut().peek().await) {
                    (None, None) => break,
                    // Dirty-only: take dirty.
                    (Some(_), None) => Ordering::Less,
                    // Lower-only, or lower errored (surfaced when consumed below).
                    (None, Some(_)) | (Some(_), Some(Err(_))) => Ordering::Greater,
                    (Some((tk, _)), Some(Ok((bk, _)))) => {
                        front_cmp(scan.dir, &tk.coordinate, &bk.coordinate)
                    }
                };
                match order {
                    // Dirty cell comes first (no lower cell at this key): emit it.
                    Ordering::Less => {
                        let (key, value) = &top[ti];
                        ti += 1;
                        if let DirtyVal::Set(bytes) = value {
                            yield (key.clone(), P::from_value(bytes.clone()));
                        }
                    }
                    // Tie: dirty wins, the shadowed lower cell is dropped.
                    Ordering::Equal => {
                        let (key, value) = &top[ti];
                        ti += 1;
                        let _ = bottom.as_mut().next().await.transpose()?;
                        if let DirtyVal::Set(bytes) = value {
                            yield (key.clone(), P::from_value(bytes.clone()));
                        }
                    }
                    // Lower cell comes first (untouched by this handler): emit it.
                    Ordering::Greater => {
                        if let Some(item) = bottom.as_mut().next().await {
                            yield item?;
                        }
                    }
                }
            }
        }
    }
}

/// Ordering of two coordinates in the scan direction: the one that should be
/// yielded *first* compares [`Ordering::Less`].
fn front_cmp(dir: Direction, top: &Coordinate, bottom: &Coordinate) -> Ordering {
    match dir {
        Direction::Forward => top.cmp(bottom),
        Direction::Backward => bottom.cmp(top),
    }
}
