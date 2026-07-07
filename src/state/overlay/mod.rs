//! The dirty overlay over a committed cell store.
//!
//! [`Overlay`] is the transactional view a session reads and writes through: a
//! per-event [`DirtyStore`] buffering this handler's `set`/`clear` outcomes,
//! layered over a lower committed [`CellStore`] (`Cached<CassandraStore>` in
//! production, `MemoryCellStore` in tests). Backend genericity comes from the
//! `L: CellStore` bound on the read methods.
//!
//! - `get` short-circuits the dirty leg: a buffered `Set` returns those bytes,
//!   a `Cleared` returns known-absence, an untouched cell falls through to
//!   `lower.get`.
//! - `scan_cells` lazily merges the dirty leg against `lower.scan_cells` in
//!   `coordinate` order — **dirty wins on a key tie**, a dirty `Cleared`
//!   **hides** the lower cell, an untouched cell falls through.
//!
//! The overlay deliberately does **not** implement [`CellStore`]: it exposes
//! only the two transactional reads, so cache-fill and promote paths cannot
//! route through it — a `Cached<Overlay<…>>` composition or a cache fill of
//! this handler's uncommitted dirty value is uncompilable, and durable writes
//! go through [`Overlay::lower`] by construction.
//!
//! # `Send`
//!
//! The merge must stay `Send` (the `+ Send` bound on the `scan_cells`
//! signature). [`scc::TreeIndex`]'s range iterator borrows a `!Send` guard, so
//! the dirty leg is taken as an **owned** sorted snapshot
//! ([`DirtyStore::section_snapshot`], which drops the guard before returning)
//! and merged synchronously against the still-lazy lower stream — nothing
//! `!Send` is held across an `.await`. The snapshot is bounded by what *this*
//! handler buffered (O(handler writes), not O(partition)), so materializing it
//! is not the collect-then-merge the design forbids for large backing ranges.

use super::cell::Committed;
use super::cell_key::{CellKey, Coordinate, Direction, Scan};
use super::dirty::{DirtyStore, DirtyVal};
use super::event_ref::EventRef;
use super::identity::CollectionId;
use super::store::CellStore;
use async_stream::try_stream;
use bytes::Bytes;
use futures::{Stream, StreamExt};
use std::cmp::Ordering;
use std::sync::Arc;

/// A dirty overlay over a lower committed [`CellStore`].
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
    /// stages/promotes through it at `finalize`/`commit_apply`).
    #[must_use]
    pub fn lower(&self) -> &L {
        &self.lower
    }
}

impl<L> Overlay<L>
where
    L: CellStore,
{
    /// Reads one cell through the overlay: a buffered `Set` returns those
    /// bytes, a `Cleared` returns known-absence, an untouched cell falls
    /// through to `lower.get`.
    ///
    /// # Errors
    ///
    /// Propagates the lower store's error on a dirty miss (the dirty leg
    /// itself is infallible).
    pub async fn get<'a>(
        &'a self,
        collection: &'a CollectionId,
        cell: &'a CellKey,
        own: EventRef,
    ) -> Result<Committed, L::Error> {
        match self.dirty.lookup(collection, cell) {
            Some(DirtyVal::Set(bytes)) => Ok(Committed::new(Some(bytes))),
            Some(DirtyVal::Cleared) => Ok(Committed::new(None)),
            None => self.lower.get(collection, cell, own).await,
        }
    }

    /// Scans a range through the overlay, lazily merging the dirty leg
    /// against `lower.scan_cells` in `coordinate` order — dirty wins on a key
    /// tie, a dirty `Cleared` hides the lower cell.
    pub fn scan_cells<'a>(
        &'a self,
        collection: &'a CollectionId,
        scan: Scan<'a>,
        own: EventRef,
    ) -> impl Stream<Item = Result<(CellKey, Bytes), L::Error>> + Send + 'a {
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
        // Strip the limit from the lower leg: a dirty `Cleared` hides a lower
        // cell and a dirty `Set` adds one, so the limit must bound the MERGED
        // output, not the lower leg in isolation. It is counted below; the lower
        // stream stays lazy, so dropping early stops its paging.
        let bottom = self.lower.scan_cells(
            collection,
            Scan {
                limit: None,
                ..scan
            },
            own,
        );
        try_stream! {
            // `top` is an owned, pre-sorted snapshot (the guard was dropped when
            // it was built), walked by index; `bottom` stays a lazy stream.
            let mut ti = 0usize;
            let mut yielded = 0usize;
            let mut bottom = std::pin::pin!(bottom.peekable());
            loop {
                // Apply the merged-output limit (handles `Some(0)` → yield none).
                if scan.limit.is_some_and(|n| yielded >= n) {
                    break;
                }
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
                            yield (key.clone(), bytes.clone());
                            yielded += 1;
                        }
                    }
                    // Tie: dirty wins, the shadowed lower cell is dropped.
                    Ordering::Equal => {
                        let (key, value) = &top[ti];
                        ti += 1;
                        let _ = bottom.as_mut().next().await.transpose()?;
                        if let DirtyVal::Set(bytes) = value {
                            yield (key.clone(), bytes.clone());
                            yielded += 1;
                        }
                    }
                    // Lower cell comes first (untouched by this handler): emit it.
                    Ordering::Greater => {
                        if let Some(item) = bottom.as_mut().next().await {
                            yield item?;
                            yielded += 1;
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
