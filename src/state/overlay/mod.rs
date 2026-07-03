//! The dirty overlay over a committed cell store.
//!
//! [`Overlay`] is the transactional view a session reads and writes through: a
//! per-event [`DirtyStore`] buffering this handler's `set`/`clear` outcomes,
//! layered over a lower committed [`CellStore`] (`Cached<CassandraStore>` in
//! production, `MemoryCellStore` in tests). It implements [`CellStore`] so the
//! session and collections are written once, backend-generic.
//!
//! - `get` short-circuits the dirty leg: a buffered `Set` returns those bytes,
//!   a `Cleared` returns known-absence, an untouched cell falls through to
//!   `lower.get`.
//! - `scan_cells` lazily merges the dirty leg against `lower.scan_cells` in
//!   `coordinate` order — **dirty wins on a key tie**, a dirty `Cleared`
//!   **hides** the lower cell, an untouched cell falls through.
//! - the batch mutators and `provisional_cells` delegate straight to `lower`
//!   (durable writes happen at `finalize`, never at buffer time).
//!
//! # `Send` (R1)
//!
//! The merge must stay `Send` (the `+ Send` bound on
//! [`CellStore::scan_cells`]). [`scc::TreeIndex`]'s range iterator borrows a
//! `!Send` guard, so the dirty leg is taken as an **owned** sorted snapshot
//! ([`DirtyStore::section_snapshot`], which drops the guard before returning)
//! and merged synchronously against the still-lazy lower stream — nothing
//! `!Send` is held across an `.await`. The snapshot is bounded by what *this*
//! handler buffered (O(handler writes), not O(partition)), so materializing it
//! is not the collect-then-merge the design forbids for large backing ranges.

use super::cell::{Committed, ProvisionalCell, ProvisionalWrite};
use super::cell_key::{CellKey, Coordinate, Direction, Scan};
use super::dirty::{DirtyStore, DirtyVal};
use super::event_ref::EventRef;
use super::identity::{CollectionId, CollectionRef};
use super::store::CellStore;
use crate::timers::duration::CompactDuration;
use async_stream::try_stream;
use bytes::Bytes;
use futures::{Stream, StreamExt};
use std::cmp::Ordering;
use std::future::Future;
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

    /// Buffers a set into the dirty leg.
    pub fn buffer_set(&self, collection: &CollectionId, cell: &CellKey, bytes: &[u8]) {
        self.dirty.set(collection, cell, bytes);
    }

    /// Buffers a clear into the dirty leg.
    pub fn buffer_clear(&self, collection: &CollectionId, cell: &CellKey) {
        self.dirty.clear(collection, cell);
    }
}

impl<L> CellStore for Overlay<L>
where
    L: CellStore,
{
    type Error = L::Error;

    async fn get<'a>(
        &'a self,
        collection: &'a CollectionId,
        cell: &'a CellKey,
        own: EventRef,
    ) -> Result<Committed, Self::Error> {
        match self.dirty.lookup(collection, cell) {
            Some(DirtyVal::Set(bytes)) => Ok(Committed::new(Some(bytes))),
            Some(DirtyVal::Cleared) => Ok(Committed::new(None)),
            None => self.lower.get(collection, cell, own).await,
        }
    }

    fn scan_cells<'a>(
        &'a self,
        collection: &'a CollectionId,
        scan: Scan<'a>,
        own: EventRef,
    ) -> impl Stream<Item = Result<(CellKey, Bytes), Self::Error>> + Send + 'a {
        let dir = scan.dir;
        let limit = scan.limit;
        let mut top = self.dirty.section_snapshot(collection, scan.section);
        // Bound the dirty leg to the scan's range in `dir` before merging:
        // `section_snapshot` yields the whole section, so without this a dirty
        // cell outside the range (or on the wrong side of `start`) would leak
        // into a bounded scan. The lower leg is already range-bounded.
        top.retain(|(key, _)| scan.contains(&key.coordinate));
        // The snapshot is ascending; the lower leg is in `dir` order, so align.
        if dir == Direction::Backward {
            top.reverse();
        }
        // Strip the limit from the lower leg: a dirty `Cleared` hides a lower
        // cell and a dirty `Set` adds one, so the limit must bound the MERGED
        // output, not the lower leg in isolation. It is counted below; the lower
        // stream stays lazy, so dropping early stops its paging.
        let bottom = self.lower.scan_cells(
            collection,
            Scan {
                section: scan.section,
                start: scan.start,
                dir,
                end: scan.end,
                limit: None,
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
                if limit.is_some_and(|n| yielded >= n) {
                    break;
                }
                let order = match (top.get(ti), bottom.as_mut().peek().await) {
                    (None, None) => break,
                    // Dirty-only: take dirty.
                    (Some(_), None) => Ordering::Less,
                    // Lower-only, or lower errored (surfaced when consumed below).
                    (None, Some(_)) | (Some(_), Some(Err(_))) => Ordering::Greater,
                    (Some((tk, _)), Some(Ok((bk, _)))) => {
                        front_cmp(dir, &tk.coordinate, &bk.coordinate)
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

    fn get_for_cache<'a>(
        &'a self,
        collection: &'a CollectionId,
        cell: &'a CellKey,
        own: EventRef,
    ) -> impl Future<Output = Result<(Committed, Option<CompactDuration>), Self::Error>> + Send + 'a
    {
        // Cache-fill mirrors the lower committed projection (and its real TTL),
        // never this handler's uncommitted dirty overlay — so delegate straight
        // to `lower` rather than inheriting the default, which would route
        // through `Overlay::get` and cache the dirty value with a `None` TTL.
        self.lower.get_for_cache(collection, cell, own)
    }

    fn scan_for_cache<'a>(
        &'a self,
        collection: &'a CollectionId,
        scan: Scan<'a>,
        own: EventRef,
    ) -> impl Stream<Item = Result<(CellKey, Bytes, Option<CompactDuration>), Self::Error>> + Send + 'a
    {
        // Cache-fill bypasses the dirty overlay by design (see `get_for_cache`).
        self.lower.scan_for_cache(collection, scan, own)
    }

    fn provisional_cells<'a>(
        &'a self,
        collection: &'a CollectionId,
    ) -> impl Stream<Item = Result<(CellKey, ProvisionalCell), Self::Error>> + Send + 'a {
        self.lower.provisional_cells(collection)
    }

    async fn provisional_cell_at<'a>(
        &'a self,
        collection: &'a CollectionId,
        cell: &'a CellKey,
    ) -> Result<Option<ProvisionalCell>, Self::Error> {
        self.lower.provisional_cell_at(collection, cell).await
    }

    fn write_provisional<'a>(
        &'a self,
        collection: &'a CollectionRef,
        writes: &'a [(CellKey, ProvisionalWrite)],
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
        self.lower.write_provisional(collection, writes)
    }

    fn write_resolved<'a>(
        &'a self,
        collection: &'a CollectionRef,
        cells: &'a [(CellKey, Option<Bytes>)],
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
        self.lower.write_resolved(collection, cells)
    }

    fn mark_resolved<'a>(
        &'a self,
        collection: &'a CollectionRef,
        cells: &'a [CellKey],
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
        self.lower.mark_resolved(collection, cells)
    }

    fn commit_provisional<'a>(
        &'a self,
        collection: &'a CollectionRef,
        writes: &'a [(CellKey, ProvisionalWrite)],
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
        self.lower.commit_provisional(collection, writes)
    }

    fn abort_provisional<'a>(
        &'a self,
        collection: &'a CollectionRef,
        writes: &'a [(CellKey, ProvisionalWrite)],
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
        self.lower.abort_provisional(collection, writes)
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

#[cfg(test)]
mod tests;
