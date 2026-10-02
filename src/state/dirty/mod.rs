//! The per-partition dirty cell workspace.
//!
//! [`DirtyStore`] is the in-memory write buffer the [`Overlay`] reads and
//! writes: handler `set`/`clear` ops land here as the latest staged outcome per
//! cell, and `finalize` reads them back when it stages provisional cells. It
//! holds, per cell, just [`DirtyVal::Set`]`(bytes)` or [`DirtyVal::Cleared`] —
//! single-writer-per-key makes every cell last-writer-wins, so there is no op
//! algebra and no compaction fold.
//!
//! # One shared structure, per-key sub-ranges
//!
//! The store is **one per-partition shared** [`scc::TreeIndex`] keyed by
//! `DirtyKey` = `(key, state_type, name, cell)`, ordered so a single event's
//! cells (one Kafka `key`) form a contiguous sub-range. Single-writer-per-key
//! makes one key's sub-range exclusively owned during its event, so
//! [`DirtyStore::clear_event`] (a point-removal sweep over that key's
//! sub-range — see [`remove_span`] for why not `remove_range_sync`) is
//! race-free, with no `Mutex` and no per-event map; its only allocation is
//! the doomed-key snapshot, bounded by what the event buffered. The full
//! `(state_type, name)` in the key prevents same-key / different-collection
//! collisions in the shared tree.
//!
//! Section clears ride a **sibling marker tree**: [`DirtyStore::clear_section`]
//! upserts a *dirty clear marker* keyed `(key, state_type, name, section)` and
//! discards the section's already-buffered cells, so from that program point
//! the section reads as deleted and later `set`s repopulate it. The overlay
//! consults the marker on reads ([`DirtyStore::section_cleared`]);
//! [`DirtyStore::touched`] reports each collection's cleared sections beside
//! its cells; [`DirtyStore::clear_event`] / [`DirtyStore::remove_collection`]
//! sweep both trees.
//!
//! The dirty store is volatile and discarded at each settle/attempt boundary —
//! it is **never** a durability or recovery source. Crash recovery runs off the
//! Cassandra provisional cells and collection evidence.
//!
//! [`Overlay`]: crate::state::overlay::Overlay

use super::CELLS_INLINE;
use super::cell_key::{CellKey, Section};
use super::identity::{CollectionId, StateName, StateType};
use crate::Key;
use crate::state::cell_key::CellRef;
use bytes::Bytes;
use scc::Guard;
use smallvec::SmallVec;

mod keys;
#[cfg(test)]
mod tests;

pub(crate) use keys::Edge;
pub(in crate::state) use keys::remove_span;
use keys::{
    CollectionScope, DirtyKey, DirtyRef, KeyScope, MarkerCollectionScope, MarkerKey,
    MarkerKeyScope, SectionScope, dirty_key, marker_key,
};

/// Inline capacity of one event's touched-collection work-list; an event
/// touches a handful of collections.
const COLLECTIONS_INLINE: usize = 4;

/// One collection-section's snapshotted cells (`(cell, outcome)`), owned and
/// coordinate-ordered.
pub type CellSnapshot = SmallVec<[(CellKey, DirtyVal); CELLS_INLINE]>;

/// One collection's dirty cells in committed-write form (`(cell, data)`),
/// owned and coordinate-ordered — the [`CellStore::write_resolved`] input
/// shape, inline like [`CellSnapshot`] so a `commit()` of a Value or a small
/// Map/Deque allocates nothing.
///
/// [`CellStore::write_resolved`]: crate::state::store::CellStore::write_resolved
pub type ResolvedCells = SmallVec<[(CellKey, Option<Bytes>); CELLS_INLINE]>;

/// Inline capacity of one collection's cleared-section list; a collection has
/// two or three sections.
const SECTIONS_INLINE: usize = 2;

/// One collection's sections under a standing dirty clear marker, inline for
/// the handful of sections a collection has.
pub type ClearedSections = SmallVec<[Section; SECTIONS_INLINE]>;

/// One event's touched cells grouped by collection: `(state_type, name)`, the
/// sections it cleared (dirty clear markers), and its [`CellSnapshot`].
pub type TouchedCollection = ((StateType, StateName), ClearedSections, CellSnapshot);

/// One event's distinct touched `(state_type, name)` collections, inline for
/// the common handful. This is the collection identity alone, without cell
/// payloads; [`DirtyStore::touched_collections`] returns it.
pub type TouchedCollectionNames = SmallVec<[(StateType, StateName); COLLECTIONS_INLINE]>;

/// One event's touched collections — the `finalize` work-list, inline for the
/// common handful.
pub type TouchedCollections = SmallVec<[TouchedCollection; COLLECTIONS_INLINE]>;

/// The latest staged outcome for a dirty cell (last-writer-wins).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DirtyVal {
    /// The cell was set to these bytes.
    Set(Bytes),

    /// The cell was cleared (set-to-absent).
    Cleared,
}

impl DirtyVal {
    /// The committed bytes this outcome stages to (`Set` → its bytes,
    /// `Cleared` → absence).
    #[must_use]
    pub fn into_data(self) -> Option<Bytes> {
        match self {
            Self::Set(bytes) => Some(bytes),
            Self::Cleared => None,
        }
    }
}

/// In-memory dirty cell store: the latest [`DirtyVal`] per touched cell plus
/// the standing dirty clear markers, keyed by `DirtyKey`/`MarkerKey`, shared
/// per partition.
#[derive(Debug, Default)]
pub struct DirtyStore {
    entries: scc::TreeIndex<DirtyKey, DirtyVal>,
    markers: scc::TreeIndex<MarkerKey, ()>,
}

impl DirtyStore {
    /// Creates an empty dirty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Buffers a set of one cell's bytes (last-writer-wins), taking the one
    /// owned copy a staged cell requires.
    pub fn set(&self, collection: &CollectionId, cell: &CellKey, bytes: &[u8]) {
        self.set_owned(collection, cell, Bytes::copy_from_slice(bytes));
    }

    /// [`Self::set`] over an already-owned payload: a caller that encoded into
    /// its own `Bytes` moves it in rather than paying a second copy.
    pub fn set_owned(&self, collection: &CollectionId, cell: &CellKey, bytes: Bytes) {
        self.entries
            .upsert_sync(dirty_key(collection, cell), DirtyVal::Set(bytes));
    }

    /// Buffers a clear of one cell (last-writer-wins).
    pub fn clear(&self, collection: &CollectionId, cell: &CellKey) {
        self.entries
            .upsert_sync(dirty_key(collection, cell), DirtyVal::Cleared);
    }

    /// Buffers a dirty clear marker for one collection-section and discards
    /// the section's already-buffered cells: from this program point the
    /// section reads as deleted, and later `set`s repopulate it —
    /// last-writer-wins per cell plus the marker, no op algebra. Discarding
    /// buffered `Cleared` cells too is sound: under the marker they read
    /// identically (absent), and the durable clear's gap erase subsumes every
    /// pre-marker outcome — `finalize` skips a `Cleared` cell in a cleared
    /// section for the same reason. Race-free per key for the same reason as
    /// [`Self::clear_event`].
    pub fn clear_section(&self, collection: &CollectionId, section: Section) {
        self.markers
            .upsert_sync(marker_key(collection, section), ());
        remove_span(&self.entries, SectionScope::range(collection, section));
    }

    /// Whether a dirty clear marker stands for the collection-section — the
    /// [`Overlay`] read hook. Lock-free [`scc::TreeIndex::peek_with`] for the
    /// reason on [`Self::lookup`].
    ///
    /// [`Overlay`]: crate::state::overlay::Overlay
    #[must_use]
    pub fn section_cleared(&self, collection: &CollectionId, section: Section) -> bool {
        self.markers
            .peek_with(&marker_key(collection, section), |_, ()| ())
            .is_some()
    }

    /// The cell's buffered outcome, if any — the [`Overlay`] point lookup.
    ///
    /// Uses the lock-free [`scc::TreeIndex::peek_with`], not `read_sync`: a
    /// [`DirtyVal`] is replaced wholesale by [`Self::set`]/[`Self::clear`]
    /// (never interior-mutated), so the lock-free snapshot read is exactly
    /// right — and it avoids `read_sync`'s lock-retry loop, which spins
    /// forever on a key that was just drained by [`Self::remove_collection`]
    /// in the same (single-threaded) event (the `commit()` → re-read path).
    ///
    /// [`Overlay`]: crate::state::overlay::Overlay
    #[must_use]
    pub fn lookup(&self, collection: &CollectionId, cell: CellRef<'_>) -> Option<DirtyVal> {
        self.entries
            .peek_with(&DirtyRef(collection, cell), |_, value| value.clone())
    }

    /// Returns owned dirty cells from one collection section in coordinate
    /// order. The snapshot releases the [`Guard`] before the overlay awaits
    /// its merge. [`crate::state::overlay::Overlay::scan`] uses this
    /// snapshot for either projection.
    #[must_use]
    pub fn section_snapshot(&self, collection: &CollectionId, section: Section) -> CellSnapshot {
        let guard = Guard::new();
        self.entries
            .range(SectionScope::range(collection, section), &guard)
            .map(|(k, v)| (k.cell.clone(), v.clone()))
            .collect()
    }

    /// An owned, coordinate-ordered snapshot of one collection's dirty cells
    /// across every section, already in committed-write form — the mid-handler
    /// `commit()`'s drain read (cells only; [`Self::cleared_sections`] is its
    /// clear half). Same owned-snapshot rationale as
    /// [`Self::section_snapshot`].
    #[must_use]
    pub fn collection_snapshot(&self, collection: &CollectionId) -> ResolvedCells {
        let guard = Guard::new();
        self.entries
            .range(CollectionScope::range(collection), &guard)
            .map(|(k, v)| (k.cell.clone(), v.clone().into_data()))
            .collect()
    }

    /// One collection's sections under a standing dirty clear marker — the
    /// mid-handler `commit()`'s clear half, beside
    /// [`Self::collection_snapshot`].
    #[must_use]
    pub fn cleared_sections(&self, collection: &CollectionId) -> ClearedSections {
        let guard = Guard::new();
        self.markers
            .range(MarkerCollectionScope::range(collection), &guard)
            .map(|(key, ())| key.section)
            .collect()
    }

    /// Discards one collection's buffered outcomes and dirty clear markers —
    /// the drain shared by the mid-handler `commit()` (which first wrote them
    /// through) and `rollback()` (which discards them outright). Race-free
    /// because both callers hold the session operation gate
    /// (`SessionGate` in [`crate::state::session`]) for their whole body, so
    /// no other session op can interleave with the drain.
    pub fn remove_collection(&self, collection: &CollectionId) {
        remove_span(&self.entries, CollectionScope::range(collection));
        remove_span(&self.markers, MarkerCollectionScope::range(collection));
    }

    /// Whether any dirty cell or clear marker is buffered for the collection —
    /// the mid-handler `rollback()`'s `Applied`/`NoOp` probe, beside
    /// [`Self::remove_collection`]. A lock-free range peek over both trees.
    #[must_use]
    pub fn collection_dirty(&self, collection: &CollectionId) -> bool {
        let guard = Guard::new();
        self.entries
            .range(CollectionScope::range(collection), &guard)
            .next()
            .is_some()
            || self
                .markers
                .range(MarkerCollectionScope::range(collection), &guard)
                .next()
                .is_some()
    }

    /// Groups this event's (one `key`) dirty state by collection — the
    /// `finalize` work-list. Each entry pairs a `(state_type, name)` with its
    /// cleared sections (dirty clear markers) and its touched
    /// `(cell, outcome)` set; a marker-only collection appears with an empty
    /// cell set. The caller rebuilds the [`CollectionId`] from its own state
    /// key.
    #[must_use]
    pub fn touched(&self, key: &Key) -> TouchedCollections {
        let guard = Guard::new();
        let mut grouped = TouchedCollections::new();
        for (marker_key, ()) in self
            .markers
            .range(MarkerKeyScope::range(key.clone()), &guard)
        {
            let collection = (marker_key.state_type, marker_key.name.clone());
            match grouped.iter_mut().find(|(c, ..)| *c == collection) {
                Some((_, cleared, _)) => cleared.push(marker_key.section),
                None => grouped.push((
                    collection,
                    ClearedSections::from_iter([marker_key.section]),
                    CellSnapshot::new(),
                )),
            }
        }
        for (dirty_key, value) in self.entries.range(KeyScope::range(key.clone()), &guard) {
            let collection = (dirty_key.state_type, dirty_key.name.clone());
            let entry = (dirty_key.cell.clone(), value.clone());
            match grouped.iter_mut().find(|(c, ..)| *c == collection) {
                Some((_, _, cells)) => cells.push(entry),
                None => grouped.push((
                    collection,
                    ClearedSections::new(),
                    SmallVec::from_iter([entry]),
                )),
            }
        }
        grouped
    }

    /// The distinct `(state_type, name)` collections this event's dirty overlay
    /// touched, for the single `key`. Projects the marker and entry ranges to
    /// their collection identity. Clones no cell values or snapshots. This is
    /// [`Self::touched`] without the per-cell payload, for callers that need
    /// only the collection names.
    #[must_use]
    pub fn touched_collections(&self, key: &Key) -> TouchedCollectionNames {
        let guard = Guard::new();
        let mut names = TouchedCollectionNames::new();
        let mut push_distinct = |collection: (StateType, StateName)| {
            if !names.contains(&collection) {
                names.push(collection);
            }
        };
        for (marker_key, ()) in self
            .markers
            .range(MarkerKeyScope::range(key.clone()), &guard)
        {
            push_distinct((marker_key.state_type, marker_key.name.clone()));
        }
        for (dirty_key, _value) in self.entries.range(KeyScope::range(key.clone()), &guard) {
            push_distinct((dirty_key.state_type, dirty_key.name.clone()));
        }
        names
    }

    /// Discards every cell and dirty clear marker buffered for one event's
    /// `key` — the clear-at-settle / reset move. Race-free:
    /// single-writer-per-key makes this key's sub-range exclusively owned
    /// during its event.
    pub fn clear_event(&self, key: &Key) {
        remove_span(&self.entries, KeyScope::range(key.clone()));
        remove_span(&self.markers, MarkerKeyScope::range(key.clone()));
    }
}
