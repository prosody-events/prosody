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
//! [`DirtyStore::clear_event`] (a [`scc::TreeIndex::remove_range_sync`] over
//! that key's sub-range) is race-free and allocates nothing per event — no
//! `Mutex`, no per-event map. The full `(state_type, name)` in the key prevents
//! same-key / different-collection collisions in the shared tree.
//!
//! The dirty store is volatile and discarded at each settle/attempt boundary —
//! it is **never** a durability or recovery source. Crash recovery runs off the
//! Cassandra provisional cells and the commit oracle.
//!
//! [`Overlay`]: crate::state::overlay::Overlay

use super::cell_key::CellKey;
use super::identity::{CollectionId, StateName, StateType};
use crate::Key;
use bytes::Bytes;
use scc::Guard;
use smallvec::SmallVec;
use std::cmp::Ordering;

/// Inline capacity of one collection's snapshotted cell set; small
/// Maps/Deques and every Value stay inline.
const CELLS_INLINE: usize = 8;

/// Inline capacity of one event's touched-collection work-list; an event
/// touches a handful of collections.
const COLLECTIONS_INLINE: usize = 4;

/// One collection-section's snapshotted cells (`(cell, outcome)`), owned and
/// coordinate-ordered.
pub type CellSnapshot = SmallVec<[(CellKey, DirtyVal); CELLS_INLINE]>;

/// One collection's dirty cells in committed-write form (`(cell, data)`),
/// owned and coordinate-ordered — the [`CellStore::write_resolved`] input
/// shape, inline like [`CellSnapshot`] so a flush of a Value or a small
/// Map/Deque allocates nothing.
///
/// [`CellStore::write_resolved`]: crate::state::store::CellStore::write_resolved
pub type ResolvedCells = SmallVec<[(CellKey, Option<Bytes>); CELLS_INLINE]>;

/// One event's touched cells grouped by collection: `(state_type, name)` and
/// its [`CellSnapshot`].
pub type TouchedCollection = ((StateType, StateName), CellSnapshot);

/// One event's touched collections — the `finalize` work-list, inline for the
/// common handful.
pub type TouchedCollections = SmallVec<[TouchedCollection; COLLECTIONS_INLINE]>;

/// One dirty cell's address in the shared tree: the event's Kafka `key`, the
/// collection (`state_type`, `name`), and the intra-collection [`CellKey`].
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct DirtyKey {
    key: Key,
    state_type: StateType,
    name: StateName,
    cell: CellKey,
}

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

/// In-memory dirty cell store: the latest [`DirtyVal`] per touched cell, keyed
/// by `DirtyKey`, shared per partition.
#[derive(Debug, Default)]
pub struct DirtyStore {
    entries: scc::TreeIndex<DirtyKey, DirtyVal>,
}

impl DirtyStore {
    /// Creates an empty dirty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Buffers a set of one cell's bytes (last-writer-wins).
    pub fn set(&self, collection: &CollectionId, cell: &CellKey, bytes: &[u8]) {
        self.entries.upsert_sync(
            dirty_key(collection, cell),
            DirtyVal::Set(Bytes::copy_from_slice(bytes)),
        );
    }

    /// Buffers a clear of one cell (last-writer-wins).
    pub fn clear(&self, collection: &CollectionId, cell: &CellKey) {
        self.entries
            .upsert_sync(dirty_key(collection, cell), DirtyVal::Cleared);
    }

    /// The cell's buffered outcome, if any — the [`Overlay`] point lookup.
    ///
    /// Uses the lock-free [`scc::TreeIndex::peek_with`], not `read_sync`: a
    /// [`DirtyVal`] is replaced wholesale by [`Self::set`]/[`Self::clear`]
    /// (never interior-mutated), so the lock-free snapshot read is exactly
    /// right — and it avoids `read_sync`'s lock-retry loop, which spins
    /// forever on a key that was just drained by [`Self::remove_collection`]
    /// in the same (single-threaded) event (the flush → re-read path).
    ///
    /// [`Overlay`]: crate::state::overlay::Overlay
    #[must_use]
    pub fn lookup(&self, collection: &CollectionId, cell: &CellKey) -> Option<DirtyVal> {
        self.entries
            .peek_with(&dirty_key(collection, cell), |_, value| value.clone())
    }

    /// An owned, coordinate-ordered snapshot of one collection-section's dirty
    /// cells — the [`Overlay`] scan's dirty leg.
    ///
    /// Copies the narrow `(key, state_type, name, section)` sub-range into an
    /// owned `SmallVec` and drops the `!Send` [`Guard`] before returning, so
    /// the overlay merge holds nothing `!Send` across an `.await` (the `+
    /// Send` requirement on the scan stream — see [`Overlay`]'s `scan_cells`).
    ///
    /// [`Overlay`]: crate::state::overlay::Overlay
    #[must_use]
    pub fn section_snapshot(
        &self,
        collection: &CollectionId,
        section: super::cell_key::Section,
    ) -> CellSnapshot {
        let scope = SectionScope {
            key: collection.state_key().key.clone(),
            state_type: collection.state_type(),
            name: collection.name().clone(),
            section,
        };
        let guard = Guard::new();
        self.entries
            .range(scope.clone()..=scope, &guard)
            .map(|(k, v)| (k.cell.clone(), v.clone()))
            .collect()
    }

    /// An owned, coordinate-ordered snapshot of one collection's dirty cells
    /// across every section, already in committed-write form — the mid-handler
    /// flush's drain read. Same owned-snapshot rationale as
    /// [`Self::section_snapshot`].
    #[must_use]
    pub fn collection_snapshot(&self, collection: &CollectionId) -> ResolvedCells {
        let scope = collection_scope(collection);
        let guard = Guard::new();
        self.entries
            .range(scope.clone()..=scope, &guard)
            .map(|(k, v)| (k.cell.clone(), v.clone().into_data()))
            .collect()
    }

    /// Discards one collection's buffered outcomes (after a mid-handler flush
    /// wrote them through). Race-free for the same reason as
    /// [`Self::clear_event`]: no handler op is in flight while the handler
    /// itself awaits the flush.
    pub fn remove_collection(&self, collection: &CollectionId) {
        let scope = collection_scope(collection);
        self.entries.remove_range_sync(scope.clone()..=scope);
    }

    /// Groups this event's (one `key`) dirty cells by collection — the
    /// `finalize` work-list. Each entry pairs a `(state_type, name)` with its
    /// touched `(cell, outcome)` set; the caller rebuilds the [`CollectionId`]
    /// from its own state key.
    #[must_use]
    pub fn touched(&self, key: &Key) -> TouchedCollections {
        let scope = KeyScope(key.clone());
        let guard = Guard::new();
        let mut grouped = TouchedCollections::new();
        for (dirty_key, value) in self.entries.range(scope.clone()..=scope, &guard) {
            let collection = (dirty_key.state_type, dirty_key.name.clone());
            let entry = (dirty_key.cell.clone(), value.clone());
            match grouped.iter_mut().find(|(c, _)| *c == collection) {
                Some((_, cells)) => cells.push(entry),
                None => grouped.push((collection, SmallVec::from_iter([entry]))),
            }
        }
        grouped
    }

    /// Discards every cell buffered for one event's `key` — the clear-at-settle
    /// / reset move. Race-free: single-writer-per-key makes this key's
    /// sub-range exclusively owned during its event.
    pub fn clear_event(&self, key: &Key) {
        let scope = KeyScope(key.clone());
        self.entries.remove_range_sync(scope.clone()..=scope);
    }
}

#[cfg(test)]
mod tests;
/// Builds the tree key for one cell of a collection.
fn dirty_key(collection: &CollectionId, cell: &CellKey) -> DirtyKey {
    DirtyKey {
        key: collection.state_key().key.clone(),
        state_type: collection.state_type(),
        name: collection.name().clone(),
        cell: cell.clone(),
    }
}

/// Query matching every [`DirtyKey`] sharing one Kafka `key` — the whole-event
/// sub-range, for [`DirtyStore::touched`] and [`DirtyStore::clear_event`].
#[derive(Clone, PartialEq, Eq)]
struct KeyScope(Key);

impl scc::Equivalent<DirtyKey> for KeyScope {
    fn equivalent(&self, key: &DirtyKey) -> bool {
        self.0 == key.key
    }
}

impl scc::Comparable<DirtyKey> for KeyScope {
    fn compare(&self, key: &DirtyKey) -> Ordering {
        self.0.cmp(&key.key)
    }
}

/// Builds the query matching every [`DirtyKey`] in `collection`.
fn collection_scope(collection: &CollectionId) -> CollectionScope {
    CollectionScope {
        key: collection.state_key().key.clone(),
        state_type: collection.state_type(),
        name: collection.name().clone(),
    }
}

/// Query matching every [`DirtyKey`] in one collection — the mid-handler
/// flush sub-range, for [`DirtyStore::collection_snapshot`] and
/// [`DirtyStore::remove_collection`]. Compares on `(key, state_type, name)`,
/// ignoring the cell, so the inclusive range spans the collection's cells
/// across every section in coordinate order.
#[derive(Clone, PartialEq, Eq)]
struct CollectionScope {
    key: Key,
    state_type: StateType,
    name: StateName,
}

impl CollectionScope {
    fn cmp_key(&self, key: &DirtyKey) -> Ordering {
        self.key
            .cmp(&key.key)
            .then(self.state_type.cmp(&key.state_type))
            .then(self.name.cmp(&key.name))
    }
}

impl scc::Equivalent<DirtyKey> for CollectionScope {
    fn equivalent(&self, key: &DirtyKey) -> bool {
        self.cmp_key(key) == Ordering::Equal
    }
}

impl scc::Comparable<DirtyKey> for CollectionScope {
    fn compare(&self, key: &DirtyKey) -> Ordering {
        self.cmp_key(key)
    }
}

/// Query matching every [`DirtyKey`] in one collection-section — the scan leg
/// sub-range, for [`DirtyStore::section_snapshot`]. Compares on
/// `(key, state_type, name, section)`, ignoring the coordinate, so the
/// inclusive range spans exactly that section's cells in coordinate order.
#[derive(Clone, PartialEq, Eq)]
struct SectionScope {
    key: Key,
    state_type: StateType,
    name: StateName,
    section: super::cell_key::Section,
}

impl SectionScope {
    fn cmp_key(&self, key: &DirtyKey) -> Ordering {
        self.key
            .cmp(&key.key)
            .then(self.state_type.cmp(&key.state_type))
            .then(self.name.cmp(&key.name))
            .then(self.section.cmp(&key.cell.section))
    }
}

impl scc::Equivalent<DirtyKey> for SectionScope {
    fn equivalent(&self, key: &DirtyKey) -> bool {
        self.cmp_key(key) == Ordering::Equal
    }
}

impl scc::Comparable<DirtyKey> for SectionScope {
    fn compare(&self, key: &DirtyKey) -> Ordering {
        self.cmp_key(key)
    }
}
