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
//! Cassandra provisional cells and the commit oracle.
//!
//! [`Overlay`]: crate::state::overlay::Overlay

use super::cell_key::{CellKey, Section};
use super::identity::{CollectionId, StateName, StateType};
use crate::Key;
use bytes::Bytes;
use scc::Guard;
use smallvec::SmallVec;
use std::cmp::Ordering;
use std::ops::RangeInclusive;

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

/// One dirty clear marker's address in the marker tree: the event's Kafka
/// `key`, the collection, and the cleared [`Section`]. A sibling of
/// [`DirtyKey`], not a widened one — the cell tree's key and its ordering stay
/// untouched.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct MarkerKey {
    key: Key,
    state_type: StateType,
    name: StateName,
    section: Section,
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
    /// through) and `rollback()` (which discards them outright). Race-free for
    /// the same reason as [`Self::clear_event`]: no handler op is in flight
    /// while the handler itself awaits the commit or calls the rollback.
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

    /// Discards every cell and dirty clear marker buffered for one event's
    /// `key` — the clear-at-settle / reset move. Race-free:
    /// single-writer-per-key makes this key's sub-range exclusively owned
    /// during its event.
    pub fn clear_event(&self, key: &Key) {
        remove_span(&self.entries, KeyScope::range(key.clone()));
        remove_span(&self.markers, MarkerKeyScope::range(key.clone()));
    }
}

#[cfg(test)]
mod tests;

/// Removes every entry of `tree` inside `range`: snapshot the span's keys
/// through the range read, then remove each key point-wise.
///
/// Deliberately **not** [`scc::TreeIndex::remove_range_sync`]: scc 3.8's bulk
/// range removal can leave the tree in a shape where a later `range` seek
/// through this module's strict-separator bounds ([`Edge`]) misses surviving
/// entries even though point reads still find them — observed as `touched` /
/// `section_snapshot` answering empty after a `clear_section`, i.e. buffered
/// writes silently dropped from the stage. Point removal keeps every later
/// seek sound (pinned by `clear_section_keeps_sibling_section_ranges` in the
/// sibling tests). The doomed-key snapshot is bounded by what this event
/// buffered, inline for the common handful of cells.
fn remove_span<K, V, Q>(tree: &scc::TreeIndex<K, V>, range: RangeInclusive<Q>)
where
    K: Clone + Ord + 'static,
    V: Clone + 'static,
    Q: scc::Comparable<K>,
{
    let guard = Guard::new();
    let doomed: SmallVec<[K; CELLS_INLINE]> = tree
        .range(range, &guard)
        .map(|(key, _)| key.clone())
        .collect();
    drop(guard);
    for key in &doomed {
        tree.remove_sync(key);
    }
}

/// Builds the tree key for one cell of a collection.
fn dirty_key(collection: &CollectionId, cell: &CellKey) -> DirtyKey {
    DirtyKey {
        key: collection.state_key().key.clone(),
        state_type: collection.state_type(),
        name: collection.name().clone(),
        cell: cell.clone(),
    }
}

/// Builds the marker-tree key for one cleared section of a collection.
fn marker_key(collection: &CollectionId, section: Section) -> MarkerKey {
    MarkerKey {
        key: collection.state_key().key.clone(),
        state_type: collection.state_type(),
        name: collection.name().clone(),
        section,
    }
}

/// Which edge of a prefix sub-range a scope bound marks.
///
/// Each scope query below matches a *span* of [`DirtyKey`]s by comparing on a
/// prefix of the key and ignoring the rest (`SectionScope` ignores the
/// coordinate, `KeyScope` ignores everything past the Kafka `key`). A single
/// value compared this way is `Equal` to the whole span, and `scc`'s range
/// start-seek positions by descending the tree against the bound: a bound that
/// is `Equal` to many keys can land the seek in the *middle* of the span and
/// silently skip every cell before it. So a range bound is never such a fat
/// value — it is a strict separator whose comparison tie-breaks past the span:
/// `Low` sinks just below the span's first key, `High` rises just above its
/// last, neither ever `Equal` to a stored key, keeping the seek at the span's
/// true edge so the whole sub-range is visited. Consequently every scope's
/// [`scc::Equivalent`] impl is always false — the contract-consistent
/// definition (`equivalent ⇔ compare == Equal`); widening it to prefix
/// equality would desynchronize it from `compare`.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Edge {
    Low,
    High,
}

impl Edge {
    /// The ordering to return once the prefix compares `Equal`: a `Low` bound
    /// sinks below the span, a `High` bound rises above it.
    fn beyond(self) -> Ordering {
        match self {
            Self::Low => Ordering::Less,
            Self::High => Ordering::Greater,
        }
    }
}

/// Bounding query for every [`DirtyKey`] sharing one Kafka `key` — the
/// whole-event sub-range, for [`DirtyStore::touched`] and
/// [`DirtyStore::clear_event`]. Compares on the Kafka `key` alone; see [`Edge`]
/// for why each bound is a strict separator.
#[derive(Clone, PartialEq, Eq)]
struct KeyScope {
    key: Key,
    edge: Edge,
}

impl KeyScope {
    /// The inclusive separator pair spanning one Kafka `key`'s cells.
    fn range(key: Key) -> RangeInclusive<Self> {
        Self {
            key: key.clone(),
            edge: Edge::Low,
        }..=Self {
            key,
            edge: Edge::High,
        }
    }
}

impl scc::Equivalent<DirtyKey> for KeyScope {
    fn equivalent(&self, key: &DirtyKey) -> bool {
        scc::Comparable::compare(self, key) == Ordering::Equal
    }
}

impl scc::Comparable<DirtyKey> for KeyScope {
    fn compare(&self, key: &DirtyKey) -> Ordering {
        self.key.cmp(&key.key).then(self.edge.beyond())
    }
}

/// Bounding query for every [`DirtyKey`] in one collection — the mid-handler
/// `commit()`/`rollback()` sub-range, for [`DirtyStore::collection_snapshot`]
/// and [`DirtyStore::remove_collection`]. Compares on
/// `(key, state_type, name)`, ignoring the cell, so the range spans the
/// collection's cells across every section in coordinate order; see [`Edge`]
/// for the strict-separator bounds.
#[derive(Clone, PartialEq, Eq)]
struct CollectionScope {
    key: Key,
    state_type: StateType,
    name: StateName,
    edge: Edge,
}

impl CollectionScope {
    /// The inclusive separator pair spanning `collection`'s cells.
    fn range(collection: &CollectionId) -> RangeInclusive<Self> {
        let at = |edge| Self {
            key: collection.state_key().key.clone(),
            state_type: collection.state_type(),
            name: collection.name().clone(),
            edge,
        };
        at(Edge::Low)..=at(Edge::High)
    }

    fn cmp_key(&self, key: &DirtyKey) -> Ordering {
        self.key
            .cmp(&key.key)
            .then(self.state_type.cmp(&key.state_type))
            .then(self.name.cmp(&key.name))
    }
}

impl scc::Equivalent<DirtyKey> for CollectionScope {
    fn equivalent(&self, key: &DirtyKey) -> bool {
        scc::Comparable::compare(self, key) == Ordering::Equal
    }
}

impl scc::Comparable<DirtyKey> for CollectionScope {
    fn compare(&self, key: &DirtyKey) -> Ordering {
        self.cmp_key(key).then(self.edge.beyond())
    }
}

/// Bounding query for every [`DirtyKey`] in one collection-section — the scan
/// leg sub-range, for [`DirtyStore::section_snapshot`]. Compares on
/// `(key, state_type, name, section)`, ignoring the coordinate, so the range
/// spans exactly that section's cells in coordinate order; see [`Edge`] for the
/// strict-separator bounds.
#[derive(Clone, PartialEq, Eq)]
struct SectionScope {
    key: Key,
    state_type: StateType,
    name: StateName,
    section: Section,
    edge: Edge,
}

impl SectionScope {
    /// The inclusive separator pair spanning `collection`'s cells in `section`.
    fn range(collection: &CollectionId, section: Section) -> RangeInclusive<Self> {
        let at = |edge| Self {
            key: collection.state_key().key.clone(),
            state_type: collection.state_type(),
            name: collection.name().clone(),
            section,
            edge,
        };
        at(Edge::Low)..=at(Edge::High)
    }

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
        scc::Comparable::compare(self, key) == Ordering::Equal
    }
}

impl scc::Comparable<DirtyKey> for SectionScope {
    fn compare(&self, key: &DirtyKey) -> Ordering {
        self.cmp_key(key).then(self.edge.beyond())
    }
}

/// [`KeyScope`]'s marker-tree twin: bounds every [`MarkerKey`] sharing one
/// Kafka `key`, for [`DirtyStore::touched`] and [`DirtyStore::clear_event`].
/// Deliberately a plain duplicate of the cell-tree scope, not generic scope
/// machinery — two flat structs read better than a scope abstraction. See
/// [`Edge`] for the strict-separator bounds.
#[derive(Clone, PartialEq, Eq)]
struct MarkerKeyScope {
    key: Key,
    edge: Edge,
}

impl MarkerKeyScope {
    /// The inclusive separator pair spanning one Kafka `key`'s markers.
    fn range(key: Key) -> RangeInclusive<Self> {
        Self {
            key: key.clone(),
            edge: Edge::Low,
        }..=Self {
            key,
            edge: Edge::High,
        }
    }
}

impl scc::Equivalent<MarkerKey> for MarkerKeyScope {
    fn equivalent(&self, key: &MarkerKey) -> bool {
        scc::Comparable::compare(self, key) == Ordering::Equal
    }
}

impl scc::Comparable<MarkerKey> for MarkerKeyScope {
    fn compare(&self, key: &MarkerKey) -> Ordering {
        self.key.cmp(&key.key).then(self.edge.beyond())
    }
}

/// [`CollectionScope`]'s marker-tree twin: bounds every [`MarkerKey`] in one
/// collection, for [`DirtyStore::remove_collection`]. See [`Edge`] for the
/// strict-separator bounds.
#[derive(Clone, PartialEq, Eq)]
struct MarkerCollectionScope {
    key: Key,
    state_type: StateType,
    name: StateName,
    edge: Edge,
}

impl MarkerCollectionScope {
    /// The inclusive separator pair spanning `collection`'s markers.
    fn range(collection: &CollectionId) -> RangeInclusive<Self> {
        let at = |edge| Self {
            key: collection.state_key().key.clone(),
            state_type: collection.state_type(),
            name: collection.name().clone(),
            edge,
        };
        at(Edge::Low)..=at(Edge::High)
    }

    fn cmp_key(&self, key: &MarkerKey) -> Ordering {
        self.key
            .cmp(&key.key)
            .then(self.state_type.cmp(&key.state_type))
            .then(self.name.cmp(&key.name))
    }
}

impl scc::Equivalent<MarkerKey> for MarkerCollectionScope {
    fn equivalent(&self, key: &MarkerKey) -> bool {
        scc::Comparable::compare(self, key) == Ordering::Equal
    }
}

impl scc::Comparable<MarkerKey> for MarkerCollectionScope {
    fn compare(&self, key: &MarkerKey) -> Ordering {
        self.cmp_key(key).then(self.edge.beyond())
    }
}
