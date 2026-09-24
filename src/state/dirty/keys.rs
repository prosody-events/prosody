//! Tree keys and range probes for the dirty cell and marker trees.

use super::CELLS_INLINE;
use crate::Key;
use crate::state::cell_key::{CellKey, CellRef, Section};
use crate::state::identity::{CollectionId, StateName, StateType};
use scc::Guard;
use smallvec::SmallVec;
use std::cmp::Ordering;
use std::ops::RangeInclusive;

/// One dirty cell's address in the shared tree: the event's Kafka `key`, the
/// collection (`state_type`, `name`), and the intra-collection [`CellKey`].
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct DirtyKey {
    pub(super) key: Key,
    pub(super) state_type: StateType,
    pub(super) name: StateName,
    pub(super) cell: CellKey,
}

/// One dirty clear marker's address in the marker tree: the event's Kafka
/// `key`, the collection, and the cleared [`Section`]. A sibling of
/// [`DirtyKey`], not a widened one — the cell tree's key and its ordering stay
/// untouched.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct MarkerKey {
    pub(super) key: Key,
    pub(super) state_type: StateType,
    pub(super) name: StateName,
    pub(super) section: Section,
}

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
/// sibling tests). The doomed-key snapshot is bounded by the span's size,
/// inline for the common handful of entries.
pub(in crate::state) fn remove_span<K, V, Q>(tree: &scc::TreeIndex<K, V>, range: RangeInclusive<Q>)
where
    K: Clone + Ord,
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

/// A borrowed tree lookup with the same ordering as `DirtyKey`.
pub(super) struct DirtyRef<'a>(pub(super) &'a CollectionId, pub(super) CellRef<'a>);

impl scc::Equivalent<DirtyKey> for DirtyRef<'_> {
    fn equivalent(&self, key: &DirtyKey) -> bool {
        scc::Comparable::compare(self, key) == Ordering::Equal
    }
}

impl scc::Comparable<DirtyKey> for DirtyRef<'_> {
    fn compare(&self, key: &DirtyKey) -> Ordering {
        (
            &self.0.state_key().key,
            self.0.state_type(),
            self.0.name(),
            self.1,
        )
            .cmp(&(&key.key, key.state_type, &key.name, key.cell.as_ref()))
    }
}

/// Builds the tree key for one cell of a collection.
pub(super) fn dirty_key(collection: &CollectionId, cell: &CellKey) -> DirtyKey {
    DirtyKey {
        key: collection.state_key().key.clone(),
        state_type: collection.state_type(),
        name: collection.name().clone(),
        cell: cell.clone(),
    }
}

/// Builds the marker-tree key for one cleared section of a collection.
pub(super) fn marker_key(collection: &CollectionId, section: Section) -> MarkerKey {
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
pub(crate) enum Edge {
    Low,
    High,
}

impl Edge {
    /// The ordering to return once the prefix compares `Equal`: a `Low` bound
    /// sinks below the span, a `High` bound rises above it.
    pub(crate) fn beyond(self) -> Ordering {
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
pub(super) struct KeyScope {
    key: Key,
    edge: Edge,
}

impl KeyScope {
    /// The inclusive separator pair spanning one Kafka `key`'s cells.
    pub(super) fn range(key: Key) -> RangeInclusive<Self> {
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
pub(super) struct CollectionScope {
    key: Key,
    state_type: StateType,
    name: StateName,
    edge: Edge,
}

impl CollectionScope {
    /// The inclusive separator pair spanning `collection`'s cells.
    pub(super) fn range(collection: &CollectionId) -> RangeInclusive<Self> {
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
pub(super) struct SectionScope {
    key: Key,
    state_type: StateType,
    name: StateName,
    section: Section,
    edge: Edge,
}

impl SectionScope {
    /// The inclusive separator pair spanning `collection`'s cells in `section`.
    pub(super) fn range(collection: &CollectionId, section: Section) -> RangeInclusive<Self> {
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
pub(super) struct MarkerKeyScope {
    key: Key,
    edge: Edge,
}

impl MarkerKeyScope {
    /// The inclusive separator pair spanning one Kafka `key`'s markers.
    pub(super) fn range(key: Key) -> RangeInclusive<Self> {
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
pub(super) struct MarkerCollectionScope {
    key: Key,
    state_type: StateType,
    name: StateName,
    edge: Edge,
}

impl MarkerCollectionScope {
    /// The inclusive separator pair spanning `collection`'s markers.
    pub(super) fn range(collection: &CollectionId) -> RangeInclusive<Self> {
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
