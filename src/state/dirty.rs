//! In-memory dirty cell workspace.
//!
//! [`DirtyStore`] is the per-event write buffer: handler `set`/`clear` ops land
//! here, and the durability boundary reads them back when it stages provisional
//! cells. There is exactly one dirty store per event (per kind), owned uniquely
//! by the session's lane — never shared, never cloned — so it is a bare
//! lock-free [`scc::HashMap`], not an `Arc<Mutex<…>>`.
//!
//! It holds **one combined op per cell**, keyed by `(collection, cell address)`
//! ([`CollectionKind::CellAddr`]). A `set`/`clear` folds the new op into the
//! cell's existing op via [`CollectionKind::combine`] in arrival order, so a
//! hot write-loop on one cell stays O(1) — the op never grows into an unbounded
//! vector. Value collections are single-cell (`CellAddr = ()`) and
//! last-writer-wins, so the combined op is just the latest `Set`/`Clear`,
//! exactly as the original Value-only dirty store compacted.
//!
//! A cell absent from the map is untouched: its read returns
//! [`Read::Unknown`](crate::state::Read) and higher layers fall through to the
//! committed value.
//!
//! The dirty store is volatile and rebuilt per event — it is never a durability
//! or recovery source. Crash recovery runs off the Cassandra provisional cells
//! and the commit oracle.

use super::identity::{CollectionId, CollectionKind};
use super::value::ValueKind;
use ahash::RandomState;
use smallvec::SmallVec;
use std::collections::HashMap;

/// Inline capacity of the `finalize` work-list: the touched-collection count
/// most events stay at or below, so the keyset never spills to the heap.
const TOUCHED_INLINE: usize = 4;

/// Inline capacity of one collection's touched cell-set: a Value collection
/// has one cell; small Maps/Deques stay inline.
const CELLS_INLINE: usize = 4;

/// The map key: one cell, identified by its collection and address.
type CellKey<K> = (CollectionId<K>, <K as CollectionKind>::CellAddr);

/// One collection's touched cells (`(addr, op)`), grouped for batch staging.
type CellGroup<K> =
    SmallVec<[(<K as CollectionKind>::CellAddr, <K as CollectionKind>::Op); CELLS_INLINE]>;

/// One collection's id paired with its touched cells.
pub(crate) type TouchedCollection<K> = (CollectionId<K>, CellGroup<K>);

/// Back-compat alias for the Value-kind dirty store. Value is the only kind
/// wired in production today; the type stays generic so a future kind reuses
/// the same compact-on-write buffer.
pub type DirtyValueStore = DirtyStore<ValueKind>;

/// In-memory dirty cell store: one compacted [`CollectionKind::Op`] per touched
/// cell, keyed by `(collection, address)`.
#[derive(Debug)]
pub struct DirtyStore<K>
where
    K: CollectionKind,
{
    entries: scc::HashMap<CellKey<K>, K::Op, RandomState>,
}

impl<K> Default for DirtyStore<K>
where
    K: CollectionKind,
{
    fn default() -> Self {
        Self {
            entries: scc::HashMap::default(),
        }
    }
}

impl<K> DirtyStore<K>
where
    K: CollectionKind,
{
    /// Creates an empty dirty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Discards every buffered op in place, reusing the allocation. The lane
    /// calls this from `reset` at an attempt boundary; per-key serialization
    /// guarantees no handler op is in flight when it runs.
    pub(crate) fn clear_all(&self) {
        self.entries.clear_sync();
    }

    /// Buffers a set of one cell's bytes, folding it into the cell's existing
    /// op via [`CollectionKind::combine`] (arrival order).
    pub(crate) async fn set(&self, collection: &CollectionId<K>, addr: &K::CellAddr, cell: &[u8]) {
        self.combine_in(collection, addr, K::set_op(cell)).await;
    }

    /// Buffers a clear of one cell, folding it into the cell's existing op.
    pub(crate) async fn clear(&self, collection: &CollectionId<K>, addr: &K::CellAddr) {
        self.combine_in(collection, addr, K::clear_op()).await;
    }

    /// Folds `newest` into the cell's buffered op (or inserts it).
    async fn combine_in(&self, collection: &CollectionId<K>, addr: &K::CellAddr, newest: K::Op) {
        let key = (collection.clone(), addr.clone());
        let entry = self.entries.entry_async(key).await;
        entry
            .and_modify(|existing| *existing = K::combine(existing.clone(), newest.clone()))
            .or_insert(newest);
    }

    /// The cell's buffered combined op, if any.
    pub(crate) fn pending_op(
        &self,
        collection: &CollectionId<K>,
        addr: &K::CellAddr,
    ) -> Option<K::Op> {
        let key = (collection.clone(), addr.clone());
        self.entries.read_sync(&key, |_, op| op.clone())
    }

    /// Removes one cell's buffered op (after a mid-handler flush).
    pub(crate) fn clear_cell(&self, collection: &CollectionId<K>, addr: &K::CellAddr) {
        let key = (collection.clone(), addr.clone());
        self.entries.remove_sync(&key);
    }

    /// Groups every touched cell by its collection — the `finalize` work-list,
    /// shaped for per-collection batch staging.
    ///
    /// Each entry pairs a collection with the `(address, op)` set touched this
    /// event. scc exposes no borrowing iterator, and the work-list must outlive
    /// `finalize`'s async fan-out, so it is materialized here. Distinct
    /// collections are distinct durable partitions, so the lane fans out across
    /// the outer `Vec`; cells within one collection batch into one mutation.
    pub(crate) fn touched_cells(&self) -> SmallVec<[TouchedCollection<K>; TOUCHED_INLINE]> {
        let mut grouped: HashMap<CollectionId<K>, CellGroup<K>> = HashMap::new();
        self.entries.iter_sync(|(id, addr), op| {
            grouped
                .entry(id.clone())
                .or_default()
                .push((addr.clone(), op.clone()));
            true
        });
        grouped.into_iter().collect()
    }
}
