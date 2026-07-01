//! In-memory provisional-coordinate index that bounds the recovery sweep.
//!
//! The recovery sweep
//! ([`sweep_provisional`](super::resolve::sweep_provisional)) must find every
//! in-flight provisional cell without full-scanning the partition. This type is
//! the per-partition memory of *which* coordinates are provisional, so a
//! warm-and-clean quiescence resolves to **zero durable queries** and a cold
//! (post-crash / post-rebalance) sweep costs only one bounded seed read plus
//! one point read per provisional cell.
//!
//! It has two halves, both scoped by [`CollectionId`] (which embeds the key):
//!
//! * `coords` — the live provisional `(collection, cell)` coordinates, "the
//!   cells the sweep must visit." Maintained by the three cell-store mutators
//!   after each durable ack.
//! * `seeded` — the collections whose one-time bounded durable seed read has
//!   run. A fresh store starts every collection **unseeded**; an unseeded
//!   collection must not be trusted to be complete, so its first sweep seeds
//!   from the durable index before short-circuiting.
//!
//! # Invariant
//!
//! **`is_seeded(c)` ⟹ `coords` holds every durably-provisional coordinate of
//! `c`.** The mutators uphold it: a successful stage records its coordinates; a
//! successful promote/rollback clears them; a *failed* stage
//! [`unseed`](Self::unseed)s the collection (a partial durable stage the stale
//! set would otherwise miss), so the next sweep re-seeds from the durable index
//! and restores completeness. A failed promote/rollback leaves the coordinate
//! in place — a harmless over-report the sweep's point-read filter drops.
//!
//! The set is a derived cache, never a source of truth: it is minted fresh per
//! partition acquisition (on `CassandraStore`/`MemoryCellStore`, behind an
//! [`Arc`] so the per-event store clones share one instance) and is always
//! reconstructable from the durable `kind=Index` markers (Cassandra) or the
//! stored provisional cells (memory). Same-[`CollectionId`] cell-store
//! operations are serialized system-wide (one handler per key), so a
//! collection's latch and coordinate transitions never race; `scc` covers
//! concurrency across distinct collections.
//!
//! # Memory bound
//!
//! `coords` self-cleans — an entry is removed the moment its cell resolves — so
//! it is bounded by the partition's **in-flight** provisional cells. `seeded`
//! has no per-op eviction (persisting it across a key's quiet windows is what
//! keeps a recurring key's later quiescences zero-query; evicting it there
//! would re-seed on every activity burst and defeat the goal), so it grows to
//! one bare [`CollectionId`] per `(key, collection)` swept during the
//! acquisition, dropped in full when the store drops at partition revocation.
//! This matches the sibling per-partition cache exactly — `Cached`'s `Coverage`
//! (`cached::coverage`) is likewise a revocation-scoped, unevicted
//! `CollectionId`-keyed map (`CovVolatile`) — and `seeded` is strictly smaller
//! (a bare key vs. an interval set, and only over *swept* keys, a subset of
//! read keys). It is not the forbidden hot-path amortized scratch buffer: it is
//! partition-lifetime cache state, and every entry is a pure re-seed hint
//! (dropping one only costs one bounded `kind=Index` read). Should
//! partition-lifetime per-key cache growth ever need bounding, it is a
//! pre-existing and larger concern on `Coverage` first, and should be bounded
//! uniformly across both — not bolted asymmetrically onto this set.
//!
//! [`Arc`]: std::sync::Arc

use super::cell_key::CellKey;
use super::identity::CollectionId;
use ahash::RandomState;
use scc::HashSet;

/// Per-partition provisional-coordinate index gating the recovery sweep. See
/// the [module docs](self) for the invariant it upholds.
#[derive(Debug, Default)]
pub(in crate::state) struct ProvisionalIndex {
    /// Collections whose one-time durable seed read has run.
    seeded: HashSet<CollectionId, RandomState>,
    /// Live provisional coordinates, keyed by their owning collection.
    coords: HashSet<(CollectionId, CellKey), RandomState>,
}

impl ProvisionalIndex {
    /// Records `cell` as provisional in `collection` (after a durable stage
    /// ack).
    pub(in crate::state) async fn record(&self, collection: &CollectionId, cell: &CellKey) {
        let _ = self
            .coords
            .insert_async((collection.clone(), cell.clone()))
            .await;
    }

    /// Clears `cell` from `collection` (after a durable promote/rollback ack).
    pub(in crate::state) async fn clear(&self, collection: &CollectionId, cell: &CellKey) {
        self.coords
            .remove_async(&(collection.clone(), cell.clone()))
            .await;
    }

    /// Whether `collection`'s seed read has run; an unseeded collection must
    /// not short-circuit the sweep.
    pub(in crate::state) async fn is_seeded(&self, collection: &CollectionId) -> bool {
        self.seeded.contains_async(collection).await
    }

    /// Marks `collection` seeded once its bounded durable seed read completes.
    pub(in crate::state) async fn mark_seeded(&self, collection: &CollectionId) {
        let _ = self.seeded.insert_async(collection.clone()).await;
    }

    /// Drops `collection`'s seeded latch, forcing the next sweep to re-seed
    /// from the durable index. Called when a stage write fails and the set
    /// may have missed a coordinate that nonetheless landed durably.
    pub(in crate::state) async fn unseed(&self, collection: &CollectionId) {
        self.seeded.remove_async(collection).await;
    }

    /// Snapshots `collection`'s provisional coordinates into a `Vec` sized to
    /// the number provisional — the recovery drain buffer. Empty ⟹ the warm
    /// sweep issues no durable reads.
    pub(in crate::state) fn snapshot(&self, collection: &CollectionId) -> Vec<CellKey> {
        let mut out = Vec::new();
        self.coords.iter_sync(|(id, cell)| {
            if id == collection {
                out.push(cell.clone());
            }
            true
        });
        out
    }
}
