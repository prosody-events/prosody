//! Fjall-backed cell cache.
//!
//! [`FjallCellCache`] stores one tagged cell per [`CellKey`] in a fjall
//! keyspace: the committed-cell K/V store
//! [`Cached`](crate::state::cached::Cached) serves point hits from. It does
//! **not** implement `CellStore`: it is a
//! concrete *partial* upper (it can only answer what it has mirrored), so a
//! bare cache view can never be mistaken for a complete store — a miss asserts
//! nothing and always falls through (KV2, owned by `Cached`).
//!
//! Three consumers share the workspace: the committed cell entries (the
//! `cache` keyspace), the warm provisional index + seeded latch (the `index`
//! keyspace), and the `MarkerPresence` latch (also `index`). All three
//! observe the one shared **cache fuse** (`FjallCellCache::fuse_blown`);
//! how a blown fuse partitions them is documented on `Cached`'s retry
//! posture.
//!
//! # Workspace ownership
//!
//! In production the cache **owns** its [`FjallWorkspace`] (built via
//! [`FjallCellCache::for_workspace`]). The workspace's `Drop` deletes the fjall
//! keyspace, so the cache must hold it alive for the whole partition
//! assignment — it lives in the partition's state manager and drops only at
//! revocation. Test caches built from a bare handle ([`FjallCellCache::new`])
//! own no workspace.
//!
//! # Three-valued reads and TTL co-expiry
//!
//! Unlike the durable stores (Memory/Cassandra) whose `get` returns only
//! `Present`/`Absent`, the cache observes a third state: an entry that has
//! never been populated. That state is encoded as the **absence of an entry**
//! in the fjall keyspace, and decodes as the codec's three-valued
//! `Read::Unknown` (see the `codec` module's cell-frame doc for the tag/expiry
//! wire layout). The cache enforces the TTL on read against its `Clock` (an
//! expired entry reads as a miss). The payload is stored verbatim — fjall
//! block-compresses the on-disk data block via LZ4, so there is no per-cell
//! codec layer.
//!
//! # Blocking I/O
//!
//! fjall's public API is synchronous, so the cache's reads and writes are
//! dispatched through [`tokio::task::spawn_blocking`], which clones the cheap
//! `Arc`-backed handle into each blocking closure.

mod codec;
mod error;
mod workspace;

#[cfg(test)]
pub(crate) mod test_db;
#[cfg(test)]
mod tests;

pub use error::FjallCellCacheError;
pub use workspace::{FjallClient, FjallClientError, FjallWorkspace};

use self::codec::Read;
use crate::state::CollectionId;
use crate::state::cell::{Committed, ProvisionalWrite};
use crate::state::cell_key::{CellKey, Section};
use crate::state::store::{CellBuffer, CommittedBatch, CoordinateBatch};
use ahash::RandomState;
use bytes::Bytes;
use educe::Educe;
use fjall::{Database, Keyspace, OwnedWriteBatch, Slice};
use opentelemetry::global::meter;
use opentelemetry::metrics::Counter;
use smallvec::SmallVec;
use std::collections::HashSet;
use std::future::Future;
use std::ops::Bound;
#[cfg(test)]
use std::sync::atomic::AtomicU64;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::task::spawn_blocking;
use tracing::warn;

/// Rows examined per blocking hop of a chunked
/// [`delete_section`](FjallCellCache::delete_section) walk: each hop collects
/// at most this many keys in one [`spawn_blocking`], deletes them in one
/// bounded write batch, then re-seeks from the last key it saw. A section
/// delete therefore holds O(hop) keys in RAM — never the whole section (the
/// bounded-RAM invariant) — while the synchronous fjall range guard still
/// never crosses an `.await`.
const SCAN_HOP_ROWS: usize = 256;

/// The one-way cache-fuse counter, bumped once per blow (see
/// [`FjallCellCache::blow_fuse`]).
static FUSE_BLOWN: LazyLock<Counter<u64>> = LazyLock::new(|| {
    meter("prosody")
        .u64_counter("prosody.state.cache_fuse_blown")
        .with_description("Keyed-state cache fuses blown (one per degraded assignment)")
        .with_unit("{fuse}")
        .build()
});

/// The cache's `now` source for TTL co-expiry, in milliseconds since the Unix
/// epoch.
///
/// A non-`dyn` seam: production reads the [`Wall`](Self::Wall) clock; a test
/// can pin time with `Fixed` and advance the shared counter past
/// a stamped expiry **without sleeping**, so the TTL-expiry property is
/// deterministic. The cache stamps expiries with the same source it reads them
/// against, so the two never disagree.
#[derive(Clone, Educe)]
#[educe(Debug)]
pub(crate) enum Clock {
    /// The system wall clock.
    Wall,
    /// A test-controlled clock over a shared millisecond counter.
    #[cfg(test)]
    Fixed(#[educe(Debug(ignore))] Arc<AtomicU64>),
}

impl Clock {
    /// The current time in milliseconds since the Unix epoch. The wall arm
    /// saturates a pre-epoch clock to 0 (a misconfigured host only expires
    /// fjall entries early, which self-heals via fall-through).
    #[must_use]
    pub fn now_ms(&self) -> u64 {
        match self {
            Self::Wall => SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX)),
            #[cfg(test)]
            Self::Fixed(now) => now.load(Ordering::Relaxed),
        }
    }
}

/// The three-state result of a [`FjallCellCache::get`].
#[derive(Clone, Debug)]
pub(crate) enum CacheRead {
    /// An unexpired entry (a `Present` value or an authoritative `Absent`).
    Hit(Committed),
    /// An entry exists but its stamped expiry has passed; the caller falls
    /// through to the lower store and re-publishes a fresh entry.
    Expired,
    /// No entry exists (the cell was never published, or its entry was
    /// deleted by a repair).
    Miss,
}

/// Fjall-backed cell cache.
#[derive(Clone, Educe)]
#[educe(Debug)]
pub struct FjallCellCache {
    #[educe(Debug(ignore))]
    inner: Arc<Inner>,
    clock: Clock,
    /// The one-way **cache fuse**, shared by every clone and every
    /// [`MarkerPresence`] handle of one workspace (see
    /// [`fuse_blown`](Self::fuse_blown)).
    #[educe(Debug(ignore))]
    fuse: Arc<AtomicBool>,
    /// Test-only fault seam: when set, every [`put`](Self::put),
    /// [`put_batch`](Self::put_batch), and [`commit_batch`](Self::commit_batch)
    /// returns an engine error without touching fjall, so a test can force a
    /// publish failure (the D1 repair path).
    #[cfg(test)]
    #[educe(Debug(ignore))]
    fail_puts: Arc<AtomicBool>,
    /// Test-only fault seam: when set, [`index_snapshot`](Self::index_snapshot)
    /// returns an engine error, so a test can force the warm coords read to
    /// fail while the collection stays seeded — the branch that must
    /// degrade the recovery sweep to the cold durable re-seed rather than
    /// fabricate an empty (clean) sweep that would strand a provisional
    /// cell.
    #[cfg(test)]
    #[educe(Debug(ignore))]
    fail_index_snapshot: Arc<AtomicBool>,
    /// Test-only fault seam: when set, [`index_record`](Self::index_record)
    /// and [`index_record_batch`](Self::index_record_batch)
    /// return an engine error, so a test can force a cold-seed record to fail
    /// and assert the collection is left **unseeded** (the next sweep re-seeds)
    /// rather than latched seeded over an incomplete coords set.
    #[cfg(test)]
    #[educe(Debug(ignore))]
    fail_index_record: Arc<AtomicBool>,
    /// Test-only fault seam: a countdown of delete-side calls to fail — each
    /// failure decrements it — consulted by
    /// [`delete_batch`](Self::delete_batch),
    /// [`delete_section`](Self::delete_section), and
    /// [`index_unseed`](Self::index_unseed), so a test can make exactly the
    /// next N must-succeed deletes fail (forcing the retry path, or blowing
    /// the fuse past the budget) and then heal automatically.
    #[cfg(test)]
    #[educe(Debug(ignore))]
    fail_deletes: Arc<AtomicU64>,
    /// Test-only fault seam: when set, [`get_batch`](Self::get_batch)'s
    /// blocking probe returns an engine error for the whole hop, so a test
    /// can force the batch probe to error over a live entry (the read-fill
    /// no-delete degrade).
    #[cfg(test)]
    #[educe(Debug(ignore))]
    fail_reads: Arc<AtomicBool>,
    /// Test-only: counts blocking hops [`get_batch`](Self::get_batch) launches.
    /// Bumped INSIDE its `spawn_blocking` closure (the seam, not the method
    /// boundary), so an accidental per-key probe loop would count `>1`.
    #[cfg(test)]
    #[educe(Debug(ignore))]
    blocking_probes: Arc<AtomicU64>,
}

/// Backing for a [`FjallCellCache`]: either a bare cache handle plus its
/// owning database (tests) or an owned per-partition workspace whose cache
/// handle the cache operates and whose `Drop` deletes the keyspace at
/// revocation (production).
///
/// The [`Database`] is held in both arms because batch writes are issued
/// through [`Database::batch`], not the keyspace handle. The `index` keyspace
/// (warm provisional coordinates and the cold-seed and marker-presence
/// latches)
/// rides alongside `cache` in
/// both arms purely for lifecycle co-location — it shares the workspace's
/// lifecycle (cold at a fresh assignment, dropped at revocation). Index and
/// cell-cache writes are **not** issued as one cross-keyspace batch; the warm
/// index is a rebuildable hint (a fresh assignment re-seeds from the durable
/// event marker), so they need no atomicity with the committed-value write.
enum Inner {
    Bare {
        database: Database,
        cache: Keyspace,
        index: Keyspace,
    },
    Owned(FjallWorkspace),
}

impl Inner {
    /// The cache keyspace handle this cache operates.
    fn handle(&self) -> &Keyspace {
        match self {
            Self::Bare { cache, .. } => cache,
            Self::Owned(workspace) => workspace.cache_handle(),
        }
    }

    /// The warm-index keyspace handle (provisional coordinates and the
    /// cold-seed and marker-presence latches).
    fn index_handle(&self) -> &Keyspace {
        match self {
            Self::Bare { index, .. } => index,
            Self::Owned(workspace) => workspace.index_handle(),
        }
    }

    /// The database the cache keyspace belongs to — the owner of [`batch`]
    /// writes.
    ///
    /// [`batch`]: Database::batch
    fn database(&self) -> &Database {
        match self {
            Self::Bare { database, .. } => database,
            Self::Owned(workspace) => workspace.database(),
        }
    }
}

impl FjallCellCache {
    /// Builds a cache over opened `cache` + `index` `Keyspace`s and their
    /// owning `Database`, owning no workspace.
    ///
    /// The caller owns the database the handles belong to and is responsible
    /// for keeping them alive for the cache's lifetime. Used by tests;
    /// production uses [`Self::for_workspace`], which owns the workspace.
    #[must_use]
    pub fn new(database: Database, cache: Keyspace, index: Keyspace) -> Self {
        Self::from_parts(
            Inner::Bare {
                database,
                cache,
                index,
            },
            Clock::Wall,
        )
    }

    /// Builds a bare cache over `cache` + `index` driven by a test-controlled
    /// [`Clock`], so a TTL-expiry property can advance time past a stamped
    /// expiry deterministically.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn with_clock(
        database: Database,
        cache: Keyspace,
        index: Keyspace,
        clock: Clock,
    ) -> Self {
        Self::from_parts(
            Inner::Bare {
                database,
                cache,
                index,
            },
            clock,
        )
    }

    /// Builds the production cache, taking ownership of the per-partition
    /// [`FjallWorkspace`].
    ///
    /// The cache operates the workspace's cache handle and holds the workspace
    /// alive, so the workspace's `Drop` — which deletes the fjall keyspace —
    /// fires only when the cache (and thus the partition's state manager) is
    /// dropped at revocation.
    #[must_use]
    pub fn for_workspace(workspace: FjallWorkspace) -> Self {
        Self::from_parts(Inner::Owned(workspace), Clock::Wall)
    }

    /// The single struct-literal site, so the cfg-gated test fields stay in one
    /// place.
    fn from_parts(inner: Inner, clock: Clock) -> Self {
        Self {
            inner: Arc::new(inner),
            clock,
            fuse: Arc::new(AtomicBool::new(false)),
            #[cfg(test)]
            fail_puts: Arc::new(AtomicBool::new(false)),
            #[cfg(test)]
            fail_index_snapshot: Arc::new(AtomicBool::new(false)),
            #[cfg(test)]
            fail_index_record: Arc::new(AtomicBool::new(false)),
            #[cfg(test)]
            fail_deletes: Arc::new(AtomicU64::new(0)),
            #[cfg(test)]
            fail_reads: Arc::new(AtomicBool::new(false)),
            #[cfg(test)]
            blocking_probes: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Whether the workspace's one-way **cache fuse** has blown.
    ///
    /// The fuse lives in the shared inner state, so every [`Cached`] clone and
    /// [`MarkerPresence`] handle of one workspace observes the same bit — a
    /// per-clone fuse would let the next event's intact clone serve the very
    /// stale hit the blown clone made unreachable. Consumers snapshot it once
    /// at their own entry (one admission decision per verb); it never resets
    /// within an assignment and dies with the workspace (its removal path).
    ///
    /// [`Cached`]: crate::state::cached::Cached
    #[must_use]
    pub(crate) fn fuse_blown(&self) -> bool {
        self.fuse.load(Ordering::Relaxed)
    }

    /// Blows the cache fuse: permanently (for this assignment) degrades the
    /// cache to a loud durable passthrough. Called by a must-succeed delete
    /// that exhausted its retry budget — the delete "lands or fuses", so a
    /// sick local disk can never stall settlement or leave a stale entry
    /// reachable. Loud by contract: the first blow warns and bumps the metric
    /// once per degraded assignment; later calls are no-ops.
    pub(crate) fn blow_fuse(&self) {
        if !self.fuse.swap(true, Ordering::Relaxed) {
            warn!("keyed-state cache fuse blown; degrading the assignment to durable reads");
            FUSE_BLOWN.add(1, &[]);
        }
    }

    /// Test handle on the [`put`](Self::put) fault seam: returns the shared
    /// flag a test sets to force every publish to fail (then unsets to
    /// heal).
    #[cfg(test)]
    #[must_use]
    pub fn fail_puts(&self) -> Arc<AtomicBool> {
        self.fail_puts.clone()
    }

    /// Test handle on the [`index_snapshot`](Self::index_snapshot) fault seam:
    /// the shared flag a test sets to force the warm coords read to fail.
    #[cfg(test)]
    #[must_use]
    pub fn fail_index_snapshot(&self) -> Arc<AtomicBool> {
        self.fail_index_snapshot.clone()
    }

    /// Test handle on the [`index_record`](Self::index_record) fault seam: the
    /// shared flag a test sets to force a cold-seed record to fail.
    #[cfg(test)]
    #[must_use]
    pub fn fail_index_record(&self) -> Arc<AtomicBool> {
        self.fail_index_record.clone()
    }

    /// Test handle on the delete-side fault seam: the shared countdown of
    /// [`delete_batch`](Self::delete_batch) /
    /// [`delete_section`](Self::delete_section)
    /// / [`index_unseed`](Self::index_unseed) calls to fail before healing.
    #[cfg(test)]
    #[must_use]
    pub fn fail_deletes(&self) -> Arc<AtomicU64> {
        self.fail_deletes.clone()
    }

    /// Test handle on the [`get_batch`](Self::get_batch) fault seam: the shared
    /// flag a test sets to force the batch probe's whole blocking hop to error
    /// (then unsets to heal).
    #[cfg(test)]
    #[must_use]
    pub fn fail_reads(&self) -> Arc<AtomicBool> {
        self.fail_reads.clone()
    }

    /// Test-only: blocking hops [`get_batch`](Self::get_batch) has launched —
    /// exactly one per batch probe, however many keys it carries.
    #[cfg(test)]
    #[must_use]
    pub fn probe_hops(&self) -> u64 {
        self.blocking_probes.load(Ordering::Relaxed)
    }

    /// Consumes one charge of the delete-side fault seam, returning the
    /// injected error while charges remain.
    #[cfg(test)]
    fn injected_delete_failure(&self) -> Result<(), FjallCellCacheError> {
        if self
            .fail_deletes
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_sub(1))
            .is_ok()
        {
            return Err(FjallCellCacheError::Injected);
        }
        Ok(())
    }

    /// The cache's `now` source, shared by reads (expiry checks) and the
    /// [`Cached`](crate::state::cached::Cached) cache's expiry stamping so the
    /// two never disagree.
    #[must_use]
    pub(crate) fn clock(&self) -> &Clock {
        &self.clock
    }

    /// Test-only: writes raw `bytes` verbatim at the cell's key, so a test can
    /// seed a corrupt frame the read path must degrade on.
    #[cfg(test)]
    pub(crate) async fn seed_raw_cell(
        &self,
        collection: &CollectionId,
        cell: &CellKey,
        bytes: Bytes,
    ) -> Result<(), FjallCellCacheError> {
        write_cell(
            self.inner.handle(),
            codec::cell_key(collection, cell),
            bytes,
        )
        .await
    }

    /// Mints a [`MarkerPresence`] handle over this cache's warm-index keyspace
    /// — the bottom store's bounded marker-checked latch, sharing the
    /// workspace's per-assignment lifecycle (cold exactly when the store's RAM
    /// memo is, dropped at revocation).
    #[must_use]
    pub(crate) fn presence(&self) -> MarkerPresence {
        MarkerPresence {
            index: self.inner.index_handle().clone(),
            fuse: self.fuse.clone(),
        }
    }

    /// Looks up one cell's committed value as a three-state [`CacheRead`]: a
    /// [`Hit`](CacheRead::Hit) on an unexpired `Present`/`Absent` entry, an
    /// [`Expired`](CacheRead::Expired) when the entry exists but its stamped
    /// expiry has passed, or a [`Miss`](CacheRead::Miss) when no entry exists.
    ///
    /// The caller distinguishes these because a coordinate may have no entry
    /// (nothing was ever published — a `Miss`) or an entry that floor-expired
    /// (fall through and re-fetch — an `Expired`). Both differ from a present
    /// `Absent` tag (`Hit(Committed(None))`), which is an authoritative answer.
    pub(crate) async fn get(
        &self,
        collection: &CollectionId,
        cell: &CellKey,
    ) -> Result<CacheRead, FjallCellCacheError> {
        let (expiry, read) = self.read_decoded(collection, cell).await?;
        Ok(classify(expiry, read, self.clock.now_ms()))
    }

    /// Batch twin of [`get`](Self::get): probes every coordinate of one
    /// `(collection, section)` batch in a SINGLE [`spawn_blocking`] hop — never
    /// one per cell, so a warm chunk costs one blocking-pool round-trip, not
    /// one per position.
    ///
    /// `Ok(Some(values))` iff EVERY position is an unexpired hit (a `Present`
    /// value or an authoritative `Absent` tag), index-aligned to `batch`.
    /// `Ok(None)` if any position is a miss (no entry) or floor-expired — the
    /// caller then refetches the whole batch from durable truth. `Err` on a
    /// join failure, a per-key engine error, or a decode failure — the caller
    /// degrades that read to a durable one, exactly as the point path does on a
    /// fjall read error.
    ///
    /// The closure reads all keys before returning (no short-circuit at the
    /// first miss) so the whole batch costs one hop; classification then
    /// samples the clock ONCE and may short-circuit `Ok(None)` at the first
    /// non-hit.
    pub(crate) async fn get_batch(
        &self,
        collection: &CollectionId,
        section: Section,
        batch: &CoordinateBatch,
    ) -> Result<Option<CommittedBatch>, FjallCellCacheError> {
        // Encode every key up front (bounded, sized once): small requests stay
        // inline and the owned keys move into the blocking closure.
        let keys: CellBuffer<SmallVec<[u8; 32]>> = batch
            .iter()
            .map(|coordinate| {
                codec::cell_key(
                    collection,
                    &CellKey {
                        section,
                        coordinate: coordinate.clone(),
                    },
                )
            })
            .collect();
        let handle = self.inner.handle().clone();
        #[cfg(test)]
        let (fail_reads, probes) = (self.fail_reads.clone(), self.blocking_probes.clone());
        // ONE blocking hop reads every key exhaustively; a per-key engine error
        // (or the injected fault) fails the whole hop, mirroring how `read_cell`
        // surfaces one via `??`.
        let raws = spawn_blocking(
            move || -> Result<CellBuffer<Option<Slice>>, FjallCellCacheError> {
                #[cfg(test)]
                probes.fetch_add(1, Ordering::Relaxed);
                #[cfg(test)]
                if fail_reads.load(Ordering::Relaxed) {
                    return Err(FjallCellCacheError::Injected);
                }
                let mut out = SmallVec::with_capacity(keys.len());
                for key in &keys {
                    out.push(handle.get(key.as_slice())?);
                }
                Ok(out)
            },
        )
        .await??;
        // One clock sample classifies every position; decode failures propagate.
        let now = self.clock.now_ms();
        let mut hits: CommittedBatch = SmallVec::new();
        for raw in raws {
            let (expiry, read) = codec::decode_cell(raw.as_deref())?;
            match classify(expiry, read, now) {
                CacheRead::Hit(committed) => hits.push(committed),
                CacheRead::Miss | CacheRead::Expired => return Ok(None),
            }
        }
        Ok(Some(hits))
    }

    /// The absolute expiry (millis; `0` = never) stamped on the cell's current
    /// fjall entry, or `None` when no entry exists. Unlike `get`,
    /// it does **not** treat a passed stamp as a miss: the caller is about to
    /// **re-publish** the cell and wants to *preserve* its existing co-expiry
    /// anchor. The promote (`commit_provisional`) uses this so the committed
    /// value inherits the death set at stage time (`mark_resolved` does not
    /// re-stamp the durable TTL), rather than overhanging it with a fresh
    /// stamp.
    ///
    /// Test-only: the sole caller is the `#[cfg(test)]`
    /// `Cached::stored_expiry` co-expiry probe.
    #[cfg(test)]
    pub(crate) async fn stored_expiry(
        &self,
        collection: &CollectionId,
        cell: &CellKey,
    ) -> Result<Option<u64>, FjallCellCacheError> {
        let (expiry, read) = self.read_decoded(collection, cell).await?;
        Ok(match read {
            Read::Unknown => None,
            _ => Some(expiry),
        })
    }

    /// Reads and decodes one cell's raw fjall frame: the shared prologue
    /// behind [`get`](Self::get) and `stored_expiry`,
    /// which differ only in how they treat an expired stamp.
    async fn read_decoded(
        &self,
        collection: &CollectionId,
        cell: &CellKey,
    ) -> Result<(u64, Read<Bytes>), FjallCellCacheError> {
        let raw = read_cell(self.inner.handle(), codec::cell_key(collection, cell)).await?;
        codec::decode_cell(raw.as_deref())
    }

    /// Write-through: publishes one cell's committed projection with an
    /// absolute `expiry` (`0` = never). A present value writes the payload
    /// cell; a known-absent value writes the `Absent` tag. The expiry
    /// mirrors the durable row's TTL death so the entry co-expires
    /// (FLOOR-rounded, so it never outlives the durable value).
    pub(crate) async fn put(
        &self,
        collection: &CollectionId,
        cell: &CellKey,
        value: &Committed,
        expiry: u64,
    ) -> Result<(), FjallCellCacheError> {
        #[cfg(test)]
        if self.fail_puts.load(Ordering::Relaxed) {
            return Err(FjallCellCacheError::Injected);
        }
        let frame = encode_frame(value.get(), expiry);
        write_cell(
            self.inner.handle(),
            codec::cell_key(collection, cell),
            frame,
        )
        .await
    }

    /// Write-through publish of a *batch* of committed cell projections in a
    /// **single** [`spawn_blocking`] over one atomic [`OwnedWriteBatch`]
    /// (the shared `run_batch` ceremony).
    ///
    /// Each `(cell, projection, expiry)` is encoded to a frame and inserted;
    /// `commit` lands the whole set as one fjall mutation, so a multi-cell
    /// cache update is never torn (mirroring the same-partition `UNLOGGED
    /// BATCH` the Cassandra side uses). This collapses the per-cell settle
    /// writes from N blocking thread-hops to one. The single-cell write-through
    /// paths keep [`put`](Self::put).
    ///
    /// On a commit failure the caller's posture depends on the write's
    /// provenance: a settle write-through (via
    /// [`publish_written`](crate::state::cached::Cached)) deletes the written
    /// cells' entries (D1 — the durable value moved, so a stale entry would
    /// serve the pre-write value verbatim), while a read-fill (via
    /// [`get_many`](crate::state::cached::Cached)) degrades with NO delete — a
    /// miss/expired/error prior state left nothing stale, and a live entry
    /// hidden by a read error equals what the next read resolves.
    pub(crate) async fn put_batch(
        &self,
        collection: &CollectionId,
        cells: impl IntoIterator<Item = (CellKey, Committed, u64)>,
    ) -> Result<(), FjallCellCacheError> {
        #[cfg(test)]
        if self.fail_puts.load(Ordering::Relaxed) {
            return Err(FjallCellCacheError::Injected);
        }
        // Encode every key + frame up front (bounded, sized once from the
        // caller's iterator) so the blocking closure only touches fjall; the
        // owned key/frame pairs move into it. Building `framed` directly from the
        // projected iterator avoids an intermediate collect on the settle path.
        let framed: CellBuffer<(SmallVec<[u8; 32]>, Bytes)> = cells
            .into_iter()
            .map(|(cell, value, expiry)| {
                (
                    codec::cell_key(collection, &cell),
                    encode_frame(value.get(), expiry),
                )
            })
            .collect();
        let handle = self.inner.handle().clone();
        let capacity = framed.len();
        self.run_batch(handle, capacity, move |batch, handle| {
            for (key, frame) in &framed {
                batch.insert(handle, key.as_slice(), frame.as_ref());
            }
            Ok(())
        })
        .await
    }

    /// The **settle transform** (D5): rewrites each staged cell's entry
    /// `prev → data` at its **stage-anchored** expiry, atomically, in a single
    /// [`spawn_blocking`] over one [`OwnedWriteBatch`]. Called by
    /// [`Cached::commit_provisional`](crate::state::cached::Cached) strictly
    /// **before** the lower promote — the commit verdict is already fixed when
    /// that verb runs, so `data` *is* the logical committed projection and
    /// installing it pre-call keeps the staged cells warm and correct even if
    /// the promote then fails or the settle future is dropped. Reusing the
    /// stage expiry is load-bearing: the lower promote keeps `data`'s death
    /// set at stage time, so a fresh `now + ttl` would overhang the durable
    /// row's death.
    ///
    /// **Idempotent because the frame is not marked.** fjall frames carry no
    /// stage/committed discriminator — `stage_expiry` decodes any valid cell
    /// frame — so a sweep-retried transform re-reads the stage-anchored expiry
    /// it wrote the first time and rewrites byte-equivalent bytes. The delete
    /// arm (a missing or unreadable entry is removed in the same atomic batch)
    /// is reached only by genuinely missing/corrupt entries, never by a retry.
    /// The supporting routing lemma: between transform attempts no successful
    /// fill can restamp a coordinate still eligible for the next transform — a
    /// fall-through read of a still-listed provisional coordinate must
    /// *resolve* it first, and the sweep rebuilds its write set from durable
    /// provisional state, so a resolved coordinate drops out before the retry
    /// ever sees it. A drop between this transform and the commit site's
    /// scoped section delete is equivalent to a drop before the verb: the
    /// armed sweep re-runs both, and both are idempotent.
    ///
    /// Any failure — including the `fail_puts` seam and a join error — returns
    /// `Err` so the caller runs its must-succeed delete fallback over the same
    /// entries.
    pub(crate) async fn commit_batch(
        &self,
        collection: &CollectionId,
        writes: &[(CellKey, ProvisionalWrite)],
    ) -> Result<(), FjallCellCacheError> {
        #[cfg(test)]
        if self.fail_puts.load(Ordering::Relaxed) {
            return Err(FjallCellCacheError::Injected);
        }
        // Owned closure inputs, bounded and sized once: the cell key plus the
        // committed `data` to rewrite at its read-back stage expiry.
        let mut inputs: CellBuffer<(SmallVec<[u8; 32]>, Option<Bytes>)> =
            SmallVec::with_capacity(writes.len());
        for (cell, write) in writes {
            inputs.push((codec::cell_key(collection, cell), write.data().cloned()));
        }
        let database = self.inner.database().clone();
        let handle = self.inner.handle().clone();
        spawn_blocking(move || {
            let mut batch = OwnedWriteBatch::with_capacity(database, inputs.len());
            for (key, data) in &inputs {
                match stage_expiry(&handle, key) {
                    Some(expiry) => {
                        let frame = encode_frame(data.as_ref(), expiry);
                        batch.insert(&handle, key.as_slice(), frame.as_ref());
                    }
                    // Missing/unreadable stage entry: delete it in the same
                    // atomic batch (cold, safe — the next read falls through).
                    None => batch.remove(&handle, key.as_slice()),
                }
            }
            batch.commit()
        })
        .await??;
        Ok(())
    }

    /// Deletes a batch of committed cell entries in one atomic
    /// [`OwnedWriteBatch`] — the D-site repair primitive (keys built at exact
    /// size). Idempotent: removing an absent key is a no-op.
    pub(crate) async fn delete_batch(
        &self,
        collection: &CollectionId,
        cells: &[CellKey],
    ) -> Result<(), FjallCellCacheError> {
        #[cfg(test)]
        self.injected_delete_failure()?;
        let mut keys: CellBuffer<SmallVec<[u8; 32]>> = SmallVec::with_capacity(cells.len());
        for cell in cells {
            keys.push(codec::cell_key(collection, cell));
        }
        let handle = self.inner.handle().clone();
        let capacity = keys.len();
        self.run_batch(handle, capacity, move |batch, handle| {
            for key in &keys {
                batch.remove(handle, key.as_slice());
            }
            Ok(())
        })
        .await
    }

    /// Deletes one `(collection, section)`'s committed cell entries, walking
    /// the section's key range in fixed hops of [`SCAN_HOP_ROWS`]: each hop is
    /// one [`spawn_blocking`] that collects at most a hop of keys, deletes
    /// them in one bounded write batch, and re-seeks past the last key it
    /// examined. Never one whole-section batch — that would hold O(cached
    /// cells) keys in RAM (the bounded-RAM invariant). Idempotent (a deleted
    /// key is not found again), so a must-succeed retry re-walks safely.
    ///
    /// `exclude` names cells whose entries survive the delete — the commit
    /// site's staged coordinates (the set equation on
    /// [`Cached::commit_provisional`](crate::state::cached::Cached)); every
    /// other caller passes `&[]` for a whole-section delete. The exclusion set
    /// is encoded **once, in the committed-cell key form** (`codec::cell_key`
    /// — the same form the walk yields; the provisional-*index* encoding
    /// carries an extra kind byte and would never match, silently deleting the
    /// staged survivors) and held hashed, so each walked key costs O(1)
    /// expected and memory stays O(|exclude| + one hop).
    pub(crate) async fn delete_section(
        &self,
        collection: &CollectionId,
        section: Section,
        exclude: &[CellKey],
    ) -> Result<(), FjallCellCacheError> {
        #[cfg(test)]
        self.injected_delete_failure()?;
        let excluded: Arc<HashSet<SmallVec<[u8; 32]>, RandomState>> = Arc::new(
            exclude
                .iter()
                .map(|cell| codec::cell_key(collection, cell))
                .collect(),
        );
        let prefix = codec::section_prefix(collection, section);
        let handle = self.inner.handle().clone();
        let database = self.inner.database().clone();
        let mut lo: Bound<Vec<u8>> = Bound::Included(prefix.to_vec());
        loop {
            let hop_handle = handle.clone();
            let hop_database = database.clone();
            let hop_excluded = excluded.clone();
            let hop_lo = lo;
            let resume = spawn_blocking(move || -> fjall::Result<Option<Vec<u8>>> {
                // A `Vec`: bounded by `SCAN_HOP_ROWS` and always spilling past
                // any small inline on this recovery/must-succeed-delete path.
                let mut doomed: Vec<Vec<u8>> = Vec::new();
                let mut resume: Option<Vec<u8>> = None;
                let mut examined = 0usize;
                for guard in hop_handle.range((hop_lo, Bound::Unbounded)) {
                    let (key, _) = guard.into_inner()?;
                    // The range's upper side is open; the prefix check is what
                    // stops the walk at the section boundary.
                    if !key.starts_with(&prefix) {
                        break;
                    }
                    examined += 1;
                    if !hop_excluded.contains(key.as_ref()) {
                        doomed.push(key.to_vec());
                    }
                    if examined >= SCAN_HOP_ROWS {
                        resume = Some(key.to_vec());
                        break;
                    }
                }
                let mut batch = OwnedWriteBatch::with_capacity(hop_database, doomed.len());
                for key in &doomed {
                    batch.remove(&hop_handle, key.as_slice());
                }
                batch.commit()?;
                Ok(resume)
            })
            .await??;
            match resume {
                // The hop stopped on its budget; re-seek just past the last
                // examined key.
                Some(key) => lo = Bound::Excluded(key),
                None => return Ok(()),
            }
        }
    }

    // --- Warm index: provisional coordinates + cold-seed latch ---------------
    //
    // The warm provisional-coordinate cache the recovery sweep short-circuits
    // on. It lives
    // in the per-partition `index` keyspace, cold at a fresh assignment and
    // dropped at revocation. It is the disk-spilling relocation of the former
    // in-RAM `ProvisionalIndex`; the durable Cassandra event marker remains
    // the authoritative cold-recovery source (a fresh assignment re-seeds
    // from it).

    /// Whether `collection`'s one-time cold seed has run (the seeded latch).
    pub(crate) async fn index_seeded(
        &self,
        collection: &CollectionId,
    ) -> Result<bool, FjallCellCacheError> {
        let raw = read_cell(
            self.inner.index_handle(),
            codec::index_seeded_key(collection),
        )
        .await?;
        Ok(raw.is_some())
    }

    /// Marks `collection` seeded once its bounded cold seed read completes.
    pub(crate) async fn index_mark_seeded(
        &self,
        collection: &CollectionId,
    ) -> Result<(), FjallCellCacheError> {
        write_index_empty(
            self.inner.index_handle(),
            codec::index_seeded_key(collection),
        )
        .await
    }

    /// Drops `collection`'s seeded latch, forcing the next sweep to re-seed
    /// from the durable index (used when a stage write fails and a
    /// coordinate may have landed durably that the coords set now misses).
    /// A delete-side op: must-succeed at its call sites (a latch left true
    /// over an incomplete snapshot would short-circuit every later sweep), so
    /// it honors the shared delete fault seam.
    pub(crate) async fn index_unseed(
        &self,
        collection: &CollectionId,
    ) -> Result<(), FjallCellCacheError> {
        #[cfg(test)]
        self.injected_delete_failure()?;
        let handle = self.inner.index_handle().clone();
        let key = codec::index_seeded_key(collection);
        spawn_blocking(move || handle.remove(key.as_slice())).await??;
        Ok(())
    }

    /// Records `cell` as a live provisional coordinate of `collection`.
    pub(crate) async fn index_record(
        &self,
        collection: &CollectionId,
        cell: &CellKey,
    ) -> Result<(), FjallCellCacheError> {
        #[cfg(test)]
        if self.fail_index_record.load(Ordering::Relaxed) {
            return Err(FjallCellCacheError::Injected);
        }
        write_index_empty(
            self.inner.index_handle(),
            codec::index_coord_key(collection, cell),
        )
        .await
    }

    /// Records a batch of live provisional coordinates of `collection` in one
    /// atomic [`OwnedWriteBatch`] — the settle-time counterpart of the
    /// streaming single-key [`index_record`](Self::index_record), collapsing N
    /// blocking hops to one. All-or-nothing: on a failure the caller cannot
    /// know which coordinates landed durably, so it drops the seeded latch
    /// ([`index_unseed`](Self::index_unseed)) and the next sweep re-seeds.
    pub(crate) async fn index_record_batch<'a, I>(
        &self,
        collection: &CollectionId,
        cells: I,
    ) -> Result<(), FjallCellCacheError>
    where
        I: ExactSizeIterator<Item = &'a CellKey>,
    {
        #[cfg(test)]
        if self.fail_index_record.load(Ordering::Relaxed) {
            return Err(FjallCellCacheError::Injected);
        }
        let keys = index_keys(collection, cells);
        let handle = self.inner.index_handle().clone();
        let capacity = keys.len();
        self.run_batch(handle, capacity, move |batch, handle| {
            for key in &keys {
                batch.insert(handle, key.as_slice(), [].as_slice());
            }
            Ok(())
        })
        .await
    }

    /// Clears a batch of resolved provisional coordinates from `collection` in
    /// one atomic [`OwnedWriteBatch`]. A failure is a harmless over-report:
    /// the sweep's point-read filter drops already-resolved coordinates.
    pub(crate) async fn index_clear_batch<'a, I>(
        &self,
        collection: &CollectionId,
        cells: I,
    ) -> Result<(), FjallCellCacheError>
    where
        I: ExactSizeIterator<Item = &'a CellKey>,
    {
        let keys = index_keys(collection, cells);
        let handle = self.inner.index_handle().clone();
        let capacity = keys.len();
        self.run_batch(handle, capacity, move |batch, handle| {
            for key in &keys {
                batch.remove(handle, key.as_slice());
            }
            Ok(())
        })
        .await
    }

    /// Snapshots `collection`'s live provisional coordinates — the recovery
    /// drain buffer, sized to `#provisional`. Empty ⟹ the warm sweep issues no
    /// Cassandra reads. Collected in one [`spawn_blocking`] over the bounded
    /// `Coord` prefix range (the guard cannot cross an `.await`).
    pub(crate) async fn index_snapshot(
        &self,
        collection: &CollectionId,
    ) -> Result<Vec<CellKey>, FjallCellCacheError> {
        #[cfg(test)]
        if self.fail_index_snapshot.load(Ordering::Relaxed) {
            return Err(FjallCellCacheError::Injected);
        }
        let handle = self.inner.index_handle().clone();
        let prefix = codec::index_coord_prefix(collection);
        spawn_blocking(move || {
            // A `Vec`: unbounded (∝ #provisional) → `Vec` is correct.
            let mut out: Vec<CellKey> = Vec::new();
            for guard in handle.prefix(prefix) {
                let (key, _) = guard.into_inner()?;
                out.push(codec::coord_cell_key(&key));
            }
            Ok(out)
        })
        .await?
    }

    /// Runs `fill` over a fresh [`OwnedWriteBatch`] against `handle` and
    /// commits it, all in a single blocking hop — the shared ceremony behind
    /// every all-or-nothing batch mutator except
    /// [`commit_batch`](Self::commit_batch) (which reads stage expiries inside
    /// its own closure) and the hopping
    /// [`delete_section`](Self::delete_section).
    fn run_batch(
        &self,
        handle: Keyspace,
        capacity: usize,
        fill: impl FnOnce(&mut OwnedWriteBatch, &Keyspace) -> fjall::Result<()> + Send + 'static,
    ) -> impl Future<Output = Result<(), FjallCellCacheError>> + Send {
        let database = self.inner.database().clone();
        let task = spawn_blocking(move || {
            let mut batch = OwnedWriteBatch::with_capacity(database, capacity);
            fill(&mut batch, &handle)?;
            batch.commit()
        });
        async move {
            task.await??;
            Ok(())
        }
    }
}

/// A `Clone` handle over the per-partition warm-index keyspace recording
/// **marker presence**: whether a collection's durable event marker has been
/// consulted this assignment. The bounded, disk-backed replacement for the
/// bottom store's former in-RAM checked set — the rows live in the
/// per-assignment `index` keyspace, so the workspace `Drop` and the startup
/// orphan sweep reclaim them at revocation.
///
/// **Infallible by design.** fjall is never a durability or recovery source,
/// so a fjall error degrades to a re-check, never an under-report:
/// [`contains`](Self::contains) reads as **unchecked** (`false`) on any error
/// and [`set`](Self::set) warns and continues. Each failure costs one redundant
/// durable marker point read — under a *persistent* fjall failure that is a
/// continuous per-consult fallback (one durable read per consult until fjall
/// heals), never a wrong answer. Neither can lose a durable marker: the
/// standing RAM map is untouched by presence failures (the memo and ownership
/// invariants live on the bottom store's `MarkerMemo`).
///
/// A blown cache fuse fuses this latch too — `contains` answers unchecked and
/// `set` no-ops — purely for uniformity (blown means zero fjall dependence);
/// the contract above already makes the degradation safe.
#[derive(Clone, Educe)]
#[educe(Debug)]
pub(crate) struct MarkerPresence {
    #[educe(Debug(ignore))]
    index: Keyspace,
    #[educe(Debug(ignore))]
    fuse: Arc<AtomicBool>,
}

impl MarkerPresence {
    /// Whether `collection`'s durable marker has been consulted this
    /// assignment. Any fjall error reads as **unchecked** (`false`) — a
    /// redundant durable re-check, never an under-report.
    pub(crate) async fn contains(&self, collection: &CollectionId) -> bool {
        if self.fuse.load(Ordering::Relaxed) {
            return false;
        }
        match read_cell(&self.index, codec::index_presence_key(collection)).await {
            Ok(raw) => raw.is_some(),
            Err(error) => {
                warn!(%error, "marker-presence read failed; treating collection as unchecked");
                false
            }
        }
    }

    /// Records `collection`'s durable marker as consulted. A fjall error is
    /// swallowed (warn): the collection stays unchecked and the next consult
    /// pays one redundant durable read.
    pub(crate) async fn set(&self, collection: &CollectionId) {
        if self.fuse.load(Ordering::Relaxed) {
            return;
        }
        if let Err(error) =
            write_index_empty(&self.index, codec::index_presence_key(collection)).await
        {
            warn!(%error, "marker-presence write failed; leaving collection unchecked");
        }
    }
}

/// Encodes a cell's presence/absence into its stored frame at `expiry`: a
/// present `payload` writes the payload cell, `None` writes the `Absent` tag.
fn encode_frame(payload: Option<&Bytes>, expiry: u64) -> Bytes {
    match payload {
        Some(payload) => codec::encode_present_cell(payload, expiry),
        None => codec::encode_absent_cell(expiry),
    }
}

/// The encoded warm-index keys of a batch of coordinates — built up front on a
/// [`CellBuffer`] inline buffer (bounded, sized once) so the common small write
/// set stays on the stack and the blocking batch closure only touches fjall.
fn index_keys<'a>(
    collection: &CollectionId,
    cells: impl ExactSizeIterator<Item = &'a CellKey>,
) -> CellBuffer<SmallVec<[u8; 32]>> {
    let mut keys: CellBuffer<SmallVec<[u8; 32]>> = SmallVec::with_capacity(cells.len());
    for cell in cells {
        keys.push(codec::index_coord_key(collection, cell));
    }
    keys
}

/// Reads the raw cell at `key`, or `None` when the key is absent — one
/// blocking hop. Generic over the key so a variable-length `SmallVec` cell key
/// and a fixed-size `[u8; N]` index key both read without a bridging copy.
async fn read_cell(
    cache: &Keyspace,
    key: impl AsRef<[u8]> + Send + 'static,
) -> Result<Option<Slice>, FjallCellCacheError> {
    let cache = cache.clone();
    Ok(spawn_blocking(move || cache.get(key)).await??)
}

/// Writes `cell` at `key`, overwriting any existing cell — one blocking hop.
/// Generic over the key so a variable-length `SmallVec` cell key and a
/// fixed-size `[u8; N]` index key both write without a bridging copy.
async fn write_cell(
    cache: &Keyspace,
    key: impl AsRef<[u8]> + Send + 'static,
    cell: Bytes,
) -> Result<(), FjallCellCacheError> {
    let cache = cache.clone();
    spawn_blocking(move || cache.insert(key.as_ref(), cell.as_ref())).await??;
    Ok(())
}

/// Inserts an empty-valued warm-index key (presence-as-boolean), one blocking
/// hop. Generic over the key so a fixed-size `[u8; N]` index key inserts
/// without a bridging copy.
async fn write_index_empty(
    handle: &Keyspace,
    key: impl AsRef<[u8]> + Send + 'static,
) -> Result<(), FjallCellCacheError> {
    let handle = handle.clone();
    spawn_blocking(move || handle.insert(key.as_ref(), [].as_slice())).await??;
    Ok(())
}

/// Whether an absolute `expiry` (millis; `0` = never) has passed at `now`.
fn expired(expiry: u64, now: u64) -> bool {
    expiry != codec::NEVER_EXPIRES && now >= expiry
}

/// Classifies a decoded cell frame `(expiry, read)` sampled at `now` into a
/// [`CacheRead`]: a [`Miss`](CacheRead::Miss) when no entry exists, an
/// [`Expired`](CacheRead::Expired) when the stamped expiry has passed, else a
/// [`Hit`](CacheRead::Hit) on the present value or authoritative absent tag.
///
/// The single classifier shared by the point [`get`](FjallCellCache::get) and
/// the batch [`get_batch`](FjallCellCache::get_batch) probe, so the stale-serve
/// rules (a `Read::Unknown` is a miss; a passed expiry is never a hit) cannot
/// drift between the two paths.
fn classify(expiry: u64, read: Read<Bytes>, now: u64) -> CacheRead {
    match read {
        Read::Unknown => CacheRead::Miss,
        _ if expired(expiry, now) => CacheRead::Expired,
        Read::Present(payload) => CacheRead::Hit(Committed::new(Some(payload))),
        Read::Absent => CacheRead::Hit(Committed::new(None)),
    }
}

/// Reads the absolute stage expiry stamped on the cell at `key` back from the
/// cache keyspace, or `None` when no entry exists or the read/decode fails —
/// the transform then deletes the entry in the same atomic batch so the next
/// read falls through and self-heals. Runs inside
/// [`commit_batch`](FjallCellCache::commit_batch)'s blocking closure, so it
/// uses the synchronous keyspace `get` directly.
fn stage_expiry(handle: &Keyspace, key: &[u8]) -> Option<u64> {
    let raw = match handle.get(key) {
        Ok(raw) => raw,
        Err(error) => {
            warn!(%error, "committed-value cache commit expiry read failed; degrading");
            return None;
        }
    };
    match codec::decode_cell(raw.as_deref()) {
        Ok((_, Read::Unknown)) => None,
        Ok((expiry, _)) => Some(expiry),
        Err(error) => {
            warn!(%error, "committed-value cache commit expiry decode failed; degrading");
            None
        }
    }
}
