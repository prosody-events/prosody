//! Fjall-backed cell cache.
//!
//! [`FjallCellCache`] stores one tagged cell per [`CellKey`] in a fjall
//! keyspace. It is the committed-value cache the
//! [`Cached`](crate::state::cached::Cached) coverage combinator serves from:
//! point `get`s and, over a covered scan sub-range,
//! ordered `scan_present` range reads. It does
//! **not** implement [`CellStore`](crate::state::store::CellStore): it is a
//! concrete *partial* upper (it can only answer what it has mirrored), so a
//! bare cache view can never be mistaken for a complete store — completeness is
//! the coverage map's job, owned by `Cached`.
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
pub use workspace::{AssignmentEpoch, FjallClient, FjallClientError, FjallWorkspace};

use self::codec::Read;
use crate::state::CollectionId;
use crate::state::cell::{Committed, ProvisionalWrite};
use crate::state::cell_key::{CellKey, Coordinate, Direction, Scan, Section};
use async_stream::try_stream;
use bytes::Bytes;
use educe::Educe;
use fjall::{Database, Guard, Keyspace, OwnedWriteBatch, Slice};
use futures::{Stream, StreamExt};
use std::ops::Bound;
use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::task::coop::cooperative;
use tokio::task::spawn_blocking;
use tracing::warn;

/// Rows examined per blocking hop of a chunked
/// [`scan_present`](FjallCellCache::scan_present): each hop collects at most
/// this many rows in one [`spawn_blocking`], then re-seeks from the last key it
/// saw. A covered drain therefore holds O(hop) hits in RAM — never the whole
/// covered interval — while the synchronous fjall range guard still never
/// crosses an `.await`.
const SCAN_HOP_ROWS: usize = 256;

/// The cache's `now` source for TTL co-expiry, in milliseconds since the Unix
/// epoch.
///
/// A non-`dyn` seam: production reads the [`Wall`](Self::Wall) clock; a test
/// can pin time with [`Fixed`](Self::Fixed) and advance the shared counter past
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
    /// No entry exists (never written, or a gap within a covered scan range).
    Miss,
}

/// One item of a `FjallCellCache::scan_present` covered serve: a live present
/// cell, or an expired covered coordinate the caller must re-fetch from the
/// lower store (FLOOR rounding can expire a fjall entry just before its durable
/// row, so an expired covered coordinate is a gap, not an absence).
#[derive(Clone, Debug)]
pub enum ScanHit {
    /// An unexpired present cell, served straight from the cache.
    Present(CellKey, Bytes),
    /// A covered coordinate whose fjall entry expired; fall through to the
    /// lower store for it.
    Expired(CellKey),
}

/// The per-cell outcome [`FjallCellCache::commit_batch`] reports: whether the
/// re-published committed value now **covers** the coordinate, or the
/// coordinate must be **uncovered** (its stage entry was missing/unreadable,
/// or the atomic batch commit failed) so the next read falls through and
/// self-heals.
#[derive(Clone, Copy, Debug)]
pub(crate) enum CoverDecision {
    /// The committed value was re-published; cover the coordinate.
    Cover,
    /// Uncover the coordinate; the next read re-publishes from the lower store.
    Punch,
}

/// Fjall-backed cell cache.
#[derive(Clone, Educe)]
#[educe(Debug)]
pub struct FjallCellCache {
    #[educe(Debug(ignore))]
    inner: Arc<Inner>,
    clock: Clock,
    /// Test-only fault seam: when set, every [`put`](Self::put) returns an
    /// engine error without touching fjall, so a test can force the publish
    /// failure that uncovers a coordinate (the only mutator `punch`).
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
    /// Test-only fault seam: a countdown of [`cover_store`](Self::cover_store)
    /// calls to fail (each failure decrements it), so a test can make exactly
    /// the next N coverage rewrites fail — forcing the must-succeed punch's
    /// retry path — and then heal automatically.
    #[cfg(test)]
    #[educe(Debug(ignore))]
    fail_cover_stores: Arc<AtomicU64>,
}

/// Backing for a [`FjallCellCache`]: either a bare cache handle plus its
/// owning database (tests) or an owned per-partition workspace whose cache
/// handle the cache operates and whose `Drop` deletes the keyspace at
/// revocation (production).
///
/// The [`Database`] is held in both arms because batch writes are issued
/// through [`Database::batch`], not the keyspace handle. The `index` keyspace
/// (warm provisional coordinates + scan coverage) rides alongside `cache` in
/// both arms purely for lifecycle co-location — it shares the workspace's epoch
/// (cold at a fresh assignment, dropped at revocation). Index and cell-cache
/// writes are **not** issued as one cross-keyspace batch; the warm index is a
/// rebuildable hint (a fresh epoch re-seeds from the durable `kind=Index`), so
/// they need no atomicity with the committed-value write.
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

    /// The warm-index keyspace handle (provisional coordinates + coverage).
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
            #[cfg(test)]
            fail_puts: Arc::new(AtomicBool::new(false)),
            #[cfg(test)]
            fail_index_snapshot: Arc::new(AtomicBool::new(false)),
            #[cfg(test)]
            fail_index_record: Arc::new(AtomicBool::new(false)),
            #[cfg(test)]
            fail_cover_stores: Arc::new(AtomicU64::new(0)),
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

    /// Test handle on the [`cover_store`](Self::cover_store) fault seam: the
    /// shared countdown of coverage rewrites to fail before healing.
    #[cfg(test)]
    #[must_use]
    pub fn fail_cover_stores(&self) -> Arc<AtomicU64> {
        self.fail_cover_stores.clone()
    }

    /// The cache's `now` source, shared by reads (expiry checks) and the
    /// [`Cached`](crate::state::cached::Cached) cache's expiry stamping so the
    /// two never disagree.
    #[must_use]
    pub(crate) fn clock(&self) -> &Clock {
        &self.clock
    }

    /// Looks up one cell's committed value as a three-state [`CacheRead`]: a
    /// [`Hit`](CacheRead::Hit) on an unexpired `Present`/`Absent` entry, an
    /// [`Expired`](CacheRead::Expired) when the entry exists but its stamped
    /// expiry has passed, or a [`Miss`](CacheRead::Miss) when no entry exists.
    ///
    /// The caller distinguishes these because a covered coordinate may be a gap
    /// with no entry (genuine absence) — a `Miss` — or an entry that floor-
    /// expired (a gap to re-fetch) — an `Expired`. Both differ from a present
    /// `Absent` tag (`Hit(Committed(None))`).
    pub(crate) async fn get(
        &self,
        collection: &CollectionId,
        cell: &CellKey,
    ) -> Result<CacheRead, FjallCellCacheError> {
        let (expiry, read) = self.read_decoded(collection, cell).await?;
        Ok(match read {
            Read::Unknown => CacheRead::Miss,
            _ if expired(expiry, self.clock.now_ms()) => CacheRead::Expired,
            Read::Present(payload) => CacheRead::Hit(Committed::new(Some(payload))),
            Read::Absent => CacheRead::Hit(Committed::new(None)),
        })
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
    /// behind [`get`](Self::get) and [`stored_expiry`](Self::stored_expiry),
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
    /// paths keep [`put`](Self::put). On a batch commit failure, the caller
    /// uncovers every coordinate (Cov3 — see
    /// [`Cached`](crate::state::cached::Cached)'s module doc).
    pub(crate) async fn put_batch(
        &self,
        collection: &CollectionId,
        cells: &[(CellKey, Committed, u64)],
    ) -> Result<(), FjallCellCacheError> {
        #[cfg(test)]
        if self.fail_puts.load(Ordering::Relaxed) {
            return Err(FjallCellCacheError::Injected);
        }
        // Encode every frame up front (bounded, sized once) so the blocking
        // closure only touches fjall; the owned key/frame pairs move into it.
        let mut framed: Vec<(Vec<u8>, Bytes)> = Vec::with_capacity(cells.len());
        for (cell, value, expiry) in cells {
            let frame = encode_frame(value.get(), *expiry);
            framed.push((codec::cell_key(collection, cell), frame));
        }
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

    /// Promotes a batch of staged cells into committed cache entries in a
    /// **single** [`spawn_blocking`] over one atomic [`OwnedWriteBatch`]: for
    /// each write it reads the cell's stage-anchored expiry back from fjall and
    /// re-publishes `data` at that **same** expiry, then commits the whole set
    /// at once. Reusing the stage expiry is load-bearing — the lower promote
    /// keeps `data`'s death set at stage time, so a fresh `now + ttl` would
    /// overhang it (Cov1 — see [`Cached`](crate::state::cached::Cached)'s
    /// module doc).
    ///
    /// Returns one [`CoverDecision`] per write, aligned to `writes` order:
    /// [`Cover`](CoverDecision::Cover) when the cell was re-published,
    /// [`Punch`](CoverDecision::Punch) when its stage entry was missing or
    /// unreadable, or — for **every** write — when the batch commit failed.
    /// Best-effort: it never returns an error, so the caller can return the
    /// lower promote's `Result` verbatim (the Incomplete trap — likewise
    /// defined there).
    pub(crate) async fn commit_batch(
        &self,
        collection: &CollectionId,
        writes: &[(CellKey, ProvisionalWrite)],
    ) -> Vec<CoverDecision> {
        #[cfg(test)]
        if self.fail_puts.load(Ordering::Relaxed) {
            return vec![CoverDecision::Punch; writes.len()];
        }
        // Owned closure inputs, bounded and sized once: the cell key plus the
        // committed `data` to re-publish at its read-back stage expiry.
        let mut inputs: Vec<(Vec<u8>, Option<Bytes>)> = Vec::with_capacity(writes.len());
        for (cell, write) in writes {
            inputs.push((codec::cell_key(collection, cell), write.data().cloned()));
        }
        let database = self.inner.database().clone();
        let handle = self.inner.handle().clone();
        spawn_blocking(move || {
            let mut batch = OwnedWriteBatch::with_capacity(database, inputs.len());
            let mut decisions = Vec::with_capacity(inputs.len());
            for (key, data) in &inputs {
                match stage_expiry(&handle, key) {
                    Some(expiry) => {
                        let frame = encode_frame(data.as_ref(), expiry);
                        batch.insert(&handle, key.as_slice(), frame.as_ref());
                        decisions.push(CoverDecision::Cover);
                    }
                    None => decisions.push(CoverDecision::Punch),
                }
            }
            // A commit failure means none of the inserts landed, so uncover
            // every coordinate (not just the ones marked Cover).
            if batch.commit().is_ok() {
                decisions
            } else {
                vec![CoverDecision::Punch; inputs.len()]
            }
        })
        .await
        .unwrap_or_else(|error| {
            warn!(%error, "committed-value cache commit batch task failed; uncovering");
            vec![CoverDecision::Punch; writes.len()]
        })
    }

    /// Streams the committed cells of one `(collection, section)` over the
    /// scan's coordinate range, in [`Scan::dir`] order — the range-read the
    /// coverage cache serves a covered scan sub-range from.
    ///
    /// `Absent` (cleared) entries are skipped, so the stream yields only
    /// present cells, exactly as the lower store's `scan_cells` does for a
    /// covered range. An **expired** present cell is yielded as
    /// [`ScanHit::Expired`] so the covered serve can fall through and
    /// re-fetch it (FLOOR rounding makes an entry expire slightly before
    /// its durable row, so an expired covered coordinate must not read as
    /// absent — it is treated as a gap).
    ///
    /// The drain is **chunked**: each [`spawn_blocking`] hop examines at most
    /// `SCAN_HOP_ROWS` rows and re-seeks from the last key it saw, so a
    /// covered interval of any size costs O(hop) RAM per hop — never one
    /// whole-interval buffer — while fjall's synchronous range guard still
    /// never crosses an `.await`. [`Scan::limit`] caps the total hits yielded
    /// (a caller consuming fewer simply stops polling, ending the hops early).
    /// Each yielded item is coop-wrapped so a large drain yields to the
    /// runtime.
    ///
    /// The fjall keyspace is shared across **all** collections, so the scan is
    /// bounded to the `(collection, section)` byte prefix on both ends — an
    /// unbounded high bound stops at the section's upper boundary, never
    /// bleeding into the next section or another collection. A per-row prefix
    /// check guards the same invariant defensively (see `scan_hop`).
    pub(crate) fn scan_present<'a>(
        &'a self,
        collection: &'a CollectionId,
        scan: Scan<'a>,
    ) -> impl Stream<Item = Result<ScanHit, FjallCellCacheError>> + Send + 'a {
        self.scan_present_hopping(collection, scan, SCAN_HOP_ROWS)
    }

    /// [`scan_present`](Self::scan_present) with an explicit per-hop row
    /// budget, so tests can drive many re-seek hops over a small fixture.
    fn scan_present_hopping<'a>(
        &'a self,
        collection: &'a CollectionId,
        scan: Scan<'a>,
        hop_rows: usize,
    ) -> impl Stream<Item = Result<ScanHit, FjallCellCacheError>> + Send + 'a {
        let handle = self.inner.handle().clone();
        let section = scan.section;
        let dir = scan.dir;
        let section_prefix = codec::section_prefix(collection, section);
        let (lo, hi) = scan.low_high();
        let mut lo_bound = byte_low_bound(&section_prefix, lo);
        let mut hi_bound = byte_high_bound(&section_prefix, hi);
        let mut remaining = scan.limit;
        let now = self.clock.now_ms();

        try_stream! {
            loop {
                if remaining == Some(0) {
                    break;
                }
                let budget = hop_rows.max(1);
                let need = remaining.map_or(budget, |n| n.min(budget));
                let hop_handle = handle.clone();
                let hop_prefix = section_prefix.clone();
                let (hop_lo, hop_hi) = (lo_bound.clone(), hi_bound.clone());
                let (hits, resume) = spawn_blocking(move || {
                    let rows = hop_handle.range((hop_lo, hop_hi));
                    match dir {
                        Direction::Forward => {
                            scan_hop(rows, &hop_prefix, section, now, budget, need, dir)
                        }
                        Direction::Backward => {
                            scan_hop(rows.rev(), &hop_prefix, section, now, budget, need, dir)
                        }
                    }
                })
                .await??;
                remaining = remaining.map(|n| n.saturating_sub(hits.len()));
                for hit in hits {
                    yield hit;
                }
                match resume {
                    // The hop stopped on a budget/need bound; re-seek just past
                    // the last examined key on the direction's moving side.
                    Some(key) => match dir {
                        Direction::Forward => lo_bound = Bound::Excluded(key),
                        Direction::Backward => hi_bound = Bound::Excluded(key),
                    },
                    None => break,
                }
            }
        }
        .then(|item| cooperative(async move { item }))
    }

    // --- Warm index: provisional coordinates + cold-seed latch ---------------
    //
    // The warm provisional index the recovery sweep short-circuits on. It lives
    // in the per-partition `index` keyspace, cold at a fresh epoch and dropped
    // at revocation. It is the disk-spilling relocation of the former in-RAM
    // `ProvisionalIndex`; the durable Cassandra `kind=Index` markers remain the
    // authoritative cold-recovery source (a fresh epoch re-seeds from them).

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
    pub(crate) async fn index_unseed(
        &self,
        collection: &CollectionId,
    ) -> Result<(), FjallCellCacheError> {
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
            let mut out: Vec<CellKey> = Vec::new();
            for guard in handle.prefix(&prefix) {
                let (key, _) = guard.into_inner()?;
                out.push(codec::coord_cell_key(&key));
            }
            Ok(out)
        })
        .await?
    }

    // --- Warm coverage: on-disk `(collection, section)` interval sets ---------
    //
    // Each `(collection, section)`'s covered intervals live as a `Cover` prefix
    // range in the `index` keyspace, one entry per interval keyed by its
    // low-bound frame → high-bound frame. This is the disk-spilling relocation
    // of the former in-RAM `Coverage.sections` map: reads load the bounded
    // per-section interval list, writes rewrite it, all within one blocking hop.

    /// Loads one `(collection, section)`'s stored coverage intervals as
    /// `(lo, hi)` bound pairs in the fjall range's raw key order, which is
    /// tag-first and **not** `cmp_low` — the caller re-sorts via
    /// `IntervalSet::from_pairs`. Bounded by the section's merged interval
    /// count.
    pub(crate) async fn cover_load(
        &self,
        collection: &CollectionId,
        section: Section,
    ) -> Result<Vec<(Bound<Coordinate>, Bound<Coordinate>)>, FjallCellCacheError> {
        let handle = self.inner.index_handle().clone();
        let prefix = codec::index_cover_prefix(collection, section);
        spawn_blocking(move || {
            let mut out: Vec<(Bound<Coordinate>, Bound<Coordinate>)> = Vec::new();
            for guard in handle.prefix(&prefix) {
                let (key, value) = guard.into_inner()?;
                let lo = codec::cover_low_bound(&key)?;
                let hi = codec::decode_bound(value.as_ref())?;
                out.push((lo, hi));
            }
            Ok(out)
        })
        .await?
    }

    /// Rewrites one `(collection, section)`'s coverage: clears its whole
    /// `Cover` prefix range and re-inserts `intervals`, atomically in one
    /// [`OwnedWriteBatch`]. Rewriting the whole section (never incremental key
    /// edits) means a merge that shifts a low bound can never leave a stale
    /// interval key behind.
    pub(crate) async fn cover_store(
        &self,
        collection: &CollectionId,
        section: Section,
        intervals: &[(Bound<Coordinate>, Bound<Coordinate>)],
    ) -> Result<(), FjallCellCacheError> {
        #[cfg(test)]
        if self
            .fail_cover_stores
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_sub(1))
            .is_ok()
        {
            return Err(FjallCellCacheError::Injected);
        }
        let handle = self.inner.index_handle().clone();
        let prefix = codec::index_cover_prefix(collection, section);
        // Encode every key/value frame up front (bounded, sized once) so the
        // blocking closure only touches fjall.
        let mut framed: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(intervals.len());
        for (lo, hi) in intervals {
            framed.push((
                codec::index_cover_key(collection, section, lo),
                codec::encode_bound(hi),
            ));
        }
        let capacity = framed.len() + 1;
        self.run_batch(handle, capacity, move |batch, handle| {
            // Clear the section's whole existing range, then re-insert the merged
            // set. `remove_range`-by-prefix has no batch primitive, so stale keys
            // are removed by first collecting them in the same guard scan.
            let stale: Vec<Slice> = handle
                .prefix(&prefix)
                .map(|guard| guard.into_inner().map(|(key, _)| key))
                .collect::<Result<_, _>>()?;
            for key in &stale {
                batch.remove(handle, key.as_ref());
            }
            for (key, value) in &framed {
                batch.insert(handle, key.as_slice(), value.as_slice());
            }
            Ok(())
        })
        .await
    }

    /// Runs `fill` over a fresh [`OwnedWriteBatch`] against `handle` and
    /// commits it, all in a single blocking hop — the shared ceremony behind
    /// every all-or-nothing batch mutator except
    /// [`commit_batch`](Self::commit_batch), which reports a per-cell
    /// [`CoverDecision`] instead of a bare result.
    async fn run_batch(
        &self,
        handle: Keyspace,
        capacity: usize,
        fill: impl FnOnce(&mut OwnedWriteBatch, &Keyspace) -> fjall::Result<()> + Send + 'static,
    ) -> Result<(), FjallCellCacheError> {
        let database = self.inner.database().clone();
        spawn_blocking(move || {
            let mut batch = OwnedWriteBatch::with_capacity(database, capacity);
            fill(&mut batch, &handle)?;
            batch.commit()
        })
        .await??;
        Ok(())
    }
}

/// One bounded hop of a chunked [`FjallCellCache::scan_present`]: walks `rows`
/// (already oriented in the scan direction) collecting up to `need` hits from
/// at most `budget` rows, and returns the hits plus the re-seek key — `Some`
/// (the last raw key examined) when the hop stopped on a bound and more rows
/// may remain, `None` when the range was exhausted.
///
/// The fjall keyspace is shared across all collections, so a row outside the
/// section prefix is never yielded: walking forward it marks the section's
/// upper boundary (reachable only through the missing-successor unbounded high
/// bound) and ends the scan; walking backward it *precedes* the section (the
/// low byte bound pins the other side), so it is skipped but still counted
/// against the budget, keeping every hop's work bounded.
fn scan_hop(
    rows: impl Iterator<Item = Guard>,
    section_prefix: &[u8],
    section: Section,
    now: u64,
    budget: usize,
    need: usize,
    dir: Direction,
) -> Result<(Vec<ScanHit>, Option<Vec<u8>>), FjallCellCacheError> {
    let mut hits: Vec<ScanHit> = Vec::with_capacity(need);
    let mut examined = 0usize;
    for guard in rows {
        let (key, value) = guard.into_inner()?;
        examined += 1;
        if key.starts_with(section_prefix) {
            let (expiry, read) = codec::decode_cell(Some(value.as_ref()))?;
            let cell = CellKey {
                section,
                coordinate: codec::coordinate_of(&key),
            };
            if expired(expiry, now) {
                // The fjall entry expired before its durable row could have
                // (FLOOR): the covered coordinate must fall through, not read
                // as absent.
                hits.push(ScanHit::Expired(cell));
            } else if let Read::Present(payload) = read {
                hits.push(ScanHit::Present(cell, payload));
            }
        } else if dir == Direction::Forward {
            return Ok((hits, None));
        }
        if hits.len() >= need || examined >= budget {
            return Ok((hits, Some(key.as_ref().to_vec())));
        }
    }
    Ok((hits, None))
}

/// Encodes a cell's presence/absence into its stored frame at `expiry`: a
/// present `payload` writes the payload cell, `None` writes the `Absent` tag.
fn encode_frame(payload: Option<&Bytes>, expiry: u64) -> Bytes {
    match payload {
        Some(payload) => codec::encode_present_cell(payload, expiry),
        None => codec::encode_absent_cell(expiry),
    }
}

/// The encoded warm-index keys of a batch of coordinates — built up front
/// (bounded, sized once) so the blocking batch closure only touches fjall.
fn index_keys<'a>(
    collection: &CollectionId,
    cells: impl ExactSizeIterator<Item = &'a CellKey>,
) -> Vec<Vec<u8>> {
    let mut keys: Vec<Vec<u8>> = Vec::with_capacity(cells.len());
    for cell in cells {
        keys.push(codec::index_coord_key(collection, cell));
    }
    keys
}

/// Reads the raw cell at `key`, or `None` when the key is absent — one
/// blocking hop.
async fn read_cell(cache: &Keyspace, key: Vec<u8>) -> Result<Option<Slice>, FjallCellCacheError> {
    let cache = cache.clone();
    Ok(spawn_blocking(move || cache.get(key)).await??)
}

/// Writes `cell` at `key`, overwriting any existing cell — one blocking hop.
async fn write_cell(
    cache: &Keyspace,
    key: Vec<u8>,
    cell: Bytes,
) -> Result<(), FjallCellCacheError> {
    let cache = cache.clone();
    spawn_blocking(move || cache.insert(key.as_slice(), cell.as_ref())).await??;
    Ok(())
}

/// Inserts an empty-valued warm-index key (presence-as-boolean), one blocking
/// hop.
async fn write_index_empty(handle: &Keyspace, key: Vec<u8>) -> Result<(), FjallCellCacheError> {
    let handle = handle.clone();
    spawn_blocking(move || handle.insert(key.as_slice(), [].as_slice())).await??;
    Ok(())
}

/// Whether an absolute `expiry` (millis; `0` = never) has passed at `now`.
fn expired(expiry: u64, now: u64) -> bool {
    expiry != codec::NEVER_EXPIRES && now >= expiry
}

/// Reads the absolute stage expiry stamped on the cell at `key` back from the
/// cache keyspace, or `None` when no entry exists or the read/decode fails (a
/// failure logs and degrades — the caller then uncovers the coordinate so the
/// next read self-heals). Runs inside
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

/// The fjall byte key bound opening a covered scan's low side. `Unbounded`
/// starts at the section prefix itself (the least key in the section); a
/// bounded coordinate appends to the prefix, preserving exclusivity.
fn byte_low_bound(section_prefix: &[u8], lo: Bound<&Coordinate>) -> Bound<Vec<u8>> {
    match lo {
        Bound::Unbounded => Bound::Included(section_prefix.to_vec()),
        Bound::Included(c) => Bound::Included(byte_key(section_prefix, c)),
        Bound::Excluded(c) => Bound::Excluded(byte_key(section_prefix, c)),
    }
}

/// The fjall byte key bound closing a covered scan's high side. `Unbounded`
/// stops at the section's upper boundary (the successor prefix), so the scan
/// never crosses into the next section or another collection.
fn byte_high_bound(section_prefix: &[u8], hi: Bound<&Coordinate>) -> Bound<Vec<u8>> {
    match hi {
        Bound::Unbounded => section_upper_bound(section_prefix),
        Bound::Included(c) => Bound::Included(byte_key(section_prefix, c)),
        Bound::Excluded(c) => Bound::Excluded(byte_key(section_prefix, c)),
    }
}

/// The full fjall key for `coordinate` within `section_prefix`.
fn byte_key(section_prefix: &[u8], coordinate: &Coordinate) -> Vec<u8> {
    let coordinate = coordinate.as_bytes();
    let mut key = Vec::with_capacity(section_prefix.len() + coordinate.len());
    key.extend_from_slice(section_prefix);
    key.extend_from_slice(coordinate);
    key
}

/// The smallest byte key strictly greater than every key carrying
/// `section_prefix` — the lexicographic successor (increment the rightmost
/// non-`0xFF` byte, drop the tail). An all-`0xFF` prefix has no successor, so
/// the scan runs unbounded-high (the per-item prefix check then stops it).
fn section_upper_bound(section_prefix: &[u8]) -> Bound<Vec<u8>> {
    let mut bound = section_prefix.to_vec();
    while let Some(last) = bound.last_mut() {
        if *last < u8::MAX {
            *last += 1;
            return Bound::Excluded(bound);
        }
        bound.pop();
    }
    Bound::Unbounded
}
