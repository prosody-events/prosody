//! A disk cache for committed cell projections.
//!
//! [`FjallCellCache`] stores [`CacheEntry`] frames for point and batch reads.
//! [`Projection`] converts each frame into the requested answer.
//! A value frame can answer either projection. A presence frame cannot answer a
//! value read. Presence reads borrow frames without a payload copy.
//! [`CacheRead`] distinguishes hits, expired answers, unknown answers, and
//! corrupt frames. [`Cached`](crate::state::cached::Cached) supplies durable
//! reads when this cache cannot answer.
//!
//! The workspace also stores provisional cells and completed admission checks.
//! All components share one cache-disabled state.
//!
//! # Workspace ownership
//!
//! [`FjallCellCache::for_workspace`] retains its [`FjallWorkspace`] for the
//! partition assignment. The workspace removes its keyspace when the assignment
//! ends. Test caches from [`FjallCellCache::new`] use a shared database without
//! an owned workspace.
//!
//! # Expiry and storage
//!
//! Each frame carries an absolute expiry. [`Clock`] checks that expiry during
//! reads. Hits carry the remaining TTL. The codec stores value payloads
//! verbatim. The cache follows the expiry contract in
//! [`Cached`](crate::state::cached::Cached).
//!
//! Fjall uses synchronous I/O. Reads and writes use
//! [`tokio::task::spawn_blocking`].

mod codec;
mod error;
mod workspace;

#[cfg(test)]
pub(crate) mod test_db;
#[cfg(test)]
mod tests;

pub(crate) use error::FjallCellCacheError;
#[cfg(test)]
pub(crate) use workspace::FjallClientError;
pub(crate) use workspace::{FjallClient, FjallWorkspace};

use crate::state::CollectionId;
use crate::state::backend::AdmissionChecks;
use crate::state::cell::{CacheEntry, Committed, Projection, ProvisionalWrite, Read, Values};
use crate::state::cell_key::{CellKey, Section};
use crate::state::store::{CellBuffer, CoordinateBatch};
use crate::state::store_types::Durable;
use crate::timers::duration::CompactDuration;
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

/// Assignments that disabled their cell cache after a repair failure.
///
/// [`FjallCellCache::disable`] increments this counter once per assignment.
static CACHE_DISABLED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    meter("prosody")
        .u64_counter("prosody.state.cell.cache.disabled_assignments")
        .with_description("Keyed-state assignments that disabled their cell cache")
        .with_unit("{assignment}")
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

/// The four-state result of a [`FjallCellCache::get`].
#[derive(Clone, Debug)]
pub(crate) enum CacheRead<P: Projection = Values> {
    /// An unexpired answer with its remaining durable TTL.
    Hit(Durable<P>),
    /// An entry exists but its stamped expiry has passed; the caller falls
    /// through to the lower store and re-publishes a fresh entry.
    Expired,
    /// The entry is missing or cannot answer this projection.
    Miss,
    /// The frame at this position does not decode. A fill overwrites it.
    Corrupt,
}

/// Fjall-backed cell cache.
#[derive(Clone, Educe)]
#[educe(Debug)]
pub(crate) struct FjallCellCache {
    #[educe(Debug(ignore))]
    inner: Arc<Inner>,
    clock: Clock,
    /// The cache-disabled state for all workspace handles.
    #[educe(Debug(ignore))]
    disabled: Arc<AtomicBool>,
    /// Test-only fault seam: when set, every [`put`](Self::put),
    /// [`put_batch`](Self::put_batch), and [`commit_batch`](Self::commit_batch)
    /// returns an engine error without touching fjall, so a test can force a
    /// publish failure (the failed-publish cache guard repair path).
    #[cfg(test)]
    #[educe(Debug(ignore))]
    fail_puts: Arc<AtomicBool>,
    /// Test-only fault seam: a countdown of delete-side calls to fail — each
    /// failure decrements it — consulted by
    /// [`delete_batch`](Self::delete_batch),
    /// [`delete_section`](Self::delete_section), and
    /// [`index_unseed`](Self::index_unseed), so a test can make exactly the
    /// next N must-succeed deletes fail (forcing the retry path, or blowing
    /// cache disablement past the budget) and then heal automatically.
    #[cfg(test)]
    #[educe(Debug(ignore))]
    fail_deletes: Arc<AtomicU64>,
    /// Test-only fault: point and batch probes return an engine error when set.
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
/// (warm provisional coordinates and the cold-seed and marker-check
/// latches)
/// rides alongside `cache` in
/// both arms purely for lifecycle co-location — it shares the workspace's
/// lifecycle (cold at a fresh assignment, dropped at revocation). Index and
/// cell-cache writes are **not** issued as one cross-keyspace batch; the warm
/// index is a rebuildable hint (a fresh assignment re-seeds from the durable
/// event marker), so they need no atomicity with the committed-value write.
enum Inner {
    #[cfg(test)]
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
            #[cfg(test)]
            Self::Bare { cache, .. } => cache,
            Self::Owned(workspace) => workspace.cache_handle(),
        }
    }

    /// The warm-index keyspace handle (provisional coordinates and the
    /// cold-seed rows and marker-check rows).
    fn index_handle(&self) -> &Keyspace {
        match self {
            #[cfg(test)]
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
            #[cfg(test)]
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
    #[cfg(test)]
    pub(crate) fn new(database: Database, cache: Keyspace, index: Keyspace) -> Self {
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
            disabled: Arc::new(AtomicBool::new(false)),
            #[cfg(test)]
            fail_puts: Arc::new(AtomicBool::new(false)),
            #[cfg(test)]
            fail_deletes: Arc::new(AtomicU64::new(0)),
            #[cfg(test)]
            fail_reads: Arc::new(AtomicBool::new(false)),
            #[cfg(test)]
            blocking_probes: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Reports whether this assignment has disabled its cache.
    ///
    /// All cache and marker-check handles observe the same state.
    /// This state does not reset during an assignment.
    #[must_use]
    pub(crate) fn is_disabled(&self) -> bool {
        self.disabled.load(Ordering::Relaxed)
    }

    /// Disables the cache for this assignment.
    ///
    /// A disabled cache sends all operations to durable storage.
    pub(crate) fn disable(&self) {
        self.marker_checks().disable();
    }

    /// Test handle on the [`put`](Self::put) fault seam: returns the shared
    /// flag a test sets to force every publish to fail (then unsets to
    /// heal).
    #[cfg(test)]
    #[must_use]
    pub fn fail_puts(&self) -> Arc<AtomicBool> {
        self.fail_puts.clone()
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

    /// The shared fault flag for point and batch probes.
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

    /// Returns a marker-check handle for this workspace.
    #[must_use]
    pub(crate) fn marker_checks(&self) -> MarkerCheckSet {
        MarkerCheckSet {
            index: self.inner.index_handle().clone(),
            disabled: self.disabled.clone(),
        }
    }

    /// Reads one projection and its remaining TTL from an unexpired frame.
    /// A decode failure returns `Corrupt`. Only engine and join failures return
    /// `Err`.
    pub(crate) async fn get<P: Projection>(
        &self,
        collection: &CollectionId,
        cell: &CellKey,
    ) -> Result<CacheRead<P>, FjallCellCacheError> {
        #[cfg(test)]
        if self.fail_reads.load(Ordering::Relaxed) {
            return Err(FjallCellCacheError::Injected);
        }
        let raw = read_cell(self.inner.handle(), codec::cell_key(collection, cell)).await?;
        let (expiry, entry) = match codec::decode_frame(raw.as_deref()) {
            Ok(frame) => frame,
            Err(error) => {
                warn!(%error, "cell cache frame does not decode");
                return Ok(CacheRead::Corrupt);
            }
        };
        Ok(classify::<P>(
            expiry,
            entry.map_or(Read::Unknown, P::from_cached),
            self.clock.now_ms(),
        ))
    }

    /// Probes every coordinate in one blocking call and returns one result per
    /// position. A decode failure returns `Corrupt` at that position.
    /// Only engine and join failures return `Err`.
    pub(crate) async fn get_batch<P: Projection>(
        &self,
        collection: &CollectionId,
        section: Section,
        batch: &CoordinateBatch,
    ) -> Result<CellBuffer<CacheRead<P>>, FjallCellCacheError> {
        let raws = self.read_batch(collection, section, batch).await?;
        let now = self.clock.now_ms();
        let mut reads = CellBuffer::with_capacity(raws.len());
        for raw in raws {
            let (expiry, entry) = match codec::decode_frame(raw.as_deref()) {
                Ok(frame) => frame,
                Err(error) => {
                    warn!(%error, "cell cache frame does not decode");
                    reads.push(CacheRead::Corrupt);
                    continue;
                }
            };
            reads.push(classify::<P>(
                expiry,
                entry.map_or(Read::Unknown, P::from_cached),
                now,
            ));
        }
        Ok(reads)
    }

    async fn read_batch(
        &self,
        collection: &CollectionId,
        section: Section,
        batch: &CoordinateBatch,
    ) -> Result<CellBuffer<Option<Slice>>, FjallCellCacheError> {
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
        spawn_blocking(
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
        .await?
    }

    /// Returns the stored expiry, including expired frames, or `None` for a
    /// missing entry. Zero means no expiry. Tests use this to check the
    /// durable expiry contract.
    #[cfg(test)]
    pub(crate) async fn stored_expiry(
        &self,
        collection: &CollectionId,
        cell: &CellKey,
    ) -> Result<Option<u64>, FjallCellCacheError> {
        let raw = read_cell(self.inner.handle(), codec::cell_key(collection, cell)).await?;
        codec::frame_expiry(raw.as_deref())
    }

    /// Publishes one committed projection with an absolute expiry.
    /// [`Projection::into_cached`] selects the frame contents. Zero expiry
    /// means no expiry.
    pub(crate) async fn put<P: Projection>(
        &self,
        collection: &CollectionId,
        cell: &CellKey,
        value: Committed<P>,
        expiry: u64,
    ) -> Result<(), FjallCellCacheError> {
        #[cfg(test)]
        if self.fail_puts.load(Ordering::Relaxed) {
            return Err(FjallCellCacheError::Injected);
        }
        let frame = encode_frame(&P::into_cached(value.into_inner()), expiry);
        write_cell(
            self.inner.handle(),
            codec::cell_key(collection, cell),
            frame,
        )
        .await
    }

    /// Publishes committed projections in one atomic [`OwnedWriteBatch`].
    /// A failed commit leaves the cache unchanged.
    /// A durable write caller removes old entries after a failed cache update.
    /// A read fill caller retains old entries because durable state did not
    /// change.
    pub(crate) async fn put_batch<P: Projection>(
        &self,
        collection: &CollectionId,
        cells: impl IntoIterator<Item = (CellKey, Committed<P>, u64)>,
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
                    encode_frame(&P::into_cached(value.into_inner()), expiry),
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

    /// Publishes each staged cell's committed value at its stage expiry.
    /// One atomic [`OwnedWriteBatch`] runs in [`spawn_blocking`].
    /// [`Cached`](crate::state::cached::Cached) calls this after the durable
    /// promote returns. It disables the cache if publication does not complete.
    ///
    /// The promote preserves the durable cell's expiry. This transform retains
    /// the cached expiry, so a repeated transform cannot extend retention.
    /// The transform removes missing or unreadable entries. The next read loads
    /// them from the durable store.
    ///
    /// Any failure returns `Err` so the caller removes the affected entries.
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
                        let frame = codec::encode_frame(
                            data.as_deref()
                                .map_or(CacheEntry::Absent, CacheEntry::Value),
                            expiry,
                        );
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
    /// [`OwnedWriteBatch`] — the must-succeed repair primitive (keys built at
    /// exact size). Idempotent: removing an absent key is a no-op.
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

    /// Deletes committed entries from one collection section in hops of at most
    /// [`SCAN_HOP_ROWS`] keys.
    /// Each hop uses [`spawn_blocking`], deletes keys in one bounded batch, and
    /// resumes after the last examined key.
    /// The operation never holds the whole section in RAM. Deleted keys
    /// disappear, so retries can safely repeat the scan.
    ///
    /// `exclude` names the staged coordinates that survive
    /// [`Cached::commit_provisional`](crate::state::cached::Cached).
    /// Other callers pass `&[]` to delete the whole section.
    /// The exclusion set encodes each coordinate once with `codec::cell_key`,
    /// the same form that the scan returns.
    /// A hash set gives expected O(1) work per scanned key and O(|exclude| +
    /// one hop) memory.
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

/// Stores admission proofs in the assignment's disk workspace.
/// Workspace deletion and the startup orphan sweep reclaim the rows.
#[derive(Clone, Educe)]
#[educe(Debug)]
pub(crate) struct MarkerCheckSet {
    #[educe(Debug(ignore))]
    index: Keyspace,
    #[educe(Debug(ignore))]
    disabled: Arc<AtomicBool>,
}

impl MarkerCheckSet {
    /// Disables this assignment once, with its log and counter.
    fn disable(&self) {
        if !self.disabled.swap(true, Ordering::Relaxed) {
            warn!("keyed-state cell cache disabled for this assignment; using durable reads");
            CACHE_DISABLED.add(1, &[]);
        }
    }
}

impl AdmissionChecks for MarkerCheckSet {
    type Error = FjallCellCacheError;

    async fn contains(&self, key: &crate::Key) -> Result<bool, Self::Error> {
        if self.disabled.load(Ordering::Relaxed) {
            return Ok(false);
        }
        let index = self.index.clone();
        let key = key.clone();
        Ok(spawn_blocking(move || index.contains_key(key.as_bytes())).await??)
    }

    async fn mark(&self, key: &crate::Key) -> Result<(), Self::Error> {
        if self.disabled.load(Ordering::Relaxed) {
            return Ok(());
        }
        let index = self.index.clone();
        let key = key.clone();
        spawn_blocking(move || index.insert(key.as_bytes(), [])).await??;
        Ok(())
    }

    async fn unmark(&self, key: &crate::Key) -> Result<(), Self::Error> {
        let index = self.index.clone();
        let key = key.clone();
        let result = async {
            spawn_blocking(move || index.remove(key.as_bytes())).await??;
            Ok(())
        }
        .await;
        if result.is_err() {
            self.disable();
        }
        result
    }
}

/// Borrows the owned payload for frame encoding.
fn encode_frame(entry: &CacheEntry<Bytes>, expiry: u64) -> Bytes {
    let borrowed = match entry {
        CacheEntry::Absent => CacheEntry::Absent,
        CacheEntry::Exists => CacheEntry::Exists,
        CacheEntry::Value(bytes) => CacheEntry::Value(bytes.as_ref()),
    };
    codec::encode_frame(borrowed, expiry)
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

/// Whether an absolute `expiry` (millis; `0` = never) has passed at `now`.
fn expired(expiry: u64, now: u64) -> bool {
    expiry != codec::NEVER_EXPIRES && now >= expiry
}

/// Classifies a projected frame at `now` and gives each hit its remaining TTL.
/// Point and batch reads share this classifier.
fn classify<P: Projection>(expiry: u64, read: Read<P::Payload>, now: u64) -> CacheRead<P> {
    let remaining = || {
        (expiry != codec::NEVER_EXPIRES).then(|| {
            CompactDuration::new(
                u32::try_from(expiry.saturating_sub(now) / 1_000).unwrap_or(u32::MAX),
            )
        })
    };
    match read {
        _ if expired(expiry, now) => CacheRead::Expired,
        Read::Unknown => CacheRead::Miss,
        Read::Present(payload) => CacheRead::Hit((Committed::new(Some(payload)), remaining())),
        Read::Absent => CacheRead::Hit((Committed::new(None), remaining())),
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
    match codec::frame_expiry(raw.as_deref()) {
        Ok(expiry) => expiry,
        Err(error) => {
            warn!(%error, "committed-value cache commit expiry decode failed; degrading");
            None
        }
    }
}
