//! A disk cache for committed cell projections.
//!
//! [`FjallCellCache`] stores [`CacheEntry`](crate::state::cell::CacheEntry)
//! frames for point and batch reads. [`Projection`] converts each frame into
//! the requested answer. A value frame can answer either projection. A presence
//! frame cannot answer a value read. Presence reads borrow frames without a
//! payload copy. [`CacheRead`] distinguishes hits, expired answers, unknown
//! answers, and corrupt frames. [`Cached`](crate::state::cached::Cached)
//! supplies durable reads when this cache cannot answer.
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

use crate::state::cell_key::{CellKey, CellRef, Section};
use crate::state::store::{Answers, CellBuffer, ReadBatch};
mod checks;
mod clock;
mod codec;
mod error;
#[cfg(test)]
mod faults;
mod io;
mod workspace;

#[cfg(test)]
pub(crate) mod test_db;
#[cfg(test)]
mod tests;

pub(crate) use checks::MarkerCheckSet;
pub(crate) use clock::Clock;
pub(crate) use error::FjallCellCacheError;
#[cfg(test)]
pub(crate) use faults::Faults;
#[cfg(test)]
pub(crate) use workspace::FjallClientError;
use workspace::Inner;
pub(crate) use workspace::{FjallClient, FjallWorkspace};

use crate::state::CollectionId;
use crate::state::cell::{Committed, Projection, ProvisionalWrite, Values};
use crate::state::store::Durable;
use bytes::Bytes;
use educe::Educe;
#[cfg(test)]
use fjall::{Database, Keyspace};
use smallvec::SmallVec;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::task::spawn_blocking;

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
    /// Test-only fault seams shared by every clone.
    #[cfg(test)]
    #[educe(Debug(ignore))]
    faults: Faults,
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
            faults: Faults::default(),
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

    /// The test-only fault seams of this cache.
    #[cfg(test)]
    pub(crate) fn faults(&self) -> &Faults {
        &self.faults
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
        io::write_cell(
            self.inner.handle(),
            codec::cell_key(collection, cell.as_ref()),
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
        cell: CellRef<'_>,
    ) -> Result<CacheRead<P>, FjallCellCacheError> {
        #[cfg(test)]
        self.faults.read()?;
        let raw = io::read_cell(self.inner.handle(), codec::cell_key(collection, cell)).await?;
        Ok(io::probe::<P>(raw.as_deref(), self.clock.now_ms()))
    }

    /// Probes every coordinate in one blocking call and returns one result per
    /// position. A decode failure returns `Corrupt` at that position.
    /// Only engine and join failures return `Err`.
    pub(crate) async fn get_batch<P: Projection>(
        &self,
        collection: &CollectionId,
        section: Section,
        batch: &ReadBatch<'_>,
    ) -> Result<Answers<CacheRead<P>>, FjallCellCacheError> {
        // Encode every key up front (bounded, sized once): small requests stay
        // inline and the owned keys move into the blocking closure.
        let keys = batch.map(|&coordinate| {
            codec::cell_key(
                collection,
                CellRef {
                    section,
                    coordinate,
                },
            )
        });
        let handle = self.inner.handle().clone();
        let now = self.clock.now_ms();
        #[cfg(test)]
        let faults = self.faults.clone();
        // ONE blocking hop reads every key exhaustively; a per-key engine error
        // (or the injected fault) fails the whole hop, mirroring how `read_cell`
        // surfaces one via `??`.
        spawn_blocking(move || {
            #[cfg(test)]
            faults.probe()?;
            keys.try_map(|key| {
                let raw = handle.get(key.as_slice())?;
                Ok(io::probe::<P>(raw.as_deref(), now))
            })
        })
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
        let raw = io::read_cell(
            self.inner.handle(),
            codec::cell_key(collection, cell.as_ref()),
        )
        .await?;
        codec::frame_expiry(raw.as_deref())
    }

    /// Publishes one committed projection with an absolute expiry.
    /// [`Projection::into_cached`] selects the frame contents. Zero expiry
    /// means no expiry.
    pub(crate) async fn put<P: Projection>(
        &self,
        collection: &CollectionId,
        cell: CellRef<'_>,
        value: Committed<P>,
        expiry: u64,
    ) -> Result<(), FjallCellCacheError> {
        #[cfg(test)]
        self.faults.put()?;
        let frame = io::encode_frame(&P::into_cached(value.into_inner()), expiry);
        io::write_cell(
            self.inner.handle(),
            codec::cell_key(collection, cell),
            frame,
        )
        .await
    }

    /// Publishes committed projections in one atomic
    /// [`OwnedWriteBatch`](fjall::OwnedWriteBatch). A failed commit leaves
    /// the cache unchanged. A durable write caller removes old entries
    /// after a failed cache update. A read fill caller retains old entries
    /// because durable state did not change.
    pub(crate) async fn put_batch<'a, P: Projection>(
        &self,
        collection: &CollectionId,
        cells: impl IntoIterator<Item = (CellRef<'a>, Committed<P>, u64)>,
    ) -> Result<(), FjallCellCacheError> {
        #[cfg(test)]
        self.faults.put()?;
        // Encode every key + frame up front (bounded, sized once from the
        // caller's iterator) so the blocking closure only touches fjall; the
        // owned key/frame pairs move into it. Building `framed` directly from the
        // projected iterator avoids an intermediate collect on the settle path.
        let framed: CellBuffer<(SmallVec<[u8; 32]>, Bytes)> = cells
            .into_iter()
            .map(|(cell, value, expiry)| {
                (
                    codec::cell_key(collection, cell),
                    io::encode_frame(&P::into_cached(value.into_inner()), expiry),
                )
            })
            .collect();
        let handle = self.inner.handle().clone();
        let capacity = framed.len();
        io::run_batch(
            self.inner.database().clone(),
            handle,
            capacity,
            move |batch, handle| {
                for (key, frame) in &framed {
                    batch.insert(handle, key.as_slice(), frame.as_ref());
                }
                Ok(())
            },
        )
        .await
    }

    /// Publishes each staged cell's committed value at its stage expiry.
    /// One atomic [`OwnedWriteBatch`](fjall::OwnedWriteBatch) runs in
    /// [`spawn_blocking`]. [`Cached`](crate::state::cached::Cached) calls
    /// this after the durable promote returns. It disables the cache if
    /// publication does not complete.
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
        self.faults.put()?;
        // Owned closure inputs, bounded and sized once: the cell key plus the
        // committed `data` to rewrite at its read-back stage expiry.
        let mut inputs: CellBuffer<(SmallVec<[u8; 32]>, Option<Bytes>)> =
            SmallVec::with_capacity(writes.len());
        for (cell, write) in writes {
            inputs.push((
                codec::cell_key(collection, cell.as_ref()),
                write.data().cloned(),
            ));
        }
        let database = self.inner.database().clone();
        let handle = self.inner.handle().clone();
        spawn_blocking(move || io::commit_stage(database, &handle, &inputs)).await??;
        Ok(())
    }

    /// Deletes the committed entries of `cells` in one atomic
    /// [`OwnedWriteBatch`](fjall::OwnedWriteBatch). Repairs depend on this
    /// delete. Deleting an absent entry does nothing, so a retry is safe.
    pub(crate) async fn delete_batch<'a>(
        &self,
        collection: &CollectionId,
        cells: impl IntoIterator<Item = CellRef<'a>>,
    ) -> Result<(), FjallCellCacheError> {
        #[cfg(test)]
        self.faults.delete()?;
        let keys: CellBuffer<SmallVec<[u8; 32]>> = cells
            .into_iter()
            .map(|cell| codec::cell_key(collection, cell))
            .collect();
        let handle = self.inner.handle().clone();
        let capacity = keys.len();
        io::run_batch(
            self.inner.database().clone(),
            handle,
            capacity,
            move |batch, handle| {
                for key in &keys {
                    batch.remove(handle, key.as_slice());
                }
                Ok(())
            },
        )
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
    /// Other callers pass an empty set to delete the whole section.
    /// The exclusion set encodes each coordinate once with `codec::cell_key`,
    /// the same form that the scan returns.
    /// A hash set gives expected O(1) work per scanned key and O(|exclude| +
    /// one hop) memory.
    pub(crate) async fn delete_section<'a>(
        &self,
        collection: &CollectionId,
        section: Section,
        exclude: impl IntoIterator<Item = CellRef<'a>>,
    ) -> Result<(), FjallCellCacheError> {
        #[cfg(test)]
        self.faults.delete()?;
        let excluded = exclude
            .into_iter()
            .map(|cell| codec::cell_key(collection, cell))
            .collect();
        io::delete_section(
            self.inner.database().clone(),
            self.inner.handle().clone(),
            codec::section_prefix(collection, section),
            Arc::new(excluded),
        )
        .await
    }
}
