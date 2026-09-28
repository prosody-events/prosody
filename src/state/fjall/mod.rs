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
//! The keyspace also stores the assignment's admission markers. The cache and
//! its markers share one cache-disabled state.
//!
//! # Keyspace ownership
//!
//! A production cache owns its assignment's keyspace. When the last clone
//! drops, the keyspace is queued for deletion; see [`CacheSlot`]. Test caches
//! from [`FjallCellCache::new`] use a shared keyspace and delete nothing.
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
mod client;
mod clock;
mod codec;
mod error;
#[cfg(test)]
mod faults;
mod io;
mod lifecycle;

#[cfg(test)]
pub(crate) mod test_db;
#[cfg(test)]
mod tests;

pub(crate) use checks::MarkerCheckSet;
pub(crate) use client::FjallClient;
#[cfg(test)]
pub(crate) use client::FjallClientError;
pub(crate) use clock::Clock;
pub(crate) use error::FjallCellCacheError;
#[cfg(test)]
pub(crate) use faults::Faults;
pub(crate) use lifecycle::CacheSlot;
use lifecycle::Retire;

use crate::state::CollectionId;
use crate::state::cell::{Committed, Projection, ProvisionalWrite, Values};
use crate::state::store::Durable;
use bytes::Bytes;
use educe::Educe;
use fjall::{Database, Keyspace};
use opentelemetry::global::meter;
use opentelemetry::metrics::Counter;
use smallvec::SmallVec;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock};
use tokio::task::spawn_blocking;
use tracing::warn;

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
    database: Database,
    #[educe(Debug(ignore))]
    keyspace: Keyspace,
    clock: Clock,
    /// The cache-disabled state shared by every clone.
    #[educe(Debug(ignore))]
    disabled: Arc<AtomicBool>,
    /// Test-only fault seams shared by every clone.
    #[cfg(test)]
    #[educe(Debug(ignore))]
    faults: Faults,
    /// Queues the keyspace for deletion when the last clone drops. `None` only
    /// for test caches. Declared after `keyspace`, so that handle drops first.
    #[educe(Debug(ignore))]
    _retire: Option<Arc<Retire>>,
}

impl FjallCellCache {
    /// Builds a cache over a `keyspace` that it never deletes. Tests pass a
    /// shared keyspace and may drive `clock` to expire entries.
    fn new(database: Database, keyspace: Keyspace, clock: Clock) -> Self {
        Self {
            database,
            keyspace,
            clock,
            disabled: Arc::new(AtomicBool::new(false)),
            #[cfg(test)]
            faults: Faults::default(),
            _retire: None,
        }
    }

    /// Builds the production cache, which owns `keyspace` until `retire` drops.
    fn for_keyspace(database: Database, keyspace: Keyspace, retire: Retire) -> Self {
        Self {
            _retire: Some(Arc::new(retire)),
            ..Self::new(database, keyspace, Clock::Wall)
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

    /// Disables the cache for this assignment, with one log and one count.
    ///
    /// A disabled cache sends all operations to durable storage.
    pub(crate) fn disable(&self) {
        if !self.disabled.swap(true, Ordering::Relaxed) {
            warn!("keyed-state cell cache disabled for this assignment; using durable reads");
            CACHE_DISABLED.add(1, &[]);
        }
    }

    /// The assignment's keyspace.
    pub(super) fn keyspace(&self) -> &Keyspace {
        &self.keyspace
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
            &self.keyspace,
            codec::cell_key(collection, cell.as_ref()),
            bytes,
        )
        .await
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
        let raw = io::read_cell(&self.keyspace, codec::cell_key(collection, cell)).await?;
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
        let handle = self.keyspace.clone();
        let clock = self.clock.clone();
        #[cfg(test)]
        let faults = self.faults.clone();
        // ONE blocking hop reads every key exhaustively; a per-key engine error
        // (or the injected fault) fails the whole hop, mirroring how
        // `read_cell` surfaces one via `??`. As in `get`, each expiry
        // check reads the clock after its read.
        spawn_blocking(move || {
            #[cfg(test)]
            faults.probe()?;
            keys.try_map(|key| {
                let raw = handle.get(key.as_slice())?;
                Ok(io::probe::<P>(raw.as_deref(), clock.now_ms()))
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
        let raw = io::read_cell(&self.keyspace, codec::cell_key(collection, cell.as_ref())).await?;
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
        io::write_cell(&self.keyspace, codec::cell_key(collection, cell), frame).await
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
        // owned key/frame pairs move into it. Building `framed` directly from
        // the projected iterator avoids an intermediate collect on the
        // settle path.
        let framed: CellBuffer<(SmallVec<[u8; 32]>, Bytes)> = cells
            .into_iter()
            .map(|(cell, value, expiry)| {
                (
                    codec::cell_key(collection, cell),
                    io::encode_frame(&P::into_cached(value.into_inner()), expiry),
                )
            })
            .collect();
        let handle = self.keyspace.clone();
        let capacity = framed.len();
        io::run_batch(
            self.database.clone(),
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
        let database = self.database.clone();
        let handle = self.keyspace.clone();
        io::blocking(move || io::commit_stage(database, &handle, &inputs)).await
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
        let handle = self.keyspace.clone();
        let capacity = keys.len();
        io::run_batch(
            self.database.clone(),
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
            self.database.clone(),
            self.keyspace.clone(),
            codec::section_prefix(collection, section),
            Arc::new(excluded),
        )
        .await
    }
}
