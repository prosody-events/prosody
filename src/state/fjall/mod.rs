//! Fjall-backed cell cache.
//!
//! [`FjallCellCache`] stores one tagged cell per [`CellKey`] in a fjall
//! partition. It is the committed-value cache the
//! [`Cached`](crate::state::cached::Cached) coverage combinator serves from:
//! point [`get`](FjallCellCache::get)s and, over a covered scan sub-range,
//! ordered [`scan_present`](FjallCellCache::scan_present) range reads. It does
//! **not** implement [`CellStore`](crate::state::store::CellStore): it is a
//! concrete *partial* upper (it can only answer what it has mirrored), so a
//! bare cache view can never be mistaken for a complete store — completeness is
//! the coverage map's job, owned by `Cached`.
//!
//! # Workspace ownership
//!
//! In production the cache **owns** its [`FjallWorkspace`] (built via
//! [`FjallCellCache::for_workspace`]). The workspace's `Drop` deletes the fjall
//! partition, so the cache must hold it alive for the whole partition
//! assignment — it lives in the partition's state manager and drops only at
//! revocation. Test caches built from a bare handle ([`FjallCellCache::new`])
//! own no workspace.
//!
//! # Three-valued reads and TTL co-expiry
//!
//! Unlike the durable stores (Memory/Cassandra) whose `get` returns only
//! `Present`/`Absent`, the cache observes a third state: an entry that
//! has never been populated. That state is encoded as the **absence of an
//! entry** in the fjall partition, and decodes as
//! [`Read::Unknown`]. Tag byte `0x00` is
//! `Absent` (known cleared); tag byte `0x01` is `Present`. Each frame carries
//! an absolute expiry header (`[tag][expiry: u64 BE millis][payload]`, `0` =
//! never) mirroring the durable row's TTL death — fjall has no native per-entry
//! TTL, so the cache enforces it on read against its [`Clock`] (an expired
//! entry reads as a miss). The payload is stored verbatim — fjall
//! block-compresses the on-disk data block via LZ4, so there is no per-cell
//! codec layer.
//!
//! # Blocking I/O
//!
//! fjall's public API is synchronous, so the cache's reads and writes are
//! dispatched through [`tokio::task::spawn_blocking`] (in the `cell_io`
//! submodule), which clones the cheap `Arc`-backed handle into each blocking
//! closure.

mod cell_io;
mod codec;
mod config;
mod error;
mod workspace;

#[cfg(test)]
mod tests;

pub use config::FjallConfiguration;
pub use error::FjallCellCacheError;
pub use workspace::{AssignmentEpoch, FjallClient, FjallClientError, FjallWorkspace};

use crate::state::CollectionId;
use crate::state::cell::Committed;
use crate::state::cell_key::{CellKey, Coordinate, Direction, Section};
use crate::state::transaction::Read;
use bytes::Bytes;
use educe::Educe;
use fjall::PartitionHandle;
use futures::{Stream, StreamExt, stream};
use std::iter;
use std::ops::Bound;
use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::AtomicBool;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::task::coop::cooperative;
use tokio::task::spawn_blocking;

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
pub enum Clock {
    /// The system wall clock.
    Wall,
    /// A test-controlled clock over a shared millisecond counter.
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
            Self::Fixed(now) => now.load(Ordering::Relaxed),
        }
    }
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
}

/// Backing for a [`FjallCellCache`]: either a bare cache handle (tests) or an
/// owned per-partition workspace whose cache handle the cache operates and
/// whose `Drop` deletes the partition at revocation (production).
enum Inner {
    Bare(PartitionHandle),
    Owned(FjallWorkspace),
}

impl Inner {
    /// The cache partition handle this cache operates.
    fn partition(&self) -> &PartitionHandle {
        match self {
            Self::Bare(partition) => partition,
            Self::Owned(workspace) => workspace.cache_handle(),
        }
    }
}

impl FjallCellCache {
    /// Builds a cache over an opened cache `PartitionHandle`, owning no
    /// workspace.
    ///
    /// The caller owns the keyspace the handle belongs to and is responsible
    /// for keeping it (and the partition) alive for the cache's lifetime. Used
    /// by tests; production uses [`Self::for_workspace`], which owns the
    /// workspace.
    #[must_use]
    pub fn new(partition: PartitionHandle) -> Self {
        Self::from_parts(Inner::Bare(partition), Clock::Wall)
    }

    /// Builds a bare cache over `partition` driven by a test-controlled
    /// [`Clock`], so a TTL-expiry property can advance time past a stamped
    /// expiry deterministically.
    #[cfg(test)]
    #[must_use]
    pub fn with_clock(partition: PartitionHandle, clock: Clock) -> Self {
        Self::from_parts(Inner::Bare(partition), clock)
    }

    /// Builds the production cache, taking ownership of the per-partition
    /// [`FjallWorkspace`].
    ///
    /// The cache operates the workspace's cache handle and holds the workspace
    /// alive, so the workspace's `Drop` — which deletes the fjall partition —
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

    /// The cache's `now` source, shared by reads (expiry checks) and the
    /// [`Cached`](crate::state::cached::Cached) cache's expiry stamping so the
    /// two never disagree.
    #[must_use]
    pub fn clock(&self) -> &Clock {
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
    ///
    /// # Errors
    ///
    /// Returns [`FjallCellCacheError`] when the cache read or cell decode
    /// fails.
    pub async fn get(
        &self,
        collection: &CollectionId,
        cell: &CellKey,
    ) -> Result<CacheRead, FjallCellCacheError> {
        let raw =
            cell_io::read_cell(self.inner.partition(), codec::cell_key(collection, cell)).await?;
        let (expiry, read) = codec::decode_cell(raw.as_deref())?;
        Ok(match read {
            Read::Unknown => CacheRead::Miss,
            _ if expired(expiry, self.clock.now_ms()) => CacheRead::Expired,
            Read::Present(payload) => CacheRead::Hit(Committed::new(Some(payload))),
            Read::Absent => CacheRead::Hit(Committed::new(None)),
        })
    }

    /// The absolute expiry (millis; `0` = never) stamped on the cell's current
    /// fjall entry, or `None` when no entry exists. Unlike [`get`](Self::get),
    /// it does **not** treat a passed stamp as a miss: the caller is about to
    /// **re-publish** the cell and wants to *preserve* its existing co-expiry
    /// anchor. The promote (`commit_provisional`) uses this so the committed
    /// value inherits the death set at stage time (`mark_resolved` does not
    /// re-stamp the durable TTL), rather than overhanging it with a fresh
    /// stamp.
    ///
    /// # Errors
    ///
    /// Returns [`FjallCellCacheError`] when the cache read or cell decode
    /// fails.
    pub async fn stored_expiry(
        &self,
        collection: &CollectionId,
        cell: &CellKey,
    ) -> Result<Option<u64>, FjallCellCacheError> {
        let raw =
            cell_io::read_cell(self.inner.partition(), codec::cell_key(collection, cell)).await?;
        let (expiry, read) = codec::decode_cell(raw.as_deref())?;
        Ok(match read {
            Read::Unknown => None,
            _ => Some(expiry),
        })
    }

    /// Write-through: publishes one cell's committed projection with an
    /// absolute `expiry` (`0` = never). A present value writes the payload
    /// cell; a known-absent value writes the `Absent` tag. The expiry
    /// mirrors the durable row's TTL death so the entry co-expires
    /// (FLOOR-rounded, so it never outlives the durable value).
    ///
    /// # Errors
    ///
    /// Returns [`FjallCellCacheError`] when the cache write fails.
    pub async fn put(
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
        let frame = match value.get() {
            Some(payload) => codec::encode_present_cell(payload, expiry),
            None => codec::encode_absent_cell(expiry),
        };
        cell_io::write_cell(
            self.inner.partition(),
            codec::cell_key(collection, cell),
            frame,
        )
        .await
    }

    /// Streams the committed cells of one `(collection, section)` whose
    /// coordinate falls in `[lo, hi]`, in `dir` order — the range-read the
    /// coverage cache serves a covered scan sub-range from.
    ///
    /// `Absent` (cleared) entries are skipped, so the stream yields only
    /// present cells, exactly as the lower store's `scan_cells` does for a
    /// covered range. An **expired** present cell is yielded as
    /// [`ScanHit::Expired`] so the covered serve can fall through and
    /// re-fetch it (FLOOR rounding makes an entry expire slightly before
    /// its durable row, so an expired covered coordinate must not read as
    /// absent — it is treated as a gap). The window is bounded by the
    /// covered interval, so it is collected in a single [`spawn_blocking`]
    /// (fjall's range iterator is synchronous and its guard is not held
    /// across an `.await`); each yielded item is then coop-wrapped
    /// so a large covered drain yields to the runtime.
    ///
    /// The fjall partition is shared across **all** collections, so the scan is
    /// bounded to the `(collection, section)` byte prefix on both ends — an
    /// unbounded `hi` stops at the section's upper boundary, never bleeding
    /// into the next section or another collection. A per-item prefix check
    /// guards the same invariant defensively.
    pub fn scan_present<'a>(
        &'a self,
        collection: &'a CollectionId,
        section: Section,
        lo: Bound<&'a Coordinate>,
        hi: Bound<&'a Coordinate>,
        dir: Direction,
    ) -> impl Stream<Item = Result<ScanHit, FjallCellCacheError>> + Send + 'a {
        let partition = self.inner.partition().clone();
        let section_prefix = codec::section_prefix(collection, section);
        let lo_bound = byte_low_bound(&section_prefix, lo);
        let hi_bound = byte_high_bound(&section_prefix, hi);
        let now = self.clock.now_ms();

        let collected = async move {
            spawn_blocking(move || {
                let mut window: Vec<ScanHit> = Vec::new();
                for kv in partition.range((lo_bound, hi_bound)) {
                    let (key, value) = kv?;
                    // Defensive: the byte bounds already confine the scan to the
                    // section, so this never trips — but it guarantees no other
                    // collection's cell can be served even if a bound is wrong.
                    if !key.starts_with(&section_prefix) {
                        break;
                    }
                    let (expiry, read) = codec::decode_cell(Some(value.as_ref()))?;
                    let cell = CellKey {
                        section,
                        coordinate: codec::coordinate_of(&key),
                    };
                    if expired(expiry, now) {
                        // The fjall entry expired before its durable row could
                        // have (FLOOR): the covered coordinate must fall through,
                        // not read as absent.
                        window.push(ScanHit::Expired(cell));
                    } else if let Read::Present(payload) = read {
                        window.push(ScanHit::Present(cell, payload));
                    }
                }
                Ok::<_, FjallCellCacheError>(window)
            })
            .await?
        };

        stream::once(collected)
            .map(move |result| match result {
                Ok(mut window) => {
                    if dir == Direction::Backward {
                        window.reverse();
                    }
                    stream::iter(window.into_iter().map(Ok)).left_stream()
                }
                Err(error) => stream::iter(iter::once(Err(error))).right_stream(),
            })
            .flatten()
            .then(|item| cooperative(async move { item }))
    }
}

/// The three-state result of a [`FjallCellCache::get`].
#[derive(Clone, Debug)]
pub enum CacheRead {
    /// An unexpired entry (a `Present` value or an authoritative `Absent`).
    Hit(Committed),
    /// An entry exists but its stamped expiry has passed; the caller falls
    /// through to the lower store and re-publishes a fresh entry.
    Expired,
    /// No entry exists (never written, or a gap within a covered scan range).
    Miss,
}

/// One item of a [`FjallCellCache::scan_present`] covered serve: a live present
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

/// Whether an absolute `expiry` (millis; `0` = never) has passed at `now`.
fn expired(expiry: u64, now: u64) -> bool {
    expiry != codec::NEVER_EXPIRES && now >= expiry
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
