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
//! # Three-valued reads
//!
//! Unlike the durable stores (Memory/Cassandra) whose `get` returns only
//! `Present`/`Absent`, the cache observes a third state: an entry that
//! has never been populated. That state is encoded as the **absence of an
//! entry** in the fjall partition, and decodes as
//! [`Read::Unknown`]. Tag byte `0x00` is
//! `Absent` (known cleared); tag byte `0x01` is `Present` with the
//! raw payload bytes that follow (stored verbatim — fjall block-compresses
//! the on-disk data block via LZ4, so there is no per-cell codec layer).
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
use tokio::task::coop::cooperative;
use tokio::task::spawn_blocking;

/// Fjall-backed cell cache.
#[derive(Clone, Educe)]
#[educe(Debug)]
pub struct FjallCellCache {
    #[educe(Debug(ignore))]
    inner: Arc<Inner>,
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
        Self {
            inner: Arc::new(Inner::Bare(partition)),
        }
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
        Self {
            inner: Arc::new(Inner::Owned(workspace)),
        }
    }

    /// Looks up one cell's committed value: `Some` on a `Present`/`Absent`
    /// cache hit, `None` on a removed/never-written miss (the caller falls
    /// through to the lower store).
    ///
    /// # Errors
    ///
    /// Returns [`FjallCellCacheError`] when the cache read or cell decode
    /// fails.
    pub async fn get(
        &self,
        collection: &CollectionId,
        cell: &CellKey,
    ) -> Result<Option<Committed>, FjallCellCacheError> {
        let raw =
            cell_io::read_cell(self.inner.partition(), codec::cell_key(collection, cell)).await?;
        Ok(match codec::decode_cell(raw.as_deref())? {
            Read::Present(payload) => Some(Committed::new(Some(payload))),
            Read::Absent => Some(Committed::new(None)),
            Read::Unknown => None,
        })
    }

    /// Write-through: patches one cell to a known-committed value. A present
    /// value writes the payload cell; a known-absent value writes the `Absent`
    /// tag.
    ///
    /// # Errors
    ///
    /// Returns [`FjallCellCacheError`] when the cache write fails.
    pub async fn put(
        &self,
        collection: &CollectionId,
        cell: &CellKey,
        value: &Committed,
    ) -> Result<(), FjallCellCacheError> {
        let frame = match value.get() {
            Some(payload) => codec::encode_present_cell(payload),
            None => codec::encode_absent_cell(),
        };
        cell_io::write_cell(
            self.inner.partition(),
            codec::cell_key(collection, cell),
            frame,
        )
        .await
    }

    /// Removes one cell so the next read decodes `Unknown` (a miss), unlike a
    /// `put` of a known-absent value, which writes an authoritative `Absent`
    /// cell.
    ///
    /// # Errors
    ///
    /// Returns [`FjallCellCacheError`] when the cache remove fails.
    pub async fn invalidate(
        &self,
        collection: &CollectionId,
        cell: &CellKey,
    ) -> Result<(), FjallCellCacheError> {
        cell_io::remove_cell(self.inner.partition(), codec::cell_key(collection, cell)).await
    }

    /// Streams the **present** committed cells of one `(collection, section)`
    /// whose coordinate falls in `[lo, hi]`, in `dir` order — the range-read
    /// the coverage cache serves a covered scan sub-range from.
    ///
    /// `Absent` (cleared) entries are skipped, so the stream yields only
    /// present committed bytes, exactly as the lower store's `scan_cells`
    /// does for a covered range. The window is bounded by the covered
    /// interval, so it is collected in a single [`spawn_blocking`] (fjall's
    /// range iterator is synchronous and its guard is not held across an
    /// `.await`); each yielded item is then coop-wrapped so a large covered
    /// drain yields to the runtime.
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
    ) -> impl Stream<Item = Result<(CellKey, Bytes), FjallCellCacheError>> + Send + 'a {
        let partition = self.inner.partition().clone();
        let section_prefix = codec::section_prefix(collection, section);
        let lo_bound = byte_low_bound(&section_prefix, lo);
        let hi_bound = byte_high_bound(&section_prefix, hi);

        let collected = async move {
            spawn_blocking(move || {
                let mut window: Vec<(CellKey, Bytes)> = Vec::new();
                for kv in partition.range((lo_bound, hi_bound)) {
                    let (key, value) = kv?;
                    // Defensive: the byte bounds already confine the scan to the
                    // section, so this never trips — but it guarantees no other
                    // collection's cell can be served even if a bound is wrong.
                    if !key.starts_with(&section_prefix) {
                        break;
                    }
                    if let Read::Present(payload) = codec::decode_cell(Some(value.as_ref()))? {
                        let cell = CellKey {
                            section,
                            coordinate: codec::coordinate_of(&key),
                        };
                        window.push((cell, payload));
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
