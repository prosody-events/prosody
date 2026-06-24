//! Fjall-backed cell cache.
//!
//! [`FjallCellCache`] stores one tagged cell per [`CellKey`] in a fjall
//! partition. It is the write-through committed-value cache consumed by the
//! [`Cached`](crate::state::cached::Cached) combinator — a transitional
//! read-through optimization over the authoritative bottom store. It does
//! **not** implement [`CellStore`](crate::state::store::CellStore): it is a
//! concrete *partial* upper, so a bare cache view can never be mistaken for a
//! complete store.
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
use crate::state::cell_key::CellKey;
use crate::state::transaction::Read;
use educe::Educe;
use fjall::PartitionHandle;
use std::sync::Arc;

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
}
