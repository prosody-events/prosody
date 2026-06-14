//! Fjall-backed Value cache.
//!
//! `FjallValueStore` stores a per-collection cell in a fjall partition and is
//! wired as the per-partition [`CommittedCache`] for the
//! [`PartitionStateStore`](crate::state::partition_store::PartitionStateStore);
//! the dirty Value workspace is the Fjall-backed [`FjallDirtyValueStore`].
//!
//! # Three-valued reads
//!
//! Unlike the durable stores (Memory/Cassandra) whose `get` returns only
//! `Present`/`Absent`, the cache observes a third state: an entry that
//! has never been populated. That state is encoded as the **absence of an
//! entry** in the fjall partition, and decodes as
//! [`Read::Unknown`]. Tag byte `0x00` is
//! `Absent` (known cleared); tag byte `0x01` is `Present` with the
//! encoded payload bytes that follow.
//!
//! # Blocking I/O
//!
//! fjall's public API is synchronous, so the cache's reads and writes are
//! dispatched through [`tokio::task::spawn_blocking`] (in the `cell_io`
//! submodule), which clones the cheap `Arc`-backed handle into each blocking
//! closure. The lone exception is the dirty store's synchronous
//! [`PendingOpSource`](crate::state::value::PendingOpSource) path: that trait
//! is synchronous, so its methods call fjall directly off the caller's thread
//! rather than through `spawn_blocking` (see the `dirty` submodule).

mod cell_io;
mod codec;
mod config;
mod dirty;
mod error;
mod workspace;

#[cfg(test)]
mod tests;

pub use config::FjallConfiguration;
pub use dirty::{FjallDirtyValueStore, FjallDirtyValueStoreProvider, FjallFactoryError};
pub use error::FjallValueStoreError;
pub use workspace::{AssignmentEpoch, FjallClient, FjallClientError, FjallWorkspace};

use crate::state::cell::Committed;
use crate::state::partition_store::CommittedCache;
use crate::state::value::{ValueKind, ValueStore};
use crate::state::{CollectionId, Read};
use bytes::Bytes;
use educe::Educe;
use fjall::PartitionHandle;
use std::sync::Arc;

/// Fjall-backed Value cache store.
#[derive(Clone, Educe)]
#[educe(Debug)]
pub struct FjallValueStore {
    inner: Arc<Inner>,
}

#[derive(Educe)]
#[educe(Debug)]
struct Inner {
    #[educe(Debug(ignore))]
    partition: PartitionHandle,
}

impl FjallValueStore {
    /// Builds a cache store over an opened cache `PartitionHandle`.
    ///
    /// The caller owns the keyspace the handle belongs to and is
    /// responsible for keeping it alive for the store's lifetime —
    /// production passes [`FjallWorkspace::cache_handle`], whose keyspace
    /// the per-process [`FjallClient`] holds open.
    #[must_use]
    pub fn new(partition: PartitionHandle) -> Self {
        Self {
            inner: Arc::new(Inner { partition }),
        }
    }
}

impl ValueStore for FjallValueStore {
    type Error = FjallValueStoreError;

    async fn get<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
    ) -> Result<Read<Bytes>, Self::Error> {
        let raw =
            cell_io::read_cell(&self.inner.partition, codec::collection_prefix(collection)).await?;
        codec::decode_cell(raw.as_deref())
    }

    async fn set<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
        payload: Bytes,
    ) -> Result<(), Self::Error> {
        cell_io::write_cell(
            &self.inner.partition,
            codec::collection_prefix(collection),
            codec::encode_present_cell(&payload)?,
        )
        .await
    }

    async fn clear<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
    ) -> Result<(), Self::Error> {
        cell_io::write_cell(
            &self.inner.partition,
            codec::collection_prefix(collection),
            codec::encode_absent_cell(),
        )
        .await
    }
}

/// Committed-value cache over the fjall cache partition.
///
/// The three-valued cell read maps onto the cache lookup: a `Present`/`Absent`
/// cell is a committed-value hit (`Some`), a removed/never-written cell is a
/// miss (`None`). Patching to a present committed value writes the payload
/// cell; patching to a known-absent committed value writes the `Absent` tag.
impl CommittedCache<ValueKind> for FjallValueStore {
    type Error = FjallValueStoreError;

    async fn get<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
        (): &'a (),
    ) -> Result<Option<Committed>, Self::Error> {
        Ok(match ValueStore::get(self, collection).await? {
            Read::Present(payload) => Some(Committed::new(Some(payload))),
            Read::Absent => Some(Committed::new(None)),
            Read::Unknown => None,
        })
    }

    async fn put<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
        (): &'a (),
        value: &'a Committed,
    ) -> Result<(), Self::Error> {
        match value.get() {
            Some(payload) => ValueStore::set(self, collection, payload.clone()).await,
            None => ValueStore::clear(self, collection).await,
        }
    }

    async fn invalidate<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
        (): &'a (),
    ) -> Result<(), Self::Error> {
        // True invalidation: removes the cell so the next read decodes
        // `Read::Unknown` (a miss), unlike `ValueStore::clear`, which writes
        // an authoritative `Absent` cell.
        cell_io::remove_cell(&self.inner.partition, codec::collection_prefix(collection)).await
    }
}
