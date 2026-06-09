//! Fjall-backed Value cache.
//!
//! `FjallValueStore` implements [`ValueStore`] by storing a per-collection
//! cell in a fjall partition. It is wired as the **cache** half of
//! [`LayeredValueStore`](crate::state::layered::LayeredValueStore); the
//! dirty Value workspace is the Fjall-backed [`FjallDirtyValueStore`].
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
