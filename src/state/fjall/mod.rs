//! Fjall-backed Value cache.
//!
//! `FjallValueStore` implements [`ValueStore`] by storing a per-collection
//! cell in a fjall partition. It is wired as the **cache** half of
//! [`LayeredValueStore`](crate::state::layered::LayeredValueStore); the
//! dirty Value workspace remains the in-memory
//! [`MemoryDirtyValueStore`](crate::state::memory::MemoryDirtyValueStore).
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
//! Every fjall call is dispatched through [`tokio::task::spawn_blocking`]
//! because fjall's public API is synchronous. Each blocking closure
//! clones a cheap `Arc<Inner>` so the closure is `'static`.

mod codec;
mod config;
mod dirty;
mod error;
mod workspace;

#[cfg(test)]
mod tests;

pub use config::{FjallConfiguration, FjallConfigurationBuilder};
pub use dirty::{
    FjallDirtyValueStore, FjallDirtyValueStoreFactory, FjallDirtyValueStoreProvider,
    FjallFactoryError,
};
pub use error::FjallValueStoreError;
pub use workspace::{AssignmentEpoch, FjallClient, FjallClientError, FjallWorkspace};

use crate::state::value::{ValueKind, ValueStore};
use crate::state::{CollectionId, Read};
use bytes::Bytes;
use educe::Educe;
use fjall::{Config, Keyspace, PartitionCreateOptions, PartitionHandle};
use std::sync::Arc;
use tokio::task::spawn_blocking;

/// Fjall-backed Value cache store.
#[derive(Clone, Educe)]
#[educe(Debug)]
pub struct FjallValueStore {
    inner: Arc<Inner>,
}

#[derive(Educe)]
#[educe(Debug)]
struct Inner {
    // Held to keep the keyspace alive for the lifetime of the partition
    // handle; never accessed directly after construction.
    #[educe(Debug(ignore))]
    _keyspace: Keyspace,
    #[educe(Debug(ignore))]
    partition: PartitionHandle,
}

impl FjallValueStore {
    /// Opens the configured cache root and creates (or opens) the named
    /// partition for Value cache entries.
    ///
    /// Production deployments are expected to mount `config.cache_dir`
    /// before construction; this constructor does **not** create the root
    /// directory implicitly so a misconfigured mount fails fast instead
    /// of silently scribbling to disk.
    ///
    /// # Errors
    ///
    /// Returns [`FjallValueStoreError::Engine`] when the keyspace or
    /// partition cannot be opened.
    pub fn new(
        config: &FjallConfiguration,
        partition_name: &str,
    ) -> Result<Self, FjallValueStoreError> {
        let keyspace = Config::new(&config.cache_dir).open()?;
        let partition =
            keyspace.open_partition(partition_name, PartitionCreateOptions::default())?;
        Ok(Self {
            inner: Arc::new(Inner {
                _keyspace: keyspace,
                partition,
            }),
        })
    }

    /// Builds a cache store over `workspace`'s per-partition cache handle,
    /// sharing the workspace's already-open keyspace — fjall locks the
    /// cache directory, so opening it a second time would fail.
    #[must_use]
    pub fn with_workspace(workspace: &FjallWorkspace) -> Self {
        Self {
            inner: Arc::new(Inner {
                _keyspace: workspace.keyspace().as_ref().clone(),
                partition: workspace.cache_handle().clone(),
            }),
        }
    }

    /// Writes an encoded cache cell at `collection`'s key, dispatching the
    /// blocking fjall insert off the async runtime.
    async fn insert_cell(
        &self,
        collection: &CollectionId<ValueKind>,
        value: Bytes,
    ) -> Result<(), FjallValueStoreError> {
        let key = codec::value_cache_key(collection);
        let inner = Arc::clone(&self.inner);
        spawn_blocking(move || inner.partition.insert(key, value.as_ref())).await??;
        Ok(())
    }
}

#[cfg(test)]
impl FjallValueStore {
    /// Opens a fjall cache rooted at `dir`'s path under the partition
    /// name `"value_cache"`.
    ///
    /// # Errors
    ///
    /// Returns [`FjallValueStoreError::Engine`] when the keyspace cannot
    /// be opened.
    pub fn for_test(dir: &tempfile::TempDir) -> Result<Self, FjallValueStoreError> {
        let config = FjallConfiguration {
            cache_dir: dir.path().to_path_buf(),
        };
        Self::new(&config, "value_cache")
    }
}

impl ValueStore for FjallValueStore {
    type Error = FjallValueStoreError;

    async fn get<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
    ) -> Result<Read<Bytes>, Self::Error> {
        let key = codec::value_cache_key(collection);
        let inner = Arc::clone(&self.inner);
        let raw = spawn_blocking(move || inner.partition.get(key)).await??;
        codec::decode_cell(raw.as_deref())
    }

    async fn set<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
        payload: Bytes,
    ) -> Result<(), Self::Error> {
        self.insert_cell(collection, codec::encode_present_cell(&payload)?)
            .await
    }

    async fn clear<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
    ) -> Result<(), Self::Error> {
        self.insert_cell(collection, codec::encode_absent_cell())
            .await
    }
}
