//! Fjall-backed Value cache.
//!
//! `FjallValueStore` implements [`ValueStore`] by storing a per-collection
//! cell in a fjall partition. Slice 6 wires it as the **cache** half of
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

use crate::state::value::{StoredPayload, ValueKind, ValueStore};
use crate::state::{CollectionId, Read};
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
    ) -> Result<Read<StoredPayload>, Self::Error> {
        let key = codec::value_cache_key(collection);
        let inner = Arc::clone(&self.inner);
        let raw = spawn_blocking(move || inner.partition.get(key)).await??;
        codec::decode_cell(raw.as_deref())
    }

    async fn set<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
        payload: StoredPayload,
    ) -> Result<(), Self::Error> {
        let key = codec::value_cache_key(collection);
        let value = codec::encode_present_cell(&payload)?;
        let inner = Arc::clone(&self.inner);
        spawn_blocking(move || inner.partition.insert(key, value.as_ref())).await??;
        Ok(())
    }

    async fn clear<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
    ) -> Result<(), Self::Error> {
        let key = codec::value_cache_key(collection);
        let value = codec::encode_absent_cell();
        let inner = Arc::clone(&self.inner);
        spawn_blocking(move || inner.partition.insert(key, value.as_ref())).await??;
        Ok(())
    }
}
