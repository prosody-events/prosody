//! The reader's shared infrastructure bundle.
//!
//! The handles a [`StateReader`](super::StateReader) clones at construction:
//! the backend stores, the message loader, and the shared read-through cache.
//! Building one bundle and minting several readers with
//! [`StateReader::new`](super::StateReader::new) honors
//! construct-once-clone-handles. A later change folds a heavy `connect`
//! constructor (scylla session, heartbeat registry, prepared queries) onto
//! this type; until then callers open those handles once and hand them to
//! [`SharedDeps::cassandra`] or [`SharedDeps::memory`].

use crate::codec::Codec;
use crate::loader::{KafkaLoader, MemoryLoader};
use crate::state::cassandra::{
    CassandraCellResources, CassandraDescriptorIdentityStore, CassandraPublicationStore,
};
use crate::state::memory::{MemoryCells, MemoryDescriptorIdentityStore, MemoryPublicationStore};
use crate::state_reader::cache::ReaderCache;
use crate::state_reader::loader::ReaderLoader;
use crate::state_reader::stores::ReaderStores;
use std::sync::Arc;

/// Shared reader infrastructure: stores, loader, and the byte-budgeted cache.
/// Clone shares the underlying handles.
pub struct SharedDeps<C: Codec> {
    stores: ReaderStores,
    loader: Arc<ReaderLoader<C>>,
    cache: ReaderCache,
}

impl<C: Codec> Clone for SharedDeps<C> {
    fn clone(&self) -> Self {
        Self {
            stores: self.stores.clone(),
            loader: self.loader.clone(),
            cache: self.cache.clone(),
        }
    }
}

impl<C: Codec> SharedDeps<C> {
    /// An in-memory bundle over the given shared stores and loader, with a
    /// wall-clock cache sized to `budget` declared bytes. The mock arm the
    /// consumer's mock mode composes readers from.
    #[must_use]
    pub fn memory(
        cells: MemoryCells,
        publications: MemoryPublicationStore,
        identities: MemoryDescriptorIdentityStore,
        loader: MemoryLoader<C::Payload>,
        budget: u64,
    ) -> Self {
        Self {
            stores: ReaderStores::Memory {
                cells,
                publications,
                identities,
            },
            loader: Arc::new(ReaderLoader::Memory(loader)),
            cache: ReaderCache::with_budget(budget),
        }
    }

    /// A Cassandra-backed bundle over already-opened handles (the oracle-free
    /// cell resources, the publication and identity stores) and a Kafka message
    /// loader, with a wall-clock cache sized to `budget` declared bytes.
    ///
    /// Takes the concrete [`KafkaLoader`] and wraps it internally, mirroring
    /// [`Self::memory`]: pairing a Cassandra store bundle with a non-Kafka
    /// loader arm is then unrepresentable at the call site.
    ///
    /// Takes the handles rather than a connection config: the heavy `connect`
    /// (scylla session, prepared queries, heartbeat registry) is composed once
    /// by the owning process and its handles cloned in — the
    /// construct-once-clone-handles posture the shared bundle exists for.
    #[must_use]
    pub fn cassandra(
        cells: CassandraCellResources,
        publications: CassandraPublicationStore,
        identities: CassandraDescriptorIdentityStore,
        loader: KafkaLoader<C>,
        budget: u64,
    ) -> Self {
        Self {
            stores: ReaderStores::Cassandra {
                cells,
                publications,
                identities,
            },
            loader: Arc::new(ReaderLoader::Kafka(loader)),
            cache: ReaderCache::with_budget(budget),
        }
    }

    /// A bundle over arbitrary reader stores, loader, and cache — the seam the
    /// scripted-fault suites build through.
    #[cfg(test)]
    pub(crate) fn from_parts(
        stores: ReaderStores,
        loader: ReaderLoader<C>,
        cache: ReaderCache,
    ) -> Self {
        Self {
            stores,
            loader: Arc::new(loader),
            cache,
        }
    }

    pub(crate) fn stores(&self) -> &ReaderStores {
        &self.stores
    }

    pub(crate) fn loader(&self) -> &Arc<ReaderLoader<C>> {
        &self.loader
    }

    pub(crate) fn cache(&self) -> &ReaderCache {
        &self.cache
    }
}
