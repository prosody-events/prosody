//! The reader's shared infrastructure bundle.
//!
//! [`SharedDeps`] is the one named owner of every handle a cross-group reader
//! and the owning consumer share: the backend stores, the message loader, the
//! byte-budgeted read-through cache, the topic partition-count source, and the
//! heartbeat registry. Building one bundle and cloning its handles onward —
//! into several [`StateReader`](super::StateReader)s and into the consumer that
//! writes the state — is how the crate honors construct-once-clone-handles:
//! exactly one scylla session and one Kafka client back a whole process.
//!
//! Every field is a cheap-clone handle, so cloning the bundle clones handles,
//! never resources. [`SharedDeps::connect`] opens the Cassandra session,
//! prepares the reader's queries, and builds the Kafka loader; the registry is
//! built **first** because [`KafkaLoader::new`] registers its poll-loop
//! heartbeat at construction and must have somewhere to report.

use crate::cassandra::CassandraStore;
use crate::cassandra::config::CassandraConfiguration;
use crate::codec::Codec;
use crate::consumer::ConsumerConfiguration;
use crate::consumer::storage::SharedStorage;
use crate::heartbeat::HeartbeatRegistry;
use crate::loader::{KafkaLoader, MemoryLoader};
use crate::state::cassandra::{
    CassandraCellResources, CassandraDescriptorIdentityStore, CassandraPublicationStore,
    CellQueries, IdentityQueries, PublicationQueries,
};
use crate::state::first_write::PartitionCounts;
use crate::state::memory::{MemoryCells, MemoryDescriptorIdentityStore, MemoryPublicationStore};
use crate::state_reader::cache::ReaderCache;
use crate::state_reader::error::StateReaderError;
use crate::state_reader::loader::ReaderLoader;
use crate::state_reader::partitioner::PartitionCount;
use crate::state_reader::stores::ReaderStores;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::Duration;
use tokio::try_join;

#[cfg(test)]
use std::sync::atomic::{AtomicU64, Ordering};

/// Default reader read-through cache byte budget when neither
/// `read_cache_size_bytes` nor `cache_size_bytes` is configured.
pub(crate) const DEFAULT_READER_CACHE_SIZE_BYTES: NonZeroU64 = match NonZeroU64::new(1_048_576_u64)
{
    Some(budget) => budget,
    None => NonZeroU64::MIN,
};

/// A monotonic per-construction id, copied verbatim by [`SharedDeps::clone`]
/// and into every [`StateReader`](super::StateReader) it mints. Two readers
/// carry the same id iff they descend from the same real construction, so the
/// composition test can prove "exactly one bundle" without a racy global count.
#[cfg(test)]
static NEXT_DEPS_ID: AtomicU64 = AtomicU64::new(0);

#[cfg(test)]
fn next_instance_id() -> u64 {
    NEXT_DEPS_ID.fetch_add(1, Ordering::Relaxed)
}

/// Shared reader infrastructure: stores, loader, byte-budgeted cache, topic
/// partition counts, and the heartbeat registry. Clone shares the underlying
/// handles (see the module docs).
pub struct SharedDeps<C: Codec> {
    stores: ReaderStores,
    // `Arc` over an already-cheap-clone enum so this bundle's `Clone` needs no
    // `C::Payload: Clone` bound — `ReaderLoader<C>: Clone` would require it.
    loader: Arc<ReaderLoader<C>>,
    cache: ReaderCache,
    partition_counts: PartitionCounts,
    heartbeats: HeartbeatRegistry,
    /// Bundle-wide default read-cache TTL for readers whose descriptor does
    /// not set `.read_cache(...)`; the composing client feeds
    /// `KeyedStateConfiguration::read_cache_ttl` through here.
    default_read_cache_ttl: Option<Duration>,
    #[cfg(test)]
    instance_id: u64,
}

impl<C: Codec> Clone for SharedDeps<C> {
    fn clone(&self) -> Self {
        Self {
            stores: self.stores.clone(),
            loader: self.loader.clone(),
            cache: self.cache.clone(),
            partition_counts: self.partition_counts.clone(),
            heartbeats: self.heartbeats.clone(),
            default_read_cache_ttl: self.default_read_cache_ttl,
            #[cfg(test)]
            instance_id: self.instance_id,
        }
    }
}

impl<C: Codec> SharedDeps<C> {
    /// An in-memory bundle over the given shared stores and loader, with a
    /// wall-clock cache sized to `budget` declared bytes. The mock arm the
    /// consumer's mock mode composes readers from. `group_id`/`stall_threshold`
    /// seed the heartbeat registry — unused only from the reader's `is_stalled`
    /// view, since a mock has no loader poll loop — so the bundle's shape
    /// matches [`Self::connect`].
    #[must_use]
    pub fn memory(
        group_id: String,
        stall_threshold: Duration,
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
            partition_counts: PartitionCounts::Memory(PartitionCount::MOCK),
            heartbeats: HeartbeatRegistry::new(group_id, stall_threshold),
            default_read_cache_ttl: None,
            #[cfg(test)]
            instance_id: next_instance_id(),
        }
    }

    /// Opens a Cassandra-backed bundle: the heartbeat registry, one scylla
    /// session, the reader's prepared cell/identity/publication queries, and a
    /// Kafka message loader, with a read-through cache sized to
    /// `read_cache_size_bytes`.
    ///
    /// The registry is built first: [`KafkaLoader::new`] registers its
    /// poll-loop heartbeat at construction, so a loader built before the
    /// registry would have nowhere to report. The one session (`store`) is
    /// cloned into every store handle; the keyspace comes from
    /// `cassandra_config`, which the composition also feeds to the consumer's
    /// `StorePair`, so session and keyspace stay consistent.
    ///
    /// # Errors
    ///
    /// [`StateReaderError::Store`] if the session cannot open, any prepared
    /// query fails, or the Kafka loader cannot be created.
    pub async fn connect(
        consumer_config: &ConsumerConfiguration,
        cassandra_config: &CassandraConfiguration,
        read_cache_size_bytes: NonZeroU64,
    ) -> Result<Self, StateReaderError>
    where
        C::Payload: Clone,
    {
        let heartbeats = HeartbeatRegistry::new(
            consumer_config.group_id.clone(),
            consumer_config.stall_threshold,
        );
        let store = CassandraStore::new(cassandra_config)
            .await
            .map_err(|e| StateReaderError::store(&e))?;
        let keyspace = &cassandra_config.keyspace;

        // The three query sets each need only session + keyspace, so prepare
        // them concurrently rather than paying three serial round trips.
        let (cell_queries, identity_queries, publication_queries) = try_join!(
            CellQueries::new(store.session(), keyspace),
            IdentityQueries::new(store.session(), keyspace),
            PublicationQueries::new(store.session(), keyspace),
        )
        .map_err(|e| StateReaderError::store(&e))?;
        let cells = CassandraCellResources::new(store.clone(), Arc::new(cell_queries));
        let identities =
            CassandraDescriptorIdentityStore::new(store.clone(), Arc::new(identity_queries));
        let publications =
            CassandraPublicationStore::new(store.clone(), Arc::new(publication_queries));

        // Registers the loader's poll-loop heartbeat into the registry built
        // above — the reason the registry is constructed first.
        let loader = KafkaLoader::<C>::for_consumer(consumer_config, &heartbeats)
            .map_err(|e| StateReaderError::store(&e))?;

        Ok(Self {
            stores: ReaderStores::Cassandra {
                cells,
                publications,
                identities,
            },
            loader: Arc::new(ReaderLoader::Kafka(loader)),
            cache: ReaderCache::with_budget(read_cache_size_bytes.get()),
            partition_counts: PartitionCounts::Kafka {
                bootstrap: Arc::from(consumer_config.bootstrap_servers.clone()),
            },
            heartbeats,
            default_read_cache_ttl: None,
            #[cfg(test)]
            instance_id: next_instance_id(),
        })
    }

    /// Returns this bundle with `ttl` as its default read-cache TTL, applied
    /// to readers whose descriptor does not set `.read_cache(...)` explicitly
    /// (an explicit descriptor TTL always wins). The composing client feeds
    /// `KeyedStateConfiguration::read_cache_ttl` through here; `None` leaves
    /// descriptor-silent collections uncached.
    #[must_use]
    pub fn with_default_read_cache_ttl(mut self, ttl: Option<Duration>) -> Self {
        self.default_read_cache_ttl = ttl;
        self
    }

    /// A bundle over arbitrary reader stores, loader, and cache — the seam the
    /// scripted-fault suites build through. The partition-count and heartbeat
    /// fields are inert defaults (these suites never fetch counts or probe
    /// stalls).
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
            partition_counts: PartitionCounts::Memory(PartitionCount::MOCK),
            heartbeats: HeartbeatRegistry::new("reader-test".to_owned(), Duration::from_secs(30)),
            default_read_cache_ttl: None,
            instance_id: next_instance_id(),
        }
    }

    /// Whether any registered heartbeat has stalled. Folds over every heartbeat
    /// registered in this bundle's registry (for a reader-only process, the
    /// loader's poll loop).
    #[must_use]
    pub fn is_stalled(&self) -> bool {
        self.heartbeats.any_stalled()
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

    pub(crate) fn default_read_cache_ttl(&self) -> Option<Duration> {
        self.default_read_cache_ttl
    }

    /// The bundle's heartbeat registry, cloned into the consumer so its stall
    /// probe covers the shared loader's heartbeat exactly as a self-built
    /// loader's.
    pub(crate) fn heartbeats(&self) -> &HeartbeatRegistry {
        &self.heartbeats
    }

    /// The topic partition-count source (cheap-clone handle) the consumer's
    /// publication template uses.
    pub(crate) fn partition_counts(&self) -> PartitionCounts {
        self.partition_counts.clone()
    }

    /// The already-constructed shareable storage a consumer reuses instead of
    /// building its own.
    pub(crate) fn shared_storage(&self) -> SharedStorage {
        match &self.stores {
            ReaderStores::Cassandra {
                cells,
                publications,
                identities,
            } => SharedStorage::Cassandra {
                store: cells.session.clone(),
                cells: cells.clone(),
                identities: identities.clone(),
                publications: publications.clone(),
            },
            ReaderStores::Memory {
                cells,
                publications,
                identities,
            } => SharedStorage::Memory {
                cells: cells.clone(),
                identities: identities.clone(),
                publications: publications.clone(),
            },
            // The scripted-fault backend is never handed to a consumer; map it
            // to fresh in-memory stores to keep this total (no consumer path
            // observes them).
            #[cfg(test)]
            ReaderStores::Scripted { .. } => SharedStorage::Memory {
                cells: MemoryCells::new(),
                identities: MemoryDescriptorIdentityStore::new(),
                publications: MemoryPublicationStore::new(),
            },
        }
    }

    /// The bundle's Kafka loader, or `None` for a memory bundle. A `Clone`
    /// shares the client and poll thread. A Cassandra consumer arm handed a
    /// memory bundle rejects on this `None`.
    pub(crate) fn kafka_loader(&self) -> Option<KafkaLoader<C>>
    where
        C::Payload: Clone,
    {
        match &*self.loader {
            ReaderLoader::Kafka(loader) => Some(loader.clone()),
            ReaderLoader::Memory(_) => None,
        }
    }

    /// The bundle's in-memory loader, or `None` for a Kafka bundle.
    pub(crate) fn memory_loader(&self) -> Option<MemoryLoader<C::Payload>>
    where
        C::Payload: Clone,
    {
        match &*self.loader {
            ReaderLoader::Memory(loader) => Some(loader.clone()),
            ReaderLoader::Kafka(_) => None,
        }
    }

    /// The construction id shared by every clone and every reader minted from
    /// this bundle (see [`NEXT_DEPS_ID`]).
    #[cfg(test)]
    pub(crate) fn instance_id(&self) -> u64 {
        self.instance_id
    }
}
