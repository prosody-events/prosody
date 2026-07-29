//! Infrastructure shared by standalone readers and their owning consumer.

use crate::cassandra::CassandraStore;
use crate::cassandra::config::CassandraConfiguration;
use crate::codec::Codec;
use crate::consumer::ConsumerConfiguration;
use crate::consumer::storage::{StoreCreationError, StorePair, StorePairInputs};
use crate::heartbeat::HeartbeatRegistry;
use crate::high_level::config::TriggerStoreConfiguration;
use crate::loader::{KafkaLoader, MemoryLoader};
use crate::state::cassandra::{
    CassandraCellResources, CassandraDescriptorIdentityStore, CassandraPublicationStore,
    CellQueries, IdentityQueries, PublicationQueries,
};
use crate::state::memory::{MemoryCells, MemoryDescriptorIdentityStore, MemoryPublicationStore};
use crate::state_reader::backend::{
    CassandraReaderBackend, ConsumerReaderBackend, MemoryReaderBackend, ReaderBackend,
    ReaderComponents,
};
use crate::state_reader::cache::ReaderCache;
use crate::state_reader::error::StateReaderError;
use std::marker::PhantomData;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::Duration;
use tokio::try_join;

#[cfg(test)]
use std::sync::atomic::{AtomicU64, Ordering};

pub(crate) const DEFAULT_READER_CACHE_SIZE_BYTES: NonZeroU64 = match NonZeroU64::new(1_048_576) {
    Some(budget) => budget,
    None => NonZeroU64::MIN,
};

#[cfg(test)]
static NEXT_DEPS_ID: AtomicU64 = AtomicU64::new(0);

#[cfg(test)]
fn next_instance_id() -> u64 {
    NEXT_DEPS_ID.fetch_add(1, Ordering::Relaxed)
}

/// One backend family, cache, and heartbeat registry shared by every reader.
pub struct SharedDeps<C: Codec, B = MemoryReaderBackend<C>> {
    backend: Arc<B>,
    codec: PhantomData<fn() -> C>,
    cache: ReaderCache,
    heartbeats: HeartbeatRegistry,
    default_read_cache_ttl: Option<Duration>,
    #[cfg(test)]
    instance_id: u64,
}

impl<C, B> Clone for SharedDeps<C, B>
where
    C: Codec,
{
    fn clone(&self) -> Self {
        Self {
            backend: self.backend.clone(),
            codec: PhantomData,
            cache: self.cache.clone(),
            heartbeats: self.heartbeats.clone(),
            default_read_cache_ttl: self.default_read_cache_ttl,
            #[cfg(test)]
            instance_id: self.instance_id,
        }
    }
}

impl<C> SharedDeps<C, MemoryReaderBackend<C>>
where
    C: Codec,
    C::Payload: Clone,
{
    /// Builds an in-memory reader family.
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
        Self::build(
            ReaderComponents::new(cells, publications, identities, loader),
            ReaderCache::with_budget(budget),
            HeartbeatRegistry::new(group_id, stall_threshold),
        )
    }
}

impl<C> SharedDeps<C, CassandraReaderBackend<C>>
where
    C: Codec,
    C::Payload: Clone,
{
    /// Opens one Cassandra session and one Kafka loader.
    ///
    /// # Errors
    ///
    /// Returns a storage error when Cassandra preparation or Kafka loader
    /// construction fails.
    pub async fn connect(
        consumer: &ConsumerConfiguration,
        cassandra: &CassandraConfiguration,
        budget: NonZeroU64,
    ) -> Result<Self, StateReaderError> {
        let heartbeats =
            HeartbeatRegistry::new(consumer.group_id.clone(), consumer.stall_threshold);
        let store = CassandraStore::new(cassandra)
            .await
            .map_err(|error| StateReaderError::store(&error))?;
        let keyspace = &cassandra.keyspace;
        let (cells, identities, publications) = try_join!(
            CellQueries::new(store.session(), keyspace),
            IdentityQueries::new(store.session(), keyspace),
            PublicationQueries::new(store.session(), keyspace),
        )
        .map_err(|error| StateReaderError::store(&error))?;
        let loader = KafkaLoader::for_consumer(consumer, &heartbeats)
            .map_err(|error| StateReaderError::store(&error))?;
        let backend = ReaderComponents::new(
            CassandraCellResources::new(store.clone(), Arc::new(cells)),
            CassandraPublicationStore::new(store.clone(), Arc::new(publications)),
            CassandraDescriptorIdentityStore::new(store, Arc::new(identities)),
            loader,
        );
        Ok(Self::build(
            backend,
            ReaderCache::with_budget(budget.get()),
            heartbeats,
        ))
    }
}

impl<C, B> SharedDeps<C, B>
where
    C: Codec,
    B: ReaderBackend<C>,
{
    fn build(backend: B, cache: ReaderCache, heartbeats: HeartbeatRegistry) -> Self {
        Self {
            backend: Arc::new(backend),
            codec: PhantomData,
            cache,
            heartbeats,
            default_read_cache_ttl: None,
            #[cfg(test)]
            instance_id: next_instance_id(),
        }
    }

    #[cfg(test)]
    pub(crate) fn from_parts(backend: B, cache: ReaderCache) -> Self {
        Self::build(
            backend,
            cache,
            HeartbeatRegistry::new("reader-test".to_owned(), Duration::from_secs(30)),
        )
    }

    /// Sets the fallback cache TTL for reads without an explicit policy.
    #[must_use]
    pub fn with_default_read_cache_ttl(mut self, ttl: Option<Duration>) -> Self {
        self.default_read_cache_ttl = ttl;
        self
    }

    /// Reports whether the shared Kafka loader heartbeat is stalled.
    #[must_use]
    pub fn is_stalled(&self) -> bool {
        self.heartbeats.any_stalled()
    }

    pub(crate) fn backend(&self) -> &Arc<B> {
        &self.backend
    }

    pub(crate) fn cache(&self) -> &ReaderCache {
        &self.cache
    }

    pub(crate) fn default_read_cache_ttl(&self) -> Option<Duration> {
        self.default_read_cache_ttl
    }

    pub(crate) fn heartbeats(&self) -> &HeartbeatRegistry {
        &self.heartbeats
    }

    pub(crate) async fn build_store_pair(
        &self,
        config: &TriggerStoreConfiguration,
        inputs: StorePairInputs,
    ) -> Result<StorePair, StoreCreationError>
    where
        B: ConsumerReaderBackend<C>,
    {
        self.backend.build_store_pair(config, inputs).await
    }

    pub(crate) fn memory_loader(&self) -> Option<MemoryLoader<C::Payload>>
    where
        B: ConsumerReaderBackend<C>,
    {
        self.backend.memory_loader()
    }

    pub(crate) fn memory_cells(&self) -> Option<MemoryCells>
    where
        B: ConsumerReaderBackend<C>,
    {
        self.backend.memory_cells()
    }

    pub(crate) fn memory_identities(&self) -> Option<MemoryDescriptorIdentityStore>
    where
        B: ConsumerReaderBackend<C>,
    {
        self.backend.memory_identities()
    }

    pub(crate) fn kafka_loader(&self) -> Option<KafkaLoader<C>>
    where
        B: ConsumerReaderBackend<C>,
    {
        self.backend.kafka_loader()
    }

    #[cfg(test)]
    pub(crate) fn instance_id(&self) -> u64 {
        self.instance_id
    }
}
