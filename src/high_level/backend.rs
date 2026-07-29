//! Compile-time storage choices for the high-level client.

use crate::cassandra::config::CassandraConfiguration;
use crate::codec::Codec;
use crate::consumer::{ConsumerConfiguration, KeyedStateConfiguration};
use crate::high_level::config::TriggerStoreConfiguration;
use crate::loader::MemoryLoader;
use crate::state::memory::{MemoryCells, MemoryDescriptorIdentityStore, MemoryPublicationStore};
use crate::state_reader::DEFAULT_READER_CACHE_SIZE_BYTES;
use crate::state_reader::{
    CassandraReaderBackend, MemoryReaderBackend, ReaderBackend, SharedDeps, StateReaderError,
};
use async_trait::async_trait;
use std::marker::PhantomData;
use std::num::NonZeroU64;

/// Builds the reader family matching one high-level storage choice.
#[async_trait]
pub trait ClientBackend<C>: Clone + Send + Sync + 'static
where
    C: Codec,
{
    /// Reader components shared with the consumer.
    type Reader: ReaderBackend<C>;

    /// Matching low-level trigger-store configuration.
    fn trigger_store(&self) -> TriggerStoreConfiguration;

    /// Builds the shared reader components.
    async fn build_reader(
        &self,
        consumer: &ConsumerConfiguration,
        keyed_state: &KeyedStateConfiguration,
    ) -> Result<SharedDeps<C, Self::Reader>, StateReaderError>;
}

/// In-memory high-level client backend.
pub struct MemoryClientBackend<C>(PhantomData<fn() -> C>);

impl<C> MemoryClientBackend<C> {
    /// Selects in-memory storage.
    #[must_use]
    pub const fn new() -> Self {
        Self(PhantomData)
    }
}

impl<C> Clone for MemoryClientBackend<C> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<C> Copy for MemoryClientBackend<C> {}

impl<C> Default for MemoryClientBackend<C> {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl<C> ClientBackend<C> for MemoryClientBackend<C>
where
    C: Codec,
    C::Payload: Clone,
{
    type Reader = MemoryReaderBackend<C>;

    fn trigger_store(&self) -> TriggerStoreConfiguration {
        TriggerStoreConfiguration::InMemory
    }

    async fn build_reader(
        &self,
        consumer: &ConsumerConfiguration,
        keyed_state: &KeyedStateConfiguration,
    ) -> Result<SharedDeps<C, Self::Reader>, StateReaderError> {
        Ok(SharedDeps::memory(
            consumer.group_id.clone(),
            consumer.stall_threshold,
            MemoryCells::new(),
            MemoryPublicationStore::new(),
            MemoryDescriptorIdentityStore::new(),
            MemoryLoader::new(),
            reader_cache_size(keyed_state).get(),
        )
        .with_default_read_cache_ttl(keyed_state.read_cache_ttl))
    }
}

/// Cassandra high-level client backend.
pub struct CassandraClientBackend<C> {
    config: CassandraConfiguration,
    codec: PhantomData<fn() -> C>,
}

impl<C> CassandraClientBackend<C> {
    /// Selects Cassandra storage.
    #[must_use]
    pub fn new(config: CassandraConfiguration) -> Self {
        Self {
            config,
            codec: PhantomData,
        }
    }
}

impl<C> Clone for CassandraClientBackend<C> {
    fn clone(&self) -> Self {
        Self::new(self.config.clone())
    }
}

#[async_trait]
impl<C> ClientBackend<C> for CassandraClientBackend<C>
where
    C: Codec,
    C::Payload: Clone,
{
    type Reader = CassandraReaderBackend<C>;

    fn trigger_store(&self) -> TriggerStoreConfiguration {
        TriggerStoreConfiguration::Cassandra(self.config.clone())
    }

    async fn build_reader(
        &self,
        consumer: &ConsumerConfiguration,
        keyed_state: &KeyedStateConfiguration,
    ) -> Result<SharedDeps<C, Self::Reader>, StateReaderError> {
        Ok(
            SharedDeps::connect(consumer, &self.config, reader_cache_size(keyed_state))
                .await?
                .with_default_read_cache_ttl(keyed_state.read_cache_ttl),
        )
    }
}

fn reader_cache_size(config: &KeyedStateConfiguration) -> NonZeroU64 {
    config
        .read_cache_size_bytes
        .or(config.cache_size_bytes)
        .unwrap_or(DEFAULT_READER_CACHE_SIZE_BYTES)
}
