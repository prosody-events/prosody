//! Compile-time storage choices for the high-level client.

use crate::cassandra::config::CassandraConfiguration;
use crate::codec::Codec;
use crate::consumer::{ConsumerConfiguration, KeyedStateConfiguration};
use crate::loader::MemoryLoader;
use crate::state::memory::{MemoryCells, MemoryDescriptorIdentityStore, MemoryPublicationStore};
use crate::state_reader::{
    CassandraReaderBackend, MemoryReaderBackend, ReaderBackend, SharedDeps, StateReaderError,
};
use std::marker::PhantomData;

mod sealed {
    pub trait Sealed {}
}

/// Binds the components of one built-in high-level storage choice.
///
/// This trait compresses the concrete client types. Downstream implementations
/// are not supported; use [`MemoryClientBackend`] or
/// [`CassandraClientBackend`].
pub trait ClientBackend<C>: sealed::Sealed + Clone + Send + Sync + 'static
where
    C: Codec,
{
    /// Reader components shared with the consumer.
    type Reader: ReaderBackend<C>;

    /// Builds the shared reader components.
    fn build_reader(
        &self,
        consumer: &ConsumerConfiguration,
        keyed_state: &KeyedStateConfiguration,
    ) -> impl Future<Output = Result<SharedDeps<C, Self::Reader>, StateReaderError>> + Send;
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

impl<C> sealed::Sealed for MemoryClientBackend<C> {}

impl<C> Default for MemoryClientBackend<C> {
    fn default() -> Self {
        Self::new()
    }
}

impl<C> ClientBackend<C> for MemoryClientBackend<C>
where
    C: Codec,
    C::Payload: Clone,
{
    type Reader = MemoryReaderBackend<C>;

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
            keyed_state.reader_cache_size().get(),
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

impl<C> sealed::Sealed for CassandraClientBackend<C> {}

impl<C> ClientBackend<C> for CassandraClientBackend<C>
where
    C: Codec,
    C::Payload: Clone,
{
    type Reader = CassandraReaderBackend<C>;

    async fn build_reader(
        &self,
        consumer: &ConsumerConfiguration,
        keyed_state: &KeyedStateConfiguration,
    ) -> Result<SharedDeps<C, Self::Reader>, StateReaderError> {
        Ok(
            SharedDeps::connect(consumer, &self.config, keyed_state.reader_cache_size())
                .await?
                .with_default_read_cache_ttl(keyed_state.read_cache_ttl),
        )
    }
}
