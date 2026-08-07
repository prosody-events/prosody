//! Compile-time storage choices for the high-level client.

use crate::cassandra::config::CassandraConfiguration;
use crate::codec::Codec;
use crate::consumer::ConsumerError;
use crate::high_level::deps::ReaderConfiguration;
use crate::loader::MemoryLoader;
use crate::peer::{GrpcRouter, LocalRouter, PeerConfiguration, Router};
use crate::state::memory::{MemoryCells, MemoryDescriptorIdentityStore, MemoryPublicationStore};
use crate::state_reader::{
    CassandraReaderBackend, MemoryReaderBackend, ReaderBackend, StateReaderDependencies,
    StateReaderError,
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
    /// Peer route selected with this backend.
    type Router: Router;

    /// Builds the shared reader components.
    fn build_reader(
        &self,
        config: &ReaderConfiguration,
    ) -> impl Future<Output = Result<StateReaderDependencies<C, Self::Reader>, StateReaderError>> + Send;

    /// Builds the peer route over the shared reader backend.
    fn build_router(
        &self,
        config: &PeerConfiguration,
        reader: &StateReaderDependencies<C, Self::Reader>,
    ) -> impl Future<Output = Result<Self::Router, ConsumerError>> + Send;
}

/// In-memory high-level client backend.
///
/// The backend constructs one store family shared by readers and consumers.
pub struct MemoryClientBackend<C: Codec> {
    cells: MemoryCells,
    publications: MemoryPublicationStore,
    identities: MemoryDescriptorIdentityStore,
    loader: MemoryLoader<C::Payload>,
}

impl<C> MemoryClientBackend<C>
where
    C: Codec,
    C::Payload: Send + Sync + 'static,
{
    /// Selects in-memory storage.
    #[must_use]
    pub fn new() -> Self {
        Self {
            cells: MemoryCells::new(),
            publications: MemoryPublicationStore::new(),
            identities: MemoryDescriptorIdentityStore::new(),
            loader: MemoryLoader::new(),
        }
    }
}

impl<C: Codec> Clone for MemoryClientBackend<C> {
    fn clone(&self) -> Self {
        Self {
            cells: self.cells.clone(),
            publications: self.publications.clone(),
            identities: self.identities.clone(),
            loader: self.loader.clone(),
        }
    }
}

impl<C> Default for MemoryClientBackend<C>
where
    C: Codec,
    C::Payload: Send + Sync + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<C: Codec> sealed::Sealed for MemoryClientBackend<C> {}

impl<C> ClientBackend<C> for MemoryClientBackend<C>
where
    C: Codec,
    C::Payload: Clone,
{
    type Reader = MemoryReaderBackend<C>;
    type Router = LocalRouter;

    async fn build_reader(
        &self,
        config: &ReaderConfiguration,
    ) -> Result<StateReaderDependencies<C, Self::Reader>, StateReaderError> {
        Ok(StateReaderDependencies::memory(
            config.group_id.clone(),
            config.stall_threshold,
            self.cells.clone(),
            self.publications.clone(),
            self.identities.clone(),
            self.loader.clone(),
            config.cache_size,
        )
        .with_default_read_cache_ttl(config.cache_ttl))
    }

    async fn build_router(
        &self,
        config: &PeerConfiguration,
        _reader: &StateReaderDependencies<C, Self::Reader>,
    ) -> Result<Self::Router, ConsumerError> {
        LocalRouter::new(config).await
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
    type Router = GrpcRouter;

    async fn build_reader(
        &self,
        config: &ReaderConfiguration,
    ) -> Result<StateReaderDependencies<C, Self::Reader>, StateReaderError> {
        Ok(StateReaderDependencies::cassandra_with_loader(
            &self.config,
            config.loader.clone(),
            config.cache_size,
            config.stall_threshold,
        )
        .await?
        .with_default_read_cache_ttl(config.cache_ttl))
    }

    async fn build_router(
        &self,
        config: &PeerConfiguration,
        reader: &StateReaderDependencies<C, Self::Reader>,
    ) -> Result<Self::Router, ConsumerError> {
        GrpcRouter::start(config, reader.backend().as_ref()).await
    }
}
