//! Concrete component families used by standalone readers.

use crate::cassandra::config::CassandraConfiguration;
use crate::codec::Codec;
use crate::consumer::storage::{
    MemoryArmInputs, StatefulStorePair, StoreCreationError, StorePair, StorePairInputs,
};
use crate::error::ClassifyError;
use crate::loader::{KafkaLoader, MemoryLoader, MessageLoader};
use crate::state::cassandra::{
    CassandraCellResources, CassandraCellStoreError, CassandraDescriptorIdentityStore,
    CassandraPublicationStore,
};
use crate::state::cell_key::{CellKey, Scan, Section};
use crate::state::descriptor_identity::DescriptorIdentityStore;
use crate::state::identity::CollectionId;
use crate::state::memory::{MemoryCells, MemoryDescriptorIdentityStore, MemoryPublicationStore};
use crate::state::publication::PublicationStore;
use crate::state::store::{CellBuffer, CoordinateBatch};
use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;
use std::convert::Infallible;
use std::error::Error;
use std::marker::PhantomData;

#[cfg(test)]
use crate::codec::JsonCodec;
#[cfg(test)]
use crate::state::access::StateAccessError;
#[cfg(test)]
use crate::state::tests::support::ScriptedPublicationStore;
#[cfg(test)]
use crate::state_reader::tests::support::{CountingIdentityStore, ScriptedCellSource};

/// Committed cell reads that never consult the owner's commit oracle.
pub trait CommittedCellSource: Clone + Send + Sync + 'static {
    /// Read failure.
    type Error: ClassifyError + Error + Send + Sync + 'static;

    /// Reads one committed cell.
    fn load(
        &self,
        id: &CollectionId,
        cell: &CellKey,
    ) -> impl Future<Output = Result<Option<Bytes>, Self::Error>> + Send;

    /// Reads an index-aligned batch of committed cells.
    fn load_many(
        &self,
        id: &CollectionId,
        section: Section,
        batch: &CoordinateBatch,
    ) -> impl Future<Output = Result<CellBuffer<Option<Bytes>>, Self::Error>> + Send;

    /// Streams committed cells in `scan` order.
    fn scan<'a>(
        &'a self,
        id: &'a CollectionId,
        scan: Scan<'a>,
    ) -> impl Stream<Item = Result<(CellKey, Bytes), Self::Error>> + Send + 'a;
}

impl CommittedCellSource for CassandraCellResources {
    type Error = CassandraCellStoreError;

    async fn load(&self, id: &CollectionId, cell: &CellKey) -> Result<Option<Bytes>, Self::Error> {
        Self::read_committed(self, id, cell).await
    }

    async fn load_many(
        &self,
        id: &CollectionId,
        section: Section,
        batch: &CoordinateBatch,
    ) -> Result<CellBuffer<Option<Bytes>>, Self::Error> {
        Self::read_committed_many(self, id, section, batch).await
    }

    fn scan<'a>(
        &'a self,
        id: &'a CollectionId,
        scan: Scan<'a>,
    ) -> impl Stream<Item = Result<(CellKey, Bytes), Self::Error>> + Send + 'a {
        Self::scan_committed(self, id, scan)
    }
}

impl CommittedCellSource for MemoryCells {
    type Error = Infallible;

    async fn load(&self, id: &CollectionId, cell: &CellKey) -> Result<Option<Bytes>, Self::Error> {
        Ok(Self::read_committed(self, id, cell))
    }

    async fn load_many(
        &self,
        id: &CollectionId,
        section: Section,
        batch: &CoordinateBatch,
    ) -> Result<CellBuffer<Option<Bytes>>, Self::Error> {
        Ok(Self::read_committed_many(self, id, section, batch))
    }

    fn scan<'a>(
        &'a self,
        id: &'a CollectionId,
        scan: Scan<'a>,
    ) -> impl Stream<Item = Result<(CellKey, Bytes), Self::Error>> + Send + 'a {
        Self::scan_committed(self, id, scan)
    }
}

#[cfg(test)]
impl CommittedCellSource for ScriptedCellSource {
    type Error = StateAccessError;

    async fn load(&self, id: &CollectionId, cell: &CellKey) -> Result<Option<Bytes>, Self::Error> {
        Self::read_committed(self, id, cell)
    }

    async fn load_many(
        &self,
        id: &CollectionId,
        section: Section,
        batch: &CoordinateBatch,
    ) -> Result<CellBuffer<Option<Bytes>>, Self::Error> {
        Self::read_committed_many(self, id, section, batch)
    }

    fn scan<'a>(
        &'a self,
        id: &'a CollectionId,
        scan: Scan<'a>,
    ) -> impl Stream<Item = Result<(CellKey, Bytes), Self::Error>> + Send + 'a {
        Self::scan_committed(self, id, scan)
    }
}

/// The cells, routing rows, identities, and loader of one reader composition.
pub trait ReaderBackend<C>: Send + Sync + 'static
where
    C: Codec,
{
    /// Committed cell source.
    type Cells: CommittedCellSource;
    /// Routing-row store.
    type Publications: PublicationStore;
    /// Frozen descriptor-identity store.
    type Identities: DescriptorIdentityStore;
    /// Message loader used by message-backed collections.
    type Loader: MessageLoader<Payload = C::Payload>;

    /// Committed cell source.
    fn cells(&self) -> &Self::Cells;
    /// Routing-row store.
    fn publications(&self) -> &Self::Publications;
    /// Descriptor-identity store.
    fn identities(&self) -> &Self::Identities;
    /// Message loader.
    fn loader(&self) -> &Self::Loader;
}

mod consumer {
    use super::{Codec, ReaderBackend, StatefulStorePair, StoreCreationError, StorePairInputs};

    #[async_trait::async_trait]
    pub(crate) trait Backend<C>: ReaderBackend<C>
    where
        C: Codec,
    {
        async fn build_store_pair(
            &self,
            inputs: StorePairInputs,
        ) -> Result<StatefulStorePair<C>, StoreCreationError>;
    }
}

/// Reader family that can also supply a consumer's matching stores.
pub(crate) trait ConsumerReaderBackend<C>: ReaderBackend<C> + consumer::Backend<C>
where
    C: Codec,
{
}

impl<C, B> ConsumerReaderBackend<C> for B
where
    C: Codec,
    B: ReaderBackend<C> + consumer::Backend<C>,
{
}

/// One concrete reader component family.
#[derive(Clone)]
pub struct ReaderComponents<C, S, P, I, L, R = ()> {
    cells: S,
    publications: P,
    identities: I,
    loader: L,
    consumer: R,
    codec: PhantomData<fn() -> C>,
}

impl<C, S, P, I, L, R> ReaderComponents<C, S, P, I, L, R> {
    pub(crate) fn new(cells: S, publications: P, identities: I, loader: L, consumer: R) -> Self {
        Self {
            cells,
            publications,
            identities,
            loader,
            consumer,
            codec: PhantomData,
        }
    }
}

impl<C, S, P, I, L, R> ReaderBackend<C> for ReaderComponents<C, S, P, I, L, R>
where
    C: Codec,
    S: CommittedCellSource,
    P: PublicationStore,
    I: DescriptorIdentityStore,
    L: MessageLoader<Payload = C::Payload> + 'static,
    R: Send + Sync + 'static,
{
    type Cells = S;
    type Identities = I;
    type Loader = L;
    type Publications = P;

    fn cells(&self) -> &S {
        &self.cells
    }

    fn publications(&self) -> &P {
        &self.publications
    }

    fn identities(&self) -> &I {
        &self.identities
    }

    fn loader(&self) -> &L {
        &self.loader
    }
}

/// Cassandra stores with a Kafka loader.
pub type CassandraReaderBackend<C> = ReaderComponents<
    C,
    CassandraCellResources,
    CassandraPublicationStore,
    CassandraDescriptorIdentityStore,
    KafkaLoader<C>,
    CassandraConfiguration,
>;

/// In-memory stores and loader.
pub type MemoryReaderBackend<C> = ReaderComponents<
    C,
    MemoryCells,
    MemoryPublicationStore,
    MemoryDescriptorIdentityStore,
    MemoryLoader<<C as Codec>::Payload>,
    (),
>;

#[async_trait]
impl<C> consumer::Backend<C> for CassandraReaderBackend<C>
where
    C: Codec,
    C::Payload: Clone,
{
    async fn build_store_pair(
        &self,
        inputs: StorePairInputs,
    ) -> Result<StatefulStorePair<C>, StoreCreationError> {
        StorePair::cassandra_with(
            &self.consumer,
            inputs,
            self.cells().session.clone(),
            self.cells().clone(),
            self.identities().clone(),
            self.publications().clone(),
            self.loader().clone(),
        )
        .await
    }
}

#[async_trait]
impl<C> consumer::Backend<C> for MemoryReaderBackend<C>
where
    C: Codec,
    C::Payload: Clone,
{
    async fn build_store_pair(
        &self,
        inputs: StorePairInputs,
    ) -> Result<StatefulStorePair<C>, StoreCreationError> {
        StorePair::memory_with(
            inputs.dedup_ttl,
            inputs.timer_spans,
            self.publications().clone(),
            MemoryArmInputs {
                loader: self.loader().clone(),
                cells: self.cells().clone(),
                identities: self.identities().clone(),
            },
        )
    }
}

#[cfg(test)]
pub(crate) type ScriptedReaderBackend = ReaderComponents<
    JsonCodec,
    ScriptedCellSource,
    ScriptedPublicationStore,
    CountingIdentityStore,
    MemoryLoader<serde_json::Value>,
    (),
>;
