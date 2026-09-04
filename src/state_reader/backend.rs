//! Concrete component families used by standalone readers.

use crate::codec::Codec;
use crate::consumer::middleware::deduplication::{
    CassandraDeduplicationStoreProvider, MemoryDeduplicationStoreProvider,
};
use crate::consumer::middleware::defer::message::store::{
    CassandraMessageDeferStoreProvider, MemoryMessageDeferStoreProvider,
};
use crate::consumer::middleware::defer::timer::store::{
    CassandraTimerDeferStoreProvider, MemoryTimerDeferStoreProvider,
};
use crate::consumer::storage::components::{cassandra, memory};
use crate::consumer::storage::{ComponentsOf, ConsumerStorageBackend, ConsumerStorageInputs};
use crate::consumer::{
    CassandraStateProvider, ConsumerError, KafkaObserver, KeyedStateInputs, MemoryStateProvider,
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
use crate::state::store::{CellBuffer, CoordinateBatch, PresenceBatch};
use crate::timers::store::cassandra::CassandraTriggerStoreProvider;
use crate::timers::store::memory::InMemoryTriggerStoreProvider;
use bytes::Bytes;
use futures::{Stream, StreamExt};
use std::convert::Infallible;
use std::error::Error;
use std::future::ready;
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

    /// Streams keys with a committed cell projection in `scan` order.
    /// This read does not consult the oracle or run owner-side repair.
    fn scan_presence<'a>(
        &'a self,
        id: &'a CollectionId,
        scan: Scan<'a>,
    ) -> impl Stream<Item = Result<CellKey, Self::Error>> + Send + 'a;

    /// Reads index-aligned committed presence values.
    /// This read does not consult the oracle or run owner-side repair.
    fn load_presence_many(
        &self,
        id: &CollectionId,
        section: Section,
        batch: &CoordinateBatch,
    ) -> impl Future<Output = Result<PresenceBatch, Self::Error>> + Send;
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

    fn scan_presence<'a>(
        &'a self,
        id: &'a CollectionId,
        scan: Scan<'a>,
    ) -> impl Stream<Item = Result<CellKey, Self::Error>> + Send + 'a {
        Self::scan_committed_keys(self, id, scan)
    }

    async fn load_presence_many(
        &self,
        id: &CollectionId,
        section: Section,
        batch: &CoordinateBatch,
    ) -> Result<PresenceBatch, Self::Error> {
        Self::read_committed_presence_many(self, id, section, batch).await
    }
}

impl CommittedCellSource for MemoryCells {
    type Error = Infallible;

    fn load(
        &self,
        id: &CollectionId,
        cell: &CellKey,
    ) -> impl Future<Output = Result<Option<Bytes>, Self::Error>> + Send {
        ready(Ok(Self::read_committed(self, id, cell)))
    }

    fn load_many(
        &self,
        id: &CollectionId,
        section: Section,
        batch: &CoordinateBatch,
    ) -> impl Future<Output = Result<CellBuffer<Option<Bytes>>, Self::Error>> + Send {
        ready(Ok(Self::read_committed_many(self, id, section, batch)))
    }

    fn scan<'a>(
        &'a self,
        id: &'a CollectionId,
        scan: Scan<'a>,
    ) -> impl Stream<Item = Result<(CellKey, Bytes), Self::Error>> + Send + 'a {
        Self::scan_committed(self, id, scan)
    }

    fn scan_presence<'a>(
        &'a self,
        id: &'a CollectionId,
        scan: Scan<'a>,
    ) -> impl Stream<Item = Result<CellKey, Self::Error>> + Send + 'a {
        Self::scan_committed(self, id, scan).map(|item| item.map(|(key, _)| key))
    }

    fn load_presence_many(
        &self,
        id: &CollectionId,
        section: Section,
        batch: &CoordinateBatch,
    ) -> impl Future<Output = Result<PresenceBatch, Self::Error>> + Send {
        ready(Ok(Self::read_committed_many(self, id, section, batch)
            .into_iter()
            .map(|value| value.is_some())
            .collect()))
    }
}

#[cfg(test)]
impl CommittedCellSource for ScriptedCellSource {
    type Error = StateAccessError;

    async fn load(&self, id: &CollectionId, cell: &CellKey) -> Result<Option<Bytes>, Self::Error> {
        let read = Self::read_committed(self, id, cell);
        // After the read, before the answer: a concurrency test can require
        // every reader to arrive here before any of them leaves.
        self.meet().await;
        read
    }

    fn load_many(
        &self,
        id: &CollectionId,
        section: Section,
        batch: &CoordinateBatch,
    ) -> impl Future<Output = Result<CellBuffer<Option<Bytes>>, Self::Error>> + Send {
        ready(Self::read_committed_many(self, id, section, batch))
    }

    fn scan<'a>(
        &'a self,
        id: &'a CollectionId,
        scan: Scan<'a>,
    ) -> impl Stream<Item = Result<(CellKey, Bytes), Self::Error>> + Send + 'a {
        Self::scan_committed(self, id, scan)
    }

    fn scan_presence<'a>(
        &'a self,
        id: &'a CollectionId,
        scan: Scan<'a>,
    ) -> impl Stream<Item = Result<CellKey, Self::Error>> + Send + 'a {
        Self::scan_committed(self, id, scan).map(|item| item.map(|(key, _)| key))
    }

    fn load_presence_many(
        &self,
        id: &CollectionId,
        section: Section,
        batch: &CoordinateBatch,
    ) -> impl Future<Output = Result<PresenceBatch, Self::Error>> + Send {
        ready(
            Self::read_committed_many(self, id, section, batch)
                .map(|values| values.into_iter().map(|value| value.is_some()).collect()),
        )
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

/// Reader family that can also supply a consumer's matching stores.
pub(crate) trait ConsumerReaderBackend<C>:
    ReaderBackend<C> + ConsumerStorageBackend<C>
where
    C: Codec,
{
}

impl<C, B> ConsumerReaderBackend<C> for B
where
    C: Codec,
    B: ReaderBackend<C> + ConsumerStorageBackend<C>,
{
}

/// One concrete reader component family.
#[derive(Clone)]
pub struct ReaderComponents<C, S, P, I, L> {
    cells: S,
    publications: P,
    identities: I,
    loader: L,
    codec: PhantomData<fn() -> C>,
}

impl<C, S, P, I, L> ReaderComponents<C, S, P, I, L> {
    pub(crate) fn new(cells: S, publications: P, identities: I, loader: L) -> Self {
        Self {
            cells,
            publications,
            identities,
            loader,
            codec: PhantomData,
        }
    }

    /// Returns the cell component for another shared client component.
    #[cfg(test)]
    pub(crate) const fn cells_ref(&self) -> &S {
        &self.cells
    }
}

impl<C, S, P, I, L> ReaderBackend<C> for ReaderComponents<C, S, P, I, L>
where
    C: Codec,
    S: CommittedCellSource,
    P: PublicationStore,
    I: DescriptorIdentityStore,
    L: MessageLoader<Payload = C::Payload> + 'static,
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
>;

/// In-memory stores and loader.
pub type MemoryReaderBackend<C> = ReaderComponents<
    C,
    MemoryCells,
    MemoryPublicationStore,
    MemoryDescriptorIdentityStore,
    MemoryLoader<<C as Codec>::Payload>,
>;

impl<C> ConsumerStorageBackend<C> for CassandraReaderBackend<C>
where
    C: Codec,
    C::Payload: crate::EventIdentity + crate::EventType + Clone + Send + Sync + 'static,
{
    type Dedup = CassandraDeduplicationStoreProvider;
    type EventLoader = KafkaLoader<C>;
    type Messages = CassandraMessageDeferStoreProvider;
    type State = CassandraStateProvider<C>;
    type Timers = CassandraTimerDeferStoreProvider;
    type Trigger = CassandraTriggerStoreProvider;

    async fn build_consumer_components(
        &self,
        inputs: ConsumerStorageInputs,
        keyed_state: &KeyedStateInputs,
        observer: KafkaObserver,
    ) -> Result<ComponentsOf<C, Self>, ConsumerError> {
        cassandra::<C>(
            inputs,
            keyed_state,
            self.cells().clone(),
            self.identities().clone(),
            self.publications().clone(),
            self.loader().clone(),
            observer,
        )
        .await
    }
}

impl<C> ConsumerStorageBackend<C> for MemoryReaderBackend<C>
where
    C: Codec,
    C::Payload: crate::EventIdentity + crate::EventType + Clone + Send + Sync + 'static,
{
    type Dedup = MemoryDeduplicationStoreProvider;
    type EventLoader = MemoryLoader<C::Payload>;
    type Messages = MemoryMessageDeferStoreProvider;
    type State = MemoryStateProvider<C::Payload>;
    type Timers = MemoryTimerDeferStoreProvider;
    type Trigger = InMemoryTriggerStoreProvider;

    async fn build_consumer_components(
        &self,
        inputs: ConsumerStorageInputs,
        keyed_state: &KeyedStateInputs,
        _observer: KafkaObserver,
    ) -> Result<ComponentsOf<C, Self>, ConsumerError> {
        memory::<C>(
            inputs,
            keyed_state,
            self.cells().clone(),
            self.publications().clone(),
            self.identities().clone(),
            self.loader().clone(),
        )
        .await
    }
}

#[cfg(test)]
pub(crate) type ScriptedReaderBackend = ReaderComponents<
    JsonCodec,
    ScriptedCellSource,
    ScriptedPublicationStore,
    CountingIdentityStore,
    MemoryLoader<serde_json::Value>,
>;
