//! Concrete operational components for one monomorphized consumer backend.

use super::{ConsumerStorageInputs, StoreCreationError, dedup_ttl_seconds};
use crate::Codec;
use crate::consumer::error::ConsumerError;
use crate::consumer::middleware::deduplication::DeduplicationStoreProvider;
use crate::consumer::middleware::deduplication::cassandra::CassandraDeduplicationStoreProvider;
use crate::consumer::middleware::deduplication::memory::MemoryDeduplicationStoreProvider;
use crate::consumer::middleware::deduplication::queries::DeduplicationQueries;
use crate::consumer::middleware::defer::message::store::MessageDeferStoreProvider;
use crate::consumer::middleware::defer::message::store::cassandra::MessageQueries;
use crate::consumer::middleware::defer::message::store::{
    CassandraMessageDeferStoreProvider, MemoryMessageDeferStoreProvider,
};
use crate::consumer::middleware::defer::segment::CassandraSegmentStore;
use crate::consumer::middleware::defer::timer::store::TimerDeferStoreProvider;
use crate::consumer::middleware::defer::timer::store::cassandra::queries::Queries as TimerQueries;
use crate::consumer::middleware::defer::timer::store::{
    CassandraTimerDeferStoreProvider, MemoryTimerDeferStoreProvider,
};
use crate::consumer::observer::KafkaObserver;
use crate::consumer::wiring::state::{
    CassandraStateProvider, KeyedStateInputs, MemoryStateProvider, cassandra_state_provider,
    memory_state_provider,
};
use crate::loader::MessageLoader;
use crate::loader::{KafkaLoader, MemoryLoader};
use crate::state::cassandra::{
    CassandraCellResources, CassandraDescriptorIdentityStore, CassandraPublicationStore,
};
use crate::state::manager::{PartitionStateManager, PartitionStateProvider};
use crate::state::memory::{MemoryCells, MemoryDescriptorIdentityStore, MemoryPublicationStore};
use crate::state::session::EventSession;
use crate::timers::store::TriggerStoreProvider;
use crate::timers::store::cassandra::CassandraTriggerStoreProvider;
use crate::timers::store::memory::InMemoryTriggerStoreProvider;
use std::sync::Arc;
use tracing::debug;

/// Providers and state wiring selected by one concrete backend.
pub(crate) struct ConsumerComponents<T, M, R, D, S, L> {
    pub(crate) trigger: T,
    pub(crate) messages: M,
    pub(crate) timers: R,
    pub(crate) dedup: D,
    pub(crate) state: S,
    pub(crate) loader: L,
}

pub(crate) type ComponentsOf<C, B> = ConsumerComponents<
    <B as ConsumerStorageBackend<C>>::Trigger,
    <B as ConsumerStorageBackend<C>>::Messages,
    <B as ConsumerStorageBackend<C>>::Timers,
    <B as ConsumerStorageBackend<C>>::Dedup,
    <B as ConsumerStorageBackend<C>>::State,
    <B as ConsumerStorageBackend<C>>::EventLoader,
>;

/// Builds one concrete operational component family.
pub(crate) trait ConsumerStorageBackend<C>: Send + Sync + Sized
where
    C: Codec,
    <<Self::State as PartitionStateProvider<
        <Self::Trigger as TriggerStoreProvider>::Store,
    >>::Manager as PartitionStateManager>::Session: EventSession<Loader = Self::EventLoader>,
{
    type Trigger: TriggerStoreProvider;
    type Messages: MessageDeferStoreProvider;
    type Timers: TimerDeferStoreProvider;
    type Dedup: DeduplicationStoreProvider;
    type State: PartitionStateProvider<<Self::Trigger as TriggerStoreProvider>::Store>;
    type EventLoader: MessageLoader<Payload = C::Payload> + 'static;

    fn build_consumer_components(
        &self,
        inputs: ConsumerStorageInputs,
        keyed_state: &KeyedStateInputs,
        observer: KafkaObserver,
    ) -> impl Future<Output = Result<ComponentsOf<C, Self>, ConsumerError>> + Send;
}

pub(crate) async fn memory<C>(
    inputs: ConsumerStorageInputs,
    keyed_state: &KeyedStateInputs,
    cells: MemoryCells,
    publications: MemoryPublicationStore,
    identities: MemoryDescriptorIdentityStore,
    loader: MemoryLoader<C::Payload>,
) -> Result<
    ConsumerComponents<
        InMemoryTriggerStoreProvider,
        MemoryMessageDeferStoreProvider,
        MemoryTimerDeferStoreProvider,
        MemoryDeduplicationStoreProvider,
        MemoryStateProvider<C::Payload>,
        MemoryLoader<C::Payload>,
    >,
    ConsumerError,
>
where
    C: Codec,
    C::Payload: crate::EventIdentity + crate::EventType + Clone + Send + Sync + 'static,
{
    dedup_ttl_seconds(inputs.dedup_ttl)?;
    let dedup = MemoryDeduplicationStoreProvider::new();
    let publisher = keyed_state.memory_publication_setup(publications)?;
    let state = memory_state_provider::<C>(
        keyed_state,
        dedup.clone(),
        cells,
        identities,
        loader.clone(),
        publisher,
    );
    Ok(ConsumerComponents {
        trigger: InMemoryTriggerStoreProvider::new(),
        messages: MemoryMessageDeferStoreProvider::new(),
        timers: MemoryTimerDeferStoreProvider::with_linking(inputs.timer_spans),
        dedup,
        state,
        loader,
    })
}

pub(crate) async fn cassandra<C>(
    inputs: ConsumerStorageInputs,
    keyed_state: &KeyedStateInputs,
    cells: CassandraCellResources,
    identities: CassandraDescriptorIdentityStore,
    publications: CassandraPublicationStore,
    loader: KafkaLoader<C>,
    observer: KafkaObserver,
) -> Result<
    ConsumerComponents<
        CassandraTriggerStoreProvider,
        CassandraMessageDeferStoreProvider,
        CassandraTimerDeferStoreProvider,
        CassandraDeduplicationStoreProvider,
        CassandraStateProvider<C>,
        KafkaLoader<C>,
    >,
    ConsumerError,
>
where
    C: Codec,
    C::Payload: crate::EventIdentity + crate::EventType + Clone + Send + Sync + 'static,
{
    let store = cells.session.clone();
    let keyspace = store.keyspace();
    let trigger = CassandraTriggerStoreProvider::with_store(store.clone(), keyspace).await?;
    let segment = CassandraSegmentStore::new(store.clone(), keyspace)
        .await
        .map_err(StoreCreationError::from)?;
    let messages = CassandraMessageDeferStoreProvider::new(
        store.clone(),
        Arc::new(
            MessageQueries::new(store.session(), keyspace)
                .await
                .map_err(StoreCreationError::from)?,
        ),
        segment.clone(),
    );
    let timers = CassandraTimerDeferStoreProvider::new(
        store.clone(),
        Arc::new(
            TimerQueries::new(store.session(), keyspace)
                .await
                .map_err(StoreCreationError::from)?,
        ),
        segment,
        inputs.timer_spans,
    );
    let ttl = dedup_ttl_seconds(inputs.dedup_ttl)?;
    debug!(ttl_secs = ttl, "deduplication store TTL");
    let dedup = CassandraDeduplicationStoreProvider::new(
        store.clone(),
        Arc::new(
            DeduplicationQueries::new(store.session(), keyspace)
                .await
                .map_err(StoreCreationError::from)?,
        ),
        ttl,
        inputs.dedup_cache_capacity,
    );
    let publisher = keyed_state.cassandra_publication_setup(publications, observer);
    let state = cassandra_state_provider::<C>(
        keyed_state,
        dedup.clone(),
        cells,
        identities,
        loader.clone(),
        publisher,
    )?;
    Ok(ConsumerComponents {
        trigger,
        messages,
        timers,
        dedup,
        state,
        loader,
    })
}
