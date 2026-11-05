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
use futures::TryFutureExt;
use std::sync::Arc;
use tokio::try_join;
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
    let ttl = dedup_ttl_seconds(inputs.dedup_ttl)?;
    debug!(ttl_secs = ttl, "deduplication store TTL");

    let store = cells.session.clone();
    let keyspace = store.keyspace();
    let session = store.session();
    let (trigger, segment, message_queries, timer_queries, dedup_queries, publisher, cache) = try_join!(
        CassandraTriggerStoreProvider::with_store(store.clone(), keyspace).err_into(),
        CassandraSegmentStore::new(store.clone(), keyspace).map_err(creation_error),
        MessageQueries::new(session, keyspace).map_err(creation_error),
        TimerQueries::new(session, keyspace).map_err(creation_error),
        DeduplicationQueries::new(session, keyspace).map_err(creation_error),
        keyed_state.cassandra_publication_setup(publications),
        keyed_state.open_cache(),
    )?;

    let messages = CassandraMessageDeferStoreProvider::new(
        store.clone(),
        Arc::new(message_queries),
        segment.clone(),
    );
    let timers = CassandraTimerDeferStoreProvider::new(
        store.clone(),
        Arc::new(timer_queries),
        segment,
        inputs.timer_spans,
    );
    let dedup = CassandraDeduplicationStoreProvider::new(
        store,
        Arc::new(dedup_queries),
        ttl,
        inputs.dedup_cache_capacity,
    );
    let state = cassandra_state_provider::<C>(
        keyed_state,
        dedup.clone(),
        cells,
        identities,
        cache,
        loader.clone(),
        publisher,
    );
    Ok(ConsumerComponents {
        trigger,
        messages,
        timers,
        dedup,
        state,
        loader,
    })
}

/// Converts a store preparation failure into a consumer construction error.
fn creation_error(error: impl Into<StoreCreationError>) -> ConsumerError {
    error.into().into()
}
