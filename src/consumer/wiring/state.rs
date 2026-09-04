//! Keyed-state wiring: the inputs every mode derives once, and the per-backend
//! state providers built from them.

use crate::consumer::config::ConsumerConfiguration;
use crate::consumer::error::{ConsumerError, KeyedStateInitError};
use crate::consumer::middleware::deduplication::{
    CassandraDeduplicationStoreProvider, MemoryDeduplicationStoreProvider,
};
use crate::consumer::observer::KafkaObserver;
use crate::error::ClassifyError;
use crate::loader::{KafkaLoader, MemoryLoader};
use crate::state::cassandra::{
    CassandraCellResources, CassandraDescriptorIdentityStore, CassandraPublicationStore,
};
use crate::state::config::KeyedStateConfiguration;
use crate::state::fjall::FjallClient;
use crate::state::manager::StateManagerProvider;
use crate::state::memory::{MemoryCells, MemoryDescriptorIdentityStore, MemoryPublicationStore};
use crate::state::production::{CassandraStateBackendFactory, MemoryStateBackendFactory};
use crate::state::publication::PublicationStore;
use crate::state::publisher::{
    FixedPartitionCount, PartitionCountSource, PublicationOwner, PublicationTopics,
};
use crate::state::registry::CollectionDefRegistry;
use crate::state_reader::PartitionCount;
use crate::timers::duration::CompactDuration;
use crate::{ByteSize, Codec, ConsumerGroup, EventIdentity, EventType, Topic};
use std::fs;
use std::sync::Arc;

pub(crate) type MemoryStateProvider<P> = StateManagerProvider<
    MemoryStateBackendFactory<MemoryDeduplicationStoreProvider>,
    MemoryLoader<P>,
    Option<PublicationOwner<MemoryPublicationStore, FixedPartitionCount>>,
>;

pub(crate) type CassandraStateProvider<C> = StateManagerProvider<
    CassandraStateBackendFactory<CassandraDeduplicationStoreProvider>,
    KafkaLoader<C>,
    Option<PublicationOwner<CassandraPublicationStore, KafkaObserver>>,
>;

/// Keyed-state wiring inputs shared by every mode.
pub(crate) struct KeyedStateInputs {
    config: KeyedStateConfiguration,
    group: ConsumerGroup,
    pub(in crate::consumer) version: Arc<str>,
    registry: Arc<CollectionDefRegistry>,
    topics: Option<PublicationTopics>,
    mock: bool,
}

impl KeyedStateInputs {
    /// Validates the registrations and derives the shared inputs. The dedup
    /// version doubles as the recovery oracle's hash version, so both
    /// middlewares read it from one source.
    pub(in crate::consumer) fn new(
        config: KeyedStateConfiguration,
        consumer_config: &ConsumerConfiguration,
        dedup_version: &str,
    ) -> Result<Self, ConsumerError> {
        // Fail before backend I/O instead of waiting for the first rebalance.
        CompactDuration::try_from(consumer_config.slab_size)
            .map_err(ConsumerError::InvalidSlabSize)?;
        let registry = Arc::new(config.build_registry().map_err(KeyedStateInitError::from)?);
        let topics: Vec<Topic> = consumer_config
            .subscribed_topics
            .iter()
            .map(|topic| Topic::from(topic.as_str()))
            .collect();
        Ok(Self {
            config,
            group: Arc::from(consumer_config.group_id.as_str()),
            version: Arc::from(dedup_version),
            registry,
            topics: PublicationTopics::new(topics),
            mock: consumer_config.mock,
        })
    }

    /// Builds the per-partition keyed-state provider over a branch's
    /// backend and loader. The partition loop acquires one state manager
    /// per assignment from it; the pending-index scanner travels inside
    /// the backend the factory mints.
    fn provider<B, L, P>(
        &self,
        backend: B,
        loader: L,
        publisher: P,
    ) -> StateManagerProvider<B, L, P> {
        StateManagerProvider::new(
            backend,
            loader,
            publisher,
            self.registry.clone(),
            self.group.clone(),
            self.config.recovery_delay,
        )
    }

    /// Publication setup for a Cassandra arm. The count source is the primary
    /// consumer's own observation, so the routing row advertises the topology
    /// that consumer sees. The observer is the one carried by
    /// [`StartupServices`](super::runtime::StartupServices).
    pub(in crate::consumer) fn cassandra_publication_setup(
        &self,
        store: CassandraPublicationStore,
        observer: KafkaObserver,
    ) -> Option<PublicationOwner<CassandraPublicationStore, KafkaObserver>> {
        self.publication_setup(store, observer)
    }

    /// Publication setup for mock-mode memory storage. The fixed partition
    /// count is the mock cluster's topology. A live Kafka consumer using
    /// in-memory storage cannot publish because this backend has no real topic
    /// partition-count source.
    pub(in crate::consumer) fn memory_publication_setup(
        &self,
        store: MemoryPublicationStore,
    ) -> Result<Option<PublicationOwner<MemoryPublicationStore, FixedPartitionCount>>, ConsumerError>
    {
        if self.registry.has_published() && !self.mock {
            return Err(KeyedStateInitError::PublishedMemoryStorage.into());
        }
        Ok(self.publication_setup(store, FixedPartitionCount(PartitionCount::MOCK)))
    }

    /// Builds the assignment owner for one backend's publication store.
    /// The two typed wrappers above choose the count source, so a mock topology
    /// can never reach a Cassandra routing row.
    ///
    /// The owner runs only on partition zero of the first topic in lexical
    /// order. It replaces the group's full routing set during assignment
    /// acquisition. The low-level
    /// [`ProsodyConsumer::new`](crate::consumer::ProsodyConsumer::new)
    /// constructor never calls this: it rejects registrations.
    fn publication_setup<S, N>(&self, store: S, counts: N) -> Option<PublicationOwner<S, N>>
    where
        S: PublicationStore,
        N: PartitionCountSource,
    {
        let (Some(subsystem), Some(topics)) = (self.config.subsystem.clone(), self.topics.clone())
        else {
            return None;
        };
        Some(PublicationOwner::new(
            subsystem,
            self.group.clone(),
            store,
            counts,
            self.registry.clone(),
            topics,
        ))
    }
}

/// Builds the keyed-state provider for an in-memory backend (and the stateless
/// Cassandra path): the in-memory durable store, backend factory,
/// and the caller's in-memory message loader, wrapped in the partition state
/// provider. The pipeline also hands this loader to message defer. Other arms
/// take their concrete bundle's loader.
/// The factory is store-type agnostic — the commit oracle's trigger store
/// handle arrives per partition via
/// [`PartitionStateProvider::acquire`](crate::state::manager::PartitionStateProvider::acquire)
/// — so the concrete return type serves any trigger backend.
pub(in crate::consumer) fn memory_state_provider<C: Codec>(
    keyed_state: &KeyedStateInputs,
    dedup_provider: MemoryDeduplicationStoreProvider,
    cells: MemoryCells,
    identities: MemoryDescriptorIdentityStore,
    loader: MemoryLoader<C::Payload>,
    publisher: Option<PublicationOwner<MemoryPublicationStore, FixedPartitionCount>>,
) -> MemoryStateProvider<C::Payload>
where
    C::Payload: EventType + Clone + EventIdentity + Send + Sync + 'static,
{
    // `cells` and `identities` come from the shared bundle. A reader built from
    // the same bundle observes this consumer's committed writes.
    let backend = MemoryStateBackendFactory::new(
        cells,
        identities,
        keyed_state.registry.clone(),
        dedup_provider,
        keyed_state.group.clone(),
    );
    keyed_state.provider(backend, loader, publisher)
}

/// Builds the keyed-state provider for a Cassandra backend. It opens the fjall
/// workspace, mints the backend factory over the caller's
/// Kafka loader, and wraps it in the partition state provider. Shared by every
/// constructor's Cassandra arm; the caller owns the loader so the pipeline can
/// hand the same one to its message-defer middleware.
pub(in crate::consumer) fn cassandra_state_provider<C: Codec>(
    keyed_state: &KeyedStateInputs,
    dedup_provider: CassandraDeduplicationStoreProvider,
    cell_store: CassandraCellResources,
    identity_store: CassandraDescriptorIdentityStore,
    loader: KafkaLoader<C>,
    publisher: Option<PublicationOwner<CassandraPublicationStore, KafkaObserver>>,
) -> Result<CassandraStateProvider<C>, ConsumerError>
where
    C::Payload: EventType + Clone + EventIdentity + Send + Sync + 'static,
{
    // The fjall workspace root is wiped on restart (Cassandra is
    // authoritative), so creating the default directory here is safe.
    fs::create_dir_all(&keyed_state.config.cache_dir)?;
    let fjall_client = FjallClient::open(
        &keyed_state.config.cache_dir,
        keyed_state.config.owned_cache_size.map(ByteSize::nonzero),
    )
    .map_err(|error| KeyedStateInitError::Cache {
        message: format!("{error:#}"),
        category: error.classify_error(),
    })?;
    let backend = CassandraStateBackendFactory::new(
        fjall_client,
        cell_store,
        identity_store,
        keyed_state.registry.clone(),
        dedup_provider,
        keyed_state.group.clone(),
    );
    Ok(keyed_state.provider(backend, loader, publisher))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::JsonCodec;
    use crate::state::descriptor::{StateDescriptor, value_state};
    use crate::subsystem::SubsystemName;
    use color_eyre::Result;

    /// Published routing from memory is valid only when the Kafka topology is
    /// also mocked. A live consumer has no typed partition-count source for
    /// the in-memory publication store.
    #[tokio::test]
    async fn published_memory_state_requires_mock_mode() -> Result<()> {
        let mut state = KeyedStateConfiguration::builder()
            .subsystem(Some(SubsystemName::try_new("orders")?))
            .build()?;
        let _ = state.register(value_state::<JsonCodec>("cart").published(true));
        let consumer = ConsumerConfiguration::builder()
            .bootstrap_servers(vec!["unused:9092".to_owned()])
            .group_id("orders")
            .subscribed_topics(&["orders".to_owned()])
            .mock(false)
            .build()?;
        let inputs = KeyedStateInputs::new(state, &consumer, "v1")?;

        let result = inputs.memory_publication_setup(MemoryPublicationStore::new());

        assert!(matches!(
            result,
            Err(ConsumerError::KeyedState(
                KeyedStateInitError::PublishedMemoryStorage
            ))
        ));
        Ok(())
    }
}
