//! Keyed-state wiring: the inputs every mode derives once, and the per-backend
//! state providers built from them.

use crate::consumer::config::ConsumerConfiguration;
use crate::consumer::error::{ConsumerError, KeyedStateInitError, RoutingError};
use crate::consumer::middleware::deduplication::{
    CassandraDeduplicationStoreProvider, MemoryDeduplicationStoreProvider,
};
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
use crate::state::publisher::{PublicationOwner, PublicationTopics, RoutingSet};
use crate::state::registry::CollectionDefRegistry;
use crate::state_reader::PartitionCount;
use crate::subsystem::SubsystemName;
use crate::timers::duration::CompactDuration;
use crate::{ByteSize, Codec, ConsumerGroup, EventIdentity, EventType, METADATA_TIMEOUT, Topic};
use rdkafka::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::RDKafkaErrorCode;
use rdkafka::metadata::{Metadata, MetadataPartition};
use smallvec::SmallVec;
use std::convert::Infallible;
use std::fs;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::spawn_blocking;

pub(crate) type MemoryStateProvider<P> = StateManagerProvider<
    MemoryStateBackendFactory<MemoryDeduplicationStoreProvider>,
    MemoryLoader<P>,
    Option<PublicationOwner<MemoryPublicationStore>>,
>;

pub(crate) type CassandraStateProvider<C> = StateManagerProvider<
    CassandraStateBackendFactory<CassandraDeduplicationStoreProvider>,
    KafkaLoader<C>,
    Option<PublicationOwner<CassandraPublicationStore>>,
>;

/// Keyed-state wiring inputs shared by every mode.
pub(crate) struct KeyedStateInputs {
    config: KeyedStateConfiguration,
    group: ConsumerGroup,
    pub(in crate::consumer) version: Arc<str>,
    registry: Arc<CollectionDefRegistry>,
    topics: Option<PublicationTopics>,
    bootstrap_servers: String,
    mock: bool,
    dedup_ttl: CompactDuration,
}

impl KeyedStateInputs {
    /// Validates the registrations and derives the shared inputs. The dedup
    /// version doubles as the recovery oracle's hash version, so both
    /// middlewares read it from one source.
    pub(in crate::consumer) fn new(
        config: KeyedStateConfiguration,
        consumer_config: &ConsumerConfiguration,
        dedup_version: &str,
        dedup_ttl: Duration,
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
            bootstrap_servers: consumer_config.bootstrap_servers.join(","),
            mock: consumer_config.mock,
            dedup_ttl: CompactDuration::new(u32::try_from(dedup_ttl.as_secs()).unwrap_or(u32::MAX)),
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
            self.dedup_ttl,
        )
    }

    /// Publication setup for a Cassandra arm. The routing set comes from
    /// broker metadata, which this call fetches once.
    ///
    /// # Errors
    ///
    /// [`ConsumerError`] when the fetch fails, or when the metadata cannot
    /// supply a partition count for every subscribed topic.
    pub(in crate::consumer) async fn cassandra_publication_setup(
        &self,
        store: CassandraPublicationStore,
    ) -> Result<Option<PublicationOwner<CassandraPublicationStore>>, ConsumerError> {
        let Some((subsystem, topics)) = self.publication() else {
            return Ok(None);
        };
        // Only published collections need counts; see `RoutingSet`.
        let routes = if self.registry.has_published() {
            fetch_routes(
                self.bootstrap_servers.clone(),
                self.group.clone(),
                topics.clone(),
            )
            .await?
        } else {
            topics.withdrawal(&self.group)
        };
        Ok(Some(PublicationOwner::new(
            subsystem,
            store,
            self.registry.clone(),
            routes,
        )))
    }

    /// Publication setup for mock-mode memory storage. Every topic gets the
    /// mock cluster's partition count. A live Kafka consumer using in-memory
    /// storage cannot publish, because that count is not the real one.
    pub(in crate::consumer) fn memory_publication_setup(
        &self,
        store: MemoryPublicationStore,
    ) -> Result<Option<PublicationOwner<MemoryPublicationStore>>, ConsumerError> {
        if self.registry.has_published() && !self.mock {
            return Err(KeyedStateInitError::PublishedMemoryStorage.into());
        }
        Ok(self.publication().map(|(subsystem, topics)| {
            let Ok(routes) =
                topics.route(&self.group, |_| Ok::<_, Infallible>(PartitionCount::MOCK));
            PublicationOwner::new(subsystem, store, self.registry.clone(), routes)
        }))
    }

    /// The subsystem and topic set the publication owner serves, or `None`
    /// without a subsystem or topics.
    ///
    /// The owner runs only on partition zero of the first topic in lexical
    /// order. It replaces the group's full routing set during assignment
    /// acquisition. The low-level
    /// [`ProsodyConsumer::new`](crate::consumer::ProsodyConsumer::new)
    /// constructor never calls this: it rejects registrations.
    fn publication(&self) -> Option<(SubsystemName, &PublicationTopics)> {
        Some((self.config.subsystem.clone()?, self.topics.as_ref()?))
    }
}

/// Builds the keyed-state provider for an in-memory backend (and the stateless
/// Cassandra path): the in-memory durable store, backend factory,
/// and the caller's in-memory message loader, wrapped in the partition state
/// provider. The pipeline also hands this loader to message defer. Other arms
/// take their concrete bundle's loader.
/// The returned provider supports any trigger backend.
pub(in crate::consumer) fn memory_state_provider<C: Codec>(
    keyed_state: &KeyedStateInputs,
    dedup_provider: MemoryDeduplicationStoreProvider,
    cells: MemoryCells,
    identities: MemoryDescriptorIdentityStore,
    loader: MemoryLoader<C::Payload>,
    publisher: Option<PublicationOwner<MemoryPublicationStore>>,
) -> MemoryStateProvider<C::Payload>
where
    C::Payload: EventType + Clone + EventIdentity + Send + Sync + 'static,
{
    // `cells` and `identities` come from the shared bundle. A reader built from
    // the same bundle observes this consumer's committed writes.
    let backend = MemoryStateBackendFactory::new(
        cells,
        identities,
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
    publisher: Option<PublicationOwner<CassandraPublicationStore>>,
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

/// Reads every topic's partition count from broker metadata.
///
/// A short-lived client fetches the metadata. It has no group id, so it never
/// joins the consumer group, and its drop does not wait for a group leave.
async fn fetch_routes(
    bootstrap_servers: String,
    group: ConsumerGroup,
    topics: PublicationTopics,
) -> Result<RoutingSet, ConsumerError> {
    spawn_blocking(move || {
        let client: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", bootstrap_servers)
            .create()?;
        let metadata = client.fetch_metadata(None, METADATA_TIMEOUT)?;
        let routes = topics
            .route(&group, |topic| partition_count(&metadata, topic))
            .map_err(KeyedStateInitError::from)?;
        Ok(routes)
    })
    .await
    .map_err(ConsumerError::StartupTask)?
}

/// The partition count that broker metadata reports for `topic`.
///
/// The count is the number of partition ids. A partition without a leader
/// still counts, because the broker lists its id.
fn partition_count(metadata: &Metadata, topic: &str) -> Result<PartitionCount, RoutingError> {
    if topic.starts_with('^') {
        return Err(RoutingError::Pattern(topic.to_owned()));
    }
    let broker = |code| RoutingError::Broker(topic.to_owned(), code);
    let entry = metadata
        .topics()
        .iter()
        .find(|entry| entry.name() == topic)
        .ok_or_else(|| broker(RDKafkaErrorCode::UnknownTopicOrPartition))?;
    if let Some(code) = entry.error() {
        return Err(broker(code.into()));
    }
    contiguous_count(entry.partitions().iter().map(MetadataPartition::id), topic)
}

/// The count of `ids`, which must be exactly `0..count` for a positive count.
fn contiguous_count(
    ids: impl Iterator<Item = i32>,
    topic: &str,
) -> Result<PartitionCount, RoutingError> {
    let invalid = || RoutingError::Invalid(topic.to_owned());
    let mut ids: SmallVec<[i32; 64]> = ids.collect();
    ids.sort_unstable();
    let count = i32::try_from(ids.len()).map_err(|_| invalid())?;
    if !ids.iter().copied().eq(0_i32..count) {
        return Err(invalid());
    }
    PartitionCount::try_from(count).map_err(|_| invalid())
}

#[cfg(test)]
mod tests;
