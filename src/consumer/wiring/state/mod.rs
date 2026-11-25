//! Keyed-state wiring: the inputs every mode derives once, and the per-backend
//! state providers built from them.

use crate::consumer::config::ConsumerConfiguration;
use crate::consumer::error::{ConsumerError, KeyedStateInitError, RoutingError};
use crate::consumer::middleware::deduplication::{
    CassandraDeduplicationStoreProvider, MemoryDeduplicationStoreProvider,
};
use crate::error::ClassifyError;
use crate::kafka::{LogContext, base_config};
use crate::loader::{KafkaLoader, MemoryLoader};
use crate::state::cassandra::{
    CassandraCellResources, CassandraDescriptorIdentityStore, CassandraPublicationStore,
};
use crate::state::config::KeyedStateConfiguration;
use crate::state::fjall::{FjallClient, OwnedPartitions};
use crate::state::manager::StateManagerProvider;
use crate::state::memory::{MemoryCells, MemoryDescriptorIdentityStore, MemoryPublicationStore};
use crate::state::production::{CassandraStateBackendFactory, MemoryStateBackendFactory};
use crate::state::publisher::Routes;
use crate::state::registry::CollectionDefRegistry;
use crate::state_reader::PartitionCount;
use crate::timers::duration::CompactDuration;
use crate::{Codec, ConsumerGroup, EventIdentity, EventType, METADATA_TIMEOUT, Topic};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::RDKafkaErrorCode;
use rdkafka::metadata::Metadata;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::spawn_blocking;

pub(crate) type MemoryStateProvider<P> = StateManagerProvider<
    MemoryStateBackendFactory<MemoryDeduplicationStoreProvider>,
    MemoryLoader<P>,
>;

pub(crate) type CassandraStateProvider<C> = StateManagerProvider<
    CassandraStateBackendFactory<CassandraDeduplicationStoreProvider>,
    KafkaLoader<C>,
>;

/// Keyed-state wiring inputs shared by every mode.
pub(crate) struct KeyedStateInputs {
    config: KeyedStateConfiguration,
    group: ConsumerGroup,
    pub(in crate::consumer) version: Arc<str>,
    /// Set by the rebalance callback and read by the local cache.
    pub(in crate::consumer) owned: OwnedPartitions,
    registry: Arc<CollectionDefRegistry>,
    topics: Vec<Topic>,
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
        let subscribed = &consumer_config.subscribed_topics;
        if registry.has_published()
            && let Some(pattern) = subscribed.iter().find(|name| name.starts_with('^'))
        {
            return Err(KeyedStateInitError::from(RoutingError::Pattern(pattern.clone())).into());
        }
        let topics = subscribed
            .iter()
            .map(|topic| Topic::from(topic.as_str()))
            .collect();
        Ok(Self {
            config,
            group: Arc::from(consumer_config.group_id.as_str()),
            version: Arc::from(dedup_version),
            owned: OwnedPartitions::default(),
            registry,
            topics,
            bootstrap_servers: consumer_config.bootstrap_servers.join(","),
            mock: consumer_config.mock,
            dedup_ttl: CompactDuration::new(u32::try_from(dedup_ttl.as_secs()).unwrap_or(u32::MAX)),
        })
    }

    /// Builds the per-partition keyed-state provider over a branch's
    /// backend and loader. The partition loop acquires one state manager
    /// per assignment from it; the pending-index scanner travels inside
    /// the backend the factory mints.
    fn provider<B, L>(&self, backend: B, loader: L) -> StateManagerProvider<B, L> {
        StateManagerProvider::new(
            backend,
            loader,
            self.registry.clone(),
            self.group.clone(),
            self.dedup_ttl,
        )
    }

    /// Opens the local keyed-state cache that a Cassandra backend reads
    /// through.
    ///
    /// # Errors
    ///
    /// [`KeyedStateInitError::Cache`] when the cache cannot be opened.
    pub(in crate::consumer) async fn open_cache(&self) -> Result<FjallClient, ConsumerError> {
        let opened = FjallClient::open(&self.config, self.owned.clone()).await;
        opened.map_err(|error| {
            KeyedStateInitError::Cache {
                message: format!("{error:#}"),
                category: error.classify_error(),
            }
            .into()
        })
    }

    /// The routes for a Cassandra arm. A consumer with a published collection
    /// reads the partition count of each subscribed topic from broker
    /// metadata. This call fetches it once. Otherwise this call fetches
    /// nothing.
    ///
    /// # Errors
    ///
    /// [`ConsumerError`] when the fetch fails, or when the metadata cannot
    /// supply a partition count for every subscribed topic.
    pub(in crate::consumer) async fn cassandra_routes(
        &self,
        store: CassandraPublicationStore,
    ) -> Result<Routes<CassandraPublicationStore>, ConsumerError> {
        // Only published collections need counts.
        let counts = if self.registry.has_published() {
            fetch_counts(self.bootstrap_servers.clone(), self.topics.clone()).await?
        } else {
            Vec::new()
        };
        Ok(self.routes(store, counts))
    }

    /// The routes for mock-mode memory storage. Every topic gets the mock
    /// cluster's partition count. A live Kafka consumer using in-memory
    /// storage cannot publish, because that count is not the real one.
    pub(in crate::consumer) fn memory_routes(
        &self,
        store: MemoryPublicationStore,
    ) -> Result<Routes<MemoryPublicationStore>, ConsumerError> {
        if self.registry.has_published() && !self.mock {
            return Err(KeyedStateInitError::PublishedMemoryStorage.into());
        }
        let counts = self
            .topics
            .iter()
            .map(|&topic| (topic, PartitionCount::MOCK));
        Ok(self.routes(store, counts))
    }

    /// The routes under the configured subsystem. A consumer without a
    /// subsystem publishes nothing. The low-level
    /// [`ProsodyConsumer::new`](crate::consumer::ProsodyConsumer::new)
    /// constructor never calls this: it rejects registrations.
    fn routes<S>(
        &self,
        store: S,
        counts: impl IntoIterator<Item = (Topic, PartitionCount)>,
    ) -> Routes<S> {
        match &self.config.subsystem {
            Some(subsystem) => Routes::new(store, subsystem.clone(), self.group.clone(), counts),
            None => Routes::Unpublished,
        }
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
    routes: Routes<MemoryPublicationStore>,
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
        routes,
    );
    keyed_state.provider(backend, loader)
}

/// Builds the keyed-state provider for a Cassandra backend: the backend
/// factory over the opened cache and the caller's Kafka loader, wrapped in the
/// partition state provider. The pipeline hands the same loader to its
/// message-defer middleware.
pub(in crate::consumer) fn cassandra_state_provider<C: Codec>(
    keyed_state: &KeyedStateInputs,
    dedup_provider: CassandraDeduplicationStoreProvider,
    cell_store: CassandraCellResources,
    identity_store: CassandraDescriptorIdentityStore,
    cache: FjallClient,
    loader: KafkaLoader<C>,
    routes: Routes<CassandraPublicationStore>,
) -> CassandraStateProvider<C>
where
    C::Payload: EventType + Clone + EventIdentity + Send + Sync + 'static,
{
    let backend = CassandraStateBackendFactory::new(
        cache,
        cell_store,
        identity_store,
        keyed_state.registry.clone(),
        dedup_provider,
        keyed_state.group.clone(),
        routes,
    );
    keyed_state.provider(backend, loader)
}

/// Reads every topic's partition count from broker metadata.
///
/// Partition counts never change, because a Prosody deployment never increases
/// them. So this call runs once, at construction, and every routing row is
/// final. Do not take counts from librdkafka statistics. A statistics report
/// lists only the topics that this client held.
///
/// A short-lived client fetches the metadata. It has no group id, so it never
/// joins the consumer group, and its drop does not wait for a group leave.
async fn fetch_counts(
    bootstrap_servers: String,
    topics: Vec<Topic>,
) -> Result<Vec<(Topic, PartitionCount)>, ConsumerError> {
    spawn_blocking(move || {
        let client: BaseConsumer<LogContext> =
            base_config(bootstrap_servers).create_with_context(LogContext)?;
        let metadata = client.fetch_metadata(None, METADATA_TIMEOUT)?;
        let counts = topics
            .into_iter()
            .map(|topic| Ok((topic, partition_count(&metadata, topic.as_ref())?)))
            .collect::<Result<_, RoutingError>>()
            .map_err(KeyedStateInitError::from)?;
        Ok(counts)
    })
    .await
    .map_err(ConsumerError::StartupTask)?
}

/// The partition count that broker metadata reports for `topic`: the number
/// of partitions it lists. A partition without a leader still counts, because
/// the broker lists it.
fn partition_count(metadata: &Metadata, topic: &str) -> Result<PartitionCount, RoutingError> {
    let broker = |code| RoutingError::Broker(topic.to_owned(), code);
    let entry = metadata
        .topics()
        .iter()
        .find(|entry| entry.name() == topic)
        .ok_or_else(|| broker(RDKafkaErrorCode::UnknownTopicOrPartition))?;
    if let Some(code) = entry.error() {
        return Err(broker(code.into()));
    }
    let invalid = || RoutingError::Invalid(topic.to_owned());
    let count = i32::try_from(entry.partitions().len()).map_err(|_| invalid())?;
    PartitionCount::try_from(count).map_err(|_| invalid())
}

#[cfg(test)]
mod tests;
