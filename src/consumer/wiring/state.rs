//! Keyed-state wiring: the inputs every mode derives once, and the per-backend
//! state providers built from them.

use crate::consumer::config::ConsumerConfiguration;
use crate::consumer::error::{ConsumerError, KeyedStateInitError};
use crate::consumer::middleware::deduplication::{
    CassandraDeduplicationStoreProvider, MemoryDeduplicationStoreProvider,
};
use crate::consumer::middleware::defer::DeferInitError;
use crate::consumer::observer::KafkaObserver;
use crate::consumer::storage::SharedStorage;
use crate::error::ClassifyError;
use crate::heartbeat::HeartbeatRegistry;
use crate::loader::{KafkaLoader, MemoryLoader};
use crate::state::cassandra::{
    CassandraCellResources, CassandraDescriptorIdentityStore, CassandraPublicationStore,
};
use crate::state::config::KeyedStateConfiguration;
use crate::state::first_write::{
    PartitionCounts, PublicationBackend, PublisherTemplate, reconcile_publications,
};
use crate::state::fjall::FjallClient;
use crate::state::manager::StateManagerProvider;
use crate::state::memory::{MemoryCells, MemoryDescriptorIdentityStore, MemoryPublicationStore};
use crate::state::production::{CassandraStateBackendFactory, MemoryStateBackendFactory};
use crate::state::registry::CollectionDefRegistry;
use crate::state_reader::{PartitionCount, SharedDeps};
use crate::timers::duration::CompactDuration;
use crate::{Codec, ConsumerGroup, EventIdentity, EventType};
use std::fs;
use std::sync::Arc;

/// What [`memory_arm_inputs`] returns: `(loader, cells, identities)`.
type MemoryArmInputs<P> = (MemoryLoader<P>, MemoryCells, MemoryDescriptorIdentityStore);

/// Keyed-state wiring inputs shared by every mode's storage branches.
pub(in crate::consumer) struct KeyedStateInputs {
    config: KeyedStateConfiguration,
    group: ConsumerGroup,
    pub(in crate::consumer) version: Arc<str>,
    registry: Arc<CollectionDefRegistry>,
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
        // Fail fast on an invalid timer slab size — the partition loop would
        // otherwise only log and fall back to a default at rebalance time.
        CompactDuration::try_from(consumer_config.slab_size)
            .map_err(ConsumerError::InvalidSlabSize)?;
        let registry = Arc::new(config.build_registry().map_err(KeyedStateInitError::from)?);
        Ok(Self {
            config,
            group: Arc::from(consumer_config.group_id.as_str()),
            version: Arc::from(dedup_version),
            registry,
        })
    }

    /// Builds the per-partition keyed-state provider over a branch's
    /// backend and loader. The partition loop acquires one state manager
    /// per assignment from it; the pending-index scanner travels inside
    /// the backend the factory mints.
    fn provider<B, L>(
        &self,
        backend: B,
        loader: L,
        publisher_template: Option<PublisherTemplate>,
    ) -> StateManagerProvider<B, L> {
        StateManagerProvider::new(
            backend,
            loader,
            self.registry.clone(),
            self.group.clone(),
            self.config.recovery_delay,
            publisher_template,
        )
    }

    /// Publication setup for a Cassandra arm. The count source is the primary
    /// consumer's own observation, so the routing row advertises the topology
    /// that consumer sees. Pass the same observer the mode hands to
    /// [`initialize_consumer`](super::runtime::initialize_consumer).
    pub(in crate::consumer) async fn cassandra_publication_setup(
        &self,
        store: CassandraPublicationStore,
        observer: KafkaObserver,
    ) -> Result<Option<PublisherTemplate>, ConsumerError> {
        self.publication_setup(
            PublicationBackend::Cassandra(store),
            PartitionCounts::Observed(observer),
        )
        .await
    }

    /// Publication setup for a memory arm — mock mode, or a consumer
    /// configured with in-memory trigger storage. Both keep their routing rows
    /// in-process, where the only reader is one sharing this consumer's
    /// bundle, and both advertise the fixed [`PartitionCount::MOCK`] topology.
    ///
    /// That count is the mock cluster's own, so mock mode routes correctly. A
    /// non-mock consumer with in-memory triggers advertises it too, so a
    /// bundle-sharing reader reaches the writer's segment only when the real
    /// topic has that many partitions.
    pub(in crate::consumer) async fn memory_publication_setup(
        &self,
        store: MemoryPublicationStore,
    ) -> Result<Option<PublisherTemplate>, ConsumerError> {
        self.publication_setup(
            PublicationBackend::Memory(store),
            PartitionCounts::Fixed(PartitionCount::MOCK),
        )
        .await
    }

    /// Runs startup reconciliation and, when publishing is active, builds the
    /// first-write publisher template for one storage arm's publication store.
    /// The two typed wrappers above choose the count source, so a mock topology
    /// can never reach a Cassandra routing row.
    ///
    /// Reconciliation runs whenever a subsystem is configured. It retires this
    /// group's routing rows for collections no longer published, the
    /// `.published(false)` path. Correctness rests on two facts: at most one
    /// instance owns each partition, and a deploy stops the old instance
    /// before starting the new one.
    ///
    /// The template is built only when a collection is actually published.
    /// Otherwise there is nothing to advertise, and `None` disables the
    /// first-write barrier. The low-level
    /// [`ProsodyConsumer::new`](crate::consumer::ProsodyConsumer::new)
    /// constructor never calls this: it rejects registrations.
    ///
    /// # Errors
    ///
    /// A transient reconciliation failure, such as a broken publication store,
    /// propagates so the caller's build fails and the deploy retries.
    /// Per-collection permanent decode failures are logged and skipped inside
    /// [`reconcile_publications`].
    async fn publication_setup(
        &self,
        store: PublicationBackend,
        counts: PartitionCounts,
    ) -> Result<Option<PublisherTemplate>, ConsumerError> {
        let Some(subsystem) = self.config.subsystem.clone() else {
            return Ok(None);
        };
        reconcile_publications(&store, &self.registry, &subsystem, &self.group)
            .await
            .map_err(|error| KeyedStateInitError::Publication {
                message: format!("{error:#}"),
                category: error.classify_error(),
            })?;
        if !self.registry.has_published() {
            return Ok(None);
        }
        Ok(Some(PublisherTemplate::new(
            subsystem,
            self.group.clone(),
            Arc::new(store),
            counts,
            self.registry.clone(),
        )))
    }
}

/// Builds the keyed-state provider for a
/// [`StorePair::Memory`](crate::consumer::storage::StorePair::Memory) arm (and
/// the stateless Cassandra arm): the in-memory durable store, backend factory,
/// and the caller's in-memory message loader, wrapped in the partition state
/// provider. The pipeline also hands this loader to message defer. Other arms
/// take the bundle's loader when one is supplied, otherwise a fresh one.
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
    publisher_template: Option<PublisherTemplate>,
) -> StateManagerProvider<
    MemoryStateBackendFactory<MemoryDeduplicationStoreProvider>,
    MemoryLoader<C::Payload>,
>
where
    C::Payload: EventType + Clone + EventIdentity + Send + Sync + 'static,
{
    // `cells`/`identities` come from the shared bundle when one is supplied. A
    // reader built from the same bundle then observes this consumer's committed
    // writes (mock read-your-writes). A no-bundle consumer passes fresh stores.
    let backend = MemoryStateBackendFactory::new(
        cells,
        identities,
        keyed_state.registry.clone(),
        dedup_provider,
        keyed_state.group.clone(),
    );
    keyed_state.provider(backend, loader, publisher_template)
}

/// The memory cell and identity stores backing the state provider. Returns the
/// shared bundle's stores when one is supplied, so a reader built from the same
/// bundle sees this consumer's committed writes. Otherwise returns fresh
/// stores.
///
/// A Cassandra bundle cannot back a memory arm. The composition derives both
/// from one config, so this mismatch is reported as
/// [`ConsumerError::SharedDepsBackendMismatch`]. See [`cassandra_loader`] for
/// the mirror.
fn shared_memory_handles(
    shared: Option<SharedStorage>,
) -> Result<(MemoryCells, MemoryDescriptorIdentityStore), ConsumerError> {
    match shared {
        Some(SharedStorage::Memory {
            cells, identities, ..
        }) => Ok((cells, identities)),
        Some(SharedStorage::Cassandra { .. }) => Err(ConsumerError::SharedDepsBackendMismatch),
        None => Ok((MemoryCells::new(), MemoryDescriptorIdentityStore::new())),
    }
}

/// The memory-arm inputs a consumer resolves from an optional shared bundle.
/// These come from the bundle when one is supplied, so a reader built from the
/// same bundle sees this consumer's committed writes. Otherwise they are fresh
/// mock defaults.
pub(in crate::consumer) fn memory_arm_inputs<C: Codec>(
    deps: Option<&SharedDeps<C>>,
    shared: Option<SharedStorage>,
) -> Result<MemoryArmInputs<C::Payload>, ConsumerError>
where
    C::Payload: Clone,
{
    let loader = deps.and_then(SharedDeps::memory_loader).unwrap_or_default();
    let (cells, identities) = shared_memory_handles(shared)?;
    Ok((loader, cells, identities))
}

/// The Cassandra-arm input a consumer resolves from an optional shared bundle:
/// the Kafka loader. A supplied bundle's `Clone` shares the client and poll
/// thread. Otherwise the loader is freshly built.
///
/// A memory bundle cannot back a Cassandra arm. That mismatch is reported as
/// [`ConsumerError::SharedDepsBackendMismatch`].
pub(in crate::consumer) fn cassandra_loader<C: Codec>(
    deps: Option<&SharedDeps<C>>,
    consumer_config: &ConsumerConfiguration,
    heartbeats: &HeartbeatRegistry,
) -> Result<KafkaLoader<C>, ConsumerError>
where
    C::Payload: Clone,
{
    match deps {
        Some(deps) => deps
            .kafka_loader()
            .ok_or(ConsumerError::SharedDepsBackendMismatch),
        None => KafkaLoader::<C>::for_consumer(consumer_config, heartbeats)
            .map_err(|error| ConsumerError::from(DeferInitError::from(error))),
    }
}

/// Builds the keyed-state provider for a
/// [`StorePair::Cassandra`](crate::consumer::storage::StorePair::Cassandra)
/// arm: opens the fjall workspace, mints the backend factory over the caller's
/// Kafka loader, and wraps it in the partition state provider. Shared by every
/// constructor's Cassandra arm; the caller owns the loader so the pipeline can
/// hand the same one to its message-defer middleware.
pub(in crate::consumer) fn cassandra_state_provider<C: Codec>(
    keyed_state: &KeyedStateInputs,
    dedup_provider: CassandraDeduplicationStoreProvider,
    cell_store: CassandraCellResources,
    identity_store: CassandraDescriptorIdentityStore,
    loader: KafkaLoader<C>,
    publisher_template: Option<PublisherTemplate>,
) -> Result<
    StateManagerProvider<
        CassandraStateBackendFactory<CassandraDeduplicationStoreProvider>,
        KafkaLoader<C>,
    >,
    ConsumerError,
>
where
    C::Payload: EventType + Clone + EventIdentity + Send + Sync + 'static,
{
    // The fjall workspace root is wiped on restart (Cassandra is
    // authoritative), so creating the default directory here is safe.
    fs::create_dir_all(&keyed_state.config.cache_dir)?;
    let fjall_client = FjallClient::open(
        &keyed_state.config.cache_dir,
        keyed_state.config.cache_size_bytes,
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
    Ok(keyed_state.provider(backend, loader, publisher_template))
}
