//! The direct mode: dispatch each event straight to the handler with no
//! middleware. The durability boundary never runs, so keyed-state
//! registrations are rejected.

use crate::cassandra::CassandraStore;
use crate::consumer::ProsodyConsumer;
use crate::consumer::config::ConsumerConfiguration;
use crate::consumer::error::{ConsumerError, KeyedStateInitError};
use crate::consumer::handler::{EventHandler, HandlerProvider};
use crate::consumer::kafka_context::PartitionProviders;
use crate::consumer::middleware::deduplication::{
    DEFAULT_IDEMPOTENCE_VERSION, MemoryDeduplicationStoreProvider,
};
use crate::consumer::observer::KafkaObserver;
use crate::consumer::storage::StoreCreationError;
use crate::consumer::wiring::runtime::{StartupServices, initialize_consumer};
use crate::consumer::wiring::state::{KeyedStateInputs, memory_state_provider};
use crate::heartbeat::HeartbeatRegistry;
use crate::high_level::config::TriggerStoreConfiguration;
use crate::loader::MemoryLoader;
use crate::peer::NoPeer;
use crate::state::config::KeyedStateConfiguration;
use crate::state::memory::{MemoryCells, MemoryDescriptorIdentityStore};
use crate::telemetry::Telemetry;
use crate::timers::store::TriggerStoreProvider;
use crate::timers::store::cassandra::CassandraTriggerStoreProvider;
use crate::timers::store::memory::InMemoryTriggerStoreProvider;
use crate::{Codec, EventIdentity, EventType};
use std::sync::Arc;
use validator::Validate;

impl<C: Codec> ProsodyConsumer<C>
where
    C::Payload: EventType + Clone,
{
    /// Creates a low-level `ProsodyConsumer` that runs an [`EventHandler`]
    /// directly, with **no middleware**.
    ///
    /// This is the lower of the two consumer layers. It wires the partition
    /// machinery and an empty keyed-state backend, then dispatches each
    /// message and timer straight to the handler — no retry, deduplication,
    /// monopolization, or defer middleware runs, and the `settle` durability
    /// boundary never executes. The handler owns its own commit decisions
    /// through the `Uncommitted` types.
    ///
    /// Because the durability boundary never runs here, keyed-state
    /// collections can be neither staged nor recovered: registering any is
    /// rejected with
    /// [`KeyedStateInitError::StateUnsupported`].
    /// Use a high-level constructor ([`Self::pipeline_consumer`],
    /// [`Self::low_latency_consumer`]) for keyed state and the full middleware
    /// stack — those take a
    /// [`FallibleHandler`](crate::consumer::FallibleHandler).
    ///
    /// # Errors
    ///
    /// Returns a `ConsumerError` if the configuration is invalid, keyed-state
    /// collections are registered, or store/consumer creation fails.
    pub async fn new<T>(
        consumer_config: &ConsumerConfiguration,
        trigger_store_config: &TriggerStoreConfiguration,
        keyed_state_config: KeyedStateConfiguration,
        handler_provider: T,
        telemetry: Telemetry,
    ) -> Result<Self, ConsumerError>
    where
        C::Payload: EventIdentity + Send + Sync + 'static,
        T: HandlerProvider,
        T::Handler: EventHandler<Payload = C::Payload>,
    {
        consumer_config.validate()?;
        keyed_state_config.validate()?;

        // The `settle` durability boundary never runs on this path, so a
        // registered collection could never be staged or recovered. Reject it
        // rather than silently accept a non-functional registration.
        if keyed_state_config.has_registrations() {
            return Err(KeyedStateInitError::StateUnsupported.into());
        }

        let heartbeats = HeartbeatRegistry::new(
            consumer_config.group_id.clone(),
            consumer_config.stall_threshold,
        );

        let keyed_state = KeyedStateInputs::new(
            keyed_state_config,
            consumer_config,
            DEFAULT_IDEMPOTENCE_VERSION,
        )?;

        let managers = Arc::default();

        let services = StartupServices {
            version: keyed_state.version.clone(),
            telemetry: &telemetry,
            heartbeats,
            observer: KafkaObserver::new(&consumer_config.group_id),
            managers,
        };

        match (consumer_config.mock, trigger_store_config) {
            (true, _) | (false, TriggerStoreConfiguration::InMemory) => {
                initialize_direct::<T, _, C>(
                    consumer_config,
                    handler_provider,
                    InMemoryTriggerStoreProvider::new(),
                    &keyed_state,
                    services,
                )
                .await
            }
            (false, TriggerStoreConfiguration::Cassandra(config)) => {
                let store = CassandraStore::new(config)
                    .await
                    .map_err(StoreCreationError::from)?;
                let trigger =
                    CassandraTriggerStoreProvider::with_store(store, &config.keyspace).await?;
                initialize_direct::<T, _, C>(
                    consumer_config,
                    handler_provider,
                    trigger,
                    &keyed_state,
                    services,
                )
                .await
            }
        }
    }
}

async fn initialize_direct<T, P, C>(
    consumer: &ConsumerConfiguration,
    handler: T,
    trigger: P,
    keyed_state: &KeyedStateInputs,
    services: StartupServices<'_, C::Payload>,
) -> Result<ProsodyConsumer<C>, ConsumerError>
where
    T: HandlerProvider,
    T::Handler: EventHandler<Payload = C::Payload>,
    P: TriggerStoreProvider,
    C: Codec,
    C::Payload: EventIdentity + EventType + Clone,
{
    let state = memory_state_provider::<C>(
        keyed_state,
        MemoryDeduplicationStoreProvider::new(),
        MemoryCells::new(),
        MemoryDescriptorIdentityStore::new(),
        MemoryLoader::new(),
        None,
    );
    Box::pin(initialize_consumer::<_, _, _, C, _>(
        consumer,
        handler,
        PartitionProviders {
            triggers: trigger,
            state,
        },
        services,
        NoPeer,
    ))
    .await
}
