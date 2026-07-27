//! The direct mode: dispatch each event straight to the handler with no
//! middleware. The durability boundary never runs, so keyed-state
//! registrations are rejected.

use crate::consumer::ProsodyConsumer;
use crate::consumer::config::ConsumerConfiguration;
use crate::consumer::error::{ConsumerError, KeyedStateInitError};
use crate::consumer::handler::{EventHandler, HandlerProvider};
use crate::consumer::middleware::deduplication::{
    DEFAULT_IDEMPOTENCE_VERSION, MemoryDeduplicationStoreProvider,
};
use crate::consumer::observer::KafkaObserver;
use crate::consumer::storage::StorePair;
use crate::consumer::wiring::runtime::{StartupServices, initialize_consumer};
use crate::consumer::wiring::state::{KeyedStateInputs, memory_state_provider};
use crate::heartbeat::HeartbeatRegistry;
use crate::high_level::config::TriggerStoreConfiguration;
use crate::loader::MemoryLoader;
use crate::state::config::KeyedStateConfiguration;
use crate::state::memory::{MemoryCells, MemoryDescriptorIdentityStore};
use crate::telemetry::Telemetry;
use crate::{Codec, EventIdentity, EventType};
use std::num::NonZeroUsize;
use std::time::Duration;
use validator::Validate;

pub(super) async fn build<T, C>(
    consumer_config: &ConsumerConfiguration,
    trigger_store_config: &TriggerStoreConfiguration,
    keyed_state_config: KeyedStateConfiguration,
    handler_provider: T,
    telemetry: Telemetry,
) -> Result<ProsodyConsumer<C>, ConsumerError>
where
    C: Codec,
    C::Payload: EventType + Clone + EventIdentity + Send + Sync + 'static,
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

    // Build the empty keyed-state backend the partition machinery requires.
    // Deduplication is structurally mandatory in the store layer, but with
    // no dedup/state middleware on this path the dedup store stays inert and
    // its oracle is never consulted — an empty registry stages nothing — so a
    // minimal cache capacity is sufficient.
    let stores = StorePair::new(
        trigger_store_config,
        consumer_config.mock,
        Duration::default(),
        NonZeroUsize::MIN,
        consumer_config.timer_spans,
        // The direct mode shares no infrastructure bundle.
        None,
    )
    .await?;
    let keyed_state = KeyedStateInputs::new(
        keyed_state_config,
        consumer_config,
        DEFAULT_IDEMPOTENCE_VERSION,
    )?;

    let services = StartupServices {
        version: keyed_state.version.clone(),
        telemetry: &telemetry,
        heartbeats,
        observer: KafkaObserver::new(&consumer_config.group_id),
    };

    match stores {
        StorePair::Memory {
            trigger_provider,
            dedup_provider,
            ..
        } => {
            let state_provider = memory_state_provider::<C>(
                &keyed_state,
                dedup_provider,
                MemoryCells::new(),
                MemoryDescriptorIdentityStore::new(),
                MemoryLoader::<C::Payload>::new(),
                // The direct mode rejects registrations, so nothing is ever
                // published: no publisher, no reconcile.
                None,
            );
            initialize_consumer::<_, _, _, C>(
                consumer_config,
                handler_provider,
                trigger_provider,
                state_provider,
                services,
            )
            .await
        }
        StorePair::Cassandra {
            trigger_provider, ..
        } => {
            // Stateless consumer: the `settle` boundary never runs and the
            // registry is provably empty (rejected above otherwise), so no
            // session can ever stage. Back keyed state with the inert memory
            // provider rather than `cassandra_state_provider`, which would
            // otherwise spawn a loader `BaseConsumer` + poll thread and
            // create the fjall cache dir for a backend that stages nothing.
            // The real Cassandra `trigger_provider` still drives the timer
            // system, and its per-partition store handle is what the (never
            // consulted) commit oracle receives at acquisition.
            let state_provider = memory_state_provider::<C>(
                &keyed_state,
                MemoryDeduplicationStoreProvider::new(),
                MemoryCells::new(),
                MemoryDescriptorIdentityStore::new(),
                MemoryLoader::<C::Payload>::new(),
                // Stateless: registrations are rejected, nothing publishes.
                None,
            );
            initialize_consumer::<_, _, _, C>(
                consumer_config,
                handler_provider,
                trigger_provider,
                state_provider,
                services,
            )
            .await
        }
    }
}
