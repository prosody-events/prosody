//! What every mode does before its mode-specific work: validate the
//! configuration, open storage, and build the common middleware block.
//!
//! Two submodules complete the assembly. [`state`] derives the keyed-state
//! inputs and the per-backend state providers. [`runtime`] takes the finished
//! middleware stack and starts the consumer: Kafka client, subscription, and
//! poll loop.

use crate::cassandra::config::CassandraConfiguration;
use crate::consumer::config::TypedConsumerSetup;
use crate::consumer::config::{
    CommonConfiguration, ConsumerConfiguration, ConsumerSetup, validate_recovery_ttl_margin,
};
use crate::consumer::error::{ConsumerError, KeyedStateInitError};
use crate::consumer::middleware::cancellation::CancellationMiddleware;
use crate::consumer::middleware::deduplication::{
    DeduplicationMiddleware, DeduplicationStoreProvider,
};
use crate::consumer::middleware::scheduler::SchedulerMiddleware;
use crate::consumer::middleware::telemetry::TelemetryMiddleware;
use crate::consumer::middleware::timeout::TimeoutMiddleware;
use crate::consumer::middleware::{ComposedMiddleware, HandlerMiddleware};
use crate::consumer::observer::KafkaObserver;
use crate::consumer::storage::{ComponentsOf, ConsumerStorageBackend, ConsumerStorageInputs};
use crate::consumer::wiring::state::KeyedStateInputs;
use crate::heartbeat::HeartbeatRegistry;
use crate::loader::LoaderConfiguration;
use crate::loader::MemoryLoader;
use crate::state::memory::{MemoryCells, MemoryDescriptorIdentityStore, MemoryPublicationStore};
use crate::state_reader::ConsumerReaderBackend;
use crate::state_reader::{CassandraReaderBackend, MemoryReaderBackend, StateReaderDependencies};
use crate::telemetry::Telemetry;
use crate::{Codec, EventIdentity};
use std::sync::Arc;
use validator::Validate;

pub(in crate::consumer) mod peer;
pub(in crate::consumer) mod runtime;
pub(in crate::consumer) mod state;

#[cfg(test)]
mod tests;

/// The concrete common-block composition `build_common_middleware` returns
/// (innermost `telemetry` to outermost `dedup`). Named — not an opaque `impl
/// HandlerMiddleware` — so the chains layered on top stay fully concrete and
/// the crate-internal settlement classification of the composed handler
/// remains provable at the `EventHandler` boundary.
pub(in crate::consumer) type CommonMiddleware<DP, P> = ComposedMiddleware<
    DeduplicationMiddleware<DP, P>,
    ComposedMiddleware<
        CancellationMiddleware,
        ComposedMiddleware<
            SchedulerMiddleware,
            ComposedMiddleware<TimeoutMiddleware, TelemetryMiddleware, P>,
            P,
        >,
        P,
    >,
    P,
>;

/// Validates shared configuration and builds one already-selected backend.
pub(in crate::consumer) async fn build_typed_state<C, B>(
    setup: &TypedConsumerSetup<'_, C, B>,
) -> Result<
    (
        ComponentsOf<C, B>,
        KeyedStateInputs,
        HeartbeatRegistry,
        KafkaObserver,
    ),
    ConsumerError,
>
where
    C: Codec,
    C::Payload: Clone + EventIdentity + crate::EventType + Send + Sync + 'static,
    B: ConsumerReaderBackend<C> + ConsumerStorageBackend<C>,
{
    setup.consumer.validate()?;
    let keyed_state_config = setup.common.keyed_state.clone();
    keyed_state_config.validate()?;
    let dedup = setup.common.dedup.clone();
    if keyed_state_config.has_registrations() {
        validate_recovery_ttl_margin(dedup.ttl, keyed_state_config.recovery_delay)
            .map_err(KeyedStateInitError::from)?;
    }
    let heartbeats = setup.deps.heartbeats().clone();
    let keyed_state = KeyedStateInputs::new(keyed_state_config, setup.consumer, &dedup.version)?;
    let observer = KafkaObserver::new(&setup.consumer.group_id);
    let components = setup
        .deps
        .build_consumer_components(
            ConsumerStorageInputs {
                dedup_ttl: dedup.ttl,
                dedup_cache_capacity: dedup.cache_capacity,
                timer_spans: setup.consumer.timer_spans,
            },
            &keyed_state,
            observer.clone(),
        )
        .await?;
    Ok((components, keyed_state, heartbeats, observer))
}

pub(in crate::consumer) fn memory_deps<C>(
    setup: &ConsumerSetup<'_>,
) -> StateReaderDependencies<C, MemoryReaderBackend<C>>
where
    C: Codec,
    C::Payload: Clone,
{
    StateReaderDependencies::memory(
        setup.consumer.group_id.clone(),
        setup.consumer.stall_threshold,
        MemoryCells::new(),
        MemoryPublicationStore::new(),
        MemoryDescriptorIdentityStore::new(),
        MemoryLoader::new(),
        setup.common.keyed_state.reader_cache_size(),
    )
    .with_default_read_cache_ttl(setup.common.keyed_state.read_cache_ttl)
}

pub(in crate::consumer) async fn cassandra_deps<C>(
    setup: &ConsumerSetup<'_>,
    config: &CassandraConfiguration,
) -> Result<StateReaderDependencies<C, CassandraReaderBackend<C>>, ConsumerError>
where
    C: Codec,
    C::Payload: Clone,
{
    Ok(StateReaderDependencies::cassandra_with_loader(
        config,
        LoaderConfiguration::for_consumer(
            setup.consumer,
            setup.common.keyed_state.subsystem.as_ref(),
        ),
        setup.common.keyed_state.reader_cache_size(),
        setup.consumer.stall_threshold,
    )
    .await?
    .with_default_read_cache_ttl(setup.common.keyed_state.read_cache_ttl))
}

/// Builds the common middleware shared by every mode — the single place any
/// common-middleware component is constructed.
///
/// This is the whole cross-mode set: telemetry, timeout, scheduler,
/// cancellation, and **deduplication** (the mandatory commit oracle, here a
/// stateless duplicate filter over `context.message_marker()`; the `settle`
/// boundary records the marker directly, gated on the typed `Settlement`
/// classification — not in this stack). It runs outer→inner as
/// `dedup → cancellation → scheduler → timeout → telemetry → handler`. Each
/// mode layers only its *mode-specific* middleware (retry, monopolization,
/// defer, failure-topic, log) OUTSIDE the returned block.
///
/// The associated backend family supplies the concrete deduplication provider.
pub(in crate::consumer) fn build_common_middleware<DP, P>(
    config: &CommonConfiguration,
    consumer_config: &ConsumerConfiguration,
    telemetry: Telemetry,
    dedup_provider: DP,
) -> Result<CommonMiddleware<DP, P>, ConsumerError>
where
    DP: DeduplicationStoreProvider,
    P: Send + Sync + 'static + EventIdentity,
{
    let scheduler_middleware = SchedulerMiddleware::new(&config.scheduler, &telemetry)?;
    let timeout_middleware =
        TimeoutMiddleware::new(&config.timeout, consumer_config.stall_threshold)?;
    let telemetry_middleware =
        TelemetryMiddleware::new(telemetry, Arc::from(consumer_config.group_id.as_str()));
    let dedup_middleware =
        DeduplicationMiddleware::new(&config.dedup, &consumer_config.group_id, dedup_provider)?;

    Ok(telemetry_middleware
        .layer(timeout_middleware)
        .layer(scheduler_middleware)
        .layer(CancellationMiddleware::new())
        .layer(dedup_middleware))
}
