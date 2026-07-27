//! What every mode does before its mode-specific work: validate the
//! configuration, open storage, and build the common middleware block.
//!
//! Two submodules complete the assembly. [`state`] derives the keyed-state
//! inputs and the per-backend state providers. [`runtime`] takes the finished
//! middleware stack and starts the consumer: Kafka client, subscription, and
//! poll loop.

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
use crate::consumer::storage::{SharedStorage, StorePair};
use crate::consumer::wiring::state::KeyedStateInputs;
use crate::heartbeat::HeartbeatRegistry;
use crate::state_reader::SharedDeps;
use crate::telemetry::Telemetry;
use crate::{Codec, EventIdentity};
use std::sync::Arc;
use validator::Validate;

pub(in crate::consumer) mod runtime;
pub(in crate::consumer) mod state;

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

/// What [`build_shared_state`] returns: the store pair, keyed-state inputs, the
/// heartbeat registry, any storage reused from a bundle, and the consumer's one
/// Kafka observation handle.
pub(in crate::consumer) type SharedState = (
    StorePair,
    KeyedStateInputs,
    HeartbeatRegistry,
    Option<SharedStorage>,
    KafkaObserver,
);

/// Builds the core shared by every constructor: the trigger and defer store
/// pair, keyed-state inputs, the heartbeat registry, any storage reused from a
/// [`SharedDeps`] bundle, and the consumer's Kafka observation handle.
///
/// The observer is created here, before any storage `match`, so every arm of
/// every mode threads the same instance to its primary consumer. A second
/// observer would silently split the observation stream.
///
/// Validates the consumer and keyed-state configuration up front, before
/// [`StorePair::new`]'s Cassandra IO, so all callers fail fast uniformly. The
/// canonical `consumer_config.validate()` in
/// [`initialize_consumer`](runtime::initialize_consumer) remains the single
/// invariant chokepoint; this early validation is the fail-fast guard.
pub(in crate::consumer) async fn build_shared_state<C: Codec>(
    setup: &ConsumerSetup<'_, C>,
) -> Result<SharedState, ConsumerError> {
    let ConsumerSetup {
        consumer: consumer_config,
        trigger_store: trigger_store_config,
        common: common_config,
        deps,
    } = setup;
    let deps = deps.as_ref();
    consumer_config.validate()?;
    // Clone (never drain) the registered keyed-state config: callers pass
    // `common_config` by reference and retain the intact configuration, so a
    // re-subscribe rebuilds the registry from the same registrations and
    // existing `Registered<_>` tokens stay valid.
    let keyed_state_config = common_config.keyed_state.clone();
    keyed_state_config.validate()?;
    let dedup_config = common_config.dedup.clone();
    // The commit oracle is the dedup marker; a provisional cell must resolve
    // while its marker still lives, so the dedup TTL must clear the recovery
    // window with margin (see `validate_recovery_ttl_margin`). Only gate
    // this when state is actually registered — an inert state layer arms no
    // backstop, so a short dedup TTL on a stateless consumer is harmless.
    if keyed_state_config.has_registrations() {
        validate_recovery_ttl_margin(dedup_config.ttl, keyed_state_config.recovery_delay)
            .map_err(KeyedStateInitError::from)?;
    }
    // Reuse the bundle's already-constructed storage when one is supplied, so
    // no second session/publication store is built on the Configured→Running
    // transition.
    let shared = deps.map(SharedDeps::shared_storage);
    // Take the bundle's heartbeat registry when supplied, so the consumer's
    // stall probe covers the shared loader's poll-loop heartbeat. Build a fresh
    // one only on the no-bundle path.
    let heartbeats = match deps {
        Some(deps) => deps.heartbeats().clone(),
        None => HeartbeatRegistry::new(
            consumer_config.group_id.clone(),
            consumer_config.stall_threshold,
        ),
    };
    // Create both stores atomically — ensures trigger and defer stores match.
    let stores = StorePair::new(
        trigger_store_config,
        consumer_config.mock,
        dedup_config.ttl,
        dedup_config.cache_capacity,
        consumer_config.timer_spans,
        shared.as_ref(),
    )
    .await?;
    let keyed_state =
        KeyedStateInputs::new(keyed_state_config, consumer_config, &dedup_config.version)?;
    let observer = KafkaObserver::new(&consumer_config.group_id);
    Ok((stores, keyed_state, heartbeats, shared, observer))
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
/// Because the deduplication middleware needs the per-partition dedup store,
/// this is called INSIDE the storage `match` arm where the concrete
/// `dedup_provider` is in hand.
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
