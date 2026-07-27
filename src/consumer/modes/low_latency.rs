//! The low-latency mode: retry, then route to a failure topic, then retry that
//! — all layered outside the common block.
//!
//! The three layers run in that sequence. The inner retry caps transient errors
//! at `max_retries`. The failure topic then routes the exhausted failure. The
//! outermost retry re-dispatches that routing forever, since there is nothing
//! left to fall back to.

use crate::consumer::ProsodyConsumer;
use crate::consumer::config::{ConsumerSetup, LowLatencyMiddlewareConfiguration};
use crate::consumer::error::ConsumerError;
use crate::consumer::middleware::retry::RetryMiddleware;
use crate::consumer::middleware::topic::FailureTopicMiddleware;
use crate::consumer::middleware::{FallibleHandler, HandlerMiddleware};
use crate::consumer::storage::StorePair;
use crate::consumer::wiring::runtime::{StartupServices, initialize_consumer};
use crate::consumer::wiring::state::{
    cassandra_arm_inputs, cassandra_state_provider, memory_arm_inputs, memory_state_provider,
};
use crate::consumer::wiring::{build_common_middleware, build_shared_state};
use crate::producer::ProsodyProducer;
use crate::state::first_write::PublicationBackend;
use crate::telemetry::Telemetry;
use crate::{Codec, EventIdentity, EventType};

pub(super) async fn build<T, C>(
    setup: ConsumerSetup<'_, C>,
    low_latency_config: LowLatencyMiddlewareConfiguration,
    producer: ProsodyProducer<C>,
    telemetry: Telemetry,
    handler: T,
) -> Result<ProsodyConsumer<C>, ConsumerError>
where
    C: Codec,
    C::Payload: EventType + Clone + EventIdentity + Send + Sync + 'static,
    T: FallibleHandler<Payload = C::Payload> + Clone + Send + Sync + 'static,
{
    let (stores, keyed_state, heartbeats, shared, observer) = build_shared_state(&setup).await?;
    let version = keyed_state.version.clone();
    let retry_middleware = RetryMiddleware::new(low_latency_config.retry)?;
    let topic_middleware = FailureTopicMiddleware::new(
        low_latency_config.failure_topic,
        setup.consumer.group_id.clone(),
        producer,
    )?;
    // One `StartupServices` for the whole construction: whichever storage arm
    // runs moves the same observer into the primary consumer.
    let services = StartupServices {
        version,
        telemetry: &telemetry,
        heartbeats,
        observer,
    };
    // dedup is inside the common block; failure-topic/retry layer OUTSIDE it.
    match stores {
        StorePair::Memory {
            trigger_provider,
            dedup_provider,
            publication_store,
            ..
        } => {
            let (loader, cells, identities, partition_counts) =
                memory_arm_inputs(setup.deps.as_ref(), shared)?;
            let publisher_template = keyed_state
                .publication_setup(
                    PublicationBackend::Memory(publication_store),
                    partition_counts,
                )
                .await?;
            let state_provider = memory_state_provider::<C>(
                &keyed_state,
                dedup_provider.clone(),
                cells,
                identities,
                loader,
                publisher_template,
            );
            let provider = build_common_middleware::<_, C::Payload>(
                setup.common,
                setup.consumer,
                telemetry.clone(),
                dedup_provider,
            )?
            .layer(retry_middleware.clone())
            .layer(topic_middleware)
            .layer(retry_middleware)
            .into_provider(handler);
            initialize_consumer::<_, _, _, C>(
                setup.consumer,
                provider,
                trigger_provider,
                state_provider,
                services,
            )
        }
        StorePair::Cassandra {
            trigger_provider,
            dedup_provider,
            cell_store,
            identity_store,
            publication_store,
            ..
        } => {
            let (loader, partition_counts) =
                cassandra_arm_inputs(setup.deps.as_ref(), setup.consumer, &services.heartbeats)?;
            let publisher_template = keyed_state
                .publication_setup(
                    PublicationBackend::Cassandra(publication_store),
                    partition_counts,
                )
                .await?;
            let state_provider = cassandra_state_provider::<C>(
                &keyed_state,
                dedup_provider.clone(),
                cell_store,
                identity_store,
                loader,
                publisher_template,
            )?;
            let provider = build_common_middleware::<_, C::Payload>(
                setup.common,
                setup.consumer,
                telemetry.clone(),
                dedup_provider,
            )?
            .layer(retry_middleware.clone())
            .layer(topic_middleware)
            .layer(retry_middleware)
            .into_provider(handler);
            initialize_consumer::<_, _, _, C>(
                setup.consumer,
                provider,
                trigger_provider,
                state_provider,
                services,
            )
        }
    }
}
