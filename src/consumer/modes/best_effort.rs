//! The best-effort mode: one logging layer outside the common block. A failure
//! is logged once and never retried; the settle boundary then settles the event
//! as it stands.

use crate::consumer::ProsodyConsumer;
use crate::consumer::config::ConsumerSetup;
use crate::consumer::error::ConsumerError;
use crate::consumer::middleware::log::LogMiddleware;
use crate::consumer::middleware::{FallibleHandler, HandlerMiddleware};
use crate::consumer::storage::StorePair;
use crate::consumer::wiring::runtime::{StartupServices, initialize_consumer};
use crate::consumer::wiring::state::{
    cassandra_loader, cassandra_state_provider, memory_arm_inputs, memory_state_provider,
};
use crate::consumer::wiring::{build_common_middleware, build_shared_state};
use crate::telemetry::Telemetry;
use crate::{Codec, EventIdentity, EventType};

impl<C: Codec> ProsodyConsumer<C>
where
    C::Payload: EventType + Clone,
{
    /// Creates a new `ProsodyConsumer` with logging middleware for failure
    /// handling.
    ///
    /// The best-effort approach is the simplest — it tries to process
    /// messages once, logs any failures, and moves on. This approach should
    /// only be used for development or for services where occasional
    /// message loss is acceptable.
    pub(crate) async fn best_effort_consumer<T>(
        setup: ConsumerSetup<'_, C>,
        telemetry: Telemetry,
        handler: T,
    ) -> Result<Self, ConsumerError>
    where
        C::Payload: EventIdentity + Send + Sync + 'static,
        T: FallibleHandler<Payload = C::Payload> + Clone + Send + Sync + 'static,
    {
        let (stores, keyed_state, heartbeats, shared, observer) =
            build_shared_state(&setup).await?;

        let services = StartupServices {
            version: keyed_state.version.clone(),
            telemetry: &telemetry,
            heartbeats,
            observer,
        };

        // dedup lives inside the common block; `log` layers OUTSIDE it and forwards
        // the failure verbatim. Nothing retries it, so the `settle` boundary
        // settles the event. Built per storage arm because the dedup store lives
        // there.
        match stores {
            StorePair::Memory {
                trigger_provider,
                dedup_provider,
                publication_store,
                ..
            } => {
                let (loader, cells, identities) = memory_arm_inputs(setup.deps.as_ref(), shared)?;
                let publisher_template = keyed_state
                    .memory_publication_setup(publication_store)
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
                .layer(LogMiddleware::new())
                .into_provider(handler);
                initialize_consumer::<_, _, _, C>(
                    setup.consumer,
                    provider,
                    trigger_provider,
                    state_provider,
                    services,
                )
                .await
            }
            StorePair::Cassandra {
                trigger_provider,
                dedup_provider,
                cell_store,
                identity_store,
                publication_store,
                ..
            } => {
                let loader =
                    cassandra_loader(setup.deps.as_ref(), setup.consumer, &services.heartbeats)?;
                let publisher_template = keyed_state
                    .cassandra_publication_setup(publication_store, services.observer.clone())
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
                .layer(LogMiddleware::new())
                .into_provider(handler);
                initialize_consumer::<_, _, _, C>(
                    setup.consumer,
                    provider,
                    trigger_provider,
                    state_provider,
                    services,
                )
                .await
            }
        }
    }
}
