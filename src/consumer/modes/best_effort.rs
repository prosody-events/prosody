//! The best-effort mode: one logging layer outside the common block. A failure
//! is logged once and never retried; the settle boundary then settles the event
//! as it stands.

use crate::consumer::config::TypedConsumerSetup;
use crate::consumer::error::ConsumerError;
use crate::consumer::kafka_context::PartitionProviders;
use crate::consumer::middleware::log::LogMiddleware;
use crate::consumer::middleware::{FallibleHandler, HandlerMiddleware};
use crate::consumer::wiring::peer::{NoPeer, prepare_requester};
use crate::consumer::wiring::runtime::{StartupServices, initialize_consumer};
use crate::consumer::wiring::{build_common_middleware, build_typed_state};
use crate::consumer::{Managers, ProsodyConsumer};
use crate::state_reader::ConsumerReaderBackend;
use crate::telemetry::Telemetry;
use crate::{Codec, EventIdentity, EventType};
use std::sync::Arc;

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
    pub(crate) async fn best_effort_consumer<T, B>(
        setup: TypedConsumerSetup<'_, C, B>,
        telemetry: Telemetry,
        handler: T,
    ) -> Result<Self, ConsumerError>
    where
        C::Payload: EventIdentity + Send + Sync + 'static,
        B: ConsumerReaderBackend<C>,
        T: FallibleHandler<Payload = C::Payload> + Clone + Send + Sync + 'static,
    {
        let (components, keyed_state, heartbeats, observer) = build_typed_state(&setup).await?;

        let middleware = build_common_middleware::<_, C::Payload>(
            setup.common,
            setup.consumer,
            telemetry.clone(),
            components.dedup,
        )?
        .layer(LogMiddleware::new());
        let managers: Arc<Managers<C::Payload>> = Arc::default();
        let provider = middleware.into_provider(handler);
        let providers = PartitionProviders {
            triggers: components.trigger,
            state: components.state,
        };
        let services = StartupServices {
            version: keyed_state.version.clone(),
            telemetry: &telemetry,
            heartbeats,
            observer,
            managers: Arc::clone(&managers),
            responder: keyed_state.subsystem().cloned(),
        };
        // Preparation is the last fallible step of this mode: no `?` after it
        // could drop a served listener.
        match setup.common.peer.as_ref() {
            Some(peer) => {
                let attach = prepare_requester(
                    peer,
                    setup.deps.backend().as_ref(),
                    managers,
                    &services.heartbeats,
                )
                .await?;
                Box::pin(initialize_consumer::<_, _, _, C, _>(
                    setup.consumer,
                    provider,
                    providers,
                    services,
                    attach,
                ))
                .await
            }
            None => {
                Box::pin(initialize_consumer::<_, _, _, C, _>(
                    setup.consumer,
                    provider,
                    providers,
                    services,
                    NoPeer,
                ))
                .await
            }
        }
    }
}
