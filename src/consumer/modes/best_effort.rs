//! The best-effort mode: one logging layer outside the common block. A failure
//! is logged once and never retried; the settle boundary then settles the event
//! as it stands.

use super::ResponsePolicy;
use crate::consumer::config::TypedConsumerSetup;
use crate::consumer::error::ConsumerError;
use crate::consumer::kafka_context::PartitionProviders;
use crate::consumer::middleware::log::LogMiddleware;
use crate::consumer::middleware::{FallibleHandler, HandlerMiddleware};
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
    pub(crate) async fn best_effort_consumer_with_policy<T, B, RP>(
        setup: TypedConsumerSetup<'_, C, B>,
        telemetry: Telemetry,
        handler: T,
        response: RP,
    ) -> Result<Self, ConsumerError>
    where
        C::Payload: EventIdentity + Send + Sync + 'static,
        B: ConsumerReaderBackend<C>,
        T: FallibleHandler<Payload = C::Payload> + Clone + Send + Sync + 'static,
        RP: ResponsePolicy<T>,
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
        let (leaf, resources) = response.terminate(handler);
        let provider = middleware.with_provider(leaf);
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
        };
        Box::pin(initialize_consumer::<_, _, _, C, _>(
            setup.consumer,
            provider,
            providers,
            services,
            resources,
        ))
        .await
    }
}
