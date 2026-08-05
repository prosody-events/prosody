//! The low-latency mode: retry, then route to a failure topic, then retry that
//! — all layered outside the common block.
//!
//! The three layers run in that sequence. The inner retry caps transient errors
//! at `max_retries`. The failure topic then routes the exhausted failure. The
//! outermost retry re-dispatches that routing forever, since there is nothing
//! left to fall back to.

use crate::consumer::config::{
    ConsumerSetup, LowLatencyMiddlewareConfiguration, TypedConsumerSetup,
};
use crate::consumer::error::{ConsumerError, PeerInitError};
use crate::consumer::kafka_context::PartitionProviders;
use crate::consumer::middleware::retry::RetryMiddleware;
use crate::consumer::middleware::topic::FailureTopicMiddleware;
use crate::consumer::middleware::{FallibleHandler, HandlerMiddleware};
use crate::consumer::wiring::peer::{NoPeer, prepare_requester, prepare_responding};
use crate::consumer::wiring::runtime::{StartupServices, initialize_consumer};
use crate::consumer::wiring::{
    build_common_middleware, build_typed_state, cassandra_deps, memory_deps,
};
use crate::consumer::{Managers, ProsodyConsumer};
use crate::high_level::config::TriggerStoreConfiguration;
use crate::producer::ProsodyProducer;
use crate::state_reader::ConsumerReaderBackend;
use crate::telemetry::Telemetry;
use crate::{Codec, EventIdentity, EventType};
use std::sync::Arc;

impl<C: Codec> ProsodyConsumer<C>
where
    C::Payload: EventType + Clone,
{
    /// Creates a new `ProsodyConsumer` with a low-latency strategy.
    ///
    /// The low-latency strategy prioritizes throughput by quickly moving
    /// problematic messages to a failure topic instead of retrying
    /// indefinitely. This strategy:
    ///
    /// 1. First attempts to process the message with retries
    /// 2. If processing still fails, sends the message to a failure topic
    /// 3. Retries sending to the failure topic if that fails
    ///
    /// # Errors
    ///
    /// Returns a `ConsumerError` if the consumer creation fails.
    pub async fn low_latency_consumer<T>(
        setup: ConsumerSetup<'_>,
        low_latency_config: LowLatencyMiddlewareConfiguration,
        producer: ProsodyProducer<C>,
        telemetry: Telemetry,
        handler: T,
    ) -> Result<Self, ConsumerError>
    where
        C::Payload: EventIdentity + Send + Sync + 'static,
        T: FallibleHandler<Payload = C::Payload> + Clone + Send + Sync + 'static,
    {
        match (setup.consumer.mock, setup.trigger_store) {
            (true, _) | (false, TriggerStoreConfiguration::InMemory) => {
                let deps = memory_deps(&setup);
                Self::low_latency_consumer_with_backend(
                    TypedConsumerSetup {
                        consumer: setup.consumer,
                        common: setup.common,
                        deps,
                    },
                    low_latency_config,
                    producer,
                    telemetry,
                    handler,
                )
                .await
            }
            (false, TriggerStoreConfiguration::Cassandra(config)) => {
                let deps = cassandra_deps(&setup, config).await?;
                Self::low_latency_consumer_with_backend(
                    TypedConsumerSetup {
                        consumer: setup.consumer,
                        common: setup.common,
                        deps,
                    },
                    low_latency_config,
                    producer,
                    telemetry,
                    handler,
                )
                .await
            }
        }
    }

    /// Creates a low-latency consumer that answers peer requests.
    ///
    /// The answer moves the handler's final result, so an answered record fires
    /// no `after_commit` on `handler`. See [`FallibleHandler`].
    ///
    /// # Errors
    ///
    /// Returns [`PeerInitError::PeerRequired`] without peer configuration.
    /// Returns [`PeerInitError::SubsystemRequired`] without a subsystem name.
    /// Returns [`ConsumerError`] when another startup step fails.
    pub async fn low_latency_responding_consumer<T, R>(
        setup: ConsumerSetup<'_>,
        low_latency_config: LowLatencyMiddlewareConfiguration,
        producer: ProsodyProducer<C>,
        telemetry: Telemetry,
        handler: T,
    ) -> Result<Self, ConsumerError>
    where
        C::Payload: EventIdentity + Send + Sync + 'static,
        T: FallibleHandler<Payload = C::Payload> + Clone + Send + Sync + 'static,
        T::Output: Sync + 'static,
        T::Error: Sync + 'static,
        R: Codec<Payload = Result<T::Output, T::Error>>,
    {
        match (setup.consumer.mock, setup.trigger_store) {
            (true, _) | (false, TriggerStoreConfiguration::InMemory) => {
                let deps = memory_deps(&setup);
                Self::low_latency_responding_consumer_with_backend::<T, R, _>(
                    TypedConsumerSetup {
                        consumer: setup.consumer,
                        common: setup.common,
                        deps,
                    },
                    low_latency_config,
                    producer,
                    telemetry,
                    handler,
                )
                .await
            }
            (false, TriggerStoreConfiguration::Cassandra(config)) => {
                let deps = cassandra_deps(&setup, config).await?;
                Self::low_latency_responding_consumer_with_backend::<T, R, _>(
                    TypedConsumerSetup {
                        consumer: setup.consumer,
                        common: setup.common,
                        deps,
                    },
                    low_latency_config,
                    producer,
                    telemetry,
                    handler,
                )
                .await
            }
        }
    }

    pub(crate) async fn low_latency_consumer_with_backend<T, B>(
        setup: TypedConsumerSetup<'_, C, B>,
        low_latency_config: LowLatencyMiddlewareConfiguration,
        producer: ProsodyProducer<C>,
        telemetry: Telemetry,
        handler: T,
    ) -> Result<Self, ConsumerError>
    where
        C::Payload: EventIdentity + Send + Sync + 'static,
        B: ConsumerReaderBackend<C>,
        T: FallibleHandler<Payload = C::Payload> + Clone + Send + Sync + 'static,
    {
        let (components, keyed_state, heartbeats, observer) = build_typed_state(&setup).await?;
        let retry = RetryMiddleware::new(low_latency_config.retry)?;
        let topic = FailureTopicMiddleware::new(
            low_latency_config.failure_topic,
            setup.consumer.group_id.clone(),
            producer,
        )?;
        let middleware = build_common_middleware::<_, C::Payload>(
            setup.common,
            setup.consumer,
            telemetry.clone(),
            components.dedup,
        )?
        .layer(retry.clone())
        .layer(topic)
        .layer(retry);
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

    pub(crate) async fn low_latency_responding_consumer_with_backend<T, R, B>(
        setup: TypedConsumerSetup<'_, C, B>,
        low_latency_config: LowLatencyMiddlewareConfiguration,
        producer: ProsodyProducer<C>,
        telemetry: Telemetry,
        handler: T,
    ) -> Result<Self, ConsumerError>
    where
        C::Payload: EventIdentity + Send + Sync + 'static,
        B: ConsumerReaderBackend<C>,
        T: FallibleHandler<Payload = C::Payload> + Clone + Send + Sync + 'static,
        T::Output: Sync + 'static,
        T::Error: Sync + 'static,
        R: Codec<Payload = Result<T::Output, T::Error>>,
    {
        let (components, keyed_state, heartbeats, observer) = build_typed_state(&setup).await?;
        let retry = RetryMiddleware::new(low_latency_config.retry)?;
        let topic = FailureTopicMiddleware::new(
            low_latency_config.failure_topic,
            setup.consumer.group_id.clone(),
            producer,
        )?;
        let middleware = build_common_middleware::<_, C::Payload>(
            setup.common,
            setup.consumer,
            telemetry.clone(),
            components.dedup,
        )?
        .layer(retry.clone())
        .layer(topic)
        .layer(retry);
        let managers: Arc<Managers<C::Payload>> = Arc::default();
        let peer = setup
            .common
            .peer
            .as_ref()
            .ok_or(PeerInitError::PeerRequired)?;
        let subsystem = keyed_state
            .subsystem()
            .cloned()
            .ok_or(PeerInitError::SubsystemRequired)?;
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
        // Preparation is the last fallible step of this mode, and no `?` runs
        // between it and the termination below.
        let prepared = prepare_responding::<R, _, _>(
            peer,
            setup.deps.backend().as_ref(),
            subsystem,
            managers,
            &services.heartbeats,
        )
        .await?;
        let (provider, attach) = prepared.terminate(&middleware, handler);
        Box::pin(initialize_consumer::<_, _, _, C, _>(
            setup.consumer,
            provider,
            providers,
            services,
            attach,
        ))
        .await
    }
}
