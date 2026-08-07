//! The pipeline mode: retry, defer, and monopolization middleware layered
//! outside the common block.

use crate::consumer::config::{
    CommonConfiguration, ConsumerConfiguration, ConsumerSetup, PipelineMiddlewareConfiguration,
    TypedConsumerSetup,
};
use crate::consumer::error::ConsumerError;
use crate::consumer::kafka_context::PartitionProviders;
use crate::consumer::middleware::deduplication::DeduplicationStoreProvider;
use crate::consumer::middleware::defer::message::store::MessageDeferStoreProvider;
use crate::consumer::middleware::defer::timer::store::TimerDeferStoreProvider;
use crate::consumer::middleware::defer::{
    DeferConfiguration, FailureTracker, MessageDeferMiddleware, TimerDeferMiddleware,
};
use crate::consumer::middleware::monopolization::MonopolizationMiddleware;
use crate::consumer::middleware::retry::RetryMiddleware;
use crate::consumer::middleware::{FallibleHandler, HandlerMiddleware};
use crate::consumer::observer::KafkaObserver;
use crate::consumer::wiring::runtime::{StartupServices, initialize_consumer};
use crate::consumer::wiring::{
    build_common_middleware, build_typed_state, cassandra_deps, memory_deps,
};
use crate::consumer::{Managers, ProsodyConsumer};
use crate::heartbeat::HeartbeatRegistry;
use crate::high_level::config::TriggerStoreConfiguration;
use crate::loader::MessageLoader;
use crate::peer::{ConsumerRouter, NoPeer, responding_provider};
use crate::state::manager::{PartitionStateManager, PartitionStateProvider};
use crate::state::session::EventSession;
use crate::state_reader::ConsumerReaderBackend;
use crate::subsystem::SubsystemName;
use crate::telemetry::Telemetry;
use crate::timers::store::TriggerStoreProvider;
use crate::{Codec, EventIdentity, EventType};
use std::sync::Arc;

/// Everything any concrete backend needs to finish a pipeline consumer.
struct PipelineMiddlewareStack {
    consumer_config: ConsumerConfiguration,
    defer_config: DeferConfiguration,
    common_config: CommonConfiguration,
    failure_tracker: FailureTracker,
    monopolization_middleware: MonopolizationMiddleware,
    retry_middleware: RetryMiddleware,
    heartbeats: HeartbeatRegistry,
    telemetry: Telemetry,
    observer: KafkaObserver,
}

/// Groups the response route to keep consumer construction within seven
/// arguments.
struct ResponseTarget<'a, R> {
    router: &'a R,
    subsystem: SubsystemName,
}

impl PipelineMiddlewareStack {
    async fn into_consumer<T, MP, TP, DP, PP, SP, L, C>(
        self,
        message_defer_middleware: MessageDeferMiddleware<MP, L, FailureTracker>,
        timer_provider: TP,
        dedup_provider: DP,
        partition_providers: PartitionProviders<PP, SP>,
        handler: T,
    ) -> Result<ProsodyConsumer<C>, ConsumerError>
    where
        T: FallibleHandler<Payload = C::Payload> + Clone + Send + Sync + 'static,
        MP: MessageDeferStoreProvider,
        TP: TimerDeferStoreProvider,
        DP: DeduplicationStoreProvider,
        PP: TriggerStoreProvider,
        SP: PartitionStateProvider<PP::Store>,
        <SP::Manager as PartitionStateManager>::Session:
            EventSession<Loader: MessageLoader<Payload = C::Payload>>,
        L: MessageLoader<Payload = C::Payload> + 'static,
        C: Codec,
        C::Payload: Send + Sync + 'static + EventIdentity + EventType + Clone,
    {
        let timer_defer_middleware = TimerDeferMiddleware::new(
            self.defer_config,
            timer_provider,
            self.failure_tracker,
            &self.consumer_config,
            &self.telemetry,
        );

        let version: Arc<str> = Arc::from(self.common_config.dedup.version.as_str());

        // This stack runs outer→inner as:
        //
        //   retry → message_defer → timer_defer → monopolization
        //     → dedup → common(cancellation → scheduler → timeout
        //     → telemetry) → handler
        //
        // See `build_common_middleware` for what the common block owns. Two
        // placements here are required for correctness. Retry stays OUTERMOST
        // so every attempt is a fresh dispatch. The dedup filter sits INSIDE
        // message-defer so a deferred reload's duplicate check sees the reload
        // identity override.
        let common_middleware = build_common_middleware::<_, C::Payload>(
            &self.common_config,
            &self.consumer_config,
            self.telemetry.clone(),
            dedup_provider,
        )?;
        let middleware = common_middleware
            .layer(self.monopolization_middleware)
            .layer(timer_defer_middleware)
            .layer(message_defer_middleware)
            .layer(self.retry_middleware);
        let managers: Arc<Managers<C::Payload>> = Arc::default();
        let provider = middleware.into_provider(handler);
        let services = StartupServices {
            version,
            telemetry: &self.telemetry,
            heartbeats: self.heartbeats,
            observer: self.observer,
            managers: Arc::clone(&managers),
        };
        // Preparation is the last fallible step of this mode: no `?` after it
        // could drop a served listener.
        Box::pin(initialize_consumer::<_, _, _, C, _>(
            &self.consumer_config,
            provider,
            partition_providers,
            services,
            NoPeer,
        ))
        .await
    }

    /// Builds a responding consumer without a shared tail helper.
    ///
    /// The telemetry borrow overlaps moves from this value. Keep this tail in
    /// one function so the borrow remains clear.
    async fn into_responding_consumer<T, R, MP, TP, DP, PP, SP, L, C, RT: ConsumerRouter>(
        self,
        message_defer_middleware: MessageDeferMiddleware<MP, L, FailureTracker>,
        timer_provider: TP,
        dedup_provider: DP,
        partition_providers: PartitionProviders<PP, SP>,
        handler: T,
        response: ResponseTarget<'_, RT>,
    ) -> Result<ProsodyConsumer<C>, ConsumerError>
    where
        T: FallibleHandler<Payload = C::Payload> + Clone + Send + Sync + 'static,
        T::Output: Sync + 'static,
        T::Error: Sync + 'static,
        R: Codec<Payload = Result<T::Output, T::Error>>,
        MP: MessageDeferStoreProvider,
        TP: TimerDeferStoreProvider,
        DP: DeduplicationStoreProvider,
        PP: TriggerStoreProvider,
        SP: PartitionStateProvider<PP::Store>,
        <SP::Manager as PartitionStateManager>::Session:
            EventSession<Loader: MessageLoader<Payload = C::Payload>>,
        L: MessageLoader<Payload = C::Payload> + 'static,
        C: Codec,
        C::Payload: Send + Sync + 'static + EventIdentity + EventType + Clone,
    {
        let timer_defer_middleware = TimerDeferMiddleware::new(
            self.defer_config,
            timer_provider,
            self.failure_tracker,
            &self.consumer_config,
            &self.telemetry,
        );
        let version: Arc<str> = Arc::from(self.common_config.dedup.version.as_str());
        let common_middleware = build_common_middleware::<_, C::Payload>(
            &self.common_config,
            &self.consumer_config,
            self.telemetry.clone(),
            dedup_provider,
        )?;
        let middleware = common_middleware
            .layer(self.monopolization_middleware)
            .layer(timer_defer_middleware)
            .layer(message_defer_middleware)
            .layer(self.retry_middleware);
        let managers: Arc<Managers<C::Payload>> = Arc::default();
        let services = StartupServices {
            version,
            telemetry: &self.telemetry,
            heartbeats: self.heartbeats,
            observer: self.observer,
            managers: Arc::clone(&managers),
        };
        // Preparation is the last fallible step of this mode, and no `?` runs
        // between it and the termination below.
        let (provider, resources) = responding_provider::<_, R, _, _>(
            response.router,
            response.subsystem,
            &middleware,
            handler,
        );
        Box::pin(initialize_consumer::<_, _, _, C, _>(
            &self.consumer_config,
            provider,
            partition_providers,
            services,
            resources,
        ))
        .await
    }
}

impl<C: Codec> ProsodyConsumer<C>
where
    C::Payload: EventType + Clone,
{
    /// Creates a new `ProsodyConsumer` with a retry strategy for pipeline
    /// processing.
    ///
    /// Pipeline processing emphasizes reliability with automatic retries on
    /// failure. Messages that fail processing will be retried with
    /// exponential backoff. Includes monopolization detection to prevent
    /// single keys from consuming excessive processing time.
    ///
    /// # Errors
    ///
    /// Returns a `ConsumerError` if the consumer creation fails.
    pub async fn pipeline_consumer<T, RT: ConsumerRouter>(
        setup: ConsumerSetup<'_>,
        pipeline_config: PipelineMiddlewareConfiguration,
        telemetry: Telemetry,
        handler: T,
        router: &RT,
    ) -> Result<Self, ConsumerError>
    where
        C::Payload: EventIdentity,
        T: FallibleHandler<Payload = C::Payload> + Clone + Send + Sync + 'static,
    {
        match (setup.consumer.mock, setup.trigger_store) {
            (true, _) | (false, TriggerStoreConfiguration::InMemory) => {
                let deps = memory_deps(&setup);
                Self::pipeline_consumer_with_backend(
                    TypedConsumerSetup {
                        consumer: setup.consumer,
                        common: setup.common,
                        deps,
                    },
                    pipeline_config,
                    telemetry,
                    handler,
                    router,
                )
                .await
            }
            (false, TriggerStoreConfiguration::Cassandra(config)) => {
                let deps = cassandra_deps(&setup, config).await?;
                Self::pipeline_consumer_with_backend(
                    TypedConsumerSetup {
                        consumer: setup.consumer,
                        common: setup.common,
                        deps,
                    },
                    pipeline_config,
                    telemetry,
                    handler,
                    router,
                )
                .await
            }
        }
    }

    /// Creates a pipeline consumer that answers peer requests.
    ///
    /// The answer moves the handler's final result, so an answered record fires
    /// no `after_commit` on `handler`. See [`FallibleHandler`].
    ///
    /// # Errors
    ///
    /// Returns [`ConsumerError`] when another startup step fails.
    pub async fn pipeline_responding_consumer<T, R, RT: ConsumerRouter>(
        setup: ConsumerSetup<'_>,
        pipeline_config: PipelineMiddlewareConfiguration,
        telemetry: Telemetry,
        handler: T,
        router: &RT,
        subsystem: SubsystemName,
    ) -> Result<Self, ConsumerError>
    where
        C::Payload: EventIdentity,
        T: FallibleHandler<Payload = C::Payload> + Clone + Send + Sync + 'static,
        T::Output: Sync + 'static,
        T::Error: Sync + 'static,
        R: Codec<Payload = Result<T::Output, T::Error>>,
    {
        match (setup.consumer.mock, setup.trigger_store) {
            (true, _) | (false, TriggerStoreConfiguration::InMemory) => {
                let deps = memory_deps(&setup);
                Self::pipeline_responding_consumer_with_backend::<T, R, _, RT>(
                    TypedConsumerSetup {
                        consumer: setup.consumer,
                        common: setup.common,
                        deps,
                    },
                    pipeline_config,
                    telemetry,
                    handler,
                    router,
                    subsystem,
                )
                .await
            }
            (false, TriggerStoreConfiguration::Cassandra(config)) => {
                let deps = cassandra_deps(&setup, config).await?;
                Self::pipeline_responding_consumer_with_backend::<T, R, _, RT>(
                    TypedConsumerSetup {
                        consumer: setup.consumer,
                        common: setup.common,
                        deps,
                    },
                    pipeline_config,
                    telemetry,
                    handler,
                    router,
                    subsystem,
                )
                .await
            }
        }
    }

    pub(crate) async fn pipeline_consumer_with_backend<T, B, RT: ConsumerRouter>(
        setup: TypedConsumerSetup<'_, C, B>,
        pipeline_config: PipelineMiddlewareConfiguration,
        telemetry: Telemetry,
        handler: T,
        _router: &RT,
    ) -> Result<Self, ConsumerError>
    where
        C::Payload: EventIdentity + Send + Sync + 'static,
        B: ConsumerReaderBackend<C>,
        T: FallibleHandler<Payload = C::Payload> + Clone + Send + Sync + 'static,
    {
        let PipelineMiddlewareConfiguration {
            retry,
            monopolization,
            defer,
        } = pipeline_config;
        let (components, _keyed_state, heartbeats, observer) = build_typed_state(&setup).await?;
        let failure_tracker = FailureTracker::new(
            defer.failure_window,
            defer.failure_threshold,
            &telemetry,
            &heartbeats,
        );
        let stack = PipelineMiddlewareStack {
            consumer_config: setup.consumer.clone(),
            defer_config: defer.clone(),
            common_config: setup.common.clone(),
            failure_tracker: failure_tracker.clone(),
            monopolization_middleware: MonopolizationMiddleware::new(&monopolization, &telemetry)?,
            retry_middleware: RetryMiddleware::new(retry)?,
            heartbeats,
            telemetry,
            observer,
        };
        let message_defer = MessageDeferMiddleware::new(
            defer,
            setup.consumer,
            components.messages,
            failure_tracker,
            components.loader,
            &setup.common.dedup.version,
            &stack.telemetry,
        )?;
        stack
            .into_consumer::<_, _, _, _, _, _, _, C>(
                message_defer,
                components.timers,
                components.dedup,
                PartitionProviders {
                    triggers: components.trigger,
                    state: components.state,
                },
                handler,
            )
            .await
    }

    pub(crate) async fn pipeline_responding_consumer_with_backend<T, R, B, RT: ConsumerRouter>(
        setup: TypedConsumerSetup<'_, C, B>,
        pipeline_config: PipelineMiddlewareConfiguration,
        telemetry: Telemetry,
        handler: T,
        router: &RT,
        subsystem: SubsystemName,
    ) -> Result<Self, ConsumerError>
    where
        C::Payload: EventIdentity + Send + Sync + 'static,
        B: ConsumerReaderBackend<C>,
        T: FallibleHandler<Payload = C::Payload> + Clone + Send + Sync + 'static,
        T::Output: Sync + 'static,
        T::Error: Sync + 'static,
        R: Codec<Payload = Result<T::Output, T::Error>>,
    {
        let PipelineMiddlewareConfiguration {
            retry,
            monopolization,
            defer,
        } = pipeline_config;
        let (components, _keyed_state, heartbeats, observer) = build_typed_state(&setup).await?;
        let failure_tracker = FailureTracker::new(
            defer.failure_window,
            defer.failure_threshold,
            &telemetry,
            &heartbeats,
        );
        let stack = PipelineMiddlewareStack {
            consumer_config: setup.consumer.clone(),
            defer_config: defer.clone(),
            common_config: setup.common.clone(),
            failure_tracker: failure_tracker.clone(),
            monopolization_middleware: MonopolizationMiddleware::new(&monopolization, &telemetry)?,
            retry_middleware: RetryMiddleware::new(retry)?,
            heartbeats,
            telemetry,
            observer,
        };
        let message_defer = MessageDeferMiddleware::new(
            defer,
            setup.consumer,
            components.messages,
            failure_tracker,
            components.loader,
            &setup.common.dedup.version,
            &stack.telemetry,
        )?;
        stack
            .into_responding_consumer::<_, R, _, _, _, _, _, _, C, RT>(
                message_defer,
                components.timers,
                components.dedup,
                PartitionProviders {
                    triggers: components.trigger,
                    state: components.state,
                },
                handler,
                ResponseTarget { router, subsystem },
            )
            .await
    }
}
