//! The pipeline mode: retry, defer, and monopolization middleware layered
//! outside the common block.

use crate::consumer::config::{
    CommonConfiguration, ConsumerConfiguration, ConsumerSetup, PipelineMiddlewareConfiguration,
    TypedConsumerSetup,
};
use crate::consumer::error::{ConsumerError, PeerInitError};
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
use crate::consumer::wiring::peer::{NoPeer, prepare_requester, prepare_responding};
use crate::consumer::wiring::runtime::{StartupServices, initialize_consumer};
use crate::consumer::wiring::{
    build_common_middleware, build_typed_state, cassandra_deps, memory_deps,
};
use crate::consumer::{Managers, ProsodyConsumer};
use crate::heartbeat::HeartbeatRegistry;
use crate::high_level::config::TriggerStoreConfiguration;
use crate::loader::MessageLoader;
use crate::state::manager::{PartitionStateManager, PartitionStateProvider};
use crate::state::session::CellWrite;
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
    subsystem: Option<SubsystemName>,
}

impl PipelineMiddlewareStack {
    async fn into_consumer<T, MP, TP, DP, PP, SP, L, C, B>(
        self,
        message_defer_middleware: MessageDeferMiddleware<MP, L, FailureTracker>,
        timer_provider: TP,
        dedup_provider: DP,
        partition_providers: PartitionProviders<PP, SP>,
        handler: T,
        backend: &B,
    ) -> Result<ProsodyConsumer<C>, ConsumerError>
    where
        T: FallibleHandler<Payload = C::Payload> + Clone + Send + Sync + 'static,
        MP: MessageDeferStoreProvider,
        TP: TimerDeferStoreProvider,
        DP: DeduplicationStoreProvider,
        PP: TriggerStoreProvider,
        SP: PartitionStateProvider<PP::Store>,
        <SP::Manager as PartitionStateManager>::Session:
            CellWrite<Loader: MessageLoader<Payload = C::Payload>>,
        L: MessageLoader<Payload = C::Payload> + 'static,
        C: Codec,
        C::Payload: Send + Sync + 'static + EventIdentity + EventType + Clone,
        B: ConsumerReaderBackend<C>,
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
            responder: None,
        };
        // Preparation is the last fallible step of this mode: no `?` after it
        // could drop a served listener.
        match self.common_config.peer.as_ref() {
            Some(peer) => {
                let attach =
                    prepare_requester(peer, backend, managers, &services.heartbeats).await?;
                Box::pin(initialize_consumer::<_, _, _, C, _>(
                    &self.consumer_config,
                    provider,
                    partition_providers,
                    services,
                    attach,
                ))
                .await
            }
            None => {
                Box::pin(initialize_consumer::<_, _, _, C, _>(
                    &self.consumer_config,
                    provider,
                    partition_providers,
                    services,
                    NoPeer,
                ))
                .await
            }
        }
    }

    /// Builds a responding consumer without a shared tail helper.
    ///
    /// The telemetry borrow overlaps moves from this value. Keep this tail in
    /// one function so the borrow remains clear.
    async fn into_responding_consumer<T, R, MP, TP, DP, PP, SP, L, C, B>(
        self,
        message_defer_middleware: MessageDeferMiddleware<MP, L, FailureTracker>,
        timer_provider: TP,
        dedup_provider: DP,
        partition_providers: PartitionProviders<PP, SP>,
        handler: T,
        backend: &B,
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
            CellWrite<Loader: MessageLoader<Payload = C::Payload>>,
        L: MessageLoader<Payload = C::Payload> + 'static,
        C: Codec,
        C::Payload: Send + Sync + 'static + EventIdentity + EventType + Clone,
        B: ConsumerReaderBackend<C>,
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
        let peer = self
            .common_config
            .peer
            .as_ref()
            .ok_or(PeerInitError::PeerRequired)?;
        let subsystem = self
            .subsystem
            .clone()
            .ok_or(PeerInitError::SubsystemRequired)?;
        let services = StartupServices {
            version,
            telemetry: &self.telemetry,
            heartbeats: self.heartbeats,
            observer: self.observer,
            managers: Arc::clone(&managers),
            responder: Some(subsystem.clone()),
        };
        // Preparation is the last fallible step of this mode, and no `?` runs
        // between it and the termination below.
        let prepared =
            prepare_responding::<R, _, _>(peer, backend, subsystem, managers, &services.heartbeats)
                .await?;
        let (provider, attach) = prepared.terminate(&middleware, handler);
        Box::pin(initialize_consumer::<_, _, _, C, _>(
            &self.consumer_config,
            provider,
            partition_providers,
            services,
            attach,
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
    pub async fn pipeline_consumer<T>(
        setup: ConsumerSetup<'_>,
        pipeline_config: PipelineMiddlewareConfiguration,
        telemetry: Telemetry,
        handler: T,
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
    /// Returns [`PeerInitError::PeerRequired`] without peer configuration.
    /// Returns [`PeerInitError::SubsystemRequired`] without a subsystem name.
    /// Returns [`ConsumerError`] when another startup step fails.
    pub async fn pipeline_responding_consumer<T, R>(
        setup: ConsumerSetup<'_>,
        pipeline_config: PipelineMiddlewareConfiguration,
        telemetry: Telemetry,
        handler: T,
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
                Self::pipeline_responding_consumer_with_backend::<T, R, _>(
                    TypedConsumerSetup {
                        consumer: setup.consumer,
                        common: setup.common,
                        deps,
                    },
                    pipeline_config,
                    telemetry,
                    handler,
                )
                .await
            }
            (false, TriggerStoreConfiguration::Cassandra(config)) => {
                let deps = cassandra_deps(&setup, config).await?;
                Self::pipeline_responding_consumer_with_backend::<T, R, _>(
                    TypedConsumerSetup {
                        consumer: setup.consumer,
                        common: setup.common,
                        deps,
                    },
                    pipeline_config,
                    telemetry,
                    handler,
                )
                .await
            }
        }
    }

    pub(crate) async fn pipeline_consumer_with_backend<T, B>(
        setup: TypedConsumerSetup<'_, C, B>,
        pipeline_config: PipelineMiddlewareConfiguration,
        telemetry: Telemetry,
        handler: T,
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
        let (components, keyed_state, heartbeats, observer) = build_typed_state(&setup).await?;
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
            subsystem: keyed_state.subsystem().cloned(),
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
            .into_consumer::<_, _, _, _, _, _, _, C, B>(
                message_defer,
                components.timers,
                components.dedup,
                PartitionProviders {
                    triggers: components.trigger,
                    state: components.state,
                },
                handler,
                setup.deps.backend().as_ref(),
            )
            .await
    }

    pub(crate) async fn pipeline_responding_consumer_with_backend<T, R, B>(
        setup: TypedConsumerSetup<'_, C, B>,
        pipeline_config: PipelineMiddlewareConfiguration,
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
        let PipelineMiddlewareConfiguration {
            retry,
            monopolization,
            defer,
        } = pipeline_config;
        let (components, keyed_state, heartbeats, observer) = build_typed_state(&setup).await?;
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
            subsystem: keyed_state.subsystem().cloned(),
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
            .into_responding_consumer::<_, R, _, _, _, _, _, _, C, B>(
                message_defer,
                components.timers,
                components.dedup,
                PartitionProviders {
                    triggers: components.trigger,
                    state: components.state,
                },
                handler,
                setup.deps.backend().as_ref(),
            )
            .await
    }
}
