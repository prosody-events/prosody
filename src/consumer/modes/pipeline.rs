//! The pipeline mode: retry, defer, and monopolization middleware layered
//! outside the common block.

use crate::consumer::ProsodyConsumer;
use crate::consumer::config::{
    CommonConfiguration, ConsumerConfiguration, ConsumerSetup, PipelineMiddlewareConfiguration,
};
use crate::consumer::error::ConsumerError;
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
use crate::consumer::storage::StorePair;
use crate::consumer::wiring::runtime::{StartupServices, initialize_consumer};
use crate::consumer::wiring::state::{
    KeyedStateInputs, cassandra_loader, cassandra_state_provider, memory_arm_inputs,
    memory_state_provider,
};
use crate::consumer::wiring::{build_common_middleware, build_shared_state};
use crate::heartbeat::HeartbeatRegistry;
use crate::loader::MessageLoader;
use crate::state::manager::{PartitionStateManager, PartitionStateProvider};
use crate::state::session::CellWrite;
use crate::state_reader::ConsumerReaderBackend;
use crate::telemetry::Telemetry;
use crate::timers::store::TriggerStoreProvider;
use crate::{Codec, EventIdentity, EventType};
use std::sync::Arc;

/// Everything both storage arms need to finish a pipeline consumer.
struct PipelineMiddlewareStack {
    consumer_config: ConsumerConfiguration,
    defer_config: DeferConfiguration,
    common_config: CommonConfiguration,
    failure_tracker: FailureTracker,
    monopolization_middleware: Option<MonopolizationMiddleware>,
    retry_middleware: RetryMiddleware,
    heartbeats: HeartbeatRegistry,
    telemetry: Telemetry,
    observer: KafkaObserver,
}

impl PipelineMiddlewareStack {
    async fn into_consumer<T, MP, TP, DP, PP, SP, L, C>(
        self,
        message_defer_middleware: MessageDeferMiddleware<MP, L, FailureTracker>,
        timer_provider: TP,
        dedup_provider: DP,
        trigger_provider: PP,
        state_provider: SP,
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
            CellWrite<Loader: MessageLoader<Payload = C::Payload>>,
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
        let common_middleware = build_common_middleware::<DP, C::Payload>(
            &self.common_config,
            &self.consumer_config,
            self.telemetry.clone(),
            dedup_provider,
        )?;
        let provider = common_middleware
            .layer(self.monopolization_middleware)
            .layer(timer_defer_middleware)
            .layer(message_defer_middleware)
            .layer(self.retry_middleware)
            .into_provider(handler);

        initialize_consumer::<_, _, _, C>(
            &self.consumer_config,
            provider,
            trigger_provider,
            state_provider,
            StartupServices {
                version,
                telemetry: &self.telemetry,
                heartbeats: self.heartbeats,
                observer: self.observer,
            },
        )
        .await
    }
}

/// Builds the storage pair, keyed-state inputs, and shared middleware stack
/// for [`ProsodyConsumer::pipeline_consumer`].
async fn prepare_pipeline_stack<C, B>(
    setup: &ConsumerSetup<'_, C, B>,
    pipeline_config: PipelineMiddlewareConfiguration,
    telemetry: Telemetry,
) -> Result<(StorePair, KeyedStateInputs, PipelineMiddlewareStack), ConsumerError>
where
    C: Codec,
    C::Payload: Clone,
    B: ConsumerReaderBackend<C>,
{
    let PipelineMiddlewareConfiguration {
        retry: retry_config,
        monopolization: monopolization_config,
        defer: defer_config,
    } = pipeline_config;
    let (stores, keyed_state, heartbeats, observer) = build_shared_state(setup).await?;
    let monopolization_middleware =
        MonopolizationMiddleware::new(&monopolization_config, &telemetry)?;
    let failure_tracker = FailureTracker::new(
        defer_config.failure_window,
        defer_config.failure_threshold,
        &telemetry,
        &heartbeats,
    );

    let stack = PipelineMiddlewareStack {
        consumer_config: setup.consumer.clone(),
        defer_config,
        common_config: setup.common.clone(),
        failure_tracker,
        monopolization_middleware,
        retry_middleware: RetryMiddleware::new(retry_config)?,
        heartbeats,
        telemetry,
        observer,
    };

    Ok((stores, keyed_state, stack))
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
        setup: ConsumerSetup<'_, C>,
        pipeline_config: PipelineMiddlewareConfiguration,
        telemetry: Telemetry,
        handler: T,
    ) -> Result<Self, ConsumerError>
    where
        C::Payload: EventIdentity,
        T: FallibleHandler<Payload = C::Payload> + Clone + Send + Sync + 'static,
    {
        Self::pipeline_consumer_with_backend(setup, pipeline_config, telemetry, handler).await
    }

    pub(crate) async fn pipeline_consumer_with_backend<T, B>(
        setup: ConsumerSetup<'_, C, B>,
        pipeline_config: PipelineMiddlewareConfiguration,
        telemetry: Telemetry,
        handler: T,
    ) -> Result<Self, ConsumerError>
    where
        C::Payload: EventIdentity,
        B: ConsumerReaderBackend<C>,
        T: FallibleHandler<Payload = C::Payload> + Clone + Send + Sync + 'static,
    {
        let (stores, keyed_state, stack) =
            prepare_pipeline_stack(&setup, pipeline_config, telemetry).await?;
        let deps = setup.deps;

        match stores {
            StorePair::Memory {
                trigger_provider,
                message_provider,
                timer_provider,
                dedup_provider,
                publication_store,
            } => {
                let (loader, cells, identities) = memory_arm_inputs(deps.as_ref());
                let publisher_template = keyed_state
                    .memory_publication_setup(publication_store)
                    .await?;
                let state_provider = memory_state_provider::<C>(
                    &keyed_state,
                    dedup_provider.clone(),
                    cells,
                    identities,
                    loader.clone(),
                    publisher_template,
                );
                let message_defer_middleware = MessageDeferMiddleware::new(
                    stack.defer_config.clone(),
                    &stack.consumer_config,
                    message_provider,
                    stack.failure_tracker.clone(),
                    loader,
                    &stack.common_config.dedup.version,
                    &stack.telemetry,
                )?;
                stack
                    .into_consumer::<_, _, _, _, _, _, _, C>(
                        message_defer_middleware,
                        timer_provider,
                        dedup_provider,
                        trigger_provider,
                        state_provider,
                        handler,
                    )
                    .await
            }
            StorePair::Cassandra {
                trigger_provider,
                message_provider,
                timer_provider,
                dedup_provider,
                cell_store,
                identity_store,
                publication_store,
            } => {
                let loader =
                    cassandra_loader(deps.as_ref(), &stack.consumer_config, &stack.heartbeats)?;
                let publisher_template = keyed_state
                    .cassandra_publication_setup(publication_store, stack.observer.clone())
                    .await?;
                let state_provider = cassandra_state_provider::<C>(
                    &keyed_state,
                    dedup_provider.clone(),
                    cell_store,
                    identity_store,
                    loader.clone(),
                    publisher_template,
                )?;
                let message_defer_middleware = MessageDeferMiddleware::new(
                    stack.defer_config.clone(),
                    &stack.consumer_config,
                    message_provider,
                    stack.failure_tracker.clone(),
                    loader,
                    &stack.common_config.dedup.version,
                    &stack.telemetry,
                )?;
                stack
                    .into_consumer::<_, _, _, _, _, _, _, C>(
                        message_defer_middleware,
                        timer_provider,
                        dedup_provider,
                        trigger_provider,
                        state_provider,
                        handler,
                    )
                    .await
            }
        }
    }
}
