//! The four ways a consumer is constructed, one submodule per strategy.
//!
//! `direct` dispatches straight to the handler with no middleware. `pipeline`
//! retries and defers. `low_latency` routes exhausted failures to a topic.
//! `best_effort` logs them and moves on.
//!
//! The four public constructors stay in one inherent impl block here, each
//! delegating to its submodule. Do not move a constructor into its module:
//! `clippy::multiple_inherent_impl` fires on inherent impls that share a self
//! type and predicates even across files, and these four have no distinct
//! bounds to separate them by.

use crate::consumer::ProsodyConsumer;
use crate::consumer::config::{
    ConsumerConfiguration, ConsumerSetup, LowLatencyMiddlewareConfiguration,
    PipelineMiddlewareConfiguration,
};
use crate::consumer::error::ConsumerError;
use crate::consumer::handler::{EventHandler, HandlerProvider};
use crate::consumer::middleware::FallibleHandler;
use crate::high_level::config::TriggerStoreConfiguration;
use crate::producer::ProsodyProducer;
use crate::state::config::KeyedStateConfiguration;
use crate::telemetry::Telemetry;
use crate::{Codec, EventIdentity, EventType};

mod best_effort;
mod direct;
mod low_latency;
mod pipeline;

impl<C: Codec> ProsodyConsumer<C>
where
    C::Payload: EventType + Clone,
{
    /// Creates a low-level `ProsodyConsumer` that runs an [`EventHandler`]
    /// directly, with **no middleware**.
    ///
    /// This is the lower of the two consumer layers. It wires the partition
    /// machinery and an empty keyed-state backend, then dispatches each
    /// message and timer straight to the handler — no retry, deduplication,
    /// monopolization, or defer middleware runs, and the `settle` durability
    /// boundary never executes. The handler owns its own commit decisions
    /// through the `Uncommitted` types.
    ///
    /// Because the durability boundary never runs here, keyed-state
    /// collections can be neither staged nor recovered: registering any is
    /// rejected with
    /// [`KeyedStateInitError::StateUnsupported`](crate::consumer::KeyedStateInitError::StateUnsupported).
    /// Use a high-level constructor ([`Self::pipeline_consumer`],
    /// [`Self::low_latency_consumer`]) for keyed state and the full middleware
    /// stack — those take a [`FallibleHandler`].
    ///
    /// # Errors
    ///
    /// Returns a `ConsumerError` if the configuration is invalid, keyed-state
    /// collections are registered, or store/consumer creation fails.
    pub async fn new<T>(
        consumer_config: &ConsumerConfiguration,
        trigger_store_config: &TriggerStoreConfiguration,
        keyed_state_config: KeyedStateConfiguration,
        handler_provider: T,
        telemetry: Telemetry,
    ) -> Result<Self, ConsumerError>
    where
        T: HandlerProvider,
        T::Handler: EventHandler<Payload = C::Payload>,
        C::Payload: EventIdentity + Send + Sync + 'static,
    {
        direct::build(
            consumer_config,
            trigger_store_config,
            keyed_state_config,
            handler_provider,
            telemetry,
        )
        .await
    }

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
        T: FallibleHandler<Payload = C::Payload> + Clone + Send + Sync + 'static,
        C::Payload: EventIdentity + Clone,
    {
        pipeline::build(setup, pipeline_config, telemetry, handler).await
    }

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
        setup: ConsumerSetup<'_, C>,
        low_latency_config: LowLatencyMiddlewareConfiguration,
        producer: ProsodyProducer<C>,
        telemetry: Telemetry,
        handler: T,
    ) -> Result<Self, ConsumerError>
    where
        T: FallibleHandler<Payload = C::Payload> + Clone + Send + Sync + 'static,
        C::Payload: EventIdentity + Clone + Send + Sync + 'static,
    {
        low_latency::build(setup, low_latency_config, producer, telemetry, handler).await
    }

    /// Creates a new `ProsodyConsumer` with logging middleware for failure
    /// handling.
    ///
    /// The best-effort approach is the simplest - it tries to process
    /// messages once, logs any failures, and moves on. This approach should
    /// only be used for development or for services where occasional
    /// message loss is acceptable.
    pub(crate) async fn best_effort_consumer<T>(
        setup: ConsumerSetup<'_, C>,
        telemetry: Telemetry,
        handler: T,
    ) -> Result<Self, ConsumerError>
    where
        T: FallibleHandler<Payload = C::Payload> + Clone + Send + Sync + 'static,
        C::Payload: EventIdentity + Clone + Send + Sync + 'static,
    {
        best_effort::build(setup, telemetry, handler).await
    }
}
