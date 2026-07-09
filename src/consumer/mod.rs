//! Kafka consumer implementation for high-level message consumption and
//! processing.
//!
//! This module provides an abstraction for consuming messages from Kafka topics
//! with support for:
//!
//! - Per-key concurrency with ordered processing within keys
//! - Automatic partition assignment and revocation handling
//! - Offset management with exactly-once semantics
//! - Global message buffering with bounded concurrency
//! - Backpressure handling and flow control
//! - Error handling with configurable retry strategies
//! - Distributed tracing integration
//! - Health and readiness probes
//!
//! # Architecture
//!
//! The consumer architecture is centered around these key components:
//!
//! - `ProsodyConsumer`: The main entry point that coordinates all consumer
//!   operations
//! - `PartitionManager`: Manages message processing for a single Kafka
//!   partition
//! - `EventHandler`: User-implemented trait for message processing logic
//! - Failure strategies: Composable error handling mechanisms
//!
//! # Usage
//!
//! To use this consumer, implement the `EventHandler` trait with your message
//! processing logic, configure the consumer through `ConsumerConfiguration`,
//! and start processing:
//!
//! ```
//! use prosody::consumer::event_context::EventContext;
//! use prosody::consumer::message::UncommittedMessage;
//! use prosody::consumer::middleware::CloneProvider;
//! use prosody::consumer::{
//!     ConsumerConfiguration, DemandType, EventHandler, Keyed, KeyedStateConfiguration,
//!     ProsodyConsumer, Uncommitted,
//! };
//! use prosody::high_level::config::TriggerStoreConfiguration;
//! use prosody::telemetry::Telemetry;
//! use prosody::timers::{UncommittedTimer, store::TriggerStore};
//!
//! // Implement your message handler
//! #[derive(Clone)]
//! struct MyHandler;
//!
//! impl EventHandler for MyHandler {
//!     type Payload = serde_json::Value;
//!
//!     async fn on_message<C>(
//!         &self,
//!         context: C,
//!         message: UncommittedMessage<serde_json::Value>,
//!         _demand_type: DemandType,
//!     ) where
//!         C: EventContext,
//!     {
//!         // Process the message
//!         println!("Processing message with key: {}", message.key());
//!
//!         // Commit the message when processing is complete
//!         message.commit().await;
//!     }
//!
//!     async fn on_timer<C, U>(&self, context: C, timer: U, _demand_type: DemandType)
//!     where
//!         C: EventContext,
//!         U: UncommittedTimer,
//!     {
//!         // Process the timer
//!         println!("Processing timer");
//!
//!         // Commit the timer when processing is complete
//!         timer.commit().await;
//!     }
//!
//!     async fn shutdown(self) {
//!         // Clean up resources
//!     }
//! }
//!
//! // Create and start the consumer
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let config = ConsumerConfiguration::builder()
//!     .bootstrap_servers(vec!["kafka:9092".to_string()])
//!     .group_id("my-consumer-group")
//!     .subscribed_topics(vec!["my-topic".to_string()])
//!     .build()?;
//!
//! let telemetry = Telemetry::new();
//!
//! let consumer: ProsodyConsumer = ProsodyConsumer::new(
//!     &config,
//!     &TriggerStoreConfiguration::InMemory,
//!     KeyedStateConfiguration::builder().build()?,
//!     CloneProvider::new(MyHandler),
//!     telemetry,
//! )
//! .await?;
//!
//! // The consumer will process messages until shutdown is called
//! # Ok(())
//! # }
//! ```
//!
//! # Failure Handling
//!
//! The consumer supports several error handling strategies:
//!
//! - **Pipeline processing**: Messages that fail processing are retried with
//!   backoff
//! - **Low latency processing**: Failed messages are sent to a failure topic
//! - **Best effort processing**: Failed messages are logged and discarded

pub use crate::consumer::event_context::EventContext;
pub use crate::consumer::event_context::TerminationSignals;
use crate::consumer::kafka_context::{ManagerRegistry, PartitionProviders, new_context};
pub use crate::consumer::kafka_state::{
    MessageDescriptor, MessageRef, MessageRefCodec, MessageRefCodecError, MessageResolver,
    MessageStateError, message_state,
};
pub use crate::consumer::message::ConsumerMessage;
use crate::consumer::message::UncommittedMessage;
pub use crate::consumer::middleware::FallibleHandler;
use crate::consumer::middleware::HandlerMiddleware;
use crate::consumer::middleware::cancellation::CancellationMiddleware;
use crate::consumer::middleware::deduplication::{
    CassandraDeduplicationStoreProvider, DEFAULT_IDEMPOTENCE_VERSION, DeduplicationConfiguration,
    DeduplicationMiddleware, DeduplicationStoreProvider, MemoryDeduplicationStoreProvider,
};
use crate::consumer::middleware::defer::message::store::MessageDeferStoreProvider;
use crate::consumer::middleware::defer::timer::store::TimerDeferStoreProvider;
use crate::consumer::middleware::defer::{
    DeferConfiguration, DeferInitError, FailureTracker, MessageDeferMiddleware,
    TimerDeferMiddleware,
};
use crate::consumer::middleware::log::LogMiddleware;
use crate::consumer::middleware::monopolization::{
    MonopolizationConfiguration, MonopolizationInitError, MonopolizationMiddleware,
};
use crate::consumer::middleware::retry::{RetryConfiguration, RetryMiddleware};
use crate::consumer::middleware::scheduler::{
    SchedulerConfiguration, SchedulerInitError, SchedulerMiddleware,
};
use crate::consumer::middleware::telemetry::TelemetryMiddleware;
use crate::consumer::middleware::timeout::{
    TimeoutConfiguration, TimeoutInitError, TimeoutMiddleware,
};
use crate::consumer::middleware::topic::{FailureTopicConfiguration, FailureTopicMiddleware};
use crate::consumer::partition::PartitionManager;
use crate::consumer::poll::PollConfig;
use crate::consumer::poll::poll;
use crate::consumer::probes::ProbeServer;
use crate::consumer::storage::StorePair;
use crate::heartbeat::HeartbeatRegistry;
use crate::high_level::config::TriggerStoreConfiguration;
use crate::loader::{KafkaLoader, KafkaLoaderConfiguration, MemoryLoader, MessageLoader};
pub use crate::otel::SpanRelation;
use crate::producer::ProsodyProducer;
use crate::state::cassandra::{CassandraCellResources, CassandraDescriptorIdentityStore};
pub use crate::state::config::{KeyedStateConfiguration, KeyedStateConfigurationBuilderError};
use crate::state::fjall::{FjallClient, FjallClientError};
use crate::state::manager::{PartitionStateManager, PartitionStateProvider, StateManagerProvider};
use crate::state::memory::{MemoryCells, MemoryDescriptorIdentityStore};
use crate::state::production::{CassandraStateBackendFactory, MemoryStateBackendFactory};
use crate::state::registry::{CollectionDefRegistry, RegisterStateError};
use crate::state::session::CellSession;
use crate::telemetry::Telemetry;
use crate::timers::UncommittedTimer;
use crate::timers::duration::CompactDuration;
use crate::timers::duration::CompactDurationError;
use crate::timers::store::TriggerStoreProvider;
use crate::timers::store::cassandra::CassandraTriggerStoreError;
use crate::util::{
    from_duration_env_with_fallback, from_env, from_env_with_fallback,
    from_option_env_with_fallback, from_optional_vec_env, from_vec_env,
};
use crate::{Codec, ConsumerGroup, EventIdentity, EventType, JsonCodec};
use crate::{MOCK_CLUSTER_BOOTSTRAP, Partition, Topic};
use ahash::HashMap;
use crossbeam_utils::CachePadded;
use derive_builder::Builder;
use educe::Educe;
use futures::executor::block_on;
use parking_lot::{Mutex, RwLock};
use rdkafka::ClientConfig;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::KafkaError;
use serde::Serialize;
use std::env::var;
use std::fmt::Debug;
use std::fs;
use std::future::Future;
use std::io;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;
use thiserror::Error;
use tokio::sync::watch;
use tokio::task::{JoinHandle, spawn_blocking};
use tracing::error;
use validator::{Validate, ValidationErrors};
use whoami::hostname;

pub(crate) mod decode;
pub mod event_context;
mod extractor;
mod kafka_context;
pub mod kafka_state;
pub mod message;
pub mod middleware;
pub(crate) mod partition;
mod poll;
mod probes;
pub mod storage;

#[cfg(test)]
mod tests;

/// Atomic counter for tracking changes in partition watermarks.
///
/// Used to efficiently determine when offsets need to be committed without
/// requiring a full scan of all partition managers.
type WatermarkVersion = CachePadded<AtomicUsize>;

/// Thread-safe storage for partition managers.
///
/// Maps (Topic, Partition) pairs to their corresponding `PartitionManager`
/// instances. Protected by a `RwLock` to allow concurrent reads with exclusive
/// writes.
type Managers<P> = RwLock<HashMap<(Topic, Partition), PartitionManager<P>>>;

/// Environment variable name for the Kafka consumer group ID.
const PROSODY_GROUP_ID: &str = "PROSODY_GROUP_ID";

/// Represents the type of demand being processed.
///
/// Demand types allow the system to distinguish between normal processing
/// and failure handling scenarios, enabling different processing behaviors
/// for the same event type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum DemandType {
    /// Normal demand represents the initial processing attempt of an event.
    Normal,

    /// Failure demand represents retry processing after a previous failure.
    /// This is typically created by retry middleware when an event fails
    /// and needs to be reprocessed.
    Failure,
}

/// This trait is implemented by message types that have a key field,
/// allowing key-based message routing and processing.
pub trait Keyed {
    /// The type of the key.
    type Key;

    /// Retrieves the key of the item.
    ///
    /// # Returns
    ///
    /// A reference to the key.
    fn key(&self) -> &Self::Key;
}

/// Provides transaction-like semantics for event processing acknowledgment.
///
/// The [`Uncommitted`] trait enables reliable event processing by requiring
/// explicit acknowledgment after processing. Events that implement this trait
/// must be either committed (successfully processed) or aborted (failed
/// processing) to ensure proper resource cleanup and delivery guarantees.
///
/// ## Transaction Semantics
///
/// The trait provides a simple two-phase commit protocol:
/// 1. **Processing**: Application processes the delivered event
/// 2. **Acknowledgment**: Application calls [`Uncommitted::commit()`] or
///    [`Uncommitted::abort()`]
///
/// ## Reliability Guarantees
///
/// - **At-least-once delivery**: Events are delivered at least once until
///   committed
/// - **Resource cleanup**: Proper acknowledgment ensures resources are cleaned
///   up
/// - **Fault tolerance**: Uncommitted events survive application crashes
/// - **Graceful shutdown**: Uncommitted events are handled during shutdown
pub trait Uncommitted {
    /// Acknowledges successful processing of the event.
    ///
    /// This method should be called when the event has been successfully
    /// processed and should be permanently removed from the system. Committing
    /// an event typically triggers cleanup operations and prevents redelivery.
    fn commit(self) -> impl Future<Output = ()> + Send;

    /// Acknowledges failed processing of the event.
    ///
    /// This method should be called when event processing is shutting down and
    /// cannot continue. Abort should only be called when the partition is being
    /// revoked.
    fn abort(self) -> impl Future<Output = ()> + Send;
}

/// Provides handlers for processing messages from specific partitions.
///
/// This trait allows creating custom message handlers for each partition,
/// enabling partition-specific processing logic if needed.
pub trait HandlerProvider: Send + Sync + 'static {
    /// The type of message handler provided.
    type Handler: EventHandler + Send + Sync + 'static;

    /// Creates a handler for a specific topic and partition.
    ///
    /// # Arguments
    ///
    /// * `topic` - The topic of the partition.
    /// * `partition` - The partition number.
    ///
    /// # Returns
    ///
    /// A handler for the specified topic and partition.
    fn handler_for_partition(&self, topic: Topic, partition: Partition) -> Self::Handler;
}

/// Defines the behavior for handling consumed Kafka messages.
///
/// This is the primary trait to implement for message processing logic.
/// It provides methods for processing messages and handling shutdown.
pub trait EventHandler {
    /// The payload type carried by messages delivered to this handler.
    type Payload: Send + Sync + 'static;

    /// Processes a consumed message.
    ///
    /// This method should contain the business logic for message processing.
    /// It should commit or abort the message when processing is complete.
    ///
    /// # Arguments
    ///
    /// * `context` - The message context providing metadata and shutdown
    ///   notification.
    /// * `message` - The uncommitted message to process.
    /// * `demand_type` - Whether this is normal processing or failure retry.
    ///
    /// # Returns
    ///
    /// A future that resolves when the message is handled.
    fn on_message<C>(
        &self,
        context: C,
        message: UncommittedMessage<Self::Payload>,
        demand_type: DemandType,
    ) -> impl Future<Output = ()> + Send
    where
        C: EventContext<Payload = Self::Payload>;

    /// Handles timer events when they fire.
    ///
    /// This method is called when a scheduled timer reaches its execution time
    /// and is delivered to the application for processing. The timer must be
    /// explicitly committed or aborted after processing to ensure proper
    /// resource cleanup.
    ///
    /// # Arguments
    ///
    /// * `context` - The event processing context with access to timer
    ///   management
    /// * `timer` - The uncommitted timer event that fired
    /// * `demand_type` - Whether this is normal processing or failure retry.
    ///
    /// # Processing Requirements
    ///
    /// Implementations must ensure that the timer is properly acknowledged:
    /// - Call `timer.commit()` after successful processing
    /// - Call `timer.abort()` if processing fails or should be retried
    ///
    /// # Returns
    ///
    /// A future that resolves when timer processing is complete. Note that
    /// this future completing does not automatically commit the timer.
    fn on_timer<C, T>(
        &self,
        context: C,
        timer: T,
        demand_type: DemandType,
    ) -> impl Future<Output = ()> + Send
    where
        C: EventContext<Payload = Self::Payload>,
        T: UncommittedTimer;

    /// Shuts down the message handler.
    ///
    /// This method is called when the consumer is shutting down.
    /// It should clean up any resources used by the handler.
    ///
    /// # Returns
    ///
    /// A future that resolves when the shutdown is complete.
    fn shutdown(self) -> impl Future<Output = ()> + Send;
}

/// Configuration for the Kafka consumer.
///
/// This struct holds all the necessary configuration options for creating a
/// Kafka consumer. It uses the Builder pattern for flexible initialization and
/// supports loading values from environment variables.
#[derive(Builder, Clone, Debug, Validate)]
pub struct ConsumerConfiguration {
    /// List of Kafka bootstrap servers.
    ///
    /// Environment variable: `PROSODY_BOOTSTRAP_SERVERS`
    /// Default: None (must be specified)
    ///
    /// At least one server must be specified.
    #[builder(default = "from_vec_env(\"PROSODY_BOOTSTRAP_SERVERS\")?", setter(into))]
    #[validate(length(min = 1_u64))]
    pub bootstrap_servers: Vec<String>,

    /// Consumer group ID.
    ///
    /// Environment variable: `PROSODY_GROUP_ID`
    /// Default: None (must be specified)
    ///
    /// The group ID must be a non-empty string and should be unique for each
    /// logically separate consumer application. Consumers with the same group
    /// ID will form a consumer group and share the load of consuming topics.
    #[builder(default = "from_env(PROSODY_GROUP_ID)?", setter(into))]
    #[validate(length(min = 1_u64))]
    pub group_id: String,

    /// List of topics to subscribe to.
    ///
    /// Environment variable: `PROSODY_SUBSCRIBED_TOPICS`
    /// Default: None (must be specified)
    ///
    /// At least one topic must be specified.
    #[builder(default = "from_vec_env(\"PROSODY_SUBSCRIBED_TOPICS\")?", setter(into))]
    #[validate(length(min = 1_u64))]
    pub subscribed_topics: Vec<String>,

    /// Allowed event type prefixes.
    ///
    /// Environment variable: `PROSODY_ALLOWED_EVENTS`
    /// Default: None
    ///
    /// If specified, only messages with event types matching these prefixes
    /// will be processed. If not specified, all events are allowed.
    #[builder(
        default = "from_optional_vec_env(\"PROSODY_ALLOWED_EVENTS\")?",
        setter(into)
    )]
    #[validate(length(min = 1_u64))]
    pub allowed_events: Option<Vec<String>>,

    /// Maximum number of uncommitted messages.
    ///
    /// Environment variable: `PROSODY_MAX_UNCOMMITTED`
    /// Default: 64
    ///
    /// Controls the global limit of messages being processed concurrently
    /// across all partitions. This provides backpressure when the system is
    /// under high load by pausing message consumption when the limit is
    /// reached. Also determines the buffer size for message queues.
    #[builder(
        default = "from_env_with_fallback(\"PROSODY_MAX_UNCOMMITTED\", 64)?",
        setter(into)
    )]
    #[validate(range(min = 1_usize))]
    pub max_uncommitted: usize,

    /// Duration of inactivity allowed before considering a partition stalled.
    ///
    /// Environment variable: `PROSODY_STALL_THRESHOLD`
    /// Default: 5 minutes
    ///
    /// Used by the liveness probe to determine if a partition's processing has
    /// stalled. If message processing takes longer than this duration, the
    /// partition is considered stalled, and the liveness probe will report an
    /// unhealthy status.
    #[builder(
        default = "from_duration_env_with_fallback(\"PROSODY_STALL_THRESHOLD\", \
                   Duration::from_mins(5))?",
        setter(into)
    )]
    pub stall_threshold: Duration,

    /// Timeout for partition shutdown.
    ///
    /// Environment variable: `PROSODY_SHUTDOWN_TIMEOUT`
    /// Default: 30 seconds
    ///
    /// Determines how long to wait for in-flight tasks to complete during
    /// partition shutdown. After this threshold is reached, any remaining
    /// tasks will be aborted.
    #[builder(
        default = "from_duration_env_with_fallback(\"PROSODY_SHUTDOWN_TIMEOUT\", \
                   Duration::from_secs(30))?",
        setter(into)
    )]
    pub shutdown_timeout: Duration,

    /// Interval between poll operations.
    ///
    /// Environment variable: `PROSODY_POLL_INTERVAL`
    /// Default: 100 milliseconds
    ///
    /// Controls how frequently the consumer polls Kafka for new messages.
    #[builder(
        default = "from_duration_env_with_fallback(\"PROSODY_POLL_INTERVAL\", \
                   Duration::from_millis(100))?",
        setter(into)
    )]
    pub poll_interval: Duration,

    /// Interval between commit operations.
    ///
    /// Environment variable: `PROSODY_COMMIT_INTERVAL`
    /// Default: 1 second
    ///
    /// Controls how frequently offsets are auto-committed to Kafka.
    #[builder(
        default = "from_duration_env_with_fallback(\"PROSODY_COMMIT_INTERVAL\", \
                   Duration::from_secs(1))?",
        setter(into)
    )]
    pub commit_interval: Duration,

    /// Use a mock consumer for testing purposes.
    ///
    /// Environment variable: `PROSODY_MOCK`
    /// Default: false
    ///
    /// When true, uses a mock Kafka cluster for testing instead of the
    /// configured bootstrap servers.
    #[builder(
        default = "from_env_with_fallback(\"PROSODY_MOCK\", false)?",
        setter(into)
    )]
    pub mock: bool,

    /// Port for the probe server.
    ///
    /// Environment variable: `PROSODY_PROBE_PORT`
    /// Default: Some(8000)
    ///
    /// If set, starts an HTTP server on this port for health and readiness
    /// probes. Set to None to disable the probe server.
    #[builder(
        default = "from_option_env_with_fallback(\"PROSODY_PROBE_PORT\", 8000)?",
        setter(into)
    )]
    pub probe_port: Option<u16>,

    #[builder(
        default = "from_duration_env_with_fallback(\"PROSODY_SLAB_SIZE\", \
                   Duration::from_hours(1))?",
        setter(into)
    )]
    /// Duration for timer slab partitioning.
    ///
    /// This setting controls how timers are partitioned into time-based slabs
    /// for efficient storage and retrieval. Smaller slabs provide more precise
    /// time ranges but increase metadata overhead, while larger slabs reduce
    /// overhead but may be less efficient for sparse timer patterns.
    ///
    /// # Recommended Values
    ///
    /// - **High-frequency timers**: 5-15 minutes
    /// - **Medium-frequency timers**: 15-60 minutes
    /// - **Low-frequency timers**: 1-4 hours
    ///
    /// # Default
    ///
    /// Defaults to 1 hour if not specified or if parsing from environment
    /// fails.
    pub slab_size: Duration,

    /// Span relation for message execution spans.
    ///
    /// Controls how the `receive` span connects to the `OTel` context
    /// propagated from the Kafka message producer.
    ///
    /// Environment variable: `PROSODY_MESSAGE_SPANS`
    /// Default: `child` (child-of relationship)
    #[builder(default = "from_env_with_fallback(\"PROSODY_MESSAGE_SPANS\", SpanRelation::Child)?")]
    pub message_spans: SpanRelation,

    /// Span relation for timer execution spans.
    ///
    /// Controls how timer spans connect to the `OTel` context stored when the
    /// timer was scheduled.
    ///
    /// Environment variable: `PROSODY_TIMER_SPANS`
    /// Default: `follows_from`
    #[builder(
        default = "from_env_with_fallback(\"PROSODY_TIMER_SPANS\", SpanRelation::FollowsFrom)?"
    )]
    pub timer_spans: SpanRelation,

    /// Tuning for the Kafka message loader (deferred-retry reload and
    /// keyed-state message resolution).
    ///
    /// The loader is consumer-wide and shares this struct's connection and
    /// concurrency settings; only its own knobs live here. Env-loaded like the
    /// rest of the configuration (`PROSODY_LOADER_*`).
    #[builder(default = "KafkaLoaderConfiguration::builder().build().map_err(|e| e.to_string())?")]
    #[validate(nested)]
    pub loader: KafkaLoaderConfiguration,
}

impl ConsumerConfiguration {
    /// Creates a new `ConsumerConfigurationBuilder`.
    ///
    /// This method provides a convenient way to start building a
    /// `ConsumerConfiguration` using the builder pattern.
    ///
    /// # Returns
    ///
    /// A default `ConsumerConfigurationBuilder` instance.
    #[must_use]
    pub fn builder() -> ConsumerConfigurationBuilder {
        ConsumerConfigurationBuilder::default()
    }
}

/// Holds the runtime state of the consumer.
///
/// This struct encapsulates the components that make up the running state
/// of a `ProsodyConsumer`, allowing them to be managed together.
struct RuntimeState {
    /// Handle to the polling task.
    poll_handle: JoinHandle<()>,

    /// Optional probe server for health and readiness checks.
    probe_server: Option<ProbeServer>,
}

impl ConsumerConfigurationBuilder {
    /// Retrieves the currently configured consumer group.
    ///
    /// Checks both the explicitly configured group ID and the environment
    /// variable.
    ///
    /// # Returns
    ///
    /// An option containing the consumer group if configured.
    #[must_use]
    pub(crate) fn configured_consumer_group(&self) -> Option<String> {
        self.group_id.clone().or_else(|| var(PROSODY_GROUP_ID).ok())
    }
}

/// Configuration shared by every consumer mode (pipeline, low-latency, and
/// best-effort).
///
/// Carries the scheduler, timeout, and deduplication configurations, plus the
/// mode-independent keyed-state configuration. Named without a "middleware"
/// qualifier because keyed state is not middleware — its durability sequence
/// runs at the `settle` boundary, outside the middleware stack.
#[derive(Clone, Debug)]
pub struct CommonConfiguration {
    /// Scheduler configuration for fair work-conserving dispatch.
    pub scheduler: SchedulerConfiguration,
    /// Timeout configuration for handler execution limits.
    pub timeout: TimeoutConfiguration,
    /// Deduplication configuration.
    ///
    /// Deduplication runs in **every** consumer mode: it is the commit oracle
    /// the keyed-state recovery path reads (a message's dedup row existing
    /// means it committed), so it cannot be pipeline-specific.
    pub dedup: DeduplicationConfiguration,
    /// Keyed-state configuration (mode-independent, always-on; inert when no
    /// collections are registered).
    pub keyed_state: KeyedStateConfiguration,
}

/// Configuration for middleware specific to pipeline consumers.
///
/// Bundles the retry, monopolization, and defer configurations that are only
/// used by the pipeline processing mode.
#[derive(Clone, Debug)]
pub struct PipelineMiddlewareConfiguration {
    /// Retry configuration for failed messages.
    pub retry: RetryConfiguration,
    /// Monopolization detection configuration.
    pub monopolization: MonopolizationConfiguration,
    /// Defer middleware configuration.
    pub defer: DeferConfiguration,
}

/// Configuration for middleware specific to low-latency consumers.
///
/// Bundles the retry and failure-topic configurations that are only
/// used by the low-latency processing mode.
#[derive(Clone, Debug)]
pub struct LowLatencyMiddlewareConfiguration {
    /// Retry configuration for failed messages.
    pub retry: RetryConfiguration,
    /// Failure topic configuration for routing unrecoverable messages.
    pub failure_topic: FailureTopicConfiguration,
}

/// High-level Kafka consumer implementation.
///
/// `ProsodyConsumer` is the main entry point for consuming messages from Kafka
/// topics. It manages partition assignments, message processing, and graceful
/// shutdown.
///
/// The consumer supports different message processing strategies:
/// - Pipeline processing with retries
/// - Low-latency processing with failure topic
/// - Best-effort processing with logging
///
/// The `C` type parameter is the [`Codec`] used to deserialize incoming
/// payloads; the consumer's payload type is `C::Payload`.
#[derive(Clone, Educe)]
#[educe(Debug)]
pub struct ProsodyConsumer<C: Codec = JsonCodec> {
    /// Flag to signal consumer shutdown.
    #[educe(Debug(ignore))]
    shutdown: Arc<AtomicBool>,

    /// Thread-safe storage for partition managers.
    #[educe(Debug(ignore))]
    managers: Arc<Managers<C::Payload>>,

    /// Current assigned-partition count, republished by the rebalance callback
    /// after every assign/revoke. Awaited by
    /// [`ProsodyConsumer::wait_for_assigned_partitions`].
    #[educe(Debug(ignore))]
    assignment: watch::Receiver<u32>,

    /// Runtime state of the consumer.
    #[educe(Debug(ignore))]
    runtime_state: Arc<Mutex<Option<RuntimeState>>>,

    /// Heartbeat registry for consumer-level actors.
    #[educe(Debug(ignore))]
    heartbeats: HeartbeatRegistry,
}

/// Keyed-state wiring inputs shared by both storage branches of
/// [`ProsodyConsumer::pipeline_consumer`].
struct KeyedStateInputs {
    config: KeyedStateConfiguration,
    group: ConsumerGroup,
    version: Arc<str>,
    registry: Arc<CollectionDefRegistry>,
}

impl KeyedStateInputs {
    /// Validates the registrations and derives the shared inputs. The dedup
    /// version doubles as the recovery oracle's hash version, so both
    /// middlewares read it from one source.
    fn new(
        config: KeyedStateConfiguration,
        consumer_config: &ConsumerConfiguration,
        dedup_version: &str,
    ) -> Result<Self, ConsumerError> {
        // Fail fast on an invalid timer slab size — the partition loop would
        // otherwise only log and fall back to a default at rebalance time.
        CompactDuration::try_from(consumer_config.slab_size)
            .map_err(ConsumerError::InvalidSlabSize)?;
        let registry = Arc::new(config.build_registry().map_err(KeyedStateInitError::from)?);
        Ok(Self {
            config,
            group: Arc::from(consumer_config.group_id.as_str()),
            version: Arc::from(dedup_version),
            registry,
        })
    }

    /// Builds the per-partition keyed-state provider over a branch's
    /// backend and loader. The partition loop acquires one state manager
    /// per assignment from it; the pending-index scanner now travels inside
    /// the backend the factory mints.
    fn provider<B, L>(&self, backend: B, loader: L) -> StateManagerProvider<B, L> {
        StateManagerProvider::new(
            backend,
            loader,
            self.registry.clone(),
            self.group.clone(),
            self.config.recovery_delay,
        )
    }
}

/// Shared middleware components for pipeline consumer construction.
///
/// Groups the middleware and configuration that are common to both memory
/// and Cassandra storage backends, reducing parameter counts.
struct PipelineMiddlewareStack {
    consumer_config: ConsumerConfiguration,
    defer_config: DeferConfiguration,
    common_config: CommonConfiguration,
    failure_tracker: FailureTracker,
    monopolization_middleware: Option<MonopolizationMiddleware>,
    retry_middleware: RetryMiddleware,
    heartbeats: HeartbeatRegistry,
    telemetry: Telemetry,
}

impl PipelineMiddlewareStack {
    fn build<T, MP, TP, DP, PP, SP, L, C>(
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
            CellSession<Loader: MessageLoader<Payload = C::Payload>>,
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

        // `build_common_middleware` constructs the whole common block —
        // including dedup — so the pipeline only layers its mode-specific
        // middleware OUTSIDE it. `.layer(x)` makes `x` the OUTERMOST layer, so
        // the stack runs outer→inner as:
        //
        //   retry → message_defer → timer_defer → monopolization
        //     → dedup → common(cancellation → scheduler → timeout
        //     → telemetry) → handler
        //
        // The keyed-state durability sequence is NOT in this stack — it runs
        // once after the stack returns, owned by the `settle` boundary (see
        // `consumer::middleware`'s `FallibleEventHandler` docs). Three residual
        // order facts remain: retry stays OUTERMOST so each attempt is a fresh
        // dispatch that resets the event's session (and the registered marker);
        // the defer middlewares sit OUTSIDE the common block so a defer-swallow
        // resets the session before swallowing into `Ok(Deferred)`, leaving no
        // marker to flush (the reload is therefore not deduped); and `dedup`
        // stays in-stack as a duplicate filter so the deferred-message reload
        // still filters producer duplicates. `monopolization` is state-agnostic.
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
            version,
            provider,
            trigger_provider,
            state_provider,
            &self.telemetry,
            self.heartbeats,
        )
    }
}

/// Multiplier on `recovery_delay` for the minimum deduplication TTL: the
/// dedup marker is the commit oracle, so it must outlive the recovery window
/// by a wide margin to survive rebalances and retries. See
/// [`validate_recovery_ttl_margin`].
const RECOVERY_TTL_DELAY_MULTIPLIER: u64 = 48;

/// Absolute floor for the deduplication TTL when state is registered, in
/// seconds (1 hour) — the larger of this and `48 × recovery_delay` applies.
const MIN_RECOVERY_EVIDENCE_TTL_SECONDS: u64 = 3_600;

/// Validates that the deduplication TTL clears the keyed-state recovery
/// window (resolution-before-evidence-expiry): `dedup.ttl ≥
/// max(48 × recovery_delay, 1h)`. The dedup marker is the commit oracle a
/// provisional cell is resolved against, so if the marker expires first the
/// cell can no longer be resolved correctly and a committed write is lost.
///
/// # Errors
///
/// Returns [`RecoveryTtlMarginError`] when the dedup TTL is below the margin.
fn validate_recovery_ttl_margin(
    dedup_ttl: Duration,
    recovery_delay: CompactDuration,
) -> Result<(), RecoveryTtlMarginError> {
    let recovery_delay_seconds = u64::from(recovery_delay.seconds());
    let required_seconds = recovery_delay_seconds
        .saturating_mul(RECOVERY_TTL_DELAY_MULTIPLIER)
        .max(MIN_RECOVERY_EVIDENCE_TTL_SECONDS);
    let dedup_ttl_seconds = dedup_ttl.as_secs();
    if dedup_ttl_seconds < required_seconds {
        return Err(RecoveryTtlMarginError {
            dedup_ttl: dedup_ttl_seconds,
            recovery_delay: recovery_delay_seconds,
            required: required_seconds,
        });
    }
    Ok(())
}

/// Builds the storage core shared by every constructor: the deduplication
/// store pair, keyed-state inputs, and heartbeat registry.
///
/// Validates `consumer_config` and `keyed_state_config` up front, before
/// [`StorePair::new`]'s Cassandra IO, so all callers fail fast uniformly. The
/// canonical `consumer_config.validate()` in [`initialize_consumer`] remains
/// the single invariant chokepoint; this early validation is the fail-fast
/// guard.
async fn build_shared_state(
    consumer_config: &ConsumerConfiguration,
    trigger_store_config: &TriggerStoreConfiguration,
    common_config: &CommonConfiguration,
) -> Result<(StorePair, KeyedStateInputs, HeartbeatRegistry), ConsumerError> {
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
    let heartbeats = HeartbeatRegistry::new(
        consumer_config.group_id.clone(),
        consumer_config.stall_threshold,
    );
    // Create both stores atomically — ensures trigger and defer stores match.
    let stores = StorePair::new(
        trigger_store_config,
        consumer_config.mock,
        dedup_config.ttl,
        dedup_config.cache_capacity,
        keyed_state_config.default_ttl,
        consumer_config.timer_spans,
    )
    .await?;
    let keyed_state =
        KeyedStateInputs::new(keyed_state_config, consumer_config, &dedup_config.version)?;
    Ok((stores, keyed_state, heartbeats))
}

/// Builds the storage pair, keyed-state inputs, and shared middleware stack
/// for [`ProsodyConsumer::pipeline_consumer`], enforcing the keyed-state
/// deduplication gate.
async fn prepare_pipeline_stack(
    consumer_config: &ConsumerConfiguration,
    trigger_store_config: &TriggerStoreConfiguration,
    pipeline_config: PipelineMiddlewareConfiguration,
    common_config: &CommonConfiguration,
    telemetry: Telemetry,
) -> Result<(StorePair, KeyedStateInputs, PipelineMiddlewareStack), ConsumerError> {
    let PipelineMiddlewareConfiguration {
        retry: retry_config,
        monopolization: monopolization_config,
        defer: defer_config,
    } = pipeline_config;
    let (stores, keyed_state, heartbeats) =
        build_shared_state(consumer_config, trigger_store_config, common_config).await?;
    let monopolization_middleware =
        MonopolizationMiddleware::new(&monopolization_config, &telemetry)?;
    let failure_tracker = FailureTracker::new(
        defer_config.failure_window,
        defer_config.failure_threshold,
        &telemetry,
        &heartbeats,
    );

    let stack = PipelineMiddlewareStack {
        consumer_config: consumer_config.clone(),
        defer_config,
        common_config: common_config.clone(),
        failure_tracker,
        monopolization_middleware,
        retry_middleware: RetryMiddleware::new(retry_config)?,
        heartbeats,
        telemetry,
    };

    Ok((stores, keyed_state, stack))
}

/// Builds the common middleware shared by every mode — the single place any
/// common-middleware component is constructed.
///
/// This is the whole cross-mode set: telemetry, timeout, scheduler,
/// cancellation, and **deduplication** (the mandatory commit oracle, here a
/// duplicate filter + marker register — the marker is flushed later by the
/// `settle` boundary, not in this stack). It runs outer→inner as
/// `dedup → cancellation → scheduler → timeout → telemetry → handler`. Each
/// mode layers only its *mode-specific* middleware (retry, monopolization,
/// defer, failure-topic, log) OUTSIDE the returned block.
///
/// Because the deduplication middleware needs the per-partition dedup store,
/// this is called INSIDE the storage `match` arm where the concrete
/// `dedup_provider` is in hand.
fn build_common_middleware<DP, P>(
    config: &CommonConfiguration,
    consumer_config: &ConsumerConfiguration,
    telemetry: Telemetry,
    dedup_provider: DP,
) -> Result<impl HandlerMiddleware<P>, ConsumerError>
where
    DP: DeduplicationStoreProvider,
    P: Send + Sync + 'static + EventIdentity,
{
    let scheduler_middleware = SchedulerMiddleware::new(&config.scheduler, &telemetry)?;
    let timeout_middleware =
        TimeoutMiddleware::new(&config.timeout, consumer_config.stall_threshold)?;
    let telemetry_middleware =
        TelemetryMiddleware::new(telemetry, Arc::from(consumer_config.group_id.as_str()));
    let dedup_middleware = DeduplicationMiddleware::new(
        config.dedup.clone(),
        &consumer_config.group_id,
        dedup_provider,
    )?;

    Ok(telemetry_middleware
        .layer(timeout_middleware)
        .layer(scheduler_middleware)
        .layer(CancellationMiddleware::new())
        .layer(dedup_middleware))
}

/// Builds the keyed-state provider for a [`StorePair::Memory`] arm (and the
/// stateless Cassandra arm): the in-memory durable store, backend factory, and
/// the caller's in-memory message loader, wrapped in the partition state
/// provider. The pipeline passes a `loader` it also hands to message defer; the
/// other constructors pass a fresh one. The factory is store-type agnostic —
/// the commit oracle's trigger store handle arrives per partition via
/// [`PartitionStateProvider::acquire`] — so the concrete return type serves any
/// trigger backend.
fn memory_state_provider<C: Codec>(
    keyed_state: &KeyedStateInputs,
    dedup_provider: MemoryDeduplicationStoreProvider,
    loader: MemoryLoader<C::Payload>,
) -> StateManagerProvider<
    MemoryStateBackendFactory<MemoryDeduplicationStoreProvider>,
    MemoryLoader<C::Payload>,
>
where
    C::Payload: EventType + Clone + EventIdentity + Send + Sync + 'static,
{
    let backend = MemoryStateBackendFactory::new(
        MemoryCells::new(),
        MemoryDescriptorIdentityStore::new(),
        keyed_state.registry.clone(),
        dedup_provider,
        keyed_state.group.clone(),
    );
    keyed_state.provider(backend, loader)
}

/// Builds the keyed-state provider for a [`StorePair::Cassandra`] arm: opens
/// the fjall workspace, mints the backend factory over the caller's Kafka
/// loader, and wraps it in the partition state provider. Shared by every
/// constructor's Cassandra arm; the caller owns the loader so the pipeline can
/// hand the same one to its message-defer middleware.
fn cassandra_state_provider<C: Codec>(
    keyed_state: &KeyedStateInputs,
    dedup_provider: CassandraDeduplicationStoreProvider,
    cell_store: CassandraCellResources,
    identity_store: CassandraDescriptorIdentityStore,
    loader: KafkaLoader<C>,
) -> Result<
    StateManagerProvider<
        CassandraStateBackendFactory<CassandraDeduplicationStoreProvider>,
        KafkaLoader<C>,
    >,
    ConsumerError,
>
where
    C::Payload: EventType + Clone + EventIdentity + Send + Sync + 'static,
{
    // The fjall workspace root is wiped on restart (Cassandra is
    // authoritative), so creating the default directory here is safe.
    fs::create_dir_all(&keyed_state.config.cache_dir)?;
    let fjall_client =
        FjallClient::open(&keyed_state.config.cache_dir).map_err(KeyedStateInitError::from)?;
    let backend = CassandraStateBackendFactory::new(
        fjall_client,
        cell_store,
        identity_store,
        keyed_state.registry.clone(),
        dedup_provider,
        keyed_state.group.clone(),
    );
    Ok(keyed_state.provider(backend, loader))
}

/// Initializes a Prosody consumer with a trigger store provider, wiring the
/// partition machinery to a Kafka consumer and starting its background poll
/// loop. The provider creates per-partition stores with independent caches.
///
/// # Errors
///
/// This function returns an error if:
/// - The hostname cannot be retrieved for the client ID
/// - The Kafka consumer cannot be created with the provided configuration
/// - Topic subscription fails
/// - The probe server cannot be started (if enabled)
fn initialize_consumer<T, P, SP, C>(
    consumer_config: &ConsumerConfiguration,
    version: Arc<str>,
    handler_provider: T,
    trigger_provider: P,
    state_provider: SP,
    telemetry: &Telemetry,
    heartbeats: HeartbeatRegistry,
) -> Result<ProsodyConsumer<C>, ConsumerError>
where
    T: HandlerProvider,
    T::Handler: EventHandler<Payload = C::Payload>,
    P: TriggerStoreProvider,
    SP: PartitionStateProvider<P::Store>,
    <SP::Manager as PartitionStateManager>::Session:
        CellSession<Loader: MessageLoader<Payload = C::Payload>>,
    C: Codec,
    C::Payload: EventType + Clone + EventIdentity,
{
    // Validate the configuration
    consumer_config.validate()?;

    // Initialize shared state
    let watermark_version: Arc<WatermarkVersion> = Arc::default();
    let managers: Arc<Managers<C::Payload>> = Arc::default();
    let shutdown: Arc<AtomicBool> = Arc::default();
    let (assignment_tx, assignment) = watch::channel(0u32);

    // Create the consumer context with the message handler and shared state
    let context = new_context(
        consumer_config,
        handler_provider,
        PartitionProviders {
            triggers: trigger_provider,
            state: state_provider,
        },
        watermark_version.clone(),
        ManagerRegistry {
            managers: managers.clone(),
            assignment_tx,
        },
        telemetry.sender(),
        version,
    )?;

    // Use mock cluster for testing or real bootstrap servers
    let bootstrap = if consumer_config.mock {
        MOCK_CLUSTER_BOOTSTRAP.clone()
    } else {
        consumer_config.bootstrap_servers.join(",")
    };

    // Configure and create the Kafka consumer with optimal settings
    let mut client_config = ClientConfig::new();
    client_config
        .set("bootstrap.servers", bootstrap)
        .set("client.id", hostname()?)
        .set("group.id", &consumer_config.group_id)
        .set("enable.auto.commit", "true")
        .set(
            "auto.commit.interval.ms",
            consumer_config.commit_interval.as_millis().to_string(),
        )
        .set("enable.auto.offset.store", "false")
        .set("auto.offset.reset", "earliest")
        .set("partition.assignment.strategy", "cooperative-sticky")
        .set_log_level(RDKafkaLogLevel::Error);

    let consumer: BaseConsumer<_> = client_config.create_with_context(context)?;

    // Subscribe to the specified topics
    let topics: Vec<&str> = consumer_config
        .subscribed_topics
        .iter()
        .map(String::as_str)
        .collect();

    consumer.subscribe(&topics)?;

    // Spawn the background polling task with monitoring
    let poll_interval = consumer_config.poll_interval;
    let heartbeat = heartbeats.register("Kafka poll loop");
    let cloned_managers = managers.clone();
    let cloned_heartbeat = heartbeat.clone();
    let cloned_shutdown = shutdown.clone();
    let max_message_count = consumer_config.max_uncommitted;
    let message_spans = consumer_config.message_spans;
    let poll_handle = spawn_blocking(move || {
        poll(PollConfig {
            poll_interval,
            max_message_count,
            consumer,
            codec: C::default(),
            watermark_version: &watermark_version,
            managers: &cloned_managers,
            heartbeat: &cloned_heartbeat,
            shutdown: &cloned_shutdown,
            message_spans,
        });
    });

    // Start optional probe server for health monitoring
    let probe_server = consumer_config
        .probe_port
        .filter(|_| !consumer_config.mock)
        .map(|port| ProbeServer::new(port, managers.clone(), heartbeats.clone()))
        .transpose()?;

    let runtime_state = Arc::new(Mutex::new(Some(RuntimeState {
        poll_handle,
        probe_server,
    })));

    Ok(ProsodyConsumer {
        shutdown,
        managers,
        assignment,
        runtime_state,
        heartbeats,
    })
}

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
    /// rejected with [`KeyedStateInitError::StateUnsupported`]. Use a
    /// high-level constructor ([`Self::pipeline_consumer`],
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
        consumer_config.validate()?;
        keyed_state_config.validate()?;

        // The `settle` durability boundary never runs on this path, so a
        // registered collection could never be staged or recovered. Reject it
        // rather than silently accept a non-functional registration.
        if keyed_state_config.has_registrations() {
            return Err(KeyedStateInitError::StateUnsupported.into());
        }

        let heartbeats = HeartbeatRegistry::new(
            consumer_config.group_id.clone(),
            consumer_config.stall_threshold,
        );

        // Build the empty keyed-state backend the partition machinery requires.
        // Deduplication is structurally mandatory in the store layer, but with
        // no dedup/state middleware on this path the dedup store stays inert and
        // its oracle is never consulted — an empty registry stages nothing — so a
        // minimal cache capacity is sufficient.
        let stores = StorePair::new(
            trigger_store_config,
            consumer_config.mock,
            Duration::default(),
            NonZeroUsize::MIN,
            keyed_state_config.default_ttl,
            consumer_config.timer_spans,
        )
        .await?;
        let keyed_state = KeyedStateInputs::new(
            keyed_state_config,
            consumer_config,
            DEFAULT_IDEMPOTENCE_VERSION,
        )?;

        match stores {
            StorePair::Memory {
                trigger_provider,
                dedup_provider,
                ..
            } => {
                let state_provider = memory_state_provider::<C>(
                    &keyed_state,
                    dedup_provider,
                    MemoryLoader::<C::Payload>::new(),
                );
                initialize_consumer::<_, _, _, C>(
                    consumer_config,
                    keyed_state.version.clone(),
                    handler_provider,
                    trigger_provider,
                    state_provider,
                    &telemetry,
                    heartbeats,
                )
            }
            StorePair::Cassandra {
                trigger_provider, ..
            } => {
                // Stateless consumer: the `settle` boundary never runs and the
                // registry is provably empty (rejected above otherwise), so no
                // session can ever stage. Back keyed state with the inert memory
                // provider rather than `cassandra_state_provider`, which would
                // otherwise spawn a loader `BaseConsumer` + poll thread and
                // create the fjall cache dir for a backend that stages nothing.
                // The real Cassandra `trigger_provider` still drives the timer
                // system, and its per-partition store handle is what the (never
                // consulted) commit oracle receives at acquisition.
                let state_provider = memory_state_provider::<C>(
                    &keyed_state,
                    MemoryDeduplicationStoreProvider::new(),
                    MemoryLoader::<C::Payload>::new(),
                );
                initialize_consumer::<_, _, _, C>(
                    consumer_config,
                    keyed_state.version.clone(),
                    handler_provider,
                    trigger_provider,
                    state_provider,
                    &telemetry,
                    heartbeats,
                )
            }
        }
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
        consumer_config: &ConsumerConfiguration,
        trigger_store_config: &TriggerStoreConfiguration,
        pipeline_config: PipelineMiddlewareConfiguration,
        common_config: &CommonConfiguration,
        telemetry: Telemetry,
        handler: T,
    ) -> Result<Self, ConsumerError>
    where
        T: FallibleHandler<Payload = C::Payload> + Clone + Send + Sync + 'static,
        C::Payload: EventIdentity + Clone,
    {
        let (stores, keyed_state, stack) = prepare_pipeline_stack(
            consumer_config,
            trigger_store_config,
            pipeline_config,
            common_config,
            telemetry,
        )
        .await?;

        match stores {
            StorePair::Memory {
                trigger_provider,
                message_provider,
                timer_provider,
                dedup_provider,
            } => {
                // Memory backend is also the mock-mode path, which must not
                // touch Kafka — pair it with an in-memory loader, shared by
                // message defer and the keyed-state Kafka-message handles.
                let loader = MemoryLoader::<C::Payload>::new();
                let state_provider = memory_state_provider::<C>(
                    &keyed_state,
                    dedup_provider.clone(),
                    loader.clone(),
                );
                let message_defer_middleware = MessageDeferMiddleware::new(
                    stack.defer_config.clone(),
                    &stack.consumer_config,
                    message_provider,
                    stack.failure_tracker.clone(),
                    loader,
                    &stack.telemetry,
                )?;
                stack.build::<_, _, _, _, _, _, _, C>(
                    message_defer_middleware,
                    timer_provider,
                    dedup_provider,
                    trigger_provider,
                    state_provider,
                    handler,
                )
            }
            StorePair::Cassandra {
                trigger_provider,
                message_provider,
                timer_provider,
                dedup_provider,
                cell_store,
                identity_store,
            } => {
                // One Kafka loader per consumer: built once here and shared by
                // cloning. A clone shares the channel, semaphore, cache, and the
                // single background poll thread — so the message-defer middleware
                // and the keyed-state provider resolve through the same loader,
                // never two `BaseConsumer`s.
                let loader =
                    KafkaLoader::<C>::for_consumer(&stack.consumer_config, &stack.heartbeats)
                        .map_err(DeferInitError::from)?;
                let state_provider = cassandra_state_provider::<C>(
                    &keyed_state,
                    dedup_provider.clone(),
                    cell_store,
                    identity_store,
                    loader.clone(),
                )?;
                let message_defer_middleware = MessageDeferMiddleware::new(
                    stack.defer_config.clone(),
                    &stack.consumer_config,
                    message_provider,
                    stack.failure_tracker.clone(),
                    loader,
                    &stack.telemetry,
                )?;
                stack.build::<_, _, _, _, _, _, _, C>(
                    message_defer_middleware,
                    timer_provider,
                    dedup_provider,
                    trigger_provider,
                    state_provider,
                    handler,
                )
            }
        }
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
        consumer_config: &ConsumerConfiguration,
        trigger_store_config: &TriggerStoreConfiguration,
        low_latency_config: LowLatencyMiddlewareConfiguration,
        common_config: &CommonConfiguration,
        producer: ProsodyProducer<C>,
        telemetry: Telemetry,
        handler: T,
    ) -> Result<Self, ConsumerError>
    where
        T: FallibleHandler<Payload = C::Payload> + Clone + Send + Sync + 'static,
        C::Payload: EventIdentity + Clone + Send + Sync + 'static,
    {
        let LowLatencyMiddlewareConfiguration {
            retry: retry_config,
            failure_topic: topic_config,
        } = low_latency_config;
        let (stores, keyed_state, heartbeats) =
            build_shared_state(consumer_config, trigger_store_config, common_config).await?;
        let version = keyed_state.version.clone();
        let retry_middleware = RetryMiddleware::new(retry_config)?;
        let topic_middleware =
            FailureTopicMiddleware::new(topic_config, consumer_config.group_id.clone(), producer)?;

        // dedup lives inside the common block; the failure-topic/retry chain
        // layers OUTSIDE it, so a routed failure (which the topic middleware
        // swallows) is never recorded as a commit. The keyed-state durability
        // sequence runs after the stack returns, in the `settle` boundary.
        // Built per storage arm because the dedup store lives there.
        match stores {
            StorePair::Memory {
                trigger_provider,
                dedup_provider,
                ..
            } => {
                let state_provider = memory_state_provider::<C>(
                    &keyed_state,
                    dedup_provider.clone(),
                    MemoryLoader::new(),
                );
                let provider = build_common_middleware::<_, C::Payload>(
                    common_config,
                    consumer_config,
                    telemetry.clone(),
                    dedup_provider,
                )?
                .layer(retry_middleware.clone()) // retry the task a fixed number of times
                .layer(topic_middleware) // write to failure topic
                .layer(retry_middleware) // retry writing to the failure topic indefinitely
                .into_provider(handler);
                initialize_consumer::<_, _, _, C>(
                    consumer_config,
                    version,
                    provider,
                    trigger_provider,
                    state_provider,
                    &telemetry,
                    heartbeats,
                )
            }
            StorePair::Cassandra {
                trigger_provider,
                dedup_provider,
                cell_store,
                identity_store,
                ..
            } => {
                let loader = KafkaLoader::<C>::for_consumer(consumer_config, &heartbeats)
                    .map_err(DeferInitError::from)?;
                let state_provider = cassandra_state_provider::<C>(
                    &keyed_state,
                    dedup_provider.clone(),
                    cell_store,
                    identity_store,
                    loader,
                )?;
                let provider = build_common_middleware::<_, C::Payload>(
                    common_config,
                    consumer_config,
                    telemetry.clone(),
                    dedup_provider,
                )?
                .layer(retry_middleware.clone())
                .layer(topic_middleware)
                .layer(retry_middleware)
                .into_provider(handler);
                initialize_consumer::<_, _, _, C>(
                    consumer_config,
                    version,
                    provider,
                    trigger_provider,
                    state_provider,
                    &telemetry,
                    heartbeats,
                )
            }
        }
    }

    /// Creates a new `ProsodyConsumer` with logging middleware for failure
    /// handling.
    ///
    /// The best-effort approach is the simplest - it tries to process
    /// messages once, logs any failures, and moves on. This approach should
    /// only be used for development or for services where occasional
    /// message loss is acceptable.
    pub(crate) async fn best_effort_consumer<T>(
        consumer_config: &ConsumerConfiguration,
        trigger_store_config: &TriggerStoreConfiguration,
        common_config: &CommonConfiguration,
        telemetry: Telemetry,
        handler: T,
    ) -> Result<Self, ConsumerError>
    where
        T: FallibleHandler<Payload = C::Payload> + Clone + Send + Sync + 'static,
        C::Payload: EventIdentity + Clone + Send + Sync + 'static,
    {
        let (stores, keyed_state, heartbeats) =
            build_shared_state(consumer_config, trigger_store_config, common_config).await?;
        let version = keyed_state.version.clone();

        // dedup lives inside the common block; `log` (which swallows failures)
        // layers OUTSIDE it. The keyed-state durability sequence runs after the
        // stack returns, in the `settle` boundary. Built per storage arm
        // because the dedup store lives there.
        match stores {
            StorePair::Memory {
                trigger_provider,
                dedup_provider,
                ..
            } => {
                let state_provider = memory_state_provider::<C>(
                    &keyed_state,
                    dedup_provider.clone(),
                    MemoryLoader::new(),
                );
                let provider = build_common_middleware::<_, C::Payload>(
                    common_config,
                    consumer_config,
                    telemetry.clone(),
                    dedup_provider,
                )?
                .layer(LogMiddleware::new())
                .into_provider(handler);
                initialize_consumer::<_, _, _, C>(
                    consumer_config,
                    version,
                    provider,
                    trigger_provider,
                    state_provider,
                    &telemetry,
                    heartbeats,
                )
            }
            StorePair::Cassandra {
                trigger_provider,
                dedup_provider,
                cell_store,
                identity_store,
                ..
            } => {
                let loader = KafkaLoader::<C>::for_consumer(consumer_config, &heartbeats)
                    .map_err(DeferInitError::from)?;
                let state_provider = cassandra_state_provider::<C>(
                    &keyed_state,
                    dedup_provider.clone(),
                    cell_store,
                    identity_store,
                    loader,
                )?;
                let provider = build_common_middleware::<_, C::Payload>(
                    common_config,
                    consumer_config,
                    telemetry.clone(),
                    dedup_provider,
                )?
                .layer(LogMiddleware::new())
                .into_provider(handler);
                initialize_consumer::<_, _, _, C>(
                    consumer_config,
                    version,
                    provider,
                    trigger_provider,
                    state_provider,
                    &telemetry,
                    heartbeats,
                )
            }
        }
    }
}

impl<C: Codec> ProsodyConsumer<C> {
    /// Returns the number of currently assigned partitions.
    ///
    /// This method is useful for monitoring how many partitions have been
    /// assigned to this consumer instance by Kafka's partition assignment
    /// strategy.
    ///
    /// # Returns
    ///
    /// The number of partitions currently assigned to this consumer.
    #[must_use]
    pub fn assigned_partition_count(&self) -> u32 {
        get_assigned_partition_count(&self.managers)
    }

    /// Awaits until at least `count` partitions are assigned, returning the
    /// observed count.
    ///
    /// The rebalance callback republishes the assignment count after every
    /// assign/revoke, so this awaits that transition rather than polling
    /// [`assigned_partition_count`](Self::assigned_partition_count). Returns
    /// immediately if the current count already satisfies `count`. If the
    /// publishing side has shut down, it falls back to a direct read.
    pub async fn wait_for_assigned_partitions(&self, count: u32) -> u32 {
        match self
            .assignment
            .clone()
            .wait_for(|&assigned| assigned >= count)
            .await
        {
            Ok(assigned) => *assigned,
            Err(_) => self.assigned_partition_count(),
        }
    }

    /// Checks if any assigned partition or consumer-level actor is stalled.
    ///
    /// A partition is considered stalled if it hasn't processed messages
    /// within the configured stall threshold duration. Consumer-level actors
    /// (main poll loop, defer middleware) are also monitored for stalls.
    ///
    /// # Returns
    ///
    /// `true` if any partition or consumer-level actor is stalled, `false`
    /// otherwise.
    #[must_use]
    pub fn is_stalled(&self) -> bool {
        get_is_stalled(&self.managers) || self.heartbeats.any_stalled()
    }

    /// Initiates a graceful shutdown of the Kafka consumer.
    ///
    /// This method stops polling for new messages and waits for any in-flight
    /// message processing to complete or timeout.
    pub async fn shutdown(mut self) {
        self.execute_shutdown().await;
    }

    /// Executes the consumer shutdown process.
    ///
    /// This method is used by both the public shutdown method and the drop
    /// handler to ensure resources are properly cleaned up.
    async fn execute_shutdown(&mut self) {
        // Signal the consumer to shut down
        self.shutdown.store(true, Ordering::Relaxed);

        // Attempt to take the handle from the mutex
        let Some(RuntimeState {
            poll_handle,
            probe_server,
        }) = self.runtime_state.lock().take()
        else {
            return;
        };

        // Wait for the polling task to complete
        if let Err(error) = poll_handle.await {
            error!("consumer shutdown failed: {error:#}");
        }

        // Shutdown probe server if it exists
        if let Some(probe_server) = probe_server {
            probe_server.shutdown().await;
        }
    }
}

/// Ensures graceful shutdown when the consumer is dropped.
///
/// This implementation guarantees that resources are cleaned up even if
/// the consumer is dropped without explicitly calling `shutdown()`.
impl<C: Codec> Drop for ProsodyConsumer<C> {
    fn drop(&mut self) {
        block_on(self.execute_shutdown());
    }
}

/// Returns the number of assigned partitions.
///
/// # Arguments
///
/// * `managers` - The map of partition managers.
///
/// # Returns
///
/// The number of partitions currently assigned to this consumer.
pub(crate) fn get_assigned_partition_count<P: Send + Sync + 'static>(
    managers: &Managers<P>,
) -> u32 {
    managers.read().len() as u32
}

/// Checks if any partition is stalled.
///
/// A partition is considered stalled if it hasn't processed messages within
/// the configured stall threshold duration or if its processing loop is
/// blocked.
///
/// # Arguments
///
/// * `managers` - The map of partition managers.
///
/// # Returns
///
/// `true` if any partition is stalled, `false` otherwise.
pub(crate) fn get_is_stalled<P: Send + Sync + 'static>(managers: &Managers<P>) -> bool {
    managers.read().values().any(PartitionManager::is_stalled)
}

/// Errors that can occur during consumer operations.
///
/// This enum covers various error conditions that might occur when
/// creating, configuring, or operating a Kafka consumer.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ConsumerError {
    /// Indicates invalid consumer configuration.
    #[error("invalid consumer configuration: {0:#}")]
    Configuration(#[from] ValidationErrors),

    /// Indicates an invalid event type pattern.
    #[error("invalid allowed events pattern: {0:#}")]
    AllowedEventsPattern(#[from] aho_corasick::BuildError),

    /// Indicates an IO failure.
    #[error("IO error: {0:#}")]
    Io(#[from] io::Error),

    /// Indicates a failure to retrieve the hostname.
    #[error("failed to get hostname: {0:#}")]
    Hostname(#[from] whoami::Error),

    /// Indicates a Kafka operation failure.
    #[error("Kafka operation failed: {0:#}")]
    Kafka(#[from] KafkaError),

    /// Indicates a Cassandra trigger store operation failure.
    #[error("Cassandra trigger store operation failed: {0:#}")]
    CassandraTriggerStore(Box<CassandraTriggerStoreError>),

    /// Indicates a scheduler initialization failure.
    #[error("Scheduler initialization failed: {0:#}")]
    Scheduler(#[from] SchedulerInitError),

    /// Indicates a timeout middleware initialization failure.
    #[error("Timeout initialization failed: {0:#}")]
    Timeout(#[from] TimeoutInitError),

    /// Indicates a monopolization middleware initialization failure.
    #[error("Monopolization initialization failed: {0:#}")]
    Monopolization(#[from] MonopolizationInitError),

    /// Indicates a defer middleware initialization failure.
    #[error("Defer initialization failed: {0:#}")]
    Defer(#[from] DeferInitError),

    /// Indicates storage backend creation failure.
    #[error("Failed to create storage backend: {0:#}")]
    StorageBackend(Box<storage::StoreCreationError>),

    /// Indicates an invalid timer slab size.
    #[error("Invalid timer slab size: {0:#}")]
    InvalidSlabSize(#[from] CompactDurationError),

    /// Indicates a keyed-state initialization failure.
    #[error("Keyed-state initialization failed: {0:#}")]
    KeyedState(#[from] KeyedStateInitError),
}

/// Errors raised while wiring the keyed-state layer into a pipeline
/// consumer.
#[derive(Debug, Error)]
pub enum KeyedStateInitError {
    /// The keyed-state configuration could not be built from the environment.
    #[error("invalid keyed-state configuration: {0:#}")]
    Configuration(#[from] KeyedStateConfigurationBuilderError),

    /// A descriptor registration was invalid or conflicted.
    #[error(transparent)]
    Register(#[from] RegisterStateError),

    /// The deduplication TTL is too short to outlive the keyed-state
    /// recovery window.
    #[error(transparent)]
    RecoveryTtlMargin(#[from] RecoveryTtlMarginError),

    /// The local fjall cache could not be opened.
    #[error("failed to open the keyed-state cache: {0:#}")]
    Cache(#[from] FjallClientError),

    /// Keyed-state collections were registered on the low-level
    /// [`ProsodyConsumer::new`] constructor, which runs no state middleware to
    /// stage or recover them. Build a high-level consumer (e.g.
    /// [`ProsodyConsumer::pipeline_consumer`]) to use keyed state.
    #[error(
        "keyed-state collections require a high-level consumer; the low-level `new` constructor \
         runs no state middleware"
    )]
    StateUnsupported,
}

/// The deduplication TTL is below the keyed-state recovery margin: a
/// provisional cell could outlive its commit-oracle marker and be lost under
/// crash recovery. Returned at consumer build when state collections are
/// registered. Raise `PROSODY_IDEMPOTENCE_TTL` or lower
/// `recovery_delay` so `dedup.ttl ≥ max(48 × recovery_delay, 1h)` holds.
///
/// All fields are in seconds.
#[derive(Debug, Error)]
#[error(
    "deduplication TTL {dedup_ttl} seconds is below the keyed-state recovery margin of {required} \
     seconds (the larger of 48 × recovery_delay {recovery_delay}s and 3600s); a provisional cell \
     could outlive its commit-oracle marker and be lost"
)]
pub struct RecoveryTtlMarginError {
    /// The configured deduplication TTL.
    dedup_ttl: u64,

    /// The configured keyed-state recovery delay.
    recovery_delay: u64,

    /// The minimum deduplication TTL the configuration must meet.
    required: u64,
}

impl From<CassandraTriggerStoreError> for ConsumerError {
    fn from(e: CassandraTriggerStoreError) -> Self {
        Self::CassandraTriggerStore(Box::new(e))
    }
}

impl From<storage::StoreCreationError> for ConsumerError {
    fn from(e: storage::StoreCreationError) -> Self {
        Self::StorageBackend(Box::new(e))
    }
}
