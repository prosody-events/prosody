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
//! - Message deduplication
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
//! use prosody::consumer::{
//!     ConsumerConfiguration, EventHandler, Keyed, ProsodyConsumer, Uncommitted,
//! };
//! use prosody::high_level::config::TriggerStoreConfiguration;
//! use prosody::timers::{UncommittedTimer, store::TriggerStore};
//!
//! // Implement your message handler
//! #[derive(Clone)]
//! struct MyHandler;
//!
//! impl EventHandler for MyHandler {
//!     async fn on_message<C>(&self, context: C, message: UncommittedMessage)
//!     where
//!         C: EventContext,
//!     {
//!         // Process the message
//!         println!("Processing message with key: {}", message.key());
//!
//!         // Commit the message when processing is complete
//!         message.commit().await;
//!     }
//!
//!     async fn on_timer<C, U>(&self, context: C, timer: U)
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
//! let consumer =
//!     ProsodyConsumer::new(&config, &TriggerStoreConfiguration::InMemory, MyHandler).await?;
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
//!
//! # Modules
//!
//! - `context`: Manages Kafka partition assignments and revocations
//! - `extractor`: Extracts tracing context from Kafka message headers
//! - `message`: Core message types for Kafka message processing
//! - `partition`: Manages per-partition message processing
//! - `poll`: Implements the Kafka message polling loop
//! - `failure`: Error handling strategies for message processing
//! - `heartbeat`: Monitoring for stalled processes
//! - `probes`: HTTP endpoints for health and readiness checking

use crate::consumer::poll::PollConfig;
use crate::consumer::probes::ProbeServer;
use crate::high_level::config::TriggerStoreConfiguration;
use std::env::var;
use std::fmt::Debug;
use std::future::Future;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use ahash::HashMap;
use aho_corasick::{AhoCorasick, StartKind};
use crossbeam_utils::CachePadded;
use derive_builder::Builder;
use educe::Educe;
use futures::executor::block_on;
use parking_lot::{Mutex, RwLock};
use rdkafka::ClientConfig;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::KafkaError;
use thiserror::Error;
use tokio::task::{JoinHandle, spawn_blocking};
use tracing::error;
use validator::{Validate, ValidationErrors};
use whoami::fallible::hostname;

use crate::consumer::event_context::EventContext;
use crate::consumer::kafka_context::Context;
use crate::consumer::message::UncommittedMessage;
use crate::consumer::middleware::concurrency::{
    ConcurrencyLimitConfiguration, ConcurrencyLimitMiddleware,
};
use crate::consumer::middleware::log::LogMiddleware;
use crate::consumer::middleware::retry::{RetryConfiguration, RetryMiddleware};
use crate::consumer::middleware::shutdown::ShutdownMiddleware;
use crate::consumer::middleware::topic::{FailureTopicConfiguration, FailureTopicMiddleware};
use crate::consumer::middleware::{FallibleHandler, HandlerMiddleware};
use crate::consumer::partition::PartitionManager;
use crate::consumer::poll::poll;
use crate::heartbeat::Heartbeat;
use crate::producer::ProsodyProducer;
use crate::timers::UncommittedTimer;
use crate::timers::store::TriggerStore;
use crate::timers::store::cassandra::{CassandraTriggerStore, CassandraTriggerStoreError};
use crate::timers::store::memory::InMemoryTriggerStore;
use crate::util::{
    from_duration_env_with_fallback, from_env, from_env_with_fallback,
    from_option_env_with_fallback, from_optional_vec_env, from_vec_env,
};
use crate::{MOCK_CLUSTER_BOOTSTRAP, Partition, Topic};

pub mod event_context;
mod extractor;
mod kafka_context;
pub mod message;
pub mod middleware;
mod partition;
mod poll;
mod probes;

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
type Managers = RwLock<HashMap<(Topic, Partition), PartitionManager>>;

/// Consumer runtime components returned by consumer initialization.
///
/// Contains the partition managers and runtime state necessary for operating
/// a consumer instance. This type alias eliminates clippy warnings about
/// complex return types.
type ConsumerComponents = (Arc<Managers>, Arc<Mutex<Option<RuntimeState>>>);

/// Environment variable name for the Kafka consumer group ID.
const PROSODY_GROUP_ID: &str = "PROSODY_GROUP_ID";

/// Defines a type with an associated key.
///
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
    ///
    /// # Returns
    ///
    /// A future that resolves when the message is handled.
    fn on_message<C>(
        &self,
        context: C,
        message: UncommittedMessage,
    ) -> impl Future<Output = ()> + Send
    where
        C: EventContext;

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
    ///
    /// # Processing Requirements
    ///
    /// Implementations must ensure that the timer is properly acknowledged:
    /// - Call [`timer.commit()`] after successful processing
    /// - Call [`timer.abort()`] if processing fails or should be retried
    ///
    /// # Returns
    ///
    /// A future that resolves when timer processing is complete. Note that
    /// this future completing does not automatically commit the timer.
    fn on_timer<C, T>(&self, context: C, timer: T) -> impl Future<Output = ()> + Send
    where
        C: EventContext,
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

/// Implements `HandlerProvider` for any `EventHandler` that is clonable.
///
/// This allows a single handler instance to be used for all partitions.
impl<T> HandlerProvider for T
where
    T: EventHandler + Clone + Send + Sync + 'static,
{
    type Handler = Self;

    fn handler_for_partition(&self, _: Topic, _: Partition) -> Self::Handler {
        self.clone()
    }
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

    /// Maximum number of enqueued messages per key.
    ///
    /// Environment variable: `PROSODY_MAX_ENQUEUED_PER_KEY`
    /// Default: 8
    ///
    /// Controls how many messages with the same key can be queued before
    /// backpressuring.
    #[builder(
        default = "from_env_with_fallback(\"PROSODY_MAX_ENQUEUED_PER_KEY\", 8)?",
        setter(into)
    )]
    #[validate(range(min = 1_usize))]
    pub max_enqueued_per_key: usize,

    /// Partition idempotence cache size.
    ///
    /// Environment variable: `PROSODY_IDEMPOTENCE_CACHE_SIZE`
    /// Default: 4096
    ///
    /// Size of the cache used to deduplicate messages with the same event ID.
    /// Set to 0 to disable message deduplication.
    #[builder(
        default = "from_env_with_fallback(\"PROSODY_IDEMPOTENCE_CACHE_SIZE\", 4096)?",
        setter(into)
    )]
    pub idempotence_cache_size: usize,

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
                   Duration::from_secs(5 * 60))?",
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
        default = "from_duration_env_with_fallback(\"PROSODY_SLAB_SIZE\", Duration::from_secs(10 \
                   * 60))?",
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
    /// Defaults to 10 minutes if not specified or if parsing from environment
    /// fails.
    pub slab_size: Duration,
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
    pub fn configured_consumer_group(&self) -> Option<String> {
        self.group_id.clone().or_else(|| var(PROSODY_GROUP_ID).ok())
    }
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
#[derive(Clone, Educe)]
#[educe(Debug)]
pub struct ProsodyConsumer {
    /// Flag to signal consumer shutdown.
    #[educe(Debug(ignore))]
    shutdown: Arc<AtomicBool>,

    /// Thread-safe storage for partition managers.
    #[educe(Debug(ignore))]
    managers: Arc<Managers>,

    /// Runtime state of the consumer.
    #[educe(Debug(ignore))]
    runtime_state: Arc<Mutex<Option<RuntimeState>>>,
}

impl ProsodyConsumer {
    /// Creates a new `ProsodyConsumer` instance.
    ///
    /// This is the low-level constructor that takes a custom handler provider.
    /// For common use cases, consider using the specialized constructors like
    /// `pipeline_consumer` or `low_latency_consumer`.
    ///
    /// # Arguments
    ///
    /// * `consumer_config` - The consumer configuration.
    /// * `trigger_store_config` - The trigger store configuration.
    /// * `handler_provider` - The message handler provider.
    ///
    /// # Returns
    ///
    /// A Result containing the new `ProsodyConsumer` instance or a
    /// `ConsumerError`.
    ///
    /// # Errors
    ///
    /// Returns a `ConsumerError` if:
    /// - The configuration is invalid
    /// - The hostname cannot be retrieved
    /// - The Kafka consumer cannot be created
    /// - The consumer fails to subscribe to the specified topics
    pub async fn new<T>(
        consumer_config: &ConsumerConfiguration,
        trigger_store_config: &TriggerStoreConfiguration,
        handler_provider: T,
    ) -> Result<Self, ConsumerError>
    where
        T: HandlerProvider,
    {
        // Validate the configuration
        consumer_config.validate()?;

        // Initialize shared state
        let watermark_version: Arc<WatermarkVersion> = Arc::default();
        let managers: Arc<Managers> = Arc::default();
        let shutdown: Arc<AtomicBool> = Arc::default();

        // Build event type search automaton for filtering messages
        let allowed_events = consumer_config
            .allowed_events
            .as_ref()
            .map(|prefixes| {
                AhoCorasick::builder()
                    .start_kind(StartKind::Anchored)
                    .build(prefixes)
            })
            .transpose()?;

        let (managers, runtime_state) = match trigger_store_config {
            TriggerStoreConfiguration::InMemory => initialize_consumer(
                consumer_config,
                handler_provider,
                InMemoryTriggerStore::new(),
                watermark_version.clone(),
                managers.clone(),
                allowed_events,
                shutdown.clone(),
            )?,
            TriggerStoreConfiguration::Cassandra(cassandra_config) => initialize_consumer(
                consumer_config,
                handler_provider,
                CassandraTriggerStore::new(cassandra_config).await?,
                watermark_version.clone(),
                managers.clone(),
                allowed_events,
                shutdown.clone(),
            )?,
        };

        Ok(Self {
            shutdown,
            managers,
            runtime_state,
        })
    }

    /// Creates a new `ProsodyConsumer` with a retry strategy for pipeline
    /// processing.
    ///
    /// Pipeline processing emphasizes reliability with automatic retries on
    /// failure. Messages that fail processing will be retried with
    /// exponential backoff.
    ///
    /// # Arguments
    ///
    /// * `consumer_config` - The consumer configuration.
    /// * `trigger_store_config` - The trigger store configuration.
    /// * `retry_config` - The retry configuration.
    /// * `handler` - The fallible message handler.
    ///
    /// # Returns
    ///
    /// A Result containing the new `ProsodyConsumer` instance or a
    /// `ConsumerError`.
    ///
    /// # Errors
    ///
    /// Returns a `ConsumerError` if the consumer creation fails.
    pub async fn pipeline_consumer<T>(
        consumer_config: &ConsumerConfiguration,
        trigger_store_config: &TriggerStoreConfiguration,
        retry_config: RetryConfiguration,
        concurrency_config: ConcurrencyLimitConfiguration,
        handler: T,
    ) -> Result<Self, ConsumerError>
    where
        T: FallibleHandler + Clone + Send + Sync + 'static,
    {
        let concurrency_middleware = ConcurrencyLimitMiddleware::new(&concurrency_config)?;
        let retry_middleware = RetryMiddleware::new(retry_config)?;

        // Apply concurrency limiting first, then shutdown and retry
        let middleware = concurrency_middleware
            .layer(ShutdownMiddleware)
            .layer(retry_middleware);
        let handler = middleware.with_handler(handler);
        Self::new(consumer_config, trigger_store_config, handler).await
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
    /// # Arguments
    ///
    /// * `consumer_config` - The consumer configuration.
    /// * `trigger_store_config` - The trigger store configuration.
    /// * `retry_config` - The retry configuration.
    /// * `topic_config` - The failure topic configuration.
    /// * `producer` - The Prosody producer for sending messages to the failure
    ///   topic.
    /// * `handler` - The fallible message handler.
    ///
    /// # Returns
    ///
    /// A Result containing the new `ProsodyConsumer` instance or a
    /// `ConsumerError`.
    ///
    /// # Errors
    ///
    /// Returns a `ConsumerError` if the consumer creation fails.
    pub async fn low_latency_consumer<T>(
        consumer_config: &ConsumerConfiguration,
        trigger_store_config: &TriggerStoreConfiguration,
        retry_config: RetryConfiguration,
        topic_config: FailureTopicConfiguration,
        concurrency_config: ConcurrencyLimitConfiguration,
        producer: ProsodyProducer,
        handler: T,
    ) -> Result<Self, ConsumerError>
    where
        T: FallibleHandler + Clone + Send + Sync + 'static,
    {
        let concurrency_middleware = ConcurrencyLimitMiddleware::new(&concurrency_config)?;
        let group_id = consumer_config.group_id.clone();
        let retry_middleware = RetryMiddleware::new(retry_config)?;
        let topic_middleware = FailureTopicMiddleware::new(topic_config, group_id, producer)?;

        // Compose middleware: concurrency → shutdown → retry → failure topic → retry
        let middleware = concurrency_middleware // limit global concurrency first
            .layer(ShutdownMiddleware) // stop processing if shutting down partition
            .layer(retry_middleware.clone()) // retry processing up to limit
            .layer(topic_middleware) // write to failure topic
            .layer(retry_middleware); // retry writing to failure topic

        let handler = middleware.with_handler(handler);
        Self::new(consumer_config, trigger_store_config, handler).await
    }

    /// Creates a new `ProsodyConsumer` with logging middleware for failure
    /// handling.
    ///
    /// The best-effort approach is the simplest - it tries to process
    /// messages once, logs any failures, and moves on. This approach should
    /// only be used for development or for services where occasional
    /// message loss is acceptable.
    ///
    /// # Arguments
    ///
    /// * `consumer_config` - The consumer configuration.
    /// * `trigger_store_config` - The trigger store configuration.
    /// * `handler` - The fallible message handler.
    ///
    /// # Returns
    ///
    /// A Result containing the new `ProsodyConsumer` instance or a
    /// `ConsumerError`.
    ///
    /// # Errors
    ///
    /// Returns a `ConsumerError` if the consumer creation fails.
    pub async fn best_effort_consumer<T>(
        consumer_config: &ConsumerConfiguration,
        trigger_store_config: &TriggerStoreConfiguration,
        concurrency_config: ConcurrencyLimitConfiguration,
        handler: T,
    ) -> Result<Self, ConsumerError>
    where
        T: FallibleHandler + Clone + Send + Sync + 'static,
    {
        let concurrency_middleware = ConcurrencyLimitMiddleware::new(&concurrency_config)?;

        // Apply concurrency limiting first, then shutdown and logging
        let middleware = concurrency_middleware
            .layer(ShutdownMiddleware)
            .layer(LogMiddleware);
        Self::new(
            consumer_config,
            trigger_store_config,
            middleware.with_handler(handler),
        )
        .await
    }

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

    /// Checks if any assigned partition is stalled.
    ///
    /// A partition is considered stalled if it hasn't processed messages
    /// within the configured stall threshold duration.
    ///
    /// # Returns
    ///
    /// `true` if any partition is stalled, `false` otherwise.
    #[must_use]
    pub fn is_stalled(&self) -> bool {
        get_is_stalled(&self.managers)
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
impl Drop for ProsodyConsumer {
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
fn get_assigned_partition_count(managers: &Managers) -> u32 {
    managers.read().len() as u32
}

/// Initializes and starts consumer components for Kafka message processing.
///
/// This function creates and configures the core consumer infrastructure
/// including the Kafka consumer, polling task, and optional probe server. It
/// sets up the consumer with proper configuration, subscribes to topics, and
/// returns the initialized components ready for message processing.
///
/// # Arguments
///
/// * `config` - The consumer configuration containing Kafka settings and
///   processing options
/// * `handler_provider` - Factory for creating message handlers per partition
/// * `trigger_store` - Storage backend for timer triggers
/// * `watermark_version` - Atomic counter for tracking offset changes
/// * `managers` - Thread-safe storage for partition managers
/// * `allowed_events` - Optional event type filter for message processing
/// * `shutdown` - Atomic flag for coordinating consumer shutdown
///
/// # Returns
///
/// A Result containing the initialized consumer components (managers and
/// runtime state) or a `ConsumerError` if initialization fails.
///
/// # Errors
///
/// This function returns an error if:
/// - The hostname cannot be retrieved for the client ID
/// - The Kafka consumer cannot be created with the provided configuration
/// - Topic subscription fails
/// - The probe server cannot be started (if enabled)
fn initialize_consumer<T, S>(
    config: &ConsumerConfiguration,
    handler_provider: T,
    trigger_store: S,
    watermark_version: Arc<WatermarkVersion>,
    managers: Arc<Managers>,
    allowed_events: Option<AhoCorasick>,
    shutdown: Arc<AtomicBool>,
) -> Result<ConsumerComponents, ConsumerError>
where
    T: HandlerProvider,
    S: TriggerStore,
{
    // Create the consumer context with the message handler and shared state
    let context = Context::new(
        config,
        handler_provider,
        trigger_store,
        watermark_version.clone(),
        managers.clone(),
        allowed_events,
    );

    // Use mock cluster for testing or real bootstrap servers
    let bootstrap = if config.mock {
        MOCK_CLUSTER_BOOTSTRAP.clone()
    } else {
        config.bootstrap_servers.join(",")
    };

    // Configure and create the Kafka consumer with optimal settings
    let mut client_config = ClientConfig::new();
    client_config
        .set("bootstrap.servers", bootstrap)
        .set("client.id", hostname()?)
        .set("group.id", &config.group_id)
        .set("enable.auto.commit", "true")
        .set(
            "auto.commit.interval.ms",
            config.commit_interval.as_millis().to_string(),
        )
        .set("enable.auto.offset.store", "false")
        .set("auto.offset.reset", "earliest")
        .set("partition.assignment.strategy", "cooperative-sticky")
        .set_log_level(RDKafkaLogLevel::Error);

    let consumer: BaseConsumer<_> = client_config.create_with_context(context)?;

    // Subscribe to the specified topics
    let topics: Vec<&str> = config
        .subscribed_topics
        .iter()
        .map(String::as_str)
        .collect();

    consumer.subscribe(&topics)?;

    // Spawn the background polling task with monitoring
    let poll_interval = config.poll_interval;
    let heartbeat = Heartbeat::new("Kafka poll loop", config.stall_threshold);
    let cloned_managers = managers.clone();
    let cloned_heartbeat = heartbeat.clone();
    let max_message_count = config.max_uncommitted;
    let poll_handle = spawn_blocking(move || {
        poll(PollConfig {
            poll_interval,
            max_message_count,
            consumer,
            watermark_version: &watermark_version,
            managers: &cloned_managers,
            heartbeat: &cloned_heartbeat,
            shutdown: &shutdown,
        });
    });

    // Start optional probe server for health monitoring
    let probe_server = config
        .probe_port
        .filter(|_| !config.mock)
        .map(|port| ProbeServer::new(port, managers.clone(), heartbeat))
        .transpose()?;

    let runtime_state = Arc::new(Mutex::new(Some(RuntimeState {
        poll_handle,
        probe_server,
    })));

    Ok((managers, runtime_state))
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
fn get_is_stalled(managers: &Managers) -> bool {
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

    /// Indicates a Kafka operation failure.
    #[error("Kafka operation failed: {0:#}")]
    Kafka(#[from] KafkaError),

    /// Indicates a Cassandra trigger store operation failure.
    #[error("Cassandra operation failed: {0:#}")]
    Cassandra(#[from] CassandraTriggerStoreError),
}
