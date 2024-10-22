//! Kafka consumer implementation for high-level message consumption and
//! processing.
//!
//! This module provides a robust abstraction for consuming messages from Kafka
//! topics, handling:
//!
//! - Offset management
//! - Consumer group coordination
//! - Message deserialization
//! - Backpressure management
//! - Error handling and recovery
//! - Distributed tracing
//!
//! The module consists of several submodules:
//!
//! - `context`: Manages Kafka partition assignments and revocations.
//! - `extractor`: Extracts metadata from Kafka messages for distributed
//!   tracing.
//! - `message`: Defines structures for message contexts and consumer messages.
//! - `partition`: Handles partition-specific operations and state management.
//! - `poll`: Implements the core message polling and processing loop.
//! - `failure`: Handles error handling, retry strategies, and failure topic
//!   management.
//! - `probes`: Implements a probe server for health and readiness checks.
//!
//! Users should primarily interact with the [`ProsodyConsumer`] struct,
//! configured via [`ConsumerConfiguration`]. Custom message processing logic is
//! defined by implementing the [`EventHandler`] trait.

use crate::consumer::probes::ProbeServer;
use std::fmt::Debug;
use std::future::Future;
use std::io;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use ahash::HashMap;
use crossbeam_utils::CachePadded;
use derive_builder::Builder;
use educe::Educe;
use futures::executor::block_on;
use parking_lot::{Mutex, RwLock};
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::KafkaError;
use rdkafka::ClientConfig;
use thiserror::Error;
use tokio::task::{spawn_blocking, JoinHandle};
use tracing::error;
use validator::{Validate, ValidationErrors};
use whoami::fallible::hostname;

use crate::consumer::context::Context;
use crate::consumer::failure::retry::{RetryConfiguration, RetryStrategy};
use crate::consumer::failure::shutdown::ShutdownStrategy;
use crate::consumer::failure::topic::{FailureTopicConfiguration, FailureTopicStrategy};
use crate::consumer::failure::{FailureStrategy, FallibleHandler};
use crate::consumer::message::{MessageContext, UncommittedMessage};
use crate::consumer::partition::PartitionManager;
use crate::consumer::poll::poll;
use crate::producer::ProsodyProducer;
use crate::util::{
    from_duration_env_with_fallback, from_env, from_env_with_fallback,
    from_option_env_with_fallback, from_vec_env,
};
use crate::{Partition, Topic};

mod context;
mod extractor;
pub mod failure;
pub mod message;
mod partition;
mod poll;
mod probes;

/// Atomic counter for tracking changes in partition watermarks.
type WatermarkVersion = CachePadded<AtomicUsize>;

/// Thread-safe storage for partition managers.
type Managers = RwLock<HashMap<(Topic, Partition), PartitionManager>>;

/// Defines a type with an associated key.
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

/// Provides handlers for processing messages from specific partitions.
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
pub trait EventHandler {
    /// Processes a consumed message.
    ///
    /// # Arguments
    ///
    /// * `context` - The message context.
    /// * `message` - The consumed message.
    ///
    /// # Returns
    ///
    /// A future that resolves when the message is handled.
    fn on_message(
        &self,
        context: MessageContext,
        message: UncommittedMessage,
    ) -> impl Future<Output = ()> + Send;

    /// Shuts down the message handler.
    ///
    /// # Returns
    ///
    /// A future that resolves when the shutdown is complete.
    fn shutdown(self) -> impl Future<Output = ()> + Send;
}

impl<T> HandlerProvider for T
where
    T: EventHandler + Clone + Send + Sync + 'static,
{
    type Handler = Self;

    fn handler_for_partition(&self, _: Topic, _: Partition) -> Self::Handler {
        self.clone()
    }
}

impl<T, Fut> EventHandler for T
where
    T: Fn(MessageContext, UncommittedMessage) -> Fut + Send + Sync,
    Fut: Future<Output = ()> + Send,
{
    async fn on_message(&self, context: MessageContext, message: UncommittedMessage) {
        self(context, message).await;
    }

    async fn shutdown(self) {}
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
    #[builder(default = "from_env(\"PROSODY_GROUP_ID\")?", setter(into))]
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

    /// Maximum number of uncommitted messages.
    ///
    /// Environment variable: `PROSODY_MAX_UNCOMMITTED`
    /// Default: 32
    #[builder(
        default = "from_env_with_fallback(\"PROSODY_MAX_UNCOMMITTED\", 32)?",
        setter(into)
    )]
    #[validate(range(min = 1_usize))]
    pub max_uncommitted: usize,

    /// Maximum number of enqueued messages per key.
    ///
    /// Environment variable: `PROSODY_MAX_ENQUEUED_PER_KEY`
    /// Default: 8
    #[builder(
        default = "from_env_with_fallback(\"PROSODY_MAX_ENQUEUED_PER_KEY\", 8)?",
        setter(into)
    )]
    #[validate(range(min = 1_usize))]
    pub max_enqueued_per_key: usize,

    /// Timeout for partition shutdown.
    ///
    /// Environment variable: `PROSODY_PARTITION_SHUTDOWN_TIMEOUT`
    /// Default: 5 seconds
    ///
    /// If set to None (or if the environment variable is set to "none"), the
    /// partition will immediately be shutdown without waiting for in-flight
    /// tasks to complete. As these tasks will not be committed, they will be
    /// retried when the partition is rebalanced to a new node. This may be
    /// appropriate when performing user-facing processing where delays due to
    /// rebalancing must be minimized.
    #[builder(
        default = "from_duration_env_with_fallback(\"PROSODY_PARTITION_SHUTDOWN_TIMEOUT\", \
                   Duration::from_secs(5))?",
        setter(into)
    )]
    pub partition_shutdown_timeout: Duration,

    /// Interval between poll operations.
    ///
    /// Environment variable: `PROSODY_POLL_INTERVAL`
    /// Default: 100 milliseconds
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
    #[builder(
        default = "from_env_with_fallback(\"PROSODY_MOCK\", false)?",
        setter(into)
    )]
    pub mock: bool,

    /// Port for the probe server.
    ///
    /// Environment variable: `PROSODY_PROBE_PORT`
    /// Default: Some(8000)
    #[builder(
        default = "from_option_env_with_fallback(\"PROSODY_PROBE_PORT\", 8000)?",
        setter(into)
    )]
    pub probe_port: Option<u16>,
}

impl ConsumerConfiguration {
    /// Creates a new `ConsumerConfigurationBuilder`.
    ///
    /// This method is a convenient way to start building a
    /// `ConsumerConfiguration`.
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
struct RuntimeState {
    /// Handle to the polling task.
    poll_handle: JoinHandle<()>,
    /// Optional probe server for health and readiness checks.
    probe_server: Option<ProbeServer>,
}

/// High-level Kafka consumer implementation.
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
    /// # Arguments
    ///
    /// * `config` - The consumer configuration.
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
    pub fn new<T>(
        config: &ConsumerConfiguration,
        handler_provider: T,
    ) -> Result<Self, ConsumerError>
    where
        T: HandlerProvider,
    {
        // Validate the configuration
        config.validate()?;

        // Initialize shared state
        let watermark_version: Arc<WatermarkVersion> = Arc::default();
        let managers: Arc<Managers> = Arc::default();
        let shutdown: Arc<AtomicBool> = Arc::default();

        // Create the consumer context with the message handler and shared state
        let context: Context<T> = Context::new(
            config,
            handler_provider,
            watermark_version.clone(),
            managers.clone(),
        );

        // Configure and create the Kafka consumer
        let mut client_config = ClientConfig::new();
        client_config
            .set("bootstrap.servers", config.bootstrap_servers.join(","))
            .set("client.id", hostname()?)
            .set("group.id", &config.group_id)
            .set("enable.auto.commit", "false")
            .set("enable.auto.offset.store", "false")
            .set("auto.offset.reset", "earliest")
            .set("partition.assignment.strategy", "cooperative-sticky")
            .set_log_level(RDKafkaLogLevel::Error);

        // Set up mock broker if configured
        if config.mock {
            client_config.set("test.mock.num.brokers", "3");
        }

        let consumer: BaseConsumer<_> = client_config.create_with_context(context)?;

        // Subscribe to the specified topics
        let topics: Vec<&str> = config
            .subscribed_topics
            .iter()
            .map(String::as_str)
            .collect();

        consumer.subscribe(&topics)?;

        // Spawn a blocking task to continuously poll for messages
        let poll_interval = config.poll_interval;
        let commit_interval = config.commit_interval;
        let cloned_managers = managers.clone();
        let cloned_shutdown = shutdown.clone();
        let poll_handle = spawn_blocking(move || {
            poll(
                poll_interval,
                commit_interval,
                &consumer,
                &watermark_version,
                &cloned_managers,
                &cloned_shutdown,
            );
        });

        let probe_server = config
            .probe_port
            .map(|port| ProbeServer::new(port, managers.clone()))
            .transpose()?;

        let runtime_state = Arc::new(Mutex::new(Some(RuntimeState {
            poll_handle,
            probe_server,
        })));

        Ok(Self {
            shutdown,
            managers,
            runtime_state,
        })
    }

    /// Creates a new `ProsodyConsumer` with a retry strategy for pipeline
    /// processing.
    ///
    /// # Arguments
    ///
    /// * `consumer_config` - The consumer configuration.
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
    pub fn pipeline_consumer<T>(
        consumer_config: &ConsumerConfiguration,
        retry_config: RetryConfiguration,
        handler: T,
    ) -> Result<Self, ConsumerError>
    where
        T: FallibleHandler + Clone + Send + Sync + 'static,
    {
        let retry_strategy = RetryStrategy::new(retry_config)?;
        let strategy = ShutdownStrategy.and_then(retry_strategy);
        let handler = strategy.with_handler(handler);
        Self::new(consumer_config, handler)
    }

    /// Creates a new `ProsodyConsumer` with a low-latency strategy.
    ///
    /// # Arguments
    ///
    /// * `consumer_config` - The consumer configuration.
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
    pub fn low_latency_consumer<T>(
        consumer_config: &ConsumerConfiguration,
        retry_config: RetryConfiguration,
        topic_config: FailureTopicConfiguration,
        producer: ProsodyProducer,
        handler: T,
    ) -> Result<Self, ConsumerError>
    where
        T: FallibleHandler + Clone + Send + Sync + 'static,
    {
        let group_id = consumer_config.group_id.clone();
        let retry_strategy = RetryStrategy::new(retry_config)?;
        let topic_strategy = FailureTopicStrategy::new(topic_config, group_id, producer)?;

        let strategy = ShutdownStrategy // stop processing if shutting down partition
            .and_then(retry_strategy.clone()) // retry processing up to limit
            .and_then(topic_strategy) // write to failure topic
            .and_then(retry_strategy); // retry writing to failure topic

        let handler = strategy.with_handler(handler);
        Self::new(consumer_config, handler)
    }

    /// Returns the number of currently assigned partitions.
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
    /// # Returns
    ///
    /// `true` if any partition is stalled, `false` otherwise.
    #[must_use]
    pub fn is_stalled(&self) -> bool {
        get_is_stalled(&self.managers)
    }

    /// Initiates a graceful shutdown of the Kafka consumer.
    pub async fn shutdown(mut self) {
        self.execute_shutdown().await;
    }

    /// Executes the consumer shutdown process.
    ///
    /// This method is used by both the public shutdown method and the drop
    /// handler.
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
/// The number of assigned partitions.
fn get_assigned_partition_count(managers: &Managers) -> u32 {
    managers.read().len() as u32
}

/// Checks if any partition is stalled.
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
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ConsumerError {
    /// Indicates invalid consumer configuration.
    #[error("invalid consumer configuration: {0:#}")]
    Configuration(#[from] ValidationErrors),

    /// Indicates an IO failure.
    #[error("IO error: {0:#}")]
    Io(#[from] io::Error),

    /// Indicates a Kafka operation failure.
    #[error("Kafka operation failed: {0:#}")]
    Kafka(#[from] KafkaError),
}
