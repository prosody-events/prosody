//! Kafka consumer implementation for high-level message consumption and
//! processing.
//!
//! This module provides a robust abstraction for consuming messages from Kafka
//! topics, handling complex operations such as:
//!
//! - Offset management: Tracking and committing message offsets to ensure
//!   at-least-once message processing semantics.
//! - Consumer group coordination: Participating in consumer groups for load
//!   balancing across multiple consumer instances.
//! - Message deserialization: Converting raw Kafka messages into typed Rust
//!   structures for easier processing.
//! - Backpressure management: Limiting the number of in-flight messages to
//!   prevent overwhelming downstream systems.
//! - Error handling and recovery: Gracefully handling various error scenarios
//!   and attempting to recover when possible.
//! - Distributed tracing: Integrating with OpenTelemetry for tracing message
//!   flow across distributed systems.
//!
//! The module consists of several submodules:
//!
//! - `context`: Manages Kafka partition assignments and revocations.
//! - `extractor`: Extracts metadata from Kafka messages for distributed
//!   tracing.
//! - `message`: Defines structures for message contexts and consumer messages.
//! - `partition`: Handles partition-specific operations and state management.
//! - `poll`: Implements the core message polling and processing loop.
//!
//! Users should primarily interact with the [`ProsodyConsumer`] struct,
//! configured via [`ConsumerConfiguration`]. Custom message processing logic is
//! defined by implementing the [`MessageHandler`] trait.

use std::fmt::Display;
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
use parking_lot::Mutex;
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
use crate::consumer::message::{ConsumerMessage, MessageContext};
use crate::consumer::partition::PartitionManager;
use crate::consumer::poll::poll;
use crate::util::{
    from_duration_env_with_fallback, from_env, from_env_with_fallback,
    from_option_duration_env_with_fallback, from_vec_env,
};
use crate::{Partition, Topic};

mod context;
mod extractor;
pub mod message;
mod partition;
mod poll;

/// Atomic counter for tracking changes in partition watermarks.
///
/// Used to efficiently notify the poll loop when offsets should be committed.
type WatermarkVersion = CachePadded<AtomicUsize>;

/// Thread-safe storage for partition managers.
///
/// Maps topic-partition pairs to their respective [`PartitionManager`]s.
type Managers = Mutex<HashMap<(Topic, Partition), PartitionManager>>;

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

/// Defines the behavior for handling consumed Kafka messages.
pub trait MessageHandler {
    /// The error type returned by the handler.
    type Error: Display;

    /// Processes a consumed message.
    ///
    /// # Arguments
    ///
    /// * `context` - Mutable reference to the message context.
    /// * `message` - The consumed message.
    ///
    /// # Returns
    ///
    /// A future that resolves to a Result indicating success or failure of
    /// message handling.
    fn handle(
        &self,
        context: &mut MessageContext,
        message: ConsumerMessage,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// Configuration for the Kafka consumer.
///
/// This struct holds all the necessary configuration options for creating a
/// Kafka consumer. It uses the Builder pattern for flexible initialization and
/// supports loading values from environment variables.
#[derive(Builder, Clone, Validate)]
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
        default = "from_option_duration_env_with_fallback(\"PROSODY_PARTITION_SHUTDOWN_TIMEOUT\", \
                   Duration::from_secs(5))?",
        setter(into)
    )]
    pub partition_shutdown_timeout: Option<Duration>,

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

/// High-level Kafka consumer implementation.
#[derive(Clone, Educe)]
#[educe(Debug)]
pub struct ProsodyConsumer {
    #[educe(Debug(ignore))]
    shutdown: Arc<AtomicBool>,

    #[educe(Debug(ignore))]
    handle: Arc<Mutex<Option<JoinHandle<()>>>>,
}

impl ProsodyConsumer {
    /// Creates a new `ProsodyConsumer` instance.
    ///
    /// # Arguments
    ///
    /// * `config` - The consumer configuration.
    /// * `message_handler` - The message handler implementation.
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
    pub fn new<T>(config: ConsumerConfiguration, message_handler: T) -> Result<Self, ConsumerError>
    where
        T: MessageHandler + Clone + Send + Sync + 'static,
    {
        // Validate the configuration
        config.validate()?;

        // Initialize shared state
        let watermark_version: Arc<WatermarkVersion> = Arc::default();
        let managers: Arc<Managers> = Arc::default();
        let shutdown: Arc<AtomicBool> = Arc::default();

        // Create the consumer context with the message handler and shared state
        let context = Context::new(
            &config,
            message_handler,
            watermark_version.clone(),
            managers.clone(),
        );

        // Configure and create the Kafka consumer
        let mut client_config = ClientConfig::new();
        client_config
            .set("bootstrap.servers", config.bootstrap_servers.join(","))
            .set("client.id", hostname()?)
            .set("group.id", config.group_id)
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
        let cloned_shutdown = shutdown.clone();
        let handle = Arc::new(Mutex::new(Some(spawn_blocking(move || {
            poll(
                config.poll_interval,
                config.commit_interval,
                &consumer,
                &watermark_version,
                &managers,
                &cloned_shutdown,
            );
        }))));

        Ok(Self { shutdown, handle })
    }

    /// Initiates a graceful shutdown of the Kafka consumer.
    ///
    /// This method signals the consumer to stop and waits for the polling task
    /// to complete.
    pub async fn shutdown(mut self) {
        self.execute_shutdown().await;
    }

    /// Execute a consumer shutdown.
    ///
    /// Used by both the public shutdown method and the drop handler.
    async fn execute_shutdown(&mut self) {
        // Signal the consumer to shut down
        self.shutdown.store(true, Ordering::Relaxed);

        // Attempt to take the handle from the mutex
        let Some(handle) = self.handle.lock().take() else {
            return;
        };

        // Wait for the polling task to complete
        if let Err(error) = handle.await {
            error!("consumer shutdown failed: {error:#}");
        }
    }
}

impl Drop for ProsodyConsumer {
    fn drop(&mut self) {
        block_on(self.execute_shutdown());
    }
}

/// Errors that can occur during consumer operations.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ConsumerError {
    /// Indicates invalid consumer configuration.
    #[error("invalid consumer configuration: {0:#}")]
    Configuration(#[from] ValidationErrors),

    /// Indicates a failure to retrieve the hostname.
    #[error("failed to retrieve hostname: {0:#}")]
    Hostname(#[from] io::Error),

    /// Indicates a Kafka operation failure.
    #[error("Kafka operation failed: {0:#}")]
    Kafka(#[from] KafkaError),
}
