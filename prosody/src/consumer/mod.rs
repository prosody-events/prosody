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
//! The module is composed of several submodules:
//!
//! - `context`: Manages Kafka partition assignments and revocations,
//!   integrating with Kafka's rebalance callbacks.
//! - `extractor`: Provides functionality for extracting metadata from Kafka
//!   messages, particularly for distributed tracing purposes.
//! - `message`: Defines structures for managing message contexts and consumer
//!   messages, including tracking shutdown signals and committing messages
//!   after processing.
//! - `partition`: Handles partition-specific operations and state management.
//! - `poll`: Implements the core message polling and processing loop, including
//!   offset commits and partition management.
//!
//! The main entry point for using this module is the [`KafkaConsumer`] struct,
//! which is configured using the [`ConsumerConfiguration`] struct. Users should
//! implement the [`MessageHandler`] trait to define custom message processing
//! logic.
//!
//! This module is designed to provide a high-level, ergonomic interface for
//! consuming and processing Kafka messages while handling complex scenarios
//! such as consumer group re-balancing, offset management, and distributed
//! tracing.
use std::error::Error;
use std::future::Future;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use ahash::HashMap;
use confique::Config;
use crossbeam_utils::CachePadded;
use educe::Educe;
use parking_lot::Mutex;
use rdkafka::ClientConfig;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::KafkaError;
use serde::Deserialize;
use thiserror::Error;
use tokio::task::{JoinHandle, spawn_blocking};
use tracing::error;
use validator::{Validate, ValidationErrors};
use whoami::fallible::hostname;

use crate::{Partition, Topic};
use crate::consumer::context::Context;
use crate::consumer::message::{ConsumerMessage, MessageContext};
use crate::consumer::partition::PartitionManager;
use crate::consumer::poll::poll;

mod context;
mod extractor;
pub mod message;
mod partition;
mod poll;

/// Thread-safe, cache-padded atomic usize used to detect changes in partition
/// watermarks.
///
/// This type is used as a simple mechanism to notify the poll loop when
/// partition watermarks have changed, indicating that offsets should be
/// committed. The value is incremented each time a watermark changes, allowing
/// the poll loop to detect changes efficiently without checking individual
/// partition watermarks.
type WatermarkVersion = CachePadded<AtomicUsize>;

/// Thread-safe hash map of partition managers.
///
/// This type maps topic-partition pairs to their respective
/// [`PartitionManager`]s. It's used to track and manage the state of each
/// assigned partition, including message buffers and offset information.
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
    type Error: Error;

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
#[derive(Config, Deserialize, Validate)]
pub struct ConsumerConfiguration {
    /// List of Kafka bootstrap servers.
    #[config(env = "PROSODY_BOOTSTRAP_SERVERS")]
    #[validate(length(min = 1_u64))]
    pub bootstrap_servers: Vec<String>,

    /// Consumer group ID.
    #[config(env = "PROSODY_GROUP_ID")]
    #[validate(length(min = 1_u64))]
    pub group_id: String,

    /// List of topics to subscribe to.
    #[config(env = "PROSODY_SUBSCRIBED_TOPICS")]
    #[validate(length(min = 1_u64))]
    pub subscribed_topics: Vec<String>,

    /// Maximum number of uncommitted messages.
    #[config(env = "PROSODY_MAX_UNCOMMITTED", default = 32)]
    #[validate(range(min = 1_usize))]
    pub max_uncommitted: usize,

    /// Maximum number of enqueued messages per key.
    #[config(env = "PROSODY_MAX_ENQUEUED_PER_KEY", default = 8)]
    #[validate(range(min = 1_usize))]
    pub max_enqueued_per_key: usize,

    /// Timeout for partition shutdown.
    #[config(env = "PROSODY_PARTITION_SHUTDOWN_TIMEOUT")]
    #[serde(with = "humantime_serde", default = "default_partition_timeout")]
    pub partition_shutdown_timeout: Option<Duration>,

    /// Interval between poll operations.
    #[config(env = "PROSODY_POLL_INTERVAL", default = "100ms")]
    #[serde(with = "humantime_serde")]
    pub poll_interval: Duration,

    /// Interval between commit operations.
    #[config(env = "PROSODY_COMMIT_INTERVAL", default = "1s")]
    #[serde(with = "humantime_serde")]
    pub commit_interval: Duration,
}

/// High-level Kafka consumer implementation.
#[derive(Clone, Educe)]
#[educe(Debug)]
pub struct KafkaConsumer {
    #[educe(Debug(ignore))]
    shutdown: Arc<AtomicBool>,

    #[educe(Debug(ignore))]
    handle: Arc<Mutex<Option<JoinHandle<()>>>>,
}

impl KafkaConsumer {
    /// Creates a new `KafkaConsumer` instance.
    ///
    /// # Arguments
    ///
    /// * `config` - The consumer configuration.
    /// * `message_handler` - The message handler implementation.
    ///
    /// # Returns
    ///
    /// A Result containing the new `KafkaConsumer` instance or a
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
        let consumer: BaseConsumer<_> = ClientConfig::new()
            .set("bootstrap.servers", config.bootstrap_servers.join(","))
            .set("client.id", hostname()?)
            .set("group.id", config.group_id)
            .set("enable.auto.offset.store", "false")
            .set("auto.offset.reset", "earliest")
            .set("partition.assignment.strategy", "cooperative-sticky")
            .set_log_level(RDKafkaLogLevel::Error)
            .create_with_context(context)?;

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
    pub async fn shutdown(self) {
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

/// Provides the default partition shutdown timeout.
///
/// # Returns
///
/// An Option containing a Duration of 5 seconds.
#[allow(clippy::unnecessary_wraps)]
fn default_partition_timeout() -> Option<Duration> {
    Some(Duration::from_secs(5))
}
