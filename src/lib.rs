//! # Prosody
//!
//! Prosody is a high-level Kafka client library for Rust, featuring robust
//! consumer and producer implementations with integrated OpenTelemetry support
//! for distributed tracing.
//!
//! ## Features
//!
//! - **Kafka Consumer**: Efficiently consume messages with support for offset
//!   management and consumer groups.
//! - **Kafka Producer**: Reliably produce messages with idempotent delivery.
//! - **Distributed Tracing**: Seamless integration with OpenTelemetry for
//!   enhanced observability in microservice architectures.
//! - **Configurable**: Flexible configuration through environment variables.
//! - **Asynchronous**: Built on top of Tokio for high-performance asynchronous
//!   operations.
//! - **Backpressure Management**: Intelligent partition pausing to handle
//!   processing backlogs.
//! - **Mocking Support**: Ability to use mock Kafka brokers for testing
//!   purposes.
//! - **High-Level Client**: Unified management of producer and consumer
//!   operations.
//! - **Failure Handling**: Configurable strategies for handling message
//!   processing failures.
//!
//! ## High-Level Client Example
//!
//! ```no_run
//! use prosody::consumer::ConsumerConfiguration;
//! use prosody::consumer::failure::retry::RetryConfiguration;
//! use prosody::consumer::failure::topic::FailureTopicConfigurationBuilder;
//! use prosody::consumer::failure::{FallibleHandler, ClassifyError};
//! use prosody::consumer::message::{ConsumerMessage, MessageContext};
//! use prosody::high_level::mode::Mode;
//! use prosody::high_level::{HighLevelClient};
//! use prosody::producer::ProducerConfiguration;
//! use serde_json::json;
//! use std::convert::Infallible;
//! use std::error::Error;
//!
//! #[derive(Clone)]
//! struct MyHandler;
//!
//! impl FallibleHandler for MyHandler {
//!     type Error = Infallible;
//!
//!     async fn on_message(
//!         &self,
//!         context: MessageContext,
//!         message: ConsumerMessage
//!     ) -> Result<(), Self::Error> {
//!         println!("Received: {message:?}");
//!         Ok(())
//!     }
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let bootstrap_servers = ["localhost:9092".to_owned()];
//!
//!     let mut producer_config = ProducerConfiguration::builder();
//!     producer_config.bootstrap_servers(bootstrap_servers.clone());
//!
//!     let mut consumer_config = ConsumerConfiguration::builder();
//!     consumer_config.bootstrap_servers(bootstrap_servers)
//!         .group_id("my-group")
//!         .subscribed_topics(["my-topic".to_owned()]);
//!
//!     let retry_config = RetryConfiguration::builder();
//!
//!     let client = HighLevelClient::new(
//!         Mode::Pipeline,
//!         &producer_config,
//!         &consumer_config,
//!         &retry_config,
//!         &FailureTopicConfigurationBuilder::default(),
//!     )?;
//!
//!     client.subscribe(MyHandler)?;
//!
//!     let topic = "my-topic".into();
//!     client.send(topic, "message-key", &json!({"value": "Hello, Kafka!"})).await?;
//!
//!     // Run your application logic here
//!
//!     client.unsubscribe().await?;
//!     Ok(())
//! }
//! ```
//!
//! ## High-Level Client Modes
//!
//! Prosody's `HighLevelClient` supports two operational modes:
//!
//! ### Pipeline Mode
//!
//! Designed for applications that require all messages to be processed or sent
//! in order. It ensures:
//! - Ordered handling of all messages
//! - Indefinite retries for failed operations based on the retry configuration
//! - Ideal for pipeline applications where order is crucial
//!
//! ### Low-Latency Mode
//!
//! Optimized for applications prioritizing quick processing or sending,
//! tolerating occasional message failures. It features:
//! - Low-latency operations
//! - A retry mechanism for failed operations
//! - For consumers: Sends persistently failing messages to a failure topic
//! - For producers: Returns an error after a configurable number of retries
//! - Ideal for applications where speed is crucial and failed messages can be
//!   handled separately
//!
//! ## Configuration
//!
//! Prosody can be configured through environment variables or programmatically
//! using the builder pattern. Both `ConsumerConfiguration` and
//! `ProducerConfiguration` use this approach. The builder pattern automatically
//! falls back to environment variables for any unspecified field. This means
//! you can mix and match programmatic configuration with environment variables,
//! giving you flexibility in how you set up your Kafka clients.
//!
//! For a full list of configuration options and their associated environment
//! variables, please refer to the README.
//!
//!
//! # Architecture
//!
//! Prosody is designed to provide efficient and parallel processing of Kafka
//! messages while maintaining order for messages with the same key.
//!
//! ## Consumer Architecture
//!
//! The consumer in Prosody is built around the concept of partition-level
//! parallelism and key-based ordering:
//!
//! 1. **Partition-Level Parallelism**: Each Kafka partition is managed by a
//!    separate `PartitionManager`. This allows for parallel processing of
//!    messages from different partitions. The `PartitionManager` is responsible
//!    for buffering messages and tracking offsets for its assigned partition.
//!
//! 2. **Key-Based Queuing**: Within each partition, messages are further
//!    divided based on their keys. Each unique key within a partition has its
//!    own bounded queue. This ensures that messages with the same key are
//!    processed in order.
//!
//! 3. **Concurrent Processing**: Different keys can be processed concurrently,
//!    even within the same partition, allowing for high throughput. The
//!    `PartitionManager` can process messages from different key queues
//!    simultaneously.
//!
//! 4. **Ordered Processing**: Messages with the same key are processed
//!    sequentially from their respective queue, ensuring ordered processing for
//!    each key.
//!
//! 5. **Polling Mechanism**: The `ProsodyConsumer` uses a polling mechanism to
//!    efficiently fetch messages from Kafka brokers.
//!
//! 6. **Partition Pausing**: If a partition becomes backed up (i.e., its queues
//!    are full), Prosody will pause consumption from that specific partition.
//!    Other partitions continue to make progress, ensuring that a slowdown in
//!    one partition doesn't affect the entire consumer.
//!
//! ## Message Flow
//!
//! 1. The `ProsodyConsumer` polls messages from Kafka Brokers.
//! 2. Messages are dispatched to the appropriate `PartitionManager` based on
//!    their topic and partition.
//! 3. The `PartitionManager` enqueues the message in the correct key-based
//!    queue according to the message key (e.g., User ID, Product ID).
//! 4. Messages are processed sequentially from each key queue, invoking the
//!    user-provided `EventHandler`.
//! 5. After processing, the latest processed offset for the key is updated.
//! 6. The `PartitionManager` tracks the partition's high watermark committed
//!    offset.
//! 7. The Prosody Consumer periodically commits these offsets back to Kafka,
//!    ensuring at-least-once message processing semantics.
//! 8. If a partition's queues become full, that specific partition is paused
//!    until the backlog is processed.
//!
//! Throughout this flow, OpenTelemetry is used to create and propagate
//! distributed traces, allowing for end-to-end visibility of message processing
//! across different services.
//!
//! This architecture allows Prosody to achieve high throughput by processing
//! different partitions and keys concurrently, while still maintaining strict
//! ordering for messages with the same key. It also provides backpressure
//! management by limiting the number of in-flight messages per key and
//! partition through bounded queues and selective partition pausing.
//!
//! ## Mocking Support
//!
//! Prosody provides mocking support for both consumers and producers, allowing
//! for easier testing of Kafka-dependent components. When the `mock`
//! configuration option is set to `true`, Prosody will use mock Kafka brokers
//! instead of connecting to real ones. This feature is particularly useful for
//! unit testing and continuous integration environments where setting up a real
//! Kafka cluster might be impractical.

#![allow(clippy::multiple_crate_versions)]

use ::tracing::info;
use fixedstr::Flexstr;
use internment::Intern;
use rdkafka::mocking::MockCluster;
use serde_json::Value;
use std::mem::forget;
use std::sync::LazyLock;

pub mod admin;
pub mod consumer;
pub mod high_level;
pub mod producer;
pub mod propagator;
pub mod tracing;
mod util;

/// A lazily initialized mock Kafka cluster for testing.
///
/// Creates a single shared mock cluster with 3 brokers and a test topic to
/// facilitate testing without requiring a real Kafka cluster. The cluster is
/// initialized the first time it's accessed and persists for the duration of
/// the program.
#[allow(clippy::unwrap_used)]
static MOCK_CLUSTER_BOOTSTRAP: LazyLock<String> = LazyLock::new(|| {
    let cluster = MockCluster::new(3).unwrap();
    let bootstrap = cluster.bootstrap_servers();

    cluster.create_topic("test-topic", 3, 3).unwrap();

    // Keep cluster alive for program duration
    forget(cluster);

    info!("started mock cluster on {bootstrap}");
    bootstrap
});

/// An interned string representing a Kafka topic name.
///
/// Using string interning provides efficient storage and comparison of topic
/// names by maintaining a single copy of each unique topic name.
pub type Topic = Intern<str>;

/// A partition identifier within a Kafka topic.
pub type Partition = i32;

/// The length of a UUID string (36 characters) plus one byte for length.
const UUID_STR_LEN: usize = 36 + 1;

/// A compact string optimized for UUID-length keys.
///
/// Uses `Flexstr` to efficiently store message keys up to UUID length without
/// heap allocation.
pub type Key = Flexstr<UUID_STR_LEN>;

/// A JSON value containing a Kafka message's content.
///
/// Stores message payloads as arbitrary JSON to support flexible message
/// formats.
pub type Payload = Value;

/// An offset position within a Kafka partition.
pub type Offset = i64;

/// A compact string for storing event identifiers.
///
/// Uses `Flexstr` to efficiently store event IDs up to UUID length without heap
/// allocation.
pub type EventId = Flexstr<UUID_STR_LEN>;

/// A borrowed string slice for event identifiers.
pub type BorrowedEventId = str;
