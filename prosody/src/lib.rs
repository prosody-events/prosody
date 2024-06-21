//! A high-level Kafka client library with support for distributed tracing.
//!
//! This crate provides abstractions for interacting with Apache Kafka,
//! offering both consumer and producer implementations. It integrates
//! OpenTelemetry for distributed tracing, allowing for better observability
//! in microservice architectures.
//!
//! # Features
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
//!
//! # Examples
//!
//! ## Producer Example
//!
//! ```rust
//! use prosody::Topic;
//! use prosody::producer::{ProducerConfiguration, Producer};
//! use serde_json::json;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let config = ProducerConfiguration::builder()
//!         .bootstrap_servers(["localhost:9092".to_string()])
//!         .mock(true) // use mock producer for example
//!         .build()?;
//!
//!     let producer = Producer::new(&config)?;
//!
//!     let topic: Topic = "my-topic".into();
//!     producer.send(topic, "message-key", json!({"value": "Hello, Kafka!"})).await?;
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Consumer Example
//!
//! ```rust
//! use prosody::consumer::message::{ConsumerMessage, MessageContext};
//! use prosody::consumer::{ConsumerConfiguration, KafkaConsumer, MessageHandler};
//! use prosody::Topic;
//! use std::time::Duration;
//!
//! #[derive(Clone)]
//! struct MyMessageHandler;
//!
//! impl MessageHandler for MyMessageHandler {
//!     type Error = std::io::Error;
//!
//!     async fn handle(
//!         &self,
//!         context: &mut MessageContext,
//!         message: ConsumerMessage,
//!     ) -> Result<(), Self::Error> {
//!         println!("Received: {:?}", message);
//!         message.commit();
//!         Ok(())
//!     }
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let config = ConsumerConfiguration::builder()
//!         .bootstrap_servers(["localhost:9092".to_string()])
//!         .group_id("my-group")
//!         .subscribed_topics(["my-topic".to_string()])
//!         .mock(true) // use mock consumer for example
//!         .build()?;
//!
//!     let consumer = KafkaConsumer::new(config, MyMessageHandler)?;
//!
//!     // Run your application logic here
//!
//!     consumer.shutdown().await;
//!     Ok(())
//! }
//! ```
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
//! 5. **Polling Mechanism**: The `KafkaConsumer` uses a polling mechanism to
//!    efficiently fetch messages from Kafka brokers.
//!
//! 6. **Partition Pausing**: If a partition becomes backed up (i.e., its queues
//!    are full), Prosody will pause consumption from that specific partition.
//!    Other partitions continue to make progress, ensuring that a slowdown in
//!    one partition doesn't affect the entire consumer.
//!
//! ## Message Flow
//!
//! 1. The Prosody `KafkaConsumer` polls messages from Kafka Brokers.
//! 2. Messages are dispatched to the appropriate `PartitionManager` based on
//!    their topic and partition.
//! 3. The `PartitionManager` enqueues the message in the correct key-based
//!    queue according to the message key (e.g., User ID, Product ID).
//! 4. Messages are processed sequentially from each key queue, invoking the
//!    user-provided `MessageHandler`.
//! 5. After processing, the latest processed offset for the key is updated.
//! 6. Periodically, the `PartitionManager` collects the latest processed
//!    offsets from all its key queues.
//! 7. The Prosody Consumer commits these offsets back to Kafka, ensuring
//!    at-least-once message processing semantics.
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

use internment::Intern;
use serde_json::Value;

pub mod consumer;
pub mod producer;
mod propagator;
mod util;

/// A Kafka topic name.
///
/// This type uses string interning for efficient storage and comparison of
/// topic names.
pub type Topic = Intern<str>;

/// A Kafka partition number.
pub type Partition = i32;

/// A Kafka message key.
///
/// This type uses `Box<str>` for efficient storage of variable-length strings.
pub type Key = Box<str>;

/// A Kafka message payload.
///
/// The payload is stored as a JSON Value, allowing for flexible message
/// content.
pub type Payload = Value;

/// A Kafka message offset.
pub type Offset = i64;
