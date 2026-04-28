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
//! use prosody::prelude::*;
//! use serde_json::json;
//! use std::convert::Infallible;
//!
//! #[derive(Clone)]
//! struct MyHandler;
//!
//! impl FallibleHandler for MyHandler {
//!     type Payload = serde_json::Value;
//!     type Error = Infallible;
//!     type Output = ();
//!
//!     async fn on_message<C>(
//!         &self,
//!         _context: C,
//!         message: ConsumerMessage<serde_json::Value>,
//!         _demand_type: DemandType,
//!     ) -> Result<(), Self::Error>
//!     where
//!         C: EventContext,
//!     {
//!         println!("Received: {message:?}");
//!         Ok(())
//!     }
//!
//!     async fn on_timer<C>(
//!         &self,
//!         _context: C,
//!         trigger: Trigger,
//!         _demand_type: DemandType,
//!     ) -> Result<(), Self::Error>
//!     where
//!         C: EventContext,
//!     {
//!         println!("Timer fired for key: {}", trigger.key);
//!         Ok(())
//!     }
//!
//!     async fn shutdown(self) {}
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let bootstrap_servers = vec!["localhost:9094".to_owned()];
//!
//!     let mut consumer_config = ConsumerConfiguration::builder();
//!     consumer_config
//!         .bootstrap_servers(bootstrap_servers.clone())
//!         .group_id("my-group")
//!         .subscribed_topics(["my-topic".to_owned()]);
//!
//!     let mut producer_config = ProducerConfiguration::builder();
//!     producer_config
//!         .bootstrap_servers(bootstrap_servers)
//!         .source_system("my-source");
//!
//!     let mut cassandra_config = CassandraConfigurationBuilder::default();
//!     cassandra_config.nodes(vec!["localhost:9042".to_owned()]);
//!
//!     let consumer_builders = ConsumerBuilders {
//!         consumer: consumer_config,
//!         ..ConsumerBuilders::default()
//!     };
//!
//!     let client: HighLevelClient<MyHandler> = HighLevelClient::new(
//!         Mode::Pipeline,
//!         &mut producer_config,
//!         &consumer_builders,
//!         &cassandra_config,
//!     )?;
//!
//!     client.subscribe(MyHandler).await?;
//!
//!     client.send("my-topic".into(), "message-key", &json!({"value": "Hello, Kafka!"})).await?;
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

#![allow(
    clippy::multiple_crate_versions,
    reason = "Transitive dependencies have version conflicts outside our control"
)]

use ::tracing::info;
use fixedstr::Flexstr;
use internment::Intern;
use rdkafka::mocking::MockCluster;
use std::env;
use std::mem::forget;
use std::sync::{Arc, LazyLock};

pub mod admin;
pub mod cassandra;
/// Wire-format abstraction for pluggable message encoding and decoding.
pub mod codec;
pub mod consumer;
pub mod error;
pub mod heartbeat;
pub mod high_level;
pub mod otel;
pub mod prelude;
pub mod producer;
pub mod propagator;
pub mod telemetry;
pub mod timers;
pub mod tracing;
mod util;

pub use crate::codec::{Codec, JsonCodec};
pub use crate::error::{ClassifyError, ErrorCategory};

/// A lazily initialized mock Kafka cluster for testing.
///
/// Creates a single shared mock cluster with 3 brokers and topics from the
/// `PROSODY_SUBSCRIBED_TOPICS` environment variable to facilitate testing
/// without requiring a real Kafka cluster. The cluster is initialized the first
/// time it's accessed and persists for the duration of the program.
#[expect(
    clippy::expect_used,
    reason = "LazyLock requires non-fallible closure; test infra cannot recover from failure"
)]
static MOCK_CLUSTER_BOOTSTRAP: LazyLock<String> = LazyLock::new(|| {
    let cluster = MockCluster::new(3).expect("Failed to create mock Kafka cluster");
    let bootstrap = cluster.bootstrap_servers();

    // Create topics from environment variable if set
    if let Ok(topics_str) = env::var("PROSODY_SUBSCRIBED_TOPICS") {
        for topic in topics_str.split(',') {
            let topic = topic.trim();
            if !topic.is_empty() {
                cluster
                    .create_topic(topic, 3, 3)
                    .expect("Failed to create mock topic");
            }
        }
    }

    // Keep cluster alive for program duration
    forget(cluster);

    info!("started mock cluster on {bootstrap}");
    bootstrap
});

/// The length of a UUID string (36 characters) plus one byte for length.
const UUID_STR_LEN: usize = 36 + 1;

/// The system originating a message.
///
/// Used to track which service or component generated a particular message,
/// enabling loop detection and audit trails.
pub type SourceSystem = Flexstr<UUID_STR_LEN>;

/// An interned string representing a Kafka topic name.
///
/// Using string interning provides efficient storage and comparison of topic
/// names by maintaining a single copy of each unique topic name.
pub type Topic = Intern<str>;

/// A partition identifier within a Kafka topic.
pub type Partition = i32;

/// A compact string optimized for UUID-length keys.
///
/// Uses an Arc so the key can be cheaply cloned
pub type Key = Arc<str>;

/// A consumer group identifier.
///
/// Uses an Arc so the consumer group can be cheaply cloned across components.
pub type ConsumerGroup = Arc<str>;

/// An offset position within a Kafka partition.
pub type Offset = i64;

/// A key qualified by its topic-partition context.
///
/// Uniquely identifies a message key within Kafka's coordinate system. Two
/// messages with the same `Key` from different topic-partitions are distinct
/// `TopicPartitionKey`s. This prevents key collisions in cross-partition state
/// like the scheduler's virtual time tracking.
///
/// The name mirrors Kafka's `TopicPartition` concept with the addition of the
/// message key.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct TopicPartitionKey {
    /// The topic this key belongs to.
    pub topic: Topic,
    /// The partition this key belongs to.
    pub partition: Partition,
    /// The message key.
    pub key: Key,
}

impl TopicPartitionKey {
    /// Creates a new topic-partition-qualified key.
    #[must_use]
    pub fn new(topic: Topic, partition: Partition, key: Key) -> Self {
        Self {
            topic,
            partition,
            key,
        }
    }
}

/// Source system header used to prevent processing loops.
const SOURCE_SYSTEM_HEADER: &str = "source-system";

/// Defines event identity behavior for messages that contain unique
/// identifiers.
///
/// This trait enables idempotent message processing by providing access to
/// event identifiers that can be used to deduplicate messages. Implementations
/// should extract identifiers from their internal representation and provide
/// them in a consistent format.
///
/// # Usage
///
/// Typically used by consumers to detect and skip duplicate messages that may
/// be delivered due to retries or network issues. The event ID should be stable
/// across message delivery attempts.
pub trait EventIdentity {
    /// Returns a reference to this event's identifier if one exists.
    ///
    /// Returns `None` if the event has no identifier.
    fn event_id(&self) -> Option<&str>;
}

/// Provides access to the event type field within a payload.
///
/// Used to extract event type identifiers for event filtering and routing.
/// Implementations should extract the type from their internal representation.
pub trait EventType {
    /// Returns the event type string if present.
    fn event_type(&self) -> Option<&str>;
}

/// Constructs a payload suitable for replaying a timer event.
///
/// Used by failure-topic middleware to build a synthetic payload when routing
/// timer failures to a DLQ topic.
pub trait TimerReplayPayload: Sized {
    /// Creates a payload representing a timer replay event.
    ///
    /// # Arguments
    ///
    /// * `key` - The timer key.
    /// * `time` - The timer timestamp as an RFC 3339 string.
    fn timer_replay(key: &str, time: &str) -> Self;
}

/// Manages processing resources (spans and permits) for deterministic cleanup.
///
/// Ensures tracing spans and semaphore permits are released immediately when
/// processing completes, rather than waiting for unpredictable garbage
/// collection.
pub trait ProcessScope {
    /// Guard that releases processing resources on drop.
    type Guard;

    /// Creates a guard that releases resources when processing completes.
    fn process_scope(&self) -> Self::Guard;
}

/// Test utilities available only during test compilation.
#[cfg(test)]
pub(crate) mod test_util {
    use std::sync::LazyLock;
    use tokio::runtime::{Builder, Runtime};

    /// Shared multi-threaded runtime for all unit tests in the crate.
    #[expect(
        clippy::expect_used,
        reason = "LazyLock requires non-fallible closure; test infra cannot recover from failure"
    )]
    pub static TEST_RUNTIME: LazyLock<Runtime> = LazyLock::new(|| {
        Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("Failed to create tokio runtime")
    });
}
