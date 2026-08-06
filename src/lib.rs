#![doc = include_str!("../README.md")]
//! # Subsystems
//!
//! Three subsystems make up the crate; each documents itself in detail on its
//! own module:
//!
//! - **Consumer** ([`consumer`]) — hierarchical Kafka consumption (consumer →
//!   partition manager → per-key queues): partition-level parallelism, ordered
//!   processing within each key, and capacity-based backpressure. Kafka
//!   partition ownership plus per-key dispatch guarantee at most one live
//!   handler per key — and one owner per partition — anywhere in the cluster,
//!   so handlers and the state layers below never contend with a concurrent
//!   writer.
//! - **Timers** ([`timers`]) — persistent scheduled events, partitioned into
//!   time-based slabs behind the pluggable
//!   [`TriggerStore`](timers::store::TriggerStore) (Cassandra or in-memory) and
//!   fired by an in-memory scheduler that preloads upcoming slabs.
//! - **Keyed state** ([`state`]) — typed Value/Map/Deque collections over a
//!   uniform cell store, registered per collection and bound by handlers
//!   through the event context. Durability is one provisional cell per value
//!   with no write-ahead log: after the middleware stack returns, a single
//!   settle boundary stages the event's writes, arms the recovery backstop
//!   timer, records the dedup marker, commits, and promotes; recovery resolves
//!   durable provisional cells through the commit oracle.

#![allow(
    clippy::multiple_crate_versions,
    reason = "Transitive dependencies have version conflicts outside our control"
)]

use ::tracing::info;
use fixedstr::Flexstr;
use internment::Intern;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::mocking::MockCluster;
use std::env;
use std::sync::mpsc::{SyncSender, sync_channel};
use std::sync::{Arc, LazyLock};
use std::thread::{self, park};

pub mod admin;
pub mod cassandra;
/// Wire-format abstraction for pluggable message encoding and decoding.
pub mod codec;
pub mod consumer;
pub mod error;
pub mod heartbeat;
pub mod high_level;
pub mod loader;
pub mod otel;
pub mod prelude;
pub mod producer;
pub mod propagator;
pub mod requester;
mod response;
mod router;
mod segment;
mod size;
pub mod state;
pub mod state_reader;
pub mod subsystem;
pub mod telemetry;
pub mod timers;
pub mod tracing;
mod util;

pub use crate::codec::{Codec, JsonCodec};
pub use crate::error::{ClassifyError, ErrorCategory};
pub use crate::router::config::{
    PeerConfiguration, PeerConfigurationBuilder, PeerConfigurationBuilderError,
};

/// A lazily initialized mock Kafka cluster for testing.
///
/// Creates a single shared mock cluster with 3 brokers and topics from the
/// `PROSODY_SUBSCRIBED_TOPICS` environment variable to facilitate testing
/// without requiring a real Kafka cluster. The cluster is initialized the
/// first time it's accessed and persists for the duration of the program.
///
/// `rdkafka::MockCluster` holds a raw `*mut` pointer and is therefore not
/// `Sync` — it can't live inside a `static`. Instead, a dedicated owner
/// thread holds the cluster on its stack and waits for topic commands. The
/// bootstrap servers return through a channel. This keeps the
/// resource owned by a real stack frame (no `mem::forget`, no `Box::leak`,
/// no `unsafe` Sync impl) while keeping the cluster alive for the lifetime
/// of the process.
struct MockClusterState {
    bootstrap: String,
    topics: SyncSender<MockTopic>,
}

struct MockTopic {
    name: String,
    ready: SyncSender<Result<(), String>>,
}

#[expect(
    clippy::expect_used,
    reason = "LazyLock requires non-fallible closure; test infra cannot recover from failure"
)]
static MOCK_CLUSTER: LazyLock<MockClusterState> = LazyLock::new(|| {
    let (tx, rx) = sync_channel::<String>(1);
    let (topics, topic_rx) = sync_channel::<MockTopic>(1);
    thread::Builder::new()
        .name("prosody-mock-cluster".into())
        .spawn(move || {
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

            tx.send(bootstrap)
                .expect("Failed to publish mock cluster bootstrap");
            loop {
                if let Ok(topic) = topic_rx.recv() {
                    let created = match cluster.create_topic(&topic.name, 3, 3) {
                        Ok(())
                        | Err(KafkaError::MockCluster(RDKafkaErrorCode::TopicAlreadyExists)) => {
                            Ok(())
                        }
                        Err(error) => Err(error.to_string()),
                    };
                    drop(topic.ready.send(created));
                } else {
                    loop {
                        park();
                    }
                }
            }
        })
        .expect("Failed to spawn mock-cluster owner thread");
    let bootstrap = rx
        .recv()
        .expect("mock-cluster owner thread failed to start");
    info!("started mock cluster on {bootstrap}");
    MockClusterState { bootstrap, topics }
});

fn mock_cluster_bootstrap() -> String {
    MOCK_CLUSTER.bootstrap.clone()
}

fn create_mock_topic(name: &str) -> Result<(), String> {
    let (ready, created) = sync_channel(1);
    MOCK_CLUSTER
        .topics
        .send(MockTopic {
            name: name.to_owned(),
            ready,
        })
        .map_err(|error| error.to_string())?;
    created.recv().map_err(|error| error.to_string())?
}

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

pub use segment::SegmentId;
pub use size::{ByteSize, ByteSizeError};

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
mod tests;
#[cfg(test)]
pub(crate) use tests::test_util;
