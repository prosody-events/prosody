//! Kafka consumer implementation for high-level message consumption and
//! processing.
//!
//! This module provides an abstraction for consuming messages from Kafka topics
//! with support for:
//!
//! - Per-key concurrency with ordered processing within keys
//! - Automatic partition assignment and revocation handling
//! - Offset management with at-least-once delivery semantics
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
//! - `KafkaObserver`: What the primary Kafka client knows about itself, shared
//!   with the consumer context
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

pub(crate) use crate::consumer::config::TypedConsumerSetup;
pub use crate::consumer::config::{
    CommonConfiguration, ConsumerConfiguration, ConsumerConfigurationBuilder,
    ConsumerConfigurationBuilderError, ConsumerSetup, LowLatencyMiddlewareConfiguration,
    MockConfigurationError, PipelineMiddlewareConfiguration, RecoveryTtlMarginError,
};
pub use crate::consumer::error::{ConsumerError, KeyedStateInitError};
pub use crate::consumer::event_context::{EventContext, TerminationSignals};
pub use crate::consumer::handler::{DemandType, EventHandler, HandlerProvider, Keyed, Uncommitted};
pub use crate::consumer::kafka_state::{
    MessageCell, MessageDescriptor, MessageRef, MessageRefCodec, MessageRefCodecError,
    MessageResolver, MessageStateError, message_deque_state, message_map_state, message_state,
};
pub use crate::consumer::message::ConsumerMessage;
pub use crate::consumer::middleware::{FallibleHandler, RepinProof};
pub(crate) use crate::consumer::observer::KafkaObserver;
use crate::consumer::partition::PartitionManager;
use crate::consumer::probes::ProbeServer;
pub(crate) use crate::consumer::wiring::state::{
    CassandraStateProvider, KeyedStateInputs, MemoryStateProvider,
};
use crate::heartbeat::HeartbeatRegistry;
pub use crate::otel::SpanRelation;
pub use crate::state::config::{KeyedStateConfiguration, KeyedStateConfigurationBuilderError};
// `descriptor::Keyed` (the key-axis lifter) is deliberately not re-exported
// here: it would shadow the message-routing `Keyed` trait re-exported here.
pub use crate::state::descriptor::{CellResolver, CellType, FromSession, WithResolver};
use crate::{Codec, JsonCodec, Partition, Topic};
use ahash::HashMap;
use crossbeam_utils::CachePadded;
use educe::Educe;
use futures::executor::block_on;
use parking_lot::{Mutex, RwLock};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::error;

mod config;
pub(crate) mod decode;
mod error;
pub mod event_context;
mod extractor;
mod handler;
mod kafka_context;
pub mod kafka_state;
pub mod message;
pub mod middleware;
mod modes;
// Crate-wide, not `pub(in crate::consumer)`: keyed-state publication reads the
// observed partition count from outside this module.
pub(crate) mod observer;
pub(crate) mod partition;
mod poll;
// Crate-wide: the peer listener's health service answers from the same
// readiness and liveness predicates this module's HTTP probes serve.
pub(crate) mod probes;
pub mod storage;
mod wiring;

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
pub(crate) type Managers<P> = RwLock<HashMap<(Topic, Partition), PartitionManager<P>>>;

/// Holds the runtime state of the consumer.
///
/// This struct encapsulates the components that make up the running state
/// of a `ProsodyConsumer`, allowing them to be managed together.
struct RuntimeState {
    poll_handle: JoinHandle<()>,
    probe_server: Option<ProbeServer>,
    /// The consumer's Kafka observation handle. Shutdown retires its gauge
    /// series so a stopped consumer stops contributing to `sum` aggregations.
    observer: KafkaObserver,
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

impl<C: Codec> ProsodyConsumer<C> {
    /// Returns the number of currently assigned partitions.
    ///
    /// This method is useful for monitoring how many partitions have been
    /// assigned to this consumer instance by Kafka's partition assignment
    /// strategy.
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
        self.shutdown.store(true, Ordering::Relaxed);

        let Some(RuntimeState {
            poll_handle,
            probe_server,
            observer,
        }) = self.runtime_state.lock().take()
        else {
            return;
        };

        if let Err(error) = poll_handle.await {
            error!("consumer shutdown failed: {error:#}");
        }

        // The `BaseConsumer` is dropped inside the poll task, so its close-time
        // polling — which can deliver one last statistics sample — finished
        // before the join handle resolved. Nothing can record over this.
        observer.retire_gauges();

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

pub(crate) fn get_assigned_partition_count<P: Send + Sync + 'static>(
    managers: &Managers<P>,
) -> u32 {
    u32::try_from(managers.read().len()).unwrap_or(u32::MAX)
}

/// Checks if any partition is stalled.
pub(crate) fn get_is_stalled<P: Send + Sync + 'static>(managers: &Managers<P>) -> bool {
    managers.read().values().any(PartitionManager::is_stalled)
}
