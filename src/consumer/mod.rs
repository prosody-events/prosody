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
pub use crate::consumer::error::{
    ConsumerError, KeyedStateInitError, PeerInitError, ShutdownError,
};
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
use crate::consumer::wiring::peer::PeerHandles;
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
mod sweep;
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
    peer: Option<PeerHandles>,
}

/// What one teardown still holds after the Kafka poll loop stops.
///
/// Only the caller that takes [`RuntimeState`] out of the shared mutex builds
/// one, and exactly one caller ever does. So holding this value is what proves
/// a caller owns the teardown, and every other caller gets `None` and touches
/// nothing shared. Whether this consumer joined the peer fleet is the separate
/// question `peer` answers.
struct Teardown {
    probe_server: Option<ProbeServer>,
    observer: KafkaObserver,
    peer: Option<PeerHandles>,
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
#[derive(Educe)]
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

/// Every clone reaches the same consumer, so a clone is a second owner and
/// never a passive view. The first clone to
/// [`shutdown`](ProsodyConsumer::shutdown) stops the consumer, and so does the
/// first clone to drop.
///
/// Written out rather than derived, because a derive would demand `C: Clone`.
/// No field holds a `C`, so the codec never has to be cloneable.
impl<C: Codec> Clone for ProsodyConsumer<C> {
    fn clone(&self) -> Self {
        Self {
            shutdown: Arc::clone(&self.shutdown),
            managers: Arc::clone(&self.managers),
            assignment: self.assignment.clone(),
            runtime_state: Arc::clone(&self.runtime_state),
            heartbeats: self.heartbeats.clone(),
        }
    }
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

    /// Stops this consumer, in the one order that does not drop work it already
    /// owes.
    ///
    /// **This function owns the teardown order**, and its body reads as that
    /// order:
    ///
    /// 1. Close peer request admission, so no new request enters while the
    ///    handlers finish.
    /// 2. Stop the poll loop and wait for in-flight message processing.
    /// 3. Sweep every partition manager the final revoke left behind.
    /// 4. Retire the observation gauges and stop the probe server.
    /// 5. Tear the peer runtime down and report what it found.
    ///
    /// The peer runtime stays live through steps 1 to 4, because a partition
    /// handler's commit hook can still queue an owed response. Tear the peer
    /// down earlier and the process drops responses it already owes, and fails
    /// requests it could still answer. Step 3 is what bounds step 5. The
    /// compiler keeps that order: step 3 mints the proof step 5 demands, so
    /// step 5 cannot be written ahead of it.
    ///
    /// This is the supported teardown, and it is the only one that waits. A
    /// consumer that is dropped instead runs steps 1, 2 and 4, and only *asks*
    /// the coordinator to run step 5. It runs no sweep and waits for no peer
    /// step, so a runtime that already stops may run none of them.
    ///
    /// A second call, or a call on a clone whose sibling already ran, finds no
    /// runtime state and answers `Ok(())` without touching anything shared.
    /// That success is not an observation: the other caller made it.
    ///
    /// # Errors
    ///
    /// Returns [`ShutdownError::PollLoop`] when the poll loop task did not end
    /// cleanly. Returns [`ShutdownError::Directory`] or
    /// [`ShutdownError::Teardown`] when the peer teardown could not confirm the
    /// removal of this node from the directory, or could not report what it
    /// did. A row that survives expires on its lease.
    ///
    /// A poll failure wins over a peer report. It happens first, and it is the
    /// more surprising of the two.
    pub async fn shutdown(mut self) -> Result<(), ShutdownError> {
        let Some((teardown, poll_failure)) = self.stop_polling().await else {
            return Ok(());
        };
        let swept = sweep::drain_managers(&self.managers).await;
        let peer_report = match teardown.release(swept).await {
            Some(peer) => peer.stop().await,
            None => Ok(()),
        };
        match poll_failure {
            Some(failure) => Err(failure),
            None => peer_report,
        }
    }

    /// Closes peer request admission, stops the poll loop, and waits for it.
    ///
    /// Answers `None` to every caller but the one that takes the runtime state,
    /// so a losing caller runs no step of the teardown at all. The winner also
    /// gets the poll loop's join failure, because
    /// [`shutdown`](Self::shutdown) reports it and [`Drop`](Self::drop) can
    /// only log it.
    async fn stop_polling(&mut self) -> Option<(Teardown, Option<ShutdownError>)> {
        let RuntimeState {
            poll_handle,
            probe_server,
            observer,
            peer,
        } = self.runtime_state.lock().take()?;

        if let Some(peer) = &peer {
            peer.close_admission();
        }
        self.shutdown.store(true, Ordering::Relaxed);

        let poll_failure = match poll_handle.await {
            Ok(()) => None,
            Err(error) => Some(ShutdownError::PollLoop {
                message: format!("{error:#}"),
            }),
        };
        Some((
            Teardown {
                probe_server,
                observer,
                peer,
            },
            poll_failure,
        ))
    }
}

impl Teardown {
    /// Retires the observation gauges, stops the probe server, and hands the
    /// still-live peer handles back.
    ///
    /// The [`Swept`](sweep::Swept) proof is the parameter, and only the sweep
    /// mints one, so this step cannot be written ahead of the sweep. The peer
    /// teardown reads the return value, so neither can that.
    async fn release(mut self, _swept: sweep::Swept) -> Option<PeerHandles> {
        let peer = self.peer.take();
        self.stop_observation().await;
        peer
    }

    /// Retires the observation gauges and stops the probe server.
    ///
    /// This consumes the value, so any peer handles it still holds drop here.
    /// Dropping them asks the coordinator to tear the peer runtime down, and
    /// nothing here waits for that teardown.
    /// [`Drop`](ProsodyConsumer::drop) calls this instead of
    /// [`release`](Self::release), because it runs no sweep and holds no proof.
    async fn stop_observation(self) {
        // The `BaseConsumer` is dropped inside the poll task, so its close-time
        // polling — which can deliver one last statistics sample — finished
        // before the join handle resolved. Nothing can record over this.
        self.observer.retire_gauges();

        if let Some(probe_server) = self.probe_server {
            probe_server.shutdown().await;
        }
    }
}

/// Starts the teardown when a consumer is dropped without a shutdown.
///
/// Dropping the peer stop sender starts the peer teardown. `Drop` does not wait
/// for it, so a process that exits at once can leave a row until its lease
/// expires.
///
/// `Drop` also runs no partition sweep, which
/// [`shutdown`](ProsodyConsumer::shutdown) makes step 3. This path blocks the
/// thread that drops the consumer, and the sweep waits for the work every
/// retained manager still runs. So a teardown started here is not bounded by
/// the sweep. A manager the final revoke left behind can hold a response send
/// handle open.
///
/// This path does await the probe server task. A current-thread runtime cannot
/// drive that task while this blocks its only thread, so call `shutdown` from
/// such a runtime.
///
/// `Drop` cannot return, so it logs the poll loop's join failure that
/// [`shutdown`](ProsodyConsumer::shutdown) reports.
impl<C: Codec> Drop for ProsodyConsumer<C> {
    fn drop(&mut self) {
        block_on(async {
            if let Some((teardown, poll_failure)) = self.stop_polling().await {
                if let Some(error) = poll_failure {
                    error!("consumer shutdown failed: {error:#}");
                }
                teardown.stop_observation().await;
            }
        });
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
