//! Manages message processing and offset tracking for individual Kafka
//! partitions.
//!
//! This module orchestrates concurrent message processing while maintaining
//! ordering guarantees within key groups:
//!
//! - Processes messages with different keys concurrently for high throughput
//! - Preserves strict ordering for messages with the same key
//! - Tracks and commits message offsets for at-least-once processing
//! - Manages graceful shutdown of partition processing
//! - Implements backpressure through the bounded message channel
//!
//! The core component is `PartitionManager`, which coordinates all aspects
//! of partition-level message processing.

use crate::consumer::EventHandler;
use crate::consumer::message::ConsumerRecord;
use crate::consumer::partition::offsets::OffsetTracker;
use crate::heartbeat::HeartbeatRegistry;
use crate::loader::MessageLoader;
use crate::otel::SpanRelation;
use crate::state::manager::{PartitionStateManager, PartitionStateProvider};
use crate::state::session::EventSession;
use crate::telemetry::sender::TelemetrySender;
use crate::timers::TimerSemaphores;
use crate::timers::duration::CompactDuration;
use crate::timers::store::TriggerStoreProvider;
use crate::{EventIdentity, EventType, Offset, Partition, Topic};
use aho_corasick::AhoCorasick;
use crossbeam_utils::CachePadded;
use educe::Educe;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;
use tokio::spawn;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::mpsc::{Receiver, Sender, channel};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep_until};
use tracing::{debug, error, instrument};

mod dispatch;
mod keyed;
mod metrics;
pub mod offsets;
mod runtime;
mod util;

#[cfg(test)]
use dispatch::{guarded_dispatch, process_event};
use runtime::handle_messages;

#[cfg(test)]
mod tests;

/// Grace period numerator: handlers run uninterrupted for this fraction of
/// `shutdown_timeout` before the abort signal fires.
const GRACE_PERIOD_NUMERATOR: u32 = 4;
/// Grace period denominator.
const GRACE_PERIOD_DENOMINATOR: u32 = 5;

/// Lifecycle phase of a partition, used to coordinate shutdown across all
/// partition subsystems.
///
/// Phases advance monotonically in declaration order, which also defines their
/// [`PartialOrd`] / [`Ord`] ordering. Consumers react at different thresholds:
///
/// - `>= Draining` — stop accepting new work
/// - `>= Cancelling` — abort in-flight handlers
/// - `>= Terminating` — hard stop, drop everything
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub enum ShutdownPhase {
    /// Normal operation.
    #[default]
    Running,
    /// Dispatch halted; in-flight handlers continue uninterrupted.
    Draining,
    /// Abort signal sent to handlers; operations short-circuit with a
    /// `Shutdown` error.
    Cancelling,
    /// Hard stop — drain loop exits, remaining queued work is dropped.
    Terminating,
}

/// Information about the Kafka partition being processed.
struct PartitionInfo {
    /// The Kafka topic name.
    topic: Topic,
    /// The partition number within the topic.
    partition: Partition,
}

/// Runtime context for partition message processing.
///
/// Groups the channels and trackers needed for partition processing,
/// separating runtime state from static configuration.
///
/// `P` is the payload type carried by incoming [`ConsumerMessage`]s.
struct PartitionContext<P> {
    /// Tracks offset commits and processing progress.
    offsets: OffsetTracker,
    /// Channel receiving messages to process.
    message_rx: Receiver<ConsumerRecord<P>>,
    /// Registry for monitoring processing and timer heartbeats.
    heartbeats: HeartbeatRegistry,
    /// Channel receiving shutdown phase transitions.
    shutdown_rx: watch::Receiver<ShutdownPhase>,
}

/// Configuration settings for a partition manager.
///
/// Contains all the parameters needed to configure message processing
/// for a Kafka partition, including buffer sizes, timer concurrency,
/// and filtering options.
///
/// `S` is a [`TriggerStoreProvider`] that creates per-partition stores.
/// `SP` is a [`PartitionStateProvider`] that acquires per-partition
/// keyed-state managers. `P` is the payload type carried by consumed
/// messages.
#[derive(Clone, Debug)]
pub struct PartitionConfiguration<S, SP, P> {
    /// Consumer group identifier
    pub group_id: Arc<str>,

    /// Maximum size of message buffers
    pub buffer_size: usize,

    /// Maximum number of uncommitted messages allowed
    pub max_uncommitted: usize,

    /// Optional automaton for filtering messages by event type
    pub allowed_events: Option<AhoCorasick>,

    /// Timeout duration for shutdown operations
    pub shutdown_timeout: Duration,

    /// Duration of inactivity allowed before considering a partition stalled
    pub stall_threshold: Duration,

    /// Shared counter tracking watermark updates
    pub watermark_version: Arc<CachePadded<AtomicUsize>>,

    /// Deduplication hash version. Threaded into the per-message
    /// [`EventRef::Message`] dedup id so recovery resolves a message's
    /// committed state by the exact id the settle boundary records.
    pub version: Arc<str>,

    /// Trigger store provider — creates per-partition stores with independent
    /// caches.
    pub trigger_provider: S,

    /// Keyed-state provider — acquires per-partition state managers.
    pub state_provider: SP,

    /// Timer slab size
    pub timer_slab_size: CompactDuration,

    /// Per-type semaphores bounding in-flight timer events across all
    /// partitions
    pub timer_semaphores: Arc<TimerSemaphores>,

    /// Telemetry sender for creating partition-scoped telemetry senders
    pub telemetry_sender: TelemetrySender,

    /// How timer dispatch spans relate to the propagated `OTel` context.
    pub timer_spans: SpanRelation,

    /// Phantom marker for the payload type, used to keep `P` consistent
    /// between [`PartitionConfiguration`] and [`PartitionManager`].
    pub(crate) _payload: PhantomData<fn() -> P>,
}

/// Manages message processing and offset tracking for a single Kafka partition.
///
/// Coordinates concurrent message processing by:
/// - Queuing messages by key to maintain ordering for each key
/// - Tracking and committing message offsets to ensure at-least-once processing
/// - Managing graceful partition shutdown during rebalancing
/// - Enforcing backpressure through the bounded message channel
/// - Monitoring for processing stalls
///
/// `P` is the payload type carried by consumed messages.
#[derive(Educe)]
#[educe(Debug)]
pub struct PartitionManager<P> {
    /// The Kafka topic this partition belongs to
    topic: Topic,
    /// The partition number this manager handles
    partition: Partition,

    /// Tracks offset commits and processing progress
    #[educe(Debug(ignore))]
    offsets: OffsetTracker,

    /// Channel for sending messages to be processed
    #[educe(Debug(ignore))]
    message_tx: Sender<ConsumerRecord<P>>,

    /// Heartbeat registry
    #[educe(Debug(ignore))]
    heartbeats: HeartbeatRegistry,

    /// Drives partition shutdown phase transitions
    #[educe(Debug(ignore))]
    shutdown_tx: watch::Sender<ShutdownPhase>,

    /// Total time budget for shutdown phase transitions
    shutdown_timeout: Duration,

    /// Handle for the message processing task
    #[educe(Debug(ignore))]
    handle: JoinHandle<()>,
}

impl<P: Send + 'static> PartitionManager<P> {
    /// Creates a new partition manager.
    pub fn new<T, S, SP>(
        config: PartitionConfiguration<S, SP, P>,
        handler: T,
        topic: Topic,
        partition: Partition,
    ) -> Self
    where
        T: EventHandler<Payload = P> + Send + Sync + 'static,
        S: TriggerStoreProvider,
        SP: PartitionStateProvider<S::Store>,
        <SP::Manager as PartitionStateManager>::Session:
            EventSession<Loader: MessageLoader<Payload = P>>,
        P: Sync + EventType + EventIdentity,
    {
        // Initialize offset tracker to manage offset state
        let offsets = OffsetTracker::new(
            topic,
            partition,
            config.max_uncommitted,
            config.stall_threshold,
            config.watermark_version.clone(),
        );

        // Initialize heartbeats, channels, and shutdown signals
        let heartbeats =
            HeartbeatRegistry::new(format!("{topic}:{partition}"), config.stall_threshold);
        let (message_tx, message_rx) = channel(config.buffer_size);
        let (shutdown_tx, shutdown_rx) = watch::channel(ShutdownPhase::default());
        let shutdown_timeout = config.shutdown_timeout;

        // Spawn the background task for message handling
        let partition_info = PartitionInfo { topic, partition };
        let context = PartitionContext {
            offsets: offsets.clone(),
            message_rx,
            heartbeats: heartbeats.clone(),
            shutdown_rx,
        };
        let handle = spawn(handle_messages(config, partition_info, handler, context));

        Self {
            topic,
            partition,
            offsets,
            message_tx,
            heartbeats,
            shutdown_tx,
            shutdown_timeout,
            handle,
        }
    }

    /// Checks if the partition can accept more messages.
    ///
    /// This method indicates whether the internal message queue has capacity
    /// for more messages, which is used to implement backpressure.
    pub fn has_capacity(&self) -> bool {
        self.message_tx.capacity() > 0
    }

    /// Attempts to enqueue a message for processing.
    ///
    /// This non-blocking method tries to send a message to the internal
    /// processing queue without waiting. If the queue is full or closed, the
    /// original message is returned to the caller in the `Err` variant.
    pub(crate) fn try_send_record(
        &self,
        message: ConsumerRecord<P>,
    ) -> Result<(), ConsumerRecord<P>> {
        self.message_tx
            .try_send(message)
            .map_err(|error| match error {
                TrySendError::Closed(message) | TrySendError::Full(message) => message,
            })
    }

    /// Gets the current committed offset watermark.
    ///
    /// The watermark represents the highest contiguous offset that has been
    /// successfully processed and committed. This is used for offset management
    /// and reporting consumer progress. Returns `None` if no messages have
    /// been committed yet.
    pub fn watermark(&self) -> Option<Offset> {
        self.offsets.watermark()
    }

    /// Checks if message processing has stalled.
    ///
    /// A partition is considered stalled if any of:
    /// - The offset tracker detects uncommitted offsets beyond the stall
    ///   threshold
    /// - The message processing heartbeat hasn't been updated within the stall
    ///   threshold
    /// - The timer system heartbeat (if present) hasn't been updated within the
    ///   stall threshold
    ///
    /// This method is used by health monitoring systems to detect processing
    /// issues.
    pub fn is_stalled(&self) -> bool {
        self.offsets.is_stalled() || self.heartbeats.any_stalled()
    }

    /// Initiates an orderly partition shutdown.
    ///
    /// This method performs a graceful shutdown sequence:
    /// 1. Closes the message channel to prevent new messages
    /// 2. Signals handlers to shut down gracefully
    /// 3. Waits for in-flight messages to complete processing
    /// 4. Performs final offset commits
    ///
    /// Used during consumer rebalancing or application shutdown. Returns the
    /// final committed offset watermark, or `None` if an error occurs during
    /// shutdown.
    #[instrument(level = "debug")]
    pub async fn shutdown(self) -> Option<Offset> {
        // Close the message channel to stop accepting new messages
        drop(self.message_tx);

        // Advance to Draining immediately, then schedule Cancelling and
        // Terminating at 80% and 100% of shutdown_timeout respectively.
        // If send returns Err, all receivers have dropped (partition already
        // exited) — no point spawning the phase task.
        if self.shutdown_tx.send(ShutdownPhase::Draining).is_ok() {
            debug!(topic = %self.topic, partition = self.partition, phase = "draining", "shutdown phase transition");

            let now = Instant::now();
            let grace = self.shutdown_timeout * GRACE_PERIOD_NUMERATOR / GRACE_PERIOD_DENOMINATOR;
            let cancelling_at = now + grace;
            let terminating_at = now + self.shutdown_timeout;
            let topic = self.topic;
            let partition = self.partition;
            let shutdown_tx = self.shutdown_tx;

            spawn(async move {
                sleep_until(cancelling_at).await;
                if shutdown_tx.send(ShutdownPhase::Cancelling).is_err() {
                    return;
                }
                debug!(topic = %topic, partition, phase = "cancelling", "shutdown phase transition");

                sleep_until(terminating_at).await;
                let _ = shutdown_tx.send(ShutdownPhase::Terminating);
                debug!(topic = %topic, partition, phase = "terminating", "shutdown phase transition");
            });
        }

        // Wait for message processing to complete
        if let Err(error) = self.handle.await {
            error!(
                topic = %self.topic,
                partition = self.partition,
                "error occurred while shutting down partition: {error:#}"
            );
            return None;
        }

        // Perform final offset commit and return the watermark
        self.offsets.shutdown().await
    }
}
