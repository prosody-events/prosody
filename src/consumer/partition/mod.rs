//! Manages message processing and offset tracking for individual Kafka
//! partitions.
//!
//! This module orchestrates concurrent message processing while maintaining
//! ordering guarantees within key groups:
//!
//! - Processes messages with different keys concurrently for high throughput
//! - Preserves strict ordering for messages with the same key
//! - Tracks and commits message offsets for exactly-once processing
//! - Manages graceful shutdown of partition processing
//! - Implements backpressure through the bounded message channel
//! - Deduplicates messages using event IDs
//!
//! The core component is `PartitionManager`, which coordinates all aspects
//! of partition-level message processing.

use crate::consumer::event_context::{EventContext, TimerContext};
use crate::consumer::message::{ConsumerMessage, UncommittedEvent, UncommittedMessage};
use crate::consumer::partition::keyed::KeyManager;
use crate::consumer::partition::offsets::OffsetTracker;
use crate::consumer::{DemandType, EventHandler, Keyed, Uncommitted};
use crate::heartbeat::HeartbeatRegistry;
use crate::telemetry::sender::TelemetrySender;
use crate::timers::duration::CompactDuration;
use crate::timers::store::{SegmentId, TriggerStore};
use crate::timers::{PendingTimer, TimerManager, TimerManagerConfig};
use crate::{EventId, EventIdentity, Key, Offset, Partition, ProcessScope, Topic};
use ahash::RandomState;
use aho_corasick::{AhoCorasick, Anchored, Input};
use async_stream::stream;
use crossbeam_utils::CachePadded;
use educe::Educe;
use futures::stream::select;
use futures::{Stream, StreamExt, pin_mut};
use quick_cache::UnitWeighter;
use quick_cache::unsync::Cache;
use serde_json::Value;
use std::future::{Ready, ready};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;
use tokio::spawn;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::mpsc::{Receiver, Sender, channel};
use tokio::sync::{Semaphore, watch};
use tokio::task::JoinHandle;
use tokio::task::coop::cooperative;
use tokio::time::sleep;
use tracing::{debug, debug_span, error, info, info_span, instrument};
use uuid::Uuid;

mod keyed;
pub mod offsets;
mod util;

#[cfg(test)]
mod test;

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
struct PartitionContext {
    /// Tracks offset commits and processing progress.
    offsets: OffsetTracker,
    /// Channel receiving messages to process.
    message_rx: Receiver<ConsumerMessage>,
    /// Registry for monitoring processing and timer heartbeats.
    heartbeats: HeartbeatRegistry,
    /// Channel receiving shutdown signal.
    shutdown_rx: watch::Receiver<bool>,
}

/// Configuration settings for a partition manager.
///
/// Contains all the parameters needed to configure message processing
/// for a Kafka partition, including buffer sizes, timer concurrency,
/// and filtering options.
#[derive(Clone, Debug)]
pub struct PartitionConfiguration<T> {
    /// Consumer group identifier
    pub group_id: Arc<str>,

    /// Maximum size of message buffers
    pub buffer_size: usize,

    /// Maximum number of uncommitted messages allowed
    pub max_uncommitted: usize,

    /// Size of idempotence cache for message deduplication
    pub idempotence_cache_size: usize,

    /// Optional automaton for filtering messages by event type
    pub allowed_events: Option<AhoCorasick>,

    /// Timeout duration for shutdown operations
    pub shutdown_timeout: Duration,

    /// Duration of inactivity allowed before considering a partition stalled
    pub stall_threshold: Duration,

    /// Shared counter tracking watermark updates
    pub watermark_version: Arc<CachePadded<AtomicUsize>>,

    /// Timer store
    pub trigger_store: T,

    /// Timer slab size
    pub timer_slab_size: CompactDuration,

    /// Global semaphore bounding in-flight timer events across all partitions
    pub timer_semaphore: Arc<Semaphore>,

    /// Telemetry sender for creating partition-scoped telemetry senders
    pub telemetry_sender: TelemetrySender,
}

/// Manages message processing and offset tracking for a single Kafka partition.
///
/// Coordinates concurrent message processing by:
/// - Queuing messages by key to maintain ordering for each key
/// - Tracking and committing message offsets to ensure at-least-once processing
/// - Managing graceful partition shutdown during rebalancing
/// - Enforcing backpressure through the bounded message channel
/// - Monitoring for processing stalls
#[derive(Educe)]
#[educe(Debug)]
pub struct PartitionManager {
    /// The partition number this manager handles
    partition: Partition,

    /// Tracks offset commits and processing progress
    #[educe(Debug(ignore))]
    offsets: OffsetTracker,

    /// Channel for sending messages to be processed
    #[educe(Debug(ignore))]
    message_tx: Sender<ConsumerMessage>,

    /// Heartbeat registry
    #[educe(Debug(ignore))]
    heartbeats: HeartbeatRegistry,

    /// Signals shutdown to message handlers
    #[educe(Debug(ignore))]
    shutdown_tx: watch::Sender<bool>,

    /// Handle for the message processing task
    #[educe(Debug(ignore))]
    handle: JoinHandle<()>,
}

impl PartitionManager {
    /// Creates a new partition manager.
    ///
    /// # Arguments
    ///
    /// * `config` - The partition configuration
    /// * `handler` - The message handler that will process messages
    /// * `topic` - The Kafka topic this partition belongs to
    /// * `partition` - The partition number
    ///
    /// # Returns
    ///
    /// A new `PartitionManager` instance
    pub fn new<T, S>(
        config: PartitionConfiguration<S>,
        handler: T,
        topic: Topic,
        partition: Partition,
    ) -> Self
    where
        T: EventHandler + Send + Sync + 'static,
        S: TriggerStore,
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
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

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
            partition,
            offsets,
            message_tx,
            heartbeats,
            shutdown_tx,
            handle,
        }
    }

    /// Checks if the partition can accept more messages.
    ///
    /// This method indicates whether the internal message queue has capacity
    /// for more messages, which is used to implement backpressure.
    ///
    /// # Returns
    ///
    /// `true` if there is space in the message queue, `false` if the queue is
    /// at capacity
    pub fn has_capacity(&self) -> bool {
        self.message_tx.capacity() > 0
    }

    /// Attempts to enqueue a message for processing.
    ///
    /// This non-blocking method tries to send a message to the internal
    /// processing queue without waiting. If the queue is full or closed,
    /// the original message is returned.
    ///
    /// # Arguments
    ///
    /// * `message` - The message to enqueue
    ///
    /// # Returns
    ///
    /// `Ok(())` if the message was enqueued, or `Err(ConsumerMessage)`
    /// containing the original message if the queue is full or closed
    pub fn try_send(&self, message: ConsumerMessage) -> Result<(), ConsumerMessage> {
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
    /// and reporting consumer progress.
    ///
    /// # Returns
    ///
    /// The highest contiguous committed offset if any messages have been
    /// committed, or `None` if no messages have been committed
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
    ///
    /// # Returns
    ///
    /// `true` if messages are not being processed within the configured stall
    /// threshold, `false` otherwise
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
    /// Used during consumer rebalancing or application shutdown.
    ///
    /// # Returns
    ///
    /// The final committed offset watermark if shutdown completes successfully,
    /// or `None` if an error occurs during shutdown
    #[instrument(level = "debug")]
    pub async fn shutdown(self) -> Option<Offset> {
        // Close the message channel to stop accepting new messages
        drop(self.message_tx);

        // Signal handlers to shut down
        if let Err(error) = self.shutdown_tx.send(true) {
            debug!(
                partition = self.partition,
                "did not send shutdown signal to handlers: {error:#}"
            );
        }

        // Wait for message processing to complete
        if let Err(error) = self.handle.await {
            error!(
                partition = self.partition,
                "error occurred while shutting down partition: {error:#}"
            );
            return None;
        }

        // Perform final offset commit and return the watermark
        self.offsets.shutdown().await
    }
}

/// Initializes a timer manager for the partition, retrying on failure until
/// the shutdown signal is received.
///
/// Returns `None` if shutdown is signaled before initialization succeeds.
async fn init_timer_manager<S>(
    config: &PartitionConfiguration<S>,
    partition_info: &PartitionInfo,
    segment_id: SegmentId,
    name: &str,
    heartbeats: &HeartbeatRegistry,
    shutdown_rx: &watch::Receiver<bool>,
) -> Option<(
    impl Stream<Item = PendingTimer<S>> + use<S>,
    TimerManager<S>,
)>
where
    S: TriggerStore,
{
    loop {
        if *shutdown_rx.borrow() {
            return None;
        }

        let timer_config = TimerManagerConfig {
            segment_id,
            slab_size: config.timer_slab_size,
            name: name.to_owned(),
            store: config.trigger_store.clone(),
            telemetry: config
                .telemetry_sender
                .for_partition(partition_info.topic, partition_info.partition),
            source: config.group_id.clone(),
        };

        match TimerManager::new(
            timer_config,
            heartbeats.clone(),
            shutdown_rx.clone(),
            config.timer_semaphore.clone(),
        )
        .await
        {
            Ok(result) => return Some(result),
            Err(error) => {
                error!("failed to initialize timer manager: {error:#}; retrying");
                sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

/// Processes messages for a partition.
///
/// This function implements the main message processing pipeline:
/// - Filters duplicate and out-of-order messages
/// - Manages per-key message queues to preserve ordering
/// - Handles offset tracking and commitment
/// - Performs message deduplication
/// - Applies backpressure when needed
/// - Handles graceful shutdown when requested
///
/// # Arguments
///
/// * `config` - The partition configuration
/// * `partition_info` - Information about the partition being processed
/// * `handler` - Handler that processes messages
/// * `context` - Runtime context containing channels and trackers
async fn handle_messages<T, S>(
    config: PartitionConfiguration<S>,
    partition_info: PartitionInfo,
    handler: T,
    context: PartitionContext,
) where
    T: EventHandler,
    S: TriggerStore,
{
    let PartitionContext {
        offsets,
        message_rx,
        heartbeats,
        shutdown_rx,
    } = context;

    let mut highest_offset_seen = -1;

    // Initialize idempotence cache if configured
    let mut idempotence_cache =
        NonZeroUsize::new(config.idempotence_cache_size).map(|size| Cache::new(size.into()));

    // Create a processing pipeline for incoming messages
    let message_events = build_message_stream(
        &offsets,
        message_rx,
        &config.group_id,
        &mut highest_offset_seen,
        &mut idempotence_cache,
        config.allowed_events.as_ref(),
    );

    let name = format!(
        "{}:{}/{}",
        config.group_id, partition_info.topic, partition_info.partition
    );
    let segment_id = Uuid::new_v5(&Uuid::NAMESPACE_URL, name.as_bytes());

    let Some((timer_stream, timer_manager)) = init_timer_manager(
        &config,
        &partition_info,
        segment_id,
        &name,
        &heartbeats,
        &shutdown_rx,
    )
    .await
    else {
        return;
    };

    let timer_events = stream! {
        let timer_stream = timer_stream;
        pin_mut!(timer_stream);

        while let Some(timer) = cooperative(timer_stream.next()).await {
            yield UncommittedEvent::Timer(timer);
        }
    };
    let combined_stream = select(message_events, timer_events);

    // Define how to process each message
    let process = |event: UncommittedEvent<S>| async {
        // Process message with handler
        debug!(?event, "calling handler");

        match event {
            UncommittedEvent::Message(message) => {
                let context = TimerContext::new(
                    message.key().clone(),
                    shutdown_rx.clone(),
                    timer_manager.clone(),
                );
                let cloned_context = context.clone();

                let _guard = message.process_scope();
                handler
                    .on_message(context, message, DemandType::Normal)
                    .await;

                // Prevent the context from being used outside of processing
                cloned_context.invalidate();
            }
            UncommittedEvent::Timer(timer) => {
                if let Some(firing) = timer.fire().await {
                    let context = TimerContext::new(
                        firing.key().clone(),
                        shutdown_rx.clone(),
                        timer_manager.clone(),
                    );
                    let cloned_context = context.clone();

                    let _guard = firing.process_scope();
                    handler.on_timer(context, firing, DemandType::Normal).await;

                    // Prevent the context from being used outside of processing
                    cloned_context.invalidate();
                }
            }
        }
    };

    // Create key manager to handle concurrent processing while maintaining key
    // order
    KeyManager::<UncommittedEvent<S>, _, _>::new(process)
        .process_messages(
            combined_stream,
            heartbeats.register("event processor"),
            shutdown_rx.clone(),
            config.shutdown_timeout,
        )
        .await;

    // Clean up handler resources after processing completes
    handler.shutdown().await;
}

/// Builds a message processing stream with filtering and deduplication.
///
/// Creates a stream that:
/// - Filters out duplicate messages based on offsets
/// - Reserves offsets for processing
/// - Prevents consumer group loops by filtering messages from the same group
/// - Filters messages based on their event type (if filtering is configured)
/// - Deduplicates messages based on their event IDs
///
/// # Arguments
///
/// * `offsets` - Offset tracker for managing message offsets
/// * `message_rx` - Receiver channel for incoming messages
/// * `group_id` - Consumer group identifier
/// * `highest_offset_seen` - Tracks the highest offset processed
/// * `idempotence_cache` - Cache for detecting duplicate event IDs
/// * `allowed_events` - Optional filter for permitted event types
///
/// # Returns
///
/// A stream of [`UncommittedEvent`] items (each wrapping an
/// [`UncommittedMessage`] or a timer) ready for processing
fn build_message_stream<T>(
    offsets: &OffsetTracker,
    mut message_rx: Receiver<ConsumerMessage>,
    group_id: &str,
    highest_offset_seen: &mut i64,
    idempotence_cache: &mut Option<Cache<Key, EventId, UnitWeighter, RandomState>>,
    allowed_events: Option<&AhoCorasick>,
) -> impl Stream<Item = UncommittedEvent<T>>
where
    T: TriggerStore,
{
    stream! {
        while let Some(message) = message_rx.recv().await {
            // Apply filter_rewind - skip messages with offsets we've already processed
            if !filter_rewind(highest_offset_seen, &message).await {
                continue;
            }

            // Apply reserve_offset - reserve offset and convert to UncommittedMessage
            let Some(uncommitted) = reserve_offset(offsets, message).await else {
                continue;
            };

            // Apply filter_loops - filter out messages from same consumer group
            let Some(uncommitted) = filter_loops(group_id, uncommitted).await else {
                continue;
            };

            // Apply filter_event_type - filter based on allowed event types
            let Some(uncommitted) = filter_event_type(allowed_events, uncommitted).await else {
                continue;
            };

            // Apply filter_duplicate - filter duplicate messages based on event IDs
            let Some(uncommitted) = filter_duplicate(idempotence_cache, uncommitted).await else {
                continue;
            };

            yield UncommittedEvent::Message(uncommitted);
        }
    }
}

/// Filters out messages with offsets we've already processed.
///
/// This prevents processing duplicate messages that might be delivered by
/// Kafka, especially after consumer rebalances.
///
/// # Arguments
///
/// * `highest_offset_seen` - Reference to the highest offset already processed
/// * `message` - The message to check
///
/// # Returns
///
/// `true` if the message should be processed, `false` if it should be filtered
/// out
fn filter_rewind(highest_offset_seen: &mut i64, message: &ConsumerMessage) -> Ready<bool> {
    let partition = message.partition();
    let offset = message.offset();

    // Skip messages with offsets we've already seen
    if offset <= *highest_offset_seen {
        debug_span!(
            parent: message.span(),
            "message.filtered",
            %partition, %offset, reason = "stale"
        )
        .in_scope(|| {
            debug!("filtering stale partition {partition} offset {offset}");
        });

        return ready(false);
    }

    // Update the highest offset seen
    *highest_offset_seen = offset;
    ready(true)
}

/// Reserves an offset for a message and converts it to an uncommitted message.
///
/// # Arguments
///
/// * `offsets` - The offset tracker to reserve offsets from
/// * `received` - The consumer message to process
///
/// # Returns
///
/// `Some(UncommittedMessage)` if the offset was successfully reserved,
/// `None` if the reservation failed
async fn reserve_offset(
    offsets: &OffsetTracker,
    received: ConsumerMessage,
) -> Option<UncommittedMessage> {
    // Attempt to reserve the offset
    received
        .span()
        .in_scope(|| async {
            match offsets.take(received.offset()).await {
                Ok(uncommitted_offset) => Some(received.into_uncommitted(uncommitted_offset)),
                Err(error) => {
                    error!(
                        ?received,
                        "unable to take uncommitted offset: {error:#}; discarding message"
                    );
                    None
                }
            }
        })
        .await
}

/// Filters out messages produced by the same consumer group to prevent loops.
///
/// # Arguments
///
/// * `group_id` - The consumer group ID
/// * `message` - The message to check
///
/// # Returns
///
/// `Some(message)` if the message should be processed,
/// `None` if it should be filtered out
async fn filter_loops(group_id: &str, message: UncommittedMessage) -> Option<UncommittedMessage> {
    // Check if the message comes from the same source system as our own consumer
    // group
    if message
        .source_system()
        .is_some_and(|source_system| source_system.as_str() == group_id)
    {
        info_span!(
            parent: message.span(),
            "message.filtered",
            reason = "source-system-loop"
        )
        .in_scope(|| {
            debug!("skipping message because source system header matches the group identifier");
        });

        // Commit the message and filter it out
        message.commit().await;
        return None;
    }

    Some(message)
}

/// Filters messages based on their event type if filtering is enabled.
///
/// Only messages with event types matching the allowed patterns will be
/// processed.
///
/// # Arguments
///
/// * `allowed_events` - Optional automaton defining allowed event type patterns
/// * `message` - The message to check
///
/// # Returns
///
/// `Some(message)` if the message should be processed,
/// `None` if it should be filtered out
async fn filter_event_type(
    allowed_events: Option<&AhoCorasick>,
    message: UncommittedMessage,
) -> Option<UncommittedMessage> {
    // Extract event type from message payload if present
    let Some(event_type) = message.payload().get("type").and_then(Value::as_str) else {
        return Some(message);
    };

    // Check if the event type is allowed
    if allowed_events.as_ref().is_some_and(|automaton| {
        let input = Input::new(event_type).anchored(Anchored::Yes);
        automaton.find(input).is_none()
    }) {
        info_span!(
            parent: message.span(),
            "message.filtered",
            reason = "event-type"
        )
        .in_scope(|| {
            debug!("skipping message because {event_type} is not an allowed event type");
        });

        // Commit the message and filter it out
        message.commit().await;
        return None;
    }

    Some(message)
}

/// Filters out duplicate messages based on their event IDs.
///
/// Uses the idempotence cache to detect and filter messages that have already
/// been processed, ensuring exactly-once processing semantics.
///
/// # Arguments
///
/// * `idempotence_cache` - Cache for detecting duplicate event IDs
/// * `message` - The message to check
///
/// # Returns
///
/// `Some(message)` if the message should be processed,
/// `None` if it should be filtered out as a duplicate
async fn filter_duplicate(
    idempotence_cache: &mut Option<Cache<Key, EventId, UnitWeighter, RandomState>>,
    message: UncommittedMessage,
) -> Option<UncommittedMessage> {
    // Skip deduplication if no cache is configured
    let Some(idempotence_cache) = idempotence_cache else {
        return Some(message);
    };

    // If the message has no event ID, remove any existing entry for this key
    let Some(event_id) = message.payload().event_id() else {
        idempotence_cache.remove(message.key());
        return Some(message);
    };

    match idempotence_cache.get_mut_or_guard(message.key()) {
        // Item is not in the cache; insert it
        Err(guard) => {
            guard.insert(event_id.into());
            Some(message)
        }

        // Existing item could not be retrieved; skip it
        Ok(None) => Some(message),

        // Item is in the cache
        Ok(Some(mut value)) => {
            // Check if the event ID in the cache matches the message's event ID
            if value.as_str() == event_id {
                // Record a span and skip the message
                info_span!(
                    parent: message.span(),
                    "message.filtered",
                    reason = "duplicate-event-id",
                    event_id
                )
                .in_scope(|| {
                    info!("message with id {event_id} already processed; skipping");
                });

                message.commit().await;
                None
            } else {
                // Update the cache with the new event ID
                *value = event_id.into();
                Some(message)
            }
        }
    }
}
