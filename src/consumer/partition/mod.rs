//! Manages message processing and offset tracking for individual Kafka partitions.
//!
//! This module orchestrates concurrent message processing while maintaining
//! ordering guarantees within key groups:
//!
//! - Processes messages with different keys concurrently for high throughput
//! - Preserves strict ordering for messages with the same key
//! - Tracks and commits message offsets for exactly-once processing
//! - Manages graceful shutdown of partition processing
//! - Implements backpressure through buffer capacity limits
//! - Deduplicates messages using event IDs
//!
//! The core component is `PartitionManager`, which coordinates all aspects
//! of partition-level message processing.

use aho_corasick::{AhoCorasick, Anchored, Input};
use crossbeam_utils::CachePadded;
use educe::Educe;
use futures::{Stream, StreamExt};
use serde_json::Value;
use std::future::{Ready, ready};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;
use thiserror::Error;
use tokio::spawn;
use tokio::sync::mpsc::error::{SendError, TrySendError};
use tokio::sync::mpsc::{Receiver, Sender, channel};
use tokio::sync::{Semaphore, watch};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, debug_span, error, info, info_span, instrument};

use crate::consumer::heartbeat::Heartbeat;
use crate::consumer::message::{ConsumerMessage, MessageContext, UncommittedMessage};
use crate::consumer::partition::keyed::KeyManager;
use crate::consumer::partition::offsets::OffsetTracker;
use crate::consumer::{EventHandler, Keyed};
use crate::deduplication::IdempotenceCache;
use crate::{Offset, Partition, Topic};

mod keyed;
pub mod offsets;
mod util;

#[cfg(test)]
mod test;

/// Configuration settings for a partition manager.
///
/// Contains all the parameters needed to configure message processing
/// for a Kafka partition, including buffer sizes, concurrency limits,
/// and filtering options.
#[derive(Clone, Debug)]
pub struct PartitionConfiguration {
    /// Consumer group identifier
    pub group_id: Arc<str>,

    /// Maximum size of message buffers
    pub buffer_size: usize,

    /// Maximum number of uncommitted messages allowed
    pub max_uncommitted: usize,

    /// Maximum number of queued messages per key
    pub max_enqueued_per_key: usize,

    /// Size of idempotence cache
    pub idempotence_cache_size: usize,

    /// Optional automaton for filtering messages by event type
    pub allowed_events: Option<AhoCorasick>,

    /// Timeout duration for shutdown operations
    pub shutdown_timeout: Duration,

    /// Duration of inactivity allowed before considering a partition stalled
    pub stall_threshold: Duration,

    /// Shared counter tracking watermark updates
    pub watermark_version: Arc<CachePadded<AtomicUsize>>,

    /// Global concurrency limit
    pub global_limit: Arc<Semaphore>,
}

/// Manages message processing and offset tracking for a single Kafka partition.
///
/// Coordinates concurrent message processing by:
/// - Queuing messages by key to maintain ordering for each key
/// - Tracking and committing message offsets to ensure at-least-once processing
/// - Managing graceful partition shutdown during rebalancing
/// - Enforcing backpressure through queue capacity limits
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

    /// Heartbeat used to detect stalled processing loop
    heartbeat: Heartbeat,

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
    pub fn new<T>(
        config: PartitionConfiguration,
        handler: T,
        topic: Topic,
        partition: Partition,
    ) -> Self
    where
        T: EventHandler + Send + Sync + 'static,
    {
        // Initialize offset tracker to manage offset state
        let offsets = OffsetTracker::new(
            topic,
            partition,
            config.max_uncommitted,
            config.stall_threshold,
            config.watermark_version.clone(),
        );

        // Create descriptive name for the heartbeat monitor
        let name = format!("{topic}/{partition} event loop");

        // Initialize heartbeat, channels, and shutdown signals
        let heartbeat = Heartbeat::new(name, config.stall_threshold);
        let (message_tx, message_rx) = channel(config.buffer_size);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        // Spawn the background task for message handling
        let handle = spawn(handle_messages(
            config,
            handler,
            offsets.clone(),
            message_rx,
            heartbeat.clone(),
            shutdown_rx,
        ));

        Self {
            partition,
            offsets,
            message_tx,
            heartbeat,
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
    /// A partition is considered stalled if either:
    /// - The offset tracker detects uncommitted offsets beyond the stall
    ///   threshold
    /// - The message processing heartbeat hasn't been updated within the stall
    ///   threshold
    ///
    /// This method is used by health monitoring systems to detect processing
    /// issues.
    ///
    /// # Returns
    ///
    /// `true` if messages are not being processed within the configured stall
    /// threshold, `false` otherwise
    pub fn is_stalled(&self) -> bool {
        self.offsets.is_stalled() || self.heartbeat.is_stalled()
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
/// * `handler` - Handler that processes messages
/// * `offsets` - Tracks offset commits and processing progress
/// * `message_rx` - Channel receiving messages to process
/// * `heartbeat` - Heartbeat used to detect stalled processing loop
/// * `shutdown_rx` - Channel receiving shutdown signal
async fn handle_messages<T>(
    config: PartitionConfiguration,
    handler: T,
    offsets: OffsetTracker,
    message_rx: Receiver<ConsumerMessage>,
    heartbeat: Heartbeat,
    shutdown_rx: watch::Receiver<bool>,
) where
    T: EventHandler,
{
    let mut highest_offset_seen = -1;
    let mut idempotence_cache = IdempotenceCache::new(config.idempotence_cache_size);

    // Create a processing pipeline for incoming messages
    let stream = build_stream(
        &offsets,
        message_rx,
        &config.group_id,
        &mut highest_offset_seen,
        &mut idempotence_cache,
        config.allowed_events.as_ref(),
    );

    // Define how to process each message
    let process = |message: UncommittedMessage| async {
        // Acquire a semaphore to bound global concurrency
        debug!(?message, "acquiring permit");
        let _permit = match config.global_limit.acquire().await {
            Ok(permit) => permit,
            Err(error) => {
                error!(
                    ?message,
                    "failed to acquire permit: {error:#}; aborting message"
                );
                message.abort();
                return;
            }
        };

        // Process message with handler
        debug!(?message, "permit acquired; calling handler");
        let context = MessageContext::new(shutdown_rx.clone());
        handler.on_message(context, message).await;
    };

    // Create key manager to handle concurrent processing while maintaining key
    // order
    KeyManager::new(process, config.max_enqueued_per_key)
        .process_messages(
            stream,
            heartbeat,
            shutdown_rx.clone(),
            config.max_uncommitted,
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
/// A stream of `UncommittedMessage` objects ready for processing
fn build_stream(
    offsets: &OffsetTracker,
    message_rx: Receiver<ConsumerMessage>,
    group_id: &str,
    highest_offset_seen: &mut i64,
    idempotence_cache: &mut IdempotenceCache,
    allowed_events: Option<&AhoCorasick>,
) -> impl Stream<Item = UncommittedMessage> {
    ReceiverStream::new(message_rx)
        // Skip messages with offsets we've already seen (handles duplicates from librdkafka)
        .filter(|message| filter_rewind(highest_offset_seen, message))
        // Reserve offset and create uncommitted message
        .filter_map(async |received| reserve_offset(offsets, received).await)
        // Filter out messages where the source system matches the group identifier
        // This prevents processing messages produced by this consumer group (loop avoidance)
        .filter_map(move |message| filter_loops(group_id, message))
        // Filter messages based on event type if filtering is enabled
        .filter_map(move |message| filter_event_type(allowed_events, message))
        // Filter out duplicate messages using idempotence cache
        .filter_map(|message| filter_duplicate(idempotence_cache, message))
}

/// Filters out messages with offsets we've already processed.
///
/// This prevents processing duplicate messages that might be delivered by Kafka,
/// especially after consumer rebalances.
///
/// # Arguments
///
/// * `highest_offset_seen` - Reference to the highest offset already processed
/// * `message` - The message to check
///
/// # Returns
///
/// `true` if the message should be processed, `false` if it should be filtered out
fn filter_rewind(highest_offset_seen: &mut i64, message: &ConsumerMessage) -> Ready<bool> {
    let partition = message.partition();
    let offset = message.offset();

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
    let span = received.span().clone();
    let _enter = span.enter();

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
fn filter_loops(group_id: &str, message: UncommittedMessage) -> Ready<Option<UncommittedMessage>> {
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

        message.commit();
        return ready(None);
    }

    ready(Some(message))
}

/// Filters messages based on their event type if filtering is enabled.
///
/// Only messages with event types matching the allowed patterns will be processed.
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
fn filter_event_type(
    allowed_events: Option<&AhoCorasick>,
    message: UncommittedMessage,
) -> Ready<Option<UncommittedMessage>> {
    let Some(event_type) = message.payload().get("type").and_then(Value::as_str) else {
        return ready(Some(message));
    };

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

        message.commit();
        return ready(None);
    }

    ready(Some(message))
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
fn filter_duplicate(
    idempotence_cache: &mut IdempotenceCache,
    message: UncommittedMessage,
) -> Ready<Option<UncommittedMessage>> {
    if let Some(event_id) = idempotence_cache.check_duplicate(message.key(), message.payload()) {
        info_span!(
            parent: message.span(),
            "message.filtered",
            reason = "duplicate-event-id",
            event_id
        )
        .in_scope(|| {
            info!("message with id {event_id} already processed; skipping");
        });

        message.commit();
        return ready(None);
    }

    ready(Some(message))
}

/// Errors that can occur during partition operations.
#[derive(Debug, Error)]
pub enum PartitionError {
    /// Message could not be sent because the partition was shut down
    #[error("failed to send; partition has been shutdown")]
    Shutdown(#[from] SendError<UncommittedMessage>),
}
