//! A partition manager that processes messages concurrently while maintaining
//! ordering within message groups.
//!
//! This module orchestrates message processing for individual Kafka partition
//! by:
//! - Supporting concurrent processing of messages with different keys
//! - Preserving message order within each key
//! - Tracking and committing message offsets
//! - Handling graceful partition shutdown
//! - Managing message queue backpressure
//! - Deduplicating messages using idempotency identifiers

use crossbeam_utils::CachePadded;
use educe::Educe;
use futures::StreamExt;
use std::future::ready;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;
use thiserror::Error;
use tokio::spawn;
use tokio::sync::mpsc::error::{SendError, TrySendError};
use tokio::sync::mpsc::{Receiver, Sender, channel};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, error, info, instrument};

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

/// Manages message processing and offset tracking for a single Kafka partition.
///
/// Coordinates concurrent message processing by:
/// - Queuing messages by key to maintain ordering within key groups
/// - Tracking and committing message offsets
/// - Managing partition shutdown
/// - Enforcing backpressure through queue capacity limits
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
    /// * `topic` - The Kafka topic this partition belongs to
    /// * `partition` - The partition number
    /// * `message_handler` - Handler that processes the messages
    /// * `buffer_size` - Maximum number of unprocessed messages to buffer
    /// * `max_uncommitted` - Maximum number of messages processed but not
    ///   committed
    /// * `max_enqueued_per_key` - Maximum number of pending messages per key
    /// * `idempotence_cache_size` - Size of the cache for deduplicating
    ///   messages. Set to 0 to disable.
    /// * `shutdown_timeout` - How long to wait for message processing to
    ///   complete during shutdown
    /// * `watermark_version` - Counter tracking changes to committed offset
    ///   watermarks
    ///
    /// # Returns
    ///
    /// A new `PartitionManager` instance
    #[allow(clippy::too_many_arguments)]
    pub fn new<T>(
        topic: Topic,
        partition: Partition,
        message_handler: T,
        buffer_size: usize,
        max_uncommitted: usize,
        max_enqueued_per_key: usize,
        idempotence_cache_size: usize,
        shutdown_timeout: Duration,
        watermark_version: Arc<CachePadded<AtomicUsize>>,
    ) -> Self
    where
        T: EventHandler + Send + Sync + 'static,
    {
        let offsets = OffsetTracker::new(
            topic,
            partition,
            max_uncommitted,
            shutdown_timeout,
            watermark_version,
        );

        let (message_tx, message_rx) = channel(buffer_size);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let handle = spawn(handle_messages(
            message_handler,
            offsets.clone(),
            message_rx,
            max_enqueued_per_key,
            max_uncommitted,
            idempotence_cache_size,
            shutdown_rx,
            shutdown_timeout,
        ));

        Self {
            partition,
            offsets,
            message_tx,
            shutdown_tx,
            handle,
        }
    }

    /// Checks if the partition can accept more messages.
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
    /// # Returns
    ///
    /// The highest contiguous committed offset if any messages have been
    /// committed, or `None` if no messages have been committed
    pub fn watermark(&self) -> Option<Offset> {
        self.offsets.watermark()
    }

    /// Checks if message processing has stalled.
    ///
    /// # Returns
    ///
    /// `true` if messages are not being processed within the configured stall
    /// threshold, `false` otherwise
    pub fn is_stalled(&self) -> bool {
        self.offsets.is_stalled()
    }

    /// Initiates an orderly partition shutdown.
    ///
    /// Closes the message channel, signals handlers to shut down, and waits for
    /// in-flight messages to complete processing.
    ///
    /// # Returns
    ///
    /// The final committed offset watermark if shutdown completes successfully,
    /// or `None` if an error occurs during shutdown
    #[instrument(level = "debug")]
    pub async fn shutdown(self) -> Option<Offset> {
        // Close the message channel
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

        // Perform final offset commit and get watermark
        self.offsets.shutdown().await
    }
}

/// Processes messages for a partition.
///
/// Coordinates message processing by:
/// - Filtering duplicate and out-of-order messages
/// - Managing per-key message queues
/// - Handling offset tracking and commitment
/// - Gracefully shutting down when requested
///
/// # Arguments
///
/// * `message_handler` - Handler that processes messages
/// * `offsets` - Tracks offset commits and processing progress
/// * `message_rx` - Channel receiving messages to process
/// * `max_enqueued_per_key` - Maximum number of pending messages per key
/// * `max_concurrency` - Maximum number of concurrent tasks executing.
/// * `idempotence_cache_size` - Size of cache for deduplicating messages by
///   event ID
/// * `shutdown_rx` - Channel receiving shutdown signal
/// * `shutdown_timeout` - How long to wait for processing to complete during
///   shutdown
#[allow(clippy::too_many_arguments)]
async fn handle_messages<T>(
    message_handler: T,
    offsets: OffsetTracker,
    message_rx: Receiver<ConsumerMessage>,
    max_enqueued_per_key: usize,
    max_concurrency: usize,
    idempotence_cache_size: usize,
    shutdown_rx: watch::Receiver<bool>,
    shutdown_timeout: Duration,
) where
    T: EventHandler,
{
    let mut highest_offset_seen = -1;
    let mut idempotence_cache = IdempotenceCache::new(idempotence_cache_size);

    // Filter out duplicate and out-of-order messages
    let stream = ReceiverStream::new(message_rx).filter(|message| {
        let partition = message.partition();
        let offset = message.offset();

        // Skip messages with offsets we've already seen
        if offset <= highest_offset_seen {
            debug!("filtering stale partition {partition} offset {offset}");
            return ready(false);
        }
        highest_offset_seen = offset;

        // Skip messages with duplicate event IDs
        if let Some(event_id) = idempotence_cache.check_duplicate(message.key(), message.payload())
        {
            info!("message with id {event_id} already processed; skipping");
            return ready(false);
        }

        ready(true)
    });

    // Process messages
    let process = |received: ConsumerMessage| async {
        // Reserve offset and create uncommitted message
        let message = match offsets.take(received.offset()).await {
            Ok(uncommitted_offset) => received.into_uncommitted(uncommitted_offset),
            Err(error) => {
                error!(
                    ?received,
                    "unable to take uncommitted offset: {error:#}; discarding message"
                );
                return;
            }
        };

        // Process message with handler
        let context = MessageContext::new(shutdown_rx.clone());
        message_handler.on_message(context, message).await;
    };

    // Create key manager to handle concurrent processing
    KeyManager::new(process, max_enqueued_per_key)
        .process_messages(
            stream,
            shutdown_rx.clone(),
            max_concurrency,
            shutdown_timeout,
        )
        .await;

    message_handler.shutdown().await;
}

/// Errors that can occur during partition operations.
#[derive(Debug, Error)]
pub enum PartitionError {
    /// Message could not be sent because the partition was shut down
    #[error("failed to send; partition has been shutdown")]
    Shutdown(#[from] SendError<UncommittedMessage>),
}
