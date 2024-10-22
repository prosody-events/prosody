//! Manages a partition in a consumer system, handling messages and offset
//! tracking.
//!
//! This module provides functionality to enqueue and process messages
//! concurrently, maintain a record of offset progression, and handle graceful
//! shutdown operations. It uses a key-based approach to manage message
//! processing and ensures efficient handling of offsets.

use std::future::ready;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::Duration;

use crossbeam_utils::CachePadded;
use educe::Educe;
use futures::StreamExt;
use thiserror::Error;
use tokio::spawn;
use tokio::sync::mpsc::error::{SendError, TrySendError};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, error, instrument};

use crate::consumer::message::{ConsumerMessage, MessageContext, UncommittedMessage};
use crate::consumer::partition::keyed::KeyManager;
use crate::consumer::partition::offsets::OffsetTracker;
use crate::consumer::EventHandler;
use crate::{Offset, Partition, Topic};

mod keyed;
pub mod offsets;
mod util;

/// Manages a single partition with associated message processing and offset
/// management.
///
/// This struct ensures efficient message processing by queuing and handling
/// messages, tracking offset progress, and managing partition-specific actions
/// such as shutdown.
#[derive(Educe)]
#[educe(Debug)]
pub struct PartitionManager {
    /// Identifier for this partition.
    partition: Partition,

    /// Manages offset tracking and commit states.
    #[educe(Debug(ignore))]
    offsets: OffsetTracker,

    /// Channel for sending messages to be processed.
    #[educe(Debug(ignore))]
    message_tx: Sender<ConsumerMessage>,

    /// Watch channel for sending shutdown signals to handlers.
    #[educe(Debug(ignore))]
    shutdown_tx: watch::Sender<bool>,

    /// Asynchronous handle for the message processing task.
    #[educe(Debug(ignore))]
    handle: JoinHandle<()>,
}

impl PartitionManager {
    /// Creates a new `PartitionManager` with specified configurations.
    ///
    /// # Arguments
    ///
    /// * `topic` - The Kafka topic associated with this partition.
    /// * `partition` - The partition identifier.
    /// * `message_handler` - Handler responsible for processing messages.
    /// * `buffer_size` - Capacity of the message channel.
    /// * `max_uncommitted` - Maximum number of uncommitted offsets.
    /// * `max_enqueued_per_key` - Maximum number of messages to hold per key.
    /// * `shutdown_timeout` - Duration to wait before forcefully shutting down.
    /// * `watermark_version` - Shared variable to track watermark updates.
    ///
    /// # Returns
    ///
    /// A new instance of `PartitionManager`.
    #[allow(clippy::too_many_arguments)]
    pub fn new<T>(
        topic: Topic,
        partition: Partition,
        message_handler: T,
        buffer_size: usize,
        max_uncommitted: usize,
        max_enqueued_per_key: usize,
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
    /// `true` if the partition has capacity for more messages, `false`
    /// otherwise.
    pub fn has_capacity(&self) -> bool {
        self.message_tx.capacity() > 0
    }

    /// Attempts to send a message to the partition.
    ///
    /// # Arguments
    ///
    /// * `message` - The message to be sent.
    ///
    /// # Returns
    ///
    /// `Ok(())` if the message was sent successfully, or `Err(ConsumerMessage)`
    /// containing the original message if the partition is full or closed.
    pub fn try_send(&self, message: ConsumerMessage) -> Result<(), ConsumerMessage> {
        self.message_tx
            .try_send(message)
            .map_err(|error| match error {
                TrySendError::Closed(message) | TrySendError::Full(message) => message,
            })
    }

    /// Retrieves the current watermark, indicating the progress of committed
    /// offsets.
    ///
    /// # Returns
    ///
    /// An `Option<Offset>` containing the current watermark if available, or
    /// `None` if not set.
    pub fn watermark(&self) -> Option<Offset> {
        self.offsets.watermark()
    }

    /// Checks if message processing for this partition has stalled.
    ///
    /// # Returns
    ///
    /// `true` if the partition is stalled, `false` otherwise.
    pub fn is_stalled(&self) -> bool {
        self.offsets.is_stalled()
    }

    /// Initiates the shutdown of the partition, waiting for the completion of
    /// all tasks.
    ///
    /// # Returns
    ///
    /// An `Option<Offset>` containing the final watermark if shutdown was
    /// successful, or `None` if an error occurred during shutdown.
    #[instrument(level = "debug")]
    pub async fn shutdown(self) -> Option<Offset> {
        // Close the message channel
        drop(self.message_tx);

        // Send shutdown signal
        if let Err(error) = self.shutdown_tx.send(true) {
            error!(
                partition = self.partition,
                "failed to send shutdown signal to handlers: {error:#}"
            );
        }

        // Wait for the message processing task to complete
        if let Err(error) = self.handle.await {
            error!(
                partition = self.partition,
                "error occurred while shutting down partition: {error:#}"
            );
            return None;
        }

        // Perform final shutdown operations and return the last watermark
        self.offsets.shutdown().await
    }
}

/// Asynchronously handles incoming messages for a partition.
///
/// This function processes messages according to the provided message handler,
/// using a `KeyManager` to manage queuing and processing of messages based on
/// their keys. It facilitates concurrent processing within the constraints of
/// maximum queue sizes per key.
///
/// # Arguments
///
/// * `message_handler` - The handler responsible for processing messages.
/// * `offsets` - An `OffsetTracker` managing the commitment of message offsets.
/// * `message_rx` - A receiver channel from which messages are received for
///   processing.
/// * `max_enqueued_per_key` - The maximum number of messages that can be queued
///   per key before blocking.
/// * `shutdown_rx` - A receiver for shutdown signals.
/// * `shutdown_timeout` - Optional timeout for how long to wait on shutdown
///   before forcefully stopping message processing.
async fn handle_messages<T>(
    message_handler: T,
    offsets: OffsetTracker,
    message_rx: Receiver<ConsumerMessage>,
    max_enqueued_per_key: usize,
    shutdown_rx: watch::Receiver<bool>,
    shutdown_timeout: Duration,
) where
    T: EventHandler,
{
    let process = |received: ConsumerMessage| async {
        // Attempt to take an offset for the received message
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

        // Process the message using the provided message handler
        let context = MessageContext::new(shutdown_rx.clone());
        message_handler.on_message(context, message).await;
    };

    // Create and run a KeyManager to manage concurrent message processing
    let mut highest_offset_seen = -1;
    KeyManager::new(process, max_enqueued_per_key)
        .process_messages(
            ReceiverStream::new(message_rx).filter(|message| {
                let partition = message.partition();
                let offset = message.offset();

                // Filter out messages with offsets we've already seen
                if offset <= highest_offset_seen {
                    debug!("filtering stale partition {} offset {}", partition, offset);
                    return ready(false);
                }
                highest_offset_seen = offset;
                ready(true)
            }),
            shutdown_rx.clone(),
            shutdown_timeout,
        )
        .await;

    message_handler.shutdown().await;
}

/// Errors that can occur during partition management operations.
#[derive(Debug, Error)]
pub enum PartitionError {
    /// Error when message sending fails due to the partition being shut down.
    #[error("failed to send; partition has been shutdown")]
    Shutdown(#[from] SendError<UncommittedMessage>),
}
