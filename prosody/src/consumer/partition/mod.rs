//! Manages a partition in a consumer system, handling messages and managing
//! offsets. This module provides functionality to enqueue and process messages
//! concurrently, maintain a record of offset progression, and handle graceful
//! shutdown operations.

use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::Duration;

use crossbeam_utils::CachePadded;
use educe::Educe;
use thiserror::Error;
use tokio::spawn;
use tokio::sync::mpsc::error::{SendError, TrySendError};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{error, info_span, instrument, Instrument};

use crate::consumer::message::{MessageContext, UntrackedMessage};
use crate::consumer::partition::keyed::KeyManager;
use crate::consumer::partition::offsets::OffsetTracker;
use crate::consumer::MessageHandler;
use crate::{Offset, Partition};

mod keyed;
pub mod offsets;
mod util;

/// Manages a single partition with associated message processing and offset
/// management. It ensures message processing is queued and handled efficiently,
/// tracking the offset progress and handling partition-specific actions such as
/// shutdown.
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
    message_tx: Sender<UntrackedMessage>,

    /// Asynchronous handle for the message processing task.
    #[educe(Debug(ignore))]
    handle: JoinHandle<()>,
}

impl PartitionManager {
    /// Creates a new `PartitionManager` with specified configurations for
    /// message handling and offset management.
    ///
    /// # Arguments
    /// * `partition` - The partition identifier.
    /// * `message_handler` - Handler responsible for processing messages.
    /// * `buffer_size` - Capacity of the message channel.
    /// * `max_uncommitted` - Maximum number of uncommitted offsets.
    /// * `max_enqueued_per_key` - Maximum number of messages to hold per key.
    /// * `shutdown_timeout` - Optional duration to wait before forcefully
    ///   shutting down.
    /// * `watermark_version` - Shared variable to track watermark updates.
    pub fn new<T>(
        partition: Partition,
        message_handler: T,
        buffer_size: usize,
        max_uncommitted: usize,
        max_enqueued_per_key: usize,
        shutdown_timeout: Option<Duration>,
        watermark_version: Arc<CachePadded<AtomicUsize>>,
    ) -> Self
    where
        T: MessageHandler + Send + Sync + 'static,
    {
        let offsets = OffsetTracker::new(max_uncommitted, watermark_version);
        let (message_tx, message_rx) = channel(buffer_size);

        let handle = spawn(handle_messages(
            message_handler,
            offsets.clone(),
            message_rx,
            max_enqueued_per_key,
            shutdown_timeout,
        ));

        Self {
            partition,
            offsets,
            message_tx,
            handle,
        }
    }

    /// Checks if the partition can accept more messages.
    pub fn has_capacity(&self) -> bool {
        self.message_tx.capacity() > 0
    }

    /// Attempts to send a message to the partition. If the partition is full or
    /// closed, the message is returned.
    pub fn try_send(&self, message: UntrackedMessage) -> Result<(), UntrackedMessage> {
        self.message_tx
            .try_send(message)
            .map_err(|error| match error {
                TrySendError::Closed(message) | TrySendError::Full(message) => message,
            })
    }

    /// Retrieves the current watermark, indicating the progress of committed
    /// offsets.
    pub fn watermark(&self) -> Option<Offset> {
        self.offsets.watermark()
    }

    /// Initiates the shutdown of the partition, waiting for the completion of
    /// all tasks.
    #[instrument(level = "debug")]
    pub async fn shutdown(self) -> Option<Offset> {
        drop(self.message_tx);

        if let Err(error) = self.handle.await {
            error!(%self.partition, "error occurred while shutting down partition: {error:#}");
        }

        self.offsets.shutdown().await
    }
}

/// Asynchronously handles incoming messages by processing them according to the
/// provided message handler. It uses a `KeyManager` to manage the queuing and
/// processing of messages based on keys derived from the messages, facilitating
/// concurrent processing within the constraints of maximum queue sizes per key.
///
/// # Arguments
/// * `message_handler` - The handler responsible for the business logic of
///   processing messages.
/// * `offsets` - An `OffsetTracker` managing the commitment of message offsets.
/// * `message_rx` - A receiver channel from which messages are received for
///   processing.
/// * `max_enqueued_per_key` - The maximum number of messages that can be queued
///   per key before blocking.
/// * `shutdown_timeout` - Optional timeout for how long to wait on shutdown
///   before forcefully stopping message processing.
async fn handle_messages<T>(
    message_handler: T,
    offsets: OffsetTracker,
    message_rx: Receiver<UntrackedMessage>,
    max_enqueued_per_key: usize,
    shutdown_timeout: Option<Duration>,
) where
    T: MessageHandler,
{
    let process = |received: UntrackedMessage, shutdown_rx: watch::Receiver<bool>| {
        let parent_span = received.span.clone();
        let span = info_span!(parent: &parent_span, "process-message",);

        async {
            // Attempt to take an offset for the received message, and if successful,
            // transform it into a consumer message for processing.
            let message = match offsets.take(received.offset).await {
                Ok(uncommitted_offset) => received.into_consumer_message(uncommitted_offset),
                Err(error) => {
                    error!(
                        ?received,
                        "unable to take uncommitted offset: {error:#}; discarding message"
                    );
                    return;
                }
            };

            // Handle the message using the provided message handler
            let mut context = MessageContext::new(shutdown_rx);
            if let Err(error) = message_handler.handle(&mut context, message).await {
                error!("message handler returned an error: {error:#}");
            }
        }
        .instrument(span)
    };

    // Create and run a KeyManager to manage concurrent message processing,
    // using the defined processing logic and managing shutdown timing.
    KeyManager::new(process, max_enqueued_per_key)
        .process_messages(ReceiverStream::new(message_rx), shutdown_timeout)
        .await;
}

/// Defines errors related to partition management, specifically during message
/// sending.
#[derive(Debug, Error)]
pub enum PartitionError {
    /// Error when message sending fails due to the partition being shut down.
    #[error("failed to send; partition has been shutdown")]
    Shutdown(#[from] SendError<UntrackedMessage>),
}
