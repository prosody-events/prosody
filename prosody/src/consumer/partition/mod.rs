use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;

use crossbeam_utils::CachePadded;
use educe::Educe;
use thiserror::Error;
use tokio::spawn;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::mpsc::error::{SendError, TrySendError};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{error, info_span, instrument, Instrument};

use crate::{Offset, Partition};
use crate::consumer::{MessageContext, MessageHandler};
use crate::consumer::message::UntrackedMessage;
use crate::consumer::partition::keyed::KeyManager;
use crate::consumer::partition::offsets::OffsetTracker;

mod keyed;
pub mod offsets;
mod util;

#[derive(Educe)]
#[educe(Debug)]
pub struct PartitionManager {
    partition: Partition,

    #[educe(Debug(ignore))]
    offsets: OffsetTracker,

    #[educe(Debug(ignore))]
    message_tx: Sender<UntrackedMessage>,

    #[educe(Debug(ignore))]
    handle: JoinHandle<()>,
}

impl PartitionManager {
    pub fn new<T>(
        partition: Partition,
        message_handler: T,
        buffer_size: usize,
        max_uncommitted: usize,
        max_enqueued: usize,
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
            max_enqueued,
            shutdown_timeout,
        ));

        Self {
            partition,
            offsets,
            message_tx,
            handle,
        }
    }

    pub fn has_capacity(&self) -> bool {
        self.message_tx.capacity() > 0
    }

    pub fn try_send(&self, message: UntrackedMessage) -> Result<(), UntrackedMessage> {
        self.message_tx
            .try_send(message)
            .map_err(|error| match error {
                TrySendError::Full(message) => message,
                TrySendError::Closed(message) => message,
            })
    }

    pub async fn send(&self, message: UntrackedMessage) -> Result<(), PartitionError> {
        self.message_tx.send(message).await?;
        Ok(())
    }

    pub fn watermark(&self) -> Option<Offset> {
        self.offsets.watermark()
    }

    #[instrument(level = "debug")]
    pub async fn shutdown(self) -> Option<Offset> {
        drop(self.message_tx);

        if let Err(error) = self.handle.await {
            error!(%self.partition, "error occurred while shutting down partition: {error:#}");
        }

        self.offsets.shutdown().await
    }
}

async fn handle_messages<T>(
    message_handler: T,
    offsets: OffsetTracker,
    message_rx: Receiver<UntrackedMessage>,
    max_enqueued: usize,
    shutdown_timeout: Option<Duration>,
) where
    T: MessageHandler,
{
    let process = |received: UntrackedMessage| {
        let parent_span = received.span.clone();
        let span = info_span!(parent: &parent_span, "process-message",);

        async {
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

            if let Err(error) = message_handler.handle(&MessageContext, message).await {
                error!("message handler returned an error: {error:#}");
            }
        }
        .instrument(span)
    };

    KeyManager::new(process, max_enqueued)
        .process_messages(ReceiverStream::new(message_rx), shutdown_timeout)
        .await;
}

#[derive(Debug, Error)]
pub enum PartitionError {
    #[error("failed to send; partition has been shutdown")]
    Shutdown(#[from] SendError<UntrackedMessage>),
}
