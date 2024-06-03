use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use crossbeam_utils::CachePadded;
use educe::Educe;
use thiserror::Error;
use tokio::spawn;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::mpsc::error::SendError;
use tokio::task::JoinHandle;
use tracing::{debug, error};

use crate::consumer::{Keyed, MessageHandler};
use crate::consumer::message::ConsumerMessage;
use crate::consumer::offsets::OffsetTracker;
use crate::Partition;

mod keyed;
mod util;

#[derive(Educe)]
#[educe(Debug)]
pub struct PartitionManager {
    partition: Partition,

    #[educe(Debug(ignore))]
    offsets: OffsetTracker,

    #[educe(Debug(ignore))]
    message_tx: Sender<ConsumerMessage>,

    #[educe(Debug(ignore))]
    handle: JoinHandle<()>,
}

impl PartitionManager {
    pub fn new<T>(
        partition: Partition,
        message_handler: T,
        buffer_size: usize,
        max_uncommitted: usize,
        watermark_version: Arc<CachePadded<AtomicUsize>>,
    ) -> Self
    where
        T: MessageHandler + Send + 'static,
    {
        let offsets = OffsetTracker::new(max_uncommitted, watermark_version);
        let (message_tx, message_rx) = channel(buffer_size);
        let handle = spawn(handle_messages(partition, message_handler, message_rx));

        Self {
            partition,
            offsets,
            message_tx,
            handle,
        }
    }

    pub async fn send(&self, message: ConsumerMessage) -> Result<(), PartitionError> {
        self.message_tx.send(message).await?;
        Ok(())
    }

    pub async fn shutdown(self) {
        debug!(%self.partition, "shutting down partition");
        drop(self.message_tx);

        if let Err(error) = self.handle.await {
            error!(%self.partition, "error occurred while shutting down partition: {error:#}");
        }
    }
}

async fn handle_messages<T>(
    partition: Partition,
    message_handler: T,
    mut message_rx: Receiver<ConsumerMessage>,
) where
    T: MessageHandler,
{
    while let Some(message) = message_rx.recv().await {}
}

#[derive(Debug, Error)]
pub enum PartitionError {
    #[error("failed to send; partition has been shutdown")]
    Shutdown(#[from] SendError<ConsumerMessage>),
}
