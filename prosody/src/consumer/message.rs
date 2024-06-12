use educe::Educe;
use tokio::sync::watch::Receiver;
use tracing::{debug, Span};

use crate::{Key, Offset, Partition, Payload, Topic};
use crate::consumer::Keyed;
use crate::consumer::partition::offsets::UncommittedOffset;

#[derive(Educe)]
#[educe(Debug)]
pub struct MessageContext {
    shutdown_rx: Receiver<bool>,
}

impl MessageContext {
    pub(crate) fn new(shutdown_rx: Receiver<bool>) -> Self {
        Self { shutdown_rx }
    }

    pub async fn wait_for_shutdown(&mut self) {
        let _ = self.shutdown_rx.wait_for(|&is_shutdown| is_shutdown).await;
    }
}

#[derive(Educe)]
#[educe(Debug)]
pub struct ConsumerMessage {
    topic: Topic,
    partition: Partition,
    offset: Offset,
    key: Key,

    #[educe(Debug(ignore))]
    payload: Payload,

    #[educe(Debug(ignore))]
    span: Span,

    #[educe(Debug(ignore))]
    uncommitted_offset: UncommittedOffset,
}

impl ConsumerMessage {
    #[must_use]
    pub fn topic(&self) -> &'static str {
        self.topic.as_ref()
    }

    #[must_use]
    pub fn partition(&self) -> Partition {
        self.partition
    }

    #[must_use]
    pub fn offset(&self) -> Offset {
        self.offset
    }

    #[must_use]
    pub fn payload(&self) -> &Payload {
        &self.payload
    }

    #[must_use]
    pub fn span(&self) -> &Span {
        &self.span
    }

    pub fn commit(self) {
        debug!(%self.topic, %self.partition, %self.key, %self.offset, "committing message");
        self.uncommitted_offset.commit();
    }
}

impl Keyed for ConsumerMessage {
    type Key = Key;

    fn key(&self) -> &Self::Key {
        &self.key
    }
}

#[derive(Educe)]
#[educe(Debug)]
pub(crate) struct UntrackedMessage {
    pub(crate) topic: Topic,
    pub(crate) partition: Partition,
    pub(crate) offset: Offset,
    pub(crate) key: Key,

    #[educe(Debug(ignore))]
    pub(crate) payload: Payload,

    #[educe(Debug(ignore))]
    pub(crate) span: Span,
}

impl UntrackedMessage {
    pub(crate) fn into_consumer_message(
        self,
        uncommitted_offset: UncommittedOffset,
    ) -> ConsumerMessage {
        ConsumerMessage {
            topic: self.topic,
            partition: self.partition,
            offset: self.offset,
            key: self.key,
            payload: self.payload,
            span: self.span,
            uncommitted_offset,
        }
    }
}

impl Keyed for UntrackedMessage {
    type Key = Key;

    fn key(&self) -> &Self::Key {
        &self.key
    }
}
