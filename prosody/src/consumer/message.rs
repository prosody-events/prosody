use educe::Educe;
use tracing::{debug, Span};

use crate::{Key, Offset, Partition, Payload, Topic};
use crate::consumer::offsets::OffsetPermit;

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
    permit: OffsetPermit,
}

impl ConsumerMessage {
    pub fn topic(&self) -> &'static str {
        self.topic.as_ref()
    }

    pub fn partition(&self) -> Partition {
        self.partition
    }

    pub fn offset(&self) -> Offset {
        self.offset
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn payload(&self) -> &Payload {
        &self.payload
    }

    pub fn span(&self) -> &Span {
        &self.span
    }

    pub fn commit(self) {
        debug!(%self.topic, %self.partition, %self.key, %self.offset, "committing message");
        self.permit.commit();
    }
}
