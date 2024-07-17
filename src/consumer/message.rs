//! Provides core functionality for handling consumer messages within a
//! Kafka message processing system. This module defines structures for
//! managing message contexts and for defining the behavior of consumer
//! messages, including tracking shutdown signals and committing messages after
//! processing.

use educe::Educe;
use tracing::{debug, Span};

use crate::consumer::partition::offsets::UncommittedOffset;
use crate::consumer::Keyed;
use crate::{Key, Offset, Partition, Payload, Topic};

/// Represents the context for a message within a consumer
#[derive(Educe)]
#[educe(Debug)]
pub struct MessageContext;

impl MessageContext {
    /// Creates a new message context
    pub(crate) fn new() -> Self {
        Self
    }
}

/// Represents a message consumed from a topic with associated metadata and
/// state for committing.
#[derive(Educe)]
#[educe(Debug)]
pub struct UncommittedMessage {
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

impl UncommittedMessage {
    /// Returns a reference to the message's topic.
    #[must_use]
    pub fn topic(&self) -> &'static str {
        self.topic.as_ref()
    }

    /// Returns the partition of the message.
    #[must_use]
    pub fn partition(&self) -> Partition {
        self.partition
    }

    /// Returns the offset of the message.
    #[must_use]
    pub fn offset(&self) -> Offset {
        self.offset
    }

    /// Returns a reference to the message's payload.
    #[must_use]
    pub fn payload(&self) -> &Payload {
        &self.payload
    }

    /// Returns a reference to the associated span for tracing.
    #[must_use]
    pub fn span(&self) -> &Span {
        &self.span
    }

    /// Take ownership of the key, payload, and uncommitted offset.
    #[must_use]
    pub fn into_inner(self) -> (ConsumerMessage, UncommittedOffset) {
        let message = ConsumerMessage {
            topic: self.topic,
            partition: self.partition,
            offset: self.offset,
            key: self.key,
            payload: self.payload,
            span: self.span,
        };

        (message, self.uncommitted_offset)
    }

    /// Commits the message, marking its offset as processed.
    pub fn commit(self) {
        debug!(%self.topic, %self.partition, %self.key, %self.offset, "committing message");
        self.uncommitted_offset.commit();
    }
}

impl Keyed for UncommittedMessage {
    type Key = Key;

    /// Returns a reference to the message's key.
    fn key(&self) -> &Self::Key {
        &self.key
    }
}

/// Represents a message that is not yet being tracked for offset watermarks.
#[derive(Educe)]
#[educe(Debug)]
pub struct ConsumerMessage {
    pub topic: Topic,
    pub partition: Partition,
    pub offset: Offset,
    pub key: Key,

    #[educe(Debug(ignore))]
    pub payload: Payload,

    #[educe(Debug(ignore))]
    pub span: Span,
}

impl ConsumerMessage {
    /// Converts an `UntrackedMessage` into a `ConsumerMessage` by adding
    /// uncommitted offset tracking.
    ///
    /// # Arguments
    /// * `uncommitted_offset` - The uncommitted offset to be associated with
    ///   the message.
    #[must_use]
    pub fn into_uncommitted(self, uncommitted_offset: UncommittedOffset) -> UncommittedMessage {
        UncommittedMessage {
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

impl Keyed for ConsumerMessage {
    type Key = Key;

    /// Returns a reference to the message's key.
    fn key(&self) -> &Self::Key {
        &self.key
    }
}
