//! Core functionality for handling consumer messages in a Kafka message
//! processing system.
//!
//! This module defines structures for managing message contexts and consumer
//! messages, including tracking shutdown signals and committing messages after
//! processing.

use chrono::{DateTime, Utc};
use educe::Educe;
use tokio::sync::watch;
use tracing::{debug, error, Span};

use crate::consumer::partition::offsets::UncommittedOffset;
use crate::consumer::Keyed;
use crate::{Key, Offset, Partition, Payload, Topic};

/// Represents the context for a message within a consumer.
#[derive(Clone, Debug)]
pub struct MessageContext {
    shutdown_rx: watch::Receiver<bool>,
}

impl MessageContext {
    /// Creates a new message context.
    ///
    /// This method is intended for internal use within the crate.
    pub(crate) fn new(shutdown_rx: watch::Receiver<bool>) -> Self {
        Self { shutdown_rx }
    }

    pub async fn on_shutdown(&self) {
        if let Err(error) = self
            .shutdown_rx
            .clone()
            .wait_for(|is_shutdown| *is_shutdown)
            .await
        {
            error!("shutdown hook failed: {error:#}");
        }
    }
}

/// A message consumed from a topic with associated metadata and commit state.
#[derive(Educe)]
#[educe(Debug)]
pub struct UncommittedMessage {
    topic: Topic,
    partition: Partition,
    offset: Offset,
    key: Key,
    timestamp: DateTime<Utc>,

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

    /// Returns the message timestamp.
    #[must_use]
    pub fn timestamp(&self) -> &DateTime<Utc> {
        &self.timestamp
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

    /// Takes ownership of the key, payload, and uncommitted offset.
    ///
    /// # Returns
    ///
    /// A tuple containing a `ConsumerMessage` and an `UncommittedOffset`.
    #[must_use]
    pub fn into_inner(self) -> (ConsumerMessage, UncommittedOffset) {
        let message = ConsumerMessage {
            topic: self.topic,
            partition: self.partition,
            offset: self.offset,
            key: self.key,
            timestamp: self.timestamp,
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

/// A message that is not yet being tracked for offset watermarks.
#[derive(Clone, Educe)]
#[educe(Debug)]
pub struct ConsumerMessage {
    /// The topic from which the message was consumed.
    pub topic: Topic,

    /// The partition from which the message was consumed.
    pub partition: Partition,

    /// The offset of the message within its partition.
    pub offset: Offset,

    /// The key associated with the message.
    pub key: Key,

    /// The timestamp of the message.
    pub timestamp: DateTime<Utc>,

    /// The payload of the message.
    #[educe(Debug(ignore))]
    pub payload: Payload,

    /// The tracing span associated with this message.
    #[educe(Debug(ignore))]
    pub span: Span,
}

impl ConsumerMessage {
    /// Converts a `ConsumerMessage` into an `UncommittedMessage` by adding
    /// uncommitted offset tracking.
    ///
    /// # Arguments
    ///
    /// * `uncommitted_offset` - The uncommitted offset to be associated with
    ///   the message.
    ///
    /// # Returns
    ///
    /// An `UncommittedMessage` with the same data as the original
    /// `ConsumerMessage`, but with added offset tracking.
    #[must_use]
    pub fn into_uncommitted(self, uncommitted_offset: UncommittedOffset) -> UncommittedMessage {
        UncommittedMessage {
            topic: self.topic,
            partition: self.partition,
            offset: self.offset,
            key: self.key,
            timestamp: self.timestamp,
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
