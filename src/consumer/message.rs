//! Core functionality for handling consumer messages in a Kafka message
//! processing system.
//!
//! This module defines structures for managing message contexts and consumer
//! messages, including tracking shutdown signals and committing messages after
//! processing.

use std::fmt::Debug;
use std::future::Future;
use std::sync::Arc;

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
    ///
    /// # Arguments
    ///
    /// * `shutdown_rx` - A receiver for shutdown signals.
    pub(crate) fn new(shutdown_rx: watch::Receiver<bool>) -> Self {
        Self { shutdown_rx }
    }

    /// Waits for a shutdown signal.
    ///
    /// Returns a future that completes when a partition shutdown signal is
    /// received.
    ///
    /// # Errors
    ///
    /// Logs an error message if the shutdown hook fails.
    pub fn on_shutdown(&self) -> impl Future<Output = ()> + Send {
        let mut shutdown_rx = self.shutdown_rx.clone();
        async move {
            if let Err(error) = shutdown_rx.wait_for(|is_shutdown| *is_shutdown).await {
                error!("shutdown hook failed: {error:#}");
            }
        }
    }

    /// Checks if a shutdown signal has been received.
    ///
    /// # Returns
    ///
    /// `true` if a shutdown signal has been received, `false` otherwise.
    #[must_use]
    pub fn should_shutdown(&self) -> bool {
        *self.shutdown_rx.borrow()
    }
}

/// A message consumed from a topic with associated metadata and commit state.
#[derive(Educe)]
#[educe(Debug)]
pub struct UncommittedMessage {
    inner: ConsumerMessage,

    #[educe(Debug(ignore))]
    uncommitted_offset: UncommittedOffset,
}

impl UncommittedMessage {
    /// Returns the message's topic.
    #[must_use]
    pub fn topic(&self) -> Topic {
        self.inner.topic()
    }

    /// Returns the partition of the message.
    #[must_use]
    pub fn partition(&self) -> Partition {
        self.inner.partition()
    }

    /// Returns the offset of the message.
    #[must_use]
    pub fn offset(&self) -> Offset {
        self.inner.offset()
    }

    /// Returns the message timestamp.
    #[must_use]
    pub fn timestamp(&self) -> &DateTime<Utc> {
        self.inner.timestamp()
    }

    /// Returns a reference to the message's payload.
    #[must_use]
    pub fn payload(&self) -> &Payload {
        self.inner.payload()
    }

    /// Returns a reference to the associated span for tracing.
    #[must_use]
    pub fn span(&self) -> &Span {
        self.inner.span()
    }

    /// Takes ownership of the message and returns its components.
    ///
    /// # Returns
    ///
    /// A tuple containing a `ConsumerMessage` and an `UncommittedOffset`.
    #[must_use]
    pub fn into_inner(self) -> (ConsumerMessage, UncommittedOffset) {
        (self.inner, self.uncommitted_offset)
    }

    /// Commits the message, marking its offset as processed.
    pub fn commit(self) {
        debug!(
            topic = self.topic().as_ref(),
            partition = self.partition(),
            key = self.key(),
            offset = self.offset(),
            "committing message"
        );
        self.uncommitted_offset.commit();
    }

    /// Aborts the message, halting any further progress on this partition. This
    /// should only be called during shutdown.
    pub fn abort(self) {
        debug!(
            topic = self.topic().as_ref(),
            partition = self.partition(),
            key = self.key(),
            offset = self.offset(),
            "aborting message"
        );
        self.uncommitted_offset.abort();
    }
}

impl Keyed for UncommittedMessage {
    type Key = Key;

    /// Returns a reference to the message's key.
    fn key(&self) -> &Self::Key {
        self.inner.key()
    }
}

/// A message that is cheap to clone and which does not track offset commits
#[derive(Clone, Debug)]
pub struct ConsumerMessage(Arc<ConsumerMessageValue>);

/// Contains the actual message data and metadata.
#[derive(Clone, Educe)]
#[educe(Debug)]
pub struct ConsumerMessageValue {
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
    /// Creates a new `ConsumerMessage`.
    ///
    /// # Arguments
    ///
    /// * `topic` - The topic from which the message was consumed.
    /// * `partition` - The partition from which the message was consumed.
    /// * `offset` - The offset of the message within its partition.
    /// * `key` - The key associated with the message.
    /// * `timestamp` - The timestamp of the message.
    /// * `payload` - The payload of the message.
    /// * `span` - The tracing span associated with this message.
    #[must_use]
    pub fn new(
        topic: Topic,
        partition: Partition,
        offset: Offset,
        key: Key,
        timestamp: DateTime<Utc>,
        payload: Payload,
        span: Span,
    ) -> Self {
        Self(Arc::new(ConsumerMessageValue {
            topic,
            partition,
            offset,
            key,
            timestamp,
            payload,
            span,
        }))
    }

    /// Returns the message's topic.
    #[must_use]
    pub fn topic(&self) -> Topic {
        self.0.topic
    }

    /// Returns the partition of the message.
    #[must_use]
    pub fn partition(&self) -> Partition {
        self.0.partition
    }

    /// Returns the offset of the message.
    #[must_use]
    pub fn offset(&self) -> Offset {
        self.0.offset
    }

    /// Returns the message timestamp.
    #[must_use]
    pub fn timestamp(&self) -> &DateTime<Utc> {
        &self.0.timestamp
    }

    /// Returns a reference to the message's payload.
    #[must_use]
    pub fn payload(&self) -> &Payload {
        &self.0.payload
    }

    /// Returns a reference to the associated span for tracing.
    #[must_use]
    pub fn span(&self) -> &Span {
        &self.0.span
    }

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
            inner: self,
            uncommitted_offset,
        }
    }

    /// Converts a `ConsumerMessage` into a `ConsumerMessageValue`.
    #[must_use]
    pub fn into_value(self) -> ConsumerMessageValue {
        Arc::unwrap_or_clone(self.0)
    }
}

impl Keyed for ConsumerMessage {
    type Key = Key;

    /// Returns a reference to the message's key.
    fn key(&self) -> &Self::Key {
        &self.0.key
    }
}
