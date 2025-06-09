//! Core message types and processing for Kafka consumers.
//!
//! This module defines the message types used for processing Kafka messages:
//!
//! - `UncommittedEvent` – A unified enum for message and timer events that
//!   require acknowledgment.
//! - `UncommittedMessage` – A message with uncommitted offset tracking.
//! - `ConsumerMessage` – A clonable, offset-agnostic message container.
//! - `ConsumerMessageValue` – The raw message data and metadata.

use chrono::{DateTime, Utc};
use educe::Educe;
use std::sync::Arc;
use tracing::{Span, debug};

use crate::consumer::partition::offsets::UncommittedOffset;
use crate::consumer::{Keyed, Uncommitted};
use crate::timers::PendingTimer;
use crate::timers::store::TriggerStore;
use crate::{
    BorrowedEventId, EventId, EventIdentity, Key, Offset, Partition, Payload, SourceSystem, Topic,
};

/// A unified event that must be explicitly committed or aborted.
///
/// This enum wraps either a Kafka message (`UncommittedMessage`) or a timer
/// event (`UncommittedTimer`) and provides a single interface for acknowledging
/// both types of events.
///
/// # Type Parameters
///
/// * `T` – The `TriggerStore` implementation for the timer variant.
#[derive(Educe)]
#[educe(Debug(bound = ""))]
pub enum UncommittedEvent<T>
where
    T: TriggerStore,
{
    /// A message event requiring offset commit/abort.
    Message(UncommittedMessage),

    /// A timer event requiring commit/abort.
    Timer(PendingTimer<T>),
}

impl<T> Keyed for UncommittedEvent<T>
where
    T: TriggerStore,
{
    type Key = Key;

    fn key(&self) -> &Self::Key {
        match self {
            Self::Message(message) => message.key(),
            Self::Timer(timer) => timer.key(),
        }
    }
}

impl<T> Uncommitted for UncommittedEvent<T>
where
    T: TriggerStore,
{
    async fn commit(self) {
        match self {
            UncommittedEvent::Message(message) => message.commit().await,
            UncommittedEvent::Timer(timer) => timer.commit().await,
        }
    }

    async fn abort(self) {
        match self {
            UncommittedEvent::Message(message) => message.abort().await,
            UncommittedEvent::Timer(timer) => timer.abort().await,
        }
    }
}

impl<T> From<UncommittedMessage> for UncommittedEvent<T>
where
    T: TriggerStore,
{
    fn from(value: UncommittedMessage) -> Self {
        Self::Message(value)
    }
}

impl<T> From<PendingTimer<T>> for UncommittedEvent<T>
where
    T: TriggerStore,
{
    fn from(value: PendingTimer<T>) -> Self {
        Self::Timer(value)
    }
}

/// A Kafka message with offset tracking for commits/aborts.
///
/// Wraps a `ConsumerMessage` and its `UncommittedOffset` handler.
#[derive(Educe)]
#[educe(Debug)]
pub struct UncommittedMessage {
    inner: ConsumerMessage,

    #[educe(Debug(ignore))]
    uncommitted_offset: UncommittedOffset,
}

impl UncommittedMessage {
    /// Returns the message's source system, if present.
    #[must_use]
    pub fn source_system(&self) -> Option<&SourceSystem> {
        self.inner.source_system()
    }

    /// Returns the message's topic.
    #[must_use]
    pub fn topic(&self) -> Topic {
        self.inner.topic()
    }

    /// Returns the message's partition.
    #[must_use]
    pub fn partition(&self) -> Partition {
        self.inner.partition()
    }

    /// Returns the message's offset.
    #[must_use]
    pub fn offset(&self) -> Offset {
        self.inner.offset()
    }

    /// Returns the message's timestamp.
    #[must_use]
    pub fn timestamp(&self) -> &DateTime<Utc> {
        self.inner.timestamp()
    }

    /// Returns the message's payload.
    #[must_use]
    pub fn payload(&self) -> &Payload {
        self.inner.payload()
    }

    /// Returns the message's tracing span.
    #[must_use]
    pub fn span(&self) -> &Span {
        self.inner.span()
    }

    /// Decomposes the message into its inner components.
    #[must_use]
    pub fn into_inner(self) -> (ConsumerMessage, UncommittedOffset) {
        (self.inner, self.uncommitted_offset)
    }
}

impl Uncommitted for UncommittedMessage {
    async fn commit(self) {
        debug!(
            topic = self.topic().as_ref(),
            partition = self.partition(),
            key = self.key().as_str(),
            offset = self.offset(),
            "committing message"
        );
        self.uncommitted_offset.commit();
    }

    async fn abort(self) {
        debug!(
            topic = self.topic().as_ref(),
            partition = self.partition(),
            key = self.key().as_str(),
            offset = self.offset(),
            "aborting message"
        );
        self.uncommitted_offset.abort();
    }
}

impl Keyed for UncommittedMessage {
    type Key = Key;

    fn key(&self) -> &Self::Key {
        self.inner.key()
    }
}

impl EventIdentity for UncommittedMessage {
    type BorrowedEventId = BorrowedEventId;
    type EventId = EventId;

    fn event_id(&self) -> Option<&Self::BorrowedEventId> {
        self.payload().event_id()
    }
}

/// A lightweight, clonable Kafka message without offset tracking.
///
/// Internally wraps its data in an `Arc<ConsumerMessageValue>`.
#[derive(Clone, Debug)]
pub struct ConsumerMessage(Arc<ConsumerMessageValue>);

/// The full data structure for a consumer message.
#[derive(Clone, Educe)]
#[educe(Debug)]
pub struct ConsumerMessageValue {
    /// The system that originated this message.
    pub source_system: Option<SourceSystem>,

    /// The Kafka topic name.
    pub topic: Topic,

    /// The partition index.
    pub partition: Partition,

    /// The offset within the partition.
    pub offset: Offset,

    /// The message key.
    pub key: Key,

    /// The timestamp when the broker sent the message.
    pub timestamp: DateTime<Utc>,

    /// The message payload.
    #[educe(Debug(ignore))]
    pub payload: Payload,

    /// Span for distributed tracing.
    #[educe(Debug(ignore))]
    pub span: Span,
}

impl ConsumerMessage {
    /// Creates a new consumer message.
    ///
    /// # Arguments
    ///
    /// * `source_system` – The system originating the message.
    /// * `topic` – The message's topic.
    /// * `partition` – The message's partition.
    /// * `offset` – The message's offset.
    /// * `key` – The message's key.
    /// * `timestamp` – The message's timestamp.
    /// * `payload` – The message's payload.
    /// * `span` – The message's tracing span.
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn new(
        source_system: Option<SourceSystem>,
        topic: Topic,
        partition: Partition,
        offset: Offset,
        key: Key,
        timestamp: DateTime<Utc>,
        payload: Payload,
        span: Span,
    ) -> Self {
        Self(Arc::new(ConsumerMessageValue {
            source_system,
            topic,
            partition,
            offset,
            key,
            timestamp,
            payload,
            span,
        }))
    }

    /// Returns the message's source system, if present.
    #[must_use]
    pub fn source_system(&self) -> Option<&SourceSystem> {
        self.0.source_system.as_ref()
    }

    /// Returns the message's topic.
    #[must_use]
    pub fn topic(&self) -> Topic {
        self.0.topic
    }

    /// Returns the message's partition.
    #[must_use]
    pub fn partition(&self) -> Partition {
        self.0.partition
    }

    /// Returns the message's offset.
    #[must_use]
    pub fn offset(&self) -> Offset {
        self.0.offset
    }

    /// Returns the message's timestamp.
    #[must_use]
    pub fn timestamp(&self) -> &DateTime<Utc> {
        &self.0.timestamp
    }

    /// Returns the message's payload.
    #[must_use]
    pub fn payload(&self) -> &Payload {
        &self.0.payload
    }

    /// Returns the message's tracing span.
    #[must_use]
    pub fn span(&self) -> &Span {
        &self.0.span
    }

    /// Converts this message into an `UncommittedMessage`.
    ///
    /// # Arguments
    ///
    /// * `uncommitted_offset` – The offset tracking state to attach.
    #[must_use]
    pub fn into_uncommitted(self, uncommitted_offset: UncommittedOffset) -> UncommittedMessage {
        UncommittedMessage {
            inner: self,
            uncommitted_offset,
        }
    }

    /// Extracts the inner `ConsumerMessageValue`, cloning if needed.
    #[must_use]
    pub fn into_value(self) -> ConsumerMessageValue {
        Arc::unwrap_or_clone(self.0)
    }
}

impl Keyed for ConsumerMessage {
    type Key = Key;

    fn key(&self) -> &Self::Key {
        &self.0.key
    }
}
