//! Core message types and processing for Kafka consumers.
//!
//! This module defines the message types used for processing Kafka messages and
//! timer events. It provides abstractions for acknowledging (commit or abort)
//! both message and timer deliveries.
//!
//! - `UncommittedEvent` – Unified enum for message and timer events requiring
//!   acknowledgment.
//! - `UncommittedMessage` – A Kafka message paired with offset-tracking state.
//! - `ConsumerMessage` – A clonable, offset-agnostic container for message
//!   data.
//! - `ConsumerMessageValue` – The raw data behind `ConsumerMessage`.

use arc_swap::ArcSwapOption;
use chrono::{DateTime, Utc};
use educe::Educe;
use std::sync::Arc;
use tokio::sync::OwnedSemaphorePermit;
use tracing::{Span, debug};

use crate::consumer::partition::offsets::UncommittedOffset;
use crate::consumer::{Keyed, Uncommitted};
use crate::timers::PendingTimer;
use crate::timers::store::TriggerStore;
use crate::{
    BorrowedEventId, EventId, EventIdentity, Key, Offset, Partition, Payload, ProcessScope,
    SourceSystem, Topic,
};

/// A unified event that must be explicitly committed or aborted.
///
/// This enum wraps either a Kafka message (`UncommittedMessage`) or a timer
/// event (`PendingTimer<T>`) and provides a single interface for acknowledging
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
    /// A message event requiring offset commit or abort.
    Message(UncommittedMessage),

    /// A timer event requiring commit or abort.
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
    /// Commit the underlying message or timer.
    async fn commit(self) {
        match self {
            UncommittedEvent::Message(message) => message.commit().await,
            UncommittedEvent::Timer(timer) => timer.commit().await,
        }
    }

    /// Abort the underlying message or timer.
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

/// A Kafka message with offset tracking for commit/abort semantics.
///
/// Wraps a `ConsumerMessage` and its corresponding `UncommittedOffset` handler.
#[derive(Educe)]
#[educe(Debug)]
pub struct UncommittedMessage {
    inner: ConsumerMessage,

    #[educe(Debug(ignore))]
    uncommitted_offset: UncommittedOffset,
}

impl UncommittedMessage {
    /// Returns the optional source system identifier from message headers.
    ///
    /// # Returns
    ///
    /// An `Option` containing the source system if present.
    #[must_use]
    pub fn source_system(&self) -> Option<&SourceSystem> {
        self.inner.source_system()
    }

    /// Returns the message's Kafka topic.
    #[must_use]
    pub fn topic(&self) -> Topic {
        self.inner.topic()
    }

    /// Returns the message's partition index.
    #[must_use]
    pub fn partition(&self) -> Partition {
        self.inner.partition()
    }

    /// Returns the message's offset within the partition.
    #[must_use]
    pub fn offset(&self) -> Offset {
        self.inner.offset()
    }

    /// Returns the message's broker timestamp.
    #[must_use]
    pub fn timestamp(&self) -> &DateTime<Utc> {
        self.inner.timestamp()
    }

    /// Returns a reference to the deserialized JSON payload.
    #[must_use]
    pub fn payload(&self) -> &Payload {
        self.inner.payload()
    }

    /// Returns the tracing span associated with this message.
    ///
    /// Returns `Span::none()` if processing resources have been released.
    #[must_use]
    pub fn span(&self) -> Span {
        self.inner.span()
    }

    /// Decomposes into its inner `ConsumerMessage` and the offset-tracking
    /// guard.
    ///
    /// # Returns
    ///
    /// A tuple `(ConsumerMessage, UncommittedOffset)`.
    #[must_use]
    pub fn into_inner(self) -> (ConsumerMessage, UncommittedOffset) {
        (self.inner, self.uncommitted_offset)
    }

    fn processing_state(&self) -> Arc<ArcSwapOption<ProcessingState>> {
        self.inner.processing_state.clone()
    }
}

impl Uncommitted for UncommittedMessage {
    /// Commit the message offset to Kafka and log the action.
    async fn commit(self) {
        debug!(
            topic = self.topic().as_ref(),
            partition = self.partition(),
            key = self.key().as_ref(),
            offset = self.offset(),
            "committing message"
        );
        self.uncommitted_offset.commit();
    }

    /// Abort the message processing and log the action.
    async fn abort(self) {
        debug!(
            topic = self.topic().as_ref(),
            partition = self.partition(),
            key = self.key().as_ref(),
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

/// RAII guard that releases processing resources (spans and permits) on drop.
///
/// Ensures deterministic cleanup when message processing completes, rather than
/// waiting for unpredictable garbage collection timing.
pub struct MessageProcessGuard(Arc<ArcSwapOption<ProcessingState>>);

impl Drop for MessageProcessGuard {
    fn drop(&mut self) {
        self.0.store(None);
    }
}

impl ProcessScope for UncommittedMessage {
    type Guard = MessageProcessGuard;

    fn process_scope(&self) -> Self::Guard {
        MessageProcessGuard(self.processing_state())
    }
}

/// A lightweight, clonable Kafka message without offset tracking.
///
/// Internally wraps its data in an `Arc<ConsumerMessageValue>` with shared
/// processing state across clones.
#[derive(Clone, Debug)]
pub struct ConsumerMessage {
    value: Arc<ConsumerMessageValue>,
    processing_state: Arc<ArcSwapOption<ProcessingState>>,
}

#[derive(Educe)]
#[educe(Debug)]
struct ProcessingState {
    /// Tracing span for this message.
    #[educe(Debug(ignore))]
    span: Span,

    /// Permit used to bound buffering
    permit: OwnedSemaphorePermit,
}

/// The full data and metadata for a consumer message.
///
/// Owned by `ConsumerMessage` and shared via `Arc`.
#[derive(Educe)]
#[educe(Debug)]
pub struct ConsumerMessageValue {
    /// Optional header indicating the source system that produced the message.
    pub source_system: Option<SourceSystem>,

    /// Name of the Kafka topic.
    pub topic: Topic,

    /// Index of the partition.
    pub partition: Partition,

    /// Offset of the message in the partition.
    pub offset: Offset,

    /// Message key, used for routing and deduplication.
    pub key: Key,

    /// Broker timestamp when the message was produced.
    pub timestamp: DateTime<Utc>,

    /// JSON payload of the message.
    #[educe(Debug(ignore))]
    pub payload: Payload,
}

impl ConsumerMessage {
    /// Create a new `ConsumerMessage` from raw components.
    ///
    /// # Arguments
    ///
    /// * `source_system` – Optional source system identifier.
    /// * `topic` – Kafka topic name.
    /// * `partition` – Partition index.
    /// * `offset` – Offset within the partition.
    /// * `key` – Message key.
    /// * `timestamp` – Broker timestamp.
    /// * `payload` – Message payload as JSON.
    /// * `span` – Tracing span for distributed context.
    /// * `permit` – Semaphore permit for backpressure management.
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
        permit: OwnedSemaphorePermit,
    ) -> Self {
        let value = Arc::new(ConsumerMessageValue {
            source_system,
            topic,
            partition,
            offset,
            key,
            timestamp,
            payload,
        });

        let processing_state = ArcSwapOption::from_pointee(ProcessingState { span, permit }).into();

        Self {
            value,
            processing_state,
        }
    }

    /// Create a `ConsumerMessage` from a decoded message with processing state.
    ///
    /// This is used after `decode_message` returns a `DecodedMessage` to
    /// attach the semaphore permit and span for the processing phase.
    ///
    /// # Arguments
    ///
    /// * `value` - Shared immutable message data
    /// * `span` - Tracing span for this processing phase
    /// * `permit` - Semaphore permit for backpressure management
    #[must_use]
    pub fn from_decoded(
        value: Arc<ConsumerMessageValue>,
        span: Span,
        permit: OwnedSemaphorePermit,
    ) -> Self {
        let processing_state = ArcSwapOption::from_pointee(ProcessingState { span, permit }).into();

        Self {
            value,
            processing_state,
        }
    }

    /// Returns the optional source system identifier.
    #[must_use]
    pub fn source_system(&self) -> Option<&SourceSystem> {
        self.value.source_system.as_ref()
    }

    /// Returns the message's topic.
    #[must_use]
    pub fn topic(&self) -> Topic {
        self.value.topic
    }

    /// Returns the message's partition.
    #[must_use]
    pub fn partition(&self) -> Partition {
        self.value.partition
    }

    /// Returns the message's offset.
    #[must_use]
    pub fn offset(&self) -> Offset {
        self.value.offset
    }

    /// Returns the message's timestamp.
    #[must_use]
    pub fn timestamp(&self) -> &DateTime<Utc> {
        &self.value.timestamp
    }

    /// Returns the JSON payload.
    #[must_use]
    pub fn payload(&self) -> &Payload {
        &self.value.payload
    }

    /// Returns the tracing span associated with this message.
    ///
    /// Returns `Span::none()` if processing resources have been released.
    #[must_use]
    pub fn span(&self) -> Span {
        let state = self.processing_state.load();
        state
            .as_deref()
            .map_or_else(Span::none, |state| state.span.clone())
    }

    /// Convert into `UncommittedMessage` by attaching offset-tracking state.
    ///
    /// # Arguments
    ///
    /// * `uncommitted_offset` – The offset guard to manage commit/abort.
    #[must_use]
    pub fn into_uncommitted(self, uncommitted_offset: UncommittedOffset) -> UncommittedMessage {
        UncommittedMessage {
            inner: self,
            uncommitted_offset,
        }
    }
}

impl Keyed for ConsumerMessage {
    type Key = Key;

    fn key(&self) -> &Self::Key {
        &self.value.key
    }
}
