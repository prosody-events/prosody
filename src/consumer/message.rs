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
use crate::{EventIdentity, Key, Offset, Partition, ProcessScope, SourceSystem, Topic};

/// A unified event that must be explicitly committed or aborted.
///
/// This enum wraps either a Kafka message (`UncommittedMessage`) or a timer
/// event (`PendingTimer<T>`) and provides a single interface for acknowledging
/// both types of events.
///
/// # Type Parameters
///
/// * `T` – The `TriggerStore` implementation for the timer variant.
/// * `P` – The payload type carried by the message variant.
#[derive(Educe)]
#[educe(Debug(bound = ""))]
pub enum UncommittedEvent<T, P>
where
    T: TriggerStore,
{
    /// A message event requiring offset commit or abort.
    Message(UncommittedMessage<P>),

    /// A timer event requiring commit or abort.
    Timer(PendingTimer<T>),
}

impl<T, P> Keyed for UncommittedEvent<T, P>
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

impl<T, P> From<UncommittedMessage<P>> for UncommittedEvent<T, P>
where
    T: TriggerStore,
{
    fn from(value: UncommittedMessage<P>) -> Self {
        Self::Message(value)
    }
}

impl<T, P> From<PendingTimer<T>> for UncommittedEvent<T, P>
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
#[educe(Debug(bound = ""))]
pub struct UncommittedMessage<P> {
    inner: ConsumerMessage<P>,

    #[educe(Debug(ignore))]
    uncommitted_offset: UncommittedOffset,
}

impl<P> UncommittedMessage<P> {
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

    /// Returns a reference to the deserialized payload.
    #[must_use]
    pub fn payload(&self) -> &P {
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
    /// A tuple `(ConsumerMessage<P>, UncommittedOffset)`.
    #[must_use]
    pub fn into_inner(self) -> (ConsumerMessage<P>, UncommittedOffset) {
        (self.inner, self.uncommitted_offset)
    }

    fn processing_state(&self) -> Arc<ArcSwapOption<ProcessingState>> {
        self.inner.processing_state.clone()
    }
}

impl<P: Send + Sync + 'static> Uncommitted for UncommittedMessage<P> {
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

impl<P> Keyed for UncommittedMessage<P> {
    type Key = Key;

    fn key(&self) -> &Self::Key {
        self.inner.key()
    }
}

impl<P: EventIdentity> EventIdentity for UncommittedMessage<P> {
    fn event_id(&self) -> Option<&str> {
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

impl<P: Send + 'static> ProcessScope for UncommittedMessage<P> {
    type Guard = MessageProcessGuard;

    fn process_scope(&self) -> Self::Guard {
        MessageProcessGuard(self.processing_state())
    }
}

/// A lightweight, clonable Kafka message without offset tracking.
///
/// Internally wraps its data in an `Arc<ConsumerMessageValue<P>>` with shared
/// processing state across clones.
#[derive(Educe)]
#[educe(Clone, Debug(bound = ""))]
pub struct ConsumerMessage<P> {
    value: Arc<ConsumerMessageValue<P>>,
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
#[educe(Debug(bound = ""))]
pub struct ConsumerMessageValue<P> {
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

    /// Deserialized message payload.
    #[educe(Debug(ignore))]
    pub payload: P,
}

#[cfg(test)]
impl Default for ConsumerMessageValue<serde_json::Value> {
    fn default() -> Self {
        Self {
            source_system: None,
            topic: "test-topic".into(),
            partition: 0,
            offset: 0,
            key: "test-key".into(),
            timestamp: chrono::Utc::now(),
            payload: serde_json::json!({}),
        }
    }
}

impl<P> ConsumerMessage<P> {
    /// Create a new `ConsumerMessage` from a message value and processing state
    /// components.
    ///
    /// # Arguments
    ///
    /// * `value` – The message data (topic, partition, offset, key, timestamp,
    ///   payload, etc.).
    /// * `span` – Tracing span for distributed context.
    /// * `permit` – Semaphore permit for backpressure management.
    #[must_use]
    pub fn new(value: ConsumerMessageValue<P>, span: Span, permit: OwnedSemaphorePermit) -> Self {
        let value = Arc::new(value);
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
        value: Arc<ConsumerMessageValue<P>>,
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

    /// Returns the deserialized payload.
    #[must_use]
    pub fn payload(&self) -> &P {
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
    pub fn into_uncommitted(self, uncommitted_offset: UncommittedOffset) -> UncommittedMessage<P> {
        UncommittedMessage {
            inner: self,
            uncommitted_offset,
        }
    }

    /// Create a test message with minimal dependencies.
    ///
    /// Creates a `ConsumerMessage` suitable for unit testing without requiring
    /// complex setup. Each message gets its own semaphore permit.
    ///
    /// # Arguments
    ///
    /// * `topic` - Kafka topic
    /// * `partition` - Partition index
    /// * `offset` - Message offset
    /// * `key` - Message key
    /// * `payload` - Message payload
    ///
    /// # Errors
    ///
    /// Returns an error if semaphore permit acquisition fails (should never
    /// happen since the semaphore is created with 10 permits and we acquire
    /// 1).
    #[cfg(test)]
    pub fn for_testing(
        topic: Topic,
        partition: Partition,
        offset: Offset,
        key: Key,
        payload: P,
    ) -> color_eyre::Result<Self> {
        use color_eyre::eyre::eyre;
        use tokio::sync::Semaphore;

        let semaphore = Arc::new(Semaphore::new(10));
        let permit = semaphore
            .try_acquire_owned()
            .map_err(|e| eyre!("Semaphore should always have capacity: {e}"))?;

        Ok(Self::new(
            ConsumerMessageValue {
                source_system: None,
                topic,
                partition,
                offset,
                key,
                timestamp: chrono::Utc::now(),
                payload,
            },
            tracing::Span::current(),
            permit,
        ))
    }
}

impl<P> Keyed for ConsumerMessage<P> {
    type Key = Key;

    fn key(&self) -> &Self::Key {
        &self.value.key
    }
}
