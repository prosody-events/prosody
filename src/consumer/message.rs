//! Core message types and processing for Kafka consumers.
//!
//! This module defines the message types and contexts used for processing Kafka
//! messages:
//!
//! - `EventContext` – Provides shutdown notification capabilities and timer
//!   scheduling operations.
//! - `UncommittedEvent` – A unified enum for message and timer events that
//!   require acknowledgment.
//! - `UncommittedMessage` – A message with uncommitted offset tracking.
//! - `ConsumerMessage` – A clonable, offset-agnostic message container.
//! - `ConsumerMessageValue` – The raw message data and metadata.

use chrono::{DateTime, Utc};
use educe::Educe;
use futures::stream::iter;
use futures::{StreamExt, TryStreamExt};
use std::fmt::Debug;
use std::future::Future;
use std::sync::Arc;
use tokio::sync::watch;
use tracing::{Span, debug, error, info_span};

use crate::consumer::partition::offsets::UncommittedOffset;
use crate::consumer::{Keyed, Uncommitted};
use crate::timers::datetime::CompactDateTime;
use crate::timers::error::TimerManagerError;
use crate::timers::store::TriggerStore;
use crate::timers::{DELETE_CONCURRENCY, TimerManager, Trigger, UncommittedTimer};
use crate::{
    BorrowedEventId, EventId, EventIdentity, Key, Offset, Partition, Payload, SourceSystem, Topic,
};

/// The context for message processing within a consumer.
///
/// Provides the current message key, shutdown notification, and a handle to
/// the `TimerManager` for scheduling and managing timers.
///
/// # Type Parameters
///
/// * `T` – The `TriggerStore` implementation used by the timer manager.
#[derive(Educe)]
#[educe(Debug, Clone(bound()))]
pub struct EventContext<T> {
    key: Key,

    #[educe(Debug(ignore))]
    shutdown_rx: watch::Receiver<bool>,

    #[educe(Debug(ignore))]
    timers: TimerManager<T>,
}

impl<T> EventContext<T>
where
    T: TriggerStore,
{
    /// Create a new `EventContext`.
    ///
    /// # Arguments
    ///
    /// * `key` – The message key for partition affinity.
    /// * `shutdown_rx` – A watch receiver that signals shutdown when `true`.
    /// * `timers` – The `TimerManager` instance for scheduling timers.
    pub(crate) fn new(
        key: Key,
        shutdown_rx: watch::Receiver<bool>,
        timers: TimerManager<T>,
    ) -> Self {
        Self {
            key,
            shutdown_rx,
            timers,
        }
    }

    /// Returns a future that completes when a shutdown signal is received.
    ///
    /// # Panics
    ///
    /// This method logs an error if waiting on the shutdown channel fails,
    /// but does not panic.
    pub fn on_shutdown(&self) -> impl Future<Output = ()> + Send {
        let mut shutdown_rx = self.shutdown_rx.clone();
        async move {
            if let Err(error) = shutdown_rx.wait_for(|is_shutdown| *is_shutdown).await {
                error!("shutdown hook failed: {error:#}");
            }
        }
    }

    /// Schedule a timer for the current key.
    ///
    /// # Arguments
    ///
    /// * `time` – The execution time for the timer.
    ///
    /// # Errors
    ///
    /// Returns a `TimerManagerError` if the scheduler or store operation fails.
    pub async fn schedule(&self, time: CompactDateTime) -> Result<(), TimerManagerError<T::Error>> {
        let span = info_span!("timer", key = %self.key, time = %time);
        self.timers
            .schedule(Trigger {
                key: self.key.clone(),
                time,
                span,
            })
            .await
    }

    /// Remove all existing timers for the key, then schedule a new one.
    ///
    /// This first unschedules any existing timers for the key in parallel,
    /// then adds the new timer.
    ///
    /// # Arguments
    ///
    /// * `time` – The new execution time for the timer.
    ///
    /// # Errors
    ///
    /// Returns a `TimerManagerError` if any unschedule or schedule operation
    /// fails.
    pub async fn clear_and_schedule(
        &self,
        time: CompactDateTime,
    ) -> Result<(), TimerManagerError<T::Error>> {
        let span = info_span!("timer", key = %self.key, time = %time);

        // Unschedule all existing triggers for this key
        iter(self.timers.scheduled_triggers(&self.key).await?)
            .map(|trigger| {
                let span_clone = span.clone();
                async move {
                    // Link tracing spans
                    span_clone.follows_from(&trigger.span);
                    self.timers.unschedule(&trigger.key, trigger.time).await
                }
            })
            .buffer_unordered(DELETE_CONCURRENCY)
            .try_collect::<()>()
            .await?;

        // Schedule the new trigger
        self.timers
            .schedule(Trigger {
                key: self.key.clone(),
                time,
                span,
            })
            .await
    }

    /// Unschedule a specific timer for the current key.
    ///
    /// # Arguments
    ///
    /// * `time` – The execution time of the timer to remove.
    ///
    /// # Errors
    ///
    /// Returns a `TimerManagerError` if the unschedule operation fails.
    pub async fn unschedule(
        &self,
        time: CompactDateTime,
    ) -> Result<(), TimerManagerError<T::Error>> {
        self.timers.unschedule(&self.key, time).await
    }

    /// Unschedule all timers for the current key.
    ///
    /// # Errors
    ///
    /// Returns a `TimerManagerError` if any unschedule operation fails.
    pub async fn clear_scheduled(&self) -> Result<(), TimerManagerError<T::Error>> {
        self.timers.unschedule_all(&self.key).await
    }

    /// Retrieve all scheduled times for the current key.
    ///
    /// # Errors
    ///
    /// Returns a `TimerManagerError` if the retrieval from storage fails.
    pub async fn scheduled(&self) -> Result<Vec<CompactDateTime>, TimerManagerError<T::Error>> {
        Ok(self
            .timers
            .scheduled_times(&self.key)
            .await?
            .into_iter()
            .collect())
    }

    /// Return `true` if a shutdown signal has been triggered.
    #[must_use]
    pub fn should_shutdown(&self) -> bool {
        *self.shutdown_rx.borrow()
    }
}

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
    Timer(UncommittedTimer<T>),
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

impl<T> From<UncommittedTimer<T>> for UncommittedEvent<T>
where
    T: TriggerStore,
{
    fn from(value: UncommittedTimer<T>) -> Self {
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
