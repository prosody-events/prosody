//! Core message types and processing for Kafka consumers.
//!
//! This module defines the message types and contexts used for processing Kafka
//! messages:
//!
//! - `MessageContext` - Provides shutdown notification capabilities
//! - `UncommittedMessage` - A message with uncommitted offset tracking
//! - `ConsumerMessage` - A message container optimized for cloning
//! - `ConsumerMessageValue` - The raw message data and metadata
//!
//! The types work together to provide message lifecycle management, offset
//! tracking, and shutdown coordination.

use chrono::{DateTime, Utc};
use educe::Educe;
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
use crate::timers::{TimerManager, Trigger, UncommittedTimer};
use crate::{
    BorrowedEventId, EventId, EventIdentity, Key, Offset, Partition, Payload, SourceSystem, Topic,
};

/// The context for message processing within a consumer.
///
/// Provides shutdown notification capabilities to coordinate graceful shutdown.
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
    /// Creates a new message context.
    ///
    /// # Arguments
    ///
    /// * `shutdown_rx` - Receiver for shutdown signals
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

    /// Waits for a shutdown signal.
    ///
    /// Returns a future that completes when a partition shutdown signal is
    /// received.
    ///
    /// # Errors
    ///
    /// Logs an error if the shutdown hook fails.
    pub fn on_shutdown(&self) -> impl Future<Output = ()> + Send + use<T> {
        let mut shutdown_rx = self.shutdown_rx.clone();
        async move {
            if let Err(error) = shutdown_rx.wait_for(|is_shutdown| *is_shutdown).await {
                error!("shutdown hook failed: {error:#}");
            }
        }
    }

    pub async fn schedule(&self, time: CompactDateTime) -> Result<(), TimerManagerError<T::Error>> {
        self.timers
            .schedule(Trigger {
                key: self.key.clone(),
                time,
                span: info_span!("timer", key = %self.key, time = %time.to_string()),
            })
            .await
    }

    // pub async fn clear_and_schedule(
    //     &self,
    //     time: CompactDateTime,
    // ) -> Result<(), TimerManagerError<T::Error>> {
    //     let span = info_span!("timer", key = %self.key, time =
    // %time.to_string());
    //
    //     iter(self.timers.scheduled(&self.key).await?)
    //         .map(|trigger| {
    //             let cloned_span = span.clone();
    //
    //             async move {
    //                 cloned_span.follows_from(&trigger.span);
    //                 self.timers.unschedule(&trigger.key, trigger.time).await
    //             }
    //         })
    //         .buffer_unordered(DELETE_CONCURRENCY)
    //         .try_collect::<()>()
    //         .await?;
    //
    //     self.timers
    //         .schedule(Trigger {
    //             key: self.key.clone(),
    //             time,
    //             span,
    //         })
    //         .await
    // }

    pub async fn unschedule(
        &self,
        time: CompactDateTime,
    ) -> Result<(), TimerManagerError<T::Error>> {
        self.timers.unschedule(&self.key, time).await
    }

    pub async fn clear_scheduled(&self) -> Result<(), TimerManagerError<T::Error>> {
        self.timers.unschedule_all(&self.key).await
    }

    pub async fn scheduled(&self) -> Result<Vec<CompactDateTime>, TimerManagerError<T::Error>> {
        Ok(self
            .timers
            .scheduled(&self.key)
            .await?
            .into_iter()
            .collect())
    }

    /// Checks if a shutdown signal has been received.
    ///
    /// # Returns
    ///
    /// `true` if shutdown was signaled, `false` otherwise.
    #[must_use]
    pub fn should_shutdown(&self) -> bool {
        *self.shutdown_rx.borrow()
    }
}

/// A union type representing either a message or timer event that requires
/// acknowledgment.
///
/// [`UncommittedEvent`] provides a unified interface for handling both message
/// and timer events within the consumer processing pipeline. Both event types
/// follow the same transaction-like semantics where they must be explicitly
/// committed or aborted after processing.
///
/// ## Purpose
///
/// This enum enables:
/// - **Unified Processing**: Handle both message and timer events with the same
///   interface
/// - **Key-based Ordering**: Maintain ordering guarantees across both event
///   types
/// - **Transaction Semantics**: Consistent commit/abort behavior for all event
///   types
/// - **Type Safety**: Compile-time guarantees about event handling
///
/// ## Usage Pattern
///
/// Typically used in scenarios where messages and timers need to be processed
/// with the same ordering and reliability guarantees, such as in key-based
/// processing pipelines.
#[derive(Educe)]
#[educe(Debug(bound = ""))]
pub enum UncommittedEvent<T>
where
    T: TriggerStore,
{
    /// A message event that requires acknowledgment.
    ///
    /// Contains an [`UncommittedMessage`] that was received from a Kafka topic
    /// and must be committed or aborted after processing to ensure proper
    /// offset management and delivery guarantees.
    Message(UncommittedMessage),

    /// A timer event that requires acknowledgment.
    ///
    /// Contains an [`UncommittedTimer`] that fired at its scheduled time
    /// and must be committed or aborted after processing to ensure proper
    /// timer lifecycle management and cleanup.
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

/// A message with uncommitted offset tracking.
///
/// Wraps a `ConsumerMessage` and tracks its offset commitment state.
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

    /// Decomposes the message into its parts.
    ///
    /// # Returns
    ///
    /// A tuple containing the inner `ConsumerMessage` and `UncommittedOffset`.
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

/// A message that is cheap to clone and which does not track offset commits
#[derive(Clone, Debug)]
pub struct ConsumerMessage(Arc<ConsumerMessageValue>);

/// The raw message data and metadata.
#[derive(Clone, Educe)]
#[educe(Debug)]
pub struct ConsumerMessageValue {
    /// The system originating the message
    pub source_system: Option<SourceSystem>,

    /// The message's topic
    pub topic: Topic,

    /// The message's partition
    pub partition: Partition,

    /// The message's offset in its partition
    pub offset: Offset,

    /// The message's key
    pub key: Key,

    /// The message's timestamp
    pub timestamp: DateTime<Utc>,

    /// The message's payload
    #[educe(Debug(ignore))]
    pub payload: Payload,

    /// The message's tracing span
    #[educe(Debug(ignore))]
    pub span: Span,
}

impl ConsumerMessage {
    /// Creates a new consumer message.
    ///
    /// # Arguments
    ///
    /// * `source_system` - The system originating the message
    /// * `topic` - The message's topic
    /// * `partition` - The message's partition
    /// * `offset` - The message's offset
    /// * `key` - The message's key
    /// * `timestamp` - The message's timestamp
    /// * `payload` - The message's payload
    /// * `span` - The message's tracing span
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

    /// Converts this message to an uncommitted message.
    ///
    /// # Arguments
    ///
    /// * `uncommitted_offset` - The offset tracking state to attach
    ///
    /// # Returns
    ///
    /// A new `UncommittedMessage` containing this message and the offset state.
    #[must_use]
    pub fn into_uncommitted(self, uncommitted_offset: UncommittedOffset) -> UncommittedMessage {
        UncommittedMessage {
            inner: self,
            uncommitted_offset,
        }
    }

    /// Extracts the inner message value.
    ///
    /// # Returns
    ///
    /// The contained `ConsumerMessageValue`.
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
