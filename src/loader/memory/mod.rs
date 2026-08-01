//! In-memory [`MessageLoader`] implementation, used for tests and the
//! mock-mode consumer path — wherever a
//! [`MessageLoader`] is needed without touching Kafka.
//!
//! [`MemoryLoader`] loads messages by exact offset coordinates. Unlike the
//! Kafka loader, it requires explicit message storage via
//! [`MemoryLoader::store_message`].

use super::{MessageLoader, PermitMode};
use crate::consumer::message::{ConsumerMessage, ConsumerMessageValue};
use crate::error::{ClassifyError, ErrorCategory};
use crate::otel::SpanRelation;
use crate::related_span;
use crate::{Key, Offset, Partition, Topic};
use ahash::HashMap;
use chrono::Utc;
use opentelemetry::Context;
use parking_lot::RwLock;
use std::future::Future;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::{Semaphore, TryAcquireError};
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt as _;

/// Type alias for the message storage map: each stored message keeps the
/// storer's trace context so reloads can relate their `load` span to it,
/// mirroring the Kafka loader's persisted-context reconstruction.
type MessageStorage<P> =
    HashMap<(Topic, Partition, Offset), (Arc<ConsumerMessageValue<P>>, Context)>;

/// In-memory message loader for testing.
///
/// Stores messages in a shared `HashMap` and loads them by exact offset
/// coordinates. Messages must be explicitly stored via
/// [`MemoryLoader::store_message`] before they can be loaded.
///
/// # Example
///
/// ```ignore
/// use serde_json::json;
///
/// let loader = MemoryLoader::new();
/// loader.store_message(topic, 0, 100, key, json!({"value": 42}));
///
/// let message = loader.load_message(topic, 0, 100).await?;
/// assert_eq!(message.offset(), 100);
/// ```
pub struct MemoryLoader<P> {
    messages: Arc<RwLock<MessageStorage<P>>>,
    semaphore: Arc<Semaphore>,
    message_spans: SpanRelation,
}

impl<P> Clone for MemoryLoader<P> {
    fn clone(&self) -> Self {
        Self {
            messages: self.messages.clone(),
            semaphore: self.semaphore.clone(),
            message_spans: self.message_spans,
        }
    }
}

impl<P: Send + Sync + 'static> MemoryLoader<P> {
    /// Creates a new in-memory loader with empty storage and the default
    /// `message_spans` relation; see [`Self::with_message_spans`].
    #[must_use]
    pub fn new() -> Self {
        Self::with_message_spans(SpanRelation::default())
    }

    /// Creates a new in-memory loader that relates reload `load` spans to the
    /// storing caller's context per `message_spans`, mirroring the Kafka
    /// loader's configured relation.
    #[must_use]
    pub fn with_message_spans(message_spans: SpanRelation) -> Self {
        Self::with_capacity_and_spans(1000, message_spans)
    }

    #[cfg(test)]
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self::with_capacity_and_spans(capacity, SpanRelation::default())
    }

    fn with_capacity_and_spans(capacity: usize, message_spans: SpanRelation) -> Self {
        Self {
            messages: Arc::new(RwLock::new(HashMap::default())),
            semaphore: Arc::new(Semaphore::new(capacity)),
            message_spans,
        }
    }

    /// Stores a message for later loading.
    ///
    /// Messages must be stored before they can be loaded via
    /// [`MessageLoader::load_message`].
    pub fn store_message(
        &self,
        topic: Topic,
        partition: Partition,
        offset: Offset,
        key: Key,
        payload: P,
    ) {
        let message_value = Arc::new(ConsumerMessageValue {
            source_system: None,
            topic,
            partition,
            offset,
            key,
            timestamp: Utc::now(),
            payload,
            // This loader serves messages it was handed, not Kafka records, so
            // there are no headers to read a request tag from.
            request: None,
        });
        let context = Span::current().context();
        self.messages
            .write()
            .insert((topic, partition, offset), (message_value, context));
    }

    /// Removes a message from storage.
    ///
    /// Used to simulate message deletion or compaction. Subsequent load
    /// attempts will return [`MemoryLoaderError::NotFound`].
    #[cfg(test)]
    pub fn remove_message(&self, topic: Topic, partition: Partition, offset: Offset) {
        self.messages.write().remove(&(topic, partition, offset));
    }

    /// Clears all stored messages.
    pub fn clear(&self) {
        self.messages.write().clear();
    }

    /// Returns the number of stored messages.
    #[must_use]
    pub fn len(&self) -> usize {
        self.messages.read().len()
    }

    /// Returns `true` if no messages are stored.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.messages.read().is_empty()
    }

    async fn load_message_impl(
        &self,
        topic: Topic,
        partition: Partition,
        offset: Offset,
        mode: PermitMode,
    ) -> Result<ConsumerMessage<P>, MemoryLoaderError> {
        let semaphore = self.semaphore.clone();
        let permit = match mode {
            PermitMode::Wait => semaphore
                .acquire_owned()
                .await
                .map_err(|_| MemoryLoaderError::LoaderShutdown)?,
            PermitMode::Available => {
                semaphore.try_acquire_owned().map_err(|error| match error {
                    TryAcquireError::Closed => MemoryLoaderError::LoaderShutdown,
                    TryAcquireError::NoPermits => MemoryLoaderError::CapacityExhausted,
                })?
            }
        };
        // Look up the message
        let messages = self.messages.read();
        let (message_value, context) = messages
            .get(&(topic, partition, offset))
            .ok_or(MemoryLoaderError::NotFound(topic, partition, offset))?
            .clone();
        drop(messages);

        // Reload span related to the storer's context, the memory twin of
        // the Kafka loader's create_load_span. `cached` is hardcoded `false`
        // (this loader has no cache) so the two twins export identical
        // attribute sets.
        let span = related_span!(
            self.message_spans,
            context,
            "load",
            partition = partition,
            offset = offset,
            topic = %topic,
            key = %message_value.key,
            cached = false,
        );
        Ok(ConsumerMessage::from_decoded(message_value, span, permit))
    }
}

impl<P: Send + Sync + 'static> Default for MemoryLoader<P> {
    fn default() -> Self {
        Self::new()
    }
}

impl<P: Clone + Send + Sync + 'static> MessageLoader for MemoryLoader<P> {
    type Error = MemoryLoaderError;
    type Payload = P;

    fn load_message(
        &self,
        topic: Topic,
        partition: Partition,
        offset: Offset,
    ) -> impl Future<Output = Result<ConsumerMessage<Self::Payload>, Self::Error>> + Send {
        self.load_message_impl(topic, partition, offset, PermitMode::Wait)
    }

    fn try_load_message(
        &self,
        topic: Topic,
        partition: Partition,
        offset: Offset,
    ) -> impl Future<Output = Result<ConsumerMessage<Self::Payload>, Self::Error>> + Send {
        self.load_message_impl(topic, partition, offset, PermitMode::Available)
    }
}

/// Errors that can occur during in-memory message loading.
#[derive(Clone, Debug, Error)]
pub enum MemoryLoaderError {
    /// The requested message was not found in storage.
    #[error("Message {0}/{1}:{2} not found in memory storage")]
    NotFound(Topic, Partition, Offset),

    /// The loader has been shut down and cannot process requests.
    #[error("Loader has shut down")]
    LoaderShutdown,

    /// Every loader permit is held.
    #[error("Loader capacity is exhausted")]
    CapacityExhausted,
}

impl ClassifyError for MemoryLoaderError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            // Terminal - loader cannot operate
            Self::LoaderShutdown => ErrorCategory::Terminal,

            Self::CapacityExhausted => ErrorCategory::Transient,

            // Permanent - message doesn't exist
            Self::NotFound(..) => ErrorCategory::Permanent,
        }
    }
}

#[cfg(test)]
mod tests;
