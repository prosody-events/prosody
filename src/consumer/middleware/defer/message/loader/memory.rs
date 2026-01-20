//! In-memory message loader for testing defer middleware.
//!
//! Provides a [`MemoryLoader`] that stores messages in memory and loads them by
//! offset coordinates. Unlike the Kafka loader, this loader requires explicit
//! message storage via [`MemoryLoader::store_message`].

use super::MessageLoader;
use crate::consumer::message::{ConsumerMessage, ConsumerMessageValue};
use crate::error::{ClassifyError, ErrorCategory};
use crate::{Key, Offset, Partition, Topic};
use ahash::HashMap;
use chrono::Utc;
use parking_lot::RwLock;
use serde_json::Value;
use std::future::Future;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::Semaphore;
use tracing::Span;

/// Type alias for the message storage map.
type MessageStorage = HashMap<(Topic, Partition, Offset), Arc<ConsumerMessageValue>>;

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
#[derive(Clone)]
pub struct MemoryLoader {
    messages: Arc<RwLock<MessageStorage>>,
    semaphore: Arc<Semaphore>,
}

impl MemoryLoader {
    /// Creates a new in-memory loader with empty storage.
    #[must_use]
    pub fn new() -> Self {
        Self {
            messages: Arc::new(RwLock::new(HashMap::default())),
            // Large semaphore since we don't care about backpressure in tests
            semaphore: Arc::new(Semaphore::new(1000)),
        }
    }

    /// Stores a message for later loading.
    ///
    /// Messages must be stored before they can be loaded via
    /// [`MessageLoader::load_message`].
    ///
    /// # Arguments
    ///
    /// * `topic` - The topic of the message
    /// * `partition` - The partition of the message
    /// * `offset` - The offset of the message
    /// * `key` - The message key
    /// * `payload` - The message payload as JSON
    pub fn store_message(
        &self,
        topic: Topic,
        partition: Partition,
        offset: Offset,
        key: Key,
        payload: Value,
    ) {
        let message_value = Arc::new(ConsumerMessageValue {
            source_system: None,
            topic,
            partition,
            offset,
            key,
            timestamp: Utc::now(),
            payload,
        });
        self.messages
            .write()
            .insert((topic, partition, offset), message_value);
    }

    /// Removes a message from storage.
    ///
    /// Used to simulate message deletion or compaction. Subsequent load
    /// attempts will return [`MemoryLoaderError::NotFound`].
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

    /// Implementation of message loading.
    async fn load_message_impl(
        &self,
        topic: Topic,
        partition: Partition,
        offset: Offset,
    ) -> Result<ConsumerMessage, MemoryLoaderError> {
        // Acquire a dummy permit to construct ConsumerMessage
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| MemoryLoaderError::LoaderShutdown)?;

        // Look up the message
        let messages = self.messages.read();
        let message_value = messages
            .get(&(topic, partition, offset))
            .ok_or(MemoryLoaderError::NotFound(topic, partition, offset))?
            .clone();
        drop(messages);

        // Create a ConsumerMessage with the loaded message value
        // Note: Uses current span since we don't have original span context
        Ok(ConsumerMessage::from_decoded(
            message_value,
            Span::current(),
            permit,
        ))
    }
}

impl Default for MemoryLoader {
    fn default() -> Self {
        Self::new()
    }
}

impl MessageLoader for MemoryLoader {
    type Error = MemoryLoaderError;

    fn load_message(
        &self,
        topic: Topic,
        partition: Partition,
        offset: Offset,
    ) -> impl Future<Output = Result<ConsumerMessage, Self::Error>> + Send {
        self.load_message_impl(topic, partition, offset)
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
}

impl ClassifyError for MemoryLoaderError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            // Terminal - loader cannot operate
            Self::LoaderShutdown => ErrorCategory::Terminal,

            // Permanent - message doesn't exist
            Self::NotFound(..) => ErrorCategory::Permanent,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consumer::Keyed;
    use color_eyre::Result;
    use serde_json::json;

    #[tokio::test]
    async fn test_store_and_load() -> Result<()> {
        let loader = MemoryLoader::new();
        let topic = Topic::from("test-topic");
        let key = Key::from("test-key");
        let payload = json!({"key": "value", "num": 42_i32});

        loader.store_message(topic, 0_i32, 100_i64, key.clone(), payload.clone());

        let message = loader.load_message(topic, 0_i32, 100_i64).await?;
        assert_eq!(message.offset(), 100_i64);
        assert_eq!(message.partition(), 0_i32);
        assert_eq!(message.key(), &key);
        assert_eq!(message.payload(), &payload);
        Ok(())
    }

    #[tokio::test]
    async fn test_not_found() {
        let loader = MemoryLoader::new();
        let topic = Topic::from("test-topic");

        let result = loader.load_message(topic, 0, 100).await;
        assert!(matches!(result, Err(MemoryLoaderError::NotFound(..))));
    }

    #[tokio::test]
    async fn test_remove_message() {
        let loader = MemoryLoader::new();
        let topic = Topic::from("test-topic");
        let key = Key::from("test-key");
        let payload = json!({"key": "value"});

        loader.store_message(topic, 0, 100, key, payload);
        assert_eq!(loader.len(), 1);

        loader.remove_message(topic, 0, 100);
        assert_eq!(loader.len(), 0);

        let result = loader.load_message(topic, 0, 100).await;
        assert!(matches!(result, Err(MemoryLoaderError::NotFound(..))));
    }

    #[tokio::test]
    async fn test_clear() {
        let loader = MemoryLoader::new();
        let topic = Topic::from("test-topic");
        let key = Key::from("test-key");

        loader.store_message(topic, 0_i32, 100_i64, key.clone(), json!({"a": 1_i32}));
        loader.store_message(topic, 0_i32, 101_i64, key, json!({"b": 2_i32}));
        assert_eq!(loader.len(), 2_usize);

        loader.clear();
        assert_eq!(loader.len(), 0_usize);
        assert!(loader.is_empty());
    }

    #[tokio::test]
    async fn test_clone_shares_storage() -> Result<()> {
        let loader1 = MemoryLoader::new();
        let topic = Topic::from("test-topic");
        let key = Key::from("test-key");
        let payload = json!({"shared": true});

        loader1.store_message(topic, 0_i32, 100_i64, key, payload.clone());

        let loader2 = loader1.clone();
        let message = loader2.load_message(topic, 0_i32, 100_i64).await?;
        assert_eq!(message.payload(), &payload);
        Ok(())
    }
}
