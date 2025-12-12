//! Provider trait for creating partition-specific defer stores.

use super::DeferStore;
use crate::consumer::middleware::ClassifyError;
use crate::{Partition, Topic};
use std::error::Error;
use std::future::Future;

/// Factory for creating partition-specific [`DeferStore`] instances.
///
/// The provider holds shared resources (Cassandra session, prepared queries)
/// and creates store instances with the correct segment context.
///
/// # Usage
///
/// ```text
/// // Create provider once at application startup
/// let provider = CassandraDeferStoreProvider::new(session, keyspace).await?;
///
/// // Create store for each partition as needed
/// let store = provider.create_store(topic, partition, &consumer_group).await?;
/// ```
///
/// # Implementations
///
/// - `CassandraDeferStoreProvider`: Creates `CassandraDeferStore` instances
/// - `MemoryDeferStoreProvider`: Creates `MemoryDeferStore` instances (for
///   testing)
pub trait DeferStoreProvider: Clone + Send + Sync + 'static {
    /// The store type created by this provider.
    ///
    /// The store's error type must match this provider's error type to allow
    /// unified error handling in the lazy store wrapper.
    type Store: DeferStore<Error = Self::Error>;

    /// Error type for store creation and store operations.
    ///
    /// This unified error type is used for both provider creation errors
    /// and errors from the created store instances.
    type Error: Error + ClassifyError + Send + Sync + 'static;

    /// Creates a store for the specified partition.
    ///
    /// # Arguments
    ///
    /// * `topic` - Kafka topic
    /// * `partition` - Kafka partition
    /// * `consumer_group` - Consumer group ID
    ///
    /// # Behavior
    ///
    /// 1. Computes segment ID as `UUIDv5` of
    ///    `"{topic}/{partition}:{consumer_group}"`
    /// 2. Inserts segment metadata row (idempotent)
    /// 3. Returns store instance with segment context
    ///
    /// # Errors
    ///
    /// Returns error if segment metadata insertion fails (Cassandra) or other
    /// initialization failure.
    fn create_store(
        &self,
        topic: Topic,
        partition: Partition,
        consumer_group: &str,
    ) -> impl Future<Output = Result<Self::Store, Self::Error>> + Send;
}
