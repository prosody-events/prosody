//! Provider trait for creating partition-specific message defer stores.

use super::MessageDeferStore;
use crate::{Partition, Topic};

/// Factory for creating partition-specific [`MessageDeferStore`] instances.
///
/// The provider holds shared resources (Cassandra session, prepared queries)
/// and creates store instances with the correct segment context.
///
/// # Design
///
/// Store creation is **synchronous**. For Cassandra stores, the actual I/O
/// (segment persistence) is deferred to first use via [`LazySegment`].
///
/// [`LazySegment`]: crate::consumer::middleware::defer::segment::LazySegment
///
/// # Usage
///
/// ```text
/// // Create provider once at startup
/// let provider = CassandraMessageDeferStoreProvider::new(store, queries, segment_store);
///
/// // Create stores for each partition (sync, no I/O)
/// let store = provider.create_store(topic, partition, &consumer_group);
/// ```
///
/// # Implementations
///
/// - [`CassandraMessageDeferStoreProvider`](super::CassandraMessageDeferStoreProvider):
///   Creates `CassandraMessageDeferStore` instances
/// - [`MemoryMessageDeferStoreProvider`](super::MemoryMessageDeferStoreProvider):
///   Creates `MemoryMessageDeferStore` instances (for testing)
pub trait MessageDeferStoreProvider: Clone + Send + Sync + 'static {
    /// The store type created by this provider.
    type Store: MessageDeferStore;

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
    /// This method is **synchronous**. For Cassandra stores, the store
    /// internally uses [`LazySegment`] to defer segment persistence to
    /// first use.
    ///
    /// [`LazySegment`]: crate::consumer::middleware::defer::segment::LazySegment
    fn create_store(&self, topic: Topic, partition: Partition, consumer_group: &str)
    -> Self::Store;
}
