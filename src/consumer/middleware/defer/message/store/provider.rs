//! Factory trait for partition-scoped message defer stores.

use super::MessageDeferStore;
use crate::{Partition, Topic};

/// Factory for partition-specific [`MessageDeferStore`] instances.
///
/// Holds shared resources and creates stores with the correct segment context.
/// Store creation is **synchronous**; Cassandra stores defer I/O via
/// [`LazySegment`](crate::consumer::middleware::defer::segment::LazySegment).
pub trait MessageDeferStoreProvider: Clone + Send + Sync + 'static {
    /// The store type created by this provider.
    type Store: MessageDeferStore;

    /// Creates a store for the specified segment (synchronous, no I/O).
    ///
    /// `cache_size` sizes the store's internal write-through cache. Memory
    /// providers may ignore it; Cassandra providers forward it to the
    /// `quick_cache::sync::Cache` inside the store.
    fn create_store(
        &self,
        topic: Topic,
        partition: Partition,
        consumer_group: &str,
        cache_size: usize,
    ) -> Self::Store;
}
