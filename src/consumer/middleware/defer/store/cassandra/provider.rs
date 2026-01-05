//! Provider for creating Cassandra defer stores with shared resources.

use super::queries::Queries;
use super::{CassandraDeferStore, CassandraDeferStoreError};
use crate::cassandra::CassandraStore;
use crate::consumer::middleware::defer::segment::Segment;
use crate::consumer::middleware::defer::store::MessageDeferStoreProvider;
use std::sync::Arc;
use tracing::instrument;

/// Provider for creating [`CassandraDeferStore`] instances.
///
/// Holds shared resources (Cassandra session, prepared queries) and creates
/// store instances for a given [`Segment`].
///
/// # Usage
///
/// ```text
/// // Create provider once at startup
/// let provider = CassandraDeferStoreProvider::with_store(cassandra_store, keyspace).await?;
///
/// // Create stores for each segment (segment already persisted via SegmentStore)
/// let store = provider.create_store(&segment).await?;
/// ```
#[derive(Clone, Debug)]
pub struct CassandraDeferStoreProvider {
    store: CassandraStore,
    queries: Arc<Queries>,
}

impl CassandraDeferStoreProvider {
    /// Creates a new provider using an existing `CassandraStore`.
    ///
    /// This allows sharing a single Cassandra session across multiple
    /// components (e.g., trigger store and defer store).
    ///
    /// # Arguments
    ///
    /// * `store` - Existing `CassandraStore` to share
    /// * `keyspace` - Cassandra keyspace name for query preparation
    ///
    /// # Errors
    ///
    /// Returns error if query preparation fails.
    pub async fn with_store(
        store: CassandraStore,
        keyspace: &str,
    ) -> Result<Self, CassandraDeferStoreError> {
        let queries = Arc::new(Queries::new(store.session(), keyspace).await?);

        Ok(Self { store, queries })
    }
}

impl MessageDeferStoreProvider for CassandraDeferStoreProvider {
    type Error = CassandraDeferStoreError;
    type Store = CassandraDeferStore;

    #[instrument(level = "debug", skip(self), err)]
    async fn create_store(&self, segment: &Segment) -> Result<Self::Store, Self::Error> {
        // Segment metadata is already persisted via SegmentStore.
        // We just use the segment ID for store operations.
        Ok(CassandraDeferStore {
            store: self.store.clone(),
            queries: Arc::clone(&self.queries),
            segment_id: segment.id(),
        })
    }
}
