//! Provider for creating Cassandra defer stores with shared resources.

use super::queries::Queries;
use super::{CassandraMessageDeferStore, CassandraMessageDeferStoreError};
use crate::cassandra::CassandraStore;
use crate::consumer::middleware::defer::message::store::MessageDeferStoreProvider;
use crate::consumer::middleware::defer::segment::Segment;
use std::sync::Arc;
use tracing::instrument;

/// Provider for creating [`CassandraMessageDeferStore`] instances.
///
/// Holds shared resources (Cassandra session, prepared queries) and creates
/// store instances for a given [`Segment`].
///
/// # Usage
///
/// ```text
/// // Create provider once at startup
/// let provider = CassandraMessageDeferStoreProvider::with_store(cassandra_store, keyspace).await?;
///
/// // Create stores for each segment (segment already persisted via SegmentStore)
/// let store = provider.create_store(&segment).await?;
/// ```
#[derive(Clone, Debug)]
pub struct CassandraMessageDeferStoreProvider {
    store: CassandraStore,
    queries: Arc<Queries>,
}

impl CassandraMessageDeferStoreProvider {
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
    ) -> Result<Self, CassandraMessageDeferStoreError> {
        let queries = Arc::new(Queries::new(store.session(), keyspace).await?);

        Ok(Self { store, queries })
    }
}

impl MessageDeferStoreProvider for CassandraMessageDeferStoreProvider {
    type Error = CassandraMessageDeferStoreError;
    type Store = CassandraMessageDeferStore;

    #[instrument(level = "debug", skip(self), err)]
    async fn create_store(&self, segment: &Segment) -> Result<Self::Store, Self::Error> {
        // Segment metadata is already persisted via SegmentStore.
        // We just use the segment ID for store operations.
        Ok(CassandraMessageDeferStore {
            store: self.store.clone(),
            queries: Arc::clone(&self.queries),
            segment_id: segment.id(),
        })
    }
}
