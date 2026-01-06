//! Provider for creating Cassandra timer defer stores with shared resources.

use super::queries::Queries;
use super::{CassandraTimerDeferStore, CassandraTimerDeferStoreError};
use crate::cassandra::CassandraStore;
use crate::consumer::middleware::defer::segment::Segment;
use crate::consumer::middleware::defer::timer::store::TimerDeferStoreProvider;
use std::sync::Arc;
use tracing::instrument;

/// Provider for creating [`CassandraTimerDeferStore`] instances.
///
/// Holds shared resources (Cassandra session, prepared queries) and creates
/// store instances for a given [`Segment`].
#[derive(Clone, Debug)]
pub struct CassandraTimerDeferStoreProvider {
    store: CassandraStore,
    queries: Arc<Queries>,
}

impl CassandraTimerDeferStoreProvider {
    /// Creates a new provider using an existing `CassandraStore`.
    ///
    /// This allows sharing a single Cassandra session across multiple
    /// components (e.g., trigger store, message defer store, and timer defer
    /// store).
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
    ) -> Result<Self, CassandraTimerDeferStoreError> {
        let queries = Arc::new(Queries::new(store.session(), keyspace).await?);

        Ok(Self { store, queries })
    }
}

impl TimerDeferStoreProvider for CassandraTimerDeferStoreProvider {
    type Error = CassandraTimerDeferStoreError;
    type Store = CassandraTimerDeferStore;

    #[instrument(level = "debug", skip(self), err)]
    async fn create_store(&self, segment: &Segment) -> Result<Self::Store, Self::Error> {
        // Segment metadata is already persisted via SegmentStore.
        // We just use the segment ID for store operations.
        Ok(CassandraTimerDeferStore {
            store: self.store.clone(),
            queries: Arc::clone(&self.queries),
            segment_id: segment.id(),
        })
    }
}
