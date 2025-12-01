//! Provider for creating Cassandra defer stores with shared resources.

use super::queries::Queries;
use super::{CassandraDeferStore, CassandraDeferStoreError};
use crate::cassandra::CassandraStore;
use crate::cassandra::errors::CassandraStoreError;
use crate::consumer::middleware::defer::store::{DeferStoreProvider, compute_segment_id};
use crate::{Partition, Topic};
use std::sync::Arc;
use tracing::instrument;

/// Provider for creating [`CassandraDeferStore`] instances.
///
/// Holds shared resources (Cassandra session, prepared queries) and creates
/// store instances with the correct segment context for each partition.
///
/// # Usage
///
/// ```text
/// // Create provider once at startup
/// let provider = CassandraDeferStoreProvider::with_store(cassandra_store, keyspace).await?;
///
/// // Create stores for each partition as needed
/// let store = provider.create_store(topic, partition, &consumer_group).await?;
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

impl DeferStoreProvider for CassandraDeferStoreProvider {
    type Error = CassandraDeferStoreError;
    type Store = CassandraDeferStore;

    #[instrument(skip(self), err)]
    async fn create_store(
        &self,
        topic: Topic,
        partition: Partition,
        consumer_group: &str,
    ) -> Result<Self::Store, Self::Error> {
        let segment_id = compute_segment_id(topic, partition, consumer_group);

        // Insert segment metadata (idempotent)
        self.store
            .session()
            .execute_unpaged(
                &self.queries.insert_segment,
                (&segment_id, topic.as_ref(), partition, consumer_group),
            )
            .await
            .map_err(|e| CassandraDeferStoreError::from(CassandraStoreError::from(e)))?;

        Ok(CassandraDeferStore {
            store: self.store.clone(),
            queries: Arc::clone(&self.queries),
            segment_id,
        })
    }
}
