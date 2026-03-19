//! Cassandra-backed deduplication store.

use super::queries::DeduplicationQueries;
use super::store::{DeduplicationStore, DeduplicationStoreProvider};
use crate::cassandra::CassandraStore;
use crate::cassandra::errors::CassandraStoreError;
use crate::{Partition, Topic};
use std::sync::Arc;
use uuid::Uuid;

/// Cassandra-backed deduplication store.
///
/// All instances share the same Cassandra session and prepared queries.
/// Reads and writes target a global deduplication table keyed by UUID.
#[derive(Clone, Debug)]
pub struct CassandraDeduplicationStore {
    store: CassandraStore,
    queries: Arc<DeduplicationQueries>,
    ttl: i32,
}

impl DeduplicationStore for CassandraDeduplicationStore {
    type Error = CassandraStoreError;

    async fn exists(&self, id: Uuid) -> Result<bool, Self::Error> {
        let result = self
            .store
            .session()
            .execute_unpaged(&self.queries.check_exists, (id,))
            .await?;

        let has_rows = result
            .into_rows_result()?
            .maybe_first_row::<(Uuid,)>()?
            .is_some();

        Ok(has_rows)
    }

    async fn insert(&self, id: Uuid) -> Result<(), Self::Error> {
        self.store
            .session()
            .execute_unpaged(&self.queries.insert_with_ttl, (id, self.ttl))
            .await?;
        Ok(())
    }
}

/// Factory for Cassandra deduplication stores.
///
/// Holds shared resources; all created stores share the same session, queries,
/// and TTL configuration.
#[derive(Clone, Debug)]
pub struct CassandraDeduplicationStoreProvider {
    store: CassandraStore,
    queries: Arc<DeduplicationQueries>,
    ttl: i32,
}

impl CassandraDeduplicationStoreProvider {
    /// Creates a new provider.
    #[must_use]
    pub fn new(store: CassandraStore, queries: Arc<DeduplicationQueries>, ttl: i32) -> Self {
        Self {
            store,
            queries,
            ttl,
        }
    }
}

impl DeduplicationStoreProvider for CassandraDeduplicationStoreProvider {
    type Store = CassandraDeduplicationStore;

    fn create_store(
        &self,
        _topic: Topic,
        _partition: Partition,
        _consumer_group: &str,
    ) -> Self::Store {
        CassandraDeduplicationStore {
            store: self.store.clone(),
            queries: self.queries.clone(),
            ttl: self.ttl,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cassandra::CassandraConfiguration;

    crate::dedup_store_tests!(async {
        let config = CassandraConfiguration::builder()
            .nodes(vec!["localhost:9042".to_owned()])
            .keyspace("prosody_test".to_owned())
            .build()
            .map_err(|e| color_eyre::eyre::eyre!("Config build failed: {e}"))?;

        let cassandra_store = CassandraStore::new(&config).await?;
        let queries = Arc::new(
            DeduplicationQueries::new(cassandra_store.session(), "prosody_test")
                .await?
                .with_local_one_consistency(),
        );

        Ok::<_, color_eyre::Report>(CassandraDeduplicationStore {
            store: cassandra_store,
            queries,
            ttl: 3600_i32,
        })
    });
}
