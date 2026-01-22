//! Cassandra-backed message defer store with TTL and segment-based
//! partitioning.

use crate::cassandra::CassandraStore;
use crate::cassandra::errors::CassandraStoreError;
use crate::consumer::middleware::defer::error::CassandraDeferStoreError;
use crate::consumer::middleware::defer::message::store::MessageDeferStore;
use crate::consumer::middleware::defer::message::store::cassandra::queries::Queries;
use crate::consumer::middleware::defer::message::store::provider::MessageDeferStoreProvider;
use crate::consumer::middleware::defer::segment::{CassandraSegmentStore, LazySegment};
use crate::{ConsumerGroup, Key, Offset, Partition, Topic};
use scylla::client::session::Session;
use std::fmt;
use std::sync::Arc;
use tracing::instrument;

pub mod queries;

pub use queries::Queries as MessageQueries;

/// Cassandra-backed message defer store.
///
/// # Storage Model
///
/// - **Partition key**: `(segment_id, key)` where segment = `UUIDv5` of
///   `{topic}/{partition}:{consumer_group}`
/// - **Clustering**: `offset ASC` for FIFO ordering
/// - **Retry count**: Static column (shared per key)
/// - **TTL**: Fixed duration from [`CassandraStore::base_ttl()`]
///
/// Uses [`LazySegment`] to defer segment persistence until first access,
/// allowing synchronous store creation in `handler_for_partition`.
#[derive(Clone)]
pub struct CassandraMessageDeferStore {
    store: CassandraStore,
    queries: Arc<Queries>,
    segment: LazySegment<CassandraSegmentStore>,
}

impl CassandraMessageDeferStore {
    /// Creates a store; segment persisted lazily on first access.
    #[must_use]
    pub fn new(
        store: CassandraStore,
        queries: Arc<Queries>,
        segment_store: CassandraSegmentStore,
        topic: Topic,
        partition: Partition,
        consumer_group: ConsumerGroup,
    ) -> Self {
        let segment = LazySegment::new(segment_store, topic, partition, consumer_group);
        Self {
            store,
            queries,
            segment,
        }
    }

    fn session(&self) -> &Session {
        self.store.session()
    }

    async fn segment_id(&self) -> Result<uuid::Uuid, CassandraDeferStoreError> {
        let segment = self.segment.get().await?;
        Ok(segment.id())
    }
}

impl fmt::Debug for CassandraMessageDeferStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CassandraMessageDeferStore")
            .field("segment", &self.segment)
            .finish_non_exhaustive()
    }
}

impl MessageDeferStore for CassandraMessageDeferStore {
    type Error = CassandraDeferStoreError;

    #[instrument(level = "debug", skip(self), err)]
    async fn defer_first_message(&self, key: &Key, offset: Offset) -> Result<(), Self::Error> {
        let segment_id = self.segment_id().await?;
        let ttl = self.store.base_ttl();

        self.session()
            .execute_unpaged(
                &self.queries.insert_deferred_message_with_retry_count,
                (&segment_id, key.as_ref(), offset, 0_i32, ttl),
            )
            .await
            .map_err(CassandraStoreError::from)?;

        Ok(())
    }

    #[instrument(level = "debug", skip(self), err)]
    async fn get_next_deferred_message(
        &self,
        key: &Key,
    ) -> Result<Option<(Offset, u32)>, Self::Error> {
        let segment_id = self.segment_id().await?;

        let result = self
            .session()
            .execute_unpaged(
                &self.queries.get_next_deferred_message,
                (&segment_id, key.as_ref()),
            )
            .await
            .map_err(CassandraStoreError::from)?;

        let row_opt = result
            .into_rows_result()
            .map_err(CassandraStoreError::from)?
            .maybe_first_row::<(Option<i64>, Option<i32>)>()
            .map_err(CassandraStoreError::from)?;

        // NULL offset means static column exists but no clustering rows
        Ok(row_opt.and_then(|(offset_opt, retry_count_opt)| {
            offset_opt.map(|offset_raw| {
                let offset = Offset::from(offset_raw);
                let retry_count = retry_count_opt.and_then(|c| c.try_into().ok()).unwrap_or(0);
                (offset, retry_count)
            })
        }))
    }

    #[instrument(level = "debug", skip(self), err)]
    async fn append_deferred_message(&self, key: &Key, offset: Offset) -> Result<(), Self::Error> {
        let segment_id = self.segment_id().await?;
        let ttl = self.store.base_ttl();

        // Omitting retry_count preserves static column
        self.session()
            .execute_unpaged(
                &self.queries.insert_deferred_message_without_retry_count,
                (&segment_id, key.as_ref(), offset, ttl),
            )
            .await
            .map_err(CassandraStoreError::from)?;

        Ok(())
    }

    #[instrument(level = "debug", skip(self), err)]
    async fn remove_deferred_message(&self, key: &Key, offset: Offset) -> Result<(), Self::Error> {
        let segment_id = self.segment_id().await?;

        self.session()
            .execute_unpaged(
                &self.queries.remove_deferred_message,
                (&segment_id, key.as_ref(), offset),
            )
            .await
            .map_err(CassandraStoreError::from)?;

        Ok(())
    }

    #[instrument(level = "debug", skip(self), err)]
    async fn set_retry_count(&self, key: &Key, retry_count: u32) -> Result<(), Self::Error> {
        let segment_id = self.segment_id().await?;
        let ttl = self.store.base_ttl();
        let retry_count: i32 = retry_count.try_into().unwrap_or(i32::MAX);

        self.session()
            .execute_unpaged(
                &self.queries.update_retry_count,
                (ttl, retry_count, &segment_id, key.as_ref()),
            )
            .await
            .map_err(CassandraStoreError::from)?;

        Ok(())
    }

    #[instrument(level = "debug", skip(self), err)]
    async fn delete_key(&self, key: &Key) -> Result<(), Self::Error> {
        let segment_id = self.segment_id().await?;

        self.session()
            .execute_unpaged(&self.queries.delete_key, (&segment_id, key.as_ref()))
            .await
            .map_err(CassandraStoreError::from)?;

        Ok(())
    }
}

/// Factory for partition-scoped Cassandra message defer stores.
///
/// Holds shared Cassandra resources; creates stores with correct segment
/// context per partition.
#[derive(Clone, Debug)]
pub struct CassandraMessageDeferStoreProvider {
    store: CassandraStore,
    queries: Arc<Queries>,
    segment_store: CassandraSegmentStore,
}

impl CassandraMessageDeferStoreProvider {
    /// Creates a provider with shared Cassandra resources.
    #[must_use]
    pub fn new(
        store: CassandraStore,
        queries: Arc<Queries>,
        segment_store: CassandraSegmentStore,
    ) -> Self {
        Self {
            store,
            queries,
            segment_store,
        }
    }
}

impl MessageDeferStoreProvider for CassandraMessageDeferStoreProvider {
    type Store = CassandraMessageDeferStore;

    fn create_store(
        &self,
        topic: Topic,
        partition: Partition,
        consumer_group: &str,
    ) -> Self::Store {
        CassandraMessageDeferStore::new(
            self.store.clone(),
            self.queries.clone(),
            self.segment_store.clone(),
            topic,
            partition,
            Arc::from(consumer_group),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cassandra::{CassandraConfiguration, CassandraStore};
    use crate::defer_store_tests;
    use crate::{ConsumerGroup, Partition, Topic};

    // Property-based tests using model equivalence
    defer_store_tests!(async {
        let config = CassandraConfiguration::builder()
            .nodes(vec!["localhost:9042".to_owned()])
            .keyspace("prosody_test".to_owned())
            .build()
            .map_err(|e| color_eyre::eyre::eyre!("Config build failed: {e}"))?;

        let cassandra_store = CassandraStore::new(&config).await?;
        let segment_store =
            CassandraSegmentStore::new(cassandra_store.clone(), "prosody_test").await?;
        let queries = Arc::new(Queries::new(cassandra_store.session(), "prosody_test").await?);
        let defer_store = CassandraMessageDeferStore::new(
            cassandra_store,
            queries,
            segment_store,
            Topic::from("test-topic"),
            Partition::from(0_i32),
            Arc::from("test-consumer-group") as ConsumerGroup,
        );
        Ok::<_, color_eyre::Report>(defer_store)
    });
}
