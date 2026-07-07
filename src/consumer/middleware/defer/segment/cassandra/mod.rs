//! Cassandra-backed segment persistence.

use super::Segment;
use super::store::SegmentStore;
use crate::SegmentId;
use crate::cassandra::errors::CassandraStoreError;
use crate::cassandra::{CassandraStore, TABLE_DEFERRED_SEGMENTS};
use crate::cassandra_queries;
use crate::consumer::middleware::defer::error::CassandraDeferStoreError;
use crate::{ConsumerGroup, Partition, Topic};
use scylla::client::session::Session;
use std::sync::Arc;
use tracing::instrument;

cassandra_queries! {
    struct SegmentQueries {
        /// Upsert segment metadata.
        insert_segment: (
            "INSERT INTO $keyspace.{} (id, topic, partition, consumer_group) VALUES (?, ?, ?, ?)",
            TABLE_DEFERRED_SEGMENTS
        ),

        /// Get segment by ID.
        get_segment: (
            "SELECT topic, partition, consumer_group FROM $keyspace.{} WHERE id = ?",
            TABLE_DEFERRED_SEGMENTS
        ),
    }
}

/// Cassandra-backed segment store.
#[derive(Clone, Debug)]
pub struct CassandraSegmentStore {
    store: CassandraStore,
    queries: Arc<SegmentQueries>,
}

impl CassandraSegmentStore {
    /// Creates a store; prepares queries against the given keyspace.
    ///
    /// # Errors
    ///
    /// Returns error if query preparation fails.
    pub async fn new(
        store: CassandraStore,
        keyspace: &str,
    ) -> Result<Self, CassandraDeferStoreError> {
        let queries = Arc::new(SegmentQueries::new(store.session(), keyspace).await?);
        Ok(Self { store, queries })
    }

    fn session(&self) -> &Session {
        self.store.session()
    }
}

impl SegmentStore for CassandraSegmentStore {
    type Error = CassandraDeferStoreError;

    #[instrument(level = "debug", skip(self), err)]
    async fn get_or_create_segment(&self, segment: Segment) -> Result<Segment, Self::Error> {
        let topic: &str = segment.topic().as_ref();
        let consumer_group: &str = segment.consumer_group().as_ref();

        self.session()
            .execute_unpaged(
                &self.queries.insert_segment,
                (segment.id(), topic, segment.partition(), consumer_group),
            )
            .await
            .map_err(CassandraStoreError::from)?;

        Ok(segment)
    }

    #[instrument(level = "debug", skip(self), err)]
    async fn get_segment(&self, segment_id: &SegmentId) -> Result<Option<Segment>, Self::Error> {
        let result = self
            .session()
            .execute_unpaged(&self.queries.get_segment, (segment_id,))
            .await
            .map_err(CassandraStoreError::from)?;

        let row_opt = result
            .into_rows_result()
            .map_err(CassandraStoreError::from)?
            .maybe_first_row::<(String, i32, String)>()
            .map_err(CassandraStoreError::from)?;

        Ok(row_opt.map(|(topic, partition, consumer_group)| {
            Segment::with_id(
                *segment_id,
                Topic::from(topic.as_ref()),
                Partition::from(partition),
                Arc::from(consumer_group) as ConsumerGroup,
            )
        }))
    }
}

#[cfg(test)]
mod tests;
