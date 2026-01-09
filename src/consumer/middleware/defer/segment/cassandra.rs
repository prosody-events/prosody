//! Cassandra-backed segment persistence.

use super::store::SegmentStore;
use super::{Segment, SegmentId};
use crate::cassandra::errors::CassandraStoreError;
use crate::cassandra::{CassandraStore, TABLE_DEFERRED_SEGMENTS};
use crate::cassandra_queries;
use crate::consumer::middleware::{ClassifyError, ErrorCategory};
use crate::{ConsumerGroup, Partition, Topic};
use scylla::client::session::Session;
use std::sync::Arc;
use thiserror::Error;
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
    ) -> Result<Self, CassandraSegmentStoreError> {
        let queries = Arc::new(SegmentQueries::new(store.session(), keyspace).await?);
        Ok(Self { store, queries })
    }

    fn session(&self) -> &Session {
        self.store.session()
    }
}

impl SegmentStore for CassandraSegmentStore {
    type Error = CassandraSegmentStoreError;

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

/// Cassandra segment store errors.
#[derive(Debug, Error)]
pub enum CassandraSegmentStoreError {
    /// Error from Cassandra operations.
    #[error("cassandra error: {0:#}")]
    Cassandra(#[from] CassandraStoreError),
}

impl ClassifyError for CassandraSegmentStoreError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::Cassandra(error) => error.classify_error(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cassandra::CassandraConfiguration;

    #[tokio::test]
    async fn test_cassandra_get_or_create_segment() -> color_eyre::Result<()> {
        let config = CassandraConfiguration::builder()
            .nodes(vec!["localhost:9042".to_owned()])
            .keyspace("prosody_test".to_owned())
            .build()
            .map_err(|e| color_eyre::eyre::eyre!("Config build failed: {e}"))?;

        let cassandra_store = CassandraStore::new(&config).await?;
        let segment_store = CassandraSegmentStore::new(cassandra_store, "prosody_test").await?;

        let segment = Segment::new(
            Topic::from("test-topic"),
            Partition::from(0_i32),
            Arc::from("test-group") as ConsumerGroup,
        );
        let segment_id = segment.id();

        // Create segment
        let result = segment_store.get_or_create_segment(segment.clone()).await?;
        assert_eq!(result.id(), segment_id);

        // Verify it can be retrieved
        let retrieved = segment_store.get_segment(&segment_id).await?;
        assert_eq!(retrieved, Some(segment));

        Ok(())
    }
}
