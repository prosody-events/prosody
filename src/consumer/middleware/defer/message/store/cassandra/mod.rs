//! Cassandra-based implementation of [`MessageDeferStore`].
//!
//! Provides persistent storage for deferred messages using Apache Cassandra
//! with automatic schema migration and optimized TTL management.

use crate::cassandra::CassandraStore;
use crate::cassandra::errors::CassandraStoreError;
use crate::consumer::middleware::defer::error::CassandraDeferStoreError;
use crate::consumer::middleware::defer::message::store::MessageDeferStore;
use crate::consumer::middleware::defer::message::store::cassandra::queries::Queries;
use crate::consumer::middleware::defer::segment::{LazySegment, SegmentStore};
use crate::{Key, Offset};
use scylla::client::session::Session;
use std::fmt;
use std::sync::Arc;
use tracing::instrument;

pub mod provider;
mod queries;

pub use provider::CassandraMessageDeferStoreProvider;

/// Cassandra-based implementation of [`MessageDeferStore`].
///
/// Provides persistent storage for deferred messages with automatic TTL
/// management and retry count tracking via static columns.
///
/// # Design
///
/// - **Two tables**: `deferred_segments` (metadata) and `deferred_offsets`
///   (data)
/// - **Partition key**: `(segment_id, key)` in offsets table
/// - **Segment ID**: `UUIDv5` hash of `{topic}/{partition}:{consumer_group}`
///   (resolved lazily from [`LazySegment`])
/// - **Retry count**: Stored as static column (shared across all offsets for a
///   key)
/// - **TTL management**: Uses fixed TTL from `CassandraStore::base_ttl()`
/// - **Ordering**: Clustering by offset ASC ensures FIFO processing
///
/// # Lazy Initialization
///
/// The store holds a [`LazySegment`] which defers segment persistence until
/// first use. This allows the store to be created in synchronous context
/// (e.g., `handler_for_partition`) while deferring I/O to the first async
/// operation.
#[derive(Clone)]
pub struct CassandraMessageDeferStore<S: SegmentStore> {
    store: CassandraStore,
    queries: Arc<Queries>,
    segment: LazySegment<S>,
}

impl<S: SegmentStore> CassandraMessageDeferStore<S> {
    /// Returns a reference to the Cassandra session.
    fn session(&self) -> &Session {
        self.store.session()
    }
}

impl<S: SegmentStore> fmt::Debug for CassandraMessageDeferStore<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CassandraMessageDeferStore")
            .field("segment", &self.segment)
            .finish_non_exhaustive()
    }
}

impl<S> MessageDeferStore for CassandraMessageDeferStore<S>
where
    S: SegmentStore<Error: Into<CassandraDeferStoreError>>,
{
    type Error = CassandraDeferStoreError;

    #[instrument(level = "debug", skip(self), err)]
    async fn defer_first_message(&self, key: &Key, offset: Offset) -> Result<(), Self::Error> {
        let segment_id = self.segment.get().await.map_err(Into::into)?.id();
        let ttl = self.store.base_ttl();

        // INSERT with retry_count=0 for first failure
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
        let segment_id = self.segment.get().await.map_err(Into::into)?.id();

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

        // Filter out rows where offset is NULL (only static column set, no clustering
        // rows)
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
        let segment_id = self.segment.get().await.map_err(Into::into)?.id();
        let ttl = self.store.base_ttl();

        // Don't include retry_count in INSERT - leaves static column unchanged
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
        let segment_id = self.segment.get().await.map_err(Into::into)?.id();

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
        let segment_id = self.segment.get().await.map_err(Into::into)?.id();
        let ttl = self.store.base_ttl();
        let retry_count_i32: i32 =
            retry_count
                .try_into()
                .map_err(|_| CassandraDeferStoreError::InvalidRetryCount {
                    retry_count,
                    reason: "retry count exceeds i32::MAX",
                })?;

        self.session()
            .execute_unpaged(
                &self.queries.update_retry_count,
                (ttl, retry_count_i32, &segment_id, key.as_ref()),
            )
            .await
            .map_err(CassandraStoreError::from)?;

        Ok(())
    }

    #[instrument(level = "debug", skip(self), err)]
    async fn delete_key(&self, key: &Key) -> Result<(), Self::Error> {
        let segment_id = self.segment.get().await.map_err(Into::into)?.id();

        self.session()
            .execute_unpaged(&self.queries.delete_key, (&segment_id, key.as_ref()))
            .await
            .map_err(CassandraStoreError::from)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cassandra::{CassandraConfiguration, CassandraStore};
    use crate::consumer::middleware::defer::segment::{LazySegment, MemorySegmentStore};
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
        let provider =
            CassandraMessageDeferStoreProvider::with_store(cassandra_store, "prosody_test").await?;
        let segment = LazySegment::new(
            MemorySegmentStore::new(),
            Topic::from("test-topic"),
            Partition::from(0_i32),
            Arc::from("test-consumer-group") as ConsumerGroup,
        );
        let defer_store = provider.build(segment);
        Ok::<_, color_eyre::Report>(defer_store)
    });
}
