//! Cassandra-based implementation of [`DeferStore`].
//!
//! Provides persistent storage for deferred messages using Apache Cassandra
//! with automatic schema migration and optimized TTL management.

use crate::Offset;
use crate::cassandra::CassandraStore;
use crate::cassandra::errors::CassandraStoreError;
use crate::consumer::middleware::defer::store::DeferStore;
use crate::consumer::middleware::defer::store::cassandra::queries::Queries;
use crate::consumer::middleware::defer::store::key_ref::DeferKeyRef;
use crate::consumer::middleware::{ClassifyError, ErrorCategory};
use crate::timers::datetime::CompactDateTime;
use scylla::client::session::Session;
use std::sync::Arc;
use thiserror::Error;
use tracing::instrument;
use uuid::Uuid;

mod queries;

/// Cassandra-based implementation of [`DeferStore`].
///
/// Provides persistent storage for deferred messages with automatic TTL
/// management and retry count tracking via static columns.
///
/// # Design
///
/// - **Single table**: `deferred_messages` with `(defer_key, offset)` primary
///   key
/// - **Retry count**: Stored as static column (shared across all offsets for a
///   key)
/// - **TTL management**: Uses `CassandraStore::calculate_ttl()` for safe TTL
///   values
/// - **Ordering**: Clustering by offset ASC ensures FIFO processing
#[derive(Clone, Debug)]
pub struct CassandraDeferStore {
    store: CassandraStore,
    queries: Arc<Queries>,
}

impl CassandraDeferStore {
    /// Creates a new Cassandra defer store using an existing `CassandraStore`.
    ///
    /// This allows sharing a single Cassandra session across multiple stores
    /// (e.g., trigger store and defer store), avoiding the creation of multiple
    /// sessions which is not allowed.
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
    ) -> Result<Self, CassandraStoreError> {
        let queries = Arc::new(Queries::new(store.session(), keyspace).await?);

        Ok(Self { store, queries })
    }

    /// Returns a reference to the Cassandra session.
    fn session(&self) -> &Session {
        self.store.session()
    }
}

impl DeferStore for CassandraDeferStore {
    type Error = CassandraDeferStoreError;

    #[instrument(skip(self), err)]
    async fn get_next_deferred_message(
        &self,
        key: &DeferKeyRef<'_>,
    ) -> Result<Option<(Offset, u32)>, Self::Error> {
        let defer_key = Uuid::from(key);

        let result = self
            .session()
            .execute_unpaged(&self.queries.get_next_deferred_message, (&defer_key,))
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

    #[instrument(skip(self), err)]
    async fn defer_first_message(
        &self,
        key: &DeferKeyRef<'_>,
        offset: Offset,
        expected_retry_time: CompactDateTime,
    ) -> Result<(), Self::Error> {
        let defer_key = Uuid::from(key);

        let ttl = self
            .store
            .calculate_ttl(expected_retry_time)
            .ok_or(CassandraDeferStoreError::TtlCalculationFailed)?;

        // INSERT with retry_count=0 for first failure
        self.session()
            .execute_unpaged(
                &self.queries.insert_deferred_message_with_retry_count,
                (&defer_key, offset, 0_i32, ttl),
            )
            .await
            .map_err(CassandraStoreError::from)?;

        Ok(())
    }

    #[instrument(skip(self), err)]
    async fn append_deferred_message(
        &self,
        key: &DeferKeyRef<'_>,
        offset: Offset,
        expected_retry_time: CompactDateTime,
    ) -> Result<(), Self::Error> {
        let defer_key = Uuid::from(key);

        let ttl = self
            .store
            .calculate_ttl(expected_retry_time)
            .ok_or(CassandraDeferStoreError::TtlCalculationFailed)?;

        // Don't include retry_count in INSERT - leaves static column unchanged
        self.session()
            .execute_unpaged(
                &self.queries.insert_deferred_message_without_retry_count,
                (&defer_key, offset, ttl),
            )
            .await
            .map_err(CassandraStoreError::from)?;

        Ok(())
    }

    #[instrument(skip(self), err)]
    async fn remove_deferred_message(
        &self,
        key: &DeferKeyRef<'_>,
        offset: Offset,
    ) -> Result<(), Self::Error> {
        let defer_key = Uuid::from(key);

        self.session()
            .execute_unpaged(&self.queries.remove_deferred_message, (&defer_key, offset))
            .await
            .map_err(CassandraStoreError::from)?;

        Ok(())
    }

    #[instrument(skip(self), err)]
    async fn set_retry_count(
        &self,
        key: &DeferKeyRef<'_>,
        retry_count: u32,
    ) -> Result<(), Self::Error> {
        let defer_key = Uuid::from(key);

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
                (retry_count_i32, &defer_key),
            )
            .await
            .map_err(CassandraStoreError::from)?;

        Ok(())
    }

    #[instrument(skip(self), err)]
    async fn delete_key(&self, key: &DeferKeyRef<'_>) -> Result<(), Self::Error> {
        let defer_key = Uuid::from(key);

        self.session()
            .execute_unpaged(&self.queries.delete_key, (&defer_key,))
            .await
            .map_err(CassandraStoreError::from)?;

        Ok(())
    }
}

/// Errors that can occur in Cassandra defer store operations.
#[derive(Debug, Error)]
pub enum CassandraDeferStoreError {
    /// Error from Cassandra operations.
    #[error("cassandra error: {0:#}")]
    Cassandra(#[from] CassandraStoreError),

    /// TTL calculation failed (time too far in future).
    #[error("TTL calculation failed - time exceeds Cassandra limits")]
    TtlCalculationFailed,

    /// Invalid retry count value.
    #[error("invalid retry count {retry_count}: {reason}")]
    InvalidRetryCount {
        /// The invalid retry count value.
        retry_count: u32,
        /// Why the value is invalid.
        reason: &'static str,
    },
}

impl ClassifyError for CassandraDeferStoreError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            // Delegate Cassandra errors to their classification
            Self::Cassandra(error) => error.classify_error(),

            // TTL calculation failures are permanent - the requested time is invalid
            Self::TtlCalculationFailed => ErrorCategory::Permanent,

            // Invalid retry count is a programming error - terminal
            Self::InvalidRetryCount { .. } => ErrorCategory::Terminal,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cassandra::{CassandraConfiguration, CassandraStore};
    use crate::defer_store_tests;

    // Property-based tests using model equivalence
    defer_store_tests!(async {
        let config = CassandraConfiguration::builder()
            .nodes(vec!["localhost:9042".to_owned()])
            .keyspace("prosody_test".to_owned())
            .build()
            .map_err(|e| color_eyre::eyre::eyre!("Config build failed: {e}"))?;

        let store = CassandraStore::new(&config).await?;
        let defer_store = CassandraDeferStore::with_store(store, "prosody_test").await?;
        Ok::<_, color_eyre::Report>(defer_store)
    });
}
