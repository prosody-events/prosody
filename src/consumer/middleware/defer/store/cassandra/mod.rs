//! Cassandra-based implementation of [`DeferStore`].
//!
//! Provides persistent storage for deferred messages using Apache Cassandra
//! with automatic schema migration and optimized TTL management.

use crate::Offset;
use crate::cassandra::errors::CassandraStoreError;
use crate::cassandra::{CassandraConfiguration, CassandraStore};
use crate::consumer::middleware::defer::store::DeferStore;
use crate::consumer::middleware::defer::store::cassandra::queries::Queries;
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
/// - **Single table**: `deferred_messages` with `(key_id, offset)` primary key
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
    /// Creates a new Cassandra defer store with the given configuration.
    ///
    /// Initializes the connection to Cassandra, runs schema migrations,
    /// and prepares all required queries.
    ///
    /// # Arguments
    ///
    /// * `config` - Cassandra connection and TTL configuration
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - Connection to Cassandra fails
    /// - Schema migration fails
    /// - Query preparation fails
    pub async fn new(config: &CassandraConfiguration) -> Result<Self, CassandraStoreError> {
        let store = CassandraStore::new(config).await?;
        let queries = Arc::new(Queries::new(store.session(), &config.keyspace).await?);

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
        key_id: &Uuid,
    ) -> Result<Option<(Offset, u32)>, Self::Error> {
        let result = self
            .session()
            .execute_unpaged(&self.queries.get_next_deferred_message, (key_id,))
            .await
            .map_err(CassandraStoreError::from)?;

        let row_opt = result
            .into_rows_result()
            .map_err(CassandraStoreError::from)?
            .maybe_first_row::<(i64, Option<i32>)>()
            .map_err(CassandraStoreError::from)?;

        Ok(row_opt.map(|(offset_raw, retry_count_opt)| {
            let offset = Offset::from(offset_raw);
            let retry_count = retry_count_opt.and_then(|c| c.try_into().ok()).unwrap_or(0);
            (offset, retry_count)
        }))
    }

    #[instrument(skip(self), fields(key_id = %key_id, offset = %offset), err)]
    async fn append_deferred_message(
        &self,
        key_id: &Uuid,
        offset: Offset,
        expected_retry_time: CompactDateTime,
        retry_count: Option<u32>,
    ) -> Result<(), Self::Error> {
        let ttl = self
            .store
            .calculate_ttl(expected_retry_time)
            .ok_or(CassandraDeferStoreError::TtlCalculationFailed)?;

        let retry_count_i32: Option<i32> = retry_count.and_then(|c| c.try_into().ok());

        self.session()
            .execute_unpaged(
                &self.queries.insert_deferred_message,
                (key_id, offset, retry_count_i32, ttl),
            )
            .await
            .map_err(CassandraStoreError::from)?;

        Ok(())
    }

    #[instrument(skip(self), fields(key_id = %key_id, offset = %offset), err)]
    async fn remove_deferred_message(
        &self,
        key_id: &Uuid,
        offset: Offset,
    ) -> Result<(), Self::Error> {
        self.session()
            .execute_unpaged(&self.queries.remove_deferred_message, (key_id, offset))
            .await
            .map_err(CassandraStoreError::from)?;

        Ok(())
    }

    #[instrument(skip(self), fields(key_id = %key_id, retry_count = retry_count), err)]
    async fn set_retry_count(&self, key_id: &Uuid, retry_count: u32) -> Result<(), Self::Error> {
        let retry_count_i32: i32 =
            retry_count
                .try_into()
                .map_err(|_| CassandraDeferStoreError::InvalidRetryCount {
                    retry_count,
                    reason: "retry count exceeds i32::MAX",
                })?;

        self.session()
            .execute_unpaged(&self.queries.update_retry_count, (retry_count_i32, key_id))
            .await
            .map_err(CassandraStoreError::from)?;

        Ok(())
    }

    #[instrument(skip(self), fields(key_id = %key_id), err)]
    async fn delete_key(&self, key_id: &Uuid) -> Result<(), Self::Error> {
        self.session()
            .execute_unpaged(&self.queries.delete_key, (key_id,))
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
