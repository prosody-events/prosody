//! Cassandra implementation of [`DeferStoreProvider`] for production use.

use super::DeferStoreProvider;
use crate::cassandra::CassandraStore;
use crate::cassandra::errors::CassandraStoreError;
use crate::consumer::middleware::defer::message::store::cassandra::{
    CassandraMessageDeferStore, MessageQueries,
};
use crate::consumer::middleware::defer::segment::{
    CassandraSegmentStore, CassandraSegmentStoreError, LazySegment,
};
use crate::consumer::middleware::defer::timer::store::cassandra::{
    CassandraTimerDeferStore, TimerQueries,
};
use std::sync::Arc;

/// Cassandra defer store provider for production use.
///
/// Creates [`CassandraMessageDeferStore`] and [`CassandraTimerDeferStore`]
/// instances that persist data to Apache Cassandra. All stores share the same
/// Cassandra session and segment store.
///
/// # Usage
///
/// ```rust,no_run
/// use prosody::cassandra::{CassandraConfiguration, CassandraStore};
/// use prosody::consumer::middleware::defer::CassandraDeferStoreProvider;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let config = CassandraConfiguration::builder()
///     .nodes(vec!["localhost:9042".to_owned()])
///     .keyspace("my_keyspace".to_owned())
///     .build()?;
/// let cassandra_store = CassandraStore::new(&config).await?;
/// let provider = CassandraDeferStoreProvider::new(cassandra_store, "my_keyspace").await?;
/// // Pass to MessageDeferMiddleware
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct CassandraDeferStoreProvider {
    segment: CassandraSegmentStore,
    store: CassandraStore,
    message_queries: Arc<MessageQueries>,
    timer_queries: Arc<TimerQueries>,
}

/// Errors that can occur during Cassandra provider initialization.
#[derive(Debug, thiserror::Error)]
pub enum CassandraDeferProviderError {
    /// Error creating segment store.
    #[error("segment store error: {0:#}")]
    Segment(#[from] CassandraSegmentStoreError),

    /// Error preparing queries.
    #[error("query preparation error: {0:#}")]
    Queries(#[from] CassandraStoreError),
}

impl CassandraDeferStoreProvider {
    /// Creates a new Cassandra defer store provider.
    ///
    /// Initializes segment store and prepares queries for message and timer
    /// stores using the provided Cassandra session.
    ///
    /// # Arguments
    ///
    /// * `store` - Existing `CassandraStore` to share
    /// * `keyspace` - Cassandra keyspace name for query preparation
    ///
    /// # Errors
    ///
    /// Returns error if segment store or query preparation fails.
    pub async fn new(
        store: CassandraStore,
        keyspace: &str,
    ) -> Result<Self, CassandraDeferProviderError> {
        let segment = CassandraSegmentStore::new(store.clone(), keyspace).await?;
        let message_queries = Arc::new(MessageQueries::new(store.session(), keyspace).await?);
        let timer_queries = Arc::new(TimerQueries::new(store.session(), keyspace).await?);

        Ok(Self {
            segment,
            store,
            message_queries,
            timer_queries,
        })
    }
}

impl DeferStoreProvider for CassandraDeferStoreProvider {
    type MessageStore = CassandraMessageDeferStore<CassandraSegmentStore>;
    type Segment = CassandraSegmentStore;
    type TimerStore = CassandraTimerDeferStore<CassandraSegmentStore>;

    fn segment(&self) -> &Self::Segment {
        &self.segment
    }

    fn create_message_store(&self, segment: LazySegment<Self::Segment>) -> Self::MessageStore {
        CassandraMessageDeferStore::new(
            self.store.clone(),
            Arc::clone(&self.message_queries),
            segment,
        )
    }

    fn create_timer_store(&self, segment: LazySegment<Self::Segment>) -> Self::TimerStore {
        CassandraTimerDeferStore::new(self.store.clone(), Arc::clone(&self.timer_queries), segment)
    }
}
