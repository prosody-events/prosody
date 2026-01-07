//! Provider for creating Cassandra timer defer stores with shared resources.

use super::CassandraTimerDeferStore;
use super::queries::Queries;
use crate::cassandra::CassandraStore;
use crate::consumer::middleware::defer::error::CassandraDeferStoreError;
use crate::consumer::middleware::defer::message::handler::TimerStoreProvider;
use crate::consumer::middleware::defer::segment::{LazySegment, SegmentStore};
use std::sync::Arc;

/// Provider for creating [`CassandraTimerDeferStore`] instances.
///
/// Holds shared resources (Cassandra session, prepared queries) and creates
/// store instances for a given [`LazySegment`].
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
    ) -> Result<Self, CassandraDeferStoreError> {
        let queries = Arc::new(Queries::new(store.session(), keyspace).await?);

        Ok(Self { store, queries })
    }

    /// Creates a store for the specified segment.
    ///
    /// The store is created synchronously; segment initialization is deferred
    /// until the first store operation.
    #[must_use]
    pub fn build<S: SegmentStore>(&self, segment: LazySegment<S>) -> CassandraTimerDeferStore<S> {
        CassandraTimerDeferStore {
            store: self.store.clone(),
            queries: Arc::clone(&self.queries),
            segment,
        }
    }
}

impl<S> TimerStoreProvider<S> for CassandraTimerDeferStoreProvider
where
    S: SegmentStore<Error: Into<CassandraDeferStoreError>>,
{
    type Store = CassandraTimerDeferStore<S>;

    fn create_store(&self, segment: LazySegment<S>) -> Self::Store {
        self.build(segment)
    }
}
