//! Store construction and the [`TriggerStoreProvider`] impl.
//!
//! Prepared statements are expensive to create — each one involves a
//! round-trip to Cassandra and a parse/plan cycle.
//! [`CassandraTriggerStoreProvider`] therefore prepares queries once and shares
//! them (via `Arc<Queries>`) across every per-segment store it creates. Each
//! store still gets its own `state_cache` so per-partition state doesn't bleed
//! between segments.
//!
//! [`cassandra_store`] is a simpler entry point that owns its own session;
//! prefer [`CassandraTriggerStoreProvider`] wherever multiple segments are
//! served from the same process.
//!
//! [`TriggerStoreProvider`]: crate::timers::store::TriggerStoreProvider

use crate::cassandra::{CassandraConfiguration, CassandraStore};
use crate::otel::SpanRelation;
use crate::timers::store::Segment;
use crate::timers::store::TriggerStoreProvider;
use crate::timers::store::adapter::TableAdapter;
use crate::timers::store::cassandra::CassandraTriggerStore;
use crate::timers::store::cassandra::error::CassandraTriggerStoreError;
use crate::timers::store::cassandra::queries::Queries;
use std::sync::Arc;

/// Creates a new Cassandra trigger store.
///
/// Returns an implementation of `TriggerStore` backed by Apache Cassandra.
/// This is the recommended way to create a Cassandra store.
///
/// # Arguments
///
/// * `config` - Cassandra connection and TTL configuration
/// * `segment` - Segment this store is scoped to
/// * `timer_relation` - Span relation for timer execution spans
///
/// # Errors
///
/// Returns [`CassandraTriggerStoreError`] if:
/// - Connection to Cassandra fails
/// - Schema migration fails
/// - Query preparation fails
///
/// # Example
///
/// ```rust,ignore
/// let config = CassandraConfiguration { ... };
/// let store = cassandra_store(&config, segment, SpanRelation::Link).await?;
/// let manager = TimerManager::new(..., store);
/// ```
pub async fn cassandra_store(
    config: &CassandraConfiguration,
    segment: Segment,
    timer_relation: SpanRelation,
) -> Result<TableAdapter<CassandraTriggerStore>, CassandraTriggerStoreError> {
    let store = CassandraStore::new(config).await?;
    let cassandra =
        CassandraTriggerStore::with_store(store, &config.keyspace, segment, timer_relation).await?;
    Ok(TableAdapter::new(cassandra))
}

/// Factory holding shared Cassandra resources for creating per-segment stores.
///
/// Each call to `create_store` produces a `CassandraTriggerStore` with its own
/// independent `state_cache` but sharing the Cassandra session and prepared
/// statements.
#[derive(Clone)]
pub struct CassandraTriggerStoreProvider {
    store: CassandraStore,
    queries: Arc<Queries>,
    timer_relation: SpanRelation,
}

impl CassandraTriggerStoreProvider {
    /// Creates a new provider from an existing store setup.
    ///
    /// # Arguments
    ///
    /// * `store` - Shared Cassandra session
    /// * `queries` - Shared prepared statements
    /// * `timer_relation` - Span relation for timer execution spans
    #[must_use]
    pub fn new(store: CassandraStore, queries: Arc<Queries>, timer_relation: SpanRelation) -> Self {
        Self {
            store,
            queries,
            timer_relation,
        }
    }

    /// Creates a new provider by preparing queries against an existing
    /// `CassandraStore`.
    ///
    /// This is the high-level constructor used by `StorePair` to create the
    /// provider from raw configuration. Queries are prepared once and shared
    /// across all stores created by this provider.
    ///
    /// # Errors
    ///
    /// Returns error if query preparation fails.
    pub async fn with_store(
        store: CassandraStore,
        keyspace: &str,
        timer_relation: SpanRelation,
    ) -> Result<Self, CassandraTriggerStoreError> {
        let queries = Arc::new(Queries::new(store.session(), keyspace).await?);
        Ok(Self {
            store,
            queries,
            timer_relation,
        })
    }
}

impl TriggerStoreProvider for CassandraTriggerStoreProvider {
    type Store = TableAdapter<CassandraTriggerStore>;

    fn create_store(&self, segment: Segment) -> Self::Store {
        TableAdapter::new(CassandraTriggerStore::with_shared(
            self.store.clone(),
            Arc::clone(&self.queries),
            segment,
            self.timer_relation,
        ))
    }
}
