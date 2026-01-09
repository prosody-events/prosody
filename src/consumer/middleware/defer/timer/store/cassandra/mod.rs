//! Cassandra-based implementation of [`TimerDeferStore`].
//!
//! Provides persistent storage for deferred timers using Apache Cassandra
//! with automatic schema migration and optimized TTL management.
//!
//! # Year 2038 Limitation
//!
//! `original_time` is stored as Cassandra `int` (i32) but represents a u32
//! Unix timestamp. Values ≥2^31 (2038-01-19 03:14:07 UTC) are stored as
//! negative i32. Cassandra sorts negative before positive, so:
//!
//! - **Pre-2038 only**: Correct order (all positive i32, ascending)
//! - **Post-2038 only**: Correct order (all negative i32, ascending within
//!   negatives)
//! - **Spanning 2038**: **Incorrect** - post-2038 timers (negative) sort before
//!   pre-2038 (positive)
//!
//! **Why acceptable**: Timer defer handles retry delays (seconds to hours).
//! A single key having deferred timers that span the 2038 boundary requires
//! timers to remain deferred for 12+ years - not a realistic scenario.
//!
//! See [`get_slab_range`](crate::timers::store::cassandra) for the two-query
//! wrap-around solution if this limitation ever needs addressing.

use crate::cassandra::CassandraStore;
use crate::cassandra::errors::CassandraStoreError;
use crate::consumer::middleware::defer::error::CassandraDeferStoreError;
use crate::consumer::middleware::defer::segment::{CassandraSegmentStore, LazySegment};
use crate::consumer::middleware::defer::timer::store::TimerDeferStore;
use crate::consumer::middleware::defer::timer::store::cassandra::queries::Queries;
use crate::consumer::middleware::defer::timer::store::provider::TimerDeferStoreProvider;
use crate::timers::datetime::CompactDateTime;
use crate::timers::{TimerType, Trigger};
use crate::{ConsumerGroup, Key, Partition, Topic};
use async_stream::try_stream;
use futures::{Stream, TryStreamExt, pin_mut};
use opentelemetry::propagation::{TextMapCompositePropagator, TextMapPropagator};
use scylla::client::session::Session;
use scylla::statement::prepared::PreparedStatement;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use tokio::task::coop::cooperative;
use tracing::{debug, info_span, instrument};
use tracing_opentelemetry::OpenTelemetrySpanExt;

pub mod queries;

pub use queries::Queries as TimerQueries;

/// Cassandra-based implementation of [`TimerDeferStore`].
///
/// Provides persistent storage for deferred timers with automatic TTL
/// management and retry count tracking via static columns.
///
/// # Design
///
/// - **Two tables**: `deferred_segments` (metadata) and `deferred_timers`
///   (data)
/// - **Partition key**: `(segment_id, key)` in timers table
/// - **Clustering column**: `original_time ASC` ensures FIFO processing
/// - **Segment ID**: `UUIDv5` hash of `{topic}/{partition}:{consumer_group}`
/// - **Retry count**: Stored as static column (shared across all timers for a
///   key)
/// - **TTL management**: Uses `CassandraStore::calculate_ttl(original_time)`
/// - **Span storage**: `frozen<map<text, text>>` for OpenTelemetry W3C context
///
/// # Lazy Initialization
///
/// The store uses a [`LazySegment`] which defers segment persistence to
/// Cassandra until first use. This allows the store to be created in
/// synchronous context (e.g., `handler_for_partition`) while deferring I/O to
/// the first async operation.
#[derive(Clone)]
pub struct CassandraTimerDeferStore {
    store: CassandraStore,
    queries: Arc<Queries>,
    segment: LazySegment<CassandraSegmentStore>,
}

impl CassandraTimerDeferStore {
    /// Creates a new Cassandra timer defer store for the given partition.
    ///
    /// The `segment_store` is used to persist segment metadata on first access.
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

    /// Returns a reference to the Cassandra session.
    fn session(&self) -> &Session {
        self.store.session()
    }

    /// Returns a reference to the OpenTelemetry propagator.
    fn propagator(&self) -> &TextMapCompositePropagator {
        self.store.propagator()
    }

    /// Gets the segment ID, lazily initializing and persisting if needed.
    async fn segment_id(&self) -> Result<uuid::Uuid, CassandraDeferStoreError> {
        let segment = self.segment.get().await?;
        Ok(segment.id())
    }

    /// Injects span context into a map for storage.
    fn inject_span_context(&self, trigger: &Trigger) -> HashMap<String, String> {
        let span = trigger.span();
        let context = span.context();
        let mut span_map: HashMap<String, String> = HashMap::with_capacity(2);
        self.propagator().inject_context(&context, &mut span_map);
        span_map
    }

    /// Extracts span context from stored map and creates a new linked span.
    fn extract_and_create_span(
        &self,
        key: &Key,
        time: CompactDateTime,
        span_map: &HashMap<String, String>,
    ) -> tracing::Span {
        let context = self.propagator().extract(span_map);
        let span = info_span!(
            "timer_defer.load",
            key = %key,
            time = %time,
        );
        let _ = span.set_parent(context);
        span
    }
}

impl fmt::Debug for CassandraTimerDeferStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CassandraTimerDeferStore")
            .field("segment", &self.segment)
            .finish_non_exhaustive()
    }
}

impl TimerDeferStore for CassandraTimerDeferStore {
    type Error = CassandraDeferStoreError;

    #[instrument(level = "debug", skip(self), err)]
    async fn defer_first_timer(&self, trigger: &Trigger) -> Result<(), Self::Error> {
        let segment_id = self.segment_id().await?;
        let ttl = self.store.calculate_ttl(trigger.time);
        let span_map = self.inject_span_context(trigger);

        self.session()
            .execute_unpaged(
                &self.queries.insert_deferred_timer_with_retry_count,
                (
                    &segment_id,
                    trigger.key.as_ref(),
                    trigger.time,
                    &span_map,
                    0_i32,
                    ttl,
                ),
            )
            .await
            .map_err(CassandraStoreError::from)?;

        debug!(
            key = ?trigger.key,
            time = %trigger.time,
            "Stored first deferred timer"
        );

        Ok(())
    }

    // Note: This query relies on Cassandra's ASC clustering order to return the
    // oldest timer. CompactDateTime (u32) is stored as i32 in Cassandra, causing
    // values >= 2^31 (year 2038+) to appear negative. Since Cassandra sorts
    // negative values before positive, a key with timers spanning the 2038
    // boundary would return post-2038 timers first (incorrect FIFO order).
    //
    // This is acceptable because timer defer handles retry delays (seconds to
    // hours), not multi-year spans. A single key spanning 2038 is not a
    // realistic scenario. If needed, see `get_slab_range` in the timer store
    // for the two-query wrap-around solution.
    #[instrument(level = "debug", skip(self), err)]
    async fn get_next_deferred_timer(
        &self,
        key: &Key,
    ) -> Result<Option<(Trigger, u32)>, Self::Error> {
        let segment_id = self.segment_id().await?;

        let result = self
            .session()
            .execute_unpaged(
                &self.queries.get_next_deferred_timer,
                (&segment_id, key.as_ref()),
            )
            .await
            .map_err(CassandraStoreError::from)?;

        let row_opt = result
            .into_rows_result()
            .map_err(CassandraStoreError::from)?
            .maybe_first_row::<(
                Option<CompactDateTime>,
                Option<HashMap<String, String>>,
                Option<i32>,
            )>()
            .map_err(CassandraStoreError::from)?;

        // Filter out rows where original_time is NULL (only static column set,
        // no clustering rows)
        Ok(
            row_opt.and_then(|(time_opt, span_map_opt, retry_count_opt)| {
                time_opt.map(|time| {
                    let span_map = span_map_opt.unwrap_or_default();
                    let span = self.extract_and_create_span(key, time, &span_map);
                    let retry_count = retry_count_opt.and_then(|c| c.try_into().ok()).unwrap_or(0);

                    let trigger = Trigger::new(key.clone(), time, TimerType::Application, span);

                    (trigger, retry_count)
                })
            }),
        )
    }

    #[instrument(level = "debug", skip(self), err)]
    async fn append_deferred_timer(&self, trigger: &Trigger) -> Result<(), Self::Error> {
        let segment_id = self.segment_id().await?;
        let ttl = self.store.calculate_ttl(trigger.time);
        let span_map = self.inject_span_context(trigger);

        // Don't include retry_count in INSERT - leaves static column unchanged
        self.session()
            .execute_unpaged(
                &self.queries.insert_deferred_timer_without_retry_count,
                (
                    &segment_id,
                    trigger.key.as_ref(),
                    trigger.time,
                    &span_map,
                    ttl,
                ),
            )
            .await
            .map_err(CassandraStoreError::from)?;

        debug!(
            key = ?trigger.key,
            time = %trigger.time,
            "Appended additional deferred timer"
        );

        Ok(())
    }

    #[instrument(level = "debug", skip(self), err)]
    async fn remove_deferred_timer(
        &self,
        key: &Key,
        time: CompactDateTime,
    ) -> Result<(), Self::Error> {
        let segment_id = self.segment_id().await?;

        self.session()
            .execute_unpaged(
                &self.queries.remove_deferred_timer,
                (&segment_id, key.as_ref(), time),
            )
            .await
            .map_err(CassandraStoreError::from)?;

        debug!(
            key = ?key,
            time = %time,
            "Removed deferred timer"
        );

        Ok(())
    }

    #[instrument(level = "debug", skip(self), err)]
    async fn set_retry_count(&self, key: &Key, retry_count: u32) -> Result<(), Self::Error> {
        let segment_id = self.segment_id().await?;
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

        debug!(
            key = ?key,
            retry_count = retry_count,
            "Updated retry count"
        );

        Ok(())
    }

    #[instrument(level = "debug", skip(self), err)]
    async fn delete_key(&self, key: &Key) -> Result<(), Self::Error> {
        let segment_id = self.segment_id().await?;

        self.session()
            .execute_unpaged(&self.queries.delete_key, (&segment_id, key.as_ref()))
            .await
            .map_err(CassandraStoreError::from)?;

        debug!(
            key = ?key,
            "Deleted key from timer defer store"
        );

        Ok(())
    }

    fn deferred_times(
        &self,
        key: &Key,
    ) -> impl Stream<Item = Result<CompactDateTime, Self::Error>> + Send + 'static {
        deferred_times_stream(
            self.store.clone(),
            self.queries.get_deferred_times.clone(),
            self.segment.clone(),
            key.clone(),
        )
    }
}

/// Helper to create deferred times stream without capturing `&self`.
fn deferred_times_stream(
    store: CassandraStore,
    query: PreparedStatement,
    segment: LazySegment<CassandraSegmentStore>,
    key: Key,
) -> impl Stream<Item = Result<CompactDateTime, CassandraDeferStoreError>> + Send + 'static {
    try_stream! {
        let seg = segment.get().await?;
        let segment_id = seg.id();

        let stream = store.session()
            .execute_iter(query, (&segment_id, key.as_ref()))
            .await
            .map_err(CassandraStoreError::from)?
            .rows_stream::<(Option<CompactDateTime>,)>()
            .map_err(CassandraStoreError::from)?;

        pin_mut!(stream);
        while let Some((time_opt,)) = cooperative(stream.try_next())
            .await
            .map_err(CassandraStoreError::from)?
        {
            if let Some(time) = time_opt {
                yield time;
            }
        }
    }
}

/// Provider for creating [`CassandraTimerDeferStore`] instances.
///
/// Holds shared resources (Cassandra session, prepared queries, segment store)
/// and creates store instances with the correct segment context for each
/// partition.
///
/// # Usage
///
/// ```text
/// // Create provider once at startup
/// let provider = CassandraTimerDeferStoreProvider::new(
///     cassandra_store,
///     queries,
///     segment_store,
/// );
///
/// // Create stores for each partition as needed
/// let store = provider.create_store(topic, partition, &consumer_group);
/// ```
#[derive(Clone, Debug)]
pub struct CassandraTimerDeferStoreProvider {
    store: CassandraStore,
    queries: Arc<Queries>,
    segment_store: CassandraSegmentStore,
}

impl CassandraTimerDeferStoreProvider {
    /// Creates a new provider with the given Cassandra resources.
    ///
    /// # Arguments
    ///
    /// * `store` - Shared Cassandra store
    /// * `queries` - Pre-prepared timer defer queries
    /// * `segment_store` - Segment store for persisting segment metadata
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

impl TimerDeferStoreProvider for CassandraTimerDeferStoreProvider {
    type Store = CassandraTimerDeferStore;

    fn create_store(
        &self,
        topic: Topic,
        partition: Partition,
        consumer_group: &str,
    ) -> Self::Store {
        CassandraTimerDeferStore::new(
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

    // Note: These tests require a running Cassandra instance
    // Run with: cargo test timer_defer_cassandra -- --ignored

    #[tokio::test]
    async fn test_cassandra_timer_defer_store() -> color_eyre::Result<()> {
        let config = CassandraConfiguration::builder()
            .nodes(vec!["localhost:9042".to_owned()])
            .keyspace("prosody_test".to_owned())
            .build()
            .map_err(|e| color_eyre::eyre::eyre!("Config build failed: {e}"))?;

        let cassandra_store = CassandraStore::new(&config).await?;
        let segment_store =
            CassandraSegmentStore::new(cassandra_store.clone(), "prosody_test").await?;
        let queries = Arc::new(Queries::new(cassandra_store.session(), "prosody_test").await?);
        let defer_store = CassandraTimerDeferStore::new(
            cassandra_store,
            queries,
            segment_store,
            Topic::from("test-topic"),
            Partition::from(0_i32),
            Arc::from("test-consumer-group") as ConsumerGroup,
        );

        // Test basic operations
        let key: Key = Arc::from("test-key");
        let time = CompactDateTime::from(1000_u32);
        let trigger = Trigger::new(
            key.clone(),
            time,
            TimerType::Application,
            tracing::Span::current(),
        );

        // Initially not deferred
        assert!(defer_store.is_deferred(&key).await?.is_none());

        // Defer first timer
        defer_store.defer_first_timer(&trigger).await?;

        // Now should be deferred
        assert_eq!(defer_store.is_deferred(&key).await?, Some(0));

        // Get next timer
        let (retrieved, retry_count) = defer_store
            .get_next_deferred_timer(&key)
            .await?
            .ok_or_else(|| color_eyre::eyre::eyre!("expected timer"))?;
        assert_eq!(retrieved.time, time);
        assert_eq!(retry_count, 0);

        // Clean up
        defer_store.delete_key(&key).await?;
        assert!(defer_store.is_deferred(&key).await?.is_none());

        Ok(())
    }
}
