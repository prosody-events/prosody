//! Cassandra-backed timer defer store with TTL and span context preservation.
//!
//! # Year 2038 Note
//!
//! `original_time` (u32 Unix timestamp) is stored as Cassandra `int` (i32).
//! Post-2038 values appear negative and sort before positive. This only affects
//! keys with timers spanning the 2038 boundary - unrealistic for retry delays.

use crate::cassandra::CassandraStore;
use crate::cassandra::errors::CassandraStoreError;
use crate::consumer::middleware::defer::error::CassandraDeferStoreError;
use crate::consumer::middleware::defer::segment::{CassandraSegmentStore, LazySegment};
use crate::consumer::middleware::defer::timer::store::TimerDeferStore;
use crate::consumer::middleware::defer::timer::store::cassandra::queries::Queries;
use crate::consumer::middleware::defer::timer::store::provider::TimerDeferStoreProvider;
use crate::otel::SpanRelation;
use crate::related_span;
use crate::timers::datetime::CompactDateTime;
use crate::timers::{TimerType, Trigger};
use crate::{ConsumerGroup, Key, Partition, Topic};
use futures::TryStreamExt;
use opentelemetry::propagation::{TextMapCompositePropagator, TextMapPropagator};
use scylla::client::session::Session;
use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::sync::Arc;
use tracing::{debug, instrument};
use tracing_opentelemetry::OpenTelemetrySpanExt;

pub mod queries;

pub use queries::Queries as TimerQueries;

/// Cassandra-backed timer defer store.
///
/// # Storage Model
///
/// - **Partition key**: `(segment_id, key)` where segment = `UUIDv5` of
///   `{topic}/{partition}:{consumer_group}`
/// - **Clustering**: `original_time ASC` for FIFO ordering
/// - **Retry count**: Static column (shared per key)
/// - **Span context**: `frozen<map<text, text>>` (W3C trace format)
/// - **TTL**: Time-based via [`CassandraStore::calculate_ttl()`]
///
/// Uses [`LazySegment`] to defer segment persistence until first access.
#[derive(Clone)]
pub struct CassandraTimerDeferStore {
    store: CassandraStore,
    queries: Arc<Queries>,
    segment: LazySegment<CassandraSegmentStore>,
    timer_spans: SpanRelation,
}

impl CassandraTimerDeferStore {
    /// Creates a store; segment persisted lazily on first access.
    #[must_use]
    pub fn new(
        store: CassandraStore,
        queries: Arc<Queries>,
        segment_store: CassandraSegmentStore,
        topic: Topic,
        partition: Partition,
        consumer_group: ConsumerGroup,
        timer_spans: SpanRelation,
    ) -> Self {
        let segment = LazySegment::new(segment_store, topic, partition, consumer_group);
        Self {
            store,
            queries,
            segment,
            timer_spans,
        }
    }

    fn session(&self) -> &Session {
        self.store.session()
    }

    fn propagator(&self) -> &TextMapCompositePropagator {
        self.store.propagator()
    }

    async fn segment_id(&self) -> Result<uuid::Uuid, CassandraDeferStoreError> {
        let segment = self.segment.get().await?;
        Ok(segment.id())
    }

    /// Serializes span context to W3C trace format for storage.
    fn inject_span_context(&self, trigger: &Trigger) -> HashMap<String, String> {
        let span = trigger.span();
        let context = span.context();
        let mut span_map: HashMap<String, String> = HashMap::with_capacity(2);
        self.propagator().inject_context(&context, &mut span_map);
        span_map
    }

    /// Deserializes span context and creates linked span.
    fn extract_and_create_span(
        &self,
        key: &Key,
        time: CompactDateTime,
        span_map: &HashMap<String, String>,
    ) -> tracing::Span {
        let context = self.propagator().extract(span_map);
        related_span!(self.timer_spans, context, "timer_defer.load", key = %key, time = %time, cached = false)
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

    // See module docs for year 2038 limitation discussion.
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

        // NULL time means static column exists but no clustering rows
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

    fn deferred_times(
        &self,
        key: &Key,
    ) -> impl Future<Output = Result<Vec<CompactDateTime>, Self::Error>> + Send + 'static {
        let store = self.store.clone();
        let query = self.queries.get_deferred_times.clone();
        let segment = self.segment.clone();
        let key = key.clone();

        async move {
            let seg = segment.get().await?;
            let segment_id = seg.id();

            store
                .session()
                .execute_iter(query, (&segment_id, key.as_ref()))
                .await
                .map_err(CassandraStoreError::from)?
                .rows_stream::<(Option<CompactDateTime>,)>()
                .map_err(CassandraStoreError::from)?
                .try_filter_map(|(time_opt,)| async move { Ok(time_opt) })
                .try_collect()
                .await
                .map_err(CassandraStoreError::from)
                .map_err(Self::Error::from)
        }
    }

    #[instrument(level = "debug", skip(self), err)]
    async fn append_deferred_timer(&self, trigger: &Trigger) -> Result<(), Self::Error> {
        let segment_id = self.segment_id().await?;
        let ttl = self.store.calculate_ttl(trigger.time);
        let span_map = self.inject_span_context(trigger);

        // Omitting retry_count preserves static column
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
        let retry_count: i32 = retry_count.try_into().unwrap_or(i32::MAX);

        self.session()
            .execute_unpaged(
                &self.queries.update_retry_count,
                (ttl, retry_count, &segment_id, key.as_ref()),
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
}

/// Factory for partition-scoped Cassandra timer defer stores.
///
/// Holds shared Cassandra resources; creates stores with correct segment
/// context per partition.
#[derive(Clone, Debug)]
pub struct CassandraTimerDeferStoreProvider {
    store: CassandraStore,
    queries: Arc<Queries>,
    segment_store: CassandraSegmentStore,
    timer_spans: SpanRelation,
}

impl CassandraTimerDeferStoreProvider {
    /// Creates a provider with shared Cassandra resources.
    #[must_use]
    pub fn new(
        store: CassandraStore,
        queries: Arc<Queries>,
        segment_store: CassandraSegmentStore,
        timer_spans: SpanRelation,
    ) -> Self {
        Self {
            store,
            queries,
            segment_store,
            timer_spans,
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
            self.timer_spans,
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
            SpanRelation::default(),
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
