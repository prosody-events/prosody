//! Cassandra-based implementation of [`TimerDeferStore`].
//!
//! Provides persistent storage for deferred timers using Apache Cassandra
//! with automatic schema migration and optimized TTL management.

use crate::Key;
use crate::cassandra::CassandraStore;
use crate::cassandra::errors::CassandraStoreError;
use crate::consumer::middleware::defer::segment::SegmentId;
use crate::consumer::middleware::defer::timer::store::TimerDeferStore;
use crate::consumer::middleware::defer::timer::store::cassandra::queries::Queries;
use crate::consumer::middleware::{ClassifyError, ErrorCategory};
use crate::timers::datetime::CompactDateTime;
use crate::timers::{TimerType, Trigger};
use async_stream::try_stream;
use futures::{Stream, TryStreamExt, pin_mut};
use opentelemetry::propagation::{TextMapCompositePropagator, TextMapPropagator};
use scylla::client::session::Session;
use scylla::statement::prepared::PreparedStatement;
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;
use tokio::task::coop::cooperative;
use tracing::{debug, info_span, instrument};
use tracing_opentelemetry::OpenTelemetrySpanExt;

pub mod provider;
mod queries;

pub use provider::CassandraTimerDeferStoreProvider;

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
///   (provided via
///   [`Segment`](crate::consumer::middleware::defer::segment::Segment))
/// - **Retry count**: Stored as static column (shared across all timers for a
///   key)
/// - **TTL management**: Uses `CassandraStore::calculate_ttl(original_time)`
/// - **Span storage**: `frozen<map<text, text>>` for OpenTelemetry W3C context
#[derive(Clone, Debug)]
pub struct CassandraTimerDeferStore {
    store: CassandraStore,
    queries: Arc<Queries>,
    segment_id: SegmentId,
}

impl CassandraTimerDeferStore {
    /// Returns a reference to the Cassandra session.
    fn session(&self) -> &Session {
        self.store.session()
    }

    /// Returns a reference to the OpenTelemetry propagator.
    fn propagator(&self) -> &TextMapCompositePropagator {
        self.store.propagator()
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

impl TimerDeferStore for CassandraTimerDeferStore {
    type Error = CassandraTimerDeferStoreError;

    #[instrument(level = "debug", skip(self), err)]
    async fn defer_first_timer(&self, trigger: &Trigger) -> Result<(), Self::Error> {
        let ttl = self.store.calculate_ttl(trigger.time);
        let span_map = self.inject_span_context(trigger);

        self.session()
            .execute_unpaged(
                &self.queries.insert_deferred_timer_with_retry_count,
                (
                    &self.segment_id,
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
        let result = self
            .session()
            .execute_unpaged(
                &self.queries.get_next_deferred_timer,
                (&self.segment_id, key.as_ref()),
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
        let ttl = self.store.calculate_ttl(trigger.time);
        let span_map = self.inject_span_context(trigger);

        // Don't include retry_count in INSERT - leaves static column unchanged
        self.session()
            .execute_unpaged(
                &self.queries.insert_deferred_timer_without_retry_count,
                (
                    &self.segment_id,
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
        self.session()
            .execute_unpaged(
                &self.queries.remove_deferred_timer,
                (&self.segment_id, key.as_ref(), time),
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
        let ttl = self.store.base_ttl();
        let retry_count_i32: i32 = retry_count.try_into().map_err(|_| {
            CassandraTimerDeferStoreError::InvalidRetryCount {
                retry_count,
                reason: "retry count exceeds i32::MAX",
            }
        })?;

        self.session()
            .execute_unpaged(
                &self.queries.update_retry_count,
                (ttl, retry_count_i32, &self.segment_id, key.as_ref()),
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
        self.session()
            .execute_unpaged(&self.queries.delete_key, (&self.segment_id, key.as_ref()))
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
            self.segment_id,
            key.clone(),
        )
    }
}

/// Helper to create deferred times stream without capturing `&self`.
fn deferred_times_stream(
    store: CassandraStore,
    query: PreparedStatement,
    segment_id: SegmentId,
    key: Key,
) -> impl Stream<Item = Result<CompactDateTime, CassandraTimerDeferStoreError>> + Send + 'static {
    try_stream! {
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

/// Errors that can occur in Cassandra timer defer store operations.
#[derive(Debug, Error)]
pub enum CassandraTimerDeferStoreError {
    /// Error from Cassandra operations.
    #[error("cassandra error: {0:#}")]
    Cassandra(#[from] CassandraStoreError),

    /// Invalid retry count value.
    #[error("invalid retry count {retry_count}: {reason}")]
    InvalidRetryCount {
        /// The invalid retry count value.
        retry_count: u32,
        /// Why the value is invalid.
        reason: &'static str,
    },
}

impl ClassifyError for CassandraTimerDeferStoreError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            // Delegate Cassandra errors to their classification
            Self::Cassandra(error) => error.classify_error(),

            // Invalid retry count is a programming error - terminal
            Self::InvalidRetryCount { .. } => ErrorCategory::Terminal,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cassandra::{CassandraConfiguration, CassandraStore};
    use crate::consumer::middleware::defer::segment::Segment;
    use crate::consumer::middleware::defer::timer::store::TimerDeferStoreProvider;
    use crate::{ConsumerGroup, Partition, Topic};

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
        let provider =
            CassandraTimerDeferStoreProvider::with_store(cassandra_store, "prosody_test").await?;
        let segment = Segment::new(
            Topic::from("test-topic"),
            Partition::from(0_i32),
            Arc::from("test-consumer-group") as ConsumerGroup,
        );
        let defer_store = provider.create_store(&segment).await?;

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
