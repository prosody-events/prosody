//! Cassandra-backed timer defer store with internal write-through cache.
//!
//! Eliminates tombstone reads by maintaining a `next_timer` static UDT column
//! that always equals the minimum live timer row. `get_next` becomes a single
//! static-column read with zero clustering scan.
//!
//! # Year 2038 Note
//!
//! `original_time` (u32 Unix timestamp) is stored as Cassandra `int` (i32).
//! Post-2038 values appear negative and sort before positive. This only affects
//! keys with timers spanning the 2038 boundary — unrealistic for retry delays.

use crate::cassandra::CassandraStore;
use crate::cassandra::errors::CassandraStoreError;
use crate::consumer::middleware::defer::error::CassandraDeferStoreError;
use crate::consumer::middleware::defer::segment::{CassandraSegmentStore, LazySegment};
use crate::consumer::middleware::defer::timer::store::cassandra::queries::{
    DeferredNextTimer, Queries,
};
use crate::consumer::middleware::defer::timer::store::provider::TimerDeferStoreProvider;
use crate::consumer::middleware::defer::timer::store::{
    CachedTimerEntry, TimerDeferStore, TimerRetryCompletionResult,
};
use crate::otel::SpanRelation;
use crate::related_span;
use crate::timers::datetime::CompactDateTime;
use crate::timers::{TimerType, Trigger};
use crate::{ConsumerGroup, Key, Partition, Topic};
use futures::TryStreamExt;
use opentelemetry::Context;
use opentelemetry::propagation::{TextMapCompositePropagator, TextMapPropagator};
use quick_cache::sync::Cache;
use scylla::client::session::Session;
use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::sync::Arc;
use tracing::{debug, instrument};
use tracing_opentelemetry::OpenTelemetrySpanExt;

pub mod queries;

pub use queries::DeferredNextTimer as TimerNextHint;
pub use queries::Queries as TimerQueries;

const TIMER_DEFER_CACHE_CAPACITY: usize = 8_192;

/// Cassandra-backed timer defer store with internal write-through cache.
///
/// # Storage Model
///
/// - **Partition key**: `(segment_id, key)` where segment = `UUIDv5` of
///   `{topic}/{partition}:{consumer_group}`
/// - **Clustering**: `original_time ASC` for FIFO ordering
/// - **Static columns**: `next_timer frozen<deferred_next_timer>`, `retry_count
///   int`
/// - **Span context**: `frozen<map<text, text>>` on each clustering row (W3C)
/// - **TTL**: Time-based via [`CassandraStore::calculate_ttl()`]
///
/// `next_timer` strictly encodes the minimum live timer (I1). The UDT bundles
/// `time` and `span` atomically so they can never drift (I4).
#[derive(Clone)]
pub struct CassandraTimerDeferStore {
    store: CassandraStore,
    queries: Arc<Queries>,
    segment: LazySegment<CassandraSegmentStore>,
    timer_spans: SpanRelation,
    /// Write-through cache: `key → Option<CachedTimerEntry>`.
    /// `Some(None)` = known-empty; `Some(Some(_))` = live; `None` = unknown.
    cache: Arc<Cache<Key, Option<CachedTimerEntry>>>,
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
            cache: Arc::new(Cache::new(TIMER_DEFER_CACHE_CAPACITY)),
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

    /// Deserializes span context from a span map and creates a linked span.
    fn extract_context(&self, span_map: &HashMap<String, String>) -> Context {
        self.propagator().extract(span_map)
    }

    /// Serializes a cached [`Context`] back into the W3C span-map storage form.
    fn span_map_from_context(&self, context: &Context) -> HashMap<String, String> {
        let mut span_map: HashMap<String, String> = HashMap::with_capacity(2);
        self.propagator().inject_context(context, &mut span_map);
        span_map
    }

    /// Creates a linked span from a stored context.
    /// `cached` mirrors the old `CachedTimerDeferStore` attribution: `true`
    /// when served from the in-memory write-through cache, `false` on a DB
    /// read.
    fn create_span_from_context(
        &self,
        key: &Key,
        time: CompactDateTime,
        context: &Context,
        cached: bool,
    ) -> tracing::Span {
        related_span!(
            self.timer_spans,
            context.clone(),
            "timer_defer.load",
            key = %key,
            time = %time,
            cached = cached
        )
    }

    /// Reads `(next_timer, retry_count)` static columns from the DB.
    async fn read_next_static(
        &self,
        segment_id: &uuid::Uuid,
        key: &Key,
    ) -> Result<Option<CachedTimerEntry>, CassandraDeferStoreError> {
        let result = self
            .session()
            .execute_unpaged(&self.queries.get_next_static, (segment_id, key.as_ref()))
            .await
            .map_err(CassandraStoreError::from)?;

        let row_opt = result
            .into_rows_result()
            .map_err(CassandraStoreError::from)?
            .maybe_first_row::<(Option<DeferredNextTimer>, Option<i32>)>()
            .map_err(CassandraStoreError::from)?;

        Ok(row_opt.and_then(|(udt_opt, retry_opt)| {
            udt_opt.map(|udt| {
                let context = self.extract_context(&udt.span);
                let retry_count = retry_opt.and_then(|c| c.try_into().ok()).unwrap_or(0);
                CachedTimerEntry {
                    time: udt.time,
                    context,
                    retry_count,
                }
            })
        }))
    }

    /// Probes for the first clustering row strictly after `after_time`.
    async fn probe_next(
        &self,
        segment_id: &uuid::Uuid,
        key: &Key,
        after_time: CompactDateTime,
    ) -> Result<Option<DeferredNextTimer>, CassandraDeferStoreError> {
        let result = self
            .session()
            .execute_unpaged(
                &self.queries.probe_next,
                (segment_id, key.as_ref(), after_time),
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

        Ok(row_opt.and_then(|(time_opt, span_opt, _)| {
            time_opt.map(|time| DeferredNextTimer {
                time,
                span: span_opt.unwrap_or_default(),
            })
        }))
    }

    /// Resolves cache entry; falls back to a static-column DB read on miss.
    async fn resolve_cache_or_read(
        &self,
        key: &Key,
    ) -> Result<(uuid::Uuid, Option<CachedTimerEntry>), CassandraDeferStoreError> {
        let segment_id = self.segment_id().await?;
        if let Some(cached) = self.cache.get(key.as_ref()) {
            return Ok((segment_id, cached));
        }
        let db_val = self.read_next_static(&segment_id, key).await?;
        Ok((segment_id, db_val))
    }

    /// Converts a `DeferredNextTimer` UDT into a `CachedTimerEntry`.
    fn udt_to_cache_entry(&self, udt: &DeferredNextTimer) -> CachedTimerEntry {
        let context = self.extract_context(&udt.span);
        CachedTimerEntry {
            time: udt.time,
            context,
            retry_count: 0,
        }
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
        // Consult current state so we don't violate I1 when the caller's
        // "fresh key" precondition is violated: the new row must not raise
        // `next_timer` above a lower live time that already exists.
        let (segment_id, cached) = self.resolve_cache_or_read(&trigger.key).await?;
        let ttl = self.store.calculate_ttl(trigger.time);
        let span_map = self.inject_span_context(trigger);

        let (next_time, next_span_map) = match &cached {
            Some(entry) if entry.time < trigger.time => {
                // A lower live time already exists: keep it as next_timer.
                // Reconstruct its span map from the cached context.
                (entry.time, self.span_map_from_context(&entry.context))
            }
            _ => (trigger.time, span_map.clone()),
        };
        let next_timer = DeferredNextTimer {
            time: next_time,
            span: next_span_map,
        };

        self.session()
            .execute_unpaged(
                &self.queries.insert_deferred_timer_with_retry_count,
                (
                    &segment_id,
                    trigger.key.as_ref(),
                    trigger.time,
                    &span_map,
                    0_i32,
                    &next_timer,
                    ttl,
                ),
            )
            .await
            .map_err(CassandraStoreError::from)?;

        let context = self.extract_context(&next_timer.span);
        self.cache.insert(
            Arc::clone(&trigger.key),
            Some(CachedTimerEntry {
                time: next_time,
                context,
                retry_count: 0,
            }),
        );

        debug!(key = ?trigger.key, time = %trigger.time, "Stored first deferred timer");
        Ok(())
    }

    #[instrument(level = "debug", skip(self), err)]
    async fn get_next_deferred_timer(
        &self,
        key: &Key,
    ) -> Result<Option<(Trigger, u32)>, Self::Error> {
        // Cache hit — span is attributed as cached (mirrors old CachedTimerDeferStore)
        if let Some(cached) = self.cache.get(key.as_ref()) {
            return Ok(cached.map(|entry| {
                let span = self.create_span_from_context(key, entry.time, &entry.context, true);
                let trigger = Trigger::new(key.clone(), entry.time, TimerType::Application, span);
                (trigger, entry.retry_count)
            }));
        }

        // Cache miss: single static-column + UDT read, zero clustering scan
        let segment_id = self.segment_id().await?;
        let entry_opt = self.read_next_static(&segment_id, key).await?;
        self.cache.insert(Arc::clone(key), entry_opt.clone());

        Ok(entry_opt.map(|entry| {
            let span = self.create_span_from_context(key, entry.time, &entry.context, false);
            let trigger = Trigger::new(key.clone(), entry.time, TimerType::Application, span);
            (trigger, entry.retry_count)
        }))
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
    async fn complete_retry_success(
        &self,
        key: &Key,
        time: CompactDateTime,
    ) -> Result<TimerRetryCompletionResult, Self::Error> {
        let (segment_id, cached) = self.resolve_cache_or_read(key).await?;
        let ttl = self.store.base_ttl();

        let cur_next_time = cached.as_ref().map(|e| e.time);

        if cur_next_time == Some(time) {
            // FIFO hot path: probe for the successor before deleting
            let found = self.probe_next(&segment_id, key, time).await?;
            if let Some(next_udt) = found {
                self.session()
                    .execute_unpaged(
                        &self.queries.batch_complete_retry,
                        (
                            &segment_id,
                            key.as_ref(),
                            time,
                            ttl,
                            &next_udt,
                            &segment_id,
                            key.as_ref(),
                        ),
                    )
                    .await
                    .map_err(CassandraStoreError::from)?;

                let entry = self.udt_to_cache_entry(&next_udt);
                let next_time = entry.time;
                let context = entry.context.clone();
                self.cache.insert(Arc::clone(key), Some(entry));

                debug!(key = ?key, time = %time, next_time = %next_time, "Completed FIFO timer retry");
                Ok(TimerRetryCompletionResult::MoreTimers { next_time, context })
            } else {
                self.delete_key(key).await?;
                debug!(key = ?key, time = %time, "Completed last timer retry");
                Ok(TimerRetryCompletionResult::Completed)
            }
        } else {
            // Non-FIFO path: offset is not the minimum; leave next_timer alone
            self.session()
                .execute_unpaged(
                    &self.queries.batch_complete_retry_no_advance,
                    (
                        &segment_id,
                        key.as_ref(),
                        time, // DELETE params
                        ttl,
                        &segment_id,
                        key.as_ref(), // UPDATE retry_count=0 params
                    ),
                )
                .await
                .map_err(CassandraStoreError::from)?;

            // cur_next is unchanged; retry_count reset to 0
            if let Some(entry) = cached {
                let context = entry.context.clone();
                let next_time = entry.time;
                self.cache.insert(
                    Arc::clone(key),
                    Some(CachedTimerEntry {
                        time: next_time,
                        context: context.clone(),
                        retry_count: 0,
                    }),
                );
                Ok(TimerRetryCompletionResult::MoreTimers { next_time, context })
            } else {
                // Empty partition — contract violation, handle gracefully
                let _ = self.cache.remove(key.as_ref());
                Ok(TimerRetryCompletionResult::Completed)
            }
        }
    }

    #[instrument(level = "debug", skip(self), err)]
    async fn append_deferred_timer(&self, trigger: &Trigger) -> Result<(), Self::Error> {
        let (segment_id, cached) = self.resolve_cache_or_read(&trigger.key).await?;
        let ttl = self.store.calculate_ttl(trigger.time);
        let span_map = self.inject_span_context(trigger);

        match &cached {
            None => {
                // Empty partition: INSERT + initialize next_timer in one BATCH so
                // I1 holds from the first row. `retry_count` is untouched — it may
                // already hold an orphan value from a prior `set_retry_count` on
                // this key, which we must preserve. Invalidate the cache so the
                // next read picks up the real static-column values from the DB.
                let new_udt = DeferredNextTimer {
                    time: trigger.time,
                    span: span_map.clone(),
                };
                self.session()
                    .execute_unpaged(
                        &self.queries.batch_append_with_next,
                        (
                            &segment_id,
                            trigger.key.as_ref(),
                            trigger.time,
                            &span_map,
                            ttl, // INSERT params
                            ttl,
                            &new_udt,
                            &segment_id,
                            trigger.key.as_ref(), // UPDATE params
                        ),
                    )
                    .await
                    .map_err(CassandraStoreError::from)?;

                let _ = self.cache.remove(trigger.key.as_ref());
            }
            Some(entry) if trigger.time < entry.time => {
                // Out-of-order: lower next_timer in the same BATCH
                let new_udt = DeferredNextTimer {
                    time: trigger.time,
                    span: span_map.clone(),
                };
                self.session()
                    .execute_unpaged(
                        &self.queries.batch_append_with_next,
                        (
                            &segment_id,
                            trigger.key.as_ref(),
                            trigger.time,
                            &span_map,
                            ttl, // INSERT params
                            ttl,
                            &new_udt,
                            &segment_id,
                            trigger.key.as_ref(), // UPDATE params
                        ),
                    )
                    .await
                    .map_err(CassandraStoreError::from)?;

                let context = self.extract_context(&span_map);
                self.cache.insert(
                    Arc::clone(&trigger.key),
                    Some(CachedTimerEntry {
                        time: trigger.time,
                        context,
                        retry_count: entry.retry_count,
                    }),
                );
            }
            Some(_) => {
                // Monotonic append: current next_timer remains the minimum, just INSERT
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
            }
        }

        debug!(key = ?trigger.key, time = %trigger.time, "Appended additional deferred timer");
        Ok(())
    }

    #[instrument(level = "debug", skip(self), err)]
    async fn remove_deferred_timer(
        &self,
        key: &Key,
        time: CompactDateTime,
    ) -> Result<(), Self::Error> {
        let (segment_id, cached) = self.resolve_cache_or_read(key).await?;
        let ttl = self.store.base_ttl();

        let cur_next_time = cached.as_ref().map(|e| e.time);

        if cur_next_time == Some(time) {
            // Min-removal: must repair next_timer in the same BATCH
            let found = self.probe_next(&segment_id, key, time).await?;
            match found {
                Some(next_udt) => {
                    self.session()
                        .execute_unpaged(
                            &self.queries.batch_remove_and_repair_next,
                            (
                                &segment_id,
                                key.as_ref(),
                                time, // DELETE params
                                ttl,
                                &next_udt,
                                &segment_id,
                                key.as_ref(), // UPDATE params
                            ),
                        )
                        .await
                        .map_err(CassandraStoreError::from)?;

                    let context = self.extract_context(&next_udt.span);
                    let cur_rc = cached.as_ref().map_or(0, |e| e.retry_count);
                    self.cache.insert(
                        Arc::clone(key),
                        Some(CachedTimerEntry {
                            time: next_udt.time,
                            context,
                            retry_count: cur_rc,
                        }),
                    );
                }
                None => {
                    self.delete_key(key).await?;
                }
            }
        } else {
            // Non-min removal: plain DELETE, next_timer unchanged
            self.session()
                .execute_unpaged(
                    &self.queries.remove_deferred_timer,
                    (&segment_id, key.as_ref(), time),
                )
                .await
                .map_err(CassandraStoreError::from)?;
        }

        debug!(key = ?key, time = %time, "Removed deferred timer");
        Ok(())
    }

    #[instrument(level = "debug", skip(self), err)]
    async fn set_retry_count(&self, key: &Key, retry_count: u32) -> Result<(), Self::Error> {
        let segment_id = self.segment_id().await?;
        let ttl = self.store.base_ttl();
        let retry_count_i32: i32 = retry_count.try_into().unwrap_or(i32::MAX);

        self.session()
            .execute_unpaged(
                &self.queries.update_retry_count,
                (ttl, retry_count_i32, &segment_id, key.as_ref()),
            )
            .await
            .map_err(CassandraStoreError::from)?;

        // Update retry_count in cache in-place
        if let Some(Some(entry)) = self.cache.get(key.as_ref()) {
            self.cache.insert(
                Arc::clone(key),
                Some(CachedTimerEntry {
                    time: entry.time,
                    context: entry.context.clone(),
                    retry_count,
                }),
            );
        } else {
            self.cache.remove(key.as_ref());
        }

        debug!(key = ?key, retry_count, "Updated retry count");
        Ok(())
    }

    #[instrument(level = "debug", skip(self), err)]
    async fn delete_key(&self, key: &Key) -> Result<(), Self::Error> {
        let segment_id = self.segment_id().await?;

        self.session()
            .execute_unpaged(&self.queries.delete_key, (&segment_id, key.as_ref()))
            .await
            .map_err(CassandraStoreError::from)?;

        self.cache.insert(Arc::clone(key), None);

        debug!(key = ?key, "Deleted key from timer defer store");
        Ok(())
    }
}

#[cfg(test)]
impl CassandraTimerDeferStore {
    /// Reads `next_timer` UDT directly from Cassandra for I1/I4 invariant
    /// assertions.
    async fn read_next_timer_for_invariant_check(
        &self,
        key: &Key,
    ) -> color_eyre::Result<Option<(CompactDateTime, HashMap<String, String>)>> {
        let segment_id = self
            .segment_id()
            .await
            .map_err(|e| color_eyre::eyre::eyre!("{e}"))?;
        let result = self
            .session()
            .execute_unpaged(&self.queries.get_next_static, (&segment_id, key.as_ref()))
            .await?;
        let row = result
            .into_rows_result()?
            .maybe_first_row::<(Option<DeferredNextTimer>, Option<i32>)>()?;
        Ok(row.and_then(|(udt_opt, _)| udt_opt.map(|udt| (udt.time, udt.span))))
    }
}

/// Factory for partition-scoped Cassandra timer defer stores.
///
/// Each call to [`create_store`](TimerDeferStoreProvider::create_store)
/// produces a [`CassandraTimerDeferStore`] with its **own independent cache**
/// scoped to that partition's lifetime. The Cassandra session and prepared
/// statements are shared across partitions (cheap, read-only), but the
/// write-through cache is **never** shared across partitions — sharing would
/// cause data corruption (stale timer entries for wrong partitions).
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
        // Each call creates a new store with its own fresh cache.
        // The cache must never outlive or be shared across partition assignments.
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
    use crate::{ConsumerGroup, Partition, Topic};

    pub(super) async fn build_test_store() -> color_eyre::Result<CassandraTimerDeferStore> {
        let config = CassandraConfiguration::builder()
            .nodes(vec!["localhost:9042".to_owned()])
            .keyspace("prosody_test".to_owned())
            .build()
            .map_err(|e| color_eyre::eyre::eyre!("Config build failed: {e}"))?;
        let cassandra_store = CassandraStore::new(&config).await?;
        let segment_store =
            CassandraSegmentStore::new(cassandra_store.clone(), "prosody_test").await?;
        let queries = Arc::new(Queries::new(cassandra_store.session(), "prosody_test").await?);
        Ok(CassandraTimerDeferStore::new(
            cassandra_store,
            queries,
            segment_store,
            Topic::from("test-topic"),
            Partition::from(0_i32),
            Arc::from(format!("test-consumer-group-{}", uuid::Uuid::new_v4())) as ConsumerGroup,
            SpanRelation::default(),
        ))
    }

    crate::timer_defer_store_tests!(async { build_test_store().await });

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

        let key: Key = Arc::from("test-key");
        let time = CompactDateTime::from(1000_u32);
        let trigger = Trigger::new(
            key.clone(),
            time,
            TimerType::Application,
            tracing::Span::current(),
        );

        assert!(defer_store.is_deferred(&key).await?.is_none());

        defer_store.defer_first_timer(&trigger).await?;
        assert_eq!(defer_store.is_deferred(&key).await?, Some(0));

        let (retrieved, retry_count) = defer_store
            .get_next_deferred_timer(&key)
            .await?
            .ok_or_else(|| color_eyre::eyre::eyre!("expected timer"))?;
        assert_eq!(retrieved.time, time);
        assert_eq!(retry_count, 0);

        defer_store.delete_key(&key).await?;
        assert!(defer_store.is_deferred(&key).await?.is_none());

        Ok(())
    }
}

/// Invariant tests: directly assert I1 (`next_timer.time` == model minimum) and
/// I4 (`next_timer` present ⟺ live rows present) after every operation.
#[cfg(test)]
mod invariant_tests {
    use super::*;
    use crate::consumer::middleware::defer::timer::store::tests::prop_timer_defer_store::{
        TestKeyComponents, TimerDeferModel, TimerDeferOperation, TimerDeferTestInput,
    };
    use crate::tracing::init_test_logging;
    use quickcheck::{QuickCheck, TestResult};
    use tokio::runtime::Builder;
    use tracing::Instrument;

    /// Asserts I1/I4 after each op.
    #[test]
    fn test_timer_defer_store_i1_i4_invariant() {
        init_test_logging();
        let _span = tracing::info_span!("test_i1_i4").entered();
        QuickCheck::new().quickcheck(prop_i1_i4 as fn(TimerDeferTestInput) -> TestResult);
    }

    fn prop_i1_i4(input: TimerDeferTestInput) -> TestResult {
        let span = tracing::Span::current();
        let runtime = match Builder::new_multi_thread().enable_all().build() {
            Ok(rt) => rt,
            Err(e) => return TestResult::error(format!("Runtime: {e}")),
        };
        let store = match runtime.block_on(tests::build_test_store().instrument(span.clone())) {
            Ok(s) => s,
            Err(e) => return TestResult::error(format!("Store: {e}")),
        };
        let input_dbg = format!("{input:#?}");
        match runtime.block_on(
            async move {
                let mut model = TimerDeferModel::new();
                for (op_idx, op) in input.operations.iter().enumerate() {
                    let key_index = key_index_of(op);
                    let key = input.key_components[key_index].key.clone();

                    model.apply(op, &input.key_components);
                    apply_op(&store, op, &input.key_components).await;

                    let db_next = store
                        .read_next_timer_for_invariant_check(&key)
                        .await
                        .map_err(|e| color_eyre::eyre::eyre!("op #{op_idx}: {e}"))?;
                    let model_min = model.get_next(&key).map(|(t, _)| t);

                    let db_time = db_next.as_ref().map(|(t, _)| *t);
                    if db_time != model_min {
                        return Err(color_eyre::eyre::eyre!(
                            "I1 after op #{op_idx} key={key}: db={db_time:?} model={model_min:?}"
                        ));
                    }
                }
                Ok::<_, color_eyre::Report>(())
            }
            .instrument(span),
        ) {
            Ok(()) => TestResult::passed(),
            Err(e) => TestResult::error(format!("{e}\nFailing input:\n{input_dbg}")),
        }
    }

    fn key_index_of(op: &TimerDeferOperation) -> usize {
        match op {
            TimerDeferOperation::GetNext(i)
            | TimerDeferOperation::IsDeferred(i)
            | TimerDeferOperation::DeleteKey(i) => *i,
            TimerDeferOperation::DeferFirst { key_index, .. }
            | TimerDeferOperation::DeferAdditional { key_index, .. }
            | TimerDeferOperation::CompleteRetrySuccess { key_index, .. }
            | TimerDeferOperation::IncrementRetryCount { key_index, .. }
            | TimerDeferOperation::Append { key_index, .. }
            | TimerDeferOperation::Remove { key_index, .. }
            | TimerDeferOperation::SetRetryCount { key_index, .. } => *key_index,
        }
    }

    async fn apply_op(
        store: &CassandraTimerDeferStore,
        op: &TimerDeferOperation,
        kcs: &[TestKeyComponents],
    ) {
        match op {
            TimerDeferOperation::GetNext(i) => {
                let _ = store.get_next_deferred_timer(&kcs[*i].key).await;
            }
            TimerDeferOperation::IsDeferred(i) => {
                let _ = store.is_deferred(&kcs[*i].key).await;
            }
            TimerDeferOperation::DeferFirst { key_index, time } => {
                let trigger = Trigger::new(
                    kcs[*key_index].key.clone(),
                    *time,
                    TimerType::Application,
                    tracing::Span::current(),
                );
                let _ = store.defer_first_timer(&trigger).await;
            }
            TimerDeferOperation::DeferAdditional { key_index, time } => {
                let trigger = Trigger::new(
                    kcs[*key_index].key.clone(),
                    *time,
                    TimerType::Application,
                    tracing::Span::current(),
                );
                let _ = store.defer_additional_timer(&trigger).await;
            }
            TimerDeferOperation::CompleteRetrySuccess { key_index, time } => {
                let _ = store
                    .complete_retry_success(&kcs[*key_index].key, *time)
                    .await;
            }
            TimerDeferOperation::IncrementRetryCount {
                key_index,
                current_retry_count,
            } => {
                let _ = store
                    .increment_retry_count(&kcs[*key_index].key, *current_retry_count)
                    .await;
            }
            TimerDeferOperation::Append { key_index, time } => {
                let trigger = Trigger::new(
                    kcs[*key_index].key.clone(),
                    *time,
                    TimerType::Application,
                    tracing::Span::current(),
                );
                let _ = store.append_deferred_timer(&trigger).await;
            }
            TimerDeferOperation::Remove { key_index, time } => {
                let _ = store
                    .remove_deferred_timer(&kcs[*key_index].key, *time)
                    .await;
            }
            TimerDeferOperation::SetRetryCount {
                key_index,
                retry_count,
            } => {
                let _ = store
                    .set_retry_count(&kcs[*key_index].key, *retry_count)
                    .await;
            }
            TimerDeferOperation::DeleteKey(i) => {
                let _ = store.delete_key(&kcs[*i].key).await;
            }
        }
    }
}
