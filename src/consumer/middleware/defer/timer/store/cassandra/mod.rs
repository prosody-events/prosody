//! Cassandra-backed timer defer store with internal write-through cache.
//!
//! Eliminates tombstone reads by maintaining a `next_timer` static UDT column
//! that always equals the minimum live timer row. `get_next` becomes a single
//! static-column read that resolves on the live tail of the partition
//! (`ORDER BY original_time DESC LIMIT 1`), skipping the FIFO tombstone
//! graveyard at low `original_time`.
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
use crate::{Key, Partition, Topic};
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

pub(crate) mod queries;

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
    ///
    /// `cache_size` sizes the internal write-through
    /// `quick_cache::sync::Cache`.
    #[must_use]
    pub fn new(
        store: CassandraStore,
        queries: Arc<Queries>,
        segment: LazySegment<CassandraSegmentStore>,
        timer_spans: SpanRelation,
        cache_size: usize,
    ) -> Self {
        Self {
            store,
            queries,
            segment,
            timer_spans,
            cache: Arc::new(Cache::new(cache_size)),
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

    /// Serializes a trigger's span context to W3C trace format for storage.
    fn inject_span_context(&self, trigger: &Trigger) -> HashMap<String, String> {
        self.span_map_from_context(&trigger.context())
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

    /// Reconstructs the retry trigger from a stored context, carrying the
    /// live reload span.
    ///
    /// The cache stores [`Context`], never [`tracing::Span`] — spans must
    /// finish to flush, so a fresh span is created on every read. Reload time
    /// is dispatch time for a deferred retry: this path never passes through
    /// `set_dispatch_span`, so the `timer_defer.load` span built here per the
    /// configured relation IS the dispatch span, and the trigger carries it
    /// live for the handler. `cached` is `true` when served from the
    /// in-memory write-through cache, `false` on a DB read.
    fn reconstruct_trigger(
        &self,
        key: &Key,
        time: CompactDateTime,
        context: &Context,
        cached: bool,
    ) -> Trigger {
        let span = related_span!(
            self.timer_spans,
            context.clone(),
            "timer_defer.load",
            key = %key,
            timer.fire_time = %time.to_rfc3339(),
            timer.type = ?TimerType::Application,
            cached = cached
        );
        let trigger = Trigger::new(key.clone(), time, TimerType::Application, span.clone());
        trigger.set_span(span);
        trigger
    }

    /// Reads `(next_timer, retry_count)` static columns from the DB.
    ///
    /// Handles legacy partitions (written before the `next_timer` static
    /// column existed): when `next_timer = NULL` but `retry_count != NULL`,
    /// the partition is ambiguous — it could be post-migration orphan
    /// `retry_count` (empty partition) or pre-migration clustering rows with
    /// no cached hint. A `probe_min` disambiguates, and on a hit we fire a
    /// `repair_next_timer` UPDATE so subsequent reads take the fast path.
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

        match row_opt {
            None | Some((None, None)) => Ok(None),
            Some((Some(udt), retry_opt)) => {
                let context = self.extract_context(&udt.span);
                let retry_count = retry_opt.and_then(|c| c.try_into().ok()).unwrap_or(0);
                Ok(Some(CachedTimerEntry {
                    time: udt.time,
                    context,
                    retry_count,
                }))
            }
            Some((None, Some(rc_raw))) => {
                self.repair_legacy_partition(segment_id, key, rc_raw).await
            }
        }
    }

    /// Lazy on-read repair of a legacy partition whose `next_timer` was never
    /// populated. Probes for the minimum live clustering row; on a hit, writes
    /// a reconstructed `DeferredNextTimer` UDT back via an LWW UPDATE and
    /// returns the synthesized entry. On a miss the partition is truly empty
    /// (orphan `retry_count`), and no repair is issued.
    async fn repair_legacy_partition(
        &self,
        segment_id: &uuid::Uuid,
        key: &Key,
        raw_retry_count: i32,
    ) -> Result<Option<CachedTimerEntry>, CassandraDeferStoreError> {
        let probe = self
            .session()
            .execute_unpaged(&self.queries.probe_min, (segment_id, key.as_ref()))
            .await
            .map_err(CassandraStoreError::from)?;

        let row_opt = probe
            .into_rows_result()
            .map_err(CassandraStoreError::from)?
            .maybe_first_row::<(Option<CompactDateTime>, Option<HashMap<String, String>>)>()
            .map_err(CassandraStoreError::from)?;

        let Some((Some(min_time), span_opt)) = row_opt else {
            // No clustering row exists — orphan `retry_count`. `next_timer =
            // NULL` is already the correct state; do not issue a repair.
            return Ok(None);
        };

        let span_map = span_opt.unwrap_or_default();
        let next_udt = DeferredNextTimer {
            time: min_time,
            span: span_map.clone(),
        };

        // Match next_timer's TTL to its referenced clustering row so the static
        // hint cannot expire before the row it points to.
        let ttl = self.store.calculate_ttl(min_time);
        self.session()
            .execute_unpaged(
                &self.queries.repair_next_timer,
                (ttl, &next_udt, segment_id, key.as_ref()),
            )
            .await
            .map_err(CassandraStoreError::from)?;

        let context = self.extract_context(&span_map);
        let retry_count: u32 = raw_retry_count.try_into().unwrap_or(0);
        Ok(Some(CachedTimerEntry {
            time: min_time,
            context,
            retry_count,
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
            .maybe_first_row::<(Option<CompactDateTime>, Option<HashMap<String, String>>)>()
            .map_err(CassandraStoreError::from)?;

        Ok(row_opt.and_then(|(time_opt, span_opt)| {
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

    /// First-deferral INSERT given pre-resolved partition state. Writes the
    /// clustering row (with span), `next_timer` (preserving any lower live
    /// time), and `retry_count = 0` in a single statement, atomically wiping
    /// any orphan static left by a prior `set_retry_count`. Shared by
    /// `defer_first_timer` and the empty-partition recovery branch of
    /// `append_deferred_timer`.
    async fn defer_first_timer_resolved(
        &self,
        segment_id: &uuid::Uuid,
        trigger: &Trigger,
        cached: Option<CachedTimerEntry>,
    ) -> Result<(), CassandraDeferStoreError> {
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
                    segment_id,
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

        Ok(())
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
        self.defer_first_timer_resolved(&segment_id, trigger, cached)
            .await?;

        debug!(key = ?trigger.key, time = %trigger.time, "Stored first deferred timer");
        Ok(())
    }

    #[instrument(level = "debug", skip(self), err)]
    async fn get_next_deferred_timer(
        &self,
        key: &Key,
    ) -> Result<Option<(Trigger, u32)>, Self::Error> {
        // Cache hit — a fresh span is created from the cached Context and
        // attributed as cached
        if let Some(cached) = self.cache.get(key.as_ref()) {
            return Ok(cached.map(|entry| {
                let trigger = self.reconstruct_trigger(key, entry.time, &entry.context, true);
                (trigger, entry.retry_count)
            }));
        }

        // Cache miss: single static-column + UDT read, reverse-scan skips
        // the FIFO tombstone graveyard at low `original_time`.
        let segment_id = self.segment_id().await?;
        let entry_opt = self.read_next_static(&segment_id, key).await?;
        self.cache.insert(Arc::clone(key), entry_opt.clone());

        Ok(entry_opt.map(|entry| {
            let trigger = self.reconstruct_trigger(key, entry.time, &entry.context, false);
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

        // Empty partition: precondition violation. delete_key wipes any
        // orphan static (matching message store) and reports Completed.
        let Some(entry) = cached else {
            self.delete_key(key).await?;
            return Ok(TimerRetryCompletionResult::Completed);
        };

        let cur_next_time = Some(entry.time);

        if cur_next_time == Some(time) {
            // FIFO hot path: probe for the successor before deleting
            let found = self.probe_next(&segment_id, key, time).await?;
            if let Some(next_udt) = found {
                // Match next_timer's TTL to the referenced successor row so the
                // static hint cannot expire before the row it points to.
                let ttl = self.store.calculate_ttl(next_udt.time);
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
            // Non-FIFO path: offset is not the minimum; leave next_timer alone.
            // Only retry_count is rewritten — base_ttl is safe (read_next_static
            // defaults retry_opt = None to 0, matching the value being written).
            let ttl = self.store.base_ttl();
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
        }
    }

    #[instrument(level = "debug", skip(self), err)]
    async fn append_deferred_timer(&self, trigger: &Trigger) -> Result<(), Self::Error> {
        let (segment_id, cached) = self.resolve_cache_or_read(&trigger.key).await?;
        let ttl = self.store.calculate_ttl(trigger.time);
        let span_map = self.inject_span_context(trigger);

        match &cached {
            None => {
                // Empty partition: precondition violation (caller should have
                // used defer_first_timer). Recover via the shared
                // first-deferral INSERT, which writes retry_count = 0 and so
                // wipes any orphan static left by a prior set_retry_count.
                self.defer_first_timer_resolved(&segment_id, trigger, None)
                    .await?;
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

        let cur_next_time = cached.as_ref().map(|e| e.time);

        if cur_next_time == Some(time) {
            // Min-removal: must repair next_timer in the same BATCH
            let found = self.probe_next(&segment_id, key, time).await?;
            match found {
                Some(next_udt) => {
                    // Match next_timer's TTL to the referenced successor row so
                    // the static hint cannot expire before the row it points to.
                    let ttl = self.store.calculate_ttl(next_udt.time);
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
        // No-op on a partition with no live timers. Cassandra's blind UPDATE
        // upserts a static-only row and leaves an orphan retry_count after
        // all timers have been processed. resolve_cache_or_read also fires
        // legacy repair, so a partition with clustering rows but a NULL
        // next_timer still resolves as Some(_) here.
        let (segment_id, cached) = self.resolve_cache_or_read(key).await?;
        let Some(entry) = cached else {
            debug!(key = ?key, "set_retry_count on empty partition: no-op");
            return Ok(());
        };

        let ttl = self.store.base_ttl();
        let retry_count_i32: i32 = retry_count.try_into().unwrap_or(i32::MAX);

        self.session()
            .execute_unpaged(
                &self.queries.update_retry_count,
                (ttl, retry_count_i32, &segment_id, key.as_ref()),
            )
            .await
            .map_err(CassandraStoreError::from)?;

        self.cache.insert(
            Arc::clone(key),
            Some(CachedTimerEntry {
                time: entry.time,
                context: entry.context,
                retry_count,
            }),
        );

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
        cache_size: usize,
    ) -> Self::Store {
        // Each call creates a new store with its own fresh cache.
        // The cache must never outlive or be shared across partition assignments.
        let segment = LazySegment::new(
            self.segment_store.clone(),
            topic,
            partition,
            Arc::from(consumer_group),
        );
        CassandraTimerDeferStore::new(
            self.store.clone(),
            self.queries.clone(),
            segment,
            self.timer_spans,
            cache_size,
        )
    }
}

/// Test-only helper methods on the store, used by the sibling test modules
/// below to read raw Cassandra partition state and seed legacy fixtures.
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

    /// Reads the raw partition state for the no-orphan invariant: any
    /// clustering row, the static `next_timer`, and the static
    /// `retry_count`. Returns `(has_clustering, next_timer_set,
    /// retry_count_set)`.
    async fn read_partition_liveness_for_invariant_check(
        &self,
        key: &Key,
    ) -> color_eyre::Result<(bool, bool, bool)> {
        let segment_id = self
            .segment_id()
            .await
            .map_err(|e| color_eyre::eyre::eyre!("{e}"))?;

        let static_row = self
            .session()
            .execute_unpaged(&self.queries.get_next_static, (&segment_id, key.as_ref()))
            .await?
            .into_rows_result()?
            .maybe_first_row::<(Option<DeferredNextTimer>, Option<i32>)>()?;
        let (next_set, rc_set) = match static_row {
            None => (false, false),
            Some((udt_opt, rc_opt)) => (udt_opt.is_some(), rc_opt.is_some()),
        };

        let probe = self
            .session()
            .execute_unpaged(&self.queries.probe_min, (&segment_id, key.as_ref()))
            .await?
            .into_rows_result()?
            .maybe_first_row::<(Option<CompactDateTime>, Option<HashMap<String, String>>)>()?;
        let has_clustering = matches!(probe, Some((Some(_), _)));

        Ok((has_clustering, next_set, rc_set))
    }

    /// Seeds a pre-migration legacy partition for tests: inserts clustering
    /// rows without touching `next_timer`, and optionally sets a static
    /// `retry_count`. Invalidates the cache so subsequent reads hit the DB
    /// and exercise the lazy on-read repair path.
    pub(crate) async fn seed_legacy_for_test(
        &self,
        key: &Key,
        clustering_times: &[CompactDateTime],
        retry_count: Option<u32>,
    ) -> color_eyre::Result<()> {
        let segment_id = self
            .segment_id()
            .await
            .map_err(|e| color_eyre::eyre::eyre!("{e}"))?;
        let base_ttl = self.store.base_ttl();

        for &time in clustering_times {
            let ttl = self.store.calculate_ttl(time);
            let span_map: HashMap<String, String> = HashMap::new();
            self.session()
                .execute_unpaged(
                    &self.queries.insert_deferred_timer_without_retry_count,
                    (&segment_id, key.as_ref(), time, &span_map, ttl),
                )
                .await?;
        }

        if let Some(rc) = retry_count {
            let rc_i32: i32 = rc.try_into().unwrap_or(i32::MAX);
            self.session()
                .execute_unpaged(
                    &self.queries.update_retry_count,
                    (base_ttl, rc_i32, &segment_id, key.as_ref()),
                )
                .await?;
        }

        let _ = self.cache.remove(key.as_ref());
        Ok(())
    }
}

#[cfg(test)]
mod tests;
