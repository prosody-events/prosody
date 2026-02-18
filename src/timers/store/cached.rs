//! Write-through cache for trigger stores.
//!
//! Provides `CachedTriggerStore`, a transparent cache wrapper around any
//! [`TriggerStore`] implementation. Caches key-level trigger data to avoid
//! redundant database reads for recently written timers.
//!
//! # Design
//!
//! - **Write-through**: All writes go to the inner store first. The cache is
//!   updated only after the inner write succeeds.
//! - **Read-populate**: Cache misses delegate to the inner store and populate
//!   the cache on completion.
//! - **Context, not Span**: Caches [`opentelemetry::Context`] instead of
//!   [`tracing::Span`] to avoid span lifecycle issues (spans get replaced with
//!   `Span::none()` after processing completes).

use crate::Key;
use crate::timers::datetime::CompactDateTime;
use crate::timers::slab::{Slab, SlabId};
use crate::timers::store::{Segment, SegmentId, TriggerStore};
use crate::timers::{TimerType, Trigger};
use async_stream::try_stream;
use futures::TryStreamExt;
use futures::stream::Stream;
use opentelemetry::Context;
use quick_cache::sync::Cache;
use smallvec::SmallVec;
use std::future::Future;
use std::ops::RangeInclusive;
use std::sync::Arc;
use tracing::{Span, info_span, instrument};
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// Cached entry for a single trigger.
///
/// Stores the trigger time and OpenTelemetry context (not `Span`) for trace
/// continuity. See
/// [`CachedTimerDeferStore`](crate::consumer::middleware::defer::timer::store::cached::CachedTimerDeferStore)
/// for the rationale on caching `Context` instead of `Span`.
#[derive(Clone)]
pub struct CachedTriggerEntry {
    /// Trigger time.
    pub time: CompactDateTime,
    /// OpenTelemetry context for trace continuity.
    pub context: Context,
}

/// Cache value: stack-allocated for the singleton case (most common).
type CacheValue = SmallVec<[CachedTriggerEntry; 1]>;

/// Cache key: `(SegmentId, Key, TimerType)`.
type CacheKey = (SegmentId, Key, TimerType);

/// Creates a span for a cached trigger load, linked to the stored parent
/// context.
fn create_span_from_context(key: &Key, time: CompactDateTime, context: &Context) -> Span {
    let span = info_span!("trigger.cached_load", key = %key, time = %time, cached = true);
    let _ = span.set_parent(context.clone());
    span
}

/// Extracts a [`CachedTriggerEntry`] from a [`Trigger`].
fn extract_cache_entry(trigger: &Trigger) -> CachedTriggerEntry {
    let span = trigger.span();
    let context = span.context();
    CachedTriggerEntry {
        time: trigger.time,
        context,
    }
}

/// Write-through cache wrapper for any [`TriggerStore`].
///
/// Caches key-level trigger data (`get_key_triggers`, `get_key_times`) and
/// updates the cache on writes (`add_trigger`, `remove_trigger`,
/// `clear_and_schedule`). Segment and slab operations pass through unchanged.
///
/// # Cache States
///
/// | State | Meaning |
/// |-------|---------|
/// | `Present, len=0` | Empty - no timers for `(key, type)` |
/// | `Present, len=1` | Singleton - stack-allocated |
/// | `Present, len>1` | Overflow - heap-allocated |
/// | `Absent` | Unknown - must read from DB |
///
/// # Invariants
///
/// - Cached vec is sorted by time (ascending)
/// - Cache is updated ONLY after successful DB write
/// - Cache entries are scoped to the local process (no cross-node consistency)
#[derive(Clone)]
pub struct CachedTriggerStore<S> {
    inner: S,
    cache: Arc<Cache<CacheKey, CacheValue>>,
}

impl<S> CachedTriggerStore<S> {
    /// Creates a new cached store wrapping `inner` with the given capacity.
    #[must_use]
    pub fn new(inner: S, capacity: usize) -> Self {
        Self {
            inner,
            cache: Arc::new(Cache::new(capacity)),
        }
    }
}

impl<S> TriggerStore for CachedTriggerStore<S>
where
    S: TriggerStore,
{
    type Error = S::Error;

    // =====================================================================
    // Pass-through: Segment operations
    // =====================================================================

    fn get_segment(
        &self,
        segment_id: &SegmentId,
    ) -> impl Future<Output = Result<Option<Segment>, Self::Error>> + Send {
        self.inner.get_segment(segment_id)
    }

    fn insert_segment(
        &self,
        segment: Segment,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.inner.insert_segment(segment)
    }

    // =====================================================================
    // Pass-through: Slab operations
    // =====================================================================

    fn get_slab_range(
        &self,
        segment_id: &SegmentId,
        range: RangeInclusive<SlabId>,
    ) -> impl Stream<Item = Result<SlabId, Self::Error>> + Send {
        self.inner.get_slab_range(segment_id, range)
    }

    fn get_slab_triggers_all_types(
        &self,
        slab: &Slab,
    ) -> impl Stream<Item = Result<Trigger, Self::Error>> + Send {
        self.inner.get_slab_triggers_all_types(slab)
    }

    fn delete_slab(
        &self,
        segment_id: &SegmentId,
        slab_id: SlabId,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.inner.delete_slab(segment_id, slab_id)
    }

    // =====================================================================
    // Cached reads: get_key_times, get_key_triggers
    // =====================================================================

    fn get_key_times(
        &self,
        segment_id: &SegmentId,
        timer_type: TimerType,
        key: &Key,
    ) -> impl Stream<Item = Result<CompactDateTime, Self::Error>> + Send {
        let cache = self.cache.clone();
        let inner = self.inner.clone();
        let segment_id = *segment_id;
        let key = key.clone();
        let cache_key = (segment_id, key.clone(), timer_type);

        try_stream! {
            // Check cache first
            if let Some(entries) = cache.get(&cache_key) {
                for entry in &entries {
                    yield entry.time;
                }
                return;
            }

            // Cache miss: read from inner store
            let mut collected = SmallVec::new();
            let stream = inner.get_key_triggers(&segment_id, timer_type, &key);
            futures::pin_mut!(stream);

            while let Some(trigger) = stream.try_next().await? {
                let time = trigger.time;
                collected.push(extract_cache_entry(&trigger));
                yield time;
            }

            // Populate cache (sorted by construction since inner returns sorted)
            cache.insert(cache_key, collected);
        }
    }

    fn get_key_triggers(
        &self,
        segment_id: &SegmentId,
        timer_type: TimerType,
        key: &Key,
    ) -> impl Stream<Item = Result<Trigger, Self::Error>> + Send {
        let cache = self.cache.clone();
        let inner = self.inner.clone();
        let segment_id = *segment_id;
        let key = key.clone();
        let cache_key = (segment_id, key.clone(), timer_type);

        try_stream! {
            // Check cache first
            if let Some(entries) = cache.get(&cache_key) {
                for entry in &entries {
                    let span = create_span_from_context(&key, entry.time, &entry.context);
                    let trigger = Trigger::new(key.clone(), entry.time, timer_type, span);
                    yield trigger;
                }
                return;
            }

            // Cache miss: read from inner store
            let mut collected = SmallVec::new();
            let stream = inner.get_key_triggers(&segment_id, timer_type, &key);
            futures::pin_mut!(stream);

            while let Some(trigger) = stream.try_next().await? {
                collected.push(extract_cache_entry(&trigger));
                yield trigger;
            }

            // Populate cache (sorted by construction since inner returns sorted)
            cache.insert(cache_key, collected);
        }
    }

    // =====================================================================
    // Cached writes: add_trigger, remove_trigger, clear_and_schedule
    // =====================================================================

    #[instrument(level = "debug", skip(self, segment, slab, trigger), err)]
    async fn add_trigger(
        &self,
        segment: &Segment,
        slab: Slab,
        trigger: Trigger,
    ) -> Result<(), Self::Error> {
        // Write to inner store first (write-through)
        self.inner
            .add_trigger(segment, slab, trigger.clone())
            .await?;

        // Update cache if entry exists (don't populate on write-only)
        let cache_key = (segment.id, trigger.key.clone(), trigger.timer_type);
        if let Some(mut entries) = self.cache.get(&cache_key) {
            let new_entry = extract_cache_entry(&trigger);
            // Insert sorted by time
            let pos = entries
                .binary_search_by_key(&new_entry.time, |e| e.time)
                .unwrap_or_else(|i| i);
            entries.insert(pos, new_entry);
            self.cache.insert(cache_key, entries);
        }

        Ok(())
    }

    #[instrument(level = "debug", skip(self, segment, slab), err)]
    async fn remove_trigger(
        &self,
        segment: &Segment,
        slab: &Slab,
        key: &Key,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> Result<(), Self::Error> {
        // Write to inner store first (write-through)
        self.inner
            .remove_trigger(segment, slab, key, time, timer_type)
            .await?;

        // Update cache if entry exists
        let cache_key = (segment.id, key.clone(), timer_type);
        if let Some(mut entries) = self.cache.get(&cache_key) {
            entries.retain(|e| e.time != time);
            self.cache.insert(cache_key, entries);
        }

        Ok(())
    }

    #[instrument(
        level = "debug",
        skip(self, segment, new_slab, new_trigger, old_slabs),
        err
    )]
    async fn clear_and_schedule(
        &self,
        segment: &Segment,
        new_slab: Slab,
        new_trigger: Trigger,
        old_slabs: Vec<Slab>,
    ) -> Result<(), Self::Error> {
        // Write to inner store first (write-through)
        self.inner
            .clear_and_schedule(segment, new_slab, new_trigger.clone(), old_slabs)
            .await?;

        // Always populate cache: clear_and_schedule guarantees singleton result
        let cache_key = (segment.id, new_trigger.key.clone(), new_trigger.timer_type);
        let entry = extract_cache_entry(&new_trigger);
        self.cache.insert(cache_key, SmallVec::from_elem(entry, 1));

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::timers::store::adapter::TableAdapter;
    use crate::timers::store::memory::{InMemoryTriggerStore, memory_store};
    use crate::trigger_store_tests;
    use std::convert::Infallible;

    fn cached_memory_store() -> CachedTriggerStore<TableAdapter<InMemoryTriggerStore>> {
        CachedTriggerStore::new(memory_store(), 10_000)
    }

    // Run the full suite of TriggerStore compliance tests.
    // Low-level tests use InMemoryTriggerStore directly (cache is transparent).
    // High-level tests use CachedTriggerStore<TableAdapter<InMemoryTriggerStore>>.
    trigger_store_tests!(
        InMemoryTriggerStore,
        |_slab_size| async { Result::<_, Infallible>::Ok(InMemoryTriggerStore::new()) },
        CachedTriggerStore<TableAdapter<InMemoryTriggerStore>>,
        |_slab_size| async { Result::<_, Infallible>::Ok(cached_memory_store()) }
    );

    mod unit {
        use super::*;
        use crate::timers::duration::CompactDuration;
        use crate::timers::store::{Segment, SegmentVersion};
        use uuid::Uuid;

        fn test_trigger(key: &str, time_secs: u32) -> Trigger {
            let key: Key = Arc::from(key);
            let time = CompactDateTime::from(time_secs);
            Trigger::new(key, time, TimerType::Application, Span::current())
        }

        fn test_segment() -> Segment {
            Segment {
                id: Uuid::new_v4(),
                name: "test".to_owned(),
                slab_size: CompactDuration::new(300),
                version: SegmentVersion::V2,
            }
        }

        fn test_slab(segment: &Segment, time: CompactDateTime) -> Slab {
            Slab::from_time(segment.id, segment.slab_size, time)
        }

        fn cached_store() -> CachedTriggerStore<TableAdapter<InMemoryTriggerStore>> {
            CachedTriggerStore::new(memory_store(), 1000)
        }

        #[tokio::test]
        async fn test_cache_miss_then_hit() -> color_eyre::Result<()> {
            let store = cached_store();
            let segment = test_segment();
            store.insert_segment(segment.clone()).await?;

            let trigger = test_trigger("key-1", 1000);
            let slab = test_slab(&segment, trigger.time);
            store.add_trigger(&segment, slab, trigger.clone()).await?;

            // First read: cache miss, populates cache
            let triggers: Vec<_> = store
                .get_key_triggers(&segment.id, TimerType::Application, &trigger.key)
                .try_collect()
                .await?;
            assert_eq!(triggers.len(), 1);
            assert_eq!(triggers[0].time, trigger.time);

            // Second read: cache hit
            let triggers: Vec<_> = store
                .get_key_triggers(&segment.id, TimerType::Application, &trigger.key)
                .try_collect()
                .await?;
            assert_eq!(triggers.len(), 1);
            assert_eq!(triggers[0].time, trigger.time);

            Ok(())
        }

        #[tokio::test]
        async fn test_clear_and_schedule_populates_cache() -> color_eyre::Result<()> {
            let store = cached_store();
            let segment = test_segment();
            store.insert_segment(segment.clone()).await?;

            let trigger = test_trigger("key-1", 1000);
            let slab = test_slab(&segment, trigger.time);

            // clear_and_schedule always populates cache
            store
                .clear_and_schedule(&segment, slab, trigger.clone(), vec![])
                .await?;

            // Read should hit cache
            let triggers: Vec<_> = store
                .get_key_triggers(&segment.id, TimerType::Application, &trigger.key)
                .try_collect()
                .await?;
            assert_eq!(triggers.len(), 1);
            assert_eq!(triggers[0].time, trigger.time);

            Ok(())
        }

        #[tokio::test]
        async fn test_add_trigger_updates_existing_cache() -> color_eyre::Result<()> {
            let store = cached_store();
            let segment = test_segment();
            store.insert_segment(segment.clone()).await?;

            let t1 = test_trigger("key-1", 1000);
            let slab1 = test_slab(&segment, t1.time);
            store.add_trigger(&segment, slab1, t1.clone()).await?;

            // Populate cache via read
            let _: Vec<_> = store
                .get_key_triggers(&segment.id, TimerType::Application, &t1.key)
                .try_collect()
                .await?;

            // Add second trigger (cache should be updated)
            let t2 = test_trigger("key-1", 2000);
            let slab2 = test_slab(&segment, t2.time);
            store.add_trigger(&segment, slab2, t2.clone()).await?;

            // Read should show both (from cache)
            let triggers: Vec<_> = store
                .get_key_triggers(&segment.id, TimerType::Application, &t1.key)
                .try_collect()
                .await?;
            assert_eq!(triggers.len(), 2);
            assert_eq!(triggers[0].time, t1.time);
            assert_eq!(triggers[1].time, t2.time);

            Ok(())
        }

        #[tokio::test]
        async fn test_remove_trigger_updates_cache() -> color_eyre::Result<()> {
            let store = cached_store();
            let segment = test_segment();
            store.insert_segment(segment.clone()).await?;

            let t1 = test_trigger("key-1", 1000);
            let t2 = test_trigger("key-1", 2000);
            let slab1 = test_slab(&segment, t1.time);
            let slab2 = test_slab(&segment, t2.time);
            store
                .add_trigger(&segment, slab1.clone(), t1.clone())
                .await?;
            store
                .add_trigger(&segment, slab2.clone(), t2.clone())
                .await?;

            // Populate cache
            let _: Vec<_> = store
                .get_key_triggers(&segment.id, TimerType::Application, &t1.key)
                .try_collect()
                .await?;

            // Remove first trigger
            store
                .remove_trigger(&segment, &slab1, &t1.key, t1.time, TimerType::Application)
                .await?;

            // Cache should show only second trigger
            let triggers: Vec<_> = store
                .get_key_triggers(&segment.id, TimerType::Application, &t1.key)
                .try_collect()
                .await?;
            assert_eq!(triggers.len(), 1);
            assert_eq!(triggers[0].time, t2.time);

            Ok(())
        }

        #[tokio::test]
        async fn test_clear_and_schedule_replaces_cache() -> color_eyre::Result<()> {
            let store = cached_store();
            let segment = test_segment();
            store.insert_segment(segment.clone()).await?;

            // Add two triggers
            let t1 = test_trigger("key-1", 1000);
            let t2 = test_trigger("key-1", 2000);
            let slab1 = test_slab(&segment, t1.time);
            let slab2 = test_slab(&segment, t2.time);
            store
                .add_trigger(&segment, slab1.clone(), t1.clone())
                .await?;
            store
                .add_trigger(&segment, slab2.clone(), t2.clone())
                .await?;

            // Populate cache
            let _: Vec<_> = store
                .get_key_triggers(&segment.id, TimerType::Application, &t1.key)
                .try_collect()
                .await?;

            // clear_and_schedule replaces all with one new trigger
            let t3 = test_trigger("key-1", 3000);
            let slab3 = test_slab(&segment, t3.time);
            store
                .clear_and_schedule(&segment, slab3, t3.clone(), vec![slab1, slab2])
                .await?;

            // Cache should show only the new trigger
            let triggers: Vec<_> = store
                .get_key_triggers(&segment.id, TimerType::Application, &t1.key)
                .try_collect()
                .await?;
            assert_eq!(triggers.len(), 1);
            assert_eq!(triggers[0].time, t3.time);

            Ok(())
        }

        #[tokio::test]
        async fn test_get_key_times_cached() -> color_eyre::Result<()> {
            let store = cached_store();
            let segment = test_segment();
            store.insert_segment(segment.clone()).await?;

            let trigger = test_trigger("key-1", 1000);
            let slab = test_slab(&segment, trigger.time);
            store
                .clear_and_schedule(&segment, slab, trigger.clone(), vec![])
                .await?;

            // get_key_times should read from cache
            let times: Vec<_> = store
                .get_key_times(&segment.id, TimerType::Application, &trigger.key)
                .try_collect()
                .await?;
            assert_eq!(times.len(), 1);
            assert_eq!(times[0], trigger.time);

            Ok(())
        }

        #[tokio::test]
        async fn test_type_isolation() -> color_eyre::Result<()> {
            let store = cached_store();
            let segment = test_segment();
            store.insert_segment(segment.clone()).await?;

            let app = Trigger::new(
                Arc::from("key-1"),
                CompactDateTime::from(1000_u32),
                TimerType::Application,
                Span::current(),
            );
            let deferred = Trigger::new(
                Arc::from("key-1"),
                CompactDateTime::from(2000_u32),
                TimerType::DeferredMessage,
                Span::current(),
            );

            let slab_app = test_slab(&segment, app.time);
            let slab_def = test_slab(&segment, deferred.time);
            store.add_trigger(&segment, slab_app, app.clone()).await?;
            store
                .add_trigger(&segment, slab_def, deferred.clone())
                .await?;

            // Query Application type
            let app_triggers: Vec<_> = store
                .get_key_triggers(&segment.id, TimerType::Application, &app.key)
                .try_collect()
                .await?;
            assert_eq!(app_triggers.len(), 1);
            assert_eq!(app_triggers[0].time, app.time);

            // Query DeferredMessage type
            let def_triggers: Vec<_> = store
                .get_key_triggers(&segment.id, TimerType::DeferredMessage, &deferred.key)
                .try_collect()
                .await?;
            assert_eq!(def_triggers.len(), 1);
            assert_eq!(def_triggers[0].time, deferred.time);

            Ok(())
        }

        #[tokio::test]
        async fn test_pass_through_segment_ops() -> color_eyre::Result<()> {
            let store = cached_store();
            let segment = test_segment();

            store.insert_segment(segment.clone()).await?;
            let retrieved = store.get_segment(&segment.id).await?;

            assert!(retrieved.is_some());
            let retrieved = retrieved.ok_or_else(|| color_eyre::eyre::eyre!("expected segment"))?;
            assert_eq!(retrieved.id, segment.id);
            assert_eq!(retrieved.name, segment.name);

            Ok(())
        }
    }
}
