//! In-memory timer defer store for testing.
//!
//! Uses [`scc::HashMap`] for lock-free concurrent access. All data is volatile.

use super::TimerDeferStore;
use super::provider::TimerDeferStoreProvider;
use crate::consumer::middleware::defer::segment::{LazySegment, MemorySegmentStore};
use crate::otel::SpanRelation;
use crate::related_span;
use crate::timers::datetime::CompactDateTime;
use crate::timers::{TimerType, Trigger};
use crate::{Key, Partition, SegmentId, Topic};
use ahash::RandomState;
use opentelemetry::Context;
use scc::HashMap;
use std::collections::BTreeMap;
use std::convert::Infallible;
use std::future::Future;
use std::sync::Arc;

/// Topic, partition, and group a standalone [`MemoryTimerDeferStore`] is
/// scoped to.
const STANDALONE_SEGMENT: &str = "memory";

/// Timer entry with span context for reconstruction.
#[derive(Clone, Debug)]
struct StoredTimer {
    key: Key,
    time: CompactDateTime,
    context: Context,
}

impl StoredTimer {
    fn from_trigger(trigger: &Trigger) -> Self {
        Self {
            key: trigger.key.clone(),
            time: trigger.time,
            context: trigger.context(),
        }
    }

    /// Reconstructs the retry trigger, carrying the live reload span.
    ///
    /// Reload time is dispatch time for a deferred retry: this path never
    /// passes through `set_dispatch_span`, so the `timer_defer.load` span
    /// built here from the stored scheduling context per `linking` IS the
    /// dispatch span, and the trigger carries it live for the handler.
    fn to_trigger(&self, linking: SpanRelation) -> Trigger {
        let span = related_span!(linking, self.context.clone(), "timer_defer.load", key = %self.key, timer.fire_time = %self.time.to_rfc3339(), timer.type = ?TimerType::Application, cached = false);
        let trigger = Trigger::new(
            self.key.clone(),
            self.time,
            TimerType::Application,
            span.clone(),
        );
        trigger.set_span(span);
        trigger
    }
}

/// In-memory timer defer store.
///
/// Lock-free via [`scc::HashMap`]. Each key maps to a
/// `BTreeMap<CompactDateTime, StoredTimer>` (sorted queue) plus a shared retry
/// counter. Thread-safe and cheap to clone.
///
/// The store reads and writes one segment of the substrate its provider owns.
/// Partition isolation comes from the segment id in every map key.
#[derive(Clone, Debug)]
pub struct MemoryTimerDeferStore {
    segment: LazySegment<MemorySegmentStore>,
    inner: Arc<Inner>,
    timer_spans: SpanRelation,
}

impl MemoryTimerDeferStore {
    /// A standalone store over its own substrate and one fixed segment.
    ///
    /// Callers that need several segments to share rows mint their stores from
    /// one [`MemoryTimerDeferStoreProvider`] instead.
    #[must_use]
    pub fn new(timer_spans: SpanRelation) -> Self {
        MemoryTimerDeferStoreProvider::new(MemorySegmentStore::new(), timer_spans).create_store(
            Topic::from(STANDALONE_SEGMENT),
            0,
            STANDALONE_SEGMENT,
            0,
        )
    }

    /// The segment this store addresses, registering it on first call.
    async fn segment_id(&self) -> Result<SegmentId, Infallible> {
        Ok(self.segment.get().await?.id())
    }
}

impl Default for MemoryTimerDeferStore {
    fn default() -> Self {
        Self::new(SpanRelation::default())
    }
}

/// The queue of one key in one segment, and that key's retry count.
type DeferredTimers = (BTreeMap<CompactDateTime, StoredTimer>, u32);

/// Storage: (`segment`, `key`) → (`sorted timers`, `retry_count`).
#[derive(Debug)]
struct Inner {
    deferred: HashMap<(SegmentId, Key), DeferredTimers, RandomState>,
}

impl Default for Inner {
    fn default() -> Self {
        Self {
            deferred: HashMap::with_hasher(RandomState::new()),
        }
    }
}

impl TimerDeferStore for MemoryTimerDeferStore {
    type Error = Infallible;

    async fn defer_first_timer(&self, trigger: &Trigger) -> Result<(), Self::Error> {
        let segment = self.segment_id().await?;
        let stored = StoredTimer::from_trigger(trigger);
        let time = trigger.time;

        self.inner
            .deferred
            .entry_async((segment, trigger.key.clone()))
            .await
            .and_modify(|(timers, retry_count)| {
                timers.insert(time, stored.clone());
                *retry_count = 0;
            })
            .or_insert_with(|| {
                let mut timers = BTreeMap::new();
                timers.insert(time, stored);
                (timers, 0)
            });

        Ok(())
    }

    async fn get_next_deferred_timer(
        &self,
        key: &Key,
    ) -> Result<Option<(Trigger, u32)>, Self::Error> {
        let segment = self.segment_id().await?;
        let linking = self.timer_spans;
        let result = self
            .inner
            .deferred
            .get_async(&(segment, Arc::clone(key)))
            .await
            .and_then(|entry| {
                let (timers, retry_count) = entry.get();
                timers
                    .first_key_value()
                    .map(|(_, stored)| (stored.to_trigger(linking), *retry_count))
            });

        Ok(result)
    }

    fn deferred_times(
        &self,
        key: &Key,
    ) -> impl Future<Output = Result<Vec<CompactDateTime>, Self::Error>> + Send + 'static {
        let segment = self.segment.clone();
        let inner = Arc::clone(&self.inner);
        let key = key.clone();

        async move {
            let segment = segment.get().await?.id();
            Ok(inner
                .deferred
                .get_async(&(segment, key))
                .await
                .map(|entry| {
                    let (timers, _) = entry.get();
                    timers.keys().copied().collect::<Vec<_>>()
                })
                .unwrap_or_default())
        }
    }

    async fn append_deferred_timer(&self, trigger: &Trigger) -> Result<(), Self::Error> {
        let segment = self.segment_id().await?;
        let stored = StoredTimer::from_trigger(trigger);
        let time = trigger.time;

        self.inner
            .deferred
            .entry_async((segment, trigger.key.clone()))
            .await
            .and_modify(|(timers, _)| {
                timers.insert(time, stored.clone());
            })
            .or_insert_with(|| {
                // Shouldn't happen (should use defer_first_timer first)
                // but handle gracefully with retry_count=0
                let mut timers = BTreeMap::new();
                timers.insert(time, stored);
                (timers, 0)
            });

        Ok(())
    }

    async fn remove_deferred_timer(
        &self,
        key: &Key,
        time: CompactDateTime,
    ) -> Result<(), Self::Error> {
        // Remove the timer; if it was the last one, drop the entry. Once all
        // deferred timers for a key are processed, the entry is dead state
        // (retry_count = 0 ≡ retry_count absent), matching Cassandra's
        // delete_key on min-only-row removal. Atomic via remove_if_async.
        let segment = self.segment_id().await?;
        let _ = self
            .inner
            .deferred
            .remove_if_async(&(segment, Arc::clone(key)), |(timers, _)| {
                timers.remove(&time);
                timers.is_empty()
            })
            .await;

        Ok(())
    }

    async fn set_retry_count(&self, key: &Key, retry_count: u32) -> Result<(), Self::Error> {
        // No-op on a key with no timers. Production only calls this with an
        // active timer present; creating an entry here would leave an orphan
        // (entry with empty BTreeMap), violating "no entry after all timers
        // are processed."
        let segment = self.segment_id().await?;
        let _ = self
            .inner
            .deferred
            .entry_async((segment, key.clone()))
            .await
            .and_modify(|(_, current)| {
                *current = retry_count;
            });

        Ok(())
    }

    async fn delete_key(&self, key: &Key) -> Result<(), Self::Error> {
        let segment = self.segment_id().await?;
        self.inner
            .deferred
            .remove_async(&(segment, Arc::clone(key)))
            .await;
        Ok(())
    }
}

/// Hands out per-segment views of one shared in-memory timer defer store.
///
/// The shared map is memory mode's **durable substrate**: two stores minted
/// for the same segment observe each other's rows, exactly as two Cassandra
/// stores over one partition do. A fresh map per `create_store` would make
/// every durable row vanish with the store that wrote it. The map is keyed by
/// segment id and key, so segments cannot collide. It drops with the provider.
#[derive(Clone, Debug, Default)]
pub struct MemoryTimerDeferStoreProvider {
    segments: MemorySegmentStore,
    inner: Arc<Inner>,
    timer_spans: SpanRelation,
}

impl MemoryTimerDeferStoreProvider {
    /// Creates a provider that registers its segments in `segments` and links
    /// reload spans through `timer_spans`.
    #[must_use]
    pub fn new(segments: MemorySegmentStore, timer_spans: SpanRelation) -> Self {
        Self {
            segments,
            inner: Arc::new(Inner::default()),
            timer_spans,
        }
    }

    /// Keys with a non-empty deferred queue in one segment. A snapshot; it
    /// drops with the caller's stream.
    pub(crate) async fn keys(&self, segment: SegmentId) -> Vec<Key> {
        let mut out = Vec::new();
        self.inner
            .deferred
            .iter_async(|(id, key), (timers, _)| {
                if *id == segment && !timers.is_empty() {
                    out.push(Arc::clone(key));
                }
                true
            })
            .await;
        out
    }
}

impl TimerDeferStoreProvider for MemoryTimerDeferStoreProvider {
    type Store = MemoryTimerDeferStore;

    fn create_store(
        &self,
        topic: Topic,
        partition: Partition,
        consumer_group: &str,
        _cache_size: usize,
    ) -> Self::Store {
        MemoryTimerDeferStore {
            segment: LazySegment::new(
                self.segments.clone(),
                topic,
                partition,
                Arc::from(consumer_group),
            ),
            inner: Arc::clone(&self.inner),
            timer_spans: self.timer_spans,
        }
    }
}

#[cfg(test)]
mod tests;
