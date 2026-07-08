//! In-memory timer defer store for testing.
//!
//! Uses [`scc::HashMap`] for lock-free concurrent access. All data is volatile.

use super::TimerDeferStore;
use super::provider::TimerDeferStoreProvider;
use crate::otel::SpanRelation;
use crate::related_span;
use crate::timers::datetime::CompactDateTime;
use crate::timers::{TimerType, Trigger};
use crate::{Key, Partition, Topic};
use ahash::RandomState;
use opentelemetry::Context;
use scc::HashMap;
use std::collections::BTreeMap;
use std::convert::Infallible;
use std::future::Future;
use std::sync::Arc;
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// Timer entry with span context for reconstruction.
#[derive(Clone, Debug)]
struct StoredTimer {
    key: Key,
    time: CompactDateTime,
    context: Context,
}

impl StoredTimer {
    fn from_trigger(trigger: &Trigger) -> Self {
        let span = trigger.span();
        Self {
            key: trigger.key.clone(),
            time: trigger.time,
            context: span.context(),
        }
    }

    /// Reconstructs trigger with fresh span linked to stored context.
    fn to_trigger(&self, linking: SpanRelation) -> Trigger {
        let span = related_span!(linking, self.context.clone(), "timer_defer.load", key = %self.key, time = %self.time, cached = false);
        Trigger::new(self.key.clone(), self.time, TimerType::Application, span)
    }
}

/// In-memory timer defer store.
///
/// Lock-free via [`scc::HashMap`]. Each key maps to a
/// `BTreeMap<CompactDateTime, StoredTimer>` (sorted queue) plus a shared retry
/// counter. Thread-safe and cheap to clone.
///
/// Each store instance is scoped to a segment; partition isolation comes from
/// creating separate instances per partition.
#[derive(Clone, Debug)]
pub struct MemoryTimerDeferStore {
    inner: Arc<Inner>,
    timer_spans: SpanRelation,
}

impl MemoryTimerDeferStore {
    /// Creates an empty store with the given span relation.
    #[must_use]
    pub fn new(timer_spans: SpanRelation) -> Self {
        Self {
            inner: Arc::new(Inner::default()),
            timer_spans,
        }
    }
}

impl Default for MemoryTimerDeferStore {
    fn default() -> Self {
        Self::new(SpanRelation::default())
    }
}

/// Storage: `key` → (`sorted timers`, `retry_count`).
#[derive(Debug)]
struct Inner {
    deferred: HashMap<Key, (BTreeMap<CompactDateTime, StoredTimer>, u32), RandomState>,
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
        let stored = StoredTimer::from_trigger(trigger);
        let time = trigger.time;

        self.inner
            .deferred
            .entry_async(trigger.key.clone())
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
        let linking = self.timer_spans;
        let result = self
            .inner
            .deferred
            .get_async(key.as_ref())
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
        let inner = Arc::clone(&self.inner);
        let key = key.clone();

        async move {
            Ok(inner
                .deferred
                .get_async(key.as_ref())
                .await
                .map(|entry| {
                    let (timers, _) = entry.get();
                    timers.keys().copied().collect::<Vec<_>>()
                })
                .unwrap_or_default())
        }
    }

    async fn append_deferred_timer(&self, trigger: &Trigger) -> Result<(), Self::Error> {
        let stored = StoredTimer::from_trigger(trigger);
        let time = trigger.time;

        self.inner
            .deferred
            .entry_async(trigger.key.clone())
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
        let _ = self
            .inner
            .deferred
            .remove_if_async(key.as_ref(), |(timers, _)| {
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
        let _ = self
            .inner
            .deferred
            .entry_async(key.clone())
            .await
            .and_modify(|(_, current)| {
                *current = retry_count;
            });

        Ok(())
    }

    async fn delete_key(&self, key: &Key) -> Result<(), Self::Error> {
        self.inner.deferred.remove_async(key.as_ref()).await;
        Ok(())
    }
}

/// Creates isolated in-memory stores per partition.
#[derive(Clone, Copy, Debug, Default)]
pub struct MemoryTimerDeferStoreProvider {
    timer_spans: SpanRelation,
}

impl MemoryTimerDeferStoreProvider {
    /// Creates a new provider.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a provider with a specific span linking strategy.
    #[must_use]
    pub fn with_linking(timer_spans: SpanRelation) -> Self {
        Self { timer_spans }
    }
}

impl TimerDeferStoreProvider for MemoryTimerDeferStoreProvider {
    type Store = MemoryTimerDeferStore;

    fn create_store(
        &self,
        _topic: Topic,
        _partition: Partition,
        _consumer_group: &str,
        _cache_size: usize,
    ) -> Self::Store {
        MemoryTimerDeferStore {
            inner: Arc::new(Inner::default()),
            timer_spans: self.timer_spans,
        }
    }
}
