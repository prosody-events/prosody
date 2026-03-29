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
    timer_relation: SpanRelation,
}

impl MemoryTimerDeferStore {
    /// Creates an empty store.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Inner::default()),
            timer_relation: SpanRelation::default(),
        }
    }
}

impl Default for MemoryTimerDeferStore {
    fn default() -> Self {
        Self::new()
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
        let linking = self.timer_relation;
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
        let _ = self
            .inner
            .deferred
            .entry_async(key.clone())
            .await
            .and_modify(|(timers, _)| {
                timers.remove(&time);
            });

        Ok(())
    }

    async fn set_retry_count(&self, key: &Key, retry_count: u32) -> Result<(), Self::Error> {
        self.inner
            .deferred
            .entry_async(key.clone())
            .await
            .and_modify(|(_, current)| {
                *current = retry_count;
            })
            .or_insert_with(|| (BTreeMap::new(), retry_count));

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
    timer_relation: SpanRelation,
}

impl MemoryTimerDeferStoreProvider {
    /// Creates a new provider.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a provider with a specific span linking strategy.
    #[must_use]
    pub fn with_linking(timer_relation: SpanRelation) -> Self {
        Self { timer_relation }
    }
}

impl TimerDeferStoreProvider for MemoryTimerDeferStoreProvider {
    type Store = MemoryTimerDeferStore;

    fn create_store(
        &self,
        _topic: Topic,
        _partition: Partition,
        _consumer_group: &str,
    ) -> Self::Store {
        MemoryTimerDeferStore {
            inner: Arc::new(Inner::default()),
            timer_relation: self.timer_relation,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracing::Span;

    fn create_test_store() -> MemoryTimerDeferStore {
        MemoryTimerDeferStore::new()
    }

    fn test_trigger(key: &str, time_secs: u32) -> Trigger {
        let key: Key = Arc::from(key);
        let time = CompactDateTime::from(time_secs);
        Trigger::new(key, time, TimerType::Application, Span::current())
    }

    #[tokio::test]
    async fn test_get_nonexistent_key() -> color_eyre::Result<()> {
        let store = create_test_store();
        let key: Key = Arc::from("test-key-1");

        let result = store.get_next_deferred_timer(&key).await?;
        assert!(result.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn test_defer_first_and_get() -> color_eyre::Result<()> {
        let store = create_test_store();
        let trigger = test_trigger("test-key-1", 1000);

        store.defer_first_timer(&trigger).await?;

        let result = store.get_next_deferred_timer(&trigger.key).await?;
        assert!(result.is_some());
        let (returned_trigger, retry_count) =
            result.ok_or_else(|| color_eyre::eyre::eyre!("expected trigger"))?;
        assert_eq!(returned_trigger.time, trigger.time);
        assert_eq!(returned_trigger.key, trigger.key);
        assert_eq!(retry_count, 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_multiple_timers_returns_oldest() -> color_eyre::Result<()> {
        let store = create_test_store();
        let key = "test-key-1";

        // Defer first timer at time 1000
        store.defer_first_timer(&test_trigger(key, 1000)).await?;
        // Append timer at time 500 (earlier)
        store.append_deferred_timer(&test_trigger(key, 500)).await?;
        // Append timer at time 1500 (later)
        store
            .append_deferred_timer(&test_trigger(key, 1500))
            .await?;

        let key: Key = Arc::from(key);
        let result = store.get_next_deferred_timer(&key).await?;
        assert!(result.is_some());
        let (trigger, retry_count) =
            result.ok_or_else(|| color_eyre::eyre::eyre!("expected trigger"))?;
        // Should return the oldest (time 500)
        assert_eq!(trigger.time, CompactDateTime::from(500_u32));
        assert_eq!(retry_count, 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_remove_timer() -> color_eyre::Result<()> {
        let store = create_test_store();
        let trigger = test_trigger("test-key-1", 1000);

        store.defer_first_timer(&trigger).await?;
        store
            .remove_deferred_timer(&trigger.key, trigger.time)
            .await?;

        let result = store.get_next_deferred_timer(&trigger.key).await?;
        assert!(result.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn test_remove_nonexistent() -> color_eyre::Result<()> {
        let store = create_test_store();
        let key: Key = Arc::from("test-key-1");

        // Should not error
        store
            .remove_deferred_timer(&key, CompactDateTime::from(42_u32))
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_set_retry_count() -> color_eyre::Result<()> {
        let store = create_test_store();
        let trigger = test_trigger("test-key-1", 1000);

        store.defer_first_timer(&trigger).await?;
        store.set_retry_count(&trigger.key, 5).await?;

        let result = store.get_next_deferred_timer(&trigger.key).await?;
        assert!(result.is_some());
        let (_, retry_count) = result.ok_or_else(|| color_eyre::eyre::eyre!("expected trigger"))?;
        assert_eq!(retry_count, 5);
        Ok(())
    }

    #[tokio::test]
    async fn test_is_deferred() -> color_eyre::Result<()> {
        let store = create_test_store();
        let trigger = test_trigger("test-key-1", 1000);

        // Not deferred initially
        assert!(store.is_deferred(&trigger.key).await?.is_none());

        // Defer and check
        store.defer_first_timer(&trigger).await?;
        assert_eq!(store.is_deferred(&trigger.key).await?, Some(0));

        // Update retry count and check
        store.set_retry_count(&trigger.key, 3).await?;
        assert_eq!(store.is_deferred(&trigger.key).await?, Some(3));

        Ok(())
    }

    #[tokio::test]
    async fn test_complete_retry_success_more_timers() -> color_eyre::Result<()> {
        use super::super::TimerRetryCompletionResult;

        let store = create_test_store();
        let key = "test-key-1";

        // Defer two timers
        store.defer_first_timer(&test_trigger(key, 1000)).await?;
        store
            .defer_additional_timer(&test_trigger(key, 2000))
            .await?;
        store.set_retry_count(&Arc::from(key), 5).await?;

        // Complete first timer
        let key_arc: Key = Arc::from(key);
        let result = store
            .complete_retry_success(&key_arc, CompactDateTime::from(1000_u32))
            .await?;

        // Should return MoreTimers with next time and context
        assert!(matches!(
            result,
            TimerRetryCompletionResult::MoreTimers { next_time, .. } if next_time == CompactDateTime::from(2000_u32)
        ));

        // Retry count should be reset to 0
        let (_, retry_count) = store
            .get_next_deferred_timer(&key_arc)
            .await?
            .ok_or_else(|| color_eyre::eyre::eyre!("expected next timer"))?;
        assert_eq!(retry_count, 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_complete_retry_success_completed() -> color_eyre::Result<()> {
        use super::super::TimerRetryCompletionResult;

        let store = create_test_store();
        let trigger = test_trigger("test-key-1", 1000);

        store.defer_first_timer(&trigger).await?;

        let result = store
            .complete_retry_success(&trigger.key, trigger.time)
            .await?;

        assert!(matches!(result, TimerRetryCompletionResult::Completed));

        // Key should no longer be deferred
        assert!(store.is_deferred(&trigger.key).await?.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_increment_retry_count() -> color_eyre::Result<()> {
        let store = create_test_store();
        let trigger = test_trigger("test-key-1", 1000);

        store.defer_first_timer(&trigger).await?;

        let new_count = store.increment_retry_count(&trigger.key, 0).await?;
        assert_eq!(new_count, 1);

        let new_count = store.increment_retry_count(&trigger.key, 1).await?;
        assert_eq!(new_count, 2);

        let (_, retry_count) = store
            .get_next_deferred_timer(&trigger.key)
            .await?
            .ok_or_else(|| color_eyre::eyre::eyre!("expected trigger"))?;
        assert_eq!(retry_count, 2);

        Ok(())
    }
}
