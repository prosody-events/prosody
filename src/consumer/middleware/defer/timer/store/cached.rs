//! Write-through cache for timer defer stores.

use super::{CachedTimerEntry, TimerDeferStore, TimerRetryCompletionResult};
use crate::Key;
use crate::timers::TimerType;
use crate::timers::Trigger;
use crate::timers::datetime::CompactDateTime;
use futures::Stream;
use opentelemetry::Context;
use quick_cache::sync::Cache;
use std::sync::Arc;
use tracing::{Span, info_span};
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// Cache: `key` → `Option<CachedTimerEntry>`.
type TimerDeferCache = Cache<Key, Option<CachedTimerEntry>>;

/// Write-through cache for any [`TimerDeferStore`].
///
/// Caches `get_next_deferred_timer` results. Writes go to store first, then
/// update or conservatively invalidate cache. Uses smart invalidation:
/// monotonic appends preserve cache; out-of-order appends update to new
/// minimum.
///
/// Caches `Context` rather than `Trigger` because spans get replaced with
/// `Span::none()` after processing.
#[derive(Clone)]
pub struct CachedTimerDeferStore<S> {
    store: S,
    cache: Arc<TimerDeferCache>,
}

impl<S> CachedTimerDeferStore<S>
where
    S: TimerDeferStore,
{
    /// Wraps a store with a cache of the given capacity.
    #[must_use]
    pub fn new(store: S, capacity: usize) -> Self {
        Self {
            store,
            cache: Arc::new(Cache::new(capacity)),
        }
    }

    fn create_span_from_context(key: &Key, time: CompactDateTime, context: &Context) -> Span {
        let span = info_span!("timer_defer.cache_hit", key = %key, time = %time);
        let _ = span.set_parent(context.clone());
        span
    }

    fn extract_cache_entry(trigger: &Trigger, retry_count: u32) -> CachedTimerEntry {
        let span = trigger.span();
        let context = span.context();
        CachedTimerEntry {
            time: trigger.time,
            context,
            retry_count,
        }
    }

    /// Smart cache update: preserve if monotonic, update if new minimum,
    /// invalidate if uncertain.
    fn update_cache_after_append(&self, trigger: &Trigger) {
        let key = &trigger.key;
        let time = trigger.time;

        if let Some(cached) = self.cache.get(key.as_ref())
            && let Some(entry) = cached
        {
            if time >= entry.time {
                // New timer doesn't change minimum (monotonic append case)
                // Preserve cache - this is the common case
                return;
            }
            // New timer is earlier - update cache with new minimum
            // Keep existing retry_count as it applies to the queue as a whole
            let new_entry = CachedTimerEntry {
                time,
                context: trigger.span().context(),
                retry_count: entry.retry_count,
            };
            self.cache.insert(Arc::clone(key), Some(new_entry));
            return;
        }

        // No cached entry or was None - invalidate to be safe
        self.cache.remove(key.as_ref());
    }
}

impl<S> TimerDeferStore for CachedTimerDeferStore<S>
where
    S: TimerDeferStore,
{
    type Error = S::Error;

    async fn defer_first_timer(&self, trigger: &Trigger) -> Result<(), Self::Error> {
        // Write through to store first (for durability)
        self.store.defer_first_timer(trigger).await?;

        // Pre-populate cache ONLY if we're confident this is truly the first
        // timer
        match self.cache.get(trigger.key.as_ref()) {
            Some(None) => {
                // Was cached as not-deferred - safe to update to deferred
                let entry = Self::extract_cache_entry(trigger, 0);
                self.cache.insert(Arc::clone(&trigger.key), Some(entry));
            }
            Some(Some(_)) | None => {
                // Either already had deferred state (contract violation) or not
                // cached. Be conservative: invalidate so next read queries
                // store for true minimum
                self.cache.remove(trigger.key.as_ref());
            }
        }

        Ok(())
    }

    async fn defer_additional_timer(&self, trigger: &Trigger) -> Result<(), Self::Error> {
        // Write through to store first
        self.store.defer_additional_timer(trigger).await?;

        // Update cache using smart invalidation
        self.update_cache_after_append(trigger);

        Ok(())
    }

    async fn complete_retry_success(
        &self,
        key: &Key,
        time: CompactDateTime,
    ) -> Result<TimerRetryCompletionResult, Self::Error> {
        // Write through to store first
        let result = self.store.complete_retry_success(key, time).await?;

        // Update cache based on result (store tells us exact new state)
        match &result {
            TimerRetryCompletionResult::MoreTimers { next_time, context } => {
                // More timers exist, retry_count reset to 0
                let entry = CachedTimerEntry {
                    time: *next_time,
                    context: context.clone(),
                    retry_count: 0,
                };
                self.cache.insert(Arc::clone(key), Some(entry));
            }
            TimerRetryCompletionResult::Completed => {
                // Key deleted, no more timers
                self.cache.insert(Arc::clone(key), None);
            }
        }

        Ok(result)
    }

    async fn increment_retry_count(
        &self,
        key: &Key,
        current_retry_count: u32,
    ) -> Result<u32, Self::Error> {
        // Write through to store first
        let new_count = self
            .store
            .increment_retry_count(key, current_retry_count)
            .await?;

        // Update retry count in-place if we have a cached entry.
        // This is safe because retry_count changes don't affect time ordering.
        if let Some(cached) = self.cache.get(key.as_ref())
            && let Some(entry) = cached
        {
            let updated_entry = CachedTimerEntry {
                time: entry.time,
                context: entry.context.clone(),
                retry_count: new_count,
            };
            self.cache.insert(Arc::clone(key), Some(updated_entry));
            return Ok(new_count);
        }

        // No cached entry or was None - invalidate so next read repopulates
        self.cache.remove(key.as_ref());

        Ok(new_count)
    }

    async fn get_next_deferred_timer(
        &self,
        key: &Key,
    ) -> Result<Option<(Trigger, u32)>, Self::Error> {
        // Check cache first
        if let Some(cached) = self.cache.get(key.as_ref()) {
            return Ok(cached.map(|entry| {
                let span = Self::create_span_from_context(key, entry.time, &entry.context);
                let trigger = Trigger::new(key.clone(), entry.time, TimerType::Application, span);
                (trigger, entry.retry_count)
            }));
        }

        // Cache miss - query store
        let result = self.store.get_next_deferred_timer(key).await?;

        // Populate cache
        match &result {
            Some((trigger, retry_count)) => {
                let entry = Self::extract_cache_entry(trigger, *retry_count);
                self.cache.insert(Arc::clone(key), Some(entry));
            }
            None => {
                self.cache.insert(Arc::clone(key), None);
            }
        }

        Ok(result)
    }

    fn deferred_times(
        &self,
        key: &Key,
    ) -> impl Stream<Item = Result<CompactDateTime, Self::Error>> + Send + 'static {
        // Cache only holds the minimum time, not all times - delegate to store
        self.store.deferred_times(key)
    }

    async fn append_deferred_timer(&self, trigger: &Trigger) -> Result<(), Self::Error> {
        // Write through to store first
        self.store.append_deferred_timer(trigger).await?;

        // Update cache using smart invalidation
        self.update_cache_after_append(trigger);

        Ok(())
    }

    async fn remove_deferred_timer(
        &self,
        key: &Key,
        time: CompactDateTime,
    ) -> Result<(), Self::Error> {
        // Write through to store first
        self.store.remove_deferred_timer(key, time).await?;

        // Invalidate: don't know if more timers exist.
        // If this was the last timer, get_next_deferred_timer would return
        // None. If more timers exist, we need to query to find the next
        // one.
        self.cache.remove(key.as_ref());

        Ok(())
    }

    async fn set_retry_count(&self, key: &Key, retry_count: u32) -> Result<(), Self::Error> {
        // Write through to store first
        self.store.set_retry_count(key, retry_count).await?;

        // Update retry count in-place if we have a cached entry.
        if let Some(cached) = self.cache.get(key.as_ref())
            && let Some(entry) = cached
        {
            let updated_entry = CachedTimerEntry {
                time: entry.time,
                context: entry.context.clone(),
                retry_count,
            };
            self.cache.insert(Arc::clone(key), Some(updated_entry));
            return Ok(());
        }

        // No cached entry or was None - invalidate to be safe
        self.cache.remove(key.as_ref());

        Ok(())
    }

    async fn delete_key(&self, key: &Key) -> Result<(), Self::Error> {
        // Write through to store first
        self.store.delete_key(key).await?;

        // Update cache with known state: key is deleted
        self.cache.insert(Arc::clone(key), None);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consumer::middleware::defer::timer::store::memory::MemoryTimerDeferStore;

    fn create_test_store() -> MemoryTimerDeferStore {
        MemoryTimerDeferStore::new()
    }

    fn test_trigger(key: &str, time_secs: u32) -> Trigger {
        let key: Key = Arc::from(key);
        let time = CompactDateTime::from(time_secs);
        Trigger::new(key, time, TimerType::Application, Span::current())
    }

    #[tokio::test]
    async fn test_cache_hit_on_repeated_get() -> color_eyre::Result<()> {
        let store = create_test_store();
        let cached_store = CachedTimerDeferStore::new(store, 100);
        let trigger = test_trigger("test-key-1", 1000);

        // First defer
        cached_store.defer_first_timer(&trigger).await?;

        // First get (cache miss, populates cache)
        let result1 = cached_store.get_next_deferred_timer(&trigger.key).await?;
        assert!(result1.is_some());
        let (t1, rc1) = result1.ok_or_else(|| color_eyre::eyre::eyre!("expected timer"))?;
        assert_eq!(t1.time, trigger.time);
        assert_eq!(rc1, 0);

        // Second get (cache hit - should return same result without querying
        // store)
        let result2 = cached_store.get_next_deferred_timer(&trigger.key).await?;
        assert!(result2.is_some());
        let (t2, rc2) = result2.ok_or_else(|| color_eyre::eyre::eyre!("expected timer"))?;
        assert_eq!(t2.time, trigger.time);
        assert_eq!(rc2, 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_cache_update_on_increment() -> color_eyre::Result<()> {
        let store = create_test_store();
        let cached_store = CachedTimerDeferStore::new(store, 100);
        let trigger = test_trigger("test-key-1", 1000);

        // Defer and populate cache
        cached_store.defer_first_timer(&trigger).await?;
        cached_store.get_next_deferred_timer(&trigger.key).await?;

        // Increment retry count (should update cache in-place)
        let new_count = cached_store.increment_retry_count(&trigger.key, 0).await?;
        assert_eq!(new_count, 1);

        // Next get should see updated retry_count (cache hit, no store query)
        let result = cached_store.get_next_deferred_timer(&trigger.key).await?;
        let (_, retry_count) = result.ok_or_else(|| color_eyre::eyre::eyre!("expected timer"))?;
        assert_eq!(retry_count, 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_cache_cleared_on_complete_last_timer() -> color_eyre::Result<()> {
        let store = create_test_store();
        let cached_store = CachedTimerDeferStore::new(store, 100);
        let trigger = test_trigger("test-key-1", 1000);

        // Defer one timer
        cached_store.defer_first_timer(&trigger).await?;

        // Complete it
        let result = cached_store
            .complete_retry_success(&trigger.key, trigger.time)
            .await?;

        assert!(matches!(result, TimerRetryCompletionResult::Completed));

        // Cache should show None (key is not deferred)
        let cached = cached_store.get_next_deferred_timer(&trigger.key).await?;
        assert!(cached.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_cache_update_on_complete_success() -> color_eyre::Result<()> {
        let store = create_test_store();
        let cached_store = CachedTimerDeferStore::new(store, 100);
        let key = "test-key-1";

        // Defer two timers
        cached_store
            .defer_first_timer(&test_trigger(key, 1000))
            .await?;
        cached_store
            .defer_additional_timer(&test_trigger(key, 2000))
            .await?;

        // Complete first timer
        let key_arc: Key = Arc::from(key);
        let result = cached_store
            .complete_retry_success(&key_arc, CompactDateTime::from(1000_u32))
            .await?;

        // Should return MoreTimers with next time
        assert!(matches!(
            result,
            TimerRetryCompletionResult::MoreTimers { next_time, .. } if next_time == CompactDateTime::from(2000_u32)
        ));

        // Cache should be updated to point to next timer with retry_count=0
        let cached = cached_store.get_next_deferred_timer(&key_arc).await?;
        let (trigger, retry_count) =
            cached.ok_or_else(|| color_eyre::eyre::eyre!("expected timer"))?;
        assert_eq!(trigger.time, CompactDateTime::from(2000_u32));
        assert_eq!(retry_count, 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_defer_additional_out_of_order() -> color_eyre::Result<()> {
        let store = create_test_store();
        let cached_store = CachedTimerDeferStore::new(store, 100);
        let key = "test-key-1";

        // Defer first timer at time 1000
        cached_store
            .defer_first_timer(&test_trigger(key, 1000))
            .await?;

        // Defer additional timer with SMALLER time (out-of-order scenario)
        // Smart invalidation should update cache with new minimum
        cached_store
            .defer_additional_timer(&test_trigger(key, 500))
            .await?;

        // Next read should see time 500 (the smaller one) as next
        let key_arc: Key = Arc::from(key);
        let result = cached_store.get_next_deferred_timer(&key_arc).await?;
        let (trigger, _) = result.ok_or_else(|| color_eyre::eyre::eyre!("expected timer"))?;
        assert_eq!(trigger.time, CompactDateTime::from(500_u32));

        Ok(())
    }

    #[tokio::test]
    async fn test_defer_additional_monotonic() -> color_eyre::Result<()> {
        let store = create_test_store();
        let cached_store = CachedTimerDeferStore::new(store, 100);
        let key = "test-key-1";

        // Defer first timer - cache pre-populated
        cached_store
            .defer_first_timer(&test_trigger(key, 1000))
            .await?;

        // Append monotonically increasing times - cache should be preserved
        cached_store
            .defer_additional_timer(&test_trigger(key, 2000))
            .await?;

        // Cache should still have time 1000 as minimum (not invalidated!)
        let key_arc: Key = Arc::from(key);
        let result = cached_store.get_next_deferred_timer(&key_arc).await?;
        let (trigger, _) = result.ok_or_else(|| color_eyre::eyre::eyre!("expected timer"))?;
        assert_eq!(trigger.time, CompactDateTime::from(1000_u32));

        Ok(())
    }

    #[tokio::test]
    async fn test_cache_negative_results() -> color_eyre::Result<()> {
        let store = create_test_store();
        let cached_store = CachedTimerDeferStore::new(store, 100);
        let key: Key = Arc::from("test-key-1");

        // Query non-existent key (should cache None)
        let result1 = cached_store.get_next_deferred_timer(&key).await?;
        assert!(result1.is_none());

        // Second query should hit cache
        let result2 = cached_store.get_next_deferred_timer(&key).await?;
        assert!(result2.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_delete_key_updates_cache() -> color_eyre::Result<()> {
        let store = create_test_store();
        let cached_store = CachedTimerDeferStore::new(store, 100);
        let trigger = test_trigger("test-key-1", 1000);

        // Defer timer and populate cache
        cached_store.defer_first_timer(&trigger).await?;
        cached_store.get_next_deferred_timer(&trigger.key).await?;

        // Delete key (should update cache to None)
        cached_store.delete_key(&trigger.key).await?;

        // Cache should show None
        let result = cached_store.get_next_deferred_timer(&trigger.key).await?;
        assert!(result.is_none());

        Ok(())
    }
}
