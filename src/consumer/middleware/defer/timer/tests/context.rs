//! Tests for `TimerDeferContext` wrapper.
//!
//! Verifies that the context wrapper correctly unifies active and deferred
//! timer operations, delegating appropriately based on deferral state.

use super::*;
use crate::consumer::Keyed;
use crate::consumer::middleware::defer::timer::context::TimerDeferContext;
use crate::consumer::middleware::defer::timer::store::memory::MemoryTimerDeferStore;
use crate::consumer::middleware::defer::timer::store::{CachedTimerDeferStore, TimerDeferStore};
use crate::timers::datetime::CompactDateTime;
use crate::timers::{TimerType, Trigger};
use crate::tracing::init_test_logging;
use futures::TryStreamExt;
use futures::stream;
use std::convert::Infallible;
use std::sync::Arc;

// ============================================================================
// KeyedMockContext - Context that implements Keyed trait
// ============================================================================

/// Mock context with `Keyed` trait for testing `TimerDeferContext`.
#[derive(Clone)]
pub struct KeyedMockContext {
    inner: MockContext,
    key: Key,
    /// Tracks timer times by type for verification.
    active_timers: Arc<Mutex<Vec<(CompactDateTime, TimerType)>>>,
}

impl KeyedMockContext {
    #[must_use]
    pub fn new(key: &str) -> Self {
        Self {
            inner: MockContext::new(),
            key: Arc::from(key),
            active_timers: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Returns all active Application timers for this context.
    #[must_use]
    pub fn active_application_timers(&self) -> Vec<CompactDateTime> {
        self.active_timers
            .lock()
            .iter()
            .filter(|(_, t)| *t == TimerType::Application)
            .map(|(time, _)| *time)
            .collect()
    }

    /// Returns all active `DeferredTimer` timers for this context.
    #[must_use]
    pub fn active_deferred_timers(&self) -> Vec<CompactDateTime> {
        self.active_timers
            .lock()
            .iter()
            .filter(|(_, t)| *t == TimerType::DeferredTimer)
            .map(|(time, _)| *time)
            .collect()
    }
}

impl Keyed for KeyedMockContext {
    type Key = Key;

    fn key(&self) -> &Self::Key {
        &self.key
    }
}

impl EventContext for KeyedMockContext {
    type Error = Infallible;

    fn should_cancel(&self) -> bool {
        self.inner.should_cancel()
    }

    fn on_cancel(&self) -> impl Future<Output = ()> + Send + 'static {
        self.inner.on_cancel()
    }

    fn cancel(&self) {
        self.inner.cancel();
    }

    fn schedule(
        &self,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.active_timers.lock().push((time, timer_type));
        self.inner.schedule(time, timer_type)
    }

    fn clear_and_schedule(
        &self,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        // Clear existing timers of this type, then add new one
        self.active_timers.lock().retain(|(_, t)| *t != timer_type);
        self.active_timers.lock().push((time, timer_type));
        self.inner.clear_and_schedule(time, timer_type)
    }

    fn unschedule(
        &self,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.active_timers
            .lock()
            .retain(|(t, tt)| !(*t == time && *tt == timer_type));
        self.inner.unschedule(time, timer_type)
    }

    fn clear_scheduled(
        &self,
        timer_type: TimerType,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.active_timers.lock().retain(|(_, t)| *t != timer_type);
        self.inner.clear_scheduled(timer_type)
    }

    fn invalidate(self) {
        self.inner.invalidate();
    }

    fn scheduled(
        &self,
        timer_type: TimerType,
    ) -> impl Stream<Item = Result<CompactDateTime, Self::Error>> + Send + 'static {
        let times: Vec<CompactDateTime> = self
            .active_timers
            .lock()
            .iter()
            .filter(|(_, t)| *t == timer_type)
            .map(|(time, _)| *time)
            .collect();
        stream::iter(times.into_iter().map(Ok))
    }
}

// ============================================================================
// Test Helper
// ============================================================================

struct ContextTestHarness {
    store: CachedTimerDeferStore<MemoryTimerDeferStore>,
    inner_context: KeyedMockContext,
}

impl ContextTestHarness {
    fn new(key: &str) -> Self {
        let store = CachedTimerDeferStore::new(MemoryTimerDeferStore::new(), 100);
        let inner_context = KeyedMockContext::new(key);
        Self {
            store,
            inner_context,
        }
    }

    fn create_wrapped_context(
        &self,
    ) -> TimerDeferContext<KeyedMockContext, CachedTimerDeferStore<MemoryTimerDeferStore>> {
        TimerDeferContext::new(
            self.inner_context.clone(),
            self.store.clone(),
            self.inner_context.key.clone(),
        )
    }

    fn key(&self) -> &Key {
        &self.inner_context.key
    }

    async fn defer_timer(&self, time_secs: u32) -> color_eyre::Result<()> {
        let trigger = self.create_trigger(time_secs);
        self.store.defer_first_timer(&trigger).await?;
        Ok(())
    }

    async fn defer_additional_timer(&self, time_secs: u32) -> color_eyre::Result<()> {
        let trigger = self.create_trigger(time_secs);
        self.store.defer_additional_timer(&trigger).await?;
        Ok(())
    }

    fn create_trigger(&self, time_secs: u32) -> Trigger {
        let time = CompactDateTime::from(time_secs);
        Trigger::new(
            self.inner_context.key.clone(),
            time,
            TimerType::Application,
            tracing::Span::current(),
        )
    }

    async fn is_deferred(&self) -> color_eyre::Result<bool> {
        Ok(self.store.is_deferred(self.key()).await?.is_some())
    }

    /// Returns all deferred times for this key, sorted ascending.
    async fn deferred_times(&self) -> color_eyre::Result<Vec<CompactDateTime>> {
        use futures::TryStreamExt;
        let times: Vec<CompactDateTime> =
            self.store.deferred_times(self.key()).try_collect().await?;
        Ok(times)
    }
}

// ============================================================================
// Tests
// ============================================================================

/// `schedule()` when NOT deferred delegates to inner context.
#[test]
fn schedule_when_not_deferred_adds_to_active() {
    init_test_logging();

    TEST_RUNTIME.block_on(async {
        let harness = ContextTestHarness::new("test-key");
        let context = harness.create_wrapped_context();

        // Key is not deferred
        assert!(!harness.is_deferred().await.ok()?);

        // Schedule via wrapped context
        let time = CompactDateTime::from(1000_u32);
        context.schedule(time, TimerType::Application).await.ok()?;

        // Should have been added to inner context's active timers
        let active = harness.inner_context.active_application_timers();
        assert!(active.contains(&time), "Timer should be in active store");

        // Should NOT be in defer store
        let deferred = harness
            .store
            .get_next_deferred_timer(harness.key())
            .await
            .ok()?;
        assert!(deferred.is_none(), "Timer should not be in defer store");

        Some(())
    });
}

/// `schedule()` when deferred appends to defer store.
#[test]
fn schedule_when_deferred_appends_to_store() {
    init_test_logging();

    TEST_RUNTIME.block_on(async {
        let harness = ContextTestHarness::new("test-key");

        // First, defer a timer to make the key deferred
        harness.defer_timer(1000).await.ok()?;
        assert!(harness.is_deferred().await.ok()?);

        let context = harness.create_wrapped_context();

        // Schedule a NEW timer via wrapped context
        let time = CompactDateTime::from(2000_u32);
        context.schedule(time, TimerType::Application).await.ok()?;

        // Timer should be appended to defer store (not inner context)
        let deferred = harness.deferred_times().await.ok()?;
        assert!(
            deferred.contains(&time),
            "Timer should be in defer store; got: {deferred:?}"
        );

        // Should NOT be in inner context's active timers
        let active = harness.inner_context.active_application_timers();
        assert!(
            !active.contains(&time),
            "Timer should NOT be in active store when key is deferred"
        );

        // CLIENT INVARIANT: Timer must appear in scheduled() regardless of internal
        // storage
        let scheduled: Vec<CompactDateTime> = context
            .scheduled(TimerType::Application)
            .try_collect()
            .await
            .ok()?;
        assert!(
            scheduled.contains(&time),
            "Timer scheduled while deferred must appear in scheduled(); got: {scheduled:?}"
        );

        Some(())
    });
}

/// `unschedule()` removes from both stores when deferred.
#[test]
fn unschedule_removes_from_both_stores() {
    init_test_logging();

    TEST_RUNTIME.block_on(async {
        let harness = ContextTestHarness::new("test-key");

        // Add timer to active store first
        harness
            .inner_context
            .schedule(CompactDateTime::from(1000_u32), TimerType::Application)
            .await
            .ok()?;

        // Defer timers at 500 and 1500 to make key deferred
        harness.defer_timer(500).await.ok()?;
        harness.defer_additional_timer(1500).await.ok()?;

        let context = harness.create_wrapped_context();

        // Unschedule the timer at 1000 (in active store)
        context
            .unschedule(CompactDateTime::from(1000_u32), TimerType::Application)
            .await
            .ok()?;

        // Should be removed from active store
        let active = harness.inner_context.active_application_timers();
        assert!(
            !active.contains(&CompactDateTime::from(1000_u32)),
            "Timer should be removed from active store"
        );

        // Unschedule the timer at 500 (in defer store)
        context
            .unschedule(CompactDateTime::from(500_u32), TimerType::Application)
            .await
            .ok()?;

        // Should be removed from defer store (only 1500 remains)
        let deferred = harness.deferred_times().await.ok()?;
        assert!(
            !deferred.contains(&CompactDateTime::from(500_u32)),
            "Timer at 500 should be removed from defer store"
        );
        assert!(
            deferred.contains(&CompactDateTime::from(1500_u32)),
            "Timer at 1500 should still be in defer store"
        );

        // CLIENT INVARIANT: scheduled() must reflect unschedule operations
        let scheduled: Vec<CompactDateTime> = context
            .scheduled(TimerType::Application)
            .try_collect()
            .await
            .ok()?;
        assert!(
            !scheduled.contains(&CompactDateTime::from(500_u32)),
            "Unscheduled timer must not appear in scheduled()"
        );
        assert!(
            !scheduled.contains(&CompactDateTime::from(1000_u32)),
            "Unscheduled timer must not appear in scheduled()"
        );
        assert!(
            scheduled.contains(&CompactDateTime::from(1500_u32)),
            "Remaining timer must appear in scheduled()"
        );

        Some(())
    });
}

/// `clear_scheduled()` clears both stores and cancels `DeferredTimer`.
#[test]
fn clear_scheduled_clears_both_stores_and_cancels_deferred_timer() {
    init_test_logging();

    TEST_RUNTIME.block_on(async {
        let harness = ContextTestHarness::new("test-key");

        // Add timer to active store
        harness
            .inner_context
            .schedule(CompactDateTime::from(1000_u32), TimerType::Application)
            .await
            .ok()?;

        // Defer a timer
        harness.defer_timer(500).await.ok()?;

        // Schedule a DeferredTimer (simulates the retry timer)
        harness
            .inner_context
            .schedule(CompactDateTime::from(600_u32), TimerType::DeferredTimer)
            .await
            .ok()?;

        let context = harness.create_wrapped_context();

        // Clear all Application timers
        context.clear_scheduled(TimerType::Application).await.ok()?;

        // Active store should have no Application timers
        let active = harness.inner_context.active_application_timers();
        assert!(
            active.is_empty(),
            "Active Application timers should be cleared"
        );

        // Defer store should be cleared (key deleted)
        let deferred = harness.deferred_times().await.ok()?;
        assert!(
            deferred.is_empty(),
            "Defer store should be cleared; got: {deferred:?}"
        );

        // Key should no longer be deferred
        assert!(
            !harness.is_deferred().await.ok()?,
            "Key should not be deferred after clear_scheduled"
        );

        // DeferredTimer should be cancelled
        let deferred_timers = harness.inner_context.active_deferred_timers();
        assert!(
            deferred_timers.is_empty(),
            "DeferredTimer should be cancelled; got: {deferred_timers:?}"
        );

        Some(())
    });
}

/// `scheduled()` merges both stores sorted and deduplicated.
#[test]
fn scheduled_merges_both_stores_sorted_deduplicated() {
    init_test_logging();

    TEST_RUNTIME.block_on(async {
        let harness = ContextTestHarness::new("test-key");

        // Add timers to active store: 1000, 3000
        harness
            .inner_context
            .schedule(CompactDateTime::from(1000_u32), TimerType::Application)
            .await
            .ok()?;
        harness
            .inner_context
            .schedule(CompactDateTime::from(3000_u32), TimerType::Application)
            .await
            .ok()?;

        // Defer timers at 500, 2000
        harness.defer_timer(500).await.ok()?;
        harness.defer_additional_timer(2000).await.ok()?;

        let context = harness.create_wrapped_context();

        // Get scheduled times - should be merged and sorted
        let times: Vec<CompactDateTime> = context
            .scheduled(TimerType::Application)
            .try_collect()
            .await
            .ok()?;

        // Should be sorted: 500, 1000, 2000, 3000
        assert_eq!(
            times,
            vec![
                CompactDateTime::from(500_u32),
                CompactDateTime::from(1000_u32),
                CompactDateTime::from(2000_u32),
                CompactDateTime::from(3000_u32),
            ],
            "Times should be merged and sorted"
        );

        Some(())
    });
}

/// `scheduled()` deduplicates when same time exists in both stores.
#[test]
fn scheduled_deduplicates_when_same_time_in_both_stores() {
    init_test_logging();

    TEST_RUNTIME.block_on(async {
        let harness = ContextTestHarness::new("test-key");

        // Add timer at 1000 to active store
        harness
            .inner_context
            .schedule(CompactDateTime::from(1000_u32), TimerType::Application)
            .await
            .ok()?;

        // Defer timer at the SAME time 1000
        harness.defer_timer(1000).await.ok()?;

        let context = harness.create_wrapped_context();

        // Get scheduled times - should be deduplicated
        let times: Vec<CompactDateTime> = context
            .scheduled(TimerType::Application)
            .try_collect()
            .await
            .ok()?;

        // Should have only ONE entry at 1000
        assert_eq!(times.len(), 1, "Duplicate time should appear only once");
        assert_eq!(times[0], CompactDateTime::from(1000_u32));

        Some(())
    });
}

/// `clear_and_schedule()` makes key not deferred after completion.
#[test]
fn clear_and_schedule_makes_key_not_deferred() {
    init_test_logging();

    TEST_RUNTIME.block_on(async {
        let harness = ContextTestHarness::new("test-key");

        // Defer a timer
        harness.defer_timer(1000).await.ok()?;
        assert!(harness.is_deferred().await.ok()?, "Key should be deferred");

        let context = harness.create_wrapped_context();

        // Clear and schedule a new timer
        let new_time = CompactDateTime::from(2000_u32);
        context
            .clear_and_schedule(new_time, TimerType::Application)
            .await
            .ok()?;

        // Key should no longer be deferred
        assert!(
            !harness.is_deferred().await.ok()?,
            "Key should not be deferred after clear_and_schedule"
        );

        // New timer should be in active store
        let active = harness.inner_context.active_application_timers();
        assert!(
            active.contains(&new_time),
            "New timer should be in active store"
        );

        // DeferredTimer should be cleared
        let deferred_timers = harness.inner_context.active_deferred_timers();
        assert!(
            deferred_timers.is_empty(),
            "DeferredTimer should be cleared"
        );

        Some(())
    });
}

/// Client invariant: schedule/unschedule while deferred behaves identically to
/// not deferred.
///
/// This test verifies the key behavioral equivalence requirement: from the
/// client's perspective, timer operations should work the same regardless of
/// internal deferral state. The client should be able to:
/// 1. Schedule a timer and see it in `scheduled()`
/// 2. Unschedule that timer and no longer see it in `scheduled()`
#[test]
fn client_operations_work_identically_when_deferred() {
    init_test_logging();

    TEST_RUNTIME.block_on(async {
        let harness = ContextTestHarness::new("test-key");

        // Make key deferred by deferring an initial timer
        harness.defer_timer(1000).await.ok()?;
        assert!(harness.is_deferred().await.ok()?);

        let context = harness.create_wrapped_context();

        // STEP 1: Schedule a new timer while deferred
        let client_timer = CompactDateTime::from(2000_u32);
        context
            .schedule(client_timer, TimerType::Application)
            .await
            .ok()?;

        // CLIENT INVARIANT: Scheduled timer must be visible
        let scheduled: Vec<CompactDateTime> = context
            .scheduled(TimerType::Application)
            .try_collect()
            .await
            .ok()?;
        assert!(
            scheduled.contains(&client_timer),
            "schedule() while deferred: timer must appear in scheduled()"
        );

        // STEP 2: Unschedule the timer
        context
            .unschedule(client_timer, TimerType::Application)
            .await
            .ok()?;

        // CLIENT INVARIANT: Unscheduled timer must be gone
        let scheduled: Vec<CompactDateTime> = context
            .scheduled(TimerType::Application)
            .try_collect()
            .await
            .ok()?;
        assert!(
            !scheduled.contains(&client_timer),
            "unschedule() while deferred: timer must not appear in scheduled()"
        );

        // Original deferred timer should still be there
        assert!(
            scheduled.contains(&CompactDateTime::from(1000_u32)),
            "Original deferred timer should remain"
        );

        // STEP 3: Schedule again and use clear_and_schedule to replace
        context
            .schedule(client_timer, TimerType::Application)
            .await
            .ok()?;
        let replacement = CompactDateTime::from(3000_u32);
        context
            .clear_and_schedule(replacement, TimerType::Application)
            .await
            .ok()?;

        // CLIENT INVARIANT: Only the replacement timer should exist
        let scheduled: Vec<CompactDateTime> = context
            .scheduled(TimerType::Application)
            .try_collect()
            .await
            .ok()?;
        assert_eq!(
            scheduled,
            vec![replacement],
            "clear_and_schedule() should leave only the new timer"
        );

        // Key should no longer be deferred (clear_and_schedule deletes the key)
        assert!(
            !harness.is_deferred().await.ok()?,
            "clear_and_schedule should un-defer the key"
        );

        Some(())
    });
}

/// Non-Application timer types pass through to inner context.
#[test]
fn non_application_timers_pass_through() {
    init_test_logging();

    TEST_RUNTIME.block_on(async {
        let harness = ContextTestHarness::new("test-key");

        // Defer a timer to make key deferred
        harness.defer_timer(1000).await.ok()?;
        assert!(harness.is_deferred().await.ok()?);

        let context = harness.create_wrapped_context();

        // Schedule a DeferredMessage timer (internal type)
        let time = CompactDateTime::from(2000_u32);
        context
            .schedule(time, TimerType::DeferredMessage)
            .await
            .ok()?;

        // Should go directly to inner context, not defer store
        let has_deferred_msg = harness
            .inner_context
            .active_timers
            .lock()
            .iter()
            .any(|(t, tt)| *t == time && *tt == TimerType::DeferredMessage);
        assert!(
            has_deferred_msg,
            "DeferredMessage should pass through to inner context"
        );

        Some(())
    });
}

// ============================================================================
// Error Handling Tests
// ============================================================================

mod error_handling {
    use super::*;
    use crate::consumer::middleware::defer::timer::context::TimerDeferContextError;
    use crate::consumer::middleware::defer::timer::store::TimerRetryCompletionResult;
    use crate::consumer::middleware::{ClassifyError, ErrorCategory};
    use std::error::Error;
    use std::fmt::{self, Display, Formatter};

    /// Test error that can be classified as transient or permanent.
    #[derive(Debug, Clone)]
    struct TestStoreError {
        category: ErrorCategory,
    }

    impl TestStoreError {
        fn transient() -> Self {
            Self {
                category: ErrorCategory::Transient,
            }
        }
    }

    impl Display for TestStoreError {
        fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
            write!(f, "test store error ({:?})", self.category)
        }
    }

    impl Error for TestStoreError {}

    impl ClassifyError for TestStoreError {
        fn classify_error(&self) -> ErrorCategory {
            self.category
        }
    }

    /// Store wrapper that injects an error after yielding N items from
    /// `deferred_times`.
    #[derive(Clone)]
    struct FailAfterNStore {
        inner: CachedTimerDeferStore<MemoryTimerDeferStore>,
        /// Error after this many items (0 = error immediately on first poll)
        fail_after: usize,
    }

    impl TimerDeferStore for FailAfterNStore {
        type Error = TestStoreError;

        async fn defer_first_timer(&self, trigger: &Trigger) -> Result<(), Self::Error> {
            self.inner
                .defer_first_timer(trigger)
                .await
                .map_err(|_| TestStoreError::transient())
        }

        async fn defer_additional_timer(&self, trigger: &Trigger) -> Result<(), Self::Error> {
            self.inner
                .defer_additional_timer(trigger)
                .await
                .map_err(|_| TestStoreError::transient())
        }

        async fn complete_retry_success(
            &self,
            key: &Key,
            time: CompactDateTime,
        ) -> Result<TimerRetryCompletionResult, Self::Error> {
            self.inner
                .complete_retry_success(key, time)
                .await
                .map_err(|_| TestStoreError::transient())
        }

        async fn increment_retry_count(&self, key: &Key, current: u32) -> Result<u32, Self::Error> {
            self.inner
                .increment_retry_count(key, current)
                .await
                .map_err(|_| TestStoreError::transient())
        }

        async fn get_next_deferred_timer(
            &self,
            key: &Key,
        ) -> Result<Option<(Trigger, u32)>, Self::Error> {
            self.inner
                .get_next_deferred_timer(key)
                .await
                .map_err(|_| TestStoreError::transient())
        }

        fn deferred_times(
            &self,
            key: &Key,
        ) -> impl Stream<Item = Result<CompactDateTime, Self::Error>> + Send + 'static {
            let inner_stream = self.inner.deferred_times(key);
            let fail_after = self.fail_after;

            async_stream::try_stream! {
                use futures::StreamExt;
                futures::pin_mut!(inner_stream);

                let mut yielded = 0;
                while let Some(result) = inner_stream.next().await {
                    if yielded >= fail_after {
                        // Inject error before yielding this item
                        Err(TestStoreError::transient())?;
                    }
                    yielded += 1;
                    yield result.map_err(|_| TestStoreError::transient())?;
                }
            }
        }

        async fn append_deferred_timer(&self, trigger: &Trigger) -> Result<(), Self::Error> {
            self.inner
                .append_deferred_timer(trigger)
                .await
                .map_err(|_| TestStoreError::transient())
        }

        async fn remove_deferred_timer(
            &self,
            key: &Key,
            time: CompactDateTime,
        ) -> Result<(), Self::Error> {
            self.inner
                .remove_deferred_timer(key, time)
                .await
                .map_err(|_| TestStoreError::transient())
        }

        async fn set_retry_count(&self, key: &Key, count: u32) -> Result<(), Self::Error> {
            self.inner
                .set_retry_count(key, count)
                .await
                .map_err(|_| TestStoreError::transient())
        }

        async fn delete_key(&self, key: &Key) -> Result<(), Self::Error> {
            self.inner
                .delete_key(key)
                .await
                .map_err(|_| TestStoreError::transient())
        }
    }

    /// Helper to set up a failing store with N deferred timers.
    async fn setup_failing_store(
        timer_count: usize,
        fail_after: usize,
    ) -> color_eyre::Result<(KeyedMockContext, FailAfterNStore, Key)> {
        let inner_store = CachedTimerDeferStore::new(MemoryTimerDeferStore::new(), 100);
        let inner_context = KeyedMockContext::new("test-key");
        let key: Key = Arc::from("test-key");

        // Defer N timers at times 1000, 2000, 3000, ...
        for i in 0..timer_count {
            let time_secs = ((i + 1) * 1000) as u32;
            let trigger = Trigger::new(
                key.clone(),
                CompactDateTime::from(time_secs),
                TimerType::Application,
                tracing::Span::current(),
            );
            if i == 0 {
                inner_store.defer_first_timer(&trigger).await?;
            } else {
                inner_store.defer_additional_timer(&trigger).await?;
            }
        }

        let failing_store = FailAfterNStore {
            inner: inner_store,
            fail_after,
        };

        Ok((inner_context, failing_store, key))
    }

    /// Collects items from the scheduled stream until error or completion.
    async fn collect_until_error<C, S>(
        context: &TimerDeferContext<C, S>,
    ) -> (
        Vec<CompactDateTime>,
        Option<TimerDeferContextError<C::Error, S::Error>>,
    )
    where
        C: EventContext + Clone + Send + Sync,
        S: TimerDeferStore + Clone + Send + Sync,
    {
        let mut collected = Vec::new();
        let stream = context.scheduled(TimerType::Application);
        futures::pin_mut!(stream);

        loop {
            match stream.try_next().await {
                Ok(Some(time)) => collected.push(time),
                Ok(None) => return (collected, None),
                Err(e) => return (collected, Some(e)),
            }
        }
    }

    /// T070: `scheduled()` propagates store error on immediate failure.
    ///
    /// Tests that when the deferred store fails on the very first poll,
    /// the error is correctly propagated as `TimerDeferContextError::Store`.
    #[test]
    fn scheduled_propagates_immediate_store_error() {
        init_test_logging();

        TEST_RUNTIME.block_on(async {
            // Set up store with 3 timers that fails on first poll (fail_after=0)
            let (inner_context, failing_store, key) = setup_failing_store(3, 0).await.ok()?;
            let context = TimerDeferContext::new(inner_context, failing_store, key);

            let (collected, error) = collect_until_error(&context).await;

            // Should get error immediately with no items collected
            assert!(error.is_some(), "Should have received a Store error");
            assert!(
                matches!(error, Some(TimerDeferContextError::Store(_))),
                "Error should be Store variant"
            );
            assert!(
                collected.is_empty(),
                "Should not yield any items before immediate error; got: {collected:?}"
            );

            Some(())
        });
    }

    /// T070b: `scheduled()` propagates store error after yielding some items.
    ///
    /// Tests that when the deferred store fails mid-iteration, previously
    /// yielded items are preserved and the error is correctly wrapped.
    #[test]
    fn scheduled_propagates_mid_iteration_store_error() {
        init_test_logging();

        TEST_RUNTIME.block_on(async {
            // Set up store with 5 timers that fails after yielding 2 (fail_after=2)
            let (inner_context, failing_store, key) = setup_failing_store(5, 2).await.ok()?;
            let context = TimerDeferContext::new(inner_context, failing_store, key);

            let (collected, error) = collect_until_error(&context).await;

            // Should get error after some items
            assert!(error.is_some(), "Should have received a Store error");
            assert!(
                matches!(error, Some(TimerDeferContextError::Store(_))),
                "Error should be Store variant"
            );

            // The merge algorithm looks ahead, so the number of items yielded
            // depends on timing. But we should have at least 1 item (first was
            // fetched successfully) and fewer than all 5.
            assert!(
                !collected.is_empty(),
                "Should yield at least one item before error"
            );
            assert!(
                collected.len() < 5,
                "Should not yield all items; got {} items",
                collected.len()
            );

            // Items should be in sorted order
            for window in collected.windows(2) {
                assert!(
                    window[0] <= window[1],
                    "Items should be sorted; got: {collected:?}"
                );
            }

            Some(())
        });
    }

    /// T070c: `scheduled()` returns all items when store succeeds.
    ///
    /// Control test to ensure normal operation works correctly.
    #[test]
    fn scheduled_returns_all_items_on_success() {
        init_test_logging();

        TEST_RUNTIME.block_on(async {
            // Set up store with 4 timers that never fails (fail_after > count)
            let (inner_context, failing_store, key) = setup_failing_store(4, 100).await.ok()?;
            let context = TimerDeferContext::new(inner_context, failing_store, key);

            let (collected, error) = collect_until_error(&context).await;

            // Should complete without error
            assert!(error.is_none(), "Should not have error; got: {error:?}");

            // Should have all 4 items in sorted order
            assert_eq!(
                collected,
                vec![
                    CompactDateTime::from(1000_u32),
                    CompactDateTime::from(2000_u32),
                    CompactDateTime::from(3000_u32),
                    CompactDateTime::from(4000_u32),
                ],
                "Should yield all items in order"
            );

            Some(())
        });
    }
}

/// `TimerDeferContextError` classifies errors correctly by delegation.
#[test]
fn context_error_classification_delegates_correctly() {
    use crate::consumer::middleware::defer::timer::context::TimerDeferContextError;
    use crate::consumer::middleware::{ClassifyError, ErrorCategory};

    init_test_logging();

    // Create errors with known classifications
    let transient_error = OutcomeError::transient();
    let permanent_error = OutcomeError::permanent();

    // Context errors should delegate to inner error classification
    let context_transient: TimerDeferContextError<OutcomeError, Infallible> =
        TimerDeferContextError::Context(transient_error);
    assert!(
        matches!(context_transient.classify_error(), ErrorCategory::Transient),
        "Context(Transient) should classify as Transient"
    );

    let context_permanent: TimerDeferContextError<OutcomeError, Infallible> =
        TimerDeferContextError::Context(permanent_error);
    assert!(
        matches!(context_permanent.classify_error(), ErrorCategory::Permanent),
        "Context(Permanent) should classify as Permanent"
    );

    // Store errors should delegate to inner error classification
    let store_transient: TimerDeferContextError<Infallible, OutcomeError> =
        TimerDeferContextError::Store(OutcomeError::transient());
    assert!(
        matches!(store_transient.classify_error(), ErrorCategory::Transient),
        "Store(Transient) should classify as Transient"
    );

    let store_permanent: TimerDeferContextError<Infallible, OutcomeError> =
        TimerDeferContextError::Store(OutcomeError::permanent());
    assert!(
        matches!(store_permanent.classify_error(), ErrorCategory::Permanent),
        "Store(Permanent) should classify as Permanent"
    );
}
