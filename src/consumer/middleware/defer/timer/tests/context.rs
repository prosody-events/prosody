//! Tests for `TimerDeferContext` wrapper.
//!
//! Verifies that the context wrapper correctly unifies active and deferred
//! timer operations, delegating appropriately based on deferral state.

use super::*;
use crate::consumer::Keyed;
use crate::consumer::event_context::TerminationSignals;
use crate::consumer::middleware::defer::timer::context::TimerDeferContext;
use crate::consumer::middleware::defer::timer::store::TimerDeferStore;
use crate::consumer::middleware::defer::timer::store::memory::MemoryTimerDeferStore;
use crate::otel::SpanRelation;
use crate::timers::datetime::CompactDateTime;
use crate::timers::{TimerType, Trigger};
use crate::tracing::init_test_logging;
use std::convert::Infallible;
use std::future::{Future, ready};
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

impl TerminationSignals for KeyedMockContext {
    fn is_shutdown(&self) -> bool {
        self.inner.is_shutdown()
    }

    fn is_message_cancelled(&self) -> bool {
        self.inner.is_message_cancelled()
    }

    fn on_shutdown(&self) -> impl Future<Output = ()> + Send + 'static {
        self.inner.on_shutdown()
    }

    fn on_message_cancelled(&self) -> impl Future<Output = ()> + Send + 'static {
        self.inner.on_message_cancelled()
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

    fn uncancel(&self) {
        self.inner.uncancel();
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
    ) -> impl Future<Output = Result<Vec<CompactDateTime>, Self::Error>> + Send + 'static {
        let times: Vec<CompactDateTime> = self
            .active_timers
            .lock()
            .iter()
            .filter(|(_, t)| *t == timer_type)
            .map(|(time, _)| *time)
            .collect();
        ready(Ok(times))
    }
}

// ============================================================================
// Test Helper
// ============================================================================

struct ContextTestHarness {
    store: MemoryTimerDeferStore,
    inner_context: KeyedMockContext,
}

impl ContextTestHarness {
    fn new(key: &str) -> Self {
        let store = MemoryTimerDeferStore::new(SpanRelation::default());
        let inner_context = KeyedMockContext::new(key);
        Self {
            store,
            inner_context,
        }
    }

    fn create_wrapped_context(&self) -> TimerDeferContext<KeyedMockContext, MemoryTimerDeferStore> {
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
        Ok(self.store.deferred_times(self.key()).await?)
    }
}

// ============================================================================
// Tests
// ============================================================================

/// `schedule()` when NOT deferred delegates to inner context.
#[test]
fn schedule_when_not_deferred_adds_to_active() -> color_eyre::Result<()> {
    init_test_logging();

    TEST_RUNTIME.block_on(async {
        let harness = ContextTestHarness::new("test-key");
        let context = harness.create_wrapped_context();

        // Key is not deferred
        assert!(!harness.is_deferred().await?);

        // Schedule via wrapped context
        let time = CompactDateTime::from(1000_u32);
        context.schedule(time, TimerType::Application).await?;

        // Should have been added to inner context's active timers
        let active = harness.inner_context.active_application_timers();
        assert!(active.contains(&time), "Timer should be in active store");

        // Should NOT be in defer store
        let deferred = harness.store.get_next_deferred_timer(harness.key()).await?;
        assert!(deferred.is_none(), "Timer should not be in defer store");

        Ok(())
    })
}

/// `schedule()` when deferred appends to defer store.
#[test]
fn schedule_when_deferred_appends_to_store() -> color_eyre::Result<()> {
    init_test_logging();

    TEST_RUNTIME.block_on(async {
        let harness = ContextTestHarness::new("test-key");

        // First, defer a timer to make the key deferred
        harness.defer_timer(1000).await?;
        assert!(harness.is_deferred().await?);

        let context = harness.create_wrapped_context();

        // Schedule a NEW timer via wrapped context
        let time = CompactDateTime::from(2000_u32);
        context.schedule(time, TimerType::Application).await?;

        // Timer should be appended to defer store (not inner context)
        let deferred = harness.deferred_times().await?;
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
        let scheduled: Vec<CompactDateTime> = context.scheduled(TimerType::Application).await?;
        assert!(
            scheduled.contains(&time),
            "Timer scheduled while deferred must appear in scheduled(); got: {scheduled:?}"
        );

        Ok(())
    })
}

/// `unschedule()` removes from both stores when deferred.
#[test]
fn unschedule_removes_from_both_stores() -> color_eyre::Result<()> {
    init_test_logging();

    TEST_RUNTIME.block_on(async {
        let harness = ContextTestHarness::new("test-key");

        // Add timer to active store first
        harness
            .inner_context
            .schedule(CompactDateTime::from(1000_u32), TimerType::Application)
            .await?;

        // Defer timers at 500 and 1500 to make key deferred
        harness.defer_timer(500).await?;
        harness.defer_additional_timer(1500).await?;

        let context = harness.create_wrapped_context();

        // Unschedule the timer at 1000 (in active store)
        context
            .unschedule(CompactDateTime::from(1000_u32), TimerType::Application)
            .await?;

        // Should be removed from active store
        let active = harness.inner_context.active_application_timers();
        assert!(
            !active.contains(&CompactDateTime::from(1000_u32)),
            "Timer should be removed from active store"
        );

        // Unschedule the timer at 500 (in defer store)
        context
            .unschedule(CompactDateTime::from(500_u32), TimerType::Application)
            .await?;

        // Should be removed from defer store (only 1500 remains)
        let deferred = harness.deferred_times().await?;
        assert!(
            !deferred.contains(&CompactDateTime::from(500_u32)),
            "Timer at 500 should be removed from defer store"
        );
        assert!(
            deferred.contains(&CompactDateTime::from(1500_u32)),
            "Timer at 1500 should still be in defer store"
        );

        // CLIENT INVARIANT: scheduled() must reflect unschedule operations
        let scheduled: Vec<CompactDateTime> = context.scheduled(TimerType::Application).await?;
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

        Ok(())
    })
}

/// `clear_scheduled()` clears both stores and cancels `DeferredTimer`.
#[test]
fn clear_scheduled_clears_both_stores_and_cancels_deferred_timer() -> color_eyre::Result<()> {
    init_test_logging();

    TEST_RUNTIME.block_on(async {
        let harness = ContextTestHarness::new("test-key");

        // Add timer to active store
        harness
            .inner_context
            .schedule(CompactDateTime::from(1000_u32), TimerType::Application)
            .await?;

        // Defer a timer
        harness.defer_timer(500).await?;

        // Schedule a DeferredTimer (simulates the retry timer)
        harness
            .inner_context
            .schedule(CompactDateTime::from(600_u32), TimerType::DeferredTimer)
            .await?;

        let context = harness.create_wrapped_context();

        // Clear all Application timers
        context.clear_scheduled(TimerType::Application).await?;

        // Active store should have no Application timers
        let active = harness.inner_context.active_application_timers();
        assert!(
            active.is_empty(),
            "Active Application timers should be cleared"
        );

        // Defer store should be cleared (key deleted)
        let deferred = harness.deferred_times().await?;
        assert!(
            deferred.is_empty(),
            "Defer store should be cleared; got: {deferred:?}"
        );

        // Key should no longer be deferred
        assert!(
            !harness.is_deferred().await?,
            "Key should not be deferred after clear_scheduled"
        );

        // DeferredTimer should be cancelled
        let deferred_timers = harness.inner_context.active_deferred_timers();
        assert!(
            deferred_timers.is_empty(),
            "DeferredTimer should be cancelled; got: {deferred_timers:?}"
        );

        Ok(())
    })
}

/// `scheduled()` merges both stores sorted and deduplicated.
#[test]
fn scheduled_merges_both_stores_sorted_deduplicated() -> color_eyre::Result<()> {
    init_test_logging();

    TEST_RUNTIME.block_on(async {
        let harness = ContextTestHarness::new("test-key");

        // Add timers to active store: 1000, 3000
        harness
            .inner_context
            .schedule(CompactDateTime::from(1000_u32), TimerType::Application)
            .await?;
        harness
            .inner_context
            .schedule(CompactDateTime::from(3000_u32), TimerType::Application)
            .await?;

        // Defer timers at 500, 2000
        harness.defer_timer(500).await?;
        harness.defer_additional_timer(2000).await?;

        let context = harness.create_wrapped_context();

        // Get scheduled times - should be merged and sorted
        let times: Vec<CompactDateTime> = context.scheduled(TimerType::Application).await?;

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

        Ok(())
    })
}

/// `scheduled()` deduplicates when same time exists in both stores.
#[test]
fn scheduled_deduplicates_when_same_time_in_both_stores() -> color_eyre::Result<()> {
    init_test_logging();

    TEST_RUNTIME.block_on(async {
        let harness = ContextTestHarness::new("test-key");

        // Add timer at 1000 to active store
        harness
            .inner_context
            .schedule(CompactDateTime::from(1000_u32), TimerType::Application)
            .await?;

        // Defer timer at the SAME time 1000
        harness.defer_timer(1000).await?;

        let context = harness.create_wrapped_context();

        // Get scheduled times - should be deduplicated
        let times: Vec<CompactDateTime> = context.scheduled(TimerType::Application).await?;

        // Should have only ONE entry at 1000
        assert_eq!(times.len(), 1, "Duplicate time should appear only once");
        assert_eq!(times[0], CompactDateTime::from(1000_u32));

        Ok(())
    })
}

/// `clear_and_schedule()` makes key not deferred after completion.
#[test]
fn clear_and_schedule_makes_key_not_deferred() -> color_eyre::Result<()> {
    init_test_logging();

    TEST_RUNTIME.block_on(async {
        let harness = ContextTestHarness::new("test-key");

        // Defer a timer
        harness.defer_timer(1000).await?;
        assert!(harness.is_deferred().await?, "Key should be deferred");

        let context = harness.create_wrapped_context();

        // Clear and schedule a new timer
        let new_time = CompactDateTime::from(2000_u32);
        context
            .clear_and_schedule(new_time, TimerType::Application)
            .await?;

        // Key should no longer be deferred
        assert!(
            !harness.is_deferred().await?,
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

        Ok(())
    })
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
fn client_operations_work_identically_when_deferred() -> color_eyre::Result<()> {
    init_test_logging();

    TEST_RUNTIME.block_on(async {
        let harness = ContextTestHarness::new("test-key");

        // Make key deferred by deferring an initial timer
        harness.defer_timer(1000).await?;
        assert!(harness.is_deferred().await?);

        let context = harness.create_wrapped_context();

        // STEP 1: Schedule a new timer while deferred
        let client_timer = CompactDateTime::from(2000_u32);
        context
            .schedule(client_timer, TimerType::Application)
            .await?;

        // CLIENT INVARIANT: Scheduled timer must be visible
        let scheduled: Vec<CompactDateTime> = context.scheduled(TimerType::Application).await?;
        assert!(
            scheduled.contains(&client_timer),
            "schedule() while deferred: timer must appear in scheduled()"
        );

        // STEP 2: Unschedule the timer
        context
            .unschedule(client_timer, TimerType::Application)
            .await?;

        // CLIENT INVARIANT: Unscheduled timer must be gone
        let scheduled: Vec<CompactDateTime> = context.scheduled(TimerType::Application).await?;
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
            .await?;
        let replacement = CompactDateTime::from(3000_u32);
        context
            .clear_and_schedule(replacement, TimerType::Application)
            .await?;

        // CLIENT INVARIANT: Only the replacement timer should exist
        let scheduled: Vec<CompactDateTime> = context.scheduled(TimerType::Application).await?;
        assert_eq!(
            scheduled,
            vec![replacement],
            "clear_and_schedule() should leave only the new timer"
        );

        // Key should no longer be deferred (clear_and_schedule deletes the key)
        assert!(
            !harness.is_deferred().await?,
            "clear_and_schedule should un-defer the key"
        );

        Ok(())
    })
}

/// Non-Application timer types pass through to inner context.
#[test]
fn non_application_timers_pass_through() -> color_eyre::Result<()> {
    init_test_logging();

    TEST_RUNTIME.block_on(async {
        let harness = ContextTestHarness::new("test-key");

        // Defer a timer to make key deferred
        harness.defer_timer(1000).await?;
        assert!(harness.is_deferred().await?);

        let context = harness.create_wrapped_context();

        // Schedule a DeferredMessage timer (internal type)
        let time = CompactDateTime::from(2000_u32);
        context.schedule(time, TimerType::DeferredMessage).await?;

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

        Ok(())
    })
}

// ============================================================================
// Error Handling Tests
// ============================================================================

mod error_handling {
    use super::*;
    use crate::consumer::middleware::defer::timer::context::TimerDeferContextError;
    use crate::consumer::middleware::defer::timer::store::TimerRetryCompletionResult;
    use crate::error::{ClassifyError, ErrorCategory};
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

    /// Store wrapper that injects an error from `deferred_times` when the
    /// total item count exceeds `fail_after`.
    ///
    /// With Vec semantics errors are all-or-nothing: if the count exceeds the
    /// threshold the entire call returns `Err` with no partial results.
    #[derive(Clone)]
    struct FailAfterNStore {
        inner: MemoryTimerDeferStore,
        /// Inject an error when `deferred_times` returns more than this many
        /// items. Set to `usize::MAX` (or any value ≥ item count) for success.
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
        ) -> impl Future<Output = Result<Vec<CompactDateTime>, Self::Error>> + Send + 'static
        {
            let inner_fut = self.inner.deferred_times(key);
            let fail_after = self.fail_after;

            async move {
                let times = inner_fut.await.map_err(|_| TestStoreError::transient())?;
                if fail_after < times.len() {
                    Err(TestStoreError::transient())
                } else {
                    Ok(times)
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
        let inner_store = MemoryTimerDeferStore::new(SpanRelation::default());
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

    /// `scheduled()` propagates store error on immediate failure.
    ///
    /// Tests that when the deferred store fails (`fail_after=0` means any count
    /// triggers an error), the error is correctly propagated as
    /// `TimerDeferContextError::Store`.
    #[test]
    fn scheduled_propagates_immediate_store_error() -> color_eyre::Result<()> {
        init_test_logging();

        TEST_RUNTIME.block_on(async {
            // Set up store with 3 timers that fails (fail_after=0 < 3)
            let (inner_context, failing_store, key) = setup_failing_store(3, 0).await?;
            let context = TimerDeferContext::new(inner_context, failing_store, key);

            let result = context.scheduled(TimerType::Application).await;

            assert!(
                matches!(result, Err(TimerDeferContextError::Store(_))),
                "Error should be Store variant; got: {result:?}"
            );

            Ok(())
        })
    }

    /// `scheduled()` returns all items when store succeeds.
    ///
    /// Control test to ensure normal operation works correctly.
    #[test]
    fn scheduled_returns_all_items_on_success() -> color_eyre::Result<()> {
        init_test_logging();

        TEST_RUNTIME.block_on(async {
            // Set up store with 4 timers that never fails (fail_after > count)
            let (inner_context, failing_store, key) = setup_failing_store(4, 100).await?;
            let context = TimerDeferContext::new(inner_context, failing_store, key);

            let times = context.scheduled(TimerType::Application).await?;

            // Should have all 4 items in sorted order
            assert_eq!(
                times,
                vec![
                    CompactDateTime::from(1000_u32),
                    CompactDateTime::from(2000_u32),
                    CompactDateTime::from(3000_u32),
                    CompactDateTime::from(4000_u32),
                ],
                "Should yield all items in order"
            );

            Ok(())
        })
    }
}

/// `TimerDeferContextError` classifies errors correctly by delegation.
#[test]
fn context_error_classification_delegates_correctly() {
    use crate::consumer::middleware::defer::timer::context::TimerDeferContextError;
    use crate::error::{ClassifyError, ErrorCategory};

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
