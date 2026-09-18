//! Checks context errors and partial reads.

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
    ) -> impl Future<Output = Result<Vec<CompactDateTime>, Self::Error>> + Send + 'static {
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

/// `TimerDeferContextError` classifies errors correctly by delegation.
#[test]
fn context_error_classification_delegates_correctly() {
    use crate::consumer::middleware::defer::timer::context::TimerDeferContextError;
    use crate::error::{ClassifyError, ErrorCategory};

    init_test_logging();

    // Create errors with known classifications
    let transient_error = OutcomeError::Transient;
    let permanent_error = OutcomeError::Permanent;

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
        TimerDeferContextError::Store(OutcomeError::Transient);
    assert!(
        matches!(store_transient.classify_error(), ErrorCategory::Transient),
        "Store(Transient) should classify as Transient"
    );

    let store_permanent: TimerDeferContextError<Infallible, OutcomeError> =
        TimerDeferContextError::Store(OutcomeError::Permanent);
    assert!(
        matches!(store_permanent.classify_error(), ErrorCategory::Permanent),
        "Store(Permanent) should classify as Permanent"
    );
}
