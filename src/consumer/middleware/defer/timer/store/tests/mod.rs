//! Test module for timer defer store implementations.
//!
//! Contains property-based tests and shared test utilities for verifying
//! [`TimerDeferStore`](super::TimerDeferStore) implementations.

pub mod prop_timer_defer_store;

/// Alias for the result returned by timer defer store tests.
pub type TestStoreResult = Result<(), String>;

/// Generate comprehensive test suite for a `TimerDeferStore` implementation.
///
/// This macro creates property-based tests using `QuickCheck` to verify
/// that a `TimerDeferStore` implementation correctly handles all operations
/// by comparing against a simple reference model.
///
/// # Usage
///
/// ```rust,ignore
/// timer_defer_store_tests!(async { MyStore::new().await });
/// ```
///
/// # Arguments
///
/// * `$store_constructor` - Expression that creates a `Result<Store, Error>`
#[macro_export]
macro_rules! timer_defer_store_tests {
    ($store_constructor:expr) => {
        use quickcheck::{QuickCheck, TestResult};
        use tokio::runtime::Builder;
        use tracing::Instrument;
        use $crate::consumer::middleware::defer::timer::store::tests::prop_timer_defer_store::*;

        #[test]
        fn test_timer_defer_store_model_equivalence() {
            // Initialize test logging
            $crate::tracing::init_test_logging();
            let _span = tracing::info_span!("test").entered();

            QuickCheck::new()
                .quickcheck(prop_model_equivalence as fn(TimerDeferTestInput) -> TestResult);
        }

        fn prop_model_equivalence(input: TimerDeferTestInput) -> TestResult {
            // Capture the current span to propagate into async runtime
            let span = tracing::Span::current();

            // Create runtime for this test invocation
            let runtime = match Builder::new_multi_thread().enable_all().build() {
                Ok(rt) => rt,
                Err(e) => return TestResult::error(format!("Failed to create runtime: {e}")),
            };

            // Create store instance - evaluates the constructor expression
            let store = match runtime.block_on(($store_constructor).instrument(span.clone())) {
                Ok(s) => s,
                Err(e) => return TestResult::error(format!("Failed to create store: {e:?}")),
            };

            // Call the async test function within the same runtime
            match runtime.block_on(
                async { prop_timer_defer_store_model_equivalence(&store, input).await }
                    .instrument(span),
            ) {
                Ok(()) => TestResult::passed(),
                Err(e) => TestResult::error(format!("{e:?}")),
            }
        }
    };
}
