//! Tests for the [`DeduplicationStore`](super::store::DeduplicationStore) trait
//! implementations.
//!
//! This module contains property-based tests that verify the behavior
//! of any implementation of `DeduplicationStore` using model-based testing.

mod handler;
pub mod prop_dedup_store;

/// Generate a comprehensive test suite for a `DeduplicationStore`
/// implementation.
///
/// This macro creates property-based tests using `QuickCheck` to verify
/// that a `DeduplicationStore` implementation correctly handles all operations
/// by comparing against a simple reference model.
///
/// # Usage
///
/// ```rust,ignore
/// dedup_store_tests!(async { Ok::<_, Report>(MyStore::new()) });
/// ```
///
/// # Arguments
///
/// * `$store_constructor` - Expression that creates a `Result<Store, Error>`
#[macro_export]
macro_rules! dedup_store_tests {
    ($store_constructor:expr) => {
        use quickcheck::{QuickCheck, TestResult};
        use tokio::runtime::Builder;
        use tracing::Instrument;
        use $crate::consumer::middleware::deduplication::tests::prop_dedup_store::*;

        #[test]
        fn test_dedup_store_model_equivalence() {
            $crate::tracing::init_test_logging();
            let _span = tracing::info_span!("test").entered();

            QuickCheck::new()
                .quickcheck(prop_model_equivalence as fn(DeduplicationTestInput) -> TestResult);
        }

        fn prop_model_equivalence(input: DeduplicationTestInput) -> TestResult {
            let span = tracing::Span::current();

            let runtime = match Builder::new_multi_thread().enable_all().build() {
                Ok(rt) => rt,
                Err(e) => return TestResult::error(format!("Failed to create runtime: {e}")),
            };

            let store = match runtime.block_on(($store_constructor).instrument(span.clone())) {
                Ok(s) => s,
                Err(e) => return TestResult::error(format!("Failed to create store: {e:?}")),
            };

            match runtime.block_on(
                async { prop_dedup_store_model_equivalence(&store, input).await }.instrument(span),
            ) {
                Ok(()) => TestResult::passed(),
                Err(e) => TestResult::error(format!("{e:?}")),
            }
        }
    };
}
