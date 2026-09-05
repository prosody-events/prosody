//! Tests for the [`DeduplicationStore`](super::store::DeduplicationStore) trait
//! implementations.
//!
//! This module contains property-based tests that verify the behavior
//! of any implementation of `DeduplicationStore` using model-based testing.

mod handler;
pub mod prop_dedup_store;

/// Test marker presence across assignments from one provider.
#[macro_export]
macro_rules! dedup_store_tests {
    ($provider_constructor:expr $(, tests = $count:expr)?) => {
        use quickcheck::{QuickCheck, TestResult};
        use tokio::runtime::Builder;
        use tracing::Instrument;
        use $crate::consumer::middleware::deduplication::tests::prop_dedup_store::*;

        #[test]
        fn test_dedup_store_model_equivalence() {
            $crate::tracing::init_test_logging();
            let _span = tracing::info_span!("test").entered();

            QuickCheck::new()$(.tests($count))?
                .quickcheck(prop_model_equivalence as fn(DeduplicationTestInput) -> TestResult);
        }

        fn prop_model_equivalence(input: DeduplicationTestInput) -> TestResult {
            let span = tracing::Span::current();

            let runtime = match Builder::new_multi_thread().enable_all().build() {
                Ok(rt) => rt,
                Err(e) => return TestResult::error(format!("Failed to create runtime: {e}")),
            };

            let provider = match runtime.block_on(($provider_constructor).instrument(span.clone())) {
                Ok(s) => s,
                Err(e) => return TestResult::error(format!("Failed to create provider: {e:?}")),
            };

            match runtime.block_on(
                async { prop_dedup_store_model_equivalence(&provider, input).await }.instrument(span),
            ) {
                Ok(()) => TestResult::passed(),
                Err(e) => TestResult::error(format!("{e:?}")),
            }
        }
    };
}
