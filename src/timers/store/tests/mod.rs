//! Tests for the `TriggerStore` trait implementations.
//!
//! This module contains property-based tests that verify the behavior
//! of any implementation of the `TriggerStore` trait. The tests ensure
//! that triggers with the same key and time act as upserts, maintaining
//! data consistency across all storage backends.

use crate::Key;
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::timers::store::{Segment, SegmentVersion};
use crate::timers::{TimerType, Trigger};
use quickcheck::{Arbitrary, Gen};
use std::fmt::Debug;
use uuid::Uuid;

/// Common utilities and helpers for trigger store tests.
pub mod common;
/// Tests for key contention scenarios in the trigger store.
pub mod contention;
/// Tests for operations spanning multiple slabs.
pub mod cross_slab;
/// Property-based tests for V2 high-level dual-index operations.
pub mod prop_high_level;
/// Property-based tests for key trigger table operations.
pub mod prop_key_triggers;
/// Property-based tests for segment table operations.
pub mod prop_segments;
/// Property-based tests for slab metadata table operations.
pub mod prop_slab_metadata;
/// Property-based tests for slab trigger table operations.
pub mod prop_slab_triggers;
/// Tests for segment management in the trigger store.
// Removed segment.rs - redundant with prop_segments.rs property-based tests
/// Tests for sequential interleavings of trigger operations.
pub mod sequential_interleavings;
/// Tests for slab-related functionality in the trigger store.
pub mod slabs;
/// Tests verifying trigger consistency across operations.
pub mod trigger_consistency;
/// Tests for basic trigger add/remove/clear operations.
pub mod trigger_operations;

/// Alias for the result returned by trigger store tests.
pub type TestStoreResult = Result<(), String>;

// Implement Arbitrary trait for test types - these should only be available
// during testing

impl Arbitrary for CompactDuration {
    fn arbitrary(g: &mut Gen) -> Self {
        // Generate durations between 1 second and 24 hours
        let seconds = u32::arbitrary(g) % (24 * 60 * 60) + 1;
        CompactDuration::new(seconds)
    }
}

impl Arbitrary for CompactDateTime {
    fn arbitrary(g: &mut Gen) -> Self {
        // Current time plus a random offset
        let offset_seconds = i64::arbitrary(g) % (30 * 24 * 60 * 60);
        // Generate an epoch timestamp as u32
        let now = chrono::Utc::now().timestamp() as u32;
        let unsigned_offset = (offset_seconds.unsigned_abs() % u64::from(u32::MAX)) as u32;
        let time_value = now.saturating_add(unsigned_offset);
        CompactDateTime::from(time_value)
    }
}

impl Arbitrary for Segment {
    fn arbitrary(g: &mut Gen) -> Self {
        let id = Uuid::new_v4();
        let name = format!("segment-{}", u16::arbitrary(g));
        let slab_size = CompactDuration::arbitrary(g);

        Segment {
            id,
            name,
            slab_size,
            version: SegmentVersion::V2,
        }
    }
}

#[derive(Debug, Clone)]
/// Represents an operation performed on a trigger.
pub enum TriggerOperation {
    /// Add operation for a trigger.
    Add,
    /// Remove operation for a trigger.
    Remove,
    /// Clear operation for a trigger.
    Clear,
}

/// Test input for segment operations
#[derive(Debug, Clone)]
pub struct SegmentTestInput {
    /// Segments involved in the test.
    pub segments: Vec<Segment>,
}

impl Arbitrary for SegmentTestInput {
    fn arbitrary(g: &mut Gen) -> Self {
        let count = usize::arbitrary(g) % 10 + 1; // 1-10 segments
        let segments = (0..count).map(|_| Segment::arbitrary(g)).collect();

        SegmentTestInput { segments }
    }
}

/// Test input for trigger operations
#[derive(Debug, Clone)]
pub struct TriggerTestInput {
    /// The segment under test.
    /// Segment on which the trigger sequence is applied.
    /// The segment in which to apply the operation sequence.
    /// The segment in which to apply the operation sequence.
    pub segment: Segment,
    /// The trigger entries to use in tests.
    pub triggers: Vec<Trigger>,
}

impl Arbitrary for TriggerTestInput {
    fn arbitrary(g: &mut Gen) -> Self {
        let segment = Segment::arbitrary(g);

        let key_count = usize::arbitrary(g) % 5 + 1; // 1-5 keys
        let triggers_per_key = usize::arbitrary(g) % 10 + 1; // 1-10 triggers per key

        let mut triggers = Vec::new();
        for i in 0..key_count {
            let key = format!("key-{i}");
            for _ in 0..triggers_per_key {
                let time = CompactDateTime::arbitrary(g);
                triggers.push(Trigger::new(
                    Key::from(key.clone()),
                    time,
                    TimerType::Application,
                    tracing::Span::current(),
                ));
            }
        }

        TriggerTestInput { segment, triggers }
    }
}

/// Represents a sequence of operations on the same trigger
#[derive(Debug, Clone)]
pub struct TriggerSequence {
    /// Segment the triggers belong to
    pub segment: Segment,
    /// Key identifying the trigger.
    pub key: Key,
    /// Sequence of trigger times to test.
    pub times: Vec<CompactDateTime>,
    /// Corresponding operations (add, remove, clear) for each time.
    pub operations: Vec<TriggerOperation>,
}

impl Arbitrary for TriggerSequence {
    fn arbitrary(g: &mut Gen) -> Self {
        let segment = Segment::arbitrary(g);

        let key_str = format!("key-{}", u16::arbitrary(g));
        let key = Key::from(key_str);

        // Generate 1-10 times
        let count = usize::arbitrary(g) % 10 + 1;
        let mut times = Vec::with_capacity(count);
        for _ in 0..count {
            times.push(CompactDateTime::arbitrary(g));
        }

        // Generate operation sequence with a bias towards adding
        let op_count = usize::arbitrary(g) % 15 + 5; // 5-20 operations
        let mut operations = Vec::with_capacity(op_count);
        for _ in 0..op_count {
            // Bias towards Add (60%), Remove (30%), Clear (10%)
            let choice = u8::arbitrary(g) % 10;
            let op = match choice {
                0..=5 => TriggerOperation::Add,
                6..=8 => TriggerOperation::Remove,
                _ => TriggerOperation::Clear,
            };
            operations.push(op);
        }

        TriggerSequence {
            segment,
            key,
            times,
            operations,
        }
    }
}

/// Generate comprehensive test suite for a `TriggerStore` implementation.
///
/// This macro creates a full suite of property-based tests using `QuickCheck`
/// to verify that a `TriggerStore` implementation correctly handles all
/// operations across various scenarios including concurrent access, data
/// consistency, and error conditions.
///
/// The macro expects an async constructor that returns a `Result<Store,
/// Error>`.
///
/// # Usage
///
/// ```rust,ignore
/// trigger_store_tests!(MyStore, MyStore::new(), 100);
/// ```
///
/// # Arguments
///
/// * `$store_type` - The type implementing `TriggerStore`
/// * `$store_constructor` - Async expression that creates a new instance of the
///   store and returns `Result<$store_type, Error>`
/// * `$test_count` - Number of property tests to run for each test function
#[macro_export]
macro_rules! trigger_store_tests {
    // Variant without test_count - uses QuickCheck's default
    ($operations_type:ty, $operations_constructor:expr, $store_type:ty, $store_constructor:expr) => {
        #[cfg(test)]
        mod tests {
            use super::*;
            use quickcheck::{QuickCheck, TestResult};
            use tokio::runtime::Builder;
            use $crate::timers::store::tests;

            trigger_store_tests!(@test_functions_default);
            trigger_store_tests!(@prop_functions, $operations_type, $operations_constructor, $store_type, $store_constructor);
        }
    };

    // Variant with explicit test_count
    ($operations_type:ty, $operations_constructor:expr, $store_type:ty, $store_constructor:expr, $test_count:expr) => {
        #[cfg(test)]
        mod tests {
            use super::*;
            use quickcheck::{QuickCheck, TestResult};
            use tokio::runtime::Builder;
            use $crate::timers::store::tests;

            trigger_store_tests!(@test_functions, $test_count);
            trigger_store_tests!(@prop_functions, $operations_type, $operations_constructor, $store_type, $store_constructor);
        }
    };

    // Helper to initialize test tracing with OpenTelemetry layer and active span
    (@init_test_tracing) => {{
        use opentelemetry::trace::TracerProvider;
        use opentelemetry_sdk::trace::SdkTracerProvider;
        use tracing::subscriber::set_global_default;
        use tracing_subscriber::filter::LevelFilter;
        use tracing_subscriber::fmt;
        use tracing_subscriber::layer::SubscriberExt;
        use tracing_subscriber::Registry;

        let tracer = SdkTracerProvider::builder().build().tracer("prosody-test");
        let telemetry_layer = tracing_opentelemetry::layer().with_tracer(tracer);

        let subscriber = Registry::default()
            .with(telemetry_layer)
            .with(fmt::layer().with_test_writer())
            .with(LevelFilter::ERROR);

        let _ = set_global_default(subscriber);

        // Return an active span guard to ensure Span::current() works during test
        tracing::info_span!("test").entered()
    }};

    // Test functions using QuickCheck's default
    (@test_functions_default) => {
        #[test]
        fn test_get_slab_range() {
            let _span = trigger_store_tests!(@init_test_tracing);
            QuickCheck::new().quickcheck(
                prop_get_slab_range as fn($crate::timers::store::Segment) -> TestResult,
            );
        }

        #[test]
        fn test_trigger_operations() {
            let _span = trigger_store_tests!(@init_test_tracing);
            QuickCheck::new().quickcheck(
                prop_trigger_operations
                    as fn($crate::timers::store::tests::TriggerTestInput) -> TestResult,
            );
        }

        #[test]
        fn test_trigger_consistency() {
            let _span = trigger_store_tests!(@init_test_tracing);
            QuickCheck::new().quickcheck(
                prop_trigger_consistency
                    as fn($crate::timers::store::tests::TriggerTestInput) -> TestResult,
            );
        }

        #[test]
        fn test_operation_sequences() {
            let _span = trigger_store_tests!(@init_test_tracing);
            QuickCheck::new().quickcheck(
                prop_operation_sequences
                    as fn($crate::timers::store::tests::TriggerSequence) -> TestResult,
            );
        }

        #[test]
        fn test_cross_slab_operations() {
            let _span = trigger_store_tests!(@init_test_tracing);
            QuickCheck::new().quickcheck(
                prop_cross_slab_operations as fn($crate::timers::store::Segment) -> TestResult,
            );
        }

        #[test]
        fn test_key_contention() {
            let _span = trigger_store_tests!(@init_test_tracing);
            QuickCheck::new().quickcheck(
                prop_key_contention as fn($crate::timers::store::Segment) -> TestResult,
            );
        }

        #[test]
        fn test_sequential_interleavings() {
            let _span = trigger_store_tests!(@init_test_tracing);
            QuickCheck::new().quickcheck(
                prop_sequential_interleavings
                    as fn($crate::timers::store::tests::TriggerTestInput) -> TestResult,
            );
        }

        #[test]
        fn test_prop_segment_model_equivalence() {
            let _span = trigger_store_tests!(@init_test_tracing);
            QuickCheck::new().quickcheck(
                prop_segment_model_equivalence as fn($crate::timers::store::tests::prop_segments::SegmentTestInput) -> TestResult,
            );
        }

        #[test]
        fn test_prop_slab_metadata_model_equivalence() {
            let _span = trigger_store_tests!(@init_test_tracing);
            QuickCheck::new().quickcheck(
                prop_slab_metadata_model_equivalence as fn($crate::timers::store::tests::prop_slab_metadata::SlabMetadataTestInput) -> TestResult,
            );
        }

        #[test]
        fn test_prop_slab_trigger_model_equivalence() {
            let _span = trigger_store_tests!(@init_test_tracing);
            QuickCheck::new().quickcheck(
                prop_slab_trigger_model_equivalence as fn($crate::timers::store::tests::prop_slab_triggers::SlabTriggerTestInput) -> TestResult,
            );
        }

        #[test]
        fn test_prop_key_trigger_model_equivalence() {
            let _span = trigger_store_tests!(@init_test_tracing);
            QuickCheck::new().quickcheck(
                prop_key_trigger_model_equivalence as fn($crate::timers::store::tests::prop_key_triggers::KeyTriggerTestInput) -> TestResult,
            );
        }

        #[test]
        fn test_prop_high_level_dual_index_consistency() {
            let _span = trigger_store_tests!(@init_test_tracing);
            QuickCheck::new().quickcheck(
                prop_high_level_dual_index_consistency as fn($crate::timers::store::tests::prop_high_level::HighLevelTestInput) -> TestResult,
            );
        }
    };

    // Test functions with explicit test count
    (@test_functions, $test_count:expr) => {
        // Removed test_segment_operations - redundant with prop_segment_model_equivalence

        // Removed test_slab_operations - uses non-public API (insert_slab, get_slabs)

        #[test]
        fn test_get_slab_range() {
            let _span = trigger_store_tests!(@init_test_tracing);
            QuickCheck::new().tests($test_count).quickcheck(
                prop_get_slab_range as fn($crate::timers::store::Segment) -> TestResult,
            );
        }

        #[test]
        fn test_trigger_operations() {
            let _span = trigger_store_tests!(@init_test_tracing);
            QuickCheck::new().tests($test_count).quickcheck(
                prop_trigger_operations
                    as fn($crate::timers::store::tests::TriggerTestInput) -> TestResult,
            );
        }

        #[test]
        fn test_trigger_consistency() {
            let _span = trigger_store_tests!(@init_test_tracing);
            QuickCheck::new().tests($test_count).quickcheck(
                prop_trigger_consistency
                    as fn($crate::timers::store::tests::TriggerTestInput) -> TestResult,
            );
        }

        #[test]
        fn test_operation_sequences() {
            let _span = trigger_store_tests!(@init_test_tracing);
            QuickCheck::new().tests($test_count).quickcheck(
                prop_operation_sequences
                    as fn($crate::timers::store::tests::TriggerSequence) -> TestResult,
            );
        }

        #[test]
        fn test_cross_slab_operations() {
            let _span = trigger_store_tests!(@init_test_tracing);
            QuickCheck::new().tests($test_count).quickcheck(
                prop_cross_slab_operations as fn($crate::timers::store::Segment) -> TestResult,
            );
        }

        #[test]
        fn test_key_contention() {
            let _span = trigger_store_tests!(@init_test_tracing);
            QuickCheck::new().tests($test_count / 2).quickcheck(
                prop_key_contention as fn($crate::timers::store::Segment) -> TestResult,
            );
        }

        #[test]
        fn test_sequential_interleavings() {
            let _span = trigger_store_tests!(@init_test_tracing);
            QuickCheck::new().tests($test_count).quickcheck(
                prop_sequential_interleavings
                    as fn($crate::timers::store::tests::TriggerTestInput) -> TestResult,
            );
        }#[test]
        fn test_prop_segment_model_equivalence() {
            let _span = trigger_store_tests!(@init_test_tracing);
            QuickCheck::new().tests($test_count).quickcheck(
                prop_segment_model_equivalence as fn($crate::timers::store::tests::prop_segments::SegmentTestInput) -> TestResult,
            );
        }

        #[test]
        fn test_prop_slab_metadata_model_equivalence() {
            let _span = trigger_store_tests!(@init_test_tracing);
            QuickCheck::new().tests($test_count).quickcheck(
                prop_slab_metadata_model_equivalence as fn($crate::timers::store::tests::prop_slab_metadata::SlabMetadataTestInput) -> TestResult,
            );
        }

        #[test]
        fn test_prop_slab_trigger_model_equivalence() {
            let _span = trigger_store_tests!(@init_test_tracing);
            QuickCheck::new().tests($test_count).quickcheck(
                prop_slab_trigger_model_equivalence as fn($crate::timers::store::tests::prop_slab_triggers::SlabTriggerTestInput) -> TestResult,
            );
        }

        #[test]
        fn test_prop_key_trigger_model_equivalence() {
            let _span = trigger_store_tests!(@init_test_tracing);
            QuickCheck::new().tests($test_count).quickcheck(
                prop_key_trigger_model_equivalence as fn($crate::timers::store::tests::prop_key_triggers::KeyTriggerTestInput) -> TestResult,
            );
        }#[test]
        fn test_prop_high_level_dual_index_consistency() {
            let _span = trigger_store_tests!(@init_test_tracing);
            QuickCheck::new().tests($test_count).quickcheck(
                prop_high_level_dual_index_consistency as fn($crate::timers::store::tests::prop_high_level::HighLevelTestInput) -> TestResult,
            );
        }};

    // Property functions
    (@prop_functions, $operations_type:ty, $operations_constructor:expr, $store_type:ty, $store_constructor:expr) => {
        fn prop_segment_model_equivalence(
            input: $crate::timers::store::tests::prop_segments::SegmentTestInput,
        ) -> TestResult {
            use $crate::timers::store::tests::prop_segments::prop_segment_model_equivalence;
            use tracing::Instrument;

            // Capture the current span to propagate into async runtime
            let span = tracing::Span::current();

            // Create runtime for this test invocation
            let runtime = match Builder::new_multi_thread().enable_all().build() {
                Ok(rt) => rt,
                Err(e) => return TestResult::error(format!("Failed to create runtime: {e}")),
            };

            // Create operations instance with the test's slab_size
            let slab_size = input.slab_size;
            let operations = match runtime.block_on(async { ($operations_constructor)(slab_size).await }.instrument(span.clone())) {
                Ok(ops) => ops,
                Err(e) => return TestResult::error(format!("Failed to create operations: {e:?}")),
            };

            // Call the async test function within the same runtime
            match runtime.block_on(async { prop_segment_model_equivalence(&operations, input).await }.instrument(span)) {
                Ok(()) => TestResult::passed(),
                Err(e) => TestResult::error(format!("{e:?}")),
            }
        }

        fn prop_slab_metadata_model_equivalence(
            input: $crate::timers::store::tests::prop_slab_metadata::SlabMetadataTestInput,
        ) -> TestResult {
            use $crate::timers::store::tests::prop_slab_metadata::prop_slab_metadata_model_equivalence;
            use tracing::Instrument;

            // Capture the current span to propagate into async runtime
            let span = tracing::Span::current();

            // Create runtime for this test invocation
            let runtime = match Builder::new_multi_thread().enable_all().build() {
                Ok(rt) => rt,
                Err(e) => return TestResult::error(format!("Failed to create runtime: {e}")),
            };

            // Create operations instance with the test's slab_size
            let slab_size = input.slab_size;
            let operations = match runtime.block_on(async { ($operations_constructor)(slab_size).await }.instrument(span.clone())) {
                Ok(ops) => ops,
                Err(e) => return TestResult::error(format!("Failed to create operations: {e:?}")),
            };

            // Call the async test function within the same runtime
            match runtime.block_on(async { prop_slab_metadata_model_equivalence(&operations, input).await }.instrument(span)) {
                Ok(()) => TestResult::passed(),
                Err(e) => TestResult::error(format!("{e:?}")),
            }
        }

        fn prop_slab_trigger_model_equivalence(
            input: $crate::timers::store::tests::prop_slab_triggers::SlabTriggerTestInput,
        ) -> TestResult {
            use $crate::timers::store::tests::prop_slab_triggers::prop_slab_trigger_model_equivalence;
            use tracing::Instrument;

            // Capture the current span to propagate into async runtime
            let span = tracing::Span::current();

            // Create runtime for this test invocation
            let runtime = match Builder::new_multi_thread().enable_all().build() {
                Ok(rt) => rt,
                Err(e) => return TestResult::error(format!("Failed to create runtime: {e}")),
            };

            // Create operations instance with the test's slab_size
            let slab_size = input.slab_size;
            let operations = match runtime.block_on(async { ($operations_constructor)(slab_size).await }.instrument(span.clone())) {
                Ok(ops) => ops,
                Err(e) => return TestResult::error(format!("Failed to create operations: {e:?}")),
            };

            // Call the async test function within the same runtime
            match runtime.block_on(async { prop_slab_trigger_model_equivalence(&operations, input).await }.instrument(span)) {
                Ok(()) => TestResult::passed(),
                Err(e) => TestResult::error(format!("{e:?}")),
            }
        }

        fn prop_key_trigger_model_equivalence(
            input: $crate::timers::store::tests::prop_key_triggers::KeyTriggerTestInput,
        ) -> TestResult {
            use $crate::timers::store::tests::prop_key_triggers::prop_key_trigger_model_equivalence;
            use tracing::Instrument;

            // Capture the current span to propagate into async runtime
            let span = tracing::Span::current();

            // Create runtime for this test invocation
            let runtime = match Builder::new_multi_thread().enable_all().build() {
                Ok(rt) => rt,
                Err(e) => return TestResult::error(format!("Failed to create runtime: {e}")),
            };

            // Create operations instance with the test's slab_size
            let slab_size = input.slab_size;
            let operations = match runtime.block_on(async { ($operations_constructor)(slab_size).await }.instrument(span.clone())) {
                Ok(ops) => ops,
                Err(e) => return TestResult::error(format!("Failed to create operations: {e:?}")),
            };

            // Call the async test function within the same runtime
            match runtime.block_on(async { prop_key_trigger_model_equivalence(&operations, input).await }.instrument(span)) {
                Ok(()) => TestResult::passed(),
                Err(e) => TestResult::error(format!("{e:?}")),
            }
        }

        fn prop_high_level_dual_index_consistency(
            input: $crate::timers::store::tests::prop_high_level::HighLevelTestInput,
        ) -> TestResult {
            use $crate::timers::store::tests::prop_high_level::prop_high_level_dual_index_consistency;
            use tracing::Instrument;

            // Capture the current span to propagate into async runtime
            let span = tracing::Span::current();

            // Create runtime for this test invocation
            let runtime = match Builder::new_multi_thread().enable_all().build() {
                Ok(rt) => rt,
                Err(e) => return TestResult::error(format!("Failed to create runtime: {e}")),
            };

            // Create store instance with the test's slab_size
            let slab_size = input.slab_size;
            let store = match runtime.block_on(async { ($store_constructor)(slab_size).await }.instrument(span.clone())) {
                Ok(s) => s,
                Err(e) => return TestResult::error(format!("Failed to create store: {e:?}")),
            };

            // Call the async test function within the same runtime
            match runtime.block_on(async { prop_high_level_dual_index_consistency(&store, input).await }.instrument(span)) {
                Ok(()) => TestResult::passed(),
                Err(e) => TestResult::error(format!("{e:?}")),
            }
        }

        fn prop_get_slab_range(segment: $crate::timers::store::Segment) -> TestResult {
            use tracing::Instrument;

            // Capture the current span to propagate into async runtime
            let span = tracing::Span::current();

            // Create runtime for this test invocation
            let runtime = match Builder::new_multi_thread().enable_all().build() {
                Ok(rt) => rt,
                Err(e) => return TestResult::error(format!("Failed to create runtime: {e}")),
            };

            // Create store instance with segment's slab_size
            let slab_size = segment.slab_size;
            let store = match runtime.block_on(async { ($store_constructor)(slab_size).await }.instrument(span.clone())) {
                Ok(s) => s,
                Err(e) => return TestResult::error(format!("Failed to create store: {e:?}")),
            };

            match runtime.block_on(tests::slabs::test_get_slab_range(&store, &segment).instrument(span)) {
                Ok(()) => TestResult::passed(),
                Err(e) => TestResult::error(e),
            }
        }

        fn prop_trigger_operations(input: tests::TriggerTestInput) -> TestResult {
            use tracing::Instrument;

            if input.triggers.is_empty() {
                return TestResult::discard();
            }

            // Capture the current span to propagate into async runtime
            let span = tracing::Span::current();

            // Create runtime for this test invocation
            let runtime = match Builder::new_multi_thread().enable_all().build() {
                Ok(rt) => rt,
                Err(e) => return TestResult::error(format!("Failed to create runtime: {e}")),
            };

            // Create store instance with segment's slab_size
            let slab_size = input.segment.slab_size;
            let store = match runtime.block_on(async { ($store_constructor)(slab_size).await }.instrument(span.clone())) {
                Ok(s) => s,
                Err(e) => return TestResult::error(format!("Failed to create store: {e:?}")),
            };

            match runtime.block_on(tests::trigger_operations::test_trigger_operations(
                &store, &input,
            ).instrument(span)) {
                Ok(()) => TestResult::passed(),
                Err(e) => TestResult::error(e),
            }
        }

        fn prop_trigger_consistency(input: tests::TriggerTestInput) -> TestResult {
            use tracing::Instrument;

            if input.triggers.is_empty() {
                return TestResult::discard();
            }

            // Capture the current span to propagate into async runtime
            let span = tracing::Span::current();

            // Create runtime for this test invocation
            let runtime = match Builder::new_multi_thread().enable_all().build() {
                Ok(rt) => rt,
                Err(e) => return TestResult::error(format!("Failed to create runtime: {e}")),
            };

            // Create store instance with segment's slab_size
            let slab_size = input.segment.slab_size;
            let store = match runtime.block_on(async { ($store_constructor)(slab_size).await }.instrument(span.clone())) {
                Ok(s) => s,
                Err(e) => return TestResult::error(format!("Failed to create store: {e:?}")),
            };

            match runtime.block_on(tests::trigger_consistency::test_trigger_consistency(
                &store, &input,
            ).instrument(span)) {
                Ok(()) => TestResult::passed(),
                Err(e) => TestResult::error(e),
            }
        }

        fn prop_operation_sequences(input: tests::TriggerSequence) -> TestResult {
            use tracing::Instrument;

            // Capture the current span to propagate into async runtime
            let span = tracing::Span::current();

            // Create runtime for this test invocation
            let runtime = match Builder::new_multi_thread().enable_all().build() {
                Ok(rt) => rt,
                Err(e) => return TestResult::error(format!("Failed to create runtime: {e}")),
            };

            // Create store instance with segment's slab_size
            let slab_size = input.segment.slab_size;
            let store = match runtime.block_on(async { ($store_constructor)(slab_size).await }.instrument(span.clone())) {
                Ok(s) => s,
                Err(e) => return TestResult::error(format!("Failed to create store: {e:?}")),
            };

            match runtime.block_on(tests::trigger_operations::test_operation_sequences(
                &store, &input,
            ).instrument(span)) {
                Ok(()) => TestResult::passed(),
                Err(e) => TestResult::error(e),
            }
        }

        fn prop_cross_slab_operations(segment: $crate::timers::store::Segment) -> TestResult {
            use tracing::Instrument;

            // Capture the current span to propagate into async runtime
            let span = tracing::Span::current();

            // Create runtime for this test invocation
            let runtime = match Builder::new_multi_thread().enable_all().build() {
                Ok(rt) => rt,
                Err(e) => return TestResult::error(format!("Failed to create runtime: {e}")),
            };

            // Create store instance with segment's slab_size
            let slab_size = segment.slab_size;
            let store = match runtime.block_on(async { ($store_constructor)(slab_size).await }.instrument(span.clone())) {
                Ok(s) => s,
                Err(e) => return TestResult::error(format!("Failed to create store: {e:?}")),
            };

            match runtime.block_on(tests::cross_slab::test_cross_slab_operations(
                &store, &segment,
            ).instrument(span)) {
                Ok(()) => TestResult::passed(),
                Err(e) => TestResult::error(e),
            }
        }

        fn prop_key_contention(segment: $crate::timers::store::Segment) -> TestResult {
            use tracing::Instrument;

            // Capture the current span to propagate into async runtime
            let span = tracing::Span::current();

            // Create runtime for this test invocation
            let runtime = match Builder::new_multi_thread().enable_all().build() {
                Ok(rt) => rt,
                Err(e) => return TestResult::error(format!("Failed to create runtime: {e}")),
            };

            // Create store instance with segment's slab_size
            let slab_size = segment.slab_size;
            let store = match runtime.block_on(async { ($store_constructor)(slab_size).await }.instrument(span.clone())) {
                Ok(s) => s,
                Err(e) => return TestResult::error(format!("Failed to create store: {e:?}")),
            };

            match runtime.block_on(tests::contention::test_key_contention(&store, &segment).instrument(span)) {
                Ok(()) => TestResult::passed(),
                Err(e) => TestResult::error(e),
            }
        }

        fn prop_sequential_interleavings(input: tests::TriggerTestInput) -> TestResult {
            use tracing::Instrument;

            if input.triggers.is_empty() {
                return TestResult::discard();
            }

            // Capture the current span to propagate into async runtime
            let span = tracing::Span::current();

            // Create runtime for this test invocation
            let runtime = match Builder::new_multi_thread().enable_all().build() {
                Ok(rt) => rt,
                Err(e) => return TestResult::error(format!("Failed to create runtime: {e}")),
            };

            // Create store instance with segment's slab_size
            let slab_size = input.segment.slab_size;
            let store = match runtime.block_on(async { ($store_constructor)(slab_size).await }.instrument(span.clone())) {
                Ok(s) => s,
                Err(e) => return TestResult::error(format!("Failed to create store: {e:?}")),
            };

            match runtime.block_on(
                tests::sequential_interleavings::test_sequential_interleavings(&store, &input).instrument(span),
            ) {
                Ok(()) => TestResult::passed(),
                Err(e) => TestResult::error(e),
            }
        }

    };


}
