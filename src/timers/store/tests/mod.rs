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

/// Tests for cleanup operations of the trigger store.
pub mod cleanup_operations;
/// Common utilities and helpers for trigger store tests.
pub mod common;
/// Tests for key contention scenarios in the trigger store.
pub mod contention;
/// Tests for operations spanning multiple slabs.
pub mod cross_slab;
/// Tests for timer schema migration from v1 to v2.
pub mod migration;
/// Tests for primitive trigger store operations.
pub mod primitive_operations;
/// Property-based tests for key trigger table operations.
pub mod prop_key_triggers;
/// Property-based tests for segment table operations.
pub mod prop_segments;
/// Property-based tests for slab metadata table operations.
pub mod prop_slab_metadata;
/// Property-based tests for slab trigger table operations.
pub mod prop_slab_triggers;
/// Property-based tests for v1 key trigger operations.
pub mod prop_v1_key_triggers;
/// Property-based tests for v1 slab metadata table operations.
pub mod prop_v1_slab_metadata;
/// Property-based tests for v1 slab trigger table operations.
pub mod prop_v1_slab_triggers;
/// Tests for segment management in the trigger store.
pub mod segment;
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
    ($store_type:ty, $store_constructor:expr, $test_count:expr) => {
        #[cfg(test)]
        mod tests {
            use super::*;
            use quickcheck::{QuickCheck, TestResult};
            use std::sync::LazyLock;
            use tokio::runtime::Builder;
            use $crate::timers::store::tests;

            // Shared runtime instance, created lazily
            static RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
                Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .unwrap()
            });

            // Shared store instance, created lazily
            static STORE: LazyLock<$store_type> = LazyLock::new(|| {
                RUNTIME.block_on(async {
                    ($store_constructor).await
                        .expect("Failed to create shared store instance")
                })
            });

            // Helper function to get the shared runtime
            fn get_runtime() -> &'static tokio::runtime::Runtime {
                &RUNTIME
            }

            // Helper function to get the shared store
            fn get_store() -> &'static $store_type {
                &STORE
            }

            trigger_store_tests!(@test_functions, $test_count);
            trigger_store_tests!(@prop_functions);
        }
    };

    // Common test function definitions
    (@test_functions, $test_count:expr) => {
        #[test]
        fn test_segment_operations() {
            QuickCheck::new().tests($test_count).quickcheck(
                prop_segment_operations
                    as fn($crate::timers::store::tests::SegmentTestInput) -> TestResult,
            );
        }

        #[test]
        fn test_slab_operations() {
            QuickCheck::new().tests($test_count).quickcheck(
                prop_slab_operations as fn($crate::timers::store::Segment) -> TestResult,
            );
        }

        #[test]
        fn test_get_slab_range() {
            QuickCheck::new().tests($test_count).quickcheck(
                prop_get_slab_range as fn($crate::timers::store::Segment) -> TestResult,
            );
        }

        #[test]
        fn test_trigger_operations() {
            QuickCheck::new().tests($test_count).quickcheck(
                prop_trigger_operations
                    as fn($crate::timers::store::tests::TriggerTestInput) -> TestResult,
            );
        }

        #[test]
        fn test_trigger_consistency() {
            QuickCheck::new().tests($test_count).quickcheck(
                prop_trigger_consistency
                    as fn($crate::timers::store::tests::TriggerTestInput) -> TestResult,
            );
        }

        #[test]
        fn test_operation_sequences() {
            QuickCheck::new().tests($test_count).quickcheck(
                prop_operation_sequences
                    as fn($crate::timers::store::tests::TriggerSequence) -> TestResult,
            );
        }

        #[test]
        fn test_cross_slab_operations() {
            QuickCheck::new().tests($test_count).quickcheck(
                prop_cross_slab_operations as fn($crate::timers::store::Segment) -> TestResult,
            );
        }

        #[test]
        fn test_key_contention() {
            QuickCheck::new().tests($test_count / 2).quickcheck(
                prop_key_contention as fn($crate::timers::store::Segment) -> TestResult,
            );
        }

        #[test]
        fn test_primitive_operations() {
            QuickCheck::new().tests($test_count).quickcheck(
                prop_primitive_operations
                    as fn($crate::timers::store::tests::TriggerTestInput) -> TestResult,
            );
        }

        #[test]
        fn test_cleanup_operations() {
            QuickCheck::new().tests($test_count).quickcheck(
                prop_cleanup_operations
                    as fn($crate::timers::store::tests::TriggerTestInput) -> TestResult,
            );
        }

        #[test]
        fn test_sequential_interleavings() {
            QuickCheck::new().tests($test_count).quickcheck(
                prop_sequential_interleavings
                    as fn($crate::timers::store::tests::TriggerTestInput) -> TestResult,
            );
        }

        #[test]
        fn test_needs_migration_logic() {
            QuickCheck::new().tests($test_count).quickcheck(
                prop_needs_migration_logic as fn($crate::timers::store::Segment) -> TestResult,
            );
        }

        #[test]
        fn test_segment_version_update() {
            QuickCheck::new().tests($test_count).quickcheck(
                prop_segment_version_update as fn($crate::timers::store::Segment) -> TestResult,
            );
        }

        #[test]
        fn test_v2_segment_creation() {
            QuickCheck::new().tests($test_count).quickcheck(
                prop_v2_segment_creation as fn($crate::timers::store::Segment) -> TestResult,
            );
        }

        #[test]
        fn test_mixed_timer_types() {
            QuickCheck::new().tests($test_count).quickcheck(
                prop_mixed_timer_types as fn($crate::timers::store::Segment) -> TestResult,
            );
        }

        #[test]
        fn test_v1_slab_enumeration() {
            QuickCheck::new().tests($test_count).quickcheck(
                prop_v1_slab_enumeration as fn($crate::timers::store::Segment) -> TestResult,
            );
        }

        #[test]
        fn test_v1_trigger_retrieval() {
            QuickCheck::new().tests($test_count).quickcheck(
                prop_v1_trigger_retrieval as fn($crate::timers::store::Segment) -> TestResult,
            );
        }

        #[test]
        fn test_v1_slab_roundtrip() {
            QuickCheck::new().tests($test_count).quickcheck(
                prop_v1_slab_roundtrip as fn($crate::timers::store::Segment) -> TestResult,
            );
        }

        #[test]
        fn test_v1_trigger_roundtrip() {
            QuickCheck::new().tests($test_count).quickcheck(
                prop_v1_trigger_roundtrip as fn($crate::timers::store::Segment) -> TestResult,
            );
        }

        #[test]
        fn test_multi_slab_v1_migration() {
            QuickCheck::new().tests($test_count).quickcheck(
                prop_multi_slab_v1_migration as fn($crate::timers::store::Segment) -> TestResult,
            );
        }

        #[test]
        fn test_crash_before_version_update() {
            QuickCheck::new().tests($test_count).quickcheck(
                prop_crash_before_version_update as fn($crate::timers::store::Segment) -> TestResult,
            );
        }

        #[test]
        fn test_crash_after_version_update() {
            QuickCheck::new().tests($test_count).quickcheck(
                prop_crash_after_version_update as fn($crate::timers::store::Segment) -> TestResult,
            );
        }

        #[test]
        fn test_migration_idempotency() {
            QuickCheck::new().tests($test_count).quickcheck(
                prop_migration_idempotency as fn($crate::timers::store::Segment) -> TestResult,
            );
        }

        #[test]
        fn test_crash_before_slab_size_update() {
            QuickCheck::new().tests($test_count).quickcheck(
                prop_crash_before_slab_size_update as fn($crate::timers::store::Segment) -> TestResult,
            );
        }

        #[test]
        fn test_crash_after_slab_size_update() {
            QuickCheck::new().tests($test_count).quickcheck(
                prop_crash_after_slab_size_update as fn($crate::timers::store::Segment) -> TestResult,
            );
        }

        #[test]
        fn test_slab_size_change_with_multiple_slabs() {
            QuickCheck::new().tests($test_count).quickcheck(
                prop_slab_size_change_with_multiple_slabs as fn($crate::timers::store::Segment) -> TestResult,
            );
        }

        #[test]
        fn test_prop_segment_model_equivalence() {
            QuickCheck::new().tests($test_count).quickcheck(
                prop_segment_model_equivalence as fn($crate::timers::store::tests::prop_segments::SegmentTestInput) -> TestResult,
            );
        }

        #[test]
        fn test_prop_slab_metadata_model_equivalence() {
            QuickCheck::new().tests($test_count).quickcheck(
                prop_slab_metadata_model_equivalence as fn($crate::timers::store::tests::prop_slab_metadata::SlabMetadataTestInput) -> TestResult,
            );
        }

        #[test]
        fn test_prop_slab_trigger_model_equivalence() {
            QuickCheck::new().tests($test_count).quickcheck(
                prop_slab_trigger_model_equivalence as fn($crate::timers::store::tests::prop_slab_triggers::SlabTriggerTestInput) -> TestResult,
            );
        }

        #[test]
        fn test_prop_key_trigger_model_equivalence() {
            QuickCheck::new().tests($test_count).quickcheck(
                prop_key_trigger_model_equivalence as fn($crate::timers::store::tests::prop_key_triggers::KeyTriggerTestInput) -> TestResult,
            );
        }

        #[test]
        fn test_prop_v1_slab_metadata_model_equivalence() {
            QuickCheck::new().tests($test_count).quickcheck(
                prop_v1_slab_metadata_model_equivalence as fn($crate::timers::store::tests::prop_v1_slab_metadata::V1SlabMetadataTestInput) -> TestResult,
            );
        }

        #[test]
        fn test_prop_v1_slab_trigger_model_equivalence() {
            QuickCheck::new().tests($test_count).quickcheck(
                prop_v1_slab_trigger_model_equivalence as fn($crate::timers::store::tests::prop_v1_slab_triggers::V1SlabTriggerTestInput) -> TestResult,
            );
        }

        #[test]
        fn test_prop_v1_key_trigger_model_equivalence() {
            QuickCheck::new().tests($test_count).quickcheck(
                prop_v1_key_trigger_model_equivalence as fn($crate::timers::store::tests::prop_v1_key_triggers::V1KeyTriggerTestInput) -> TestResult,
            );
        }

    };

    // Property functions
    (@prop_functions) => {
        fn prop_segment_model_equivalence(
            input: $crate::timers::store::tests::prop_segments::SegmentTestInput,
        ) -> TestResult {
            use $crate::timers::store::tests::prop_segments::test_prop_segment_model_equivalence;
            let store = get_store();
            test_prop_segment_model_equivalence(store, input)
        }

        fn prop_slab_metadata_model_equivalence(
            input: $crate::timers::store::tests::prop_slab_metadata::SlabMetadataTestInput,
        ) -> TestResult {
            use $crate::timers::store::tests::prop_slab_metadata::test_prop_slab_metadata_model_equivalence;
            let store = get_store();
            test_prop_slab_metadata_model_equivalence(store, input)
        }

        fn prop_slab_trigger_model_equivalence(
            input: $crate::timers::store::tests::prop_slab_triggers::SlabTriggerTestInput,
        ) -> TestResult {
            use $crate::timers::store::tests::prop_slab_triggers::test_prop_slab_trigger_model_equivalence;
            let store = get_store();
            test_prop_slab_trigger_model_equivalence(store, input)
        }

        fn prop_key_trigger_model_equivalence(
            input: $crate::timers::store::tests::prop_key_triggers::KeyTriggerTestInput,
        ) -> TestResult {
            use $crate::timers::store::tests::prop_key_triggers::test_prop_key_trigger_model_equivalence;
            let store = get_store();
            test_prop_key_trigger_model_equivalence(store, input)
        }

        fn prop_v1_slab_metadata_model_equivalence(
            input: $crate::timers::store::tests::prop_v1_slab_metadata::V1SlabMetadataTestInput,
        ) -> TestResult {
            use $crate::timers::store::tests::prop_v1_slab_metadata::test_prop_v1_slab_metadata_model_equivalence;
            let store = get_store();
            test_prop_v1_slab_metadata_model_equivalence(store, input)
        }

        fn prop_v1_slab_trigger_model_equivalence(
            input: $crate::timers::store::tests::prop_v1_slab_triggers::V1SlabTriggerTestInput,
        ) -> TestResult {
            use $crate::timers::store::tests::prop_v1_slab_triggers::test_prop_v1_slab_trigger_model_equivalence;
            let store = get_store();
            test_prop_v1_slab_trigger_model_equivalence(store, input)
        }

        fn prop_v1_key_trigger_model_equivalence(
            input: $crate::timers::store::tests::prop_v1_key_triggers::V1KeyTriggerTestInput,
        ) -> TestResult {
            use $crate::timers::store::tests::prop_v1_key_triggers::test_prop_v1_key_trigger_model_equivalence;
            let store = get_store();
            test_prop_v1_key_trigger_model_equivalence(store, input)
        }

        fn prop_segment_operations(
            input: $crate::timers::store::tests::SegmentTestInput,
        ) -> TestResult {
            let runtime = get_runtime();
            let store = get_store();

            match runtime.block_on(tests::segment::test_segment_operations(store, &input)) {
                Ok(()) => TestResult::passed(),
                Err(e) => TestResult::error(e),
            }
        }

        fn prop_slab_operations(segment: $crate::timers::store::Segment) -> TestResult {
            let runtime = get_runtime();
            let store = get_store();

            match runtime.block_on(tests::slabs::test_slab_operations(store, &segment)) {
                Ok(()) => TestResult::passed(),
                Err(e) => TestResult::error(e),
            }
        }

        fn prop_get_slab_range(segment: $crate::timers::store::Segment) -> TestResult {
            let runtime = get_runtime();
            let store = get_store();

            match runtime.block_on(tests::slabs::test_get_slab_range(store, &segment)) {
                Ok(()) => TestResult::passed(),
                Err(e) => TestResult::error(e),
            }
        }

        fn prop_trigger_operations(input: tests::TriggerTestInput) -> TestResult {
            if input.triggers.is_empty() {
                return TestResult::discard();
            }

            let runtime = get_runtime();
            let store = get_store();

            match runtime.block_on(tests::trigger_operations::test_trigger_operations(
                store, &input,
            )) {
                Ok(()) => TestResult::passed(),
                Err(e) => TestResult::error(e),
            }
        }

        fn prop_trigger_consistency(input: tests::TriggerTestInput) -> TestResult {
            if input.triggers.is_empty() {
                return TestResult::discard();
            }

            let runtime = get_runtime();
            let store = get_store();

            match runtime.block_on(tests::trigger_consistency::test_trigger_consistency(
                store, &input,
            )) {
                Ok(()) => TestResult::passed(),
                Err(e) => TestResult::error(e),
            }
        }

        fn prop_operation_sequences(input: tests::TriggerSequence) -> TestResult {
            let runtime = get_runtime();
            let store = get_store();

            match runtime.block_on(tests::trigger_operations::test_operation_sequences(
                store, &input,
            )) {
                Ok(()) => TestResult::passed(),
                Err(e) => TestResult::error(e),
            }
        }

        fn prop_cross_slab_operations(segment: $crate::timers::store::Segment) -> TestResult {
            let runtime = get_runtime();
            let store = get_store();

            match runtime.block_on(tests::cross_slab::test_cross_slab_operations(
                store, &segment,
            )) {
                Ok(()) => TestResult::passed(),
                Err(e) => TestResult::error(e),
            }
        }

        fn prop_key_contention(segment: $crate::timers::store::Segment) -> TestResult {
            let runtime = get_runtime();
            let store = get_store();

            match runtime.block_on(tests::contention::test_key_contention(store, &segment)) {
                Ok(()) => TestResult::passed(),
                Err(e) => TestResult::error(e),
            }
        }

        fn prop_primitive_operations(input: tests::TriggerTestInput) -> TestResult {
            if input.triggers.is_empty() {
                return TestResult::discard();
            }

            let runtime = get_runtime();
            let store = get_store();

            match runtime.block_on(tests::primitive_operations::test_primitive_operations(
                store, &input,
            )) {
                Ok(()) => TestResult::passed(),
                Err(e) => TestResult::error(e),
            }
        }

        fn prop_cleanup_operations(input: tests::TriggerTestInput) -> TestResult {
            if input.triggers.is_empty() {
                return TestResult::discard();
            }

            let runtime = get_runtime();
            let store = get_store();

            match runtime.block_on(tests::cleanup_operations::test_cleanup_operations(
                store, &input,
            )) {
                Ok(()) => TestResult::passed(),
                Err(e) => TestResult::error(e),
            }
        }

        fn prop_sequential_interleavings(input: tests::TriggerTestInput) -> TestResult {
            if input.triggers.is_empty() {
                return TestResult::discard();
            }

            let runtime = get_runtime();
            let store = get_store();

            match runtime.block_on(
                tests::sequential_interleavings::test_sequential_interleavings(store, &input),
            ) {
                Ok(()) => TestResult::passed(),
                Err(e) => TestResult::error(e),
            }
        }

        fn prop_needs_migration_logic(segment: $crate::timers::store::Segment) -> TestResult {
            let store = get_store();

            match tests::migration::test_needs_migration_logic(store, &segment) {
                Ok(()) => TestResult::passed(),
                Err(e) => TestResult::error(e),
            }
        }

        fn prop_segment_version_update(segment: $crate::timers::store::Segment) -> TestResult {
            let runtime = get_runtime();
            let store = get_store();

            match runtime.block_on(tests::migration::test_segment_version_update(store, &segment))
            {
                Ok(()) => TestResult::passed(),
                Err(e) => TestResult::error(e),
            }
        }

        fn prop_v2_segment_creation(segment: $crate::timers::store::Segment) -> TestResult {
            let runtime = get_runtime();
            let store = get_store();

            match runtime.block_on(tests::migration::test_v2_segment_creation(store, &segment)) {
                Ok(()) => TestResult::passed(),
                Err(e) => TestResult::error(e),
            }
        }

        fn prop_mixed_timer_types(segment: $crate::timers::store::Segment) -> TestResult {
            let runtime = get_runtime();
            let store = get_store();

            match runtime.block_on(tests::migration::test_mixed_timer_types(store, &segment)) {
                Ok(()) => TestResult::passed(),
                Err(e) => TestResult::error(e),
            }
        }

        fn prop_v1_slab_enumeration(segment: $crate::timers::store::Segment) -> TestResult {
            let runtime = get_runtime();
            let store = get_store();

            match runtime.block_on(tests::migration::test_v1_slab_enumeration(store, &segment)) {
                Ok(()) => TestResult::passed(),
                Err(e) => TestResult::error(e),
            }
        }

        fn prop_v1_trigger_retrieval(segment: $crate::timers::store::Segment) -> TestResult {
            let runtime = get_runtime();
            let store = get_store();

            match runtime.block_on(tests::migration::test_v1_trigger_retrieval(store, &segment)) {
                Ok(()) => TestResult::passed(),
                Err(e) => TestResult::error(e),
            }
        }

        fn prop_v1_slab_roundtrip(segment: $crate::timers::store::Segment) -> TestResult {
            let runtime = get_runtime();
            let store = get_store();

            match runtime.block_on(tests::migration::test_v1_slab_roundtrip(store, &segment)) {
                Ok(()) => TestResult::passed(),
                Err(e) => TestResult::error(e),
            }
        }

        fn prop_v1_trigger_roundtrip(segment: $crate::timers::store::Segment) -> TestResult {
            let runtime = get_runtime();
            let store = get_store();

            match runtime.block_on(tests::migration::test_v1_trigger_roundtrip(store, &segment)) {
                Ok(()) => TestResult::passed(),
                Err(e) => TestResult::error(e),
            }
        }

        fn prop_multi_slab_v1_migration(segment: $crate::timers::store::Segment) -> TestResult {
            let runtime = get_runtime();
            let store = get_store();

            match runtime.block_on(tests::migration::test_multi_slab_v1_migration(store, &segment)) {
                Ok(()) => TestResult::passed(),
                Err(e) => TestResult::error(e),
            }
        }

        fn prop_crash_before_version_update(segment: $crate::timers::store::Segment) -> TestResult {
            let runtime = get_runtime();
            let store = get_store();

            match runtime.block_on(tests::migration::test_crash_before_version_update(store, &segment)) {
                Ok(()) => TestResult::passed(),
                Err(e) => TestResult::error(e),
            }
        }

        fn prop_crash_after_version_update(segment: $crate::timers::store::Segment) -> TestResult {
            let runtime = get_runtime();
            let store = get_store();

            match runtime.block_on(tests::migration::test_crash_after_version_update(store, &segment)) {
                Ok(()) => TestResult::passed(),
                Err(e) => TestResult::error(e),
            }
        }

        fn prop_migration_idempotency(segment: $crate::timers::store::Segment) -> TestResult {
            let runtime = get_runtime();
            let store = get_store();

            match runtime.block_on(tests::migration::test_migration_idempotency(store, &segment)) {
                Ok(()) => TestResult::passed(),
                Err(e) => TestResult::error(e),
            }
        }

        fn prop_crash_before_slab_size_update(segment: $crate::timers::store::Segment) -> TestResult {
            let runtime = get_runtime();
            let store = get_store();

            match runtime.block_on(tests::migration::test_crash_before_slab_size_update(store, &segment)) {
                Ok(()) => TestResult::passed(),
                Err(e) => TestResult::error(e),
            }
        }

        fn prop_crash_after_slab_size_update(segment: $crate::timers::store::Segment) -> TestResult {
            let runtime = get_runtime();
            let store = get_store();

            match runtime.block_on(tests::migration::test_crash_after_slab_size_update(store, &segment)) {
                Ok(()) => TestResult::passed(),
                Err(e) => TestResult::error(e),
            }
        }

        fn prop_slab_size_change_with_multiple_slabs(segment: $crate::timers::store::Segment) -> TestResult {
            let runtime = get_runtime();
            let store = get_store();

            match runtime.block_on(tests::migration::test_slab_size_change_with_multiple_slabs(store, &segment)) {
                Ok(()) => TestResult::passed(),
                Err(e) => TestResult::error(e),
            }
        }
    };


}
