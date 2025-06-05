//! Tests for the `TriggerStore` trait implementations.
//!
//! This module contains property-based tests that verify the behavior
//! of any implementation of the `TriggerStore` trait.

/// Tests for cleanup operations of the trigger store.
pub mod cleanup_operations;
/// Common utilities and helpers for trigger store tests.
pub mod common;
/// Tests for key contention scenarios in the trigger store.
pub mod contention;
/// Tests for operations spanning multiple slabs.
pub mod cross_slab;
/// Tests for primitive trigger store operations.
pub mod primitive_operations;
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

use crate::Key;
use crate::timers::Trigger;
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::timers::store::{Segment, TriggerStore};
use quickcheck::{Arbitrary, Gen};
use std::fmt::Debug;
use tracing::Span;
use uuid::Uuid;

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

#[cfg(test)]
impl Arbitrary for Segment {
    fn arbitrary(g: &mut Gen) -> Self {
        let id = Uuid::new_v4();
        let name = format!("segment-{}", u16::arbitrary(g));
        let slab_size = CompactDuration::arbitrary(g);

        Segment {
            id,
            name,
            slab_size,
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

#[cfg(test)]
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

#[cfg(test)]
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
                triggers.push(Trigger {
                    key: Key::from(key.clone()),
                    time,
                    span: Span::current(),
                });
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

#[cfg(test)]
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

/// Runs all `TriggerStore` tests on the given store implementation
#[cfg(test)]
pub async fn run_all_tests<S>(store_constructor: impl Fn() -> S) -> Vec<TestStoreResult>
where
    S: TriggerStore + Send + Sync,
    S::Error: Debug,
{
    // Generate test inputs with quickcheck
    let mut g = Gen::new(100);
    let segment_input = SegmentTestInput::arbitrary(&mut g);
    let trigger_input = TriggerTestInput::arbitrary(&mut g);
    let segment = Segment::arbitrary(&mut g);

    // Run all tests with a fresh store instance for each
    vec![
        segment::test_segment_operations(&store_constructor(), &segment_input).await,
        slabs::test_slab_operations(&store_constructor(), &segment).await,
        trigger_operations::test_trigger_operations(&store_constructor(), &trigger_input).await,
        trigger_consistency::test_trigger_consistency(&store_constructor(), &trigger_input).await,
        sequential_interleavings::test_sequential_interleavings(
            &store_constructor(),
            &trigger_input,
        )
        .await,
        cross_slab::test_cross_slab_operations(&store_constructor(), &segment).await,
        contention::test_key_contention(&store_constructor(), &segment).await,
        primitive_operations::test_primitive_operations(&store_constructor(), &trigger_input).await,
        cleanup_operations::test_cleanup_operations(&store_constructor(), &trigger_input).await,
    ]
}

/// Macro to define standardized tests for any `TriggerStore` implementation.
///
/// # Arguments
///
/// * `$store_type` - The type of the `TriggerStore` implementation being tested
/// * `$store_constructor` - Expression that constructs a new instance of the
///   store
#[macro_export]
macro_rules! trigger_store_tests {
    ($store_type:ty, $store_constructor:expr) => {
        #[cfg(test)]
        mod tests {
            use super::*;
            use quickcheck::{QuickCheck, TestResult};
            use tokio::runtime::Builder;
            use $crate::timers::store::tests;

            #[test]
            fn test_segment_operations() {
                QuickCheck::new().tests(100).quickcheck(
                    prop_segment_operations
                        as fn($crate::timers::store::tests::SegmentTestInput) -> TestResult,
                );
            }

            #[test]
            fn test_slab_operations() {
                QuickCheck::new().tests(100).quickcheck(
                    prop_slab_operations as fn($crate::timers::store::Segment) -> TestResult,
                );
            }

            #[test]
            fn test_trigger_operations() {
                QuickCheck::new().tests(100).quickcheck(
                    prop_trigger_operations
                        as fn($crate::timers::store::tests::TriggerTestInput) -> TestResult,
                );
            }

            #[test]
            fn test_trigger_consistency() {
                QuickCheck::new().tests(100).quickcheck(
                    prop_trigger_consistency
                        as fn($crate::timers::store::tests::TriggerTestInput) -> TestResult,
                );
            }

            #[test]
            fn test_operation_sequences() {
                QuickCheck::new().tests(100).quickcheck(
                    prop_operation_sequences
                        as fn($crate::timers::store::tests::TriggerSequence) -> TestResult,
                );
            }

            #[test]
            fn test_cross_slab_operations() {
                QuickCheck::new().tests(100).quickcheck(
                    prop_cross_slab_operations as fn($crate::timers::store::Segment) -> TestResult,
                );
            }

            #[test]
            fn test_key_contention() {
                QuickCheck::new().tests(50).quickcheck(
                    prop_key_contention as fn($crate::timers::store::Segment) -> TestResult,
                );
            }

            // Tests all primitive operations directly for complete low-level coverage
            #[test]
            fn test_primitive_operations() {
                QuickCheck::new().tests(100).quickcheck(
                    prop_primitive_operations
                        as fn($crate::timers::store::tests::TriggerTestInput) -> TestResult,
                );
            }

            // Tests cleanup operations and their interaction with persistence
            #[test]
            fn test_cleanup_operations() {
                QuickCheck::new().tests(100).quickcheck(
                    prop_cleanup_operations
                        as fn($crate::timers::store::tests::TriggerTestInput) -> TestResult,
                );
            }

            #[test]
            fn test_sequential_interleavings() {
                QuickCheck::new().tests(100).quickcheck(
                    prop_sequential_interleavings
                        as fn($crate::timers::store::tests::TriggerTestInput) -> TestResult,
                );
            }

            #[test]
            fn test_all() {
                // Create runtime for async operations
                let runtime = match Builder::new_current_thread().enable_all().build() {
                    Ok(rt) => rt,
                    Err(e) => panic!("Failed to build runtime: {}", e),
                };

                // Run all tests with a store constructor
                let results = runtime.block_on(tests::run_all_tests(|| $store_constructor));

                // Check results
                for (i, result) in results.into_iter().enumerate() {
                    assert!(
                        result.is_ok(),
                        "Test #{} failed: {}",
                        i,
                        result.unwrap_err()
                    );
                }
            }

            fn prop_segment_operations(
                input: $crate::timers::store::tests::SegmentTestInput,
            ) -> TestResult {
                let runtime = match Builder::new_current_thread().enable_all().build() {
                    Ok(rt) => rt,
                    Err(e) => return TestResult::error(format!("Failed to build runtime: {}", e)),
                };

                let store = $store_constructor;
                match runtime.block_on(tests::segment::test_segment_operations(&store, &input)) {
                    Ok(()) => TestResult::passed(),
                    Err(e) => TestResult::error(e),
                }
            }

            fn prop_slab_operations(segment: $crate::timers::store::Segment) -> TestResult {
                let runtime = match Builder::new_current_thread().enable_all().build() {
                    Ok(rt) => rt,
                    Err(e) => return TestResult::error(format!("Failed to build runtime: {}", e)),
                };

                let store = $store_constructor;
                match runtime.block_on(tests::slabs::test_slab_operations(&store, &segment)) {
                    Ok(()) => TestResult::passed(),
                    Err(e) => TestResult::error(e),
                }
            }

            fn prop_trigger_operations(input: tests::TriggerTestInput) -> TestResult {
                if input.triggers.is_empty() {
                    return TestResult::discard();
                }

                let runtime = match Builder::new_current_thread().enable_all().build() {
                    Ok(rt) => rt,
                    Err(e) => return TestResult::error(format!("Failed to build runtime: {}", e)),
                };

                let store = $store_constructor;
                match runtime.block_on(tests::trigger_operations::test_trigger_operations(
                    &store, &input,
                )) {
                    Ok(()) => TestResult::passed(),
                    Err(e) => TestResult::error(e),
                }
            }

            fn prop_trigger_consistency(input: tests::TriggerTestInput) -> TestResult {
                if input.triggers.is_empty() {
                    return TestResult::discard();
                }

                let runtime = match Builder::new_current_thread().enable_all().build() {
                    Ok(rt) => rt,
                    Err(e) => return TestResult::error(format!("Failed to build runtime: {}", e)),
                };

                let store = $store_constructor;
                match runtime.block_on(tests::trigger_consistency::test_trigger_consistency(
                    &store, &input,
                )) {
                    Ok(()) => TestResult::passed(),
                    Err(e) => TestResult::error(e),
                }
            }

            fn prop_operation_sequences(input: tests::TriggerSequence) -> TestResult {
                let runtime = match Builder::new_current_thread().enable_all().build() {
                    Ok(rt) => rt,
                    Err(e) => return TestResult::error(format!("Failed to build runtime: {}", e)),
                };

                let store = $store_constructor;
                match runtime.block_on(tests::trigger_operations::test_operation_sequences(
                    &store, &input,
                )) {
                    Ok(()) => TestResult::passed(),
                    Err(e) => TestResult::error(e),
                }
            }

            fn prop_cross_slab_operations(segment: $crate::timers::store::Segment) -> TestResult {
                let runtime = match Builder::new_current_thread().enable_all().build() {
                    Ok(rt) => rt,
                    Err(e) => return TestResult::error(format!("Failed to build runtime: {}", e)),
                };

                let store = $store_constructor;
                match runtime.block_on(tests::cross_slab::test_cross_slab_operations(
                    &store, &segment,
                )) {
                    Ok(()) => TestResult::passed(),
                    Err(e) => TestResult::error(e),
                }
            }

            fn prop_key_contention(segment: $crate::timers::store::Segment) -> TestResult {
                let runtime = match Builder::new_current_thread().enable_all().build() {
                    Ok(rt) => rt,
                    Err(e) => return TestResult::error(format!("Failed to build runtime: {}", e)),
                };

                let store = $store_constructor;
                match runtime.block_on(tests::contention::test_key_contention(&store, &segment)) {
                    Ok(()) => TestResult::passed(),
                    Err(e) => TestResult::error(e),
                }
            }

            fn prop_primitive_operations(input: tests::TriggerTestInput) -> TestResult {
                if input.triggers.is_empty() {
                    return TestResult::discard();
                }

                let runtime = match Builder::new_current_thread().enable_all().build() {
                    Ok(rt) => rt,
                    Err(e) => return TestResult::error(format!("Failed to build runtime: {}", e)),
                };

                let store = $store_constructor;
                match runtime.block_on(tests::primitive_operations::test_primitive_operations(
                    &store, &input,
                )) {
                    Ok(()) => TestResult::passed(),
                    Err(e) => TestResult::error(e),
                }
            }

            fn prop_cleanup_operations(input: tests::TriggerTestInput) -> TestResult {
                if input.triggers.is_empty() {
                    return TestResult::discard();
                }

                let runtime = match Builder::new_current_thread().enable_all().build() {
                    Ok(rt) => rt,
                    Err(e) => return TestResult::error(format!("Failed to build runtime: {}", e)),
                };

                let store = $store_constructor;
                match runtime.block_on(tests::cleanup_operations::test_cleanup_operations(
                    &store, &input,
                )) {
                    Ok(()) => TestResult::passed(),
                    Err(e) => TestResult::error(e),
                }
            }

            fn prop_sequential_interleavings(input: tests::TriggerTestInput) -> TestResult {
                if input.triggers.is_empty() {
                    return TestResult::discard();
                }

                let runtime = match Builder::new_current_thread().enable_all().build() {
                    Ok(rt) => rt,
                    Err(e) => return TestResult::error(format!("Failed to build runtime: {}", e)),
                };

                let store = $store_constructor;
                match runtime.block_on(
                    tests::sequential_interleavings::test_sequential_interleavings(&store, &input),
                ) {
                    Ok(()) => TestResult::passed(),
                    Err(e) => TestResult::error(e),
                }
            }
        }
    };
}
