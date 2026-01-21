//! This module tests message ordering in the Prosody system using
//! property-based testing with `QuickCheck`. It verifies that messages are
//! received in the order they were produced per key, utilizing integration
//! tests with Kafka, via the Prosody library.

use std::collections::BTreeSet;
use std::env;

use prosody::tracing::init_test_logging;
use quickcheck::{QuickCheck, TestResult};

mod common;
use common::{TEST_RUNTIME, TestInput, run_test};

/// Tests that messages are received in the order they were produced for each
/// key. This function leverages property-based testing using `QuickCheck`,
/// which generates various input scenarios to ensure correct order. It supports
/// integration testing with Kafka through the Prosody library.
#[test]
fn receives_all_in_key_order() {
    // Determine the number of tests to run from an environment variable,
    // defaulting to 3 if the variable is not set or invalid.
    let test_count = env::var("INTEGRATION_TESTS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(3);

    // Start tracing for logging and debugging.
    init_test_logging();

    // Use QuickCheck to run property-based tests that validate message ordering.
    QuickCheck::new()
        .tests(test_count)
        .quickcheck(prop as fn(TestInput) -> TestResult);
}

/// Property function for `QuickCheck` to verify message ordering.
///
/// Uses the shared Tokio runtime to asynchronously run the `run_test` function
/// with generated test input to ensure the correct ordering of messages.
///
/// # Arguments
///
/// * `input` - The `TestInput` containing test parameters with messages and
///   configuration values for the test.
///
/// # Returns
///
/// * `TestResult::passed()` if the test succeeds.
/// * `TestResult::error()` with an error message if the test fails.
/// * `TestResult::discard()` if the input is invalid.
fn prop(input: TestInput) -> TestResult {
    // Discard test cases that have invalid configurations, such as empty messages.
    if input.messages.is_empty() || input.messages.values().any(BTreeSet::is_empty) {
        return TestResult::discard();
    }

    // Run the test within the shared runtime and return the outcome.
    match TEST_RUNTIME.block_on(run_test(input)) {
        Ok(()) => TestResult::passed(),
        Err(e) => TestResult::error(e.to_string()),
    }
}
