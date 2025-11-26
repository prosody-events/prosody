//! Property tests for defer middleware.
//!
//! Verifies middleware invariants using trace-based specification:
//! - Timer coverage: every deferred key has an active timer
//! - FIFO order: timer fires for head offset
//! - Retry increment: retry count increases on transient failure
//! - Backoff bounds: delay within configured range
//! - Cleanup: timer cleared when queue empty

use super::TEST_RUNTIME;
use super::harness::TestHarness;
use super::types::{TimerOutcome, Trace, TraceEvent};
use crate::consumer::middleware::defer::store::DeferStore;
use crate::tracing::init_test_logging;
use quickcheck::TestResult;
use quickcheck_macros::quickcheck;

// ============================================================================
// Property Tests
// ============================================================================

/// Property: Timer coverage is maintained after every operation.
///
/// **Invariant**: For every key with deferred messages, there is an active
/// timer. For every key without deferred messages, there is no timer.
#[quickcheck]
fn prop_timer_coverage(trace: Trace) -> TestResult {
    init_test_logging();
    let Trace { events, key_count } = trace;

    TEST_RUNTIME.block_on(async {
        let mut harness = match TestHarness::new(key_count) {
            Ok(h) => h,
            Err(e) => return TestResult::error(format!("Harness construction failed: {e}")),
        };

        for event in &events {
            if let Err(e) = harness.execute_event(event).await {
                return TestResult::error(format!("Execution failed: {e}"));
            }

            // Verify timer coverage after each event
            if let Err(e) = harness.verify_invariants().await {
                return TestResult::error(format!("Timer coverage violation: {e}"));
            }
        }

        TestResult::passed()
    })
}

/// Property: FIFO order is maintained for deferred messages.
///
/// **Invariant**: When a timer fires, it processes the oldest (lowest offset)
/// message for that key.
#[quickcheck]
fn prop_fifo_order(trace: Trace) -> TestResult {
    init_test_logging();
    let Trace { events, key_count } = trace;

    TEST_RUNTIME.block_on(async {
        let mut harness = match TestHarness::new(key_count) {
            Ok(h) => h,
            Err(e) => return TestResult::error(format!("Harness construction failed: {e}")),
        };

        for event in &events {
            // For timer events, verify FIFO before execution
            if let TraceEvent::Timer(timer) = event {
                let key_ref = harness.key_ref(timer.key_idx);

                let result = harness.store().get_next_deferred_message(&key_ref).await;
                match result {
                    Ok(Some((head_offset, _))) => {
                        if head_offset != timer.offset {
                            return TestResult::error(format!(
                                "FIFO violation: head {} != timer {}",
                                head_offset, timer.offset
                            ));
                        }
                    }
                    Ok(None) => {
                        return TestResult::error(format!(
                            "Timer for key {} but key not deferred",
                            timer.key_idx
                        ));
                    }
                    Err(e) => {
                        return TestResult::error(format!("Store error: {e}"));
                    }
                }
            }

            if let Err(e) = harness.execute_event(event).await {
                return TestResult::error(format!("Execution failed: {e}"));
            }
        }

        TestResult::passed()
    })
}

/// Property: Retry count increments on transient timer failure.
///
/// **Invariant**: After a `TimerOutcome::Transient`, the retry count for that
/// offset increases by exactly 1.
#[quickcheck]
fn prop_retry_increment(trace: Trace) -> TestResult {
    init_test_logging();
    let Trace { events, key_count } = trace;

    TEST_RUNTIME.block_on(async {
        let mut harness = match TestHarness::new(key_count) {
            Ok(h) => h,
            Err(e) => return TestResult::error(format!("Harness construction failed: {e}")),
        };

        for event in &events {
            if let TraceEvent::Timer(timer) = event
                && matches!(timer.outcome, TimerOutcome::Transient { .. })
            {
                // Get retry count before
                let before = harness.get_retry_count(timer.key_idx).await.ok().flatten();

                if let Err(e) = harness.execute_event(event).await {
                    return TestResult::error(format!("Execution failed: {e}"));
                }

                // Get retry count after
                let after = harness.get_retry_count(timer.key_idx).await.ok().flatten();

                // Verify increment
                let expected = before.unwrap_or(0) + 1;
                let actual = after.unwrap_or(0);

                if actual != expected {
                    return TestResult::error(format!(
                        "Retry increment violation: expected {expected}, got {actual}"
                    ));
                }

                continue;
            }

            if let Err(e) = harness.execute_event(event).await {
                return TestResult::error(format!("Execution failed: {e}"));
            }
        }

        TestResult::passed()
    })
}

/// Property: Backoff duration is within bounds.
///
/// **Invariant**: When a timer is scheduled, the delay is between 0 and
/// `max_backoff` (plus jitter tolerance).
#[quickcheck]
fn prop_backoff_bounds(trace: Trace) -> TestResult {
    init_test_logging();
    let Trace { events, key_count } = trace;

    TEST_RUNTIME.block_on(async {
        let mut harness = match TestHarness::new(key_count) {
            Ok(h) => h,
            Err(e) => return TestResult::error(format!("Harness construction failed: {e}")),
        };

        for event in &events {
            if let Err(e) = harness.execute_event(event).await {
                return TestResult::error(format!("Execution failed: {e}"));
            }
            // TODO: Verify scheduled timer times against max_backoff bounds
        }

        TestResult::passed()
    })
}

/// Property: Timer is cleared when queue becomes empty.
///
/// **Invariant**: After the last message for a key completes (success or
/// permanent failure), the timer is cleared.
#[quickcheck]
fn prop_cleanup(trace: Trace) -> TestResult {
    init_test_logging();
    let Trace { events, key_count } = trace;

    TEST_RUNTIME.block_on(async {
        let mut harness = match TestHarness::new(key_count) {
            Ok(h) => h,
            Err(e) => return TestResult::error(format!("Harness construction failed: {e}")),
        };

        for event in &events {
            if let Err(e) = harness.execute_event(event).await {
                return TestResult::error(format!("Execution failed: {e}"));
            }

            // After each event, verify cleanup invariant
            for key_idx in 0..key_count {
                let key = harness.key(key_idx);
                let key_ref = harness.key_ref(key_idx);

                let is_deferred = harness
                    .store()
                    .get_next_deferred_message(&key_ref)
                    .await
                    .ok()
                    .flatten()
                    .is_some();

                let has_timer = harness.capture().has_active_timer(key);

                // If not deferred, should not have timer
                if !is_deferred && has_timer {
                    return TestResult::error(format!(
                        "Cleanup violation: key {key_idx} not deferred but has timer"
                    ));
                }

                // If deferred, should have timer
                if is_deferred && !has_timer {
                    return TestResult::error(format!(
                        "Cleanup violation: key {key_idx} deferred but no timer"
                    ));
                }
            }
        }

        TestResult::passed()
    })
}
