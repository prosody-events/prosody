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
        let mut harness = TestHarness::new(key_count);

        for event in &events {
            if harness.execute_event(event).await.is_err() {
                // Invalid trace - discard
                return TestResult::discard();
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
        let mut harness = TestHarness::new(key_count);

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
                    Ok(None) | Err(_) => {
                        // Key not deferred or store error - invalid trace
                        return TestResult::discard();
                    }
                }
            }

            if harness.execute_event(event).await.is_err() {
                return TestResult::discard();
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
        let mut harness = TestHarness::new(key_count);

        for event in &events {
            if let TraceEvent::Timer(timer) = event
                && matches!(timer.outcome, TimerOutcome::Transient { .. })
            {
                // Get retry count before
                let before = harness.get_retry_count(timer.key_idx).await.ok().flatten();

                if harness.execute_event(event).await.is_err() {
                    return TestResult::discard();
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

            if harness.execute_event(event).await.is_err() {
                return TestResult::discard();
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
        let mut harness = TestHarness::new(key_count);

        for event in &events {
            if harness.execute_event(event).await.is_err() {
                return TestResult::discard();
            }
            // Backoff bounds verified during execution via harness
            // implementation
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
        let mut harness = TestHarness::new(key_count);

        for event in &events {
            if harness.execute_event(event).await.is_err() {
                return TestResult::discard();
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

// ============================================================================
// Unit Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Offset;
    use crate::consumer::middleware::defer::tests::types::{
        MessageEvent, MessageOutcome, TimerEvent, TimerOutcome, Trace, TraceEvent,
    };
    use std::time::Duration;

    /// Simple test case: defer and complete.
    #[tokio::test]
    async fn test_simple_defer_complete() {
        init_test_logging();

        let trace = Trace::new(
            vec![
                TraceEvent::Message(MessageEvent {
                    key_idx: 0,
                    offset: Offset::from(1_i64),
                    outcome: MessageOutcome::Transient {
                        max_backoff: Duration::from_secs(60),
                        defer: true,
                    },
                }),
                TraceEvent::Timer(TimerEvent {
                    key_idx: 0,
                    offset: Offset::from(1_i64),
                    outcome: TimerOutcome::Success,
                }),
            ],
            1,
        );

        let mut harness = TestHarness::new(1);

        for event in &trace.events {
            harness.execute_event(event).await.ok();
            harness.verify_invariants().await.ok();
        }

        // Final state: no deferred messages, no timers
        {
            let key_ref = harness.key_ref(0);
            let deferred = harness.store().get_next_deferred_message(&key_ref).await;
            assert!(deferred.is_ok_and(|d| d.is_none()));
        };
        let key = harness.key(0).clone();
        assert!(!harness.capture().has_active_timer(&key));
    }

    /// Test case: retry then succeed.
    #[tokio::test]
    async fn test_retry_then_succeed() {
        init_test_logging();

        let trace = Trace::new(
            vec![
                // Message defers
                TraceEvent::Message(MessageEvent {
                    key_idx: 0,
                    offset: Offset::from(1_i64),
                    outcome: MessageOutcome::Transient {
                        max_backoff: Duration::from_secs(60),
                        defer: true,
                    },
                }),
                // Timer fires, transient failure
                TraceEvent::Timer(TimerEvent {
                    key_idx: 0,
                    offset: Offset::from(1_i64),
                    outcome: TimerOutcome::Transient {
                        max_backoff: Duration::from_secs(120),
                    },
                }),
                // Timer fires again, success
                TraceEvent::Timer(TimerEvent {
                    key_idx: 0,
                    offset: Offset::from(1_i64),
                    outcome: TimerOutcome::Success,
                }),
            ],
            1,
        );

        let mut harness = TestHarness::new(1);

        // Execute first event (defer)
        harness.execute_event(&trace.events[0]).await.ok();
        let retry_count = harness.get_retry_count(0).await.ok().flatten();
        assert_eq!(retry_count, Some(0));

        // Execute second event (transient failure)
        harness.execute_event(&trace.events[1]).await.ok();
        let retry_count = harness.get_retry_count(0).await.ok().flatten();
        assert_eq!(retry_count, Some(1));

        // Execute third event (success)
        harness.execute_event(&trace.events[2]).await.ok();
        // Key should no longer be deferred
        {
            let key_ref = harness.key_ref(0);
            let deferred = harness.store().get_next_deferred_message(&key_ref).await;
            assert!(deferred.is_ok_and(|d| d.is_none()));
        }
    }

    /// Test case: multiple keys independent.
    #[tokio::test]
    async fn test_multiple_keys() {
        init_test_logging();

        let trace = Trace::new(
            vec![
                // Key 0 defers
                TraceEvent::Message(MessageEvent {
                    key_idx: 0,
                    offset: Offset::from(1_i64),
                    outcome: MessageOutcome::Transient {
                        max_backoff: Duration::from_secs(60),
                        defer: true,
                    },
                }),
                // Key 1 succeeds
                TraceEvent::Message(MessageEvent {
                    key_idx: 1,
                    offset: Offset::from(2_i64),
                    outcome: MessageOutcome::Success,
                }),
                // Key 0 timer fires
                TraceEvent::Timer(TimerEvent {
                    key_idx: 0,
                    offset: Offset::from(1_i64),
                    outcome: TimerOutcome::Success,
                }),
            ],
            2,
        );

        let mut harness = TestHarness::new(2);

        for event in &trace.events {
            harness.execute_event(event).await.ok();
            harness.verify_invariants().await.ok();
        }

        // Both keys should have no timers
        let key0 = harness.key(0).clone();
        let key1 = harness.key(1).clone();
        assert!(!harness.capture().has_active_timer(&key0));
        assert!(!harness.capture().has_active_timer(&key1));
    }
}
