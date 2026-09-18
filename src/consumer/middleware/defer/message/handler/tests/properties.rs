//! Property tests for defer middleware.
//!
//! Verifies middleware invariants using trace-based specification:
//! - Timer coverage: every deferred key has an active timer
//! - FIFO order: timer fires for head offset
//! - Retry increment: retry count increases on transient failure
//! - Backoff bounds: delay within configured range
//! - Cleanup: timer cleared when queue empty

use super::TEST_RUNTIME;
use super::faults::FaultedTrace;
use super::harness::TestHarness;
use super::types::{MessageOutcome, TimerOutcome, Trace, TraceEvent};
use crate::consumer::DemandType;
use crate::consumer::middleware::defer::message::store::MessageDeferStore;
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::tracing::init_test_logging;
use color_eyre::eyre::ensure;
use color_eyre::eyre::eyre;
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
                let key = harness.key(timer.key_idx);

                let result = harness.store().get_next_deferred_message(key).await;
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

/// A reload reports the stored retry count, plus one for the failure that
/// deferred the event, plus the outer demand's retry ordinal. A transient
/// failure increments the stored retry count by one.
#[quickcheck]
fn prop_retry_increment(trace: Trace, demand: DemandType) -> color_eyre::Result<()> {
    init_test_logging();
    let Trace { events, key_count } = trace;

    TEST_RUNTIME.block_on(async {
        let mut harness = TestHarness::new(key_count)?;

        for event in &events {
            if let TraceEvent::Timer(timer) = event {
                let before = harness
                    .get_retry_count(timer.key_idx)
                    .await?
                    .ok_or_else(|| eyre!("Deferred head is absent"))?;
                // Drain the calls of earlier events.
                let _ = harness.processed_messages();
                harness.execute_timer(timer, demand).await?;
                let calls = harness.processed_messages();
                match timer.outcome {
                    TimerOutcome::LoaderPermanent | TimerOutcome::LoaderTransient { .. } => {
                        assert!(calls.is_empty());
                    }
                    _ => {
                        assert_eq!(calls.len(), 1);
                        assert_eq!(
                            calls[0].demand.retry(),
                            before.saturating_add(1).saturating_add(demand.retry())
                        );
                    }
                }
                if matches!(
                    timer.outcome,
                    TimerOutcome::Transient { .. } | TimerOutcome::LoaderTransient { .. }
                ) {
                    assert_eq!(
                        harness.get_retry_count(timer.key_idx).await?,
                        Some(before + 1)
                    );
                }
            } else {
                harness.execute_event(event).await?;
            }
        }

        Ok(())
    })
}

/// Property: Backoff duration is within bounds.
///
/// **Invariant**: When a timer is scheduled, the delay is between 0 and
/// `max_backoff`. The handler uses full jitter: `rand() * min(base * 2^retry,
/// max)`.
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
            // Extract max_backoff and key_idx if this event schedules a timer
            let backoff_check: Option<(usize, CompactDuration)> = match event {
                TraceEvent::Message(msg) => match &msg.outcome {
                    MessageOutcome::Transient {
                        max_backoff,
                        defer: true,
                    } => Some((msg.key_idx, *max_backoff)),
                    _ => None,
                },
                TraceEvent::Timer(timer) => match &timer.outcome {
                    TimerOutcome::Transient { max_backoff }
                    | TimerOutcome::LoaderTransient { max_backoff } => {
                        Some((timer.key_idx, *max_backoff))
                    }
                    _ => None,
                },
            };

            // Capture time before execution
            let before_time = match CompactDateTime::now() {
                Ok(t) => t,
                Err(e) => return TestResult::error(format!("CompactDateTime::now failed: {e}")),
            };

            if let Err(e) = harness.execute_event(event).await {
                return TestResult::error(format!("Execution failed: {e}"));
            }

            // Verify backoff bounds if this event scheduled a timer
            if let Some((key_idx, max_backoff)) = backoff_check {
                let key = harness.key(key_idx);
                if let Some(scheduled_time) = harness.capture().get_timer_time(key) {
                    // Scheduled time must be >= before_time (not in the past)
                    if scheduled_time < before_time {
                        return TestResult::error(format!(
                            "Backoff violation: scheduled {scheduled_time} < before {before_time}"
                        ));
                    }

                    // Scheduled time must be <= before_time + max_backoff
                    // Add small tolerance (1 second) for timing variance
                    let tolerance = CompactDuration::new(1);
                    let total_backoff = max_backoff + tolerance; // saturating add
                    let Ok(max_allowed) = before_time.add_duration(total_backoff) else {
                        continue; // Skip if time overflows
                    };
                    if scheduled_time > max_allowed {
                        return TestResult::error(format!(
                            "Backoff violation: scheduled {scheduled_time} > max allowed \
                             {max_allowed} (max_backoff={max_backoff:?})"
                        ));
                    }
                }
            }
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

                let is_deferred = harness
                    .store()
                    .get_next_deferred_message(key)
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

/// Property: Per-key message processing order is maintained.
///
/// **Invariant**: For any given key, messages are processed in offset order.
/// At-least-once semantics allow duplicates, but not out-of-order processing.
#[quickcheck]
fn prop_processing_order(trace: Trace) -> TestResult {
    init_test_logging();
    let Trace { events, key_count } = trace;

    TEST_RUNTIME.block_on(async {
        let mut harness = match TestHarness::new(key_count) {
            Ok(h) => h,
            Err(e) => return TestResult::error(format!("Harness construction failed: {e}")),
        };

        // Execute all events
        for event in &events {
            if let Err(e) = harness.execute_event(event).await {
                return TestResult::error(format!("Execution failed: {e}"));
            }
        }

        // Get all processed messages
        let processed = harness.processed_messages();

        // Group by key and verify order
        let mut per_key_offsets: ahash::HashMap<&crate::Key, Vec<crate::Offset>> =
            ahash::HashMap::default();
        for msg in &processed {
            per_key_offsets
                .entry(&msg.key)
                .or_default()
                .push(msg.offset);
        }

        // For each key, verify offsets are non-decreasing
        for (key, offsets) in per_key_offsets {
            for window in offsets.windows(2) {
                let prev = window[0];
                let curr = window[1];
                if curr < prev {
                    return TestResult::error(format!(
                        "Processing order violation: key {key:?} processed offset {prev} before \
                         {curr}"
                    ));
                }
            }
        }

        TestResult::passed()
    })
}

/// Every deferred key has a retry timer after each settled event.
/// Store faults, timer faults, and lost timers preserve this invariant.
/// An empty queue can retain a timer after a clear fault.
/// This property checks only the queue-to-timer direction.
#[quickcheck]
fn prop_timer_coverage_under_faults(trace: FaultedTrace) -> TestResult {
    let result: color_eyre::Result<()> = TEST_RUNTIME.block_on(async {
        let mut harness = TestHarness::new(trace.trace.key_count)?;
        for (event, fault) in trace.trace.events.iter().zip(trace.faults) {
            harness.execute_faulted(event, fault).await?;
            for index in 0..trace.trace.key_count {
                let key = harness.key(index);
                let deferred = harness.store().is_deferred(key).await?.is_some();
                ensure!(
                    !deferred || harness.capture().has_active_timer(key),
                    "Key {index} has a queue without a timer; event: {event:?}; fault: {fault:?}"
                );
            }
        }
        Ok(())
    });
    match result {
        Ok(()) => TestResult::passed(),
        Err(error) => TestResult::error(format!("{error:?}")),
    }
}

/// A consumed store or timer fault aborts the source. The redelivery commits.
/// An unconsumed fault changes nothing.
#[quickcheck]
fn prop_fault_abandons_source(trace: FaultedTrace) -> TestResult {
    let result: color_eyre::Result<()> = TEST_RUNTIME.block_on(async {
        let mut harness = TestHarness::new(trace.trace.key_count)?;
        for (event, fault) in trace.trace.events.iter().zip(trace.faults) {
            let passes = harness.execute_faulted(event, fault).await?;
            if let Some(first) = passes.first() {
                ensure!(
                    first.committed != first.consumed,
                    "Events: {:?}; event: {event:?}; fault: {fault:?}; passes: {passes:?}",
                    trace.trace.events
                );
                ensure!(
                    passes.len() == 1 + usize::from(first.consumed),
                    "Events: {:?}; passes: {passes:?}",
                    trace.trace.events
                );
                if let Some(second) = passes.get(1) {
                    ensure!(
                        second.committed && !second.consumed,
                        "Events: {:?}; passes: {passes:?}",
                        trace.trace.events
                    );
                }
            }
        }
        Ok(())
    });
    match result {
        Ok(()) => TestResult::passed(),
        Err(error) => TestResult::error(format!("{error:?}")),
    }
}
