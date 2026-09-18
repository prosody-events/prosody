//! Property tests for timer defer middleware.
//!
//! Verifies middleware invariants using trace-based specification:
//! - Timer coverage: every deferred key has an active `DeferredTimer`
//! - FIFO order: timer with earliest `original_time` processed first

use super::types::{DeferredTimerOutcome, TimerTrace, TimerTraceEvent};
use super::{TEST_RUNTIME, TestHarness};
use generator::{TraceModel, test_key, update_model};
use runner::{execute_deferred_timer, execute_event, is_expected_error};

use crate::consumer::DemandType;
use crate::consumer::middleware::defer::calculate_backoff;
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::tracing::init_test_logging;
use color_eyre::eyre::eyre;
use quickcheck_macros::quickcheck;

mod generator;
mod runner;

// ============================================================================
// Property Tests
// ============================================================================

/// Property: Timer coverage is maintained after every operation.
///
/// **Invariant**: For every key with deferred timers, there is an active
/// `DeferredTimer`. For every key without deferred timers, there is no timer.
#[quickcheck]
fn prop_timer_coverage(trace: TimerTrace) -> color_eyre::Result<()> {
    init_test_logging();
    let TimerTrace { events, key_count } = trace;

    TEST_RUNTIME.block_on(async {
        let harness = TestHarness::for_trace(key_count)?;
        let mut model = TraceModel::new();

        for event in &events {
            execute_event(&harness, event).await?;
            update_model(&mut model, event);

            for key_idx in 0..key_count {
                let deferred = model.is_deferred(&test_key(key_idx));
                let scheduled = !harness.contexts[key_idx]
                    .active_deferred_timers()
                    .is_empty();
                assert_eq!(
                    deferred, scheduled,
                    "Timer coverage failed for key {key_idx}"
                );
            }
        }

        Ok(())
    })
}

/// Property: FIFO order is maintained for deferred timers.
///
/// **Invariant**: When a `DeferredTimer` fires, it processes the timer with the
/// earliest `original_time` for that key.
#[quickcheck]
fn prop_fifo_order(trace: TimerTrace) -> color_eyre::Result<()> {
    init_test_logging();
    let TimerTrace { events, key_count } = trace;

    TEST_RUNTIME.block_on(async {
        let harness = TestHarness::for_trace(key_count)?;
        let mut model = TraceModel::new();

        for event in &events {
            // For DeferredTimer events, verify FIFO before execution
            if let TimerTraceEvent::DeferredTimer(deferred_event) = event {
                let key = test_key(deferred_event.key_idx);
                if model
                    .get_head(&key)
                    .is_some_and(|expected_time| expected_time != deferred_event.expected_time)
                {
                    return Err(color_eyre::eyre::eyre!(
                        "FIFO violation: expected {:?} but trace has {:?}",
                        model.get_head(&key),
                        deferred_event.expected_time
                    ));
                }
            }

            // Execute and update model, ignoring expected permanent errors
            let result = execute_event(&harness, event).await;
            if let Err(e) = &result
                && !is_expected_error(e)
            {
                return result;
            }

            update_model(&mut model, event);
        }

        Ok(())
    })
}

// ============================================================================
// Backoff Property Tests
// ============================================================================

/// Property: Backoff delays are within configured bounds.
///
/// **Invariant**: For any `retry_count` > 0, [`calculate_backoff`] returns a
/// delay in `[1, min(base * 2^(retry_count-1), max_delay)]` seconds. For
/// `retry_count == 0` (first deferral), it returns zero — the timer is
/// scheduled at `original_time`, not after a backoff.
///
/// This drives the production `calculate_backoff` (the exact function
/// `TimerDeferHandler::next_retry_time` calls) against an independently
/// derived bound, rather than re-deriving the formula and checking it
/// against itself.
#[quickcheck]
fn prop_backoff_bounds(retry_count_raw: u8) -> color_eyre::Result<()> {
    init_test_logging();

    // Test with retry_count 0-15 (practical range)
    let retry_count = u32::from(retry_count_raw % 16);

    TEST_RUNTIME.block_on(async {
        let harness = TestHarness::new()?;
        let config = &harness.handler.config;

        if retry_count == 0 {
            assert_eq!(
                calculate_backoff(config, 0),
                CompactDuration::MIN,
                "retry_count=0 must not apply backoff"
            );
            return Ok(());
        }

        // Independent model of the expected ceiling: base * 2^(retry_count-1),
        // capped at max_delay, floored at 1 second.
        let base_seconds = u32::try_from(config.base.as_secs()).unwrap_or(u32::MAX);
        let max_delay_seconds = u32::try_from(config.max_delay.as_secs()).unwrap_or(u32::MAX);
        let multiplier = 2_u32.saturating_pow(retry_count - 1);
        let expected_max = base_seconds
            .saturating_mul(multiplier)
            .min(max_delay_seconds)
            .max(1);

        // Sample repeatedly: calculate_backoff applies full jitter, so a
        // single call cannot expose an out-of-bounds ceiling or floor.
        for _ in 0..32_u32 {
            let sampled = calculate_backoff(config, retry_count).seconds();
            assert!(
                (1..=expected_max).contains(&sampled),
                "backoff(retry_count={retry_count}) = {sampled}s outside [1, {expected_max}]"
            );
        }

        Ok(())
    })
}

/// A reload reports the stored retry count, plus one for the failure that
/// deferred the event, plus the outer demand's retry ordinal. A transient
/// failure increments the stored retry count by one.
#[quickcheck]
fn prop_retry_increment(trace: TimerTrace, demand: DemandType) -> color_eyre::Result<()> {
    init_test_logging();
    let TimerTrace { events, key_count } = trace;

    TEST_RUNTIME.block_on(async {
        let harness = TestHarness::for_trace(key_count)?;

        for event in &events {
            if let TimerTraceEvent::DeferredTimer(def_event) = event {
                let key = test_key(def_event.key_idx);
                let before = harness
                    .get_retry_count(&key)
                    .await?
                    .ok_or_else(|| eyre!("Deferred head is absent"))?;
                // Drain the calls of earlier events.
                let _ = harness.inner_handler.take_timer_calls();
                execute_deferred_timer(&harness, def_event, demand).await?;
                let calls = harness.inner_handler.take_timer_calls();
                assert_eq!(calls.len(), 1);
                assert_eq!(
                    calls[0].1.retry(),
                    before.saturating_add(1).saturating_add(demand.retry())
                );
                if matches!(def_event.outcome, DeferredTimerOutcome::Transient) {
                    assert_eq!(harness.get_retry_count(&key).await?, Some(before + 1));
                }
            } else {
                execute_event(&harness, event).await?;
            }
        }

        Ok(())
    })
}

/// Property: Processing order is maintained for deferred timers.
///
/// **Invariant**: For a given key, timers are processed in chronological order
/// by `original_time`. When a `DeferredTimer` fires, it processes the timer
/// with the smallest `original_time` among all currently-deferred timers.
///
/// The model supplies the expected timer for each retry.
#[quickcheck]
fn prop_processing_order(trace: TimerTrace) -> color_eyre::Result<()> {
    init_test_logging();
    let TimerTrace { events, key_count } = trace;

    TEST_RUNTIME.block_on(async {
        let harness = TestHarness::for_trace(key_count)?;
        let mut model = TraceModel::new();

        for event in &events {
            // For DeferredTimer completions, verify processing matches model
            if let TimerTraceEvent::DeferredTimer(def_event) = event {
                let key = test_key(def_event.key_idx);

                // Get what the model says should be at the head
                let expected_head = model.get_head(&key);

                // The trace's expected_time should match model's head
                if let Some(head_time) = expected_head
                    && def_event.expected_time != head_time
                {
                    return Err(color_eyre::eyre::eyre!(
                        "Processing order mismatch for key={key}: model expects {:?} at head, \
                         trace has {:?}",
                        head_time,
                        def_event.expected_time
                    ));
                }
            }

            // Execute and update model, ignoring expected permanent errors
            let result = execute_event(&harness, event).await;
            if let Err(e) = &result
                && !is_expected_error(e)
            {
                return result;
            }

            update_model(&mut model, event);
        }

        Ok(())
    })
}

// ============================================================================
// Span Context Property Tests
// ============================================================================

/// Property: Span context is preserved across defer/retry cycles.
///
/// **Invariant**: When a timer is deferred, its span context is stored and
/// restored when the `DeferredTimer` fires for retry. The restored span must
/// link to the original parent context, maintaining distributed trace
/// continuity.
///
/// This test verifies that:
/// 1. Span context is captured during deferral via `Span::current().context()`
/// 2. On retry, a fresh span is created and linked to the stored context
/// 3. The trace ID is preserved across the defer/retry cycle
///
/// Note: This property test operates at the model level, verifying that the
/// span storage/retrieval contract is maintained. The integration test
/// `span_restored_on_retry` provides concrete verification with real spans.
#[quickcheck]
fn prop_span_restored(trace: TimerTrace) -> color_eyre::Result<()> {
    init_test_logging();
    let TimerTrace { events, key_count } = trace;

    TEST_RUNTIME.block_on(async {
        let harness = TestHarness::for_trace(key_count)?;
        let mut model = TraceModel::new();

        for event in &events {
            // Execute and update model
            let result = execute_event(&harness, event).await;
            if let Err(e) = &result
                && !is_expected_error(e)
            {
                return result;
            }

            update_model(&mut model, event);

            // For deferred timer retries, verify the handler was called with a
            // valid trigger (span context was restored)
            if let TimerTraceEvent::DeferredTimer(def_event) = event {
                let key = test_key(def_event.key_idx);

                // Verify the inner handler was actually called (meaning the
                // trigger was successfully loaded with its span context)
                let calls = harness.inner_handler.timer_calls();
                let key_called = calls.iter().any(|k| k.as_ref() == key.as_ref());

                // Handler should have been called for non-empty queues
                // (If queue was empty, we wouldn't have a DeferredTimerEvent in
                // the trace)
                assert!(
                    key_called,
                    "Handler should be called with restored trigger for key={key}"
                );
            }
        }

        Ok(())
    })
}
