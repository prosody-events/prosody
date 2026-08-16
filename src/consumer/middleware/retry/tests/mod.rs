use super::*;
use crate::consumer::message::ConsumerMessage;
use crate::consumer::middleware::tests::test_support::{
    MockEventContext, ScriptedHandler, ScriptedHook, TestError, create_test_message,
    create_test_trigger,
};
use crate::error::ErrorCategory;
use crate::timers::TimerType;
use crate::timers::datetime::CompactDateTime;
use color_eyre::eyre::{Result, bail};
use quickcheck::{QuickCheck, TestResult};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;
use tokio::runtime::Builder;
use tokio::time::{sleep as tokio_sleep, timeout};
use tracing::Span;

use dispatch_signals::create_offset_tracker;

fn create_retry_handler<T>(handler: T, max_retries: u32) -> RetryHandler<T> {
    RetryHandler {
        base_delay_millis: 1, // Very short for tests
        max_delay_millis: 10,
        max_retries,
        handler,
    }
}

// === Classification Property ===
//
// `RetryHandler::run` makes one decision per attempt: retry (Transient,
// attempts left), stop-with-error (Permanent/Terminal, or Transient with
// attempts exhausted), or stop-with-success. `expected_outcome` walks a
// scripted failure sequence through that same decision and predicts the
// call count, the final Ok/Err, and the demand-type sequence
// (`Normal` on attempt 1, `Failure` on every retry) — one property replaces
// five single-path examples (success-first-try, transient-then-succeeds,
// permanent-immediate, terminal-immediate, first-attempt-demand-type).
// `transient_error_fails_after_max_retries` stays as a literal anchor.

#[test]
fn prop_retry_classification_arithmetic() {
    /// Predicts `(call_count, is_err, demand_types)` for a scripted
    /// `failures` sequence and `max_retries`, mirroring `RetryHandler::run`'s
    /// per-attempt classification exactly.
    fn expected_outcome(
        failures: &[ErrorCategory],
        max_retries: u32,
    ) -> (usize, bool, Vec<DemandType>) {
        let mut demand_types = Vec::new();
        let mut attempt: u32 = 0;
        loop {
            attempt += 1;
            demand_types.push(if attempt == 1 {
                DemandType::Normal
            } else {
                DemandType::Failure
            });
            let Some(&category) = failures.get((attempt - 1) as usize) else {
                return (attempt as usize, false, demand_types);
            };
            match category {
                ErrorCategory::Transient if attempt <= max_retries => {}
                _ => return (attempt as usize, true, demand_types),
            }
        }
    }

    fn property(raw_failures: Vec<u8>, max_retries_raw: u8) -> TestResult {
        // Bound both axes so each iteration's paused-clock retry loop stays
        // fast while still crossing the zero/non-zero and
        // exhausted/not-exhausted boundaries.
        let failures: Vec<ErrorCategory> = raw_failures
            .into_iter()
            .take(6)
            .map(|b| match b % 3 {
                0 => ErrorCategory::Transient,
                1 => ErrorCategory::Permanent,
                _ => ErrorCategory::Terminal,
            })
            .collect();
        let max_retries = u32::from(max_retries_raw % 4);
        let (expected_calls, expected_err, expected_demand_types) =
            expected_outcome(&failures, max_retries);

        let runtime = Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build();
        let Ok(runtime) = runtime else {
            return TestResult::error("failed to build paused runtime");
        };
        runtime.block_on(async move {
            let handler = ScriptedHandler::failing_then_success(failures);
            let retry_handler = create_retry_handler(handler.clone(), max_retries);
            let context = MockEventContext::new();
            let message = match create_test_message() {
                Ok(message) => message,
                Err(error) => return TestResult::error(format!("create_test_message: {error}")),
            };

            let result =
                FallibleHandler::on_message(&retry_handler, context, message, DemandType::Normal)
                    .await;

            if result.is_err() != expected_err {
                return TestResult::error(format!(
                    "result.is_err()={} expected={expected_err}",
                    result.is_err(),
                ));
            }
            if handler.call_count() != expected_calls {
                return TestResult::error(format!(
                    "call_count={} expected={expected_calls}",
                    handler.call_count(),
                ));
            }
            let demand_types = handler.recorded_demand_types();
            if demand_types != expected_demand_types {
                return TestResult::error(format!(
                    "demand_types={demand_types:?} expected={expected_demand_types:?}",
                ));
            }
            TestResult::passed()
        })
    }

    QuickCheck::new().quickcheck(property as fn(Vec<u8>, u8) -> TestResult);
}

/// Named anchor: pins the exact "1 + `max_retries`" call count as a literal,
/// independent of `prop_retry_classification_arithmetic`'s model.
#[tokio::test]
async fn transient_error_fails_after_max_retries() -> Result<()> {
    let handler = ScriptedHandler::always_failing(ErrorCategory::Transient);
    let retry_handler = create_retry_handler(handler.clone(), 3);
    let context = MockEventContext::new();
    let message = create_test_message()?;

    let result =
        FallibleHandler::on_message(&retry_handler, context, message, DemandType::Normal).await;

    assert!(result.is_err(), "Should fail after max retries");
    // First attempt + 3 retries = 4 total calls
    assert_eq!(
        handler.call_count(),
        4,
        "Should attempt 1 + max_retries times"
    );
    Ok(())
}

// === Shutdown Tests ===

#[tokio::test]
async fn shutdown_during_retry_sleep_returns_error() -> Result<()> {
    let handler = ScriptedHandler::always_failing(ErrorCategory::Transient);
    // Use longer delays to give time for shutdown signal
    let retry_handler = RetryHandler {
        base_delay_millis: 1000, // 1 second base delay
        max_delay_millis: 10000,
        max_retries: 10,
        handler: handler.clone(),
    };
    let context = MockEventContext::new();
    let message = create_test_message()?;

    // Spawn the retry operation
    let ctx = context.clone();
    let handle = tokio::spawn(async move {
        FallibleHandler::on_message(&retry_handler, ctx, message, DemandType::Normal).await
    });

    // Wait a bit for the first failure and retry sleep to start
    tokio_sleep(Duration::from_millis(50)).await;

    // Signal shutdown
    context.request_shutdown();

    // Should complete quickly due to shutdown; the deadline is a hang-guard,
    // not the assertion.
    let Ok(join_result) = timeout(Duration::from_millis(500), handle).await else {
        bail!("retry loop did not observe the shutdown signal within 500ms");
    };
    let result = join_result?;

    assert!(result.is_err(), "Should return error on shutdown");
    Ok(())
}

// === Timer Path Smoke ===
//
// The timer path shares `RetryHandler::run` verbatim with the message path
// (`on_timer` differs only in what it invokes); the classification
// arithmetic is proved once above. This smoke just confirms the timer
// dispatch wires into that shared loop at all.

#[tokio::test]
async fn timer_transient_error_retries_then_succeeds() {
    let handler = ScriptedHandler::failing_then_success(vec![ErrorCategory::Transient]);
    let retry_handler = create_retry_handler(handler.clone(), 3);
    let context = MockEventContext::new();
    let trigger = create_test_trigger();

    let result =
        FallibleHandler::on_timer(&retry_handler, context, trigger, DemandType::Normal).await;

    assert!(result.is_ok());
    assert_eq!(handler.call_count(), 2);
}

// === Backoff Calculation Tests ===

#[test]
fn sleep_time_has_exponential_growth_with_jitter() {
    let handler = ScriptedHandler::success();
    let retry_handler = RetryHandler {
        base_delay_millis: 100,
        max_delay_millis: 10000,
        max_retries: 10,
        handler,
    };

    // Collect multiple samples to verify jitter randomness
    let mut samples_attempt_1: Vec<u64> = Vec::new();
    let mut samples_attempt_3: Vec<u64> = Vec::new();

    for _ in 0_u32..100_u32 {
        samples_attempt_1.push(retry_handler.sleep_time(1).as_millis() as u64);
        samples_attempt_3.push(retry_handler.sleep_time(3).as_millis() as u64);
    }

    // Attempt 1: exp_backoff = 2^1 * 100 = 200ms, jitter in [0, 200)
    let max_attempt_1 = samples_attempt_1.iter().max().copied().unwrap_or(u64::MAX);
    assert!(max_attempt_1 < 200, "Attempt 1 jitter should be < 200ms");

    // Attempt 3: exp_backoff = 2^3 * 100 = 800ms, jitter in [0, 800)
    let max_attempt_3 = samples_attempt_3.iter().max().copied().unwrap_or(u64::MAX);
    assert!(max_attempt_3 < 800, "Attempt 3 jitter should be < 800ms");

    // Verify there's some variation (jitter is working)
    let min_attempt_3 = samples_attempt_3.iter().min().copied().unwrap_or(u64::MAX);
    assert!(
        max_attempt_3 > min_attempt_3 + 50,
        "Jitter should introduce variation"
    );
}

#[test]
fn sleep_time_survives_zero_backoff() {
    // A sub-millisecond base delay truncates exp_backoff to 0; the jitter
    // bound must clamp instead of panicking on an empty range.
    let retry_handler = RetryHandler {
        base_delay_millis: 0,
        max_delay_millis: 0,
        max_retries: 10,
        handler: ScriptedHandler::success(),
    };

    assert_eq!(retry_handler.sleep_time(1), Duration::ZERO);
}

#[test]
fn sleep_time_capped_at_max_delay() {
    let handler = ScriptedHandler::success();
    let retry_handler = RetryHandler {
        base_delay_millis: 100,
        max_delay_millis: 500,
        max_retries: 10,
        handler,
    };

    // Attempt 10: exp_backoff = 2^10 * 100 = 102400ms, but capped at 500ms
    // Jitter should be in [0, 500)
    for _ in 0_u32..100_u32 {
        let sleep = retry_handler.sleep_time(10).as_millis() as u64;
        assert!(sleep < 500, "Sleep time should be capped at max_delay");
    }
}

mod attempt_boundary;
mod dispatch_signals;
