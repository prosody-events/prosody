use super::*;

#[test]
fn test_first_failure_defers_and_retries() -> Result<()> {
    init_test_logging();

    let env = TEST_RUNTIME.block_on(DeferTestEnvironment::new());
    run_iterations(env, &run_first_failure_defers_and_retries)
}

async fn run_first_failure_defers_and_retries(
    env: &mut DeferTestEnvironment,
    iteration: u64,
) -> Result<()> {
    let key = format!("test-key-{iteration}");
    let value = i64::try_from(iteration)?;

    // Budget exactly one failure: the first attempt fails and defers, and
    // the immediate retry (retry_count=0) succeeds. The fixed budget makes
    // the event stream deterministic regardless of when the retry fires.
    env.fail_value_times(value, 1);

    env.send_message(&key, json!({ "value": value })).await?;

    // Should receive transient failure event
    let event = env.expect_event(&key, 5).await?;
    ensure!(
        matches!(event, HandlerEvent::MessageFailedTransient { key: ref event_key, value: event_value }
            if *event_key == key && event_value == value),
        "Expected transient failure for value={value}, got: {event:?}"
    );

    // Defer middleware handles the timer internally and retries via
    // on_message (on_timer is NOT called for DeferRetry timers).
    let event = env.expect_event(&key, 10).await?;
    ensure!(
        matches!(event, HandlerEvent::MessageSuccess { key: ref event_key, value: event_value }
            if *event_key == key && event_value == value),
        "Expected retry to succeed, got: {event:?}"
    );

    Ok(())
}

/// Test: Multiple messages for same key are queued and processed in order.
#[test]
fn test_multiple_messages_queued_in_order() -> Result<()> {
    init_test_logging();

    let env = TEST_RUNTIME.block_on(DeferTestEnvironment::new());
    run_iterations(env, &run_multiple_messages_queued_in_order)
}

async fn run_multiple_messages_queued_in_order(
    env: &mut DeferTestEnvironment,
    iteration: u64,
) -> Result<()> {
    let key = format!("test-key-{iteration}");
    let base = i64::try_from(iteration)? * 3;
    let values = [base + 1, base + 2, base + 3];

    // Budget exactly two failures for msg1: the first attempt and its
    // immediate retry (retry_count=0) fail, the second failure re-defers
    // with retry_count=1 (base backoff, ~1s), and that timer-driven retry
    // succeeds. The fixed budget makes the whole event stream a
    // deterministic sequence — no mid-flight reconfiguration racing the
    // backoff timer, and no wall-clock assertion anywhere.
    env.fail_value_times(values[0], 2);

    // Send all 3 messages quickly (before timer fires)
    for value in values {
        env.send_message(&key, json!({ "value": value })).await?;
    }

    // msg1 fails its first attempt, then its immediate retry.
    for attempt in 1_u32..=2 {
        let event = env.expect_event(&key, 5).await?;
        ensure!(
            matches!(event, HandlerEvent::MessageFailedTransient { key: ref event_key, value }
                if *event_key == key && value == values[0]),
            "Expected transient failure {attempt} for value={}, got: {event:?}",
            values[0]
        );
    }

    // msgs 2 and 3 must stay queued behind the deferred msg1. The handler
    // emits an event for EVERY invocation, so out-of-order processing would
    // surface here as a Success(2)/Success(3) arriving before Success(1) —
    // the ordered drain proves the queueing invariant by content, with the
    // deadline only as a hang-guard.
    for expected_value in values {
        let event = env.expect_event(&key, 10).await?;
        ensure!(
            matches!(event, HandlerEvent::MessageSuccess { key: ref event_key, value }
                if *event_key == key && value == expected_value),
            "Expected MessageSuccess {{ value: {expected_value} }}, got: {event:?}"
        );
    }

    Ok(())
}

/// The payload value the permanent-error test's handler rejects permanently.
const PERMANENT_VALUE: i64 = 999;

/// Test: Permanent errors are NOT deferred (they are irrecoverable).
#[test]
fn test_permanent_errors_not_deferred() -> Result<()> {
    init_test_logging();

    let env = TEST_RUNTIME.block_on(DeferTestEnvironment::new_with_permanent_error_handler(
        PERMANENT_VALUE,
    ));
    run_iterations(env, &run_permanent_errors_not_deferred)
}

async fn run_permanent_errors_not_deferred(
    env: &mut DeferTestEnvironment,
    iteration: u64,
) -> Result<()> {
    let permanent_key = format!("test-key-{iteration}-permanent");
    let ok_key = format!("test-key-{iteration}-ok");
    // Distinct from PERMANENT_VALUE for every iteration.
    let ok_value = 1000 + i64::try_from(iteration)?;

    // Send a message that fails with permanent error (LogMiddleware logs it)
    env.send_message(&permanent_key, json!({ "value": PERMANENT_VALUE }))
        .await?;

    // Send a successful message with different key to verify consumer continues
    env.send_message(&ok_key, json!({ "value": ok_value }))
        .await?;

    // Should immediately get success for the ok key (not deferred)
    let event = env.expect_event(&ok_key, 5).await?;
    ensure!(
        matches!(event, HandlerEvent::MessageSuccess { key: ref event_key, value }
            if *event_key == ok_key && value == ok_value),
        "Expected {ok_key} to succeed immediately (permanent errors don't defer)"
    );

    // No timer should fire for the permanent key (permanent errors aren't
    // retried) — and no legal event source of any kind exists in this window.
    env.expect_no_event(2000).await?;

    Ok(())
}
