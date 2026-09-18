//! Shutdown tests for deferred messages.

use super::*;

// ============================================================================
// Shutdown proof tests
//
// These tests verify the composition: cancellation middleware (inside) +
// defer middleware (outside). On shutdown, cancellation converts Transient
// errors to Terminal so defer's existing guard prevents any store write.
// ============================================================================

/// The stacked handler type used in shutdown proof tests:
/// defer(cancellation(inner)), mirroring production middleware ordering.
type ShutdownProofHandler = MessageDeferHandler<
    CancellationHandler<OutcomeHandler>,
    MemoryMessageDeferStore,
    MemoryLoader<serde_json::Value>,
    TraceBasedDecider,
>;

/// Builds a [`ShutdownProofHandler`] for shutdown proof tests.
///
/// Returns `(handler, store, loader, inner_handler, capture, decider)` so
/// callers can control outcomes and inspect store state after each call.
fn build_shutdown_proof_stack(
    topic: Topic,
    partition: Partition,
) -> color_eyre::Result<(
    ShutdownProofHandler,
    MemoryMessageDeferStore,
    MemoryLoader<serde_json::Value>,
    OutcomeHandler,
    TimerCapture,
    TraceBasedDecider,
)> {
    let inner = OutcomeHandler::new();
    let cancellation = CancellationHandler::new(inner.clone());
    let store = MemoryMessageDeferStore::new();
    let loader = MemoryLoader::new();
    let capture = TimerCapture::new();
    let decider = TraceBasedDecider::new();

    let config = DeferConfiguration::builder()
        .base(Duration::from_secs(1))
        .max_delay(Duration::from_hours(1))
        .failure_threshold(0.9_f64)
        .build()?;

    let telemetry = Telemetry::new();
    let sender = telemetry.partition_sender(topic, partition);

    let handler = MessageDeferHandler {
        handler: cancellation,
        loader: loader.clone(),
        store: store.clone(),
        decider: decider.clone(),
        config,
        topic,
        partition,
        sender,
        source: Arc::from("test"),
        dedup_version: Arc::from("1"),
    };

    Ok((handler, store, loader, inner, capture, decider))
}

/// Stores a test payload in the loader and builds the matching consumer
/// message.
fn seed_message(
    loader: &MemoryLoader<serde_json::Value>,
    topic: Topic,
    partition: Partition,
    offset: Offset,
    key: &Key,
) -> color_eyre::Result<ConsumerMessage<serde_json::Value>> {
    let payload = serde_json::json!({"test": true});
    loader.store_message(topic, partition, offset, key.clone(), payload.clone());
    ConsumerMessage::for_testing(topic, partition, offset, key.clone(), payload)
}

/// Proves that a transient handler error is NOT deferred when the partition
/// is shutting down.
///
/// The cancellation middleware (inside defer) converts Transient → Terminal on
/// shutdown. Defer's existing guard (`if !Transient { return Err }`) then
/// propagates the error without writing to the store, preventing tombstones.
#[test]
fn transient_error_not_deferred_on_shutdown() -> color_eyre::Result<()> {
    init_test_logging();

    TEST_RUNTIME.block_on(async {
        let topic = Topic::from("test-topic");
        let partition = Partition::from(0_i32);
        let key: Key = Arc::from("test-key");

        let (handler, store, loader, inner, _capture, decider) =
            build_shutdown_proof_stack(topic, partition)?;

        let message = seed_message(&loader, topic, partition, Offset::from(1_i64), &key)?;

        inner.set_outcome(HandlerOutcome::Transient);
        decider.set_next(true); // decider would allow deferral if not for shutdown

        // Shutdown is signaled by the inner handler mid-execution, not before
        // the call. This exercises the post-call promotion path in
        // CancellationHandler rather than the pre-call short-circuit.
        let ctx = MockEventContext::new();
        inner.set_shutdown_trigger(ctx.clone());

        let result = handler.on_message(ctx, message, DemandType::Normal).await;

        // Error must propagate; the middleware must not absorb it.
        let Err(err) = result else {
            bail!("expected on_message to fail during shutdown, got Ok");
        };

        // Must classify as Terminal so FallibleEventHandler aborts the offset,
        // letting the incoming consumer replay the message after rebalance.
        assert!(
            matches!(err.classify_error(), ErrorCategory::Terminal),
            "Shutdown-aborted deferral must be Terminal so the offset is aborted"
        );

        // No row must appear in the defer store.
        let deferred = store.is_deferred(&key).await?;
        assert!(
            deferred.is_none(),
            "Key must not be deferred when shutdown is in progress"
        );

        Ok(())
    })
}

/// Proves that a transient handler error during timer retry is NOT re-deferred
/// when the partition is shutting down.
///
/// The message is already committed to the defer store from a prior deferral.
/// On retry, if the handler fails transiently while shutdown is active, the
/// cancellation middleware converts the error to Terminal. Defer's retry path
/// propagates Terminal errors without modifying the store, so `retry_count` is
/// unchanged and no timer is rescheduled — the existing row stays in place for
/// the new consumer to pick up after rebalance.
#[test]
fn transient_retry_not_redeferred_on_shutdown() -> color_eyre::Result<()> {
    init_test_logging();

    TEST_RUNTIME.block_on(async {
        let topic = Topic::from("test-topic");
        let partition = Partition::from(0_i32);
        let key: Key = Arc::from("test-key");

        let (handler, store, loader, inner, capture, decider) =
            build_shutdown_proof_stack(topic, partition)?;

        // Pre-store message so the loader can serve it during timer retry.
        let message = seed_message(&loader, topic, partition, Offset::from(1_i64), &key)?;

        // --- Step 1: Defer normally (no shutdown) ---
        inner.set_outcome(HandlerOutcome::Transient);
        decider.set_next(true);

        let key_ctx = KeyedCapturingContext::new(key.clone(), capture.clone());
        handler
            .on_message(key_ctx, message, DemandType::Normal)
            .await?;

        // Confirm deferred with retry_count == 0.
        let retry_count_before = store
            .is_deferred(&key)
            .await?
            .ok_or_else(|| eyre!("expected key to be deferred"))?;
        assert_eq!(retry_count_before, 0);

        // --- Step 2: Fire timer, triggering shutdown mid-execution ---
        // Shutdown is signaled by the inner handler during the retry call,
        // exercising the post-call promotion path in CancellationHandler.
        let trigger_time = capture
            .get_timer_time(&key)
            .ok_or_else(|| eyre!("expected an active timer for the deferred key"))?;
        inner.set_outcome(HandlerOutcome::Transient);

        let ctx = MockEventContext::new();
        inner.set_shutdown_trigger(ctx.clone());

        let trigger = Trigger::for_testing(key.clone(), trigger_time, TimerType::DeferredMessage);
        let result = handler.on_timer(ctx, trigger, DemandType::Normal).await;

        // Error must propagate as Terminal; no re-deferral must happen.
        let Err(err) = result else {
            bail!("expected on_timer to fail during shutdown, got Ok");
        };
        assert!(
            matches!(err.classify_error(), ErrorCategory::Terminal),
            "Shutdown-aborted re-deferral must be Terminal so the timer is aborted"
        );

        // Retry count must be unchanged — no increment_retry_count call.
        let state_after = store.get_next_deferred_message(&key).await?;
        assert_eq!(
            state_after,
            Some((Offset::from(1_i64), 0)),
            "Retry count must not be incremented during shutdown"
        );

        Ok(())
    })
}
