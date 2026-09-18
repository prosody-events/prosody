//! Checks error classification and disabled deferral.

use super::*;
use crate::cassandra::errors::CassandraStoreError;
use crate::consumer::middleware::defer::CassandraDeferStoreError;
use crate::loader::KafkaLoaderError;
use scylla::errors::ExecutionError;

#[test]
fn store_write_failure_retries_via_retry_middleware() {
    // This test verifies the error classification chain that enables the
    // composition where timer defer is inside message defer to work correctly.
    //
    // When a timer defer store operation fails with a transient error (e.g.,
    // Cassandra timeout), the error must:
    // 1. Be wrapped as DeferError::Store by TimerDeferHandler
    // 2. Classify as Transient (delegating to inner error classification)
    // 3. Enable MessageDeferHandler to catch and defer via message-based retry
    //
    // This test validates the classification chain using CassandraStoreError
    // which can produce transient errors (resource exhaustion, timeouts).
    init_test_logging();

    // Simulate a transient Cassandra error (no nodes available in plan)
    // This represents cluster unavailability during partitions or maintenance
    let execution_error = ExecutionError::EmptyPlan;
    let cassandra_store_error = CassandraStoreError::from(execution_error);

    // Verify Cassandra store error classifies as transient
    let cassandra_classification = cassandra_store_error.classify_error();
    assert!(
        matches!(cassandra_classification, ErrorCategory::Transient),
        "EmptyPlan should classify as transient"
    );

    // Wrap in CassandraDeferStoreError (unified error type)
    let timer_store_error = CassandraDeferStoreError::Cassandra(cassandra_store_error);
    assert!(
        matches!(timer_store_error.classify_error(), ErrorCategory::Transient),
        "CassandraDeferStoreError should delegate to inner Cassandra classification"
    );

    // Wrap in DeferError::Store (as TimerDeferHandler does)
    let defer_error: DeferError<CassandraDeferStoreError, OutcomeError, KafkaLoaderError> =
        DeferError::Store(timer_store_error);

    // Final verification: DeferError::Store classifies as transient
    assert!(
        matches!(defer_error.classify_error(), ErrorCategory::Transient),
        "DeferError::Store with transient Cassandra error should classify as transient, enabling \
         message defer middleware to handle via message-based retry"
    );

    // Verify permanent handler errors still propagate correctly
    let permanent_error: DeferError<CassandraDeferStoreError, OutcomeError, KafkaLoaderError> =
        DeferError::Handler(OutcomeError::Permanent);
    assert!(
        matches!(permanent_error.classify_error(), ErrorCategory::Permanent),
        "DeferError::Handler with permanent error should classify as permanent"
    );
}

#[test]
fn disabled_config_propagates_errors_no_deferral() -> color_eyre::Result<()> {
    // When `enabled: false`, transient errors propagate to the caller instead of
    // being absorbed by deferral. No deferral occurs.
    //
    // New failures propagate to retry middleware (no deferral for either messages
    // or timers).
    init_test_logging();

    TEST_RUNTIME.block_on(async {
        // Create harness with disabled configuration
        let harness = TestHarness::with_enabled(false)?;

        // Set handler to return transient error
        harness.inner_handler.set_outcome(HandlerOutcome::Transient);
        // Decider would say yes, but config.enabled=false takes precedence
        harness.decider.set_next(true);

        let trigger = TestHarness::create_trigger("disabled-test-key", 1000);
        let result = harness
            .handler
            .on_timer(
                harness.context().clone(),
                trigger.clone(),
                DemandType::Normal,
            )
            .await;

        // Should fail - error propagates instead of being absorbed
        assert!(
            result.is_err(),
            "With enabled=false, transient error should propagate"
        );

        // Verify it's a Handler error (the transient error wrapped)
        let err = result
            .err()
            .ok_or_else(|| color_eyre::eyre::eyre!("Expected error"))?;
        assert!(
            matches!(err, DeferError::Handler(_)),
            "Error should be DeferError::Handler containing the transient error"
        );

        // Key should NOT be deferred
        let retry_count = harness.get_retry_count("disabled-test-key").await?;
        assert!(
            retry_count.is_none(),
            "Key should NOT be deferred when config.enabled=false"
        );

        // No DeferredTimer should be scheduled
        assert!(
            harness
                .context()
                .scheduled(TimerType::DeferredTimer)
                .await?
                .is_empty(),
            "No DeferredTimer should be scheduled when disabled"
        );

        // Inner handler should have been called exactly once
        assert_eq!(
            harness.inner_handler.timer_calls().len(),
            1,
            "Inner handler should be called once"
        );

        Ok(())
    })
}
