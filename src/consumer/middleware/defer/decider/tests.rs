use super::*;
use crate::consumer::DemandType;
use crate::telemetry::Telemetry;
use crate::{Partition, Topic};
use tokio::task::yield_now;
use tokio::time::sleep;

#[test]
fn always_defer_returns_true() {
    let decider = AlwaysDefer;
    assert!(decider.should_defer());
    assert!(decider.should_defer()); // multiple calls
}

#[test]
fn trace_based_decider_defaults_to_true() {
    let decider = TraceBasedDecider::new();
    assert!(decider.should_defer());
}

#[test]
fn trace_based_decider_returns_configured_value() {
    let decider = TraceBasedDecider::new();

    decider.set_next(false);
    assert!(!decider.should_defer());

    decider.set_next(true);
    assert!(decider.should_defer());

    decider.set_next(false);
    assert!(!decider.should_defer());
}

#[test]
fn trace_based_decider_is_clone_safe() {
    let decider1 = TraceBasedDecider::new();
    let decider2 = decider1.clone();

    decider1.set_next(false);
    assert!(!decider2.should_defer()); // Clones share state
}

// ========================================================================
// FailureTracker Tests
// ========================================================================

/// Helper to emit success telemetry events
fn emit_success(telemetry: &Telemetry) {
    let sender = telemetry.partition_sender(Topic::from("test"), Partition::from(0_i32));
    sender.handler_succeeded(Arc::from("test-key"), DemandType::Normal);
}

/// Helper to emit failure telemetry events
fn emit_failure(telemetry: &Telemetry) {
    let sender = telemetry.partition_sender(Topic::from("test"), Partition::from(0_i32));
    sender.handler_failed(Arc::from("test-key"), DemandType::Normal);
}

#[tokio::test]
async fn test_new_tracker() {
    let telemetry = Telemetry::new();
    let tracker = FailureTracker::new(
        Duration::from_mins(1),
        0.9_f64,
        &telemetry,
        &HeartbeatRegistry::test(),
    );
    // Give actor time to initialize
    yield_now().await;
    assert!(tracker.should_defer());
    assert!((tracker.failure_rate() - 0.0_f64).abs() < f64::EPSILON);
}

#[tokio::test]
async fn test_only_successes() {
    let telemetry = Telemetry::new();
    let tracker = FailureTracker::new(
        Duration::from_mins(1),
        0.9_f64,
        &telemetry,
        &HeartbeatRegistry::test(),
    );

    emit_success(&telemetry);
    emit_success(&telemetry);
    emit_success(&telemetry);

    // Give actor time to process
    yield_now().await;

    assert!(tracker.should_defer());
    assert!((tracker.failure_rate() - 0.0_f64).abs() < f64::EPSILON);
}

#[tokio::test]
async fn test_only_failures() {
    let telemetry = Telemetry::new();
    let tracker = FailureTracker::new(
        Duration::from_mins(1),
        0.9_f64,
        &telemetry,
        &HeartbeatRegistry::test(),
    );

    emit_failure(&telemetry);
    emit_failure(&telemetry);
    emit_failure(&telemetry);

    // Give actor time to process
    yield_now().await;

    assert!(!tracker.should_defer());
    assert!((tracker.failure_rate() - 1.0_f64).abs() < f64::EPSILON);
}

#[tokio::test]
async fn test_below_threshold() {
    let telemetry = Telemetry::new();
    let tracker = FailureTracker::new(
        Duration::from_mins(1),
        0.5_f64,
        &telemetry,
        &HeartbeatRegistry::test(),
    );

    emit_success(&telemetry);
    emit_success(&telemetry);
    emit_failure(&telemetry);

    // Give actor time to process
    yield_now().await;

    // 1/3 = 0.333... < 0.5
    assert!(tracker.should_defer());
    assert!((tracker.failure_rate() - 0.333_f64).abs() < 0.01_f64);
}

#[tokio::test]
async fn test_above_threshold() {
    let telemetry = Telemetry::new();
    let tracker = FailureTracker::new(
        Duration::from_mins(1),
        0.5_f64,
        &telemetry,
        &HeartbeatRegistry::test(),
    );

    emit_success(&telemetry);
    emit_failure(&telemetry);
    emit_failure(&telemetry);

    // Give actor time to process
    yield_now().await;

    // 2/3 = 0.666... > 0.5
    assert!(!tracker.should_defer());
    assert!((tracker.failure_rate() - 0.666_f64).abs() < 0.01_f64);
}

#[tokio::test]
async fn test_at_threshold() {
    let telemetry = Telemetry::new();
    let tracker = FailureTracker::new(
        Duration::from_mins(1),
        0.5_f64,
        &telemetry,
        &HeartbeatRegistry::test(),
    );

    emit_success(&telemetry);
    emit_failure(&telemetry);

    // Give actor time to process
    yield_now().await;

    // 1/2 = 0.5 == 0.5 (not less than, so should_defer = false)
    assert!(!tracker.should_defer());
    assert!((tracker.failure_rate() - 0.5_f64).abs() < f64::EPSILON);
}

#[tokio::test]
async fn test_window_expiration() {
    let telemetry = Telemetry::new();
    let tracker = FailureTracker::new(
        Duration::from_millis(100),
        0.5_f64,
        &telemetry,
        &HeartbeatRegistry::test(),
    );

    // Record failures that will expire
    emit_failure(&telemetry);
    emit_failure(&telemetry);

    // Give actor time to process
    yield_now().await;

    // Wait for events to expire
    sleep(Duration::from_millis(150)).await;

    // Record new success
    emit_success(&telemetry);

    // Give actor time to process
    yield_now().await;

    // Old failures should be pruned, only success remains
    assert!(tracker.should_defer());
    assert!((tracker.failure_rate() - 0.0_f64).abs() < f64::EPSILON);
}

#[tokio::test]
async fn test_concurrent_access() {
    let telemetry = Telemetry::new();
    let tracker = FailureTracker::new(
        Duration::from_mins(1),
        0.5_f64,
        &telemetry,
        &HeartbeatRegistry::test(),
    );

    let telemetry_clone = telemetry.clone();
    let handle1 = spawn(async move {
        for _ in 0_i32..10_i32 {
            emit_success(&telemetry_clone);
        }
    });

    let telemetry_clone = telemetry.clone();
    let handle2 = spawn(async move {
        for _ in 0_i32..5_i32 {
            emit_failure(&telemetry_clone);
        }
    });

    assert!(handle1.await.is_ok());
    assert!(handle2.await.is_ok());

    // Give actor time to process all events
    yield_now().await;

    // 5 failures / 15 total = 0.333... < 0.5
    assert!(tracker.should_defer());
    assert!((tracker.failure_rate() - 0.333_f64).abs() < 0.01_f64);
}
