use super::*;
use tokio::time::{Duration, Instant as TokioInstant, sleep};

#[tokio::test]
async fn test_heartbeat_initially_active() {
    let threshold = Duration::from_millis(100);
    let heartbeat = Heartbeat::new("test_initial", threshold);
    heartbeat.beat();
    assert!(
        !heartbeat.is_stalled(),
        "Heartbeat should be active immediately after a beat"
    );
}

#[tokio::test]
async fn test_heartbeat_becomes_stalled() {
    let threshold = Duration::from_millis(100);
    let heartbeat = Heartbeat::new("test_stall", threshold);
    heartbeat.beat();
    sleep(Duration::from_millis(50)).await;
    assert!(
        !heartbeat.is_stalled(),
        "Heartbeat should be active before the stall threshold is exceeded"
    );
    sleep(Duration::from_millis(60)).await;
    assert!(
        heartbeat.is_stalled(),
        "Heartbeat should be stalled after inactivity exceeds the threshold"
    );
}

#[tokio::test]
async fn test_next_sleep_duration() {
    let threshold = Duration::from_millis(100);
    let heartbeat = Heartbeat::new("test_next", threshold);
    let expected_sleep = threshold / HEARTBEAT_MARGIN;
    let start = TokioInstant::now();
    heartbeat.next().await;
    let elapsed = TokioInstant::now().duration_since(start);
    assert!(
        elapsed >= expected_sleep,
        "next() did not sleep for the expected duration (expected at least {expected_sleep:?}, \
         got {elapsed:?})",
    );
}
