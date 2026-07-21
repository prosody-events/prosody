use super::*;
use quanta::Clock;
use tokio::time::{Duration, Instant as TokioInstant};

#[test]
fn test_heartbeat_initially_active() {
    let (clock, _mock) = Clock::mock();
    let heartbeat = Heartbeat::with_clock("test_initial", Duration::from_millis(100), clock);
    heartbeat.beat();
    assert!(
        !heartbeat.is_stalled(),
        "Heartbeat should be active immediately after a beat"
    );
}

#[test]
fn test_heartbeat_becomes_stalled() {
    let (clock, mock) = Clock::mock();
    let heartbeat = Heartbeat::with_clock("test_stall", Duration::from_millis(100), clock);
    heartbeat.beat();

    mock.increment(Duration::from_millis(50));
    assert!(
        !heartbeat.is_stalled(),
        "Heartbeat should be active before the stall threshold is exceeded"
    );

    mock.increment(Duration::from_millis(60));
    assert!(
        heartbeat.is_stalled(),
        "Heartbeat should be stalled after inactivity exceeds the threshold"
    );

    heartbeat.beat();
    assert!(
        !heartbeat.is_stalled(),
        "Heartbeat should recover to active after a fresh beat"
    );
}

#[tokio::test(start_paused = true)]
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
