//! Integration tests for timer functionality in the Prosody system.
//!
//! This module tests the timer scheduling, triggering, and cancellation
//! capabilities of Prosody consumers. It verifies that timers can be set
//! from message handlers, triggered at the correct times, and properly
//! canceled when needed.

#![recursion_limit = "256"]

use ahash::HashSet;
use color_eyre::eyre::{Result, ensure, eyre};
use prosody::cassandra::config::CassandraConfigurationBuilder;
use prosody::consumer::event_context::EventContext;
use prosody::high_level::mode::Mode;
use prosody::high_level::{ConsumerBuilders, HighLevelClient};
use prosody::producer::ProducerConfigurationBuilder;
use prosody::telemetry::TelemetryEmitterConfiguration;
use prosody::tracing::init_test_logging;
use prosody::{
    consumer::ConsumerConfigurationBuilder,
    consumer::message::{ConsumerMessage, UncommittedMessage},
    consumer::middleware::{CloneProvider, FallibleHandler},
    consumer::{DemandType, EventHandler, Keyed, Uncommitted},
    timers::TimerType,
    timers::Trigger,
    timers::UncommittedTimer,
    timers::datetime::CompactDateTime,
    timers::duration::CompactDuration,
};
use serde_json::{Value, json};
use std::time::Duration;
use tokio::sync::mpsc::{Receiver, Sender, channel};
use tokio::time::timeout;
use tracing::info;
use uuid::Uuid;

mod common;
use common::handler::TestError;
use common::kafka::ConsumerEnv;

/// Hang-guard for an individual wait on an event that *must* arrive (a message
/// reaching the handler, a timer firing). These tests assert on event
/// *content* — keys, payloads, scheduled times — which is deterministic; the
/// wait itself only guards against a genuine hang, so it is sized generously.
/// A slow or degraded cluster must never trip it.
const RECEIVE_TIMEOUT: Duration = Duration::from_mins(1);

/// Generous outer hang-guard: bounds total test runtime while staying well
/// above the per-event waits inside the harness, so a genuinely hung step
/// surfaces its own granular error before this fires. Sized so a slow or
/// degraded cluster never trips it on mere slowness.
const TIMER_TEST_TIMEOUT: Duration = Duration::from_mins(3);

/// Test handler that schedules timers based on incoming messages and tracks
/// timer events.
#[derive(Clone)]
struct TimerTestHandler {
    /// Channel for sending timer events to the test
    timer_tx: Sender<TimerEvent>,
    /// Channel for sending message events to the test
    message_tx: Sender<MessageEvent>,
}

/// Represents a timer event for test verification
#[derive(Debug, Clone, PartialEq)]
struct TimerEvent {
    key: String,
    time: CompactDateTime,
}

/// Represents a message event for test verification
#[derive(Debug, Clone, PartialEq)]
struct MessageEvent {
    key: String,
    payload: Value,
}

impl EventHandler for TimerTestHandler {
    type Payload = Value;

    async fn on_message<C>(
        &self,
        context: C,
        message: UncommittedMessage<Value>,
        _demand_type: DemandType,
    ) where
        C: EventContext<Payload = Self::Payload>,
    {
        let (msg, uncommitted) = message.into_inner();
        let key = msg.key().to_string();
        let payload = msg.payload().clone();

        // Send message event for verification
        if let Err(e) = self
            .message_tx
            .send(MessageEvent {
                key: key.clone(),
                payload: payload.clone(),
            })
            .await
        {
            tracing::error!("Failed to send message event: {e}");
            uncommitted.commit().await;
            return;
        }

        // Handle different message types
        if let Some(action) = payload.get("action").and_then(|v| v.as_str()) {
            match action {
                "schedule_timer" => {
                    // Support both absolute time and delay-based scheduling
                    if let Some(target_time_secs) =
                        payload.get("target_time_secs").and_then(Value::as_u64)
                    {
                        // Absolute time scheduling
                        let schedule_time = CompactDateTime::from(target_time_secs as u32);
                        if let Err(e) = context
                            .schedule(schedule_time, TimerType::Application)
                            .await
                        {
                            tracing::error!("Failed to schedule timer for key {key}: {e}");
                        } else {
                            info!("Scheduled timer for key {key} at time {schedule_time}");
                        }
                    } else if let Some(delay_secs) =
                        payload.get("delay_secs").and_then(Value::as_u64)
                    {
                        // Delay-based scheduling
                        let delay = CompactDuration::new(delay_secs as u32);
                        match CompactDateTime::now().and_then(|now| now.add_duration(delay)) {
                            Ok(schedule_time) => {
                                if let Err(e) = context
                                    .schedule(schedule_time, TimerType::Application)
                                    .await
                                {
                                    tracing::error!("Failed to schedule timer for key {key}: {e}");
                                } else {
                                    info!("Scheduled timer for key {key} at time {schedule_time}");
                                }
                            }
                            Err(e) => {
                                tracing::error!("Failed to calculate schedule time: {e}");
                            }
                        }
                    }
                }
                "cancel_timer" => {
                    // Clear all scheduled timers for this key
                    if let Err(e) = context.clear_scheduled(TimerType::Application).await {
                        tracing::error!("Failed to cancel timers for key {key}: {e}");
                    } else {
                        info!("Canceled all timers for key {key}");
                    }
                }
                _ => {
                    info!("Received message with unknown action: {action}");
                }
            }
        }

        uncommitted.commit().await;
    }

    async fn on_timer<C, U>(&self, _context: C, timer: U, _demand_type: DemandType)
    where
        C: EventContext<Payload = Self::Payload>,
        U: UncommittedTimer,
    {
        let key = timer.key().to_string();
        let time = timer.time();

        info!("Timer triggered for key {key} at time {time}");

        // Send timer event for verification
        let timer_event = TimerEvent {
            key: key.clone(),
            time,
        };
        if let Err(e) = self.timer_tx.send(timer_event).await {
            tracing::error!("Failed to send timer event: {e}");
        }

        timer.commit().await;
    }

    async fn shutdown(self) {
        info!("TimerTestHandler shutdown");
    }
}

/// Handler that schedules a timer on first message, then calls
/// `clear_and_schedule` on second message and reports both the replacement
/// time and the actual trigger time so the test can verify the inline
/// replacement path.
#[derive(Clone)]
struct InlineReplacementHandler {
    messages: Sender<String>,
    replacement_time: Sender<CompactDateTime>,
    timer_fired: Sender<CompactDateTime>,
}

impl FallibleHandler for InlineReplacementHandler {
    type Error = TestError;
    type Output = ();
    type Payload = Value;

    async fn on_message<C>(
        &self,
        ctx: C,
        msg: ConsumerMessage<Value>,
        _demand_type: DemandType,
    ) -> Result<(), Self::Error>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        let key = msg.key().to_string();
        let step = msg
            .payload()
            .get("step")
            .and_then(Value::as_i64)
            .ok_or(TestError)?;

        if step == 1 {
            let schedule_time = CompactDateTime::now()
                .and_then(|now| now.add_duration(CompactDuration::new(3)))
                .map_err(|_| TestError)?;
            ctx.schedule(schedule_time, TimerType::Application)
                .await
                .map_err(|_| TestError)?;
        } else {
            let new_time = CompactDateTime::now()
                .and_then(|now| now.add_duration(CompactDuration::new(5)))
                .map_err(|_| TestError)?;
            ctx.clear_and_schedule(new_time, TimerType::Application)
                .await
                .map_err(|_| TestError)?;
            let _ = self.replacement_time.send(new_time).await;
        }

        let _ = self.messages.send(key).await;
        Ok(())
    }

    async fn on_timer<C>(
        &self,
        _ctx: C,
        trigger: Trigger,
        _demand_type: DemandType,
    ) -> Result<(), Self::Error>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        let _ = self.timer_fired.send(trigger.time).await;
        Ok(())
    }

    async fn shutdown(self) {}
}

/// Test environment wrapping [`ConsumerEnv`] with the timer handler's event
/// channels and timer-specific assertion helpers.
struct TestEnvironment {
    env: ConsumerEnv,
    timer_rx: Receiver<TimerEvent>,
    message_rx: Receiver<MessageEvent>,
}

impl TestEnvironment {
    /// Create a new test environment with all necessary components
    async fn new(test_name: &str) -> Result<Self> {
        // Set up channels for test events
        let (timer_tx, timer_rx) = channel(50);
        let (message_tx, message_rx) = channel(50);

        let handler = TimerTestHandler {
            timer_tx,
            message_tx,
        };
        let env =
            ConsumerEnv::new(test_name, async move |_| Ok(CloneProvider::new(handler))).await?;

        Ok(Self {
            env,
            timer_rx,
            message_rx,
        })
    }

    /// Send a message with the given key and payload
    async fn send_message(&self, key: &str, payload: Value) -> Result<()> {
        self.env.send_message(key, payload).await
    }

    /// Send a timer scheduling message
    async fn schedule_timer(&self, key: &str, delay_secs: u32) -> Result<()> {
        let message = json!({
            "action": "schedule_timer",
            "delay_secs": delay_secs
        });
        self.send_message(key, message).await
    }

    /// Send a timer scheduling message with absolute time
    async fn schedule_timer_at(&self, key: &str, target_time_secs: u64) -> Result<()> {
        let message = json!({
            "action": "schedule_timer",
            "target_time_secs": target_time_secs
        });
        self.send_message(key, message).await
    }

    /// Send a timer cancellation message
    async fn cancel_timer(&self, key: &str) -> Result<()> {
        let message = json!({
            "action": "cancel_timer"
        });
        self.send_message(key, message).await
    }

    /// Wait for a message event under the receive hang-guard
    async fn expect_message(&mut self) -> Result<MessageEvent> {
        common::receive::expect_event(&mut self.message_rx, RECEIVE_TIMEOUT).await
    }

    /// Wait for a timer event under the receive hang-guard
    async fn expect_timer(&mut self) -> Result<TimerEvent> {
        common::receive::expect_event(&mut self.timer_rx, RECEIVE_TIMEOUT).await
    }

    /// Wait for exactly `count` timer events, then verify no extras arrive
    async fn expect_timers(&mut self, count: usize) -> Result<Vec<TimerEvent>> {
        let mut received_timers = Vec::with_capacity(count);

        for i in 0..count {
            let timer_event = common::receive::expect_event(&mut self.timer_rx, RECEIVE_TIMEOUT)
                .await
                .map_err(|e| eyre!("waiting for timer {} of {}: {e}", i + 1, count))?;
            received_timers.push(timer_event);
        }

        // Verify no extra timers are received
        if let Ok(Some(extra_timer)) =
            timeout(Duration::from_millis(100), self.timer_rx.recv()).await
        {
            return Err(eyre!(
                "Received unexpected extra timer for key '{}'",
                extra_timer.key
            ));
        }

        Ok(received_timers)
    }

    /// Verify that no timer event occurs within the given window
    async fn expect_no_timer(&mut self, window_secs: u32) -> Result<()> {
        common::receive::expect_no_event(
            &mut self.timer_rx,
            Duration::from_secs(u64::from(window_secs)),
        )
        .await
    }

    /// Verify a message event matches expected key and payload
    fn verify_message_event(
        event: &MessageEvent,
        expected_key: &str,
        expected_payload: &Value,
    ) -> Result<()> {
        ensure!(
            event.key == expected_key,
            "Message key mismatch: expected '{}', got '{}'",
            expected_key,
            event.key
        );
        ensure!(
            event.payload == *expected_payload,
            "Message payload mismatch: expected {:?}, got {:?}",
            expected_payload,
            event.payload
        );
        Ok(())
    }

    /// Verify a timer event matches expected key
    fn verify_timer_event(event: &TimerEvent, expected_key: &str) -> Result<()> {
        ensure!(
            event.key == expected_key,
            "Timer key mismatch: expected '{}', got '{}'",
            expected_key,
            event.key
        );
        info!(
            "Timer triggered for key '{}' at time {}",
            event.key, event.time
        );
        Ok(())
    }

    /// Verify timers are in chronological order
    fn verify_timer_order(timers: &[TimerEvent]) -> Result<()> {
        for i in 1..timers.len() {
            ensure!(
                timers[i - 1].time.epoch_seconds() <= timers[i].time.epoch_seconds(),
                "Timers not in chronological order: timer {} at {} came after timer {} at {}",
                timers[i - 1].key,
                timers[i - 1].time,
                timers[i].key,
                timers[i].time
            );
        }
        Ok(())
    }

    /// Verify timers contain exactly the expected keys
    fn verify_timer_keys(timers: &[TimerEvent], expected_keys: &HashSet<String>) -> Result<()> {
        let actual_keys: HashSet<String> = timers.iter().map(|t| t.key.clone()).collect();
        ensure!(
            actual_keys == *expected_keys,
            "Timer keys mismatch: expected {:?}, got {:?}",
            expected_keys,
            actual_keys
        );
        Ok(())
    }

    /// Clean up resources (consumer shutdown, topic deletion)
    async fn cleanup(self) {
        self.env.shutdown().await;
    }
}

/// Run a test with timeout and proper error handling
async fn run_test<F, Fut>(test_name: &str, test_fn: F) -> Result<()>
where
    F: FnOnce(TestEnvironment) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    init_test_logging();

    let result = timeout(TIMER_TEST_TIMEOUT, async {
        let env = TestEnvironment::new(test_name).await?;

        test_fn(env).await
    })
    .await;

    result.map_err(|_| eyre!("Test '{test_name}' timed out after {TIMER_TEST_TIMEOUT:?}"))?
}

/// Build a `HighLevelClient` for the inline-replacement test — the one test
/// in this file that must drive `clear_and_schedule` through the full
/// pipeline-mode middleware stack rather than the direct-consumer harness.
fn build_inline_replacement_client(
    source_topic: &str,
    telemetry_topic: &str,
) -> Result<HighLevelClient<InlineReplacementHandler>> {
    let mut producer_builder = ProducerConfigurationBuilder::default();
    producer_builder
        .bootstrap_servers(vec!["localhost:9094".to_owned()])
        .source_system("timer-inline-replacement-test");

    let mut consumer_builder = ConsumerConfigurationBuilder::default();
    consumer_builder
        .bootstrap_servers(vec!["localhost:9094".to_owned()])
        .group_id(Uuid::new_v4().to_string())
        .subscribed_topics(vec![source_topic.to_owned()])
        .probe_port(None);

    let consumer_builders = ConsumerBuilders {
        consumer: consumer_builder,
        emitter: TelemetryEmitterConfiguration {
            topic: telemetry_topic.to_owned(),
            enabled: true,
        },
        ..ConsumerBuilders::new()?
    };

    let mut cassandra_builder = CassandraConfigurationBuilder::default();
    cassandra_builder.nodes(vec!["localhost:9042".to_owned()]);

    let client = HighLevelClient::new(
        Mode::Pipeline,
        &mut producer_builder,
        &consumer_builders,
        &cassandra_builder,
    )?;
    Ok(client)
}

/// Tests basic timer scheduling and triggering functionality.
#[tokio::test]
async fn test_timer_scheduling_and_triggering() -> Result<()> {
    run_test("timer-test", |mut env| async move {
        let key = "test-key";
        let delay_secs = 1u32;
        let schedule_message = json!({
            "action": "schedule_timer",
            "delay_secs": delay_secs
        });

        // Schedule the timer
        env.schedule_timer(key, delay_secs).await?;

        // Verify message was received
        let message_event = env.expect_message().await?;
        TestEnvironment::verify_message_event(&message_event, key, &schedule_message)?;

        // Wait for timer to trigger
        let timer_event = env.expect_timer().await?;
        TestEnvironment::verify_timer_event(&timer_event, key)?;

        // Clean up
        env.cleanup().await;
        Ok(())
    })
    .await
}

/// Tests edge case: scheduling multiple timers for the same key.
#[tokio::test]
async fn test_same_key_multiple_timers() -> Result<()> {
    run_test("timer-same-key-test", |mut env| async move {
        let key = "same-key";
        let delays = vec![1u32, 2u32, 3u32];

        // Schedule multiple timers for the same key
        for delay_secs in &delays {
            env.schedule_timer(key, *delay_secs).await?;
        }

        // Verify all schedule messages were received
        for i in 0..delays.len() {
            env.expect_message()
                .await
                .map_err(|e| eyre!("Failed to receive schedule message {}: {}", i + 1, e))?;
        }

        // Wait for all timer events
        let received_timers = env.expect_timers(delays.len()).await?;

        // Verify all timers are for the same key
        for timer_event in &received_timers {
            TestEnvironment::verify_timer_event(timer_event, key)?;
        }

        // Verify timers triggered in chronological order
        TestEnvironment::verify_timer_order(&received_timers)?;

        // Clean up
        env.cleanup().await;
        Ok(())
    })
    .await
}

/// Tests immediate timer scheduling (1 second delay).
#[tokio::test]
async fn test_immediate_timer() -> Result<()> {
    run_test("timer-immediate-test", |mut env| async move {
        let key = "immediate-key";
        let delay_secs = 1u32;

        let start_time = CompactDateTime::now()?;

        // Schedule immediate timer
        env.schedule_timer(key, delay_secs).await?;

        // Verify message was received
        env.expect_message().await?;

        // Wait for timer to trigger
        let timer_event = env.expect_timer().await?;
        TestEnvironment::verify_timer_event(&timer_event, key)?;

        // Verify timing accuracy (should trigger within 1-2 seconds)
        let end_time = CompactDateTime::now()?;
        let elapsed = end_time.epoch_seconds() - start_time.epoch_seconds();
        ensure!(
            (1..=3).contains(&elapsed),
            "Timer took {} seconds, expected 1-3 seconds",
            elapsed
        );

        // Clean up
        env.cleanup().await;
        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_timer_scheduled_time_accuracy() -> Result<()> {
    run_test("timer-accuracy-test", |mut env| async move {
        let key = "accuracy-key";

        // Calculate a specific target time (2 seconds from now)
        let target_time_secs = u64::from(
            CompactDateTime::now()
                .map_err(|e| eyre!("Failed to get current time: {e}"))?
                .add_duration(CompactDuration::new(2))
                .map_err(|e| eyre!("Failed to add duration: {e}"))?
                .epoch_seconds(),
        );

        let expected_time = CompactDateTime::from(target_time_secs as u32);
        let schedule_message = json!({
            "action": "schedule_timer",
            "target_time_secs": target_time_secs
        });

        // Schedule timer at specific absolute time
        env.schedule_timer_at(key, target_time_secs).await?;

        // Verify message was received
        let message_event = env.expect_message().await?;
        TestEnvironment::verify_message_event(&message_event, key, &schedule_message)?;

        // Wait for timer to trigger
        let timer_event = env.expect_timer().await?;
        TestEnvironment::verify_timer_event(&timer_event, key)?;

        // Verify timer accuracy - should match exactly
        ensure!(
            timer_event.time == expected_time,
            "Timer triggered at different time than scheduled. Expected: {}, Actual: {}",
            expected_time,
            timer_event.time
        );

        // Additional verification: timer should have triggered at the right wall-clock
        // time
        let actual_trigger_time = CompactDateTime::now()?;
        let time_diff = actual_trigger_time
            .epoch_seconds()
            .abs_diff(expected_time.epoch_seconds());
        ensure!(
            time_diff <= 1,
            "Timer triggered too far from expected time. Expected: {}, Now: {}, Diff: {} seconds",
            expected_time,
            actual_trigger_time,
            time_diff
        );

        info!(
            "✓ Timer accuracy test passed: scheduled time {} matches trigger time {}",
            expected_time, timer_event.time
        );

        // Clean up
        env.cleanup().await;
        Ok(())
    })
    .await
}

/// Tests timer cancellation functionality.
#[tokio::test]
async fn test_timer_cancellation() -> Result<()> {
    run_test("timer-cancellation-test", |mut env| async move {
        let key = "cancellation-key";
        let delay_secs = 3u32;

        // Schedule a timer
        env.schedule_timer(key, delay_secs).await?;

        // Verify schedule message was received
        let message_event = env.expect_message().await?;
        let expected_schedule_message = json!({
            "action": "schedule_timer",
            "delay_secs": delay_secs
        });
        TestEnvironment::verify_message_event(&message_event, key, &expected_schedule_message)?;

        // Cancel the timer
        env.cancel_timer(key).await?;

        // Verify cancellation message was received
        let cancel_message_event = env.expect_message().await?;
        let expected_cancel_message = json!({
            "action": "cancel_timer"
        });
        TestEnvironment::verify_message_event(
            &cancel_message_event,
            key,
            &expected_cancel_message,
        )?;

        // Verify no timer fires (wait longer than the original delay)
        env.expect_no_timer(delay_secs + 2).await?;

        // Clean up
        env.cleanup().await;
        Ok(())
    })
    .await
}

/// Tests multiple timers with different keys and timing.
#[tokio::test]
async fn test_multiple_timers() -> Result<()> {
    run_test("timer-multiple-test", |mut env| async move {
        // Schedule multiple timers with staggered delays
        let timers_data = vec![("key1", 1u32), ("key2", 2u32), ("key3", 3u32)];

        for (key, delay_secs) in &timers_data {
            env.schedule_timer(key, *delay_secs).await?;
        }

        // Verify all schedule messages were received
        for i in 0..timers_data.len() {
            env.expect_message()
                .await
                .map_err(|e| eyre!("Failed to receive schedule message {}: {}", i, e))?;
        }

        // Collect timer events as they trigger
        let received_timers = env.expect_timers(timers_data.len()).await?;

        // Verify timers are in chronological order
        TestEnvironment::verify_timer_order(&received_timers)?;

        // Verify all expected keys are present
        let expected_keys: HashSet<String> =
            timers_data.iter().map(|(k, _)| (*k).to_owned()).collect();
        TestEnvironment::verify_timer_keys(&received_timers, &expected_keys)?;

        // Log all received timer events
        for timer_event in &received_timers {
            info!(
                "Timer for key {} triggered with scheduled time: {}",
                timer_event.key, timer_event.time
            );
        }

        // Clean up
        env.cleanup().await;
        Ok(())
    })
    .await
}

/// Tests timer behavior for different keys.
#[tokio::test]
async fn test_timer_different_keys() -> Result<()> {
    run_test("timer-keys-test", |mut env| async move {
        // Schedule timers for different keys
        let timers = vec!["key-a", "key-b"];
        let delay_secs = 2u32;

        for key in &timers {
            env.schedule_timer(key, delay_secs).await?;
        }

        // Verify schedule messages were received
        for i in 0..timers.len() {
            env.expect_message()
                .await
                .map_err(|e| eyre!("Failed to receive schedule message {}: {}", i, e))?;
        }

        // Wait for timers to trigger
        let received_timers = env.expect_timers(2).await?;

        // Verify timers are in chronological order
        TestEnvironment::verify_timer_order(&received_timers)?;

        // Verify both expected keys are present
        let expected_keys: HashSet<String> = timers.iter().map(|k| (*k).to_owned()).collect();
        TestEnvironment::verify_timer_keys(&received_timers, &expected_keys)?;

        // Clean up
        env.cleanup().await;
        Ok(())
    })
    .await
}

/// Verifies the Inline→Inline tombstone-free timer replacement path:
/// `schedule` followed by `clear_and_schedule` on the same key results in
/// exactly one timer firing at the replacement time.
#[tokio::test(flavor = "multi_thread")]
async fn inline_replacement_fires_once_at_replacement_time() -> Result<()> {
    timeout(TIMER_TEST_TIMEOUT, async {
        init_test_logging();

        let (source, admin) = common::kafka::create_topic_with_partitions(1).await?;
        let (telemetry_topic, _) = common::kafka::create_topic_with_partitions(1).await?;
        let source_topic = source.to_string();

        let client: HighLevelClient<InlineReplacementHandler> =
            build_inline_replacement_client(&source_topic, telemetry_topic.as_ref())?;

        let (messages, mut msg_rx) = channel(16);
        let (replacement_time, mut replacement_time_rx) = channel(16);
        let (timer_fired, mut timer_rx) = channel(16);
        client
            .subscribe(InlineReplacementHandler {
                messages,
                replacement_time,
                timer_fired,
            })
            .await?;

        // Step 1: schedule at t+3s
        client
            .send(source, "replace-key", json!({"step": 1_i32}))
            .await?;
        let _ = timeout(RECEIVE_TIMEOUT, msg_rx.recv()).await?;

        // Step 2: clear_and_schedule at t+5s (replaces the original)
        client
            .send(source, "replace-key", json!({"step": 2_i32}))
            .await?;
        let _ = timeout(RECEIVE_TIMEOUT, msg_rx.recv()).await?;

        // Capture the replacement time the handler recorded
        let replacement_time = timeout(RECEIVE_TIMEOUT, replacement_time_rx.recv())
            .await
            .map_err(|_| eyre!("timeout waiting for replacement time"))?
            .ok_or_else(|| eyre!("replacement_time channel closed"))?;

        // Wait for the timer to fire (should be ~5s from step 2)
        let trigger_time = timeout(RECEIVE_TIMEOUT, timer_rx.recv())
            .await
            .map_err(|_| eyre!("timeout waiting for on_timer — timer never fired"))?
            .ok_or_else(|| eyre!("timer_fired channel closed"))?;

        // The timer must fire at the replacement time, not the original
        ensure!(
            trigger_time == replacement_time,
            "timer fired at {trigger_time:?} but expected replacement time {replacement_time:?}"
        );

        // Verify no second timer fires (the original t+3s was replaced)
        let second = timeout(Duration::from_secs(5), timer_rx.recv()).await;
        ensure!(second.is_err(), "expected no second timer but received one");

        client.unsubscribe().await?;
        admin.delete_topic(&source).await?;
        admin.delete_topic(&telemetry_topic).await?;
        Ok(())
    })
    .await
    .map_err(|_| eyre!("test timed out after {TIMER_TEST_TIMEOUT:?}"))?
}
