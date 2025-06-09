//! Integration tests for timer functionality in the Prosody system.
//!
//! This module tests the timer scheduling, triggering, and cancellation
//! capabilities of Prosody consumers. It verifies that timers can be set
//! from message handlers, triggered at the correct times, and properly
//! canceled when needed.

use ahash::HashSet;
use color_eyre::eyre::{Result, ensure, eyre};
use prosody::consumer::event_context::EventContext;
use prosody::{
    Topic,
    admin::ProsodyAdminClient,
    consumer::message::UncommittedMessage,
    consumer::{ConsumerConfiguration, EventHandler, Keyed, ProsodyConsumer},
    producer::{ProducerConfiguration, ProsodyProducer},
    timers::UncommittedTimer,
    timers::datetime::CompactDateTime,
    timers::duration::CompactDuration,
};
use serde_json::{Value, json};
use std::time::Duration;
use tokio::sync::mpsc::{Receiver, Sender, channel};
use tokio::time::timeout;
use tracing::{error, info};
use tracing_subscriber::fmt;
use uuid::Uuid;

#[path = "common.rs"]
mod common;

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
    async fn on_message<C>(&self, context: C, message: UncommittedMessage)
    where
        C: EventContext,
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
            error!("Failed to send message event: {e}");
            uncommitted.commit();
            return;
        }

        // Handle different message types
        if let Some(action) = payload.get("action").and_then(|v| v.as_str()) {
            match action {
                "schedule_timer" => {
                    // Support both absolute time and delay-based scheduling
                    if let Some(target_time_secs) =
                        payload.get("target_time_secs").and_then(|v| v.as_u64())
                    {
                        // Absolute time scheduling
                        let schedule_time = CompactDateTime::from(target_time_secs as u32);
                        if let Err(e) = context.schedule(schedule_time).await {
                            error!("Failed to schedule timer for key {key}: {e}");
                        } else {
                            info!("Scheduled timer for key {key} at time {schedule_time}");
                        }
                    } else if let Some(delay_secs) =
                        payload.get("delay_secs").and_then(|v| v.as_u64())
                    {
                        // Delay-based scheduling
                        let delay = CompactDuration::new(delay_secs as u32);
                        match CompactDateTime::now().and_then(|now| now.add_duration(delay)) {
                            Ok(schedule_time) => {
                                if let Err(e) = context.schedule(schedule_time).await {
                                    error!("Failed to schedule timer for key {key}: {e}");
                                } else {
                                    info!("Scheduled timer for key {key} at time {schedule_time}");
                                }
                            }
                            Err(e) => {
                                error!("Failed to calculate schedule time: {e}");
                            }
                        }
                    }
                }
                "cancel_timer" => {
                    // Clear all scheduled timers for this key
                    if let Err(e) = context.clear_scheduled().await {
                        error!("Failed to cancel timers for key {key}: {e}");
                    } else {
                        info!("Canceled all timers for key {key}");
                    }
                }
                _ => {
                    info!("Received message with unknown action: {action}");
                }
            }
        }

        uncommitted.commit();
    }

    async fn on_timer<C, U>(&self, _context: C, timer: U)
    where
        C: EventContext,
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
            error!("Failed to send timer event: {e}");
        }

        timer.commit().await;
    }

    async fn shutdown(self) {
        info!("TimerTestHandler shutdown");
    }
}

/// Test environment that encapsulates common setup and provides helper methods
struct TestEnvironment {
    topic: Topic,
    admin_client: ProsodyAdminClient,
    consumer: ProsodyConsumer,
    producer: ProsodyProducer,
    timer_rx: Receiver<TimerEvent>,
    message_rx: Receiver<MessageEvent>,
}

impl TestEnvironment {
    /// Create a new test environment with all necessary components
    async fn new(test_name: &str) -> Result<Self> {
        let topic: Topic = format!("{}-{}", test_name, Uuid::new_v4()).as_str().into();
        let bootstrap = vec!["localhost:9094".to_owned()];
        let admin_client = ProsodyAdminClient::new(&bootstrap)?;
        admin_client.create_topic(&topic, 1, 1).await?;

        // Set up channels for test events
        let (timer_tx, timer_rx) = channel(50);
        let (message_tx, message_rx) = channel(50);

        // Create handler and consumer
        let handler = TimerTestHandler {
            timer_tx,
            message_tx,
        };

        let group_id = format!("{}-consumer-{}", test_name, Uuid::new_v4());
        let consumer_config = ConsumerConfiguration::builder()
            .bootstrap_servers(bootstrap.clone())
            .group_id(&group_id)
            .subscribed_topics(&[topic.to_string()])
            .probe_port(None)
            .build()?;

        let consumer = ProsodyConsumer::new(&consumer_config, handler)?;

        // Create producer
        let producer_config = ProducerConfiguration::builder()
            .bootstrap_servers(bootstrap.clone())
            .source_system("test-producer")
            .build()?;
        let producer = ProsodyProducer::new(&producer_config)?;

        // Give consumer time to start and subscribe
        tokio::time::sleep(Duration::from_secs(2)).await;

        Ok(Self {
            topic,
            admin_client,
            consumer,
            producer,
            timer_rx,
            message_rx,
        })
    }

    /// Send a message with the given key and payload
    async fn send_message(&self, key: &str, payload: &Value) -> Result<()> {
        self.producer
            .send([], self.topic.clone(), key, payload)
            .await
            .map_err(|e| eyre!("Failed to send message: {}", e))
    }

    /// Send a timer scheduling message
    async fn schedule_timer(&self, key: &str, delay_secs: u32) -> Result<()> {
        let message = json!({
            "action": "schedule_timer",
            "delay_secs": delay_secs
        });
        self.send_message(key, &message).await
    }

    /// Send a timer scheduling message with absolute time
    async fn schedule_timer_at(&self, key: &str, target_time_secs: u64) -> Result<()> {
        let message = json!({
            "action": "schedule_timer",
            "target_time_secs": target_time_secs
        });
        self.send_message(key, &message).await
    }

    /// Send a timer cancellation message
    async fn cancel_timer(&self, key: &str) -> Result<()> {
        let message = json!({
            "action": "cancel_timer"
        });
        self.send_message(key, &message).await
    }

    /// Wait for a message event with timeout
    async fn expect_message(&mut self, timeout_secs: u64) -> Result<MessageEvent> {
        timeout(Duration::from_secs(timeout_secs), self.message_rx.recv())
            .await
            .map_err(|_| {
                eyre!(
                    "Timeout waiting for message event after {} seconds",
                    timeout_secs
                )
            })?
            .ok_or_else(|| eyre!("Message channel closed unexpectedly"))
    }

    /// Wait for a timer event with timeout
    async fn expect_timer(&mut self, timeout_secs: u64) -> Result<TimerEvent> {
        timeout(Duration::from_secs(timeout_secs), self.timer_rx.recv())
            .await
            .map_err(|_| {
                eyre!(
                    "Timeout waiting for timer event after {} seconds",
                    timeout_secs
                )
            })?
            .ok_or_else(|| eyre!("Timer channel closed unexpectedly"))
    }

    /// Wait for exact number of timer events with timeout
    async fn expect_timers(&mut self, count: usize, timeout_secs: u64) -> Result<Vec<TimerEvent>> {
        let mut received_timers = Vec::new();

        for i in 0..count {
            match timeout(Duration::from_secs(timeout_secs), self.timer_rx.recv()).await {
                Ok(Some(timer_event)) => {
                    received_timers.push(timer_event);
                }
                Ok(None) => {
                    return Err(eyre!(
                        "Timer channel closed after receiving {} of {} expected timers",
                        i,
                        count
                    ));
                }
                Err(_) => {
                    return Err(eyre!(
                        "Timeout waiting for timer {} of {} after {} seconds",
                        i + 1,
                        count,
                        timeout_secs
                    ));
                }
            }
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

    /// Verify that no timer event occurs within the given timeout
    async fn expect_no_timer(&mut self, timeout_secs: u32) -> Result<()> {
        let timer_result = timeout(
            Duration::from_secs(timeout_secs as u64),
            self.timer_rx.recv(),
        )
        .await;
        ensure!(
            timer_result.is_err(),
            "Expected no timer event, but one was received"
        );
        Ok(())
    }

    /// Verify a message event matches expected key and payload
    fn verify_message_event(
        &self,
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
    fn verify_timer_event(&self, event: &TimerEvent, expected_key: &str) -> Result<()> {
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
    fn verify_timer_order(&self, timers: &[TimerEvent]) -> Result<()> {
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
    fn verify_timer_keys(
        &self,
        timers: &[TimerEvent],
        expected_keys: &HashSet<String>,
    ) -> Result<()> {
        let actual_keys: HashSet<String> = timers.iter().map(|t| t.key.clone()).collect();
        ensure!(
            actual_keys == *expected_keys,
            "Timer keys mismatch: expected {:?}, got {:?}",
            expected_keys,
            actual_keys
        );
        Ok(())
    }
}

impl TestEnvironment {
    /// Clean up resources (consumer shutdown, topic deletion)
    async fn cleanup(self) -> Result<()> {
        // Shutdown consumer first
        self.consumer.shutdown().await;

        // Then delete the topic
        if let Err(e) = self.admin_client.delete_topic(&self.topic).await {
            error!("Failed to clean up topic {}: {}", self.topic, e);
        }

        Ok(())
    }
}

/// Run a test with timeout and proper error handling
async fn run_test<F, Fut>(test_name: &str, timeout_secs: u64, test_fn: F) -> Result<()>
where
    F: FnOnce(TestEnvironment) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let _ = fmt().compact().try_init();

    let result = timeout(Duration::from_secs(timeout_secs), async {
        let env = TestEnvironment::new(test_name).await?;
        let test_result = test_fn(env).await;
        test_result
    })
    .await;

    result.map_err(|_| {
        eyre!(
            "Test '{}' timed out after {} seconds",
            test_name,
            timeout_secs
        )
    })?
}

/// Tests basic timer scheduling and triggering functionality.
#[tokio::test]
async fn test_timer_scheduling_and_triggering() -> Result<()> {
    run_test("timer-test", 8, |mut env| async move {
        let key = "test-key";
        let delay_secs = 1u32;
        let schedule_message = json!({
            "action": "schedule_timer",
            "delay_secs": delay_secs
        });

        // Schedule the timer
        env.schedule_timer(key, delay_secs).await?;

        // Verify message was received
        let message_event = env.expect_message(5).await?;
        env.verify_message_event(&message_event, key, &schedule_message)?;

        // Wait for timer to trigger
        let timer_event = env.expect_timer(5).await?;
        env.verify_timer_event(&timer_event, key)?;

        // Clean up
        env.cleanup().await?;
        Ok(())
    })
    .await
}

/// Tests edge case: scheduling multiple timers for the same key.
#[tokio::test]
async fn test_same_key_multiple_timers() -> Result<()> {
    run_test("timer-same-key-test", 10, |mut env| async move {
        let key = "same-key";
        let delays = vec![1u32, 2u32, 3u32];

        // Schedule multiple timers for the same key
        for delay_secs in &delays {
            env.schedule_timer(key, *delay_secs).await?;
        }

        // Verify all schedule messages were received
        for i in 0..delays.len() {
            env.expect_message(3)
                .await
                .map_err(|e| eyre!("Failed to receive schedule message {}: {}", i + 1, e))?;
        }

        // Wait for all timer events
        let received_timers = env.expect_timers(delays.len(), 6).await?;

        // Verify all timers are for the same key
        for timer_event in &received_timers {
            env.verify_timer_event(timer_event, key)?;
        }

        // Verify timers triggered in chronological order
        env.verify_timer_order(&received_timers)?;

        // Clean up
        env.cleanup().await?;
        Ok(())
    })
    .await
}

/// Tests immediate timer scheduling (1 second delay).
#[tokio::test]
async fn test_immediate_timer() -> Result<()> {
    run_test("timer-immediate-test", 5, |mut env| async move {
        let key = "immediate-key";
        let delay_secs = 1u32;

        let start_time = CompactDateTime::now()?;

        // Schedule immediate timer
        env.schedule_timer(key, delay_secs).await?;

        // Verify message was received
        env.expect_message(3).await?;

        // Wait for timer to trigger
        let timer_event = env.expect_timer(3).await?;
        env.verify_timer_event(&timer_event, key)?;

        // Verify timing accuracy (should trigger within 1-2 seconds)
        let end_time = CompactDateTime::now()?;
        let elapsed = end_time.epoch_seconds() - start_time.epoch_seconds();
        ensure!(
            (1..=3).contains(&elapsed),
            "Timer took {} seconds, expected 1-3 seconds",
            elapsed
        );

        // Clean up
        env.cleanup().await?;
        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_timer_scheduled_time_accuracy() -> Result<()> {
    run_test("timer-accuracy-test", 10, |mut env| async move {
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
        let message_event = env.expect_message(5).await?;
        env.verify_message_event(&message_event, key, &schedule_message)?;

        // Wait for timer to trigger
        let timer_event = env.expect_timer(6).await?;
        env.verify_timer_event(&timer_event, key)?;

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
        env.cleanup().await?;
        Ok(())
    })
    .await
}

/// Tests timer cancellation functionality.
#[tokio::test]
async fn test_timer_cancellation() -> Result<()> {
    run_test("timer-cancellation-test", 10, |mut env| async move {
        let key = "cancellation-key";
        let delay_secs = 3u32;

        // Schedule a timer
        env.schedule_timer(key, delay_secs).await?;

        // Verify schedule message was received
        let message_event = env.expect_message(5).await?;
        let expected_schedule_message = json!({
            "action": "schedule_timer",
            "delay_secs": delay_secs
        });
        env.verify_message_event(&message_event, key, &expected_schedule_message)?;

        // Cancel the timer
        env.cancel_timer(key).await?;

        // Verify cancellation message was received
        let cancel_message_event = env.expect_message(5).await?;
        let expected_cancel_message = json!({
            "action": "cancel_timer"
        });
        env.verify_message_event(&cancel_message_event, key, &expected_cancel_message)?;

        // Verify no timer fires (wait longer than the original delay)
        env.expect_no_timer(delay_secs + 2).await?;

        // Clean up
        env.cleanup().await?;
        Ok(())
    })
    .await
}

/// Tests multiple timers with different keys and timing.
#[tokio::test]
async fn test_multiple_timers() -> Result<()> {
    run_test("timer-multiple-test", 15, |mut env| async move {
        // Schedule multiple timers with staggered delays
        let timers_data = vec![("key1", 1u32), ("key2", 2u32), ("key3", 3u32)];

        for (key, delay_secs) in &timers_data {
            env.schedule_timer(key, *delay_secs).await?;
        }

        // Verify all schedule messages were received
        for i in 0..timers_data.len() {
            env.expect_message(5)
                .await
                .map_err(|e| eyre!("Failed to receive schedule message {}: {}", i, e))?;
        }

        // Collect timer events as they trigger
        let received_timers = env.expect_timers(timers_data.len(), 8).await?;

        // Verify timers are in chronological order
        env.verify_timer_order(&received_timers)?;

        // Verify all expected keys are present
        let expected_keys: HashSet<String> =
            timers_data.iter().map(|(k, _)| k.to_string()).collect();
        env.verify_timer_keys(&received_timers, &expected_keys)?;

        // Log all received timer events
        for timer_event in &received_timers {
            info!(
                "Timer for key {} triggered with scheduled time: {}",
                timer_event.key, timer_event.time
            );
        }

        // Clean up
        env.cleanup().await?;
        Ok(())
    })
    .await
}

/// Tests timer behavior for different keys.
#[tokio::test]
async fn test_timer_different_keys() -> Result<()> {
    run_test("timer-keys-test", 10, |mut env| async move {
        // Schedule timers for different keys
        let timers = vec!["key-a", "key-b"];
        let delay_secs = 2u32;

        for key in &timers {
            env.schedule_timer(key, delay_secs).await?;
        }

        // Verify schedule messages were received
        for i in 0..timers.len() {
            env.expect_message(5)
                .await
                .map_err(|e| eyre!("Failed to receive schedule message {}: {}", i, e))?;
        }

        // Wait for timers to trigger
        let received_timers = env.expect_timers(2, 6).await?;

        // Verify timers are in chronological order
        env.verify_timer_order(&received_timers)?;

        // Verify both expected keys are present
        let expected_keys: HashSet<String> = timers.iter().map(|k| k.to_string()).collect();
        env.verify_timer_keys(&received_timers, &expected_keys)?;

        // Verify each timer has correct key
        for timer_event in &received_timers {
            env.verify_timer_event(timer_event, &timer_event.key)?;
        }

        // Clean up
        env.cleanup().await?;
        Ok(())
    })
    .await
}
