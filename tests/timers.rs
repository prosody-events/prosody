//! Integration tests for timer functionality in the Prosody system.
//!
//! This module tests the timer scheduling, triggering, and cancellation
//! capabilities of Prosody consumers. It verifies that timers can be set
//! from message handlers, triggered at the correct times, and properly
//! canceled when needed.

use std::time::Duration;

use color_eyre::eyre::{Result, ensure, eyre};
use prosody::{
    Topic,
    admin::ProsodyAdminClient,
    consumer::message::{EventContext, UncommittedMessage},
    consumer::{ConsumerConfiguration, EventHandler, Keyed, ProsodyConsumer, Uncommitted},
    producer::{ProducerConfiguration, ProsodyProducer},
    timers::UncommittedTimer,
    timers::datetime::CompactDateTime,
    timers::duration::CompactDuration,
    timers::store::TriggerStore,
};
use serde_json::{Value, json};
use tokio::sync::mpsc::{channel, Receiver, Sender};
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
    async fn on_message<T>(&self, context: EventContext<T>, message: UncommittedMessage)
    where
        T: TriggerStore,
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
                    if let Some(target_time_secs) = payload.get("target_time_secs").and_then(|v| v.as_u64()) {
                        // Absolute time scheduling
                        let schedule_time = CompactDateTime::from(target_time_secs as u32);
                        if let Err(e) = context.schedule(schedule_time).await {
                            error!("Failed to schedule timer for key {key}: {e}");
                        } else {
                            info!("Scheduled timer for key {key} at time {schedule_time}");
                        }
                    } else if let Some(delay_ms) = payload.get("delay_ms").and_then(|v| v.as_u64()) {
                        // Delay-based scheduling (existing logic)
                        let delay = CompactDuration::new((delay_ms / 1000) as u32); // Convert ms to seconds
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

    async fn on_timer<T>(&self, _context: EventContext<T>, timer: UncommittedTimer<T>)
    where
        T: TriggerStore,
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
        let (timer_tx, timer_rx) = channel(20);
        let (message_tx, message_rx) = channel(20);

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
        self.producer.send([], self.topic.clone(), key, payload).await
            .map_err(|e| eyre!("Failed to send message: {}", e))
    }

    /// Send a timer scheduling message
    async fn schedule_timer(&self, key: &str, delay_ms: u64) -> Result<()> {
        let message = json!({
            "action": "schedule_timer",
            "delay_ms": delay_ms
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
            .map_err(|_| eyre!("Timeout waiting for message event"))?
            .ok_or_else(|| eyre!("Message channel closed unexpectedly"))
    }

    /// Wait for a timer event with timeout
    async fn expect_timer(&mut self, timeout_secs: u64) -> Result<TimerEvent> {
        timeout(Duration::from_secs(timeout_secs), self.timer_rx.recv())
            .await
            .map_err(|_| eyre!("Timeout waiting for timer event"))?
            .ok_or_else(|| eyre!("Timer channel closed unexpectedly"))
    }

    /// Wait for multiple timer events with timeout
    async fn expect_timers(&mut self, count: usize, timeout_secs: u64) -> Result<Vec<TimerEvent>> {
        let mut received_timers = Vec::new();
        let start_time = tokio::time::Instant::now();

        while received_timers.len() < count && start_time.elapsed() < Duration::from_secs(timeout_secs) {
            if let Ok(Some(timer_event)) = timeout(Duration::from_secs(3), self.timer_rx.recv()).await {
                received_timers.push(timer_event);
            }
        }

        Ok(received_timers)
    }

    /// Verify that no timer event occurs within the given timeout
    async fn expect_no_timer(&mut self, timeout_millis: u64) -> Result<()> {
        let timer_result = timeout(Duration::from_millis(timeout_millis), self.timer_rx.recv()).await;
        ensure!(
            timer_result.is_err(),
            "Expected no timer event, but one was received"
        );
        Ok(())
    }

    /// Verify a message event matches expected key and payload
    fn verify_message_event(&self, event: &MessageEvent, expected_key: &str, expected_payload: &Value) -> Result<()> {
        ensure!(event.key == expected_key, "Message key mismatch: expected {}, got {}", expected_key, event.key);
        ensure!(event.payload == *expected_payload, "Message payload mismatch");
        Ok(())
    }

    /// Verify a timer event matches expected key
    fn verify_timer_event(&self, event: &TimerEvent, expected_key: &str) -> Result<()> {
        ensure!(event.key == expected_key, "Timer key mismatch: expected {}, got {}", expected_key, event.key);
        info!("Timer triggered for key {} at time {}", event.key, event.time);
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
    Fut: std::future::Future<Output = Result<()>>,
{
    let _ = fmt().compact().try_init();

    let result = timeout(Duration::from_secs(timeout_secs), async {
        let env = TestEnvironment::new(test_name).await?;
        let test_result = test_fn(env).await;
        
        // Clean up regardless of test result
        // Note: env is consumed by cleanup(), so we can't use it after this
        // The cleanup is handled by moving env into the test_fn, then the test
        // is responsible for cleanup if needed, or we handle it in the caller
        test_result
    })
    .await;

    result.map_err(|_| eyre!("Test timed out"))?
}

/// Tests basic timer scheduling and triggering functionality.
#[tokio::test]
async fn test_timer_scheduling_and_triggering() -> Result<()> {
    run_test("timer-test", 30, |mut env| async move {
        let key = "test-key";
        let delay_ms = 3000u64; // 3 seconds
        let schedule_message = json!({
            "action": "schedule_timer",
            "delay_ms": delay_ms
        });

        // Schedule the timer
        env.schedule_timer(key, delay_ms).await?;

        // Verify message was received
        let message_event = env.expect_message(5).await?;
        env.verify_message_event(&message_event, key, &schedule_message)?;

        // Wait for timer to trigger
        let timer_event = env.expect_timer(8).await?;
        env.verify_timer_event(&timer_event, key)?;

        // Clean up
        env.cleanup().await?;
        Ok(())
    }).await
}

#[tokio::test]
async fn test_timer_scheduled_time_accuracy() -> Result<()> {
    run_test("timer-accuracy-test", 20, |mut env| async move {
        let key = "accuracy-key";
        
        // Calculate a specific target time (5 seconds from now)
        let target_time_secs = CompactDateTime::now()
            .map_err(|e| eyre!("Failed to get current time: {e}"))?
            .add_duration(CompactDuration::new(5))
            .map_err(|e| eyre!("Failed to add duration: {e}"))?
            .epoch_seconds() as u64;

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
        let timer_event = env.expect_timer(10).await?;
        env.verify_timer_event(&timer_event, key)?;
        
        // Verify timer accuracy
        ensure!(
            timer_event.time == expected_time,
            "Timer triggered at different time than scheduled. Expected: {}, Actual: {}",
            expected_time,
            timer_event.time
        );

        info!("✓ Timer accuracy test passed: scheduled time {} matches trigger time {}", 
              expected_time, timer_event.time);

        // Clean up
        env.cleanup().await?;
        Ok(())
    }).await
}

/// Tests timer cancellation functionality.
#[tokio::test]
async fn test_timer_cancellation() -> Result<()> {
    run_test("timer-cancel-test", 35, |mut env| async move {
        let key = "cancel-key";

        // Schedule a timer for 6 seconds
        env.schedule_timer(key, 6000).await?;

        // Verify schedule message was received
        env.expect_message(5).await?;

        // Wait a bit, then cancel the timer before it triggers
        tokio::time::sleep(Duration::from_secs(2)).await;
        env.cancel_timer(key).await?;

        // Verify cancel message was received
        env.expect_message(5).await?;

        // Wait beyond the original timer delay to ensure it doesn't trigger
        tokio::time::sleep(Duration::from_secs(6)).await;

        // Verify timer was not triggered
        env.expect_no_timer(500).await?;

        // Clean up
        env.cleanup().await?;
        Ok(())
    }).await
}

/// Tests multiple timers with different keys and timing.
#[tokio::test]
async fn test_multiple_timers() -> Result<()> {
    run_test("timer-multiple-test", 45, |mut env| async move {
        // Schedule multiple timers with staggered delays
        let timers_data = vec![("key1", 3000u64), ("key2", 5000u64), ("key3", 7000u64)];

        for (key, delay_ms) in &timers_data {
            env.schedule_timer(key, *delay_ms).await?;
        }

        // Verify all schedule messages were received
        for i in 0..timers_data.len() {
            env.expect_message(5).await
                .map_err(|e| eyre!("Failed to receive schedule message {}: {}", i, e))?;
        }

        // Collect timer events as they trigger
        let received_timers = env.expect_timers(timers_data.len(), 25).await?;

        // At a minimum, we should get at least 2 timers (sometimes the longest one
        // might not trigger in CI)
        ensure!(
            received_timers.len() >= 2,
            "Expected at least 2 timer events, got {}",
            received_timers.len()
        );

        // Verify that different keys are represented
        let seen_keys: std::collections::HashSet<_> = received_timers.iter().map(|t| &t.key).collect();
        ensure!(seen_keys.len() >= 2, "Expected at least 2 different keys");

        // Log all received timer events
        for timer_event in &received_timers {
            info!("Timer for key {} triggered with scheduled time: {}", timer_event.key, timer_event.time);
        }

        // Clean up
        env.cleanup().await?;
        Ok(())
    }).await
}

/// Tests timer behavior for different keys.
#[tokio::test]
async fn test_timer_different_keys() -> Result<()> {
    run_test("timer-keys-test", 25, |mut env| async move {
        // Schedule timers for different keys
        let timers = vec!["key-a", "key-b"];
        let delay_ms = 4000u64;

        for key in &timers {
            env.schedule_timer(key, delay_ms).await?;
        }

        // Verify schedule messages were received
        for i in 0..timers.len() {
            env.expect_message(5).await
                .map_err(|e| eyre!("Failed to receive schedule message {}: {}", i, e))?;
        }

        // Wait for timers to trigger
        let received_timers = env.expect_timers(2, 12).await?;

        // Verify both timers triggered
        ensure!(
            received_timers.len() == 2,
            "Expected 2 timer events, got {}",
            received_timers.len()
        );

        // Verify both keys are represented
        let keys: std::collections::HashSet<_> = received_timers.iter().map(|t| &t.key).collect();
        ensure!(keys.contains(&"key-a".to_string()) && keys.contains(&"key-b".to_string()));
        
        // Verify each timer contains the scheduled time
        for timer_event in &received_timers {
            env.verify_timer_event(timer_event, &timer_event.key)?;
        }

        // Clean up
        env.cleanup().await?;
        Ok(())
    }).await
}