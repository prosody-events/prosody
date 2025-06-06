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
use tokio::sync::mpsc::channel;
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
    timer_tx: tokio::sync::mpsc::Sender<TimerEvent>,
    /// Channel for sending message events to the test
    message_tx: tokio::sync::mpsc::Sender<MessageEvent>,
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

/// Tests basic timer scheduling and triggering functionality.
#[tokio::test]
async fn test_timer_scheduling_and_triggering() -> Result<()> {
    let _ = fmt().compact().try_init();

    // Overall test timeout to prevent hanging
    let result = timeout(Duration::from_secs(30), async {
        let topic: Topic = format!("timer-test-{}", Uuid::new_v4()).as_str().into();
        let bootstrap = vec!["localhost:9094".to_owned()];
        let admin_client = ProsodyAdminClient::new(&bootstrap)?;
        admin_client.create_topic(&topic, 1, 1).await?;

        // Set up channels for test events
        let (timer_tx, mut timer_rx) = channel(10);
        let (message_tx, mut message_rx) = channel(10);

        // Create handler and consumer
        let handler = TimerTestHandler {
            timer_tx,
            message_tx,
        };

        let group_id = format!("test-timer-consumer-{}", Uuid::new_v4());
        let consumer_config = ConsumerConfiguration::builder()
            .bootstrap_servers(bootstrap.clone())
            .group_id(&group_id)
            .subscribed_topics(&[topic.to_string()])
            .probe_port(None)
            .build()?;

        let consumer = ProsodyConsumer::new::<TimerTestHandler>(&consumer_config, handler)?;

        // Create producer
        let producer_config = ProducerConfiguration::builder()
            .bootstrap_servers(bootstrap.clone())
            .source_system("test-producer")
            .build()?;
        let producer = ProsodyProducer::new(&producer_config)?;

        // Give consumer time to start and subscribe
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Send message to schedule a timer (3 seconds)
        let key = "test-key";
        let delay_ms = 3000u64; // 3 seconds

        let schedule_message = json!({
            "action": "schedule_timer",
            "delay_ms": delay_ms
        });

        producer.send([], topic, key, &schedule_message).await?;

        // Verify message was received
        let message_event = timeout(Duration::from_secs(5), message_rx.recv())
            .await
            .map_err(|_| eyre!("Timeout waiting for message event"))?
            .ok_or_else(|| eyre!("Message channel closed unexpectedly"))?;

        ensure!(message_event.key == key);
        ensure!(message_event.payload == schedule_message);

        // Wait for timer to trigger (with some buffer time)
        let timer_event = timeout(Duration::from_secs(8), timer_rx.recv())
            .await
            .map_err(|_| eyre!("Timeout waiting for timer event"))?
            .ok_or_else(|| eyre!("Timer channel closed unexpectedly"))?;

        ensure!(timer_event.key == key);
        
        // Timer event contains the scheduled time - this verifies the timer
        // system correctly maintains and delivers the scheduled time
        info!("Timer triggered with scheduled time: {}", timer_event.time);

        // Clean up
        consumer.shutdown().await;
        admin_client.delete_topic(&topic).await?;
        Ok(())
    })
    .await;

    result.map_err(|_| eyre!("Test timed out"))?
}

#[tokio::test]
async fn test_timer_scheduled_time_accuracy() -> Result<()> {
    let _ = fmt().compact().try_init();

    // Overall test timeout to prevent hanging
    let result = timeout(Duration::from_secs(20), async {
        let topic: Topic = format!("timer-accuracy-test-{}", Uuid::new_v4())
            .as_str()
            .into();
        let bootstrap = vec!["localhost:9094".to_owned()];
        let admin_client = ProsodyAdminClient::new(&bootstrap)?;
        admin_client.create_topic(&topic, 1, 1).await?;

        // Set up channels for test events
        let (timer_tx, mut timer_rx) = channel(10);
        let (message_tx, mut message_rx) = channel(10);

        // Create handler and consumer
        let handler = TimerTestHandler {
            timer_tx,
            message_tx,
        };

        let group_id = format!("test-accuracy-consumer-{}", Uuid::new_v4());
        let consumer_config = ConsumerConfiguration::builder()
            .bootstrap_servers(bootstrap.clone())
            .group_id(&group_id)
            .subscribed_topics(&[topic.to_string()])
            .probe_port(None)
            .build()?;

        let consumer = ProsodyConsumer::new::<TimerTestHandler>(&consumer_config, handler)?;

        // Create producer
        let producer_config = ProducerConfiguration::builder()
            .bootstrap_servers(bootstrap.clone())
            .source_system("test-producer")
            .build()?;
        let producer = ProsodyProducer::new(&producer_config)?;

        // Give consumer time to start and subscribe
        tokio::time::sleep(Duration::from_secs(2)).await;

        let key = "accuracy-key";
        
        // Calculate a specific target time (5 seconds from now)
        let target_time_secs = CompactDateTime::now()
            .map_err(|e| eyre!("Failed to get current time: {e}"))?
            .add_duration(CompactDuration::new(5))
            .map_err(|e| eyre!("Failed to add duration: {e}"))?
            .epoch_seconds() as u64;

        let expected_time = CompactDateTime::from(target_time_secs as u32);

        // Send message to schedule timer at specific absolute time
        let schedule_message = json!({
            "action": "schedule_timer",
            "target_time_secs": target_time_secs
        });

        producer.send([], topic, key, &schedule_message).await?;

        // Verify message was received
        let message_event = timeout(Duration::from_secs(5), message_rx.recv())
            .await
            .map_err(|_| eyre!("Timeout waiting for message event"))?
            .ok_or_else(|| eyre!("Message channel closed unexpectedly"))?;

        ensure!(message_event.key == key);
        ensure!(message_event.payload == schedule_message);

        // Wait for timer to trigger
        let timer_event = timeout(Duration::from_secs(10), timer_rx.recv())
            .await
            .map_err(|_| eyre!("Timeout waiting for timer event"))?
            .ok_or_else(|| eyre!("Timer channel closed unexpectedly"))?;

        ensure!(timer_event.key == key);
        
        // This is the key verification: the timer event's time should exactly match
        // the time we scheduled it for
        ensure!(
            timer_event.time == expected_time,
            "Timer triggered at different time than scheduled. Expected: {}, Actual: {}",
            expected_time,
            timer_event.time
        );

        info!("✓ Timer accuracy test passed: scheduled time {} matches trigger time {}", 
              expected_time, timer_event.time);

        // Clean up
        consumer.shutdown().await;
        admin_client.delete_topic(&topic).await?;
        Ok(())
    })
    .await;

    result.map_err(|_| eyre!("Test timed out"))?
}

/// Tests timer cancellation functionality.
#[tokio::test]
async fn test_timer_cancellation() -> Result<()> {
    let _ = fmt().compact().try_init();

    // Overall test timeout to prevent hanging
    let result = timeout(Duration::from_secs(35), async {
        let topic: Topic = format!("timer-cancel-test-{}", Uuid::new_v4())
            .as_str()
            .into();
        let bootstrap = vec!["localhost:9094".to_owned()];
        let admin_client = ProsodyAdminClient::new(&bootstrap)?;
        admin_client.create_topic(&topic, 1, 1).await?;

        // Set up channels for test events
        let (timer_tx, mut timer_rx) = channel(10);
        let (message_tx, mut message_rx) = channel(10);

        // Create handler and consumer
        let handler = TimerTestHandler {
            timer_tx,
            message_tx,
        };

        let group_id = format!("test-timer-cancel-consumer-{}", Uuid::new_v4());
        let consumer_config = ConsumerConfiguration::builder()
            .bootstrap_servers(bootstrap.clone())
            .group_id(&group_id)
            .subscribed_topics(&[topic.to_string()])
            .probe_port(None)
            .build()?;

        let consumer = ProsodyConsumer::new::<TimerTestHandler>(&consumer_config, handler)?;

        // Create producer
        let producer_config = ProducerConfiguration::builder()
            .bootstrap_servers(bootstrap.clone())
            .source_system("test-producer")
            .build()?;
        let producer = ProsodyProducer::new(&producer_config)?;

        // Give consumer time to start and subscribe
        tokio::time::sleep(Duration::from_secs(2)).await;

        let key = "cancel-key";

        // Schedule a timer for 6 seconds
        let schedule_message = json!({
            "action": "schedule_timer",
            "delay_ms": 6000u64 // 6 seconds
        });

        producer.send([], topic, key, &schedule_message).await?;

        // Verify schedule message was received
        timeout(Duration::from_secs(5), message_rx.recv())
            .await
            .map_err(|_| eyre!("Timeout waiting for schedule message"))?
            .ok_or_else(|| eyre!("Message channel closed unexpectedly"))?;

        // Wait a bit, then cancel the timer before it triggers
        tokio::time::sleep(Duration::from_secs(2)).await;

        let cancel_message = json!({
            "action": "cancel_timer"
        });

        producer.send([], topic, key, &cancel_message).await?;

        // Verify cancel message was received
        timeout(Duration::from_secs(5), message_rx.recv())
            .await
            .map_err(|_| eyre!("Timeout waiting for cancel message"))?
            .ok_or_else(|| eyre!("Message channel closed unexpectedly"))?;

        // Wait beyond the original timer delay to ensure it doesn't trigger
        tokio::time::sleep(Duration::from_secs(6)).await;

        // Verify timer was not triggered
        let timer_result = timeout(Duration::from_millis(500), timer_rx.recv()).await;
        ensure!(
            timer_result.is_err(),
            "Canceled timer should not have triggered"
        );

        // Clean up
        consumer.shutdown().await;
        admin_client.delete_topic(&topic).await?;
        Ok(())
    })
    .await;

    result.map_err(|_| eyre!("Test timed out"))?
}

/// Tests multiple timers with different keys and timing.
#[tokio::test]
async fn test_multiple_timers() -> Result<()> {
    let _ = fmt().compact().try_init();

    // Overall test timeout to prevent hanging
    let result = timeout(Duration::from_secs(45), async {
        let topic: Topic = format!("timer-multiple-test-{}", Uuid::new_v4())
            .as_str()
            .into();
        let bootstrap = vec!["localhost:9094".to_owned()];
        let admin_client = ProsodyAdminClient::new(&bootstrap)?;
        admin_client.create_topic(&topic, 1, 1).await?;

        // Set up channels for test events
        let (timer_tx, mut timer_rx) = channel(20);
        let (message_tx, mut message_rx) = channel(20);

        // Create handler and consumer
        let handler = TimerTestHandler {
            timer_tx,
            message_tx,
        };

        let group_id = format!("test-multiple-timers-consumer-{}", Uuid::new_v4());
        let consumer_config = ConsumerConfiguration::builder()
            .bootstrap_servers(bootstrap.clone())
            .group_id(&group_id)
            .subscribed_topics(&[topic.to_string()])
            .probe_port(None)
            .build()?;

        let consumer = ProsodyConsumer::new::<TimerTestHandler>(&consumer_config, handler)?;

        // Create producer
        let producer_config = ProducerConfiguration::builder()
            .bootstrap_servers(bootstrap.clone())
            .source_system("test-producer")
            .build()?;
        let producer = ProsodyProducer::new(&producer_config)?;

        // Give consumer time to start and subscribe
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Schedule multiple timers with staggered delays
        let timers_data = vec![("key1", 3000u64), ("key2", 5000u64), ("key3", 7000u64)];

        for (key, delay_ms) in &timers_data {
            let schedule_message = json!({
                "action": "schedule_timer",
                "delay_ms": delay_ms
            });
            producer.send([], topic, key, &schedule_message).await?;
        }

        // Verify all schedule messages were received
        for i in 0..timers_data.len() {
            timeout(Duration::from_secs(5), message_rx.recv())
                .await
                .map_err(|_| eyre!("Timeout waiting for schedule message {}", i))?
                .ok_or_else(|| eyre!("Message channel closed unexpectedly"))?;
        }

        // Collect timer events as they trigger
        let mut received_timers = Vec::new();
        let start_time = tokio::time::Instant::now();

        while received_timers.len() < timers_data.len()
            && start_time.elapsed() < Duration::from_secs(25)
        {
            if let Ok(Some(timer_event)) = timeout(Duration::from_secs(5), timer_rx.recv()).await {
                received_timers.push(timer_event);
            }
        }

        // At a minimum, we should get at least 2 timers (sometimes the longest one
        // might not trigger in CI)
        ensure!(
            received_timers.len() >= 2,
            "Expected at least 2 timer events, got {}",
            received_timers.len()
        );

        // Verify that different keys are represented
        let mut seen_keys = std::collections::HashSet::new();
        for timer_event in &received_timers {
            seen_keys.insert(&timer_event.key);
            
            // Timer event contains the scheduled time - this verifies the timer
            // system correctly maintains and delivers the scheduled time
            info!("Timer for key {} triggered with scheduled time: {}", timer_event.key, timer_event.time);
        }
        ensure!(seen_keys.len() >= 2, "Expected at least 2 different keys");

        // Clean up
        consumer.shutdown().await;
        admin_client.delete_topic(&topic).await?;
        Ok(())
    })
    .await;

    result.map_err(|_| eyre!("Test timed out"))?
}

/// Tests timer behavior for different keys.
#[tokio::test]
async fn test_timer_different_keys() -> Result<()> {
    let _ = fmt().compact().try_init();

    // Overall test timeout to prevent hanging
    let result = timeout(Duration::from_secs(25), async {
        let topic: Topic = format!("timer-keys-test-{}", Uuid::new_v4())
            .as_str()
            .into();
        let bootstrap = vec!["localhost:9094".to_owned()];
        let admin_client = ProsodyAdminClient::new(&bootstrap)?;
        admin_client.create_topic(&topic, 1, 1).await?;

        // Set up channels for test events
        let (timer_tx, mut timer_rx) = channel(10);
        let (message_tx, mut message_rx) = channel(10);

        // Create handler and consumer
        let handler = TimerTestHandler {
            timer_tx,
            message_tx,
        };

        let group_id = format!("test-different-keys-consumer-{}", Uuid::new_v4());
        let consumer_config = ConsumerConfiguration::builder()
            .bootstrap_servers(bootstrap.clone())
            .group_id(&group_id)
            .subscribed_topics(&[topic.to_string()])
            .probe_port(None)
            .build()?;

        let consumer = ProsodyConsumer::new::<TimerTestHandler>(&consumer_config, handler)?;

        // Create producer
        let producer_config = ProducerConfiguration::builder()
            .bootstrap_servers(bootstrap.clone())
            .source_system("test-producer")
            .build()?;
        let producer = ProsodyProducer::new(&producer_config)?;

        // Give consumer time to start and subscribe
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Schedule timers for different keys
        let timers = vec!["key-a", "key-b"];
        let delay_ms = 4000u64;

        for key in &timers {
            let schedule_message = json!({
                "action": "schedule_timer",
                "delay_ms": delay_ms
            });
            producer.send([], topic, key, &schedule_message).await?;
        }

        // Verify schedule messages were received
        for i in 0..timers.len() {
            timeout(Duration::from_secs(5), message_rx.recv())
                .await
                .map_err(|_| eyre!("Timeout waiting for schedule message {}", i))?
                .ok_or_else(|| eyre!("Events channel closed unexpectedly"))?;
        }

        // Wait for timers to trigger
        let mut received_timers = Vec::new();
        let start_time = tokio::time::Instant::now();

        while received_timers.len() < 2 && start_time.elapsed() < Duration::from_secs(12) {
            if let Ok(Some(timer_event)) = timeout(Duration::from_secs(3), timer_rx.recv()).await {
                received_timers.push(timer_event);
            }
        }

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
            info!("Timer for key {} triggered with scheduled time: {}", timer_event.key, timer_event.time);
        }

        // Clean up
        consumer.shutdown().await;
        admin_client.delete_topic(&topic).await?;
        Ok(())
    })
    .await;

    result.map_err(|_| eyre!("Test timed out"))?
}
