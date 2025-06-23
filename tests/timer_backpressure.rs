//! This module tests timer backpressure in the Prosody timer system.
//!
//! It simulates a scenario where timers are scheduled at a high rate from
//! messages while the consumer processes them slowly, demonstrating the
//! backpressure handling capabilities of the timer system.

use color_eyre::eyre::Result;
use prosody::{
    Topic,
    admin::ProsodyAdminClient,
    consumer::event_context::EventContext,
    consumer::message::UncommittedMessage,
    consumer::{ConsumerConfiguration, EventHandler, Keyed, ProsodyConsumer},
    high_level::config::TriggerStoreConfiguration,
    producer::{ProducerConfiguration, ProsodyProducer},
    timers::{UncommittedTimer, datetime::CompactDateTime, duration::CompactDuration},
};
use serde_json::json;
use std::time::Duration;
use tokio::spawn;
use tokio::sync::mpsc::{Sender, channel};
use tracing::{error, info};
use tracing_subscriber::fmt;
use uuid::Uuid;

#[path = "common.rs"]
mod common;

/// A handler implementation that schedules timers from messages and simulates
/// backpressure by introducing delays in timer processing.
#[derive(Clone, Debug)]
pub struct SlowTimerHandler {
    /// A channel for transmitting received timers.
    pub timers_tx: Sender<String>,
}

impl EventHandler for SlowTimerHandler {
    async fn on_message<C>(&self, context: C, message: UncommittedMessage)
    where
        C: EventContext,
    {
        let (msg, uncommitted) = message.into_inner();
        let key = msg.key().to_string();
        let payload = msg.payload();

        // Schedule a timer based on the message
        if let Some(delay_ms) = payload
            .get("schedule_timer_delay_ms")
            .and_then(serde_json::Value::as_u64)
        {
            let delay_secs = (delay_ms / 1000).max(1) as u32; // Convert to seconds, minimum 1
            let delay = CompactDuration::new(delay_secs);
            match CompactDateTime::now().and_then(|now| now.add_duration(delay)) {
                Ok(schedule_time) => {
                    if let Err(e) = context.schedule(schedule_time).await {
                        error!("Failed to schedule timer for key {}: {e}", key);
                    }
                }
                Err(e) => {
                    error!("Failed to calculate schedule time: {e}");
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
        let timer_key = timer.key().to_string();

        // Simulate timer backpressure with a delay
        tokio::time::sleep(Duration::from_millis(100)).await;

        if let Err(e) = self.timers_tx.send(timer_key.clone()).await {
            error!("failed to send timer for key {}: {e:#}", timer_key);
        }

        timer.commit().await;
    }

    async fn shutdown(self) {
        info!("SlowTimerHandler shutdown");
    }
}

/// Demonstrates backpressure in the timer processing system by
/// sending messages that schedule timers at a high rate while the
/// consumer processes them slowly.
///
/// # Errors
///
/// Returns a `Result` error if there are issues setting up the topic,
/// producer, consumer, or timer channels.
#[tokio::test]
async fn test_timer_backpressure() -> Result<()> {
    // Initialize the logger.
    let _ = fmt().compact().try_init();

    // Create a unique topic for the test
    let topic: Topic = Uuid::new_v4().to_string().as_str().into();
    let bootstrap: Vec<String> = vec!["localhost:9094".to_owned()];

    // Setup an admin client to manage the topic creation
    let admin_client = ProsodyAdminClient::new(&bootstrap)?;
    admin_client.create_topic(&topic, 4, 1).await?;

    // Use a channel with a buffer capacity to accommodate slow timer processing
    let (timers_tx, mut timers_rx) = channel(64);

    // Configure the consumer with a slow timer handler
    let consumer_config = ConsumerConfiguration::builder()
        .bootstrap_servers(bootstrap.clone())
        .group_id(Uuid::new_v4().to_string().as_str())
        .probe_port(None)
        .subscribed_topics(&[topic.to_string()])
        .build()?;

    let slow_timer_handler = SlowTimerHandler { timers_tx };
    let consumer = ProsodyConsumer::new::<SlowTimerHandler>(
        &consumer_config,
        &TriggerStoreConfiguration::InMemory,
        slow_timer_handler,
    )
    .await?;

    // Set up the producer configuration
    let producer_config = ProducerConfiguration::builder()
        .bootstrap_servers(bootstrap.clone())
        .source_system("test-timer-producer")
        .build()?;

    let producer = ProsodyProducer::new(&producer_config)?;

    // Send messages that will schedule timers
    let total = 500u32;

    // Start sending messages that schedule timers in a separate task
    spawn(async move {
        for i in 0..total {
            // Each message schedules a timer with a small delay
            let payload = json!({
                "timer_seq": i,
                "schedule_timer_delay_ms": 50 + (i * 10) // Stagger the timer delays
            });

            if let Err(e) = producer.send([], topic, &i.to_string(), &payload).await {
                error!("Failed to send message: {e}");
            }
        }
    });

    // Counter for the number of timers processed by the consumer
    let mut count = 0_u32;
    let start_time = tokio::time::Instant::now();

    // Process timers as they are received by the slow consumer
    while timers_rx.recv().await.is_some() {
        count += 1;
        if count % 50 == 0 {
            info!("Processed {count} timers so far");
        }
        if count == total {
            break;
        }
    }

    // Log the time taken to process all timers
    let total_elapsed = start_time.elapsed();
    info!("Total timers processed: {count}");
    info!("Total processing time: {total_elapsed:?}");

    // Shutdown the consumer and clean up resources
    consumer.shutdown().await;
    admin_client.delete_topic(&topic).await?;
    Ok(())
}
