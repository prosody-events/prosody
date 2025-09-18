//! Integration test for `EventContext` invalidation.
//!
//! This test verifies that cloned contexts cannot be used after the handler
//! method completes. This prevents race conditions and data corruption when
//! partition ownership changes.

use color_eyre::eyre::{Result, eyre};
use prosody::{
    Topic,
    admin::ProsodyAdminClient,
    consumer::event_context::{BoxEventContext, EventContext},
    consumer::message::UncommittedMessage,
    consumer::{ConsumerConfiguration, EventHandler, ProsodyConsumer, Uncommitted},
    producer::{ProducerConfiguration, ProsodyProducer},
    timers::{UncommittedTimer, datetime::CompactDateTime, duration::CompactDuration},
};
use serde_json::json;
use tokio::sync::mpsc::{Sender, channel};
use tokio::task::yield_now;
use tracing::info;
use uuid::Uuid;

mod common;

/// Test handler that clones contexts during processing and sends them for later
/// testing.
#[derive(Clone)]
struct ContextInvalidationHandler {
    /// Channel to send cloned contexts for later testing
    context_tx: Sender<BoxEventContext>,
}

impl ContextInvalidationHandler {
    fn new(context_tx: Sender<BoxEventContext>) -> Self {
        Self { context_tx }
    }
}

impl EventHandler for ContextInvalidationHandler {
    async fn on_message<C>(&self, context: C, message: UncommittedMessage)
    where
        C: EventContext,
    {
        info!("Processing message in handler");

        // Clone the context during processing (simulating what might happen in language
        // bindings)
        let cloned_context = context.clone().boxed();

        // Send the cloned context for testing after handler completion
        let _ = self.context_tx.send(cloned_context).await;

        // Commit the message
        message.commit().await;

        info!("Handler completing - context should be invalidated after this");
        // When this method returns, the context should be invalidated
    }

    async fn on_timer<C, U>(&self, _context: C, _timer: U)
    where
        C: EventContext,
        U: UncommittedTimer,
    {
        // Not used in this test
    }

    async fn shutdown(self) {
        info!("ContextInvalidationHandler shutdown");
    }
}

/// Tests that cloned contexts become invalid after the handler method
/// completes.
///
/// This test simulates the scenario where contexts are cloned during message
/// processing (as would happen in language bindings) and then attempts to use
/// those cloned contexts after the handler has finished executing.
///
/// # Errors
///
/// Returns an error if the test setup fails or if the invalidation behavior
/// doesn't work as expected.
#[tokio::test]
async fn test_context_invalidation_prevents_cloned_usage() -> Result<()> {
    // Initialize logging
    common::init_test_logging()?;

    // Create a unique topic for the test
    let topic: Topic = Uuid::new_v4().to_string().as_str().into();
    let bootstrap: Vec<String> = vec!["localhost:9094".to_owned()];

    // Setup admin client and create topic
    let admin_client = ProsodyAdminClient::new(&bootstrap)?;
    admin_client.create_topic(&topic, 1, 1).await?;

    info!("Created test topic: {topic}");

    // Create a channel to receive cloned contexts from the handler
    let (context_tx, mut context_rx) = channel(1);

    // Create our test handler
    let handler = ContextInvalidationHandler::new(context_tx);

    // Configure consumer
    let consumer_config = ConsumerConfiguration::builder()
        .bootstrap_servers(bootstrap.clone())
        .group_id(Uuid::new_v4().to_string().as_str())
        .probe_port(None)
        .subscribed_topics(&[topic.to_string()])
        .build()?;

    // Create consumer
    let consumer = ProsodyConsumer::new::<ContextInvalidationHandler>(
        &consumer_config,
        &common::create_cassandra_trigger_store_config(),
        handler,
    )
    .await?;

    // Configure and create producer
    let producer_config = ProducerConfiguration::builder()
        .bootstrap_servers(bootstrap)
        .source_system("test-producer")
        .build()?;

    let producer = ProsodyProducer::new(&producer_config)?;

    // Send a test message
    let test_payload = json!({ "test": "context_invalidation" });
    producer.send([], topic, "test-key", &test_payload).await?;

    info!("Sent test message, waiting for cloned context...");

    // Wait for the handler to send us the cloned context
    let cloned_context = context_rx
        .recv()
        .await
        .ok_or_else(|| eyre!("Handler did not send cloned context"))?;

    info!("Received cloned context, handler should have completed and invalidated context");

    // Yield to allow any cleanup/invalidation to complete
    yield_now().await;

    // Now try to use the cloned context - this should fail with InvalidContext
    let future_time = CompactDateTime::now()?.add_duration(CompactDuration::new(60))?;

    match cloned_context.schedule(future_time).await {
        Ok(()) => {
            return Err(eyre!(
                "UNEXPECTED: Cloned context usage succeeded when it should have failed"
            ));
        }
        Err(error) => {
            let error_string = format!("{error}");
            if error_string.contains("no longer valid") || error_string.contains("InvalidContext") {
                info!("SUCCESS: Cloned context correctly returned InvalidContext error");
            } else {
                return Err(eyre!("Expected InvalidContext error, got: {error}"));
            }
        }
    }

    // Clean up
    consumer.shutdown().await;
    admin_client.delete_topic(&topic).await?;

    info!("Test completed successfully");
    Ok(())
}
