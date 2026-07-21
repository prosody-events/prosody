//! Integration test for `EventContext` invalidation.
//!
//! This test verifies that cloned contexts cannot be used after the handler
//! method completes. This prevents race conditions and data corruption when
//! partition ownership changes.

#![recursion_limit = "256"]

use color_eyre::eyre::{Result, eyre};
use prosody::tracing::init_test_logging;
use prosody::{
    consumer::event_context::{BoxEventContext, EventContext},
    consumer::message::UncommittedMessage,
    consumer::middleware::CloneProvider,
    consumer::{DemandType, EventHandler, Uncommitted},
    timers::{TimerType, UncommittedTimer, datetime::CompactDateTime, duration::CompactDuration},
};
use serde_json::{Value, json};
use tokio::sync::mpsc::{Sender, channel};
use tracing::info;

mod common;
use common::ConsumerEnv;

/// Test handler that clones contexts during processing and sends them for later
/// testing.
#[derive(Clone)]
struct ContextInvalidationHandler {
    /// Channel to send cloned contexts for later testing
    context_tx: Sender<BoxEventContext<Value>>,
}

impl ContextInvalidationHandler {
    fn new(context_tx: Sender<BoxEventContext<Value>>) -> Self {
        Self { context_tx }
    }
}

impl EventHandler for ContextInvalidationHandler {
    type Payload = Value;

    async fn on_message<C>(
        &self,
        context: C,
        message: UncommittedMessage<Value>,
        _demand_type: DemandType,
    ) where
        C: EventContext<Payload = Self::Payload>,
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

    async fn on_timer<C, U>(&self, _context: C, _timer: U, _demand_type: DemandType)
    where
        C: EventContext<Payload = Self::Payload>,
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
    init_test_logging();

    // Create a channel to receive cloned contexts from the handler
    let (context_tx, mut context_rx) = channel(1);

    // Create our test handler
    let handler = ContextInvalidationHandler::new(context_tx);

    let env = ConsumerEnv::new("context-invalidation", async move |_config| {
        Ok(CloneProvider::new(handler))
    })
    .await?;

    // Send a test message
    let test_payload = json!({ "test": "context_invalidation" });
    env.send_message("test-key", test_payload).await?;

    info!("Sent test message, waiting for cloned context...");

    // Wait for the handler to send us the cloned context
    let cloned_context = context_rx
        .recv()
        .await
        .ok_or_else(|| eyre!("Handler did not send cloned context"))?;

    info!("Received cloned context, handler should have completed and invalidated context");

    // Join the partition task before probing: `env.shutdown()` drives the
    // consumer down and joins its task, so the hoisted `invalidate()` at the
    // end of the message arm has provably run. This also satisfies the rdkafka
    // teardown rule (shut down before propagating any failure); no early `?`
    // sits between the message send and here.
    env.shutdown().await;

    // Now try to use the cloned context - this should fail with InvalidContext
    let future_time = CompactDateTime::now()?.add_duration(CompactDuration::new(60))?;

    match cloned_context
        .schedule(future_time, TimerType::Application)
        .await
    {
        Ok(()) => Err(eyre!(
            "UNEXPECTED: Cloned context usage succeeded when it should have failed"
        )),
        Err(error) => {
            let error_string = format!("{error}");
            // The only source of this message is `TimerManagerError::InvalidContext`'s
            // `Display` impl, reached through the `Box<dyn EventContextError>` erasure.
            if error_string.contains("no longer valid") {
                info!("SUCCESS: Cloned context correctly returned InvalidContext error");
                Ok(())
            } else {
                Err(eyre!("Expected InvalidContext error, got: {error}"))
            }
        }
    }
}
