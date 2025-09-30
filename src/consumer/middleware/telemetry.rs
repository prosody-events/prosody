//! Handler lifecycle telemetry middleware.
//!
//! Records handler invocation events for observability and monitoring. Captures
//! metrics like execution time, success/failure rates, and error
//! classifications without affecting the processing flow.
//!
//! # Execution Order
//!
//! **Request Path:**
//! 1. **Record handler invocation** - Log start of processing
//! 2. Pass control to inner middleware layers
//!
//! **Response Path:**
//! 1. Receive result from inner layers
//! 2. **Record handler completion** - Log success/failure and timing
//! 3. Pass original result through unchanged
//!
//! # Telemetry Events
//!
//! - **Handler Invoked**: When processing begins
//! - **Handler Succeeded**: When processing completes successfully
//! - **Handler Failed**: When processing fails (with error category)
//! - **Execution Time**: Duration of processing
//! - **Partition Context**: Which topic-partition was processed
//!
//! # Usage
//!
//! Position for comprehensive visibility across the processing pipeline:
//!
//! ```rust
//! use prosody::consumer::middleware::*;
//!
//! let provider = ConcurrencyLimitMiddleware::new(&config)
//!     .layer(TelemetryMiddleware::new()) // Monitor entire pipeline
//!     .layer(ShutdownMiddleware)
//!     .layer(RetryMiddleware::new(retry_config))
//!     .into_provider(handler);
//! ```

use crate::consumer::Keyed;
use crate::consumer::event_context::EventContext;
use crate::consumer::message::ConsumerMessage;
use crate::consumer::middleware::{FallibleHandler, FallibleHandlerProvider, HandlerMiddleware};
use crate::telemetry::{Telemetry, partition::TelemetryPartitionSender};
use crate::timers::Trigger;
use crate::{Partition, Topic};

/// Middleware that records telemetry events during message processing.
#[derive(Clone, Debug)]
pub struct TelemetryMiddleware {
    telemetry: Telemetry,
}

/// A provider that records telemetry events during message processing.
#[derive(Clone, Debug)]
pub struct TelemetryProvider<T> {
    provider: T,
    telemetry: Telemetry,
}

/// A handler that records telemetry events during message processing.
///
/// Wraps another handler and adds telemetry recording capabilities while
/// preserving the original processing behavior and error handling.
#[derive(Clone, Debug)]
pub struct TelemetryHandler<T> {
    handler: T,
    sender: TelemetryPartitionSender,
}

impl TelemetryMiddleware {
    /// Creates a new `TelemetryMiddleware` with the provided telemetry system.
    ///
    /// # Arguments
    ///
    /// * `telemetry` - The telemetry system for creating partition senders
    #[must_use]
    pub fn new(telemetry: Telemetry) -> Self {
        Self { telemetry }
    }
}

impl HandlerMiddleware for TelemetryMiddleware {
    type Provider<T: FallibleHandlerProvider> = TelemetryProvider<T>;

    fn with_provider<T>(&self, provider: T) -> Self::Provider<T>
    where
        T: FallibleHandlerProvider,
    {
        TelemetryProvider {
            provider,
            telemetry: self.telemetry.clone(),
        }
    }
}

impl<T> FallibleHandlerProvider for TelemetryProvider<T>
where
    T: FallibleHandlerProvider,
{
    type Handler = TelemetryHandler<T::Handler>;

    fn handler_for_partition(&self, topic: Topic, partition: Partition) -> Self::Handler {
        let partition_sender = self.telemetry.partition_sender(topic, partition);
        TelemetryHandler {
            handler: self.provider.handler_for_partition(topic, partition),
            sender: partition_sender,
        }
    }
}

impl<T> FallibleHandler for TelemetryHandler<T>
where
    T: FallibleHandler,
{
    type Error = T::Error;

    /// Processes a message and records telemetry events for handler lifecycle.
    ///
    /// Records the following events:
    /// - `HandlerInvoked` when the handler is called
    /// - `HandlerSucceeded` when the handler completes successfully
    /// - `HandlerFailed` when the handler returns an error
    ///
    /// # Arguments
    ///
    /// * `context` - The context of the message being processed
    /// * `message` - The message to process
    ///
    /// # Errors
    ///
    /// Returns the original error from the wrapped handler
    async fn on_message<C>(&self, context: C, message: ConsumerMessage) -> Result<(), Self::Error>
    where
        C: EventContext,
    {
        let key = message.key().clone();

        // Record handler invocation
        self.sender.handler_invoked(key.clone());

        // Process the message with the wrapped handler
        let result = self.handler.on_message(context, message).await;

        // Record success or failure
        match &result {
            Ok(()) => self.sender.handler_succeeded(key),
            Err(_) => self.sender.handler_failed(key),
        }

        result
    }

    /// Processes a timer and records telemetry events for handler lifecycle.
    ///
    /// Records the following events:
    /// - `HandlerInvoked` when the handler is called
    /// - `HandlerSucceeded` when the handler completes successfully
    /// - `HandlerFailed` when the handler returns an error
    ///
    /// # Arguments
    ///
    /// * `context` - The context for timer processing
    /// * `trigger` - The timer trigger to process
    ///
    /// # Errors
    ///
    /// Returns the original error from the wrapped handler
    async fn on_timer<C>(&self, context: C, trigger: Trigger) -> Result<(), Self::Error>
    where
        C: EventContext,
    {
        let key = trigger.key.clone();

        // Record handler invocation
        self.sender.handler_invoked(key.clone());

        // Process the timer with the wrapped handler
        let result = self.handler.on_timer(context, trigger).await;

        // Record success or failure
        match &result {
            Ok(()) => self.sender.handler_succeeded(key),
            Err(_) => self.sender.handler_failed(key),
        }

        result
    }
}
