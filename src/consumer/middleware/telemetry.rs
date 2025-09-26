//! Telemetry middleware for message processing.
//!
//! This module provides telemetry capabilities that wrap handlers and record
//! handler lifecycle events (invoked, returned, succeeded, failed) for
//! observability and monitoring purposes.

use crate::consumer::HandlerProvider;
use crate::consumer::event_context::EventContext;
use crate::consumer::message::ConsumerMessage;
use crate::consumer::middleware::{FallibleEventHandler, FallibleHandler, HandlerMiddleware};
use crate::telemetry::sender::TelemetrySender;
use crate::timers::Trigger;

/// Middleware that records telemetry events during message processing.
#[derive(Clone, Debug)]
pub struct TelemetryMiddleware {
    sender: TelemetrySender,
}

/// A handler that records telemetry events during message processing.
///
/// Wraps another handler and adds telemetry recording capabilities while
/// preserving the original processing behavior and error handling.
#[derive(Clone, Debug)]
struct TelemetryHandler<T> {
    handler: T,
    sender: TelemetrySender,
}

impl TelemetryMiddleware {
    /// Creates a new `TelemetryMiddleware` with the provided telemetry sender.
    ///
    /// # Arguments
    ///
    /// * `sender` - The telemetry sender for recording handler events
    #[must_use]
    pub fn new(sender: TelemetrySender) -> Self {
        Self { sender }
    }
}

impl HandlerMiddleware for TelemetryMiddleware {
    /// Creates a new handler that records telemetry events from the provided
    /// handler.
    ///
    /// # Arguments
    ///
    /// * `handler` - The handler to wrap with telemetry recording capabilities
    fn with_handler<T>(&self, handler: T) -> impl HandlerProvider + FallibleHandler
    where
        T: FallibleHandler,
    {
        TelemetryHandler {
            handler,
            sender: self.sender.clone(),
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
    /// - `HandlerReturned` when the handler completes (success or failure)
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
        todo!()
        // let topic = message.topic();
        // let partition = message.partition();
        // let key = message.key().clone();
        //
        // // Record handler invocation
        // self.sender.handler_invoked(topic, partition, key.clone());
        //
        // // Process the message with the wrapped handler
        // let result = self.handler.on_message(context, message).await;
        //
        // // Record success or failure
        // match &result {
        //     Ok(()) => self.sender.handler_succeeded(topic, partition,
        // key.clone()),     Err(_) => self.sender.handler_failed(topic,
        // partition, key.clone()), }
        //
        // // Always record that the handler returned
        // self.sender.handler_returned(topic, partition, key.clone());
        //
        // result
    }

    /// Processes a timer and records telemetry events for handler lifecycle.
    ///
    /// Records the following events:
    /// - `HandlerInvoked` when the handler is called
    /// - `HandlerSucceeded` when the handler completes successfully
    /// - `HandlerFailed` when the handler returns an error
    /// - `HandlerReturned` when the handler completes (success or failure)
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
        todo!()
        // let topic = trigger.topic;
        // let partition = trigger.partition;
        // let key = trigger.key.clone();
        //
        // // Record handler invocation
        // self.sender.handler_invoked(topic, partition, key.clone());
        //
        // // Process the timer with the wrapped handler
        // let result = self.handler.on_timer(context, trigger).await;
        //
        // // Record success or failure
        // match &result {
        //     Ok(_) => self.sender.handler_succeeded(topic, partition,
        // key.clone()),     Err(_) => self.sender.handler_failed(topic,
        // partition, key.clone()), }
        //
        // // Always record that the handler returned
        // self.sender.handler_returned(topic, partition, key);
        //
        // result
    }
}

impl<T> FallibleEventHandler for TelemetryHandler<T> where T: FallibleHandler {}
