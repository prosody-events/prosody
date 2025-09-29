//! Implements shutdown middleware for graceful partition revocation handling.
//!
//! This module provides a mechanism to stop processing messages when a Kafka
//! partition is being revoked, ensuring proper handling of in-flight messages
//! and preventing new message processing.

use thiserror::Error;

use crate::consumer::event_context::EventContext;
use crate::consumer::message::ConsumerMessage;
use crate::consumer::middleware::{
    ClassifyError, ErrorCategory, FallibleEventHandler, FallibleHandler, FallibleHandlerProvider,
    HandlerMiddleware,
};
use crate::consumer::HandlerProvider;
use crate::timers::Trigger;
use crate::{Partition, Topic};

/// Middleware that checks if the partition is shutting down before running the
/// handler, preventing other middleware from delaying the shutdown.
#[derive(Clone, Copy, Debug)]
pub struct ShutdownMiddleware;

/// A provider that wraps handlers with shutdown functionality.
#[derive(Clone, Debug)]
struct ShutdownProvider<T> {
    provider: T,
}

/// A fallible provider that wraps handlers with shutdown functionality.
#[derive(Clone, Debug)]
pub struct FallibleShutdownProvider<T> {
    provider: T,
}

/// Wraps a handler with shutdown functionality.
///
/// This struct adds shutdown checks to the wrapped handler's message
/// processing, ensuring that no new messages are processed when a
/// partition is being revoked.
#[derive(Clone, Debug)]
pub struct ShutdownHandler<T> {
    handler: T,
}

impl HandlerMiddleware for ShutdownMiddleware {
    type Provider<T: FallibleHandlerProvider> = FallibleShutdownProvider<T>;

    fn with_provider<T>(&self, provider: T) -> Self::Provider<T>
    where
        T: FallibleHandlerProvider,
    {
        FallibleShutdownProvider { provider }
    }
}

impl<T> FallibleHandlerProvider for FallibleShutdownProvider<T>
where
    T: FallibleHandlerProvider,
{
    type Handler = ShutdownHandler<T::Handler>;

    fn handler_for_partition(&self, topic: Topic, partition: Partition) -> Self::Handler {
        ShutdownHandler {
            handler: self.provider.handler_for_partition(topic, partition),
        }
    }
}

impl<T> HandlerProvider for FallibleShutdownProvider<T>
where
    T: HandlerProvider<Handler: FallibleHandler>,
{
    type Handler = ShutdownHandler<T::Handler>;

    fn handler_for_partition(&self, topic: Topic, partition: Partition) -> Self::Handler {
        ShutdownHandler {
            handler: self.provider.handler_for_partition(topic, partition),
        }
    }
}

impl<T> HandlerProvider for ShutdownProvider<T>
where
    T: HandlerProvider<Handler: FallibleHandler>,
{
    type Handler = ShutdownHandler<T::Handler>;

    fn handler_for_partition(&self, topic: Topic, partition: Partition) -> Self::Handler {
        ShutdownHandler {
            handler: self.provider.handler_for_partition(topic, partition),
        }
    }
}

impl<T> FallibleHandler for ShutdownHandler<T>
where
    T: FallibleHandler,
{
    type Error = ShutdownError<T>;

    /// Processes a message, checking for shutdown conditions.
    ///
    /// # Arguments
    ///
    /// * `context` - The context of the message being processed.
    /// * `message` - The message to be processed.
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or a `ShutdownError`.
    ///
    /// # Errors
    ///
    /// Returns a `ShutdownError::Shutdown` if the partition is being revoked,
    /// or a `ShutdownError::Handler` containing the wrapped handler's error.
    async fn on_message<C>(&self, context: C, message: ConsumerMessage) -> Result<(), Self::Error>
    where
        C: EventContext,
    {
        if context.should_shutdown() {
            return Err(ShutdownError::Shutdown);
        }

        self.handler
            .on_message(context, message)
            .await
            .map_err(ShutdownError::Handler)
    }

    async fn on_timer<C>(&self, context: C, timer: Trigger) -> Result<(), Self::Error>
    where
        C: EventContext,
    {
        if context.should_shutdown() {
            return Err(ShutdownError::Shutdown);
        }
        self.handler
            .on_timer(context, timer)
            .await
            .map_err(ShutdownError::Handler)
    }
}

impl<T> FallibleEventHandler for ShutdownHandler<T> where T: FallibleHandler {}

/// Represents errors that can occur during shutdown handling.
#[derive(Debug, Error)]
pub enum ShutdownError<T>
where
    T: FallibleHandler,
{
    /// Indicates that the partition is being revoked.
    #[error("partition is being revoked")]
    Shutdown,

    /// Wraps an error from the underlying handler.
    #[error("handler error: {0:#}")]
    Handler(T::Error),
}

impl<T> ClassifyError for ShutdownError<T>
where
    T: FallibleHandler,
{
    /// Classifies the shutdown error.
    ///
    /// # Returns
    ///
    /// An `ErrorCategory` indicating the nature of the error:
    /// - `ErrorCategory::Terminal` for `ShutdownError::Shutdown`
    /// - The classification of the wrapped error for `ShutdownError::Handler`
    fn classify_error(&self) -> ErrorCategory {
        match self {
            ShutdownError::Shutdown => ErrorCategory::Terminal,
            ShutdownError::Handler(error) => error.classify_error(),
        }
    }
}
