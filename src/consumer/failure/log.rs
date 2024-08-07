//! Logging strategy for failure handling in message processing.
//!
//! This module provides a `LogStrategy` that wraps handlers and logs errors
//! when message processing fails.

use tracing::{error, info};

use crate::consumer::failure::{ClassifyError, ErrorCategory, FailureStrategy, FallibleHandler};
use crate::consumer::message::{ConsumerMessage, MessageContext, UncommittedMessage};
use crate::consumer::{HandlerProvider, MessageHandler};

/// A strategy that logs errors when message processing fails.
#[derive(Copy, Clone, Debug)]
pub struct LogStrategy;

/// A handler wrapped with logging functionality.
#[derive(Clone, Debug)]
struct LogHandler<T>(T);

impl FailureStrategy for LogStrategy {
    /// Wraps a handler with logging functionality.
    ///
    /// # Arguments
    ///
    /// * `handler` - The handler to wrap with logging functionality.
    ///
    /// # Returns
    ///
    /// A new `LogHandler` that implements both `HandlerProvider` and
    /// `FallibleHandler`.
    fn with_handler<T>(&self, handler: T) -> impl HandlerProvider + FallibleHandler
    where
        T: FallibleHandler,
    {
        LogHandler(handler)
    }
}

impl<T> FallibleHandler for LogHandler<T>
where
    T: FallibleHandler,
{
    type Error = T::Error;

    /// Handles a message and logs any errors that occur during processing.
    ///
    /// # Arguments
    ///
    /// * `context` - The context of the message being processed.
    /// * `message` - The message to be processed.
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or failure of message processing.
    ///
    /// # Errors
    ///
    /// Returns an error if the wrapped handler fails to process the message.
    async fn handle(
        &self,
        context: MessageContext,
        message: ConsumerMessage,
    ) -> Result<(), Self::Error> {
        let topic = message.topic;
        let partition = message.partition;
        let key = message.key.clone();
        let offset = message.offset;

        // Attempt to handle the message and log any errors
        self.0
            .handle(context, message)
            .await
            .inspect_err(|error| match error.classify_error() {
                ErrorCategory::Transient | ErrorCategory::Permanent => error!(
                    %topic, %partition, %key, %offset,
                    "failed to handle message: {error:#}"
                ),
                ErrorCategory::Terminal => info!(
                    %topic, %partition, %key, %offset,
                    "terminal condition encountered while handling message: {error:#}; aborting"
                ),
            })
    }
}

impl<T> MessageHandler for LogHandler<T>
where
    T: FallibleHandler,
{
    /// Handles an uncommitted message, logging any errors and managing offset
    /// commitment.
    ///
    /// # Arguments
    ///
    /// * `context` - The context of the message being processed.
    /// * `message` - The uncommitted message to be processed.
    async fn handle(&self, context: MessageContext, message: UncommittedMessage) {
        let (message, uncommitted_offset) = message.into_inner();

        // Attempt to handle the message and log if it fails
        let Err(error) = FallibleHandler::handle(self, context, message).await else {
            uncommitted_offset.commit();
            return;
        };

        // Commit or abort the offset based on the error category
        match error.classify_error() {
            ErrorCategory::Transient | ErrorCategory::Permanent => uncommitted_offset.commit(),
            ErrorCategory::Terminal => uncommitted_offset.abort(),
        }
    }

    /// Performs any necessary shutdown operations for the handler.
    ///
    /// This implementation does not require any specific shutdown behavior.
    async fn shutdown(self) {
        // No shutdown behavior needed for logging
    }
}
