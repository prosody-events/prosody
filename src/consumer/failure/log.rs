//! Logging failure handling strategy for message processing.
//!
//! This module provides logging capabilities that wrap handlers and log errors
//! according to their severity while maintaining the original error flow.

use crate::consumer::failure::{ClassifyError, ErrorCategory, FailureStrategy, FallibleHandler};
use crate::consumer::message::{ConsumerMessage, EventContext, UncommittedMessage};
use crate::consumer::{EventHandler, HandlerProvider};
use tracing::error;

/// A strategy that logs failures during message processing.
#[derive(Copy, Clone, Debug)]
pub struct LogStrategy;

/// A handler that logs failures during message processing.
///
/// Wraps another handler and adds logging capabilities while preserving the
/// original error handling behavior.
#[derive(Clone, Debug)]
struct LogHandler<T>(T);

impl FailureStrategy for LogStrategy {
    /// Creates a new handler that logs failures from the provided handler.
    ///
    /// # Arguments
    ///
    /// * `handler` - The handler to wrap with logging capabilities
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

    /// Processes a message and logs any errors that occur.
    ///
    /// # Arguments
    ///
    /// * `context` - The context of the message being processed
    /// * `message` - The message to process
    ///
    /// # Errors
    ///
    /// Returns the original error from the wrapped handler after logging it
    async fn on_message(
        &self,
        context: EventContext,
        message: ConsumerMessage,
    ) -> Result<(), Self::Error> {
        // Attempt to process the message with the wrapped handler
        let Err(error) = self.0.on_message(context, message).await else {
            return Ok(());
        };

        // Log the error based on its category
        match error.classify_error() {
            ErrorCategory::Transient => {
                error!("transient error occurred during processing: {error:#}");
            }
            ErrorCategory::Permanent => {
                error!("permanent error occurred during processing: {error:#}");
            }
            ErrorCategory::Terminal => {
                error!("terminal error occurred during processing: {error:#}");
            }
        }

        Err(error)
    }
}

impl<T> EventHandler for LogHandler<T>
where
    T: FallibleHandler,
{
    /// Processes an uncommitted message, logs any errors, and handles offset
    /// management.
    ///
    /// # Arguments
    ///
    /// * `context` - The context of the message being processed
    /// * `message` - The uncommitted message to process
    async fn on_message(&self, context: EventContext, message: UncommittedMessage) {
        let (message, uncommitted_offset) = message.into_inner();

        // Attempt message processing
        let Err(error) = self.0.on_message(context, message).await else {
            uncommitted_offset.commit();
            return;
        };

        // Handle offset management based on error category
        match error.classify_error() {
            ErrorCategory::Transient => {
                error!("transient error occurred during processing: {error:#}; discarding message");
                uncommitted_offset.commit();
            }
            ErrorCategory::Permanent => {
                error!("permanent error occurred during processing: {error:#}; discarding message");
                uncommitted_offset.commit();
            }
            ErrorCategory::Terminal => {
                error!("terminal error occurred during processing: {error:#}; aborting processing");
                uncommitted_offset.abort();
            }
        }
    }

    /// Performs shutdown operations for this handler.
    async fn shutdown(self) {}
}
