//! Implements a shutdown strategy for graceful partition revocation handling.
//!
//! This module provides a mechanism to stop processing messages when a Kafka
//! partition is being revoked, ensuring proper handling of in-flight messages
//! and preventing new message processing.

use crate::consumer::event_context::EventContext;
use crate::consumer::failure::{ClassifyError, ErrorCategory, FailureStrategy, FallibleHandler};
use crate::consumer::message::{ConsumerMessage, UncommittedMessage};
use crate::consumer::{EventHandler, HandlerProvider, Uncommitted};
use crate::timers::{Trigger, UncommittedTimer};
use thiserror::Error;

/// A strategy that checks if the partition is shutting down before running the
/// handler, preventing other strategies from delaying the shutdown.
#[derive(Clone, Copy, Debug)]
pub struct ShutdownStrategy;

impl FailureStrategy for ShutdownStrategy {
    fn with_handler<T>(&self, handler: T) -> impl HandlerProvider + FallibleHandler
    where
        T: FallibleHandler,
    {
        ShutdownHandler(handler)
    }
}

/// Wraps a handler with shutdown functionality.
///
/// This struct adds shutdown checks to the wrapped handler's message
/// processing, ensuring that no new messages are processed when a
/// partition is being revoked.
#[derive(Clone, Debug)]
pub struct ShutdownHandler<T>(T);

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

        self.0
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
        self.0
            .on_timer(context, timer)
            .await
            .map_err(ShutdownError::Handler)
    }
}

impl<T> EventHandler for ShutdownHandler<T>
where
    T: FallibleHandler,
{
    /// Handles message processing with shutdown checks.
    ///
    /// # Arguments
    ///
    /// * `context` - The context of the message being processed.
    /// * `message` - The uncommitted message to be processed.
    async fn on_message<C>(&self, context: C, message: UncommittedMessage)
    where
        C: EventContext,
    {
        let (message, uncommitted_offset) = message.into_inner();

        // Check if the partition is being revoked
        if context.should_shutdown() {
            uncommitted_offset.abort();
            return;
        }

        // Process the message and handle potential errors
        let Err(error) = self.0.on_message(context, message).await else {
            uncommitted_offset.commit();
            return;
        };

        // Commit or abort the offset based on the error category
        match error.classify_error() {
            ErrorCategory::Transient | ErrorCategory::Permanent => uncommitted_offset.commit(),
            ErrorCategory::Terminal => uncommitted_offset.abort(),
        }
    }

    async fn on_timer<C, U>(&self, context: C, timer: U)
    where
        C: EventContext,
        U: UncommittedTimer,
    {
        let (trigger, uncommitted) = timer.into_inner();

        // Check if the partition is being revoked
        if context.should_shutdown() {
            uncommitted.abort().await;
            return;
        }

        // Process the timer and handle potential errors
        let Err(error) = self.0.on_timer(context, trigger).await else {
            uncommitted.commit().await;
            return;
        };

        // Commit or abort based on the error category
        match error.classify_error() {
            ErrorCategory::Transient | ErrorCategory::Permanent => uncommitted.commit().await,
            ErrorCategory::Terminal => uncommitted.abort().await,
        }
    }

    /// Implements shutdown behavior for the handler.
    async fn shutdown(self) {}
}

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
