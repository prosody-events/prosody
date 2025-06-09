//! Logging failure handling strategy for message processing.
//!
//! This module provides logging capabilities that wrap handlers and log errors
//! according to their severity while maintaining the original error flow.

use crate::consumer::event_context::EventContext;
use crate::consumer::failure::{ClassifyError, ErrorCategory, FailureStrategy, FallibleHandler};
use crate::consumer::message::{ConsumerMessage, UncommittedMessage};
use crate::consumer::{EventHandler, HandlerProvider, Uncommitted};
use crate::timers::{Trigger, UncommittedTimer};
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
    async fn on_message<C>(&self, context: C, message: ConsumerMessage) -> Result<(), Self::Error>
    where
        C: EventContext,
    {
        // Attempt to process the message with the wrapped handler
        let Err(error) = self.0.on_message(context, message).await else {
            return Ok(());
        };

        // Log the error based on its category
        match error.classify_error() {
            ErrorCategory::Transient => {
                error!("transient error occurred during message processing: {error:#}");
            }
            ErrorCategory::Permanent => {
                error!("permanent error occurred during message processing: {error:#}");
            }
            ErrorCategory::Terminal => {
                error!("terminal error occurred during message processing: {error:#}");
            }
        }

        Err(error)
    }

    async fn on_timer<C>(&self, context: C, trigger: Trigger) -> Result<(), Self::Error>
    where
        C: EventContext,
    {
        // Attempt to process the timer with the wrapped handler
        let Err(error) = self.0.on_timer(context, trigger).await else {
            return Ok(());
        };

        // Log the error based on its category
        match error.classify_error() {
            ErrorCategory::Transient => {
                error!("transient error occurred during timer processing: {error:#}");
            }
            ErrorCategory::Permanent => {
                error!("permanent error occurred during timer processing: {error:#}");
            }
            ErrorCategory::Terminal => {
                error!("terminal error occurred during timer processing: {error:#}");
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
    async fn on_message<C>(&self, context: C, message: UncommittedMessage)
    where
        C: EventContext,
    {
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

    async fn on_timer<C, U>(&self, context: C, timer: U)
    where
        C: EventContext,
        U: UncommittedTimer,
    {
        let (timer, uncommitted_timer) = timer.into_inner();

        // Attempt message processing
        let Err(error) = self.0.on_timer(context, timer).await else {
            uncommitted_timer.commit().await;
            return;
        };

        // Handle offset management based on error category
        match error.classify_error() {
            ErrorCategory::Transient => {
                error!("transient error occurred during processing: {error:#}; discarding timer");
                uncommitted_timer.commit().await;
            }
            ErrorCategory::Permanent => {
                error!("permanent error occurred during processing: {error:#}; discarding timer");
                uncommitted_timer.commit().await;
            }
            ErrorCategory::Terminal => {
                error!("terminal error occurred during processing: {error:#}; aborting timer");
                uncommitted_timer.abort().await;
            }
        }
    }

    /// Performs shutdown operations for this handler.
    async fn shutdown(self) {}
}
