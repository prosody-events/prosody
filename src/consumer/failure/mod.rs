//! Failure handling strategies for message processing.
//!
//! This module provides traits and structures for implementing
//! and composing failure handling strategies in asynchronous
//! message processing systems.

use crate::consumer::HandlerProvider;
use crate::consumer::event_context::EventContext;
use crate::consumer::message::{ConsumerMessage, UncommittedMessage};
use crate::consumer::{EventHandler, Uncommitted};
use crate::timers::{Trigger, UncommittedTimer};
use std::convert::Infallible;
use std::fmt::Display;
use std::future::Future;

pub mod concurrency;
pub mod log;
pub mod retry;
pub mod shutdown;
pub mod topic;

/// Categorizes errors in message processing.
#[derive(Copy, Clone, Debug)]
pub enum ErrorCategory {
    /// Error is temporary and recovery is possible
    Transient,
    /// Error is permanent and irrecoverable
    Permanent,
    /// Error is due to partition shutdown
    Terminal,
}

/// Defines methods for classifying errors.
pub trait ClassifyError {
    /// Classifies the error into a specific `ErrorCategory`.
    ///
    /// # Returns
    ///
    /// An `ErrorCategory` indicating the nature of the error.
    fn classify_error(&self) -> ErrorCategory;

    /// Determines if the error is recoverable.
    ///
    /// # Returns
    ///
    /// `true` if the error is classified as `ErrorCategory::Transient`, `false`
    /// otherwise.
    fn is_recoverable(&self) -> bool {
        matches!(self.classify_error(), ErrorCategory::Transient)
    }
}

/// Defines a failure handling strategy for message processing.
pub trait FailureStrategy {
    /// Wraps a handler with this failure strategy.
    ///
    /// # Arguments
    ///
    /// * `handler` - The handler to wrap with this strategy.
    ///
    /// # Returns
    ///
    /// A new handler that implements both `HandlerProvider` and
    /// `FallibleHandler`.
    fn with_handler<T>(&self, handler: T) -> impl HandlerProvider + FallibleHandler
    where
        T: FallibleHandler;

    /// Composes this strategy with another strategy.
    ///
    /// # Arguments
    ///
    /// * `next_handler` - The next strategy to apply if this one fails.
    ///
    /// # Returns
    ///
    /// A `ComposedStrategy` combining this strategy with the next one.
    fn and_then<T>(self, next_handler: T) -> ComposedStrategy<Self, T>
    where
        Self: Sized,
    {
        ComposedStrategy(self, next_handler)
    }
}

/// Defines a handler that can fail during message processing.
pub trait FallibleHandler: Clone + Send + Sync + 'static {
    /// The error type returned by this handler.
    type Error: ClassifyError + Display + Send;

    /// Handles a message, potentially returning an error.
    ///
    /// # Arguments
    ///
    /// * `context` - The context of the message being processed.
    /// * `message` - The message to be processed.
    ///
    /// # Returns
    ///
    /// A `Future` that resolves to `Ok(())` if the message was processed
    /// successfully, or an `Err` containing the error if processing failed.
    fn on_message<C>(
        &self,
        context: C,
        message: ConsumerMessage,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send
    where
        C: EventContext;

    /// Handles timer events with potential for failure.
    ///
    /// This method is called when a scheduled timer fires and is delivered to
    /// the handler for processing. Unlike [`Self::on_message`], this method
    /// handles timer events that contain a key, execution time, and tracing
    /// span.
    ///
    /// # Arguments
    ///
    /// * `context` - The event processing context with access to timer
    ///   management
    /// * `trigger` - The timer trigger containing key, time, and span
    ///   information
    ///
    /// # Returns
    ///
    /// A [`Future`] that resolves to:
    /// - `Ok(())` if the timer was processed successfully
    /// - `Err(Self::Error)` if processing failed
    ///
    /// # Error Handling
    ///
    /// Errors returned by this method are classified using [`ClassifyError`] to
    /// determine the appropriate failure handling strategy:
    /// - **Transient errors**: May be retried with backoff
    /// - **Permanent errors**: Logged and timer may be discarded
    /// - **Terminal errors**: Cause processing to stop entirely
    ///
    /// # Implementation Requirements
    ///
    /// Implementations should:
    /// - Process the timer event according to business logic
    /// - Return appropriate error types that implement [`ClassifyError`]
    /// - Ensure processing is idempotent where possible
    /// - Handle the timer's tracing span for observability
    fn on_timer<C>(
        &self,
        context: C,
        trigger: Trigger,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send
    where
        C: EventContext;
}

/// A composition of two failure strategies.
#[derive(Clone, Debug)]
pub struct ComposedStrategy<S1, S2>(S1, S2);

impl<S1, S2> FailureStrategy for ComposedStrategy<S1, S2>
where
    S1: FailureStrategy,
    S2: FailureStrategy,
{
    fn with_handler<T>(&self, handler: T) -> impl HandlerProvider + FallibleHandler
    where
        T: FallibleHandler,
    {
        // Apply the second strategy to the result of applying the first strategy
        self.1.with_handler(self.0.with_handler(handler))
    }
}

impl ClassifyError for Infallible {
    fn classify_error(&self) -> ErrorCategory {
        ErrorCategory::Terminal
    }
}

/// Provides default `EventHandler` implementation for types that implement
/// `FallibleHandler`.
///
/// This trait implements the standard failure handling pattern:
/// 1. Extract inner message/timer and uncommitted offset/timer
/// 2. Call the `FallibleHandler` method
/// 3. Commit on success or handle errors based on classification
///
/// Types can override the default implementations to add custom behavior like
/// logging or custom error handling.
pub trait FallibleEventHandler: FallibleHandler {
    /// Called when message processing fails.
    ///
    /// # Arguments
    ///
    /// * `error` - The error that occurred during processing
    fn on_message_error(&self, _error: &Self::Error) {}

    /// Called when timer processing fails.
    ///
    /// # Arguments
    ///
    /// * `error` - The error that occurred during processing
    fn on_timer_error(&self, _error: &Self::Error) {}
}

/// Default `EventHandler` implementation for any type that implements
/// `FallibleEventHandler`.
impl<T> EventHandler for T
where
    T: FallibleEventHandler,
    T::Error: ClassifyError,
{
    async fn on_message<C>(&self, context: C, message: UncommittedMessage)
    where
        C: EventContext,
    {
        let (inner_message, uncommitted_offset) = message.into_inner();

        // Attempt to process the message
        let Err(error) = FallibleHandler::on_message(self, context, inner_message).await else {
            uncommitted_offset.commit();
            return;
        };

        // Call error handler
        self.on_message_error(&error);

        // Handle offset management based on error category
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
        let (trigger, uncommitted_timer) = timer.into_inner();

        // Attempt to process the timer
        let Err(error) = FallibleHandler::on_timer(self, context, trigger).await else {
            uncommitted_timer.commit().await;
            return;
        };

        // Call error handler
        self.on_timer_error(&error);

        // Handle timer management based on error category
        match error.classify_error() {
            ErrorCategory::Transient | ErrorCategory::Permanent => {
                uncommitted_timer.commit().await;
            }
            ErrorCategory::Terminal => {
                uncommitted_timer.abort().await;
            }
        }
    }

    async fn shutdown(self) {}
}
