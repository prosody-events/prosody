//! Middleware for message processing.
//!
//! This module provides a composable framework for handling cross-cutting
//! concerns during message processing including failure recovery, concurrency
//! limiting, lifecycle management, and observability. Middleware can be
//! combined using the `layer` method to create processing pipelines.

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

/// Defines middleware for message processing.
pub trait HandlerMiddleware {
    /// Wraps a handler with this middleware.
    ///
    /// # Arguments
    ///
    /// * `handler` - The handler to wrap with this middleware.
    ///
    /// # Returns
    ///
    /// A new handler that implements both `HandlerProvider` and
    /// `FallibleHandler`.
    fn with_handler<T>(&self, handler: T) -> impl HandlerProvider + FallibleHandler
    where
        T: FallibleHandler;

    /// Adds a middleware layer on top of this middleware (inner-to-outer
    /// composition).
    ///
    /// The new middleware becomes the outermost layer in the processing stack,
    /// executing before (and wrapping around) the existing middleware stack.
    ///
    /// # Execution Order
    ///
    /// When composing `inner.layer(outer)`, the execution flows as:
    /// 1. `outer` middleware pre-processing
    /// 2. `inner` middleware pre-processing
    /// 3. User handler execution
    /// 4. `inner` middleware post-processing
    /// 5. `outer` middleware post-processing
    ///
    /// # Arguments
    ///
    /// * `outer_middleware` - The middleware to add as the outermost layer.
    ///
    /// # Returns
    ///
    /// A `ComposedMiddleware` with the new middleware as the outer layer.
    ///
    /// # Example
    ///
    /// ```
    /// // Builds from inner to outer: concurrency -> shutdown -> retry
    /// let middleware = concurrency_middleware
    ///     .layer(shutdown_middleware) // shutdown wraps concurrency
    ///     .layer(retry_middleware); // retry wraps shutdown+concurrency
    ///
    /// // Execution: retry -> shutdown -> concurrency -> handler -> concurrency -> shutdown -> retry
    /// ```
    fn layer<T>(self, outer_middleware: T) -> ComposedMiddleware<Self, T>
    where
        Self: Sized,
    {
        ComposedMiddleware(self, outer_middleware)
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
    /// determine the appropriate failure handling approach:
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

/// A composition of two middleware components.
#[derive(Clone, Debug)]
pub struct ComposedMiddleware<M1, M2>(M1, M2);

impl<M1, M2> HandlerMiddleware for ComposedMiddleware<M1, M2>
where
    M1: HandlerMiddleware,
    M2: HandlerMiddleware,
{
    fn with_handler<T>(&self, handler: T) -> impl HandlerProvider + FallibleHandler
    where
        T: FallibleHandler,
    {
        // Apply the first middleware to the result of applying the second middleware
        // This matches Tower's pattern where M1 (outer) wraps M2 (inner)
        self.0.with_handler(self.1.with_handler(handler))
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
