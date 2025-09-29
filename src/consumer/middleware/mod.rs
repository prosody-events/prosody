//! Middleware for message processing.
//!
//! This module provides a composable framework for handling cross-cutting
//! concerns during message processing including retry logic, concurrency
//! limiting, shutdown handling, telemetry recording, failure topic routing,
//! and logging. Middleware components can be combined using the `layer` method
//! to create processing pipelines.
//!
//! ## Available Middleware
//!
//! See the individual middleware modules for specific implementations:
//! - **`concurrency`** - Global concurrency limiting with semaphore permits
//! - **`retry`** - Exponential backoff retry logic for transient failures
//! - **`shutdown`** - Graceful partition revocation handling
//! - **`telemetry`** - Handler lifecycle event recording for observability
//! - **`topic`** - Failure topic routing for permanent failures
//! - **`log`** - Error logging with severity-based categorization
//!
//! ## Middleware Composition
//!
//! Middleware are composed from inner to outer using the `layer` method, creating
//! a bidirectional execution flow where each middleware can intercept both the
//! request (on the way to the handler) and the response (on the way back).
//!
//! ```rust
//! use prosody::consumer::middleware::*;
//!
//! // Compose middleware from innermost to outermost
//! let middleware = inner_middleware
//!     .layer(middle_middleware)
//!     .layer(outer_middleware);
//! ```
//!
//! ### Execution Flow Pattern
//!
//! Each middleware wraps the inner layers, creating an "onion" pattern where
//! execution flows through all layers to reach the handler, then back out:
//!
//! **Request Path (outer → inner):**
//! 1. `OuterMiddleware` - Pre-processing, setup, validation
//! 2. `MiddleMiddleware` - Transformation, routing decisions
//! 3. `InnerMiddleware` - Resource acquisition, final checks
//! 4. **User Handler** - Actual message/timer processing
//!
//! **Response Path (inner → outer):**
//! 4. **User Handler** - Returns success or error result
//! 3. `InnerMiddleware` - Resource cleanup, immediate error handling
//! 2. `MiddleMiddleware` - Error classification, retry logic, side effects
//! 1. `OuterMiddleware` - Final error handling, logging, reporting
//!
//! ### Middleware Capabilities
//!
//! This bidirectional flow allows each middleware to:
//! - **Transform requests** before they reach inner layers
//! - **Handle responses** and errors as they bubble back up
//! - **Short-circuit execution** (e.g., validation failures, shutdown conditions)
//! - **Add side effects** (e.g., logging, metrics, external notifications)
//! - **Manage resources** (e.g., acquire/release permits, connections)
//! - **Implement retry logic** with backoff and error classification
//!
//! ## Error Classification
//!
//! The middleware system uses structured error classification via [`ErrorCategory`]:
//!
//! - **Transient** - Temporary failures that should be retried
//! - **Permanent** - Business logic errors that should not be retried
//! - **Terminal** - System-level failures requiring immediate shutdown
//!
//! Each middleware respects these classifications to determine appropriate handling behavior.

use std::convert::Infallible;
use std::fmt::Display;
use std::future::Future;

use crate::consumer::event_context::EventContext;
use crate::consumer::message::{ConsumerMessage, UncommittedMessage};
use crate::consumer::{EventHandler, HandlerProvider, Uncommitted};
use crate::timers::{Trigger, UncommittedTimer};
use crate::{Partition, Topic};

pub mod concurrency;
pub mod log;
pub mod retry;
pub mod shutdown;
pub mod telemetry;
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

/// Provides fallible handlers for processing messages from specific partitions.
///
/// This trait is similar to `HandlerProvider` but is designed to work with
/// fallible handlers that can return errors during processing. It allows
/// creating handlers that can fail and be composed with middleware that
/// handles these failures.
pub trait FallibleHandlerProvider: Send + Sync + 'static {
    /// The type of fallible handler provided.
    type Handler: FallibleHandler + Send + Sync + 'static;

    /// Creates a fallible handler for a specific topic and partition.
    ///
    /// # Arguments
    ///
    /// * `topic` - The topic of the partition.
    /// * `partition` - The partition number.
    ///
    /// # Returns
    ///
    /// A handler instance for processing messages from the specified
    /// topic-partition.
    fn handler_for_partition(&self, topic: Topic, partition: Partition) -> Self::Handler;
}

/// Defines middleware for message processing.
pub trait HandlerMiddleware {
    /// The provider type that wraps another fallible handler provider.
    type Provider<T: FallibleHandlerProvider>: FallibleHandlerProvider;

    /// Wraps a handler provider with this middleware.
    ///
    /// # Arguments
    ///
    /// * `provider` - The fallible handler provider to wrap with this
    ///   middleware.
    ///
    /// # Returns
    ///
    /// A new provider that implements `FallibleHandlerProvider`.
    fn with_provider<T>(&self, provider: T) -> Self::Provider<T>
    where
        T: FallibleHandlerProvider;

    /// Adds a middleware layer on top of this middleware (inner-to-outer
    /// composition).
    ///
    /// The new middleware becomes the outermost layer in the processing stack,
    /// creating a bidirectional wrapper around the existing middleware stack.
    ///
    /// # Execution Flow
    ///
    /// When composing `inner.layer(outer)`, execution flows through both
    /// request and response phases:
    ///
    /// **Request Phase (outer → inner):**
    /// 1. `outer` middleware request handling
    /// 2. `inner` middleware request handling
    /// 3. User handler execution
    ///
    /// **Response Phase (inner → outer):**
    /// 3. User handler returns result/error
    /// 2. `inner` middleware response handling
    /// 1. `outer` middleware response handling
    ///
    /// Each middleware can transform the request, short-circuit execution,
    /// handle errors, and add side effects on both phases.
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
    /// ```rust
    /// // Builds from inner to outer: inner -> middle -> outer
    /// let middleware = inner_middleware
    ///     .layer(middle_middleware)  // middle wraps inner
    ///     .layer(outer_middleware);  // outer wraps middle+inner
    ///
    /// // Request:  outer → middle → inner → handler
    /// // Response: handler → inner → middle → outer
    /// ```
    fn layer<T>(self, outer_middleware: T) -> ComposedMiddleware<T, Self>
    where
        Self: Sized,
    {
        ComposedMiddleware(outer_middleware, self)
    }
}

/// Defines a handler that can fail during message processing.
pub trait FallibleHandler: Send + Sync + 'static {
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

/// A provider that clones the wrapped handler for each partition.
///
/// This provider is useful when you have a handler that can be safely cloned
/// and you want to use the same handler instance logic across multiple
/// partitions.
#[derive(Clone, Debug)]
pub struct CloneProvider<T>(T);

/// A composition of two middleware components.
#[derive(Clone, Debug)]
pub struct ComposedMiddleware<M1, M2>(M1, M2);

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

impl<T> CloneProvider<T> {
    /// Creates a new `CloneProvider` that wraps the given handler.
    ///
    /// # Arguments
    ///
    /// * `inner` - The handler to wrap.
    ///
    /// # Returns
    ///
    /// A new `CloneProvider` instance.
    pub fn new(inner: T) -> Self {
        Self(inner)
    }
}

impl<M1, M2> HandlerMiddleware for ComposedMiddleware<M1, M2>
where
    M1: HandlerMiddleware,
    M2: HandlerMiddleware,
{
    type Provider<T: FallibleHandlerProvider> = M1::Provider<M2::Provider<T>>;

    fn with_provider<T>(&self, provider: T) -> Self::Provider<T>
    where
        T: FallibleHandlerProvider,
    {
        // Apply the first middleware to the result of applying the second middleware
        // This matches Tower's pattern where M1 (outer) wraps M2 (inner)
        self.0.with_provider(self.1.with_provider(provider))
    }
}

impl<T> FallibleHandlerProvider for CloneProvider<T>
where
    T: FallibleHandler + Clone + Send + Sync + 'static,
{
    type Handler = T;

    fn handler_for_partition(&self, _topic: Topic, _partition: Partition) -> Self::Handler {
        self.0.clone()
    }
}

impl<T> HandlerProvider for CloneProvider<T>
where
    T: FallibleHandler + Clone + Send + Sync + 'static,
{
    type Handler = Self;

    fn handler_for_partition(&self, _topic: Topic, _partition: Partition) -> Self::Handler {
        Self(self.0.clone())
    }
}

impl<T> FallibleHandler for CloneProvider<T>
where
    T: FallibleHandler,
{
    type Error = T::Error;

    fn on_message<C>(
        &self,
        context: C,
        message: ConsumerMessage,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send
    where
        C: EventContext,
    {
        self.0.on_message(context, message)
    }

    fn on_timer<C>(
        &self,
        context: C,
        trigger: Trigger,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send
    where
        C: EventContext,
    {
        self.0.on_timer(context, trigger)
    }
}

impl<T> FallibleEventHandler for CloneProvider<T> where T: FallibleHandler {}

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

impl ClassifyError for Infallible {
    fn classify_error(&self) -> ErrorCategory {
        ErrorCategory::Terminal
    }
}
