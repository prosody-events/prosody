//! Composable middleware framework for message processing.
//!
//! This module provides a middleware architecture for building message
//! processing pipelines from reusable components. Each middleware handles a
//! specific cross-cutting concern (retries, concurrency limiting, logging)
//! independently.
//!
//! # Architecture Overview
//!
//! The middleware system transforms your business logic through three layers:
//!
//! ```text
//! Handler → Provider → Middleware Stack → Consumer
//!   │         │           │                │
//!   │         │           │                └─ Kafka partition management
//!   │         │           └─ Cross-cutting concerns
//!   │         └─ Factory pattern for per-partition instances
//!   └─ Your business logic
//! ```
//!
//! ## Components
//!
//! - **Handler**: Your business logic implementing
//!   [`crate::consumer::EventHandler`] or [`FallibleHandler`]
//! - **Provider**: Factory creating handler instances per partition
//!   ([`crate::consumer::HandlerProvider`], [`FallibleHandlerProvider`])
//! - **Middleware**: Composable layers implementing [`HandlerMiddleware`]
//!
//! ## Why Middleware?
//!
//! Message processing requires many cross-cutting concerns: retries,
//! concurrency limits, error logging, dead letter queues, graceful shutdown,
//! and telemetry.
//!
//! Middleware provides:
//! - **Separation of concerns** - Each middleware has one responsibility
//! - **Composability** - Mix and match as needed
//! - **Reusability** - Same middleware works with any handler
//! - **Testability** - Test business logic and infrastructure separately
//!
//! # Available Middleware
//!
//! | Middleware | Purpose |
//! |------------|---------|
//! | [`scheduler`] | Fair work-conserving scheduler with global concurrency limits |
//! | [`retry`] | Exponential backoff for transient failures |
//! | [`shutdown`] | Graceful partition revocation |
//! | [`telemetry`] | Handler lifecycle observability |
//! | [`topic`] | Dead letter queue routing |
//! | [`log`] | Error categorization and logging |
//! | [`monopolization`] | Detects and prevents key-level execution monopolies |
//!
//! # Usage
//!
//! Compose middleware using [`HandlerMiddleware::layer`] and finalize with
//! [`HandlerMiddleware::into_provider`]:
//!
//! ```rust,no_run
//! # use prosody::consumer::middleware::*;
//! # use prosody::consumer::middleware::retry::{RetryMiddleware, RetryConfiguration};
//! # use prosody::consumer::middleware::shutdown::*;
//! # use prosody::consumer::DemandType;
//! # use prosody::consumer::event_context::EventContext;
//! # use prosody::consumer::message::ConsumerMessage;
//! # use prosody::timers::Trigger;
//! # use std::convert::Infallible;
//! # #[derive(Clone)]
//! # struct MyHandler;
//! # impl FallibleHandler for MyHandler {
//! #     type Error = Infallible;
//! #     async fn on_message<C>(&self, _: C, _: ConsumerMessage, _: DemandType) -> Result<(), Self::Error> { Ok(()) }
//! #     async fn on_timer<C>(&self, _: C, _: Trigger, _: DemandType) -> Result<(), Self::Error> { Ok(()) }
//! #     async fn shutdown(self) {}
//! # }
//! # let retry_config = RetryConfiguration::builder().build().unwrap();
//! # let inner_middleware = RetryMiddleware::new(retry_config).unwrap();
//! # let middle_middleware = ShutdownMiddleware;
//! # let outer_middleware = ShutdownMiddleware;
//! # let my_handler = MyHandler;
//!
//! // Basic composition pattern
//! let provider = inner_middleware
//!     .layer(middle_middleware)
//!     .layer(outer_middleware)
//!     .into_provider(my_handler);
//! ```
//!
//! ## Real Example: Production Pipeline
//!
//! ```rust,no_run
//! # use prosody::consumer::middleware::*;
//! # use prosody::consumer::middleware::retry::*;
//! # use prosody::consumer::middleware::scheduler::*;
//! # use prosody::consumer::middleware::topic::*;
//! # use prosody::consumer::middleware::shutdown::*;
//! # use prosody::producer::{ProsodyProducer, ProducerConfiguration};
//! # use prosody::consumer::DemandType;
//! # use prosody::consumer::event_context::EventContext;
//! # use prosody::consumer::message::ConsumerMessage;
//! # use prosody::telemetry::Telemetry;
//! # use prosody::timers::Trigger;
//! # use std::convert::Infallible;
//! # #[derive(Clone)]
//! # struct MyHandler;
//! # impl FallibleHandler for MyHandler {
//! #     type Error = Infallible;
//! #     async fn on_message<C>(&self, _: C, _: ConsumerMessage, _: DemandType) -> Result<(), Self::Error> { Ok(()) }
//! #     async fn on_timer<C>(&self, _: C, _: Trigger, _: DemandType) -> Result<(), Self::Error> { Ok(()) }
//! #     async fn shutdown(self) {}
//! # }
//! # let config = SchedulerConfigurationBuilder::default().build().unwrap();
//! # let retry_config = RetryConfiguration::builder().build().unwrap();
//! # let topic_config = FailureTopicConfiguration::builder().failure_topic("dlq").build().unwrap();
//! # let producer_config = ProducerConfiguration::builder().bootstrap_servers(vec!["kafka:9092".to_string()]).build().unwrap();
//! # let producer = ProsodyProducer::new(&producer_config).unwrap();
//! # let telemetry = Telemetry::default();
//! # let my_business_handler = MyHandler;
//!
//! // Low-latency consumer with full error handling
//! let provider = SchedulerMiddleware::new(&config, &telemetry).unwrap()
//!     .layer(ShutdownMiddleware)
//!     .layer(RetryMiddleware::new(retry_config.clone()).unwrap())
//!     .layer(FailureTopicMiddleware::new(topic_config, "consumer-group".to_string(), producer).unwrap())
//!     .layer(RetryMiddleware::new(retry_config).unwrap())
//!     .into_provider(my_business_handler);
//! ```
//!
//! ## Execution Flow
//!
//! Middleware creates an "onion" pattern with bidirectional execution:
//!
//! ```text
//! Request:  Outer → Middle → Inner → Handler
//! Response: Handler → Inner → Middle → Outer
//! ```
//!
//! Each layer can transform requests, handle responses, short-circuit
//! execution, add side effects, or manage resources.
//!
//! ## Error Classification
//!
//! Middleware uses [`ErrorCategory`] for structured error handling:
//!
//! - [`ErrorCategory::Transient`] - Retry with backoff
//! - [`ErrorCategory::Permanent`] - Don't retry, may route to dead letter queue
//! - [`ErrorCategory::Terminal`] - System failure, abort processing

use std::convert::Infallible;
use std::error::Error as StdError;
use std::fmt::Debug;
use std::future::Future;
use std::io::Error as IoError;

use crate::consumer::event_context::EventContext;
use crate::consumer::message::{ConsumerMessage, UncommittedMessage};
use crate::consumer::{DemandType, EventHandler, Uncommitted};
use crate::timers::{Trigger, UncommittedTimer};
use crate::{Partition, Topic};

/// Message retry mechanism that loads failed messages from specific Kafka
/// offsets.
pub mod defer;
pub mod log;
pub mod monopolization;
pub mod optional;
pub mod providers;
pub mod retry;
pub mod scheduler;
pub mod shutdown;
pub mod telemetry;
#[cfg(test)]
pub mod test_support;
pub mod timeout;
pub mod topic;

// Re-export providers for backwards compatibility and convenience
pub use providers::{CloneProvider, FallibleCloneProvider};

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

    /// Transforms this middleware stack into a provider by consuming the stack
    /// and terminating it with a fallible handler wrapped in a
    /// `FallibleCloneProvider`.
    ///
    /// This method converts the middleware stack (which implements
    /// `HandlerMiddleware`) into a provider (which implements
    /// `FallibleHandlerProvider`) by terminating the stack with the given
    /// handler.
    ///
    /// # Arguments
    ///
    /// * `handler` - The fallible handler to use as the innermost component.
    ///
    /// # Returns
    ///
    /// A provider that implements `FallibleHandlerProvider`.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use prosody::consumer::middleware::*;
    /// # use prosody::consumer::middleware::retry::*;
    /// # use prosody::consumer::DemandType;
    /// # use prosody::consumer::event_context::EventContext;
    /// # use prosody::consumer::message::ConsumerMessage;
    /// # use prosody::timers::Trigger;
    /// # use std::convert::Infallible;
    /// # #[derive(Clone)]
    /// # struct MyHandler;
    /// # impl FallibleHandler for MyHandler {
    /// #     type Error = Infallible;
    /// #     async fn on_message<C>(&self, _: C, _: ConsumerMessage, _: DemandType) -> Result<(), Self::Error> { Ok(()) }
    /// #     async fn on_timer<C>(&self, _: C, _: Trigger, _: DemandType) -> Result<(), Self::Error> { Ok(()) }
    /// #     async fn shutdown(self) {}
    /// # }
    /// # let config = RetryConfiguration::builder().build().unwrap();
    /// # let my_handler = MyHandler;
    /// let middleware = RetryMiddleware::new(config).unwrap();
    /// let provider = middleware.into_provider(my_handler);
    /// ```
    fn into_provider<H>(self, handler: H) -> Self::Provider<FallibleCloneProvider<H>>
    where
        Self: Sized,
        H: FallibleHandler + Clone + Send + Sync + 'static,
    {
        self.with_provider(FallibleCloneProvider::new(handler))
    }

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
    /// ```rust,no_run
    /// # use prosody::consumer::middleware::*;
    /// # use prosody::consumer::middleware::retry::{RetryMiddleware, RetryConfiguration};
    /// # use prosody::consumer::middleware::shutdown::ShutdownMiddleware;
    /// # let retry_config = RetryConfiguration::builder().build().unwrap();
    /// # let inner_middleware = RetryMiddleware::new(retry_config).unwrap();
    /// # let middle_middleware = ShutdownMiddleware;
    /// # let outer_middleware = ShutdownMiddleware;
    /// // Builds from inner to outer: inner -> middle -> outer
    /// let middleware = inner_middleware
    ///     .layer(middle_middleware) // middle wraps inner
    ///     .layer(outer_middleware); // outer wraps middle+inner
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
    type Error: ClassifyError + StdError + Send;

    /// Handles a message, potentially returning an error.
    ///
    /// # Arguments
    ///
    /// * `context` - The context of the message being processed.
    /// * `message` - The message to be processed.
    /// * `demand_type` - Whether this is normal processing or failure retry.
    ///
    /// # Returns
    ///
    /// A `Future` that resolves to `Ok(())` if the message was processed
    /// successfully, or an `Err` containing the error if processing failed.
    fn on_message<C>(
        &self,
        context: C,
        message: ConsumerMessage,
        demand_type: DemandType,
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
    /// * `demand_type` - Whether this is normal processing or failure retry.
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
        demand_type: DemandType,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send
    where
        C: EventContext;

    /// Shuts down the handler and cleans up any resources.
    ///
    /// This method is called when a Kafka partition is being revoked or the
    /// consumer is shutting down. It allows handlers (including middleware) to:
    /// - Clean up accumulated state
    /// - Close connections or file handles
    /// - Flush pending operations
    /// - Release resources
    ///
    /// For middleware implementations, this should:
    /// 1. Clean up middleware-specific state
    /// 2. Cascade shutdown to the inner handler
    ///
    /// # Arguments
    ///
    /// Takes ownership of `self` to ensure exclusive access during cleanup.
    ///
    /// # Returns
    ///
    /// A [`Future`] that resolves when shutdown is complete.
    ///
    /// # Implementation Requirements
    ///
    /// - **Infallible**: Shutdown should not fail - handle errors gracefully
    /// - **Idempotent**: Safe to call multiple times
    /// - **Complete**: Ensure all resources are cleaned up before returning
    /// - **Cascade**: Middleware should shutdown inner handlers
    fn shutdown(self) -> impl Future<Output = ()> + Send;
}

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

impl<T> EventHandler for T
where
    T: FallibleEventHandler,
    T::Error: ClassifyError,
{
    async fn on_message<C>(&self, context: C, message: UncommittedMessage, demand_type: DemandType)
    where
        C: EventContext,
    {
        let (inner_message, uncommitted_offset) = message.into_inner();

        // Attempt to process the message
        let Err(error) =
            FallibleHandler::on_message(self, context, inner_message, demand_type).await
        else {
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

    async fn on_timer<C, U>(&self, context: C, timer: U, demand_type: DemandType)
    where
        C: EventContext,
        U: UncommittedTimer,
    {
        let (trigger, uncommitted_timer) = timer.into_inner();

        // Attempt to process the timer
        let Err(error) = FallibleHandler::on_timer(self, context, trigger, demand_type).await
        else {
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

    async fn shutdown(self) {
        FallibleHandler::shutdown(self).await;
    }
}

impl ClassifyError for Infallible {
    fn classify_error(&self) -> ErrorCategory {
        ErrorCategory::Terminal
    }
}

impl ClassifyError for IoError {
    fn classify_error(&self) -> ErrorCategory {
        ErrorCategory::Transient
    }
}
