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
//! | [`cancellation`] | Early exit when already cancelled |
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
//! # use prosody::consumer::middleware::cancellation::CancellationMiddleware;
//! # use prosody::consumer::DemandType;
//! # use prosody::consumer::event_context::EventContext;
//! # use prosody::consumer::message::ConsumerMessage;
//! # use prosody::timers::Trigger;
//! # use std::convert::Infallible;
//! # #[derive(Clone)]
//! # struct MyHandler;
//! # impl FallibleHandler for MyHandler {
//! #     type Error = Infallible;
//! #     type Output = ();
//! #     async fn on_message<C>(&self, _: C, _: ConsumerMessage, _: DemandType) -> Result<(), Self::Error> { Ok(()) }
//! #     async fn on_timer<C>(&self, _: C, _: Trigger, _: DemandType) -> Result<(), Self::Error> { Ok(()) }
//! #     async fn shutdown(self) {}
//! # }
//! # let retry_config = RetryConfiguration::builder().build().unwrap();
//! # let inner_middleware = RetryMiddleware::new(retry_config).unwrap();
//! # let middle_middleware = CancellationMiddleware;
//! # let outer_middleware = CancellationMiddleware;
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
//! # use prosody::consumer::middleware::cancellation::CancellationMiddleware;
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
//! #     type Output = ();
//! #     async fn on_message<C>(&self, _: C, _: ConsumerMessage, _: DemandType) -> Result<(), Self::Error> { Ok(()) }
//! #     async fn on_timer<C>(&self, _: C, _: Trigger, _: DemandType) -> Result<(), Self::Error> { Ok(()) }
//! #     async fn shutdown(self) {}
//! # }
//! # let config = SchedulerConfigurationBuilder::default().build().unwrap();
//! # let retry_config = RetryConfiguration::builder().build().unwrap();
//! # let topic_config = FailureTopicConfiguration::builder().failure_topic("dlq").build().unwrap();
//! # let producer_config = ProducerConfiguration::builder().bootstrap_servers(vec!["kafka:9092".to_string()]).build().unwrap();
//! # let producer = ProsodyProducer::new(&producer_config, Telemetry::new().sender()).unwrap();
//! # let telemetry = Telemetry::default();
//! # let my_business_handler = MyHandler;
//!
//! // Low-latency consumer with full error handling
//! let provider = SchedulerMiddleware::new(&config, &telemetry).unwrap()
//!     .layer(CancellationMiddleware)
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
use crate::error::{ClassifyError, ErrorCategory};
use crate::timers::{Trigger, UncommittedTimer};
use crate::{Partition, Topic};

pub mod cancellation;
/// Message retry mechanism that loads failed messages from specific Kafka
/// offsets.
pub mod deduplication;
pub mod defer;
pub mod log;
pub mod monopolization;
pub mod optional;
pub mod providers;
pub mod retry;
pub mod scheduler;
pub mod telemetry;
#[cfg(test)]
pub mod test_support;
pub mod timeout;
pub mod topic;

// Re-export providers for backwards compatibility and convenience
pub use providers::{CloneProvider, FallibleCloneProvider};

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
    /// #     type Output = ();
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
    /// # use prosody::consumer::middleware::cancellation::CancellationMiddleware;
    /// # let retry_config = RetryConfiguration::builder().build().unwrap();
    /// # let inner_middleware = RetryMiddleware::new(retry_config).unwrap();
    /// # let middle_middleware = CancellationMiddleware;
    /// # let outer_middleware = CancellationMiddleware;
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
///
/// # Lifecycle and `Output`
///
/// Each handler call returns a typed `Self::Output` value on success, which
/// the framework then hands back to one of the apply hooks
/// ([`Self::after_commit`] or [`Self::after_abort`]) once the durability marker
/// has been resolved. This gives handlers a 2-phase-commit seam: stage external
/// state inside `on_message`/`on_timer`, return a handle in the `Ok` value, and
/// finalise (or unstage) the staged state in the apply hook with ownership of
/// that handle.
///
/// Most handlers don't need 2PC and set `type Output = ();`. The default
/// `after_commit`/`after_abort` implementations are no-ops that LLVM inlines
/// away for that case.
///
/// # EventHandler-implementor contract
///
/// Any [`EventHandler`] that drives the durability marker
/// ([`Uncommitted::commit`]/[`Uncommitted::abort`] on the inner message or
/// timer) is responsible for invoking the **matching** apply hook on the
/// inner handler before returning:
///
/// - After every `commit().await`, call `inner.after_commit(ctx, result).await`
/// - After every `abort().await`, call `inner.after_abort(ctx, result).await`
///
/// where `result` is the same `Result<Self::Output, Self::Error>` the inner
/// handler chain produced. Failing to do so silently breaks any 2PC handler
/// further down the chain. The blanket `FallibleEventHandler → EventHandler`
/// impl in this module and `RetryHandler` are the existing sites that take
/// this responsibility; any new `EventHandler` that resolves the marker must
/// follow the same pattern.
///
/// [`EventHandler`]: crate::consumer::EventHandler
/// [`Uncommitted::commit`]: crate::consumer::Uncommitted::commit
/// [`Uncommitted::abort`]: crate::consumer::Uncommitted::abort
pub trait FallibleHandler: Send + Sync + 'static {
    /// The error type returned by this handler.
    type Error: ClassifyError + StdError + Send;

    /// Value the handler returns on success and that the apply hooks
    /// ([`Self::after_commit`]/[`Self::after_abort`]) consume once the
    /// durability marker is resolved.
    ///
    /// Most handlers set `type Output = ();`. 2PC handlers carry a staging
    /// handle, a transaction token, or whatever they need to finalise /
    /// unstage. Wrapping middleware threads `Output` through using either
    /// the pass-through pattern (`type Output = Inner::Output`) or the
    /// extending pattern (`type Output = (Inner::Output, MyHandle)`);
    /// never collapse to `()` in middleware — that silently discards the
    /// inner's value and breaks 2PC composition.
    type Output: Send;

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
    /// A `Future` that resolves to `Ok(output)` carrying the typed value the
    /// handler produced on success, or an `Err` containing the error if
    /// processing failed.
    fn on_message<C>(
        &self,
        context: C,
        message: ConsumerMessage,
        demand_type: DemandType,
    ) -> impl Future<Output = Result<Self::Output, Self::Error>> + Send
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
    /// - `Ok(output)` carrying the typed value the handler produced on success
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
    ) -> impl Future<Output = Result<Self::Output, Self::Error>> + Send
    where
        C: EventContext;

    /// Runs after the durability marker has been **committed**, with ownership
    /// of the `Result` produced by the handler chain.
    ///
    /// `Ok(output)` — handler succeeded; finalise staged state.
    /// `Err(error)` — handler returned a Transient or Permanent error; the
    /// marker still committed (offset advance, dedup-insert as applicable).
    ///
    /// Per-key serialised: a same-key follow-up event will not dispatch until
    /// this returns. Failures here cannot redeliver — retry-internally or
    /// accept logging-only semantics.
    ///
    /// Wrapping middleware MUST forward this call to its inner handler with
    /// the inner-typed `Result`; see the trait-level docs for the
    /// `EventHandler` contract that ensures this hook fires.
    fn after_commit<C>(
        &self,
        _context: C,
        _result: Result<Self::Output, Self::Error>,
    ) -> impl Future<Output = ()> + Send
    where
        C: EventContext,
    {
        async {}
    }

    /// Runs after the durability marker has been **aborted**, with ownership
    /// of the `Result` produced by the handler chain.
    ///
    /// `Ok(output)` — handler succeeded but the marker was aborted anyway
    /// (e.g., shutdown intervened between `Ok` and commit; see
    /// `RetryHandler::on_message` shutdown-during-retry path).
    /// `Err(error)` — handler returned a Terminal error, or shutdown aborted
    /// a Transient retry loop.
    ///
    /// Per-key serialised; same retry / cancel-safety constraints as
    /// [`Self::after_commit`]. Same EventHandler-implementor contract.
    fn after_abort<C>(
        &self,
        _context: C,
        _result: Result<Self::Output, Self::Error>,
    ) -> impl Future<Output = ()> + Send
    where
        C: EventContext,
    {
        async {}
    }

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
        let result =
            FallibleHandler::on_message(self, context.clone(), inner_message, demand_type).await;

        if let Err(error) = &result {
            self.on_message_error(error);
            match error.classify_error() {
                ErrorCategory::Transient | ErrorCategory::Permanent => {
                    uncommitted_offset.commit();
                    self.after_commit(context, result).await;
                }
                ErrorCategory::Terminal => {
                    uncommitted_offset.abort();
                    self.after_abort(context, result).await;
                }
            }
        } else {
            uncommitted_offset.commit();
            self.after_commit(context, result).await;
        }
    }

    async fn on_timer<C, U>(&self, context: C, timer: U, demand_type: DemandType)
    where
        C: EventContext,
        U: UncommittedTimer,
    {
        let (trigger, uncommitted_timer) = timer.into_inner();

        // Attempt to process the timer
        let result = FallibleHandler::on_timer(self, context.clone(), trigger, demand_type).await;

        if let Err(error) = &result {
            self.on_timer_error(error);
            match error.classify_error() {
                ErrorCategory::Transient | ErrorCategory::Permanent => {
                    uncommitted_timer.commit().await;
                    self.after_commit(context, result).await;
                }
                ErrorCategory::Terminal => {
                    uncommitted_timer.abort().await;
                    self.after_abort(context, result).await;
                }
            }
        } else {
            uncommitted_timer.commit().await;
            self.after_commit(context, result).await;
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

#[cfg(test)]
mod after_hook_tests {
    //! Tests for the `after_commit` / `after_abort` lifecycle hooks plumbed
    //! through the blanket `FallibleEventHandler → EventHandler` impl.
    //!
    //! These tests exercise the full lifecycle: handler runs → blanket impl
    //! resolves the marker → matching apply hook fires with the correct
    //! `Result<Output, Error>` value.
    use std::error::Error as StdError;
    use std::fmt::{Display, Formatter, Result as FmtResult};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use crossbeam_utils::CachePadded;
    use parking_lot::Mutex;

    use super::*;
    use crate::consumer::EventHandler;
    use crate::consumer::message::ConsumerMessage;
    use crate::consumer::middleware::test_support::MockEventContext;
    use crate::consumer::partition::offsets::OffsetTracker;
    use crate::error::ErrorCategory;
    use crate::timers::TimerType;
    use crate::timers::Trigger;
    use crate::timers::datetime::CompactDateTime;

    /// Test error with a fixed classification. Equality compares the
    /// classification discriminant + tag, since `ErrorCategory` itself is
    /// only `Copy + Clone + Debug + Serialize`.
    #[derive(Debug, Clone)]
    struct TestError(ErrorCategory, &'static str);

    impl Display for TestError {
        fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
            write!(f, "test error ({}): {:?}", self.1, self.0)
        }
    }

    impl StdError for TestError {}

    impl ClassifyError for TestError {
        fn classify_error(&self) -> ErrorCategory {
            self.0
        }
    }

    impl PartialEq for TestError {
        fn eq(&self, other: &Self) -> bool {
            // Compare classification discriminants + tags.
            let cat_eq = matches!(
                (self.0, other.0),
                (ErrorCategory::Transient, ErrorCategory::Transient)
                    | (ErrorCategory::Permanent, ErrorCategory::Permanent)
                    | (ErrorCategory::Terminal, ErrorCategory::Terminal)
            );
            cat_eq && self.1 == other.1
        }
    }

    impl Eq for TestError {}

    impl FallibleEventHandler for ProbeHandler {}

    /// Records every lifecycle hook firing for later assertion.
    #[derive(Debug, Clone, PartialEq, Eq)]
    enum HookEvent {
        Handler,
        AfterCommit(Result<u64, TestError>),
        AfterAbort(Result<u64, TestError>),
    }

    /// Probe handler whose `Output` is a `u64` sentinel; records every
    /// lifecycle hook into a shared log.
    #[derive(Clone)]
    struct ProbeHandler {
        sentinel: u64,
        result: Result<(), TestError>,
        log: Arc<Mutex<Vec<HookEvent>>>,
    }

    impl ProbeHandler {
        fn ok(sentinel: u64) -> Self {
            Self {
                sentinel,
                result: Ok(()),
                log: Arc::default(),
            }
        }

        fn err(sentinel: u64, error: TestError) -> Self {
            Self {
                sentinel,
                result: Err(error),
                log: Arc::default(),
            }
        }
    }

    impl FallibleHandler for ProbeHandler {
        type Error = TestError;
        type Output = u64;

        async fn on_message<C>(
            &self,
            _context: C,
            _message: ConsumerMessage,
            _demand_type: DemandType,
        ) -> Result<Self::Output, Self::Error>
        where
            C: EventContext,
        {
            self.log.lock().push(HookEvent::Handler);
            self.result.clone().map(|()| self.sentinel)
        }

        async fn on_timer<C>(
            &self,
            _context: C,
            _trigger: Trigger,
            _demand_type: DemandType,
        ) -> Result<Self::Output, Self::Error>
        where
            C: EventContext,
        {
            self.log.lock().push(HookEvent::Handler);
            self.result.clone().map(|()| self.sentinel)
        }

        async fn after_commit<C>(&self, _context: C, result: Result<Self::Output, Self::Error>)
        where
            C: EventContext,
        {
            self.log.lock().push(HookEvent::AfterCommit(result));
        }

        async fn after_abort<C>(&self, _context: C, result: Result<Self::Output, Self::Error>)
        where
            C: EventContext,
        {
            self.log.lock().push(HookEvent::AfterAbort(result));
        }

        async fn shutdown(self) {}
    }

    fn make_offset_tracker() -> OffsetTracker {
        let version = Arc::new(CachePadded::new(AtomicUsize::new(0)));
        OffsetTracker::new("test-topic".into(), 0, 10, Duration::from_secs(5), version)
    }

    fn make_test_message() -> Option<ConsumerMessage> {
        use crate::consumer::message::ConsumerMessageValue;
        use std::sync::Arc;
        use tokio::sync::Semaphore;
        let semaphore = Arc::new(Semaphore::new(10));
        let permit = semaphore.try_acquire_owned().ok()?;
        Some(ConsumerMessage::new(
            ConsumerMessageValue::default(),
            tracing::Span::current(),
            permit,
        ))
    }

    #[tokio::test]
    async fn after_commit_fires_with_ok_output_after_handler_success() -> color_eyre::Result<()> {
        let handler = ProbeHandler::ok(42);
        let log = handler.log.clone();
        let context = MockEventContext::new();
        let tracker = make_offset_tracker();
        let uncommitted_offset = tracker.take(0).await?;
        let message = make_test_message()
            .ok_or_else(|| color_eyre::eyre::eyre!("failed to construct test message"))?
            .into_uncommitted(uncommitted_offset);

        EventHandler::on_message(&handler, context, message, DemandType::Normal).await;

        assert_eq!(
            log.lock().clone(),
            vec![HookEvent::Handler, HookEvent::AfterCommit(Ok(42))],
            "handler runs first, then after_commit with Ok(sentinel)",
        );
        Ok(())
    }

    #[tokio::test]
    async fn after_commit_fires_with_err_after_permanent_error() -> color_eyre::Result<()> {
        let err = TestError(ErrorCategory::Permanent, "permanent");
        let handler = ProbeHandler::err(0, err.clone());
        let log = handler.log.clone();
        let context = MockEventContext::new();
        let tracker = make_offset_tracker();
        let uncommitted_offset = tracker.take(0).await?;
        let message = make_test_message()
            .ok_or_else(|| color_eyre::eyre::eyre!("failed to construct test message"))?
            .into_uncommitted(uncommitted_offset);

        EventHandler::on_message(&handler, context, message, DemandType::Normal).await;

        assert_eq!(
            log.lock().clone(),
            vec![HookEvent::Handler, HookEvent::AfterCommit(Err(err))],
            "Permanent error commits the marker; after_commit fires with Err",
        );
        Ok(())
    }

    #[tokio::test]
    async fn after_commit_fires_with_err_after_transient_error() -> color_eyre::Result<()> {
        // Transient (when no retry middleware is in front) commits like
        // Permanent at the blanket-impl level.
        let err = TestError(ErrorCategory::Transient, "transient");
        let handler = ProbeHandler::err(0, err.clone());
        let log = handler.log.clone();
        let context = MockEventContext::new();
        let tracker = make_offset_tracker();
        let uncommitted_offset = tracker.take(0).await?;
        let message = make_test_message()
            .ok_or_else(|| color_eyre::eyre::eyre!("failed to construct test message"))?
            .into_uncommitted(uncommitted_offset);

        EventHandler::on_message(&handler, context, message, DemandType::Normal).await;

        assert_eq!(
            log.lock().clone(),
            vec![HookEvent::Handler, HookEvent::AfterCommit(Err(err))],
            "Transient error at the blanket-impl level commits + fires after_commit",
        );
        Ok(())
    }

    #[tokio::test]
    async fn after_abort_fires_with_err_after_terminal_error() -> color_eyre::Result<()> {
        let err = TestError(ErrorCategory::Terminal, "terminal");
        let handler = ProbeHandler::err(0, err.clone());
        let log = handler.log.clone();
        let context = MockEventContext::new();
        let tracker = make_offset_tracker();
        let uncommitted_offset = tracker.take(0).await?;
        let message = make_test_message()
            .ok_or_else(|| color_eyre::eyre::eyre!("failed to construct test message"))?
            .into_uncommitted(uncommitted_offset);

        EventHandler::on_message(&handler, context, message, DemandType::Normal).await;

        assert_eq!(
            log.lock().clone(),
            vec![HookEvent::Handler, HookEvent::AfterAbort(Err(err))],
            "Terminal error aborts the marker; after_abort fires with Err",
        );
        Ok(())
    }

    #[tokio::test]
    async fn hook_1_to_1_invariant_one_apply_per_dispatch() -> color_eyre::Result<()> {
        // Exactly one apply hook (after_commit OR after_abort) fires per
        // dispatch — not both, not neither.
        for category in [
            ErrorCategory::Permanent,
            ErrorCategory::Transient,
            ErrorCategory::Terminal,
        ] {
            let handler = ProbeHandler::err(0, TestError(category, "x"));
            let log = handler.log.clone();
            let context = MockEventContext::new();
            let tracker = make_offset_tracker();
            let uncommitted_offset = tracker.take(0).await?;
            let message = make_test_message()
                .ok_or_else(|| color_eyre::eyre::eyre!("failed to construct test message"))?
                .into_uncommitted(uncommitted_offset);

            EventHandler::on_message(&handler, context, message, DemandType::Normal).await;

            let recorded = log.lock().clone();
            let commit_count = recorded
                .iter()
                .filter(|e| matches!(e, HookEvent::AfterCommit(_)))
                .count();
            let abort_count = recorded
                .iter()
                .filter(|e| matches!(e, HookEvent::AfterAbort(_)))
                .count();
            assert_eq!(
                commit_count + abort_count,
                1,
                "{category:?}: exactly one apply hook should fire per dispatch ({recorded:?})",
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn after_commit_for_timer_path_with_ok_output() {
        // Timer arm of the blanket impl: build a minimal `UncommittedTimer`
        // and verify the same lifecycle.
        use std::sync::OnceLock;

        use crate::Key;
        use crate::consumer::{Keyed, Uncommitted};
        use crate::timers::UncommittedTimer;

        struct MockUncommittedTimer {
            committed: Arc<AtomicUsize>,
            aborted: Arc<AtomicUsize>,
        }

        struct MockGuard {
            committed: Arc<AtomicUsize>,
            aborted: Arc<AtomicUsize>,
        }

        impl Uncommitted for MockGuard {
            async fn commit(self) {
                self.committed.fetch_add(1, Ordering::SeqCst);
            }

            async fn abort(self) {
                self.aborted.fetch_add(1, Ordering::SeqCst);
            }
        }

        impl Keyed for MockUncommittedTimer {
            type Key = Key;

            fn key(&self) -> &Self::Key {
                static KEY: OnceLock<Key> = OnceLock::new();
                KEY.get_or_init(|| "test-key".into())
            }
        }

        impl Uncommitted for MockUncommittedTimer {
            async fn commit(self) {
                self.committed.fetch_add(1, Ordering::SeqCst);
            }

            async fn abort(self) {
                self.aborted.fetch_add(1, Ordering::SeqCst);
            }
        }

        impl UncommittedTimer for MockUncommittedTimer {
            type CommitGuard = MockGuard;

            fn time(&self) -> CompactDateTime {
                CompactDateTime::from(0_u32)
            }

            fn timer_type(&self) -> TimerType {
                TimerType::Application
            }

            fn span(&self) -> tracing::Span {
                tracing::Span::current()
            }

            fn into_inner(self) -> (Trigger, Self::CommitGuard) {
                let trigger =
                    Trigger::for_testing("test-key".into(), self.time(), self.timer_type());
                let guard = MockGuard {
                    committed: self.committed.clone(),
                    aborted: self.aborted.clone(),
                };
                (trigger, guard)
            }
        }

        let handler = ProbeHandler::ok(99);
        let log = handler.log.clone();
        let context = MockEventContext::new();
        let committed = Arc::new(AtomicUsize::new(0));
        let aborted = Arc::new(AtomicUsize::new(0));
        let timer = MockUncommittedTimer {
            committed: committed.clone(),
            aborted: aborted.clone(),
        };

        EventHandler::on_timer(&handler, context, timer, DemandType::Normal).await;

        assert_eq!(committed.load(Ordering::SeqCst), 1, "marker committed once");
        assert_eq!(aborted.load(Ordering::SeqCst), 0, "marker not aborted");
        assert_eq!(
            log.lock().clone(),
            vec![HookEvent::Handler, HookEvent::AfterCommit(Ok(99))],
            "timer Ok path: handler then after_commit with sentinel",
        );
    }

    /// Minimal pass-through middleware to verify the composition contract.
    /// Stands in for any real `FallibleHandler` middleware that wraps an
    /// inner with `type Output = Inner::Output` and forwards apply hooks.
    struct PassThroughMiddleware<T> {
        inner: T,
    }

    impl<T> FallibleHandler for PassThroughMiddleware<T>
    where
        T: FallibleHandler,
    {
        type Error = T::Error;
        type Output = T::Output;

        async fn on_message<C>(
            &self,
            context: C,
            message: ConsumerMessage,
            demand_type: DemandType,
        ) -> Result<Self::Output, Self::Error>
        where
            C: EventContext,
        {
            self.inner.on_message(context, message, demand_type).await
        }

        async fn on_timer<C>(
            &self,
            context: C,
            trigger: Trigger,
            demand_type: DemandType,
        ) -> Result<Self::Output, Self::Error>
        where
            C: EventContext,
        {
            self.inner.on_timer(context, trigger, demand_type).await
        }

        async fn after_commit<C>(&self, context: C, result: Result<Self::Output, Self::Error>)
        where
            C: EventContext,
        {
            self.inner.after_commit(context, result).await;
        }

        async fn after_abort<C>(&self, context: C, result: Result<Self::Output, Self::Error>)
        where
            C: EventContext,
        {
            self.inner.after_abort(context, result).await;
        }

        async fn shutdown(self) {
            self.inner.shutdown().await;
        }
    }

    impl<T> FallibleEventHandler for PassThroughMiddleware<T> where T: FallibleHandler {}

    #[tokio::test]
    async fn pass_through_middleware_forwards_output_to_inner_after_commit()
    -> color_eyre::Result<()> {
        let inner = ProbeHandler::ok(7);
        let log = inner.log.clone();
        let middleware = PassThroughMiddleware { inner };
        let context = MockEventContext::new();
        let tracker = make_offset_tracker();
        let uncommitted_offset = tracker.take(0).await?;
        let message = make_test_message()
            .ok_or_else(|| color_eyre::eyre::eyre!("failed to construct test message"))?
            .into_uncommitted(uncommitted_offset);

        EventHandler::on_message(&middleware, context, message, DemandType::Normal).await;

        // The inner handler observes both Handler (it ran) and AfterCommit
        // (the middleware forwarded with Ok(7)).
        assert_eq!(
            log.lock().clone(),
            vec![HookEvent::Handler, HookEvent::AfterCommit(Ok(7))],
            "pass-through middleware forwards typed output unchanged",
        );
        Ok(())
    }

    #[tokio::test]
    async fn pass_through_middleware_forwards_after_abort_on_terminal() -> color_eyre::Result<()> {
        let err = TestError(ErrorCategory::Terminal, "terminal");
        let inner = ProbeHandler::err(0, err.clone());
        let log = inner.log.clone();
        let middleware = PassThroughMiddleware { inner };
        let context = MockEventContext::new();
        let tracker = make_offset_tracker();
        let uncommitted_offset = tracker.take(0).await?;
        let message = make_test_message()
            .ok_or_else(|| color_eyre::eyre::eyre!("failed to construct test message"))?
            .into_uncommitted(uncommitted_offset);

        EventHandler::on_message(&middleware, context, message, DemandType::Normal).await;

        assert_eq!(
            log.lock().clone(),
            vec![HookEvent::Handler, HookEvent::AfterAbort(Err(err))],
            "pass-through middleware forwards after_abort on terminal",
        );
        Ok(())
    }
}
