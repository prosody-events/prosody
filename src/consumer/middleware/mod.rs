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
//! #     type Payload = serde_json::Value;
//! #     type Error = Infallible;
//! #     type Output = ();
//! #     async fn on_message<C>(&self, _: C, _: ConsumerMessage<serde_json::Value>, _: DemandType) -> Result<(), Self::Error> { Ok(()) }
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
//! #     type Payload = serde_json::Value;
//! #     type Error = Infallible;
//! #     type Output = ();
//! #     async fn on_message<C>(&self, _: C, _: ConsumerMessage<serde_json::Value>, _: DemandType) -> Result<(), Self::Error> { Ok(()) }
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
    /// #     type Payload = serde_json::Value;
    /// #     type Error = Infallible;
    /// #     type Output = ();
    /// #     async fn on_message<C>(&self, _: C, _: ConsumerMessage<serde_json::Value>, _: DemandType) -> Result<(), Self::Error> { Ok(()) }
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

/// A handler for Kafka messages and scheduled timers — the application's
/// integration point with a Prosody consumer.
///
/// Implementing this trait wires your business logic into the consumer
/// pipeline. You provide:
///
/// - An [`Error`](Self::Error) type whose variants are classified by
///   [`ClassifyError`] (drives the framework's retry vs. give-up vs. shutdown
///   decisions — see [Error classification](#error-classification)).
/// - [`on_message`](Self::on_message) — called for each Kafka message delivered
///   to the consumer.
/// - [`on_timer`](Self::on_timer) — called for each scheduled timer that fires
///   (when the consumer is configured with a timer system).
/// - [`shutdown`](Self::shutdown) — called when the consumer stops or a
///   partition is revoked.
///
/// Two optional pieces extend the basic shape for handlers that stage
/// external work during processing:
///
/// - [`Output`](Self::Output) — a typed value the handler returns on success
///   (defaults to `()`); carried into the apply hook.
/// - [`after_commit`](Self::after_commit) and
///   [`after_abort`](Self::after_abort) — apply hooks that fire after the
///   framework decides whether the just-completed invocation will be retried,
///   enabling 2-phase-commit workflows.
///
/// The consumer pipeline is itself a stack of `FallibleHandler` impls
/// (retry, deduplication, defer, telemetry, your handler at the bottom),
/// so middleware authors implement this same trait. The [module-level
/// docs](self) cover how impls are composed (layering, providers,
/// execution flow); [Implementing as middleware](#implementing-as-middleware)
/// below covers what an individual middleware impl owes its inner
/// handler.
///
/// # Error classification
///
/// Every error returned by `on_message` or `on_timer` is routed by
/// [`ClassifyError::classify_error`] into one of three categories that
/// determine how the consumer pipeline reacts:
///
/// - [`Transient`](ErrorCategory::Transient) — **retry**. A temporary problem
///   (network blip, store timeout, downstream service unavailable) that may
///   succeed later. The retry middleware reattempts; if configured, the defer
///   middleware can move the message to a timer-based retry to unblock the
///   partition.
/// - [`Permanent`](ErrorCategory::Permanent) — **give up on this message**. The
///   data itself is bad (deserialization failure, schema violation, business
///   rule rejection) and retrying won't help. The message is committed, and may
///   be routed to a dead-letter topic if the failure-topic middleware is
///   configured.
/// - [`Terminal`](ErrorCategory::Terminal) — **shut the consumer down**. The
///   process can't safely continue (corrupted local state, an invariant
///   violation) and a new instance must take over.
///
/// The classification is the contract between your handler and the
/// framework: pick the right category and the middleware stack handles
/// the rest.
///
/// # Apply hooks (optional)
///
/// Apply hooks let a handler stage external state during processing and
/// finalize or roll it back once the framework decides whether the
/// invocation will be retried. Most handlers don't need them — leave
/// [`Output`](Self::Output) as `()` and the default no-op hooks suffice.
///
/// Every `on_message` / `on_timer` invocation that runs and returns is
/// paired with one apply hook on the same handler instance:
///
/// - [`after_commit`](Self::after_commit) — the invocation is **final**. The
///   same logical message/timer will not be dispatched to this handler again.
/// - [`after_abort`](Self::after_abort) — the invocation is **not final**. The
///   same logical message/timer **will** be dispatched again (in-process retry,
///   deferred retry via a timer, or a re-poll after the durability marker
///   aborted).
///
/// Hook firing is best-effort: a process crash, unavailable bookkeeping
/// storage, or a middleware above the handler that cannot determine the
/// work outcome can skip the hook. If an invocation never runs (e.g.
/// dispatch was short-circuited above the handler), neither hook fires
/// for it. Handlers must be idempotent and must not depend on apply
/// hooks for correctness or data integrity.
///
/// The choice between the two hooks is the **work outcome of that single
/// invocation** — "is the handler going to be invoked again for this same
/// logical event?" — not what happened to the durability marker (the
/// Kafka offset commit or timer commit). The framework, including any
/// defer / retry / rescue middleware in the stack, is the source of
/// truth for that distinction. A defer middleware that commits the Kafka
/// offset to schedule a deferred retry, for example, still pairs the
/// just-completed invocation with `after_abort` — another invocation is
/// coming.
///
/// # `Output` and 2-phase commit
///
/// Each invocation returns a typed [`Self::Output`] on success, which the
/// framework hands to the matching apply hook for that invocation. This
/// gives handlers a 2-phase-commit seam: stage external state inside
/// `on_message` / `on_timer`, return a staging handle as the `Ok` value,
/// and finalize (in `after_commit`) or unstage (in `after_abort`) that
/// state in the paired hook.
///
/// # Implementing as middleware
///
/// You can skip this section if your handler sits at the bottom of the
/// stack. It covers what a `FallibleHandler` middleware (a wrapper around
/// an inner handler) must do.
///
/// 1. **Forward the handler methods.** Call `self.inner.on_message(...)` and
///    `self.inner.on_timer(...)` (await them), then decide whether to
///    short-circuit, transform the result, or pass it through. Cascade
///    `shutdown` by awaiting `self.inner.shutdown()` so inner resources are
///    released.
///
/// 2. **Wrap the inner's error.** Define an enum like `enum MyError<E> {
///    Inner(E), MyOwn(...) }`. Implement [`ClassifyError`] for it by delegating
///    `Inner` to the wrapped error's classification and classifying your own
///    variants explicitly — see [Error classification](#error-classification)
///    for the categories.
///
/// 3. **Thread `Output`.** Use `type Output = Inner::Output` if you don't add
///    staging of your own, or `type Output = (Inner::Output, MyHandle)` if you
///    do. Short-circuiting middleware that may skip the inner (deduplication,
///    filtering) typically encodes the skip in the Output type — `type Output =
///    Option<Inner::Output>`, with `None` meaning the inner did not run.
///    **Never collapse to `()`** in middleware: that discards the inner's value
///    and breaks 2PC handlers downstream.
///
/// 4. **Route apply hooks.** Fire exactly one apply hook on the inner per inner
///    invocation that ran, chosen by the per-invocation work outcome (see
///    [Apply hooks](#apply-hooks-optional)). Pass the inner's typed `Result`
///    through unchanged — never coerce errors to `Ok` or drop them, since that
///    silently breaks 2PC handlers below. An in-process retry loop that runs N
///    attempts must fire N hooks: the first N-1 are `after_abort` and the last
///    matches the terminal outcome. Hooks are never coalesced.
///
///    On a skip path (the inner did not run), suppress both apply hooks
///    on the inner — there's no invocation to pair them with.
///
/// The blanket `FallibleEventHandler → EventHandler` impl is the default
/// durability boundary for handlers without rescue middleware: each
/// invocation maps 1:1 to a single dispatch, and resolving the marker
/// coincides with the work outcome (commit ⇒ final invocation, abort ⇒
/// re-dispatch on the next poll, which produces a new invocation with
/// its own apply hook). [`retry`], defer, and topic middlewares take on
/// this responsibility for richer compositions.
///
/// [`EventHandler`]: crate::consumer::EventHandler
/// [`Uncommitted::commit`]: crate::consumer::Uncommitted::commit
/// [`Uncommitted::abort`]: crate::consumer::Uncommitted::abort
/// [`retry`]: crate::consumer::middleware::retry
pub trait FallibleHandler: Send + Sync + 'static {
    /// The payload type this handler processes.
    ///
    /// Must match the codec's `Payload` type so the consumer pipeline can
    /// deliver typed messages. Set to `serde_json::Value` for JSON consumers.
    type Payload: Send + Sync + 'static;

    /// Error type returned by [`Self::on_message`] / [`Self::on_timer`].
    ///
    /// Must implement [`ClassifyError`] so the framework can decide
    /// whether to retry, give up, or shut down for each variant — see
    /// [Error classification](#error-classification) on the trait docs.
    /// Use [`std::convert::Infallible`] for handlers that cannot fail.
    type Error: ClassifyError + StdError + Send;

    /// Success value produced by [`Self::on_message`] / [`Self::on_timer`]
    /// and handed to the matching apply hook for that invocation. This is
    /// the staging handle in a 2-phase-commit workflow — a transaction
    /// token, a deferred-write handle, or whatever the handler needs to
    /// finalize (in [`Self::after_commit`]) or unstage (in
    /// [`Self::after_abort`]).
    ///
    /// Most handlers don't need 2PC and set `type Output = ();`.
    /// Middleware authors threading `Output` through a wrapper: see
    /// [Implementing as middleware](#implementing-as-middleware) on the
    /// trait docs for the pass-through and extending-tuple patterns.
    type Output: Send;

    /// Handles a Kafka message, returning a typed [`Result`].
    ///
    /// On success, the `Ok` value is forwarded to the matching apply hook
    /// ([`Self::after_commit`] or [`Self::after_abort`], depending on
    /// whether this consumer will dispatch the same logical message
    /// again). Use it to carry a staging handle for 2-phase-commit
    /// workflows; set `Output = ()` if no staging is needed.
    ///
    /// Errors are routed by their [`ClassifyError`] category — see
    /// [Error classification](#error-classification) on the trait docs.
    /// Implementations should be idempotent: the same logical message
    /// may be redelivered after a retry, defer, or rebalance, and apply
    /// hooks may not fire on every invocation.
    ///
    /// `demand_type` distinguishes a fresh dispatch from a failure-driven
    /// retry, which implementations can use for backpressure or
    /// observability.
    fn on_message<C>(
        &self,
        context: C,
        message: ConsumerMessage<Self::Payload>,
        demand_type: DemandType,
    ) -> impl Future<Output = Result<Self::Output, Self::Error>> + Send
    where
        C: EventContext;

    /// Handles a fired timer trigger, returning a typed [`Result`].
    ///
    /// On success, the `Ok` value is forwarded to the matching apply hook
    /// ([`Self::after_commit`] or [`Self::after_abort`], depending on
    /// whether the same trigger will be redelivered). Use it to carry a
    /// staging handle for 2-phase-commit workflows; set `Output = ()` if
    /// no staging is needed.
    ///
    /// Errors are routed by their [`ClassifyError`] category — see
    /// [Error classification](#error-classification) on the trait docs.
    /// Implementations should be idempotent: a trigger can be redelivered
    /// if processing is interrupted, and apply hooks may not fire on
    /// every invocation.
    ///
    /// `demand_type` distinguishes a fresh dispatch from a failure-driven
    /// retry, which implementations can use for backpressure or
    /// observability.
    fn on_timer<C>(
        &self,
        context: C,
        trigger: Trigger,
        demand_type: DemandType,
    ) -> impl Future<Output = Result<Self::Output, Self::Error>> + Send
    where
        C: EventContext;

    /// Finalizes staged work after the just-completed invocation has been
    /// committed.
    ///
    /// Called after [`Self::on_message`] / [`Self::on_timer`] when this
    /// consumer will not invoke the handler again for the same logical
    /// event. Use this hook to finalize any external state that was staged
    /// during the invocation (the second phase of a 2-phase-commit
    /// pattern).
    ///
    /// The hook receives the exact [`Result`] the invocation returned. An
    /// `Ok` carries the staged value to commit; an `Err` means the event
    /// has been given up on (e.g. routed to a DLQ, dropped after a
    /// `Permanent` classification, or recorded as a duplicate) and any
    /// staged work should be cleaned up.
    ///
    /// # Delivery guarantees
    ///
    /// **Fires once per handler invocation that ran and returned**, paired
    /// with the invocation's typed `Result`. Each invocation pairs with
    /// either this hook or [`Self::after_abort`] — never both, and never
    /// coalesced across multiple invocations.
    ///
    /// **Best-effort.** The hook may not fire if the process crashes, the
    /// framework's bookkeeping storage is unavailable, or middleware above
    /// this handler cannot determine the work outcome. **Handlers must be
    /// idempotent and must not rely on this hook for correctness or data
    /// integrity** — recovery on the next dispatch must reach the same end
    /// state without depending on the hook having fired.
    ///
    /// # Behavior
    ///
    /// "Final" describes the per-invocation work outcome, not the
    /// durability marker. `after_commit` is the right hook whenever no
    /// further invocation is coming for this logical event — for example,
    /// after a successful invocation, a permanent error that won't be
    /// retried, or rescue middleware committing the marker on the
    /// handler's behalf (DLQ routing, deduplication).
    ///
    /// Per-key serialized: the next event for the same key will not
    /// dispatch until this hook returns. Because the hook returns no
    /// error, internal failures (e.g. a staged write that won't
    /// finalize) cannot trigger a redelivery — handle them within the
    /// hook or log and move on.
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

    /// Rolls back staged work after the just-completed invocation, before
    /// the next attempt runs.
    ///
    /// Called after [`Self::on_message`] / [`Self::on_timer`] when this
    /// consumer **will** invoke the handler again for the same logical
    /// event — another attempt is coming (an in-process retry, a deferred
    /// retry via a timer, or a re-poll after the durability marker
    /// aborted). Use this hook to unstage any external state from the
    /// invocation so the next attempt starts clean.
    ///
    /// The hook receives the exact [`Result`] the invocation returned —
    /// typically an `Err` carrying the failure that triggered the retry,
    /// but it can also be `Ok` if something above the handler decided the
    /// work must be redone anyway (e.g. a shutdown intervened between
    /// success and the durability commit).
    ///
    /// # Delivery guarantees
    ///
    /// **Fires once per handler invocation that ran and returned**, paired
    /// with the invocation's typed `Result`. Each invocation pairs with
    /// either this hook or [`Self::after_commit`]. In particular, an
    /// in-process retry session that runs N attempts produces N apply hook
    /// firings — the first N-1 are `after_abort` and the last matches the
    /// terminal outcome — never coalesced into a single hook at the end.
    ///
    /// **Best-effort.** The hook may not fire if the process crashes, the
    /// framework's bookkeeping storage is unavailable, or middleware above
    /// this handler cannot determine the work outcome. **Handlers must be
    /// idempotent and must not rely on this hook for correctness or data
    /// integrity.**
    ///
    /// # Behavior
    ///
    /// "Not final" describes the per-invocation work outcome, not the
    /// durability marker. The marker may have been aborted, or it may have
    /// been committed by rescue middleware that scheduled a redelivery —
    /// either way, `after_abort` is the right hook because the inner is
    /// going to run again.
    ///
    /// Per-key serialized; same constraints as [`Self::after_commit`].
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

    /// Shuts the handler down on consumer stop or partition revocation.
    ///
    /// Use this to flush pending work, close connections, and release
    /// resources owned by the handler. Takes `self` by value, so the
    /// handler is consumed; the framework expects shutdown to complete
    /// before this returns.
    ///
    /// The method returns no error — handle internal failures within the
    /// implementation (log and move on if necessary).
    ///
    /// Middleware implementations must cascade by calling
    /// `inner.shutdown().await`; otherwise the inner handler's resources
    /// leak.
    fn shutdown(self) -> impl Future<Output = ()> + Send;
}

/// A composition of two middleware components.
#[derive(Clone, Debug)]
pub struct ComposedMiddleware<M1, M2>(M1, M2);

/// Provides default `EventHandler` implementation for types that implement
/// `FallibleHandler`.
///
/// This is the **default durability boundary**: with no rescue / defer /
/// retry middleware below it, each `EventHandler::on_message` /
/// `EventHandler::on_timer` call performs **exactly one** invocation of
/// the inner `FallibleHandler` and pairs it with **exactly one** apply
/// hook firing — satisfying the strictly-per-invocation invariant
/// required by the [`FallibleHandler`] apply-hook contract:
///
/// 1. Extract inner message/timer and the uncommitted offset/timer.
/// 2. Invoke the `FallibleHandler` method **once**.
/// 3. On `Ok`, `Permanent`, or `Transient`: commit the marker — this invocation
///    is final from the consumer's POV — and call `after_commit` with the typed
///    result.
/// 4. On `Terminal`: abort the marker — the message/timer will be redelivered
///    on the next poll, producing a **new** invocation that will get its own
///    apply hook — and call `after_abort` with the typed error.
///
/// Because there is exactly one inner invocation per call into this impl,
/// the per-invocation invariant is satisfied trivially: one invocation
/// pairs with one apply-hook firing.
///
/// Types can override the default implementations to add custom behavior
/// like logging. Implementations that change the work-outcome shape (e.g.
/// rescue an inner error and schedule a retry while still committing the
/// marker), or any wrapping `FallibleHandler` middleware that re-invokes
/// the inner more than once per outer call, MUST NOT use this default;
/// they must drive the apply hooks according to the per-invocation rule
/// in the [`FallibleHandler`] docs — one hook per inner invocation, never
/// coalesced.
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
    type Payload = T::Payload;

    async fn on_message<C>(
        &self,
        context: C,
        message: UncommittedMessage<Self::Payload>,
        demand_type: DemandType,
    ) where
        C: EventContext,
    {
        let (inner_message, uncommitted_offset) = message.into_inner();

        // Per-invocation correctness: this method invokes the inner
        // `FallibleHandler::on_message` EXACTLY ONCE below, and every
        // control-flow path through the rest of this function fires
        // EXACTLY ONE apply hook (`after_commit` or `after_abort`)
        // carrying that single invocation's `Result`. One inner
        // invocation, one apply-hook firing — the strictly-per-invocation
        // invariant is satisfied trivially here.
        let result =
            FallibleHandler::on_message(self, context.clone(), inner_message, demand_type).await;

        // Pick the apply hook by the per-invocation work outcome. At this
        // default boundary (no rescue middleware below us) committing the
        // offset coincides with "this invocation is final" and aborting
        // it coincides with "this invocation is not final — the broker
        // will redeliver and produce a fresh invocation that gets its
        // own apply hook".
        if let Err(error) = &result {
            self.on_message_error(error);
            match error.classify_error() {
                ErrorCategory::Transient | ErrorCategory::Permanent => {
                    // Final from this consumer's POV: nothing below us is
                    // going to redeliver this message into the handler,
                    // so this invocation pairs with `after_commit`.
                    uncommitted_offset.commit();
                    self.after_commit(context, result).await;
                }
                ErrorCategory::Terminal => {
                    // Not final: aborting the offset means the broker will
                    // redeliver this message to this handler on the next
                    // poll, producing a new invocation. This invocation
                    // pairs with `after_abort`; the future invocation
                    // will get its own apply hook.
                    uncommitted_offset.abort();
                    self.after_abort(context, result).await;
                }
            }
        } else {
            // Success: final invocation, pair with `after_commit`.
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

        // Per-invocation correctness: as with the message arm, the inner
        // `FallibleHandler::on_timer` is invoked EXACTLY ONCE below, and
        // every control-flow path through the rest of this function fires
        // EXACTLY ONE apply hook carrying that single invocation's
        // `Result`. One inner invocation, one apply-hook firing.
        let result = FallibleHandler::on_timer(self, context.clone(), trigger, demand_type).await;

        // Same per-invocation work-outcome rule as the message arm: at
        // this default boundary, committing the timer marker coincides
        // with "this invocation is final" and aborting coincides with
        // "this invocation is not final — the timer will fire again,
        // producing a fresh invocation that gets its own apply hook".
        if let Err(error) = &result {
            self.on_timer_error(error);
            match error.classify_error() {
                ErrorCategory::Transient | ErrorCategory::Permanent => {
                    // Final invocation: no further invocation of the
                    // handler is coming for this trigger.
                    uncommitted_timer.commit().await;
                    self.after_commit(context, result).await;
                }
                ErrorCategory::Terminal => {
                    // Not final: aborting leaves the timer in place to
                    // fire again, so `after_abort` is the matching hook
                    // for this invocation. The next firing will produce
                    // a new invocation paired with its own apply hook.
                    uncommitted_timer.abort().await;
                    self.after_abort(context, result).await;
                }
            }
        } else {
            // Success: final invocation, pair with `after_commit`.
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
    //! Tests for the `after_commit` / `after_abort` apply hooks plumbed
    //! through the blanket `FallibleEventHandler → EventHandler` impl.
    //!
    //! These tests pin down the strictly-per-invocation apply-hook
    //! invariant for the default durability boundary: for every inner
    //! invocation of `on_message` / `on_timer` that runs and returns,
    //! exactly one of `after_commit` / `after_abort` fires, carrying the
    //! handler's typed `Result<Output, Error>` for that invocation. At
    //! this boundary (no rescue / defer / retry middleware in the stack)
    //! each call into `EventHandler::on_message` performs exactly one
    //! inner invocation, so the per-invocation invariant collapses to
    //! "one apply hook per call": `Ok` / `Permanent` / `Transient` are
    //! final invocations (`after_commit`), and `Terminal` is a non-final
    //! invocation (`after_abort`, the broker / timer will redeliver and
    //! produce a fresh invocation paired with its own apply hook).
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
        type Payload = serde_json::Value;

        async fn on_message<C>(
            &self,
            _context: C,
            _message: ConsumerMessage<Self::Payload>,
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

    fn make_test_message() -> Option<ConsumerMessage<serde_json::Value>> {
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
        // The load-bearing invariant, stated **per inner invocation**:
        // for every individual call to the inner `on_message` /
        // `on_timer` that ran and returned, exactly one apply hook
        // (after_commit OR after_abort) fires — not both, not neither,
        // and never coalesced across multiple invocations. This is what
        // 2PC handlers and rescue middleware rely on to know they are
        // guaranteed a single finalization signal per inner invocation.
        //
        // At this default boundary the blanket impl performs exactly one
        // inner invocation per outer call, so this test exercises the
        // 1:1 case directly. Wrapping middleware that re-invokes the
        // inner (e.g. a retry loop) must preserve the same invariant
        // per invocation; that is verified in the `retry` module's
        // tests.
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
        type Payload = T::Payload;

        async fn on_message<C>(
            &self,
            context: C,
            message: ConsumerMessage<Self::Payload>,
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
