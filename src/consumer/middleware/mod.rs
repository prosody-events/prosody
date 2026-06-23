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
//! # let producer: ProsodyProducer = ProsodyProducer::new(&producer_config, Telemetry::new().sender()).unwrap();
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
use std::marker::PhantomData;
use std::time::Duration;

use tokio::time::sleep;
use tracing::{error, warn};

use crate::consumer::event_context::EventContext;
use crate::consumer::message::{ConsumerMessage, UncommittedMessage};
use crate::consumer::{DemandType, EventHandler, Uncommitted};
use crate::error::{ClassifyError, ErrorCategory};
use crate::state::session::sealed::{ApplyOutcome, FinalizeOutcome};
use crate::state::session::{LifecycleAccessExt, LifecycleView};
use crate::timers::datetime::CompactDateTime;
use crate::timers::{TimerType, Trigger, UncommittedTimer};
use crate::{Partition, Topic};

/// Delay between retries of a durability step (seal / arm / marker flush)
/// that failed transiently. Mirrors the timer commit retry cadence
/// ([`crate::timers::uncommitted`]) and the state-manager init loop.
const DURABILITY_RETRY_DELAY: Duration = Duration::from_secs(1);

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

/// Defines middleware for message processing over a payload type `P`.
///
/// Payload-agnostic middleware (retry, log, cancellation, etc.) implement
/// `HandlerMiddleware<P>` for all `P`. Payload-aware middleware (defer,
/// failure topic, deduplication) implement it only for their bound payload
/// type.
///
/// # Anchoring `P`
///
/// Because payload-agnostic middleware implements `HandlerMiddleware<P>` for
/// every `P`, a bare `.layer()` chain has no way to pick `P` on its own. The
/// chain compiles without turbofish in any of these contexts:
///
/// 1. The chain ends in `.into_provider(handler)` — `P` flows from `H:
///    FallibleHandler<Payload = P>`.
/// 2. The chain is bound to an `-> impl HandlerMiddleware<P>` return type that
///    pins `P` for the caller.
/// 3. An explicit type ascription on the let binding pins
///    `ComposedMiddleware<…, P>`.
///
/// A bare-chain construction with no anchor needs UFCS or a let-binding with
/// an explicit type — usually a sign that you should terminate the chain with
/// `.into_provider(handler)` instead.
pub trait HandlerMiddleware<P: Send + Sync + 'static> {
    /// The provider type that wraps another fallible handler provider.
    ///
    /// The `where` clause constrains `T` so that only providers whose handler
    /// payload matches `P` can be wrapped. The associated-type bound
    /// `Handler: FallibleHandler<Payload = P>` propagates the payload through
    /// the chain so callers can layer middleware over an
    /// `impl HandlerMiddleware<P>` opaque return without losing the payload
    /// identity.
    type Provider<T>: FallibleHandlerProvider<Handler: FallibleHandler<Payload = P>>
    where
        T: FallibleHandlerProvider,
        T::Handler: FallibleHandler<Payload = P>;

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
        T: FallibleHandlerProvider,
        T::Handler: FallibleHandler<Payload = P>;

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
    /// let provider = RetryMiddleware::new(config).unwrap().into_provider(my_handler);
    /// ```
    fn into_provider<H>(self, handler: H) -> Self::Provider<FallibleCloneProvider<H>>
    where
        Self: Sized,
        H: FallibleHandler<Payload = P> + Clone + Send + Sync + 'static,
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
    /// # let retry_config = RetryConfiguration::builder().build().unwrap();
    /// # let inner_middleware = RetryMiddleware::new(retry_config).unwrap();
    /// # let middle_middleware = CancellationMiddleware;
    /// # let outer_middleware = CancellationMiddleware;
    /// # let my_handler = MyHandler;
    /// // Builds from inner to outer: inner -> middle -> outer
    /// let provider = inner_middleware
    ///     .layer(middle_middleware)
    ///     .layer(outer_middleware)
    ///     .into_provider(my_handler);
    ///
    /// // Request:  outer → middle → inner → handler
    /// // Response: handler → inner → middle → outer
    /// ```
    fn layer<T>(self, outer_middleware: T) -> ComposedMiddleware<T, Self, P>
    where
        Self: Sized,
        T: HandlerMiddleware<P>,
    {
        ComposedMiddleware(outer_middleware, self, PhantomData)
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
/// its own apply hook). [`retry`] owns the only other `EventHandler`
/// boundary; both route their final outcome through the shared `settle`
/// durability sequence (see [`FallibleEventHandler`]).
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
    ///
    /// Payload-specific middleware (e.g. deduplication) adds `EventIdentity`
    /// locally via a where clause on its impl, rather than requiring all
    /// payloads to satisfy `EventIdentity`.
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
        C: EventContext<Payload = Self::Payload>;

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
        C: EventContext<Payload = Self::Payload>;

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
        C: EventContext<Payload = Self::Payload>,
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
        C: EventContext<Payload = Self::Payload>,
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
///
/// The `P` payload parameter is carried as a phantom so that, once the chain is
/// anchored to a specific payload type at the first `.layer()` call, the type
/// stays fixed through subsequent chain operations — eliminating the
/// `HandlerMiddleware<_>` inference ambiguity that would otherwise arise from
/// payload-agnostic middleware impls.
#[derive(Clone, Debug)]
pub struct ComposedMiddleware<M1, M2, P>(M1, M2, PhantomData<fn() -> P>);

/// Marks a [`FallibleHandler`] as the **durability boundary**, getting the
/// blanket [`EventHandler`] impl below.
///
/// # The durability sequence has one owner: `settle`
///
/// The blanket impl invokes the inner `FallibleHandler` method **exactly
/// once**, then hands the single result to `settle` — the one place that
/// runs the keyed-state durability sequence in straight-line code:
///
/// ```text
/// final Ok  → stage provisional cells / write resolved (retry transient failures in place)
///           → arm StateRecovery backstop (clear_and_schedule; per-key singleton)
///           → flush registered dedup marker   (STRICTLY after the stage)
///           → commit the offset / trigger marker
///           → promote the staged cells (the backstop stays armed; the sweep
///             self-clears once the key goes quiet)
///           → after_commit(Ok)
/// final Err Transient/Permanent → flush registered marker → commit → after_commit(Err)
/// final Err Terminal            → abort → roll back → after_abort
/// ```
///
/// Because the marker flush is textually *after* the stage in one function,
/// the finding-1 bug class — marker written before the staged cell is durable
/// — is **unwritable**, not merely avoided. Promotion runs strictly *after*
/// the commit (invariant 3): promoting a timer write before its trigger commit
/// would resurrect it on a crash-refire. The timer marker (trigger tag) is
/// written outside the stack by the marker commit; the message marker here
/// restores message/timer symmetry.
///
/// [`RetryHandler`](retry::RetryHandler) is a second durability boundary
/// (it owns its own `EventHandler` impl so it can map shutdown to abort
/// rather than commit); it routes its final outcome through the **same**
/// `settle` / `abandon` functions, so the sequence still has a single
/// owner. No other middleware should implement `EventHandler` directly.
///
/// **Stack contract:** `settle` keys the seal to the *final* `Ok` the
/// stack returns, not the inner handler's `Ok`. In the shipped stacks no
/// reachable path maps an inner `Ok` to an outer `Err`; the defer
/// middlewares *can* (a rescue failure after inner success), which is safe
/// because retry's between-attempt reset discards the session and `settle`
/// flushes a registered marker only on a `Permanent` final. A new
/// middleware that converts inner `Ok` to outer `Err` must reset the
/// session (`reset_state_session`) or rely on those same guards.
///
/// Per-invocation apply-hook correctness is preserved: one inner invocation
/// pairs with exactly one `after_commit` / `after_abort` firing.
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

impl<P, M1, M2> HandlerMiddleware<P> for ComposedMiddleware<M1, M2, P>
where
    P: Send + Sync + 'static,
    M1: HandlerMiddleware<P>,
    M2: HandlerMiddleware<P>,
{
    type Provider<T>
        = M1::Provider<M2::Provider<T>>
    where
        T: FallibleHandlerProvider,
        T::Handler: FallibleHandler<Payload = P>;

    fn with_provider<T>(&self, provider: T) -> Self::Provider<T>
    where
        T: FallibleHandlerProvider,
        T::Handler: FallibleHandler<Payload = P>,
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
        C: EventContext<Payload = T::Payload>,
    {
        // Invoke the inner FallibleHandler EXACTLY ONCE, then hand its single
        // result to the shared durability sequence. `settle` fires EXACTLY
        // ONE apply hook, so the per-invocation invariant holds.
        let (inner_message, uncommitted_offset) = message.into_inner();
        let result =
            FallibleHandler::on_message(self, context.clone(), inner_message, demand_type).await;
        if let Err(error) = &result {
            self.on_message_error(error);
        }
        settle(self, context, uncommitted_offset, result).await;
    }

    async fn on_timer<C, U>(&self, context: C, timer: U, demand_type: DemandType)
    where
        C: EventContext<Payload = T::Payload>,
        U: UncommittedTimer,
    {
        let (trigger, uncommitted_timer) = timer.into_inner();
        let result = FallibleHandler::on_timer(self, context.clone(), trigger, demand_type).await;
        if let Err(error) = &result {
            self.on_timer_error(error);
        }
        settle(self, context, uncommitted_timer, result).await;
    }

    async fn shutdown(self) {
        FallibleHandler::shutdown(self).await;
    }
}

/// Outcome of one durability step driven by [`retry_step`].
enum StepOutcome<R> {
    /// The step succeeded, carrying its result.
    Done(R),

    /// The step failed permanently. The sequence continues defensively
    /// (the `StateRecovery` sweep cleans up any partial effect); it never
    /// flushes a marker over an uncertain seal.
    Skip,

    /// Shutdown, or a terminal failure: abandon the event — abort the
    /// marker, roll back, and let redelivery re-run from clean state.
    Abandon,
}

/// The durability sequence: the single owner of seal → arm → marker-flush →
/// commit → resolve, run once per event after the stack returns its final
/// `result`. Both the blanket [`EventHandler`] impl and
/// [`RetryHandler`](retry::RetryHandler) route their final outcome here, so
/// the wrong ordering (marker before seal) is structurally unwritable.
///
/// Fires exactly one apply hook (`after_commit` / `after_abort`) carrying
/// `result`, preserving the per-invocation apply-hook invariant.
pub(crate) async fn settle<T, C, G>(
    handler: &T,
    context: C,
    guard: G,
    result: Result<T::Output, T::Error>,
) where
    T: FallibleHandler,
    T::Error: ClassifyError,
    C: EventContext<Payload = T::Payload>,
    G: Uncommitted + Send,
{
    // The inner work is done; clear any stale message-level cancel flag so
    // the durability steps' cancel-guarded timer ops aren't short-circuited
    // (mirrors the timeout middleware uncancelling after the inner returns).
    context.uncancel();

    // Reach the event's sealed lifecycle. Every live context carries one —
    // `LifecycleAccess` binds unconditionally — so `None` means only an
    // invalidated context, which cannot seal anyway.
    let lifecycle = context.lifecycle().ok();

    match result.as_ref().err().map(ClassifyError::classify_error) {
        // Terminal: the marker aborts; the event redelivers and re-runs.
        // Nothing staged (finalize runs only on Ok), so rollback is a no-op.
        Some(ErrorCategory::Terminal) => {
            abandon(
                handler,
                context,
                guard,
                result,
                RollbackSafety::BeforeMarkerFlush,
            )
            .await;
        }
        // Final error (Transient/Permanent): nothing sealed (finalize runs
        // only on Ok). On a Permanent error the dedup middleware registered
        // the marker; flush it so the failed-but-final message deduplicates.
        // The flush is gated to Permanent: in the shipped stacks a Transient
        // final has no registered marker anyway, but a custom stack whose
        // middleware swallows a post-success failure into a Transient error
        // (without resetting the session) must not have that attempt's
        // marker certify never-sealed state. Then commit and fire the hook.
        Some(category) => {
            if category == ErrorCategory::Permanent
                && let Some(lifecycle) = &lifecycle
            {
                flush_marker_best_effort(&context, lifecycle).await;
            }
            guard.commit().await;
            handler.after_commit(context, result).await;
        }
        // Success: run the full durability sequence.
        None => settle_committed(handler, context, guard, result, lifecycle).await,
    }
}

/// The success arm of [`settle`]: seal, arm the backstop, flush the marker
/// strictly after the seal, commit, then resolve the sealed set.
async fn settle_committed<T, C, G>(
    handler: &T,
    context: C,
    guard: G,
    result: Result<T::Output, T::Error>,
    lifecycle: Option<LifecycleView<C::State>>,
) where
    T: FallibleHandler,
    C: EventContext<Payload = T::Payload>,
    G: Uncommitted + Send,
{
    let Some(lifecycle) = lifecycle else {
        // Invalidated / stateless context: just commit and fire the hook.
        guard.commit().await;
        handler.after_commit(context, result).await;
        return;
    };

    // 1. Stage provisional cells / write resolved, retrying transient
    // failures.
    let staged = match retry_step(&context, "keyed-state finalize", || lifecycle.finalize()).await {
        StepOutcome::Done(FinalizeOutcome::Staged) => true,
        StepOutcome::Done(FinalizeOutcome::Clean) => false,
        StepOutcome::Skip => {
            // Permanent stage failure: a partial stage may be durable. Arm the
            // backstop defensively so the sweep resolves it, skip the marker
            // flush (invariant: marker present ⇒ stage durable), and commit.
            // A shutdown `Abandon` from the arm is deliberately ignored:
            // committing a permanently-unstageable event beats livelocking,
            // and first-touch covers the unarmed cell. No marker was flushed
            // before this commit, so rollback-safety does not arise.
            let _ = arm_backstop(&context, &lifecycle).await;
            guard.commit().await;
            handler.after_commit(context, result).await;
            return;
        }
        StepOutcome::Abandon => {
            // Before the marker flush: the recorded staged set (if any) rolls
            // back to its committed base.
            abandon(
                handler,
                context,
                guard,
                result,
                RollbackSafety::BeforeMarkerFlush,
            )
            .await;
            return;
        }
    };

    // 2. Arm the StateRecovery backstop iff something staged. The backstop is
    // an amortized per-key singleton: the first commit of a generation arms it,
    // later commits skip while it stands, and the boundary never clears it
    // (the sweep does, on fire), so this event cannot disturb another's
    // backstop (F2).
    //
    // Arm-gates-marker (invariant 8): a backstop is the only guarantee that a
    // staged provisional cell resolves before its TTL, so if arming does not
    // succeed we must NOT certify the stage. Both a permanent failure (`Skip`)
    // and a shutdown (`Abandon`) therefore skip the marker flush and abandon
    // *before* it — the staged set rolls back to its committed base (no
    // lingering provisional, nothing to TTL out) and the offset aborts so the
    // event redelivers and re-runs. A *persistent* permanent arm failure
    // redelivers until the timer store recovers; that is preferred over
    // committing a marker-certified write that could later TTL out unresolved.
    if staged {
        match arm_backstop(&context, &lifecycle).await {
            StepOutcome::Done(()) => {}
            StepOutcome::Skip | StepOutcome::Abandon => {
                abandon(
                    handler,
                    context,
                    guard,
                    result,
                    RollbackSafety::BeforeMarkerFlush,
                )
                .await;
                return;
            }
        }
    }

    // 3. Flush the registered dedup marker — STRICTLY after the stage, so a
    // present marker always certifies a durable stage.
    match retry_step(&context, "keyed-state marker flush", || {
        lifecycle.flush_marker()
    })
    .await
    {
        StepOutcome::Done(()) => {}
        StepOutcome::Skip => {
            // Permanent flush failure: the committed stage won't be certified,
            // so recovery rolls it back. Leave the backstop armed and commit.
            guard.commit().await;
            handler.after_commit(context, result).await;
            return;
        }
        StepOutcome::Abandon => {
            // A marker-flush attempt was made before shutdown: its durability
            // is ambiguous, so the staged cells must NOT roll back. They stay
            // provisional and the armed sweep resolves them through the oracle
            // (invariant 7). Rolling back here could erase a committed write
            // that redelivery then dedup-filters.
            abandon(
                handler,
                context,
                guard,
                result,
                RollbackSafety::AfterMarkerFlush,
            )
            .await;
            return;
        }
    }

    // 4. Commit the durability marker (offset / trigger).
    guard.commit().await;

    // 5. Promote the staged cells (null `event`/`prev`, O(1) per cell). This
    // is invariant-3 correct only here, strictly after the commit: promoting a
    // timer write before its trigger commit would resurrect it on a
    // crash-refire. The backstop stays armed regardless: a `Resolved` key's
    // sweep finds nothing provisional and clears itself when the key goes
    // quiet, while an `Incomplete` promote leaves real work for that same
    // sweep to retry. No point-clear means no cross-event race.
    if lifecycle.commit_apply().await == ApplyOutcome::Incomplete {
        warn!("keyed-state promote incomplete; the StateRecovery sweep will retry");
    }

    // 6. After-commit hook (telemetry, dedup forwarding, ...).
    handler.after_commit(context, result).await;
}

/// Whether inline abandon may roll a staged set back to its committed base.
///
/// The marker flush is the boundary (invariant 7): before any flush attempt a
/// rollback is sound; after one it is not, because the flush's durability is
/// ambiguous on a write-timeout.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RollbackSafety {
    /// No marker flush has been attempted; rolling the staged set back to its
    /// committed base is safe.
    BeforeMarkerFlush,

    /// A marker flush has been attempted (durability ambiguous); leave staged
    /// cells provisional for the sweep to resolve through the oracle.
    AfterMarkerFlush,
}

/// Abandons the event: abort the marker (offset → redelivery, timer →
/// reloadable), conditionally roll back any staged set, and fire
/// `after_abort`. Reached on a terminal error or a shutdown mid-sequence.
///
/// `safety` gates the rollback (invariant 7, rollback-only-before-flush):
/// inline rollback to the committed base is sound only
/// [`RollbackSafety::BeforeMarkerFlush`]. Once a marker flush has been
/// *attempted* ([`RollbackSafety::AfterMarkerFlush`]) a write-timeout is
/// ambiguous — the marker may be durable — so rolling back could erase a
/// committed write that redelivery then dedup-filters away. In that window the
/// staged cells stay provisional and the (already-armed) `StateRecovery` sweep
/// resolves them through the oracle, which reads whether the marker landed.
pub(crate) async fn abandon<T, C, G>(
    handler: &T,
    context: C,
    guard: G,
    result: Result<T::Output, T::Error>,
    safety: RollbackSafety,
) where
    T: FallibleHandler,
    C: EventContext<Payload = T::Payload>,
    G: Uncommitted + Send,
{
    guard.abort().await;
    if let (RollbackSafety::BeforeMarkerFlush, Ok(lifecycle)) = (safety, context.lifecycle()) {
        // Best-effort: first-touch / the sweep recover anything left behind.
        // Resolves the shared session's recorded staged set (empty unless a
        // stage was recorded before the abandon point), so a fresh view is
        // equivalent to one threaded down.
        lifecycle.rollback_aborted().await;
    }
    handler.after_abort(context, result).await;
}

/// Arms the per-key `StateRecovery` backstop as an amortized singleton.
///
/// The first stateful commit on a quiet key issues one `clear_and_schedule`
/// (a type-scoped singleton overwrite — only the key's `StateRecovery` timers
/// move; user timers of other types are untouched) and marks the key armed.
/// While that backstop stands, later commits skip re-arming entirely: the
/// standing timer at first-commit-plus-`delay` already covers their staged
/// cells. So a burst of commits on one key issues a *single* timer-store write
/// rather than one per commit — the amortization the redesign's
/// tombstone-accounting depends on.
///
/// The armed flag is cleared only when the sweep fires (the manager's
/// `recover`), so the durability boundary never unschedules and one event can
/// never clear another's still-needed backstop (finding F2). Per-key
/// serialization makes the flag race-free: the sweep that consumes a backstop
/// cannot run while a commit on the same key decides whether to re-arm.
///
/// Cost and healing: at most one `clear_and_schedule` per backstop generation,
/// and the sweep fires `delay` after the first commit of a generation (on a
/// sustained hot key, periodically). Any read of a provisional collection heals
/// it immediately via first-touch (the cell store's resolving `get`). Accepted
/// residual: an
/// `Incomplete` leftover on a hot key whose collection is never read again
/// waits for the next sweep to resolve it — bounded by first-touch on any
/// access and by the cell's TTL.
async fn arm_backstop<C>(context: &C, lifecycle: &LifecycleView<C::State>) -> StepOutcome<()>
where
    C: EventContext,
{
    // Amortize: a standing backstop already covers this commit's staged cells,
    // so skip re-arming. Per-key serialization makes the standing flag reliable
    // — the sweep that consumes it cannot run while this commit decides.
    if lifecycle.backstop_armed().await {
        return StepOutcome::Done(());
    }
    let delay = lifecycle.recovery_fire_delay();
    let fire = match CompactDateTime::now().and_then(|now| now.add_duration(delay)) {
        Ok(fire) => fire,
        Err(error) => {
            error!(error = %error, "failed to compute StateRecovery fire time; skipping backstop");
            return StepOutcome::Skip;
        }
    };
    let outcome = retry_step(context, "arm StateRecovery backstop", || {
        context.clear_and_schedule(fire, TimerType::StateRecovery)
    })
    .await;
    // Record the standing backstop only after a successful arm, so a failed
    // arm leaves the flag clear and the next commit retries.
    if matches!(outcome, StepOutcome::Done(())) {
        lifecycle.mark_backstop_armed().await;
    }
    outcome
}

/// Flushes the registered marker, retrying transient failures; a permanent
/// failure or shutdown is tolerated (the failed-but-final message simply
/// isn't deduplicated and re-runs, re-failing the same way).
async fn flush_marker_best_effort<C>(context: &C, lifecycle: &LifecycleView<C::State>)
where
    C: EventContext,
{
    let _ = retry_step(context, "keyed-state marker flush", || {
        lifecycle.flush_marker()
    })
    .await;
}

/// Retries one durability step until it succeeds, is abandoned on shutdown,
/// or fails terminally; a permanent failure is skipped so the straight-line
/// sequence continues defensively. Mirrors the retry-until-shutdown idiom of
/// the timer commit loop and state-manager initialization.
async fn retry_step<C, R, E, F, Fut>(context: &C, label: &str, mut step: F) -> StepOutcome<R>
where
    C: EventContext,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<R, E>>,
    E: ClassifyError + StdError,
{
    loop {
        if context.is_shutdown() {
            return StepOutcome::Abandon;
        }
        match step().await {
            Ok(value) => return StepOutcome::Done(value),
            Err(error) => match error.classify_error() {
                ErrorCategory::Transient => {
                    error!(label, error = %error, "durability step failed; retrying");
                    sleep(DURABILITY_RETRY_DELAY).await;
                }
                ErrorCategory::Permanent => {
                    error!(label, error = %error, "durability step failed permanently; skipping");
                    return StepOutcome::Skip;
                }
                ErrorCategory::Terminal => {
                    error!(label, error = %error, "durability step terminal failure; abandoning");
                    return StepOutcome::Abandon;
                }
            },
        }
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
pub(crate) mod tests;
