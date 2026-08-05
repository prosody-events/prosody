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
//! Compose middleware with [`HandlerMiddleware::layer`] — each call adds a new
//! **outermost** layer — and finalize with
//! [`HandlerMiddleware::into_provider`], which terminates the chain with the
//! handler as the **innermost** component. Retry belongs outermost, since it
//! re-drives the whole stack:
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
//! # let handler = MyHandler;
//!
//! // Stack (outer → inner): retry → cancellation → handler.
//! let provider = CancellationMiddleware
//!     .layer(RetryMiddleware::new(retry_config).unwrap())
//!     .into_provider(handler);
//! ```
//!
//! ## Retrying around a rescue layer
//!
//! [`topic::FailureTopicMiddleware`] rescues a failed message to a dead-letter
//! topic. Wrapping it in its own outer [`retry::RetryMiddleware`] retries the
//! DLQ write itself, while the inner retry re-drives the handler:
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

use crate::consumer::event_context::EventContext;
use crate::consumer::message::{ConsumerMessage, UncommittedMessage};
use crate::consumer::{DemandType, EventHandler};
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
pub mod providers;
pub(crate) mod respond;
pub mod retry;
pub mod scheduler;
mod settle;
pub mod telemetry;
pub mod timeout;
pub mod topic;

// Re-export providers for backwards compatibility and convenience
pub use providers::{CloneProvider, FallibleCloneProvider, LeafHandler};
pub(crate) use settle::{MarkerWrite, NextAttempt, Settlement, SettlementHandler, abandon, settle};
// `RepinProof` is named by the public `EventContext::redispatch` signature, so
// it must be publicly reachable — its constructor stays module-private, so the
// re-export exposes only an unconstructable token (the `MarkerWrite` idiom).
pub use settle::RepinProof;

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
    /// handler as the innermost component — adapted through [`LeafHandler`],
    /// the chain terminator that classifies the leaf's result as final for
    /// the settlement boundary.
    ///
    /// See the [module-level usage example](crate::consumer::middleware) for a
    /// full composition; a single middleware terminates the chain the same way,
    /// e.g. `RetryMiddleware::new(config)?.into_provider(handler)`.
    fn into_provider<H>(self, handler: H) -> Self::Provider<FallibleCloneProvider<LeafHandler<H>>>
    where
        Self: Sized,
        H: FallibleHandler<Payload = P> + Clone + Send + Sync + 'static,
    {
        self.with_provider(FallibleCloneProvider::new(LeafHandler::new(handler)))
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
    /// # Example
    ///
    /// See the [module-level usage example](crate::consumer::middleware), which
    /// composes `CancellationMiddleware.layer(RetryMiddleware…)` so retry ends
    /// up outermost.
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
/// Two optional parts extend the basic shape for handlers that stage
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
/// docs](self) describe how impls are composed (layering, providers,
/// execution flow); [Implementing as middleware](#implementing-as-middleware)
/// below states what an individual middleware impl owes its inner
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
/// stack. It states what a `FallibleHandler` middleware (a wrapper around
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
/// A consumer that answers peer requests holds one exception to that rule. The
/// response payload is the handler's own final `Result`, so answering moves
/// that value out. A record that carried a request tag and was answered
/// therefore fires no [`after_commit`](FallibleHandler::after_commit) on the
/// handler. Every other record, and every record whose answer could not be
/// queued, fires the hook as usual.
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
/// Bypassed final                → commit → after_commit (no stage, no marker)
/// Final Ok  → stage provisional cells / write resolved (retry transient failures in place)
///           → arm StateRecovery backstop (clear_and_schedule; per-key singleton)
///           → record the message marker (read from the session's event
///             identity; STRICTLY after the stage)
///           → commit the offset / trigger marker
///           → promote the staged cells (the backstop stays armed; the sweep
///             self-clears once the key goes quiet)
///           → after_commit(Ok)
/// Final Err Transient/Permanent → record marker iff Permanent → commit → after_commit(Err)
/// Err Terminal                  → abort → after_abort
/// ```
///
/// Because the marker record is textually *after* the stage in one function,
/// the marker-before-durable-state bug class is **unwritable**, not merely
/// avoided. The crash-window argument for the full step order — including
/// why promotion runs strictly after the commit — lives on
/// `settle_committed` in `settle.rs`. The timer marker (trigger tag) is
/// written outside the stack by the marker commit; the message marker here
/// restores message/timer symmetry.
///
/// [`RetryHandler`](retry::RetryHandler) is a second durability boundary
/// (it owns its own `EventHandler` impl so it can map shutdown to abort
/// rather than commit); it routes its final outcome through the **same**
/// `settle` / `abandon` functions, so the sequence still has a single
/// owner. No other middleware should implement `EventHandler` directly.
///
/// **Stack contract:** whether a dispatch settles the event is a pure
/// function of the *final* result the stack returns — the crate-internal
/// `settlement()` classification. A middleware that swallows or rescues (a
/// defer swallow into `Ok(Deferred)`, a DLQ route into `Ok(Routed)`, a dedup
/// skip into `Ok(None)`) classifies its own variants `Bypassed`, so nothing
/// stages and no marker records for the swallowed attempt; there is no reset
/// protocol to remember. The blanket impl below therefore requires both this
/// trait and `SettlementHandler`.
///
/// Per-invocation apply-hook correctness is preserved: one inner invocation
/// pairs with exactly one `after_commit` / `after_abort` firing.
pub trait FallibleEventHandler: FallibleHandler {
    /// Called when message processing fails.
    fn on_message_error(&self, _error: &Self::Error) {}

    /// Called when timer processing fails.
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
    T: FallibleEventHandler + SettlementHandler,
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
