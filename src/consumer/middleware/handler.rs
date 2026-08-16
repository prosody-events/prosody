use std::error::Error as StdError;
use std::future::Future;

use crate::consumer::DemandType;
use crate::consumer::event_context::EventContext;
use crate::consumer::message::ConsumerMessage;
use crate::error::ClassifyError;
use crate::timers::Trigger;

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
/// - [`on_excise`](Self::on_excise) — called for each excise record.
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
/// so middleware authors implement this same trait. The middleware module
/// documentation describes how impls are composed (layering, providers,
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
/// - [`Transient`](crate::error::ErrorCategory::Transient) — **retry**. A
///   temporary problem (network blip, store timeout, downstream service
///   unavailable) that may succeed later. The retry middleware reattempts; if
///   configured, the defer middleware can move the message to a timer-based
///   retry to unblock the partition.
/// - [`Permanent`](crate::error::ErrorCategory::Permanent) — **give up on this
///   message**. The data itself is bad (deserialization failure, schema
///   violation, business rule rejection) and retrying won't help. The message
///   is committed, and may be routed to a dead-letter topic if the
///   failure-topic middleware is configured.
/// - [`Terminal`](crate::error::ErrorCategory::Terminal) — **shut the consumer
///   down**. The process can't safely continue (corrupted local state, an
///   invariant violation) and a new instance must take over.
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
/// 1. **Forward the handler methods.** The stack calls middleware
///    [`on_message`](Self::on_message) for messages and excise records. Apply
///    the same message policy to both record types. The leaf adapter calls the
///    user's matching method. Forward timers through
///    [`on_timer`](Self::on_timer). Implement [`on_excise`](Self::on_excise)
///    because the trait requires it. Cascade `shutdown` to the inner handler.
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
/// durability sequence (see
/// [`crate::consumer::middleware::FallibleEventHandler`]).
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

    /// Error type returned by the event methods.
    ///
    /// Must implement [`ClassifyError`] so the framework can decide
    /// whether to retry, give up, or shut down for each variant — see
    /// [Error classification](#error-classification) on the trait docs.
    /// Use [`std::convert::Infallible`] for handlers that cannot fail.
    type Error: ClassifyError + StdError + Send;

    /// Success value produced by an event method
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

    /// Handles an excise record that has a key and no payload.
    fn on_excise<C>(
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
