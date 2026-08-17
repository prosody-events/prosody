use std::marker::PhantomData;

use super::{FallibleCloneProvider, FallibleHandler, LeafHandler};
use crate::{Partition, Topic};

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

/// A composition of two middleware components.
///
/// The `P` payload parameter is carried as a phantom so that, once the chain is
/// anchored to a specific payload type at the first `.layer()` call, the type
/// stays fixed through subsequent chain operations — eliminating the
/// `HandlerMiddleware<_>` inference ambiguity that would otherwise arise from
/// payload-agnostic middleware impls.
#[derive(Clone, Debug)]
pub struct ComposedMiddleware<M1, M2, P>(M1, M2, PhantomData<fn() -> P>);

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
