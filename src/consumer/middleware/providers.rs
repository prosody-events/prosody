//! Basic handler providers for partition processing.
//!
//! Provides simple cloning-based providers that create handler instances for
//! each Kafka topic-partition. These are the fundamental building blocks used
//! by middleware to create per-partition handlers.
//!
//! These providers do not wrap an inner `FallibleHandler`; they vend a handler
//! instance per partition.
//!
//! # Available Providers
//!
//! - [`FallibleCloneProvider`] - For handlers returning `Result<(), E>`
//!   (production)
//! - [`CloneProvider`] - For infallible handlers (tests and simple cases)
//!
//! # Usage
//!
//! Providers are typically created automatically via
//! [`crate::consumer::middleware::HandlerMiddleware::into_provider`],
//! but can be used directly:
//!
//! ```rust,no_run
//! # use prosody::consumer::middleware::providers::*;
//! # let my_fallible_handler = || {};
//! # let my_event_handler = || {};
//!
//! // For fallible handlers
//! let provider = FallibleCloneProvider::new(my_fallible_handler);
//!
//! // For infallible handlers
//! let provider = CloneProvider::new(my_event_handler);
//! ```

use std::future::Future;

use crate::consumer::event_context::EventContext;
use crate::consumer::message::ConsumerMessage;
use crate::consumer::{DemandType, EventHandler, HandlerProvider, Partition, Topic};
use crate::timers::Trigger;

use super::{FallibleHandler, FallibleHandlerProvider, Settlement, SettlementHandler};

/// The chain terminator [`into_provider`] mints around the user's leaf
/// handler. Its crate-internal settlement
/// classification is final on both sides — the leaf's result is the event's
/// own outcome, by definition. Minting it here (instead of a blanket
/// classification over all handlers) keeps the classification an explicit,
/// per-wrapper obligation inside the framework while leaving public leaf
/// handlers untouched.
///
/// [`into_provider`]: super::HandlerMiddleware::into_provider
#[derive(Clone, Debug)]
pub struct LeafHandler<H>(H);

impl<H> LeafHandler<H> {
    /// Wraps the user's leaf handler; called only by
    /// [`into_provider`](super::HandlerMiddleware::into_provider).
    pub(crate) fn new(handler: H) -> Self {
        Self(handler)
    }
}

impl<H> FallibleHandler for LeafHandler<H>
where
    H: FallibleHandler,
{
    type Error = H::Error;
    type Output = H::Output;
    type Payload = H::Payload;

    async fn on_message<C>(
        &self,
        context: C,
        message: ConsumerMessage<Self::Payload>,
        demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        if message.payload().is_some() {
            self.0.on_message(context, message, demand_type).await
        } else {
            self.0.on_excise(context, message, demand_type).await
        }
    }

    async fn on_excise<C>(
        &self,
        context: C,
        message: ConsumerMessage<Self::Payload>,
        demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        self.0.on_excise(context, message, demand_type).await
    }

    fn on_timer<C>(
        &self,
        context: C,
        trigger: Trigger,
        demand_type: DemandType,
    ) -> impl Future<Output = Result<Self::Output, Self::Error>> + Send
    where
        C: EventContext<Payload = Self::Payload>,
    {
        self.0.on_timer(context, trigger, demand_type)
    }

    fn after_commit<C>(
        &self,
        context: C,
        result: Result<Self::Output, Self::Error>,
    ) -> impl Future<Output = ()> + Send
    where
        C: EventContext<Payload = Self::Payload>,
    {
        self.0.after_commit(context, result)
    }

    fn after_abort<C>(
        &self,
        context: C,
        result: Result<Self::Output, Self::Error>,
    ) -> impl Future<Output = ()> + Send
    where
        C: EventContext<Payload = Self::Payload>,
    {
        self.0.after_abort(context, result)
    }

    fn shutdown(self) -> impl Future<Output = ()> + Send {
        self.0.shutdown()
    }
}

impl<H> SettlementHandler for LeafHandler<H>
where
    H: FallibleHandler,
{
    /// The handler's own result is the event's own outcome, by definition.
    fn settlement(_result: Result<&Self::Output, &Self::Error>) -> Settlement {
        Settlement::Final
    }
}

/// A provider that clones the wrapped fallible handler for each partition.
///
/// This provider is used by the consumer for handlers that can fail during
/// processing. It implements `FallibleHandlerProvider` and creates cloned
/// instances of handlers that can return errors.
#[derive(Clone, Debug)]
pub struct FallibleCloneProvider<T>(T);

impl<T> FallibleCloneProvider<T> {
    /// Creates a new `FallibleCloneProvider` that wraps the given handler.
    pub fn new(inner: T) -> Self {
        Self(inner)
    }
}

impl<T> FallibleHandlerProvider for FallibleCloneProvider<T>
where
    T: FallibleHandler + Clone + Send + Sync + 'static,
{
    type Handler = T;

    fn handler_for_partition(&self, _topic: Topic, _partition: Partition) -> Self::Handler {
        self.0.clone()
    }
}

/// A provider that clones the wrapped infallible handler for each partition.
///
/// This provider is used in tests for handlers that never fail. It implements
/// `HandlerProvider` and creates cloned instances of handlers that implement
/// `EventHandler` directly without error handling.
#[derive(Clone, Debug)]
pub struct CloneProvider<T>(T);

impl<T> CloneProvider<T> {
    /// Creates a new `CloneProvider` that wraps the given handler.
    pub fn new(inner: T) -> Self {
        Self(inner)
    }
}

impl<T> HandlerProvider for CloneProvider<T>
where
    T: EventHandler + Clone + Send + Sync + 'static,
{
    type Handler = T;

    fn handler_for_partition(&self, _topic: Topic, _partition: Partition) -> Self::Handler {
        self.0.clone()
    }
}
