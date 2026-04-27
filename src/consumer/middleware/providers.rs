//! Basic handler providers for partition processing.
//!
//! Provides simple cloning-based providers that create handler instances for
//! each Kafka topic-partition. These are the fundamental building blocks used
//! by middleware to create per-partition handlers.
//!
//! These providers do not wrap an inner `FallibleHandler`; they vend a handler
//! instance per partition. `InfallibleWrapper`'s `on_message`/`on_timer` are
//! stubs that perform no inner dispatch (zero invocations), so the trait-default
//! no-op apply hooks correctly satisfy the per-invocation invariant. Where an
//! inner exists in this module, it is invoked at most once per call;
//! per-invocation invariant trivially upheld.
//!
//! # Available Providers
//!
//! - [`FallibleCloneProvider`] - For handlers returning `Result<(), E>`
//!   (production)
//! - [`CloneProvider`] - For infallible handlers (tests and simple cases)
//! - [`InfallibleWrapper`] - Type-erasure adapter that lets an infallible
//!   handler satisfy the `FallibleHandler` bound. **Deprecated in favor of
//!   [`CloneProvider`]**: this wrapper does not delegate dispatch to the inner
//!   `EventHandler` — its `on_message`/`on_timer` are stubs that always return
//!   `Ok(())` and perform no work.
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

use std::convert::Infallible;

use crate::consumer::event_context::EventContext;
use crate::consumer::message::ConsumerMessage;
use crate::consumer::{DemandType, EventHandler, HandlerProvider, Partition, Topic};
use crate::timers::Trigger;

use super::{FallibleHandler, FallibleHandlerProvider};

/// A provider that clones the wrapped fallible handler for each partition.
///
/// This provider is used by the consumer for handlers that can fail during
/// processing. It implements `FallibleHandlerProvider` and creates cloned
/// instances of handlers that can return errors.
#[derive(Clone, Debug)]
pub struct FallibleCloneProvider<T>(T);

impl<T> FallibleCloneProvider<T> {
    /// Creates a new `FallibleCloneProvider` that wraps the given handler.
    ///
    /// # Arguments
    ///
    /// * `inner` - The fallible handler to wrap.
    ///
    /// # Returns
    ///
    /// A new `FallibleCloneProvider` instance.
    pub fn new(inner: T) -> Self {
        Self(inner)
    }
}

impl<T> From<T> for FallibleCloneProvider<T>
where
    T: FallibleHandler + Clone + Send + Sync + 'static,
{
    fn from(inner: T) -> Self {
        Self::new(inner)
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

impl<T> From<T> for CloneProvider<T>
where
    T: EventHandler + Clone + Send + Sync + 'static,
{
    fn from(inner: T) -> Self {
        Self::new(inner)
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

/// A type-erasure adapter that allows an infallible [`EventHandler`] to satisfy
/// the [`FallibleHandler`] trait bound.
///
/// **Deprecated in favor of [`CloneProvider`].** This wrapper is a degenerate
/// adapter that exists only for type-level convenience: its
/// [`FallibleHandler::on_message`] and [`FallibleHandler::on_timer`]
/// implementations are stubs that always return `Ok(())` and **do not**
/// delegate to the wrapped `EventHandler`. As a result, no real per-event work
/// is performed by this handler.
///
/// # Apply-hook contract
///
/// Under the framework's apply-hook invariant, every dispatch of
/// `on_message`/`on_timer` that runs and returns must be paired with exactly
/// one of `after_commit` (final dispatch) or `after_abort` (retry coming).
/// Because this wrapper's dispatch methods perform no inner work, the trait
/// default no-op implementations of `after_commit` and `after_abort` are
/// correct: there is nothing to commit or roll back.
///
/// New code that needs to run a real infallible handler should use
/// [`CloneProvider`] with [`HandlerProvider`] instead, which dispatches
/// directly to the underlying `EventHandler`.
#[derive(Clone, Debug)]
pub struct InfallibleWrapper<T>(T);

impl<T> InfallibleWrapper<T> {
    /// Creates a new `InfallibleWrapper` around the given handler.
    pub fn new(inner: T) -> Self {
        Self(inner)
    }
}

impl<T> FallibleHandler for InfallibleWrapper<T>
where
    T: EventHandler + Send + Sync + 'static,
{
    type Error = Infallible;
    type Output = ();

    async fn on_message<C>(
        &self,
        _context: C,
        _message: ConsumerMessage,
        _demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext,
    {
        // Stub: this wrapper does NOT delegate to the inner EventHandler's
        // on_message. It exists only to satisfy the FallibleHandler bound at the
        // type level and performs no per-event work. Because no inner work runs,
        // the trait-default no-op `after_commit`/`after_abort` correctly satisfy
        // the apply-hook invariant. New code should use CloneProvider instead.
        Ok(())
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
        // Stub: this wrapper does NOT delegate to the inner EventHandler's
        // on_timer. It exists only to satisfy the FallibleHandler bound at the
        // type level and performs no per-event work. Because no inner work runs,
        // the trait-default no-op `after_commit`/`after_abort` correctly satisfy
        // the apply-hook invariant. New code should use CloneProvider instead.
        Ok(())
    }

    async fn shutdown(self) {
        // Cascade shutdown to the wrapped EventHandler
        self.0.shutdown().await;
    }
}
