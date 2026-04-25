//! Basic handler providers for partition processing.
//!
//! Provides simple cloning-based providers that create handler instances for
//! each Kafka topic-partition. These are the fundamental building blocks used
//! by middleware to create per-partition handlers.
//!
//! # Available Providers
//!
//! - [`FallibleCloneProvider`] - For handlers returning `Result<(), E>`
//!   (production)
//! - [`CloneProvider`] - For infallible handlers (tests and simple cases)
//! - [`InfallibleWrapper`] - Adapts infallible handlers to work with fallible
//!   middleware
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

/// A simple fallible handler wrapper for making infallible handlers work with
/// `FallibleCloneProvider`.
///
/// This is useful when you want to use an infallible handler (like test
/// handlers) with the fallible handler infrastructure.
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
    type Outcome = ();

    async fn on_message<C>(
        &self,
        _context: C,
        _message: ConsumerMessage,
        _demand_type: DemandType,
    ) -> Result<Self::Outcome, Self::Error>
    where
        C: EventContext,
    {
        // Since EventHandler::on_message doesn't return a Result, we can't actually
        // fail. This is a conceptual wrapper - in practice, infallible handlers
        // should use CloneProvider
        Ok(())
    }

    async fn on_timer<C>(
        &self,
        _context: C,
        _trigger: Trigger,
        _demand_type: DemandType,
    ) -> Result<Self::Outcome, Self::Error>
    where
        C: EventContext,
    {
        // Since EventHandler::on_timer doesn't return a Result, we can't actually fail
        // This is a conceptual wrapper - in practice, infallible handlers should use
        // CloneProvider
        Ok(())
    }

    async fn shutdown(self) {
        // Cascade shutdown to the wrapped EventHandler
        self.0.shutdown().await;
    }
}
