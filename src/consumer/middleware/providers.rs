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

use crate::consumer::{EventHandler, HandlerProvider, Partition, Topic};

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
