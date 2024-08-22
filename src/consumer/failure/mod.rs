//! Failure handling strategies for message processing.
//!
//! This module provides traits and structures for implementing
//! and composing failure handling strategies in asynchronous
//! message processing systems.

use std::fmt::Display;
use std::future::Future;

use crate::consumer::message::{ConsumerMessage, MessageContext};
use crate::consumer::HandlerProvider;

pub mod retry;
pub mod shutdown;
pub mod topic;

/// Categorizes errors in message processing.
#[derive(Debug)]
pub enum ErrorCategory {
    /// Error is temporary and recovery is possible
    Transient,
    /// Error is permanent and irrecoverable
    Permanent,
    /// Error is due to partition shutdown
    Terminal,
}

/// Defines methods for classifying errors.
pub trait ClassifyError {
    /// Classifies the error into a specific `ErrorCategory`.
    ///
    /// # Returns
    ///
    /// An `ErrorCategory` indicating the nature of the error.
    fn classify_error(&self) -> ErrorCategory;

    /// Determines if the error is recoverable.
    ///
    /// # Returns
    ///
    /// `true` if the error is classified as `ErrorCategory::Transient`, `false`
    /// otherwise.
    fn is_recoverable(&self) -> bool {
        matches!(self.classify_error(), ErrorCategory::Transient)
    }
}

/// Defines a failure handling strategy for message processing.
pub trait FailureStrategy {
    /// Wraps a handler with this failure strategy.
    ///
    /// # Arguments
    ///
    /// * `handler` - The handler to wrap with this strategy.
    ///
    /// # Returns
    ///
    /// A new handler that implements both `HandlerProvider` and
    /// `FallibleHandler`.
    fn with_handler<T>(&self, handler: T) -> impl HandlerProvider + FallibleHandler
    where
        T: FallibleHandler;

    /// Composes this strategy with another strategy.
    ///
    /// # Arguments
    ///
    /// * `next_handler` - The next strategy to apply if this one fails.
    ///
    /// # Returns
    ///
    /// A `ComposedStrategy` combining this strategy with the next one.
    fn and_then<T>(self, next_handler: T) -> ComposedStrategy<Self, T>
    where
        Self: Sized,
    {
        ComposedStrategy(self, next_handler)
    }
}

/// Defines a handler that can fail during message processing.
pub trait FallibleHandler: Clone + Send + Sync + 'static {
    /// The error type returned by this handler.
    type Error: ClassifyError + Display + Send;

    /// Handles a message, potentially returning an error.
    ///
    /// # Arguments
    ///
    /// * `context` - The context of the message being processed.
    /// * `message` - The message to be processed.
    ///
    /// # Returns
    ///
    /// A `Future` that resolves to `Ok(())` if the message was processed
    /// successfully, or an `Err` containing the error if processing failed.
    fn on_message(
        &self,
        context: MessageContext,
        message: ConsumerMessage,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// A composition of two failure strategies.
#[derive(Clone, Debug)]
pub struct ComposedStrategy<S1, S2>(S1, S2);

impl<S1, S2> FailureStrategy for ComposedStrategy<S1, S2>
where
    S1: FailureStrategy,
    S2: FailureStrategy,
{
    fn with_handler<T>(&self, handler: T) -> impl HandlerProvider + FallibleHandler
    where
        T: FallibleHandler,
    {
        // Apply the second strategy to the result of applying the first strategy
        self.1.with_handler(self.0.with_handler(handler))
    }
}
