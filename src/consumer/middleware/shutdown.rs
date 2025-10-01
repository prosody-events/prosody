//! Graceful shutdown middleware for partition revocation.
//!
//! Monitors partition revocation signals and immediately stops processing new
//! messages when a partition is being revoked. Returns terminal errors to abort
//! processing gracefully.
//!
//! # Execution Order
//!
//! **Request Path:**
//! 1. **Check shutdown signal** - Return terminal error if partition being
//!    revoked
//! 2. Pass control to inner middleware layers (if not shutting down)
//!
//! **Response Path:**
//! 1. Receive result from inner layers
//! 2. Pass result through unchanged
//!
//! # Shutdown Behavior
//!
//! - **Signal Detection**: Monitors [`EventContext::is_shutdown_requested`]
//! - **Immediate Stop**: Returns [`ShutdownError`] when revocation detected
//! - **Terminal Classification**: Error classified as
//!   [`ErrorCategory::Terminal`]
//! - **Graceful Abort**: Allows in-flight operations to complete
//!
//! # Usage
//!
//! Position early in middleware stack to prevent unnecessary processing during
//! shutdown:
//!
//! ```rust
//! use prosody::consumer::middleware::*;
//!
//! let provider = ConcurrencyLimitMiddleware::new(&config)
//!     .layer(ShutdownMiddleware) // Check shutdown early
//!     .layer(RetryMiddleware::new(retry_config))
//!     .into_provider(handler);
//! ```
//!
//! [`EventContext::is_shutdown_requested`]: crate::consumer::event_context::EventContext
//! [`ErrorCategory::Terminal`]: crate::consumer::middleware::ErrorCategory::Terminal

use thiserror::Error;
use tracing::debug;

use crate::consumer::DemandType;
use crate::consumer::event_context::EventContext;
use crate::consumer::message::ConsumerMessage;
use crate::consumer::middleware::{
    ClassifyError, ErrorCategory, FallibleHandler, FallibleHandlerProvider, HandlerMiddleware,
};
use crate::timers::Trigger;
use crate::{Partition, Topic};

/// Middleware that checks if the partition is shutting down before running the
/// handler, preventing other middleware from delaying the shutdown.
#[derive(Clone, Copy, Debug)]
pub struct ShutdownMiddleware;

/// A provider that wraps handlers with shutdown functionality.
#[derive(Clone, Debug)]
pub struct ShutdownProvider<T> {
    provider: T,
}

/// Wraps a handler with shutdown functionality.
///
/// This struct adds shutdown checks to the wrapped handler's message
/// processing, ensuring that no new messages are processed when a
/// partition is being revoked.
#[derive(Clone, Debug)]
pub struct ShutdownHandler<T> {
    handler: T,
}

impl HandlerMiddleware for ShutdownMiddleware {
    type Provider<T: FallibleHandlerProvider> = ShutdownProvider<T>;

    fn with_provider<T>(&self, provider: T) -> Self::Provider<T>
    where
        T: FallibleHandlerProvider,
    {
        ShutdownProvider { provider }
    }
}

impl<T> FallibleHandlerProvider for ShutdownProvider<T>
where
    T: FallibleHandlerProvider,
{
    type Handler = ShutdownHandler<T::Handler>;

    fn handler_for_partition(&self, topic: Topic, partition: Partition) -> Self::Handler {
        ShutdownHandler {
            handler: self.provider.handler_for_partition(topic, partition),
        }
    }
}

impl<T> FallibleHandler for ShutdownHandler<T>
where
    T: FallibleHandler,
{
    type Error = ShutdownError<T>;

    /// Processes a message, checking for shutdown conditions.
    ///
    /// # Arguments
    ///
    /// * `context` - The context of the message being processed.
    /// * `message` - The message to be processed.
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or a `ShutdownError`.
    ///
    /// # Errors
    ///
    /// Returns a `ShutdownError::Shutdown` if the partition is being revoked,
    /// or a `ShutdownError::Handler` containing the wrapped handler's error.
    async fn on_message<C>(
        &self,
        context: C,
        message: ConsumerMessage,
        demand_type: DemandType,
    ) -> Result<(), Self::Error>
    where
        C: EventContext,
    {
        if context.should_shutdown() {
            return Err(ShutdownError::Shutdown);
        }

        self.handler
            .on_message(context, message, demand_type)
            .await
            .map_err(ShutdownError::Handler)
    }

    async fn on_timer<C>(
        &self,
        context: C,
        timer: Trigger,
        demand_type: DemandType,
    ) -> Result<(), Self::Error>
    where
        C: EventContext,
    {
        if context.should_shutdown() {
            return Err(ShutdownError::Shutdown);
        }
        self.handler
            .on_timer(context, timer, demand_type)
            .await
            .map_err(ShutdownError::Handler)
    }

    async fn shutdown(self) {
        debug!("shutting down shutdown middleware handler");

        // No shutdown-specific state to clean up (signals are external)
        // Cascade shutdown to the inner handler
        self.handler.shutdown().await;
    }
}

/// Represents errors that can occur during shutdown handling.
#[derive(Debug, Error)]
pub enum ShutdownError<T>
where
    T: FallibleHandler,
{
    /// Indicates that the partition is being revoked.
    #[error("partition is being revoked")]
    Shutdown,

    /// Wraps an error from the underlying handler.
    #[error("handler error: {0:#}")]
    Handler(T::Error),
}

impl<T> ClassifyError for ShutdownError<T>
where
    T: FallibleHandler,
{
    /// Classifies the shutdown error.
    ///
    /// # Returns
    ///
    /// An `ErrorCategory` indicating the nature of the error:
    /// - `ErrorCategory::Terminal` for `ShutdownError::Shutdown`
    /// - The classification of the wrapped error for `ShutdownError::Handler`
    fn classify_error(&self) -> ErrorCategory {
        match self {
            ShutdownError::Shutdown => ErrorCategory::Terminal,
            ShutdownError::Handler(error) => error.classify_error(),
        }
    }
}
