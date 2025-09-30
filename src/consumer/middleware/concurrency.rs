//! Global concurrency limiting middleware.
//!
//! Enforces a semaphore-based limit on concurrent message/timer processing
//! across all partitions. Position as the **outermost** middleware layer for
//! system-wide backpressure.
//!
//! # Execution Order
//!
//! **Request Path:**
//! 1. **Acquire semaphore permit** - Blocks if global limit reached
//! 2. Pass control to inner middleware layers
//!
//! **Response Path:**
//! 1. Receive result from inner layers
//! 2. **Release semaphore permit** - Always occurs via RAII guard
//!
//! # Key Features
//!
//! - **System Protection**: Prevents overload by limiting total concurrent
//!   operations
//! - **Fair Access**: Semaphore ensures equitable resource distribution across
//!   partitions
//! - **Late Acquisition**: Permit acquired immediately before handler execution
//! - **Guaranteed Release**: Permit released even on panic or error
//!
//! # Usage
//!
//! Position as outermost middleware for optimal backpressure:
//!
//! ```rust
//! use prosody::consumer::middleware::*;
//!
//! let provider = ConcurrencyLimitMiddleware::new(&config)
//!     .layer(ShutdownMiddleware)
//!     .layer(RetryMiddleware::new(retry_config))
//!     .into_provider(handler);
//! ```

use std::sync::Arc;

use derive_builder::Builder;
use thiserror::Error;
use tokio::sync::{AcquireError, Semaphore};
use tracing::debug;
use validator::{Validate, ValidationErrors};

use crate::consumer::event_context::EventContext;
use crate::consumer::message::ConsumerMessage;
use crate::consumer::middleware::{
    ClassifyError, ErrorCategory, FallibleHandler, FallibleHandlerProvider, HandlerMiddleware,
};
use crate::timers::Trigger;
use crate::util::from_env_with_fallback;
use crate::{Partition, Topic};

/// Configuration for global concurrency limiting.
#[derive(Builder, Clone, Debug, Validate)]
pub struct ConcurrencyLimitConfiguration {
    /// Maximum number of concurrent operations allowed globally.
    ///
    /// Environment variable: `PROSODY_MAX_CONCURRENCY`
    /// Default: 32
    #[builder(default = "from_env_with_fallback(\"PROSODY_MAX_CONCURRENCY\", 32)?")]
    #[validate(range(min = 1_usize))]
    pub max_permits: usize,
}

/// Middleware that applies global concurrency limits.
///
/// This middleware should be composed **first** in the middleware chain to
/// ensure the concurrency permit is acquired as late as possible - right before
/// the user handler executes.
#[derive(Clone, Debug)]
pub struct ConcurrencyLimitMiddleware {
    global_limit: Arc<Semaphore>,
}

/// A provider that enforces global concurrency limits.
#[derive(Clone, Debug)]
pub struct ConcurrencyLimitProvider<T> {
    provider: T,
    global_limit: Arc<Semaphore>,
}

/// A handler that enforces global concurrency limits.
#[derive(Clone, Debug)]
pub struct ConcurrencyLimitHandler<T> {
    handler: T,
    global_limit: Arc<Semaphore>,
}

/// Error type for concurrency limit failures.
#[derive(Debug, Error)]
pub enum ConcurrencyLimitError<E> {
    /// Error from the wrapped handler.
    #[error(transparent)]
    Handler(E),

    /// Error from permit acquisition (semaphore closed).
    #[error("Failed to acquire concurrency permit: {0:#}")]
    PermitAcquisition(#[from] AcquireError),
}

impl<E> ClassifyError for ConcurrencyLimitError<E>
where
    E: ClassifyError,
{
    fn classify_error(&self) -> ErrorCategory {
        match self {
            ConcurrencyLimitError::Handler(error) => error.classify_error(),
            ConcurrencyLimitError::PermitAcquisition(_) => ErrorCategory::Terminal,
        }
    }
}

impl ConcurrencyLimitConfiguration {
    /// Creates a new [`ConcurrencyLimitConfigurationBuilder`].
    ///
    /// # Returns
    ///
    /// A [`ConcurrencyLimitConfigurationBuilder`] instance.
    #[must_use]
    pub fn builder() -> ConcurrencyLimitConfigurationBuilder {
        ConcurrencyLimitConfigurationBuilder::default()
    }
}

impl ConcurrencyLimitMiddleware {
    /// Creates a new concurrency limit middleware.
    ///
    /// # Arguments
    ///
    /// * `config` - Configuration containing the maximum number of permits
    ///
    /// # Returns
    ///
    /// A new `ConcurrencyLimitMiddleware` instance
    ///
    /// # Errors
    ///
    /// Returns [`ValidationErrors`] if the configuration is invalid.
    pub fn new(config: &ConcurrencyLimitConfiguration) -> Result<Self, ValidationErrors> {
        config.validate()?;
        let global_limit = Arc::new(Semaphore::new(config.max_permits));
        Ok(Self { global_limit })
    }
}

impl HandlerMiddleware for ConcurrencyLimitMiddleware {
    type Provider<T: FallibleHandlerProvider> = ConcurrencyLimitProvider<T>;

    fn with_provider<T>(&self, provider: T) -> Self::Provider<T>
    where
        T: FallibleHandlerProvider,
    {
        ConcurrencyLimitProvider {
            provider,
            global_limit: self.global_limit.clone(),
        }
    }
}

impl<T> FallibleHandlerProvider for ConcurrencyLimitProvider<T>
where
    T: FallibleHandlerProvider,
{
    type Handler = ConcurrencyLimitHandler<T::Handler>;

    fn handler_for_partition(&self, topic: Topic, partition: Partition) -> Self::Handler {
        ConcurrencyLimitHandler {
            handler: self.provider.handler_for_partition(topic, partition),
            global_limit: self.global_limit.clone(),
        }
    }
}

impl<T> FallibleHandler for ConcurrencyLimitHandler<T>
where
    T: FallibleHandler,
{
    type Error = ConcurrencyLimitError<T::Error>;

    async fn on_message<C>(&self, context: C, message: ConsumerMessage) -> Result<(), Self::Error>
    where
        C: EventContext,
    {
        // Acquire permit as late as possible, just before handler execution
        debug!(?message, "acquiring concurrency permit");
        let _permit = self.global_limit.acquire().await?;
        debug!(?message, "permit acquired; calling handler");

        // Call the wrapped handler
        self.handler
            .on_message(context, message)
            .await
            .map_err(ConcurrencyLimitError::Handler)
    }

    async fn on_timer<C>(&self, context: C, trigger: Trigger) -> Result<(), Self::Error>
    where
        C: EventContext,
    {
        // Acquire permit as late as possible, just before handler execution
        debug!(?trigger, "acquiring concurrency permit");
        let _permit = self.global_limit.acquire().await?;
        debug!(?trigger, "permit acquired; calling handler");

        // Call the wrapped handler
        self.handler
            .on_timer(context, trigger)
            .await
            .map_err(ConcurrencyLimitError::Handler)
    }
}
