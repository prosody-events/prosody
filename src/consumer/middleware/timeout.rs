//! Fixed timeout middleware for handler execution.
//!
//! Enforces a fixed timeout on handler invocations to prevent indefinite
//! blocking.
//!
//! # Execution
//!
//! **Request Path:**
//! 1. Race handler execution against configured timeout
//! 2. Return handler result or timeout error
//!
//! # Configuration
//!
//! - `timeout`: Fixed timeout duration (default: 80% of stall threshold)

use std::time::Duration;

use derive_builder::Builder;
use thiserror::Error;
use tokio::select;
use tokio::time::sleep;
use validator::{Validate, ValidationErrors};

use crate::consumer::DemandType;
use crate::consumer::event_context::EventContext;
use crate::consumer::message::ConsumerMessage;
use crate::consumer::middleware::{
    ClassifyError, ErrorCategory, FallibleHandler, FallibleHandlerProvider, HandlerMiddleware,
};
use crate::timers::Trigger;
use crate::util::from_option_duration_env;
use crate::{Partition, Topic};

/// Configuration for fixed timeout policy.
#[derive(Builder, Clone, Debug, Validate)]
pub struct TimeoutConfiguration {
    /// Fixed timeout duration for handler execution.
    ///
    /// Environment variable: `PROSODY_TIMEOUT`
    /// Default: 80% of stall threshold (typically 4 minutes when stall
    /// threshold is 5 minutes)
    ///
    /// Set to "none" to use the default (80% of stall threshold).
    #[builder(
        default = "from_option_duration_env(\"PROSODY_TIMEOUT\")?",
        setter(into)
    )]
    pub timeout: Option<Duration>,
}

/// Middleware that applies fixed timeouts to handler execution.
#[derive(Clone, Debug)]
pub struct TimeoutMiddleware {
    timeout: Duration,
}

/// Provider that creates timeout handlers for each partition.
#[derive(Clone, Debug)]
pub struct TimeoutProvider<T> {
    provider: T,
    timeout: Duration,
}

/// Handler wrapper that enforces timeouts on inner handler invocations.
#[derive(Clone, Debug)]
pub struct TimeoutHandler<T> {
    handler: T,
    timeout: Duration,
}

/// Errors that can occur during timeout handling.
#[derive(Debug, Error)]
pub enum TimeoutError<E> {
    /// The inner handler returned an error.
    #[error(transparent)]
    Handler(E),

    /// The operation timed out.
    #[error("Operation timed out after {0:?}")]
    Timeout(Duration),
}

/// Errors that can occur during timeout middleware initialization.
#[derive(Debug, Error)]
pub enum TimeoutInitError {
    /// Configuration validation failed.
    #[error("Invalid configuration: {0:#}")]
    Validation(#[from] ValidationErrors),
}

impl<E> ClassifyError for TimeoutError<E>
where
    E: ClassifyError,
{
    fn classify_error(&self) -> ErrorCategory {
        match self {
            TimeoutError::Handler(error) => error.classify_error(),
            TimeoutError::Timeout(_) => ErrorCategory::Transient,
        }
    }
}

impl TimeoutConfiguration {
    /// Creates a builder for constructing [`TimeoutConfiguration`].
    #[must_use]
    pub fn builder() -> TimeoutConfigurationBuilder {
        TimeoutConfigurationBuilder::default()
    }
}

impl TimeoutMiddleware {
    /// Creates a new timeout middleware with the given configuration.
    ///
    /// # Arguments
    ///
    /// * `config` - Configuration for the timeout duration
    /// * `stall_threshold` - The stall threshold duration from consumer
    ///   configuration, used to calculate the default timeout (80% of this
    ///   value)
    ///
    /// # Errors
    ///
    /// Returns an error if the configuration validation fails
    pub fn new(
        config: &TimeoutConfiguration,
        stall_threshold: Duration,
    ) -> Result<Self, TimeoutInitError> {
        config.validate()?;
        let timeout = config.timeout.unwrap_or_else(|| stall_threshold * 4 / 5);
        Ok(Self { timeout })
    }
}

impl HandlerMiddleware for TimeoutMiddleware {
    type Provider<T: FallibleHandlerProvider> = TimeoutProvider<T>;

    fn with_provider<T>(&self, provider: T) -> Self::Provider<T>
    where
        T: FallibleHandlerProvider,
    {
        TimeoutProvider {
            provider,
            timeout: self.timeout,
        }
    }
}

impl<T> FallibleHandlerProvider for TimeoutProvider<T>
where
    T: FallibleHandlerProvider,
{
    type Handler = TimeoutHandler<T::Handler>;

    fn handler_for_partition(&self, topic: Topic, partition: Partition) -> Self::Handler {
        TimeoutHandler {
            handler: self.provider.handler_for_partition(topic, partition),
            timeout: self.timeout,
        }
    }
}

impl<T> FallibleHandler for TimeoutHandler<T>
where
    T: FallibleHandler,
{
    type Error = TimeoutError<T::Error>;

    async fn on_message<C>(
        &self,
        context: C,
        message: ConsumerMessage,
        demand_type: DemandType,
    ) -> Result<(), Self::Error>
    where
        C: EventContext,
    {
        select! {
            result = self.handler.on_message(context, message, demand_type) => {
                result.map_err(TimeoutError::Handler)
            }
            () = sleep(self.timeout) => {
                Err(TimeoutError::Timeout(self.timeout))
            }
        }
    }

    async fn on_timer<C>(
        &self,
        context: C,
        trigger: Trigger,
        demand_type: DemandType,
    ) -> Result<(), Self::Error>
    where
        C: EventContext,
    {
        select! {
            result = self.handler.on_timer(context, trigger, demand_type) => {
                result.map_err(TimeoutError::Handler)
            }
            () = sleep(self.timeout) => {
                Err(TimeoutError::Timeout(self.timeout))
            }
        }
    }

    async fn shutdown(self) {
        self.handler.shutdown().await;
    }
}
