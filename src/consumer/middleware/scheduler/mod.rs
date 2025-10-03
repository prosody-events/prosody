//! Fair work-conserving scheduler for handler concurrency control.
//!
//! Enforces global concurrency limits while prioritizing keys by accumulated
//! virtual time, preventing monopolization by high-throughput keys and ensuring
//! timely execution of waiting tasks through urgency boosting.

use derive_builder::Builder;
use thiserror::Error;
use validator::{Validate, ValidationErrors};

use crate::consumer::event_context::EventContext;
use crate::consumer::message::ConsumerMessage;
use crate::consumer::middleware::scheduler::dispatch::{DispatchError, Dispatcher};
use crate::consumer::middleware::{
    ClassifyError, ErrorCategory, FallibleHandler, FallibleHandlerProvider, HandlerMiddleware,
};
use crate::consumer::{DemandType, Keyed};
use crate::telemetry::Telemetry;
use crate::timers::Trigger;
use crate::util::from_env_with_fallback;
use crate::{Partition, Topic};

mod decay;
mod dispatch;

/// Configuration for the scheduler middleware.
#[derive(Builder, Clone, Debug, Validate)]
pub struct SchedulerConfiguration {
    /// Maximum number of concurrent handler invocations across all keys.
    ///
    /// Tasks block until a permit becomes available. Lower values reduce peak
    /// load but may increase latency; higher values improve throughput but
    /// risk resource exhaustion.
    #[builder(default = "from_env_with_fallback(\"PROSODY_MAX_CONCURRENCY\", 32)?")]
    #[validate(range(min = 1_usize))]
    pub max_permits: usize,
}

/// Middleware that applies fair scheduling to handler invocations.
///
/// Wraps handlers to enforce concurrency limits and priority-based dispatch.
#[derive(Clone, Debug)]
pub struct SchedulerMiddleware {
    dispatcher: Dispatcher,
}

/// Provider that creates scheduled handlers for each partition.
#[derive(Clone, Debug)]
pub struct SchedulerProvider<T> {
    provider: T,
    dispatcher: Dispatcher,
}

/// Handler wrapper that acquires scheduler permits before delegating to inner
/// handler.
///
/// Permits are released automatically when the handler completes, allowing
/// other tasks to proceed.
#[derive(Clone, Debug)]
pub struct SchedulerHandler<T> {
    handler: T,
    dispatcher: Dispatcher,
}

/// Errors that can occur during scheduled handler execution.
#[derive(Debug, Error)]
pub enum SchedulerError<E> {
    /// The inner handler returned an error.
    #[error(transparent)]
    Handler(E),

    /// Failed to acquire a permit from the scheduler.
    #[error("Failed to acquire scheduler permit: {0:#}")]
    PermitAcquisition(#[from] DispatchError),
}

/// Errors that can occur during scheduler initialization.
#[derive(Debug, Error)]
pub enum SchedulerInitError {
    /// Configuration validation failed.
    #[error("Invalid configuration: {0}")]
    Validation(#[from] ValidationErrors),
}

impl<E> ClassifyError for SchedulerError<E>
where
    E: ClassifyError,
{
    fn classify_error(&self) -> ErrorCategory {
        match self {
            SchedulerError::Handler(error) => error.classify_error(),
            SchedulerError::PermitAcquisition(error) => error.classify_error(),
        }
    }
}

impl SchedulerConfiguration {
    /// Creates a builder for constructing [`SchedulerConfiguration`].
    #[must_use]
    pub fn builder() -> SchedulerConfigurationBuilder {
        SchedulerConfigurationBuilder::default()
    }
}

impl SchedulerMiddleware {
    /// Creates a new scheduler middleware with the given configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if the configuration is invalid.
    pub fn new(
        config: &SchedulerConfiguration,
        telemetry: &Telemetry,
    ) -> Result<Self, SchedulerInitError> {
        config.validate()?;
        let dispatcher = Dispatcher::new(config.max_permits, telemetry);
        Ok(Self { dispatcher })
    }
}

impl HandlerMiddleware for SchedulerMiddleware {
    type Provider<T: FallibleHandlerProvider> = SchedulerProvider<T>;

    fn with_provider<T>(&self, provider: T) -> Self::Provider<T>
    where
        T: FallibleHandlerProvider,
    {
        SchedulerProvider {
            provider,
            dispatcher: self.dispatcher.clone(),
        }
    }
}

impl<T> FallibleHandlerProvider for SchedulerProvider<T>
where
    T: FallibleHandlerProvider,
{
    type Handler = SchedulerHandler<T::Handler>;

    fn handler_for_partition(&self, topic: Topic, partition: Partition) -> Self::Handler {
        SchedulerHandler {
            handler: self.provider.handler_for_partition(topic, partition),
            dispatcher: self.dispatcher.clone(),
        }
    }
}

impl<T> FallibleHandler for SchedulerHandler<T>
where
    T: FallibleHandler,
{
    type Error = SchedulerError<T::Error>;

    async fn on_message<C>(
        &self,
        context: C,
        message: ConsumerMessage,
        demand_type: DemandType,
    ) -> Result<(), Self::Error>
    where
        C: EventContext,
    {
        let _permit = self
            .dispatcher
            .get_permit(message.key().clone(), demand_type)
            .await?;

        self.handler
            .on_message(context, message, demand_type)
            .await
            .map_err(SchedulerError::Handler)
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
        let _permit = self
            .dispatcher
            .get_permit(trigger.key.clone(), demand_type)
            .await?;

        self.handler
            .on_timer(context, trigger, demand_type)
            .await
            .map_err(SchedulerError::Handler)
    }

    async fn shutdown(self) {
        self.handler.shutdown().await;
    }
}
