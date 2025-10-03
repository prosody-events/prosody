#![allow(dead_code, missing_docs, unused_variables, clippy::missing_errors_doc)] //todo: remove

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

#[derive(Builder, Clone, Debug, Validate)]
pub struct SchedulerConfiguration {
    #[builder(default = "from_env_with_fallback(\"PROSODY_MAX_CONCURRENCY\", 32)?")]
    #[validate(range(min = 1_usize))]
    pub max_permits: usize,
}

#[derive(Clone, Debug)]
pub struct SchedulerMiddleware {
    dispatcher: Dispatcher,
}

#[derive(Clone, Debug)]
pub struct SchedulerProvider<T> {
    provider: T,
    dispatcher: Dispatcher,
}

#[derive(Clone, Debug)]
pub struct SchedulerHandler<T> {
    handler: T,
    dispatcher: Dispatcher,
}

#[derive(Debug, Error)]
pub enum SchedulerError<E> {
    #[error(transparent)]
    Handler(E),

    #[error("Failed to acquire scheduler permit: {0:#}")]
    PermitAcquisition(#[from] DispatchError),
}

#[derive(Debug, Error)]
pub enum SchedulerInitError {
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
    #[must_use]
    pub fn builder() -> SchedulerConfigurationBuilder {
        SchedulerConfigurationBuilder::default()
    }
}

impl SchedulerMiddleware {
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
