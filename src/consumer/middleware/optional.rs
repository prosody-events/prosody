//! Optional middleware support.
//!
//! This module provides a [`HandlerMiddleware`] implementation for
//! [`Option<M>`] that conditionally enables middleware at runtime. When `None`,
//! the middleware is bypassed entirely.
//!
//! # Example
//!
//! ```rust,ignore
//! let middleware: Option<MonopolizationMiddleware> = if config.enabled {
//!     Some(MonopolizationMiddleware::new(&config, &telemetry)?)
//! } else {
//!     None
//! };
//!
//! let provider = common_middleware
//!     .layer(middleware)
//!     .layer(other_middleware)
//!     .into_provider(handler);
//! ```

use thiserror::Error;

use crate::consumer::DemandType;
use crate::consumer::event_context::EventContext;
use crate::consumer::message::ConsumerMessage;
use crate::consumer::middleware::{
    ClassifyError, ErrorCategory, FallibleHandler, FallibleHandlerProvider, HandlerMiddleware,
};
use crate::timers::Trigger;
use crate::{Partition, Topic};

impl<M> HandlerMiddleware for Option<M>
where
    M: HandlerMiddleware,
{
    type Provider<T: FallibleHandlerProvider> = OptionProvider<M::Provider<T>, T>;

    fn with_provider<T>(&self, provider: T) -> Self::Provider<T>
    where
        T: FallibleHandlerProvider,
    {
        match self {
            Some(middleware) => OptionProvider::Enabled(middleware.with_provider(provider)),
            None => OptionProvider::Disabled(provider),
        }
    }
}

/// Provider for optional middleware.
#[derive(Clone)]
pub enum OptionProvider<E, D> {
    /// Middleware is enabled.
    Enabled(E),
    /// Middleware is disabled.
    Disabled(D),
}

impl<E, D> FallibleHandlerProvider for OptionProvider<E, D>
where
    E: FallibleHandlerProvider,
    D: FallibleHandlerProvider,
{
    type Handler = OptionHandler<E::Handler, D::Handler>;

    fn handler_for_partition(&self, topic: Topic, partition: Partition) -> Self::Handler {
        match self {
            Self::Enabled(provider) => {
                OptionHandler::Enabled(provider.handler_for_partition(topic, partition))
            }
            Self::Disabled(provider) => {
                OptionHandler::Disabled(provider.handler_for_partition(topic, partition))
            }
        }
    }
}

/// Handler for optional middleware.
#[derive(Clone)]
pub enum OptionHandler<E, D> {
    /// Middleware is enabled.
    Enabled(E),
    /// Middleware is disabled.
    Disabled(D),
}

impl<E, D> OptionHandler<E, D> {
    /// Returns the enabled handler, if present.
    #[must_use]
    pub fn enabled(self) -> Option<E> {
        match self {
            Self::Enabled(h) => Some(h),
            Self::Disabled(_) => None,
        }
    }
}

impl<E, D> FallibleHandler for OptionHandler<E, D>
where
    E: FallibleHandler,
    D: FallibleHandler,
{
    type Error = OptionError<E::Error, D::Error>;
    type Outcome = OptionOutcome<E::Outcome, D::Outcome>;

    async fn on_message<C>(
        &self,
        context: C,
        message: ConsumerMessage,
        demand_type: DemandType,
    ) -> Result<Self::Outcome, Self::Error>
    where
        C: EventContext,
    {
        match self {
            Self::Enabled(handler) => handler
                .on_message(context, message, demand_type)
                .await
                .map(OptionOutcome::Enabled)
                .map_err(OptionError::Enabled),
            Self::Disabled(handler) => handler
                .on_message(context, message, demand_type)
                .await
                .map(OptionOutcome::Disabled)
                .map_err(OptionError::Disabled),
        }
    }

    async fn on_timer<C>(
        &self,
        context: C,
        trigger: Trigger,
        demand_type: DemandType,
    ) -> Result<Self::Outcome, Self::Error>
    where
        C: EventContext,
    {
        match self {
            Self::Enabled(handler) => handler
                .on_timer(context, trigger, demand_type)
                .await
                .map(OptionOutcome::Enabled)
                .map_err(OptionError::Enabled),
            Self::Disabled(handler) => handler
                .on_timer(context, trigger, demand_type)
                .await
                .map(OptionOutcome::Disabled)
                .map_err(OptionError::Disabled),
        }
    }

    async fn after_commit<C>(
        &self,
        context: C,
        result: Result<Self::Outcome, Self::Error>,
    ) where
        C: EventContext,
    {
        match (self, result) {
            (Self::Enabled(handler), Ok(OptionOutcome::Enabled(o))) => {
                handler.after_commit(context, Ok(o)).await;
            }
            (Self::Enabled(handler), Err(OptionError::Enabled(e))) => {
                handler.after_commit(context, Err(e)).await;
            }
            (Self::Disabled(handler), Ok(OptionOutcome::Disabled(o))) => {
                handler.after_commit(context, Ok(o)).await;
            }
            (Self::Disabled(handler), Err(OptionError::Disabled(e))) => {
                handler.after_commit(context, Err(e)).await;
            }
            // Mismatched variants cannot occur: the handler that produced
            // the result is the same one being asked to consume it.
            _ => {}
        }
    }

    async fn after_abort<C>(
        &self,
        context: C,
        result: Result<Self::Outcome, Self::Error>,
    ) where
        C: EventContext,
    {
        match (self, result) {
            (Self::Enabled(handler), Ok(OptionOutcome::Enabled(o))) => {
                handler.after_abort(context, Ok(o)).await;
            }
            (Self::Enabled(handler), Err(OptionError::Enabled(e))) => {
                handler.after_abort(context, Err(e)).await;
            }
            (Self::Disabled(handler), Ok(OptionOutcome::Disabled(o))) => {
                handler.after_abort(context, Ok(o)).await;
            }
            (Self::Disabled(handler), Err(OptionError::Disabled(e))) => {
                handler.after_abort(context, Err(e)).await;
            }
            _ => {}
        }
    }

    async fn shutdown(self) {
        match self {
            Self::Enabled(handler) => handler.shutdown().await,
            Self::Disabled(handler) => handler.shutdown().await,
        }
    }
}

/// Outcome from optional middleware, mirroring the [`OptionHandler`]
/// enabled/disabled split.
#[derive(Debug)]
pub enum OptionOutcome<E, D> {
    /// Outcome from the enabled middleware.
    Enabled(E),

    /// Outcome from the disabled passthrough.
    Disabled(D),
}

/// Error from optional middleware.
#[derive(Debug, Error)]
pub enum OptionError<E, D> {
    /// Error from enabled middleware.
    #[error(transparent)]
    Enabled(E),

    /// Error from disabled passthrough.
    #[error(transparent)]
    Disabled(D),
}

impl<E, D> ClassifyError for OptionError<E, D>
where
    E: ClassifyError,
    D: ClassifyError,
{
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::Enabled(e) => e.classify_error(),
            Self::Disabled(e) => e.classify_error(),
        }
    }
}
