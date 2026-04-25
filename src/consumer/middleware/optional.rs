//! Optional middleware support.
//!
//! This module provides a [`HandlerMiddleware`] implementation for
//! [`Option<M>`] that conditionally enables middleware at runtime. When `None`,
//! the middleware is bypassed entirely.
//!
//! Exactly one of the two inner branches (Enabled or Disabled) runs per call,
//! and apply hooks are routed to the same branch via the variant tags on
//! `OptionOutput`/`OptionError`. The inner is invoked at most once per call;
//! per-invocation invariant trivially upheld.
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
    type Output = OptionOutput<E::Output, D::Output>;

    async fn on_message<C>(
        &self,
        context: C,
        message: ConsumerMessage,
        demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext,
    {
        match self {
            Self::Enabled(handler) => handler
                .on_message(context, message, demand_type)
                .await
                .map(OptionOutput::Enabled)
                .map_err(OptionError::Enabled),
            Self::Disabled(handler) => handler
                .on_message(context, message, demand_type)
                .await
                .map(OptionOutput::Disabled)
                .map_err(OptionError::Disabled),
        }
    }

    async fn on_timer<C>(
        &self,
        context: C,
        trigger: Trigger,
        demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext,
    {
        match self {
            Self::Enabled(handler) => handler
                .on_timer(context, trigger, demand_type)
                .await
                .map(OptionOutput::Enabled)
                .map_err(OptionError::Enabled),
            Self::Disabled(handler) => handler
                .on_timer(context, trigger, demand_type)
                .await
                .map(OptionOutput::Disabled)
                .map_err(OptionError::Disabled),
        }
    }

    /// Forwards the apply hook to whichever inner handler actually ran.
    ///
    /// The `OptionOutput`/`OptionError` variant tags identify which inner
    /// produced the result, so exactly one inner `after_commit` is invoked —
    /// preserving the per-handler "exactly one apply hook per dispatch"
    /// invariant. Mismatched `(self, result)` variants are unreachable: the
    /// handler that produced the result is the same one being asked to
    /// consume it.
    async fn after_commit<C>(&self, context: C, result: Result<Self::Output, Self::Error>)
    where
        C: EventContext,
    {
        match (self, result) {
            (Self::Enabled(handler), Ok(OptionOutput::Enabled(o))) => {
                handler.after_commit(context, Ok(o)).await;
            }
            (Self::Enabled(handler), Err(OptionError::Enabled(e))) => {
                handler.after_commit(context, Err(e)).await;
            }
            (Self::Disabled(handler), Ok(OptionOutput::Disabled(o))) => {
                handler.after_commit(context, Ok(o)).await;
            }
            (Self::Disabled(handler), Err(OptionError::Disabled(e))) => {
                handler.after_commit(context, Err(e)).await;
            }
            // Mismatched variants cannot occur: the handler that produced
            // the result is the same one being asked to consume it. Trip a
            // debug assertion so any future breakage of that invariant is
            // caught loudly in tests; release builds still no-op safely.
            _ => debug_assert!(false, "OptionHandler variant mismatch in after_commit"),
        }
    }

    /// Forwards the apply hook to whichever inner handler actually ran.
    ///
    /// As with [`after_commit`](Self::after_commit), the variant tags route
    /// the result to the inner that produced it, so exactly one inner
    /// `after_abort` fires. Mismatched variants are unreachable.
    async fn after_abort<C>(&self, context: C, result: Result<Self::Output, Self::Error>)
    where
        C: EventContext,
    {
        match (self, result) {
            (Self::Enabled(handler), Ok(OptionOutput::Enabled(o))) => {
                handler.after_abort(context, Ok(o)).await;
            }
            (Self::Enabled(handler), Err(OptionError::Enabled(e))) => {
                handler.after_abort(context, Err(e)).await;
            }
            (Self::Disabled(handler), Ok(OptionOutput::Disabled(o))) => {
                handler.after_abort(context, Ok(o)).await;
            }
            (Self::Disabled(handler), Err(OptionError::Disabled(e))) => {
                handler.after_abort(context, Err(e)).await;
            }
            // Mismatched variants cannot occur (see `after_commit`).
            _ => debug_assert!(false, "OptionHandler variant mismatch in after_abort"),
        }
    }

    async fn shutdown(self) {
        match self {
            Self::Enabled(handler) => handler.shutdown().await,
            Self::Disabled(handler) => handler.shutdown().await,
        }
    }
}

/// Output from optional middleware, mirroring the [`OptionHandler`]
/// enabled/disabled split.
#[derive(Debug)]
pub enum OptionOutput<E, D> {
    /// Output from the enabled middleware.
    Enabled(E),

    /// Output from the disabled passthrough.
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
