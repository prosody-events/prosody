//! Cancellation middleware for early exit when already cancelled.
//!
//! Checks cancellation state before invoking inner middleware. Prevents
//! starting new work when shutdown or cancellation has already been signaled.
//!
//! # Execution Order
//!
//! **Request Path:**
//! 1. **Check cancellation signals** - Return appropriate error if cancelled
//! 2. Pass control to inner middleware layers (if not cancelled)
//!
//! **Response Path:**
//! 1. Receive result from inner layers
//! 2. Pass result through unchanged
//!
//! # Cancellation Behavior
//!
//! The middleware distinguishes between three cases:
//!
//! - **Shutdown** (partition revoked, observed before inner ran): Returns
//!   [`CancellationError::Shutdown`] classified as [`ErrorCategory::Terminal`].
//!   Processing must stop immediately to release the partition. The inner
//!   handler did *not* run.
//!
//! - **Shutdown after inner ran**: The inner handler ran and returned a
//!   Transient error, but shutdown was signaled mid-flight. Returns
//!   [`CancellationError::ShutdownAfterInner`] (Terminal). The inner attempt
//!   *did* run and will be redelivered, so its apply-hook (`after_abort`) must
//!   still fire with the original inner error.
//!
//! - **Message cancellation**: Returns [`CancellationError::MessageCancelled`]
//!   classified as [`ErrorCategory::Transient`]. The retry middleware will
//!   continue retrying rather than aborting the message.
//!
//! # Apply hooks
//!
//! Forwards apply hooks to the inner only if the inner ran on this dispatch.
//! Short-circuiting on `Shutdown` or `MessageCancelled` suppresses both hooks
//! since the inner never executed.
//!
//! # Usage
//!
//! Position early in the stack, close to the handler, so cancellation is
//! observed before unnecessary work begins. See the
//! [module docs](crate::consumer::middleware) for a worked composition example.
//!
//! [`ErrorCategory::Terminal`]: crate::consumer::middleware::ErrorCategory::Terminal
//! [`ErrorCategory::Transient`]: crate::consumer::middleware::ErrorCategory::Transient

use thiserror::Error;
use tracing::debug;

use crate::consumer::DemandType;
use crate::consumer::event_context::EventContext;
use crate::consumer::message::ConsumerMessage;
use crate::consumer::middleware::handler::{HandlerMethod, OnExcise, OnMessage};
use crate::consumer::middleware::{
    ClassifyError, ErrorCategory, FallibleHandler, FallibleHandlerProvider, HandlerMiddleware,
    Settlement, SettlementHandler,
};
use crate::timers::Trigger;
use crate::{Partition, Topic};

/// Middleware that checks cancellation state before invoking the handler.
#[derive(Clone, Copy, Debug, Default)]
pub struct CancellationMiddleware;

impl CancellationMiddleware {
    /// Creates a new `CancellationMiddleware`.
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

/// Provider that wraps handlers with cancellation checks.
#[derive(Clone, Debug)]
pub struct CancellationProvider<T> {
    provider: T,
}

/// Handler wrapper that checks cancellation before delegating.
#[derive(Clone, Debug)]
pub struct CancellationHandler<T> {
    handler: T,
}

impl<T> CancellationHandler<T> {
    pub(crate) fn new(handler: T) -> Self {
        Self { handler }
    }
}

impl<T> CancellationHandler<T>
where
    T: FallibleHandler,
{
    async fn handle<H, C>(
        &self,
        context: C,
        message: ConsumerMessage<H::MessagePayload>,
        demand_type: DemandType,
    ) -> Result<T::Output, CancellationError<T::Error>>
    where
        H: HandlerMethod<T>,
        C: EventContext<Payload = T::Payload>,
    {
        if context.is_shutdown() {
            return Err(CancellationError::Shutdown);
        }
        if context.is_message_cancelled() {
            return Err(CancellationError::MessageCancelled);
        }
        H::call(&self.handler, context.clone(), message, demand_type)
            .await
            .map_err(|error| {
                if context.is_shutdown()
                    && matches!(error.classify_error(), ErrorCategory::Transient)
                {
                    CancellationError::ShutdownAfterInner(error)
                } else {
                    CancellationError::Handler(error)
                }
            })
    }
}

impl<P: Send + Sync + 'static> HandlerMiddleware<P> for CancellationMiddleware {
    type Provider<T>
        = CancellationProvider<T>
    where
        T: FallibleHandlerProvider,
        T::Handler: FallibleHandler<Payload = P>;

    fn with_provider<T>(&self, provider: T) -> Self::Provider<T>
    where
        T: FallibleHandlerProvider,
        T::Handler: FallibleHandler<Payload = P>,
    {
        CancellationProvider { provider }
    }
}

impl<T> FallibleHandlerProvider for CancellationProvider<T>
where
    T: FallibleHandlerProvider,
{
    type Handler = CancellationHandler<T::Handler>;

    fn handler_for_partition(&self, topic: Topic, partition: Partition) -> Self::Handler {
        CancellationHandler::new(self.provider.handler_for_partition(topic, partition))
    }
}

impl<T> FallibleHandler for CancellationHandler<T>
where
    T: FallibleHandler,
{
    type Error = CancellationError<T::Error>;
    type Output = T::Output;
    type Payload = T::Payload;

    /// Checks cancellation state, then delegates to inner handler if clear.
    /// Transient errors from the inner are promoted to
    /// [`CancellationError::ShutdownAfterInner`] if shutdown is active on
    /// return, preserving the inner error for `after_abort` routing.
    async fn on_message<C>(
        &self,
        context: C,
        message: ConsumerMessage<Self::Payload>,
        demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        self.handle::<OnMessage, _>(context, message, demand_type)
            .await
    }

    async fn on_excise<C>(
        &self,
        context: C,
        message: ConsumerMessage<()>,
        demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        self.handle::<OnExcise, _>(context, message, demand_type)
            .await
    }

    async fn on_timer<C>(
        &self,
        context: C,
        timer: Trigger,
        demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        if context.is_shutdown() {
            return Err(CancellationError::Shutdown);
        }
        if context.is_message_cancelled() {
            return Err(CancellationError::MessageCancelled);
        }

        self.handler
            .on_timer(context.clone(), timer, demand_type)
            .await
            .map_err(|error| {
                if context.is_shutdown()
                    && matches!(error.classify_error(), ErrorCategory::Transient)
                {
                    CancellationError::ShutdownAfterInner(error)
                } else {
                    CancellationError::Handler(error)
                }
            })
    }

    /// Forwards `after_commit` to the inner if the inner ran; suppresses if
    /// the dispatch was short-circuited before the inner.
    async fn after_commit<C>(&self, context: C, result: Result<Self::Output, Self::Error>)
    where
        C: EventContext<Payload = Self::Payload>,
    {
        match result {
            Ok(output) => self.handler.after_commit(context, Ok(output)).await,
            Err(
                CancellationError::Handler(inner) | CancellationError::ShutdownAfterInner(inner),
            ) => {
                self.handler.after_commit(context, Err(inner)).await;
            }
            // Inner did not run — nothing to forward.
            Err(CancellationError::Shutdown | CancellationError::MessageCancelled) => {}
        }
    }

    /// Forwards `after_abort` to the inner if the inner ran; suppresses if
    /// the dispatch was short-circuited before the inner.
    async fn after_abort<C>(&self, context: C, result: Result<Self::Output, Self::Error>)
    where
        C: EventContext<Payload = Self::Payload>,
    {
        match result {
            Ok(output) => self.handler.after_abort(context, Ok(output)).await,
            Err(
                CancellationError::Handler(inner) | CancellationError::ShutdownAfterInner(inner),
            ) => {
                self.handler.after_abort(context, Err(inner)).await;
            }
            // Inner did not run — nothing to forward.
            Err(CancellationError::Shutdown | CancellationError::MessageCancelled) => {}
        }
    }

    async fn shutdown(self) {
        debug!("shutting down cancellation handler");
        self.handler.shutdown().await;
    }
}

impl<T> SettlementHandler for CancellationHandler<T>
where
    T: SettlementHandler,
{
    fn settlement(result: Result<&Self::Output, &Self::Error>) -> Settlement {
        match result {
            // Inner ran: delegate. `ShutdownAfterInner` carries the inner's
            // own error, so its settlement is the inner's — though the
            // variant classifies Terminal, and settle's Terminal-first check
            // abandons before this classification is consulted.
            Ok(output) => T::settlement(Ok(output)),
            Err(
                CancellationError::Handler(error) | CancellationError::ShutdownAfterInner(error),
            ) => T::settlement(Err(error)),
            // Pre-inner admission rejections: the inner never ran, so the
            // result is this layer's, not the event's. (Belt and braces for
            // `Shutdown` — Terminal-first already owns its abandon.)
            Err(CancellationError::Shutdown | CancellationError::MessageCancelled) => {
                Settlement::Bypassed
            }
        }
    }
}

/// Errors from the cancellation middleware.
///
/// `Shutdown` and `MessageCancelled` mean the inner did not run (no apply
/// hook forwarded). `ShutdownAfterInner` and `Handler` mean it did.
#[derive(Debug, Error)]
pub enum CancellationError<T> {
    /// Partition revoked before the inner ran. Terminal; inner apply hooks
    /// suppressed.
    #[error("partition is being revoked")]
    Shutdown,

    /// Inner ran and returned a Transient error while shutdown was signaled;
    /// promoted to Terminal. Carries the inner error for `after_abort`.
    #[error("partition is being revoked (inner attempt: {0:#})")]
    ShutdownAfterInner(T),

    /// Message cancelled before the inner ran. Transient; inner apply hooks
    /// suppressed.
    #[error("message processing was cancelled")]
    MessageCancelled,

    /// Inner ran and returned an error that was not promoted.
    #[error("handler error: {0:#}")]
    Handler(T),
}

impl<T> ClassifyError for CancellationError<T>
where
    T: ClassifyError,
{
    fn classify_error(&self) -> ErrorCategory {
        match self {
            CancellationError::Shutdown | CancellationError::ShutdownAfterInner(_) => {
                ErrorCategory::Terminal
            }
            CancellationError::MessageCancelled => ErrorCategory::Transient,
            CancellationError::Handler(error) => error.classify_error(),
        }
    }
}

#[cfg(test)]
mod tests;
