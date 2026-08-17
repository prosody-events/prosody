//! Fixed timeout middleware for handler execution.
//!
//! Enforces a fixed timeout on the inner handler's per-event work to prevent
//! indefinite blocking. This middleware is a pure pass-through with respect
//! to the apply hooks: `Output` and `Error` are forwarded unchanged from the
//! inner handler, and `after_commit` / `after_abort` are delegated verbatim.
//!
//! # Execution
//!
//! **Work Path:**
//! 1. Race the inner handler's per-event work against the configured timeout.
//! 2. If the timeout fires first, signal cancellation via `context.cancel()`
//!    and continue awaiting the inner future. The inner handler is always given
//!    a chance to observe the signal and return `Ok` or `Err`, so the outcome
//!    that flows into the apply hook always reflects the inner's actual return.
//! 3. Reset the cancellation flag on the way out so a retry of the same logical
//!    event starts with a clean context.
//!
//! # Apply-hook invariant
//!
//! Because this middleware never short-circuits the inner handler with a
//! synthetic result and never swallows its return, the framework's
//! exactly-one apply-hook contract is preserved by simple delegation: every
//! `on_message` / `on_timer` invocation that runs to completion produces an
//! inner outcome that is forwarded to exactly one of `after_commit` or
//! `after_abort` on this handler. The inner is invoked at most once per call;
//! per-invocation invariant trivially upheld.
//!
//! # Configuration
//!
//! - `timeout`: Fixed timeout duration (default: 80% of stall threshold)

use std::future::Future;
use std::time::Duration;

use derive_builder::Builder;
use humantime::format_duration;
use thiserror::Error;
use tokio::select;
use tokio::time::{Instant, sleep};
use tracing::{debug, warn};
use validator::{Validate, ValidationErrors};

use crate::consumer::DemandType;
use crate::consumer::event_context::EventContext;
use crate::consumer::message::ConsumerMessage;
use crate::consumer::middleware::handler::{HandlerMethod, OnExcise, OnMessage};
use crate::consumer::middleware::{
    FallibleHandler, FallibleHandlerProvider, HandlerMiddleware, Settlement, SettlementHandler,
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

impl<T> TimeoutHandler<T> {
    /// Run an operation with timeout, signaling cancellation if exceeded.
    ///
    /// If the timeout fires before the operation completes, cancellation is
    /// signaled via `context.cancel()` and we continue waiting for the
    /// operation to finish. This ensures the handler has a chance to clean up
    /// before returning its result.
    async fn run_with_timeout<C, F, R, E>(
        &self,
        context: C,
        operation: F,
        event_type: &str,
    ) -> Result<R, E>
    where
        C: EventContext,
        F: Future<Output = Result<R, E>>,
    {
        let start = Instant::now();
        // Box::pin moves the downstream future to the heap. Without this,
        // the entire composed middleware chain (including large futures like
        // TimerManager::clear_and_schedule) lives on the stack frame,
        // overflowing tokio's worker thread stack.
        let mut operation = Box::pin(operation);

        select! {
            result = &mut operation => {
                debug!(
                    event_type,
                    elapsed = ?start.elapsed(),
                    "Handler completed within timeout"
                );
                result
            }
            () = sleep(self.timeout) => {
                warn!(
                    event_type,
                    timeout = %format_duration(self.timeout),
                    "Handler exceeded timeout, signaling cancellation"
                );
                context.cancel();

                // Wait for handler to finish cleanup after cancellation
                let cancel_start = Instant::now();
                let result = operation.await;
                let cleanup_elapsed = cancel_start.elapsed();

                debug!(
                    event_type,
                    cleanup_time = ?cleanup_elapsed,
                    total_elapsed = ?start.elapsed(),
                    "Handler completed after cancellation signal"
                );

                // Reset cancellation flag so retry can continue with clean state
                context.uncancel();

                result
            }
        }
    }
}

impl<T> TimeoutHandler<T>
where
    T: FallibleHandler,
{
    async fn handle<H, C>(
        &self,
        context: C,
        message: ConsumerMessage<H::MessagePayload>,
        demand_type: DemandType,
        kind: &'static str,
    ) -> Result<T::Output, T::Error>
    where
        H: HandlerMethod<T>,
        C: EventContext<Payload = T::Payload>,
    {
        self.run_with_timeout(
            context.clone(),
            H::call(&self.handler, context, message, demand_type),
            kind,
        )
        .await
    }
}

/// Errors that can occur during timeout middleware initialization.
#[derive(Debug, Error)]
pub enum TimeoutInitError {
    /// Configuration validation failed.
    #[error("Invalid configuration: {0:#}")]
    Validation(#[from] ValidationErrors),
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
    /// `stall_threshold` is the consumer's stall threshold duration; when
    /// `config` doesn't set an explicit timeout, it defaults to 80% of this
    /// value.
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

        debug!(
            timeout = %format_duration(timeout),
            stall_threshold = %format_duration(stall_threshold),
            custom = config.timeout.is_some(),
            "Timeout middleware initialized"
        );

        Ok(Self { timeout })
    }
}

impl<P: Send + Sync + 'static> HandlerMiddleware<P> for TimeoutMiddleware {
    type Provider<T>
        = TimeoutProvider<T>
    where
        T: FallibleHandlerProvider,
        T::Handler: FallibleHandler<Payload = P>;

    fn with_provider<T>(&self, provider: T) -> Self::Provider<T>
    where
        T: FallibleHandlerProvider,
        T::Handler: FallibleHandler<Payload = P>,
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
    type Error = T::Error;
    type Output = T::Output;
    type Payload = T::Payload;

    async fn on_message<C>(
        &self,
        context: C,
        message: ConsumerMessage<Self::Payload>,
        demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext<Payload = T::Payload>,
    {
        self.handle::<OnMessage, _>(context, message, demand_type, "message")
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
        self.handle::<OnExcise, _>(context, message, demand_type, "excise")
            .await
    }

    async fn on_timer<C>(
        &self,
        context: C,
        trigger: Trigger,
        demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext<Payload = T::Payload>,
    {
        self.run_with_timeout(
            context.clone(),
            self.handler.on_timer(context, trigger, demand_type),
            "timer",
        )
        .await
    }

    /// Forward the final-dispatch apply hook to the inner handler verbatim.
    ///
    /// `TimeoutHandler` is a pure pass-through: it never short-circuits the
    /// inner handler with a synthetic result. The inner handler is always
    /// given a chance to return (even after cancellation), so `result` here
    /// reflects the inner's actual return value and `after_commit` fires on
    /// the inner exactly when it would have absent this middleware.
    async fn after_commit<C>(&self, context: C, result: Result<Self::Output, Self::Error>)
    where
        C: EventContext<Payload = T::Payload>,
    {
        self.handler.after_commit(context, result).await;
    }

    /// Forward the non-final-dispatch apply hook to the inner handler verbatim.
    ///
    /// `TimeoutHandler` is a pure pass-through: it never short-circuits the
    /// inner handler with a synthetic result. The inner handler is always
    /// given a chance to return (even after cancellation), so `result` here
    /// reflects the inner's actual return value and `after_abort` fires on
    /// the inner exactly when it would have absent this middleware.
    async fn after_abort<C>(&self, context: C, result: Result<Self::Output, Self::Error>)
    where
        C: EventContext<Payload = T::Payload>,
    {
        self.handler.after_abort(context, result).await;
    }

    async fn shutdown(self) {
        debug!("Timeout handler shutting down");
        self.handler.shutdown().await;
    }
}

impl<T> SettlementHandler for TimeoutHandler<T>
where
    T: SettlementHandler,
{
    /// Pass-through: timeout adds no Output or error variants of its own (a
    /// timeout surfaces as the inner's own cancellation-shaped error).
    fn settlement(result: Result<&Self::Output, &Self::Error>) -> Settlement {
        T::settlement(result)
    }
}

#[cfg(test)]
mod tests;
