//! Exponential backoff retry middleware.
//!
//! Automatically retries transient failures using exponential backoff with
//! jitter. Only retries [`ErrorCategory::Transient`] errors - permanent and
//! terminal errors are passed through immediately.
//!
//! # Execution Order
//!
//! **Request Path:**
//! 1. Pass control to inner middleware layers
//!
//! **Response Path:**
//! 1. Receive result from inner layers
//! 2. **If error is transient**: Sleep with exponential backoff and retry
//! 3. **If error is permanent/terminal**: Pass through immediately
//! 4. **If max retries exceeded**: Pass through final error
//!
//! # Retry Logic
//!
//! - **Initial delay**: Starts with configured base delay
//! - **Exponential growth**: Each retry doubles the delay (with jitter)
//! - **Maximum delay**: Capped at configured maximum
//! - **Jitter**: Adds randomness to prevent thundering herd
//!
//! # Cancellation Handling
//!
//! The retry middleware distinguishes between two types of cancellation
//! signals:
//!
//! - **Shutdown** (partition revoked, consumer stopping): Aborts immediately.
//!   The partition must be released promptly to allow rebalancing.
//!
//! - **Message cancellation**: Treated as a transient condition. The retry loop
//!   continues, skipping any remaining sleep delay. This ensures that
//!   cancellation doesn't cause message loss when retries could still succeed.
//!
//! # Apply-Hook Contract
//!
//! [`FallibleHandler`] owns the per-invocation apply-hook contract; each
//! retry attempt is one invocation of the inner. `RetryHandler::run` owns
//! the per-attempt responsibility split: it fires `inner.after_abort`
//! between attempts and leaves the final attempt's hook to the outer layer.
//!
//! Layers above retry (failure-topic, defer, DLQ middlewares) still see
//! retry as a single dispatch — one final `Result` and one corresponding
//! apply-hook call. The intermediate per-attempt hooks fire only on retry's
//! own inner handler, never on layers above retry.
//!
//! # Usage
//!
//! Retry belongs **outermost** in the stack, and is often used more than once:
//! an inner retry re-drives the handler, and an outer retry around a
//! [`FailureTopicMiddleware`](crate::consumer::middleware::topic::FailureTopicMiddleware)
//! re-drives the dead-letter write itself. See the
//! [module docs](crate::consumer::middleware) for that worked example.
//!
//! [`ErrorCategory::Transient`]: crate::consumer::middleware::ErrorCategory::Transient

use std::time::Duration;

use derive_builder::Builder;
use tokio::select;
use tokio::time::sleep;
use tracing::debug;
use validator::{Validate, ValidationErrors};

use crate::consumer::event_context::EventContext;
use crate::consumer::message::{ConsumerMessage, UncommittedMessage};
use crate::consumer::middleware::{
    FallibleHandler, FallibleHandlerProvider, HandlerMiddleware, Settlement, SettlementHandler,
    abandon, settle,
};
use crate::consumer::{DemandType, EventHandler, HandlerProvider, Keyed};
use crate::timers::{Trigger, UncommittedTimer};
use crate::util::{from_duration_env_with_fallback, from_env_with_fallback};
use crate::{Partition, Topic};

mod engine;

use engine::{Resolution, log_message_failure, log_timer_failure};

// Configuration
// ============================================================================

/// Configuration for retry middleware.
#[derive(Builder, Clone, Debug, Validate)]
pub struct RetryConfiguration {
    /// Base exponential backoff delay.
    ///
    /// Environment variable: `PROSODY_RETRY_BASE`
    /// Default: 20 ms
    #[builder(
        default = "from_duration_env_with_fallback(\"PROSODY_RETRY_BASE\", \
                   Duration::from_millis(20))?",
        setter(into)
    )]
    base: Duration,

    /// Maximum number of retries.
    ///
    /// Environment variable: `PROSODY_MAX_RETRIES`
    /// Default: 3
    ///
    /// When composed with other retry strategies, this represents the maximum
    /// number of retries before falling back to the next middleware.
    #[builder(
        default = "from_env_with_fallback(\"PROSODY_MAX_RETRIES\", 3)?",
        setter(into)
    )]
    max_retries: u32,

    /// Maximum retry delay.
    ///
    /// Environment variable: `PROSODY_RETRY_MAX_DELAY`
    /// Default: 5 minutes
    #[builder(
        default = "from_duration_env_with_fallback(\"PROSODY_RETRY_MAX_DELAY\", \
                   Duration::from_mins(5))?",
        setter(into)
    )]
    max_delay: Duration,
}

impl RetryConfiguration {
    /// Creates a new `RetryConfigurationBuilder`.
    #[must_use]
    pub fn builder() -> RetryConfigurationBuilder {
        RetryConfigurationBuilder::default()
    }
}

/// Middleware that retries failed message processing attempts.
#[derive(Clone, Debug)]
pub struct RetryMiddleware {
    config: RetryConfiguration,
}

impl RetryMiddleware {
    /// Creates a new `RetryMiddleware` with the given configuration.
    ///
    /// # Errors
    ///
    /// Returns `ValidationErrors` if any validation defined in the
    /// `RetryConfiguration` struct fails.
    pub fn new(config: RetryConfiguration) -> Result<Self, ValidationErrors> {
        config.validate()?;
        Ok(Self { config })
    }
}

/// A provider that retries failed message processing attempts.
#[derive(Clone, Debug)]
pub struct RetryProvider<T> {
    provider: T,
    config: RetryConfiguration,
}

/// A handler wrapped with retry functionality.
#[derive(Clone, Debug)]
pub struct RetryHandler<T> {
    base_delay_millis: u64,
    max_delay_millis: u64,
    max_retries: u32,
    handler: T,
}

impl<T> RetryProvider<T> {
    /// Creates a retry handler for the given topic and partition.
    fn create_handler<H>(&self, handler: H) -> RetryHandler<H> {
        RetryHandler {
            base_delay_millis: self.config.base.as_millis() as u64,
            max_delay_millis: self.config.max_delay.as_millis() as u64,
            max_retries: self.config.max_retries,
            handler,
        }
    }
}

// ============================================================================
// Shared retry loop
// ============================================================================

/// Why a `wait_with_cancellation` call returned. Distinguishes shutdown
/// (partition revoked → abort) from message cancellation (treated as a
/// transient condition → keep retrying).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RetryWaitResult {
    Completed,
    Shutdown,
    Cancelled,
}

async fn wait_with_cancellation<C: EventContext>(
    context: &C,
    duration: Duration,
) -> RetryWaitResult {
    select! {
        () = sleep(duration) => RetryWaitResult::Completed,
        () = context.on_shutdown() => RetryWaitResult::Shutdown,
        () = context.on_message_cancelled() => RetryWaitResult::Cancelled,
    }
}

impl<P: Send + Sync + 'static> HandlerMiddleware<P> for RetryMiddleware {
    type Provider<T>
        = RetryProvider<T>
    where
        T: FallibleHandlerProvider,
        T::Handler: FallibleHandler<Payload = P>;

    fn with_provider<T>(&self, provider: T) -> Self::Provider<T>
    where
        T: FallibleHandlerProvider,
        T::Handler: FallibleHandler<Payload = P>,
    {
        RetryProvider {
            provider,
            config: self.config.clone(),
        }
    }
}

impl<T> FallibleHandlerProvider for RetryProvider<T>
where
    T: FallibleHandlerProvider,
{
    type Handler = RetryHandler<T::Handler>;

    fn handler_for_partition(&self, topic: Topic, partition: Partition) -> Self::Handler {
        self.create_handler(self.provider.handler_for_partition(topic, partition))
    }
}

impl<T> HandlerProvider for RetryProvider<T>
where
    T: FallibleHandlerProvider,
    T::Handler: SettlementHandler,
{
    type Handler = RetryHandler<T::Handler>;

    fn handler_for_partition(&self, topic: Topic, partition: Partition) -> Self::Handler {
        self.create_handler(self.provider.handler_for_partition(topic, partition))
    }
}

// ============================================================================
// FallibleHandler impl
// ============================================================================
//
// As an inner middleware, transient errors are capped at `max_retries` so an
// outer DLQ middleware can take over. All error variants collapse to `Err`.
//
// Settle-once: THIS impl is the mid-stack position — it loops attempts and
// RETURNS the final result without ever settling. Only the outermost
// position, consumed through the `EventHandler` impl below, routes
// `Resolution::Commit`/`Abort` to `settle`/`abandon`. So in a DLQ sandwich
// (retry → topic → retry → handler) the inner retry's exhaustion surfaces as
// an `Err` into the topic layer, and settlement happens exactly once, at the
// outermost boundary, over the fully-wrapped result — no marker can record
// before the topic layer has spoken.
//
// Apply-hook contract: a single retry session is ONE dispatch from the outer
// middleware's view. The outer (or the blanket impl) inspects the final
// `Result<O, E>` returned here and fires exactly one of `after_commit` /
// `after_abort` on this `RetryHandler` (the FINAL attempt's hook), which we
// forward verbatim to our inner handler. The intermediate (non-final)
// attempts inside the retry loop have already been resolved on the inner by
// the loop itself, which fires `inner.after_abort(Err(error))` on the inner
// between attempts; those firings are invisible to the outer layer.

impl<T> FallibleHandler for RetryHandler<T>
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
        let topic = message.topic();
        let partition = message.partition();
        let key = message.key();
        let offset = message.offset();
        let (resolution, _final) = self
            .run(
                context,
                demand_type,
                Some(self.max_retries),
                |ctx, dt| self.handler.on_message(ctx, message.clone(), dt),
                |ctx, error| self.handler.after_abort(ctx, Err(error)),
                |reason| {
                    log_message_failure(
                        topic.as_ref(),
                        partition,
                        key.as_ref(),
                        offset,
                        &reason,
                        "",
                    );
                },
            )
            .await;
        match resolution {
            Resolution::Commit(result) => result,
            Resolution::Abort(error) => Err(error),
        }
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
        let topic = message.topic();
        let partition = message.partition();
        let key = message.key();
        let offset = message.offset();
        let (resolution, _final) = self
            .run(
                context,
                demand_type,
                Some(self.max_retries),
                |ctx, dt| self.handler.on_excise(ctx, message.clone(), dt),
                |ctx, error| self.handler.after_abort(ctx, Err(error)),
                |reason| {
                    log_message_failure(
                        topic.as_ref(),
                        partition,
                        key.as_ref(),
                        offset,
                        &reason,
                        "",
                    );
                },
            )
            .await;
        match resolution {
            Resolution::Commit(result) => result,
            Resolution::Abort(error) => Err(error),
        }
    }

    async fn on_timer<C>(
        &self,
        context: C,
        timer: Trigger,
        demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext<Payload = T::Payload>,
    {
        let (resolution, _final) = self
            .run(
                context,
                demand_type,
                Some(self.max_retries),
                |ctx, dt| self.handler.on_timer(ctx, timer.clone(), dt),
                |ctx, error| self.handler.after_abort(ctx, Err(error)),
                |reason| log_timer_failure(&reason, ""),
            )
            .await;
        match resolution {
            Resolution::Commit(result) => result,
            Resolution::Abort(error) => Err(error),
        }
    }

    /// Forwarded verbatim; per-attempt hooks already fired inside
    /// `RetryHandler::run` (see its apply-hook split).
    async fn after_commit<C>(&self, context: C, result: Result<Self::Output, Self::Error>)
    where
        C: EventContext<Payload = T::Payload>,
    {
        self.handler.after_commit(context, result).await;
    }

    /// Forwarded verbatim; per-attempt hooks already fired inside
    /// `RetryHandler::run` (see its apply-hook split).
    async fn after_abort<C>(&self, context: C, result: Result<Self::Output, Self::Error>)
    where
        C: EventContext<Payload = T::Payload>,
    {
        self.handler.after_abort(context, result).await;
    }

    async fn shutdown(self) {
        debug!("shutting down retry handler");
        self.handler.shutdown().await;
    }
}

impl<T> SettlementHandler for RetryHandler<T>
where
    T: SettlementHandler,
{
    /// Pass-through: retry adds no Output or error variants of its own, so
    /// the final attempt's result classifies exactly as the inner's would.
    fn settlement(result: Result<&Self::Output, &Self::Error>) -> Settlement {
        T::settlement(result)
    }
}

// ============================================================================
// EventHandler impl
// ============================================================================
//
// As the outermost durability layer, transient errors retry forever (no
// fallback exists below). This impl owns the commit-vs-abort *decision* — a
// shutdown abort must redeliver, not commit, which only the `Resolution` from
// `run` distinguishes — but it delegates the durability *sequence* (stage →
// arm → marker record → commit → promote) to the shared `settle` / `abandon`
// functions, the single owner of that sequence (see the `FallibleEventHandler`
// docs). Settle-once, mirrored from the `FallibleHandler` impl above: only
// this outermost position settles; the mid-stack impl loops and returns.
// Per-attempt apply hooks are `run`'s responsibility — see its apply-hook
// split.

mod event;

#[cfg(test)]
mod tests;
