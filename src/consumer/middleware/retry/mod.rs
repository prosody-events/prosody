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

use std::cmp::min;
use std::fmt::Display;
use std::future::Future;
use std::time::Duration;

use derive_builder::Builder;
use humantime::format_duration;
use rand::RngExt;
use tokio::select;
use tokio::time::sleep;
use tracing::{debug, error, info};
use validator::{Validate, ValidationErrors};

use crate::consumer::event_context::EventContext;
use crate::consumer::message::{ConsumerMessage, UncommittedMessage};
use crate::consumer::middleware::{
    ClassifyError, ErrorCategory, FallibleHandler, FallibleHandlerProvider, HandlerMiddleware,
    abandon, settle,
};
use crate::consumer::{DemandType, EventHandler, HandlerProvider, Keyed};
use crate::state::session::LifecycleAccessExt;
use crate::state::session::sealed::StateLifecycle;
use crate::timers::{Trigger, UncommittedTimer};
use crate::util::{from_duration_env_with_fallback, from_env_with_fallback};
use crate::{Offset, Partition, Topic};

// ============================================================================
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

/// How [`RetryHandler::run`] resolved the **final attempt** of a retry
/// session (non-final attempts are not represented here; `run` owns the
/// per-attempt apply-hook split).
///
/// - [`Resolution::Commit`] — the final attempt is final from the inner's POV:
///   success, `Permanent`, or `Transient` after `max_retries`. The outer
///   commits the marker and fires `after_commit` on the inner with this
///   `Result<O, E>`.
/// - [`Resolution::Abort`] — the final attempt was cut short (shutdown
///   mid-loop, or a `Terminal` error) and this dispatch will be redelivered.
///   The marker must NOT advance, and the inner sees `after_abort(Err(error))`.
enum Resolution<O, E> {
    Commit(Result<O, E>),
    Abort(E),
}

/// Reason a retry attempt is being logged. Each variant carries the data a
/// call-site closure needs to emit a structured log with the relevant
/// per-event fields (topic / partition / key / offset for messages, none for
/// timers).
enum LogReason<'a, E> {
    Retrying {
        attempt: u32,
        error: &'a E,
        sleep: Duration,
    },
    MaxRetriesExceeded {
        attempt: u32,
        error: &'a E,
    },
    Permanent {
        attempt: u32,
        error: &'a E,
    },
    Terminal {
        attempt: u32,
        error: &'a E,
    },
}

impl<T> RetryHandler<T> {
    /// Calculates the sleep time for a given retry attempt.
    fn sleep_time(&self, attempt: u32) -> Duration {
        let exp_backoff = min(
            2u64.saturating_pow(attempt)
                .saturating_mul(self.base_delay_millis),
            self.max_delay_millis,
        );

        // `random_range` panics on an empty range: a sub-millisecond base delay
        // (or zero max delay) truncates `exp_backoff` to 0, so clamp the bound.
        let jitter = rand::rng().random_range(0..exp_backoff.max(1));
        Duration::from_millis(jitter)
    }

    /// Drives a single dispatch (message or timer) through the retry loop and
    /// returns a [`Resolution`] describing how the **final attempt** should
    /// be handled by the outer layer.
    ///
    /// `max_retries = None` means retry transient errors forever; used at the
    /// outermost layer where there is no fallback. `max_retries = Some(n)`
    /// caps transient retries at `n`, after which the call resolves to
    /// `Commit(Err)` so an outer DLQ middleware can take over.
    ///
    /// **Apply-hook responsibility split:**
    ///
    /// - For every **non-final** attempt (Transient error followed by a real
    ///   retry within this session), this loop fires `apply_abort(Err(error))`
    ///   on the inner — the inner saw an invocation that returned, and per the
    ///   per-invocation apply-hook contract that attempt is non-final (another
    ///   invocation of the inner is coming), so `after_abort` is the matching
    ///   hook.
    /// - For the **final** attempt (the one whose outcome populates the
    ///   returned `Resolution`), this loop does not fire any apply hook on the
    ///   inner. The outer call site is responsible for that one.
    async fn run<C, E, O, F, Fut, A, AFut>(
        &self,
        context: &C,
        demand_type: DemandType,
        max_retries: Option<u32>,
        mut invoke: F,
        mut apply_abort: A,
        log: impl Fn(LogReason<'_, E>),
    ) -> Resolution<O, E>
    where
        C: EventContext,
        E: ClassifyError,
        F: FnMut(DemandType) -> Fut,
        Fut: Future<Output = Result<O, E>>,
        A: FnMut(E) -> AFut,
        AFut: Future<Output = ()>,
    {
        let mut attempt: u32 = 0;
        loop {
            attempt = attempt.saturating_add(1);
            // First attempt uses the original demand type; retries surface as Failure.
            let demand = if attempt == 1 {
                demand_type
            } else {
                DemandType::Failure
            };
            let error = match invoke(demand).await {
                Ok(output) => return Resolution::Commit(Ok(output)),
                Err(error) => error,
            };

            // Only abort on shutdown. Message cancellation is treated as transient.
            // Shutdown returns Abort *without* firing apply_abort here — the
            // outer layer will fire the inner's after_abort exactly once for
            // this final attempt.
            if context.is_shutdown() {
                return Resolution::Abort(error);
            }

            match error.classify_error() {
                ErrorCategory::Transient => {
                    if matches!(max_retries, Some(max) if attempt > max) {
                        log(LogReason::MaxRetriesExceeded {
                            attempt,
                            error: &error,
                        });
                        // Final attempt: outer layer fires the apply hook.
                        return Resolution::Commit(Err(error));
                    }
                    let sleep_time = self.sleep_time(attempt);
                    log(LogReason::Retrying {
                        attempt,
                        error: &error,
                        sleep: sleep_time,
                    });
                    // Sleep BEFORE firing the per-attempt apply hook. If
                    // shutdown intervenes during the sleep, this attempt
                    // becomes the final attempt of the session and the
                    // outer's after_abort is the only apply-hook firing —
                    // we must not double-fire here.
                    if wait_with_cancellation(context, sleep_time).await
                        == RetryWaitResult::Shutdown
                    {
                        return Resolution::Abort(error);
                    }
                    // We will retry: this attempt was a non-final dispatch
                    // from the inner's POV. Per the per-invocation apply-hook
                    // invariant, fire `inner.after_abort(Err(error))` before
                    // the next invocation of the inner. Whether the sleep
                    // returned `Completed` or `Cancelled` (message
                    // cancellation, treated as transient), we are committing
                    // to another attempt on the next loop iteration.
                    apply_abort(error).await;

                    // Attempt boundary: reset the event's keyed-state
                    // session so the failed attempt's dirty ops never leak
                    // into the next attempt's transaction — and so its
                    // registered dedup marker is discarded rather than
                    // flushed by `settle` after a later attempt succeeds.
                    if let Ok(lifecycle) = context.lifecycle() {
                        lifecycle.reset();
                    }
                }
                ErrorCategory::Permanent => {
                    log(LogReason::Permanent {
                        attempt,
                        error: &error,
                    });
                    return Resolution::Commit(Err(error));
                }
                ErrorCategory::Terminal => {
                    log(LogReason::Terminal {
                        attempt,
                        error: &error,
                    });
                    return Resolution::Abort(error);
                }
            }
        }
    }
}

/// Emits a structured log for a message-path retry event. `discard_suffix` is
/// `""` in `FallibleHandler` context (the error propagates upward) and
/// `"; discarding message"` in `EventHandler` context (the marker commits and
/// the message is dropped from this consumer's perspective).
fn log_message_failure<E: Display>(
    topic: &str,
    partition: Partition,
    key: &str,
    offset: Offset,
    reason: &LogReason<'_, E>,
    discard_suffix: &str,
) {
    match *reason {
        LogReason::Retrying {
            attempt,
            error,
            sleep,
        } => error!(
            partition,
            key,
            offset,
            attempt,
            topic,
            "failed to handle message: {error:#}; retrying after {}",
            format_duration(sleep),
        ),
        LogReason::MaxRetriesExceeded { attempt, error } => error!(
            partition,
            key,
            offset,
            attempt,
            topic,
            "failed to handle message: {error:#}; maximum attempts reached",
        ),
        LogReason::Permanent { attempt, error } => error!(
            partition,
            key,
            offset,
            attempt,
            topic,
            "permanently failed to handle message: {error:#}{discard_suffix}",
        ),
        LogReason::Terminal { attempt, error } => info!(
            partition,
            key,
            offset,
            attempt,
            topic,
            "terminal condition encountered while handling message: {error:#}; aborting",
        ),
    }
}

/// Emits a structured log for a timer-path retry event. See
/// [`log_message_failure`] for the meaning of `discard_suffix`.
fn log_timer_failure<E: Display>(reason: &LogReason<'_, E>, discard_suffix: &str) {
    match *reason {
        LogReason::Retrying { error, sleep, .. } => error!(
            "failed to handle timer: {error:#}; retrying after {}",
            format_duration(sleep),
        ),
        LogReason::MaxRetriesExceeded { error, .. } => {
            error!("failed to handle timer: {error:#}; maximum attempts reached");
        }
        LogReason::Permanent { error, .. } => {
            error!("permanently failed to handle timer: {error:#}{discard_suffix}");
        }
        LogReason::Terminal { error, .. } => {
            info!("terminal condition encountered while handling timer: {error:#}; aborting");
        }
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
        let resolution = self
            .run(
                &context,
                demand_type,
                Some(self.max_retries),
                |dt| {
                    self.handler
                        .on_message(context.clone(), message.clone(), dt)
                },
                |error| self.handler.after_abort(context.clone(), Err(error)),
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
        let resolution = self
            .run(
                &context,
                demand_type,
                Some(self.max_retries),
                |dt| self.handler.on_timer(context.clone(), timer.clone(), dt),
                |error| self.handler.after_abort(context.clone(), Err(error)),
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

// ============================================================================
// EventHandler impl
// ============================================================================
//
// As the outermost durability layer, transient errors retry forever (no
// fallback exists below). This impl owns the commit-vs-abort *decision* — a
// shutdown abort must redeliver, not commit, which only the `Resolution` from
// `run` distinguishes — but it delegates the durability *sequence* (stage →
// arm → marker flush → commit → promote) to the shared `settle` / `abandon`
// functions, the single owner of that sequence (see the `FallibleEventHandler`
// docs). Per-attempt apply hooks are `run`'s responsibility — see its
// apply-hook split.

impl<T> EventHandler for RetryHandler<T>
where
    T: FallibleHandler,
    T::Error: ClassifyError,
{
    type Payload = T::Payload;

    async fn on_message<C>(
        &self,
        context: C,
        message: UncommittedMessage<Self::Payload>,
        demand_type: DemandType,
    ) where
        C: EventContext<Payload = T::Payload>,
    {
        let topic = message.topic();
        let partition = message.partition();
        let key = message.key().to_owned();
        let offset = message.offset();
        let (message, uncommitted_offset) = message.into_inner();

        let resolution = self
            .run(
                &context,
                demand_type,
                None,
                |dt| {
                    self.handler
                        .on_message(context.clone(), message.clone(), dt)
                },
                |error| self.handler.after_abort(context.clone(), Err(error)),
                |reason| {
                    log_message_failure(
                        topic.as_ref(),
                        partition,
                        key.as_ref(),
                        offset,
                        &reason,
                        "; discarding message",
                    );
                },
            )
            .await;

        match resolution {
            Resolution::Commit(result) => settle(self, context, uncommitted_offset, result).await,
            Resolution::Abort(error) => {
                // Terminal abort: nothing staged (the receipt never minted),
                // and abandon touches no state.
                abandon(self, context, uncommitted_offset, Err(error)).await;
            }
        }
    }

    async fn on_timer<C, U>(&self, context: C, timer: U, demand_type: DemandType)
    where
        C: EventContext<Payload = T::Payload>,
        U: UncommittedTimer,
    {
        let (trigger, uncommitted) = timer.into_inner();

        let resolution = self
            .run(
                &context,
                demand_type,
                None,
                |dt| self.handler.on_timer(context.clone(), trigger.clone(), dt),
                |error| self.handler.after_abort(context.clone(), Err(error)),
                |reason| log_timer_failure(&reason, "; discarding timer"),
            )
            .await;

        match resolution {
            Resolution::Commit(result) => settle(self, context, uncommitted, result).await,
            Resolution::Abort(error) => {
                // Terminal abort: nothing staged (the receipt never minted),
                // and abandon touches no state.
                abandon(self, context, uncommitted, Err(error)).await;
            }
        }
    }

    async fn shutdown(self) {
        debug!("shutting down retry handler");
        self.handler.shutdown().await;
    }
}

#[cfg(test)]
mod tests;
