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
//! # Apply-Hook Contract: Per-Invocation, No Exceptions
//!
//! The [`FallibleHandler`] trait requires, **per invocation**, that for every
//! call to `inner.on_message` / `inner.on_timer` which runs and returns, the
//! framework fires exactly one of `inner.after_commit` (final — no
//! re-dispatch into the inner is coming) or `inner.after_abort` (non-final —
//! the same logical event will be re-dispatched into the inner).
//!
//! Retry middleware re-invokes its inner once per attempt; each attempt is
//! itself an invocation that must be paired with exactly one apply hook on
//! the inner. Splitting responsibility between this middleware and the
//! framework above:
//!
//! - For every **non-final** attempt — i.e. an attempt whose error was
//!   `Transient` and which will be followed by another attempt within this
//!   retry session — this middleware fires `inner.after_abort(Err(error))` on
//!   the inner *between attempts*, before invoking the inner again. The next
//!   iteration's `on_message` / `on_timer` is a fresh dispatch from the inner's
//!   POV.
//! - For the **final** attempt — the one whose outcome the retry session
//!   resolves with (success, `Permanent`, `Terminal`, or `Transient` after
//!   `max_retries`) — this middleware does not fire any apply hook on the
//!   inner. Instead, the outer layer fires it:
//!     * When acting as inner [`FallibleHandler`], the outer middleware (or the
//!       blanket impl) inspects the final `Result<O, E>` and fires the apply
//!       hook on `RetryHandler`, which we forward verbatim to our own inner
//!       handler.
//!     * When acting as the outermost [`EventHandler`] (the durability
//!       boundary), we pair the offset/timer commit/abort with exactly one
//!       apply hook on the inner.
//!
//! In other words: an inner that is invoked N times in a retry session sees
//! N apply-hook firings on itself — the first N-1 are `after_abort(Err(...))`
//! fired by this middleware between attempts, and the last is the framework's
//! final hook for the whole session.
//!
//! Composition note: layers above retry (failure-topic, defer, DLQ
//! middlewares) still see retry as a single dispatch — they observe one
//! final `Result` from retry and one corresponding apply-hook call. The
//! intermediate per-attempt apply hooks fire only on retry's own inner
//! handler, never on layers above retry.
//!
//! # Usage
//!
//! Often used multiple times in a pipeline for different failure points:
//!
//! ```rust,no_run
//! # use prosody::consumer::middleware::*;
//! # use prosody::consumer::middleware::retry::*;
//! # use prosody::consumer::middleware::scheduler::*;
//! # use prosody::consumer::middleware::cancellation::CancellationMiddleware;
//! # use prosody::consumer::middleware::topic::*;
//! # use prosody::consumer::DemandType;
//! # use prosody::consumer::event_context::EventContext;
//! # use prosody::consumer::message::ConsumerMessage;
//! # use prosody::producer::{ProducerConfiguration, ProsodyProducer};
//! # use prosody::telemetry::Telemetry;
//! # use prosody::timers::Trigger;
//! # use std::convert::Infallible;
//! # #[derive(Clone)]
//! # struct MyHandler;
//! # impl FallibleHandler for MyHandler {
//! #     type Payload = serde_json::Value;
//! #     type Error = Infallible;
//! #     type Output = ();
//! #     async fn on_message<C>(&self, _: C, _: ConsumerMessage<serde_json::Value>, _: DemandType) -> Result<(), Self::Error> { Ok(()) }
//! #     async fn on_timer<C>(&self, _: C, _: Trigger, _: DemandType) -> Result<(), Self::Error> { Ok(()) }
//! #     async fn shutdown(self) {}
//! # }
//! # let config = SchedulerConfigurationBuilder::default().build().unwrap();
//! # let retry_config = RetryConfiguration::builder().build().unwrap();
//! # let topic_config = FailureTopicConfiguration::builder().build().unwrap();
//! # let producer_config = ProducerConfiguration::builder().bootstrap_servers(vec!["kafka:9092".to_string()]).build().unwrap();
//! # let producer: ProsodyProducer = ProsodyProducer::new(&producer_config, Telemetry::new().sender()).unwrap();
//! # let telemetry = Telemetry::default();
//! # let handler = MyHandler;
//!
//! let provider = SchedulerMiddleware::new(&config, &telemetry).unwrap()
//!     .layer(CancellationMiddleware)
//!     .layer(RetryMiddleware::new(retry_config.clone()).unwrap()) // Retry handler failures
//!     .layer(FailureTopicMiddleware::new(topic_config, "consumer-group".to_string(), producer).unwrap())
//!     .layer(RetryMiddleware::new(retry_config).unwrap()) // Retry DLQ writes
//!     .into_provider(handler);
//! ```
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
use crate::state::session::LifecycleAccess;
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
    ///
    /// # Returns
    ///
    /// A `RetryConfigurationBuilder` instance.
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
    /// # Arguments
    ///
    /// * `config` - The configuration for the retry middleware.
    ///
    /// # Returns
    ///
    /// A `Result` containing the new `RetryMiddleware` if the configuration is
    /// valid, or `ValidationErrors` if the configuration is invalid.
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
/// session. Intermediate (non-final) attempts are not represented here — the
/// loop has already paired each of them with `inner.after_abort(Err(error))`
/// before invoking the inner again. This enum describes only what the outer
/// layer should do with the marker and the apply hook for the *last* attempt.
///
/// - [`Resolution::Commit`] — the final attempt completed and is **final from
///   the inner's POV**: success (`Ok`), `Permanent`, or `Transient` after
///   `max_retries` was exhausted. No further dispatch into the inner is coming
///   for this logical event from retry's standpoint; the outer should commit
///   the marker and fire `after_commit` on the inner with this `Result<O, E>`.
///
/// - [`Resolution::Abort`] — the final attempt was cut short and a **retry of
///   this dispatch is coming via redelivery** (shutdown signalled mid-loop, or
///   a `Terminal` error). The durability marker must NOT advance, and the inner
///   should see `after_abort(Err(error))` so it knows the same logical event
///   will be re-delivered.
///
/// The mapping is performed at the call site:
///
/// - In the [`FallibleHandler`] impl, both variants collapse to `Result<O, E>`
///   and the outer middleware (or blanket impl) decides which apply hook to
///   fire. `Commit` flattens to its inner `Result`; `Abort` becomes `Err` (the
///   outer treats this as abort because the underlying error is `Terminal` or
///   shutdown-driven).
/// - In the [`EventHandler`] impl (the durability boundary), `Commit` triggers
///   `commit() + after_commit(result)` and `Abort` triggers `abort() +
///   after_abort(Err(error))`.
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

        let jitter = rand::rng().random_range(0..exp_backoff);
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
                    // into the next attempt's transaction.
                    if let Ok(lifecycle) = context.state(LifecycleAccess) {
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

    /// Forward verbatim. The outer middleware fires exactly one apply hook
    /// per retry-session dispatch on `RetryHandler`, which we pass through
    /// to the inner. This pairs with the final attempt's invocation of the
    /// inner. Apply hooks for any intermediate (non-final) attempts have
    /// already fired on the inner from inside `run`'s loop.
    async fn after_commit<C>(&self, context: C, result: Result<Self::Output, Self::Error>)
    where
        C: EventContext<Payload = T::Payload>,
    {
        self.handler.after_commit(context, result).await;
    }

    /// Forward verbatim. The outer middleware fires exactly one apply hook
    /// per retry-session dispatch on `RetryHandler`, which we pass through
    /// to the inner. This pairs with the final attempt's invocation of the
    /// inner. Apply hooks for any intermediate (non-final) attempts have
    /// already fired on the inner from inside `run`'s loop.
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
// `run` distinguishes — but it delegates the durability *sequence* (seal →
// marker flush → commit → resolve) to the shared `settle` / `abandon`
// functions, the single owner of that sequence (see the `FallibleEventHandler`
// docs). Intermediate (non-final) attempts have already been resolved on the
// inner by `run`'s loop, which fires `inner.after_abort(Err(error))` between
// attempts to satisfy the per-invocation apply-hook invariant on the inner.

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
