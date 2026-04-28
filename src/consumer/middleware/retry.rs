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
};
use crate::consumer::{DemandType, EventHandler, HandlerProvider, Keyed, Uncommitted};
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
        C: EventContext,
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
        C: EventContext,
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
        C: EventContext,
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
        C: EventContext,
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
// fallback exists below). This impl IS the durability boundary that ties the
// commit/abort of the offset (or timer) marker to the inner handler's apply
// hook for the FINAL attempt: the `Resolution` returned by `run` is mapped
// to commit + after_commit or abort + after_abort. Intermediate (non-final)
// attempts have already been resolved on the inner by `run`'s loop, which
// fires `inner.after_abort(Err(error))` between attempts to satisfy the
// per-invocation apply-hook invariant on the inner.

impl<T> EventHandler for RetryHandler<T>
where
    T: FallibleHandler,
{
    type Payload = T::Payload;

    async fn on_message<C>(
        &self,
        context: C,
        message: UncommittedMessage<Self::Payload>,
        demand_type: DemandType,
    ) where
        C: EventContext,
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
            Resolution::Commit(result) => {
                uncommitted_offset.commit();
                self.handler.after_commit(context, result).await;
            }
            Resolution::Abort(error) => {
                uncommitted_offset.abort();
                self.handler.after_abort(context, Err(error)).await;
            }
        }
    }

    async fn on_timer<C, U>(&self, context: C, timer: U, demand_type: DemandType)
    where
        C: EventContext,
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
            Resolution::Commit(result) => {
                uncommitted.commit().await;
                self.handler.after_commit(context, result).await;
            }
            Resolution::Abort(error) => {
                uncommitted.abort().await;
                self.handler.after_abort(context, Err(error)).await;
            }
        }
    }

    async fn shutdown(self) {
        debug!("shutting down retry handler");
        self.handler.shutdown().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consumer::message::{ConsumerMessage, ConsumerMessageValue};
    use crate::consumer::middleware::test_support::MockEventContext;
    use crate::timers::TimerType;
    use crate::timers::datetime::CompactDateTime;
    use parking_lot::Mutex;
    use std::error::Error;
    use std::fmt::{Display, Formatter, Result as FmtResult};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::Duration;
    use tokio::sync::Semaphore;
    use tokio::time::{sleep as tokio_sleep, timeout};
    use tracing::Span;

    /// Test error type with configurable classification.
    #[derive(Debug, Clone)]
    struct TestError(ErrorCategory);

    impl Display for TestError {
        fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
            write!(f, "test error ({:?})", self.0)
        }
    }

    impl Error for TestError {}

    impl ClassifyError for TestError {
        fn classify_error(&self) -> ErrorCategory {
            self.0
        }
    }

    /// Records every lifecycle hook firing on the inner handler in order so
    /// tests can assert the per-invocation apply-hook invariant.
    #[derive(Debug, Clone, PartialEq, Eq)]
    enum HookEvent {
        /// `on_message` / `on_timer` was invoked with this demand type.
        Invoke(DemandType),
        /// `after_commit` was fired with this `Result` shape (Ok / Err
        /// category).
        AfterCommit(Result<(), ErrorCategory>),
        /// `after_abort` was fired with this `Result` shape.
        AfterAbort(Result<(), ErrorCategory>),
    }

    /// Mock handler that tracks calls and can be configured to fail.
    #[derive(Clone)]
    struct MockHandler {
        call_count: Arc<AtomicUsize>,
        /// Sequence of results to return on successive calls.
        /// Empty means success.
        failure_sequence: Arc<Mutex<Vec<ErrorCategory>>>,
        /// Recorded demand types from calls.
        demand_types: Arc<Mutex<Vec<DemandType>>>,
        /// Ordered log of every lifecycle hook firing (invoke + apply hooks).
        hook_log: Arc<Mutex<Vec<HookEvent>>>,
    }

    impl MockHandler {
        fn success() -> Self {
            Self {
                call_count: Arc::new(AtomicUsize::new(0)),
                failure_sequence: Arc::new(Mutex::new(vec![])),
                demand_types: Arc::new(Mutex::new(vec![])),
                hook_log: Arc::new(Mutex::new(vec![])),
            }
        }

        fn failing_then_success(failures: Vec<ErrorCategory>) -> Self {
            Self {
                call_count: Arc::new(AtomicUsize::new(0)),
                failure_sequence: Arc::new(Mutex::new(failures)),
                demand_types: Arc::new(Mutex::new(vec![])),
                hook_log: Arc::new(Mutex::new(vec![])),
            }
        }

        fn always_failing(category: ErrorCategory) -> Self {
            // Create a large sequence that should outlast max_retries
            Self {
                call_count: Arc::new(AtomicUsize::new(0)),
                failure_sequence: Arc::new(Mutex::new(vec![category; 100])),
                demand_types: Arc::new(Mutex::new(vec![])),
                hook_log: Arc::new(Mutex::new(vec![])),
            }
        }

        fn call_count(&self) -> usize {
            self.call_count.load(Ordering::Relaxed)
        }

        fn recorded_demand_types(&self) -> Vec<DemandType> {
            self.demand_types.lock().clone()
        }

        fn hook_events(&self) -> Vec<HookEvent> {
            self.hook_log.lock().clone()
        }
    }

    /// Project a `Result<(), TestError>` onto the equality-friendly
    /// `Result<(), ErrorCategory>` carried by `HookEvent`.
    fn project_result(result: Result<(), TestError>) -> Result<(), ErrorCategory> {
        result.map_err(|TestError(category)| category)
    }

    impl FallibleHandler for MockHandler {
        type Error = TestError;
        type Output = ();
        type Payload = serde_json::Value;

        async fn on_message<C>(
            &self,
            _context: C,
            _message: ConsumerMessage<Self::Payload>,
            demand_type: DemandType,
        ) -> Result<Self::Output, Self::Error>
        where
            C: EventContext,
        {
            self.call_count.fetch_add(1, Ordering::Relaxed);
            self.demand_types.lock().push(demand_type);
            self.hook_log.lock().push(HookEvent::Invoke(demand_type));

            let mut seq = self.failure_sequence.lock();
            if seq.is_empty() {
                Ok(())
            } else {
                let category = seq.remove(0);
                Err(TestError(category))
            }
        }

        async fn on_timer<C>(
            &self,
            _context: C,
            _trigger: Trigger,
            demand_type: DemandType,
        ) -> Result<Self::Output, Self::Error>
        where
            C: EventContext,
        {
            self.call_count.fetch_add(1, Ordering::Relaxed);
            self.demand_types.lock().push(demand_type);
            self.hook_log.lock().push(HookEvent::Invoke(demand_type));

            let mut seq = self.failure_sequence.lock();
            if seq.is_empty() {
                Ok(())
            } else {
                let category = seq.remove(0);
                Err(TestError(category))
            }
        }

        async fn after_commit<C>(&self, _context: C, result: Result<Self::Output, Self::Error>)
        where
            C: EventContext,
        {
            self.hook_log
                .lock()
                .push(HookEvent::AfterCommit(project_result(result)));
        }

        async fn after_abort<C>(&self, _context: C, result: Result<Self::Output, Self::Error>)
        where
            C: EventContext,
        {
            self.hook_log
                .lock()
                .push(HookEvent::AfterAbort(project_result(result)));
        }

        async fn shutdown(self) {}
    }

    fn create_test_message() -> Option<ConsumerMessage<serde_json::Value>> {
        let semaphore = Arc::new(Semaphore::new(10));
        let permit = semaphore.try_acquire_owned().ok()?;
        Some(ConsumerMessage::new(
            ConsumerMessageValue::default(),
            Span::current(),
            permit,
        ))
    }

    fn create_test_trigger() -> Trigger {
        Trigger::for_testing(
            "test-key".into(),
            CompactDateTime::from(1000_u32),
            TimerType::default(),
        )
    }

    fn create_retry_handler<T>(handler: T, max_retries: u32) -> RetryHandler<T> {
        RetryHandler {
            base_delay_millis: 1, // Very short for tests
            max_delay_millis: 10,
            max_retries,
            handler,
        }
    }

    // === Success Tests ===

    #[tokio::test]
    async fn success_on_first_attempt_returns_ok_immediately() {
        let handler = MockHandler::success();
        let retry_handler = create_retry_handler(handler.clone(), 3);
        let context = MockEventContext::new();
        let Some(message) = create_test_message() else {
            return;
        };

        let result =
            FallibleHandler::on_message(&retry_handler, context, message, DemandType::Normal).await;

        assert!(result.is_ok());
        assert_eq!(handler.call_count(), 1, "Should only call handler once");
    }

    // === Transient Error Tests ===

    #[tokio::test]
    async fn transient_error_retries_then_succeeds() {
        // Fail twice with transient errors, then succeed
        let handler = MockHandler::failing_then_success(vec![
            ErrorCategory::Transient,
            ErrorCategory::Transient,
        ]);
        let retry_handler = create_retry_handler(handler.clone(), 3);
        let context = MockEventContext::new();
        let Some(message) = create_test_message() else {
            return;
        };

        let result =
            FallibleHandler::on_message(&retry_handler, context, message, DemandType::Normal).await;

        assert!(result.is_ok(), "Should succeed after retries");
        assert_eq!(handler.call_count(), 3, "Should retry twice then succeed");
    }

    #[tokio::test]
    async fn transient_error_fails_after_max_retries() {
        let handler = MockHandler::always_failing(ErrorCategory::Transient);
        let retry_handler = create_retry_handler(handler.clone(), 3);
        let context = MockEventContext::new();
        let Some(message) = create_test_message() else {
            return;
        };

        let result =
            FallibleHandler::on_message(&retry_handler, context, message, DemandType::Normal).await;

        assert!(result.is_err(), "Should fail after max retries");
        // First attempt + 3 retries = 4 total calls
        assert_eq!(
            handler.call_count(),
            4,
            "Should attempt 1 + max_retries times"
        );
    }

    // === Permanent Error Tests ===

    #[tokio::test]
    async fn permanent_error_fails_immediately_no_retry() {
        let handler = MockHandler::always_failing(ErrorCategory::Permanent);
        let retry_handler = create_retry_handler(handler.clone(), 3);
        let context = MockEventContext::new();
        let Some(message) = create_test_message() else {
            return;
        };

        let result =
            FallibleHandler::on_message(&retry_handler, context, message, DemandType::Normal).await;

        assert!(result.is_err());
        assert_eq!(handler.call_count(), 1, "Should not retry permanent errors");
    }

    // === Terminal Error Tests ===

    #[tokio::test]
    async fn terminal_error_fails_immediately_no_retry() {
        let handler = MockHandler::always_failing(ErrorCategory::Terminal);
        let retry_handler = create_retry_handler(handler.clone(), 3);
        let context = MockEventContext::new();
        let Some(message) = create_test_message() else {
            return;
        };

        let result =
            FallibleHandler::on_message(&retry_handler, context, message, DemandType::Normal).await;

        assert!(result.is_err());
        assert_eq!(handler.call_count(), 1, "Should not retry terminal errors");
    }

    // === Demand Type Tests ===

    #[tokio::test]
    async fn first_attempt_uses_original_demand_type_retries_use_failure() {
        // Fail once with transient, then succeed
        let handler = MockHandler::failing_then_success(vec![ErrorCategory::Transient]);
        let retry_handler = create_retry_handler(handler.clone(), 3);
        let context = MockEventContext::new();
        let Some(message) = create_test_message() else {
            return;
        };

        let result =
            FallibleHandler::on_message(&retry_handler, context, message, DemandType::Normal).await;

        assert!(result.is_ok());
        let demand_types = handler.recorded_demand_types();
        assert_eq!(demand_types.len(), 2);
        assert_eq!(
            demand_types[0],
            DemandType::Normal,
            "First attempt should use original"
        );
        assert_eq!(
            demand_types[1],
            DemandType::Failure,
            "Retry should use Failure"
        );
    }

    // === Shutdown Tests ===

    #[tokio::test]
    async fn shutdown_during_retry_sleep_returns_error() {
        let handler = MockHandler::always_failing(ErrorCategory::Transient);
        // Use longer delays to give time for shutdown signal
        let retry_handler = RetryHandler {
            base_delay_millis: 1000, // 1 second base delay
            max_delay_millis: 10000,
            max_retries: 10,
            handler: handler.clone(),
        };
        let context = MockEventContext::new();
        let Some(message) = create_test_message() else {
            return;
        };

        // Spawn the retry operation
        let ctx = context.clone();
        let handle = tokio::spawn(async move {
            FallibleHandler::on_message(&retry_handler, ctx, message, DemandType::Normal).await
        });

        // Wait a bit for the first failure and retry sleep to start
        tokio_sleep(Duration::from_millis(50)).await;

        // Signal shutdown
        context.request_shutdown();

        // Should complete quickly due to shutdown
        let Ok(join_result) = timeout(Duration::from_millis(500), handle).await else {
            // Timed out waiting for shutdown - test fails
            return;
        };
        let Ok(result) = join_result else {
            // Task panicked - test fails
            return;
        };

        assert!(result.is_err(), "Should return error on shutdown");
    }

    // === Timer Path Tests ===

    #[tokio::test]
    async fn timer_success_on_first_attempt() {
        let handler = MockHandler::success();
        let retry_handler = create_retry_handler(handler.clone(), 3);
        let context = MockEventContext::new();
        let trigger = create_test_trigger();

        let result =
            FallibleHandler::on_timer(&retry_handler, context, trigger, DemandType::Normal).await;

        assert!(result.is_ok());
        assert_eq!(handler.call_count(), 1);
    }

    #[tokio::test]
    async fn timer_transient_error_retries_then_succeeds() {
        let handler = MockHandler::failing_then_success(vec![ErrorCategory::Transient]);
        let retry_handler = create_retry_handler(handler.clone(), 3);
        let context = MockEventContext::new();
        let trigger = create_test_trigger();

        let result =
            FallibleHandler::on_timer(&retry_handler, context, trigger, DemandType::Normal).await;

        assert!(result.is_ok());
        assert_eq!(handler.call_count(), 2);
    }

    #[tokio::test]
    async fn timer_permanent_error_no_retry() {
        let handler = MockHandler::always_failing(ErrorCategory::Permanent);
        let retry_handler = create_retry_handler(handler.clone(), 3);
        let context = MockEventContext::new();
        let trigger = create_test_trigger();

        let result =
            FallibleHandler::on_timer(&retry_handler, context, trigger, DemandType::Normal).await;

        assert!(result.is_err());
        assert_eq!(handler.call_count(), 1);
    }

    // === Backoff Calculation Tests ===

    #[test]
    fn sleep_time_has_exponential_growth_with_jitter() {
        let handler = MockHandler::success();
        let retry_handler = RetryHandler {
            base_delay_millis: 100,
            max_delay_millis: 10000,
            max_retries: 10,
            handler,
        };

        // Collect multiple samples to verify jitter randomness
        let mut samples_attempt_1: Vec<u64> = Vec::new();
        let mut samples_attempt_3: Vec<u64> = Vec::new();

        for _ in 0_u32..100_u32 {
            samples_attempt_1.push(retry_handler.sleep_time(1).as_millis() as u64);
            samples_attempt_3.push(retry_handler.sleep_time(3).as_millis() as u64);
        }

        // Attempt 1: exp_backoff = 2^1 * 100 = 200ms, jitter in [0, 200)
        let Some(&max_attempt_1) = samples_attempt_1.iter().max() else {
            return;
        };
        assert!(max_attempt_1 < 200, "Attempt 1 jitter should be < 200ms");

        // Attempt 3: exp_backoff = 2^3 * 100 = 800ms, jitter in [0, 800)
        let Some(&max_attempt_3) = samples_attempt_3.iter().max() else {
            return;
        };
        assert!(max_attempt_3 < 800, "Attempt 3 jitter should be < 800ms");

        // Verify there's some variation (jitter is working)
        let Some(&min_attempt_3) = samples_attempt_3.iter().min() else {
            return;
        };
        assert!(
            max_attempt_3 > min_attempt_3 + 50,
            "Jitter should introduce variation"
        );
    }

    #[test]
    fn sleep_time_capped_at_max_delay() {
        let handler = MockHandler::success();
        let retry_handler = RetryHandler {
            base_delay_millis: 100,
            max_delay_millis: 500,
            max_retries: 10,
            handler,
        };

        // Attempt 10: exp_backoff = 2^10 * 100 = 102400ms, but capped at 500ms
        // Jitter should be in [0, 500)
        for _ in 0_u32..100_u32 {
            let sleep = retry_handler.sleep_time(10).as_millis() as u64;
            assert!(sleep < 500, "Sleep time should be capped at max_delay");
        }
    }

    // =========================================================================
    // Shutdown vs Cancellation Tests
    // =========================================================================
    //
    // These tests verify correct behavior for two distinct signals:
    // - **Shutdown**: Partition revoked or consumer stopping → should abort
    // - **Cancellation**: Message-level cancellation → should treat as transient,
    //   retry
    //
    // Test matrix (2×2×2 = 8 tests):
    // - Handler type: FallibleHandler vs EventHandler
    // - Method: on_message vs on_timer
    // - Signal: shutdown vs cancellation

    use crate::consumer::partition::offsets::OffsetTracker;
    use crate::consumer::{Keyed, Uncommitted};
    use crate::timers::UncommittedTimer;
    use color_eyre::eyre::{Result, bail};
    use crossbeam_utils::CachePadded;

    /// Mock commit guard for tracking commit/abort calls.
    struct MockCommitGuard {
        committed: Arc<AtomicBool>,
        aborted: Arc<AtomicBool>,
    }

    impl Uncommitted for MockCommitGuard {
        async fn commit(self) {
            self.committed.store(true, Ordering::Relaxed);
        }

        async fn abort(self) {
            self.aborted.store(true, Ordering::Relaxed);
        }
    }

    /// Mock uncommitted timer for testing `EventHandler::on_timer`.
    struct MockUncommittedTimer {
        trigger: Trigger,
        committed: Arc<AtomicBool>,
        aborted: Arc<AtomicBool>,
    }

    impl MockUncommittedTimer {
        fn new(committed: Arc<AtomicBool>, aborted: Arc<AtomicBool>) -> Self {
            Self {
                trigger: create_test_trigger(),
                committed,
                aborted,
            }
        }
    }

    impl Keyed for MockUncommittedTimer {
        type Key = crate::Key;

        fn key(&self) -> &Self::Key {
            &self.trigger.key
        }
    }

    impl Uncommitted for MockUncommittedTimer {
        async fn commit(self) {
            self.committed.store(true, Ordering::Relaxed);
        }

        async fn abort(self) {
            self.aborted.store(true, Ordering::Relaxed);
        }
    }

    impl UncommittedTimer for MockUncommittedTimer {
        type CommitGuard = MockCommitGuard;

        fn time(&self) -> CompactDateTime {
            self.trigger.time
        }

        fn timer_type(&self) -> TimerType {
            self.trigger.timer_type
        }

        fn span(&self) -> Span {
            Span::none()
        }

        fn into_inner(self) -> (Trigger, Self::CommitGuard) {
            (
                self.trigger,
                MockCommitGuard {
                    committed: self.committed,
                    aborted: self.aborted,
                },
            )
        }
    }

    fn create_offset_tracker() -> OffsetTracker {
        let version = Arc::new(CachePadded::new(AtomicUsize::new(0)));
        OffsetTracker::new("test-topic".into(), 0, 10, Duration::from_mins(5), version)
    }

    // === Shutdown Tests (should pass - abort is correct behavior) ===

    /// `FallibleHandler::on_message` should abort on shutdown signal.
    #[tokio::test]
    async fn fallible_on_message_shutdown_aborts() -> Result<()> {
        let handler = MockHandler::always_failing(ErrorCategory::Transient);
        let retry_handler = create_retry_handler(handler.clone(), 10);
        let context = MockEventContext::new();
        context.request_shutdown();

        let Some(message) = create_test_message() else {
            bail!("failed to create test message");
        };
        let result =
            FallibleHandler::on_message(&retry_handler, context, message, DemandType::Normal).await;

        assert!(result.is_err());
        assert_eq!(handler.call_count(), 1);
        Ok(())
    }

    /// `FallibleHandler::on_timer` should abort on shutdown signal.
    #[tokio::test]
    async fn fallible_on_timer_shutdown_aborts() -> Result<()> {
        let handler = MockHandler::always_failing(ErrorCategory::Transient);
        let retry_handler = create_retry_handler(handler.clone(), 10);
        let context = MockEventContext::new();
        context.request_shutdown();

        let result = FallibleHandler::on_timer(
            &retry_handler,
            context,
            create_test_trigger(),
            DemandType::Normal,
        )
        .await;

        assert!(result.is_err());
        assert_eq!(handler.call_count(), 1);
        Ok(())
    }

    /// `EventHandler::on_message` should abort offset on shutdown signal.
    #[tokio::test]
    async fn event_on_message_shutdown_aborts() -> Result<()> {
        let handler = MockHandler::always_failing(ErrorCategory::Transient);
        let retry_handler = create_retry_handler(handler.clone(), 10);
        let context = MockEventContext::new();
        context.request_shutdown();

        let tracker = create_offset_tracker();
        let uncommitted_offset = tracker.take(0).await?;
        let Some(message) = create_test_message() else {
            bail!("failed to create test message");
        };
        let uncommitted_message = message.into_uncommitted(uncommitted_offset);

        EventHandler::on_message(
            &retry_handler,
            context,
            uncommitted_message,
            DemandType::Normal,
        )
        .await;

        assert_eq!(handler.call_count(), 1);
        assert_eq!(tracker.shutdown().await, None, "offset should be aborted");
        Ok(())
    }

    /// `EventHandler::on_timer` should abort on shutdown signal.
    #[tokio::test]
    async fn event_on_timer_shutdown_aborts() -> Result<()> {
        let handler = MockHandler::always_failing(ErrorCategory::Transient);
        let retry_handler = create_retry_handler(handler.clone(), 10);
        let context = MockEventContext::new();
        context.request_shutdown();

        let committed = Arc::new(AtomicBool::new(false));
        let aborted = Arc::new(AtomicBool::new(false));
        let timer = MockUncommittedTimer::new(Arc::clone(&committed), Arc::clone(&aborted));

        EventHandler::on_timer(&retry_handler, context, timer, DemandType::Normal).await;

        assert_eq!(handler.call_count(), 1);
        assert!(aborted.load(Ordering::Relaxed));
        assert!(!committed.load(Ordering::Relaxed));
        Ok(())
    }

    // === Cancellation Tests (treats message cancellation as transient) ===

    /// `FallibleHandler::on_message` should continue retrying on cancellation.
    #[tokio::test]
    async fn fallible_on_message_cancellation_retries() -> Result<()> {
        let handler = MockHandler::always_failing(ErrorCategory::Transient);
        let retry_handler = create_retry_handler(handler.clone(), 3);
        let context = MockEventContext::new();
        context.request_cancellation();

        let Some(message) = create_test_message() else {
            bail!("failed to create test message");
        };
        let result =
            FallibleHandler::on_message(&retry_handler, context, message, DemandType::Normal).await;

        assert!(result.is_err());
        assert_eq!(handler.call_count(), 4); // 1 initial + 3 retries
        Ok(())
    }

    /// `FallibleHandler::on_timer` should continue retrying on cancellation.
    #[tokio::test]
    async fn fallible_on_timer_cancellation_retries() -> Result<()> {
        let handler = MockHandler::always_failing(ErrorCategory::Transient);
        let retry_handler = create_retry_handler(handler.clone(), 3);
        let context = MockEventContext::new();
        context.request_cancellation();

        let result = FallibleHandler::on_timer(
            &retry_handler,
            context,
            create_test_trigger(),
            DemandType::Normal,
        )
        .await;

        assert!(result.is_err());
        assert_eq!(handler.call_count(), 4); // 1 initial + 3 retries
        Ok(())
    }

    /// `EventHandler::on_message` should continue retrying on cancellation.
    #[tokio::test]
    async fn event_on_message_cancellation_retries() -> Result<()> {
        let handler = MockHandler::failing_then_success(vec![
            ErrorCategory::Transient,
            ErrorCategory::Transient,
        ]);
        let retry_handler = create_retry_handler(handler.clone(), 10);
        let context = MockEventContext::new();
        context.request_cancellation();

        let tracker = create_offset_tracker();
        let uncommitted_offset = tracker.take(0).await?;
        let Some(message) = create_test_message() else {
            bail!("failed to create test message");
        };
        let uncommitted_message = message.into_uncommitted(uncommitted_offset);

        EventHandler::on_message(
            &retry_handler,
            context,
            uncommitted_message,
            DemandType::Normal,
        )
        .await;

        assert_eq!(handler.call_count(), 3); // 2 failures + 1 success
        assert_eq!(
            tracker.shutdown().await,
            Some(0),
            "offset should be committed"
        );
        Ok(())
    }

    /// `EventHandler::on_timer` should continue retrying on cancellation.
    #[tokio::test]
    async fn event_on_timer_cancellation_retries() -> Result<()> {
        let handler = MockHandler::failing_then_success(vec![
            ErrorCategory::Transient,
            ErrorCategory::Transient,
        ]);
        let retry_handler = create_retry_handler(handler.clone(), 10);
        let context = MockEventContext::new();
        context.request_cancellation();

        let committed = Arc::new(AtomicBool::new(false));
        let aborted = Arc::new(AtomicBool::new(false));
        let timer = MockUncommittedTimer::new(Arc::clone(&committed), Arc::clone(&aborted));

        EventHandler::on_timer(&retry_handler, context, timer, DemandType::Normal).await;

        assert_eq!(handler.call_count(), 3); // 2 failures + 1 success
        assert!(committed.load(Ordering::Relaxed));
        assert!(!aborted.load(Ordering::Relaxed));
        Ok(())
    }

    // =========================================================================
    // Per-Invocation Apply-Hook Invariant Tests
    // =========================================================================
    //
    // The `FallibleHandler` apply-hook contract is **per-invocation**: every
    // call to `on_message` / `on_timer` that runs and returns is paired with
    // exactly one apply hook (`after_commit` or `after_abort`) on the same
    // handler instance. The retry middleware preserves this on its inner by
    // firing `inner.after_abort(Err(error))` between attempts; the final
    // attempt's hook is fired by the outer (FallibleHandler blanket impl or
    // EventHandler durability boundary).

    /// Two transient failures followed by a success — the inner sees three
    /// invocations, each paired with exactly one apply hook. The first two
    /// (non-final) attempts fire `after_abort(Err)` from the retry loop;
    /// the third (success, final) fires `after_commit(Ok)` via the outer
    /// blanket-impl boundary.
    #[tokio::test]
    async fn fallible_inner_sees_one_apply_hook_per_attempt_when_retries_then_succeeds()
    -> Result<()> {
        let handler = MockHandler::failing_then_success(vec![
            ErrorCategory::Transient,
            ErrorCategory::Transient,
        ]);
        let retry_handler = create_retry_handler(handler.clone(), 5);
        // Wrap in the FallibleEventHandler blanket impl by going through the
        // EventHandler path with a real durability marker. The blanket impl
        // is what fires the final attempt's apply hook on the outer
        // RetryHandler, which retry forwards to the inner.
        let context = MockEventContext::new();
        let tracker = create_offset_tracker();
        let uncommitted_offset = tracker.take(0).await?;
        let Some(message) = create_test_message() else {
            bail!("failed to create test message");
        };
        let uncommitted_message = message.into_uncommitted(uncommitted_offset);

        EventHandler::on_message(
            &retry_handler,
            context,
            uncommitted_message,
            DemandType::Normal,
        )
        .await;

        let events = handler.hook_events();
        assert_eq!(
            events,
            vec![
                HookEvent::Invoke(DemandType::Normal),
                HookEvent::AfterAbort(Err(ErrorCategory::Transient)),
                HookEvent::Invoke(DemandType::Failure),
                HookEvent::AfterAbort(Err(ErrorCategory::Transient)),
                HookEvent::Invoke(DemandType::Failure),
                HookEvent::AfterCommit(Ok(())),
            ],
            "each invocation must be paired with exactly one apply hook on the inner",
        );
        Ok(())
    }

    /// All transient failures with `max_retries = 2` exhausted: 1 initial +
    /// 2 retries = 3 invocations. The first two (non-final) attempts fire
    /// `after_abort(Err)` from the retry loop. The third (final) attempt's
    /// hook is `after_commit(Err)` because max-retries-exceeded is treated
    /// as commit (DLQ takes over) by the outer.
    ///
    /// We drive `FallibleHandler::on_message` directly here (that path
    /// honours `max_retries`; the `EventHandler` path uses `None` for
    /// retry-forever semantics at the durability boundary) and then
    /// manually invoke the outer apply hook the way an outer
    /// `FallibleEventHandler` blanket impl would for a `Transient`
    /// classification (commit + `after_commit(Err)`).
    #[tokio::test]
    async fn fallible_inner_sees_one_apply_hook_per_attempt_when_max_retries_exhausted()
    -> Result<()> {
        let handler = MockHandler::always_failing(ErrorCategory::Transient);
        let retry_handler = create_retry_handler(handler.clone(), 2);
        let context = MockEventContext::new();
        let Some(message) = create_test_message() else {
            bail!("failed to create test message");
        };

        let result = FallibleHandler::on_message(
            &retry_handler,
            context.clone(),
            message,
            DemandType::Normal,
        )
        .await;

        // Simulate the outer (FallibleEventHandler blanket impl): a
        // Transient error commits the marker and fires `after_commit`.
        assert!(
            matches!(&result, Err(TestError(ErrorCategory::Transient))),
            "max retries should exhaust to a Transient Err",
        );
        FallibleHandler::after_commit(&retry_handler, context, result).await;

        let events = handler.hook_events();
        assert_eq!(
            events,
            vec![
                HookEvent::Invoke(DemandType::Normal),
                HookEvent::AfterAbort(Err(ErrorCategory::Transient)),
                HookEvent::Invoke(DemandType::Failure),
                HookEvent::AfterAbort(Err(ErrorCategory::Transient)),
                HookEvent::Invoke(DemandType::Failure),
                HookEvent::AfterCommit(Err(ErrorCategory::Transient)),
            ],
            "max-retries-exhausted: 3 invocations, each paired with exactly one apply hook; final \
             hook is after_commit because the outer treats this as commit (DLQ takeover)",
        );
        Ok(())
    }

    /// Shutdown during a retry sleep: every attempt that ran and returned is
    /// paired with exactly one apply hook on the inner, with no double-fire
    /// for the abandoned attempt. The retry loop's `Resolution::Abort`
    /// branch on shutdown deliberately skips the per-attempt `apply_abort`
    /// so the outer's `after_abort` (fired here by `EventHandler`) is the
    /// sole apply-hook firing for the final attempt.
    ///
    /// We avoid asserting a fixed event count because the jitter floor on
    /// `sleep_time` is zero: between the first failure and the shutdown
    /// signal a second attempt may slip in. Instead we assert the
    /// invariant directly: events alternate `Invoke` / `Apply` strictly
    /// 1:1, every intermediate apply hook is `AfterAbort(Err(Transient))`,
    /// and the final apply hook is the outer's `AfterAbort` (shutdown
    /// path), never `AfterCommit`.
    #[tokio::test]
    async fn shutdown_during_sleep_does_not_double_fire_apply_hook() -> Result<()> {
        let handler = MockHandler::always_failing(ErrorCategory::Transient);
        // Long sleep so we can race the shutdown signal against it.
        let retry_handler = RetryHandler {
            base_delay_millis: 1000,
            max_delay_millis: 10_000,
            max_retries: 10,
            handler: handler.clone(),
        };
        let context = MockEventContext::new();

        let tracker = create_offset_tracker();
        let uncommitted_offset = tracker.take(0).await?;
        let Some(message) = create_test_message() else {
            bail!("failed to create test message");
        };
        let uncommitted_message = message.into_uncommitted(uncommitted_offset);

        // Spawn the dispatch and signal shutdown shortly after the first
        // attempt fails and the retry-sleep is in flight.
        let ctx = context.clone();
        let handle = tokio::spawn(async move {
            EventHandler::on_message(&retry_handler, ctx, uncommitted_message, DemandType::Normal)
                .await;
        });

        tokio_sleep(Duration::from_millis(50)).await;
        context.request_shutdown();

        // Bound the wait so a regression doesn't hang the suite.
        match timeout(Duration::from_secs(5), handle).await {
            Ok(Ok(())) => {}
            Ok(Err(_)) => bail!("dispatch task panicked"),
            Err(_) => bail!("dispatch did not finish within timeout after shutdown"),
        }

        let events = handler.hook_events();
        assert!(
            !events.is_empty() && events.len().is_multiple_of(2),
            "events must come in invoke+apply pairs; got {events:?}",
        );
        for (i, pair) in events.chunks(2).enumerate() {
            let [invoke, apply] = pair else {
                bail!("uneven event chunk: {pair:?}");
            };
            assert!(
                matches!(invoke, HookEvent::Invoke(_)),
                "pair {i} expected to start with Invoke, got {invoke:?}",
            );
            let is_last = i + 1 == events.len() / 2;
            if is_last {
                // The shutdown-abandoned final attempt must be paired with
                // exactly one `AfterAbort(Err(Transient))` from the outer
                // (NEVER `AfterCommit`, and NEVER duplicated).
                assert_eq!(
                    apply,
                    &HookEvent::AfterAbort(Err(ErrorCategory::Transient)),
                    "final pair must be after_abort fired by the outer (not from the loop), got \
                     {apply:?}",
                );
            } else {
                // Intermediate (non-final) attempts get the loop's
                // between-attempts after_abort.
                assert_eq!(
                    apply,
                    &HookEvent::AfterAbort(Err(ErrorCategory::Transient)),
                    "intermediate pair {i} expected after_abort, got {apply:?}",
                );
            }
        }
        Ok(())
    }
}
