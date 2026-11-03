use std::cmp::min;
use std::fmt::{self, Display, Formatter};
use std::future::Future;
use std::time::Duration;

use humantime::format_duration;
use rand::RngExt;
use tracing::Level;

use super::{RetryHandler, RetryWaitResult, wait_with_cancellation};
use crate::Offset;
use crate::consumer::DemandType;
use crate::consumer::event_context::EventContext;
use crate::consumer::middleware::{ClassifyError, ErrorCategory, NextAttempt};
use crate::error::{RetryAlert, log_at};
use crate::timers::Trigger;

/// How [`RetryHandler::run`] resolved the final retry attempt.
/// The run method owns apply hooks for non-final attempts.
///
/// - [`Resolution::Commit`] — the final attempt is final from the inner's POV:
///   success, `Permanent`, or `Transient` after `max_retries`. The outer routes
///   this through `settle`, which records a marker only for success or
///   `Permanent` (a `Transient` exhaustion commits the offset with no marker)
///   and fires `after_commit` on the inner with this `Result<O, E>`.
/// - [`Resolution::Abort`] — the final attempt was cut short (shutdown
///   mid-loop, or a `Terminal` error) and this dispatch will be redelivered.
///   The marker must NOT advance, and the inner sees `after_abort(Err(error))`.
pub(super) enum Resolution<O, E> {
    Commit(Result<O, E>),
    Abort(E),
}

/// A failed attempt that a retry layer logs.
pub(super) struct Failure<'a, E> {
    attempt: u32,
    error: &'a E,
    next: Next,
}

/// What a retry layer does after a failed attempt.
#[derive(Clone, Copy)]
enum Next {
    /// Retry after this delay.
    Retry(Duration),
    /// Stop, because the attempt was the last one allowed.
    GiveUp,
    /// Stop, because the error is `Permanent`.
    Skip,
    /// Stop, because the error is `Terminal`.
    Abort,
}

/// The kind of event that failed.
#[derive(Clone, Copy)]
enum Event {
    Message,
    Timer,
}

/// Which retry layer logs a failure. It sets who owns the failure's outcome.
#[derive(Clone, Copy)]
pub(super) enum Layer<'a> {
    /// A `FallibleHandler` layer. The error propagates to an outer layer.
    Inner,

    /// The `EventHandler` layer. It discards a failed event. A `Terminal`
    /// abort here holds the partition until a restart, so the partition's
    /// first abort raises this alert.
    Outermost(&'a RetryAlert),
}

impl<E> Failure<'_, E> {
    /// Returns the level for this failure at `layer`.
    fn level(&self, layer: Layer) -> Level {
        match (self.next, layer) {
            (Next::Retry(_), _) => Level::WARN,
            // The outer layer owns the outcome and logs it.
            (_, Layer::Inner) => Level::DEBUG,
            (Next::Abort, Layer::Outermost(aborts)) => aborts.level(ErrorCategory::Terminal),
            (Next::GiveUp | Next::Skip, Layer::Outermost(_)) => Level::ERROR,
        }
    }
}

impl Display for Event {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Message => "message",
            Self::Timer => "timer",
        })
    }
}

impl<E: Display> Failure<'_, E> {
    /// Writes the error, then what Prosody does next.
    fn describe(&self, event: Event, layer: Layer) -> impl Display {
        fmt::from_fn(move |f| {
            let Self {
                attempt,
                error,
                next,
            } = self;
            write!(f, "The handler failed for this {event}: {error:#}. ")?;
            match (next, layer, event) {
                (Next::Retry(delay), ..) => {
                    write!(f, "Prosody retries in {}.", format_duration(*delay))
                }
                (_, Layer::Inner, _) => write!(
                    f,
                    "Prosody stops after {attempt} attempts and returns the error to the outer \
                     middleware."
                ),
                (Next::GiveUp, ..) => {
                    write!(
                        f,
                        "Prosody stops after {attempt} attempts and skips the {event}."
                    )
                }
                (Next::Skip, ..) => write!(
                    f,
                    "The error is permanent, so Prosody skips the {event} and does not retry it."
                ),
                (Next::Abort, _, Event::Message) => f.write_str(
                    "A retry cannot fix this error, so Prosody commits no later offsets for this \
                     partition. Fix the cause, then restart the consumer.",
                ),
                (Next::Abort, _, Event::Timer) => f.write_str(
                    "A retry cannot fix this error, so the timer fires again only after a \
                     restart. Fix the cause, then restart the consumer.",
                ),
            }
        })
    }
}

impl<T> RetryHandler<T> {
    /// Calculates the sleep time for a given retry attempt.
    pub(super) fn sleep_time(&self, attempt: u32) -> Duration {
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
    ///   hook. That hook receives the EXPIRED pre-verb context (the epoch has
    ///   already advanced), so its keyed-state ops error `Terminated` — a
    ///   mid-loop commit of the failed attempt's overlay cannot land.
    /// - For the **final** attempt (the one whose outcome populates the
    ///   returned `Resolution`), this loop does not fire any apply hook on the
    ///   inner. The outer call site is responsible for that one.
    pub(super) async fn run<C, E, O, F, Fut, A, AFut>(
        &self,
        context: C,
        demand_type: DemandType,
        max_retries: Option<u32>,
        mut invoke: F,
        mut apply_abort: A,
        log: impl Fn(&Failure<'_, E>),
    ) -> (Resolution<O, E>, C)
    where
        C: EventContext,
        E: ClassifyError,
        F: FnMut(C, DemandType) -> Fut,
        Fut: Future<Output = Result<O, E>>,
        A: FnMut(C, E) -> AFut,
        AFut: Future<Output = ()>,
    {
        // `run` owns the current dispatch view and drives it across attempt
        // boundaries. The returned view is the FINAL attempt's context (a fresh
        // re-pinned Arc for any retried event), which the outer settles on —
        // never the original the framework may invalidate.
        let mut current = context;
        let mut attempt: u32 = 0;
        loop {
            attempt = attempt.saturating_add(1);
            // Attempt `n` adds `n - 1` retries to the incoming demand.
            let demand = demand_type.retried(attempt - 1);
            let error = match invoke(current.clone(), demand).await {
                Ok(output) => return (Resolution::Commit(Ok(output)), current),
                Err(error) => error,
            };

            // Only abort on shutdown. Message cancellation is treated as
            // transient. Shutdown returns Abort *without* firing apply_abort
            // here — the outer layer will fire the inner's after_abort exactly
            // once for this final attempt.
            if current.is_shutdown() {
                return (Resolution::Abort(error), current);
            }

            match error.classify_error() {
                ErrorCategory::Transient => {
                    if matches!(max_retries, Some(max) if attempt > max) {
                        log(&Failure {
                            attempt,
                            error: &error,
                            next: Next::GiveUp,
                        });
                        // Final attempt: outer layer fires the apply hook.
                        return (Resolution::Commit(Err(error)), current);
                    }
                    let sleep_time = self.sleep_time(attempt);
                    log(&Failure {
                        attempt,
                        error: &error,
                        next: Next::Retry(sleep_time),
                    });
                    // Sleep BEFORE firing the per-attempt apply hook. If
                    // shutdown intervenes during the sleep, this attempt
                    // becomes the final attempt of the session and the
                    // outer's after_abort is the only apply-hook firing —
                    // we must not double-fire here.
                    if wait_with_cancellation(&current, sleep_time).await
                        == RetryWaitResult::Shutdown
                    {
                        return (Resolution::Abort(error), current);
                    }
                    // Attempt boundary, verb-then-hook ordering: clone the
                    // stale view, run `next_attempt` (which discards this
                    // attempt's dirty overlay + bumps the epoch under the gate,
                    // then returns the re-pinned N+1 view), THEN fire the
                    // intermediate `after_abort` with the now-EXPIRED clone.
                    // The expired clone is pinned at attempt N while the epoch
                    // is now N+1, so it is state-dead by construction: a
                    // mid-loop `commit()` of the failed attempt's overlay from
                    // that hook errors `Terminated` rather than landing. Per
                    // the per-invocation apply-hook contract this attempt was
                    // non-final, so `after_abort` is the matching hook (message
                    // cancellation is treated as transient — we retry either
                    // way).
                    let expired = current.clone();
                    current = current.next_attempt().await;
                    apply_abort(expired, error).await;
                }
                ErrorCategory::Permanent => {
                    log(&Failure {
                        attempt,
                        error: &error,
                        next: Next::Skip,
                    });
                    return (Resolution::Commit(Err(error)), current);
                }
                ErrorCategory::Terminal => {
                    log(&Failure {
                        attempt,
                        error: &error,
                        next: Next::Abort,
                    });
                    return (Resolution::Abort(error), current);
                }
            }
        }
    }

    /// Logs a failed message attempt.
    pub(super) fn log_message<E: Display>(
        &self,
        key: &str,
        offset: Offset,
        failure: &Failure<'_, E>,
        layer: Layer,
    ) {
        log_at!(
            failure.level(layer),
            topic = %self.topic,
            partition = self.partition,
            key,
            offset,
            attempt = failure.attempt,
            "{}",
            failure.describe(Event::Message, layer)
        );
    }

    /// Logs a failed timer attempt.
    pub(super) fn log_timer<E: Display>(
        &self,
        trigger: &Trigger,
        failure: &Failure<'_, E>,
        layer: Layer,
    ) {
        log_at!(
            failure.level(layer),
            topic = %self.topic,
            partition = self.partition,
            key = %trigger.key,
            fire_time = %trigger.time,
            timer_type = ?trigger.timer_type,
            attempt = failure.attempt,
            "{}",
            failure.describe(Event::Timer, layer)
        );
    }
}
