use std::cmp::min;
use std::fmt::Display;
use std::future::Future;
use std::time::Duration;

use humantime::format_duration;
use rand::RngExt;
use tracing::{error, info};

use super::{RetryHandler, RetryWaitResult, wait_with_cancellation};
use crate::consumer::DemandType;
use crate::consumer::event_context::EventContext;
use crate::consumer::middleware::{ClassifyError, ErrorCategory, NextAttempt};
use crate::{Offset, Partition};

/// per-attempt apply-hook split).
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

/// Reason a retry attempt is being logged. Each variant carries the data a
/// call-site closure needs to emit a structured log with the relevant
/// per-event fields (topic / partition / key / offset for messages, none for
/// timers).
pub(super) enum LogReason<'a, E> {
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
        log: impl Fn(LogReason<'_, E>),
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
            // First attempt uses the original demand type; retries surface as Failure.
            let demand = if attempt == 1 {
                demand_type
            } else {
                DemandType::Failure
            };
            let error = match invoke(current.clone(), demand).await {
                Ok(output) => return (Resolution::Commit(Ok(output)), current),
                Err(error) => error,
            };

            // Only abort on shutdown. Message cancellation is treated as transient.
            // Shutdown returns Abort *without* firing apply_abort here — the
            // outer layer will fire the inner's after_abort exactly once for
            // this final attempt.
            if current.is_shutdown() {
                return (Resolution::Abort(error), current);
            }

            match error.classify_error() {
                ErrorCategory::Transient => {
                    if matches!(max_retries, Some(max) if attempt > max) {
                        log(LogReason::MaxRetriesExceeded {
                            attempt,
                            error: &error,
                        });
                        // Final attempt: outer layer fires the apply hook.
                        return (Resolution::Commit(Err(error)), current);
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
                    if wait_with_cancellation(&current, sleep_time).await
                        == RetryWaitResult::Shutdown
                    {
                        return (Resolution::Abort(error), current);
                    }
                    // Attempt boundary, verb-then-hook ordering: clone the stale
                    // view, run `next_attempt` (which discards this attempt's
                    // dirty overlay + bumps the epoch under the gate, then
                    // returns the re-pinned N+1 view), THEN fire the
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
                    log(LogReason::Permanent {
                        attempt,
                        error: &error,
                    });
                    return (Resolution::Commit(Err(error)), current);
                }
                ErrorCategory::Terminal => {
                    log(LogReason::Terminal {
                        attempt,
                        error: &error,
                    });
                    return (Resolution::Abort(error), current);
                }
            }
        }
    }
}

/// Emits a structured log for a message-path retry event. `discard_suffix` is
/// `""` in `FallibleHandler` context (the error propagates upward) and
/// `"; discarding message"` in `EventHandler` context (the marker commits and
/// the message is dropped from this consumer's perspective).
pub(super) fn log_message_failure<E: Display>(
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
pub(super) fn log_timer_failure<E: Display>(reason: &LogReason<'_, E>, discard_suffix: &str) {
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
