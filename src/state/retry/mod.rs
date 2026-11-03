//! Shared retries for classified store steps.

use crate::Key;
use crate::error::{ClassifyError, ErrorCategory, RetryAlert, log_at};
use std::error::Error;
use std::future::Future;
use std::time::Duration;
use tokio::time::sleep;

/// Delay between failed durability steps.
pub(crate) const DURABILITY_RETRY_DELAY: Duration = Duration::from_secs(1);

/// A store step succeeds, rejects its input, or stops at shutdown.
pub(crate) enum StepOutcome<R> {
    Done(R),
    Skip,
    Abandon,
}

/// A durability step, as its log events name it.
#[derive(Clone, Copy)]
pub(crate) struct Step<'a> {
    /// What the step does, as a verb phrase such as `commit the staged
    /// changes`.
    pub(crate) action: &'static str,
    /// The key, for a step that runs outside the event's span.
    pub(crate) key: Option<&'a Key>,
    /// The collection that the step writes, if it writes one.
    pub(crate) collection: Option<&'a str>,
    /// The sentences that say what happens after the store rejects the step.
    pub(crate) on_rejection: &'static str,
}

/// Retries transient and terminal errors until success or shutdown. A
/// permanent rejection skips the step. Only shutdown abandons it. The first
/// failure that does not heal raises `alert`, so the steps of one event share
/// one alert.
pub(crate) async fn retry_step<R, E, Fut>(
    alert: &RetryAlert,
    shutdown: impl Fn() -> bool,
    step: Step<'_>,
    mut operation: impl FnMut() -> Fut,
) -> StepOutcome<R>
where
    Fut: Future<Output = Result<R, E>>,
    E: ClassifyError + Error,
{
    loop {
        if shutdown() {
            return StepOutcome::Abandon;
        }
        let error = match operation().await {
            Ok(value) => return StepOutcome::Done(value),
            Err(error) => error,
        };
        let category = error.classify_error();
        let next = match category {
            ErrorCategory::Transient => "Prosody retries every second.",
            // A Terminal store error, such as a schema mismatch, does not clear
            // by retry.
            ErrorCategory::Terminal => {
                "A retry cannot fix this error. Later events for this key wait while Prosody \
                 retries every second."
            }
            ErrorCategory::Permanent => step.on_rejection,
        };
        log_at!(
            alert.level(category),
            key = step.key.map(display),
            collection = step.collection,
            "Prosody cannot {} for keyed state: {error:#}. {next}",
            step.action,
        );
        if category == ErrorCategory::Permanent {
            return StepOutcome::Skip;
        }
        sleep(DURABILITY_RETRY_DELAY).await;
    }
}

#[cfg(test)]
mod tests;
