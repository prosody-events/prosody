//! Shared retries for classified store steps.

use crate::error::{ClassifyError, ErrorCategory, RetryAlert};
use std::error::Error;
use std::future::Future;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{error, warn};

/// Delay between failed durability steps.
pub(crate) const DURABILITY_RETRY_DELAY: Duration = Duration::from_secs(1);

/// A store step succeeds, rejects its input, or stops at shutdown.
pub(crate) enum StepOutcome<R> {
    Done(R),
    Skip,
    Abandon,
}

/// Retries transient and terminal errors until success or shutdown. A
/// permanent rejection skips the step. Only shutdown abandons it. The first
/// failure that does not heal raises `alert`, so a caller that repeats a
/// skipped step shares one alert across the repeats.
pub(crate) async fn retry_step<R, E, Fut>(
    alert: &RetryAlert,
    shutdown: impl Fn() -> bool,
    label: &str,
    mut step: impl FnMut() -> Fut,
) -> StepOutcome<R>
where
    Fut: Future<Output = Result<R, E>>,
    E: ClassifyError + Error,
{
    loop {
        if shutdown() {
            return StepOutcome::Abandon;
        }
        let error = match step().await {
            Ok(value) => return StepOutcome::Done(value),
            Err(error) => error,
        };
        // A Terminal store error, such as a schema mismatch, does not clear by
        // retry and blocks the key.
        let category = error.classify_error();
        let skip = category == ErrorCategory::Permanent;
        let action = if skip { "skip" } else { "retry" };
        if alert.alerts(category) {
            error!(label, %error, action, "durability step failed");
        } else {
            warn!(label, %error, action, "durability step failed");
        }
        if skip {
            return StepOutcome::Skip;
        }
        sleep(DURABILITY_RETRY_DELAY).await;
    }
}

#[cfg(test)]
mod tests;
