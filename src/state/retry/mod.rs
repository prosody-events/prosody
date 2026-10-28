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

/// Retries transient and terminal errors until success or shutdown. The
/// first terminal error alerts. A permanent rejection skips the step. Only
/// shutdown abandons it.
pub(crate) async fn retry_step<R, E, Fut>(
    shutdown: impl Fn() -> bool,
    label: &str,
    mut step: impl FnMut() -> Fut,
) -> StepOutcome<R>
where
    Fut: Future<Output = Result<R, E>>,
    E: ClassifyError + Error,
{
    let mut alert = RetryAlert::default();
    loop {
        if shutdown() {
            return StepOutcome::Abandon;
        }
        match step().await {
            Ok(value) => return StepOutcome::Done(value),
            Err(error) => {
                let category = error.classify_error();
                if category == ErrorCategory::Permanent {
                    error!(label, %error, action = "skip", "durability step failed");
                    return StepOutcome::Skip;
                }
                // A Terminal store error, such as a schema mismatch, does not
                // clear by retry and blocks the key.
                if alert.alerts(category) {
                    error!(label, %error, action = "retry", "durability step failed");
                } else {
                    warn!(label, %error, action = "retry", "durability step failed");
                }
                sleep(DURABILITY_RETRY_DELAY).await;
            }
        }
    }
}

#[cfg(test)]
mod tests;
