//! Shared retries for classified store steps.

use crate::error::{ClassifyError, ErrorCategory};
use std::error::Error;
use std::future::Future;
use std::time::Duration;
use tokio::time::sleep;
use tracing::error;

/// Delay between failed durability steps.
pub(crate) const DURABILITY_RETRY_DELAY: Duration = Duration::from_secs(1);

/// A store step succeeds, rejects its input, or stops at shutdown.
pub(crate) enum StepOutcome<R> {
    Done(R),
    Skip,
    Abandon,
}

/// Retries transient and terminal errors until success or shutdown.
/// A permanent rejection skips the step. Only shutdown abandons it.
pub(crate) async fn retry_step<R, E, Fut>(
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
        match step().await {
            Ok(value) => return StepOutcome::Done(value),
            Err(error) => {
                let permanent = error.classify_error() == ErrorCategory::Permanent;
                let action = if permanent { "skip" } else { "retry" };
                error!(label, %error, action, "durability step failed");
                if permanent {
                    return StepOutcome::Skip;
                }
                sleep(DURABILITY_RETRY_DELAY).await;
            }
        }
    }
}
