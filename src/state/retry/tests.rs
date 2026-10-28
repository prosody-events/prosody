use super::*;
use crate::consumer::middleware::tests::test_support::TestError;
use crate::test_util::capture_events;
use color_eyre::eyre::{Result, bail};
use std::cell::Cell;
use tracing::Level;

const STEP_FAILED: &str = "durability step failed";

/// Fails a step twice with `category`, then succeeds. Returns the outcome and
/// how many error events the failures logged.
async fn alerts_for(category: ErrorCategory) -> (StepOutcome<()>, usize) {
    let (errors, _guard) = capture_events(Level::ERROR);
    let failures = Cell::new(2_u32);
    let outcome = retry_step(
        || false,
        "test",
        || {
            let failed = failures.get() > 0;
            failures.set(failures.get().saturating_sub(1));
            async move {
                if failed {
                    Err(TestError(category))
                } else {
                    Ok(())
                }
            }
        },
    )
    .await;
    (outcome, errors.count(STEP_FAILED))
}

/// A `Transient` failure clears by retry, so it does not alert.
#[tokio::test(start_paused = true)]
async fn transient_retry_logs_no_error() -> Result<()> {
    let (outcome, alerts) = alerts_for(ErrorCategory::Transient).await;
    let StepOutcome::Done(()) = outcome else {
        bail!("a transient failure must retry until the step succeeds");
    };
    assert_eq!(alerts, 0);
    Ok(())
}

/// A `Terminal` failure does not clear by retry, so the first attempt alerts.
/// Later attempts do not repeat the alert.
#[tokio::test(start_paused = true)]
async fn terminal_retry_alerts_once() -> Result<()> {
    let (outcome, alerts) = alerts_for(ErrorCategory::Terminal).await;
    let StepOutcome::Done(()) = outcome else {
        bail!("a terminal failure must retry until the step succeeds");
    };
    assert_eq!(alerts, 1);
    Ok(())
}

/// A `Permanent` failure skips the step and alerts once.
#[tokio::test(start_paused = true)]
async fn permanent_skip_logs_error() -> Result<()> {
    let (outcome, alerts) = alerts_for(ErrorCategory::Permanent).await;
    let StepOutcome::Skip = outcome else {
        bail!("a permanent failure must skip the step");
    };
    assert_eq!(alerts, 1);
    Ok(())
}
