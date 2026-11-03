use super::*;
use crate::consumer::middleware::tests::test_support::TestError;
use crate::test_util::capture_events;
use std::cell::Cell;
use strum::VariantArray;
use tracing::Level;

/// Each step fails twice with one category, then succeeds. The step runs twice
/// with one alert, as a caller that repeats a skipped step does. A failure
/// that heals never alerts. Any other failure alerts once, and only a
/// `Permanent` one skips the step.
#[tokio::test(start_paused = true)]
async fn retry_step_alerts_once_for_a_failure_that_does_not_heal() {
    for &category in ErrorCategory::VARIANTS {
        let (errors, _guard) = capture_events(Level::ERROR);
        let alert = RetryAlert::default();
        for _ in 0..2_u32 {
            let failures = Cell::new(2_u32);
            let outcome = retry_step(
                &alert,
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
            let skipped = matches!(outcome, StepOutcome::Skip);
            assert_eq!(
                skipped,
                category == ErrorCategory::Permanent,
                "{category:?}"
            );
        }
        let alerts = errors.count("durability step failed");
        let expected = usize::from(category != ErrorCategory::Transient);
        assert_eq!(alerts, expected, "{category:?}");
    }
}
