//! Checks source redelivery after a retry timer write fails.

use super::context::TimerOp;
use super::faults::Fault;
use super::types::{
    ApplicationTimerEvent, ApplicationTimerOutcome, DeferredTimerEvent, DeferredTimerOutcome,
    TimerTraceEvent,
};
use super::{TEST_RUNTIME, TestHarness};
use crate::consumer::Keyed;
use crate::consumer::middleware::defer::message::handler::tests::store::FaultKind;
use crate::consumer::middleware::defer::timer::store::TimerDeferStore;
use crate::timers::datetime::CompactDateTime;

/// A transient timer fault represents `TimerSchedulerError::Shutdown`.
/// The retry count write succeeds before the timer write fails.
/// The source aborts. Redelivery repeats the bookkeeping and restores a timer.
#[test]
fn timer_write_failure_after_queue_write_aborts_source() -> color_eyre::Result<()> {
    TEST_RUNTIME.block_on(async {
        let harness = TestHarness::for_keys(1)?;
        let time = CompactDateTime::from(1000_u32);
        harness
            .execute_faulted(
                &TimerTraceEvent::ApplicationTimer(ApplicationTimerEvent {
                    key_idx: 0,
                    time,
                    outcome: ApplicationTimerOutcome::Transient { defer: true },
                }),
                None,
            )
            .await?;
        let passes = harness
            .execute_faulted(
                &TimerTraceEvent::DeferredTimer(DeferredTimerEvent {
                    key_idx: 0,
                    expected_time: time,
                    outcome: DeferredTimerOutcome::Transient,
                }),
                Some(Fault::Timer(
                    TimerOp::ClearAndSchedule,
                    FaultKind::Transient,
                )),
            )
            .await?;
        assert!(passes[0].consumed && !passes[0].committed, "{passes:?}");
        assert_eq!(passes.len(), 2);
        assert!(passes[1].committed && !passes[1].consumed, "{passes:?}");
        let context = &harness.contexts[0];
        assert!(!context.active_deferred_timers().is_empty());
        assert_eq!(
            harness
                .store
                .get_next_deferred_timer(context.key())
                .await?
                .map(|(trigger, _)| trigger.time),
            Some(time)
        );
        assert_eq!(harness.store.is_deferred(context.key()).await?, Some(2));
        Ok(())
    })
}
