//! Replays the production failure: the retry timer write fails after the
//! handler succeeds.

use super::faults::Fault;
use super::types::{
    ApplicationTimerEvent, ApplicationTimerOutcome, DeferredTimerEvent, DeferredTimerOutcome,
    TimerTraceEvent,
};
use super::{TEST_RUNTIME, TestHarness};
use crate::consumer::Keyed;
use crate::consumer::middleware::defer::timer::store::TimerDeferStore;
use crate::consumer::middleware::tests::test_support::faults::{FaultKind, TimerOp};
use crate::timers::datetime::CompactDateTime;

/// A key holds two queued timers. The retry timer fires, the handler
/// succeeds, and the timer write for the next entry fails.
///
/// A terminal error aborts the first pass, which leaves the head in place,
/// and the redelivery commits. A transient error reaches another attempt
/// inside the same dispatch, so one pass commits.
#[test]
fn timer_write_failure_keeps_the_queue_covered() -> color_eyre::Result<()> {
    TEST_RUNTIME.block_on(async {
        for (kind, expected_passes) in [(FaultKind::Terminal, 2), (FaultKind::Transient, 1)] {
            let harness = TestHarness::for_keys(1)?;
            let first = CompactDateTime::from(1000_u32);
            let second = CompactDateTime::from(2000_u32);
            harness
                .execute_faulted(
                    &TimerTraceEvent::ApplicationTimer(ApplicationTimerEvent {
                        key_idx: 0,
                        time: first,
                        outcome: ApplicationTimerOutcome::Transient { defer: true },
                    }),
                    None,
                )
                .await?;
            harness
                .execute_faulted(
                    &TimerTraceEvent::ApplicationTimer(ApplicationTimerEvent {
                        key_idx: 0,
                        time: second,
                        outcome: ApplicationTimerOutcome::Queued,
                    }),
                    None,
                )
                .await?;

            let passes = harness
                .execute_faulted(
                    &TimerTraceEvent::DeferredTimer(DeferredTimerEvent {
                        key_idx: 0,
                        expected_time: first,
                        outcome: DeferredTimerOutcome::Success,
                    }),
                    Some(Fault::Timer(TimerOp::ClearAndSchedule, kind)),
                )
                .await?;

            assert_eq!(passes.len(), expected_passes, "{passes:?}");
            assert_eq!(passes[0].consumed, Some(kind), "{passes:?}");
            assert!(passes[expected_passes - 1].committed, "{passes:?}");
            if kind == FaultKind::Terminal {
                assert_eq!(
                    passes[0].head,
                    Some(i64::from(i32::from(first))),
                    "{passes:?}"
                );
            }

            let context = &harness.contexts[0];
            assert!(!context.active_deferred_timers().is_empty());
            assert_eq!(
                harness
                    .store
                    .get_next_deferred_timer(context.key())
                    .await?
                    .map(|(trigger, _)| trigger.time),
                Some(second)
            );
            assert_eq!(harness.store.is_deferred(context.key()).await?, Some(0));
        }
        Ok(())
    })
}
