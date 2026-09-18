//! Checks source redelivery after a retry timer write fails.

use super::TEST_RUNTIME;
use super::context::TimerOp;
use super::faults::Fault;
use super::harness::TestHarness;
use super::store::FaultKind;
use super::types::{MessageEvent, MessageOutcome, TimerEvent, TimerOutcome, TraceEvent};
use crate::consumer::middleware::defer::message::store::MessageDeferStore;
use crate::timers::duration::CompactDuration;

/// A transient timer fault represents `TimerSchedulerError::Shutdown`.
/// The retry count write succeeds before the timer write fails.
/// The source aborts. Redelivery repeats the bookkeeping and restores a timer.
#[test]
fn timer_write_failure_after_queue_write_aborts_source() -> color_eyre::Result<()> {
    TEST_RUNTIME.block_on(async {
        let mut harness = TestHarness::new(1)?;
        harness
            .execute_faulted(
                &TraceEvent::Message(MessageEvent {
                    key_idx: 0,
                    offset: 1,
                    outcome: MessageOutcome::Transient {
                        defer: true,
                        max_backoff: CompactDuration::MIN,
                    },
                }),
                None,
            )
            .await?;
        let passes = harness
            .execute_faulted(
                &TraceEvent::Timer(TimerEvent {
                    key_idx: 0,
                    offset: 1,
                    outcome: TimerOutcome::Transient {
                        max_backoff: CompactDuration::MIN,
                    },
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
        let key = harness.key(0);
        assert!(harness.capture().has_active_timer(key));
        assert_eq!(
            harness
                .store()
                .get_next_deferred_message(key)
                .await?
                .map(|(offset, _)| offset),
            Some(1)
        );
        assert_eq!(harness.store().is_deferred(key).await?, Some(2));
        Ok(())
    })
}
