//! Replays the production failure: the retry timer write fails after the
//! handler succeeds.

use super::TEST_RUNTIME;
use super::faults::Fault;
use super::harness::TestHarness;
use super::types::{MessageEvent, MessageOutcome, TimerEvent, TimerOutcome, TraceEvent};
use crate::consumer::middleware::defer::message::store::MessageDeferStore;
use crate::consumer::middleware::tests::test_support::faults::{FaultKind, TimerOp};
use crate::timers::duration::CompactDuration;

/// A key holds two queued messages. The retry timer fires, the handler
/// succeeds, and the timer write for the next message fails.
///
/// A terminal error aborts the first pass, which leaves the head in place,
/// and the redelivery commits. A transient error reaches another attempt
/// inside the same dispatch, so one pass commits.
#[test]
fn timer_write_failure_keeps_the_queue_covered() -> color_eyre::Result<()> {
    TEST_RUNTIME.block_on(async {
        for (kind, expected_passes) in [(FaultKind::Terminal, 2), (FaultKind::Transient, 1)] {
            let harness = TestHarness::new(1)?;
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
            harness
                .execute_faulted(
                    &TraceEvent::Message(MessageEvent {
                        key_idx: 0,
                        offset: 2,
                        outcome: MessageOutcome::Queued,
                    }),
                    None,
                )
                .await?;

            let passes = harness
                .execute_faulted(
                    &TraceEvent::Timer(TimerEvent {
                        key_idx: 0,
                        offset: 1,
                        outcome: TimerOutcome::Success,
                    }),
                    Some(Fault::Timer(TimerOp::ClearAndSchedule, kind)),
                )
                .await?;

            assert_eq!(passes.len(), expected_passes, "{passes:?}");
            assert_eq!(passes[0].consumed, Some(kind), "{passes:?}");
            assert!(passes[expected_passes - 1].committed, "{passes:?}");
            if kind == FaultKind::Terminal {
                assert_eq!(passes[0].head, Some(1), "{passes:?}");
            }

            let key = harness.key(0);
            assert!(harness.capture().has_active_timer(key));
            assert_eq!(
                harness
                    .store()
                    .get_next_deferred_message(key)
                    .await?
                    .map(|(offset, _)| offset),
                Some(2)
            );
            assert_eq!(harness.store().is_deferred(key).await?, Some(0));
        }
        Ok(())
    })
}
