//! Deterministic cases for durable changes before a timer fault.

use super::TEST_RUNTIME;
use super::context::TimerOp;
use super::harness::TestHarness;
use super::types::{Fault, MessageEvent, MessageOutcome, Step, TimerEvent, TimerOutcome};
use super::{FaultKind, StoreOp};
use crate::Offset;
use crate::consumer::DemandType;
use crate::consumer::middleware::defer::message::store::MessageDeferStore;

/// Redelivery observes queue writes that preceded the timer failure.
#[test]
fn redelivery_preserves_durable_changes() -> color_eyre::Result<()> {
    TEST_RUNTIME.block_on(async {
        for (outcome, expected) in [
            (TimerOutcome::Success, Some((Offset::from(3_i64), 0))),
            (TimerOutcome::Transient, Some((Offset::from(1_i64), 2))),
        ] {
            let mut harness = TestHarness::new(1)?;
            for offset in 1_i64..=3 {
                harness
                    .execute_message(&MessageEvent {
                        key_idx: 0,
                        offset: Offset::from(offset),
                        fault: None,
                        outcome: if offset == 1 {
                            MessageOutcome::Transient { defer: true }
                        } else {
                            MessageOutcome::Queued
                        },
                    })
                    .await?;
            }
            harness
                .execute_timer(
                    &TimerEvent {
                        key_idx: 0,
                        outcome,
                        fault: Some(Fault::Timer(
                            TimerOp::ClearAndSchedule,
                            FaultKind::Permanent,
                        )),
                    },
                    DemandType::Normal,
                )
                .await?;
            assert!(harness.passes[0].consumed);
            assert!(!harness.passes[0].committed);
            assert_eq!(harness.passes[0].before, Some((Offset::from(1_i64), 0)));
            assert_eq!(harness.passes.len(), 2);
            assert_eq!(
                harness
                    .store()
                    .get_next_deferred_message(harness.key(0))
                    .await?,
                expected
            );
            harness.verify_invariants().await?;
        }
        Ok(())
    })
}

/// A fault for an uncalled operation cannot abandon the source.
#[test]
fn uncalled_store_fault_does_not_abort() -> color_eyre::Result<()> {
    TEST_RUNTIME.block_on(async {
        let mut harness = TestHarness::new(1)?;
        harness
            .execute_message(&MessageEvent {
                key_idx: 0,
                offset: Offset::from(1_i64),
                outcome: MessageOutcome::Success,
                fault: Some(Fault::Store(
                    StoreOp::CompleteRetrySuccess,
                    FaultKind::Transient,
                )),
            })
            .await?;
        assert!(!harness.passes[0].consumed);
        assert!(harness.passes[0].committed);
        assert_eq!(harness.passes.len(), 1);
        Ok(())
    })
}

/// A transient loader fault overrides Success without a timer fault
/// consumption.
#[test]
fn loader_fault_overrides_success_without_abandonment() -> color_eyre::Result<()> {
    TEST_RUNTIME.block_on(async {
        let mut harness = TestHarness::new(3)?;
        harness.demand = DemandType::Failure { retry: 5 };
        for step in [
            Step {
                key_idx: 193,
                roll: 122,
                fault: None,
            },
            Step {
                key_idx: 19,
                roll: 0,
                fault: Some(Fault::LoaderThenTimer(
                    FaultKind::Transient,
                    TimerOp::ClearScheduled,
                    FaultKind::Transient,
                )),
            },
        ] {
            harness.execute_step(&step).await?;
        }
        assert_eq!(harness.passes.len(), 1);
        let pass = &harness.passes[0];
        assert_eq!(pass.before, Some((Offset::from(1_i64), 0)));
        assert_eq!(pass.after, Some((Offset::from(1_i64), 1)));
        assert!(pass.calls.is_empty());
        assert!(!pass.consumed);
        assert!(pass.committed);
        assert_eq!(pass.scheduled.len(), 1);
        harness.verify_invariants().await
    })
}
