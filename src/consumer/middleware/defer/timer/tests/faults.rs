//! Fault traces and source settlement records.

use super::context::TimerOp;
use super::store::StoreOp;
use super::types::{ApplicationTimerOutcome, DeferredTimerOutcome, TimerTrace, TimerTraceEvent};
use super::{HandlerOutcome, OutcomeHandler, TestHarness};
use crate::consumer::middleware::defer::message::handler::tests::store::FaultKind;
use crate::consumer::middleware::{FallibleHandler, Settlement, SettlementHandler, settle};
use crate::consumer::{DemandType, Keyed, Uncommitted};
use crate::timers::{TimerType, Trigger};
use quickcheck::{Arbitrary, Gen};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// A fault before one dispatch.
#[derive(Clone, Copy, Debug)]
pub enum Fault {
    /// Fails one store call.
    Store(StoreOp, FaultKind),
    /// Fails one timer call.
    Timer(TimerOp, FaultKind),
    /// The timer vanished outside the handler.
    LostTimer,
}

/// A base trace with one optional fault per event.
#[derive(Clone, Debug)]
pub struct FaultedTimerTrace {
    pub trace: TimerTrace,
    pub faults: Vec<Option<Fault>>,
}

/// What one dispatch of a source decided.
#[derive(Debug)]
pub struct Pass {
    /// The dispatch consumed the armed store or timer fault.
    pub consumed: bool,
    /// The boundary committed the source.
    pub committed: bool,
}

#[derive(Clone, Default)]
struct SourceGuard {
    committed: Arc<AtomicBool>,
    aborted: Arc<AtomicBool>,
}

impl Uncommitted for SourceGuard {
    async fn commit(self) {
        self.committed.store(true, Ordering::SeqCst);
    }

    async fn abort(self) {
        self.aborted.store(true, Ordering::SeqCst);
    }
}

impl SettlementHandler for OutcomeHandler {
    fn settlement(_: Result<&Self::Output, &Self::Error>) -> Settlement {
        Settlement::Final
    }
}

impl Arbitrary for Fault {
    fn arbitrary(g: &mut Gen) -> Self {
        let kind = if bool::arbitrary(g) {
            FaultKind::Transient
        } else {
            FaultKind::Permanent
        };
        match u8::arbitrary(g) % 3 {
            0 => {
                let ops = [
                    StoreOp::IsDeferred,
                    StoreOp::DeferFirst,
                    StoreOp::DeferAdditional,
                    StoreOp::CompleteRetrySuccess,
                    StoreOp::IncrementRetryCount,
                    StoreOp::GetNext,
                    StoreOp::DeleteKey,
                    StoreOp::DeferredTimes,
                    StoreOp::Append,
                    StoreOp::Remove,
                    StoreOp::SetRetryCount,
                ];
                Self::Store(ops[usize::arbitrary(g) % ops.len()], kind)
            }
            1 => {
                let ops = [
                    TimerOp::Schedule,
                    TimerOp::ClearAndSchedule,
                    TimerOp::ClearScheduled,
                    TimerOp::Scheduled,
                ];
                Self::Timer(ops[usize::arbitrary(g) % ops.len()], kind)
            }
            _ => Self::LostTimer,
        }
    }
}

impl Arbitrary for FaultedTimerTrace {
    fn arbitrary(g: &mut Gen) -> Self {
        let trace = TimerTrace::arbitrary(g);
        let faults = trace
            .events
            .iter()
            .map(|_| (u8::arbitrary(g) % 4 == 0).then(|| Fault::arbitrary(g)))
            .collect();
        Self { trace, faults }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        let faults = self.faults.clone();
        let prefixes = self.trace.shrink().map(move |trace| Self {
            faults: faults[..trace.events.len()].to_vec(),
            trace,
        });
        let value = self.clone();
        let removals = (0..self.faults.len()).filter_map(move |index| {
            value.faults[index]?;
            let mut shrunk = value.clone();
            shrunk.faults[index] = None;
            Some(shrunk)
        });
        Box::new(prefixes.chain(removals))
    }
}

/// Runs one timer event through the settlement boundary.
/// Redelivers an abandoned source once with the same trigger.
/// Returns no record when the key has no deferred timer.
pub(super) async fn execute_faulted(
    harness: &TestHarness,
    event: &TimerTraceEvent,
    fault: Option<Fault>,
) -> color_eyre::Result<Vec<Pass>> {
    let index = match event {
        TimerTraceEvent::ApplicationTimer(e) => e.key_idx,
        TimerTraceEvent::DeferredTimer(e) => e.key_idx,
    };
    let context = &harness.contexts[index];
    harness.failable_store.set_fault(None);
    context.set_fault(None);

    let (time, kind) = match event {
        TimerTraceEvent::ApplicationTimer(e) => (e.time, TimerType::Application),
        TimerTraceEvent::DeferredTimer(_) => {
            let Some(time) = context.active_deferred_timers().first().copied() else {
                return Ok(Vec::new());
            };
            (time, TimerType::DeferredTimer)
        }
    };
    let trigger = Trigger::for_testing(context.key().clone(), time, kind);
    // The source has already fired when the external timer loss occurs.
    match fault {
        Some(Fault::Store(op, kind)) => harness.failable_store.set_fault(Some((op, kind))),
        Some(Fault::Timer(op, kind)) => context.set_fault(Some((op, kind))),
        Some(Fault::LostTimer) => context.drop_deferred_timers(),
        None => {}
    }

    let mut passes = Vec::with_capacity(2);
    for _ in 0_u8..2 {
        context.clear_operations();
        let outcome = match event {
            TimerTraceEvent::ApplicationTimer(e) => match e.outcome {
                ApplicationTimerOutcome::Success | ApplicationTimerOutcome::Queued => {
                    HandlerOutcome::Success
                }
                ApplicationTimerOutcome::Permanent => HandlerOutcome::Permanent,
                ApplicationTimerOutcome::Transient { defer } => {
                    harness.decider.set_next(defer);
                    HandlerOutcome::Transient
                }
            },
            TimerTraceEvent::DeferredTimer(e) => match e.outcome {
                DeferredTimerOutcome::Success => HandlerOutcome::Success,
                DeferredTimerOutcome::Permanent => HandlerOutcome::Permanent,
                DeferredTimerOutcome::Transient => HandlerOutcome::Transient,
            },
        };
        harness.inner_handler.set_outcome(outcome);
        let pending = harness.failable_store.fault_pending() || context.fault_pending();
        let result = harness
            .handler
            .on_timer(context.clone(), trigger.clone(), DemandType::Normal)
            .await;
        let consumed =
            pending && !harness.failable_store.fault_pending() && !context.fault_pending();
        let guard = SourceGuard::default();
        settle(&harness.handler, context.clone(), guard.clone(), result).await;
        let committed = guard.committed.load(Ordering::SeqCst);
        if committed {
            context.retire_fired(&trigger);
        }
        passes.push(Pass {
            consumed,
            committed,
        });
        if !guard.aborted.load(Ordering::SeqCst) {
            break;
        }
    }
    Ok(passes)
}
