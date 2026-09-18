//! Fault traces and source settlement records.

use super::TimerOperation;
use super::capture::KeyedMockContext;
use super::store::StoreOp;
use super::types::{ApplicationTimerOutcome, DeferredTimerOutcome, TimerTrace, TimerTraceEvent};
use super::{HandlerOutcome, OutcomeHandler, TestHarness};
use crate::consumer::EventHandler;
use crate::consumer::middleware::defer::timer::store::TimerDeferStore;
use crate::consumer::middleware::tests::test_support::RecordingTimer;
pub use crate::consumer::middleware::tests::test_support::faults::Pass;
use crate::consumer::middleware::tests::test_support::faults::TimerError;
use crate::consumer::middleware::tests::test_support::faults::{Fault as StoreFault, FaultKind};
use crate::consumer::middleware::tests::test_support::faults::{retry, verify_passes};
use crate::consumer::middleware::{Settlement, SettlementHandler};
use crate::consumer::{DemandType, Keyed};
use crate::timers::{TimerType, Trigger};
use color_eyre::eyre::WrapErr;
use color_eyre::eyre::{bail, ensure};
use quickcheck::{Arbitrary, Gen, TestResult};
use quickcheck_macros::quickcheck;
use std::future::{Ready, ready};
use std::sync::atomic::Ordering;
use tokio::runtime::Builder;

/// A fault over this twin's store operations.
pub type Fault = StoreFault<StoreOp>;

/// A base trace with one optional fault per event.
#[derive(Clone, Debug)]
pub struct FaultedTrace {
    pub trace: TimerTrace,
    pub faults: Vec<Option<Fault>>,
}

impl SettlementHandler for OutcomeHandler {
    fn settlement(_: Result<&Self::Output, &Self::Error>) -> Settlement {
        Settlement::Final
    }
}

impl Arbitrary for FaultedTrace {
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
    let phase = context.next_fault.phase().clone();
    phase.reassign();
    let handler = retry(harness.handler.clone())?;
    harness.failable_store.next_fault.set(None);
    context.next_fault.set(None);

    let (time, kind) = match event {
        TimerTraceEvent::ApplicationTimer(e) => (e.time, TimerType::Application),
        TimerTraceEvent::DeferredTimer(_) => {
            let Some(time) = context.active_deferred_timers().first().copied() else {
                return Ok(Vec::new());
            };
            (time, TimerType::DeferredTimer)
        }
    };
    if kind == TimerType::Application {
        context.active_timers.lock().push((time, kind));
    }
    let trigger = Trigger::for_testing(context.key().clone(), time, kind);
    match fault {
        Some(Fault::Store(op, kind)) => harness.failable_store.next_fault.set(Some((op, kind))),
        Some(Fault::Timer(op, kind)) => context.next_fault.set(Some((op, kind))),
        // The source already fired. The trace removes the timer outside the
        // handler.
        Some(Fault::LostTimer) => context
            .active_timers
            .lock()
            .retain(|(_, kind)| *kind != TimerType::DeferredTimer),
        Some(Fault::Revoke(calls)) => phase.arm(calls),
        None => {}
    }

    let mut passes = Vec::with_capacity(2);
    for _ in 0_u8..2 {
        phase.fire((kind == TimerType::DeferredTimer).then_some(time));
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
        let (timer, committed, _) = RecordingTimer::new(trigger.clone());
        EventHandler::on_timer(&handler, context.clone(), timer, DemandType::Normal).await;
        let consumed = phase.take_consumed();
        let head = harness.store.get_next_deferred_timer(context.key()).await?;
        let covered_at_settle = harness.store.is_deferred(context.key()).await?.is_none()
            || !context.active_deferred_timers().is_empty();
        let committed = committed.load(Ordering::SeqCst) != 0;
        // A committed trigger that the dispatch did not re-arm never fires
        // again.
        if committed && let Some(time) = phase.fired() {
            context
                .active_timers
                .lock()
                .retain(|entry| *entry != (time, TimerType::DeferredTimer));
        }
        passes.push(Pass {
            consumed,
            committed,
            covered_at_settle,
            head: head.map(|(trigger, _)| i64::from(i32::from(trigger.time))),
        });
        if committed {
            break;
        }
        if matches!(fault, Some(Fault::Revoke(_))) {
            phase.reassign();
        }
    }

    if let Some(op) = harness.failable_store.next_fault.take_uncovered() {
        bail!("{op:?} left a non-empty queue without a retry timer that fires again");
    }
    Ok(passes)
}

/// Every settlement preserves coverage. A committed source leaves the key's
/// queue empty or its timer live, unless the consumed fault was permanent.
/// Every queue write leaves a retry timer that fires again. A consumed
/// permanent fault commits. Runs through the real retry handler and the real
/// settle boundary, under store faults, timer faults, lost timers, and
/// revocation after the n-th call.
#[quickcheck]
fn prop_settlement_preserves_coverage(trace: FaultedTrace) -> TestResult {
    let mut skipped = 0;
    let trace_log = format!("{trace:?}");
    let event_count = trace.trace.events.len();
    let runtime = match Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => return TestResult::error(error.to_string()),
    };
    let result: color_eyre::Result<()> = runtime.block_on(async {
        let harness = TestHarness::for_keys(trace.trace.key_count)?;
        let mut stranded = vec![false; trace.trace.key_count];
        for (event, fault) in trace.trace.events.iter().zip(trace.faults) {
            let passes = harness.execute_faulted(event, fault).await?;
            verify_passes(&passes)
                .wrap_err_with(|| format!("Event: {event:?}; fault: {fault:?}"))?;
            if passes.is_empty() {
                // A skipped event leaves the flag unchanged, because nothing
                // ran on the key.
                skipped += 1;
            } else {
                let index = match event {
                    TimerTraceEvent::ApplicationTimer(e) => e.key_idx,
                    TimerTraceEvent::DeferredTimer(e) => e.key_idx,
                };
                // A permanent timer error exempts the key: the settle boundary
                // commits the rejection, so the queue keeps its head with no
                // timer.
                stranded[index] = passes
                    .iter()
                    .any(|pass| pass.consumed == Some(FaultKind::Permanent));
            }
            for (index, stranded) in stranded.iter().enumerate() {
                let deferred = harness
                    .store
                    .is_deferred(harness.contexts[index].key())
                    .await?
                    .is_some();
                ensure!(
                    *stranded
                        || !deferred
                        || !harness.contexts[index].active_deferred_timers().is_empty(),
                    "Key {index} has a queue without a timer; event: {event:?}; fault: {fault:?}; \
                     passes: {passes:?}; skipped: {skipped}"
                );
            }
        }
        Ok(())
    });
    match result {
        Ok(()) if skipped == event_count => TestResult::discard(),
        Ok(()) => TestResult::passed(),
        Err(error) => {
            TestResult::error(format!("{error:?}; skipped: {skipped}; trace: {trace_log}"))
        }
    }
}

/// Records one successful timer operation.
pub(super) fn record_operation(
    context: &KeyedMockContext,
    operation: TimerOperation,
) -> Ready<Result<(), TimerError>> {
    context.inner.operations.lock().push(operation);
    ready(Ok(()))
}
