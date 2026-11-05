//! Fault traces and source settlement records.

use super::handler::OutcomeHandler;
use super::harness::TestHarness;
use super::store::StoreOp;
use super::types::{MessageEvent, Trace, TraceEvent};
use crate::consumer::DemandType;
use crate::consumer::EventHandler;
use crate::consumer::message::{ConsumerRecord, UncommittedMessage};
use crate::consumer::middleware::defer::message::store::MessageDeferStore;
use crate::consumer::middleware::tests::test_support::RecordingTimer;
pub use crate::consumer::middleware::tests::test_support::faults::Pass;
use crate::consumer::middleware::tests::test_support::faults::{Fault as StoreFault, FaultKind};
use crate::consumer::middleware::tests::test_support::faults::{retry, verify_passes};
use crate::consumer::middleware::{Settlement, SettlementHandler};
use crate::consumer::partition::offsets::OffsetTracker;
use crate::loader::MessageLoader;
use crate::timers::{TimerType, Trigger};
use color_eyre::eyre::WrapErr;
use color_eyre::eyre::{bail, ensure, eyre};
use quickcheck::{Arbitrary, Gen, TestResult};
use quickcheck_macros::quickcheck;
use serde_json::{Value, json};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::runtime::Builder;

/// A fault over this twin's store operations.
pub type Fault = StoreFault<StoreOp>;

/// A base trace with one optional fault per event.
#[derive(Clone, Debug)]
pub struct FaultedTrace {
    pub trace: Trace,
    pub faults: Vec<Option<Fault>>,
}

impl SettlementHandler for OutcomeHandler {
    fn settlement(_: Result<&Self::Output, &Self::Error>) -> Settlement {
        Settlement::Final
    }
}

impl Arbitrary for FaultedTrace {
    fn arbitrary(g: &mut Gen) -> Self {
        let trace = Trace::arbitrary(g);
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

/// Runs one event through the real settle boundary with an optional fault.
/// Redelivers an abandoned source once. Returns one record per dispatch.
/// Returns no record when a timer event finds no timer for its key.
/// A redelivery can advance the queue beyond the generator's model.
pub(super) async fn execute_faulted(
    harness: &TestHarness,
    event: &TraceEvent,
    fault: Option<Fault>,
) -> color_eyre::Result<Vec<Pass>> {
    let index = match event {
        TraceEvent::Message(e) => e.key_idx,
        TraceEvent::Timer(e) => e.key_idx,
    };
    let key = harness.key(index).clone();
    let phase = harness.capture().next_fault.phase().clone();
    phase.reassign();
    let handler = retry(harness.handler.clone())?;
    harness.loader.set_next_failure(None);
    harness.failable_store.next_fault.set(None);
    harness.capture().next_fault.set(None);

    let trigger = match event {
        TraceEvent::Message(e) => {
            harness.loader.store_message(
                harness.topic,
                harness.partition,
                e.offset,
                key.clone(),
                json!({"offset": e.offset, "key_idx": e.key_idx}),
            );
            None
        }
        TraceEvent::Timer(_) => {
            let Some(time) = harness.capture().get_timer_time(&key) else {
                return Ok(Vec::new());
            };
            Some(Trigger::for_testing(
                key.clone(),
                time,
                TimerType::DeferredMessage,
            ))
        }
    };
    let context = harness.context_for_key(&key);
    match fault {
        Some(Fault::Store(op, kind)) => harness.failable_store.next_fault.set(Some((op, kind))),
        Some(Fault::Timer(op, kind)) => harness.capture().next_fault.set(Some((op, kind))),
        // The source already fired. The trace removes the timer outside the
        // handler.
        Some(Fault::LostTimer) => harness.capture().drop_timers(&key),
        Some(Fault::Revoke(calls)) => phase.arm(calls),
        None => {}
    }

    let mut passes = Vec::with_capacity(2);
    for _ in 0_u8..2 {
        phase.fire(trigger.as_ref().map(|source| source.time));
        let mut tracker = None;
        let mut timer_commits = None;
        match event {
            TraceEvent::Message(e) => {
                let (source, offsets) = message_source(harness, e).await?;
                EventHandler::on_message(&handler, context.clone(), source, DemandType::Normal)
                    .await;
                tracker = Some(offsets);
            }
            TraceEvent::Timer(e) => {
                harness.arm_timer(e);
                let trigger = trigger
                    .clone()
                    .ok_or_else(|| eyre!("The timer source is absent"))?;
                let (timer, committed, _) = RecordingTimer::new(trigger);
                EventHandler::on_timer(&handler, context.clone(), timer, DemandType::Normal).await;
                timer_commits = Some(committed);
            }
        }
        let consumed = phase.take_consumed();
        let head = harness.store().get_next_deferred_message(&key).await?;
        let covered_at_settle = harness.store().is_deferred(&key).await?.is_none()
            || harness.capture().has_active_timer(&key);
        let committed = match tracker {
            Some(offsets) => offsets.shutdown().await.is_some(),
            None => timer_commits.is_some_and(|count| count.load(Ordering::SeqCst) != 0),
        };
        // A committed trigger that the dispatch did not re-arm never fires
        // again.
        if committed && let Some(time) = phase.fired() {
            harness.capture().record_clear(&key, time);
        }
        passes.push(Pass {
            consumed,
            committed,
            covered_at_settle,
            head: head.map(|(offset, _)| offset),
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

/// Attaches a real offset guard to the message source.
async fn message_source(
    harness: &TestHarness,
    event: &MessageEvent,
) -> color_eyre::Result<(UncommittedMessage<Value>, OffsetTracker)> {
    harness.arm_message(event);
    let record = harness
        .loader
        .load_message(harness.topic, harness.partition, event.offset)
        .await?;
    let ConsumerRecord::Message(message) = record else {
        return Err(eyre!("The loader returned an excise record"));
    };
    let offsets = OffsetTracker::new(
        harness.topic,
        harness.partition,
        1,
        Duration::from_secs(5),
        Arc::default(),
    );
    let source = message.into_uncommitted(offsets.take(event.offset).await?);
    Ok((source, offsets))
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
        let harness = TestHarness::new(trace.trace.key_count)?;
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
                    TraceEvent::Message(e) => e.key_idx,
                    TraceEvent::Timer(e) => e.key_idx,
                };
                // A permanent timer error exempts the key: the settle boundary
                // commits the rejection, so the queue keeps its head with no
                // timer.
                stranded[index] = passes
                    .iter()
                    .any(|pass| pass.consumed == Some(FaultKind::Permanent));
            }
            for (index, stranded) in stranded.iter().enumerate() {
                let key = harness.key(index);
                let deferred = harness.store().is_deferred(key).await?.is_some();
                ensure!(
                    *stranded || !deferred || harness.capture().has_active_timer(key),
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
