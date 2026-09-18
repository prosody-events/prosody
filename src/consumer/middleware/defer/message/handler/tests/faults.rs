//! Fault traces and source settlement records.

use super::context::TimerOp;
use super::handler::OutcomeHandler;
use super::harness::TestHarness;
use super::store::{FaultKind, StoreOp};
use super::types::{Trace, TraceEvent};
use crate::consumer::message::ConsumerRecord;
use crate::consumer::middleware::{FallibleHandler, Settlement, SettlementHandler, settle};
use crate::consumer::{DemandType, Uncommitted};
use crate::loader::MessageLoader;
use crate::timers::{TimerType, Trigger};
use color_eyre::eyre::eyre;
use quickcheck::{Arbitrary, Gen};
use serde_json::json;
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
pub struct FaultedTrace {
    pub trace: Trace,
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
    harness: &mut TestHarness,
    event: &TraceEvent,
    fault: Option<Fault>,
) -> color_eyre::Result<Vec<Pass>> {
    let index = match event {
        TraceEvent::Message(e) => e.key_idx,
        TraceEvent::Timer(e) => e.key_idx,
    };
    let key = harness.key(index).clone();
    harness.loader.set_next_failure(None);
    harness.failable_store.set_fault(None);
    harness.capture().set_fault(None);

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
    // The source has already fired when the external timer loss occurs.
    match fault {
        Some(Fault::Store(op, kind)) => harness.failable_store.set_fault(Some((op, kind))),
        Some(Fault::Timer(op, kind)) => harness.capture().set_fault(Some((op, kind))),
        Some(Fault::LostTimer) => harness.capture().drop_timers(&key),
        None => {}
    }

    let mut passes = Vec::with_capacity(2);
    for _ in 0_u8..2 {
        while harness.capture().pop_event().is_some() {}
        let pending = harness.failable_store.fault_pending() || harness.capture().fault_pending();
        let result = match event {
            TraceEvent::Message(e) => {
                harness.arm_message(e);
                let record = harness
                    .loader
                    .load_message(harness.topic, harness.partition, e.offset)
                    .await?;
                let ConsumerRecord::Message(message) = record else {
                    return Err(eyre!("The loader returned an excise record"));
                };
                harness
                    .handler
                    .on_message(context.clone(), message, DemandType::Normal)
                    .await
            }
            TraceEvent::Timer(e) => {
                harness.arm_timer(e);
                let trigger = trigger
                    .clone()
                    .ok_or_else(|| eyre!("The timer source is absent"))?;
                harness
                    .handler
                    .on_timer(context.clone(), trigger, DemandType::Normal)
                    .await
            }
        };
        let consumed = pending
            && !harness.failable_store.fault_pending()
            && !harness.capture().fault_pending();
        let guard = SourceGuard::default();
        settle(&harness.handler, context.clone(), guard.clone(), result).await;
        let committed = guard.committed.load(Ordering::SeqCst);
        if committed && let Some(trigger) = &trigger {
            harness.capture().retire_fired(&key, trigger.time);
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
