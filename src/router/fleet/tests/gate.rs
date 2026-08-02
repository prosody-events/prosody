//! The admission gate: what it admits, and when its drain finishes.

use crate::router::fleet::gate::AdmissionGate;
use color_eyre::Result;
use futures::poll;
use quickcheck::{Arbitrary, Gen, TestResult, empty_shrinker};
use quickcheck_macros::quickcheck;
use std::iter::once;
use std::pin::pin;
use tokio::runtime::Builder;

/// One step of a generated gate trace.
#[derive(Clone, Copy, Debug)]
enum GateOp {
    /// Try to enter, and keep the ticket if the gate admits.
    Enter,
    /// Leave, dropping the ticket at this position among those still held.
    Leave(usize),
    /// Close admission.
    Close,
}

impl Arbitrary for GateOp {
    fn arbitrary(g: &mut Gen) -> Self {
        // Entering is the common step, so a trace holds several tickets at once
        // and the drain has something to wait for. Closing stays rare, because
        // every step after the first close is a refusal.
        match u8::arbitrary(g) % 8 {
            0 => Self::Close,
            1..=3 => Self::Leave(usize::arbitrary(g)),
            _ => Self::Enter,
        }
    }

    /// Entering is the simplest step: it needs no ticket already held and it
    /// closes nothing. Every other step therefore reduces toward it, and a
    /// `Leave` also reduces its position.
    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        match *self {
            Self::Enter => empty_shrinker(),
            Self::Close => Box::new(once(Self::Enter)),
            Self::Leave(at) => Box::new(once(Self::Enter).chain(at.shrink().map(Self::Leave))),
        }
    }
}

/// The gate admits exactly while it is open, counts exactly what it holds, and
/// its drain finishes exactly when the last ticket leaves.
///
/// A plain model — one count and one flag — is the oracle for the first two.
/// The third is asserted by polling the drain by hand: it must stay pending
/// while any ticket lives, and become ready the moment the last one goes.
#[quickcheck]
fn prop_the_gate_admits_until_it_closes_and_drains_to_zero(trace: Vec<GateOp>) -> TestResult {
    let runtime = match Builder::new_current_thread().enable_time().build() {
        Ok(runtime) => runtime,
        Err(error) => return TestResult::error(format!("{error:?}")),
    };
    runtime.block_on(async {
        let gate = AdmissionGate::new();
        let mut held = Vec::new();
        let mut closed = false;

        for op in trace {
            match op {
                GateOp::Enter => {
                    let ticket = gate.enter();
                    assert_eq!(
                        ticket.is_some(),
                        !closed,
                        "the gate must admit exactly while it is open"
                    );
                    held.extend(ticket);
                }
                GateOp::Leave(at) => {
                    if !held.is_empty() {
                        drop(held.remove(at % held.len()));
                    }
                }
                GateOp::Close => {
                    // One poll is what closes the gate, and whether that poll
                    // finishes is exactly whether anything is still inside.
                    assert_eq!(
                        poll!(pin!(gate.close_and_drain())).is_ready(),
                        held.is_empty(),
                        "the drain must finish only when the gate holds nothing"
                    );
                    closed = true;
                }
            }
            assert_eq!(
                gate.count(),
                held.len() as u64,
                "the count must be what the gate holds"
            );
            assert_eq!(
                gate.is_closed(),
                closed,
                "the closed bit must be the model's"
            );
        }

        let mut drain = pin!(gate.close_and_drain());
        while !held.is_empty() {
            assert!(
                poll!(drain.as_mut()).is_pending(),
                "the drain must wait while {} tickets are held",
                held.len()
            );
            drop(held.pop());
        }
        assert!(
            poll!(drain.as_mut()).is_ready(),
            "the drain must finish once the last ticket has left"
        );
        TestResult::passed()
    })
}

/// A gate closed while nobody is inside drains without waiting at all.
#[test]
fn an_empty_gate_drains_at_once() -> Result<()> {
    let runtime = Builder::new_current_thread().enable_time().build()?;
    runtime.block_on(async {
        let gate = AdmissionGate::new();
        let mut drain = pin!(gate.close_and_drain());
        assert!(
            poll!(drain.as_mut()).is_ready(),
            "an empty gate must not wait"
        );
    });
    Ok(())
}
