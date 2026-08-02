//! Admission to the destination fleet, counted rather than flagged.

use std::pin::pin;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::{AcqRel, Acquire, Release};
use tokio::sync::Notify;

/// Set once admission is closed.
const CLOSED: u64 = 1 << 63;

/// Mask over the ticket count, which occupies every bit below [`CLOSED`].
const COUNT_MASK: u64 = CLOSED - 1;

/// Admission with a count, not a flag.
///
/// A caller enters before it reserves and leaves once it has committed or
/// dropped the reservation. Shutdown closes the gate and then waits for the
/// count to reach zero, so no reservation survives the close whatever the
/// timing. A boolean cannot give that: a caller can already sit between its
/// check and its reservation. Here the closed bit and the count share one word,
/// so the check and the increment are one compare-and-exchange.
pub(crate) struct AdmissionGate {
    state: AtomicU64,
    drained: Notify,
}

/// Proof that one caller is inside the gate. Dropping it leaves.
pub(crate) struct GateTicket<'a> {
    gate: &'a AdmissionGate,
}

impl AdmissionGate {
    /// An open gate with nobody inside.
    pub(crate) fn new() -> Self {
        Self {
            state: AtomicU64::new(0),
            drained: Notify::new(),
        }
    }

    /// Enters the gate, unless it is closed.
    ///
    /// The count also refuses at its 63-bit ceiling. One ticket per caller in
    /// flight puts that out of reach; the arm exists so an overflow can never
    /// reach the closed bit. A caller may therefore read `None` as "closed".
    ///
    /// The check and the increment are one word and one compare-exchange, so
    /// their atomicity is a property of the instruction and owes no test.
    pub(crate) fn enter(&self) -> Option<GateTicket<'_>> {
        let mut state = self.state.load(Acquire);
        loop {
            if state & CLOSED != 0 || state & COUNT_MASK == COUNT_MASK {
                return None;
            }
            match self
                .state
                .compare_exchange_weak(state, state + 1, AcqRel, Acquire)
            {
                Ok(_) => return Some(GateTicket { gate: self }),
                Err(observed) => state = observed,
            }
        }
    }

    /// Closes admission and returns once every ticket has left.
    ///
    /// The wakeup is registered *before* the count is read, so a ticket that
    /// leaves between the read and the await still wakes this caller.
    pub(crate) async fn close_and_drain(&self) {
        self.state.fetch_or(CLOSED, AcqRel);
        loop {
            let mut drained = pin!(self.drained.notified());
            drained.as_mut().enable();
            if self.state.load(Acquire) & COUNT_MASK == 0 {
                return;
            }
            drained.await;
        }
    }

    /// How many tickets are held.
    #[cfg(test)]
    pub(crate) fn count(&self) -> u64 {
        self.state.load(Acquire) & COUNT_MASK
    }

    /// Whether admission is closed.
    #[cfg(test)]
    pub(crate) fn is_closed(&self) -> bool {
        self.state.load(Acquire) & CLOSED != 0
    }
}

impl Drop for GateTicket<'_> {
    fn drop(&mut self) {
        if self.gate.state.fetch_sub(1, Release) & COUNT_MASK == 1 {
            self.gate.drained.notify_waiters();
        }
    }
}
