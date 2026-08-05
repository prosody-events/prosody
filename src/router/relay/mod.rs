//! One bounded hop, from the process a frame reached to the process it names.
//!
//! A process that forwards a frame stands beside its target already: it was
//! reached through the entry point that fronts that target. So it dials the
//! direct endpoint and reads no declared label at all. Forwarding is therefore
//! correct even where the labels are unset, wrong, or disagreed upon, which is
//! what makes it the fallback that always works.

#![cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "no production caller yet: consumer wiring will own the process runtime; every \
                  item here is exercised by this module's tests"
    )
)]

use crate::router::fleet::{Destination, DestinationFleet, Refusal};
use crate::router::{Framed, NodeId, RelayHop, ResponseSender, SendFailure};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::OwnedSemaphorePermit;
use tokio::time::{Instant, sleep_until, timeout_at};
use tonic::Code;
use tracing::warn;

#[cfg(test)]
mod tests;

/// What a process does with a frame that named some node.
///
/// [`Forward`](Self::Forward) carries no node id. Carrying one was examined and
/// rejected: the caller holds the target already, so the field would only add a
/// binding at every arm.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Routing {
    /// The frame names this process. Hand it to the waiter.
    Accept,
    /// The frame names another process. Send it on, once.
    Forward,
    /// The frame already passed through a relay. Refuse it.
    AlreadyRelayed,
}

/// Sends a frame on to the process it names, inside the caller's budget.
///
/// A relay holds one [`RelayHop`] alone, and it stamps no frame itself. The
/// caller passes the forwarded form, which carries the relay id by
/// construction. A parameter typed as that form would make the router name a
/// response, which this module's own rule forbids, and a marker trait would
/// signal the requirement rather than carry it.
pub(crate) struct Relay<R> {
    router: R,
}

impl<R: RelayHop> Relay<R> {
    /// Forwards through `router`.
    ///
    /// [`RelayHop`] rather than the whole [`Router`](crate::router::Router):
    /// the narrower trait offers no lookup that reads a declared label, so
    /// "a forwarder consulted the labels" is not writable here.
    pub(crate) const fn new(router: R) -> Self {
        Self { router }
    }

    /// Delivers `frame` to `target`, and answers only once that delivery is
    /// over.
    ///
    /// The status this returns covers the whole path, so a responder is never
    /// told it succeeded while the requester still waits.
    ///
    /// Nothing is spawned. The outbound call stays owned by the inbound one, so
    /// a caller that goes away cancels the call and releases the send slot with
    /// it.
    ///
    /// # Errors
    ///
    /// Returns [`RelayFailure`] when capacity, time, the lookup, or the
    /// delivery itself stopped the hop.
    pub(crate) async fn forward<F: Framed + Sync>(
        &self,
        target: NodeId,
        deadline: Instant,
        frame: &F,
    ) -> Result<(), RelayFailure> {
        // A frame whose budget was already spent when it arrived reserves
        // nothing. Answering first is what stops a caller from admitting one
        // destination after another into the table with frames that could never
        // have been delivered.
        if Instant::now() >= deadline {
            return Err(RelayFailure::DeadlineExceeded);
        }
        // The slot is taken before the lookup and held to the end of this
        // scope, so a flood of frames for other nodes buys no unmetered channel
        // into this process's send capacity.
        let (destination, _permit) = slot(self.router.fleet(), target)?;
        // One guard over the whole path — the pacing wait, the directory read
        // and the dial — so a slow lookup cannot hold the slot past the
        // caller's budget.
        match timeout_at(deadline, self.hop(&destination, target, frame, deadline)).await {
            Err(_) => Err(RelayFailure::DeadlineExceeded),
            Ok(outcome) => outcome,
        }
    }

    /// Paces, resolves the target's direct endpoint, and makes one attempt.
    ///
    /// One attempt rather than several: the responder that sent this frame
    /// keeps its own attempt budget, and a relay that retried would multiply
    /// that budget by its own.
    async fn hop<F: Framed + Sync>(
        &self,
        destination: &Destination,
        target: NodeId,
        frame: &F,
        deadline: Instant,
    ) -> Result<(), RelayFailure> {
        sleep_until(destination.next_send()).await;
        let address = self
            .router
            .direct(target)
            .await
            .map_err(|error| {
                // A directory that is down and a node that published nothing
                // both reach the caller as one status, so the difference
                // between them is only readable here.
                warn!(%error, node = %target, "peer route lookup failed");
                RelayFailure::Unreachable
            })?
            .ok_or(RelayFailure::Unreachable)?;
        match self
            .router
            .sender()
            .deliver(&address, frame, deadline)
            .await
        {
            Ok(()) => Ok(()),
            Err(SendFailure::Status(code)) => Err(RelayFailure::Target(code)),
            Err(SendFailure::Expired) => Err(RelayFailure::DeadlineExceeded),
            Err(SendFailure::Unreachable | SendFailure::Undialable) => {
                Err(RelayFailure::Unreachable)
            }
        }
    }
}

/// What `this` process does with a frame that names `target` and `relay`.
///
/// A frame is accepted only by the process it names. A frame that already names
/// a relay is never sent on again, which is what stops two processes with stale
/// directory entries from passing one frame back and forth until a deadline.
pub(crate) fn routing(this: NodeId, target: NodeId, relay: Option<NodeId>) -> Routing {
    if target == this {
        Routing::Accept
    } else if relay.is_some() {
        Routing::AlreadyRelayed
    } else {
        Routing::Forward
    }
}

/// Takes one send slot on `node` and hands the permit out.
///
/// Synchronous on purpose: a [`Reservation`](crate::router::fleet::Reservation)
/// holds a gate ticket that is not `Send`, so it must never enter an async
/// body. Here it is created and consumed inside one function, and only `Send`
/// values leave.
///
/// # Errors
///
/// Returns [`RelayFailure::NoCapacity`] when the fleet has no room, and
/// [`RelayFailure::Unreachable`] when admission has closed: a process on the
/// way out is unavailable rather than short of capacity.
fn slot(
    fleet: &DestinationFleet,
    node: NodeId,
) -> Result<(Arc<Destination>, OwnedSemaphorePermit), RelayFailure> {
    let reservation = fleet.reserve(node).map_err(|refusal| match refusal {
        Refusal::NoDestination | Refusal::NoSlot => RelayFailure::NoCapacity,
        Refusal::ShuttingDown => RelayFailure::Unreachable,
    })?;
    let destination = Arc::clone(reservation.destination());
    Ok(reservation.commit(|permit| (destination, permit)))
}

/// Why one forward did not deliver.
///
/// [`Target`](Self::Target) carries the gRPC status the hop came to, rather
/// than a code of this crate's own, for the reason [`crate::router::grpc`]
/// states.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum RelayFailure {
    /// This process has no send capacity for the target.
    #[error("the relay has no send capacity")]
    NoCapacity,

    /// The caller's budget ran out before the hop finished.
    #[error("the relay budget ran out")]
    DeadlineExceeded,

    /// The target published no direct endpoint, or nothing answered there.
    #[error("the relay target could not be reached")]
    Unreachable,

    /// The hop came to this status. A status this process's own transport
    /// produced reads the same as one the target answered, so this does not
    /// prove the target read the frame.
    #[error("the relay hop came to {0:?}")]
    Target(Code),
}
