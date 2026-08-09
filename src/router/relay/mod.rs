//! One bounded hop, from the process a frame reached to the process it names.
//!
//! A process that forwards a frame stands beside its target already: it was
//! reached through the entry point that fronts that target. So it dials the
//! direct endpoint and reads no declared label at all. Forwarding is therefore
//! correct even where the labels are unset, wrong, or disagreed upon, which is
//! what makes it the fallback that always works.

use crate::router::{Framed, NodeId, RelayHop, ResponseSender, SendFailure};
use thiserror::Error;
use tokio::time::Instant;
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

/// Sends a frame on to the process it names before the caller's deadline.
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
    /// [`RelayHop`] rather than the whole
    /// [`NetworkRouter`](crate::router::NetworkRouter):
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
    /// a caller that goes away cancels the outbound call with it.
    ///
    /// # Errors
    ///
    /// Returns [`RelayFailure`] when the deadline, lookup, or delivery stops
    /// the hop.
    pub(crate) async fn forward<F: Framed + Sync>(
        &self,
        target: NodeId,
        deadline: Instant,
        frame: &F,
    ) -> Result<(), RelayFailure> {
        // Do not resolve or dial after the incoming gRPC deadline.
        if Instant::now() >= deadline {
            return Err(RelayFailure::DeadlineExceeded);
        }
        self.hop(target, frame, deadline).await
    }

    /// Resolves the target's direct endpoint and makes one attempt.
    ///
    /// The outgoing gRPC request receives the incoming request's deadline.
    async fn hop<F: Framed + Sync>(
        &self,
        target: NodeId,
        frame: &F,
        deadline: Instant,
    ) -> Result<(), RelayFailure> {
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

/// Why one forward did not deliver.
///
/// [`Target`](Self::Target) carries the gRPC status the hop came to, rather
/// than a code of this crate's own, for the reason [`crate::router::grpc`]
/// states.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum RelayFailure {
    /// The caller's deadline elapsed before the hop finished.
    #[error("the relay deadline elapsed")]
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
