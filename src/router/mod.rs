//! Reaching any prosody process by id.
//!
//! Every peer feature routes through here, and nothing in this module knows
//! what a response is — [`NodeId`] is the only vocabulary it shares with them.

use fixedstr::Flexstr;
use std::fmt::{Display, Formatter, Result as FmtResult};
use uuid::Uuid;

pub(crate) mod directory;
pub(crate) mod runtime;

/// The host a node publishes for its peers to dial. Any ordinary hostname or
/// address stays inline; a longer one spills to the heap.
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the node directory is this alias's production caller; the selection it feeds is \
                  exercised by this module's tests"
    )
)]
pub(crate) type Host = Flexstr<64>;

/// Identifies one live prosody process.
///
/// Minted fresh at startup and **never reused across restarts**. That is
/// load-bearing rather than tidy: directory writes are unconditional, so a
/// reused id would let a late refresh or a shutdown delete from the previous
/// incarnation overwrite the new one's entry. A fresh id makes that race
/// unrepresentable without any conditional write.
///
/// On the wire it is 16 opaque bytes, so a peer that mints ids some other way
/// is still addressable.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct NodeId(Uuid);

impl NodeId {
    /// Mints an id for one incarnation of one process.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "process identity is this constructor's production caller; it is exercised \
                      by this module's tests"
        )
    )]
    pub(crate) fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// Reads an id from its 16-byte wire form.
    pub(crate) const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(Uuid::from_bytes(bytes))
    }

    /// The 16-byte wire form.
    pub(crate) const fn into_bytes(self) -> [u8; 16] {
        self.0.into_bytes()
    }
}

/// The directory stores a node id in a Cassandra `uuid` column, so the driver's
/// own `Uuid` serde carries it. This conversion is the one place the newtype is
/// unwrapped for that purpose.
impl From<NodeId> for Uuid {
    fn from(node: NodeId) -> Self {
        node.0
    }
}

impl Display for NodeId {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        Display::fmt(&self.0, f)
    }
}

/// Picks the host a node publishes for its peers, in the order a deployment can
/// supply it: the operator's configured host, else the local address the
/// operating system would route to the cluster's own dependencies, else this
/// machine's hostname.
///
/// Each fallback is consulted only when the one before it is absent, so a
/// configured host never costs a route probe and a routable local address never
/// costs a name lookup.
///
/// # Errors
///
/// Returns the hostname lookup's error, and only when neither earlier source
/// supplied a host.
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the node directory is this function's production caller; it is exercised by \
                  this module's tests"
    )
)]
pub(crate) fn select_host<R, H, E>(
    configured: Option<Host>,
    routed: R,
    hostname: H,
) -> Result<Host, E>
where
    R: FnOnce() -> Option<Host>,
    H: FnOnce() -> Result<Host, E>,
{
    if let Some(host) = configured {
        return Ok(host);
    }
    if let Some(host) = routed() {
        return Ok(host);
    }
    hostname()
}

#[cfg(test)]
mod tests;
