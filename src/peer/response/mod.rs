//! Synchrony recovery's own half: what a response is, how it is framed, and
//! how one delivery attempt is answered.

use bytes::Bytes;
use opentelemetry::KeyValue;
use opentelemetry::global::meter;
use opentelemetry::metrics::Counter;
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::str::{Utf8Error, from_utf8};
use std::sync::LazyLock;
use tonic::Code;
use uuid::Uuid;

pub(crate) mod frame;
pub(crate) mod headers;
pub(crate) mod sender;

/// The response codec format token.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct FormatToken(Bytes);

impl FormatToken {
    pub(crate) const fn make(value: &'static str) -> Self {
        Self(Bytes::from_static(value.as_bytes()))
    }

    pub(crate) fn try_from_bytes(value: Bytes) -> Result<Self, Utf8Error> {
        from_utf8(&value)?;
        Ok(Self(value))
    }

    pub(crate) fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

/// Delivery attempts this peer answered, by fixed disposition label.
static DISPOSITIONS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    meter("prosody")
        .u64_counter("prosody.response.dispositions")
        .with_description("Response delivery attempts this peer answered")
        .with_unit("{response}")
        .build()
});

/// Identifies one request across the fleet.
///
/// Minted as a `UUIDv7`, so ids sort by creation time and read chronologically
/// in a trace. On the wire it is 16 opaque bytes, so a peer that mints ids some
/// other way is still answerable.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct RequestId(Uuid);

impl RequestId {
    /// Mints an id for one request.
    pub(crate) fn new() -> Self {
        Self(Uuid::now_v7())
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

impl Display for RequestId {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        Display::fmt(&self.0, f)
    }
}

/// Gives the request header writer the UUID without another conversion API.
impl From<RequestId> for Uuid {
    fn from(request: RequestId) -> Self {
        request.0
    }
}

/// How a peer answered one delivery attempt.
///
/// Each disposition names exactly one gRPC status and only
/// [`Accepted`](Self::Accepted) names `OK`. There is deliberately no status
/// field in the response body: with no body at all, success is carried in the
/// HTTP/2 trailer and cannot be produced by an omitted field.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(test, derive(strum::VariantArray))]
pub(crate) enum ResponseDisposition {
    /// The response was handed to the waiter.
    Accepted,
    /// No request by that id is registered here.
    UnknownRequest,
    /// The request existed but has already finished.
    ClosedRequest,
    /// The frame is for another peer and has already been relayed once.
    AlreadyRelayed,
    /// No time is left inside the caller's deadline to relay the frame.
    RelayDeadlineExceeded,
    /// The target peer could not be resolved or could not be reached.
    Unreachable,
}

impl ResponseDisposition {
    /// The gRPC status this disposition is reported as.
    ///
    /// The mapping lives with the dispositions rather than with the transport,
    /// so a disposition added later cannot reach the wire without being given a
    /// status here.
    ///
    /// A relay answers three of these on its own: [`AlreadyRelayed`],
    /// [`RelayDeadlineExceeded`] and [`Unreachable`]. It passes the target's
    /// status through unchanged for every other outcome. Nothing on the
    /// wire says which process spoke, so a sender reads every status as the
    /// endpoint's own word. A per-process origin would need a wire field,
    /// and this mapping names each outcome by what gRPC statuses mean
    /// rather than by who sent it.
    ///
    /// [`AlreadyRelayed`]: Self::AlreadyRelayed
    /// [`RelayDeadlineExceeded`]: Self::RelayDeadlineExceeded
    /// [`Unreachable`]: Self::Unreachable
    pub(crate) const fn status(self) -> Code {
        match self {
            Self::Accepted => Code::Ok,
            Self::UnknownRequest | Self::ClosedRequest => Code::NotFound,
            Self::AlreadyRelayed => Code::FailedPrecondition,
            Self::RelayDeadlineExceeded => Code::DeadlineExceeded,
            Self::Unreachable => Code::Unavailable,
        }
    }

    /// What the sending peer is told this disposition was.
    ///
    /// The wording lives beside the status mapping, so the two travel together
    /// and a `Debug` rendering of a crate-internal name never reaches the wire.
    /// Every message is a literal, so a refusal formats nothing.
    pub(crate) const fn message(self) -> &'static str {
        match self {
            Self::Accepted => "the response was accepted",
            Self::UnknownRequest => "no request by that id is registered here",
            Self::ClosedRequest => "that request has already finished",
            Self::AlreadyRelayed => "the frame has already been relayed once",
            Self::RelayDeadlineExceeded => "no time is left to relay the frame",
            Self::Unreachable => "the target peer could not be reached from here",
        }
    }

    /// Counts one delivery attempt this process **decided**, under this
    /// disposition's label.
    ///
    /// A frame this process only sent on is counted at the peer that decided
    /// it, so a relay never adds a second point for one delivery.
    pub(crate) fn record(self) {
        DISPOSITIONS.add(1, &[KeyValue::new("disposition", self.label())]);
    }

    /// The stable telemetry label for this disposition.
    ///
    /// A fixed string per variant. The frame that produced the disposition
    /// arrived from the network, so nothing it carries — no peer id, no claimed
    /// subsystem — is ever a label.
    pub(crate) const fn label(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::UnknownRequest => "unknown_request",
            Self::ClosedRequest => "closed_request",
            Self::AlreadyRelayed => "already_relayed",
            Self::RelayDeadlineExceeded => "relay_deadline_exceeded",
            Self::Unreachable => "unreachable",
        }
    }
}

#[cfg(test)]
mod tests;
