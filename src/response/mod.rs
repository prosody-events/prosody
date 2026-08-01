//! Synchrony recovery's own half: what a response is, how it is framed, and
//! how one delivery attempt is answered.

use fixedstr::Flexstr;
use std::fmt::{Display, Formatter, Result as FmtResult};
use tonic::Code;
use uuid::Uuid;

pub(crate) mod frame;

/// Version of the response frame both ends of a peer link must agree on.
pub(crate) const RESPONSE_PROTOCOL_VERSION: u32 = 1;

/// Longest subsystem name a frame may carry. The bound keeps a decoded name off
/// the heap and keeps a peer-supplied string out of metric labels.
pub(crate) const SUBSYSTEM_MAX_BYTES: usize = 64;

/// Longest [`Codec::FORMAT_ID`](crate::Codec::FORMAT_ID) a frame may carry.
pub(crate) const FORMAT_MAX_BYTES: usize = 128;

/// The subsystem a response answers for, bounded by [`SUBSYSTEM_MAX_BYTES`].
pub(crate) type Subsystem = Flexstr<{ SUBSYSTEM_MAX_BYTES + 1 }>;

/// The format token a frame's payload was encoded with, bounded by
/// [`FORMAT_MAX_BYTES`].
pub(crate) type FormatToken = Flexstr<{ FORMAT_MAX_BYTES + 1 }>;

/// Identifies one request across the fleet.
///
/// Minted as a `UUIDv7`, so ids sort by creation time and read chronologically
/// in a trace. On the wire it is 16 opaque bytes, so a peer that mints ids some
/// other way is still answerable.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct RequestId(Uuid);

impl RequestId {
    /// Mints an id for one request.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "the requester is this constructor's production caller; it is exercised by \
                      this module's tests"
        )
    )]
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

/// How a node answered one delivery attempt.
///
/// Each disposition names exactly one gRPC status and only
/// [`Accepted`](Self::Accepted) names `OK`. There is deliberately no status
/// field in the response body: with no body at all, success is carried in the
/// HTTP/2 trailer and cannot be produced by an omitted field.
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the peer service is this enum's production caller; the mapping is exercised by \
                  this module's tests"
    )
)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(test, derive(strum::VariantArray))]
pub(crate) enum ResponseDisposition {
    /// The response was handed to the waiter.
    Accepted,
    /// No request by that id is registered here.
    UnknownRequest,
    /// The request existed but has already finished.
    ClosedRequest,
    /// A response for that subsystem was already recorded.
    DuplicateSubsystem,
    /// The request is not awaiting that subsystem.
    UnexpectedSubsystem,
    /// The frame's format token is not the one the waiter's codec speaks.
    FormatMismatch,
    /// The frame names no target node, or one that is not 16 bytes.
    MalformedTarget,
    /// The frame is for another node and has already been relayed once.
    AlreadyRelayed,
    /// The relay has no capacity to forward the frame.
    NoRelayCapacity,
    /// No time is left inside the caller's deadline to relay the frame.
    RelayDeadlineExceeded,
    /// The target node could not be resolved or could not be reached.
    Unreachable,
}

#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the peer service is this mapping's production caller; it is exercised by this \
                  module's tests"
    )
)]
impl ResponseDisposition {
    /// The gRPC status this disposition is reported as.
    ///
    /// The mapping lives with the dispositions rather than with the transport,
    /// so a disposition added later cannot reach the wire without being given a
    /// status here.
    pub(crate) const fn status(self) -> Code {
        match self {
            Self::Accepted => Code::Ok,
            Self::UnknownRequest | Self::ClosedRequest => Code::NotFound,
            Self::DuplicateSubsystem => Code::AlreadyExists,
            Self::UnexpectedSubsystem | Self::FormatMismatch | Self::AlreadyRelayed => {
                Code::FailedPrecondition
            }
            Self::MalformedTarget => Code::InvalidArgument,
            Self::NoRelayCapacity => Code::ResourceExhausted,
            Self::RelayDeadlineExceeded => Code::DeadlineExceeded,
            Self::Unreachable => Code::Unavailable,
        }
    }
}

#[cfg(test)]
mod tests;
