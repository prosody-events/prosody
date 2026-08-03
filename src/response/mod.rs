//! Synchrony recovery's own half: what a response is, how it is framed, and
//! how one delivery attempt is answered.

use crate::error::{ErrorCategory, UnknownErrorCategory};
use fixedstr::Flexstr;
use std::fmt::{Display, Formatter, Result as FmtResult};
use tonic::Code;
use uuid::Uuid;

pub(crate) mod frame;
pub(crate) mod headers;
pub(crate) mod sender;

/// Version of the response frame both ends of a peer link must agree on.
pub(crate) const RESPONSE_PROTOCOL_VERSION: u32 = 1;

/// Longest [`Codec::FORMAT_ID`](crate::Codec::FORMAT_ID) a frame may carry.
pub(crate) const FORMAT_MAX_BYTES: usize = 128;

/// The format token a frame's payload was encoded with, bounded by
/// [`FORMAT_MAX_BYTES`].
pub(crate) type FormatToken = Flexstr<{ FORMAT_MAX_BYTES + 1 }>;

/// How a responder classified the result one frame carries.
///
/// The wire keeps the error categories and adds one discriminant for a
/// success. Thus, a frame never states a category for a successful result.
/// Zero stays reserved, so an omitted field is malformed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ResponseStatus {
    /// The handler succeeded.
    Success,
    /// The handler failed, with this classification.
    Error(ErrorCategory),
}

/// Identifies one request across the fleet.
///
/// Minted as a `UUIDv7`, so ids sort by creation time and read chronologically
/// in a trace. On the wire it is 16 opaque bytes, so a peer that mints ids some
/// other way is still answerable.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct RequestId(Uuid);

impl From<ResponseStatus> for i32 {
    fn from(status: ResponseStatus) -> Self {
        match status {
            ResponseStatus::Success => 4,
            ResponseStatus::Error(category) => Self::from(category),
        }
    }
}

impl TryFrom<i32> for ResponseStatus {
    type Error = UnknownErrorCategory;

    fn try_from(value: i32) -> Result<Self, UnknownErrorCategory> {
        if value == 4 {
            Ok(Self::Success)
        } else {
            ErrorCategory::try_from(value).map(Self::Error)
        }
    }
}

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
