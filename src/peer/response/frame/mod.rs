//! The response frame: the wire form of one response, and the conversions each
//! end of a peer link performs on it.
//!
//! The field numbers here must match the peer Protobuf schema. A test decodes
//! the generated descriptor set so the two cannot drift apart.

use super::{FormatToken, RequestId};
use crate::error::ErrorCategory;
use crate::peer::router::PeerId;
use crate::subsystem::SubsystemName;
use bytes::Bytes;

pub(crate) mod decode;
pub(crate) mod encode;

const DELIVER_RESULT_TARGET_PEER_TAG: u32 = 1;
const DELIVER_RESULT_REQUEST_ID_TAG: u32 = 2;
const DELIVER_RESULT_SUBSYSTEM_TAG: u32 = 3;
const DELIVER_RESULT_SUCCESS_TAG: u32 = 4;
const DELIVER_RESULT_HANDLER_ERROR_TAG: u32 = 5;
const DELIVER_RESULT_RELAY_PEER_TAG: u32 = 6;
const RESPONSE_SUCCESS_FORMAT_TAG: u32 = 1;
const RESPONSE_SUCCESS_PAYLOAD_TAG: u32 = 2;
const HANDLER_ERROR_CATEGORY_TAG: u32 = 1;
const HANDLER_ERROR_MESSAGE_TAG: u32 = 2;

/// The routing fields a responder supplies for one frame.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FrameHeader {
    /// The peer waiting for this response.
    pub(crate) target: PeerId,
    /// The request this response answers.
    pub(crate) request: RequestId,
    /// The subsystem the response is for.
    pub(crate) subsystem: SubsystemName,
    /// Set only by a relay, which always writes its own id and never preserves
    /// one it received. A responder always leaves this `None`.
    pub(crate) relay: Option<PeerId>,
}

/// A successful result encoded by the user's response codec.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ResponseSuccess {
    pub(crate) format: FormatToken,
    pub(crate) payload: Bytes,
}

/// A handler failure encoded by Prosody.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct HandlerError {
    pub(crate) category: ErrorCategory,
    pub(crate) message: Bytes,
}

/// The application result carried by one response frame.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum FrameResult {
    /// User-owned bytes encoded with the response codec.
    Success(ResponseSuccess),
    /// A handler failure encoded by Prosody.
    HandlerError(HandlerError),
}

/// One decoded response frame.
///
/// The payload shares Tonic's receive allocation. The server codec starts that
/// allocation at the gRPC header size and reserves the declared frame length.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ResponseFrame {
    /// Where the frame goes.
    pub(crate) header: FrameHeader,
    /// The successful value or handler failure.
    pub(crate) result: FrameResult,
}

// Visible crate-wide to test modules: the sender's suites and the peer
// transport's reuse this module's codec and its hand-built frame builder rather
// than writing a second one of either.
#[cfg(test)]
pub(crate) mod tests;
