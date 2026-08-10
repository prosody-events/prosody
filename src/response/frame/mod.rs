//! The response frame: the wire form of one response, and the conversions each
//! end of a peer link performs on it.
//!
//! The field numbers here are the contract with `proto/peer.proto`, which is
//! the version of the message peers of different releases must agree on. A test
//! decodes the generated descriptor set so the two cannot drift apart.

use super::{FormatToken, RequestId, ResponseStatus};
use crate::router::NodeId;
use crate::subsystem::SubsystemName;
use bytes::Bytes;
#[cfg(test)]
use prost::encoding::{encoded_len_varint, key_len};

pub(crate) mod decode;
pub(crate) mod encode;

const FIELD_PROTOCOL_VERSION: u32 = 1;
const FIELD_TARGET_NODE: u32 = 2;
const FIELD_REQUEST_ID: u32 = 3;
const FIELD_SUBSYSTEM: u32 = 4;
const FIELD_FORMAT: u32 = 5;
const FIELD_STATUS: u32 = 6;
const FIELD_PAYLOAD: u32 = 7;
const FIELD_RELAY_NODE: u32 = 8;

/// Width of every identifier on the wire.
const ID_BYTES: usize = 16;

/// The encoded size of one relay identifier field.
#[cfg(test)]
const RELAY_FIELD_BYTES: usize =
    key_len(FIELD_RELAY_NODE) + encoded_len_varint(ID_BYTES as u64) + ID_BYTES;

/// The routing and classification fields a responder supplies for one frame.
///
/// The format token is deliberately absent: it is the encoding codec's
/// [`Codec::FORMAT_ID`], so a frame cannot claim a format it was not encoded
/// with. The protocol version is likewise the encoder's.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FrameHeader {
    /// The node waiting for this response.
    pub(crate) target: NodeId,
    /// The request this response answers.
    pub(crate) request: RequestId,
    /// The subsystem the response is for.
    pub(crate) subsystem: SubsystemName,
    /// How the responder classified the result.
    pub(crate) status: ResponseStatus,
    /// Set only by a relay, which always writes its own id and never preserves
    /// one it received. A responder always leaves this `None`.
    pub(crate) relay: Option<NodeId>,
}

/// One decoded response frame.
///
/// The payload shares Tonic's receive allocation. The server codec starts that
/// allocation at the gRPC header size and reserves the declared frame length.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ResponseFrame {
    /// Where the frame goes and how its result was classified.
    pub(crate) header: FrameHeader,
    /// The token the payload bytes were encoded with.
    pub(crate) format: FormatToken,
    /// The encoded response, opaque until a codec that speaks `format` reads
    /// it. Immutable ownership lets local delivery share it and lets Tonic
    /// split it from its receive buffer without copying.
    pub(crate) payload: Bytes,
}

// Visible crate-wide to test modules: the sender's suites and the peer
// transport's reuse this module's codec and its hand-built frame builder rather
// than writing a second one of either.
#[cfg(test)]
pub(crate) mod tests;
