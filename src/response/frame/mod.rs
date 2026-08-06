//! The response frame: the wire form of one response, and the conversions each
//! end of a peer link performs on it.
//!
//! The field numbers here are the contract with `proto/peer.proto`, which is
//! the version of the message peers of different releases must agree on. A test
//! decodes the generated descriptor set so the two cannot drift apart.

use super::{FormatToken, RequestId, ResponseStatus};
use crate::router::NodeId;
use crate::subsystem::SubsystemName;
use bytes::BytesMut;
use prost::encoding::{encoded_len_varint, key_len};
use thiserror::Error;

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
const RELAY_FIELD_BYTES: usize =
    key_len(FIELD_RELAY_NODE) + encoded_len_varint(ID_BYTES as u64) + ID_BYTES;

/// The one configured ceiling on an encoded frame.
///
/// It bounds the complete frame rather than the payload alone, and it is the
/// size the outgoing transport buffer is created at, so a maximum-size frame
/// never grows that buffer. Its range is checked once, here, so no later code
/// has to wonder whether a ceiling is usable.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct FrameCap(usize);

/// The routing and classification fields a responder supplies for one frame.
///
/// The format token is deliberately absent: it is the encoding codec's
/// [`Codec::FORMAT_ID`], so a frame cannot claim a format it was not encoded
/// with. The protocol version is likewise the encoder's.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct FrameHeader {
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
/// The payload is a single right-sized allocation made at the earliest point
/// its length is known, so nothing here pins the transport's receive block.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ResponseFrame {
    /// Where the frame goes and how its result was classified.
    pub(crate) header: FrameHeader,
    /// The token the payload bytes were encoded with.
    pub(crate) format: FormatToken,
    /// The encoded response, opaque until a codec that speaks `format` reads
    /// it.
    pub(crate) payload: BytesMut,
}

impl FrameCap {
    /// The ceiling a process publishes when an operator asks for none.
    pub(crate) const DEFAULT: Self = Self(64 * 1024);
    /// The widest ceiling this type admits.
    ///
    /// A reader that is already bounded by its transport passes this, so its
    /// own check is the type's upper bound rather than a second ceiling to
    /// configure.
    pub(crate) const MAX: Self = Self(Self::MAX_BYTES);
    /// Above this one frame could exhaust a receiver's whole buffer budget.
    pub(crate) const MAX_BYTES: usize = 16 * 1024 * 1024;
    /// Below this a frame's largest legal header and relay field would not fit.
    pub(crate) const MIN_BYTES: usize = key_len(FIELD_PROTOCOL_VERSION)
        + 1
        + 2 * (key_len(FIELD_TARGET_NODE) + 1 + ID_BYTES)
        + key_len(FIELD_SUBSYSTEM)
        + 1
        + SubsystemName::MAX_BYTES
        + key_len(FIELD_FORMAT)
        + 2
        + super::FORMAT_MAX_BYTES
        + key_len(FIELD_STATUS)
        + 1
        + key_len(FIELD_PAYLOAD)
        + 1
        + RELAY_FIELD_BYTES;

    /// Accepts a configured ceiling within the supported range.
    ///
    /// # Errors
    ///
    /// Returns [`FrameCapError::OutOfRange`] for anything outside
    /// [`MIN_BYTES`](Self::MIN_BYTES)..=[`MAX_BYTES`](Self::MAX_BYTES).
    pub(crate) fn new(bytes: usize) -> Result<Self, FrameCapError> {
        if (Self::MIN_BYTES..=Self::MAX_BYTES).contains(&bytes) {
            Ok(Self(bytes))
        } else {
            Err(FrameCapError::OutOfRange {
                bytes,
                min: Self::MIN_BYTES,
                max: Self::MAX_BYTES,
            })
        }
    }

    /// The ceiling, in bytes.
    pub(crate) const fn bytes(self) -> usize {
        self.0
    }
}

/// A configured frame ceiling the transport cannot use.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum FrameCapError {
    /// The ceiling is outside the supported range.
    #[error("frame cap {bytes} is outside {min}..={max} bytes")]
    OutOfRange {
        /// The configured ceiling.
        bytes: usize,
        /// The smallest usable ceiling.
        min: usize,
        /// The largest usable ceiling.
        max: usize,
    },
}

// Visible crate-wide to test modules: the sender's suites and the peer
// transport's reuse this module's codec and its hand-built frame builder rather
// than writing a second one of either.
#[cfg(test)]
pub(crate) mod tests;
