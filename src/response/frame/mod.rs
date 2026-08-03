//! The response frame: the wire form of one response, and the conversions each
//! end of a peer link performs on it.
//!
//! The field numbers here are the contract with `proto/peer.proto`, which is
//! the version of the message peers of different releases must agree on. A test
//! decodes the generated descriptor set so the two cannot drift apart.

// The `not(test)` gate is what makes this an *expectation* rather than a
// blanket permission: it holds only while these items really are
// production-dead, so the day the transport calls the last one, the gate
// reports it unfulfilled and demands the attribute be deleted.
#![cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the peer transport is this module's production caller; the conversions are \
                  exercised by this module's tests"
    )
)]

use super::{FormatToken, RequestId, ResponseStatus};
use crate::codec::Codec;
use crate::router::NodeId;
use crate::subsystem::SubsystemName;
use bytes::BytesMut;
use std::error::Error;
use thiserror::Error;

pub(crate) mod decode;
pub(crate) mod encode;

const FIELD_PROTOCOL_VERSION: u32 = 1;
const FIELD_TARGET_NODE: u32 = 2;
const FIELD_REQUEST_ID: u32 = 3;
const FIELD_SUBSYSTEM: u32 = 4;
const FIELD_FORMAT: u32 = 5;
const FIELD_CATEGORY: u32 = 6;
const FIELD_PAYLOAD: u32 = 7;
const FIELD_RELAY_NODE: u32 = 8;

/// Width of every identifier on the wire.
const ID_BYTES: usize = 16;

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
    /// Above this one frame could exhaust a receiver's whole buffer budget.
    pub(crate) const MAX_BYTES: usize = 16 * 1024 * 1024;
    /// Below this a frame's own routing fields would not fit.
    pub(crate) const MIN_BYTES: usize = 256;

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

impl ResponseFrame {
    /// Decodes the payload with `codec`, refusing a frame whose format token is
    /// not the one `codec` speaks — before a payload byte is parsed.
    ///
    /// A matching token is a protocol identifier, not a proof: two unrelated
    /// schemas may pick the same one. What it catches is the ordinary mistake
    /// of a responder upgraded ahead of its requesters.
    ///
    /// # Errors
    ///
    /// Returns [`PayloadError::FormatMismatch`] when the tokens differ, and
    /// [`PayloadError::Codec`] when the codec rejects the bytes.
    pub(crate) fn decode_with<C: Codec>(
        &mut self,
        codec: &mut C,
    ) -> Result<C::Payload, PayloadError<C::Error>> {
        if self.format.to_str() != C::FORMAT_ID {
            return Err(PayloadError::FormatMismatch {
                expected: C::FORMAT_ID,
                actual: Box::new(self.format.clone()),
            });
        }
        codec
            .deserialize(&mut self.payload)
            .map_err(PayloadError::Codec)
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

/// Why a frame's payload could not be handed back to the application.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub(crate) enum PayloadError<E: Error> {
    /// The frame was encoded with a format this codec does not speak. The token
    /// is boxed so one mismatch's diagnostic does not widen every result this
    /// error rides in; the allocation is on a path that already failed.
    #[error("frame is in format {actual}, not {expected}")]
    FormatMismatch {
        /// The token the reading codec speaks.
        expected: &'static str,
        /// The token the frame carries.
        actual: Box<FormatToken>,
    },

    /// The application's codec rejected the payload.
    #[error(transparent)]
    Codec(E),
}

// Visible to the response layer's other test modules: the sender's tests reuse
// this module's codec rather than writing a second one.
#[cfg(test)]
pub(in crate::response) mod tests;
