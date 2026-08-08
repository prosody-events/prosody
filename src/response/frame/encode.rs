//! Turning one borrowed response into owned, bounded frame data.

use super::{
    FIELD_FORMAT, FIELD_PAYLOAD, FIELD_PROTOCOL_VERSION, FIELD_RELAY_NODE, FIELD_REQUEST_ID,
    FIELD_STATUS, FIELD_SUBSYSTEM, FIELD_TARGET_NODE, FrameCap, FrameHeader, ID_BYTES,
    RELAY_FIELD_BYTES, ResponseFrame,
};
use crate::codec::{Codec, SerializeBufGuard};
use crate::response::FormatToken;
use crate::response::ResponseStatus;
use crate::response::{FORMAT_MAX_BYTES, RESPONSE_PROTOCOL_VERSION};
use crate::router::{Framed, NodeId};
use bytes::{BufMut, BytesMut};
use prost::encoding::{WireType, encode_key, encode_varint, encoded_len_varint, key_len};
use std::error::Error;
use std::marker::PhantomData;
use thiserror::Error;

/// Encodes responses through the standard codec cache and serialize buffer.
///
/// [`FrameEncoder::stage`] returns the shared buffer before any asynchronous
/// hook or route starts. The staged frame owns its bounded payload bytes.
/// Thus, a slow hook or peer cannot reserve thread-local codec resources.
pub struct FrameEncoder<C> {
    cap: FrameCap,
    _codec: PhantomData<fn() -> C>,
}

/// A response whose complete frame length has been checked against the cap.
/// Writing one cannot fail and cannot exceed that cap.
pub struct Staged {
    header: FrameHeader,
    format: &'static str,
    payload: BytesMut,
    bytes: usize,
}

/// One frame on its way to its target, with this relay's identifier.
///
/// Construction always replaces `relay_node`. A received value cannot survive
/// this hop, so loop prevention does not depend on caller cleanup.
pub(crate) struct Forwarded(ResponseFrame);

impl<C: Codec> FrameEncoder<C> {
    /// Builds an encoder that uses the standard codec and buffer caches.
    pub(crate) fn new(cap: FrameCap) -> Self {
        Self {
            cap,
            _codec: PhantomData,
        }
    }

    /// Serializes one borrowed response and checks its forwarded frame length.
    ///
    /// # Errors
    ///
    /// Returns [`EncodeError::Codec`] when the codec fails, and
    /// [`EncodeError::TooLarge`] when the forwarded frame would exceed the cap.
    /// The subsystem needs no check here: a [`SubsystemName`] is one a decoder
    /// accepts by construction.
    ///
    /// [`SubsystemName`]: crate::subsystem::SubsystemName
    pub(crate) fn stage(
        &self,
        header: &FrameHeader,
        payload: &C::Payload,
    ) -> Result<Staged, EncodeError<C::Error>> {
        const {
            assert!(
                !C::FORMAT_ID.is_empty(),
                "a codec used for responses must name a format"
            );
            assert!(
                C::FORMAT_ID.len() <= FORMAT_MAX_BYTES,
                "a codec used for responses must have a FORMAT_ID a frame can carry"
            );
        }
        let mut scratch = SerializeBufGuard::acquire();
        C::with_cached_local(|codec| codec.serialize_ref(payload, &mut scratch))
            .map_err(EncodeError::Codec)?;

        let bytes = frame_len(header, C::FORMAT_ID, scratch.len());
        let forwarded = bytes + RELAY_FIELD_BYTES as u64;
        if forwarded > self.cap.bytes() as u64 {
            return Err(EncodeError::TooLarge {
                bytes: forwarded,
                limit: self.cap.bytes(),
            });
        }
        Ok(Staged {
            header: header.clone(),
            format: C::FORMAT_ID,
            payload: BytesMut::from(&scratch[..]),
            bytes: bytes as usize,
        })
    }
}

/// A staged response is what the router delivers, so the transport never sees
/// the response vocabulary above it.
impl Framed for Staged {
    fn bytes(&self) -> usize {
        self.bytes
    }

    /// Writes the frame in field order. Field order is this encoder's choice,
    /// not a protobuf requirement; the decoder accepts any order.
    ///
    /// Every destination this crate writes into is a
    /// [`BytesMut`](bytes::BytesMut), which reserves on demand, and sizing one
    /// at the frame cap keeps it from ever having to.
    fn write<B: BufMut>(&self, dst: &mut B) {
        write_frame(&self.header, self.format, &self.payload, dst);
    }
}

impl Staged {
    pub(in crate::response) const fn header(&self) -> &FrameHeader {
        &self.header
    }

    pub(in crate::response) const fn target(&self) -> NodeId {
        self.header.target
    }

    /// Moves this process's response into the local request registry.
    ///
    /// Only the payload needs owned storage. The local path skips the protobuf
    /// header, transport buffer, socket, and receive-side decode.
    pub(in crate::response) fn into_local_frame(self) -> ResponseFrame {
        ResponseFrame {
            header: self.header,
            format: FormatToken::make(self.format),
            payload: self.payload,
        }
    }
}

impl Forwarded {
    /// Builds a forwarded frame when its encoded form fits `cap`.
    pub(crate) fn new(mut frame: ResponseFrame, relay: NodeId, cap: FrameCap) -> Option<Self> {
        frame.header.relay = Some(relay);
        (frame_len(&frame.header, frame.format.to_str(), frame.payload.len()) <= cap.bytes() as u64)
            .then_some(Self(frame))
    }
}

impl Framed for Forwarded {
    fn bytes(&self) -> usize {
        frame_len(&self.0.header, self.0.format.to_str(), self.0.payload.len()) as usize
    }

    fn write<B: BufMut>(&self, dst: &mut B) {
        write_frame(&self.0.header, self.0.format.to_str(), &self.0.payload, dst);
    }
}

/// Writes one complete frame in the stable field order.
fn write_frame<B: BufMut>(header: &FrameHeader, format: &str, payload: &[u8], dst: &mut B) {
    write_varint_field(
        FIELD_PROTOCOL_VERSION,
        u64::from(RESPONSE_PROTOCOL_VERSION),
        dst,
    );
    write_bytes_field(FIELD_TARGET_NODE, &header.target.into_bytes(), dst);
    write_bytes_field(FIELD_REQUEST_ID, &header.request.into_bytes(), dst);
    write_bytes_field(FIELD_SUBSYSTEM, header.subsystem.as_str().as_bytes(), dst);
    write_bytes_field(FIELD_FORMAT, format.as_bytes(), dst);
    write_varint_field(FIELD_STATUS, status_varint(header.status), dst);
    write_bytes_field(FIELD_PAYLOAD, payload, dst);
    if let Some(relay) = header.relay {
        write_bytes_field(FIELD_RELAY_NODE, &relay.into_bytes(), dst);
    }
}

/// The exact length [`Framed::write`] produces, summed in `u64` so no addition
/// can overflow whatever a codec emitted.
fn frame_len(header: &FrameHeader, format: &str, payload: usize) -> u64 {
    varint_field_len(FIELD_PROTOCOL_VERSION, u64::from(RESPONSE_PROTOCOL_VERSION))
        + bytes_field_len(FIELD_TARGET_NODE, ID_BYTES)
        + bytes_field_len(FIELD_REQUEST_ID, ID_BYTES)
        + bytes_field_len(FIELD_SUBSYSTEM, header.subsystem.as_str().len())
        + bytes_field_len(FIELD_FORMAT, format.len())
        + varint_field_len(FIELD_STATUS, status_varint(header.status))
        + bytes_field_len(FIELD_PAYLOAD, payload)
        + if header.relay.is_some() {
            bytes_field_len(FIELD_RELAY_NODE, ID_BYTES)
        } else {
            0
        }
}

const fn varint_field_len(tag: u32, value: u64) -> u64 {
    key_len(tag) as u64 + encoded_len_varint(value) as u64
}

const fn bytes_field_len(tag: u32, len: usize) -> u64 {
    key_len(tag) as u64 + encoded_len_varint(len as u64) as u64 + len as u64
}

/// An `int32` sign-extends to 64 bits on the wire. Every status is positive,
/// so this is always the one- or two-byte form.
fn status_varint(status: ResponseStatus) -> u64 {
    i64::from(i32::from(status)) as u64
}

// These two are all it takes to emit a complete frame, so they stay private:
// exporting them would put "framed without staging" back within reach of this
// module, where `FrameEncoder` claims it is unrepresentable. A test that needs
// a hand-built frame writes its own fields.
fn write_varint_field<B: BufMut>(tag: u32, value: u64, dst: &mut B) {
    encode_key(tag, WireType::Varint, dst);
    encode_varint(value, dst);
}

fn write_bytes_field<B: BufMut>(tag: u32, value: &[u8], dst: &mut B) {
    encode_key(tag, WireType::LengthDelimited, dst);
    encode_varint(value.len() as u64, dst);
    dst.put_slice(value);
}

/// Why one response could not be turned into a frame.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum EncodeError<E: Error> {
    /// The application's codec failed to serialize the response.
    #[error(transparent)]
    Codec(E),

    /// The forwarded frame would exceed the frame ceiling.
    #[error("framed response is {bytes} bytes, over the {limit}-byte cap")]
    TooLarge {
        /// The length the forwarded frame would have had.
        bytes: u64,
        /// The frame ceiling.
        limit: usize,
    },
}
