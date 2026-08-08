//! What the peer method puts on the wire, and what it reads back off it.
//!
//! The frame is encoded and decoded by the response layer's own writer and
//! reader, not by a generated protobuf message. That is the point of a codec
//! here: the reader enforces rules a `.proto` cannot state — one occurrence per
//! field, no field whose proto3 default is illegal, a version this build
//! speaks, bounded strings, and one right-sized payload allocation.
//!
//! The two directions are deliberately asymmetric. A responder hands the
//! transport bytes it already framed, and the peer method has no response body,
//! so the client encodes bytes and decodes nothing while the server decodes a
//! frame and encodes nothing.

use crate::response::frame::decode::{FrameDecodeError, decode_frame};
use crate::response::frame::{FrameCap, ResponseFrame};
use bytes::{Buf, BufMut, Bytes};
use tonic::Status;
use tonic::codec::{BufferSettings, Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};
use tracing::warn;

/// The compression flag and the length prefix gRPC writes before each message.
const GRPC_HEADER_BYTES: usize = 5;

/// One framed response, owned.
///
/// tonic's encoder must be `'static`, so it cannot borrow the sender's buffer.
/// A response therefore pays a right-sized allocation and a copy into this
/// value, and tonic then copies it again into the per-call buffer it owns. Both
/// copies precede one network round trip, and both buffers are bounded by the
/// frame ceiling. A pool cannot reclaim either, because tonic owns its buffer
/// until the write completes.
/// [`ResponseSender`](crate::router::ResponseSender) owns that trade.
pub(crate) struct FrameBytes(Bytes);

/// The listener's codec: it reads a frame and writes nothing.
///
/// Generated code builds this with [`Default`] and can pass it nothing, which
/// is why the counter it moves is the process's rather than a field.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct ServerFrameCodec;

/// The client's codec: it writes one already-framed response and reads nothing.
///
/// It carries the frame's length so tonic sizes its per-call buffer to that
/// exact frame. Without it tonic allocates 8 KiB per call and grows in 8 KiB
/// steps.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ClientFrameCodec {
    frame_len: usize,
}

impl FrameBytes {
    /// Takes ownership of one complete frame.
    pub(crate) const fn new(bytes: Bytes) -> Self {
        Self(bytes)
    }
}

impl Codec for ServerFrameCodec {
    type Decode = ResponseFrame;
    type Decoder = Self;
    type Encode = ();
    type Encoder = Self;

    fn encoder(&mut self) -> Self {
        *self
    }

    fn decoder(&mut self) -> Self {
        *self
    }
}

impl Encoder for ServerFrameCodec {
    type Error = Status;
    type Item = ();

    /// The peer method answers with an empty message: the outcome is the gRPC
    /// status, so there is nothing to write.
    fn encode(&mut self, (): (), _dst: &mut EncodeBuf<'_>) -> Result<(), Status> {
        Ok(())
    }

    /// The answer is always the header alone, so the per-call buffer is sized
    /// to it rather than to tonic's 8 KiB default. The second number is the
    /// streaming yield threshold, which one unary message never reaches.
    fn buffer_settings(&self) -> BufferSettings {
        BufferSettings::new(GRPC_HEADER_BYTES, GRPC_HEADER_BYTES)
    }
}

impl Decoder for ServerFrameCodec {
    type Error = Status;
    type Item = ResponseFrame;

    /// Reads one frame, and counts what it refuses.
    ///
    /// The transport rejects an over-cap message before a byte reaches this
    /// reader. This reader uses the type's upper bound as a second defense.
    /// The size arm of [`refusal`] applies only when a caller drives the codec
    /// directly.
    ///
    /// This direction keeps tonic's own receive buffer, which grows to the
    /// message once and is freed with the call. Sizing it to the frame
    /// ceiling instead would hold that ceiling open on every idle stream, and
    /// the codec is built through [`Default`] and could not read the ceiling
    /// anyway.
    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<Option<ResponseFrame>, Status> {
        match decode_frame(src, FrameCap::MAX) {
            Ok(frame) => Ok(Some(frame)),
            Err(error) => {
                // The only record of why: the status carries a literal, so the
                // detail the `Display` form names stays on this node.
                warn!(%error, "peer frame could not be read");
                Err(refusal(&error))
            }
        }
    }
}

impl ClientFrameCodec {
    /// A codec for one call carrying a frame of `frame_len` bytes.
    pub(crate) const fn new(frame_len: usize) -> Self {
        Self { frame_len }
    }
}

impl Codec for ClientFrameCodec {
    type Decode = ();
    type Decoder = Self;
    type Encode = FrameBytes;
    type Encoder = Self;

    fn encoder(&mut self) -> Self {
        *self
    }

    fn decoder(&mut self) -> Self {
        *self
    }
}

impl Encoder for ClientFrameCodec {
    type Error = Status;
    type Item = FrameBytes;

    fn encode(&mut self, item: FrameBytes, dst: &mut EncodeBuf<'_>) -> Result<(), Status> {
        dst.put_slice(&item.0);
        Ok(())
    }

    /// Exactly one frame plus its gRPC header, so tonic's per-call buffer is
    /// allocated once at the right size and never grows. The second number is
    /// the streaming yield threshold, which one unary message never reaches.
    fn buffer_settings(&self) -> BufferSettings {
        let bytes = self.frame_len.saturating_add(GRPC_HEADER_BYTES);
        BufferSettings::new(bytes, bytes)
    }
}

impl Decoder for ClientFrameCodec {
    type Error = Status;
    type Item = ();

    /// The peer method answers with the gRPC status and no body. The client's
    /// zero decoding ceiling refuses a larger answer before this reader runs,
    /// so what it consumes is always empty.
    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<Option<()>, Status> {
        src.advance(src.remaining());
        Ok(Some(()))
    }

    /// The answer is the header alone, so the receive buffer is sized to it
    /// rather than to tonic's 8 KiB default.
    fn buffer_settings(&self) -> BufferSettings {
        BufferSettings::new(GRPC_HEADER_BYTES, GRPC_HEADER_BYTES)
    }
}

/// The status one unreadable frame is refused with.
///
/// A frame over the ceiling is a size fault and a `RESOURCE`-shaped answer
/// would invite a retry, so it is `OUT_OF_RANGE`. Everything else is a peer
/// that sent bytes this build cannot read, which no retry fixes.
///
/// The wording is [`FrameDecodeError::message`] rather than the `Display` form,
/// because this port is unauthenticated: a formatted status would allocate per
/// refused frame at a rate the sender chooses, and would echo back the lengths
/// and versions that sender claimed.
fn refusal(error: &FrameDecodeError) -> Status {
    match error {
        FrameDecodeError::FrameTooLarge { .. } => Status::out_of_range(error.message()),
        FrameDecodeError::Truncated { .. }
        | FrameDecodeError::MissingField(_)
        | FrameDecodeError::RepeatedField(_)
        | FrameDecodeError::UnsupportedVersion(_)
        | FrameDecodeError::MalformedId { .. }
        | FrameDecodeError::StringTooLong { .. }
        | FrameDecodeError::InvalidUtf8(_)
        | FrameDecodeError::Subsystem(_)
        | FrameDecodeError::StatusTooWide(_)
        | FrameDecodeError::Status(_)
        | FrameDecodeError::Wire(_) => Status::invalid_argument(error.message()),
    }
}
