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

use super::TRANSPORT;
use crate::response::frame::decode::{FrameDecodeError, decode_frame};
use crate::response::frame::{FrameCap, ResponseFrame};
use bytes::{Buf, BufMut, Bytes};
use tonic::Status;
use tonic::codec::{BufferSettings, Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};

/// The compression flag and the length prefix gRPC writes before each message.
const GRPC_HEADER_BYTES: usize = 5;

/// One framed response, owned.
///
/// tonic's encoder must be `'static`, so it cannot borrow the worker's scratch.
/// A response therefore pays one right-sized copy into this value immediately
/// before a network round trip. That is the trade
/// [`ResponseSender`](crate::router::ResponseSender) documents, and it is not
/// re-litigated here: a pool cannot reclaim the bytes, because tonic owns them
/// until the write completes.
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
    /// to it rather than to tonic's 8 KiB default.
    fn buffer_settings(&self) -> BufferSettings {
        BufferSettings::new(GRPC_HEADER_BYTES, GRPC_HEADER_BYTES)
    }
}

impl Decoder for ServerFrameCodec {
    type Error = Status;
    type Item = ResponseFrame;

    /// Reads one frame, and counts what it refuses.
    ///
    /// The ceiling passed here is the type's own upper bound, not the
    /// listener's configured one: the listener sets `max_decoding_message_size`
    /// to that ceiling, so a message over it is refused before a byte reaches
    /// this reader.
    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<Option<ResponseFrame>, Status> {
        match decode_frame(src, FrameCap::MAX) {
            Ok(frame) => Ok(Some(frame)),
            Err(error) => {
                TRANSPORT.record_rejected_frame();
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
    /// allocated once at the right size and never grows.
    fn buffer_settings(&self) -> BufferSettings {
        let bytes = self.frame_len.saturating_add(GRPC_HEADER_BYTES);
        BufferSettings::new(bytes, bytes)
    }
}

impl Decoder for ClientFrameCodec {
    type Error = Status;
    type Item = ();

    /// The peer method has no response body. Anything a peer sent anyway is
    /// consumed so the stream stays aligned, and is never read.
    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<Option<()>, Status> {
        src.advance(src.remaining());
        Ok(Some(()))
    }
}

/// The status one unreadable frame is refused with.
///
/// A frame over the ceiling is a size fault and a `RESOURCE`-shaped answer
/// would invite a retry, so it is `OUT_OF_RANGE`. Everything else is a peer
/// that sent bytes this build cannot read, which no retry fixes.
fn refusal(error: &FrameDecodeError) -> Status {
    match error {
        FrameDecodeError::FrameTooLarge { .. } => Status::out_of_range(error.to_string()),
        _ => Status::invalid_argument(error.to_string()),
    }
}
