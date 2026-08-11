//! What the peer method puts on the wire, and what it reads back off it.
//!
//! The frame is encoded and decoded by the response layer's own writer and
//! reader, not by a generated protobuf message. That is the point of a codec
//! here: the reader enforces rules a `.proto` cannot state — one occurrence per
//! field, no field whose proto3 default is illegal, bounded strings, and a
//! payload slice from Tonic's receive storage.
//!
//! The two directions are deliberately asymmetric. The client writes an owned
//! frame into Tonic's final buffer. The server decodes that frame and returns
//! no response body.

use crate::response::frame::ResponseFrame;
use crate::response::frame::decode::{FrameDecodeError, decode_frame};
use crate::router::Framed;
use bytes::Buf;
use std::marker::PhantomData;
use tonic::Status;
use tonic::codec::{BufferSettings, Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};
use tracing::warn;

/// The compression flag and the length prefix gRPC writes before each message.
const GRPC_HEADER_BYTES: usize = 5;

/// The listener's codec: it reads a frame and writes nothing.
///
/// Generated code builds this with [`Default`] and can pass it nothing, which
/// is why the counter it moves is the process's rather than a field.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct ServerFrameCodec;

/// The client's codec: it writes one owned frame and reads nothing.
///
/// It carries the frame's length so tonic sizes its per-call buffer to that
/// exact frame. Without it tonic allocates 8 KiB per call and grows in 8 KiB
/// steps.
#[derive(Debug)]
pub(crate) struct ClientFrameCodec<F> {
    frame_len: usize,
    frame: PhantomData<fn() -> F>,
}

impl<F> Clone for ClientFrameCodec<F> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<F> Copy for ClientFrameCodec<F> {}

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
    /// This direction keeps tonic's own receive buffer, which grows to the
    /// message once and frees it with the call.
    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<Option<ResponseFrame>, Status> {
        match decode_frame(src) {
            Ok(frame) => Ok(Some(frame)),
            Err(error) => {
                // The only record of why: the status carries a literal, so the
                // detail the `Display` form names stays on this peer.
                warn!(%error, "peer frame could not be read");
                Err(refusal(&error))
            }
        }
    }

    /// Start with only the gRPC header. Tonic reserves the declared message
    /// length after it reads that header, so an 8 KiB initial buffer is waste.
    fn buffer_settings(&self) -> BufferSettings {
        BufferSettings::new(GRPC_HEADER_BYTES, GRPC_HEADER_BYTES)
    }
}

impl<F> ClientFrameCodec<F> {
    /// A codec for one call carrying a frame of `frame_len` bytes.
    pub(crate) const fn new(frame_len: usize) -> Self {
        Self {
            frame_len,
            frame: PhantomData,
        }
    }
}

impl<F: Framed + Sync> Codec for ClientFrameCodec<F> {
    type Decode = ();
    type Decoder = Self;
    type Encode = F;
    type Encoder = Self;

    fn encoder(&mut self) -> Self {
        *self
    }

    fn decoder(&mut self) -> Self {
        *self
    }
}

impl<F: Framed> Encoder for ClientFrameCodec<F> {
    type Error = Status;
    type Item = F;

    fn encode(&mut self, item: F, dst: &mut EncodeBuf<'_>) -> Result<(), Status> {
        item.write(dst);
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

impl<F> Decoder for ClientFrameCodec<F> {
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
/// Every error is a peer that sent bytes this build cannot read. A retry cannot
/// fix the frame.
///
/// The wording is [`FrameDecodeError::message`] rather than the `Display` form,
/// because this port is unauthenticated: a formatted status would allocate per
/// refused frame at a rate the sender chooses, and would echo back the lengths
/// that sender claimed.
fn refusal(error: &FrameDecodeError) -> Status {
    match error {
        FrameDecodeError::Truncated { .. }
        | FrameDecodeError::MissingField(_)
        | FrameDecodeError::RepeatedField(_)
        | FrameDecodeError::MalformedId { .. }
        | FrameDecodeError::StringTooLong { .. }
        | FrameDecodeError::CategoryTooWide(_)
        | FrameDecodeError::UnknownCategory(_)
        | FrameDecodeError::InvalidText(_)
        | FrameDecodeError::InvalidSubsystem(_)
        | FrameDecodeError::Protobuf(_) => Status::invalid_argument(error.message()),
    }
}
