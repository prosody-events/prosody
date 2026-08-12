use super::decode::FrameDecodeError;
use super::{FrameHeader, FrameResult, ResponseFrame, ResponseSuccess};
use crate::codec::Codec;
use crate::peer::response::RequestId;
use crate::peer::router::PeerId;
use crate::peer::router::grpc::generated::{
    DeliverResultRequest, HandlerError as WireHandlerError, ResponseSuccess as WireSuccess,
    deliver_result_request::Result as WireResult,
};
use crate::subsystem::SubsystemName;
use bytes::{Buf, Bytes, BytesMut};
use color_eyre::Result;
use prost::Message;
use std::cell::Cell;
use std::convert::Infallible;

mod decode;
mod encode;

pub(crate) fn decode_frame<B: Buf>(src: &mut B) -> Result<ResponseFrame, FrameDecodeError> {
    DeliverResultRequest::decode(src)?.try_into()
}

impl From<ResponseFrame> for DeliverResultRequest {
    fn from(frame: ResponseFrame) -> Self {
        let result = Some(match frame.result {
            FrameResult::Success(success) => WireResult::Success(WireSuccess {
                format: Bytes::copy_from_slice(success.format.as_bytes()),
                payload: success.payload,
            }),
            FrameResult::HandlerError(error) => WireResult::HandlerError(WireHandlerError {
                category: i32::from(error.category),
                message: error.message,
            }),
        });
        Self {
            target_peer: Bytes::copy_from_slice(&frame.header.target.into_bytes()),
            request_id: Bytes::copy_from_slice(&frame.header.request.into_bytes()),
            subsystem: Bytes::copy_from_slice(frame.header.subsystem.as_str().as_bytes()),
            result,
            relay_peer: frame.header.relay.map_or_else(Bytes::new, |peer| {
                Bytes::copy_from_slice(&peer.into_bytes())
            }),
        }
    }
}

thread_local! {
    static CACHE_USES: Cell<usize> = const { Cell::new(0) };
    static SERIALIZE_CAPACITY: Cell<usize> = const { Cell::new(0) };
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct CountingCodec;

impl Codec for CountingCodec {
    type Error = Infallible;
    type Payload = Vec<u8>;

    const FORMAT_ID: &'static str = "test-bytes";

    fn deserialize(&mut self, buf: &mut [u8]) -> Result<Vec<u8>, Infallible> {
        Ok(buf.to_vec())
    }

    fn deserialize_owned(&mut self, buf: BytesMut) -> Result<Vec<u8>, Infallible> {
        Ok(buf.into())
    }

    fn serialize_ref(&mut self, payload: &Vec<u8>, buf: &mut Vec<u8>) -> Result<(), Infallible> {
        SERIALIZE_CAPACITY.set(buf.capacity());
        buf.extend_from_slice(payload);
        Ok(())
    }

    fn with_cached_local<R>(f: impl FnOnce(&mut Self) -> R) -> R {
        CACHE_USES.set(CACHE_USES.get() + 1);
        f(&mut Self)
    }
}

pub(crate) fn header(subsystem: &str, relay: Option<PeerId>) -> Result<FrameHeader> {
    Ok(FrameHeader {
        target: PeerId::from_bytes([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]),
        request: RequestId::from_bytes([
            16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
        ]),
        subsystem: SubsystemName::try_new(subsystem)?,
        relay,
    })
}

pub(crate) fn success(frame: &FrameResult) -> Option<(&[u8], &[u8])> {
    match frame {
        FrameResult::Success(ResponseSuccess { format, payload }) => {
            Some((format.as_bytes(), payload.as_ref()))
        }
        FrameResult::HandlerError(_) => None,
    }
}

pub(crate) fn cache_uses() -> usize {
    CACHE_USES.get()
}

pub(crate) fn serialize_capacity() -> usize {
    SERIALIZE_CAPACITY.get()
}
