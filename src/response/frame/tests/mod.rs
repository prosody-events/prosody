use super::{FrameHeader, FrameResult, ResponseSuccess};
use crate::codec::Codec;
use crate::response::RequestId;
use crate::router::PeerId;
use crate::subsystem::SubsystemName;
use bytes::BytesMut;
use color_eyre::Result;
use std::cell::Cell;
use std::convert::Infallible;

mod decode;
mod encode;

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
