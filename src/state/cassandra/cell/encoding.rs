//! Payload-cell encoding for keyed-state collections.
//!
//! New rows use raw bytes through 16 KiB and Zstd above that size. The reader
//! supports both forms and all Zstd frames from earlier Prosody versions.

use crate::error::{ClassifyError, ErrorCategory};
use bytes::Bytes;
use std::cell::RefCell;
use std::io::{self, Cursor, Read};
use std::ops::Deref;
use thiserror::Error;
use zstd::bulk::Compressor;
use zstd::stream::read::Decoder;
use zstd::zstd_safe::{self, DCtx};

const ZSTD_LEVEL: i32 = 0;
pub(super) const CASSANDRA_COMPRESSION_BLOCK_BYTES: usize = 16 * 1024;

thread_local! {
    static CODEC: RefCell<Option<Codec>> = const { RefCell::new(None) };
}

/// One thread owns this codec state. Its scratch never escapes a codec call.
/// The scratch retains the largest decoded capacity that this thread saw.
struct Codec {
    compressor: Compressor<'static>,
    decompressor: DCtx<'static>,
    decode_scratch: Vec<u8>,
}

/// Encoded bytes for one Cassandra blob.
///
/// Raw values share the codec output. Zstd values own the compressor output.
#[derive(Debug)]
pub(super) enum EncodedPayload {
    /// Shared raw codec output.
    Raw(Bytes),
    /// Owned Zstd frame.
    Zstd(Vec<u8>),
}

impl AsRef<[u8]> for EncodedPayload {
    fn as_ref(&self) -> &[u8] {
        self
    }
}

impl Deref for EncodedPayload {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Raw(bytes) => bytes,
            Self::Zstd(bytes) => bytes,
        }
    }
}

impl Codec {
    fn new() -> io::Result<Self> {
        Ok(Self {
            compressor: Compressor::new(ZSTD_LEVEL)?,
            decompressor: DCtx::try_create()
                .ok_or_else(|| io::Error::other("Zstd decoder initialization failed"))?,
            decode_scratch: Vec::new(),
        })
    }
}

/// Encoding discriminator for value payload cells.
///
/// Values 1 through 3 are retired. Readers must reject them as permanent
/// durable-data errors.
#[repr(i16)]
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub(in crate::state::cassandra) enum Encoding {
    /// Raw codec bytes compressed with Zstd.
    Zstd = 4,
    /// Raw codec bytes without application compression.
    Raw = 5,
}

impl From<Encoding> for i16 {
    fn from(encoding: Encoding) -> Self {
        encoding as i16
    }
}

impl TryFrom<i16> for Encoding {
    type Error = EncodingError;

    fn try_from(value: i16) -> Result<Self, Self::Error> {
        match value {
            4 => Ok(Self::Zstd),
            5 => Ok(Self::Raw),
            _ => Err(EncodingError::UnknownEncoding(value)),
        }
    }
}

/// Selects one durable encoding for all present blobs in a row.
#[must_use]
pub(super) fn select_encoding(payload_len: usize) -> Encoding {
    if payload_len > CASSANDRA_COMPRESSION_BLOCK_BYTES {
        Encoding::Zstd
    } else {
        Encoding::Raw
    }
}

/// Encodes raw codec bytes with the selected durable encoding.
pub(in crate::state::cassandra) fn encode_payload(
    payload: &Bytes,
    encoding: Encoding,
) -> Result<EncodedPayload, EncodingError> {
    match encoding {
        Encoding::Zstd => compress(payload),
        Encoding::Raw => Ok(EncodedPayload::Raw(payload.clone())),
    }
}

/// Decodes durable payload bytes into a compact owned value.
pub(in crate::state::cassandra) fn decode_payload(
    bytes: &[u8],
    encoding: Encoding,
) -> Result<Bytes, EncodingError> {
    match encoding {
        Encoding::Zstd => decompress(bytes),
        Encoding::Raw => Ok(Bytes::copy_from_slice(bytes)),
    }
}

fn compress(raw: &[u8]) -> Result<EncodedPayload, EncodingError> {
    CODEC.with(|slot| {
        let mut codec = slot.borrow_mut();
        if codec.is_none() {
            *codec = Some(Codec::new().map_err(EncodingError::BadZstd)?);
        }
        let Some(codec) = codec.as_mut() else {
            return Err(EncodingError::BadZstd(io::Error::other(
                "Zstd codec initialization failed",
            )));
        };
        let mut encoded = Vec::with_capacity(zstd_safe::compress_bound(raw.len()));
        codec
            .compressor
            .compress_to_buffer(raw, &mut encoded)
            .map_err(EncodingError::BadZstd)?;
        Ok(EncodedPayload::Zstd(encoded))
    })
}

fn decompress(bytes: &[u8]) -> Result<Bytes, EncodingError> {
    CODEC.with(|slot| {
        let mut codec = slot.borrow_mut();
        if codec.is_none() {
            *codec = Some(Codec::new().map_err(EncodingError::BadZstd)?);
        }
        let Some(codec) = codec.as_mut() else {
            return Err(EncodingError::BadZstd(io::Error::other(
                "Zstd codec initialization failed",
            )));
        };

        codec.decode_scratch.clear();
        if codec.decode_scratch.capacity() > 0
            && codec
                .decompressor
                .decompress(&mut codec.decode_scratch, bytes)
                .is_ok()
        {
            return Ok(Bytes::copy_from_slice(&codec.decode_scratch));
        }

        codec.decode_scratch.clear();
        let Codec {
            decompressor,
            decode_scratch,
            ..
        } = codec;
        Decoder::with_context(Cursor::new(bytes), decompressor)
            .read_to_end(decode_scratch)
            .map_err(EncodingError::BadZstd)?;
        Ok(Bytes::copy_from_slice(decode_scratch))
    })
}

#[cfg(test)]
pub(super) fn reset_codec() {
    CODEC.with(|slot| *slot.borrow_mut() = None);
}

#[cfg(test)]
pub(super) fn decode_scratch() -> (usize, usize) {
    CODEC.with(|slot| {
        slot.borrow().as_ref().map_or((0, 0), |codec| {
            let capacity = codec.decode_scratch.capacity();
            if capacity == 0 {
                (0, 0)
            } else {
                (codec.decode_scratch.as_ptr() as usize, capacity)
            }
        })
    })
}

/// Error returned by the encoding module.
#[derive(Debug, Error)]
pub enum EncodingError {
    /// The durable column has an unknown payload encoding.
    #[error("unknown payload encoding: {0}")]
    UnknownEncoding(i16),

    /// Zstd compression or decompression failed.
    #[error("bad Zstd: {0}")]
    BadZstd(#[source] io::Error),
}

impl ClassifyError for EncodingError {
    fn classify_error(&self) -> ErrorCategory {
        ErrorCategory::Permanent
    }
}
