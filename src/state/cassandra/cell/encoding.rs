//! Payload-cell encoding for keyed-state collections.
//!
//! New rows use raw bytes through 16 KiB and Zstd above that size. The reader
//! supports both forms and Zstd values from earlier versions.

use crate::error::{ClassifyError, ErrorCategory};
use bytes::Bytes;
use std::cell::RefCell;
use std::io;
use thiserror::Error;
use zstd::bulk::Compressor;
use zstd::zstd_safe::{self, DCtx, ResetDirective};

const ZSTD_LEVEL: i32 = 0;
const ZSTD_BLOCK_HEADER_BYTES: usize = 3;
const ZSTD_MAX_BLOCK_BYTES: usize = 128 * 1_024;
pub(super) const CASSANDRA_COMPRESSION_BLOCK_BYTES: usize = 16 * 1024;

thread_local! {
    static CODEC: RefCell<Option<Codec>> = const { RefCell::new(None) };
}

/// One thread owns this codec state. Its scratch never escapes a codec call.
/// Each scratch buffer retains the largest capacity that this thread saw.
struct Codec {
    compressor: Compressor<'static>,
    decompressor: DCtx<'static>,
    encode_scratch: Vec<u8>,
    decode_scratch: Vec<u8>,
}

/// Encoded bytes paired with their durable discriminator.
pub(super) struct EncodedBlob {
    bytes: Bytes,
    encoding: Encoding,
}

impl EncodedBlob {
    #[must_use]
    pub(super) const fn encoding(&self) -> Encoding {
        self.encoding
    }
}

impl AsRef<[u8]> for EncodedBlob {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl Codec {
    fn new() -> io::Result<Self> {
        Ok(Self {
            compressor: Compressor::new(ZSTD_LEVEL)?,
            decompressor: DCtx::create(),
            encode_scratch: Vec::new(),
            decode_scratch: Vec::new(),
        })
    }
}

/// Encoding discriminator for value payload cells.
///
/// Value 4 identifies the released Zstd format. Value 0 stays invalid.
#[repr(i16)]
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub(in crate::state::cassandra) enum Encoding {
    /// Raw codec bytes compressed with Zstd.
    Zstd = 4,
    /// Raw codec bytes without application compression.
    Raw = 1,
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
            1 => Ok(Self::Raw),
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
) -> Result<Bytes, EncodingError> {
    match encoding {
        Encoding::Zstd => compress(payload),
        Encoding::Raw => Ok(payload.clone()),
    }
}

/// Selects an encoding and returns it with the matching encoded bytes.
pub(super) fn encode(payload: &Bytes) -> Result<EncodedBlob, EncodingError> {
    let encoding = select_encoding(payload.len());
    encode_payload(payload, encoding).map(|bytes| EncodedBlob { bytes, encoding })
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

fn compress(raw: &[u8]) -> Result<Bytes, EncodingError> {
    with_codec(|codec| {
        codec.encode_scratch.clear();
        codec
            .encode_scratch
            .reserve(zstd_safe::compress_bound(raw.len()));
        codec
            .compressor
            .compress_to_buffer(raw, &mut codec.encode_scratch)
            .map_err(EncodingError::BadZstd)?;
        Ok(Bytes::copy_from_slice(&codec.encode_scratch))
    })
}

fn decompress(bytes: &[u8]) -> Result<Bytes, EncodingError> {
    with_codec(|codec| {
        codec
            .decompressor
            .reset(ResetDirective::SessionOnly)
            .map_err(zstd_error)?;
        codec.decode_scratch.clear();
        let bound = validated_decompression_bound(bytes)?;
        codec.decode_scratch.reserve(bound);
        codec
            .decompressor
            .decompress(&mut codec.decode_scratch, bytes)
            .map_err(zstd_error)?;
        Ok(Bytes::copy_from_slice(&codec.decode_scratch))
    })
}

/// Returns Zstd's output bound after it passes an independent block ceiling.
///
/// Each block has a three-byte header and expands to at most 128 KiB. Thus,
/// compressed input cannot produce more than one maximum block per three
/// bytes. This ceiling rejects a corrupt declared size before allocation.
fn validated_decompression_bound(bytes: &[u8]) -> Result<usize, EncodingError> {
    let bound = zstd_safe::decompress_bound(bytes).map_err(zstd_error)?;
    validate_decompression_bound(bytes.len(), bound)
}

pub(super) fn validate_decompression_bound(
    compressed_len: usize,
    bound: u64,
) -> Result<usize, EncodingError> {
    let block_count = compressed_len / ZSTD_BLOCK_HEADER_BYTES + 1;
    let hard_bound = block_count.saturating_mul(ZSTD_MAX_BLOCK_BYTES) as u64;
    if bound > hard_bound {
        return Err(EncodingError::BadZstd(io::Error::other(
            "Zstd content size exceeds the block expansion bound",
        )));
    }
    let bound = usize::try_from(bound).map_err(|_| {
        EncodingError::BadZstd(io::Error::other(
            "Zstd decompression bound exceeds this platform",
        ))
    })?;
    if bound > isize::MAX as usize {
        return Err(EncodingError::BadZstd(io::Error::other(
            "Zstd decompression bound exceeds the maximum allocation size",
        )));
    }
    Ok(bound)
}

fn with_codec<T>(
    operation: impl FnOnce(&mut Codec) -> Result<T, EncodingError>,
) -> Result<T, EncodingError> {
    CODEC.with(|slot| {
        let mut slot = slot.borrow_mut();
        let codec = match &mut *slot {
            Some(codec) => codec,
            empty @ None => empty.insert(Codec::new().map_err(EncodingError::BadZstd)?),
        };
        operation(codec)
    })
}

fn zstd_error(code: usize) -> EncodingError {
    EncodingError::BadZstd(io::Error::other(zstd_safe::get_error_name(code)))
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
