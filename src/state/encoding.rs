//! Payload-cell encoding for keyed-state collections.
//!
//! Value cells hold raw codec bytes and select an on-disk representation via
//! [`Encoding`], which round-trips through `i16` so the durable
//! Cassandra column maps cleanly onto it.

use crate::error::{ClassifyError, ErrorCategory};
use bytes::Bytes;
use std::io;
use thiserror::Error;
use zstd::stream::{decode_all, encode_all};

const ZSTD_LEVEL: i32 = 0;

/// Encoding discriminator for Value payload cells.
///
/// Mapped to and from `i16` so durable storage can persist a small
/// integer column without leaking the named enum representation.
/// Discriminants `1`/`2` belonged to the retired `MsgPack`-wrapped cell
/// encodings and are never reused — a stale cell carrying one fails
/// loudly as [`EncodingError::UnknownEncoding`] (Permanent).
#[repr(i16)]
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum Encoding {
    /// Raw codec bytes stored verbatim.
    RawV1 = 3,

    /// Raw codec bytes compressed with zstd.
    RawZstdV1 = 4,
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
            3 => Ok(Self::RawV1),
            4 => Ok(Self::RawZstdV1),
            _ => Err(EncodingError::UnknownEncoding(value)),
        }
    }
}

/// Encodes a raw payload cell with the requested encoding.
///
/// The cell bytes are opaque to this layer — whatever codec produced them
/// (JSON, the Kafka-ref `MsgPack`) lives above the store.
///
/// # Errors
///
/// Returns [`EncodingError`] when zstd compression fails.
pub fn encode_payload(payload: &Bytes, encoding: Encoding) -> Result<Bytes, EncodingError> {
    match encoding {
        Encoding::RawV1 => Ok(payload.clone()),
        Encoding::RawZstdV1 => compress(payload),
    }
}

/// Decodes payload-cell bytes encoded with `encoding`.
///
/// # Errors
///
/// Returns [`EncodingError`] when zstd decompression fails.
pub fn decode_payload(bytes: &[u8], encoding: Encoding) -> Result<Bytes, EncodingError> {
    match encoding {
        Encoding::RawV1 => Ok(Bytes::copy_from_slice(bytes)),
        Encoding::RawZstdV1 => decompress(bytes).map(Bytes::from),
    }
}

/// Compresses `raw` at [`ZSTD_LEVEL`], the single source of both the level
/// and the [`EncodingError::BadZstd`] mapping for every encode path.
fn compress(raw: &[u8]) -> Result<Bytes, EncodingError> {
    encode_all(raw, ZSTD_LEVEL)
        .map(Bytes::from)
        .map_err(EncodingError::BadZstd)
}

/// Decompresses zstd `bytes`, the single source of the
/// [`EncodingError::BadZstd`] mapping for every decode path.
fn decompress(bytes: &[u8]) -> Result<Vec<u8>, EncodingError> {
    decode_all(bytes).map_err(EncodingError::BadZstd)
}

/// Error returned by the encoding module.
#[derive(Debug, Error)]
pub enum EncodingError {
    /// The durable column carried an unknown payload encoding.
    #[error("unknown payload encoding: {0}")]
    UnknownEncoding(i16),

    /// zstd compression or decompression failed.
    #[error("bad zstd: {0}")]
    BadZstd(#[source] io::Error),
}

impl ClassifyError for EncodingError {
    fn classify_error(&self) -> ErrorCategory {
        ErrorCategory::Permanent
    }
}
