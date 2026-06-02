//! `MsgPack` payload and WAL encoding for keyed-state collections.
//!
//! Stored payload cells use [`PayloadEncoding`]; WAL blobs use
//! [`WalFormat`]. Both discriminators round-trip via `i16` so durable
//! Cassandra columns map cleanly onto them.
//!
//! WAL stream layout (logical):
//!
//! 1. Named `WalHeader` frame (private to this module).
//! 2. `header.op_count` × `K::Op` frames.
//!
//! The header is materialized at encode time from the [`WalEnvelope`]
//! being written and validated and discarded at decode time. Persisting
//! it on the envelope would create two operation-count sources that
//! could disagree.

use super::{CollectionKind, CollectionKindId, EmptyOperationsError, WalBlob, WalEnvelope};
use crate::error::{ClassifyError, ErrorCategory};
use bytes::Bytes;
use rmp_serde::Deserializer;
use rmp_serde::decode::Error as MsgPackDecodeError;
use rmp_serde::encode::{Error as MsgPackEncodeError, write_named};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::io::{self, Cursor};
use std::num::NonZeroU64;
use thiserror::Error;
use zstd::stream::{decode_all, encode_all};

const WAL_HEADER_VERSION: u16 = 1;
const ZSTD_LEVEL: i32 = 0;

/// Encoding discriminator for [`StoredPayload`](super::StoredPayload) cells.
///
/// Mapped to and from `i16` so durable storage can persist a small
/// integer column without leaking the named enum representation.
#[repr(i16)]
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub enum PayloadEncoding {
    /// Plain `MsgPack` named-record encoding.
    MsgpackV1 = 1,

    /// `MsgPack` named-record encoding compressed with zstd.
    MsgpackZstdV1 = 2,
}

impl PayloadEncoding {
    /// Recovers a payload encoding from its durable discriminator.
    ///
    /// # Errors
    ///
    /// Returns [`EncodingError::UnknownPayloadEncoding`] when `value` does
    /// not match a known variant.
    pub fn try_from_i16(value: i16) -> Result<Self, EncodingError> {
        match value {
            1 => Ok(Self::MsgpackV1),
            2 => Ok(Self::MsgpackZstdV1),
            other => Err(EncodingError::UnknownPayloadEncoding(other)),
        }
    }

    /// Returns the durable `i16` discriminator.
    #[must_use]
    pub fn as_i16(self) -> i16 {
        self as i16
    }
}

/// Encoding discriminator for WAL streams.
///
/// Mapped to and from `i16` symmetrically with [`PayloadEncoding`].
#[repr(i16)]
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub enum WalFormat {
    /// Plain `MsgPack` header + op frames stream.
    MsgpackStreamV1 = 1,

    /// `MsgPack` header + op frames stream compressed with zstd.
    MsgpackStreamZstdV1 = 2,
}

impl WalFormat {
    /// Recovers a WAL format from its durable discriminator.
    ///
    /// # Errors
    ///
    /// Returns [`EncodingError::UnknownWalFormat`] when `value` does not
    /// match a known variant.
    pub fn try_from_i16(value: i16) -> Result<Self, EncodingError> {
        match value {
            1 => Ok(Self::MsgpackStreamV1),
            2 => Ok(Self::MsgpackStreamZstdV1),
            other => Err(EncodingError::UnknownWalFormat(other)),
        }
    }

    /// Returns the durable `i16` discriminator.
    #[must_use]
    pub fn as_i16(self) -> i16 {
        self as i16
    }

    fn is_compressed(self) -> bool {
        matches!(self, Self::MsgpackStreamZstdV1)
    }
}

/// Capability bound for [`CollectionKind::Op`] when encoding is required.
///
/// Kept separate from [`CollectionKind`]'s base bounds so kinds that
/// never participate in encoding stay unaffected.
pub trait EncodableOp: Serialize + DeserializeOwned + Send + Sync + 'static {}

impl<T> EncodableOp for T where T: Serialize + DeserializeOwned + Send + Sync + 'static {}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct WalHeader {
    version: u16,
    kind: CollectionKindId,
    op_count: NonZeroU64,
}

/// Encodes a stored payload with the requested encoding.
///
/// # Errors
///
/// Returns [`EncodingError`] when `MsgPack` serialization or zstd
/// compression fails.
pub fn encode_payload<P>(payload: &P, encoding: PayloadEncoding) -> Result<Bytes, EncodingError>
where
    P: Serialize,
{
    let raw = rmp_serde::to_vec_named(payload).map_err(EncodingError::BadMsgPackEncode)?;
    match encoding {
        PayloadEncoding::MsgpackV1 => Ok(Bytes::from(raw)),
        PayloadEncoding::MsgpackZstdV1 => compress(&raw),
    }
}

/// Decodes stored payload bytes encoded with `encoding`.
///
/// # Errors
///
/// Returns [`EncodingError`] when `MsgPack` deserialization or zstd
/// decompression fails.
pub fn decode_payload<P>(bytes: &[u8], encoding: PayloadEncoding) -> Result<P, EncodingError>
where
    P: DeserializeOwned,
{
    match encoding {
        PayloadEncoding::MsgpackV1 => {
            rmp_serde::from_slice(bytes).map_err(EncodingError::BadMsgPack)
        }
        PayloadEncoding::MsgpackZstdV1 => {
            let raw = decompress(bytes)?;
            rmp_serde::from_slice(&raw).map_err(EncodingError::BadMsgPack)
        }
    }
}

/// Encodes a typed WAL envelope as a tagged byte blob.
///
/// # Errors
///
/// Returns [`EncodingError`] when `MsgPack` serialization or zstd
/// compression fails.
pub fn encode_wal<K>(
    envelope: &WalEnvelope<K>,
    format: WalFormat,
) -> Result<WalBlob<K>, EncodingError>
where
    K: CollectionKind,
    K::Op: EncodableOp,
{
    let header = WalHeader {
        version: WAL_HEADER_VERSION,
        kind: K::ID,
        op_count: envelope.operation_count(),
    };

    let mut buf: Vec<u8> = Vec::new();
    write_named(&mut buf, &header).map_err(EncodingError::BadMsgPackEncode)?;
    for op in envelope.ops().iter() {
        write_named(&mut buf, op).map_err(EncodingError::BadMsgPackEncode)?;
    }

    let bytes = if format.is_compressed() {
        compress(&buf)?
    } else {
        Bytes::from(buf)
    };

    Ok(WalBlob::<K>::new(bytes, format))
}

/// Test-only: builds an uncompressed WAL blob whose header declares
/// `op_count` ops, followed by `tail` raw bytes (which may be empty,
/// truncated, or garbage). Lets decoder property tests drive the
/// untrusted-`op_count` preallocation path directly — `encode_wal` can only
/// produce headers whose count matches the real op frames.
///
/// # Errors
///
/// Returns [`EncodingError`] when the header frame fails to serialize.
#[cfg(test)]
pub(crate) fn raw_wal_blob_for_test<K>(
    op_count: NonZeroU64,
    tail: &[u8],
) -> Result<WalBlob<K>, EncodingError>
where
    K: CollectionKind,
{
    let header = WalHeader {
        version: WAL_HEADER_VERSION,
        kind: K::ID,
        op_count,
    };
    let mut buf: Vec<u8> = Vec::new();
    write_named(&mut buf, &header).map_err(EncodingError::BadMsgPackEncode)?;
    buf.extend_from_slice(tail);
    Ok(WalBlob::<K>::new(
        Bytes::from(buf),
        WalFormat::MsgpackStreamV1,
    ))
}

/// Decodes a typed WAL blob into a materialized envelope.
///
/// # Errors
///
/// Returns [`EncodingError`] when the header is malformed, the encoded
/// kind disagrees with `K::ID`, `MsgPack` fails, the stream ends early,
/// or any trailing bytes remain after the last op frame.
pub fn decode_wal<K>(blob: &WalBlob<K>) -> Result<WalEnvelope<K>, EncodingError>
where
    K: CollectionKind,
    K::Op: EncodableOp,
{
    if blob.format().is_compressed() {
        let raw = decompress(blob.bytes().as_ref())?;
        decode_wal_stream::<K>(&raw)
    } else {
        decode_wal_stream::<K>(blob.bytes().as_ref())
    }
}

fn decode_wal_stream<K>(raw: &[u8]) -> Result<WalEnvelope<K>, EncodingError>
where
    K: CollectionKind,
    K::Op: EncodableOp,
{
    let raw_len = u64::try_from(raw.len()).map_err(|_| EncodingError::CorruptWal)?;
    let mut cursor = Cursor::new(raw);
    let header: WalHeader = read_frame(&mut cursor)?;
    if header.version != WAL_HEADER_VERSION {
        return Err(EncodingError::UnsupportedWalHeaderVersion {
            header: header.version,
            expected: WAL_HEADER_VERSION,
        });
    }
    if header.kind != K::ID {
        return Err(EncodingError::KindMismatch {
            header: header.kind,
            expected: K::ID,
        });
    }

    let count = header.op_count.get();
    // `op_count` is attacker-controlled (it comes straight off the wire) and
    // is not validated by the header decode. Each op frame is at least one
    // byte, so a `count` larger than the bytes remaining after the header is
    // provably corrupt — reject it before it can drive an unbounded
    // `Vec::with_capacity` (capacity-overflow panic or OOM `abort`).
    let remaining = raw_len.saturating_sub(cursor.position());
    if count > remaining {
        return Err(EncodingError::CorruptWal);
    }
    // `remaining` bounds `count`, which bounds the allocation; the `try_from`
    // cannot truncate because `count <= remaining <= raw.len() <= usize::MAX`.
    let mut ops: Vec<K::Op> = Vec::with_capacity(usize::try_from(count).unwrap_or(usize::MAX));
    for _ in 0..count {
        let op: K::Op = read_frame(&mut cursor)?;
        ops.push(op);
    }

    if cursor.position() != raw_len {
        return Err(EncodingError::TrailingBytes);
    }

    WalEnvelope::<K>::try_from_ops(ops).map_err(EncodingError::from)
}

fn read_frame<T>(cursor: &mut Cursor<&[u8]>) -> Result<T, EncodingError>
where
    T: DeserializeOwned,
{
    let mut deserializer = Deserializer::new(&mut *cursor);
    T::deserialize(&mut deserializer).map_err(EncodingError::BadMsgPack)
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
    UnknownPayloadEncoding(i16),

    /// The durable column carried an unknown WAL format.
    #[error("unknown WAL format: {0}")]
    UnknownWalFormat(i16),

    /// The decoded WAL header named a different collection kind.
    #[error("WAL kind mismatch: header {header:?}, expected {expected:?}")]
    KindMismatch {
        /// Kind named by the encoded WAL header.
        header: CollectionKindId,

        /// Kind expected by the decoder.
        expected: CollectionKindId,
    },

    /// A non-empty WAL was required but the encoder received zero operations.
    #[error(transparent)]
    EmptyWalEnvelope(#[from] EmptyOperationsError),

    /// The decoded WAL header named a header version this build does not
    /// understand.
    #[error("unsupported WAL header version: header {header}, expected {expected}")]
    UnsupportedWalHeaderVersion {
        /// Header version observed on the wire.
        header: u16,

        /// Header version expected by the decoder.
        expected: u16,
    },

    /// `MsgPack` decoding failed.
    #[error("bad MsgPack decode: {0}")]
    BadMsgPack(#[source] MsgPackDecodeError),

    /// `MsgPack` encoding failed.
    #[error("bad MsgPack encode: {0}")]
    BadMsgPackEncode(#[source] MsgPackEncodeError),

    /// zstd compression or decompression failed.
    #[error("bad zstd: {0}")]
    BadZstd(#[source] io::Error),

    /// Unexpected trailing bytes were present after the WAL stream.
    #[error("trailing bytes after WAL stream")]
    TrailingBytes,

    /// Internal WAL accounting was inconsistent.
    #[error("corrupt WAL")]
    CorruptWal,
}

impl ClassifyError for EncodingError {
    fn classify_error(&self) -> ErrorCategory {
        ErrorCategory::Permanent
    }
}
