//! Plain-serde JSON codec for keyed-state cells.

use crate::error::{ClassifyError, ErrorCategory};
use bytes::Bytes;
use serde::Serialize;
use serde::de::DeserializeOwned;
#[cfg(not(target_arch = "arm"))]
use simd_json::serde::from_slice_with_buffers;
#[cfg(not(target_arch = "arm"))]
use std::cell::RefCell;
use std::error::Error;
use thiserror::Error;

#[cfg(test)]
mod tests;

/// Stable discriminator for the codec a keyed-state cell was written with.
///
/// Persisted as part of a collection's structural identity, so the values
/// are frozen: new codecs get new discriminants, never repurposed ones.
#[repr(i16)]
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum CodecId {
    /// No user codec — the cell holds a framework-defined encoding
    /// (e.g. a Kafka message reference).
    None = 0,

    /// Plain serde JSON.
    Json = 1,
}

impl CodecId {
    /// Wire discriminator persisted beside durable identity.
    ///
    /// Paired with [`Self::from_i16`]; the two are inverses by construction.
    #[must_use]
    pub fn as_i16(self) -> i16 {
        self as i16
    }

    /// Recovers a codec id from its wire discriminator, or `None` for an
    /// unknown value. Inverse of [`Self::as_i16`].
    #[must_use]
    pub fn from_i16(value: i16) -> Option<Self> {
        match value {
            0 => Some(Self::None),
            1 => Some(Self::Json),
            _ => None,
        }
    }
}

/// Encodes typed keyed-state cell values to raw bytes and back.
///
/// Unlike [`Codec`](super::Codec) — whose `Payload` is the consumer's parsed
/// message type and which participates in event-identity extraction — a state
/// codec is a plain `T ↔ Bytes` pair, generic over the cell type at the call
/// site. Implementations are `Copy` ZSTs so descriptors carrying them stay
/// const-constructible.
pub trait StateCodec: Copy + Send + Sync + 'static {
    /// Discriminator persisted in the collection's structural identity.
    const CODEC_ID: CodecId;

    /// Error type for encode/decode failures.
    type Error: ClassifyError + Error + Send + Sync + 'static;

    /// Encodes `value` to cell bytes.
    ///
    /// # Errors
    ///
    /// Returns an error if `value` cannot be serialized.
    fn encode<T: Serialize>(value: &T) -> Result<Bytes, Self::Error>;

    /// Decodes cell bytes into `T`. `bytes` is never mutated — cells live
    /// in shared [`Bytes`], so in-place parsers must copy first.
    ///
    /// # Errors
    ///
    /// Returns an error if the bytes cannot be decoded into `T`.
    fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, Self::Error>;
}

/// JSON state codec backed by `serde_json` (ARM) or `simd_json` (non-ARM).
#[derive(Clone, Copy, Debug, Default)]
pub struct JsonStateCodec;

impl StateCodec for JsonStateCodec {
    type Error = JsonStateCodecError;

    const CODEC_ID: CodecId = CodecId::Json;

    fn encode<T: Serialize>(value: &T) -> Result<Bytes, Self::Error> {
        #[cfg(target_arch = "arm")]
        {
            serde_json::to_vec(value)
                .map(Bytes::from)
                .map_err(JsonStateCodecError::Serde)
        }
        #[cfg(not(target_arch = "arm"))]
        {
            simd_json::to_vec(value)
                .map(Bytes::from)
                .map_err(JsonStateCodecError::Simd)
        }
    }

    fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, Self::Error> {
        #[cfg(target_arch = "arm")]
        {
            serde_json::from_slice(bytes).map_err(JsonStateCodecError::Serde)
        }
        #[cfg(not(target_arch = "arm"))]
        {
            // `simd_json` parses in place, overwriting its input with tape
            // data — but the caller's cell bytes must survive the decode.
            // Copy into a thread-local scratch buffer first.
            with_buffers(|scratch, buffers| {
                scratch.clear();
                scratch.extend_from_slice(bytes);
                from_slice_with_buffers(scratch, buffers).map_err(JsonStateCodecError::Simd)
            })
        }
    }
}

/// Runs `f` with this thread's scratch + `simd_json` parse buffers, reusing
/// both allocations across calls.
#[cfg(not(target_arch = "arm"))]
fn with_buffers<R>(f: impl FnOnce(&mut Vec<u8>, &mut simd_json::Buffers) -> R) -> R {
    thread_local! {
        static BUFFERS: RefCell<(Vec<u8>, simd_json::Buffers)> = RefCell::default();
    }
    BUFFERS.with_borrow_mut(|(scratch, buffers)| f(scratch, buffers))
}

/// Errors produced by [`JsonStateCodec`].
#[derive(Debug, Error)]
pub enum JsonStateCodecError {
    /// Serialization or deserialization failed via `serde_json` (ARM only).
    #[cfg(target_arch = "arm")]
    #[error("serde_json error: {0}")]
    Serde(#[source] serde_json::Error),

    /// Serialization or deserialization failed via `simd_json` (non-ARM only).
    #[cfg(not(target_arch = "arm"))]
    #[error("simd_json error: {0}")]
    Simd(#[source] simd_json::Error),
}

impl ClassifyError for JsonStateCodecError {
    fn classify_error(&self) -> ErrorCategory {
        ErrorCategory::Permanent
    }
}
