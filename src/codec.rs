//! Wire-format abstraction for encoding and decoding message payloads.

#[cfg(not(target_arch = "arm"))]
use simd_json::serde::from_slice_with_buffers;
use std::error::Error;

/// Wire-format abstraction for encoding and decoding message payloads.
///
/// Implement this trait to plug in a custom serialization format. The codec
/// is stateful to allow implementations to reuse internal buffers across calls.
pub trait Codec: Default + Send + Sync + 'static {
    /// The deserialized payload type produced and consumed by this codec.
    type Payload: Send + Sync + 'static;

    /// The error type returned when encoding or decoding fails.
    type Error: Error + Send + Sync + 'static;

    /// Deserializes a payload from raw bytes.
    ///
    /// # Errors
    ///
    /// Returns an error if the bytes cannot be decoded into `Self::Payload`.
    fn deserialize(&mut self, bytes: &[u8]) -> Result<Self::Payload, Self::Error>;

    /// Serializes a payload into the provided buffer, replacing its contents.
    ///
    /// # Errors
    ///
    /// Returns an error if `payload` cannot be encoded.
    fn serialize(&mut self, payload: &Self::Payload, buf: &mut Vec<u8>) -> Result<(), Self::Error>;
}

/// JSON codec using `serde_json` (ARM) or `simd_json` (non-ARM).
#[derive(Default)]
pub struct JsonCodec {
    #[cfg(not(target_arch = "arm"))]
    buffers: simd_json::Buffers,
}

impl Codec for JsonCodec {
    type Error = JsonCodecError;
    type Payload = serde_json::Value;

    fn deserialize(&mut self, bytes: &[u8]) -> Result<Self::Payload, Self::Error> {
        #[cfg(target_arch = "arm")]
        {
            serde_json::from_slice(bytes).map_err(JsonCodecError::Serde)
        }
        #[cfg(not(target_arch = "arm"))]
        {
            let mut owned = bytes.to_owned();
            from_slice_with_buffers(&mut owned, &mut self.buffers).map_err(JsonCodecError::Simd)
        }
    }

    fn serialize(&mut self, payload: &Self::Payload, buf: &mut Vec<u8>) -> Result<(), Self::Error> {
        *buf = serde_json::to_vec(payload).map_err(JsonCodecError::Serde)?;
        Ok(())
    }
}

/// Errors produced by [`JsonCodec`].
#[derive(Debug, thiserror::Error)]
pub enum JsonCodecError {
    /// Serialization or deserialization failed via `serde_json`.
    #[error("serde_json error: {0}")]
    Serde(#[from] serde_json::Error),

    /// Deserialization failed via `simd_json` (non-ARM only).
    #[cfg(not(target_arch = "arm"))]
    #[error("simd_json error: {0}")]
    Simd(simd_json::Error),
}
