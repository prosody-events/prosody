//! Wire-format abstraction for encoding and decoding message payloads.

use std::error::Error;

mod binary;
mod json;

pub use binary::{
    BinaryCodec, BinaryCodecError, BinaryExtractor, BinaryMetadata, BinaryPayload, JsonBinaryCodec,
    JsonExtractError, JsonExtractor,
};
pub use json::{JsonCodec, JsonCodecError, serialize_to_json};

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
    /// The buffer is passed mutably so implementations may parse in place
    /// (for example, `simd_json` rewrites the input as a tape during
    /// parsing). After this call returns, the bytes in `buf` are
    /// unspecified — callers must not read them again.
    ///
    /// # Errors
    ///
    /// Returns an error if the bytes cannot be decoded into `Self::Payload`.
    fn deserialize(&mut self, buf: &mut [u8]) -> Result<Self::Payload, Self::Error>;

    /// Appends the serialized payload to `buf`. Callers are responsible for
    /// clearing `buf` first if a fresh buffer is required.
    ///
    /// # Errors
    ///
    /// Returns an error if `payload` cannot be encoded.
    fn serialize(&mut self, payload: &Self::Payload, buf: &mut Vec<u8>) -> Result<(), Self::Error>;

    /// Runs `f` with a thread-local cached instance of this codec.
    ///
    /// The first call on each thread constructs the codec via `Default`;
    /// subsequent calls reuse the same instance, allowing internal buffers
    /// (such as `simd_json::Buffers`) to be reused across calls.
    ///
    /// Implementors should back this with a `thread_local!` of the concrete
    /// codec type so dispatch stays static.
    ///
    /// Reentrant calls on the same thread are not supported; implementations
    /// backed by `RefCell::with_borrow_mut` will panic if `f` recurses into
    /// `with_cached_local` for the same codec.
    fn with_cached_local<R>(f: impl FnOnce(&mut Self) -> R) -> R;
}
