//! Wire-format abstraction for encoding and decoding message payloads.

use std::error::Error;

mod binary;
mod json;
mod serialize_buf;

pub use binary::{
    BinaryCodec, BinaryCodecError, BinaryExtractor, BinaryMetadata, BinaryPayload, JsonBinaryCodec,
    JsonExtractError, JsonExtractor,
};
pub use json::{JsonCodec, JsonCodecError, serialize_to_json};

// Crate-internal: not part of the public codec API surface.
pub(crate) use serialize_buf::SerializeBufGuard;

/// Wire-format abstraction for encoding and decoding message payloads.
///
/// Implement this trait to plug in a custom serialization format. The codec
/// is stateful to allow implementations to reuse internal buffers across calls.
pub trait Codec: Default + Send + Sync + 'static {
    /// Stable open-dispatch token persisted in keyed-state identity rows.
    /// Never change it once cells exist: the durable per-segment identity
    /// records the token, and a deploy whose codec asserts a different one
    /// refuses to dispatch (Permanent).
    const CODEC_ID: &'static str;

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

    /// Appends the serialized payload to `buf`, consuming the payload.
    /// Callers are responsible for clearing `buf` first if a fresh buffer is
    /// required; codecs that own a wire-format byte buffer (e.g.
    /// [`BinaryCodec`]) may move it into `buf` directly when `buf` is empty,
    /// avoiding a copy.
    ///
    /// # Errors
    ///
    /// Returns an error if `payload` cannot be encoded.
    fn serialize(&mut self, payload: Self::Payload, buf: &mut Vec<u8>) -> Result<(), Self::Error>;

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

/// Maps a consumer payload type to the [`Codec`] that (de)serializes its
/// keyed-state cells, for the type-erased FFI seam.
///
/// Keyed state uses the consumer's codec, so a value cell's payload type *is*
/// the message payload type. The erased `DynEventContext` value ops carry no
/// codec type parameter, so they recover the codec from the payload through
/// this map and feed it to `value_state::<C>(name)`.
///
/// # Invariant: recovered codec matches the registration
///
/// The recovered codec's [`Codec::CODEC_ID`] rides the bound collection's
/// `structural_identity()`, which `verify_state_registration` checks at
/// access. Every reachable erased-seam user is FFI — `JsonCodec` (host
/// objects) or [`JsonBinaryCodec`] (raw bytes) — so the recovered codec is
/// *exact*. A consumer whose real codec differs (only reachable through the
/// Rust-native typed path, which never uses the erased seam) self-rejects with
/// a Permanent identity mismatch rather than misreading a cell. This map is
/// the inference; `verify_state_registration` is the enforcement.
///
/// The trait is core-local. Both known FFI payloads implement it here; a
/// future binding with a custom payload implements it for its own local type
/// (orphan-safe in either direction).
pub trait ErasedStateCodec: Send + Sync + 'static {
    /// The codec whose [`Codec::Payload`] is `Self`.
    type Codec: Codec<Payload = Self>;
}

impl ErasedStateCodec for serde_json::Value {
    type Codec = JsonCodec;
}

impl ErasedStateCodec for BinaryPayload {
    type Codec = JsonBinaryCodec;
}

#[cfg(test)]
mod tests;
