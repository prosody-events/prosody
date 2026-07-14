//! Wire-format abstraction for encoding and decoding message payloads.

use std::error::Error;

mod binary;
mod fixed;
mod json;
mod serialize_buf;

pub use binary::{
    BinaryCodec, BinaryCodecError, BinaryExtractor, BinaryFormat, BinaryMetadata, BinaryPayload,
    JsonBinaryCodec, JsonExtractError, JsonExtractor, JsonFormat, JsonPassthroughStateCodec,
    NoopExtractor,
};
pub use fixed::{FixedCodec, I64Codec, I64CodecError, PairCodecError};
pub use json::{JsonCodec, JsonCodecError, serialize_to_json};

// Crate-internal: not part of the public codec API surface.
pub(crate) use serialize_buf::SerializeBufGuard;

/// Wire-format abstraction for encoding and decoding message payloads.
///
/// Implement this trait to plug in a custom serialization format. The codec
/// is stateful to allow implementations to reuse internal buffers across calls.
pub trait Codec: Default + Send + Sync + 'static {
    /// Stable token naming the durable byte format this codec speaks,
    /// persisted in keyed-state identity rows. Three laws govern it:
    ///
    /// - **Stability:** never change it once cells exist. The group-global
    ///   descriptor-identity table freezes the token, and a deploy whose codec
    ///   asserts a different one refuses to dispatch (Permanent).
    /// - **Compatibility:** equal tokens promise mutually decodable bytes — any
    ///   conforming codec asserting this token must decode bytes written by any
    ///   other. Key codecs ([`OrderedKeyCodec`](crate::state::OrderedKeyCodec))
    ///   tighten this to byte-identical encoding.
    /// - **Completeness:** the token fully describes what stored bytes mean. A
    ///   [`CellResolver`](crate::state::descriptor::CellResolver) must never
    ///   add meaning the format doesn't imply — storage that denotes references
    ///   or pointers gets its own codec and token.
    ///
    /// Name the format, not the implementation: a token like `"json"` lets a
    /// differently-implemented reader of the same format validate against the
    /// frozen identity. [`BinaryCodec`] copies bytes verbatim, so it cannot
    /// name a format itself — its [`BinaryFormat`] parameter makes the user
    /// declare one at definition.
    const FORMAT_ID: &'static str;

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

    /// Runs `f` with an instance of this codec.
    ///
    /// The default constructs a fresh codec via `Default` per call — all a
    /// user codec needs is `FORMAT_ID`, `Payload`, `Error`, and the two
    /// serialize/deserialize methods. Override it to reuse internal buffers
    /// (such as `simd_json::Buffers`) across calls by backing it with a
    /// `thread_local!` of the concrete codec type so dispatch stays static;
    /// [`JsonCodec`] does exactly that.
    ///
    /// Reentrant calls on the same thread are not supported for a
    /// `thread_local!`-backed override: an implementation using
    /// `RefCell::with_borrow_mut` panics if `f` recurses into
    /// `with_cached_local` for the same codec.
    fn with_cached_local<R>(f: impl FnOnce(&mut Self) -> R) -> R {
        f(&mut Self::default())
    }
}

/// Maps a consumer payload type to the [`Codec`] for its keyed-state cells,
/// for the type-erased FFI seam.
///
/// The erased `DynEventContext` value ops carry no codec type parameter, so
/// they recover one from the payload through this map. The reachable
/// payloads are the FFI codecs' — `serde_json::Value` ([`JsonCodec`], for
/// the js/py/rb bindings) and [`BinaryPayload`] ([`JsonPassthroughStateCodec`],
/// for the C# binding, which hands Rust raw JSON bytes it never parses).
///
/// # Invariant: the recovered codec must match the registration
///
/// The recovered codec's [`Codec::FORMAT_ID`] rides the collection's
/// `structural_identity`, which `verify_state_registration` checks at access:
/// this map only *infers* a codec, that check *enforces* it. A consumer whose
/// real codec differs self-rejects with a Permanent identity mismatch rather
/// than misreading a cell. Every payload here maps to a `"json"`-format codec,
/// so a value collection registered by any of the four clients is
/// identity-compatible across all of them in one group.
pub trait ErasedStateCodec: Send + Sync + 'static {
    /// The codec whose [`Codec::Payload`] is `Self`.
    type Codec: Codec<Payload = Self>;

    /// Whether this payload is the JSON `null` "absent" sentinel — the value
    /// the erased seam rejects on `set`/`push` (`clear`/`remove` express
    /// deletion instead). `null` is not a storable cell value: a cell either
    /// holds a value or is absent, and JSON `null` is the erased seam's way of
    /// naming absent.
    fn is_absent_sentinel(&self) -> bool;
}

impl ErasedStateCodec for serde_json::Value {
    type Codec = JsonCodec;

    fn is_absent_sentinel(&self) -> bool {
        matches!(self, serde_json::Value::Null)
    }
}

impl ErasedStateCodec for BinaryPayload {
    type Codec = JsonPassthroughStateCodec;

    fn is_absent_sentinel(&self) -> bool {
        // No parse (the passthrough codec never parses): the seam only needs to
        // recognize the literal `null` document, ASCII-whitespace-trimmed.
        self.bytes.trim_ascii() == b"null"
    }
}

#[cfg(test)]
mod tests;
