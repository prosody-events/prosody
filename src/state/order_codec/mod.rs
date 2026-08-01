//! Order-preserving key codecs.
//!
//! The load-bearing contract: **clustering byte-order == logical key order**.
//! A codec maps a logical key to a [`Coordinate`] whose unsigned lexicographic
//! (memcmp) byte order matches the key's [`Ord`], so a forward clustering scan
//! visits cells in ascending logical order without sorting in code. This is a
//! *tested* contract (the per-codec monotonicity property), not a compiler
//! proof: a non-monotone codec silently misorders scans.

use crate::codec::Codec;
use crate::error::{ClassifyError, ErrorCategory};
use crate::state::cell_key::Coordinate;
use std::str::{Utf8Error, from_utf8};
use thiserror::Error;

/// Maps a logical key to an order-preserving [`Coordinate`] and back.
///
/// Every key codec is also a [`Codec`] over the same type — the supertrait
/// equalities pin `Payload = Key` — under the **byte-identity law**:
/// `serialize` writes exactly `encode`'s bytes and `deserialize` is `decode`.
/// A key can therefore ride as a cell *payload* with no adapter, and
/// [`Codec::FORMAT_ID`] is the one
/// durable token a key encoding freezes into a collection's identity.
///
/// The invariants every impl must satisfy (enforced by the per-codec
/// monotonicity and byte-identity property tests, not the type system):
/// - **Order preservation:** `a.cmp(b) ==
///   encode(a).as_bytes().cmp(encode(b).as_bytes())`.
/// - **Key round-trip:** `decode(encode(k).as_bytes()) == Ok(k)`.
/// - **Byte round-trip:** for any `b` a codec itself produced,
///   `encode(&decode(b)?).as_bytes() == b`. This is what makes a typed scan
///   durable-compatible: decoding a stored coordinate and re-encoding it
///   reproduces the exact stored bytes.
/// - **Byte identity:** `serialize(k)` appends exactly `encode(k).as_bytes()`.
///   Held by construction when `serialize`/`deserialize` delegate to
///   `encode`/`decode`, as every impl here does.
pub trait OrderedKeyCodec: Codec<Payload = Self::Key, Error = KeyCodecError> {
    /// The logical key type, ordered to match its encoded byte order.
    ///
    /// `Send + Sync + 'static`, not merely `Ord`: a typed scan yields the
    /// decoded key in a `Send` stream (so it must be `Send`), and a key can
    /// ride as a [`Codec`] payload (so it must be
    /// `Sync + 'static`). Every real key (`String`, `i64`, `u64`, `()`)
    /// already satisfies it.
    type Key: Ord + Send + Sync + 'static;

    /// Encodes a key to its order-preserving bytes.
    fn encode(key: &Self::Key) -> Coordinate;

    /// Decodes order-preserving bytes back to the logical key.
    ///
    /// # Errors
    ///
    /// Returns [`KeyCodecError`] when the bytes are not a valid encoding (wrong
    /// length, invalid UTF-8).
    fn decode(bytes: &[u8]) -> Result<Self::Key, KeyCodecError>;
}

/// The unit address: the single cell of a one-cell collection, at the empty
/// coordinate.
///
/// Its logical key is `()`, so a single-cell kind (Value, and any meta cell a
/// keyed kind pins to a fixed address) is addressed the same way a keyed kind
/// is — through a scoped operation's typed cell surface — without a key of its
/// own. The empty coordinate is byte-identical to a Value cell's historical
/// fixed address, so adopting it changes no durable bytes. Like every key axis,
/// its [`FORMAT_ID`](Codec::FORMAT_ID) rides a single-cell collection's
/// identity as the key-codec token.
#[derive(Clone, Copy, Debug, Default)]
pub struct UnitKey;

impl OrderedKeyCodec for UnitKey {
    type Key = ();

    fn encode((): &Self::Key) -> Coordinate {
        Coordinate::empty()
    }

    fn decode(bytes: &[u8]) -> Result<Self::Key, KeyCodecError> {
        if bytes.is_empty() {
            Ok(())
        } else {
            Err(KeyCodecError::BadLength {
                expected: 0,
                actual: bytes.len(),
            })
        }
    }
}

/// The payload half of `UnitKey` — delegates to `encode`/`decode`, so the
/// byte-identity law on [`OrderedKeyCodec`] holds by construction.
impl Codec for UnitKey {
    type Error = KeyCodecError;
    type Payload = ();

    const FORMAT_ID: &'static str = "unit.v1";

    fn deserialize(&mut self, buf: &mut [u8]) -> Result<Self::Payload, KeyCodecError> {
        Self::decode(buf)
    }

    fn serialize(
        &mut self,
        payload: Self::Payload,
        buf: &mut Vec<u8>,
    ) -> Result<(), KeyCodecError> {
        buf.extend_from_slice(Self::encode(&payload).as_bytes());
        Ok(())
    }
}

/// Order-preserving big-endian encoding of a signed `i64`.
///
/// Flipping the sign bit maps `i64::MIN..=i64::MAX` onto `u64::MIN..=u64::MAX`,
/// so the big-endian bytes compare by memcmp in signed order. Inverse:
/// [`order_preserving_i64_decode`]. This is the Deque index encoding.
#[must_use]
pub fn order_preserving_i64(value: i64) -> [u8; 8] {
    ((value as u64) ^ (1 << 63)).to_be_bytes()
}

/// Inverse of [`order_preserving_i64`].
#[must_use]
pub fn order_preserving_i64_decode(bytes: [u8; 8]) -> i64 {
    (u64::from_be_bytes(bytes) ^ (1 << 63)) as i64
}

/// `String` keys encoded as their raw UTF-8 bytes (UTF-8 byte order == `str`
/// `Ord`).
#[derive(Clone, Copy, Debug, Default)]
pub struct Utf8KeyCodec;

impl OrderedKeyCodec for Utf8KeyCodec {
    type Key = String;

    fn encode(key: &Self::Key) -> Coordinate {
        Coordinate::from_bytes(key.clone().into_bytes())
    }

    fn decode(bytes: &[u8]) -> Result<Self::Key, KeyCodecError> {
        Ok(from_utf8(bytes)?.to_owned())
    }
}

/// The payload half of `Utf8KeyCodec` — delegates to `encode`/`decode`, so the
/// byte-identity law on [`OrderedKeyCodec`] holds by construction.
impl Codec for Utf8KeyCodec {
    type Error = KeyCodecError;
    type Payload = String;

    const FORMAT_ID: &'static str = "utf8.v1";

    fn deserialize(&mut self, buf: &mut [u8]) -> Result<Self::Payload, KeyCodecError> {
        Self::decode(buf)
    }

    fn serialize(
        &mut self,
        payload: Self::Payload,
        buf: &mut Vec<u8>,
    ) -> Result<(), KeyCodecError> {
        buf.extend_from_slice(Self::encode(&payload).as_bytes());
        Ok(())
    }
}

/// `i64` keys encoded via the sign-flipped big-endian [`order_preserving_i64`].
#[derive(Clone, Copy, Debug, Default)]
pub struct I64KeyCodec;

impl OrderedKeyCodec for I64KeyCodec {
    type Key = i64;

    fn encode(key: &Self::Key) -> Coordinate {
        Coordinate::from_bytes(order_preserving_i64(*key).to_vec())
    }

    fn decode(bytes: &[u8]) -> Result<Self::Key, KeyCodecError> {
        Ok(order_preserving_i64_decode(fixed_width_8(bytes)?))
    }
}

/// The payload half of `I64KeyCodec` — delegates to `encode`/`decode`, so the
/// byte-identity law on [`OrderedKeyCodec`] holds by construction.
impl Codec for I64KeyCodec {
    type Error = KeyCodecError;
    type Payload = i64;

    const FORMAT_ID: &'static str = "i64.v1";

    fn deserialize(&mut self, buf: &mut [u8]) -> Result<Self::Payload, KeyCodecError> {
        Self::decode(buf)
    }

    fn serialize(
        &mut self,
        payload: Self::Payload,
        buf: &mut Vec<u8>,
    ) -> Result<(), KeyCodecError> {
        buf.extend_from_slice(Self::encode(&payload).as_bytes());
        Ok(())
    }
}

/// `u64` keys encoded as big-endian bytes (unsigned order == memcmp order).
#[derive(Clone, Copy, Debug, Default)]
pub struct U64KeyCodec;

impl OrderedKeyCodec for U64KeyCodec {
    type Key = u64;

    fn encode(key: &Self::Key) -> Coordinate {
        Coordinate::from_bytes(key.to_be_bytes().to_vec())
    }

    fn decode(bytes: &[u8]) -> Result<Self::Key, KeyCodecError> {
        Ok(u64::from_be_bytes(fixed_width_8(bytes)?))
    }
}

/// The payload half of `U64KeyCodec` — delegates to `encode`/`decode`, so the
/// byte-identity law on [`OrderedKeyCodec`] holds by construction.
impl Codec for U64KeyCodec {
    type Error = KeyCodecError;
    type Payload = u64;

    const FORMAT_ID: &'static str = "u64.v1";

    fn deserialize(&mut self, buf: &mut [u8]) -> Result<Self::Payload, KeyCodecError> {
        Self::decode(buf)
    }

    fn serialize(
        &mut self,
        payload: Self::Payload,
        buf: &mut Vec<u8>,
    ) -> Result<(), KeyCodecError> {
        buf.extend_from_slice(Self::encode(&payload).as_bytes());
        Ok(())
    }
}

/// Validates `bytes` is exactly 8 bytes wide, as required by the fixed-width
/// codecs ([`I64KeyCodec`], [`U64KeyCodec`]).
fn fixed_width_8(bytes: &[u8]) -> Result<[u8; 8], KeyCodecError> {
    <[u8; 8]>::try_from(bytes).map_err(|_| KeyCodecError::BadLength {
        expected: 8,
        actual: bytes.len(),
    })
}

/// Error decoding order-preserving key bytes.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum KeyCodecError {
    /// The byte slice was not the codec's fixed key width.
    #[error("bad key length: expected {expected}, got {actual}")]
    BadLength {
        /// The width the codec requires.
        expected: usize,
        /// The width the slice actually had.
        actual: usize,
    },

    /// A correctly-sized key held a value outside the codec's domain.
    #[error("bad key discriminant: {actual}")]
    BadDiscriminant {
        /// The unrecognized discriminant byte.
        actual: u8,
    },

    /// A UTF-8 key was not valid UTF-8.
    #[error("invalid utf-8 key: {0}")]
    InvalidUtf8(#[from] Utf8Error),
}

impl ClassifyError for KeyCodecError {
    fn classify_error(&self) -> ErrorCategory {
        ErrorCategory::Permanent
    }
}

#[cfg(test)]
mod tests;
