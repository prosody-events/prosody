//! Order-preserving key codecs.
//!
//! Encoded byte order must match logical key order. Collection scans rely on
//! this contract to return ordered keys without a separate sort.

use crate::codec::Codec;
use crate::error::{ClassifyError, ErrorCategory};
use crate::state::cell_key::Coordinate;
use bytes::{Bytes, BytesMut};
use std::borrow::Borrow;
use std::str::{Utf8Error, from_utf8};
use thiserror::Error;

/// Encodes borrowed keys and decodes owned keys in logical key order.
///
/// `Key` is the owned form and `Borrowed` is the input form. UTF-8 keys use
/// `String` and `str`.
/// [`Borrow`] requires both forms to have the same ordering.
///
/// Each implementation must satisfy these invariants:
/// - Encoded byte order equals logical key order.
/// - `decode(encode(key.borrow()).as_bytes())` returns the original owned key.
/// - Decoding and re-encoding a coordinate preserves its bytes.
/// - [`Codec`] serializes a key to the same bytes as `encode`.
///
/// [`Codec::FORMAT_ID`] identifies these bytes in the durable collection
/// identity.
pub trait OrderedKeyCodec: Codec<Payload = Self::Key, Error = KeyCodecError> {
    /// The owned key returned by decoders and streams.
    type Key: Borrow<Self::Borrowed> + Ord + Send + Sync + 'static;

    /// The key view accepted by point operations and query bounds.
    type Borrowed: Ord + Sync + ?Sized + 'static;

    /// Encodes a key to its order-preserving bytes.
    fn encode(key: &Self::Borrowed) -> Coordinate;

    /// Returns the exact byte length written by `serialize_key`.
    fn encoded_len(key: &Self::Borrowed) -> usize;

    /// Appends a borrowed key to reusable encoding storage.
    /// The output must match both `encode` and `Codec::serialize_ref`.
    /// Append exactly `encoded_len(key)` bytes. Reuse the supplied storage.
    ///
    /// # Errors
    ///
    /// Returns an error when the key cannot be encoded.
    fn serialize_key(
        &mut self,
        key: &Self::Borrowed,
        buf: &mut Vec<u8>,
    ) -> Result<(), KeyCodecError>;

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
/// Its logical key is `()`. A scoped operation's typed cell surface therefore
/// addresses a single-cell kind the same way it addresses a keyed kind. The
/// single-cell kind needs no key of its own. Value is one such kind. So is any
/// meta cell that a keyed kind pins to a fixed address.
///
/// The empty coordinate is byte-identical to a Value cell's historical fixed
/// address, so this address changes no durable bytes. Like every key axis, its
/// [`FORMAT_ID`](Codec::FORMAT_ID) rides a single-cell collection's identity as
/// the key-codec token.
#[derive(Clone, Copy, Debug, Default)]
pub struct UnitKey;

impl OrderedKeyCodec for UnitKey {
    type Borrowed = ();
    type Key = ();

    fn encode((): &Self::Borrowed) -> Coordinate {
        Coordinate::empty()
    }

    fn encoded_len(_key: &Self::Borrowed) -> usize {
        0
    }

    fn serialize_key(
        &mut self,
        key: &Self::Borrowed,
        buf: &mut Vec<u8>,
    ) -> Result<(), KeyCodecError> {
        self.serialize_ref(key, buf)
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
/// Every input form writes or checks zero bytes.
impl Codec for UnitKey {
    type Error = KeyCodecError;
    type Payload = ();

    const FORMAT_ID: &'static str = "unit.v1";

    fn deserialize(&mut self, buf: &mut [u8]) -> Result<Self::Payload, KeyCodecError> {
        Self::decode(buf)
    }

    fn deserialize_bytes(&mut self, buf: Bytes) -> Result<Self::Payload, KeyCodecError> {
        Self::decode(&buf)
    }

    fn serialize_ref(
        &mut self,
        payload: &Self::Payload,
        buf: &mut Vec<u8>,
    ) -> Result<(), KeyCodecError> {
        buf.extend_from_slice(Self::encode(payload).as_bytes());
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
    type Borrowed = str;
    type Key = String;

    fn encode(key: &Self::Borrowed) -> Coordinate {
        Coordinate::from_bytes(key.as_bytes().to_vec())
    }

    fn encoded_len(key: &Self::Borrowed) -> usize {
        key.len()
    }

    fn serialize_key(
        &mut self,
        key: &Self::Borrowed,
        buf: &mut Vec<u8>,
    ) -> Result<(), KeyCodecError> {
        buf.extend_from_slice(key.as_bytes());
        Ok(())
    }

    fn decode(bytes: &[u8]) -> Result<Self::Key, KeyCodecError> {
        Ok(from_utf8(bytes)?.to_owned())
    }
}

/// The payload half of `Utf8KeyCodec` — delegates to `encode`/`decode`, so the
/// byte-identity law on [`OrderedKeyCodec`] holds by construction.
/// Owned operations move the string allocation. Borrowed operations copy bytes.
impl Codec for Utf8KeyCodec {
    type Error = KeyCodecError;
    type Payload = String;

    const FORMAT_ID: &'static str = "utf8.v1";

    fn deserialize(&mut self, buf: &mut [u8]) -> Result<Self::Payload, KeyCodecError> {
        Self::decode(buf)
    }

    fn deserialize_owned(&mut self, buf: BytesMut) -> Result<Self::Payload, KeyCodecError> {
        // Ownership moves the UTF-8 allocation directly into the key string.
        String::from_utf8(buf.into())
            .map_err(|error| KeyCodecError::InvalidUtf8(error.utf8_error()))
    }

    fn serialize(
        &mut self,
        payload: Self::Payload,
        buf: &mut Vec<u8>,
    ) -> Result<(), KeyCodecError> {
        // Ownership moves the string allocation into the output when it is empty.
        if buf.is_empty() {
            *buf = payload.into_bytes();
        } else {
            buf.extend_from_slice(payload.as_bytes());
        }
        Ok(())
    }

    fn serialize_ref(
        &mut self,
        payload: &Self::Payload,
        buf: &mut Vec<u8>,
    ) -> Result<(), KeyCodecError> {
        buf.extend_from_slice(payload.as_bytes());
        Ok(())
    }
}

/// `i64` keys encoded via the sign-flipped big-endian [`order_preserving_i64`].
#[derive(Clone, Copy, Debug, Default)]
pub struct I64KeyCodec;

impl OrderedKeyCodec for I64KeyCodec {
    type Borrowed = i64;
    type Key = i64;

    fn encode(key: &Self::Borrowed) -> Coordinate {
        Coordinate::from_bytes(order_preserving_i64(*key).to_vec())
    }

    fn encoded_len(_key: &Self::Borrowed) -> usize {
        8
    }

    fn serialize_key(
        &mut self,
        key: &Self::Borrowed,
        buf: &mut Vec<u8>,
    ) -> Result<(), KeyCodecError> {
        self.serialize_ref(key, buf)
    }

    fn decode(bytes: &[u8]) -> Result<Self::Key, KeyCodecError> {
        Ok(order_preserving_i64_decode(fixed_width_8(bytes)?))
    }
}

/// The payload half of `I64KeyCodec` — delegates to `encode`/`decode`, so the
/// byte-identity law on [`OrderedKeyCodec`] holds by construction.
/// Each serialization writes one stack array. Decoding does not allocate.
impl Codec for I64KeyCodec {
    type Error = KeyCodecError;
    type Payload = i64;

    const FORMAT_ID: &'static str = "i64.v1";

    fn deserialize(&mut self, buf: &mut [u8]) -> Result<Self::Payload, KeyCodecError> {
        Self::decode(buf)
    }

    fn deserialize_bytes(&mut self, buf: Bytes) -> Result<Self::Payload, KeyCodecError> {
        Self::decode(&buf)
    }

    fn serialize_ref(
        &mut self,
        payload: &Self::Payload,
        buf: &mut Vec<u8>,
    ) -> Result<(), KeyCodecError> {
        buf.extend_from_slice(&order_preserving_i64(*payload));
        Ok(())
    }
}

/// `u64` keys encoded as big-endian bytes (unsigned order == memcmp order).
#[derive(Clone, Copy, Debug, Default)]
pub struct U64KeyCodec;

impl OrderedKeyCodec for U64KeyCodec {
    type Borrowed = u64;
    type Key = u64;

    fn encode(key: &Self::Borrowed) -> Coordinate {
        Coordinate::from_bytes(key.to_be_bytes().to_vec())
    }

    fn encoded_len(_key: &Self::Borrowed) -> usize {
        8
    }

    fn serialize_key(
        &mut self,
        key: &Self::Borrowed,
        buf: &mut Vec<u8>,
    ) -> Result<(), KeyCodecError> {
        self.serialize_ref(key, buf)
    }

    fn decode(bytes: &[u8]) -> Result<Self::Key, KeyCodecError> {
        Ok(u64::from_be_bytes(fixed_width_8(bytes)?))
    }
}

/// The payload half of `U64KeyCodec` — delegates to `encode`/`decode`, so the
/// byte-identity law on [`OrderedKeyCodec`] holds by construction.
/// Each serialization writes one stack array. Decoding does not allocate.
impl Codec for U64KeyCodec {
    type Error = KeyCodecError;
    type Payload = u64;

    const FORMAT_ID: &'static str = "u64.v1";

    fn deserialize(&mut self, buf: &mut [u8]) -> Result<Self::Payload, KeyCodecError> {
        Self::decode(buf)
    }

    fn deserialize_bytes(&mut self, buf: Bytes) -> Result<Self::Payload, KeyCodecError> {
        Self::decode(&buf)
    }

    fn serialize_ref(
        &mut self,
        payload: &Self::Payload,
        buf: &mut Vec<u8>,
    ) -> Result<(), KeyCodecError> {
        buf.extend_from_slice(&payload.to_be_bytes());
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

/// Error encoding or decoding order-preserving key bytes.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum KeyCodecError {
    /// The caller's encoding buffer cannot hold the query bounds.
    #[error("query encoding requires {required} bytes; buffer capacity is {available}")]
    InsufficientCapacity {
        /// The bytes needed to encode both bounds.
        required: usize,
        /// The capacity supplied by the caller.
        available: usize,
    },

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
