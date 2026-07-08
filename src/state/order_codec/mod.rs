//! Order-preserving key codecs.
//!
//! The load-bearing contract: **clustering byte-order == logical key order**.
//! A codec maps a logical key to a [`Coordinate`] whose unsigned lexicographic
//! (memcmp) byte order matches the key's [`Ord`], so a forward clustering scan
//! visits cells in ascending logical order without sorting in code. This is a
//! *tested* contract (the per-codec monotonicity property), not a compiler
//! proof: a non-monotone codec silently misorders scans.

use crate::error::{ClassifyError, ErrorCategory};
use crate::state::cell_key::Coordinate;
use std::str::{Utf8Error, from_utf8};
use thiserror::Error;

/// Maps a logical key to an order-preserving [`Coordinate`] and back.
///
/// The invariant every impl must satisfy (enforced by the per-codec
/// monotonicity property test, not the type system):
/// `a.cmp(b) == encode(a).as_bytes().cmp(encode(b).as_bytes())` and
/// `decode(encode(k).as_bytes()) == Ok(k)`.
pub trait OrderedKeyCodec {
    /// The logical key type, ordered to match its encoded byte order.
    type Key: Ord;

    /// Stable token frozen into a collection's durable identity, so a changed
    /// key encoding surfaces as an identity conflict.
    const KEY_CODEC_ID: &'static str;

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
#[derive(Clone, Copy, Debug)]
pub struct Utf8KeyCodec;

impl OrderedKeyCodec for Utf8KeyCodec {
    type Key = String;

    const KEY_CODEC_ID: &'static str = "utf8.v1";

    fn encode(key: &Self::Key) -> Coordinate {
        Coordinate::from_bytes(key.clone().into_bytes())
    }

    fn decode(bytes: &[u8]) -> Result<Self::Key, KeyCodecError> {
        Ok(from_utf8(bytes)?.to_owned())
    }
}

/// `i64` keys encoded via the sign-flipped big-endian [`order_preserving_i64`].
#[derive(Clone, Copy, Debug)]
pub struct I64KeyCodec;

impl OrderedKeyCodec for I64KeyCodec {
    type Key = i64;

    const KEY_CODEC_ID: &'static str = "i64.v1";

    fn encode(key: &Self::Key) -> Coordinate {
        Coordinate::from_bytes(order_preserving_i64(*key).to_vec())
    }

    fn decode(bytes: &[u8]) -> Result<Self::Key, KeyCodecError> {
        Ok(order_preserving_i64_decode(fixed_width_8(bytes)?))
    }
}

/// `u64` keys encoded as big-endian bytes (unsigned order == memcmp order).
#[derive(Clone, Copy, Debug)]
pub struct U64KeyCodec;

impl OrderedKeyCodec for U64KeyCodec {
    type Key = u64;

    const KEY_CODEC_ID: &'static str = "u64.v1";

    fn encode(key: &Self::Key) -> Coordinate {
        Coordinate::from_bytes(key.to_be_bytes().to_vec())
    }

    fn decode(bytes: &[u8]) -> Result<Self::Key, KeyCodecError> {
        Ok(u64::from_be_bytes(fixed_width_8(bytes)?))
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
