//! Keys and frames for the fjall cell cache.
//!
//! A key contains a 16-byte collection hash, one section byte, and the
//! coordinate bytes. The section and coordinate preserve order within a
//! collection.
//!
//! The collection hash uses `xxh3_128` over the collection identity.
//! The input starts with `segment_id` and the one-byte `state_type`.
//! Each variable field follows its length: `key_len`, `key`, `name_len`, then
//! `name`. Lengths use eight big-endian bytes. The hash also uses big-endian
//! bytes. Length prefixes prevent distinct identities from sharing the same
//! hash input. The cache does not detect hash collisions.
//!
//! # Cell frames
//!
//! A frame contains `[tag][expiry_millis: u64 BE][payload]`.
//! The tags encode [`CacheEntry`]: `0x00` means absent, `0x01` carries a value,
//! and `0x02` means present without a payload. Zero expiry means no expiry.
//! [`decode_frame`] borrows the payload and returns the expiry. The caller
//! applies its projection and checks the expiry. [`frame_expiry`] reads the
//! expiry without a projection.
//!
//! The assignment owns these frames. Its workspace removes them at revocation.

use super::error::FjallCellCacheError;
use crate::state::CollectionId;
use crate::state::cell::CacheEntry;
use crate::state::cell_key::{CellRef, Section};
use bytes::Bytes;
use smallvec::SmallVec;
use xxhash_rust::xxh3::Xxh3;

/// Length of the collection hash prefix that leads every fjall key (cell and
/// index alike).
const COLLECTION_PREFIX_LEN: usize = 16;

/// Tag byte for "known absent" entries.
const CACHE_TAG_ABSENT: u8 = 0x00;

/// Tag byte for a value with its payload.
const CACHE_TAG_VALUE: u8 = 0x01;

/// Tag byte for presence without a payload.
const CACHE_TAG_EXISTS: u8 = 0x02;

/// Width of the absolute-expiry header (`u64` big-endian millis) carried after
/// the tag byte by every cell frame. `0` means "never expires".
const EXPIRY_LEN: usize = 8;

/// An expiry value meaning "never expires" — stamped for a `None`-TTL
/// collection.
pub(super) const NEVER_EXPIRES: u64 = 0;

/// Length of the fixed key prefix shared by every cell of one collection
/// section: the 16-byte collection hash plus the 1-byte section discriminant.
/// A range scan over `[section_prefix, …]` stays within one section of one
/// collection; the order-preserving coordinate bytes follow.
pub(super) const SECTION_PREFIX_LEN: usize = COLLECTION_PREFIX_LEN + 1;

type DecodedFrame<'a> = (u64, Option<CacheEntry<&'a [u8]>>);

/// Returns the full fjall key for one cell: the 16-byte collection prefix
/// followed by the cell's `section` byte and order-preserving `coordinate`
/// bytes. The prefix groups a collection's cells contiguously; the section +
/// coordinate suffix orders them, so a Map/Deque prefix range is a contiguous
/// fjall range that preserves user order.
///
/// Built per point read and per point write — the dominant steady-state path —
/// so the key rides a [`SmallVec`] inline buffer: Value (17 B), Deque (25 B),
/// and short-key Map entries stay on the stack; only a long Map key spills to
/// the heap (its coordinate is genuinely unbounded).
#[must_use]
pub(super) fn cell_key(id: &CollectionId, cell: CellRef<'_>) -> SmallVec<[u8; 32]> {
    let prefix = collection_prefix(id);
    let coordinate = cell.coordinate;
    let mut key = SmallVec::with_capacity(prefix.len() + 1 + coordinate.len());
    key.extend_from_slice(&prefix);
    key.push(i8::from(cell.section).cast_unsigned());
    key.extend_from_slice(coordinate);
    key
}

/// Returns the [`SECTION_PREFIX_LEN`]-byte prefix shared by every cell of one
/// `(collection, section)`: the 16-byte collection hash followed by the
/// section's `i8` discriminant. Range scans build their byte bounds by
/// appending coordinate bytes to this prefix.
#[must_use]
pub(super) fn section_prefix(id: &CollectionId, section: Section) -> [u8; SECTION_PREFIX_LEN] {
    let mut prefix = [0; SECTION_PREFIX_LEN];
    prefix[..COLLECTION_PREFIX_LEN].copy_from_slice(&collection_prefix(id));
    prefix[COLLECTION_PREFIX_LEN] = i8::from(section).cast_unsigned();
    prefix
}

/// Returns the 16-byte collection prefix for a collection identity.
///
/// See module docs for the field layout and rationale.
#[must_use]
pub(super) fn collection_prefix(id: &CollectionId) -> [u8; COLLECTION_PREFIX_LEN] {
    let segment_bytes = id.state_key().segment_id.as_bytes();
    let key_bytes = id.state_key().key.as_bytes();
    let state_type_byte = i8::from(id.state_type()).cast_unsigned();
    let name_bytes = id.name().as_str().as_bytes();

    // Injective layout: fixed-width fields first, then each variable-length
    // field length-prefixed. A delimiter byte would not be injective — a key
    // or name containing it could shift the field boundary (Kafka keys are
    // arbitrary bytes) — so two distinct collections could share a buffer.
    //
    // Streamed through `Xxh3` (seed 0, identical to `xxh3_128`) so no transient
    // buffer is allocated. The byte sequence fed here is load-bearing: it is the
    // durable cache key, so the field order and the big-endian `u64` length
    // prefixes must stay byte-for-byte what the buffer build produced. Never
    // substitute `write_u64`/`write_u32` — those are native-endian.
    let mut hasher = Xxh3::new();
    hasher.update(segment_bytes);
    hasher.update(&[state_type_byte]);
    hasher.update(&(key_bytes.len() as u64).to_be_bytes());
    hasher.update(key_bytes);
    hasher.update(&(name_bytes.len() as u64).to_be_bytes());
    hasher.update(name_bytes);

    hasher.digest128().to_be_bytes()
}

/// Encodes a cache entry with its absolute expiry. Zero means no expiry.
#[must_use]
pub(super) fn encode_frame(entry: CacheEntry<&[u8]>, expiry: u64) -> Bytes {
    let (tag, payload) = match entry {
        CacheEntry::Absent => (CACHE_TAG_ABSENT, &[][..]),
        CacheEntry::Exists => (CACHE_TAG_EXISTS, &[][..]),
        CacheEntry::Value(payload) => (CACHE_TAG_VALUE, payload),
    };
    let mut buf = Vec::with_capacity(1 + EXPIRY_LEN + payload.len());
    buf.push(tag);
    buf.extend_from_slice(&expiry.to_be_bytes());
    buf.extend_from_slice(payload);
    Bytes::from(buf)
}

/// Decodes a frame without a payload copy. A missing entry returns `None`.
pub(super) fn decode_frame(bytes: Option<&[u8]>) -> Result<DecodedFrame<'_>, FjallCellCacheError> {
    let Some(bytes) = bytes else {
        return Ok((NEVER_EXPIRES, None));
    };
    let (tag, rest) = bytes
        .split_first()
        .ok_or(FjallCellCacheError::EmptyCacheCell)?;
    // The expiry header follows the tag in every frame.
    let expiry_bytes: [u8; EXPIRY_LEN] = rest
        .get(..EXPIRY_LEN)
        .ok_or(FjallCellCacheError::EmptyCacheCell)?
        .try_into()
        .map_err(|_| FjallCellCacheError::EmptyCacheCell)?;
    let expiry = u64::from_be_bytes(expiry_bytes);
    let payload = &rest[EXPIRY_LEN..];
    match *tag {
        CACHE_TAG_ABSENT => Ok((expiry, Some(CacheEntry::Absent))),
        // An empty payload tail is valid: a `Set` of empty bytes frames as
        // `[0x01][expiry]`, so do NOT re-add an "empty tail ⇒ corrupt" guard.
        CACHE_TAG_VALUE => Ok((expiry, Some(CacheEntry::Value(payload)))),
        CACHE_TAG_EXISTS => Ok((expiry, Some(CacheEntry::Exists))),
        other => Err(FjallCellCacheError::UnknownCacheTag(other)),
    }
}

/// Returns the expiry of any frame. A missing entry has no expiry.
pub(super) fn frame_expiry(bytes: Option<&[u8]>) -> Result<Option<u64>, FjallCellCacheError> {
    let (expiry, entry) = decode_frame(bytes)?;
    Ok(entry.map(|_| expiry))
}

#[cfg(test)]
mod tests;
