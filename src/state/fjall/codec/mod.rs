//! Cache key + cell codec for the fjall Value store.
//!
//! Two requirements drive the cache key shape:
//!
//! 1. **Point reads (Value).** Cheap, well-defined lookups by full collection
//!    identifier.
//! 2. **Prefix scans (Map/Deque, future work).** "All entries for one
//!    collection" must be a contiguous range; range queries within a collection
//!    must preserve user ordering.
//!
//! The hierarchy is `[16-byte collection hash][inner key bytes]`:
//!
//! - The collection hash is `xxh3_128` over an **injective** encoding of the
//!   collection identity: the fixed-width fields first (`segment_id` then the
//!   one-byte `state_type`), then each variable-length field length-prefixed
//!   (`key_len` as 8 big-endian bytes, then `key`; `name_len`, then `name`).
//!   The hash is serialized **big-endian** for stable cross-platform ordering.
//!   Length-prefixing (rather than a delimiter byte) keeps the encoding
//!   injective even when `key` or `name` contain the delimiter — Kafka keys are
//!   arbitrary bytes — so distinct collections cannot share an input buffer and
//!   the only residual collision risk is the hash's own ≈ 2⁻⁶⁴.
//! - For **Value**, the inner key is empty.
//! - For **Map** (future work), the inner key is the user's `EncodedMapKey`
//!   (order-preserving by trait contract).
//! - For **Deque** (future work), the inner key is the index as 8 big-endian
//!   bytes.
//!
//! Collision probability for `xxh3_128` is ≈ 2⁻⁶⁴ (birthday bound) — well
//! below practical concern for non-adversarial caches. Collisions resolve
//! via miss-then-populate cycles; the read path does not verify
//! collisions.
//!
//! "Invalidate all entries for collection X" = prefix scan + delete on
//! `hash(X)`. "Drop all cache state on partition revocation" = drop the
//! fjall keyspace.

use super::error::FjallValueStoreError;
use crate::state::encoding::{PayloadEncoding, decode_payload, encode_payload};
use crate::state::{CollectionId, CollectionKind, EventScopeId, Read};
use bytes::Bytes;
use xxhash_rust::xxh3::Xxh3;

/// Tag byte for "known absent" entries.
const CACHE_TAG_ABSENT: u8 = 0x00;

/// Tag byte for "known present" entries.
const CACHE_TAG_PRESENT: u8 = 0x01;

/// Payload encoding used for cached `Present` cells.
const CACHE_PAYLOAD_ENCODING: PayloadEncoding = PayloadEncoding::RawZstdV1;

/// Returns the 16-byte collection prefix for a typed collection identity.
///
/// See module docs for the field layout and rationale.
#[must_use]
pub(super) fn collection_prefix<K>(id: &CollectionId<K>) -> [u8; 16]
where
    K: CollectionKind,
{
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

/// Encodes an `Absent` cache cell.
#[must_use]
pub fn encode_absent_cell() -> Bytes {
    Bytes::from_static(&[CACHE_TAG_ABSENT])
}

/// Encodes a `Present` cache cell from raw payload bytes.
///
/// # Errors
///
/// Returns [`FjallValueStoreError::Encoding`] when payload encoding fails.
pub fn encode_present_cell(payload: &Bytes) -> Result<Bytes, FjallValueStoreError> {
    let payload_bytes = encode_payload(payload, CACHE_PAYLOAD_ENCODING)?;
    let mut buf = Vec::with_capacity(1 + payload_bytes.len());
    buf.push(CACHE_TAG_PRESENT);
    buf.extend_from_slice(payload_bytes.as_ref());
    Ok(Bytes::from(buf))
}

/// Decodes a cache cell into a three-valued read.
///
/// Returns:
/// - `Ok(Read::Unknown)` only when the cache had no entry (caller signals this
///   by passing `None` here).
/// - `Ok(Read::Absent)` for a `0x00`-tagged cell.
/// - `Ok(Read::Present(payload))` for a `0x01`-tagged cell with a non-empty
///   zstd-compressed payload tail.
/// - `Err(_)` for any malformed cell (empty, unknown tag, empty Present
///   payload, codec failure).
///
/// # Errors
///
/// Returns a [`FjallValueStoreError`] when the cell is malformed.
pub fn decode_cell(bytes: Option<&[u8]>) -> Result<Read<Bytes>, FjallValueStoreError> {
    let Some(bytes) = bytes else {
        return Ok(Read::Unknown);
    };
    let (tag, rest) = bytes
        .split_first()
        .ok_or(FjallValueStoreError::EmptyCacheCell)?;
    match *tag {
        CACHE_TAG_ABSENT => Ok(Read::Absent),
        CACHE_TAG_PRESENT => {
            if rest.is_empty() {
                return Err(FjallValueStoreError::EmptyPresentPayload);
            }
            let payload = decode_payload(rest, CACHE_PAYLOAD_ENCODING)?;
            Ok(Read::Present(payload))
        }
        other => Err(FjallValueStoreError::UnknownCacheTag(other)),
    }
}

/// Returns the `dirty_overlay` partition key for `(scope, collection)`.
///
/// The key is `[scope_be_16][collection_prefix_16]`: the 16-byte big-endian
/// event scope followed verbatim by the collection's [`collection_prefix`].
/// Prefixing with the scope keeps two concurrent events on the same Kafka
/// partition from colliding in the shared dirty workspace, while preserving
/// the collection hash as a stable, scannable suffix — the per-element-keying
/// structure the Map/Deque overlays will build on. No second hash is needed:
/// the scope is already fixed-width, so concatenation is injective.
///
/// The dirty workspace is a single compacted overlay cell per collection —
/// the same tagged-cell shape as the committed cache (`0x01` = pending Set,
/// `0x00` = pending Clear, key-absent = no pending op). It carries the LWW
/// final op directly, so there is no separate ops log or sequence counter.
#[must_use]
pub fn dirty_collection_key<K>(scope: EventScopeId, id: &CollectionId<K>) -> [u8; 32]
where
    K: CollectionKind,
{
    let mut buf = [0_u8; 32];
    buf[..16].copy_from_slice(&scope.get().to_be_bytes());
    buf[16..].copy_from_slice(&collection_prefix(id));
    buf
}

#[cfg(test)]
mod tests;
