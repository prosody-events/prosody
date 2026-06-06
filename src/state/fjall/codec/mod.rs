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
//! - The collection hash is `xxh3_128(segment_id || 0x00 || key || 0x00 ||
//!   state_type || 0x00 || name)`, serialized **big-endian** for stable
//!   cross-platform ordering.
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
use crate::state::value::ValueKind;
use crate::state::{CollectionId, CollectionKind, EventScopeId, Read};
use bytes::Bytes;
use xxhash_rust::xxh3::xxh3_128;

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
    let state_type_byte = u8::from_le_bytes(i8::from(id.state_type()).to_le_bytes());
    let name_bytes = id.name().as_str().as_bytes();

    let mut buf = Vec::with_capacity(segment_bytes.len() + key_bytes.len() + name_bytes.len() + 4);
    buf.extend_from_slice(segment_bytes);
    buf.push(0);
    buf.extend_from_slice(key_bytes);
    buf.push(0);
    buf.push(state_type_byte);
    buf.push(0);
    buf.extend_from_slice(name_bytes);

    xxh3_128(&buf).to_be_bytes()
}

/// Returns the cache key for a Value collection.
#[must_use]
pub fn value_cache_key(id: &CollectionId<ValueKind>) -> [u8; 16] {
    collection_prefix(id)
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
/// The key folds the event scope into the cache's existing collection hash so
/// two concurrent events on the same Kafka partition cannot collide in the
/// shared dirty workspace.
///
/// The dirty workspace is a single compacted overlay cell per collection —
/// the same tagged-cell shape as the committed cache (`0x01` = pending Set,
/// `0x00` = pending Clear, key-absent = no pending op). It carries the LWW
/// final op directly, so there is no separate ops log or sequence counter.
#[must_use]
pub fn dirty_collection_key<K>(scope: EventScopeId, id: &CollectionId<K>) -> [u8; 16]
where
    K: CollectionKind,
{
    let collection = collection_prefix(id);
    let mut buf = [0_u8; 32];
    buf[..16].copy_from_slice(&scope.get().to_be_bytes());
    buf[16..].copy_from_slice(&collection);
    xxh3_128(&buf).to_be_bytes()
}

#[cfg(test)]
mod tests;
