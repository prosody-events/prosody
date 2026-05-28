//! Cache key + cell codec for the fjall Value store.
//!
//! Two requirements drive the cache key shape:
//!
//! 1. **Point reads (Value).** Cheap, well-defined lookups by full collection
//!    identifier.
//! 2. **Prefix scans (Map/Deque, later slices).** "All entries for one
//!    collection" must be a contiguous range; range queries within a collection
//!    must preserve user ordering.
//!
//! The hierarchy is `[16-byte collection hash][inner key bytes]`:
//!
//! - The collection hash is `xxh3_128(segment_id || 0x00 || key || 0x00 ||
//!   state_type || 0x00 || name)`, serialized **big-endian** for stable
//!   cross-platform ordering.
//! - For **Value**, the inner key is empty.
//! - For **Map** (future slice), the inner key is the user's `EncodedMapKey`
//!   (order-preserving by trait contract).
//! - For **Deque** (future slice), the inner key is the index as 8 big-endian
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
use crate::state::value::{StoredPayload, ValueKind};
use crate::state::{CollectionId, CollectionKind, EventScopeId, Read};
use bytes::Bytes;
use xxhash_rust::xxh3::xxh3_128;

/// Tag byte for "known absent" entries.
const CACHE_TAG_ABSENT: u8 = 0x00;

/// Tag byte for "known present" entries.
const CACHE_TAG_PRESENT: u8 = 0x01;

/// Payload encoding used for cached `Present` cells.
const CACHE_PAYLOAD_ENCODING: PayloadEncoding = PayloadEncoding::MsgpackZstdV1;

/// Payload encoding used for dirty op values. Plain `MsgPack` — ops are
/// small and ephemeral; compressing them adds CPU for no real saving.
pub(super) const DIRTY_OP_ENCODING: PayloadEncoding = PayloadEncoding::MsgpackV1;

/// Returns the 16-byte collection prefix for a typed collection identity.
///
/// See module docs for the field layout and rationale.
#[must_use]
pub fn collection_prefix<K>(id: &CollectionId<K>) -> [u8; 16]
where
    K: CollectionKind,
{
    let segment_bytes = id.state_key().segment_id.as_bytes();
    let key_bytes = id.state_key().key.as_bytes();
    let state_type_byte = id.state_type().as_i8() as u8;
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

/// Encodes a `Present` cache cell from a stored payload.
///
/// # Errors
///
/// Returns [`FjallValueStoreError::Encoding`] when payload encoding fails.
pub fn encode_present_cell(payload: &StoredPayload) -> Result<Bytes, FjallValueStoreError> {
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
///   MsgPack+zstd payload tail.
/// - `Err(_)` for any malformed cell (empty, unknown tag, empty Present
///   payload, codec failure).
///
/// # Errors
///
/// Returns a [`FjallValueStoreError`] when the cell is malformed.
pub fn decode_cell(bytes: Option<&[u8]>) -> Result<Read<StoredPayload>, FjallValueStoreError> {
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
            let payload: StoredPayload = decode_payload(rest, CACHE_PAYLOAD_ENCODING)?;
            Ok(Read::Present(payload))
        }
        other => Err(FjallValueStoreError::UnknownCacheTag(other)),
    }
}

/// Returns the 16-byte scope-qualified prefix for a dirty workspace key.
///
/// The prefix folds the event scope into the cache's existing collection
/// hash so two concurrent events on the same Kafka partition cannot collide
/// in the shared dirty workspace.
#[must_use]
pub fn scope_collection_prefix<K>(scope: EventScopeId, id: &CollectionId<K>) -> [u8; 16]
where
    K: CollectionKind,
{
    let collection = collection_prefix(id);
    let mut buf = [0_u8; 32];
    buf[..16].copy_from_slice(&scope.get().to_be_bytes());
    buf[16..].copy_from_slice(&collection);
    xxh3_128(&buf).to_be_bytes()
}

/// Returns the `dirty_ops` partition key for `(scope, collection, seq)`.
///
/// `seq` is encoded big-endian so the ordered scan over the prefix yields
/// ops in insertion order.
#[must_use]
pub fn dirty_ops_key<K>(scope: EventScopeId, id: &CollectionId<K>, seq: u64) -> [u8; 24]
where
    K: CollectionKind,
{
    let prefix = scope_collection_prefix(scope, id);
    let mut buf = [0_u8; 24];
    buf[..16].copy_from_slice(&prefix);
    buf[16..].copy_from_slice(&seq.to_be_bytes());
    buf
}

/// Returns the `dirty_overlay` / `dirty_meta` partition key for
/// `(scope, collection)`.
#[must_use]
pub fn dirty_collection_key<K>(scope: EventScopeId, id: &CollectionId<K>) -> [u8; 16]
where
    K: CollectionKind,
{
    scope_collection_prefix(scope, id)
}

/// Encodes the `dirty_meta` value as `[next_seq u64 LE][op_count u64 LE]`.
#[must_use]
pub fn encode_dirty_meta(next_seq: u64, op_count: u64) -> [u8; 16] {
    let mut buf = [0_u8; 16];
    buf[..8].copy_from_slice(&next_seq.to_le_bytes());
    buf[8..].copy_from_slice(&op_count.to_le_bytes());
    buf
}

/// Decodes a `dirty_meta` value into `(next_seq, op_count)`.
///
/// # Errors
///
/// Returns [`FjallValueStoreError::CorruptDirtyMeta`] when the cell does
/// not have exactly 16 bytes.
pub fn decode_dirty_meta(bytes: &[u8]) -> Result<(u64, u64), FjallValueStoreError> {
    let arr: [u8; 16] = bytes
        .try_into()
        .map_err(|_| FjallValueStoreError::CorruptDirtyMeta(bytes.len()))?;
    let mut a = [0_u8; 8];
    let mut b = [0_u8; 8];
    a.copy_from_slice(&arr[..8]);
    b.copy_from_slice(&arr[8..]);
    Ok((u64::from_le_bytes(a), u64::from_le_bytes(b)))
}

#[cfg(test)]
mod tests;
