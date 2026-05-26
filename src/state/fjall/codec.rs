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
use crate::state::{CollectionId, CollectionKind, Read, StateType};
use bytes::Bytes;
use xxhash_rust::xxh3::xxh3_128;

/// Tag byte for "known absent" entries.
const CACHE_TAG_ABSENT: u8 = 0x00;

/// Tag byte for "known present" entries.
const CACHE_TAG_PRESENT: u8 = 0x01;

/// Payload encoding used for cached `Present` cells.
const CACHE_PAYLOAD_ENCODING: PayloadEncoding = PayloadEncoding::MsgpackZstdV1;

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
    let state_type_byte = state_type_to_u8(id.state_type());
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

fn state_type_to_u8(state_type: StateType) -> u8 {
    match state_type {
        StateType::Application => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::{decode_cell, encode_absent_cell, encode_present_cell, value_cache_key};
    use crate::Key;
    use crate::state::value::StoredPayload;
    use crate::state::{CollectionId, Read, StateKey, StateName, StateType, ValueKind};
    use bytes::Bytes;
    use color_eyre::eyre::Result;
    use std::sync::Arc;
    use uuid::Uuid;

    fn key(value: &str) -> Key {
        Arc::from(value)
    }

    fn collection(name: &str) -> Result<CollectionId<ValueKind>> {
        Ok(CollectionId::new(
            StateKey::new(Uuid::from_u128(0xA1B2_C3D4), key("user-1")),
            StateType::Application,
            StateName::try_new(name)?,
        ))
    }

    fn inline(value: u8) -> StoredPayload {
        StoredPayload::Inline(Bytes::from(vec![value]))
    }

    #[test]
    fn absent_round_trip() -> Result<()> {
        let bytes = encode_absent_cell();
        assert_eq!(decode_cell(Some(bytes.as_ref()))?, Read::Absent);
        Ok(())
    }

    #[test]
    fn present_round_trip() -> Result<()> {
        use color_eyre::eyre::eyre;
        let payload = inline(7);
        let bytes = encode_present_cell(&payload)?;
        let Read::Present(decoded) = decode_cell(Some(bytes.as_ref()))? else {
            return Err(eyre!("expected Present"));
        };
        assert_eq!(decoded, payload);
        Ok(())
    }

    #[test]
    fn missing_entry_decodes_as_unknown() -> Result<()> {
        assert_eq!(decode_cell(None)?, Read::Unknown);
        Ok(())
    }

    #[test]
    fn empty_cell_is_rejected() {
        assert!(decode_cell(Some(&[])).is_err());
    }

    #[test]
    fn unknown_tag_byte_is_rejected() {
        let result = decode_cell(Some(&[0xFE]));
        assert!(
            matches!(
                result,
                Err(super::FjallValueStoreError::UnknownCacheTag(0xFE))
            ),
            "expected UnknownCacheTag, got {result:?}"
        );
    }

    #[test]
    fn present_with_zero_length_payload_is_rejected() {
        let result = decode_cell(Some(&[0x01]));
        assert!(
            matches!(
                result,
                Err(super::FjallValueStoreError::EmptyPresentPayload)
            ),
            "expected EmptyPresentPayload, got {result:?}"
        );
    }

    #[test]
    fn collection_prefix_is_deterministic() -> Result<()> {
        let id = collection("profile")?;
        assert_eq!(value_cache_key(&id), value_cache_key(&id));
        Ok(())
    }

    #[test]
    fn distinct_collections_get_distinct_prefixes() -> Result<()> {
        let a = collection("profile-a")?;
        let b = collection("profile-b")?;
        assert_ne!(value_cache_key(&a), value_cache_key(&b));
        Ok(())
    }
}
