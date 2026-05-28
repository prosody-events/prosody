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
