use super::{
    collection_prefix, decode_cell, dirty_collection_key, encode_absent_cell, encode_present_cell,
};
use crate::Key;
use crate::state::{CollectionId, EventScopeId, Read, StateKey, StateName, StateType, ValueKind};
use bytes::Bytes;
use color_eyre::eyre::Result;
use quickcheck::{QuickCheck, TestResult};
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

#[test]
fn absent_round_trip() -> Result<()> {
    let cell = encode_absent_cell();
    assert_eq!(decode_cell(Some(cell.as_ref()))?, Read::Absent);
    Ok(())
}

/// Any payload round-trips through `encode_present_cell` → `decode_cell` as
/// `Read::Present` with identical bytes — the cache codec is lossless over the
/// whole byte space, including the empty payload a `Set` of empty bytes
/// produces, not just one fixed example.
#[test]
fn present_round_trip() {
    fn prop(payload: Vec<u8>) -> TestResult {
        let payload = Bytes::from(payload);
        let Ok(cell) = encode_present_cell(&payload) else {
            return TestResult::failed();
        };
        match decode_cell(Some(cell.as_ref())) {
            Ok(Read::Present(decoded)) => TestResult::from_bool(decoded == payload),
            _ => TestResult::failed(),
        }
    }

    QuickCheck::new().quickcheck(prop as fn(Vec<u8>) -> TestResult);
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
    assert_eq!(collection_prefix(&id), collection_prefix(&id));
    Ok(())
}

#[test]
fn distinct_collections_get_distinct_prefixes() -> Result<()> {
    let a = collection("profile-a")?;
    let b = collection("profile-b")?;
    assert_ne!(collection_prefix(&a), collection_prefix(&b));
    Ok(())
}

#[test]
fn dirty_key_carries_collection_prefix_as_suffix() -> Result<()> {
    let id = collection("profile")?;
    let key = dirty_collection_key(EventScopeId::new(0xDEAD_BEEF), &id);
    assert_eq!(&key[16..], &collection_prefix(&id));
    Ok(())
}

/// Distinct scopes on the same collection produce distinct dirty keys — the
/// per-event isolation that keeps two concurrent handlers on one Kafka
/// partition from colliding in the shared overlay.
#[test]
fn distinct_scopes_get_distinct_dirty_keys() {
    fn prop(scope_a: u128, scope_b: u128) -> TestResult {
        if scope_a == scope_b {
            return TestResult::discard();
        }
        let Ok(id) = collection("profile") else {
            return TestResult::failed();
        };
        let key_a = dirty_collection_key(EventScopeId::new(scope_a), &id);
        let key_b = dirty_collection_key(EventScopeId::new(scope_b), &id);
        TestResult::from_bool(key_a != key_b)
    }
    QuickCheck::new().quickcheck(prop as fn(u128, u128) -> TestResult);
}

/// The scope prefix and collection suffix are independent: the trailing 16
/// bytes track the collection regardless of scope, and the leading 16 bytes
/// track the scope regardless of collection.
#[test]
fn dirty_key_splits_into_scope_and_collection() -> Result<()> {
    let id_a = collection("profile-a")?;
    let id_b = collection("profile-b")?;
    let scope = EventScopeId::new(0x0102_0304);

    let key_a = dirty_collection_key(scope, &id_a);
    let key_b = dirty_collection_key(scope, &id_b);

    assert_eq!(&key_a[..16], &key_b[..16], "same scope ⇒ same prefix");
    assert_ne!(
        &key_a[16..],
        &key_b[16..],
        "distinct collections ⇒ distinct suffix"
    );
    assert_eq!(&key_a[..16], &scope.get().to_be_bytes());
    Ok(())
}
