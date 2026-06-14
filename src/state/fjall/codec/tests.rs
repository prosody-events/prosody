use super::{
    collection_prefix, decode_cell, dirty_collection_key, encode_absent_cell, encode_present_cell,
};
use crate::Key;
use crate::state::{
    CollectionId, CollectionKind, EventScopeId, Read, StateKey, StateName, StateType, ValueKind,
};
use bytes::Bytes;
use color_eyre::eyre::Result;
use quickcheck::{Arbitrary, Gen, QuickCheck, TestResult};
use std::sync::Arc;
use uuid::Uuid;
use xxhash_rust::xxh3::xxh3_128;

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
        let cell = encode_present_cell(&payload);
        match decode_cell(Some(cell.as_ref())) {
            Ok(Read::Present(decoded)) => TestResult::from_bool(decoded == payload),
            Ok(other) => TestResult::error(format!("round-trip produced {other:?}, not Present")),
            Err(e) => TestResult::error(format!("decode_cell failed: {e}")),
        }
    }

    QuickCheck::new().quickcheck(prop as fn(Vec<u8>) -> TestResult);
}

/// A present cell is framed `[0x01] ++ raw payload` — byte-for-byte, no
/// app-level compression. This pins Change 1: the fjall codec stores the
/// payload verbatim (fjall block-compresses on disk via LZ4), so the cell is
/// not a zstd frame. A zstd frame would begin with the magic `0x28` and differ
/// from the raw tail for any payload, so the equality check is discriminating.
#[test]
fn present_cell_is_raw_tagged_payload() {
    let payload = b"profile-payload-not-compressed".as_slice();
    let cell = encode_present_cell(payload);
    let mut expected = vec![0x01_u8];
    expected.extend_from_slice(payload);
    assert_eq!(cell.as_ref(), expected.as_slice());
}

/// A `Set` of empty bytes is a present cell distinct from `Absent`, and must
/// round-trip as `Present(empty)`. Raw framing has no compression frame to pad
/// an empty tail (zstd used to), so this pins the empty case deterministically
/// rather than leaving it to the property test's dice.
#[test]
fn empty_payload_round_trips_as_present() -> Result<()> {
    let cell = encode_present_cell(&[]);
    assert_eq!(
        decode_cell(Some(cell.as_ref()))?,
        Read::Present(Bytes::new())
    );
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
fn collection_prefix_is_deterministic() -> Result<()> {
    let id = collection("profile")?;
    assert_eq!(collection_prefix(&id), collection_prefix(&id));
    Ok(())
}

/// A collection identity whose variable-length fields are drawn from a tiny
/// null-prone alphabet, so the injectivity property reaches the corner a
/// delimiter scheme would break: a `key`/`name` containing the delimiter
/// byte. `name` is forced non-empty (`StateName` rejects empty).
#[derive(Clone, Debug)]
struct PrefixFields {
    segment: u128,
    key: String,
    name: String,
}

impl Arbitrary for PrefixFields {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            segment: u128::arbitrary(g),
            key: null_prone_string(g, false),
            name: null_prone_string(g, true),
        }
    }
}

/// Builds a short string over `{a, b, \0}` so a field that contains the byte
/// a null-delimiter scheme would use is reachable. `non_empty` guarantees at
/// least one character for `StateName`.
fn null_prone_string(g: &mut Gen, non_empty: bool) -> String {
    const ALPHABET: [char; 3] = ['a', 'b', '\0'];
    let len = usize::arbitrary(g) % 5 + usize::from(non_empty);
    (0..len)
        .map(|_| g.choose(&ALPHABET).copied().unwrap_or('a'))
        .collect()
}

fn id_from(fields: PrefixFields) -> Result<CollectionId<ValueKind>> {
    Ok(CollectionId::new(
        StateKey::new(
            Uuid::from_u128(fields.segment),
            Arc::<str>::from(fields.key),
        ),
        StateType::Application,
        StateName::try_new(&fields.name)?,
    ))
}

fn prefix_for(fields: PrefixFields) -> Result<[u8; 16]> {
    Ok(collection_prefix(&id_from(fields)?))
}

/// Pre-streaming oracle: builds the transient buffer the old
/// `collection_prefix` allocated and hashes it in one shot. The streamed
/// implementation must produce byte-identical hasher input, so this and
/// `collection_prefix` agree.
fn prefix_via_buffer<K>(id: &CollectionId<K>) -> [u8; 16]
where
    K: CollectionKind,
{
    let segment_bytes = id.state_key().segment_id.as_bytes();
    let key_bytes = id.state_key().key.as_bytes();
    let state_type_byte = i8::from(id.state_type()).cast_unsigned();
    let name_bytes = id.name().as_str().as_bytes();

    let mut buf =
        Vec::with_capacity(segment_bytes.len() + 1 + 8 + key_bytes.len() + 8 + name_bytes.len());
    buf.extend_from_slice(segment_bytes);
    buf.push(state_type_byte);
    buf.extend_from_slice(&(key_bytes.len() as u64).to_be_bytes());
    buf.extend_from_slice(key_bytes);
    buf.extend_from_slice(&(name_bytes.len() as u64).to_be_bytes());
    buf.extend_from_slice(name_bytes);

    xxh3_128(&buf).to_be_bytes()
}

/// Behavior-preservation for the streamed hash: the streamed
/// `collection_prefix` is byte-for-byte the old buffer-then-`xxh3_128` result
/// over random identities. Total proof the durable cache key is unchanged.
#[test]
fn prop_streamed_prefix_matches_buffer_oracle() {
    fn prop(fields: PrefixFields) -> TestResult {
        let id = match id_from(fields) {
            Ok(id) => id,
            Err(e) => return TestResult::error(format!("invalid identity: {e}")),
        };
        TestResult::from_bool(collection_prefix(&id) == prefix_via_buffer(&id))
    }
    QuickCheck::new().quickcheck(prop as fn(PrefixFields) -> TestResult);
}

/// Injectivity: any two collection identities that differ in at least one
/// field produce distinct 16-byte prefixes. Generalizes the prior directed
/// test (which varied only `name`) and, with the null-prone generator,
/// covers the field-boundary corner. `state_type` has a single discriminant
/// today, so the property varies the other three fields; it will cover the
/// type field once a second `StateType` exists.
#[test]
fn prop_distinct_collections_get_distinct_prefixes() {
    fn prop(a: PrefixFields, b: PrefixFields) -> TestResult {
        if (a.segment, &a.key, &a.name) == (b.segment, &b.key, &b.name) {
            return TestResult::discard();
        }
        let (pa, pb) = match (prefix_for(a), prefix_for(b)) {
            (Ok(pa), Ok(pb)) => (pa, pb),
            (a, b) => return TestResult::error(format!("prefix build failed: {a:?} / {b:?}")),
        };
        TestResult::from_bool(pa != pb)
    }
    QuickCheck::new().quickcheck(prop as fn(PrefixFields, PrefixFields) -> TestResult);
}

/// Regression: with the single `state_type` discriminant sitting at byte
/// `0x00`, the prior null-delimited encoding made `(key="x", name="\0y")`
/// and `(key="x\0", name="y")` hash the *same* buffer — a wrong-collection
/// cache hit. The length-prefixed encoding keeps the field boundary fixed,
/// so they stay distinct.
#[test]
fn null_in_key_or_name_does_not_shift_field_boundary() -> Result<()> {
    let a = prefix_for(PrefixFields {
        segment: 0xA1,
        key: "x".to_owned(),
        name: "\0y".to_owned(),
    })?;
    let b = prefix_for(PrefixFields {
        segment: 0xA1,
        key: "x\0".to_owned(),
        name: "y".to_owned(),
    })?;
    assert_ne!(
        a, b,
        "a null in key/name must not collapse the key/name boundary"
    );
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
