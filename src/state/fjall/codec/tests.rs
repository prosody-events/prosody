use super::{
    Read, collection_prefix, coord_cell_key, decode_cell, encode_absent_cell, encode_present_cell,
    index_coord_key, index_seeded_key, marker_check_key,
};
use crate::state::cell_key::{CellKey, Coordinate, Section};
use crate::state::tests::support::fixed_collection;
use crate::state::{CollectionId, StateKey, StateName, StateType};
use bytes::Bytes;
use color_eyre::eyre::Result;
use quickcheck::{Arbitrary, Gen, QuickCheck, TestResult};
use std::sync::Arc;
use uuid::Uuid;
use xxhash_rust::xxh3::xxh3_128;

/// An arbitrary non-`never` expiry, exercising the header round-trip.
const EXPIRY: u64 = 1_700_000_000_000;

#[test]
fn absent_round_trip() -> Result<()> {
    let cell = encode_absent_cell(EXPIRY);
    assert_eq!(decode_cell(Some(cell.as_ref()))?, (EXPIRY, Read::Absent));
    Ok(())
}

/// Any payload + expiry round-trips through `encode_present_cell` →
/// `decode_cell` as `(expiry, Read::Present)` with identical bytes — the cache
/// codec is lossless over the whole byte space and the expiry header, including
/// the empty payload a `Set` of empty bytes produces, not just one fixed
/// example.
#[test]
fn present_round_trip() {
    fn prop(payload: Vec<u8>, expiry: u64) -> TestResult {
        let payload = Bytes::from(payload);
        let cell = encode_present_cell(&payload, expiry);
        match decode_cell(Some(cell.as_ref())) {
            Ok((e, Read::Present(decoded))) => {
                TestResult::from_bool(e == expiry && decoded == payload)
            }
            Ok(other) => TestResult::error(format!("round-trip produced {other:?}, not Present")),
            Err(e) => TestResult::error(format!("decode_cell failed: {e}")),
        }
    }

    QuickCheck::new().quickcheck(prop as fn(Vec<u8>, u64) -> TestResult);
}

/// A present cell is framed `[0x01][expiry: u64 BE][raw payload]` —
/// byte-for-byte, no app-level compression. This tests the frame layout: the
/// fjall codec stores the payload verbatim after the tag + expiry header (fjall
/// block-compresses on disk via LZ4), so the cell is not a zstd frame.
#[test]
fn present_cell_is_raw_tagged_payload_with_expiry() {
    let payload = b"profile-payload-not-compressed".as_slice();
    let cell = encode_present_cell(payload, EXPIRY);
    let mut expected = vec![0x01_u8];
    expected.extend_from_slice(&EXPIRY.to_be_bytes());
    expected.extend_from_slice(payload);
    assert_eq!(cell.as_ref(), expected.as_slice());
}

/// A `Set` of empty bytes is a present cell distinct from `Absent`, and must
/// round-trip as `Present(empty)` with its expiry. Raw framing has no
/// compression frame to pad an empty tail (zstd used to), so this tests the
/// empty case deterministically rather than leaving it to the property test's
/// dice.
#[test]
fn empty_payload_round_trips_as_present() -> Result<()> {
    let cell = encode_present_cell(&[], 0);
    assert_eq!(
        decode_cell(Some(cell.as_ref()))?,
        (0, Read::Present(Bytes::new()))
    );
    Ok(())
}

#[test]
fn missing_entry_decodes_as_unknown() -> Result<()> {
    assert_eq!(decode_cell(None)?, (0, Read::Unknown));
    Ok(())
}

#[test]
fn empty_cell_is_rejected() {
    assert!(decode_cell(Some(&[])).is_err());
}

/// A frame with a tag but a truncated expiry header (fewer than 8 bytes) is
/// rejected — the decoder needs the whole header before the payload.
#[test]
fn truncated_expiry_header_is_rejected() {
    assert!(decode_cell(Some(&[0x01, 0x00, 0x00])).is_err());
}

#[test]
fn unknown_tag_byte_is_rejected() {
    // A full 9-byte frame (tag + expiry) with an unknown tag.
    let mut frame = vec![0xFE_u8];
    frame.extend_from_slice(&0u64.to_be_bytes());
    let result = decode_cell(Some(&frame));
    assert!(
        matches!(
            result,
            Err(super::FjallCellCacheError::UnknownCacheTag(0xFE))
        ),
        "expected UnknownCacheTag, got {result:?}"
    );
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

fn id_from(fields: PrefixFields) -> Result<CollectionId> {
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
fn prefix_via_buffer(id: &CollectionId) -> [u8; 16] {
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
/// reaches the field-boundary corner. `state_type` has a single discriminant
/// today, so the property varies the other three fields; it will reach the
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

// --- Warm-index key codec ---------------------------------------------------

/// The frozen wire bytes of a coord key, a seeded key, and a presence key —
/// any persisted encoding gets a verified-bytes test. Also proves a coord key
/// round-trips back to its `CellKey`. (Index discriminant `0x02` is retired
/// with the deleted design's interval rows; `Presence` stays `0x03`.)
#[test]
fn frozen_warm_index_bytes() -> Result<()> {
    let id = fixed_collection("frozen")?;
    let prefix = collection_prefix(&id);
    let cell = CellKey {
        section: Section::new(7),
        coordinate: Coordinate::from_bytes(vec![0xAB, 0xCD]),
    };

    // coord key: [hash][Coord=0x00][section=0x07][coordinate].
    let coord = index_coord_key(&id, &cell);
    let mut expected = prefix.to_vec();
    expected.extend_from_slice(&[0x00, 0x07, 0xAB, 0xCD]);
    assert_eq!(coord.as_slice(), expected.as_slice(), "coord key layout");
    assert_eq!(coord_cell_key(&coord), cell, "coord key round-trips");

    // seeded key: [hash][Seeded=0x01].
    let seeded = index_seeded_key(&id);
    let mut expected = prefix.to_vec();
    expected.push(0x01);
    assert_eq!(seeded.as_slice(), expected.as_slice(), "seeded key layout");

    // presence key: [hash][Presence=0x03].
    let presence = marker_check_key(&id);
    let mut expected = prefix.to_vec();
    expected.push(0x03);
    assert_eq!(
        presence.as_slice(),
        expected.as_slice(),
        "presence key layout"
    );
    Ok(())
}

/// The provisional-index coord key is byte-for-byte `[hash][Coord=0x00]
/// [section][coordinate]` over random identities, sections, and coordinates,
/// round-trips back to its `CellKey`, and stays inline exactly when its encoded
/// length fits the 32-byte `SmallVec` buffer (only a long Map coordinate
/// spills). Byte parity is the ordering proof: fjall range order is a pure
/// function of these bytes.
#[test]
fn prop_index_coord_key_bytes_spill_and_round_trip() {
    fn prop(fields: PrefixFields, section: i8, coord: Vec<u8>) -> TestResult {
        let id = match id_from(fields) {
            Ok(id) => id,
            Err(e) => return TestResult::error(format!("invalid identity: {e}")),
        };
        let mut expected = collection_prefix(&id).to_vec();
        expected.push(0x00); // IndexKind::Coord discriminant
        expected.push(section.cast_unsigned());
        expected.extend_from_slice(&coord);

        let cell = CellKey {
            section: Section::new(section),
            coordinate: Coordinate::from_bytes(coord),
        };
        let key = index_coord_key(&id, &cell);

        if key.as_slice() != expected.as_slice() {
            return TestResult::error(format!(
                "bytes diverged: got {:?}, want {:?}",
                key.as_slice(),
                expected.as_slice()
            ));
        }
        let expect_spill = expected.len() > 32;
        if key.spilled() != expect_spill {
            return TestResult::error(format!(
                "spill mismatch: len {} spilled {} want {}",
                expected.len(),
                key.spilled(),
                expect_spill
            ));
        }
        if coord_cell_key(&key) != cell {
            return TestResult::error("coord key did not round-trip".to_owned());
        }
        TestResult::from_bool(true)
    }
    QuickCheck::new().quickcheck(prop as fn(PrefixFields, i8, Vec<u8>) -> TestResult);
}

/// Pins the coord key's 32-byte inline capacity unconditionally at its exact
/// spill boundary: a `[hash=16][Coord][section]` head is 18 bytes, so a 14-byte
/// coordinate fills the buffer exactly (stays inline) and a 15-byte one is the
/// first to spill. The property above only lands on this edge
/// probabilistically; shrinking the `SmallVec` inline size regresses this
/// deterministically.
#[test]
fn index_coord_key_spill_boundary() -> Result<()> {
    let id = fixed_collection("spill-boundary")?;
    for (coord_len, want_spill) in [(14usize, false), (15usize, true)] {
        let cell = CellKey {
            section: Section::new(3),
            coordinate: Coordinate::from_bytes(vec![0xEE; coord_len]),
        };
        let key = index_coord_key(&id, &cell);
        assert_eq!(
            key.len(),
            18 + coord_len,
            "encoded length (coord {coord_len})"
        );
        assert_eq!(
            key.spilled(),
            want_spill,
            "spill at coord {coord_len} (encoded {})",
            key.len()
        );
        assert_eq!(coord_cell_key(&key), cell, "round-trip (coord {coord_len})");
    }
    Ok(())
}
