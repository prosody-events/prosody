//! Map section-freeze and frozen-byte goldens.
//!
//! The behavioral invariants (key ordering, current-membership keyset, clear,
//! crash atomicity) are proven by the memory-backed `run_map_trace` property
//! in [`crate::state::tests`]. These pin the durable wire contracts: the
//! section discriminants and the `Meta` cell addresses.

use super::*;
use bytes::BytesMut;
use quickcheck::{Arbitrary, Gen, QuickCheck, TestResult};

/// The frozen cell addresses (a durable contract — the keyset family lowers to
/// section `0` and encodes to coordinate `[2]`, the entries family to section
/// `1`) and the reset domain a `clear` covers. Coordinates `[0]`/`[1]` are
/// deliberately retired (they once held two min/max bound cells; the
/// keyset-only layout leaves the gap so a stale artifact cannot alias an old
/// frame), so a whole-layout reset over section `0` is what erases them.
///
/// The declared ids, format tokens, and section count are additionally pinned
/// by the `const` assertion beside the layout; this test pins what the two
/// cell-address helpers resolve to, which is the address every seeded-cell test
/// writes through.
#[test]
fn map_layout_is_frozen() {
    let sections = <FrozenLayout as CollectionLayout>::SECTIONS;
    assert_eq!(
        sections.iter().map(|s| i8::from(*s)).collect::<Vec<_>>(),
        vec![0, 1],
        "a whole-layout reset covers both declared sections"
    );
    assert_eq!(i8::from(keyset_cell().section), 0);
    assert_eq!(keyset_cell().coordinate.as_bytes(), &[2]);
    assert_eq!(
        i8::from(entry_cell_for(&Coordinate::from_bytes(vec![7])).section),
        1
    );
}

/// A short coordinate over a tiny null-prone alphabet, so the keyset frame's
/// length-delimited scheme is exercised at the empty coordinate and at bytes a
/// naive parser might mishandle.
fn arb_coordinate(g: &mut Gen) -> Coordinate {
    const ALPHABET: [u8; 3] = [0x00, 0x01, 0xFF];
    let len = usize::arbitrary(g) % 4;
    let bytes: Vec<u8> = (0..len)
        .map(|_| g.choose(&ALPHABET).copied().unwrap_or(0))
        .collect();
    Coordinate::from_bytes(bytes)
}

/// A strictly-ascending coordinate list, the shape a `Tracked` keyset stores.
#[derive(Clone, Debug)]
struct SortedCoords(Vec<Coordinate>);

impl Arbitrary for SortedCoords {
    fn arbitrary(g: &mut Gen) -> Self {
        let n = usize::arbitrary(g) % 6;
        let mut coords: Vec<Coordinate> = (0..n).map(|_| arb_coordinate(g)).collect();
        coords.sort();
        coords.dedup();
        Self(coords)
    }
}

/// Serialize then deserialize a keyset through the real [`MapKeysetCodec`].
fn round_trip(keyset: &Keyset) -> Result<Keyset, KeysetFrameError> {
    let mut codec = MapKeysetCodec;
    let mut buf = Vec::new();
    codec.serialize(keyset.clone(), &mut buf)?;
    let mut borrowed = Vec::new();
    codec.serialize_ref(keyset, &mut borrowed)?;
    assert_eq!(borrowed, buf, "both serializers must write the same bytes");
    let owned = codec.deserialize_owned(BytesMut::from(buf.as_slice()))?;
    let decoded = codec.deserialize(&mut buf)?;
    assert_eq!(owned, decoded, "both decoders must read the same value");
    Ok(decoded)
}

#[test]
fn owned_keyset_decode_reuses_frame_storage() -> color_eyre::Result<()> {
    let keyset = Keyset::Tracked(vec![
        Coordinate::from_bytes("alpha"),
        Coordinate::from_bytes("omega"),
    ]);
    let mut encoded = Vec::new();
    MapKeysetCodec.serialize(keyset, &mut encoded)?;
    let frame = BytesMut::from(encoded.as_slice());
    let start = frame.as_ptr() as usize;
    let end = start + frame.len();

    let Keyset::Tracked(coordinates) = MapKeysetCodec.deserialize_owned(frame)? else {
        color_eyre::eyre::bail!("tracked frame decoded as overflowed");
    };
    assert!(coordinates.iter().all(|coordinate| {
        let pointer = coordinate.as_bytes().as_ptr() as usize;
        (start..end).contains(&pointer)
    }));
    Ok(())
}

/// Decodes a raw frame through the codec (exercising the trait-boundary copy).
fn decode(mut bytes: Vec<u8>) -> Result<Keyset, KeysetFrameError> {
    let mut codec = MapKeysetCodec;
    codec.deserialize(&mut bytes)
}

/// The keyset codec round-trips both variants: `Overflowed`, and any
/// strictly-ascending `Tracked` list (including empty) through the exact-length
/// serializer and the zero-copy parser.
#[test]
fn prop_keyset_frame_round_trip() {
    fn prop(input: SortedCoords, overflowed: bool) -> TestResult {
        let keyset = if overflowed {
            Keyset::Overflowed
        } else {
            Keyset::Tracked(input.0)
        };
        match round_trip(&keyset) {
            Ok(decoded) => TestResult::from_bool(decoded == keyset),
            Err(e) => TestResult::error(format!("round-trip failed: {e}")),
        }
    }
    QuickCheck::new().quickcheck(prop as fn(SortedCoords, bool) -> TestResult);
}

/// Every malformed keyset frame is rejected with `Err` — none panic — so the
/// untrusted-frame parser is total: empty, unknown tag, truncated count,
/// truncated key, an inflated count the frame cannot hold, unsorted and
/// duplicate coordinates, and trailing bytes after either tag.
#[test]
fn keyset_frame_rejects_malformed() {
    // Empty frame — no tag.
    assert!(matches!(decode(vec![]), Err(KeysetFrameError::Truncated)));
    // Unknown tag.
    assert!(matches!(
        decode(vec![9]),
        Err(KeysetFrameError::UnknownTag(9))
    ));
    // Tracked tag with a truncated count (only 3 of 4 bytes).
    assert!(matches!(
        decode(vec![0, 0, 0, 0]),
        Err(KeysetFrameError::Truncated)
    ));
    // Count 1, length 4, but only one coordinate byte present.
    assert!(matches!(
        decode(vec![0, 0, 0, 0, 1, 0, 0, 0, 4, 0xAA]),
        Err(KeysetFrameError::Truncated)
    ));
    // Count u32::MAX with 2 payload bytes — the `with_capacity` cap must not
    // OOM; the read errors instead.
    assert!(matches!(
        decode(vec![0, 0xFF, 0xFF, 0xFF, 0xFF, 0, 0]),
        Err(KeysetFrameError::Truncated)
    ));
    // Unsorted pair: [0x02] then [0x01].
    assert!(matches!(
        decode(vec![0, 0, 0, 0, 2, 0, 0, 0, 1, 0x02, 0, 0, 0, 1, 0x01]),
        Err(KeysetFrameError::Unsorted)
    ));
    // Duplicate pair: [0x01] then [0x01].
    assert!(matches!(
        decode(vec![0, 0, 0, 0, 2, 0, 0, 0, 1, 0x01, 0, 0, 0, 1, 0x01]),
        Err(KeysetFrameError::Unsorted)
    ));
    // Trailing bytes after the Overflowed sentinel.
    assert!(matches!(
        decode(vec![1, 0x00]),
        Err(KeysetFrameError::TrailingBytes)
    ));
    // Trailing bytes after the last tracked coordinate.
    assert!(matches!(
        decode(vec![0, 0, 0, 0, 1, 0, 0, 0, 1, 0x01, 0xFF]),
        Err(KeysetFrameError::TrailingBytes)
    ));
}
