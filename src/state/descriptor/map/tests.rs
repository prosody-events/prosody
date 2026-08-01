//! Map section-freeze and frozen-byte goldens.
//!
//! The behavioral invariants (key ordering, current-membership keyset, clear,
//! crash atomicity) are proven by the memory-backed `run_map_trace` property
//! in [`crate::state::tests`]. These pin the durable wire contracts: the
//! section discriminants and the `Meta` cell addresses.

use super::*;
use quickcheck::{Arbitrary, Gen, QuickCheck, TestResult};

/// The frozen discriminants and the single `Meta` cell address (a durable
/// contract — the sections lower to `0`/`1` and the keyset encodes to `[2]`).
/// Coordinates `[0]`/`[1]` are deliberately retired (they once held two min/max
/// bound cells; the keyset-only meta layout leaves the gap so a stale artifact
/// cannot alias an old frame).
#[test]
fn map_layout_is_frozen() {
    assert_eq!(MapNs::Meta as i8, 0);
    assert_eq!(MapNs::Entries as i8, 1);
    assert_eq!(i8::from(META_SECTION), 0);
    assert_eq!(i8::from(ENTRY_SECTION), 1);

    let keyset = MapKeysetKey::encode(&());
    assert_eq!(keyset.as_bytes(), &[2]);
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
    codec.deserialize(&mut buf)
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
