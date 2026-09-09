use super::{
    AttemptId, EventMarker, EventMarkerData, MarkerPayloadError, MarkerVersion, SectionClear,
    decode_marker_payload, encode_marker_payload, evidence_ttl,
};
use crate::state::cell::{Committed, ProvisionalWrite};
use crate::state::cell_key::{CellKey, Coordinate, Section};
use crate::state::event_ref::EventRef;
use crate::state::tests::support::arb_coordinate;
use crate::state::{StateName, StateType};
use crate::timers::duration::CompactDuration;
use quickcheck::{Arbitrary, Gen, QuickCheck, TestResult};
use std::slice::from_ref;
use uuid::Uuid;

/// A fixed message event to bind every generated marker; the event is not part
/// of the payload, so decode is handed the same one it was frozen under.
fn event() -> EventRef {
    EventRef::Message {
        dedup_id: Uuid::from_u128(0xFEED),
    }
}

/// A cell over a tiny section range (so duplicate sections and cross-section
/// ordering are reachable) and the null-prone coordinate alphabet.
fn arb_cell(g: &mut Gen) -> CellKey {
    let section = Section::new(i8::arbitrary(g) % 3);
    CellKey {
        section,
        coordinate: arb_coordinate(g),
    }
}

/// A staged write whose data is present or absent (the survivor derivation
/// keeps only the present ones).
fn arb_staged(g: &mut Gen) -> Vec<(CellKey, ProvisionalWrite)> {
    let len = usize::arbitrary(g) % 5;
    (0..len)
        .map(|_| {
            let data = bool::arbitrary(g).then(|| bytes(u8::arbitrary(g)));
            (
                arb_cell(g),
                ProvisionalWrite::new(data, Committed::new(None), event()),
            )
        })
        .collect()
}

fn bytes(value: u8) -> bytes::Bytes {
    bytes::Bytes::from(vec![value])
}

/// An arbitrary constructor-normalized marker: a staged set, plus a handful of
/// section clears each frozen from its own generated staged slice (so empty and
/// multi-survivor lists, and duplicate cleared sections, all arise).
#[derive(Clone, Debug)]
struct ArbMarker(EventMarker);

impl Arbitrary for ArbMarker {
    fn arbitrary(g: &mut Gen) -> Self {
        let staged = arb_staged(g);
        let clear_count = usize::arbitrary(g) % 3;
        let clears: Vec<SectionClear> = (0..clear_count)
            .map(|_| {
                let section = Section::new(i8::arbitrary(g) % 3);
                SectionClear::frozen(section, &arb_staged(g))
            })
            .collect();
        Self(EventMarker::frozen(
            event(),
            &staged,
            &clears,
            &[].into(),
            None,
            None,
            AttemptId(Uuid::from_u128(0xA77E)),
        ))
    }
}

/// A constructor-normalized marker round-trips through its frozen payload:
/// `decode(event, encode(m)) == m` over the whole coordinate/survivor/clear
/// space, including empty coordinates, empty staged lists, and empty survivor
/// lists.
#[test]
fn prop_marker_payload_round_trips() {
    fn prop(
        marker: ArbMarker,
        names: Vec<(bool, String)>,
        ttl: Option<u32>,
        dedup: Option<u128>,
    ) -> TestResult {
        let ArbMarker(marker) = marker;
        let touched = names
            .into_iter()
            .map(|(framework, name)| {
                StateName::try_new(format!("n{name}")).map(|name| {
                    (
                        if framework {
                            StateType::Framework
                        } else {
                            StateType::Application
                        },
                        name,
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>();
        let mut touched = match touched {
            Ok(names) => names,
            Err(error) => return TestResult::error(error.to_string()),
        };
        touched.sort_unstable();
        touched.dedup();
        let marker = EventMarker::from_parts(EventMarkerData {
            version: MarkerVersion::V2,
            attempt: marker.attempt(),
            event: marker.event(),
            staged: marker.staged().to_vec(),
            clears: marker.clears().to_vec(),
            touched: touched.into(),
            evidence_ttl: ttl.map(|seconds| CompactDuration::new(seconds.max(1))),
            dedup: dedup.map(Uuid::from_u128),
        });
        let bytes = match encode_marker_payload(&marker) {
            Ok(bytes) => bytes,
            Err(e) => return TestResult::error(format!("encode failed: {e}")),
        };
        match decode_marker_payload(event(), &bytes, MarkerVersion::V2, None) {
            Ok(decoded) => TestResult::from_bool(decoded == marker),
            Err(e) => TestResult::error(format!("decode failed: {e}")),
        }
    }
    QuickCheck::new().quickcheck(
        prop as fn(ArbMarker, Vec<(bool, String)>, Option<u32>, Option<u128>) -> TestResult,
    );
}

/// The survivor definition pinned directly at its source: for any mix of
/// present- and absent-data cells across sections, both constructors —
/// [`SectionClear::frozen`] (staged shape) and
/// [`SectionClear::frozen_resolved`] (resolved shape) — keep exactly the
/// cleared section's present-data coordinates, sorted and deduped, dropping
/// absent-data cells and cells of other sections. The two constructors must
/// agree cell-for-cell.
///
/// This pins the survivor *selection* at the constructor rather than through a
/// behavioral scan trace because the survivor list's observable effect lives
/// only on Cassandra: its `write_resolved` excludes survivors from the gap
/// range-deletes so a co-written survivor outlives the batch's delete-vs-write
/// timestamp tie, whereas the memory store erases-then-rewrites — a survivor is
/// always among the written cells, so the exclusion is redundant there. That
/// physical grain sits below the memory backend's model, so `prop_memory_*`
/// traces cannot falsify a wrong survivor filter; this property does.
#[test]
fn prop_section_clear_survivors_are_present_cells() {
    /// Cells over a tiny section/coordinate alphabet so section collisions,
    /// coordinate duplicates (dedup), and present/absent mixes all arise.
    fn prop(raw_section: i8, raw_cells: Vec<(i8, u8, bool, u8)>) -> TestResult {
        let section = Section::new(raw_section % 3);
        let cell_at = |cell_section: i8, coord: u8| CellKey {
            section: Section::new(cell_section % 3),
            coordinate: Coordinate::from_bytes(vec![coord % 3]),
        };

        let resolved: Vec<(CellKey, Option<bytes::Bytes>)> = raw_cells
            .into_iter()
            .map(|(cell_section, coord, present, value)| {
                (cell_at(cell_section, coord), present.then(|| bytes(value)))
            })
            .collect();
        let staged: Vec<(CellKey, ProvisionalWrite)> = resolved
            .iter()
            .map(|(key, value)| {
                (
                    key.clone(),
                    ProvisionalWrite::new(value.clone(), Committed::new(None), event()),
                )
            })
            .collect();

        let mut expected: Vec<Coordinate> = resolved
            .iter()
            .filter(|(key, value)| key.section == section && value.is_some())
            .map(|(key, _)| key.coordinate.clone())
            .collect();
        expected.sort_unstable();
        expected.dedup();

        let from_resolved = SectionClear::frozen_resolved(section, &resolved);
        let from_staged = SectionClear::frozen(section, &staged);
        TestResult::from_bool(
            from_resolved.survivors() == expected.as_slice()
                && from_staged.survivors() == expected.as_slice(),
        )
    }
    QuickCheck::new().quickcheck(prop as fn(i8, Vec<(i8, u8, bool, u8)>) -> TestResult);
}

/// The exact frozen bytes of a deterministic marker: two staged cells across
/// two sections (one at the empty coordinate) and one clear with one survivor.
/// A round-trip property cannot prove wire freezing, so pin the bytes.
#[test]
fn frozen_marker_payload_bytes() -> color_eyre::Result<()> {
    let staged = vec![
        (
            CellKey {
                section: Section::new(0),
                coordinate: Coordinate::empty(),
            },
            ProvisionalWrite::new(Some(bytes(1)), Committed::new(None), event()),
        ),
        (
            CellKey {
                section: Section::new(7),
                coordinate: Coordinate::from_bytes(vec![0xAB]),
            },
            ProvisionalWrite::new(Some(bytes(2)), Committed::new(None), event()),
        ),
    ];
    let clear = SectionClear::frozen(
        Section::new(3),
        &[(
            CellKey {
                section: Section::new(3),
                coordinate: Coordinate::from_bytes(vec![0x10]),
            },
            ProvisionalWrite::new(Some(bytes(9)), Committed::new(None), event()),
        )],
    );
    let legacy = EventMarker::frozen(
        event(),
        &staged,
        from_ref(&clear),
        &[].into(),
        Some(CompactDuration::new(3600)),
        None,
        AttemptId(Uuid::from_u128(0xA77E)),
    );
    let marker = EventMarker::frozen(
        event(),
        &staged,
        &[clear],
        &vec![(StateType::Application, StateName::try_new("x")?)].into(),
        Some(CompactDuration::new(3600)),
        Some(Uuid::from_u128(0xD3D0)),
        AttemptId(Uuid::from_u128(0xA77E)),
    );

    let expected: Vec<u8> = vec![
        0x00, 0x00, 0x00, 0x02, // staged_count = 2
        0x00, // cell A section 0
        0x00, 0x00, 0x00, 0x00, // cell A coord_len 0
        0x07, // cell B section 7
        0x00, 0x00, 0x00, 0x01, 0xAB, // cell B coord_len 1, [0xAB]
        0x00, 0x00, 0x00, 0x01, // clears_count = 1
        0x03, // clear section 3
        0x00, 0x00, 0x00, 0x01, // survivor_count 1
        0x00, 0x00, 0x00, 0x01, 0x10, // survivor coord_len 1, [0x10]
    ];
    let decoded = decode_marker_payload(
        event(),
        &expected,
        MarkerVersion::V1,
        Some(CompactDuration::new(3600)),
    )?;
    assert_eq!(decoded.version(), MarkerVersion::V1);
    assert_eq!(decoded.staged(), legacy.staged());
    assert_eq!(decoded.clears(), legacy.clears());
    assert_eq!(decoded.evidence_ttl(), legacy.evidence_ttl());
    assert_eq!(decoded.dedup(), Some(Uuid::from_u128(0xFEED)));
    assert!(decoded.touched().is_empty());
    let mut expected_v2 = expected;
    expected_v2.extend_from_slice(&[0, 0, 0, 1, 0, 0, 0, 0, 1, b'x', 0, 0, 14, 16, 1]);
    expected_v2.extend_from_slice(Uuid::from_u128(0xD3D0).as_bytes());
    expected_v2.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xA7, 0x7E]);
    assert_eq!(
        encode_marker_payload(&marker)?.as_ref(),
        expected_v2.as_slice(),
        "frozen marker payload layout"
    );
    Ok(())
}

/// A buffer that ends inside a declared field decodes to `Truncated`, never a
/// silent prefix. An example because [`prop_marker_payload_round_trips`] only
/// encodes valid markers, so it can never synthesize a corrupt buffer — the
/// rejection paths are unreachable by that generator.
#[test]
fn truncated_payload_is_rejected() {
    // Claims one staged cell but carries no cell bytes.
    let truncated = [0x00, 0x00, 0x00, 0x01];
    assert_eq!(
        decode_marker_payload(event(), &truncated, MarkerVersion::V1, None),
        Err(MarkerPayloadError::Truncated)
    );
}

/// A count-inflated buffer — a `u32::MAX` staged count over four bytes of
/// payload — fails `Truncated` (the error is the observable; the decoder's
/// capacity cap keeps the lying count from demanding an unbounded allocation
/// on the way there). An example for the same reason as
/// `truncated_payload_is_rejected`: the round-trip property cannot forge a
/// lying count.
#[test]
fn inflated_count_is_rejected() {
    let inflated = [0xFF, 0xFF, 0xFF, 0xFF];
    assert_eq!(
        decode_marker_payload(event(), &inflated, MarkerVersion::V1, None),
        Err(MarkerPayloadError::Truncated)
    );
}

/// Bytes past the last declared field decode to `TrailingGarbage`. An example
/// for the same reason as `truncated_payload_is_rejected`: the round-trip
/// property encodes exact-length buffers and cannot append trailing bytes.
#[test]
fn trailing_garbage_is_rejected() -> color_eyre::Result<()> {
    let marker = EventMarker::frozen(
        event(),
        &[],
        &[],
        &[].into(),
        None,
        None,
        AttemptId(Uuid::from_u128(0xA77E)),
    );
    let mut bytes = encode_marker_payload(&marker)?.to_vec();
    bytes.push(0xFF);
    assert_eq!(
        decode_marker_payload(event(), &bytes, MarkerVersion::V2, None),
        Err(MarkerPayloadError::TrailingGarbage)
    );
    Ok(())
}

/// Evidence covers every touched TTL and the dedup floor. No expiry wins.
#[test]
fn prop_evidence_ttl_covers_all_collections() {
    fn prop(seconds: Vec<u32>, unbounded: bool, offset: u16, above: bool) -> bool {
        let seconds: Vec<_> = seconds.into_iter().map(|ttl| ttl % 1_000_000 + 1).collect();
        let maximum = seconds.iter().copied().max();
        let maximum_secs = maximum.unwrap_or(1);
        let floor = if above {
            maximum_secs + u32::from(offset) + 1
        } else {
            maximum_secs.saturating_sub(u32::from(offset) + 1)
        };
        let expected = if unbounded {
            None
        } else {
            maximum.map(|ttl| CompactDuration::new(ttl.max(floor)))
        };
        evidence_ttl(
            CompactDuration::new(floor),
            seconds.into_iter().enumerate().map(|(index, ttl)| {
                if unbounded && index == 0 {
                    None
                } else {
                    Some(CompactDuration::new(ttl))
                }
            }),
        ) == expected
    }
    QuickCheck::new().quickcheck(prop as fn(Vec<u32>, bool, u16, bool) -> bool);
}
