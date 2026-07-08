use super::*;
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use quickcheck::TestResult;
use quickcheck_macros::quickcheck;

/// `from_time` floor-divides epoch seconds by slab size, and `range()` is the
/// half-open window that division carves out. Generates raw `u32` seconds
/// rather than the crate's `Arbitrary for CompactDateTime` (wall-clock seeded,
/// unsuitable for a deterministic property).
#[quickcheck]
fn prop_slab_from_time_range_contains_time(secs: u32, epoch: u32) -> TestResult {
    if secs == 0 {
        return TestResult::discard();
    }

    let size = CompactDuration::new(secs);
    let slab = Slab::from_time(size, CompactDateTime::from(epoch));
    let range = slab.range();

    let expected_id = epoch / secs;
    let expected_start = u64::from(expected_id) * u64::from(secs);
    let expected_end = (expected_start + u64::from(secs)).min(u64::from(u32::MAX));

    TestResult::from_bool(
        slab.id() == expected_id
            && slab.size() == size
            && range.start.epoch_seconds() == expected_start as u32
            && range.end.epoch_seconds() == expected_end as u32
            && range.start.epoch_seconds() <= epoch
            && epoch <= range.end.epoch_seconds(),
    )
}

/// Zero-size slabs are a deliberate degenerate case (`from_time` avoids
/// dividing by zero, `range()` collapses to a point) that the generator above
/// never reaches, since it discards `secs == 0`.
#[test]
fn test_slab_zero_size_is_degenerate() {
    let size = CompactDuration::new(0);

    let from_time = Slab::from_time(size, CompactDateTime::from(123_i32));
    assert_eq!(from_time.id(), 0);

    let range = Slab::new(3, size).range();
    assert_eq!(range.start.epoch_seconds(), 0);
    assert_eq!(range.end.epoch_seconds(), 0);
}

/// `Ord`/`PartialEq` are derived over `(id, size)`; for slabs sharing a size,
/// comparison and equality must agree with comparing `id` directly.
#[quickcheck]
fn prop_slab_ordering_matches_id(id_a: SlabId, id_b: SlabId, secs: u32) -> bool {
    let size = CompactDuration::new(secs);
    let slab_a = Slab::new(id_a, size);
    let slab_b = Slab::new(id_b, size);

    slab_a.cmp(&slab_b) == id_a.cmp(&id_b) && (slab_a == slab_b) == (id_a == id_b)
}

#[test]
fn test_slab_debug() {
    let slab_id = 42;
    let size = CompactDuration::new(60);

    let slab = Slab::new(slab_id, size);
    let debug_str = format!("{slab:?}");

    assert_eq!(debug_str, format!("Slab({slab_id})"));
}

#[test]
fn test_slab_display() {
    let slab_id = 3;
    let size = CompactDuration::new(60);

    let slab = Slab::new(slab_id, size);
    let display_str = format!("{slab}");

    assert_eq!(
        display_str,
        format!(
            "{slab_id}[{}—{}]",
            CompactDateTime::from(180_i32),
            CompactDateTime::from(240_i32)
        )
    );
}
