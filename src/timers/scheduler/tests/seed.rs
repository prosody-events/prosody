//! Checks how the actor seeds its slab watermark at start.

use crate::timers::datetime::CompactDateTime;
use crate::timers::scheduler::actor::ActorState;
use crate::timers::store::StoredSegment;
use crate::timers::store::memory::memory_store;
use crate::timers::test_support::test_segment;
use quickcheck_macros::quickcheck;

/// The actor keeps a stored watermark exactly when the watermark slab has
/// ended. Cleanup writes only the watermark of an ended slab, so a valid
/// watermark is never dropped.
#[quickcheck]
fn start_keeps_only_watermarks_of_ended_slabs(
    slab_seconds: u32,
    now_seconds: u32,
    offset: u8,
) -> bool {
    let segment = test_segment("seed", slab_seconds % 604_800 + 1);
    let size = segment.slab_size.seconds();
    let start = |watermark| {
        let stored = StoredSegment::new(segment.clone(), watermark);
        let now = CompactDateTime::from(now_seconds);
        ActorState::start(memory_store(segment.clone()), stored, now).last_persisted_watermark
    };

    // Draw watermarks near the current slab, where the decision changes.
    let watermark = (now_seconds / size)
        .saturating_add(u32::from(offset % 4))
        .saturating_sub(2);
    let ended = (u64::from(watermark) + 1) * u64::from(size) <= u64::from(now_seconds);

    start(None).is_none() && start(Some(watermark)) == ended.then_some(watermark)
}
