use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::timers::store::SegmentId;
use std::fmt::{Debug, Display, Formatter};
use std::ops::Range;

pub type SlabId = u32;

#[derive(Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct Slab {
    segment_id: SegmentId,
    id: SlabId,
    size: CompactDuration,
}

impl Slab {
    pub fn new(segment_id: SegmentId, id: SlabId, size: CompactDuration) -> Self {
        Slab {
            segment_id,
            id,
            size,
        }
    }

    pub fn from_time(segment_id: SegmentId, size: CompactDuration, time: CompactDateTime) -> Self {
        let epoch_secs = time.epoch_seconds();
        let slab_secs = size.seconds();

        let id: SlabId = if slab_secs == 0 {
            0
        } else {
            epoch_secs.saturating_div(slab_secs)
        };

        Slab {
            segment_id,
            id,
            size,
        }
    }

    pub fn id(&self) -> SlabId {
        self.id
    }

    pub fn segment_id(&self) -> &SegmentId {
        &self.segment_id
    }

    pub fn size(&self) -> CompactDuration {
        self.size
    }

    pub fn range(&self) -> Range<CompactDateTime> {
        let size = self.size.seconds();
        let start = self.id.saturating_mul(size);
        let end = start.saturating_add(size);

        start.into()..end.into()
    }
}

impl Debug for Slab {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Slab({}/{})", self.segment_id, self.id)
    }
}

impl Display for Slab {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let range = self.range();
        write!(
            f,
            "{}/{}[{}—{}]",
            self.segment_id, self.id, range.start, range.end
        )
    }
}
