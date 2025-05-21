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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::timers::datetime::CompactDateTime;
    use crate::timers::duration::CompactDuration;
    use uuid::Uuid;

    #[test]
    fn test_slab_new() {
        let segment_id = Uuid::new_v4();
        let slab_id = 42;
        let size = CompactDuration::new(60); // 60 seconds

        let slab = Slab::new(segment_id, slab_id, size);

        assert_eq!(slab.segment_id(), &segment_id);
        assert_eq!(slab.id(), slab_id);
        assert_eq!(slab.size(), size);
    }

    #[test]
    fn test_slab_from_time() {
        let segment_id = Uuid::new_v4();
        let size = CompactDuration::new(60); // 60 seconds
        let time = CompactDateTime::from(123_i32); // 123 seconds since epoch

        let slab = Slab::from_time(segment_id, size, time);

        assert_eq!(slab.segment_id(), &segment_id);
        assert_eq!(slab.size(), size);
        assert_eq!(slab.id(), 2); // 123 / 60 = 2
    }

    #[test]
    fn test_slab_from_time_zero_size() {
        let segment_id = Uuid::new_v4();
        let size = CompactDuration::new(0); // Zero duration
        let time = CompactDateTime::from(123_i32); // 123 seconds since epoch

        let slab = Slab::from_time(segment_id, size, time);

        assert_eq!(slab.segment_id(), &segment_id);
        assert_eq!(slab.size(), size);
        assert_eq!(slab.id(), 0); // Slab ID should default to 0 for zero size
    }

    #[test]
    fn test_slab_range() {
        let segment_id = Uuid::new_v4();
        let slab_id = 3;
        let size = CompactDuration::new(60); // 60 seconds

        let slab = Slab::new(segment_id, slab_id, size);
        let range = slab.range();

        assert_eq!(range.start.epoch_seconds(), 180); // 3 * 60 = 180
        assert_eq!(range.end.epoch_seconds(), 240); // 180 + 60 = 240
    }

    #[test]
    fn test_slab_range_zero_size() {
        let segment_id = Uuid::new_v4();
        let slab_id = 3;
        let size = CompactDuration::new(0); // Zero duration

        let slab = Slab::new(segment_id, slab_id, size);
        let range = slab.range();

        assert_eq!(range.start.epoch_seconds(), 0); // Start should be 0
        assert_eq!(range.end.epoch_seconds(), 0); // End should also be 0
    }

    #[test]
    fn test_slab_debug() {
        let segment_id = Uuid::new_v4();
        let slab_id = 42;
        let size = CompactDuration::new(60); // 60 seconds

        let slab = Slab::new(segment_id, slab_id, size);
        let debug_str = format!("{slab:?}");

        assert_eq!(debug_str, format!("Slab({segment_id}/{slab_id})"));
    }

    #[test]
    fn test_slab_display() {
        let segment_id = Uuid::new_v4();
        let slab_id = 3;
        let size = CompactDuration::new(60); // 60 seconds

        let slab = Slab::new(segment_id, slab_id, size);
        let display_str = format!("{slab}");

        assert_eq!(
            display_str,
            format!(
                "{segment_id}/{slab_id}[{}—{}]",
                CompactDateTime::from(180_i32), // Start of range
                CompactDateTime::from(240_i32)  // End of range
            )
        );
    }

    #[test]
    fn test_slab_equality() {
        let segment_id = Uuid::new_v4();
        let size = CompactDuration::new(60); // 60 seconds

        let slab1 = Slab::new(segment_id, 1, size);
        let slab2 = Slab::new(segment_id, 1, size);
        let slab3 = Slab::new(segment_id, 2, size);

        assert_eq!(slab1, slab2);
        assert_ne!(slab1, slab3);
    }

    #[test]
    fn test_slab_ordering() {
        let segment_id = Uuid::new_v4();
        let size = CompactDuration::new(60); // 60 seconds

        let slab1 = Slab::new(segment_id, 1, size);
        let slab2 = Slab::new(segment_id, 2, size);

        assert!(slab1 < slab2);
        assert!(slab2 > slab1);
    }
}
