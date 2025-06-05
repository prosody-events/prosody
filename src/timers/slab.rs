//! Time-based partitioning for efficient timer storage and retrieval.
//!
//! This module provides the [`Slab`] type, which represents time-based
//! partitions of timer data. Slabs enable efficient range queries and storage
//! organization by grouping timers into fixed time windows.
//!
//! ## Purpose
//!
//! Slabs serve several key purposes in the timer system:
//!
//! - **Time Partitioning**: Group timers into manageable time ranges
//! - **Efficient Queries**: Enable fast lookups for timers in specific time
//!   windows
//! - **Storage Organization**: Provide a hierarchical structure for persistent
//!   storage
//! - **Memory Management**: Allow loading and unloading of timer data based on
//!   time relevance
//!
//! ## Slab Sizing
//!
//! The size of each slab determines the trade-offs between query efficiency and
//! storage overhead:
//!
//! - **Smaller slabs**: More precise time ranges, higher metadata overhead
//! - **Larger slabs**: Broader time ranges, lower metadata overhead, less
//!   precise queries
//! - **Typical sizes**: 5-60 minutes depending on timer density and access
//!   patterns
//!
//! ## Slab Calculation
//!
//! Given a time and slab size, the slab ID is calculated as:
//! ```text
//! slab_id = floor(time_seconds / slab_size_seconds)
//! ```
//!
//! This ensures that all times within a slab's duration map to the same slab
//! ID.

use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::timers::store::SegmentId;
use std::fmt::{Debug, Display, Formatter};
use std::ops::Range;

/// Unique identifier for a time-based slab within a segment.
///
/// Slab IDs are calculated based on time and slab size, providing a
/// deterministic mapping from any point in time to its containing slab. This
/// enables efficient time-range queries and storage organization.
pub type SlabId = u32;

/// A time-based partition of timer data within a segment.
///
/// A [`Slab`] represents a contiguous time range within a segment, providing
/// efficient organization and querying of timer data. All timers with execution
/// times within the slab's time range are grouped together for storage and
/// retrieval.
///
/// ## Structure
///
/// Each slab contains:
/// - **Segment ID**: Links the slab to its parent segment
/// - **Slab ID**: Numeric identifier calculated from time
/// - **Size**: Duration of the time range this slab covers
///
/// ## Time Range Calculation
///
/// The time range for a slab is calculated as:
/// ```text
/// start_time = slab_id * slab_size_seconds
/// end_time = start_time + slab_size_seconds
/// ```
///
/// ## Ordering
///
/// Slabs are ordered first by segment ID, then by slab ID, enabling efficient
/// range queries across time boundaries.
#[derive(Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct Slab {
    /// The segment this slab belongs to.
    segment_id: SegmentId,

    /// Numeric identifier for this slab within the segment.
    id: SlabId,

    /// Duration of the time range this slab covers.
    size: CompactDuration,
}

impl Slab {
    /// Creates a new slab with the specified parameters.
    ///
    /// # Arguments
    ///
    /// * `segment_id` - The segment this slab belongs to
    /// * `id` - The numeric slab identifier
    /// * `size` - The duration this slab covers
    ///
    /// # Returns
    ///
    /// A new [`Slab`] instance with the specified configuration.
    #[must_use]
    pub fn new(segment_id: SegmentId, id: SlabId, size: CompactDuration) -> Self {
        Slab {
            segment_id,
            id,
            size,
        }
    }

    /// Creates a slab that contains the specified time.
    ///
    /// This method calculates which slab a given time falls into based on
    /// the slab size. The calculation ensures that all times within the same
    /// slab duration map to the same slab ID.
    ///
    /// # Arguments
    ///
    /// * `segment_id` - The segment this slab belongs to
    /// * `size` - The duration each slab covers
    /// * `time` - The time to find the containing slab for
    ///
    /// # Returns
    ///
    /// A [`Slab`] that contains the specified time.
    ///
    /// # Edge Cases
    ///
    /// If the slab size is zero, all times map to slab ID 0.
    #[must_use]
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

    /// Returns the numeric identifier of this slab.
    ///
    /// # Returns
    ///
    /// The [`SlabId`] for this slab within its segment.
    #[must_use]
    pub fn id(&self) -> SlabId {
        self.id
    }

    /// Returns the segment identifier this slab belongs to.
    ///
    /// # Returns
    ///
    /// A reference to the [`SegmentId`] for this slab's segment.
    #[must_use]
    pub fn segment_id(&self) -> &SegmentId {
        &self.segment_id
    }

    /// Returns the duration this slab covers.
    ///
    /// # Returns
    ///
    /// The [`CompactDuration`] representing the time span of this slab.
    #[must_use]
    pub fn size(&self) -> CompactDuration {
        self.size
    }

    /// Returns the time range covered by this slab.
    ///
    /// The range spans from the slab's start time (inclusive) to its end time
    /// (exclusive), covering exactly the duration specified by the slab
    /// size.
    ///
    /// # Returns
    ///
    /// A [`Range<CompactDateTime>`] representing the time span of this slab.
    /// The start time is included in the range, while the end time is excluded.
    #[must_use]
    pub fn range(&self) -> Range<CompactDateTime> {
        let size = self.size.seconds();
        let start = self.id.saturating_mul(size);
        let end = start.saturating_add(size);

        start.into()..end.into()
    }

    /// Creates a new slab by adding the specified number to this slab's ID.
    ///
    /// This operation preserves the segment ID and size while advancing
    /// the slab ID by the specified amount.
    ///
    /// # Arguments
    ///
    /// * `number` - The number to add to this slab's ID
    ///
    /// # Returns
    ///
    /// [`Some(Slab)`] with the new ID if the addition doesn't overflow,
    /// [`None`] if the addition would overflow.
    #[must_use]
    pub fn add(&self, number: u32) -> Option<Slab> {
        let mut slab = self.clone();
        slab.id = self.id.checked_add(number)?;
        Some(slab)
    }

    /// Creates a new slab by subtracting the specified number from this slab's
    /// ID.
    ///
    /// This operation preserves the segment ID and size while moving
    /// the slab ID backward by the specified amount.
    ///
    /// # Arguments
    ///
    /// * `number` - The number to subtract from this slab's ID
    ///
    /// # Returns
    ///
    /// [`Some(Slab)`] with the new ID if the subtraction doesn't underflow,
    /// [`None`] if the subtraction would underflow.
    #[must_use]
    pub fn sub(&self, number: u32) -> Option<Slab> {
        let mut slab = self.clone();
        slab.id = self.id.checked_sub(number)?;
        Some(slab)
    }

    /// Creates the next slab chronologically after this one.
    ///
    /// This is equivalent to calling `self.add(1)` and represents the
    /// slab immediately following this one in time.
    ///
    /// # Returns
    ///
    /// [`Some(Slab)`] representing the next slab, or [`None`] if this
    /// is the maximum possible slab ID.
    #[must_use]
    pub fn next(&self) -> Option<Slab> {
        self.add(1)
    }

    /// Creates the previous slab chronologically before this one.
    ///
    /// This is equivalent to calling `self.sub(1)` and represents the
    /// slab immediately preceding this one in time.
    ///
    /// # Returns
    ///
    /// [`Some(Slab)`] representing the previous slab, or [`None`] if this
    /// is slab ID 0 (the minimum possible slab ID).
    #[must_use]
    pub fn previous(&self) -> Option<Slab> {
        self.sub(1)
    }
}

impl Debug for Slab {
    /// Formats the slab for debugging purposes.
    ///
    /// The debug output shows the segment ID and slab ID in the format:
    /// `Slab(segment_id/slab_id)`
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Slab({}/{})", self.segment_id, self.id)
    }
}

impl Display for Slab {
    /// Formats the slab for display purposes.
    ///
    /// The display output shows the segment ID, slab ID, and time range in the
    /// format: `segment_id/slab_id[start_time—end_time]`
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
