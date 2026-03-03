//! Time-based partitioning for efficient timer storage and retrieval.
//!
//! Defines the [`Slab`] type and its operations. A [`Slab`] is a fixed-duration
//! time window. Timers whose execution times fall into the same window are
//! grouped into the same slab, enabling efficient range queries and storage
//! organization.
//!
//! Slab calculations use `slab_id = floor(epoch_seconds / slab_size_seconds)`
//! to partition time. Slabs implement [`Ord`] and [`PartialOrd`], ordering
//! by slab ID.

use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use std::fmt::{Debug, Display, Formatter, Result as FmtResult};
use std::ops::Range;

/// Unique identifier for a time-based slab.
pub type SlabId = u32;

/// A time-based partition of timer data.
///
/// Groups all timers whose execution times fall within the same fixed-duration
/// window. This partitioning allows fast loading, unloading, and querying of
/// timers by time ranges.
#[derive(Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct Slab {
    id: SlabId,
    size: CompactDuration,
}

impl Slab {
    /// Creates a new slab with explicit parameters.
    ///
    /// # Arguments
    ///
    /// * `id` - The numeric slab identifier.
    /// * `size` - The duration each slab covers.
    #[must_use]
    pub fn new(id: SlabId, size: CompactDuration) -> Self {
        Slab { id, size }
    }

    /// Calculates which slab contains the specified time.
    ///
    /// # Arguments
    ///
    /// * `size` - The duration each slab covers.
    /// * `time` - The timestamp to locate.
    ///
    /// # Returns
    ///
    /// A [`Slab`] whose time range includes `time`. If `size.seconds() == 0`,
    /// returns slab ID 0 to avoid division by zero.
    #[must_use]
    pub fn from_time(size: CompactDuration, time: CompactDateTime) -> Self {
        let epoch_secs = time.epoch_seconds();
        let slab_secs = size.seconds();

        // Compute slab ID using saturating division for safety.
        let id: SlabId = if slab_secs == 0 {
            0
        } else {
            epoch_secs.saturating_div(slab_secs)
        };

        Slab { id, size }
    }

    /// Returns this slab's numeric identifier.
    #[must_use]
    pub fn id(&self) -> SlabId {
        self.id
    }

    /// Returns the duration each slab covers.
    #[must_use]
    pub fn size(&self) -> CompactDuration {
        self.size
    }

    /// Returns the time range covered by this slab.
    ///
    /// The range starts at `id * size` (inclusive) and extends to
    /// `start + size` (exclusive).
    #[must_use]
    pub fn range(&self) -> Range<CompactDateTime> {
        let size = self.size.seconds();
        let start = self.id.saturating_mul(size);
        let end = start.saturating_add(size);

        start.into()..end.into()
    }

    /// Advances the slab ID by the given amount.
    ///
    /// # Arguments
    ///
    /// * `number` - Amount to add to the current slab ID.
    ///
    /// # Returns
    ///
    /// - `Some(Slab)` with `id = self.id + number` if no overflow occurs.
    /// - `None` if the addition would overflow [`u32`].
    #[must_use]
    pub fn add(&self, number: u32) -> Option<Slab> {
        let mut slab = self.clone();
        slab.id = self.id.checked_add(number)?;
        Some(slab)
    }

    /// Moves the slab ID backward by the given amount.
    ///
    /// # Arguments
    ///
    /// * `number` - Amount to subtract from the current slab ID.
    ///
    /// # Returns
    ///
    /// - `Some(Slab)` with `id = self.id - number` if no underflow occurs.
    /// - `None` if the subtraction would underflow [`u32`].
    #[must_use]
    pub fn sub(&self, number: u32) -> Option<Slab> {
        let mut slab = self.clone();
        slab.id = self.id.checked_sub(number)?;
        Some(slab)
    }

    /// Returns the slab immediately following this one.
    ///
    /// Equivalent to `self.add(1)`.
    #[must_use]
    pub fn next(&self) -> Option<Slab> {
        self.add(1)
    }

    /// Returns the slab immediately preceding this one.
    ///
    /// Equivalent to `self.sub(1)`.
    #[must_use]
    pub fn previous(&self) -> Option<Slab> {
        self.sub(1)
    }
}

impl Debug for Slab {
    /// Debug format: `Slab(42)`.
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "Slab({})", self.id)
    }
}

impl Display for Slab {
    /// Display format: `42[180—240]`.
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        let range = self.range();
        write!(f, "{}[{}—{}]", self.id, range.start, range.end)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::timers::datetime::CompactDateTime;
    use crate::timers::duration::CompactDuration;

    #[test]
    fn test_slab_new() {
        let slab_id = 42;
        let size = CompactDuration::new(60); // 60 seconds

        let slab = Slab::new(slab_id, size);

        assert_eq!(slab.id(), slab_id);
        assert_eq!(slab.size(), size);
    }

    #[test]
    fn test_slab_from_time() {
        let size = CompactDuration::new(60); // 60 seconds
        let time = CompactDateTime::from(123_i32); // 123 seconds since epoch

        let slab = Slab::from_time(size, time);

        assert_eq!(slab.size(), size);
        assert_eq!(slab.id(), 2); // 123 / 60 = 2
    }

    #[test]
    fn test_slab_from_time_zero_size() {
        let size = CompactDuration::new(0); // Zero duration
        let time = CompactDateTime::from(123_i32); // 123 seconds since epoch

        let slab = Slab::from_time(size, time);

        assert_eq!(slab.size(), size);
        assert_eq!(slab.id(), 0); // Slab ID should default to 0 for zero size
    }

    #[test]
    fn test_slab_range() {
        let slab_id = 3;
        let size = CompactDuration::new(60); // 60 seconds

        let slab = Slab::new(slab_id, size);
        let range = slab.range();

        assert_eq!(range.start.epoch_seconds(), 180); // 3 * 60 = 180
        assert_eq!(range.end.epoch_seconds(), 240); // 180 + 60 = 240
    }

    #[test]
    fn test_slab_range_zero_size() {
        let slab_id = 3;
        let size = CompactDuration::new(0); // Zero duration

        let slab = Slab::new(slab_id, size);
        let range = slab.range();

        assert_eq!(range.start.epoch_seconds(), 0); // Start should be 0
        assert_eq!(range.end.epoch_seconds(), 0); // End should also be 0
    }

    #[test]
    fn test_slab_debug() {
        let slab_id = 42;
        let size = CompactDuration::new(60); // 60 seconds

        let slab = Slab::new(slab_id, size);
        let debug_str = format!("{slab:?}");

        assert_eq!(debug_str, format!("Slab({slab_id})"));
    }

    #[test]
    fn test_slab_display() {
        let slab_id = 3;
        let size = CompactDuration::new(60); // 60 seconds

        let slab = Slab::new(slab_id, size);
        let display_str = format!("{slab}");

        assert_eq!(
            display_str,
            format!(
                "{slab_id}[{}—{}]",
                CompactDateTime::from(180_i32), // Start of range
                CompactDateTime::from(240_i32)  // End of range
            )
        );
    }

    #[test]
    fn test_slab_equality() {
        let size = CompactDuration::new(60); // 60 seconds

        let slab1 = Slab::new(1, size);
        let slab2 = Slab::new(1, size);
        let slab3 = Slab::new(2, size);

        assert_eq!(slab1, slab2);
        assert_ne!(slab1, slab3);
    }

    #[test]
    fn test_slab_ordering() {
        let size = CompactDuration::new(60); // 60 seconds

        let slab1 = Slab::new(1, size);
        let slab2 = Slab::new(2, size);

        assert!(slab1 < slab2);
        assert!(slab2 > slab1);
    }
}
