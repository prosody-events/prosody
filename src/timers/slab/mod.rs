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
    /// Creates a new slab with the given ID and size.
    #[must_use]
    pub fn new(id: SlabId, size: CompactDuration) -> Self {
        Slab { id, size }
    }

    /// Calculates which slab contains `time`, for a given slab `size`.
    ///
    /// Returns slab ID `0` if `size.seconds() == 0`, to avoid division by zero.
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

    /// Returns the slab immediately following this one, or `None` if the
    /// slab ID would overflow [`u32`].
    #[must_use]
    pub fn next(&self) -> Option<Slab> {
        Some(Slab {
            id: self.id.checked_add(1)?,
            size: self.size,
        })
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
mod tests;
