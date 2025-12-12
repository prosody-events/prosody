//! Property-based tests for slab metadata table operations.
//!
//! Tests the low-level slab metadata CRUD operations in isolation using a
//! simple reference model to verify correctness.

use crate::timers::duration::CompactDuration;
use crate::timers::slab::{Slab, SlabId};
use crate::timers::store::SegmentId;
use crate::timers::store::operations::TriggerOperations;
use ahash::HashMap;
use futures::TryStreamExt;
use quickcheck::{Arbitrary, Gen};
use std::collections::BTreeSet;
use std::error::Error;
use std::fmt::Debug;
use std::ops::RangeInclusive;
use uuid::Uuid;

/// Operations that can be performed on the slab metadata table.
#[derive(Clone, Debug)]
pub enum SlabMetadataOperation {
    /// Insert a slab for a segment.
    InsertSlab {
        /// The segment ID.
        segment_id: SegmentId,
        /// The slab to insert.
        slab: Slab,
    },
    /// Retrieve all slab IDs for a segment.
    GetSlabs(SegmentId),
    /// Retrieve slab IDs in a range for a segment.
    GetSlabRange {
        /// The segment ID.
        segment_id: SegmentId,
        /// The range of slab IDs to retrieve.
        range: RangeInclusive<SlabId>,
    },
    /// Delete a slab from a segment.
    DeleteSlab {
        /// The segment ID.
        segment_id: SegmentId,
        /// The slab ID to delete.
        slab_id: SlabId,
    },
}

/// Test input containing isolated segment IDs and operations.
///
/// Each test trial uses randomly generated segment IDs, ensuring complete
/// isolation between trials and allowing parallel test execution.
#[derive(Clone, Debug)]
pub struct SlabMetadataTestInput {
    /// Pool of segment IDs used by operations in this trial.
    pub segment_ids: Vec<SegmentId>,
    /// Sequence of operations to apply.
    pub operations: Vec<SlabMetadataOperation>,
    /// Slab size used for all slabs in this test.
    pub slab_size: CompactDuration,
}

impl Arbitrary for SlabMetadataTestInput {
    fn arbitrary(g: &mut Gen) -> Self {
        // Generate 3 random v4 UUIDs unique to this trial
        // v4 UUIDs have a structure that prevents QuickCheck from shrinking them
        // into colliding values
        let segment_ids = vec![Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];

        // Generate a slab size for this test (1 second to 7 days to avoid TTL overflow)
        let slab_size = CompactDuration::new(u32::arbitrary(g).clamp(1, 604_800));

        // Generate 10-50 operations using these segments
        let op_count = (usize::arbitrary(g) % 40) + 10;
        let mut operations = Vec::with_capacity(op_count);

        for _ in 0..op_count {
            let idx = usize::from(u8::arbitrary(g)) % segment_ids.len();
            let segment_id = segment_ids[idx];

            // Use small slab IDs for better collision testing
            let slab_id = SlabId::from(u8::arbitrary(g) % 10);

            let op = match u8::arbitrary(g) % 4 {
                0 => {
                    // Insert operation
                    let slab = Slab::new(segment_id, slab_id, slab_size);
                    SlabMetadataOperation::InsertSlab { segment_id, slab }
                }
                1 => SlabMetadataOperation::GetSlabs(segment_id),
                2 => {
                    // GetSlabRange operation - use small ranges
                    let start = SlabId::from(u8::arbitrary(g) % 10);
                    let end = start + SlabId::from(u8::arbitrary(g) % 5);
                    SlabMetadataOperation::GetSlabRange {
                        segment_id,
                        range: start..=end,
                    }
                }
                _ => SlabMetadataOperation::DeleteSlab {
                    segment_id,
                    slab_id,
                },
            };
            operations.push(op);
        }

        Self {
            segment_ids,
            operations,
            slab_size,
        }
    }
}

/// Reference model for slab metadata table behavior.
///
/// Uses a simple `HashMap<SegmentId, BTreeSet<SlabId>>` to track which
/// slab IDs exist for each segment. [`BTreeSet`] provides natural ordering
/// and set semantics.
#[derive(Clone, Debug)]
pub struct SlabMetadataModel {
    slabs: HashMap<SegmentId, BTreeSet<SlabId>>,
}

impl Default for SlabMetadataModel {
    fn default() -> Self {
        Self::new()
    }
}

impl SlabMetadataModel {
    /// Creates a new empty slab metadata model.
    #[must_use]
    pub fn new() -> Self {
        Self {
            slabs: HashMap::default(),
        }
    }

    /// Applies an operation to the model.
    pub fn apply(&mut self, op: &SlabMetadataOperation) {
        match op {
            SlabMetadataOperation::InsertSlab { segment_id, slab } => {
                self.slabs.entry(*segment_id).or_default().insert(slab.id());
            }
            SlabMetadataOperation::GetSlabs(_) | SlabMetadataOperation::GetSlabRange { .. } => {
                // Queries don't modify state
            }
            SlabMetadataOperation::DeleteSlab {
                segment_id,
                slab_id,
            } => {
                if let Some(set) = self.slabs.get_mut(segment_id) {
                    set.remove(slab_id);
                }
            }
        }
    }

    /// Gets all slab IDs for a segment in ascending order.
    #[must_use]
    pub fn get_slabs(&self, segment_id: &SegmentId) -> Vec<SlabId> {
        self.slabs
            .get(segment_id)
            .map(|set| set.iter().copied().collect())
            .unwrap_or_default()
    }

    /// Gets slab IDs in a range for a segment in ascending order.
    #[must_use]
    pub fn get_slab_range(
        &self,
        segment_id: &SegmentId,
        range: &RangeInclusive<SlabId>,
    ) -> Vec<SlabId> {
        self.slabs
            .get(segment_id)
            .map(|set| {
                set.iter()
                    .copied()
                    .filter(|id| range.contains(id))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Returns all segment IDs in the model.
    #[must_use]
    pub fn all_segment_ids(&self) -> Vec<SegmentId> {
        self.slabs.keys().copied().collect()
    }
}

/// Verifies a single slab range query against the model.
///
/// # Errors
///
/// Returns an error if:
/// - Range query fails
/// - Returned IDs don't match model
/// - Any ID is outside the requested range
/// - Ordering is incorrect
async fn verify_slab_range<T>(
    operations: &T,
    model: &SlabMetadataModel,
    segment_id: &SegmentId,
    range: RangeInclusive<SlabId>,
) -> color_eyre::Result<()>
where
    T: TriggerOperations + Send + Sync,
    T::Error: Error + Send + Sync + 'static,
{
    let model_range = model.get_slab_range(segment_id, &range);
    let store_range: Vec<SlabId> = operations
        .get_slab_range(segment_id, range.clone())
        .try_collect()
        .await
        .map_err(|e| color_eyre::eyre::eyre!("Get slab range failed: {e:?}"))?;

    if model_range != store_range {
        return Err(color_eyre::eyre::eyre!(
            "Slab range mismatch for segment {segment_id} range {range:?}: expected \
             {model_range:?}, got {store_range:?}"
        ));
    }

    // Verify all returned IDs are in range
    for id in &store_range {
        if !range.contains(id) {
            return Err(color_eyre::eyre::eyre!(
                "Slab ID {id} is outside range {range:?} for segment {segment_id}"
            ));
        }
    }

    // Verify ordering in range query
    for window in store_range.windows(2) {
        if window[0] >= window[1] {
            return Err(color_eyre::eyre::eyre!(
                "Slab ordering violation in range {range:?} for segment {segment_id}: {:?} >= {:?}",
                window[0],
                window[1]
            ));
        }
    }

    Ok(())
}

/// Verifies that slab metadata operations match the reference model behavior.
///
/// # Test Strategy
///
/// 1. Start with empty store and model
/// 2. Apply sequence of random operations to both
/// 3. Verify that for every segment ID:
///    - `operations.get_slabs(seg)` matches `model.get_slabs(seg)`
///    - Ordering is correct (ascending slab IDs)
///    - Range queries return correct subset
///
/// Cleans up all slabs for the given segment IDs.
async fn cleanup_segment_slabs<T>(
    operations: &T,
    segment_ids: &[SegmentId],
) -> color_eyre::Result<()>
where
    T: TriggerOperations + Send + Sync,
    T::Error: Error + Send + Sync + 'static,
{
    for segment_id in segment_ids {
        let slab_ids: Vec<SlabId> = operations
            .get_slabs(segment_id)
            .try_collect()
            .await
            .map_err(|e| color_eyre::eyre::eyre!("Failed to get slabs during cleanup: {e:?}"))?;

        for slab_id in slab_ids {
            operations
                .delete_slab(segment_id, slab_id)
                .await
                .map_err(|e| {
                    color_eyre::eyre::eyre!("Failed to delete slab during cleanup: {e:?}")
                })?;
        }
    }
    Ok(())
}

/// Verifies `GetSlabs` query against model, including ordering.
async fn verify_get_slabs_query<T>(
    operations: &T,
    model: &SlabMetadataModel,
    segment_id: &SegmentId,
    op_idx: usize,
) -> color_eyre::Result<()>
where
    T: TriggerOperations + Send + Sync,
    T::Error: Error + Send + Sync + 'static,
{
    let expected = model.get_slabs(segment_id);
    let actual: Vec<SlabId> = operations
        .get_slabs(segment_id)
        .try_collect()
        .await
        .map_err(|e| color_eyre::eyre::eyre!("Op #{op_idx} GetSlabs failed: {e:?}"))?;

    if expected != actual {
        return Err(color_eyre::eyre::eyre!(
            "Op #{op_idx} GetSlabs query mismatch for segment {segment_id}: expected \
             {expected:?}, got {actual:?}"
        ));
    }

    // Verify ordering
    for window in actual.windows(2) {
        if window[0] >= window[1] {
            return Err(color_eyre::eyre::eyre!(
                "Op #{op_idx} GetSlabs ordering violation for segment {segment_id}: {window:?}"
            ));
        }
    }
    Ok(())
}

/// Verifies final state matches model for all segments.
async fn verify_final_slab_state<T>(
    operations: &T,
    model: &SlabMetadataModel,
    all_segment_ids: &[SegmentId],
) -> color_eyre::Result<()>
where
    T: TriggerOperations + Send + Sync,
    T::Error: Error + Send + Sync + 'static,
{
    for segment_id in all_segment_ids {
        // Verify get_slabs matches
        let model_slabs = model.get_slabs(segment_id);
        let store_slabs: Vec<SlabId> = operations
            .get_slabs(segment_id)
            .try_collect()
            .await
            .map_err(|e| color_eyre::eyre::eyre!("Get slabs failed: {e:?}"))?;

        if model_slabs != store_slabs {
            return Err(color_eyre::eyre::eyre!(
                "Slab list mismatch for segment {}: expected {:?}, got {:?}",
                segment_id,
                model_slabs,
                store_slabs
            ));
        }

        // Verify ordering (should be ascending)
        for window in store_slabs.windows(2) {
            if window[0] >= window[1] {
                return Err(color_eyre::eyre::eyre!(
                    "Slab ordering violation for segment {}: {:?} >= {:?}",
                    segment_id,
                    window[0],
                    window[1]
                ));
            }
        }
    }

    // Verify range queries for a few sample ranges
    for segment_id in all_segment_ids {
        for range in [0..=5, 3..=8, 5..=15] {
            verify_slab_range(operations, model, segment_id, range).await?;
        }
    }

    Ok(())
}

/// # Errors
///
/// Returns an error if:
/// - Store operations fail (insert, delete, get)
/// - Store state doesn't match model state
/// - Ordering invariants are violated
pub async fn prop_slab_metadata_model_equivalence<T>(
    operations: &T,
    input: SlabMetadataTestInput,
) -> color_eyre::Result<()>
where
    T: TriggerOperations + Send + Sync,
    T::Error: Error + Send + Sync + 'static,
{
    // Clean up slabs from this trial to ensure isolation
    cleanup_segment_slabs(operations, &input.segment_ids).await?;

    let mut model = SlabMetadataModel::new();

    // Apply all operations to both store and model, verifying queries inline
    for (op_idx, op) in input.operations.iter().enumerate() {
        match op {
            SlabMetadataOperation::InsertSlab { segment_id, slab } => {
                model.apply(op);
                operations
                    .insert_slab(segment_id, slab.clone())
                    .await
                    .map_err(|e| {
                        color_eyre::eyre::eyre!("Op #{op_idx} Insert slab failed: {e:?}")
                    })?;
            }
            SlabMetadataOperation::GetSlabs(segment_id) => {
                verify_get_slabs_query(operations, &model, segment_id, op_idx).await?;
            }
            SlabMetadataOperation::GetSlabRange { segment_id, range } => {
                verify_slab_range(operations, &model, segment_id, range.clone())
                    .await
                    .map_err(|e| color_eyre::eyre::eyre!("Op #{op_idx} GetSlabRange: {e}"))?;
            }
            SlabMetadataOperation::DeleteSlab {
                segment_id,
                slab_id,
            } => {
                model.apply(op);
                operations
                    .delete_slab(segment_id, *slab_id)
                    .await
                    .map_err(|e| {
                        color_eyre::eyre::eyre!("Op #{op_idx} Delete slab failed: {e:?}")
                    })?;
            }
        }
    }

    // Final sanity check: verify model-store equivalence for all segment IDs
    let all_segment_ids: Vec<SegmentId> = model.all_segment_ids();
    verify_final_slab_state(operations, &model, &all_segment_ids).await
}
