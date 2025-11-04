//! Property-based tests for v1 slab metadata table operations.
//!
//! Tests the low-level v1 slab metadata CRUD operations in isolation using a
//! simple reference model to verify correctness. V1 has simpler slab metadata
//! without slab size information.

use crate::timers::slab::SlabId;
use crate::timers::store::SegmentId;
use crate::timers::store::cassandra::v1::V1Operations;
use ahash::HashMap;
use futures::TryStreamExt;
use quickcheck::{Arbitrary, Gen, TestResult};
use std::collections::BTreeSet;
use uuid::Uuid;

/// Operations that can be performed on the v1 slab metadata table.
#[derive(Clone, Debug)]
pub enum V1SlabMetadataOperation {
    /// Insert a slab ID for a segment.
    InsertSlab {
        /// The segment ID.
        segment_id: SegmentId,
        /// The slab ID to insert.
        slab_id: SlabId,
    },
    /// Retrieve all slab IDs for a segment.
    GetSlabs(SegmentId),
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
pub struct V1SlabMetadataTestInput {
    /// Pool of segment IDs used by operations in this trial.
    pub segment_ids: Vec<SegmentId>,
    /// Sequence of operations to apply.
    pub operations: Vec<V1SlabMetadataOperation>,
}

impl Arbitrary for V1SlabMetadataTestInput {
    fn arbitrary(g: &mut Gen) -> Self {
        // Generate 3 random v4 UUIDs unique to this trial
        // v4 UUIDs have a structure that prevents QuickCheck from shrinking them
        // into colliding values
        let segment_ids = vec![Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];

        // Generate 10-50 operations using these segments
        let op_count = (usize::arbitrary(g) % 40) + 10;
        let mut operations = Vec::with_capacity(op_count);

        for _ in 0..op_count {
            let idx = usize::from(u8::arbitrary(g)) % segment_ids.len();
            let segment_id = segment_ids[idx];

            // Use small slab IDs for better collision testing
            let slab_id = SlabId::from(u8::arbitrary(g) % 10);

            let op = match u8::arbitrary(g) % 3 {
                0 => V1SlabMetadataOperation::InsertSlab {
                    segment_id,
                    slab_id,
                },
                1 => V1SlabMetadataOperation::GetSlabs(segment_id),
                _ => V1SlabMetadataOperation::DeleteSlab {
                    segment_id,
                    slab_id,
                },
            };
            operations.push(op);
        }

        Self {
            segment_ids,
            operations,
        }
    }
}

/// Reference model for v1 slab metadata table behavior.
///
/// Uses a simple `HashMap<SegmentId, BTreeSet<SlabId>>` to track which
/// slab IDs exist for each segment. [`BTreeSet`] provides natural ordering
/// and set semantics.
#[derive(Clone, Debug)]
pub struct V1SlabMetadataModel {
    slabs: HashMap<SegmentId, BTreeSet<SlabId>>,
}

impl Default for V1SlabMetadataModel {
    fn default() -> Self {
        Self::new()
    }
}

impl V1SlabMetadataModel {
    /// Creates a new empty v1 slab metadata model.
    #[must_use]
    pub fn new() -> Self {
        Self {
            slabs: HashMap::default(),
        }
    }

    /// Applies an operation to the model.
    pub fn apply(&mut self, op: &V1SlabMetadataOperation) {
        match op {
            V1SlabMetadataOperation::InsertSlab {
                segment_id,
                slab_id,
            } => {
                self.slabs.entry(*segment_id).or_default().insert(*slab_id);
            }
            V1SlabMetadataOperation::GetSlabs(_) => {
                // Queries don't modify state
            }
            V1SlabMetadataOperation::DeleteSlab {
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

    /// Returns all segment IDs in the model.
    #[must_use]
    pub fn all_segment_ids(&self) -> Vec<SegmentId> {
        self.slabs.keys().copied().collect()
    }
}

/// Verifies that v1 slab metadata operations match the reference model
/// behavior.
///
/// # Test Strategy
///
/// 1. Start with empty store and model
/// 2. Apply sequence of random operations to both
/// 3. Verify that for every segment ID:
///    - `store.get_slabs(seg)` matches `model.get_slabs(seg)`
///    - Ordering is correct (ascending slab IDs)
///
/// # Errors
///
/// Returns an error if:
/// - Store operations fail (insert, delete, get)
/// - Store state doesn't match model state
/// - Ordering invariants are violated
pub async fn prop_v1_slab_metadata_model_equivalence(
    operations: &V1Operations,
    input: V1SlabMetadataTestInput,
) -> color_eyre::Result<()> {
    // Clean up slabs from this trial to ensure isolation
    // Even with unique v4 UUIDs, cleanup prevents test pollution if trials fail and
    // rerun
    for segment_id in &input.segment_ids {
        // Delete all possible slab IDs from segments table (match range in Arbitrary)
        for slab_id in 0..10 {
            operations
                .delete_slab_metadata(segment_id, slab_id)
                .await
                .map_err(|e| {
                    color_eyre::eyre::eyre!(
                        "Failed to delete v1 slab metadata during cleanup: {e:?}"
                    )
                })?;
        }
    }

    let mut model = V1SlabMetadataModel::new();

    // Apply all operations to both operations and model, verifying queries inline
    for (op_idx, op) in input.operations.iter().enumerate() {
        match op {
            V1SlabMetadataOperation::InsertSlab {
                segment_id,
                slab_id,
            } => {
                model.apply(op);
                operations
                    .insert_slab(segment_id, *slab_id)
                    .await
                    .map_err(|e| {
                        color_eyre::eyre::eyre!("Op #{op_idx} Insert v1 slab failed: {e:?}")
                    })?;
            }
            V1SlabMetadataOperation::GetSlabs(segment_id) => {
                // Verify query immediately against model
                let expected = model.get_slabs(segment_id);
                let actual: Vec<SlabId> = operations
                    .get_slabs(segment_id)
                    .try_collect()
                    .await
                    .map_err(|e| {
                        color_eyre::eyre::eyre!("Op #{op_idx} GetSlabs v1 failed: {e:?}")
                    })?;

                if expected != actual {
                    return Err(color_eyre::eyre::eyre!(
                        "Op #{op_idx} GetSlabs v1 query mismatch for segment {segment_id}: \
                         expected {expected:?}, got {actual:?}"
                    ));
                }

                // Verify ordering
                for window in actual.windows(2) {
                    if window[0] >= window[1] {
                        return Err(color_eyre::eyre::eyre!(
                            "Op #{op_idx} GetSlabs v1 ordering violation for segment \
                             {segment_id}: {window:?}"
                        ));
                    }
                }
            }
            V1SlabMetadataOperation::DeleteSlab {
                segment_id,
                slab_id,
            } => {
                model.apply(op);
                operations
                    .delete_slab_metadata(segment_id, *slab_id)
                    .await
                    .map_err(|e| {
                        color_eyre::eyre::eyre!(
                            "Op #{op_idx} Delete v1 slab metadata failed: {e:?}"
                        )
                    })?;
            }
        }
    }

    // Final sanity check: verify model-operations equivalence for all segment IDs
    // (queries were already verified inline, this catches any missed state)
    let all_segment_ids: Vec<SegmentId> = model.all_segment_ids();

    for segment_id in &all_segment_ids {
        // Verify get_slabs_v1 matches
        let model_slabs = model.get_slabs(segment_id);
        let operations_slabs: Vec<SlabId> = operations
            .get_slabs(segment_id)
            .try_collect()
            .await
            .map_err(|e| color_eyre::eyre::eyre!("Get v1 slabs failed: {e:?}"))?;

        if model_slabs != operations_slabs {
            return Err(color_eyre::eyre::eyre!(
                "V1 slab list mismatch for segment {}: expected {:?}, got {:?}",
                segment_id,
                model_slabs,
                operations_slabs
            ));
        }

        // Verify ordering (should be ascending)
        for window in operations_slabs.windows(2) {
            if window[0] >= window[1] {
                return Err(color_eyre::eyre::eyre!(
                    "V1 slab ordering violation for segment {}: {:?} >= {:?}",
                    segment_id,
                    window[0],
                    window[1]
                ));
            }
        }
    }

    Ok(())
}

/// [`QuickCheck`] wrapper for v1 slab metadata model equivalence property.
pub fn test_prop_v1_slab_metadata_model_equivalence(
    operations: &V1Operations,
    input: V1SlabMetadataTestInput,
) -> TestResult {
    use tokio::runtime::Runtime;

    let Ok(rt) = Runtime::new() else {
        return TestResult::error("Failed to create tokio runtime");
    };

    let result =
        rt.block_on(async { prop_v1_slab_metadata_model_equivalence(operations, input).await });

    match result {
        Ok(()) => TestResult::passed(),
        Err(e) => TestResult::error(format!("{e:?}")),
    }
}
