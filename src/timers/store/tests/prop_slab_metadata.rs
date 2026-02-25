//! Property-based tests for slab metadata table operations.
//!
//! Tests the low-level slab metadata CRUD operations in isolation using a
//! simple reference model to verify correctness.
//!
//! Each store instance is scoped to exactly one segment. Slab operations
//! (`insert_slab`, `get_slabs`, `get_slab_range`, `delete_slab`) all operate
//! on that single segment's slab index.

use crate::timers::duration::CompactDuration;
use crate::timers::slab::{Slab, SlabId};
use crate::timers::store::operations::TriggerOperations;
use futures::TryStreamExt;
use quickcheck::{Arbitrary, Gen};
use std::collections::BTreeSet;
use std::error::Error;
use std::fmt::Debug;
use std::ops::RangeInclusive;

/// Operations that can be performed on the slab metadata table.
#[derive(Clone, Debug)]
pub enum SlabMetadataOperation {
    /// Insert a slab for the store's own segment.
    InsertSlab {
        /// The slab to insert.
        slab_id: SlabId,
    },
    /// Retrieve all slab IDs for the store's own segment.
    GetSlabs,
    /// Retrieve slab IDs in a range for the store's own segment.
    GetSlabRange {
        /// The range of slab IDs to retrieve.
        range: RangeInclusive<SlabId>,
    },
    /// Delete a slab from the store's own segment.
    DeleteSlab {
        /// The slab ID to delete.
        slab_id: SlabId,
    },
}

/// Test input containing a sequence of single-segment slab operations.
///
/// Each test trial uses a fresh store instance for isolation. All operations
/// target the single segment owned by the store.
#[derive(Clone, Debug)]
pub struct SlabMetadataTestInput {
    /// Sequence of operations to apply.
    pub operations: Vec<SlabMetadataOperation>,
    /// Slab size used for all slabs in this test.
    pub slab_size: CompactDuration,
}

impl Arbitrary for SlabMetadataTestInput {
    fn arbitrary(g: &mut Gen) -> Self {
        // Generate a slab size for this test (1 second to 7 days to avoid TTL overflow)
        let slab_size = CompactDuration::new(u32::arbitrary(g).clamp(1, 604_800));

        // Generate 10-50 operations using these segments
        let op_count = (usize::arbitrary(g) % 40) + 10;
        let mut operations = Vec::with_capacity(op_count);

        for _ in 0..op_count {
            // Use small slab IDs for better collision testing
            let slab_id = SlabId::from(u8::arbitrary(g) % 10);

            let op = match u8::arbitrary(g) % 4 {
                0 => SlabMetadataOperation::InsertSlab { slab_id },
                1 => SlabMetadataOperation::GetSlabs,
                2 => {
                    // GetSlabRange operation - use small ranges
                    let start = SlabId::from(u8::arbitrary(g) % 10);
                    let end = start + SlabId::from(u8::arbitrary(g) % 5);
                    SlabMetadataOperation::GetSlabRange { range: start..=end }
                }
                _ => SlabMetadataOperation::DeleteSlab { slab_id },
            };
            operations.push(op);
        }

        Self {
            operations,
            slab_size,
        }
    }
}

/// Reference model for slab metadata table behavior.
///
/// Tracks which slab IDs exist for the single segment. [`BTreeSet`] provides
/// natural ordering and set semantics matching the store's sorted output.
#[derive(Clone, Debug, Default)]
pub struct SlabMetadataModel {
    slabs: BTreeSet<SlabId>,
}

impl SlabMetadataModel {
    /// Creates a new empty slab metadata model.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Applies an operation to the model.
    pub fn apply(&mut self, op: &SlabMetadataOperation) {
        match op {
            SlabMetadataOperation::InsertSlab { slab_id } => {
                self.slabs.insert(*slab_id);
            }
            SlabMetadataOperation::GetSlabs | SlabMetadataOperation::GetSlabRange { .. } => {
                // Queries don't modify state
            }
            SlabMetadataOperation::DeleteSlab { slab_id } => {
                self.slabs.remove(slab_id);
            }
        }
    }

    /// Gets all slab IDs in ascending order.
    #[must_use]
    pub fn get_slabs(&self) -> Vec<SlabId> {
        self.slabs.iter().copied().collect()
    }

    /// Gets slab IDs in a range in ascending order.
    #[must_use]
    pub fn get_slab_range(&self, range: &RangeInclusive<SlabId>) -> Vec<SlabId> {
        self.slabs
            .iter()
            .copied()
            .filter(|id| range.contains(id))
            .collect()
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
    range: RangeInclusive<SlabId>,
    op_idx: usize,
) -> color_eyre::Result<()>
where
    T: TriggerOperations + Send + Sync,
    T::Error: Error + Send + Sync + 'static,
{
    let model_range = model.get_slab_range(&range);
    let store_range: Vec<SlabId> = operations
        .get_slab_range(range.clone())
        .try_collect()
        .await
        .map_err(|e| color_eyre::eyre::eyre!("Op #{op_idx} GetSlabRange failed: {e:?}"))?;

    if model_range != store_range {
        return Err(color_eyre::eyre::eyre!(
            "Op #{op_idx} Slab range mismatch for range {range:?}: expected {model_range:?}, got \
             {store_range:?}"
        ));
    }

    // Verify all returned IDs are in range
    for id in &store_range {
        if !range.contains(id) {
            return Err(color_eyre::eyre::eyre!(
                "Op #{op_idx} Slab ID {id} is outside range {range:?}"
            ));
        }
    }

    // Verify ordering in range query
    for window in store_range.windows(2) {
        if window[0] >= window[1] {
            return Err(color_eyre::eyre::eyre!(
                "Op #{op_idx} Slab ordering violation in range {range:?}: {:?} >= {:?}",
                window[0],
                window[1]
            ));
        }
    }

    Ok(())
}

/// Verifies `GetSlabs` query against model, including ordering.
async fn verify_get_slabs_query<T>(
    operations: &T,
    model: &SlabMetadataModel,
    op_idx: usize,
) -> color_eyre::Result<()>
where
    T: TriggerOperations + Send + Sync,
    T::Error: Error + Send + Sync + 'static,
{
    let expected = model.get_slabs();
    let actual: Vec<SlabId> = operations
        .get_slabs()
        .try_collect()
        .await
        .map_err(|e| color_eyre::eyre::eyre!("Op #{op_idx} GetSlabs failed: {e:?}"))?;

    if expected != actual {
        return Err(color_eyre::eyre::eyre!(
            "Op #{op_idx} GetSlabs mismatch: expected {expected:?}, got {actual:?}"
        ));
    }

    // Verify ordering
    for window in actual.windows(2) {
        if window[0] >= window[1] {
            return Err(color_eyre::eyre::eyre!(
                "Op #{op_idx} GetSlabs ordering violation: {window:?}"
            ));
        }
    }
    Ok(())
}

/// Verifies final state matches model.
async fn verify_final_slab_state<T>(
    operations: &T,
    model: &SlabMetadataModel,
) -> color_eyre::Result<()>
where
    T: TriggerOperations + Send + Sync,
    T::Error: Error + Send + Sync + 'static,
{
    // Verify get_slabs matches
    let model_slabs = model.get_slabs();
    let store_slabs: Vec<SlabId> = operations
        .get_slabs()
        .try_collect()
        .await
        .map_err(|e| color_eyre::eyre::eyre!("Final get_slabs failed: {e:?}"))?;

    if model_slabs != store_slabs {
        return Err(color_eyre::eyre::eyre!(
            "Final slab list mismatch: expected {:?}, got {:?}",
            model_slabs,
            store_slabs
        ));
    }

    // Verify ordering (should be ascending)
    for window in store_slabs.windows(2) {
        if window[0] >= window[1] {
            return Err(color_eyre::eyre::eyre!(
                "Final slab ordering violation: {:?} >= {:?}",
                window[0],
                window[1]
            ));
        }
    }

    // Verify range queries for a few sample ranges
    for range in [0..=5u32, 3..=8, 5..=15] {
        verify_slab_range(operations, model, range, usize::MAX).await?;
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
    let segment = operations.segment().clone();

    // Clean up slabs from any previous trial
    let slab_ids: Vec<SlabId> = operations
        .get_slabs()
        .try_collect()
        .await
        .map_err(|e| color_eyre::eyre::eyre!("Failed to get slabs during cleanup: {e:?}"))?;
    for slab_id in slab_ids {
        operations
            .delete_slab(slab_id)
            .await
            .map_err(|e| color_eyre::eyre::eyre!("Failed to delete slab during cleanup: {e:?}"))?;
    }

    let mut model = SlabMetadataModel::new();

    // Apply all operations to both store and model, verifying queries inline
    for (op_idx, op) in input.operations.iter().enumerate() {
        match op {
            SlabMetadataOperation::InsertSlab { slab_id } => {
                model.apply(op);
                let slab = Slab::new(segment.id, *slab_id, input.slab_size);
                operations.insert_slab(slab).await.map_err(|e| {
                    color_eyre::eyre::eyre!("Op #{op_idx} InsertSlab failed: {e:?}")
                })?;
            }
            SlabMetadataOperation::GetSlabs => {
                verify_get_slabs_query(operations, &model, op_idx).await?;
            }
            SlabMetadataOperation::GetSlabRange { range } => {
                verify_slab_range(operations, &model, range.clone(), op_idx).await?;
            }
            SlabMetadataOperation::DeleteSlab { slab_id } => {
                model.apply(op);
                operations.delete_slab(*slab_id).await.map_err(|e| {
                    color_eyre::eyre::eyre!("Op #{op_idx} DeleteSlab failed: {e:?}")
                })?;
            }
        }
    }

    // Final sanity check: verify model-store equivalence
    verify_final_slab_state(operations, &model).await
}
