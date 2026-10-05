//! Property-based tests for segment table operations.
//!
//! Tests the low-level segment CRUD operations in isolation using a simple
//! reference model to verify correctness.
//!
//! Each store instance is scoped to exactly one segment. The operations
//! (`insert_segment`, `get_segment`, `delete_segment`, the migration markers,
//! and `set_slab_watermark`) all operate on the store's own segment. This test
//! verifies that the store correctly persists, retrieves, updates, and deletes
//! its own segment and slab watermark.

use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::timers::slab::{Slab, SlabId};
use crate::timers::store::operations::TriggerOperations;
use crate::timers::store::{Segment, SegmentVersion, StoredSegment};
use quickcheck::{Arbitrary, Gen};
use std::error::Error;
use std::fmt::Debug;

/// Operations that can be performed on the segment table.
#[derive(Clone, Debug)]
pub enum SegmentOperation {
    /// Insert or update the store's own segment.
    Insert,
    /// Retrieve the store's own segment.
    Get,
    /// Delete the store's own segment.
    Delete,
    /// Update the store's own segment version.
    UpdateVersion(SegmentVersion),
    /// Rewrite the store's own slab size, which clears the watermark.
    UpdateSlabSize,
    /// Set the store's own slab watermark.
    SetWatermark(SlabId),
}

/// Test input containing a sequence of single-segment operations.
///
/// Each test trial uses a fresh store instance, ensuring isolation between
/// trials. All operations target the single segment owned by the store.
#[derive(Clone, Debug)]
pub struct SegmentTestInput {
    /// Sequence of operations to apply.
    pub operations: Vec<SegmentOperation>,
    /// Slab size used for the store's segment in this test.
    pub slab_size: CompactDuration,
}

impl Arbitrary for SegmentTestInput {
    fn arbitrary(g: &mut Gen) -> Self {
        // Draw a slab size from 1 second to 7 days, which avoids TTL overflow.
        // Do not clamp: a clamp maps most draws to 7 days.
        let slab_size = CompactDuration::new(u32::arbitrary(g) % 604_800 + 1);

        // Generate 10-50 operations
        let op_count = (usize::arbitrary(g) % 40) + 10;
        let mut operations = Vec::with_capacity(op_count);

        // Track whether segment has been inserted for valid UpdateVersion ops
        let mut inserted = false;

        for _ in 0..op_count {
            let op = match u8::arbitrary(g) % 6 {
                0 => {
                    inserted = true;
                    SegmentOperation::Insert
                }
                1 => SegmentOperation::Get,
                2 => {
                    inserted = false;
                    SegmentOperation::Delete
                }
                // No segment to update: emit a Get instead.
                _ if !inserted => SegmentOperation::Get,
                3 => SegmentOperation::UpdateVersion(SegmentVersion::V4),
                4 => SegmentOperation::UpdateSlabSize,
                _ => SegmentOperation::SetWatermark(
                    Slab::from_time(slab_size, CompactDateTime::arbitrary(g)).id(),
                ),
            };
            operations.push(op);
        }

        Self {
            operations,
            slab_size,
        }
    }
}

/// Reference model for segment table behavior.
///
/// Tracks whether the store's own segment is present, its current `version`,
/// and its slab watermark. The slab size stays the base slab size.
#[derive(Clone, Debug)]
pub struct SegmentModel {
    /// The base segment from the store (set once at test start).
    base: Segment,
    /// Whether the segment is currently present in the store.
    present: bool,
    /// Current version (may be updated by `UpdateVersion`).
    version: SegmentVersion,
    /// Current slab watermark. An insert keeps it, like a Cassandra insert
    /// that does not name the column.
    watermark: Option<SlabId>,
}

impl SegmentModel {
    /// Creates a new model tracking the given base segment.
    #[must_use]
    pub fn new(base: Segment) -> Self {
        let version = base.version;
        Self {
            base,
            present: false,
            version,
            watermark: None,
        }
    }

    /// Applies an operation to the model.
    pub fn apply(&mut self, op: &SegmentOperation) {
        match op {
            SegmentOperation::Insert => {
                self.present = true;
                // insert_segment writes the base segment; reset the version.
                self.version = self.base.version;
            }
            SegmentOperation::Get => {}
            SegmentOperation::Delete => {
                self.present = false;
                self.watermark = None;
            }
            SegmentOperation::UpdateVersion(version) => self.version = *version,
            SegmentOperation::UpdateSlabSize => self.watermark = None,
            SegmentOperation::SetWatermark(watermark) => self.watermark = Some(*watermark),
        }
    }

    /// Returns the expected segment state, if present.
    #[must_use]
    pub fn expected_segment(&self) -> Option<Segment> {
        self.present.then(|| Segment {
            version: self.version,
            ..self.base.clone()
        })
    }
}

/// Verifies that two segments have identical id, name, `slab_size`, version,
/// and slab watermark.
fn verify_segment_fields(
    expected: &Segment,
    expected_watermark: Option<SlabId>,
    actual: StoredSegment,
) -> color_eyre::Result<()> {
    let (actual, watermark) = actual.into_parts();
    if expected_watermark != watermark {
        return Err(color_eyre::eyre::eyre!(
            "Watermark mismatch for {}: expected {expected_watermark:?}, got {watermark:?}",
            expected.id
        ));
    }
    if expected.id != actual.id {
        return Err(color_eyre::eyre::eyre!(
            "ID mismatch: expected {:?}, got {:?}",
            expected.id,
            actual.id
        ));
    }
    if expected.name != actual.name {
        return Err(color_eyre::eyre::eyre!(
            "Name mismatch for {}: expected {:?}, got {:?}",
            expected.id,
            expected.name,
            actual.name
        ));
    }
    if expected.slab_size != actual.slab_size {
        return Err(color_eyre::eyre::eyre!(
            "Slab size mismatch for {}: expected {:?}, got {:?}",
            expected.id,
            expected.slab_size,
            actual.slab_size
        ));
    }
    if expected.version != actual.version {
        return Err(color_eyre::eyre::eyre!(
            "Version mismatch for {}: expected {:?}, got {:?}",
            expected.id,
            expected.version,
            actual.version
        ));
    }
    Ok(())
}

/// Verifies that segment operations match the reference model behavior.
///
/// # Test Strategy
///
/// 1. Start with an empty store (no segment persisted yet)
/// 2. Apply a sequence of random operations to both the store and the model
/// 3. After each `Get` operation, verify the store's response matches the model
/// 4. End with a `Get`, which verifies the final state
///
/// The store is scoped to exactly one segment (`operations.segment()`). All
/// operations target that segment — there is no multi-segment routing.
///
/// # Errors
///
/// Returns an error if:
/// - Store operations fail (insert, delete, get)
/// - Store state doesn't match model state after a `Get`
/// - Field values differ between model and store
pub async fn prop_segment_model_equivalence<T>(
    operations: &T,
    input: SegmentTestInput,
) -> color_eyre::Result<()>
where
    T: TriggerOperations,
    T::Error: Error + Send + Sync + 'static,
{
    // Clean up any leftover state from a previous trial
    operations
        .delete_segment()
        .await
        .map_err(|e| color_eyre::eyre::eyre!("Failed to clean up segment: {e:?}"))?;

    let mut model = SegmentModel::new(operations.segment().clone());

    // Apply all operations to both store and model, verifying queries inline
    let prefix = [
        SegmentOperation::Insert,
        SegmentOperation::Get,
        SegmentOperation::UpdateVersion(SegmentVersion::V4),
        SegmentOperation::Get,
    ];
    // The trailing `Get` checks the final state.
    let ops = prefix.iter().chain(&input.operations);
    for (op_idx, op) in ops.chain([&SegmentOperation::Get]).enumerate() {
        model.apply(op);
        match op {
            SegmentOperation::Insert => {
                operations
                    .insert_segment()
                    .await
                    .map_err(|e| color_eyre::eyre::eyre!("Op #{op_idx} Insert failed: {e:?}"))?;
            }
            SegmentOperation::Get => {
                let actual = operations
                    .get_segment()
                    .await
                    .map_err(|e| color_eyre::eyre::eyre!("Op #{op_idx} Get failed: {e:?}"))?;

                let expected = model.expected_segment();
                match (expected, actual) {
                    (Some(exp), Some(act)) => {
                        verify_segment_fields(&exp, model.watermark, act).map_err(|e| {
                            color_eyre::eyre::eyre!("Op #{op_idx} Get mismatch: {e}")
                        })?;
                    }
                    (None, None) => {
                        // Both agree segment doesn't exist — correct
                    }
                    (Some(exp), None) => {
                        return Err(color_eyre::eyre::eyre!(
                            "Op #{op_idx} Get: segment exists in model but not store (expected: \
                             {exp:?})"
                        ));
                    }
                    (None, Some(act)) => {
                        return Err(color_eyre::eyre::eyre!(
                            "Op #{op_idx} Get: segment exists in store but not model (actual: \
                             {act:?})"
                        ));
                    }
                }
            }
            SegmentOperation::Delete => {
                operations
                    .delete_segment()
                    .await
                    .map_err(|e| color_eyre::eyre::eyre!("Op #{op_idx} Delete failed: {e:?}"))?;
            }
            SegmentOperation::UpdateVersion(version) => {
                operations
                    .update_segment_version(*version)
                    .await
                    .map_err(|e| {
                        color_eyre::eyre::eyre!("Op #{op_idx} UpdateVersion failed: {e:?}")
                    })?;
            }
            SegmentOperation::UpdateSlabSize => {
                operations
                    .update_segment_slab_size(input.slab_size)
                    .await
                    .map_err(|e| {
                        color_eyre::eyre::eyre!("Op #{op_idx} UpdateSlabSize failed: {e:?}")
                    })?;
            }
            SegmentOperation::SetWatermark(watermark) => {
                operations
                    .set_slab_watermark(Some(*watermark))
                    .await
                    .map_err(|e| {
                        color_eyre::eyre::eyre!("Op #{op_idx} SetWatermark failed: {e:?}")
                    })?;
            }
        }
    }

    Ok(())
}
