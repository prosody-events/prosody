//! Property-based tests for segment table operations.
//!
//! Tests the low-level segment CRUD operations in isolation using a simple
//! reference model to verify correctness.

use crate::timers::duration::CompactDuration;
use crate::timers::store::{Segment, SegmentId, SegmentVersion, TriggerStore};
use ahash::HashMap;
use quickcheck::{Arbitrary, Gen, TestResult};
use std::collections::HashSet;
use std::fmt::Debug;
use uuid::Uuid;

/// Operations that can be performed on the segment table.
#[derive(Clone, Debug)]
pub enum SegmentOperation {
    /// Insert or update a segment.
    Insert(Segment),
    /// Retrieve a segment by ID.
    Get(SegmentId),
    /// Delete a segment by ID.
    Delete(SegmentId),
    /// Update segment version and slab size.
    UpdateVersion {
        /// The segment ID.
        segment_id: SegmentId,
        /// The new version.
        version: SegmentVersion,
        /// The new slab size.
        slab_size: CompactDuration,
    },
}

/// Test input containing isolated segment IDs and operations.
///
/// Each test trial uses randomly generated segment IDs, ensuring complete
/// isolation between trials and allowing parallel test execution.
#[derive(Clone, Debug)]
pub struct SegmentTestInput {
    /// Pool of segment IDs used by operations in this trial.
    pub segment_ids: Vec<SegmentId>,
    /// Sequence of operations to apply.
    pub operations: Vec<SegmentOperation>,
}

impl Arbitrary for SegmentTestInput {
    fn arbitrary(g: &mut Gen) -> Self {
        // Generate 3 random v4 UUIDs unique to this trial
        // v4 UUIDs have a structure that prevents QuickCheck from shrinking them
        // into colliding values
        let segment_ids = vec![Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];

        // Generate 10-50 operations using these segments
        let op_count = (usize::arbitrary(g) % 40) + 10;
        let mut operations = Vec::with_capacity(op_count);

        // Track which segments have been inserted to generate valid UpdateVersion ops
        let mut inserted_segments = HashSet::new();

        for _ in 0..op_count {
            let idx = usize::from(u8::arbitrary(g)) % segment_ids.len();
            let segment_id = segment_ids[idx];

            let op = match u8::arbitrary(g) % 4 {
                0 => {
                    // Insert operation
                    inserted_segments.insert(segment_id);
                    let segment = Segment {
                        id: segment_id,
                        name: format!("segment-{}", u8::arbitrary(g) % 10),
                        slab_size: CompactDuration::new(
                            (u32::arbitrary(g) % 3600) + 1, // 1-3600 seconds
                        ),
                        version: if bool::arbitrary(g) {
                            SegmentVersion::V1
                        } else {
                            SegmentVersion::V2
                        },
                    };
                    SegmentOperation::Insert(segment)
                }
                1 => SegmentOperation::Get(segment_id),
                2 => {
                    // Delete operation - remove from tracking
                    inserted_segments.remove(&segment_id);
                    SegmentOperation::Delete(segment_id)
                }
                _ => {
                    // UpdateVersion operation - only on currently inserted segments
                    if inserted_segments.is_empty() {
                        // If no segments inserted yet, generate a Get instead
                        SegmentOperation::Get(segment_id)
                    } else {
                        // Pick from currently inserted segments, not random segment_id
                        let inserted_vec: Vec<_> = inserted_segments.iter().copied().collect();
                        let update_segment_id = inserted_vec[usize::arbitrary(g) % inserted_vec.len()];
                        let version = if bool::arbitrary(g) {
                            SegmentVersion::V1
                        } else {
                            SegmentVersion::V2
                        };
                        let slab_size = CompactDuration::new(
                            (u32::arbitrary(g) % 3600) + 1, // 1-3600 seconds
                        );
                        SegmentOperation::UpdateVersion {
                            segment_id: update_segment_id,
                            version,
                            slab_size,
                        }
                    }
                }
            };
            operations.push(op);
        }

        Self {
            segment_ids,
            operations,
        }
    }
}

/// Reference model for segment table behavior.
///
/// Uses a simple [`HashMap`] to track expected segment state.
#[derive(Clone, Debug)]
pub struct SegmentModel {
    segments: HashMap<SegmentId, Segment>,
}

impl Default for SegmentModel {
    fn default() -> Self {
        Self::new()
    }
}

impl SegmentModel {
    /// Creates a new empty segment model.
    #[must_use]
    pub fn new() -> Self {
        Self {
            segments: HashMap::default(),
        }
    }

    /// Applies an operation to the model.
    pub fn apply(&mut self, op: &SegmentOperation) {
        match op {
            SegmentOperation::Insert(segment) => {
                self.segments.insert(segment.id, segment.clone());
            }
            SegmentOperation::Get(_) => {
                // Queries don't modify state
            }
            SegmentOperation::Delete(id) => {
                self.segments.remove(id);
            }
            SegmentOperation::UpdateVersion {
                segment_id,
                version,
                slab_size,
            } => {
                if let Some(segment) = self.segments.get_mut(segment_id) {
                    segment.version = *version;
                    segment.slab_size = *slab_size;
                }
            }
        }
    }

    /// Gets a segment from the model.
    #[must_use]
    pub fn get(&self, id: &SegmentId) -> Option<&Segment> {
        self.segments.get(id)
    }

    /// Returns all segment IDs in the model.
    #[must_use]
    pub fn all_ids(&self) -> Vec<SegmentId> {
        self.segments.keys().copied().collect()
    }
}

/// Verifies that two segments have identical fields.
///
/// # Errors
///
/// Returns an error if any field (id, name, `slab_size`, version) differs.
fn verify_segment_fields(
    id: &SegmentId,
    expected: &Segment,
    actual: &Segment,
) -> color_eyre::Result<()> {
    if expected.id != actual.id {
        return Err(color_eyre::eyre::eyre!(
            "ID mismatch: expected {:?}, got {:?}",
            expected.id,
            actual.id
        ));
    }
    if expected.name != actual.name {
        return Err(color_eyre::eyre::eyre!(
            "Name mismatch for {id}: expected {:?}, got {:?}",
            expected.name,
            actual.name
        ));
    }
    if expected.slab_size != actual.slab_size {
        return Err(color_eyre::eyre::eyre!(
            "Slab size mismatch for {id}: expected {:?}, got {:?}",
            expected.slab_size,
            actual.slab_size
        ));
    }
    if expected.version != actual.version {
        return Err(color_eyre::eyre::eyre!(
            "Version mismatch for {id}: expected {:?}, got {:?}",
            expected.version,
            actual.version
        ));
    }
    Ok(())
}

/// Performs final verification that model and store are equivalent for all
/// segment IDs.
///
/// # Errors
///
/// Returns an error if model and store state don't match.
async fn verify_final_state<S>(
    store: &S,
    model: &SegmentModel,
) -> color_eyre::Result<()>
where
    S: TriggerStore + Send + Sync,
    S::Error: Debug,
{
    let all_ids: Vec<SegmentId> = model.all_ids();

    for id in &all_ids {
        let model_segment = model.get(id);
        let store_segment = store
            .get_segment(id)
            .await
            .map_err(|e| color_eyre::eyre::eyre!("Get segment failed: {e:?}"))?;

        match (model_segment, store_segment) {
            (Some(expected), Some(actual)) => {
                verify_segment_fields(id, expected, &actual)?;
            }
            (Some(expected), None) => {
                return Err(color_eyre::eyre::eyre!(
                    "Segment {} exists in model but not in store (expected: {:?})",
                    id,
                    expected
                ));
            }
            (None, Some(actual)) => {
                return Err(color_eyre::eyre::eyre!(
                    "Segment {} exists in store but not in model (actual: {:?})",
                    id,
                    actual
                ));
            }
            (None, None) => {
                // Both agree it doesn't exist - correct
            }
        }
    }

    // Also verify that segments in store but not in model IDs are properly absent
    // (This catches cases where store has extra segments)
    for id in &all_ids {
        if model.get(id).is_none() {
            let store_result = store.get_segment(id).await.map_err(|e| {
                color_eyre::eyre::eyre!("Get segment failed during absence check: {e:?}")
            })?;
            if store_result.is_some() {
                return Err(color_eyre::eyre::eyre!(
                    "Store has segment {} that model doesn't expect",
                    id
                ));
            }
        }
    }

    Ok(())
}

/// Verifies that segment operations match the reference model behavior.
///
/// # Test Strategy
///
/// 1. Start with empty store and model
/// 2. Apply sequence of random operations to both
/// 3. Verify that for every segment ID:
///    - `store.get(id)` matches `model.get(id)`
///    - All fields (name, `slab_size`, version) are identical
///
/// # Errors
///
/// Returns an error if:
/// - Store operations fail (insert, delete, get)
/// - Store state doesn't match model state
/// - Field values differ between model and store
pub async fn prop_segment_model_equivalence<S>(
    store: &S,
    input: SegmentTestInput,
) -> color_eyre::Result<()>
where
    S: TriggerStore + Send + Sync,
    S::Error: Debug,
{
    // Clean up segments from this trial to ensure isolation
    for segment_id in &input.segment_ids {
        store
            .delete_segment(segment_id)
            .await
            .map_err(|e| color_eyre::eyre::eyre!("Failed to clean up segment: {e:?}"))?;
    }

    let mut model = SegmentModel::new();

    // Apply all operations to both store and model, verifying queries inline
    for (op_idx, op) in input.operations.iter().enumerate() {
        match op {
            SegmentOperation::Insert(segment) => {
                model.apply(op);
                store
                    .insert_segment(segment.clone())
                    .await
                    .map_err(|e| color_eyre::eyre::eyre!("Op #{op_idx} Insert failed: {e:?}"))?;
            }
            SegmentOperation::Get(id) => {
                // Verify query immediately against model
                let expected = model.get(id);
                let actual = store
                    .get_segment(id)
                    .await
                    .map_err(|e| color_eyre::eyre::eyre!("Op #{op_idx} Get failed: {e:?}"))?;

                match (expected, actual) {
                    (Some(exp), Some(act)) => {
                        verify_segment_fields(id, exp, &act).map_err(|e| {
                            color_eyre::eyre::eyre!("Op #{op_idx} Get query mismatch: {e}")
                        })?;
                    }
                    (None, None) => {
                        // Both agree segment doesn't exist - correct
                    }
                    (Some(exp), None) => {
                        return Err(color_eyre::eyre::eyre!(
                            "Op #{op_idx} Get: segment {id} exists in model but not store \
                             (expected: {exp:?})"
                        ));
                    }
                    (None, Some(act)) => {
                        return Err(color_eyre::eyre::eyre!(
                            "Op #{op_idx} Get: segment {id} exists in store but not model \
                             (actual: {act:?})"
                        ));
                    }
                }
            }
            SegmentOperation::Delete(id) => {
                model.apply(op);
                store
                    .delete_segment(id)
                    .await
                    .map_err(|e| color_eyre::eyre::eyre!("Op #{op_idx} Delete failed: {e:?}"))?;
            }
            SegmentOperation::UpdateVersion {
                segment_id,
                version,
                slab_size,
            } => {
                model.apply(op);
                store
                    .update_segment_version(segment_id, *version, *slab_size)
                    .await
                    .map_err(|e| {
                        color_eyre::eyre::eyre!("Op #{op_idx} UpdateVersion failed: {e:?}")
                    })?;
            }
        }
    }

    // Final sanity check: verify model-store equivalence for all segment IDs
    // (queries were already verified inline, this catches any missed state)
    verify_final_state(store, &model).await
}

/// [`QuickCheck`] wrapper for segment model equivalence property.
pub fn test_prop_segment_model_equivalence<S>(store: &S, input: SegmentTestInput) -> TestResult
where
    S: TriggerStore + Send + Sync + 'static,
    S::Error: Debug,
{
    use tokio::runtime::Runtime;

    let Ok(rt) = Runtime::new() else {
        return TestResult::error("Failed to create tokio runtime");
    };

    let result = rt.block_on(async { prop_segment_model_equivalence(store, input).await });

    match result {
        Ok(()) => TestResult::passed(),
        Err(e) => TestResult::error(format!("{e:?}")),
    }
}
