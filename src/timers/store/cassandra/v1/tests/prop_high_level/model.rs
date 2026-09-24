//! The independent model of the V1 dual-index trigger store.

use super::{TriggerV1Tuple, V1HighLevelOperation};
use crate::Key;
use crate::timers::slab::{Slab, SlabId};
use crate::timers::store::SegmentId;
use ahash::HashMap;
use std::collections::BTreeSet;

/// Reference model tracking V1 dual indices.
#[derive(Clone, Debug)]
pub struct V1HighLevelModel {
    /// Triggers indexed by (`segment_id`, `slab_id`).
    pub(super) slab_index: HashMap<(SegmentId, SlabId), BTreeSet<TriggerV1Tuple>>,
    /// Triggers indexed by (`segment_id`, key).
    pub(super) key_index: HashMap<(SegmentId, Key), BTreeSet<TriggerV1Tuple>>,
}

impl Default for V1HighLevelModel {
    fn default() -> Self {
        Self::new()
    }
}

impl V1HighLevelModel {
    /// Creates a new empty model.
    #[must_use]
    pub fn new() -> Self {
        Self {
            slab_index: HashMap::default(),
            key_index: HashMap::default(),
        }
    }

    /// Applies an operation to the model.
    pub fn apply(&mut self, op: &V1HighLevelOperation) {
        match op {
            V1HighLevelOperation::AddTrigger {
                segment_id,
                slab_id,
                trigger,
            } => {
                let tuple = (trigger.key.clone(), trigger.time);

                // Add to slab index
                self.slab_index
                    .entry((*segment_id, *slab_id))
                    .or_default()
                    .insert(tuple.clone());

                // Add to key index
                self.key_index
                    .entry((*segment_id, trigger.key.clone()))
                    .or_default()
                    .insert(tuple);
            }
            V1HighLevelOperation::RemoveTrigger {
                segment_id,
                slab_id,
                key,
                time,
            } => {
                let tuple = (key.clone(), *time);

                // Remove from slab index
                if let Some(triggers) = self.slab_index.get_mut(&(*segment_id, *slab_id)) {
                    triggers.remove(&tuple);
                }

                // Remove from key index
                if let Some(triggers) = self.key_index.get_mut(&(*segment_id, key.clone())) {
                    triggers.remove(&tuple);
                }
            }
            V1HighLevelOperation::ClearTriggersForKey {
                segment_id,
                key,
                slab_size,
            } => {
                // Get all triggers for this key from key index
                let triggers_to_remove: Vec<TriggerV1Tuple> = self
                    .key_index
                    .get(&(*segment_id, key.clone()))
                    .map(|set| set.iter().cloned().collect())
                    .unwrap_or_default();

                // Remove from slab index for each trigger
                for (k, time) in &triggers_to_remove {
                    let slab = Slab::from_time(*slab_size, *time);
                    if let Some(triggers) = self.slab_index.get_mut(&(*segment_id, slab.id())) {
                        triggers.remove(&(k.clone(), *time));
                    }
                }

                // Clear from key index
                self.key_index.remove(&(*segment_id, key.clone()));
            }
            V1HighLevelOperation::DeleteSlab {
                segment_id,
                slab_id,
            } => {
                // Get all triggers in this slab
                let triggers_to_remove: Vec<TriggerV1Tuple> = self
                    .slab_index
                    .get(&(*segment_id, *slab_id))
                    .map(|set| set.iter().cloned().collect())
                    .unwrap_or_default();

                // Remove from key index for each trigger
                for (key, time) in &triggers_to_remove {
                    if let Some(triggers) = self.key_index.get_mut(&(*segment_id, key.clone())) {
                        triggers.remove(&(key.clone(), *time));
                    }
                }

                // Clear slab index
                self.slab_index.remove(&(*segment_id, *slab_id));
            }
            V1HighLevelOperation::GetSlabTriggers { .. }
            | V1HighLevelOperation::GetKeyTriggers { .. } => {
                // Queries don't modify state
            }
        }
    }

    /// Gets triggers from V1 slab index.
    #[must_use]
    pub fn get_slab_triggers(
        &self,
        segment_id: &SegmentId,
        slab_id: SlabId,
    ) -> Vec<TriggerV1Tuple> {
        self.slab_index
            .get(&(*segment_id, slab_id))
            .map(|set| set.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Gets triggers from V1 key index.
    #[must_use]
    pub fn get_key_triggers(&self, segment_id: &SegmentId, key: &Key) -> Vec<TriggerV1Tuple> {
        self.key_index
            .get(&(*segment_id, key.clone()))
            .map(|set| set.iter().cloned().collect())
            .unwrap_or_default()
    }
}
