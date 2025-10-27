//! In-memory implementation of the `TriggerStore` trait.
//!
//! This module provides `InMemoryTriggerStore`, a lock-free, concurrent,
//! in-memory timer data store. It maintains dual indices for time-based and
//! key-based lookups, enabling efficient insertion, deletion, and queries
//! without external dependencies.
//!
//! # Data Organization
//!
//! - **Segments**: Metadata mapping a segment ID to its name and slab size.
//! - **Slab Index**: Tracks which slab IDs are registered under each segment.
//! - **Time Index**: Maps each `Slab` to the set of `Trigger`s scheduled within
//!   it.
//! - **Key Index**: Maps each (segment, key) pair to the set of `Trigger`s for
//!   that key.
//!
//! All maps use [`scc::HashMap`] for concurrent access, and values are stored
//! in [`BTreeSet`] to maintain sorted order where needed.

use crate::Key;
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::timers::slab::{Slab, SlabId};
use crate::timers::store::{Segment, SegmentId, TriggerStore, TriggerV1};
use crate::timers::{TimerType, Trigger};
use async_stream::{stream, try_stream};
use futures::TryStreamExt;
use futures::stream::Stream;
use scc::HashMap;
use std::collections::BTreeSet;
use std::convert::Infallible;
use std::ops::RangeInclusive;
use std::sync::Arc;
use tokio::join;
use tracing::Span;

/// In-memory, concurrent implementation of [`TriggerStore`] for testing and
/// development.
///
/// All data is held in memory; timers and segments are lost when the process
/// exits. This store supports the full `TriggerStore` trait API with
/// low-latency, operations.
///
/// # Examples
///
/// ```rust,no_run
/// use prosody::timers::store::TriggerStore;
/// use prosody::timers::store::memory::InMemoryTriggerStore;
///
/// let store = InMemoryTriggerStore::new();
/// // Now you can call TriggerStore methods on `store`
/// ```
#[derive(Clone, Debug, Default)]
pub struct InMemoryTriggerStore(Arc<Inner>);

/// Internal state for `InMemoryTriggerStore`.
///
/// Maintains concurrent maps to support dual indexing, segment management,
/// and v1 schema compatibility during migration.
#[derive(Debug, Default)]
struct Inner {
    /// Maps each segment ID to its (name, `slab_size`, version).
    segments: HashMap<SegmentId, (String, CompactDuration, Option<u8>)>,

    /// Maps each segment ID to its active set of slab IDs.
    segment_slabs: HashMap<SegmentId, BTreeSet<SlabId>>,

    /// V2 time-based index: maps (`segment_id`, `slab_size`, `slab_id`) to
    /// triggers. Matches v2 `timer_typed_slabs` table PK: ((`segment_id`,
    /// `slab_size`, `id`), `key`, `time`, `type`).
    slab_triggers: HashMap<(SegmentId, CompactDuration, SlabId), BTreeSet<Trigger>>,

    /// Key-based index: maps (segment, key) to the set of `Trigger`s for that
    /// key.
    key_triggers: HashMap<(SegmentId, Key), BTreeSet<Trigger>>,

    /// V1 time-based index: maps (`segment_id`, `slab_id`) to triggers.
    /// Matches v1 `timer_slabs` table PK: ((`segment_id`, `id`), `key`,
    /// `time`).
    slab_triggers_v1: HashMap<(SegmentId, SlabId), BTreeSet<TriggerV1>>,

    /// V1 key-based index: maps (`segment_id`, key) to triggers.
    /// Matches v1 `timer_keys` table PK: ((`segment_id`, `key`), `time`).
    key_triggers_v1: HashMap<(SegmentId, Key), BTreeSet<TriggerV1>>,
}

impl InMemoryTriggerStore {
    /// Create a new, empty in-memory trigger store.
    ///
    /// # Returns
    ///
    /// A ready-to-use `InMemoryTriggerStore`.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl TriggerStore for InMemoryTriggerStore {
    type Error = Infallible;

    // -- Segment management operations --

    /// Insert or update a segment's metadata.
    ///
    /// Registers the segment ID, name, and slab size.
    ///
    /// # Arguments
    ///
    /// * `segment` - The segment configuration to store.
    ///
    /// # Errors
    ///
    /// Never returns an error.
    async fn insert_segment(&self, segment: Segment) -> Result<(), Self::Error> {
        self.0
            .segments
            .upsert_async(
                segment.id,
                (segment.name, segment.slab_size, segment.version),
            )
            .await;

        Ok(())
    }

    /// Retrieve a segment's configuration by its ID.
    ///
    /// # Arguments
    ///
    /// * `segment_id` - Identifier of the segment to look up.
    ///
    /// # Returns
    ///
    /// * `Ok(Some(segment))` if found.
    /// * `Ok(None)` if no segment with that ID exists.
    /// * Never returns `Err`.
    async fn get_segment(&self, segment_id: &SegmentId) -> Result<Option<Segment>, Self::Error> {
        Ok(self.0.segments.get_async(segment_id).await.map(|e| {
            let (name, slab_size, version) = e.get();
            Segment {
                id: *segment_id,
                name: name.clone(),
                slab_size: *slab_size,
                version: *version,
            }
        }))
    }

    /// Delete a segment and its slab registrations.
    ///
    /// # Arguments
    ///
    /// * `segment_id` - Identifier of the segment to remove.
    ///
    /// # Errors
    ///
    /// Never returns an error.
    async fn delete_segment(&self, segment_id: &SegmentId) -> Result<(), Self::Error> {
        // Remove from both the segments map and the segment_slabs map.
        join!(
            self.0.segments.remove_async(segment_id),
            self.0.segment_slabs.remove_async(segment_id)
        );

        Ok(())
    }

    // -- Slab management operations --

    /// Stream all slab IDs registered under a segment.
    ///
    /// # Arguments
    ///
    /// * `segment_id` - Segment whose slabs to list.
    ///
    /// # Returns
    ///
    /// A stream of `SlabId`. Never yields an error.
    fn get_slabs(&self, segment_id: &SegmentId) -> impl Stream<Item = Result<SlabId, Self::Error>> {
        try_stream! {
            let Some(entry) = self.0.segment_slabs.get_async(segment_id).await else {
                return;
            };

            for &slab_id in entry.iter() {
                yield slab_id;
            }
        }
    }

    /// Stream slab IDs in a specified inclusive range.
    ///
    /// # Arguments
    ///
    /// * `segment_id` - Segment to query.
    /// * `range` - Inclusive range of slab IDs to include.
    ///
    /// # Returns
    ///
    /// A stream of `SlabId`. Never yields an error.
    fn get_slab_range(
        &self,
        segment_id: &SegmentId,
        range: RangeInclusive<SlabId>,
    ) -> impl Stream<Item = Result<SlabId, Self::Error>> {
        try_stream! {
            let Some(entry) = self.0.segment_slabs.get_async(segment_id).await else {
                return;
            };

            for &slab_id in entry.range(range) {
                yield slab_id;
            }
        }
    }

    /// Register a slab ID under a segment.
    ///
    /// # Arguments
    ///
    /// * `segment_id` - Owner segment.
    /// * `slab_id` - Identifier of the slab to add.
    ///
    /// # Errors
    ///
    /// Never returns an error.
    async fn insert_slab(&self, segment_id: &SegmentId, slab: Slab) -> Result<(), Self::Error> {
        self.0
            .segment_slabs
            .entry_async(*segment_id)
            .await
            .or_default()
            .get_mut()
            .insert(slab.id());

        Ok(())
    }

    /// Unregister a slab ID from a segment.
    ///
    /// Does not clear slab triggers; use `clear_slab_triggers` for that.
    ///
    /// # Arguments
    ///
    /// * `segment_id` - Owner segment.
    /// * `slab_id` - Identifier of the slab to remove.
    ///
    /// # Errors
    ///
    /// Never returns an error.
    async fn delete_slab(
        &self,
        segment_id: &SegmentId,
        slab_id: SlabId,
    ) -> Result<(), Self::Error> {
        let Some(mut entry) = self.0.segment_slabs.get_async(segment_id).await else {
            return Ok(());
        };

        entry.get_mut().remove(&slab_id);
        if entry.is_empty() {
            let _ = entry.remove();
        }

        Ok(())
    }

    // -- Slab trigger operations (time index) --

    /// Stream all triggers within a given slab.
    ///
    /// # Arguments
    ///
    /// * `slab` - The slab specification to query.
    ///
    /// # Returns
    ///
    /// A stream of `Trigger`. Never yields an error.
    fn get_slab_triggers(&self, slab: &Slab) -> impl Stream<Item = Result<Trigger, Self::Error>> {
        let segment_id = *slab.segment_id();
        let slab_size = slab.size();
        let slab_id = slab.id();

        try_stream! {
            let key = (segment_id, slab_size, slab_id);
            let Some(set) = self.0.slab_triggers.get_async(&key).await else {
                return;
            };

            for trigger in set.iter() {
                yield trigger.clone();
            }
        }
    }

    /// Insert a trigger into a slab's time index.
    ///
    /// # Arguments
    ///
    /// * `slab` - Slab to receive the trigger.
    /// * `trigger` - The `Trigger` to add.
    ///
    /// # Errors
    ///
    /// Never returns an error.
    async fn insert_slab_trigger(&self, slab: Slab, trigger: Trigger) -> Result<(), Self::Error> {
        let key = (*slab.segment_id(), slab.size(), slab.id());

        self.0
            .slab_triggers
            .entry_async(key)
            .await
            .or_default()
            .get_mut()
            .insert(trigger);

        Ok(())
    }

    /// Delete a specific trigger from a slab's time index.
    ///
    /// # Arguments
    ///
    /// * `slab` - Slab to modify.
    /// * `key` - Trigger's key.
    /// * `time` - Trigger's scheduled time.
    ///
    /// # Errors
    ///
    /// Never returns an error.
    async fn delete_slab_trigger(
        &self,
        slab: &Slab,
        key: &Key,
        time: CompactDateTime,
    ) -> Result<(), Self::Error> {
        let slab_key = (*slab.segment_id(), slab.size(), slab.id());

        let Some(mut entry) = self.0.slab_triggers.get_async(&slab_key).await else {
            return Ok(());
        };

        let trigger = Trigger::new(key.clone(), time, TimerType::Application, Span::current());

        entry.get_mut().remove(&trigger);
        if entry.is_empty() {
            let _ = entry.remove();
        }

        Ok(())
    }

    /// Remove all triggers from a slab's time index.
    ///
    /// # Arguments
    ///
    /// * `slab` - Slab to clear.
    ///
    /// # Errors
    ///
    /// Never returns an error.
    async fn clear_slab_triggers(&self, slab: &Slab) -> Result<(), Self::Error> {
        let key = (*slab.segment_id(), slab.size(), slab.id());
        self.0.slab_triggers.remove_async(&key).await;
        Ok(())
    }

    // -- Key trigger operations (entity index) --

    /// Stream all scheduled times for a given key.
    ///
    /// # Arguments
    ///
    /// * `segment_id` - Segment to query.
    /// * `key` - Entity key.
    ///
    /// # Returns
    ///
    /// A stream of `CompactDateTime`. Never yields an error.
    fn get_key_times(
        &self,
        segment_id: &SegmentId,
        key: &Key,
    ) -> impl Stream<Item = Result<CompactDateTime, Self::Error>> + Send {
        self.get_key_triggers(segment_id, key)
            .map_ok(|trigger| trigger.time)
    }

    /// Stream all triggers for a given key.
    ///
    /// # Arguments
    ///
    /// * `segment_id` - Segment to query.
    /// * `key` - Entity key.
    ///
    /// # Returns
    ///
    /// A stream of `Trigger`. Never yields an error.
    fn get_key_triggers(
        &self,
        segment_id: &SegmentId,
        key: &Key,
    ) -> impl Stream<Item = Result<Trigger, Self::Error>> + Send {
        try_stream! {
            let map_key = (*segment_id, key.clone());
            let Some(entry) = self.0.key_triggers.get_async(&map_key).await else {
                return;
            };

            for trigger in entry.iter() {
                yield trigger.clone();
            }
        }
    }

    /// Insert a trigger into the key-based index.
    ///
    /// # Arguments
    ///
    /// * `segment_id` - Segment for the key.
    /// * `trigger` - The `Trigger` to add.
    ///
    /// # Errors
    ///
    /// Never returns an error.
    async fn insert_key_trigger(
        &self,
        segment_id: &SegmentId,
        trigger: Trigger,
    ) -> Result<(), Self::Error> {
        let map_key = (*segment_id, trigger.key.clone());
        self.0
            .key_triggers
            .entry_async(map_key)
            .await
            .or_default()
            .get_mut()
            .insert(trigger);

        Ok(())
    }

    /// Delete a specific trigger from the key-based index.
    ///
    /// # Arguments
    ///
    /// * `segment_id` - Segment for the key.
    /// * `key` - Trigger's key.
    /// * `time` - Trigger's scheduled time.
    ///
    /// # Errors
    ///
    /// Never returns an error.
    async fn delete_key_trigger(
        &self,
        segment_id: &SegmentId,
        key: &Key,
        time: CompactDateTime,
    ) -> Result<(), Self::Error> {
        let map_key = (*segment_id, key.clone());
        let Some(mut entry) = self.0.key_triggers.get_async(&map_key).await else {
            return Ok(());
        };

        let trigger = Trigger::new(key.clone(), time, TimerType::Application, Span::none());

        entry.get_mut().remove(&trigger);
        if entry.is_empty() {
            let _ = entry.remove();
        }

        Ok(())
    }

    /// Remove all triggers for a key across both indices.
    ///
    /// # Arguments
    ///
    /// * `segment` - Segment ID.
    /// * `key` - Entity key.
    ///
    /// # Errors
    ///
    /// Never returns an error.
    async fn clear_key_triggers(&self, segment: &SegmentId, key: &Key) -> Result<(), Self::Error> {
        let map_key = (*segment, key.clone());
        self.0.key_triggers.remove_async(&map_key).await;
        Ok(())
    }

    // -- V1 migration methods --

    /// Update segment metadata including version and slab size.
    ///
    /// # Arguments
    ///
    /// * `segment_id` - Segment to update.
    /// * `new_version` - New schema version (1 or 2).
    /// * `new_slab_size` - New slab size for the segment.
    ///
    /// # Errors
    ///
    /// Never returns an error.
    async fn update_segment_version(
        &self,
        segment_id: &SegmentId,
        new_version: u8,
        new_slab_size: CompactDuration,
    ) -> Result<(), Self::Error> {
        if let Some(entry) = self.0.segments.get_async(segment_id).await {
            let (name, ..) = entry.get();
            let name = name.clone();
            drop(entry);
            self.0
                .segments
                .upsert_async(*segment_id, (name, new_slab_size, Some(new_version)))
                .await;
        }
        Ok(())
    }

    async fn insert_slab_v1(
        &self,
        segment_id: &SegmentId,
        slab_id: SlabId,
    ) -> Result<(), Self::Error> {
        let segment_id = *segment_id;
        self.0
            .segment_slabs
            .entry_async(segment_id)
            .await
            .or_insert_with(BTreeSet::new)
            .get_mut()
            .insert(slab_id);
        Ok(())
    }

    async fn insert_slab_trigger_v1(
        &self,
        segment_id: &SegmentId,
        slab_id: SlabId,
        trigger: TriggerV1,
    ) -> Result<(), Self::Error> {
        let key = (*segment_id, slab_id);
        self.0
            .slab_triggers_v1
            .entry_async(key)
            .await
            .or_insert_with(BTreeSet::new)
            .get_mut()
            .insert(trigger);
        Ok(())
    }

    /// Stream all v1 slab IDs for a segment.
    ///
    /// V1 has no segment tracking, returns empty stream.
    ///
    /// # Arguments
    ///
    /// * `segment_id` - Segment to query.
    ///
    /// # Returns
    ///
    /// Stream of slab IDs that have data for this segment.
    ///
    /// Uses the `segment_slabs` tracking map.
    fn get_slabs_v1(
        &self,
        segment_id: &SegmentId,
    ) -> impl Stream<Item = Result<SlabId, Self::Error>> + Send {
        let segment_id = *segment_id;
        let store = self.clone();

        stream! {
            let slab_ids = store
                .0
                .segment_slabs
                .get_async(&segment_id)
                .await
                .map(|entry| entry.get().clone())
                .unwrap_or_default();

            for slab_id in slab_ids {
                yield Ok(slab_id);
            }
        }
    }

    /// Stream all v1 triggers within a slab.
    ///
    /// V1 `timer_slabs` table uses PK `((segment_id, id), key, time)`.
    ///
    /// # Arguments
    ///
    /// * `segment_id` - Segment ID.
    /// * `slab_id` - Slab ID.
    ///
    /// # Returns
    ///
    /// A stream of `TriggerV1`. Never yields an error.
    fn get_slab_triggers_v1(
        &self,
        segment_id: &SegmentId,
        slab_id: SlabId,
    ) -> impl Stream<Item = Result<TriggerV1, Self::Error>> + Send {
        try_stream! {
            let key = (*segment_id, slab_id);
            let Some(entry) = self.0.slab_triggers_v1.get_async(&key).await else {
                return;
            };

            for trigger in entry.iter() {
                yield trigger.clone();
            }
        }
    }

    /// Delete a v1 slab from v1 `timer_slabs` table.
    ///
    /// V1 table uses PK `((segment_id, id), key, time)`.
    ///
    /// # Arguments
    ///
    /// * `segment_id` - Segment ID.
    /// * `slab_id` - Slab ID to delete.
    ///
    /// # Errors
    ///
    /// Never returns an error.
    async fn delete_slab_v1(
        &self,
        segment_id: &SegmentId,
        slab_id: SlabId,
    ) -> Result<(), Self::Error> {
        let key = (*segment_id, slab_id);
        self.0.slab_triggers_v1.remove_async(&key).await;
        Ok(())
    }

    /// Clear all v1 triggers for a specific key.
    ///
    /// # Arguments
    ///
    /// * `segment_id` - Segment ID.
    /// * `key` - Entity key to clear.
    ///
    /// # Errors
    ///
    /// Never returns an error.
    async fn clear_key_triggers_v1(
        &self,
        segment_id: &SegmentId,
        key: &Key,
    ) -> Result<(), Self::Error> {
        let map_key = (*segment_id, key.clone());
        self.0.key_triggers_v1.remove_async(&map_key).await;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::InMemoryTriggerStore;
    use crate::trigger_store_tests;
    use std::convert::Infallible;

    // Run the full suite of TriggerStore compliance tests on this implementation.
    trigger_store_tests!(
        InMemoryTriggerStore,
        async { Result::<_, Infallible>::Ok(InMemoryTriggerStore::new()) },
        100
    );
}
