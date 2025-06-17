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
use crate::timers::Trigger;
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::timers::slab::{Slab, SlabId};
use crate::timers::store::{Segment, SegmentId, TriggerStore};
use async_stream::try_stream;
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
/// Maintains four concurrent maps to support dual indexing and segment
/// management.
#[derive(Debug, Default)]
struct Inner {
    /// Maps each segment ID to its (name, `slab_size`).
    segments: HashMap<SegmentId, (String, CompactDuration)>,

    /// Maps each segment ID to its active set of slab IDs.
    segment_slabs: HashMap<SegmentId, BTreeSet<SlabId>>,

    /// Time-based index: maps each `Slab` to the set of `Trigger`s in that
    /// slab.
    slab_triggers: HashMap<Slab, BTreeSet<Trigger>>,

    /// Key-based index: maps (segment, key) to the set of `Trigger`s for that
    /// key.
    key_triggers: HashMap<(SegmentId, Key), BTreeSet<Trigger>>,
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
            .upsert_async(segment.id, (segment.name, segment.slab_size))
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
            let (name, slab_size) = e.get();
            Segment {
                id: *segment_id,
                name: name.clone(),
                slab_size: *slab_size,
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
    async fn insert_slab(
        &self,
        segment_id: &SegmentId,
        slab_id: SlabId,
    ) -> Result<(), Self::Error> {
        self.0
            .segment_slabs
            .entry_async(*segment_id)
            .await
            .or_default()
            .get_mut()
            .insert(slab_id);

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
        try_stream! {
            let Some(set) = self.0.slab_triggers.get_async(slab).await else {
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
        self.0
            .slab_triggers
            .entry_async(slab)
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
        let Some(mut entry) = self.0.slab_triggers.get_async(slab).await else {
            return Ok(());
        };

        let trigger = Trigger {
            key: key.clone(),
            time,
            span: Span::current(),
        };

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
        self.0.slab_triggers.remove_async(slab).await;
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

        let trigger = Trigger {
            key: key.clone(),
            time,
            span: Span::none(),
        };

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
}

#[cfg(test)]
mod test {
    use super::InMemoryTriggerStore;
    use crate::trigger_store_tests;
    use std::convert::Infallible;

    // Run the full suite of TriggerStore compliance tests on this implementation.
    trigger_store_tests!(InMemoryTriggerStore, async {
        Result::<_, Infallible>::Ok(InMemoryTriggerStore::new())
    });
}
