//! The trigger operations of the in-memory store.

use super::InMemoryTriggerStore;
use crate::Key;
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::timers::slab::{Slab, SlabId};
use crate::timers::store::operations::TriggerOperations;
use crate::timers::store::{Segment, SegmentVersion};
use crate::timers::{TimerType, Trigger};
use async_stream::try_stream;
use futures::TryStreamExt;
use futures::stream::Stream;
use smallvec::SmallVec;
use std::convert::Infallible;
use std::ops::RangeInclusive;
use tokio::join;

impl TriggerOperations for InMemoryTriggerStore {
    type Error = Infallible;

    fn segment(&self) -> &Segment {
        &self.segment
    }

    // -- Segment management operations --

    async fn insert_segment(&self) -> Result<(), Self::Error> {
        let segment = &self.segment;
        self.inner
            .segments
            .upsert_async(
                segment.id,
                (segment.name.clone(), segment.slab_size, segment.version),
            )
            .await;

        Ok(())
    }

    async fn get_segment(&self) -> Result<Option<Segment>, Self::Error> {
        let segment_id = self.segment.id;
        Ok(self.inner.segments.get_async(&segment_id).await.map(|e| {
            let (name, slab_size, version) = e.get();
            Segment {
                id: segment_id,
                name: name.clone(),
                slab_size: *slab_size,
                version: *version,
            }
        }))
    }

    async fn delete_segment(&self) -> Result<(), Self::Error> {
        let segment_id = self.segment.id;
        // Remove from both the segments map and the segment_slabs map.
        join!(
            self.inner.segments.remove_async(&segment_id),
            self.inner.segment_slabs.remove_async(&segment_id)
        );

        Ok(())
    }

    // -- Slab management operations --

    fn get_slabs(&self) -> impl Stream<Item = Result<SlabId, Self::Error>> + use<'_> {
        let segment_id = self.segment.id;
        try_stream! {
            let Some(entry) = self.inner.segment_slabs.get_async(&segment_id).await else {
                return;
            };

            for &slab_id in entry.iter() {
                yield slab_id;
            }
        }
    }

    fn get_slab_range(
        &self,
        range: RangeInclusive<SlabId>,
    ) -> impl Stream<Item = Result<SlabId, Self::Error>> + use<'_> {
        let segment_id = self.segment.id;
        try_stream! {
            let Some(entry) = self.inner.segment_slabs.get_async(&segment_id).await else {
                return;
            };

            for &slab_id in entry.range(range) {
                yield slab_id;
            }
        }
    }

    async fn insert_slab(&self, slab: Slab) -> Result<(), Self::Error> {
        let segment_id = self.segment.id;
        self.inner
            .segment_slabs
            .entry_async(segment_id)
            .await
            .or_default()
            .get_mut()
            .insert(slab.id());

        Ok(())
    }

    async fn delete_slab(&self, slab_id: SlabId) -> Result<(), Self::Error> {
        let segment_id = self.segment.id;
        let Some(mut entry) = self.inner.segment_slabs.get_async(&segment_id).await else {
            return Ok(());
        };

        entry.get_mut().remove(&slab_id);
        if entry.is_empty() {
            let _ = entry.remove();
        }

        Ok(())
    }

    async fn get_slab_watermark(&self) -> Result<Option<SlabId>, Self::Error> {
        let segment_id = self.segment.id;
        Ok(self
            .inner
            .slab_watermarks
            .get_async(&segment_id)
            .await
            .map(|entry| *entry.get()))
    }

    async fn set_slab_watermark(&self, watermark: Option<SlabId>) -> Result<(), Self::Error> {
        let segment_id = self.segment.id;
        match watermark {
            Some(w) => {
                self.inner.slab_watermarks.upsert_async(segment_id, w).await;
            }
            None => {
                self.inner.slab_watermarks.remove_async(&segment_id).await;
            }
        }
        Ok(())
    }

    async fn batch_insert_slab_with_watermark(
        &self,
        slab: Slab,
        watermark: Option<SlabId>,
    ) -> Result<(), Self::Error> {
        // In-memory mirror of the Cassandra UNLOGGED BATCH: insert the slab
        // clustering row, then lower the watermark. The memory store has no
        // crashes to atomise against, so sequential ops are equivalent.
        self.insert_slab(slab).await?;
        self.set_slab_watermark(watermark).await?;
        Ok(())
    }

    // -- Slab trigger operations (time index) --

    /// Stream all triggers of a specific type within a given slab.
    fn get_slab_triggers<'a>(
        &'a self,
        slab: &'a Slab,
        timer_type: TimerType,
    ) -> impl Stream<Item = Result<Trigger, Self::Error>> + use<'a> {
        let segment_id = self.segment.id;
        let slab_size = slab.size();
        let slab_id = slab.id();

        try_stream! {
            let partition_key = (segment_id, slab_size, slab_id);
            let Some(triggers_map) = self.inner.slab_triggers.get_async(&partition_key).await else {
                return;
            };

            // Filter triggers by timer_type using range query on BTreeMap
            for ((t_type, _key, _time), trigger) in triggers_map.iter() {
                if *t_type == timer_type {
                    yield trigger.clone();
                }
            }
        }
    }

    /// Stream ALL triggers within a slab across all timer types.
    fn get_slab_triggers_all_types(
        &self,
        slab: Slab,
    ) -> impl Stream<Item = Result<Trigger, Self::Error>> + use<'_> {
        let segment_id = self.segment.id;
        let slab_size = slab.size();
        let slab_id = slab.id();

        try_stream! {
            let partition_key = (segment_id, slab_size, slab_id);
            let Some(triggers_map) = self.inner.slab_triggers.get_async(&partition_key).await else {
                return;
            };

            // Stream all triggers from the partition
            for trigger in triggers_map.values() {
                yield trigger.clone();
            }
        }
    }

    /// Insert a trigger into a slab's time index.
    async fn insert_slab_trigger(&self, slab: Slab, trigger: Trigger) -> Result<(), Self::Error> {
        let partition_key = (self.segment.id, slab.size(), slab.id());
        let clustering_key = (trigger.timer_type, trigger.key.clone(), trigger.time);

        self.inner
            .slab_triggers
            .entry_async(partition_key)
            .await
            .or_default()
            .get_mut()
            .insert(clustering_key, trigger);

        Ok(())
    }

    /// Delete a specific trigger from a slab's time index.
    async fn delete_slab_trigger(
        &self,
        slab: &Slab,
        timer_type: TimerType,
        key: &Key,
        time: CompactDateTime,
    ) -> Result<(), Self::Error> {
        let partition_key = (self.segment.id, slab.size(), slab.id());
        let clustering_key = (timer_type, key.clone(), time);

        let Some(mut entry) = self.inner.slab_triggers.get_async(&partition_key).await else {
            return Ok(());
        };

        entry.get_mut().remove(&clustering_key);
        if entry.is_empty() {
            let _ = entry.remove();
        }

        Ok(())
    }

    /// Remove all triggers from a slab's time index across ALL timer types.
    ///
    /// This clears both Application and `DeferredMessage` timers. Used for
    /// `slab_size` migration and cleanup operations.
    async fn clear_slab_triggers(&self, slab: &Slab) -> Result<(), Self::Error> {
        // Clear the entire partition (all timer types)
        let partition_key = (self.segment.id, slab.size(), slab.id());
        self.inner.slab_triggers.remove_async(&partition_key).await;
        Ok(())
    }

    // -- Key trigger operations (entity index) --

    /// Stream all scheduled times for a given key and timer type.
    fn get_key_times<'a>(
        &'a self,
        timer_type: TimerType,
        key: &'a Key,
    ) -> impl Stream<Item = Result<CompactDateTime, Self::Error>> + Send + use<'a> {
        self.get_key_triggers(timer_type, key)
            .map_ok(|trigger| trigger.time)
    }

    /// Stream all triggers for a given key and timer type.
    fn get_key_triggers<'a>(
        &'a self,
        timer_type: TimerType,
        key: &'a Key,
    ) -> impl Stream<Item = Result<Trigger, Self::Error>> + Send + use<'a> {
        let segment_id = self.segment.id;
        try_stream! {
            let partition_key = (segment_id, key.clone());
            let Some(triggers_map) = self.inner.key_triggers.get_async(&partition_key).await else {
                return;
            };

            // Filter triggers by timer_type
            for ((t_type, _time), trigger) in triggers_map.iter() {
                if *t_type == timer_type {
                    yield trigger.clone();
                }
            }
        }
    }

    /// Stream ALL triggers for a given key across all timer types.
    fn get_key_triggers_all_types<'a>(
        &'a self,
        key: &'a Key,
    ) -> impl Stream<Item = Result<Trigger, Self::Error>> + Send + use<'a> {
        let segment_id = self.segment.id;
        try_stream! {
            let partition_key = (segment_id, key.clone());
            let Some(triggers_map) = self.inner.key_triggers.get_async(&partition_key).await else {
                return;
            };

            // Stream all triggers from the partition
            for trigger in triggers_map.values() {
                yield trigger.clone();
            }
        }
    }

    /// Upsert a trigger into the key-based index.
    ///
    /// Duplicate logical timers replace mutable metadata by construction
    /// because the map key is `(timer_type, time)`.
    async fn upsert_key_trigger(&self, trigger: Trigger) -> Result<(), Self::Error> {
        let segment_id = self.segment.id;
        let partition_key = (segment_id, trigger.key.clone());
        let clustering_key = (trigger.timer_type, trigger.time);

        self.inner
            .key_triggers
            .entry_async(partition_key)
            .await
            .or_default()
            .get_mut()
            .insert(clustering_key, trigger);

        Ok(())
    }

    /// Delete a specific trigger from the key-based index.
    async fn delete_key_trigger(
        &self,
        timer_type: TimerType,
        key: &Key,
        time: CompactDateTime,
    ) -> Result<(), Self::Error> {
        let segment_id = self.segment.id;
        let partition_key = (segment_id, key.clone());
        let clustering_key = (timer_type, time);

        let Some(mut entry) = self.inner.key_triggers.get_async(&partition_key).await else {
            return Ok(());
        };

        entry.get_mut().remove(&clustering_key);
        if entry.is_empty() {
            let _ = entry.remove();
        }

        Ok(())
    }

    /// Remove all triggers for a key and timer type from the key index.
    async fn clear_key_triggers(
        &self,
        timer_type: TimerType,
        key: &Key,
    ) -> Result<(), Self::Error> {
        let segment_id = self.segment.id;
        let partition_key = (segment_id, key.clone());
        let Some(mut entry) = self.inner.key_triggers.get_async(&partition_key).await else {
            return Ok(());
        };

        // Remove all triggers matching the timer_type
        entry
            .get_mut()
            .retain(|(t_type, _time), _trigger| *t_type != timer_type);

        if entry.is_empty() {
            let _ = entry.remove();
        }

        Ok(())
    }

    /// Atomically clears existing timers and schedules a new one in the key
    /// index.
    ///
    /// For in-memory store, this simply clears and inserts.
    async fn clear_and_schedule_key(
        &self,
        trigger: Trigger,
    ) -> Result<SmallVec<[CompactDateTime; 1]>, Self::Error> {
        let segment_id = self.segment.id;
        let partition_key = (segment_id, trigger.key.clone());
        let clustering_key = (trigger.timer_type, trigger.time);

        // Get or create the partition entry
        let mut entry = self
            .inner
            .key_triggers
            .entry_async(partition_key)
            .await
            .or_default();

        // Collect old times before clearing (exclude the new trigger's own time).
        let old_times: SmallVec<[CompactDateTime; 1]> = entry
            .get()
            .keys()
            .filter(|(t_type, time)| *t_type == trigger.timer_type && *time != trigger.time)
            .map(|(_, time)| *time)
            .collect();

        // Clear all existing triggers for this timer_type, then insert the new one.
        entry
            .get_mut()
            .retain(|(t_type, _time), _| *t_type != trigger.timer_type);
        entry.get_mut().insert(clustering_key, trigger);

        Ok(old_times)
    }

    /// Remove all triggers for a key across ALL timer types from the key index.
    async fn clear_key_triggers_all_types(&self, key: &Key) -> Result<(), Self::Error> {
        let segment_id = self.segment.id;
        let partition_key = (segment_id, key.clone());
        self.inner.key_triggers.remove_async(&partition_key).await;
        Ok(())
    }

    async fn current_trigger(
        &self,
        key: &Key,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> Result<Option<Trigger>, Self::Error> {
        let partition_key = (self.segment.id, key.clone());
        let clustering_key = (timer_type, time);
        let Some(entry) = self.inner.key_triggers.get_async(&partition_key).await else {
            return Ok(None);
        };
        // entry.get() returns &BTreeMap<...>; then look up by clustering key.
        Ok(entry.get().get(&clustering_key).cloned())
    }

    // -- V1 migration methods --

    /// Update segment metadata including version and slab size.
    async fn update_segment_version(
        &self,
        new_version: SegmentVersion,
        new_slab_size: CompactDuration,
    ) -> Result<(), Self::Error> {
        let segment_id = self.segment.id;
        if let Some(entry) = self.inner.segments.get_async(&segment_id).await {
            let (name, ..) = entry.get();
            let name = name.clone();
            drop(entry);
            self.inner
                .segments
                .upsert_async(segment_id, (name, new_slab_size, new_version))
                .await;
        }
        Ok(())
    }
}
