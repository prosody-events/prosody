use crate::Key;
use crate::timers::Trigger;
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::timers::slab::{Slab, SlabId};
use crate::timers::store::{Segment, SegmentId, TriggerStore};
use async_stream::try_stream;
use futures::stream::Stream;
use scc::HashMap;
use std::collections::BTreeSet;
use std::convert::Infallible;
use std::sync::Arc;
use tokio::join;
use tracing::Span;

#[derive(Clone, Debug, Default)]
pub struct InMemoryTriggerStore(Arc<Inner>);

#[derive(Debug, Default)]
struct Inner {
    segments: HashMap<SegmentId, (String, CompactDuration)>,
    segment_slabs: HashMap<SegmentId, BTreeSet<SlabId>>,
    slab_triggers: HashMap<Slab, BTreeSet<Trigger>>,
    key_triggers: HashMap<(SegmentId, Key), BTreeSet<CompactDateTime>>,
}

impl InMemoryTriggerStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl TriggerStore for InMemoryTriggerStore {
    type Error = Infallible;

    // Segments
    async fn insert_segment(&self, segment: Segment) -> Result<(), Self::Error> {
        self.0
            .segments
            .upsert_async(segment.id, (segment.name, segment.slab_size))
            .await;

        Ok(())
    }

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

    async fn delete_segment(&self, segment_id: &SegmentId) -> Result<(), Self::Error> {
        join!(
            self.0.segments.remove_async(segment_id),
            self.0.segment_slabs.remove_async(segment_id)
        );

        Ok(())
    }

    // Segment slabs
    fn get_slab(&self, segment_id: &SegmentId) -> impl Stream<Item = Result<SlabId, Self::Error>> {
        try_stream! {
            let Some(entry) = self.0.segment_slabs.get_async(segment_id).await else {
                return;
            };

            for &slab_id in entry.iter() {
                yield slab_id;
            }
        }
    }

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

    // Slab triggers
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

    async fn clear_slab_triggers(&self, slab: &Slab) -> Result<(), Self::Error> {
        self.0.slab_triggers.remove_async(slab).await;
        Ok(())
    }

    // Key triggers
    fn get_key_triggers(
        &self,
        segment_id: &SegmentId,
        key: &Key,
    ) -> impl Stream<Item = Result<CompactDateTime, Self::Error>> {
        try_stream! {
            let map_key = (*segment_id, key.clone());
            let Some(entry) = self.0.key_triggers.get_async(&map_key).await else {
                return;
            };

            for &time in entry.iter() {
                yield time;
            }
        }
    }

    async fn insert_key_trigger(
        &self,
        segment_id: &SegmentId,
        trigger: Trigger,
    ) -> Result<(), Self::Error> {
        let map_key = (*segment_id, trigger.key);
        self.0
            .key_triggers
            .entry_async(map_key)
            .await
            .or_default()
            .get_mut()
            .insert(trigger.time);

        Ok(())
    }

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

        entry.get_mut().remove(&time);
        if entry.is_empty() {
            let _ = entry.remove();
        }

        Ok(())
    }

    async fn clear_key_triggers(&self, segment: &SegmentId, key: &Key) -> Result<(), Self::Error> {
        let map_key = (*segment, key.clone());
        self.0.key_triggers.remove_async(&map_key).await;
        Ok(())
    }
}
