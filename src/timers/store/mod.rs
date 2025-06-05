use crate::Key;
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::timers::slab::{Slab, SlabId};
use crate::timers::manager::DELETE_CONCURRENCY;
use crate::timers::Trigger;
use futures::{Stream, TryStreamExt};
use std::error::Error;
use std::ops::RangeBounds;
use tokio::try_join;
use uuid::Uuid;

pub mod memory;

#[cfg(test)]
pub mod tests;

pub type SegmentId = Uuid;

#[derive(Clone, Debug)]
pub struct Segment {
    pub id: SegmentId,
    pub name: String,
    pub slab_size: CompactDuration,
}

pub trait TriggerStore: Clone + Send + Sync + 'static {
    type Error: Error + Send + Sync + 'static;

    // segments
    fn insert_segment(
        &self,
        segment: Segment,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    fn get_segment(
        &self,
        segment_id: &SegmentId,
    ) -> impl Future<Output = Result<Option<Segment>, Self::Error>> + Send;

    fn delete_segment(
        &self,
        segment_id: &SegmentId,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    // segment slab identifiers
    fn get_slabs(
        &self,
        segment_id: &SegmentId,
    ) -> impl Stream<Item = Result<SlabId, Self::Error>> + Send;

    fn get_slab_range<B>(
        &self,
        segment_id: &SegmentId,
        range: B,
    ) -> impl Stream<Item = Result<SlabId, Self::Error>> + Send
    where
        B: RangeBounds<SlabId> + Send;

    fn insert_slab(
        &self,
        segment_id: &SegmentId,
        slab_id: SlabId,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    fn delete_slab(
        &self,
        segment_id: &SegmentId,
        slab_id: SlabId,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    // slab triggers
    fn get_slab_triggers(
        &self,
        slab: &Slab,
    ) -> impl Stream<Item = Result<Trigger, Self::Error>> + Send;

    fn insert_slab_trigger(
        &self,
        slab: Slab,
        trigger: Trigger,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    fn delete_slab_trigger(
        &self,
        slab: &Slab,
        key: &Key,
        time: CompactDateTime,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    fn clear_slab_triggers(
        &self,
        slab: &Slab,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    // key triggers
    fn get_key_triggers(
        &self,
        segment_id: &SegmentId,
        key: &Key,
    ) -> impl Stream<Item = Result<CompactDateTime, Self::Error>> + Send;

    fn insert_key_trigger(
        &self,
        segment_id: &SegmentId,
        trigger: Trigger,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    fn delete_key_trigger(
        &self,
        segment_id: &SegmentId,
        key: &Key,
        time: CompactDateTime,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    fn clear_key_triggers(
        &self,
        segment_id: &SegmentId,
        key: &Key,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    // high-level helpers
    fn add_trigger(
        &self,
        segment: &Segment,
        slab: Slab,
        trigger: Trigger,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        let segment_id = segment.id;
        let slab_id = slab.id();
        async move {
            try_join!(
                self.insert_slab(&segment_id, slab_id),
                self.insert_slab_trigger(slab, trigger.clone()),
                self.insert_key_trigger(&segment_id, trigger),
            )?;
            Ok(())
        }
    }

    fn remove_trigger(
        &self,
        segment: &Segment,
        slab: &Slab,
        key: &Key,
        time: CompactDateTime,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        async move {
            try_join!(
                self.delete_slab_trigger(slab, key, time),
                self.delete_key_trigger(&segment.id, key, time)
            )?;
            Ok(())
        }
    }

    fn clear_triggers_for_key(
        &self,
        segment_id: &SegmentId,
        key: &Key,
        slab_size: CompactDuration,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        // pull the stream, then delete per time, then final clear
        let segment_id_copy = *segment_id;
        let key_clone = key.clone();
        let stream = self.get_key_triggers(segment_id, key);

        async move {
            stream
                .try_for_each_concurrent(DELETE_CONCURRENCY, move |time| {
                    let key_clone = key.clone();
                    let slab = Slab::from_time(segment_id_copy, slab_size, time);
                    async move { self.delete_slab_trigger(&slab, &key_clone, time).await }
                })
                .await?;

            self.clear_key_triggers(&segment_id_copy, &key_clone).await
        }
    }
}
