use crate::Key;
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::timers::slab::{Slab, SlabId};
use crate::timers::{DELETE_CONCURRENCY, Trigger};
use futures::{Stream, TryStreamExt};
use std::error::Error;
use tokio::try_join;
use uuid::Uuid;

pub mod memory;

pub type SegmentId = Uuid;

#[derive(Clone, Debug)]
pub struct Segment {
    pub id: SegmentId,
    pub name: String,
    pub slab_size: CompactDuration,
}

pub trait TriggerStore {
    type Error: Error;

    // segments
    async fn insert_segment(&self, segment: Segment) -> Result<(), Self::Error>;
    async fn get_segment(&self, segment_id: &SegmentId) -> Result<Option<Segment>, Self::Error>;
    async fn delete_segment(&self, segment_id: &SegmentId) -> Result<(), Self::Error>;

    // segment slab identifiers
    fn get_slab(&self, segment_id: &SegmentId) -> impl Stream<Item = Result<SlabId, Self::Error>>;

    async fn insert_slab(&self, segment_id: &SegmentId, slab_id: SlabId)
    -> Result<(), Self::Error>;

    async fn delete_slab(&self, segment_id: &SegmentId, slab_id: SlabId)
    -> Result<(), Self::Error>;

    // slab triggers
    fn get_slab_triggers(&self, slab: &Slab) -> impl Stream<Item = Result<Trigger, Self::Error>>;
    async fn insert_slab_trigger(&self, slab: Slab, trigger: Trigger) -> Result<(), Self::Error>;

    async fn delete_slab_trigger(
        &self,
        slab: &Slab,
        key: &Key,
        time: CompactDateTime,
    ) -> Result<(), Self::Error>;

    async fn clear_slab_triggers(&self, slab: &Slab) -> Result<(), Self::Error>;

    // key triggers
    fn get_key_triggers(
        &self,
        segment_id: &SegmentId,
        key: &Key,
    ) -> impl Stream<Item = Result<CompactDateTime, Self::Error>>;

    async fn insert_key_trigger(
        &self,
        segment_id: &SegmentId,
        trigger: Trigger,
    ) -> Result<(), Self::Error>;

    async fn delete_key_trigger(
        &self,
        segment_id: &SegmentId,
        key: &Key,
        time: CompactDateTime,
    ) -> Result<(), Self::Error>;

    async fn clear_key_triggers(&self, segment: &SegmentId, key: &Key) -> Result<(), Self::Error>;

    // high-level trigger operations
    async fn add_trigger(&self, segment: &Segment, trigger: Trigger) -> Result<(), Self::Error> {
        let segment_id = segment.id;
        let slab = Slab::from_time(segment_id, segment.slab_size, trigger.time);

        try_join!(
            self.insert_slab(&segment_id, slab.id()),
            self.insert_slab_trigger(slab, trigger.clone()),
            self.insert_key_trigger(&segment_id, trigger),
        )?;

        Ok(())
    }

    async fn remove_trigger(
        &self,
        segment: &Segment,
        key: &Key,
        time: CompactDateTime,
    ) -> Result<(), Self::Error> {
        let slab = Slab::from_time(segment.id, segment.slab_size, time);

        try_join!(
            self.delete_slab_trigger(&slab, key, time),
            self.delete_key_trigger(&segment.id, key, time)
        )?;

        // Note: We don't remove the slab_id from segment_slabs here
        // as other triggers might still exist in that slab

        Ok(())
    }

    async fn clear_triggers_for_key(
        &self,
        segment_id: &SegmentId,
        key: &Key,
        slab_size: CompactDuration,
    ) -> Result<(), Self::Error> {
        self.get_key_triggers(segment_id, key)
            .try_for_each_concurrent(DELETE_CONCURRENCY, |time| {
                let slab = Slab::from_time(*segment_id, slab_size, time);
                async move { self.delete_slab_trigger(&slab, key, time).await }
            })
            .await?;

        self.clear_key_triggers(segment_id, key).await
    }
}
