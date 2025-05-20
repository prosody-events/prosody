use crate::Key;
use crate::timers::Trigger;
use crate::timers::datetime::CompactDateTime;
use crate::timers::slab::{Slab, SlabId};
use futures::Stream;
use std::error::Error;
use std::time::Duration;
use uuid::Uuid;

pub mod memory;

pub type SegmentId = Uuid;

#[derive(Clone, Debug)]
pub struct Segment {
    id: SegmentId,
    name: String,
    slab_size: Duration,
}

pub trait TriggerStore {
    type Error: Error;

    // segments
    async fn insert_segment(&self, segment: &Segment) -> Result<(), Self::Error>;
    async fn get_segment(&self, segment_id: &SegmentId) -> Result<Option<Segment>, Self::Error>;
    async fn delete_segment(&self, segment_id: &SegmentId) -> Result<(), Self::Error>;

    // segment slab identifiers
    fn get_segment_slab_ids(
        &self,
        segment_id: &SegmentId,
    ) -> impl Stream<Item = Result<SlabId, Self::Error>>;

    async fn insert_segment_slab_id(
        &self,
        segment_id: &SegmentId,
        slab_id: SlabId,
    ) -> Result<(), Self::Error>;

    async fn delete_segment_slab_id(
        &self,
        segment_id: &SegmentId,
        slab_id: SlabId,
    ) -> Result<(), Self::Error>;

    // slab triggers
    fn get_slab_triggers(&self, slab: &Slab) -> impl Stream<Item = Result<Trigger, Self::Error>>;
    async fn insert_slab_trigger(&self, slab: &Slab, trigger: &Trigger) -> Result<(), Self::Error>;
    async fn delete_slab_trigger(&self, slab: &Slab, trigger: &Trigger) -> Result<(), Self::Error>;
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
        trigger: &Trigger,
    ) -> Result<(), Self::Error>;

    async fn delete_key_trigger(
        &self,
        segment_id: &SegmentId,
        trigger: &Trigger,
    ) -> Result<(), Self::Error>;

    async fn clear_key_triggers(&self, segment: &SegmentId, key: &Key) -> Result<(), Self::Error>;
}
