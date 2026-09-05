//! Apply coordinated writes through the store operations.

use super::RetainOldSlab;
use crate::Key;
use crate::timers::DELETE_CONCURRENCY;
use crate::timers::TimerType;
use crate::timers::Trigger;
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::timers::slab::{Slab, SlabId};
use crate::timers::store::operations::TriggerOperations;
use crate::timers::store::{Segment, SegmentId, TriggerStore};
use futures::{Stream, StreamExt, TryStreamExt, stream};
use std::future::Future;
use std::ops::RangeInclusive;
use std::sync::Arc;
use tokio::try_join;
use tracing::instrument;

/// Adapt store operations to the public timer store.
#[derive(Clone)]
pub struct TableAdapter<T> {
    operations: Arc<T>,
}

impl<T> TableAdapter<T> {
    /// Creates a new `TableAdapter` wrapping the given operations.
    pub fn new(operations: T) -> Self {
        Self {
            operations: Arc::new(operations),
        }
    }

    /// Returns a reference to the underlying operations.
    ///
    /// Provides access to low-level `TriggerOperations` methods for cases
    /// where direct primitive access is needed (e.g., migration, internal
    /// maintenance operations).
    #[must_use]
    pub fn operations(&self) -> &T {
        self.operations.as_ref()
    }
}

/// Implements the public `TriggerStore` interface using internal
/// `TriggerOperations`.
impl<T> TriggerStore for TableAdapter<T>
where
    T: TriggerOperations,
{
    type Error = T::Error;

    // ===================================================================
    // Segment accessors
    // ===================================================================

    fn segment(&self) -> Segment {
        self.operations.segment().clone()
    }

    fn segment_id(&self) -> SegmentId {
        self.operations.segment().id
    }

    fn slab_size(&self) -> CompactDuration {
        self.operations.segment().slab_size
    }

    // ===================================================================
    // Pass-through methods: Delegate directly to operations
    // ===================================================================

    fn get_segment(&self) -> impl Future<Output = Result<Option<Segment>, Self::Error>> + Send {
        self.operations.get_segment()
    }

    fn insert_segment(&self) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.operations.insert_segment()
    }

    fn get_slab_range(
        &self,
        range: RangeInclusive<SlabId>,
    ) -> impl Stream<Item = Result<SlabId, Self::Error>> + Send {
        self.operations.get_slab_range(range)
    }

    fn get_slab_triggers_all_types(
        &self,
        slab_id: SlabId,
    ) -> impl Stream<Item = Result<Trigger, Self::Error>> + Send {
        let slab = Slab::new(slab_id, self.slab_size());
        self.operations.get_slab_triggers_all_types(slab)
    }

    fn insert_slab(&self, slab: Slab) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.operations.insert_slab(slab)
    }

    fn delete_slab(&self, slab_id: SlabId) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.operations.delete_slab(slab_id)
    }

    fn get_slab_watermark(
        &self,
    ) -> impl Future<Output = Result<Option<SlabId>, Self::Error>> + Send {
        self.operations.get_slab_watermark()
    }

    fn set_slab_watermark(
        &self,
        watermark: Option<SlabId>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.operations.set_slab_watermark(watermark)
    }

    fn batch_insert_slab_with_watermark(
        &self,
        slab: Slab,
        watermark: Option<SlabId>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.operations
            .batch_insert_slab_with_watermark(slab, watermark)
    }

    fn get_key_times(
        &self,
        timer_type: TimerType,
        key: &Key,
    ) -> impl Stream<Item = Result<(CompactDateTime, i32), Self::Error>> + Send {
        self.operations.get_key_times(timer_type, key)
    }

    fn get_key_triggers(
        &self,
        timer_type: TimerType,
        key: &Key,
    ) -> impl Stream<Item = Result<Trigger, Self::Error>> + Send {
        self.operations.get_key_triggers(timer_type, key)
    }

    // ===================================================================
    // Coordinated writes: Use try_join! for best-effort dual-table
    // consistency
    // ===================================================================

    #[instrument(level = "debug", skip(self, trigger), err)]
    async fn add_trigger(&self, trigger: Trigger) -> Result<(), Self::Error> {
        let slab = Slab::from_time(self.slab_size(), trigger.time);
        // Coordinate the two trigger-row writes; slab metadata is owned by
        // the scheduler actor (which short-circuits the round-trip when the
        // slab is already known).
        try_join!(
            self.operations.insert_slab_trigger(slab, trigger.clone()),
            self.operations.upsert_key_trigger(trigger),
        )?;
        Ok(())
    }

    async fn add_key_row(&self, trigger: Trigger) -> Result<(), Self::Error> {
        self.operations.upsert_key_trigger(trigger).await
    }

    #[instrument(level = "debug", skip(self), err)]
    async fn remove_trigger(
        &self,
        key: &Key,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> Result<(), Self::Error> {
        let slab = Slab::from_time(self.slab_size(), time);
        // Coordinate: delete from both tables
        try_join!(
            self.operations
                .delete_slab_trigger(&slab, timer_type, key, time),
            self.operations.delete_key_trigger(timer_type, key, time),
        )?;
        Ok(())
    }

    async fn remove_key_row(
        &self,
        key: &Key,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> Result<(), Self::Error> {
        self.operations
            .delete_key_trigger(timer_type, key, time)
            .await
    }

    async fn remove_slab_row(
        &self,
        key: &Key,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> Result<(), Self::Error> {
        let slab = Slab::from_time(self.slab_size(), time);
        self.operations
            .delete_slab_trigger(&slab, timer_type, key, time)
            .await
    }

    async fn update_tag(
        &self,
        key: &Key,
        time: CompactDateTime,
        timer_type: TimerType,
        new_tag: i32,
    ) -> Result<(), Self::Error> {
        self.operations
            .update_tag(key, time, timer_type, new_tag)
            .await
    }

    async fn current_tag(
        &self,
        key: &Key,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> Result<Option<i32>, Self::Error> {
        self.operations.current_tag(key, time, timer_type).await
    }

    #[instrument(level = "debug", skip(self, old, new), err)]
    async fn replace(
        &self,
        old: &Trigger,
        new: Trigger,
        retain: RetainOldSlab,
    ) -> Result<(), Self::Error> {
        self.operations
            .replace_key_trigger(old, new.clone())
            .await?;
        self.operations
            .insert_slab_trigger(Slab::from_time(self.slab_size(), new.time), new)
            .await?;
        if retain == RetainOldSlab::No {
            self.remove_slab_row(&old.key, old.time, old.timer_type)
                .await?;
        }
        Ok(())
    }

    #[instrument(level = "debug", skip(self, trigger, keep), err)]
    async fn clear_and_schedule(
        &self,
        trigger: Trigger,
        keep: &[CompactDateTime],
    ) -> Result<(), Self::Error> {
        let slab_size = self.slab_size();
        let new_slab = Slab::from_time(slab_size, trigger.time);
        let key = trigger.key.clone();
        let timer_type = trigger.timer_type;

        // Write the new timer before any old source disappears.
        let write_slab = async {
            if !keep.contains(&trigger.time) {
                self.operations
                    .insert_slab_trigger(new_slab, trigger.clone())
                    .await?;
            }
            Ok::<(), Self::Error>(())
        };
        let ((), old_times) = try_join!(
            write_slab,
            self.operations.clear_and_schedule_key(trigger.clone()),
        )?;

        stream::iter(
            old_times
                .iter()
                .copied()
                .filter(|time| !keep.contains(time)),
        )
        .map(|old_time| {
            let old_slab = Slab::from_time(slab_size, old_time);
            let ops = &self.operations;
            let key = &key;
            async move {
                ops.delete_slab_trigger(&old_slab, timer_type, key, old_time)
                    .await
            }
        })
        .buffer_unordered(DELETE_CONCURRENCY)
        .try_collect::<()>()
        .await?;
        Ok(())
    }
}
