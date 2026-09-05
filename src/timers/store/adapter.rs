//! Compose writes to the key and slab rows.

use super::{RetainOldSlab, TriggerStore};
use crate::Key;
use crate::timers::datetime::CompactDateTime;
use crate::timers::slab::Slab;
use crate::timers::{DELETE_CONCURRENCY, TimerType, Trigger};
use crate::util::crash_point;
use futures::{StreamExt, TryStreamExt, stream};
use tokio::try_join;
use tracing::instrument;

/// Compose row writes without a second store trait.
#[derive(Clone)]
pub(crate) struct TableAdapter<T> {
    operations: T,
}

impl<T> TableAdapter<T> {
    pub(crate) fn new(operations: T) -> Self {
        Self { operations }
    }

    pub(crate) fn operations(&self) -> &T {
        &self.operations
    }
}

impl<T: TriggerStore> TableAdapter<T> {
    #[instrument(level = "debug", skip(self, trigger), err)]
    pub(crate) async fn add_trigger(&self, trigger: Trigger) -> Result<(), T::Error> {
        let slab = Slab::from_time(self.operations.segment().slab_size, trigger.time);
        // Coordinate the two trigger-row writes; slab metadata is owned by
        // the scheduler actor (which short-circuits the round-trip when the
        // slab is already known).
        try_join!(
            self.operations.insert_slab_trigger(slab, trigger.clone()),
            self.operations.upsert_key_trigger(trigger),
        )?;
        Ok(())
    }

    pub(crate) async fn add_key_row(&self, trigger: Trigger) -> Result<(), T::Error> {
        self.operations.upsert_key_trigger(trigger).await
    }

    #[instrument(level = "debug", skip(self), err)]
    pub(crate) async fn remove_trigger(
        &self,
        key: &Key,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> Result<(), T::Error> {
        let slab = Slab::from_time(self.operations.segment().slab_size, time);
        // Coordinate: delete from both tables
        try_join!(
            self.operations
                .delete_slab_trigger(&slab, timer_type, key, time),
            self.operations.delete_key_trigger(timer_type, key, time),
        )?;
        Ok(())
    }

    pub(crate) async fn remove_key_row(
        &self,
        key: &Key,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> Result<(), T::Error> {
        self.operations
            .delete_key_trigger(timer_type, key, time)
            .await
    }

    pub(crate) async fn remove_slab_row(
        &self,
        key: &Key,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> Result<(), T::Error> {
        let slab = Slab::from_time(self.operations.segment().slab_size, time);
        self.operations
            .delete_slab_trigger(&slab, timer_type, key, time)
            .await
    }

    /// Write the new slab row before the key swap can commit the attempt.
    /// A stop before the swap leaves a slab row for recovery to remove.
    #[instrument(level = "debug", skip(self, old, new), err)]
    pub(crate) async fn replace(
        &self,
        old: &Trigger,
        new: Trigger,
        retain: RetainOldSlab,
    ) -> Result<(), T::Error> {
        let slab = Slab::from_time(self.operations.segment().slab_size, new.time);
        self.operations
            .insert_slab_trigger(slab, new.clone())
            .await?;
        // Memory writes finish in one poll. Tests must stop between these writes.
        crash_point().await;
        self.operations.replace_key_trigger(old, new).await?;
        if retain == RetainOldSlab::No {
            self.remove_slab_row(&old.key, old.time, old.timer_type)
                .await?;
        }
        Ok(())
    }

    #[instrument(level = "debug", skip(self, trigger, keep), err)]
    pub(crate) async fn clear_and_schedule(
        &self,
        trigger: Trigger,
        keep: &[CompactDateTime],
    ) -> Result<(), T::Error> {
        let slab_size = self.operations.segment().slab_size;
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
            Ok::<(), T::Error>(())
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
