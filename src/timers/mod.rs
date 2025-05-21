#![allow(dead_code)]

use crate::Key;
use crate::timers::datetime::{CompactDateTime, CompactDateTimeError};
use crate::timers::duration::CompactDuration;
use crate::timers::range::LocalRange;
use crate::timers::scheduler::{TimerSchedulerError, TriggerScheduler};
use crate::timers::store::{Segment, SegmentId, TriggerStore};
use chrono::OutOfRangeError;
use educe::Educe;
use futures::{Stream, TryStreamExt};
use std::error::Error;
use std::fmt::Debug;
use thiserror::Error;
use tokio::sync::{mpsc, watch};
use tracing::{Instrument, Span, error};

mod active;
mod datetime;
mod duration;
mod range;
mod scheduler;
mod slab;
mod store;
mod triggers;

const DELETE_CONCURRENCY: usize = 16;

#[derive(Clone, Debug, Educe)]
#[educe(Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct Trigger {
    pub key: Key,
    pub time: CompactDateTime,

    #[educe(Hash(ignore), PartialEq(ignore), PartialOrd(ignore))]
    pub span: Span,
}

#[derive(Clone, Debug)]
pub struct TimerManager<T> {
    segment: Segment,
    range_rx: watch::Receiver<LocalRange>,
    scheduler: TriggerScheduler,
    store: T,
}

impl<T> TimerManager<T>
where
    T: TriggerStore,
{
    pub async fn new(
        segment_id: SegmentId,
        slab_size: CompactDuration,
        name: &str,
        store: T,
    ) -> Result<(mpsc::Receiver<Trigger>, Self), TimerManagerError<T::Error>> {
        let segment = get_or_create_segment(&store, segment_id, slab_size, name).await?;
        let (_range_tx, range_rx) = watch::channel(LocalRange::default());
        let (trigger_rx, scheduler) = TriggerScheduler::new();

        let manager = Self {
            segment,
            range_rx,
            scheduler,
            store,
        };

        Ok((trigger_rx, manager))
    }

    pub fn scheduled_times(
        &self,
        key: &Key,
    ) -> impl Stream<Item = Result<CompactDateTime, TimerManagerError<T::Error>>> {
        self.store
            .get_key_triggers(&self.segment.id, key)
            .map_err(TimerManagerError::Store)
    }

    pub async fn schedule(&self, trigger: Trigger) -> Result<(), TimerManagerError<T::Error>> {
        let mut range_rx = self.range_rx.clone();
        let range = range_rx
            .wait_for(|range| !range.is_loading(trigger.time))
            .await
            .map_err(|_| TimerManagerError::Shutdown)?;

        self.store
            .add_trigger(&self.segment, trigger.clone())
            .await
            .map_err(TimerManagerError::Store)?;

        if range.owns(trigger.time) {
            self.scheduler.schedule(trigger).await?;
        }

        Ok(())
    }

    pub async fn unschedule(
        &self,
        key: &Key,
        time: CompactDateTime,
    ) -> Result<(), TimerManagerError<T::Error>> {
        let mut range_rx = self.range_rx.clone();
        let range = range_rx
            .wait_for(|range| !range.is_loading(time))
            .await
            .map_err(|_| TimerManagerError::Shutdown)?;

        if range.owns(time) {
            let trigger = Trigger {
                key: key.clone(),
                time,
                span: Span::current(),
            };

            self.scheduler.unschedule(trigger).await?;
        }

        self.store
            .remove_trigger(&self.segment, key, time)
            .await
            .map_err(TimerManagerError::Store)
    }

    pub async fn unschedule_all(&self, key: &Key) -> Result<(), TimerManagerError<T::Error>> {
        let span = Span::current();

        self.scheduled_times(key)
            .try_for_each_concurrent(DELETE_CONCURRENCY, |time| {
                self.unschedule(key, time).instrument(span.clone())
            })
            .await
    }

    pub async fn is_active(&self, key: &Key, time: CompactDateTime) -> bool {
        self.scheduler.is_active(key, time).await
    }

    pub async fn mark_complete(
        &self,
        key: &Key,
        time: CompactDateTime,
    ) -> Result<(), TimerManagerError<T::Error>> {
        self.scheduler.deactivate(key, time).await;

        self.store
            .remove_trigger(&self.segment, key, time)
            .await
            .map_err(TimerManagerError::Store)?;

        Ok(())
    }
}

async fn get_or_create_segment<T>(
    store: &T,
    segment_id: SegmentId,
    slab_size: CompactDuration,
    name: &str,
) -> Result<Segment, TimerManagerError<T::Error>>
where
    T: TriggerStore,
{
    if let Some(segment) = store
        .get_segment(&segment_id)
        .await
        .map_err(TimerManagerError::Store)?
    {
        return Ok(segment);
    }

    let segment = Segment {
        id: segment_id,
        name: name.to_owned(),
        slab_size,
    };

    store
        .insert_segment(segment.clone())
        .await
        .map_err(TimerManagerError::Store)?;

    Ok(segment)
}

#[derive(Debug, Error)]
pub enum TimerManagerError<T>
where
    T: Error + Debug,
{
    #[error("Timer store error: {0:#}")]
    Store(T),

    #[error("Failed to schedule timer: {0:#}")]
    Scheduler(#[from] TimerSchedulerError),

    #[error(transparent)]
    DateTime(#[from] CompactDateTimeError),

    #[error("Time must be in the future: {0:#}")]
    PastTime(#[from] OutOfRangeError),

    #[error("Time not found")]
    NotFound,

    #[error("Timer has been shutdown")]
    Shutdown,
}
