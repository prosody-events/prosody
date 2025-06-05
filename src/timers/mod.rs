#![allow(dead_code)]

use crate::Key;
use crate::consumer::{Keyed, Uncommitted};
use crate::timers::datetime::{CompactDateTime, CompactDateTimeError};
use crate::timers::duration::CompactDuration;
use crate::timers::range::RangeLock;
use crate::timers::scheduler::{TimerSchedulerError, TriggerScheduler};
use crate::timers::slab::{Slab, SlabId};
use crate::timers::store::{Segment, SegmentId, TriggerStore};
use ahash::{HashSet, HashSetExt};
use chrono::OutOfRangeError;
use educe::Educe;
use futures::stream::iter;
use futures::{Stream, StreamExt, TryFutureExt, TryStreamExt};
use std::cmp::max;
use std::error::Error;
use std::fmt::Debug;
use std::ops::RangeInclusive;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::spawn;
use tokio::time::sleep;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{Instrument, Span, debug, error, warn};

mod active;
pub mod datetime;
pub mod duration;
mod range;
mod scheduler;
mod slab;
pub mod store;
mod triggers;

const LOAD_CONCURRENCY: usize = 32;
const DELETE_CONCURRENCY: usize = 16;
const RETRY_DURATION: Duration = Duration::from_secs(1);

#[derive(Educe)]
#[educe(Debug(bound = ""))]
pub struct UncommittedTimer<T>
where
    T: TriggerStore,
{
    trigger: Trigger,

    #[educe(Debug(ignore))]
    uncommitted: UncommittedTrigger<T>,
}

pub struct UncommittedTrigger<T>
where
    T: TriggerStore,
{
    key: Key,
    time: CompactDateTime,
    manager: TimerManager<T>,
    completed: bool,
}

impl<T> UncommittedTimer<T>
where
    T: TriggerStore,
{
    pub fn new(trigger: Trigger, manager: TimerManager<T>) -> Self {
        let key = trigger.key.clone();
        let time = trigger.time;
        let completed = false;

        Self {
            trigger,
            uncommitted: UncommittedTrigger {
                key,
                time,
                manager,
                completed,
            },
        }
    }

    pub fn into_inner(self) -> (Trigger, UncommittedTrigger<T>) {
        (self.trigger, self.uncommitted)
    }

    pub fn time(&self) -> CompactDateTime {
        self.trigger.time
    }

    pub fn span(&self) -> &Span {
        &self.trigger.span
    }

    pub async fn is_active(&self) -> bool {
        self.uncommitted.is_active().await
    }
}

impl<T> Uncommitted for UncommittedTimer<T>
where
    T: TriggerStore,
{
    async fn commit(mut self) {
        self.uncommitted.commit().await;
    }

    async fn abort(mut self) {
        self.uncommitted.abort().await;
    }
}

impl<T> Keyed for UncommittedTimer<T>
where
    T: TriggerStore,
{
    type Key = Key;

    fn key(&self) -> &Self::Key {
        &self.trigger.key
    }
}

impl<T> UncommittedTrigger<T>
where
    T: TriggerStore,
{
    pub async fn is_active(&self) -> bool {
        self.manager.is_active(&self.key, self.time).await
    }

    pub async fn commit(&mut self) {
        if self.completed {
            warn!("timer already marked as completed; ignoring commit");
            return;
        }

        loop {
            let Err(error) = self.manager.complete(&self.key, self.time).await else {
                break;
            };

            error!("failed to commit timer: {error:#}; retrying");
            sleep(RETRY_DURATION).await;
        }
    }

    pub async fn abort(&mut self) {
        if self.completed {
            warn!("timer already marked as completed; ignoring abort");
            return;
        }

        self.manager.abort(&self.key, self.time).await;
    }
}

impl<T> Drop for UncommittedTrigger<T>
where
    T: TriggerStore,
{
    fn drop(&mut self) {
        if !self.completed {
            warn!("trigger was dropped without committing");
        }
    }
}

#[derive(Clone, Debug, Educe)]
#[educe(Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct Trigger {
    pub key: Key,
    pub time: CompactDateTime,

    #[educe(Hash(ignore), PartialEq(ignore), PartialOrd(ignore))]
    pub span: Span,
}

#[derive(Educe)]
#[educe(Debug(bound = ""), Clone(bound()))]
pub struct TimerManager<T>(#[educe(Debug(ignore))] Arc<TimerManagerInner<T>>);

pub struct TimerManagerInner<T> {
    segment: Segment,
    state: RangeLock<State<T>>,
}

struct State<T> {
    store: T,
    scheduler: TriggerScheduler,
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
    ) -> Result<(impl Stream<Item = UncommittedTimer<T>>, Self), TimerManagerError<T::Error>> {
        let segment = get_or_create_segment(&store, segment_id, slab_size, name).await?;
        let (trigger_rx, scheduler) = TriggerScheduler::new();
        let state = RangeLock::new(State { store, scheduler });

        spawn(slab_loader(segment.clone(), state.clone()));

        let manager = Self(Arc::new(TimerManagerInner { segment, state }));
        let cloned_manager = manager.clone();
        let stream = ReceiverStream::new(trigger_rx)
            .map(move |trigger| UncommittedTimer::new(trigger, cloned_manager.clone()));

        Ok((stream, manager))
    }

    pub async fn scheduled_times(
        &self,
        key: &Key,
    ) -> Result<Vec<CompactDateTime>, TimerManagerError<T::Error>> {
        self.0
            .state
            .trigger_lock()
            .await
            .store
            .get_key_triggers(&self.0.segment.id, key)
            .map_err(TimerManagerError::Store)
            .try_collect()
            .await
    }

    pub async fn schedule(&self, trigger: Trigger) -> Result<(), TimerManagerError<T::Error>> {
        let slab = Slab::from_time(self.0.segment.id, self.0.segment.slab_size, trigger.time);
        let slab_id = slab.id();
        let state = self.0.state.trigger_lock().await;

        state
            .store
            .add_trigger(&self.0.segment, slab, trigger.clone())
            .await
            .map_err(TimerManagerError::Store)?;

        if state.is_owned(slab_id) {
            state.scheduler.schedule(trigger).await?;
        }

        Ok(())
    }

    pub async fn unschedule(
        &self,
        key: &Key,
        time: CompactDateTime,
    ) -> Result<(), TimerManagerError<T::Error>> {
        let slab = Slab::from_time(self.0.segment.id, self.0.segment.slab_size, time);
        let slab_id = slab.id();
        let state = self.0.state.trigger_lock().await;

        if state.is_owned(slab_id) {
            let trigger = Trigger {
                key: key.clone(),
                time,
                span: Span::current(),
            };

            state.scheduler.unschedule(trigger).await?;
        }

        state
            .store
            .remove_trigger(&self.0.segment, &slab, key, time)
            .await
            .map_err(TimerManagerError::Store)
    }

    pub async fn unschedule_all(&self, key: &Key) -> Result<(), TimerManagerError<T::Error>> {
        let span = Span::current();
        let times = self.scheduled_times(key).await?;

        let futures = times
            .into_iter()
            .map(|time| self.unschedule(key, time).instrument(span.clone()));

        iter(futures)
            .buffer_unordered(DELETE_CONCURRENCY)
            .try_collect::<()>()
            .await
    }

    pub async fn is_active(&self, key: &Key, time: CompactDateTime) -> bool {
        self.0
            .state
            .trigger_lock()
            .await
            .scheduler
            .is_active(key, time)
            .await
    }

    pub async fn complete(
        &self,
        key: &Key,
        time: CompactDateTime,
    ) -> Result<(), TimerManagerError<T::Error>> {
        let slab = Slab::from_time(self.0.segment.id, self.0.segment.slab_size, time);
        let slab_id = slab.id();
        let state = self.0.state.trigger_lock().await;

        if state.is_owned(slab_id) {
            state.scheduler.deactivate(key, time).await;
        }

        state
            .store
            .remove_trigger(&self.0.segment, &slab, key, time)
            .await
            .map_err(TimerManagerError::Store)?;

        Ok(())
    }

    pub async fn abort(&self, key: &Key, time: CompactDateTime) {
        let slab = Slab::from_time(self.0.segment.id, self.0.segment.slab_size, time);
        let slab_id = slab.id();
        let state = self.0.state.trigger_lock().await;

        if state.is_owned(slab_id) {
            state.scheduler.deactivate(key, time).await;
        }
    }
}

async fn slab_loader<T>(segment: Segment, state: RangeLock<State<T>>)
where
    T: TriggerStore,
{
    const MIN_PRELOAD_SECONDS: u32 = 30;
    const RETRY_DELAY: Duration = Duration::from_secs(1);

    let preload_seconds = max(segment.slab_size.seconds() / 2, MIN_PRELOAD_SECONDS);
    let mut loaded_slab_ids = HashSet::new();
    let mut highest_loaded_slab_id: Option<SlabId> = None;

    loop {
        // Calculate target slab based on current time + preload window
        let now = match CompactDateTime::now() {
            Ok(now) => now,
            Err(error) => {
                error!("Failed to get current time: {error:#}; retrying");
                sleep(RETRY_DELAY).await;
                continue;
            }
        };

        let target_time = match now.add_duration(CompactDuration::new(preload_seconds)) {
            Ok(time) => time,
            Err(error) => {
                error!("Failed to calculate target time: {error:#}; retrying");
                sleep(RETRY_DELAY).await;
                continue;
            }
        };

        let target_slab = Slab::from_time(segment.id, segment.slab_size, target_time);
        let target_slab_id = target_slab.id();

        // Determine range to load
        let start_slab_id = highest_loaded_slab_id.map_or(0, |id| id + 1);

        if start_slab_id <= target_slab_id {
            let load_range = start_slab_id..=target_slab_id;

            debug!("Loading slabs {start_slab_id}..={target_slab_id}");
            match load_slabs(&state, &segment, load_range).await {
                Ok(loaded) => {
                    loaded_slab_ids.extend(loaded);
                    highest_loaded_slab_id = Some(target_slab_id);
                    debug!("Successfully loaded slabs up to {target_slab_id}");
                }
                Err(error) => {
                    error!("Failed to load slabs: {error:#}; retrying in {RETRY_DELAY:?}");
                    sleep(RETRY_DELAY).await;
                    continue;
                }
            }
        }

        // Calculate when we need to load the next slab
        let Some(next_slab) = target_slab.next() else {
            error!("Out of slab range - reached maximum slab ID");
            return;
        };

        // clean up old slabs
        if let Err(error) = remove_completed_slabs(&state, &segment, &mut loaded_slab_ids).await {
            error!("Failed to remove completed slabs: {error:#}");
        }

        let wait_time = calculate_wait_time(next_slab.range().start, preload_seconds);
        if !wait_time.is_zero() {
            debug!("Waiting {wait_time:?} before next load cycle");
            sleep(wait_time).await;
        }
    }
}

/// Calculate how long to wait before loading a slab
fn calculate_wait_time(load_time: CompactDateTime, preload_seconds: u32) -> Duration {
    // Get current time
    let now = match CompactDateTime::now() {
        Ok(now) => now,
        Err(error) => {
            error!("Failed to get current time: {error:#}; loading immediately");
            return Duration::from_secs(0);
        }
    };

    // Calculate time to wait
    match load_time.duration_since(now) {
        Ok(duration) => {
            // If time is more than preload_seconds away, wait until we're closer
            if duration.as_secs() > u64::from(preload_seconds) {
                Duration::from_secs(duration.as_secs() - u64::from(preload_seconds))
            } else {
                // Already within preload window, load immediately
                Duration::from_secs(0)
            }
        }
        Err(error) => {
            match error {
                CompactDateTimeError::PastDateTime => {
                    debug!("Load time is in the past; loading immediately");
                }
                CompactDateTimeError::OutOfRange => {
                    error!("Error calculating time until load: {error:#}; loading immediately");
                }
            }
            Duration::from_secs(0)
        }
    }
}

async fn load_slabs<T>(
    state: &RangeLock<State<T>>,
    segment: &Segment,
    slab_range: RangeInclusive<SlabId>,
) -> Result<Vec<SlabId>, TimerManagerError<T::Error>>
where
    T: TriggerStore,
{
    let range_end = *slab_range.end();
    let mut state = state.slab_lock().await;
    let store_ref = &state.store;
    let scheduler_ref = &state.scheduler;

    let loaded = state
        .store
        .get_slab_range(&segment.id, slab_range)
        .map_err(TimerManagerError::Store)
        .map_ok(|slab_id| async move {
            let slab = Slab::new(segment.id, slab_id, segment.slab_size);
            load_triggers(store_ref, scheduler_ref, slab).await?;
            Ok(slab_id)
        })
        .try_buffer_unordered(LOAD_CONCURRENCY)
        .try_collect()
        .await?;

    state.extend_ownership(range_end);
    Ok(loaded)
}

async fn remove_completed_slabs<T>(
    state: &RangeLock<State<T>>,
    segment: &Segment,
    loaded_slab_ids: &mut HashSet<SlabId>,
) -> Result<(), T::Error>
where
    T: TriggerStore,
{
    let state = state.slab_lock().await;
    let store_ref = &state.store;
    let active_slab_ids = active_slab_ids(segment, &state.scheduler).await;
    let completed_slab_ids = loaded_slab_ids.difference(&active_slab_ids).copied();

    let deleted_slab_ids = iter(completed_slab_ids)
        .map(|slab_id| async move {
            store_ref.delete_slab(&segment.id, slab_id).await?;
            Ok(slab_id)
        })
        .buffer_unordered(DELETE_CONCURRENCY)
        .try_collect::<Vec<_>>()
        .await?;

    for deleted_slab_id in deleted_slab_ids {
        loaded_slab_ids.remove(&deleted_slab_id);
    }

    Ok(())
}

async fn load_triggers<T>(
    store: &T,
    scheduler: &TriggerScheduler,
    slab: Slab,
) -> Result<(), TimerManagerError<T::Error>>
where
    T: TriggerStore,
{
    store
        .get_slab_triggers(&slab)
        .map_err(TimerManagerError::Store)
        .try_for_each_concurrent(LOAD_CONCURRENCY, |trigger| {
            scheduler
                .schedule(trigger)
                .map_err(TimerManagerError::Scheduler)
        })
        .await
}

async fn active_slab_ids(segment: &Segment, scheduler: &TriggerScheduler) -> HashSet<SlabId> {
    let mut active_slab_ids = HashSet::new();

    scheduler
        .active_triggers()
        .scan_active_times(|time| {
            active_slab_ids.insert(Slab::from_time(segment.id, segment.slab_size, time).id());
        })
        .await;

    active_slab_ids
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

    #[error("Timer is inactive")]
    Inactive,

    #[error("Timer has been shutdown")]
    Shutdown,
}
