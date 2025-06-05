use crate::timers::datetime::{CompactDateTime, CompactDateTimeError};
use crate::timers::slab_lock::SlabLock;
use crate::timers::scheduler::TriggerScheduler;
use crate::timers::slab::{Slab, SlabId};
use crate::timers::store::{Segment, TriggerStore};
use crate::timers::error::TimerManagerError;
use ahash::{HashSet, HashSetExt};
use futures::stream::iter;
use futures::{StreamExt, TryStreamExt};
use std::cmp::max;
use std::ops::RangeInclusive;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{debug, error};

const LOAD_CONCURRENCY: usize = 32;
const DELETE_CONCURRENCY: usize = 16;

pub struct State<T> {
    pub store: T,
    pub scheduler: TriggerScheduler,
    max_owned: Option<SlabId>,
}

impl<T> State<T> {
    pub fn new(store: T, scheduler: TriggerScheduler) -> Self {
        Self {
            store,
            scheduler,
            max_owned: None,
        }
    }

    pub fn is_owned(&self, slab_id: SlabId) -> bool {
        self.max_owned.filter(|max| slab_id <= *max).is_some()
    }

    pub fn extend_ownership(&mut self, new_max: SlabId) {
        if self
            .max_owned
            .is_some_and(|current_max| current_max > new_max)
        {
            return;
        }

        self.max_owned = Some(new_max);
    }
}



pub async fn slab_loader<T>(segment: Segment, state: SlabLock<State<T>>)
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

        let target_time = match now.add_duration(crate::timers::duration::CompactDuration::new(preload_seconds)) {
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
    state: &SlabLock<State<T>>,
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
    state: &SlabLock<State<T>>,
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
        .try_for_each_concurrent(LOAD_CONCURRENCY, |trigger| async move {
            scheduler
                .schedule(trigger)
                .await
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

pub async fn get_or_create_segment<T>(
    store: &T,
    segment_id: crate::timers::store::SegmentId,
    slab_size: crate::timers::duration::CompactDuration,
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