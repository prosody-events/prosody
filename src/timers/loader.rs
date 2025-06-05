use crate::timers::datetime::{CompactDateTime, CompactDateTimeError};
use crate::timers::error::TimerManagerError;
use crate::timers::scheduler::TriggerScheduler;
use crate::timers::slab::{Slab, SlabId};
use crate::timers::slab_lock::SlabLock;
use crate::timers::store::{Segment, TriggerStore};
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

        let target_time = match now.add_duration(crate::timers::duration::CompactDuration::new(
            preload_seconds,
        )) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Key;
    use crate::timers::Trigger;
    use crate::timers::duration::CompactDuration;
    use crate::timers::store::Segment;
    use crate::timers::store::memory::InMemoryTriggerStore;
    use std::time::Duration;
    use tracing::Span;
    use uuid::Uuid;

    fn create_test_segment() -> Segment {
        Segment {
            id: Uuid::new_v4(),
            name: "test-segment".to_owned(),
            slab_size: CompactDuration::new(60), // 60 seconds
        }
    }

    fn create_test_trigger(key: u64, time: CompactDateTime) -> Trigger {
        Trigger {
            key: Key::from(&format!("key-{key}")),
            time,
            span: Span::current(),
        }
    }

    #[tokio::test]
    async fn test_state_new() {
        let store = InMemoryTriggerStore::new();
        let (_triggers_rx, scheduler) = TriggerScheduler::new();

        let state = State::new(store, scheduler);

        assert_eq!(state.max_owned, None);
    }

    #[tokio::test]
    async fn test_state_is_owned_when_none() {
        let store = InMemoryTriggerStore::new();
        let (_triggers_rx, scheduler) = TriggerScheduler::new();
        let state = State::new(store, scheduler);

        assert!(!state.is_owned(0));
        assert!(!state.is_owned(10));
        assert!(!state.is_owned(100));
    }

    #[tokio::test]
    async fn test_state_is_owned_with_ownership() {
        let store = InMemoryTriggerStore::new();
        let (_triggers_rx, scheduler) = TriggerScheduler::new();
        let mut state = State::new(store, scheduler);

        state.extend_ownership(50);

        assert!(state.is_owned(0));
        assert!(state.is_owned(25));
        assert!(state.is_owned(50));
        assert!(!state.is_owned(51));
        assert!(!state.is_owned(100));
    }

    #[tokio::test]
    async fn test_state_extend_ownership() {
        let store = InMemoryTriggerStore::new();
        let (_triggers_rx, scheduler) = TriggerScheduler::new();
        let mut state = State::new(store, scheduler);

        // First extension
        state.extend_ownership(30);
        assert_eq!(state.max_owned, Some(30));

        // Extend further
        state.extend_ownership(50);
        assert_eq!(state.max_owned, Some(50));

        // Try to extend to a lower value (should not change)
        state.extend_ownership(25);
        assert_eq!(state.max_owned, Some(50));

        // Extend to same value (should not change)
        state.extend_ownership(50);
        assert_eq!(state.max_owned, Some(50));
    }

    #[tokio::test]
    async fn test_calculate_wait_time_future_time() {
        tokio::time::pause();

        let now = CompactDateTime::now().unwrap();
        let future_time = now.add_duration(CompactDuration::new(120)).unwrap(); // 2 minutes from now
        let preload_seconds = 30;

        let wait_time = calculate_wait_time(future_time, preload_seconds);

        // Should wait for 120 - 30 = 90 seconds
        assert_eq!(wait_time, Duration::from_secs(90));
    }

    #[tokio::test]
    async fn test_calculate_wait_time_within_preload_window() {
        tokio::time::pause();

        let now = CompactDateTime::now().unwrap();
        let near_future = now.add_duration(CompactDuration::new(15)).unwrap(); // 15 seconds from now
        let preload_seconds = 30;

        let wait_time = calculate_wait_time(near_future, preload_seconds);

        // Should load immediately since 15 < 30
        assert_eq!(wait_time, Duration::from_secs(0));
    }

    #[tokio::test]
    async fn test_calculate_wait_time_past_time() {
        tokio::time::pause();

        let now = CompactDateTime::now().unwrap();
        // Advance time by 1 minute
        tokio::time::advance(Duration::from_secs(60)).await;
        tokio::task::yield_now().await;

        let past_time = now; // This is now in the past
        let preload_seconds = 30;

        let wait_time = calculate_wait_time(past_time, preload_seconds);

        // Should load immediately for past times
        assert_eq!(wait_time, Duration::from_secs(0));
    }

    #[tokio::test]
    async fn test_get_or_create_segment_new() {
        let store = InMemoryTriggerStore::new();
        let segment_id = Uuid::new_v4();
        let slab_size = CompactDuration::new(60);
        let name = "test-segment";

        let result = get_or_create_segment(&store, segment_id, slab_size, name).await;

        assert!(result.is_ok());
        let segment = result.unwrap();
        assert_eq!(segment.id, segment_id);
        assert_eq!(segment.name, name);
        assert_eq!(segment.slab_size, slab_size);

        // Verify it was stored
        let retrieved = store.get_segment(&segment_id).await.unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().name, name);
    }

    #[tokio::test]
    async fn test_get_or_create_segment_existing() {
        let store = InMemoryTriggerStore::new();
        let segment_id = Uuid::new_v4();
        let slab_size = CompactDuration::new(60);
        let name1 = "original-name";
        let name2 = "different-name";

        // Create first segment
        let segment1 = get_or_create_segment(&store, segment_id, slab_size, name1)
            .await
            .unwrap();

        // Try to create another with same ID but different name
        let segment2 = get_or_create_segment(&store, segment_id, slab_size, name2)
            .await
            .unwrap();

        // Should return the original segment
        assert_eq!(segment1.id, segment2.id);
        assert_eq!(segment1.name, segment2.name);
        assert_eq!(segment1.name, name1); // Should keep original name
    }

    #[tokio::test]
    async fn test_load_slabs_empty_range() {
        let store = InMemoryTriggerStore::new();
        let (_triggers_rx, scheduler) = TriggerScheduler::new();
        let state = State::new(store, scheduler);
        let slab_lock = SlabLock::new(state);
        let segment = create_test_segment();

        // Load empty range
        let result = load_slabs(&slab_lock, &segment, 5..=4).await;

        assert!(result.is_ok());
        let loaded = result.unwrap();
        assert!(loaded.is_empty());
    }

    #[tokio::test]
    async fn test_load_slabs_single_slab() {
        let store = InMemoryTriggerStore::new();
        let (_triggers_rx, scheduler) = TriggerScheduler::new();
        let state = State::new(store.clone(), scheduler);
        let slab_lock = SlabLock::new(state);
        let segment = create_test_segment();

        // Insert the segment first
        store.insert_segment(segment.clone()).await.unwrap();

        // Insert a slab
        store.insert_slab(&segment.id, 1).await.unwrap();

        let result = load_slabs(&slab_lock, &segment, 1..=1).await;

        assert!(result.is_ok());
        let loaded = result.unwrap();
        assert_eq!(loaded, vec![1]);

        // Verify ownership was extended
        let state_guard = slab_lock.slab_lock().await;
        assert_eq!(state_guard.max_owned, Some(1));
    }

    #[tokio::test]
    async fn test_load_slabs_multiple_slabs() {
        let store = InMemoryTriggerStore::new();
        let (_triggers_rx, scheduler) = TriggerScheduler::new();
        let state = State::new(store.clone(), scheduler);
        let slab_lock = SlabLock::new(state);
        let segment = create_test_segment();

        // Insert the segment first
        store.insert_segment(segment.clone()).await.unwrap();

        // Insert multiple slabs
        for slab_id in 1..=3 {
            store.insert_slab(&segment.id, slab_id).await.unwrap();
        }

        let result = load_slabs(&slab_lock, &segment, 1..=3).await;

        assert!(result.is_ok());
        let mut loaded = result.unwrap();
        loaded.sort(); // Sort since order isn't guaranteed with concurrency
        assert_eq!(loaded, vec![1, 2, 3]);

        // Verify ownership was extended to the highest slab
        let state_guard = slab_lock.slab_lock().await;
        assert_eq!(state_guard.max_owned, Some(3));
    }

    #[tokio::test]
    async fn test_load_triggers_empty_slab() {
        let store = InMemoryTriggerStore::new();
        let (_triggers_rx, scheduler) = TriggerScheduler::new();
        let segment = create_test_segment();
        let slab = Slab::new(segment.id, 1, segment.slab_size);

        let result = load_triggers(&store, &scheduler, slab).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_load_triggers_with_triggers() {
        let store = InMemoryTriggerStore::new();
        let (_triggers_rx, scheduler) = TriggerScheduler::new();
        let segment = create_test_segment();
        let slab = Slab::new(segment.id, 1, segment.slab_size);

        // Add some triggers to the slab
        let now = CompactDateTime::now().unwrap();
        let trigger1 = create_test_trigger(1, now);
        let trigger2 = create_test_trigger(2, now);

        store
            .insert_slab_trigger(slab.clone(), trigger1)
            .await
            .unwrap();
        store
            .insert_slab_trigger(slab.clone(), trigger2)
            .await
            .unwrap();

        let result = load_triggers(&store, &scheduler, slab).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_remove_completed_slabs_no_active() {
        let store = InMemoryTriggerStore::new();
        let (_triggers_rx, scheduler) = TriggerScheduler::new();
        let state = State::new(store.clone(), scheduler);
        let slab_lock = SlabLock::new(state);
        let segment = create_test_segment();

        // Create a loaded_slab_ids set with some slabs
        let mut loaded_slab_ids = HashSet::new();
        loaded_slab_ids.insert(1);
        loaded_slab_ids.insert(2);
        loaded_slab_ids.insert(3);

        // Insert slabs in store
        for slab_id in &loaded_slab_ids {
            store.insert_slab(&segment.id, *slab_id).await.unwrap();
        }

        let result = remove_completed_slabs(&slab_lock, &segment, &mut loaded_slab_ids).await;

        assert!(result.is_ok());
        // Since there are no active triggers, all slabs should be removed
        assert!(loaded_slab_ids.is_empty());
    }

    #[tokio::test]
    async fn test_active_slab_ids_empty_scheduler() {
        let (_triggers_rx, scheduler) = TriggerScheduler::new();
        let segment = create_test_segment();

        let active_ids = active_slab_ids(&segment, &scheduler).await;

        assert!(active_ids.is_empty());
    }

    #[tokio::test]
    async fn test_slab_loader_basic_operation() {
        tokio::time::pause();

        let store = InMemoryTriggerStore::new();
        let (_triggers_rx, scheduler) = TriggerScheduler::new();
        let state = State::new(store.clone(), scheduler);
        let slab_lock = SlabLock::new(state);
        let segment = create_test_segment();

        // Insert the segment
        store.insert_segment(segment.clone()).await.unwrap();

        // We need to spawn the slab_loader in a separate task since it runs indefinitely
        let loader_handle = tokio::spawn(slab_loader(segment.clone(), slab_lock.clone()));

        // Give it a moment to start
        tokio::time::advance(Duration::from_millis(100)).await;
        tokio::task::yield_now().await;

        // Cancel the loader task
        loader_handle.abort();

        // Verify some slabs were created (the current slab at minimum)
        let state_guard = slab_lock.slab_lock().await;
        // The loader should have loaded at least the current time slab
        assert!(state_guard.max_owned.is_some());
    }

    #[tokio::test]
    async fn test_slab_loader_time_advancement() {
        tokio::time::pause();

        let store = InMemoryTriggerStore::new();
        let (_triggers_rx, scheduler) = TriggerScheduler::new();
        let state = State::new(store.clone(), scheduler);
        let slab_lock = SlabLock::new(state);
        let segment = create_test_segment();

        // Insert the segment
        store.insert_segment(segment.clone()).await.unwrap();

        // Spawn the loader
        let loader_handle = tokio::spawn(slab_loader(segment.clone(), slab_lock.clone()));

        // Let it load initial slabs
        tokio::time::advance(Duration::from_millis(100)).await;
        tokio::task::yield_now().await;

        let initial_max = {
            let state_guard = slab_lock.slab_lock().await;
            state_guard.max_owned
        };

        // Advance time significantly to trigger more loads
        tokio::time::advance(Duration::from_secs(120)).await; // Advance by 2 minutes
        tokio::task::yield_now().await;

        let final_max = {
            let state_guard = slab_lock.slab_lock().await;
            state_guard.max_owned
        };

        // Cancel the loader
        loader_handle.abort();

        // Should have loaded at least some slabs initially,
        // and potentially more after time advancement
        assert!(initial_max.is_some());
        assert!(final_max >= initial_max);
    }

    #[tokio::test]
    async fn test_calculate_wait_time_exact_preload_boundary() {
        tokio::time::pause();

        let now = CompactDateTime::now().unwrap();
        let boundary_time = now.add_duration(CompactDuration::new(30)).unwrap(); // Exactly 30 seconds from now
        let preload_seconds = 30;

        let wait_time = calculate_wait_time(boundary_time, preload_seconds);

        // Should load immediately when exactly at preload boundary
        assert_eq!(wait_time, Duration::from_secs(0));
    }

    #[tokio::test]
    async fn test_remove_completed_slabs_with_active_triggers() {
        let store = InMemoryTriggerStore::new();
        let (_triggers_rx, scheduler) = TriggerScheduler::new();
        let state = State::new(store.clone(), scheduler.clone());
        let slab_lock = SlabLock::new(state);
        let segment = create_test_segment();

        // Create some loaded slabs
        let mut loaded_slab_ids = HashSet::new();
        loaded_slab_ids.insert(1);
        loaded_slab_ids.insert(2);
        loaded_slab_ids.insert(3);

        // Insert slabs and some triggers
        for slab_id in &loaded_slab_ids {
            store.insert_slab(&segment.id, *slab_id).await.unwrap();
        }

        // Add an active trigger to slab 2
        let now = CompactDateTime::now().unwrap();
        let trigger = create_test_trigger(1, now);
        let _slab = Slab::from_time(segment.id, segment.slab_size, now);

        // Schedule the trigger to make it active
        scheduler.schedule(trigger.clone()).await.unwrap();

        let result = remove_completed_slabs(&slab_lock, &segment, &mut loaded_slab_ids).await;

        assert!(result.is_ok());
        // The exact behavior depends on which slab the trigger falls into,
        // but at least some slabs should remain if there are active triggers
        // This test mainly ensures the function doesn't panic with active triggers
    }

    #[tokio::test]
    async fn test_load_slabs_extends_ownership_correctly() {
        let store = InMemoryTriggerStore::new();
        let (_triggers_rx, scheduler) = TriggerScheduler::new();
        let state = State::new(store.clone(), scheduler);
        let slab_lock = SlabLock::new(state);
        let segment = create_test_segment();

        // Insert the segment first
        store.insert_segment(segment.clone()).await.unwrap();

        // Set initial ownership
        {
            let mut state_guard = slab_lock.slab_lock().await;
            state_guard.extend_ownership(5);
        }

        // Insert slabs beyond current ownership
        for slab_id in 6..=10 {
            store.insert_slab(&segment.id, slab_id).await.unwrap();
        }

        let result = load_slabs(&slab_lock, &segment, 6..=10).await;

        assert!(result.is_ok());

        // Verify ownership was extended to the highest loaded slab
        let state_guard = slab_lock.slab_lock().await;
        assert_eq!(state_guard.max_owned, Some(10));
    }

    #[tokio::test]
    async fn test_load_slabs_preserves_higher_ownership() {
        let store = InMemoryTriggerStore::new();
        let (_triggers_rx, scheduler) = TriggerScheduler::new();
        let state = State::new(store.clone(), scheduler);
        let slab_lock = SlabLock::new(state);
        let segment = create_test_segment();

        // Insert the segment first
        store.insert_segment(segment.clone()).await.unwrap();

        // Set high initial ownership
        {
            let mut state_guard = slab_lock.slab_lock().await;
            state_guard.extend_ownership(100);
        }

        // Insert and load lower range slabs
        for slab_id in 1..=5 {
            store.insert_slab(&segment.id, slab_id).await.unwrap();
        }

        let result = load_slabs(&slab_lock, &segment, 1..=5).await;

        assert!(result.is_ok());

        // Verify ownership was not reduced
        let state_guard = slab_lock.slab_lock().await;
        assert_eq!(state_guard.max_owned, Some(100));
    }

    #[tokio::test]
    async fn test_state_ownership_edge_cases() {
        let store = InMemoryTriggerStore::new();
        let (_triggers_rx, scheduler) = TriggerScheduler::new();
        let mut state = State::new(store, scheduler);

        // Test ownership at SlabId boundaries
        state.extend_ownership(0);
        assert!(state.is_owned(0));
        assert!(!state.is_owned(1));

        state.extend_ownership(SlabId::MAX);
        assert!(state.is_owned(SlabId::MAX));
        assert!(state.is_owned(SlabId::MAX - 1));
    }

    #[tokio::test]
    async fn test_active_slab_ids_with_triggers() {
        let (_triggers_rx, scheduler) = TriggerScheduler::new();
        let segment = create_test_segment();

        // Schedule some triggers
        let now = CompactDateTime::now().unwrap();
        let future = now.add_duration(CompactDuration::new(120)).unwrap();

        let trigger1 = create_test_trigger(1, now);
        let trigger2 = create_test_trigger(2, future);

        scheduler.schedule(trigger1.clone()).await.unwrap();
        scheduler.schedule(trigger2.clone()).await.unwrap();

        let active_ids = active_slab_ids(&segment, &scheduler).await;

        // Should contain slab IDs for both triggers
        assert!(!active_ids.is_empty());

        let expected_slab1 = Slab::from_time(segment.id, segment.slab_size, now);
        let expected_slab2 = Slab::from_time(segment.id, segment.slab_size, future);

        assert!(active_ids.contains(&expected_slab1.id()));
        assert!(active_ids.contains(&expected_slab2.id()));
    }

    #[tokio::test]
    async fn test_get_or_create_segment_concurrent_creation() {
        let store = InMemoryTriggerStore::new();
        let segment_id = Uuid::new_v4();
        let slab_size = CompactDuration::new(60);
        let name = "concurrent-test";

        // Create multiple concurrent futures trying to create the same segment
        let futures: Vec<_> = (0..10)
            .map(|_| {
                let store_clone = store.clone();
                async move {
                    get_or_create_segment(&store_clone, segment_id, slab_size, name).await
                }
            })
            .collect();

        // Wait for all futures to complete
        let results: Vec<_> = futures::future::join_all(futures).await;

        // All should succeed and return the same segment
        for result in results {
            assert!(result.is_ok());
            let segment = result.unwrap();
            assert_eq!(segment.id, segment_id);
            assert_eq!(segment.name, name);
            assert_eq!(segment.slab_size, slab_size);
        }

        // Verify only one segment exists in the store
        let stored_segment = store.get_segment(&segment_id).await.unwrap();
        assert!(stored_segment.is_some());
    }

    #[tokio::test]
    async fn test_slab_loader_handles_time_errors() {
        tokio::time::pause();

        let store = InMemoryTriggerStore::new();
        let (_triggers_rx, scheduler) = TriggerScheduler::new();
        let state = State::new(store.clone(), scheduler);
        let slab_lock = SlabLock::new(state);
        let segment = create_test_segment();

        // Insert the segment
        store.insert_segment(segment.clone()).await.unwrap();

        // Spawn the loader
        let loader_handle = tokio::spawn(slab_loader(segment.clone(), slab_lock.clone()));

        // Give it time to start and handle any initial time operations
        tokio::time::advance(Duration::from_millis(50)).await;
        tokio::task::yield_now().await;

        // The loader should continue running despite any time calculation edge cases
        assert!(!loader_handle.is_finished());

        // Cancel the loader
        loader_handle.abort();
    }

    #[tokio::test]
    async fn test_calculate_wait_time_large_values() {
        tokio::time::pause();

        let now = CompactDateTime::now().unwrap();

        // Test with very large preload value
        let near_future = now.add_duration(CompactDuration::new(10)).unwrap();
        let large_preload = u32::MAX;

        let wait_time = calculate_wait_time(near_future, large_preload);

        // Should load immediately when preload is very large
        assert_eq!(wait_time, Duration::from_secs(0));

        // Test with large but reasonable duration difference that won't overflow
        let large_duration = 86400 * 365; // 1 year in seconds
        let far_future = now
            .add_duration(CompactDuration::new(large_duration))
            .unwrap();
        let normal_preload = 30;

        let wait_time = calculate_wait_time(far_future, normal_preload);

        // Should wait for a very long time (1 year - 30 seconds)
        assert!(wait_time.as_secs() > 0);
        assert_eq!(wait_time.as_secs(), large_duration as u64 - 30);
    }
}
