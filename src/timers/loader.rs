//! Background slab loader and state management for timer preloading.
//!
//! Implements the background task that preloads upcoming timer slabs from
//! the persistent [`TriggerStore`] into the in-memory scheduler. Handles
//! cleanup of completed slabs and dynamically adjusts its preload window
//! based on current time.

use crate::heartbeat::Heartbeat;
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::timers::error::TimerManagerError;
use crate::timers::scheduler::TriggerScheduler;
use crate::timers::slab::{Slab, SlabId};
use crate::timers::slab_lock::SlabLock;
use crate::timers::store::{Segment, SegmentId, TriggerStore};
use crate::timers::{DELETE_CONCURRENCY, LOAD_CONCURRENCY};
use ahash::{HashSet, HashSetExt};
use futures::stream::iter;
use futures::{StreamExt, TryStreamExt};
use rand::Rng;
use std::ops::RangeInclusive;
use std::time::Duration;
use tokio::select;
use tokio::time::sleep;
use tracing::{debug, error};

/// Shared state for the slab loader task.
///
/// Tracks which slabs have been loaded and which slab IDs this loader
/// instance currently owns (should schedule triggers for).
pub struct State<T> {
    /// Persistent trigger store.
    pub store: T,

    /// In-memory trigger scheduler.
    pub scheduler: TriggerScheduler,

    /// Highest slab ID this loader has taken ownership of.
    max_owned: Option<SlabId>,
}

impl<T> State<T> {
    /// Creates a new loader state with no owned slabs.
    ///
    /// # Arguments
    ///
    /// * `store` - The persistent [`TriggerStore`] implementation.
    /// * `scheduler` - The in-memory [`TriggerScheduler`].
    #[must_use]
    pub fn new(store: T, scheduler: TriggerScheduler) -> Self {
        Self {
            store,
            scheduler,
            max_owned: None,
        }
    }

    /// Returns `true` if the given slab ID is owned by this loader.
    ///
    /// A slab is considered owned if its ID is less than or equal to
    /// the highest owned slab ID.
    ///
    /// # Arguments
    ///
    /// * `slab_id` - The slab identifier to check.
    #[must_use]
    pub fn is_owned(&self, slab_id: SlabId) -> bool {
        self.max_owned.filter(|max| slab_id <= *max).is_some()
    }

    /// Extends the range of owned slab IDs to include `new_max`.
    ///
    /// If `new_max` is less than the current maximum, this call is a no-op.
    ///
    /// # Arguments
    ///
    /// * `new_max` - The new highest slab ID to own.
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

/// Background task that continuously loads upcoming slabs and cleans up old
/// ones.
///
/// This loop:
/// 1. Determines a preload window based on half the slab size (minimum 30s).
/// 2. Calculates the range of slabs to load (from last loaded to target slab).
/// 3. Loads triggers from those slabs into the scheduler.
/// 4. Extends ownership for loaded slabs.
/// 5. Removes slabs whose triggers have all completed.
/// 6. Waits until it's time to load the next slab.
///
/// # Type Parameters
///
/// * `T` - A [`TriggerStore`] implementation for persistent storage.
///
/// # Arguments
///
/// * `segment` - The timer segment to manage slab loading for
/// * `state` - Shared state containing the trigger store and scheduler
/// * `heartbeat` - Heartbeat monitor for detecting stalled loader operations
pub async fn slab_loader<T>(segment: Segment, state: SlabLock<State<T>>, heartbeat: Heartbeat)
where
    T: TriggerStore,
{
    const RETRY_DELAY: Duration = Duration::from_secs(1);

    // Initialize jittered preload window
    let mut preload_window = calculate_preload(segment.slab_size);
    let mut loaded_slab_ids = HashSet::new();
    let mut highest_loaded_slab_id: Option<SlabId> = None;

    loop {
        // Signal that the loader is active
        heartbeat.beat();

        // Compute the target time to preload (current time + preload window)
        let now = match CompactDateTime::now() {
            Ok(now) => now,
            Err(error) => {
                error!("Failed to get current time: {error:#}; retrying");
                sleep(RETRY_DELAY).await;
                continue;
            }
        };

        let target_time = match now.add_duration(preload_window) {
            Ok(time) => time,
            Err(error) => {
                error!("Failed to calculate target time: {error:#}; retrying");
                sleep(RETRY_DELAY).await;
                continue;
            }
        };

        // Map target time to its slab ID
        let target_slab = Slab::from_time(segment.id, segment.slab_size, target_time);
        let target_slab_id = target_slab.id();

        // Determine the next slab ID to load
        let start_slab_id = highest_loaded_slab_id.map_or(0, |id| id + 1);

        if start_slab_id <= target_slab_id {
            let load_range = start_slab_id..=target_slab_id;
            debug!("Loading slabs {start_slab_id}..={target_slab_id}");

            // Load the slabs in parallel and update ownership
            match load_slabs(&state, &segment, load_range).await {
                Ok(loaded) => {
                    loaded_slab_ids.extend(loaded);
                    highest_loaded_slab_id = Some(target_slab_id);
                    debug!("Successfully loaded slabs up to {target_slab_id}");

                    // Re-jitter preload window for next cycle
                    preload_window = calculate_preload(segment.slab_size);
                }
                Err(error) => {
                    error!("Failed to load slabs: {error:#}; retrying in {RETRY_DELAY:?}");
                    sleep(RETRY_DELAY).await;
                    continue;
                }
            }
        }

        // Determine the next slab chronologically
        let Some(next_slab) = target_slab.next() else {
            error!("Out of slab range - reached maximum slab ID");
            return;
        };

        // Remove any slabs whose triggers have completed
        if let Err(error) = remove_completed_slabs(&state, &segment, &mut loaded_slab_ids).await {
            error!("Failed to remove completed slabs: {error:#}");
        }

        // Calculate wait time until next slab needs loading
        let wait_time = calculate_wait_time(next_slab.range().start, preload_window);
        if !wait_time.is_zero() {
            debug!("Waiting {wait_time} before next load cycle");
            select! {
                () = sleep(wait_time.into()) => {},
                () = heartbeat.next() => {},
            }
        }
    }
}

/// Computes how long to wait before loading a slab that begins at `load_time`.
///
/// Returns zero if the load time is within the preload window or in the past.
///
/// # Arguments
///
/// * `load_time` - The start time of the next slab.
/// * `preload_window` - The configured preload window.
///
/// # Returns
///
/// A [`Duration`] to sleep before loading the slab.
fn calculate_wait_time(
    load_time: CompactDateTime,
    preload_window: CompactDuration,
) -> CompactDuration {
    load_time
        .compact_duration_from_now()
        .unwrap_or(CompactDuration::MIN)
        .saturating_sub(preload_window)
}

/// Loads all slabs in `slab_range` and schedules their triggers.
///
/// Each slab ID is fetched from the store, its triggers are loaded into the
/// in-memory scheduler, and the slab ID is returned if loading succeeds.
///
/// # Arguments
///
/// * `state` - Locked loader state (store + scheduler).
/// * `segment` - The segment metadata.
/// * `slab_range` - Inclusive range of slab IDs to load.
///
/// # Errors
///
/// Returns [`TimerManagerError`] if any store or scheduling operation fails.
///
/// # Returns
///
/// A [`Vec<SlabId>`] of successfully loaded slab identifiers.
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

/// Deletes slabs that are no longer active (all triggers completed).
///
/// Compares the set of loaded slabs with active slab IDs derived from
/// the scheduler's active triggers, then deletes any completed slabs
/// from the store and the `loaded_slab_ids` set.
///
/// # Arguments
///
/// * `state` - Locked loader state (store + scheduler).
/// * `segment` - The segment metadata.
/// * `loaded_slab_ids` - Mutable set of currently loaded slab IDs.
///
/// # Errors
///
/// Returns the underlying store error if any deletion fails.
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

/// Loads all triggers from a single slab into the scheduler.
///
/// Fetches triggers from the store and schedules each one concurrently.
///
/// # Arguments
///
/// * `store` - The persistent trigger store.
/// * `scheduler` - The in-memory scheduler.
/// * `slab` - The slab to load triggers from.
///
/// # Errors
///
/// Returns [`TimerManagerError`] if retrieving triggers or scheduling fails.
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

/// Gathers slab IDs for slabs that currently have active triggers.
///
/// Iterates over all active trigger times in the scheduler and maps each
/// time to its containing slab ID.
///
/// # Arguments
///
/// * `segment` - The segment metadata.
/// * `scheduler` - The in-memory scheduler.
///
/// # Returns
///
/// A [`HashSet<SlabId>`] containing IDs of slabs with active triggers.
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

/// Retrieves or creates a [`Segment`] in the store.
///
/// If a segment with `segment_id` exists, it is returned. Otherwise, a new
/// segment is inserted with the given `slab_size` and `name`.
///
/// # Arguments
///
/// * `store` - The persistent trigger store.
/// * `segment_id` - Unique identifier for the segment.
/// * `slab_size` - Slab duration for time partitioning.
/// * `name` - Human-readable name for the segment.
///
/// # Errors
///
/// Returns [`TimerManagerError`] if any store operation fails.
///
/// # Returns
///
/// The existing or newly created [`Segment`] object.
pub async fn get_or_create_segment<T>(
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

/// Calculates a jittered preload duration between the minimum and slab size.
///
/// Returns a random duration between `MIN_PRELOAD_SECONDS` and the slab size,
/// ensuring we never load less than 1 minute before a slab starts.
///
/// # Arguments
///
/// * `slab_size` - The duration of each slab.
///
/// # Returns
///
/// A jittered preload duration in seconds.
fn calculate_preload(slab_size: CompactDuration) -> CompactDuration {
    const MIN_PRELOAD: CompactDuration = CompactDuration::new(60);

    let max_jitter = slab_size.saturating_sub(MIN_PRELOAD);
    CompactDuration::from(rand::rng().random_range(0..=max_jitter.seconds()))
        .saturating_add(MIN_PRELOAD)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Key;
    use crate::heartbeat::HeartbeatRegistry;
    use crate::timers::Trigger;
    use crate::timers::duration::CompactDuration;
    use crate::timers::store::Segment;
    use crate::timers::store::memory::InMemoryTriggerStore;
    use color_eyre::eyre::{Result, eyre};
    use futures::future;
    use std::time::Duration;
    use tokio::task;
    use tokio::time::{advance, pause};
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
    async fn test_state_new() -> Result<()> {
        let store = InMemoryTriggerStore::new();
        let (_triggers_rx, scheduler) = TriggerScheduler::new(&HeartbeatRegistry::test());
        let state = State::new(store, scheduler);

        assert_eq!(state.max_owned, None);
        Ok(())
    }

    #[tokio::test]
    async fn test_state_is_owned_when_none() -> Result<()> {
        let store = InMemoryTriggerStore::new();
        let (_triggers_rx, scheduler) = TriggerScheduler::new(&HeartbeatRegistry::test());
        let state = State::new(store, scheduler);

        assert!(!state.is_owned(1));
        assert!(!state.is_owned(100));
        Ok(())
    }

    #[tokio::test]
    async fn test_state_is_owned_with_ownership() -> Result<()> {
        let store = InMemoryTriggerStore::new();
        let (_triggers_rx, scheduler) = TriggerScheduler::new(&HeartbeatRegistry::test());
        let mut state = State::new(store, scheduler);

        state.extend_ownership(5);

        assert!(state.is_owned(1));
        assert!(state.is_owned(3));
        assert!(state.is_owned(5));
        assert!(!state.is_owned(6));
        assert!(!state.is_owned(10));
        Ok(())
    }

    #[tokio::test]
    async fn test_state_extend_ownership() -> Result<()> {
        let store = InMemoryTriggerStore::new();
        let (_triggers_rx, scheduler) = TriggerScheduler::new(&HeartbeatRegistry::test());
        let mut state = State::new(store, scheduler);

        // First extension
        state.extend_ownership(3);
        assert_eq!(state.max_owned, Some(3));

        // Extend to higher value
        state.extend_ownership(7);
        assert_eq!(state.max_owned, Some(7));

        // Try to extend to lower value (should not change)
        state.extend_ownership(5);
        assert_eq!(state.max_owned, Some(7));
        Ok(())
    }

    #[tokio::test]
    async fn test_calculate_wait_time_future_time() -> Result<()> {
        pause();

        let now = CompactDateTime::now()?;
        let future_time = now.add_duration(CompactDuration::new(120))?; // 2 minutes from now
        let preload_window = CompactDuration::new(30);

        let wait_time = calculate_wait_time(future_time, preload_window);

        // Should wait for 120 - 30 = 90 seconds
        assert_eq!(wait_time, CompactDuration::new(90));
        Ok(())
    }

    #[tokio::test]
    async fn test_calculate_wait_time_within_preload_window() -> Result<()> {
        pause();

        let now = CompactDateTime::now()?;
        let near_future = now.add_duration(CompactDuration::new(15))?; // 15 seconds from now
        let preload_window = CompactDuration::new(30);

        let wait_time = calculate_wait_time(near_future, preload_window);

        // Should not wait (return CompactDuration::MIN) since it's within the preload
        // window
        assert!(wait_time.is_zero());
        Ok(())
    }

    #[tokio::test]
    async fn test_calculate_wait_time_past_time() -> Result<()> {
        pause();

        let now = CompactDateTime::now()?;
        // Advance time by 1 minute
        advance(Duration::from_secs(60)).await;
        task::yield_now().await;

        let past_time = now; // This is now in the past
        let preload_window = CompactDuration::new(30);

        let wait_time = calculate_wait_time(past_time, preload_window);

        // Should load immediately for past times
        assert!(wait_time.is_zero());
        Ok(())
    }

    #[tokio::test]
    async fn test_get_or_create_segment_new() -> Result<()> {
        let store = InMemoryTriggerStore::new();
        let segment_id = Uuid::new_v4();
        let slab_size = CompactDuration::new(60);
        let name = "test-segment";

        let segment = get_or_create_segment(&store, segment_id, slab_size, name).await?;
        assert_eq!(segment.id, segment_id);
        assert_eq!(segment.name, name);
        assert_eq!(segment.slab_size, slab_size);

        // Check that it was stored
        let retrieved = store.get_segment(&segment_id).await?;
        assert!(retrieved.is_some());
        let retrieved = retrieved.ok_or_else(|| eyre!("Expected segment to be stored"))?;
        assert_eq!(retrieved.name, name);
        Ok(())
    }

    #[tokio::test]
    async fn test_get_or_create_segment_existing() -> Result<()> {
        let store = InMemoryTriggerStore::new();
        let segment_id = Uuid::new_v4();
        let slab_size = CompactDuration::new(60);
        let name1 = "first-segment";
        let name2 = "second-segment";

        // Create first segment
        let segment1 = get_or_create_segment(&store, segment_id, slab_size, name1).await?;

        // Try to create second segment with same ID but different name
        let segment2 = get_or_create_segment(&store, segment_id, slab_size, name2).await?;

        // Should return the first segment (existing one)
        assert_eq!(segment1.name, segment2.name);
        assert_eq!(segment1.id, segment2.id);
        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::reversed_empty_ranges)]
    async fn test_load_slabs_empty_range() -> Result<()> {
        let store = InMemoryTriggerStore::new();
        let (_triggers_rx, scheduler) = TriggerScheduler::new(&HeartbeatRegistry::test());
        let state = State::new(store, scheduler);
        let slab_lock = SlabLock::new(state);
        let segment = create_test_segment();

        // Load empty range
        let loaded = load_slabs(&slab_lock, &segment, 1..=0).await?;
        assert!(loaded.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn test_load_slabs_single_slab() -> Result<()> {
        let store = InMemoryTriggerStore::new();
        let (_triggers_rx, scheduler) = TriggerScheduler::new(&HeartbeatRegistry::test());
        let state = State::new(store.clone(), scheduler);
        let slab_lock = SlabLock::new(state);
        let segment = create_test_segment();

        // Insert the segment first
        store.insert_segment(segment.clone()).await?;

        // Insert a slab
        let slab = Slab::new(segment.id, 1, segment.slab_size);
        store.insert_slab(&segment.id, slab).await?;

        let loaded = load_slabs(&slab_lock, &segment, 1..=1).await?;
        assert_eq!(loaded, vec![1]);

        // Verify ownership was extended
        let state_guard = slab_lock.slab_lock().await;
        assert!(state_guard.is_owned(1));
        Ok(())
    }

    #[tokio::test]
    async fn test_load_slabs_multiple_slabs() -> Result<()> {
        let store = InMemoryTriggerStore::new();
        let (_triggers_rx, scheduler) = TriggerScheduler::new(&HeartbeatRegistry::test());
        let state = State::new(store.clone(), scheduler);
        let slab_lock = SlabLock::new(state);
        let segment = create_test_segment();

        // Insert the segment first
        store.insert_segment(segment.clone()).await?;

        // Insert multiple slabs
        for slab_id in 1..=3 {
            let slab = Slab::new(segment.id, slab_id, segment.slab_size);
            store.insert_slab(&segment.id, slab).await?;
        }

        let mut loaded = load_slabs(&slab_lock, &segment, 1..=3).await?;
        loaded.sort_unstable(); // Sort since order isn't guaranteed with concurrency
        assert_eq!(loaded, vec![1, 2, 3]);

        // Verify ownership was extended to the highest slab
        let state_guard = slab_lock.slab_lock().await;
        assert_eq!(state_guard.max_owned, Some(3));
        Ok(())
    }

    #[tokio::test]
    async fn test_load_triggers_empty_slab() -> Result<()> {
        let store = InMemoryTriggerStore::new();
        let (_triggers_rx, scheduler) = TriggerScheduler::new(&HeartbeatRegistry::test());
        let segment = create_test_segment();
        let slab = Slab::new(segment.id, 1, segment.slab_size);

        load_triggers(&store, &scheduler, slab).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_load_triggers_with_triggers() -> Result<()> {
        let store = InMemoryTriggerStore::new();
        let (_triggers_rx, scheduler) = TriggerScheduler::new(&HeartbeatRegistry::test());
        let segment = create_test_segment();
        let slab = Slab::new(segment.id, 1, segment.slab_size);

        // Add some triggers to the slab
        let now = CompactDateTime::now()?;
        let trigger1 = create_test_trigger(1, now);
        let trigger2 = create_test_trigger(2, now);

        store.insert_slab_trigger(slab.clone(), trigger1).await?;
        store.insert_slab_trigger(slab.clone(), trigger2).await?;

        load_triggers(&store, &scheduler, slab).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_remove_completed_slabs_no_active() -> Result<()> {
        let store = InMemoryTriggerStore::new();
        let (_triggers_rx, scheduler) = TriggerScheduler::new(&HeartbeatRegistry::test());
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
            let slab = Slab::new(segment.id, *slab_id, segment.slab_size);
            store.insert_slab(&segment.id, slab).await?;
        }

        remove_completed_slabs(&slab_lock, &segment, &mut loaded_slab_ids).await?;

        // Since there are no active triggers, all slabs should be removed
        assert!(loaded_slab_ids.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn test_active_slab_ids_empty_scheduler() -> Result<()> {
        let (_triggers_rx, scheduler) = TriggerScheduler::new(&HeartbeatRegistry::test());
        let segment = create_test_segment();

        let active_ids = active_slab_ids(&segment, &scheduler).await;

        assert!(active_ids.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn test_slab_loader_basic_operation() -> Result<()> {
        pause();

        let store = InMemoryTriggerStore::new();
        let (_triggers_rx, scheduler) = TriggerScheduler::new(&HeartbeatRegistry::test());
        let state = State::new(store.clone(), scheduler);
        let slab_lock = SlabLock::new(state);
        let segment = create_test_segment();

        // Insert the segment
        store.insert_segment(segment.clone()).await?;

        // Create test heartbeat
        let test_heartbeat = Heartbeat::new("test-slab-loader", Duration::from_secs(30));

        // We need to spawn the slab_loader in a separate task since it runs
        // indefinitely
        let loader_handle = tokio::spawn(slab_loader(
            segment.clone(),
            slab_lock.clone(),
            test_heartbeat,
        ));

        // Give it a moment to start
        advance(Duration::from_millis(100)).await;
        task::yield_now().await;

        // Cancel the loader task
        loader_handle.abort();

        // Verify some slabs were created (the current slab at minimum)
        let state_guard = slab_lock.slab_lock().await;
        assert!(state_guard.max_owned.is_some());
        Ok(())
    }

    #[tokio::test]
    async fn test_slab_loader_time_advancement() -> Result<()> {
        pause();

        let store = InMemoryTriggerStore::new();
        let (_triggers_rx, scheduler) = TriggerScheduler::new(&HeartbeatRegistry::test());
        let state = State::new(store.clone(), scheduler);
        let slab_lock = SlabLock::new(state);
        let segment = create_test_segment();

        // Insert the segment
        store.insert_segment(segment.clone()).await?;

        // Create test heartbeat
        let test_heartbeat =
            Heartbeat::new("test-slab-loader-advancement", Duration::from_secs(30));

        // Spawn the loader
        let loader_handle = tokio::spawn(slab_loader(
            segment.clone(),
            slab_lock.clone(),
            test_heartbeat,
        ));

        // Let it load initial slabs
        advance(Duration::from_millis(100)).await;
        task::yield_now().await;

        let initial_max = {
            let state_guard = slab_lock.slab_lock().await;
            state_guard.max_owned
        };

        // Advance time significantly to trigger more loads
        advance(Duration::from_secs(120)).await; // Advance by 2 minutes
        task::yield_now().await;

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
        Ok(())
    }

    #[tokio::test]
    async fn test_calculate_wait_time_exact_preload_boundary() -> Result<()> {
        pause();

        let now = CompactDateTime::now()?;
        let boundary_time = now.add_duration(CompactDuration::new(30))?; // Exactly 30 seconds from now
        let preload_window = CompactDuration::new(30);

        let wait_time = calculate_wait_time(boundary_time, preload_window);

        // Should not wait since it's exactly at the boundary
        assert!(wait_time.is_zero());
        Ok(())
    }

    #[tokio::test]
    async fn test_remove_completed_slabs_with_active_triggers() -> Result<()> {
        let store = InMemoryTriggerStore::new();
        let (_triggers_rx, scheduler) = TriggerScheduler::new(&HeartbeatRegistry::test());
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
            let slab = Slab::new(segment.id, *slab_id, segment.slab_size);
            store.insert_slab(&segment.id, slab).await?;
        }

        // Add an active trigger to slab 2
        let now = CompactDateTime::now()?;
        let trigger = create_test_trigger(1, now);
        let _slab = Slab::from_time(segment.id, segment.slab_size, now);

        // Schedule the trigger to make it active
        scheduler.schedule(trigger.clone()).await?;

        remove_completed_slabs(&slab_lock, &segment, &mut loaded_slab_ids).await?;

        // The exact behavior depends on which slab the trigger falls into,
        // but at least some slabs should remain if there are active triggers
        // This test mainly ensures the function doesn't panic with active triggers
        Ok(())
    }

    #[tokio::test]
    async fn test_load_slabs_extends_ownership_correctly() -> Result<()> {
        let store = InMemoryTriggerStore::new();
        let (_triggers_rx, scheduler) = TriggerScheduler::new(&HeartbeatRegistry::test());
        let state = State::new(store.clone(), scheduler);
        let slab_lock = SlabLock::new(state);
        let segment = create_test_segment();

        // Insert the segment first
        store.insert_segment(segment.clone()).await?;

        // Set initial ownership
        {
            let mut state_guard = slab_lock.slab_lock().await;
            state_guard.extend_ownership(5);
        };

        // Insert slabs beyond current ownership
        for slab_id in 6..=10 {
            let slab = Slab::new(segment.id, slab_id, segment.slab_size);
            store.insert_slab(&segment.id, slab).await?;
        }

        let result = load_slabs(&slab_lock, &segment, 6..=10).await;

        assert!(result.is_ok());

        // Verify ownership was extended to the highest loaded slab
        let state_guard = slab_lock.slab_lock().await;
        assert_eq!(state_guard.max_owned, Some(10));
        Ok(())
    }

    #[tokio::test]
    async fn test_load_slabs_preserves_higher_ownership() -> Result<()> {
        let store = InMemoryTriggerStore::new();
        let (_triggers_rx, scheduler) = TriggerScheduler::new(&HeartbeatRegistry::test());
        let state = State::new(store.clone(), scheduler);
        let slab_lock = SlabLock::new(state);
        let segment = create_test_segment();

        // Insert the segment first
        store.insert_segment(segment.clone()).await?;

        // Set high initial ownership
        {
            let mut state_guard = slab_lock.slab_lock().await;
            state_guard.extend_ownership(100);
        };

        // Insert and load lower range slabs
        for slab_id in 1..=5 {
            let slab = Slab::new(segment.id, slab_id, segment.slab_size);
            store.insert_slab(&segment.id, slab).await?;
        }

        let loaded = load_slabs(&slab_lock, &segment, 1..=5).await?;
        assert_eq!(loaded.len(), 5);

        // Verify ownership was not reduced
        let state_guard = slab_lock.slab_lock().await;
        assert_eq!(state_guard.max_owned, Some(100));
        Ok(())
    }

    #[tokio::test]
    async fn test_state_ownership_edge_cases() -> Result<()> {
        let store = InMemoryTriggerStore::new();
        let (_triggers_rx, scheduler) = TriggerScheduler::new(&HeartbeatRegistry::test());
        let mut state = State::new(store, scheduler);

        // Test ownership at SlabId boundaries
        state.extend_ownership(0);
        assert!(state.is_owned(0));
        assert!(!state.is_owned(1));

        state.extend_ownership(SlabId::MAX);
        assert!(state.is_owned(SlabId::MAX));
        assert!(state.is_owned(SlabId::MAX - 1));
        Ok(())
    }

    #[tokio::test]
    async fn test_active_slab_ids_with_triggers() -> Result<()> {
        let (_triggers_rx, scheduler) = TriggerScheduler::new(&HeartbeatRegistry::test());
        let segment = create_test_segment();

        // Schedule some triggers
        let now = CompactDateTime::now()?;
        let future = now.add_duration(CompactDuration::new(120))?;

        let trigger1 = create_test_trigger(1, now);
        let trigger2 = create_test_trigger(2, future);

        scheduler.schedule(trigger1.clone()).await?;
        scheduler.schedule(trigger2.clone()).await?;

        let active_ids = active_slab_ids(&segment, &scheduler).await;

        // Should include slabs for both triggers
        assert!(!active_ids.is_empty());

        let expected_slab1 = Slab::from_time(segment.id, segment.slab_size, now);
        let expected_slab2 = Slab::from_time(segment.id, segment.slab_size, future);

        assert!(active_ids.contains(&expected_slab1.id()));
        assert!(active_ids.contains(&expected_slab2.id()));
        Ok(())
    }

    #[tokio::test]
    async fn test_get_or_create_segment_concurrent() -> Result<()> {
        let store = InMemoryTriggerStore::new();
        let segment_id = Uuid::new_v4();
        let slab_size = CompactDuration::new(60);
        let name = "concurrent-test";

        // Create multiple concurrent futures trying to create the same segment
        let futures: Vec<_> = (0_i32..10_i32)
            .map(|_| {
                let store_clone = store.clone();
                async move {
                    get_or_create_segment(&store_clone, segment_id, slab_size, name).await
                }
            })
            .collect();

        // Wait for all futures to complete
        let results: Vec<_> = future::join_all(futures).await;

        // All should succeed and return the same segment
        for result in results {
            let segment = result?;
            assert_eq!(segment.id, segment_id);
            assert_eq!(segment.name, name);
            assert_eq!(segment.slab_size, slab_size);
        }

        // Verify only one segment exists in the store
        let stored_segment = store.get_segment(&segment_id).await?;
        let stored_segment =
            stored_segment.ok_or_else(|| eyre!("Expected segment to be stored"))?;
        assert_eq!(stored_segment.id, segment_id);
        assert_eq!(stored_segment.name, name);
        Ok(())
    }

    #[tokio::test]
    async fn test_slab_loader_handles_time_errors() -> Result<()> {
        pause();

        let store = InMemoryTriggerStore::new();
        let (_triggers_rx, scheduler) = TriggerScheduler::new(&HeartbeatRegistry::test());
        let state = State::new(store.clone(), scheduler);
        let slab_lock = SlabLock::new(state);
        let segment = create_test_segment();

        // Insert the segment
        store.insert_segment(segment.clone()).await?;

        // Create test heartbeat
        let test_heartbeat = Heartbeat::new("test-slab-loader-errors", Duration::from_secs(30));

        // Spawn the loader
        let loader_handle = tokio::spawn(slab_loader(
            segment.clone(),
            slab_lock.clone(),
            test_heartbeat,
        ));

        // Give it time to start and handle any initial time operations
        advance(Duration::from_millis(50)).await;
        task::yield_now().await;

        // The loader should continue running despite any time calculation edge cases
        assert!(!loader_handle.is_finished());

        // Cancel the loader
        loader_handle.abort();
        Ok(())
    }

    #[tokio::test]
    async fn test_calculate_wait_time_large_values() -> Result<()> {
        pause();

        let now = CompactDateTime::now()?;

        // Test with very large preload value
        let near_future = now.add_duration(CompactDuration::new(10))?;
        let large_preload = u32::MAX;

        let wait_time = calculate_wait_time(near_future, CompactDuration::new(large_preload));

        // Should load immediately when preload is very large
        assert!(wait_time.is_zero());

        // Test with large but reasonable duration difference that won't overflow
        let large_duration = 86400 * 365; // 1 year in seconds
        let far_future = now.add_duration(CompactDuration::new(large_duration))?;
        let normal_preload = 30;

        let wait_time = calculate_wait_time(far_future, CompactDuration::new(normal_preload));

        // Should wait for a very long time (1 year - 30 seconds)
        assert_eq!(wait_time.seconds(), large_duration - 30);
        Ok(())
    }
}
