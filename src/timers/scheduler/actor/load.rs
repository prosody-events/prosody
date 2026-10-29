//! Slab loading and cleanup.
//!
//! [`load_step`] reads registered slabs into the trigger queue as they enter
//! the preload window. Each successful load then runs [`cleanup_step`], which
//! deletes completed slabs and advances the watermark.

use super::ActorState;
use crate::timers::DELETE_CONCURRENCY;
use crate::timers::active::Timers;
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::timers::slab::{Slab, SlabId};
use crate::timers::store::TriggerStore;
use futures::{StreamExt, TryStreamExt, stream};
use rand::RngExt;
use std::collections::BTreeSet;
use std::ops::RangeInclusive;
use std::pin::pin;
use std::time::Duration;
use tokio::task::coop::cooperative;
use tokio::time::Instant;
use tracing::{debug, warn};

/// Minimum jittered preload window: never start loading less than this much
/// time before a slab begins.
pub(crate) const MIN_PRELOAD: CompactDuration = CompactDuration::new(60);

/// Backoff used when no further load work can currently be scheduled.
const LOAD_IDLE_INTERVAL: Duration = Duration::from_secs(30);

/// Maximum concurrent per-slab trigger scans during a load tick.
const LOAD_CONCURRENCY: usize = 16;

/// Backoff after a Cassandra-side failure inside the actor's load/cleanup
/// arms — these errors are typically transient.
const RETRY_DELAY: Duration = Duration::from_secs(1);
/// Deletes completed persisted slabs below the current slab and then advances
/// the watermark.
///
/// Cleanup is load-driven: a successful `load_step` calls this after moving
/// the load high-water. If a delete fails, in-memory bookkeeping is left
/// untouched and a later load will retry.
pub(crate) async fn cleanup_step<T>(state: &mut ActorState<T>, timers: &Timers)
where
    T: TriggerStore,
{
    let now_slab_id = match CompactDateTime::now() {
        Ok(now) => Slab::from_time(state.segment.slab_size, now).id(),
        Err(e) => {
            warn!("cleanup_step: failed to compute now: {e:#}");
            return;
        }
    };

    let active_slab_ids = timers.slabs(state.segment.slab_size);
    let completed_slabs = completed_slab_ids(state, now_slab_id, &active_slab_ids);

    match delete_completed_slabs(&state.store, &completed_slabs).await {
        Ok(()) => {
            for slab_id in completed_slabs {
                state.known_slab_ids.remove(&slab_id);
            }
        }
        Err(e) => {
            warn!("cleanup_step: failed to delete slab: {e:#}; will retry");
            return;
        }
    }

    if let Err(e) = maybe_advance_watermark(state, now_slab_id, &active_slab_ids).await {
        warn!("cleanup_step: failed to advance watermark: {e:#}");
    }
}

fn cleanable_slab_end<T>(state: &ActorState<T>, now_slab_id: SlabId) -> Option<SlabId> {
    Some(
        now_slab_id
            .checked_sub(1)?
            .min(state.highest_loaded_slab_id?),
    )
}

fn completed_slab_ids<T>(
    state: &ActorState<T>,
    now_slab_id: SlabId,
    active_slab_ids: &BTreeSet<SlabId>,
) -> Vec<SlabId> {
    let Some(end) = cleanable_slab_end(state, now_slab_id) else {
        return Vec::new();
    };

    state
        .known_slab_ids
        .range(..=end)
        .filter(|slab_id| !active_slab_ids.contains(slab_id))
        .copied()
        .collect()
}

async fn delete_completed_slabs<T>(store: &T, slab_ids: &[SlabId]) -> Result<(), T::Error>
where
    T: TriggerStore,
{
    stream::iter(slab_ids.iter().copied())
        .map(|slab_id| async move {
            store.delete_slab(slab_id).await?;
            Ok(())
        })
        .buffer_unordered(DELETE_CONCURRENCY)
        .try_collect::<()>()
        .await
}

/// Raises `slab_watermark` using only actor-owned state.
///
/// After [`cleanup_step`] has found no more deletable slabs, every persisted
/// slab in the cleanable owned prefix is active. The actor can therefore
/// advance to one below the lowest active slab in that prefix, or to the end
/// of the prefix when it is empty, without asking storage to rediscover the
/// partition's low end.
async fn maybe_advance_watermark<T>(
    state: &mut ActorState<T>,
    now_slab_id: SlabId,
    active_slab_ids: &BTreeSet<SlabId>,
) -> Result<(), T::Error>
where
    T: TriggerStore,
{
    let Some(end) = cleanable_slab_end(state, now_slab_id) else {
        return Ok(());
    };

    let lowest_active = active_slab_ids.range(..=end).next().copied();

    let Some(candidate) = lowest_active.map_or(Some(end), |slab_id| slab_id.checked_sub(1)) else {
        return Ok(());
    };

    if state
        .last_persisted_watermark
        .is_some_and(|current| candidate <= current)
    {
        return Ok(());
    }

    state.store.set_slab_watermark(Some(candidate)).await?;
    state.last_persisted_watermark = Some(candidate);
    debug!(watermark = candidate, "advanced slab_watermark");
    Ok(())
}

/// Load tick: read slab metadata from the current load high-water up to the
/// slab containing `now + preload_window`, drain each registered slab's
/// triggers, then push the high-water forward.
///
/// This is the actor's only slab-table read path after startup. Cleanup and
/// watermark advancement consume the slab IDs learned here rather than
/// re-scanning metadata.
pub(crate) async fn load_step<T>(state: &mut ActorState<T>, timers: &mut Timers)
where
    T: TriggerStore,
{
    let now = match CompactDateTime::now() {
        Ok(t) => t,
        Err(e) => {
            warn!("load_step: failed to compute now: {e:#}");
            state.next_load_at = Instant::now() + RETRY_DELAY;
            return;
        }
    };
    let target_time = match now.add_duration(state.preload_window) {
        Ok(t) => t,
        Err(e) => {
            warn!("load_step: failed to compute target_time: {e:#}");
            state.next_load_at = Instant::now() + RETRY_DELAY;
            return;
        }
    };
    let target_slab = Slab::from_time(state.segment.slab_size, target_time);
    let target_slab_id = target_slab.id();

    let Some(start_slab_id) = next_unloaded_slab_id(state) else {
        // SlabId space exhausted (`u32::MAX` reached). Nothing more to load.
        state.next_load_at = Instant::now() + LOAD_IDLE_INTERVAL;
        return;
    };

    if start_slab_id > target_slab_id {
        // High-water already past the preload horizon. Schedule the next
        // tick for when the following slab enters the window.
        schedule_next_load(state, &target_slab);
        return;
    }

    let load_result = drain_slab_range(
        &state.store,
        start_slab_id..=target_slab_id,
        timers,
        &mut state.known_slab_ids,
    )
    .await;

    match load_result {
        Ok(count) => {
            state.highest_loaded_slab_id = Some(target_slab_id);
            debug!(
                start_slab_id,
                target_slab_id,
                triggers = count,
                "load_step: loaded range"
            );
            schedule_next_load(state, &target_slab);
            cleanup_step(state, timers).await;
        }
        Err(e) => {
            warn!(
                start_slab_id,
                target_slab_id, "load_step: stream error: {e:#}; will retry"
            );
            state.next_load_at = Instant::now() + RETRY_DELAY;
        }
    }
}

/// Drains registered slabs and their triggers into the trigger queue.
///
/// The slab metadata scan happens here, in the load loop. Empty slab rows are
/// still recorded in `known_slab_ids` so cleanup can delete them later without
/// reading the slab table again.
pub(crate) async fn drain_slab_range<T>(
    store: &T,
    range: RangeInclusive<SlabId>,
    timers: &mut Timers,
    known_slab_ids: &mut BTreeSet<SlabId>,
) -> Result<usize, T::Error>
where
    T: TriggerStore,
{
    let store_for_tasks = store.clone();
    let mut stream = pin!(
        store
            .get_slab_range(range)
            .map_ok(move |slab_id| {
                let store = store_for_tasks.clone();
                async move {
                    let triggers = pin!(store.get_slab_triggers_all_types(slab_id));
                    let slab_triggers = cooperative(triggers.try_collect::<Vec<_>>()).await?;
                    Ok::<_, T::Error>((slab_id, slab_triggers))
                }
            })
            .try_buffer_unordered(LOAD_CONCURRENCY)
    );

    let mut count = 0_usize;
    while let Some((slab_id, slab_triggers)) = cooperative(stream.try_next()).await? {
        known_slab_ids.insert(slab_id);
        for trigger in slab_triggers {
            timers.load(trigger);
            count = count.saturating_add(1);
        }
    }
    Ok(count)
}

/// Returns the next slab id the load loop should scan from, or `None` when
/// the slab-id space is exhausted.
pub(crate) fn next_unloaded_slab_id<T>(state: &ActorState<T>) -> Option<SlabId> {
    match state.highest_loaded_slab_id {
        Some(h) => h.checked_add(1),
        None => Some(
            state
                .last_persisted_watermark
                .map_or(0, |w| w.saturating_add(1)),
        ),
    }
}

/// Sets `next_load_at` to when the slab after `target_slab` should be
/// preloaded, re-rolling the jittered window for the next cycle.
fn schedule_next_load<T>(state: &mut ActorState<T>, target_slab: &Slab) {
    let Some(next_slab) = target_slab.next() else {
        state.next_load_at = Instant::now() + LOAD_IDLE_INTERVAL;
        return;
    };
    let wait = calculate_wait_time(next_slab.range().start, state.preload_window);
    state.next_load_at = Instant::now() + wait.into();
    state.preload_window = calculate_preload(state.segment.slab_size);
}

/// Computes the wait time until a slab beginning at `load_time` should be
/// preloaded. Returns zero for past or imminent slabs.
pub(crate) fn calculate_wait_time(
    load_time: CompactDateTime,
    preload_window: CompactDuration,
) -> CompactDuration {
    load_time
        .compact_duration_from_now()
        .unwrap_or(CompactDuration::MIN)
        .saturating_sub(preload_window)
}

/// Generates a jittered preload window between [`MIN_PRELOAD`, `slab_size`].
pub(crate) fn calculate_preload(slab_size: CompactDuration) -> CompactDuration {
    let max_jitter = slab_size.saturating_sub(MIN_PRELOAD);
    CompactDuration::from(rand::rng().random_range(0..=max_jitter.seconds()))
        .saturating_add(MIN_PRELOAD)
}
