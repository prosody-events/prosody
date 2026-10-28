//! Tests for the scheduler actor.

mod invariants;
mod seed;
pub(crate) mod support;

use super::actor::load::{
    MIN_PRELOAD, calculate_preload, calculate_wait_time, drain_slab_range, load_step,
    next_unloaded_slab_id,
};
use super::actor::{ActorState, handle_add, handle_hold, owns_slab};
use crate::Key;
use crate::state::TimerEventRef;
use crate::timers::active::{Queued, Row, TimerState};
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::timers::queue::TriggerQueue;
use crate::timers::slab::{Slab, SlabId};
use crate::timers::store::adapter::TableAdapter;
use crate::timers::store::memory::{InMemoryTriggerStore, memory_store};
use crate::timers::store::{Segment, StoredSegment, TriggerStore};
use crate::timers::test_support::test_segment;
use crate::timers::{TimerType, Trigger};
use color_eyre::eyre::{OptionExt, Result, ensure, eyre};
use quickcheck::QuickCheck;
use std::time::Duration as StdDuration;
use strum::VariantArray;
use tokio::runtime::Builder as RuntimeBuilder;
use tokio::time::advance;
use tracing::Span;

/// Tests use a 300s slab and a deterministic preload window so slab
/// math is predictable across runs.
const SLAB_SIZE_SECS: u32 = 300;
const PRELOAD_SECS: u32 = 120;

type TestStore = TableAdapter<InMemoryTriggerStore>;

/// Starts the actor state as `run_actor` does, with a fixed preload window.
fn start_state(
    store: TestStore,
    segment: StoredSegment,
    now: CompactDateTime,
) -> ActorState<TestStore> {
    let mut state = ActorState::start(store, segment, now);
    state.preload_window = CompactDuration::new(PRELOAD_SECS);
    state
}

/// Starts the actor state for a new segment, which has no watermark.
fn fresh_state(store: TestStore, segment: Segment) -> ActorState<TestStore> {
    start_state(store, segment.into(), CompactDateTime::MIN)
}

/// Schedules `trigger` as the manager does: hold, write the row, then add.
async fn schedule(
    state: &mut ActorState<TestStore>,
    queue: &mut TriggerQueue,
    trigger: Trigger,
    queued: Queued,
) -> Result<()> {
    handle_hold(state, queue, trigger.clone()).await?;
    state.store.add_trigger(trigger.clone()).await?;
    handle_add(state, queue, trigger, queued, Row::Written).await;
    Ok(())
}

/// Reads the persisted slab watermark through the segment read.
async fn persisted_watermark(store: &TestStore) -> Option<SlabId> {
    let Ok(segment) = store.get_segment().await;
    segment.and_then(|segment| segment.into_parts().1)
}

// ===================================================================
// Pure helper tests
// ===================================================================

#[tokio::test(start_paused = true)]
async fn test_calculate_wait_time() -> Result<()> {
    let now = CompactDateTime::now()?;
    // (offset_secs, preload_secs, expected_wait_secs)
    for (offset, preload, expected) in [
        (120, 30, 90), // future: 120s out minus 30s preload
        (15, 30, 0),   // within preload window: saturates to zero
        (30, 30, 0),   // exact preload boundary: saturates to zero
    ] {
        let target = now.add_duration(CompactDuration::new(offset))?;
        assert_eq!(
            calculate_wait_time(target, CompactDuration::new(preload)),
            CompactDuration::new(expected)
        );
    }

    // Past time: advance the clock past a previously-computed load time.
    let load_time = CompactDateTime::now()?;
    advance(StdDuration::from_mins(1)).await;
    assert!(calculate_wait_time(load_time, CompactDuration::new(30)).is_zero());
    Ok(())
}

#[test]
fn test_calculate_preload_within_bounds() {
    let slab_size = CompactDuration::new(SLAB_SIZE_SECS);
    for _ in 0_i32..32_i32 {
        let window = calculate_preload(slab_size);
        assert!(
            window >= MIN_PRELOAD && window <= slab_size,
            "preload {window} outside [{MIN_PRELOAD}, {slab_size}]"
        );
    }
}

#[test]
fn test_next_unloaded_slab_id_from_fresh_state() {
    let segment = test_segment("scheduler-test", SLAB_SIZE_SECS);
    let store = memory_store(segment.clone());
    let mut state = fresh_state(store, segment);
    // No watermark, no high-water: scan starts at 0.
    assert_eq!(next_unloaded_slab_id(&state), Some(0));

    state.last_persisted_watermark = Some(7);
    assert_eq!(next_unloaded_slab_id(&state), Some(8));

    state.highest_loaded_slab_id = Some(12);
    assert_eq!(next_unloaded_slab_id(&state), Some(13));

    state.highest_loaded_slab_id = Some(SlabId::MAX);
    // Slab-id space exhausted.
    assert_eq!(next_unloaded_slab_id(&state), None);
}

/// Repeated retirement preserves one replacement in both indexes and the queue.
#[test]
fn prop_retirement_idempotence() {
    fn property(mode: u8, tag: i16) -> Result<()> {
        RuntimeBuilder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()?
            .block_on(support::retirement_trace(
                mode,
                i32::from(tag),
                false,
                |manager, trigger| async move {
                    let event = TimerEventRef::new(trigger.timer_type, trigger.time, trigger.tag);
                    manager.retire_committed(&trigger.key, event).await?;
                    Ok(())
                },
            ))
    }
    QuickCheck::new().quickcheck(property as fn(u8, i16) -> Result<()>);
}

/// A partial load cannot preserve an old tag after the durable schedule
/// changes, and a reschedule during delivery stays in memory.
#[test]
fn prop_failed_load_reschedule() {
    fn property(tag: i16, kind: u8, delivered: bool, firing: bool) -> Result<()> {
        RuntimeBuilder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()?
            .block_on(async {
                let segment = test_segment("failed-load", SLAB_SIZE_SECS);
                let store = memory_store(segment.clone());
                store.insert_segment().await?;
                let time = CompactDateTime::from(
                    CompactDateTime::now()?.epoch_seconds().saturating_sub(600),
                );
                let slab = Slab::from_time(segment.slab_size, time);
                let timer_type = TimerType::VARIANTS[usize::from(kind) % TimerType::VARIANTS.len()];
                let old = Trigger::with_tag(
                    Key::from("replacement"),
                    time,
                    timer_type,
                    i32::from(tag),
                    Span::current(),
                );
                let mut state = fresh_state(store.clone(), segment);
                let mut queue = TriggerQueue::new();
                schedule(&mut state, &mut queue, old.clone(), Queued::Scheduled).await?;

                // Seed the successful drain before a later slab read fails.
                // The failed tick does not advance ownership.
                drain_slab_range(
                    &store,
                    slab.id()..=slab.id(),
                    &mut queue,
                    &mut state.known_slab_ids,
                )
                .await?;
                ensure!(!owns_slab(&state, slab.id()));
                let mut queued = Queued::Scheduled;
                if delivered {
                    let delivery = queue.next().await.ok_or_eyre("old timer not queued")?;
                    if firing {
                        ensure!(queue.active_triggers().fire(&delivery).await);
                        queued = Queued::FiringRescheduled;
                    }
                }
                let mut replacement = old;
                replacement.tag += 1_i32;
                schedule(&mut state, &mut queue, replacement.clone(), queued).await?;
                let active = queue.active_triggers();
                ensure!(
                    active
                        .get(&replacement.key, time, timer_type)
                        .await
                        .map(|entry| entry.state)
                        == Some(queued.into()),
                    "the reschedule left memory"
                );
                // The delivery commits, so the re-fire is due.
                active
                    .set_state(&replacement.key, time, timer_type, TimerState::Scheduled)
                    .await;

                load_step(&mut state, &mut queue).await;
                let expired = queue
                    .next()
                    .await
                    .ok_or_else(|| eyre!("replacement disappeared from the queue"))?;
                ensure!(expired.tag == replacement.tag);
                ensure!(
                    queue.active_triggers().fire(&expired).await,
                    "stale registry tag rejected the replacement"
                );
                ensure!(queue.next().await.is_none());
                Ok(())
            })
    }
    QuickCheck::new().quickcheck(property as fn(i16, u8, bool, bool) -> Result<()>);
}
