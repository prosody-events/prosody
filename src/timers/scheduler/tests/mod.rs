//! Tests for the scheduler actor.

mod concurrent;
mod invariants;
mod seed;
pub(crate) mod support;

use super::actor::ActorState;
use super::actor::load::{
    MIN_PRELOAD, calculate_preload, calculate_wait_time, drain_slab_range, load_step,
    next_unloaded_slab_id,
};
use crate::Key;
use crate::state::TimerEventRef;
use crate::timers::active::Insert;
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::timers::slab::{Slab, SlabId};
use crate::timers::store::memory::memory_store;
use crate::timers::store::operations::TriggerOperations;
use crate::timers::store::{Segment, StoredSegment, TriggerStore};
use crate::timers::test_support::test_segment;
use crate::timers::{TimerType, Trigger};
use color_eyre::eyre::{OptionExt, Result, ensure};
use quickcheck::QuickCheck;
use std::time::Duration as StdDuration;
use strum::VariantArray;
use support::{Actor, TestStore};
use tokio::runtime::Builder as RuntimeBuilder;
use tokio::time::advance;
use tracing::Span;

/// Tests use a 300s slab and a deterministic preload window so slab
/// math is predictable across runs.
const SLAB_SIZE_SECS: u32 = 300;
const PRELOAD_SECS: u32 = 120;

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

/// Repeated retirement keeps one replacement in both indexes and in memory.
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

/// How far the delivery of a loaded attempt got before a new schedule.
#[derive(Clone, Copy, Debug, VariantArray)]
enum Delivery {
    None,
    /// The attempt left the queue and waits to fire.
    Popped,
    Firing,
}

impl quickcheck::Arbitrary for Delivery {
    fn arbitrary(g: &mut quickcheck::Gen) -> Self {
        Self::VARIANTS[usize::from(u8::arbitrary(g)) % Self::VARIANTS.len()]
    }
}

/// A partial load leaves its slab unowned, but memory keeps what it holds.
/// After an insert, exactly one attempt fires: the one in the key row.
#[test]
fn prop_partial_load_keeps_held_timers() {
    fn property(tag: i16, kind: u8, delivery: Delivery, repair: bool) -> Result<()> {
        RuntimeBuilder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()?
            .block_on(partial_load(i32::from(tag), kind, delivery, repair))
    }
    QuickCheck::new().quickcheck(property as fn(i16, u8, Delivery, bool) -> Result<()>);
}

async fn partial_load(tag: i32, kind: u8, delivery: Delivery, repair: bool) -> Result<()> {
    let segment = test_segment("partial-load", SLAB_SIZE_SECS);
    let store = memory_store(segment.clone());
    store.insert_segment().await?;
    let time = CompactDateTime::from(CompactDateTime::now()?.epoch_seconds().saturating_sub(600));
    let slab = Slab::from_time(segment.slab_size, time);
    let timer_type = TimerType::VARIANTS[usize::from(kind) % TimerType::VARIANTS.len()];
    let old = Trigger::with_tag(Key::from("timer"), time, timer_type, tag, Span::current());
    let mut actor = Actor::new(fresh_state(store.clone(), segment));
    let scheduler = actor.scheduler();

    // The slab is not owned, so the schedule leaves the row to a load.
    let (old, ticket) = actor.serve(scheduler.insert(old, Insert::Schedule)).await?;
    ticket.run(store.add_trigger(old.clone())).await?;
    actor.drain().await;

    // A drain of one slab reads the row, and the slab stays unowned.
    let Actor { state, timers, .. } = &mut actor;
    drain_slab_range(
        &store,
        slab.id()..=slab.id(),
        timers,
        &mut state.known_slab_ids,
    )
    .await?;
    ensure!(
        actor
            .state
            .highest_loaded_slab_id
            .is_none_or(|end| end < slab.id())
    );
    let mut popped = None;
    if !matches!(delivery, Delivery::None) {
        let trigger = actor
            .timers
            .next()
            .await
            .ok_or_eyre("the old attempt is not queued")?;
        if matches!(delivery, Delivery::Firing) {
            ensure!(
                actor.timers.fire(&trigger.id(), trigger.tag),
                "the old attempt did not fire"
            );
        } else {
            popped = Some(trigger);
        }
    }

    // A repair queues the key row's attempt; a schedule writes both rows.
    let mut new = old.clone();
    new.tag += 1_i32;
    let insert = if repair {
        Insert::Repair
    } else {
        Insert::Schedule
    };
    let (new, ticket) = actor.serve(scheduler.insert(new, insert)).await?;
    if repair {
        store.operations().upsert_key_trigger(new.clone()).await?;
        ticket.run(store.insert_slab_trigger(new.clone())).await?;
    } else {
        ticket.run(store.add_trigger(new.clone())).await?;
    }
    actor.drain().await;
    if matches!(delivery, Delivery::Firing) {
        let commit = actor.serve(scheduler.complete(old.id())).await?;
        ensure!(commit.is_none(), "the commit deleted the new row");
    }

    load_step(&mut actor.state, &mut actor.timers).await;
    let stored = store
        .current_trigger(&old.key, time, timer_type)
        .await?
        .ok_or_eyre("the key row is gone")?;
    let mut fired = Vec::new();
    for trigger in popped.into_iter().chain(actor.timers.next().await) {
        if actor.timers.fire(&trigger.id(), trigger.tag) {
            fired.push(trigger.tag);
        }
    }
    ensure!(
        fired == [stored.tag],
        "fired {fired:?}, not the stored {}",
        stored.tag
    );
    ensure!(
        actor.timers.next().await.is_none(),
        "an attempt fired twice"
    );
    Ok(())
}
