use super::*;
use crate::Topic;
use crate::consumer::Uncommitted;
use crate::telemetry::Telemetry;
use crate::timers::UncommittedTimer;
use crate::timers::active::Listed;
use crate::timers::duration::CompactDuration;
use crate::timers::scheduler::TimerSchedulerError;
use crate::timers::slab::{Slab, SlabId};
use crate::timers::store::adapter::TableAdapter;
use crate::timers::store::memory::{InMemoryTriggerStore, memory_store};
use crate::timers::test_support::{
    create_test_trigger, setup_timer_manager, setup_timer_manager_at, test_segment, test_semaphores,
};
use crate::timers::uncommitted::UncommittedTriggerGuard;
use color_eyre::eyre::{OptionExt, Result, eyre};
use futures::{StreamExt, pin_mut};
use quickcheck::{Arbitrary, Gen, QuickCheck};
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Builder;
use tokio::sync::watch;
use tokio::task;
use tokio::time::{self, advance, timeout};
use tracing::Span;

/// Makes a scheduling request for a test coordinate.
fn timer_request(trigger: &Trigger) -> TimerRequest {
    TimerRequest::new(
        trigger.key.clone(),
        trigger.time,
        trigger.timer_type,
        Span::current(),
    )
}

const SLAB_SECONDS: u32 = 60;

type Store = TableAdapter<InMemoryTriggerStore>;

/// A memory store for a new segment. If `due`, the watermark sits below the
/// current slab, so the actor owns [`past`] slabs from the start.
async fn seeded_store(due: bool) -> Result<(Store, CompactDateTime)> {
    let store = memory_store(test_segment("seeded", SLAB_SECONDS));
    let now = CompactDateTime::now()?;
    store.insert_segment().await?;
    if due {
        store
            .set_slab_watermark(slab_id(now).checked_sub(1))
            .await?;
    }
    Ok((store, now))
}

/// A time two slabs before `now`.
fn past(now: CompactDateTime) -> CompactDateTime {
    CompactDateTime::from(now.epoch_seconds().saturating_sub(2 * SLAB_SECONDS))
}

fn slab_id(time: CompactDateTime) -> SlabId {
    Slab::from_time(CompactDuration::new(SLAB_SECONDS), time).id()
}

/// How the scheduler of `manager` lists the timer at `time`.
async fn listed(
    manager: &TimerManager<Store>,
    key: &Key,
    time: CompactDateTime,
    timer_type: TimerType,
) -> Result<Option<Listed>> {
    let listed = manager.0.scheduler.list(key.clone(), timer_type).await?;
    Ok(listed
        .into_iter()
        .find_map(|(at, listed)| (at == time).then_some(listed)))
}

/// Helper: count scheduled times for a key and timer type
async fn count_scheduled<T: TriggerStore>(
    manager: &TimerManager<T>,
    key: &Key,
    timer_type: TimerType,
) -> Result<usize> {
    Ok(manager.scheduled_times(key, timer_type).await?.len())
}

/// Waits for the next timer of `stream`.
async fn next_pending<S: Stream + Unpin>(stream: &mut S) -> Result<S::Item> {
    timeout(Duration::from_secs(10), stream.next())
        .await?
        .ok_or_eyre("timer stream ended")
}

/// Helper: wait for timer and fire it
async fn wait_and_fire<S, T>(
    stream: &mut S,
    msg: &str,
) -> Result<(Trigger, UncommittedTriggerGuard<T>)>
where
    S: Stream<Item = PendingTimer<T>> + Unpin,
    T: TriggerStore,
{
    let pending = next_pending(stream).await?;
    let firing = pending
        .fire()
        .await
        .ok_or_else(|| eyre!("{msg} - not active"))?;
    Ok(firing.into_inner())
}

#[tokio::test]
async fn test_new_timer_manager_creation() -> Result<()> {
    time::pause();

    let segment = test_segment("test-segment", 300_u32);
    let store = memory_store(segment.clone());
    let telemetry = Telemetry::new();

    let (_shutdown_tx, shutdown_rx) = watch::channel(ShutdownPhase::default());
    let config = TimerManagerConfig {
        store,
        telemetry: telemetry.partition_sender(Topic::from("test"), 0),
        source: Arc::from(""),
    };
    let result = TimerManager::new(
        config,
        HeartbeatRegistry::test(),
        shutdown_rx,
        test_semaphores(),
    )
    .await;

    assert!(result.is_ok(), "Timer manager creation should succeed");

    let (_stream, manager) = result?;
    // Manager construction succeeded; the segment was bootstrapped into
    // the store and is now owned by the scheduler actor.
    let (stored, _) = manager
        .0
        .store
        .get_segment()
        .await?
        .ok_or_else(|| eyre!("segment should be persisted after manager init"))?
        .into_parts();
    assert_eq!(stored.id, segment.id);
    assert_eq!(stored.name, segment.name);
    assert_eq!(stored.slab_size, segment.slab_size);
    Ok(())
}

#[tokio::test]
async fn test_concurrent_operations() -> Result<()> {
    time::pause();

    let (_stream, manager, _shutdown_tx) = setup_timer_manager().await?;
    let manager = Arc::new(manager);

    // Spawn multiple concurrent operations
    let mut handles = vec![];

    // Schedule timers concurrently
    for i in 0..10 {
        let manager_clone = manager.clone();
        let handle = task::spawn(async move {
            let trigger =
                create_test_trigger(&format!("concurrent-{i}"), 60 + i, TimerType::Application)?;
            manager_clone.schedule(timer_request(&trigger)).await?;
            Ok::<_, color_eyre::Report>(())
        });
        handles.push(handle);
    }

    // Wait for all operations to complete
    for handle in handles {
        handle
            .await
            .map_err(|e| eyre!("Task join error: {}", e))??;
    }

    // Verify all timers were scheduled
    for i in 0..10_u8 {
        let key = Key::from(format!("concurrent-{i}"));
        let times = manager
            .scheduled_times(&key, TimerType::Application)
            .await?;
        assert_eq!(times.len(), 1, "Timer {i} should be scheduled");
    }
    Ok(())
}

// =========================================================================
// Shutdown Lifecycle Tests
// =========================================================================

/// The scheduler actor must keep serving commands through `Draining` so
/// active handlers can finish their timer operations.
/// Seeding the phase before the actor spawns removes the send-then-observe
/// race: the actor is already in the drain window on its first iteration.
#[tokio::test]
async fn clear_and_schedule_succeeds_while_draining() -> Result<()> {
    let (_stream, manager, _shutdown_tx) = setup_timer_manager_at(ShutdownPhase::Draining).await?;

    let key = Key::from("drain-handler");
    let time = CompactDateTime::now()?.add_duration(CompactDuration::new(60))?;
    let request = TimerRequest::new(key.clone(), time, TimerType::StateRecovery, Span::current());

    manager.clear_and_schedule(request).await?;

    // The op did not merely return `Ok`; the actor persisted the trigger.
    assert_eq!(
        count_scheduled(&manager, &key, TimerType::StateRecovery).await?,
        1,
        "timer scheduled during Draining must be persisted, not silently dropped"
    );
    Ok(())
}

/// The actor serves commands through `Draining` but exits at `Cancelling`:
/// once `Cancelling` is visible, its command channel closes and every
/// subsequent scheduler-reaching op surfaces `Scheduler(Shutdown)`. The
/// retained top-of-loop gate bounds the transition race to at most one
/// command completing after `Cancelling`, so the poll loop terminates.
#[tokio::test]
async fn actor_exits_at_cancelling() -> Result<()> {
    let (_stream, manager, shutdown_tx) = setup_timer_manager_at(ShutdownPhase::Draining).await?;

    let key = Key::from("cancel-exit");
    let time = CompactDateTime::now()?.add_duration(CompactDuration::new(60))?;

    // Alive through Draining: a command succeeds.
    manager
        .clear_and_schedule(TimerRequest::new(
            key.clone(),
            time,
            TimerType::Application,
            Span::current(),
        ))
        .await?;

    // Advancing to Cancelling must terminate the actor. Poll an idempotent
    // scheduler-reaching op until it observes the closed channel; the await
    // itself synchronizes with the actor (a nextest timeout is the sole
    // hang-guard, per TESTING.md — never the assertion).
    shutdown_tx.send(ShutdownPhase::Cancelling)?;
    loop {
        match manager
            .unschedule(&Key::from("never-scheduled"), time, TimerType::Application)
            .await
        {
            Err(TimerManagerError::Scheduler(TimerSchedulerError::Shutdown)) => break,
            Ok(()) => task::yield_now().await,
            Err(e) => return Err(eyre!("unexpected error awaiting actor exit: {e:#}")),
        }
    }
    Ok(())
}

#[tokio::test]
async fn test_fire_cancelled_timer() -> Result<()> {
    // Verify fire() returns None if timer was unscheduled after delivery but
    // before fire()
    time::pause();

    let (stream, manager, _shutdown_tx) = setup_timer_manager().await?;
    pin_mut!(stream);
    let trigger = create_test_trigger("fire-cancelled-key", 1, TimerType::Application)?;

    // Schedule timer
    manager.schedule(timer_request(&trigger)).await?;

    // Advance time to trigger emission into queue
    advance(Duration::from_secs(2)).await;
    task::yield_now().await;

    // Get the pending timer from stream (timer has been delivered)
    let pending = next_pending(&mut stream).await?;

    // Unschedule the timer AFTER delivery but BEFORE calling fire()
    // This is the race window where cancellation should still work
    manager
        .unschedule(&trigger.key, trigger.time, trigger.timer_type)
        .await?;

    // Verify fire() returns None since timer was cancelled
    let result = pending.fire().await;
    assert!(
        result.is_none(),
        "fire() should return None for cancelled timer"
    );

    Ok(())
}

/// A delivered timer that drops without a commit or an abort ends its
/// delivery, so memory keeps no delivery that nothing ends.
#[tokio::test]
async fn dropped_timer_ends_its_delivery() -> Result<()> {
    time::pause();

    let (stream, manager, _shutdown_tx) = setup_timer_manager().await?;
    pin_mut!(stream);
    let trigger = create_test_trigger("dropped-key", 1, TimerType::Application)?;
    manager.schedule(timer_request(&trigger)).await?;
    advance(Duration::from_secs(2)).await;

    let pending = next_pending(&mut stream).await?;
    let firing = pending
        .fire()
        .await
        .ok_or_else(|| eyre!("the timer did not fire"))?;
    drop(firing);

    let snapshot = manager.snapshot().await?;
    assert_eq!(snapshot.in_flight, 0, "a dropped delivery still runs");
    Ok(())
}

mod cancel;
mod crud;
mod identity;
mod lifecycle;
