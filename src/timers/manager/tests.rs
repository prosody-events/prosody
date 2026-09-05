use super::*;
use crate::Topic;
use crate::consumer::{Keyed, Uncommitted};
use crate::telemetry::Telemetry;
use crate::timers::UncommittedTimer;
use crate::timers::duration::CompactDuration;
use crate::timers::scheduler::TimerSchedulerError;
use crate::timers::store::adapter::TableAdapter;
use crate::timers::store::memory::{InMemoryTriggerStore, memory_store};
use crate::timers::store::tests::common::KEY_POOL;
use crate::timers::test_support::{
    create_test_trigger, setup_timer_manager, setup_timer_manager_at, setup_timer_manager_over,
    test_segment, test_semaphores,
};
use crate::timers::uncommitted::{Fired, UncommittedTriggerGuard};
use color_eyre::eyre::{Result, ensure, eyre};
use futures::{StreamExt, pin_mut};
use quickcheck::{Arbitrary, Gen, QuickCheck, TestResult};
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;
use strum::VariantArray;
use tokio::runtime::Builder;
use tokio::sync::watch;
use tokio::task;
use tokio::time::{self, advance, timeout};
use tracing::Span;

/// Helper: count scheduled times for a key and timer type
async fn count_scheduled<T: TriggerStore>(
    manager: &TimerManager<T>,
    key: &Key,
    timer_type: TimerType,
) -> Result<usize> {
    Ok(manager.scheduled_times(key, timer_type).await?.len())
}

async fn wait_for_owned(
    manager: &TimerManager<TableAdapter<InMemoryTriggerStore>>,
    trigger: &Trigger,
) -> Result<()> {
    for _ in 0..500_u16 {
        advance(Duration::from_millis(10)).await;
        task::yield_now().await;
        if manager
            .0
            .scheduler
            .active_triggers()
            .contains(&trigger.key, trigger.time, trigger.timer_type)
            .await
        {
            return Ok(());
        }
    }
    Err(eyre!("timer slab was not owned"))
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
    let pending = stream.next().await.ok_or_else(|| eyre!("{msg}"))?;
    let firing = pending
        .fire(&watch::channel(ShutdownPhase::default()).1)
        .await
        .and_then(Fired::into_live)
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
    let stored = manager
        .0
        .store
        .get_segment()
        .await?
        .ok_or_else(|| eyre!("segment should be persisted after manager init"))?;
    assert_eq!(stored.id, segment.id);
    assert_eq!(stored.name, segment.name);
    assert_eq!(stored.slab_size, segment.slab_size);
    Ok(())
}

#[tokio::test]
async fn test_timer_stream_delivery() -> Result<()> {
    time::pause();

    let (stream, manager, _shutdown_tx) = setup_timer_manager().await?;
    pin_mut!(stream);

    // Schedule a timer for immediate execution
    let now = CompactDateTime::now()?;
    let immediate_time = now.add_duration(CompactDuration::new(1))?;
    let trigger = Trigger::new(
        Key::from("stream-test"),
        immediate_time,
        TimerType::Application,
        Span::current(),
    );

    manager.schedule_trigger(trigger.clone()).await?;

    // Advance time past the trigger time
    advance(Duration::from_secs(2)).await;
    task::yield_now().await;

    if let Some(pending_timer) = stream.next().await {
        let firing_timer = pending_timer
            .fire(&watch::channel(ShutdownPhase::default()).1)
            .await
            .and_then(Fired::into_live)
            .ok_or_else(|| eyre!("Timer should be active"))?;
        let (trigger_data, _) = firing_timer.into_inner();
        assert_eq!(trigger_data.key, trigger.key);
        assert_eq!(trigger_data.time, trigger.time);
    }

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
            manager_clone.schedule_trigger(trigger).await?;
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

#[tokio::test]
async fn test_timer_type_isolation_end_to_end() -> Result<()> {
    time::pause();

    let (stream, manager, _shutdown_tx) = setup_timer_manager().await?;
    pin_mut!(stream);
    let key = Key::from("isolation-key");
    let time = CompactDateTime::now()?.add_duration(CompactDuration::new(1))?;

    // Schedule BOTH types at same (key, time)
    let app = Trigger::new(key.clone(), time, TimerType::Application, Span::current());
    let retry = Trigger::new(
        key.clone(),
        time,
        TimerType::DeferredMessage,
        Span::current(),
    );
    manager.schedule_trigger(app).await?;
    manager.schedule_trigger(retry).await?;

    // Allow scheduler to process and verify both types are scheduled
    advance(Duration::from_millis(100)).await;
    task::yield_now().await;
    assert_eq!(
        count_scheduled(&manager, &key, TimerType::Application).await?,
        1
    );
    assert_eq!(
        count_scheduled(&manager, &key, TimerType::DeferredMessage).await?,
        1
    );

    // Advance time to trigger BOTH timers
    advance(Duration::from_secs(2)).await;
    task::yield_now().await;

    // Fire both timers (order may vary)
    let (t1, g1) = wait_and_fire(&mut stream, "First timer").await?;
    let (t2, g2) = wait_and_fire(&mut stream, "Second timer").await?;

    // Verify we got both types with correct key/time
    let types = [t1.timer_type, t2.timer_type];
    assert!(
        types.contains(&TimerType::Application),
        "Application should fire"
    );
    assert!(
        types.contains(&TimerType::DeferredMessage),
        "DeferredMessage should fire"
    );
    assert_eq!((t1.key.clone(), t1.time), (key.clone(), time));
    assert_eq!((t2.key.clone(), t2.time), (key.clone(), time));

    // Separate guards by type and commit Application only
    let (app_guard, retry_guard) = if t1.timer_type == TimerType::Application {
        (g1, g2)
    } else {
        (g2, g1)
    };
    app_guard.commit().await;

    // Verify isolation: Application is removed from DB
    // Note: DeferredMessage is still in Firing state, so it's excluded from
    // scheduled_times() (Firing state is excluded by design).
    // The important isolation property is that committing Application
    // doesn't affect DeferredMessage's ability to commit separately.
    assert_eq!(
        count_scheduled(&manager, &key, TimerType::Application).await?,
        0,
        "Application should be removed after commit"
    );
    // DeferredMessage in Firing state - excluded from scheduled_times by design
    assert_eq!(
        count_scheduled(&manager, &key, TimerType::DeferredMessage).await?,
        0,
        "DeferredMessage in Firing state is excluded from scheduled_times"
    );

    // Commit DeferredMessage and verify both gone from DB
    retry_guard.commit().await;
    assert_eq!(
        count_scheduled(&manager, &key, TimerType::Application).await?,
        0,
        "Application should remain removed"
    );
    assert_eq!(
        count_scheduled(&manager, &key, TimerType::DeferredMessage).await?,
        0,
        "DeferredMessage should be removed after commit"
    );

    Ok(())
}

#[tokio::test]
async fn test_timer_type_unschedule_isolation() -> Result<()> {
    time::pause();

    let (stream, manager, _shutdown_tx) = setup_timer_manager().await?;
    pin_mut!(stream);
    let key = Key::from("unschedule-isolation-key");
    let time = CompactDateTime::now()?.add_duration(CompactDuration::new(1))?;

    // Schedule BOTH types at same (key, time)
    let app = Trigger::new(key.clone(), time, TimerType::Application, Span::current());
    let retry = Trigger::new(
        key.clone(),
        time,
        TimerType::DeferredMessage,
        Span::current(),
    );
    manager.schedule_trigger(app).await?;
    manager.schedule_trigger(retry).await?;

    // Allow scheduler to process and verify both scheduled
    advance(Duration::from_millis(100)).await;
    task::yield_now().await;
    assert_eq!(
        count_scheduled(&manager, &key, TimerType::Application).await?,
        1
    );
    assert_eq!(
        count_scheduled(&manager, &key, TimerType::DeferredMessage).await?,
        1
    );

    // Unschedule ONLY Application and verify isolation
    manager
        .unschedule(&key, time, TimerType::Application)
        .await?;
    task::yield_now().await;
    assert_eq!(
        count_scheduled(&manager, &key, TimerType::Application).await?,
        0
    );
    assert_eq!(
        count_scheduled(&manager, &key, TimerType::DeferredMessage).await?,
        1
    );

    // Advance time - only DeferredMessage should fire
    advance(Duration::from_secs(2)).await;
    task::yield_now().await;
    let (fired, guard) = wait_and_fire(&mut stream, "DeferredMessage timer").await?;
    assert_eq!(
        fired.timer_type,
        TimerType::DeferredMessage,
        "Only DeferredMessage fires"
    );
    assert_eq!((fired.key, fired.time), (key.clone(), time));

    // Commit and verify no more timers
    guard.commit().await;
    advance(Duration::from_secs(1)).await;
    task::yield_now().await;
    assert!(
        timeout(Duration::from_millis(100), stream.next())
            .await
            .is_err()
    );

    Ok(())
}

// =========================================================================
// Shutdown Lifecycle Tests
// =========================================================================

/// The scheduler actor must keep serving commands through `Draining` so
/// in-flight handlers can arm their recovery backstop as they settle.
/// Seeding the phase before the actor spawns removes the send-then-observe
/// race: the actor is already in the drain window on its first iteration.
#[tokio::test]
async fn clear_and_schedule_succeeds_while_draining() -> Result<()> {
    let (_stream, manager, _shutdown_tx) = setup_timer_manager_at(ShutdownPhase::Draining).await?;

    let key = Key::from("drain-backstop");
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

// =========================================================================
// Reschedule Firing Timer Tests
// =========================================================================

#[tokio::test]
async fn test_reschedule_idempotent() -> Result<()> {
    // Multiple reschedules while firing are no-op (idempotent)
    time::pause();

    let (stream, manager, _shutdown_tx) = setup_timer_manager().await?;
    pin_mut!(stream);
    let trigger = create_test_trigger("idempotent-key", 1, TimerType::Application)?;

    // Schedule and fire
    manager.schedule_trigger(trigger.clone()).await?;
    advance(Duration::from_secs(2)).await;
    task::yield_now().await;

    let pending = stream.next().await.ok_or_else(|| eyre!("No timer"))?;
    let firing = pending
        .fire(&watch::channel(ShutdownPhase::default()).1)
        .await
        .and_then(Fired::into_live)
        .ok_or_else(|| eyre!("Not active"))?;

    // Reschedule multiple times - all should succeed as no-ops
    manager.schedule_trigger(trigger.clone()).await?;
    manager.schedule_trigger(trigger.clone()).await?;
    manager.schedule_trigger(trigger.clone()).await?;

    // Commit and verify only fires once more (not 3 times)
    let (_, guard) = firing.into_inner();
    guard.commit().await;

    // Advance time and verify exactly one more fire
    advance(Duration::from_secs(2)).await;
    task::yield_now().await;

    let pending2 = timeout(Duration::from_millis(100), stream.next())
        .await
        .map_err(|_| eyre!("Timer should fire again"))?
        .ok_or_else(|| eyre!("No second timer"))?;

    let firing2 = pending2
        .fire(&watch::channel(ShutdownPhase::default()).1)
        .await
        .and_then(Fired::into_live)
        .ok_or_else(|| eyre!("Second fire not active"))?;
    let (_, guard2) = firing2.into_inner();
    guard2.commit().await;

    // No more timers should fire
    advance(Duration::from_secs(2)).await;
    task::yield_now().await;
    assert!(
        timeout(Duration::from_millis(100), stream.next())
            .await
            .is_err(),
        "No more timers should fire"
    );

    Ok(())
}

#[tokio::test]
async fn test_commit_deletes_when_not_rescheduled() -> Result<()> {
    // Commit from FIRING state deletes DB row
    time::pause();

    let (stream, manager, _shutdown_tx) = setup_timer_manager().await?;
    pin_mut!(stream);
    let trigger = create_test_trigger("delete-key", 1, TimerType::Application)?;

    // Schedule and fire
    manager.schedule_trigger(trigger.clone()).await?;
    advance(Duration::from_secs(2)).await;
    task::yield_now().await;

    let pending = stream.next().await.ok_or_else(|| eyre!("No timer"))?;
    let firing = pending
        .fire(&watch::channel(ShutdownPhase::default()).1)
        .await
        .and_then(Fired::into_live)
        .ok_or_else(|| eyre!("Not active"))?;

    // Commit without rescheduling (FIRING → UNSCHEDULED)
    let (_, guard) = firing.into_inner();
    guard.commit().await;

    // Verify timer is completely removed
    let times = manager
        .scheduled_times(&trigger.key, TimerType::Application)
        .await?;
    assert!(times.is_empty(), "Timer should be deleted from DB");

    // Verify no more fires
    advance(Duration::from_secs(2)).await;
    task::yield_now().await;
    assert!(
        timeout(Duration::from_millis(100), stream.next())
            .await
            .is_err(),
        "Timer should not fire again"
    );

    Ok(())
}

#[tokio::test]
async fn test_abort_rescheduled_stays_scheduled() -> Result<()> {
    // Abort from FIRING_RESCHEDULED transitions to SCHEDULED
    time::pause();

    let (stream, manager, _shutdown_tx) = setup_timer_manager().await?;
    pin_mut!(stream);
    let trigger = create_test_trigger("abort-reschedule-key", 1, TimerType::Application)?;

    // Schedule, fire, and reschedule
    manager.schedule_trigger(trigger.clone()).await?;
    advance(Duration::from_secs(2)).await;
    task::yield_now().await;

    let pending = stream.next().await.ok_or_else(|| eyre!("No timer"))?;
    let firing = pending
        .fire(&watch::channel(ShutdownPhase::default()).1)
        .await
        .and_then(Fired::into_live)
        .ok_or_else(|| eyre!("Not active"))?;
    manager.schedule_trigger(trigger.clone()).await?;

    // Abort with reschedule (FIRING_RESCHEDULED → SCHEDULED)
    let (_, guard) = firing.into_inner();
    guard.abort().await;

    // Verify timer fires again
    advance(Duration::from_secs(2)).await;
    task::yield_now().await;

    let pending2 = timeout(Duration::from_millis(100), stream.next())
        .await
        .map_err(|_| eyre!("Timer should fire again after abort"))?
        .ok_or_else(|| eyre!("No second timer"))?;
    assert!(
        pending2
            .fire(&watch::channel(ShutdownPhase::default()).1)
            .await
            .is_some(),
        "Second fire should work"
    );

    Ok(())
}

#[tokio::test]
async fn test_reschedule_same_time_fires_again() -> Result<()> {
    // End-to-end integration test: schedule, fire, reschedule, commit, fires
    // again
    time::pause();

    let (stream, manager, _shutdown_tx) = setup_timer_manager().await?;
    pin_mut!(stream);
    let trigger = create_test_trigger("e2e-key", 1, TimerType::Application)?;

    // 1. Schedule timer
    manager.schedule_trigger(trigger.clone()).await?;

    // 2. Timer fires
    advance(Duration::from_secs(2)).await;
    task::yield_now().await;

    let pending1 = stream.next().await.ok_or_else(|| eyre!("First timer"))?;
    let firing1 = pending1
        .fire(&watch::channel(ShutdownPhase::default()).1)
        .await
        .and_then(Fired::into_live)
        .ok_or_else(|| eyre!("First fire"))?;

    // 3. Reschedule during handler
    manager.schedule_trigger(trigger.clone()).await?;

    // 4. Commit
    let (_, guard1) = firing1.into_inner();
    guard1.commit().await;

    // 5. Timer fires again
    advance(Duration::from_secs(2)).await;
    task::yield_now().await;

    let pending2 = timeout(Duration::from_millis(100), stream.next())
        .await
        .map_err(|_| eyre!("Second timer should fire"))?
        .ok_or_else(|| eyre!("No second timer"))?;

    let firing2 = pending2
        .fire(&watch::channel(ShutdownPhase::default()).1)
        .await
        .and_then(Fired::into_live)
        .ok_or_else(|| eyre!("Second fire not active"))?;

    // Verify it's the same timer
    let (trigger2, guard2) = firing2.into_inner();
    assert_eq!(trigger2.key, trigger.key);
    assert_eq!(trigger2.time, trigger.time);
    assert_eq!(trigger2.timer_type, trigger.timer_type);

    // Commit without reschedule - timer should be done
    guard2.commit().await;

    advance(Duration::from_secs(2)).await;
    task::yield_now().await;
    assert!(
        timeout(Duration::from_millis(100), stream.next())
            .await
            .is_err(),
        "Timer should not fire a third time"
    );

    Ok(())
}

// =========================================================================
// Cancel Reschedule Tests
// =========================================================================

#[tokio::test]
async fn test_unschedule_firing_noop() -> Result<()> {
    // Verify unschedule when firing (not rescheduled) is a no-op
    time::pause();

    let (stream, manager, _shutdown_tx) = setup_timer_manager().await?;
    pin_mut!(stream);
    let trigger = create_test_trigger("unschedule-firing-key", 1, TimerType::Application)?;

    // Schedule and wait for timer to fire
    manager.schedule_trigger(trigger.clone()).await?;
    advance(Duration::from_secs(2)).await;
    task::yield_now().await;

    // Fire the timer (transition to FIRING state)
    let pending = stream.next().await.ok_or_else(|| eyre!("No timer"))?;
    let firing = pending
        .fire(&watch::channel(ShutdownPhase::default()).1)
        .await
        .and_then(Fired::into_live)
        .ok_or_else(|| eyre!("Not active"))?;

    // Unschedule while firing - should be a no-op (FIRING state)
    let unschedule_result = manager
        .unschedule(&trigger.key, trigger.time, trigger.timer_type)
        .await;
    assert!(unschedule_result.is_ok(), "Unschedule should succeed");

    // Verify timer is still in FIRING state (not removed)
    let current_state = manager
        .0
        .scheduler
        .active_triggers()
        .get_state(&trigger.key, trigger.time, trigger.timer_type)
        .await;
    assert_eq!(
        current_state,
        Some(TimerState::Firing),
        "Timer should still be in Firing state"
    );

    // Commit normally - timer should be deleted since not rescheduled
    let (_, guard) = firing.into_inner();
    guard.commit().await;

    // Verify timer is completely removed
    let times = manager
        .scheduled_times(&trigger.key, TimerType::Application)
        .await?;
    assert!(times.is_empty(), "Timer should be deleted after commit");

    // No more fires
    advance(Duration::from_secs(2)).await;
    task::yield_now().await;
    assert!(
        timeout(Duration::from_millis(100), stream.next())
            .await
            .is_err(),
        "Timer should not fire again"
    );

    Ok(())
}

#[tokio::test]
async fn test_unschedule_cancels_reschedule() -> Result<()> {
    // Verify unschedule when firing+rescheduled cancels the reschedule
    time::pause();

    let (stream, manager, _shutdown_tx) = setup_timer_manager().await?;
    pin_mut!(stream);
    let trigger = create_test_trigger("cancel-reschedule-key", 1, TimerType::Application)?;

    // Schedule and wait for timer to fire
    manager.schedule_trigger(trigger.clone()).await?;
    advance(Duration::from_secs(2)).await;
    task::yield_now().await;

    // Fire the timer (transition to FIRING state)
    let pending = stream.next().await.ok_or_else(|| eyre!("No timer"))?;
    let firing = pending
        .fire(&watch::channel(ShutdownPhase::default()).1)
        .await
        .and_then(Fired::into_live)
        .ok_or_else(|| eyre!("Not active"))?;

    // Reschedule while firing (FIRING → FIRING_RESCHEDULED)
    manager.schedule_trigger(trigger.clone()).await?;

    // Verify state is FiringRescheduled
    let state_after_reschedule = manager
        .0
        .scheduler
        .active_triggers()
        .get_state(&trigger.key, trigger.time, trigger.timer_type)
        .await;
    assert_eq!(
        state_after_reschedule,
        Some(TimerState::FiringRescheduled),
        "Timer should be in FiringRescheduled state"
    );

    // Unschedule to cancel the reschedule (FIRING_RESCHEDULED → FIRING)
    let unschedule_result = manager
        .unschedule(&trigger.key, trigger.time, trigger.timer_type)
        .await;
    assert!(unschedule_result.is_ok(), "Unschedule should succeed");

    // Verify state is back to Firing
    let state_after_unschedule = manager
        .0
        .scheduler
        .active_triggers()
        .get_state(&trigger.key, trigger.time, trigger.timer_type)
        .await;
    assert_eq!(
        state_after_unschedule,
        Some(TimerState::Firing),
        "Timer should be back in Firing state"
    );

    // Commit - timer should be deleted since reschedule was cancelled
    let (_, guard) = firing.into_inner();
    guard.commit().await;

    // Verify timer is completely removed
    let times = manager
        .scheduled_times(&trigger.key, TimerType::Application)
        .await?;
    assert!(times.is_empty(), "Timer should be deleted after commit");

    // Timer should NOT fire again (reschedule was cancelled)
    advance(Duration::from_secs(2)).await;
    task::yield_now().await;
    assert!(
        timeout(Duration::from_millis(100), stream.next())
            .await
            .is_err(),
        "Timer should NOT fire again after reschedule was cancelled"
    );

    Ok(())
}

// =========================================================================
// State-Aware Query Tests: scheduled_times() filtering
// =========================================================================

#[tokio::test]
async fn test_scheduled_times_excludes_firing() -> Result<()> {
    // Verify firing timers are excluded from scheduled_times()
    time::pause();

    let (stream, manager, _shutdown_tx) = setup_timer_manager().await?;
    pin_mut!(stream);
    let trigger = create_test_trigger("exclude-firing-key", 1, TimerType::Application)?;

    // Schedule timer
    manager.schedule_trigger(trigger.clone()).await?;

    // Verify timer is in scheduled_times before firing
    let times_before = manager
        .scheduled_times(&trigger.key, TimerType::Application)
        .await?;
    assert_eq!(
        times_before.len(),
        1,
        "Timer should be in scheduled_times before firing"
    );
    assert!(times_before.contains(&trigger.time));

    // Advance time and fire the timer
    advance(Duration::from_secs(2)).await;
    task::yield_now().await;

    let pending = stream.next().await.ok_or_else(|| eyre!("No timer"))?;
    let firing = pending
        .fire(&watch::channel(ShutdownPhase::default()).1)
        .await
        .and_then(Fired::into_live)
        .ok_or_else(|| eyre!("Not active"))?;

    // Verify timer is NOT in scheduled_times while firing
    let times_during = manager
        .scheduled_times(&trigger.key, TimerType::Application)
        .await?;
    assert!(
        times_during.is_empty(),
        "Timer in Firing state should NOT be in scheduled_times"
    );

    // Commit and verify timer is removed
    let (_, guard) = firing.into_inner();
    guard.commit().await;

    let times_after = manager
        .scheduled_times(&trigger.key, TimerType::Application)
        .await?;
    assert!(times_after.is_empty(), "Timer should be gone after commit");

    Ok(())
}

#[tokio::test]
async fn test_scheduled_times_includes_rescheduled() -> Result<()> {
    // Verify FiringRescheduled timers are included in scheduled_times()
    time::pause();

    let (stream, manager, _shutdown_tx) = setup_timer_manager().await?;
    pin_mut!(stream);
    let trigger = create_test_trigger("include-rescheduled-key", 1, TimerType::Application)?;

    // Schedule timer
    manager.schedule_trigger(trigger.clone()).await?;

    // Advance time and fire the timer
    advance(Duration::from_secs(2)).await;
    task::yield_now().await;

    let pending = stream.next().await.ok_or_else(|| eyre!("No timer"))?;
    let firing = pending
        .fire(&watch::channel(ShutdownPhase::default()).1)
        .await
        .and_then(Fired::into_live)
        .ok_or_else(|| eyre!("Not active"))?;

    // While firing, timer should NOT be in scheduled_times
    let times_firing = manager
        .scheduled_times(&trigger.key, TimerType::Application)
        .await?;
    assert!(
        times_firing.is_empty(),
        "Timer in Firing state should NOT be in scheduled_times"
    );

    // Reschedule the timer (FIRING → FIRING_RESCHEDULED)
    manager.schedule_trigger(trigger.clone()).await?;

    // Now timer SHOULD be in scheduled_times (FiringRescheduled includes it)
    let times_rescheduled = manager
        .scheduled_times(&trigger.key, TimerType::Application)
        .await?;
    assert_eq!(
        times_rescheduled.len(),
        1,
        "Timer in FiringRescheduled state SHOULD be in scheduled_times"
    );
    assert!(times_rescheduled.contains(&trigger.time));

    // Commit and verify timer is still scheduled (transitions to Scheduled)
    let (_, guard) = firing.into_inner();
    guard.commit().await;

    let times_after_commit = manager
        .scheduled_times(&trigger.key, TimerType::Application)
        .await?;
    assert_eq!(
        times_after_commit.len(),
        1,
        "Timer should still be scheduled after commit from FiringRescheduled"
    );
    assert!(times_after_commit.contains(&trigger.time));

    Ok(())
}

// =========================================================================
// Type-Safe Timer Lifecycle Tests
// =========================================================================

#[tokio::test]
async fn test_fire_scheduled_timer() -> Result<()> {
    // Verify fire() returns Some for a scheduled timer
    time::pause();

    let (stream, manager, _shutdown_tx) = setup_timer_manager().await?;
    pin_mut!(stream);
    let trigger = create_test_trigger("fire-scheduled-key", 1, TimerType::Application)?;

    // Schedule timer
    manager.schedule_trigger(trigger.clone()).await?;

    // Advance time to trigger emission
    advance(Duration::from_secs(2)).await;
    task::yield_now().await;

    // Get the pending timer from stream
    let pending = stream
        .next()
        .await
        .ok_or_else(|| eyre!("Expected a pending timer"))?;

    // Verify fire() returns Some for scheduled timer
    let firing = pending
        .fire(&watch::channel(ShutdownPhase::default()).1)
        .await
        .and_then(Fired::into_live)
        .ok_or_else(|| eyre!("fire() should return Some for scheduled timer"))?;

    // Verify the FiringTimer has correct metadata
    assert_eq!(firing.time(), trigger.time);
    assert_eq!(firing.timer_type(), TimerType::Application);
    assert_eq!(firing.key(), &trigger.key);

    // Clean up
    let (_, guard) = firing.into_inner();
    guard.commit().await;

    Ok(())
}

#[tokio::test]
async fn test_fire_cancelled_timer() -> Result<()> {
    // Verify fire() returns None if timer was unscheduled after delivery but before
    // fire()
    time::pause();

    let (stream, manager, _shutdown_tx) = setup_timer_manager().await?;
    pin_mut!(stream);
    let trigger = create_test_trigger("fire-cancelled-key", 1, TimerType::Application)?;

    // Schedule timer
    manager.schedule_trigger(trigger.clone()).await?;

    // Advance time to trigger emission into queue
    advance(Duration::from_secs(2)).await;
    task::yield_now().await;

    // Get the pending timer from stream (timer has been delivered)
    let pending = stream
        .next()
        .await
        .ok_or_else(|| eyre!("Expected a pending timer"))?;

    // Unschedule the timer AFTER delivery but BEFORE calling fire()
    // This is the race window where cancellation should still work
    manager
        .unschedule(&trigger.key, trigger.time, trigger.timer_type)
        .await?;

    // Verify fire() returns None since timer was cancelled
    let result = pending
        .fire(&watch::channel(ShutdownPhase::default()).1)
        .await;
    assert!(
        result.is_none(),
        "fire() should return None for cancelled timer"
    );

    Ok(())
}

#[tokio::test]
async fn test_reschedule_abort_fires_again() -> Result<()> {
    // End-to-end integration test: reschedule then abort, timer fires again
    time::pause();

    let (stream, manager, _shutdown_tx) = setup_timer_manager().await?;
    pin_mut!(stream);
    let trigger = create_test_trigger("reschedule-abort-key", 1, TimerType::Application)?;

    // 1. Schedule timer
    manager.schedule_trigger(trigger.clone()).await?;

    // 2. Timer fires
    advance(Duration::from_secs(2)).await;
    task::yield_now().await;

    let pending1 = stream
        .next()
        .await
        .ok_or_else(|| eyre!("First timer should fire"))?;
    let firing1 = pending1
        .fire(&watch::channel(ShutdownPhase::default()).1)
        .await
        .and_then(Fired::into_live)
        .ok_or_else(|| eyre!("First fire should succeed"))?;

    // 3. Reschedule during handler (FIRING → FIRING_RESCHEDULED)
    manager.schedule_trigger(trigger.clone()).await?;

    // 4. Abort (FIRING_RESCHEDULED → SCHEDULED, timer remains in DelayQueue)
    let (_, guard1) = firing1.into_inner();
    guard1.abort().await;

    // 5. Timer should fire again (already in DelayQueue from reschedule)
    advance(Duration::from_secs(2)).await;
    task::yield_now().await;

    let pending2 = timeout(Duration::from_millis(100), stream.next())
        .await
        .map_err(|_| eyre!("Second timer should fire after abort"))?
        .ok_or_else(|| eyre!("No second timer"))?;

    let firing2 = pending2
        .fire(&watch::channel(ShutdownPhase::default()).1)
        .await
        .and_then(Fired::into_live)
        .ok_or_else(|| eyre!("Second fire should succeed"))?;

    // 6. Verify it's the same timer
    let (trigger2, guard2) = firing2.into_inner();
    assert_eq!(trigger2.key, trigger.key);
    assert_eq!(trigger2.time, trigger.time);
    assert_eq!(trigger2.timer_type, trigger.timer_type);

    // 7. Commit without reschedule - timer should be done
    guard2.commit().await;

    advance(Duration::from_secs(2)).await;
    task::yield_now().await;
    assert!(
        timeout(Duration::from_millis(100), stream.next())
            .await
            .is_err(),
        "Timer should not fire a third time"
    );

    Ok(())
}

#[tokio::test]
async fn test_abort_firing_preserves_db() -> Result<()> {
    // Verify abort from Firing state keeps DB row and protects active slab state.
    time::pause();

    let (stream, manager, _shutdown_tx) = setup_timer_manager().await?;
    pin_mut!(stream);
    let trigger = create_test_trigger("abort-firing-key", 1, TimerType::Application)?;

    // Schedule timer
    manager.schedule_trigger(trigger.clone()).await?;

    // Advance time and fire the timer
    advance(Duration::from_secs(2)).await;
    task::yield_now().await;

    let pending = stream
        .next()
        .await
        .ok_or_else(|| eyre!("Expected a pending timer"))?;
    let firing = pending
        .fire(&watch::channel(ShutdownPhase::default()).1)
        .await
        .and_then(Fired::into_live)
        .ok_or_else(|| eyre!("Not active"))?;

    // Verify timer is in Firing state - scheduled_times() excludes Firing timers
    let times_while_firing = manager
        .scheduled_times(&trigger.key, TimerType::Application)
        .await?;
    assert!(
        times_while_firing.is_empty(),
        "Timer in Firing state should be excluded from scheduled_times()"
    );

    // Abort the timer (transitions to Parked, DB preserved for recovery)
    let (_, guard) = firing.into_inner();
    guard.abort().await;

    let state_after_abort = manager
        .0
        .scheduler
        .active_triggers()
        .get_state(&trigger.key, trigger.time, trigger.timer_type)
        .await;
    assert_eq!(
        state_after_abort,
        Some(TimerState::Parked),
        "Timer should remain active as Parked after abort"
    );

    // Verify timer is still visible through scheduled_times because its DB
    // row is preserved for recovery/requeue.
    let times_after_abort = manager
        .scheduled_times(&trigger.key, TimerType::Application)
        .await?;
    assert_eq!(
        times_after_abort.len(),
        1,
        "Timer should still be in DB after abort (preserved for recovery)"
    );
    assert!(times_after_abort.contains(&trigger.time));

    Ok(())
}

#[tokio::test]
async fn test_clear_and_schedule_firing_same_time() -> Result<()> {
    // clear_and_schedule with Firing state at same time as new timer.
    // Schedule T at time X → fire → clear_and_schedule at same time X →
    // verify FiringRescheduled → commit → verify timer fires again.
    time::pause();

    let (stream, manager, _shutdown_tx) = setup_timer_manager().await?;
    pin_mut!(stream);
    let trigger = create_test_trigger("cas-firing-key", 1, TimerType::Application)?;

    // Step 1: Schedule timer T at time X
    manager.schedule_trigger(trigger.clone()).await?;
    advance(Duration::from_secs(2)).await;
    task::yield_now().await;

    // Step 2: Timer fires, enters Firing state
    let pending = stream
        .next()
        .await
        .ok_or_else(|| eyre!("Expected pending timer"))?;
    let firing = pending
        .fire(&watch::channel(ShutdownPhase::default()).1)
        .await
        .and_then(Fired::into_live)
        .ok_or_else(|| eyre!("Expected active timer"))?;
    let first_firing_tag = firing.trigger().tag;

    // Step 3: clear_and_schedule with a new timer at the SAME time X.
    // This exercises the Firing → FiringRescheduled path in clear_and_schedule
    // and the skip in unschedule_replaced_timers.
    manager
        .clear_and_schedule(TimerRequest::new(
            trigger.key.clone(),
            trigger.time,
            trigger.timer_type,
            Span::current(),
        ))
        .await?;

    let store_tag_before_commit = manager
        .0
        .store
        .current_tag(&trigger.key, trigger.time, trigger.timer_type)
        .await?
        .ok_or_else(|| eyre!("store tag missing before commit"))?;
    assert_eq!(
        store_tag_before_commit, first_firing_tag,
        "same-coordinate clear_and_schedule must preserve the WAL tag before commit"
    );

    // Step 4: Verify transition to FiringRescheduled
    let is_scheduled = manager
        .0
        .scheduler
        .active_triggers()
        .is_scheduled(&trigger.key, trigger.time, trigger.timer_type)
        .await;
    assert!(
        is_scheduled,
        "Timer should be scheduled (FiringRescheduled) after clear_and_schedule"
    );

    // Step 5: Commit the first firing. FiringRescheduled → re-queued.
    let (_, guard) = firing.into_inner();
    guard.commit().await;

    let tag_after_commit = manager
        .0
        .store
        .current_tag(&trigger.key, trigger.time, trigger.timer_type)
        .await?
        .ok_or_else(|| eyre!("tag missing after commit"))?;
    assert_ne!(
        tag_after_commit, first_firing_tag,
        "commit after same-coordinate clear_and_schedule must rotate the tag"
    );

    // The timer should still be scheduled after commit
    let times = manager
        .scheduled_times(&trigger.key, TimerType::Application)
        .await?;
    assert_eq!(
        times.len(),
        1,
        "Timer should be scheduled for re-firing after commit"
    );
    assert!(times.contains(&trigger.time));

    // Advance time again and verify the timer fires a second time
    advance(Duration::from_secs(2)).await;
    task::yield_now().await;

    let pending2 = timeout(Duration::from_secs(5), stream.next())
        .await?
        .ok_or_else(|| eyre!("Expected timer to fire again after FiringRescheduled commit"))?;
    let firing2 = pending2
        .fire(&watch::channel(ShutdownPhase::default()).1)
        .await
        .and_then(Fired::into_live)
        .ok_or_else(|| eyre!("Second firing not active"))?;

    let (refired_trigger, guard2) = firing2.into_inner();
    assert_eq!(refired_trigger.key, trigger.key);
    assert_eq!(refired_trigger.time, trigger.time);
    assert_eq!(
        refired_trigger.tag, tag_after_commit,
        "second handler must observe the rotated tag"
    );
    guard2.commit().await;

    Ok(())
}

/// Traces from property failures. Each one lost a source or fired the wrong
/// kind.
#[tokio::test]
async fn pinned_oracle_traces() -> Result<()> {
    use ManagerOracleOp::{Abort, DifferentCoordinateClear, Fire, Receipt, Restart, Schedule};
    for ops in [
        vec![Schedule, Fire, DifferentCoordinateClear, Restart, Fire],
        vec![Schedule, Fire, Receipt, Restart, Schedule, Fire],
        vec![Schedule, Fire, Schedule, Receipt, Restart, Fire],
        vec![Schedule, Fire, Abort, Fire],
        vec![
            Schedule,
            Fire,
            Receipt,
            Restart,
            Schedule,
            DifferentCoordinateClear,
            Schedule,
            Fire,
        ],
    ] {
        run_same_coordinate_oracle_trace(&ManagerOracleTrace { timer_type: 0, ops }).await?;
        time::resume();
    }
    Ok(())
}

#[test]
fn prop_same_coordinate_clear_preserves_timer_oracle() {
    QuickCheck::new().quickcheck(
        prop_same_coordinate_clear_preserves_timer_oracle_inner
            as fn(ManagerOracleTrace) -> TestResult,
    );
}

fn prop_same_coordinate_clear_preserves_timer_oracle_inner(
    trace: ManagerOracleTrace,
) -> TestResult {
    let runtime = match Builder::new_current_thread().enable_all().build() {
        Ok(runtime) => runtime,
        Err(error) => return TestResult::error(format!("runtime build failed: {error}")),
    };

    runtime.block_on(async move {
        match run_same_coordinate_oracle_trace(&trace).await {
            Ok(()) => TestResult::passed(),
            Err(error) => TestResult::error(format!("{error:?} trace={trace:?}")),
        }
    })
}

async fn run_same_coordinate_oracle_trace(trace: &ManagerOracleTrace) -> Result<()> {
    time::pause();
    let (stream, mut manager, mut shutdown_tx) = setup_timer_manager().await?;
    task::spawn(stream.for_each(|_| async {}));
    let key = Key::from("prop-oracle-key");
    let timer_type = TimerType::VARIANTS[trace.timer_type % TimerType::VARIANTS.len()];
    let base_time = CompactDateTime::now()?.add_duration(CompactDuration::new(20))?;
    let alternate_time = base_time.add_duration(CompactDuration::new(20))?;
    let mut model = TimerOracleModel::default();
    let mut attempt = None;

    for op in trace.ops.iter().copied() {
        if matches!(op, ManagerOracleOp::Restart) {
            let store = manager.0.store.clone();
            shutdown_tx.send_replace(ShutdownPhase::Cancelling);
            task::yield_now().await;
            let (stream, restarted, restarted_shutdown) = setup_timer_manager_over(store).await?;
            task::spawn(stream.for_each(|_| async {}));
            manager = restarted;
            shutdown_tx = restarted_shutdown;
            model.queued = model.slab;
            model.registry = model.slab.map(|_| TimerState::Scheduled);
            attempt = None;
        } else {
            apply_oracle_op(
                &manager,
                &key,
                timer_type,
                (base_time, alternate_time),
                &mut model,
                &mut attempt,
                op,
            )
            .await?;
        }
        ensure!(
            manager
                .0
                .store
                .current_tag(&key, base_time, timer_type)
                .await?
                == model.key,
            "key tag differs from model after {op:?}"
        );
        ensure!(
            manager.timer_state(&key, base_time, timer_type).await == model.registry,
            "registry differs from model after {op:?}: {model:?}"
        );
        ensure!(
            !(model.unswept && model.slab.is_none()),
            "receipted source lost before its sweep after {op:?}: {model:?}"
        );
    }
    Ok(())
}

async fn apply_oracle_op(
    manager: &TimerManager<TableAdapter<InMemoryTriggerStore>>,
    key: &Key,
    timer_type: TimerType,
    (base_time, alternate_time): (CompactDateTime, CompactDateTime),
    model: &mut TimerOracleModel,
    attempt: &mut Option<Trigger>,
    op: ManagerOracleOp,
) -> Result<()> {
    use ManagerOracleOp as Op;
    match op {
        Op::Schedule | Op::RescheduleSame | Op::SameCoordinateClear => {
            let request = TimerRequest::new(key.clone(), base_time, timer_type, Span::current());
            if matches!(op, Op::SameCoordinateClear) {
                manager.clear_and_schedule(request).await?;
            } else {
                manager.schedule(request).await?;
            }
            let tag = manager
                .0
                .store
                .current_tag(key, base_time, timer_type)
                .await?
                .ok_or_else(|| eyre!("schedule wrote no key tag"))?;
            model.schedule(tag)?;
            let trigger =
                Trigger::with_tag(key.clone(), base_time, timer_type, tag, Span::current());
            wait_for_owned(manager, &trigger).await?;
        }
        Op::Fire => {
            if attempt.is_none() {
                *attempt = fire_oracle(manager, key, timer_type, base_time, model).await?;
            }
        }
        Op::Receipt | Op::Retire | Op::Commit | Op::Abort => {
            if let Some(trigger) = attempt.as_ref() {
                apply_attempt_op(manager, trigger, model, op).await?;
                if !matches!(op, Op::Receipt) {
                    *attempt = None;
                }
            }
        }
        Op::DifferentCoordinateClear => {
            manager
                .clear_and_schedule(TimerRequest::new(
                    key.clone(),
                    alternate_time,
                    timer_type,
                    Span::current(),
                ))
                .await?;
            model.clear();
        }
        Op::Unschedule => {
            manager.unschedule(key, base_time, timer_type).await?;
            model.unschedule();
        }
        Op::Restart => {}
    }
    Ok(())
}

async fn apply_attempt_op(
    manager: &TimerManager<TableAdapter<InMemoryTriggerStore>>,
    trigger: &Trigger,
    model: &mut TimerOracleModel,
    op: ManagerOracleOp,
) -> Result<()> {
    use ManagerOracleOp as Op;
    use TimerState::{Firing, FiringReplaced, FiringRescheduled, Parked, Scheduled};
    match op {
        Op::Receipt => manager.receipt(trigger).await?,
        Op::Retire => manager.retire(trigger).await?,
        Op::Commit => manager.complete(trigger).await?,
        Op::Abort => manager.abort(trigger).await,
        _ => return Ok(()),
    }
    match op {
        Op::Receipt => model.unswept = true,
        Op::Retire => model.unswept = false,
        _ => {}
    }
    match (op, model.registry) {
        (Op::Receipt | Op::Retire | Op::Commit, Some(FiringRescheduled)) => {
            let tag = manager
                .0
                .store
                .current_tag(&trigger.key, trigger.time, trigger.timer_type)
                .await?
                .ok_or_else(|| eyre!("rotation wrote no key tag"))?;
            ensure!(Some(tag) != model.key, "rotation kept the old key tag");
            model.key = Some(tag);
            model.queued = Some(tag);
            model.registry = Some(Scheduled);
        }
        (Op::Receipt, Some(Firing)) => {
            model.key = None;
            model.registry = Some(Parked);
        }
        (Op::Commit, Some(Firing | Parked | Scheduled) | None) => {
            *model = TimerOracleModel::default();
        }
        (Op::Retire, Some(Firing | FiringReplaced | Parked) | None)
        | (Op::Commit, Some(FiringReplaced)) => {
            model.slab = None;
            model.registry = None;
        }
        (Op::Abort, Some(FiringRescheduled)) => model.registry = Some(Scheduled),
        (Op::Abort, Some(Firing | FiringReplaced | Scheduled)) => {
            model.registry = Some(Parked);
            model.queued = None;
        }
        _ => {}
    }
    Ok(())
}

async fn fire_oracle(
    manager: &TimerManager<TableAdapter<InMemoryTriggerStore>>,
    key: &Key,
    timer_type: TimerType,
    time: CompactDateTime,
    model: &mut TimerOracleModel,
) -> Result<Option<Trigger>> {
    let coordinate = Trigger::new(key.clone(), time, timer_type, Span::current());
    let queued = manager.0.scheduler.take_from_queue(coordinate).await?;
    ensure!(
        queued.as_ref().map(|trigger| trigger.tag) == model.queued,
        "queued tag differs from model: actual={:?}, model={model:?}",
        queued.as_ref().map(|t| t.tag)
    );
    let fire = match queued.as_ref() {
        Some(trigger) => manager.fire(trigger).await?,
        None => None,
    };
    let expected = match (model.queued, model.key) {
        (None, _) => None,
        (Some(_), None) => Some(Fire::Committed),
        (Some(q), Some(k)) if q == k => Some(Fire::Live(k)),
        (Some(_), Some(k)) => Some(Fire::Unswept(k)),
    };
    ensure!(
        fire == expected,
        "fire disagreed with tags: expected={expected:?}, actual={fire:?}"
    );
    model.queued = None;
    match (fire, queued) {
        (Some(Fire::Live(tag) | Fire::Unswept(tag)), Some(mut trigger)) => {
            ensure!(
                !(model.unswept && fire == Some(Fire::Live(tag))),
                "live fire skipped an unswept receipt: {model:?}"
            );
            model.unswept = false;
            trigger.tag = tag;
            model.registry = Some(TimerState::Firing);
            Ok(Some(trigger))
        }
        (Some(Fire::Committed), Some(trigger)) => {
            manager.retire(&trigger).await?;
            model.unswept = false;
            model.slab = None;
            model.registry = None;
            Ok(None)
        }
        _ => Ok(None),
    }
}

#[derive(Clone, Debug)]
struct ManagerOracleTrace {
    timer_type: usize,
    ops: Vec<ManagerOracleOp>,
}

#[derive(Clone, Copy, Debug)]
enum ManagerOracleOp {
    Schedule,
    Fire,
    SameCoordinateClear,
    DifferentCoordinateClear,
    Unschedule,
    Commit,
    Abort,
    Receipt,
    Retire,
    Restart,
    RescheduleSame,
}

/// The model records row tags and registry state at one coordinate.
#[derive(Default, Debug)]
struct TimerOracleModel {
    slab: Option<i32>,
    key: Option<i32>,
    queued: Option<i32>,
    registry: Option<TimerState>,
    /// A receipt happened and no retire or sweep followed.
    unswept: bool,
}

impl TimerOracleModel {
    fn schedule(&mut self, tag: i32) -> Result<()> {
        use TimerState::{Firing, FiringReplaced, FiringRescheduled, Parked, Scheduled};
        if let Some(previous) = self.key {
            ensure!(tag == previous, "schedule changed the key tag");
        }
        self.key = Some(tag);
        match self.registry {
            None => {
                self.slab = Some(tag);
                self.queued = Some(tag);
                self.registry = Some(Scheduled);
            }
            Some(Parked) => {
                self.queued = Some(tag);
                self.registry = Some(Scheduled);
            }
            Some(Firing | FiringReplaced) => {
                self.queued = Some(tag);
                self.registry = Some(FiringRescheduled);
            }
            Some(Scheduled | FiringRescheduled) => {}
        }
        Ok(())
    }

    fn clear(&mut self) {
        use TimerState::{Firing, FiringReplaced, FiringRescheduled};
        match self.registry {
            Some(Firing | FiringRescheduled) => {
                self.registry = Some(FiringReplaced);
                self.queued = None;
            }
            Some(FiringReplaced) => {}
            _ if self.key.is_some() => self.cancel_queued(),
            _ => {}
        }
        self.key = None;
    }

    fn unschedule(&mut self) {
        use TimerState::{Firing, FiringReplaced, FiringRescheduled};
        match self.registry {
            Some(FiringRescheduled) => {
                self.queued = None;
                self.registry = Some(Firing);
            }
            Some(Firing | FiringReplaced) => {}
            _ => self.cancel_queued(),
        }
    }

    fn cancel_queued(&mut self) {
        match (self.queued, self.key) {
            (None, _) => *self = Self::default(),
            (Some(q), Some(k)) if q == k => *self = Self::default(),
            (Some(_), _) => self.key = None,
        }
    }
}

impl Arbitrary for ManagerOracleTrace {
    fn arbitrary(g: &mut Gen) -> Self {
        let len = usize::from(u8::arbitrary(g) % 24);
        let mut ops = Vec::with_capacity(len);
        for _ in 0..len {
            let op = if matches!(ops.last(), Some(ManagerOracleOp::Receipt)) {
                if bool::arbitrary(g) {
                    ManagerOracleOp::Retire
                } else {
                    ManagerOracleOp::Restart
                }
            } else {
                match u8::arbitrary(g) % 11 {
                    0 => ManagerOracleOp::Schedule,
                    1 => ManagerOracleOp::Fire,
                    2 => ManagerOracleOp::SameCoordinateClear,
                    3 => ManagerOracleOp::DifferentCoordinateClear,
                    4 => ManagerOracleOp::Commit,
                    5 | 7 => ManagerOracleOp::Abort,
                    6 => ManagerOracleOp::Receipt,
                    8 => ManagerOracleOp::Restart,
                    9 => ManagerOracleOp::Unschedule,
                    _ => ManagerOracleOp::RescheduleSame,
                }
            };
            ops.push(op);
        }
        Self {
            timer_type: usize::from(u8::arbitrary(g)),
            ops,
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        // Strictly shorter prefixes only, so shrinking always terminates.
        let ops = self.ops.clone();
        let timer_type = self.timer_type;
        Box::new((0..ops.len()).rev().map(move |len| Self {
            timer_type,
            ops: ops[..len].to_vec(),
        }))
    }
}

// =========================================================================
// prop_crud_scheduled_times_oracle: non-firing CRUD ops vs. a set model
// =========================================================================

/// Second offsets the CRUD trace draws from: the current instant, two short
/// offsets that collide across keys, and ~1 year out — the boundary values
/// production sends.
const CRUD_OFFSETS: [u32; 4] = [0, 60, 120, 86_400 * 365];

/// Invariant: for timers that never fire, `scheduled_times` reports exactly
/// the set a trivial model predicts — `schedule` inserts, `unschedule` and
/// `complete` remove, `unschedule_all` clears the key, and `abort` preserves
/// visibility (the row is kept for recovery). All five ops are idempotent
/// no-ops on absent coordinates. Parity is asserted for every key after
/// every op, over random interleavings on a small key/time pool.
#[test]
fn prop_crud_scheduled_times_oracle() {
    QuickCheck::new()
        .quickcheck(prop_crud_scheduled_times_oracle_inner as fn(CrudTrace) -> TestResult);
}

fn prop_crud_scheduled_times_oracle_inner(trace: CrudTrace) -> TestResult {
    let runtime = match Builder::new_current_thread().enable_all().build() {
        Ok(runtime) => runtime,
        Err(error) => return TestResult::error(format!("runtime build failed: {error}")),
    };

    runtime.block_on(async {
        match run_crud_trace(trace).await {
            Ok(()) => TestResult::passed(),
            Err(error) => TestResult::error(format!("{error:?}")),
        }
    })
}

async fn run_crud_trace(trace: CrudTrace) -> Result<()> {
    time::pause();

    let (_stream, manager, _shutdown_tx) = setup_timer_manager().await?;
    let timer_type = TimerType::Application;
    let now = CompactDateTime::now()?;
    let times = CRUD_OFFSETS
        .iter()
        .map(|&offset| now.add_duration(CompactDuration::new(offset)))
        .collect::<Result<Vec<_>, _>>()?;
    let keys: Vec<Key> = KEY_POOL.iter().copied().map(Key::from).collect();

    // Model: the set of times `scheduled_times` must report, per key.
    let mut model = vec![BTreeSet::new(); keys.len()];

    for (step, &(key_idx, time_idx, op)) in trace.0.iter().enumerate() {
        let key = &keys[key_idx];
        let time = times[time_idx];
        match op {
            CrudOp::Schedule => {
                manager
                    .schedule(TimerRequest::new(
                        key.clone(),
                        time,
                        timer_type,
                        Span::current(),
                    ))
                    .await?;
                model[key_idx].insert(time);
            }
            CrudOp::Unschedule => {
                manager.unschedule(key, time, timer_type).await?;
                model[key_idx].remove(&time);
            }
            CrudOp::UnscheduleAll => {
                manager.unschedule_all(key, timer_type).await?;
                model[key_idx].clear();
            }
            CrudOp::Complete => {
                let trigger = Trigger::new(key.clone(), time, timer_type, Span::current());
                manager.complete(&trigger).await?;
                model[key_idx].remove(&time);
            }
            // Abort never removes the durable row (it is preserved for
            // recovery), so `scheduled_times` visibility is unchanged.
            CrudOp::Abort => {
                let trigger = Trigger::new(key.clone(), time, timer_type, Span::current());
                manager.abort(&trigger).await;
            }
        }

        for (idx, key) in keys.iter().enumerate() {
            let actual: BTreeSet<CompactDateTime> = manager
                .scheduled_times(key, timer_type)
                .await?
                .into_iter()
                .collect();
            assert_eq!(
                actual, model[idx],
                "scheduled_times diverged from the CRUD model at step {step} ({op:?} on key \
                 {key_idx}, time {time_idx}) for key index {idx}"
            );
        }
    }

    Ok(())
}

/// A trace of `(key index, time index, op)` over the pools above.
#[derive(Clone, Debug)]
struct CrudTrace(Vec<(usize, usize, CrudOp)>);

#[derive(Clone, Copy, Debug)]
enum CrudOp {
    Schedule,
    Unschedule,
    UnscheduleAll,
    Complete,
    Abort,
}

impl Arbitrary for CrudTrace {
    fn arbitrary(g: &mut Gen) -> Self {
        let len = usize::from(u8::arbitrary(g) % 40);
        let ops = (0..len)
            .map(|_| {
                let key_idx = usize::from(u8::arbitrary(g)) % KEY_POOL.len();
                let time_idx = usize::from(u8::arbitrary(g)) % CRUD_OFFSETS.len();
                // Schedule is weighted at half so traces keep timers around
                // for the removal ops to hit.
                let op = match u8::arbitrary(g) % 8 {
                    0..=3 => CrudOp::Schedule,
                    4 => CrudOp::Unschedule,
                    5 => CrudOp::UnscheduleAll,
                    6 => CrudOp::Complete,
                    _ => CrudOp::Abort,
                };
                (key_idx, time_idx, op)
            })
            .collect();
        Self(ops)
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        // Strictly shorter prefixes only, so shrinking always terminates.
        let ops = self.0.clone();
        Box::new(
            (0..ops.len())
                .rev()
                .map(move |len| Self(ops[..len].to_vec())),
        )
    }
}
