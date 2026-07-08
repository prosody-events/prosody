use super::*;
use crate::Key;
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::timers::{TimerType, Trigger};
use color_eyre::eyre::{Result, bail};
use tokio::time::{Duration, advance, pause};

#[tokio::test]
async fn test_insert_and_next() -> Result<()> {
    pause();

    let mut triggers = TriggerQueue::new();

    let key = Key::from("test-key");
    let time = CompactDateTime::now()?.add_duration(CompactDuration::new(1))?; // 1 second in the future
    let trigger = Trigger::for_testing(key.clone(), time, TimerType::Application);

    // Insert the trigger
    triggers.insert(trigger.clone()).await;

    // Advance time by 1 second to simulate the trigger expiring
    advance(Duration::from_secs(1)).await;

    // Retrieve the next expired trigger
    let Some(expired_trigger) = triggers.next().await else {
        bail!("No expired trigger found");
    };
    assert_eq!(expired_trigger, trigger);

    Ok(())
}

#[tokio::test]
async fn test_remove_clears_active_registry() -> Result<()> {
    pause();

    let mut triggers = TriggerQueue::new();

    let key = Key::from("test-key");
    let time = CompactDateTime::now()?.add_duration(CompactDuration::new(5))?; // 5 seconds in the future
    let trigger = Trigger::for_testing(key.clone(), time, TimerType::Application);

    // Insert the trigger; it is active immediately.
    triggers.insert(trigger.clone()).await;
    assert!(
        triggers
            .active_triggers()
            .contains(&key, time, TimerType::Application)
            .await
    );

    // Case 1: remove while still queued.
    triggers.remove(&trigger).await;
    assert!(
        !triggers
            .active_triggers()
            .contains(&key, time, TimerType::Application)
            .await
    );

    // Advance past the original expiration time; nothing fires.
    advance(Duration::from_secs(5)).await;
    assert!(triggers.next().await.is_none());

    // Re-insert, then let it fire via `next()` — this pops the delay-queue
    // entry (`queue_keys`) but leaves the `ActiveTriggers` entry Scheduled,
    // reproducing the "delivered but not yet transitioned to Firing" case
    // `remove`'s doc names as case 2.
    let time = CompactDateTime::now()?.add_duration(CompactDuration::new(5))?;
    let trigger = Trigger::for_testing(key.clone(), time, TimerType::Application);
    triggers.insert(trigger.clone()).await;
    advance(Duration::from_secs(5)).await;
    let Some(delivered) = triggers.next().await else {
        bail!("No expired trigger found");
    };
    assert_eq!(delivered, trigger);
    assert!(
        triggers
            .active_triggers()
            .contains(&key, time, TimerType::Application)
            .await
    );

    // Case 2: remove after delivery — clears ActiveTriggers without
    // panicking on the already-absent queue key.
    triggers.remove(&trigger).await;
    assert!(
        !triggers
            .active_triggers()
            .contains(&key, time, TimerType::Application)
            .await
    );

    // Idempotent: removing an already-removed trigger is a no-op.
    triggers.remove(&trigger).await;
    assert!(
        !triggers
            .active_triggers()
            .contains(&key, time, TimerType::Application)
            .await
    );

    Ok(())
}

#[tokio::test]
async fn test_multiple_triggers() -> Result<()> {
    pause();

    let mut triggers = TriggerQueue::new();

    let key_first = Key::from("key1");
    let time_first = CompactDateTime::now()?.add_duration(CompactDuration::new(1))?; // 1 second in the future
    let trigger_first = Trigger::for_testing(key_first.clone(), time_first, TimerType::Application);

    let key_second = Key::from("key2");
    let time_second = CompactDateTime::now()?.add_duration(CompactDuration::new(2))?; // 2 seconds in the future
    let trigger_second =
        Trigger::for_testing(key_second.clone(), time_second, TimerType::Application);

    // Insert both triggers
    triggers.insert(trigger_first.clone()).await;
    triggers.insert(trigger_second.clone()).await;

    // Advance time by 1 second to simulate the first trigger expiring
    advance(Duration::from_secs(1)).await;

    // Retrieve the next expired trigger
    let expired_trigger = triggers
        .next()
        .await
        .ok_or_else(|| color_eyre::eyre::eyre!("No expired trigger found"))?;
    assert_eq!(expired_trigger, trigger_first);

    // Advance time by another 1 second to simulate the second trigger expiring
    advance(Duration::from_secs(1)).await;

    // Retrieve the next expired trigger
    let expired_trigger = triggers
        .next()
        .await
        .ok_or_else(|| color_eyre::eyre::eyre!("No expired trigger found"))?;
    assert_eq!(expired_trigger, trigger_second);

    Ok(())
}

#[tokio::test]
async fn test_insert_duplicate_refreshes_span() -> Result<()> {
    pause();

    let mut triggers = TriggerQueue::new();

    let key = Key::from("dedup-key");
    let time = CompactDateTime::now()?.add_duration(CompactDuration::new(5))?;

    let span_a = tracing::info_span!("span_a");
    let span_b = tracing::info_span!("span_b");

    let trigger_a = Trigger::new(key.clone(), time, TimerType::Application, span_a);
    let trigger_b = Trigger::new(key.clone(), time, TimerType::Application, span_b.clone());

    // Insert first trigger with span_a
    triggers.insert(trigger_a).await;

    // Insert duplicate with span_b — should refresh span
    triggers.insert(trigger_b).await;

    // Expire and pop the trigger
    advance(Duration::from_secs(5)).await;
    let Some(expired) = triggers.next().await else {
        bail!("No expired trigger found");
    };

    // The expired trigger should carry span_b's identity
    assert_eq!(expired.span().id(), span_b.id());

    Ok(())
}
