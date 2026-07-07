use super::*;
use crate::Key;
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::timers::{TimerType, Trigger};
use color_eyre::eyre::{Result, bail};
use tokio::task::coop::cooperative;
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
    let Some(expired_trigger) = cooperative(triggers.next()).await else {
        bail!("No expired trigger found");
    };
    assert_eq!(expired_trigger, trigger);

    Ok(())
}

#[tokio::test]
async fn test_remove_trigger() -> Result<()> {
    pause();

    let mut triggers = TriggerQueue::new();

    let key = Key::from("test-key");
    let time = CompactDateTime::now()?.add_duration(CompactDuration::new(5))?; // 5 seconds in the future
    let trigger = Trigger::for_testing(key.clone(), time, TimerType::Application);

    // Insert the trigger
    triggers.insert(trigger.clone()).await;

    // Remove the trigger
    triggers.remove(&trigger).await;

    // Verify the trigger is no longer active
    assert!(
        !triggers
            .active_triggers()
            .contains(&key, time, TimerType::Application)
            .await
    );

    // Advance time by 5 seconds to simulate the trigger's original expiration time
    advance(Duration::from_secs(5)).await;

    // Ensure that no trigger is emitted
    assert!(triggers.next().await.is_none());

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
async fn test_active_triggers() -> Result<()> {
    pause();

    let mut triggers = TriggerQueue::new();

    let key = Key::from("active-key");
    let time = CompactDateTime::now()?.add_duration(CompactDuration::new(5))?; // 5 seconds in the future
    let trigger = Trigger::for_testing(key.clone(), time, TimerType::Application);

    // Insert the trigger
    triggers.insert(trigger.clone()).await;

    // Verify the trigger is active
    assert!(
        triggers
            .active_triggers()
            .contains(&key, time, TimerType::Application)
            .await
    );

    // Remove the trigger
    triggers.remove(&trigger).await;

    // Verify the trigger is no longer active
    assert!(
        !triggers
            .active_triggers()
            .contains(&key, time, TimerType::Application)
            .await
    );

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
    let Some(expired) = cooperative(triggers.next()).await else {
        bail!("No expired trigger found");
    };

    // The expired trigger should carry span_b's identity
    assert_eq!(expired.span().id(), span_b.id());

    Ok(())
}
