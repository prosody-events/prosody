use super::*;
use tracing::Span;

fn create_test_store() -> MemoryTimerDeferStore {
    MemoryTimerDeferStore::new(SpanRelation::default())
}

fn test_trigger(key: &str, time_secs: u32) -> Trigger {
    let key: Key = Arc::from(key);
    let time = CompactDateTime::from(time_secs);
    Trigger::new(key, time, TimerType::Application, Span::current())
}

#[tokio::test]
async fn test_get_nonexistent_key() -> color_eyre::Result<()> {
    let store = create_test_store();
    let key: Key = Arc::from("test-key-1");

    let result = store.get_next_deferred_timer(&key).await?;
    assert!(result.is_none());
    Ok(())
}

#[tokio::test]
async fn test_defer_first_and_get() -> color_eyre::Result<()> {
    let store = create_test_store();
    let trigger = test_trigger("test-key-1", 1000);

    store.defer_first_timer(&trigger).await?;

    let result = store.get_next_deferred_timer(&trigger.key).await?;
    assert!(result.is_some());
    let (returned_trigger, retry_count) =
        result.ok_or_else(|| color_eyre::eyre::eyre!("expected trigger"))?;
    assert_eq!(returned_trigger.time, trigger.time);
    assert_eq!(returned_trigger.key, trigger.key);
    assert_eq!(retry_count, 0);
    Ok(())
}

#[tokio::test]
async fn test_multiple_timers_returns_oldest() -> color_eyre::Result<()> {
    let store = create_test_store();
    let key = "test-key-1";

    // Defer first timer at time 1000
    store.defer_first_timer(&test_trigger(key, 1000)).await?;
    // Append timer at time 500 (earlier)
    store.append_deferred_timer(&test_trigger(key, 500)).await?;
    // Append timer at time 1500 (later)
    store
        .append_deferred_timer(&test_trigger(key, 1500))
        .await?;

    let key: Key = Arc::from(key);
    let result = store.get_next_deferred_timer(&key).await?;
    assert!(result.is_some());
    let (trigger, retry_count) =
        result.ok_or_else(|| color_eyre::eyre::eyre!("expected trigger"))?;
    // Should return the oldest (time 500)
    assert_eq!(trigger.time, CompactDateTime::from(500_u32));
    assert_eq!(retry_count, 0);
    Ok(())
}

#[tokio::test]
async fn test_remove_timer() -> color_eyre::Result<()> {
    let store = create_test_store();
    let trigger = test_trigger("test-key-1", 1000);

    store.defer_first_timer(&trigger).await?;
    store
        .remove_deferred_timer(&trigger.key, trigger.time)
        .await?;

    let result = store.get_next_deferred_timer(&trigger.key).await?;
    assert!(result.is_none());
    Ok(())
}

#[tokio::test]
async fn test_remove_nonexistent() -> color_eyre::Result<()> {
    let store = create_test_store();
    let key: Key = Arc::from("test-key-1");

    // Should not error
    store
        .remove_deferred_timer(&key, CompactDateTime::from(42_u32))
        .await?;
    Ok(())
}

#[tokio::test]
async fn test_set_retry_count() -> color_eyre::Result<()> {
    let store = create_test_store();
    let trigger = test_trigger("test-key-1", 1000);

    store.defer_first_timer(&trigger).await?;
    store.set_retry_count(&trigger.key, 5).await?;

    let result = store.get_next_deferred_timer(&trigger.key).await?;
    assert!(result.is_some());
    let (_, retry_count) = result.ok_or_else(|| color_eyre::eyre::eyre!("expected trigger"))?;
    assert_eq!(retry_count, 5);
    Ok(())
}

#[tokio::test]
async fn test_is_deferred() -> color_eyre::Result<()> {
    let store = create_test_store();
    let trigger = test_trigger("test-key-1", 1000);

    // Not deferred initially
    assert!(store.is_deferred(&trigger.key).await?.is_none());

    // Defer and check
    store.defer_first_timer(&trigger).await?;
    assert_eq!(store.is_deferred(&trigger.key).await?, Some(0));

    // Update retry count and check
    store.set_retry_count(&trigger.key, 3).await?;
    assert_eq!(store.is_deferred(&trigger.key).await?, Some(3));

    Ok(())
}

#[tokio::test]
async fn test_complete_retry_success_more_timers() -> color_eyre::Result<()> {
    use super::super::TimerRetryCompletionResult;

    let store = create_test_store();
    let key = "test-key-1";

    // Defer two timers
    store.defer_first_timer(&test_trigger(key, 1000)).await?;
    store
        .defer_additional_timer(&test_trigger(key, 2000))
        .await?;
    store.set_retry_count(&Arc::from(key), 5).await?;

    // Complete first timer
    let key_arc: Key = Arc::from(key);
    let result = store
        .complete_retry_success(&key_arc, CompactDateTime::from(1000_u32))
        .await?;

    // Should return MoreTimers with next time and context
    assert!(matches!(
        result,
        TimerRetryCompletionResult::MoreTimers { next_time, .. } if next_time == CompactDateTime::from(2000_u32)
    ));

    // Retry count should be reset to 0
    let (_, retry_count) = store
        .get_next_deferred_timer(&key_arc)
        .await?
        .ok_or_else(|| color_eyre::eyre::eyre!("expected next timer"))?;
    assert_eq!(retry_count, 0);

    Ok(())
}

#[tokio::test]
async fn test_complete_retry_success_completed() -> color_eyre::Result<()> {
    use super::super::TimerRetryCompletionResult;

    let store = create_test_store();
    let trigger = test_trigger("test-key-1", 1000);

    store.defer_first_timer(&trigger).await?;

    let result = store
        .complete_retry_success(&trigger.key, trigger.time)
        .await?;

    assert!(matches!(result, TimerRetryCompletionResult::Completed));

    // Key should no longer be deferred
    assert!(store.is_deferred(&trigger.key).await?.is_none());

    Ok(())
}

#[tokio::test]
async fn test_increment_retry_count() -> color_eyre::Result<()> {
    let store = create_test_store();
    let trigger = test_trigger("test-key-1", 1000);

    store.defer_first_timer(&trigger).await?;

    let new_count = store.increment_retry_count(&trigger.key, 0).await?;
    assert_eq!(new_count, 1);

    let new_count = store.increment_retry_count(&trigger.key, 1).await?;
    assert_eq!(new_count, 2);

    let (_, retry_count) = store
        .get_next_deferred_timer(&trigger.key)
        .await?
        .ok_or_else(|| color_eyre::eyre::eyre!("expected trigger"))?;
    assert_eq!(retry_count, 2);

    Ok(())
}
