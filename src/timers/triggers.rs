use crate::timers::Trigger;
use crate::timers::active::ActiveTriggers;
use crate::timers::scheduler::TimerSchedulerError;
use ahash::HashMap;
use std::future::poll_fn;
use tokio_util::time::{DelayQueue, delay_queue};

pub struct Triggers {
    queue: DelayQueue<Trigger>,
    queue_keys: HashMap<Trigger, delay_queue::Key>,
    active: ActiveTriggers,
}

impl Triggers {
    pub fn new() -> Self {
        Self {
            queue: DelayQueue::default(),
            queue_keys: HashMap::default(),
            active: ActiveTriggers::default(),
        }
    }

    pub fn active(&self) -> &ActiveTriggers {
        &self.active
    }

    pub async fn insert(&mut self, trigger: Trigger) -> Result<(), TimerSchedulerError> {
        let duration = trigger.time.duration_from_now()?;
        let queue_key = self.queue.insert(trigger.clone(), duration);
        self.queue_keys.insert(trigger.clone(), queue_key);
        self.active.insert(trigger).await;

        Ok(())
    }

    pub async fn next(&mut self) -> Option<Trigger> {
        let expired = poll_fn(|cx| self.queue.poll_expired(cx)).await?;
        Some(expired.into_inner())
    }

    pub async fn remove(&mut self, trigger: &Trigger) -> Result<(), TimerSchedulerError> {
        let (trigger, queue_key) = self
            .queue_keys
            .remove_entry(trigger)
            .ok_or(TimerSchedulerError::NotFound)?;

        self.queue.remove(&queue_key);
        self.active.remove(&trigger.key, trigger.time).await;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Key;
    use crate::timers::datetime::CompactDateTime;
    use crate::timers::duration::CompactDuration;
    use tokio::time::{Duration, advance, pause};
    use tracing::Span;

    #[tokio::test]
    async fn test_insert_and_next() -> Result<(), TimerSchedulerError> {
        pause();

        let mut triggers = Triggers::new();

        let key = Key::from("test-key");
        let time = CompactDateTime::now()
            .map_err(TimerSchedulerError::DateTime)?
            .add_duration(CompactDuration::new(1))
            .map_err(TimerSchedulerError::DateTime)?; // 1 second in the future
        let trigger = Trigger {
            key: key.clone(),
            time,
            span: Span::current(),
        };

        // Insert the trigger
        triggers.insert(trigger.clone()).await?;

        // Advance time by 1 second to simulate the trigger expiring
        advance(Duration::from_secs(1)).await;

        // Retrieve the next expired trigger
        if let Some(expired_trigger) = triggers.next().await {
            assert_eq!(expired_trigger, trigger);
        } else {
            return Err(TimerSchedulerError::NotFound);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_remove_trigger() -> Result<(), TimerSchedulerError> {
        tokio::time::pause();

        let mut triggers = Triggers::new();

        let key = Key::from("test-key");
        let time = CompactDateTime::now()
            .map_err(TimerSchedulerError::DateTime)?
            .add_duration(CompactDuration::new(5))
            .map_err(TimerSchedulerError::DateTime)?; // 5 seconds in the future
        let trigger = Trigger {
            key: key.clone(),
            time,
            span: Span::current(),
        };

        // Insert the trigger
        triggers.insert(trigger.clone()).await?;

        // Remove the trigger
        triggers.remove(&trigger).await?;

        // Verify the trigger is no longer active
        assert!(!triggers.active().contains(&key, time).await);

        // Advance time by 5 seconds to simulate the trigger's original expiration time
        tokio::time::advance(Duration::from_secs(5)).await;

        // Ensure that no trigger is emitted
        assert!(triggers.next().await.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_remove_nonexistent_trigger() {
        pause();

        let mut triggers = Triggers::new();

        let key = Key::from("nonexistent-key");
        let time = CompactDateTime::now()
            .and_then(|t| t.add_duration(CompactDuration::new(5)))
            .unwrap_or(CompactDateTime::MIN); // Default to MIN if there's an error
        let trigger = Trigger {
            key,
            time,
            span: Span::current(),
        };

        // Attempt to remove a nonexistent trigger
        let result = triggers.remove(&trigger).await;

        assert!(matches!(result, Err(TimerSchedulerError::NotFound)));
    }

    #[tokio::test]
    async fn test_multiple_triggers() -> Result<(), TimerSchedulerError> {
        pause();

        let mut triggers = Triggers::new();

        let key_first = Key::from("key1");
        let time_first = CompactDateTime::now()
            .map_err(TimerSchedulerError::DateTime)?
            .add_duration(CompactDuration::new(1))
            .map_err(TimerSchedulerError::DateTime)?; // 1 second in the future
        let trigger_first = Trigger {
            key: key_first.clone(),
            time: time_first,
            span: Span::current(),
        };

        let key_second = Key::from("key2");
        let time_second = CompactDateTime::now()
            .map_err(TimerSchedulerError::DateTime)?
            .add_duration(CompactDuration::new(2))
            .map_err(TimerSchedulerError::DateTime)?; // 2 seconds in the future
        let trigger_second = Trigger {
            key: key_second.clone(),
            time: time_second,
            span: Span::current(),
        };

        // Insert both triggers
        triggers.insert(trigger_first.clone()).await?;
        triggers.insert(trigger_second.clone()).await?;

        // Advance time by 1 second to simulate the first trigger expiring
        advance(Duration::from_secs(1)).await;

        // Retrieve the next expired trigger
        if let Some(expired_trigger) = triggers.next().await {
            assert_eq!(expired_trigger, trigger_first);
        } else {
            return Err(TimerSchedulerError::NotFound);
        }

        // Advance time by another 1 second to simulate the second trigger expiring
        advance(Duration::from_secs(1)).await;

        // Retrieve the next expired trigger
        if let Some(expired_trigger) = triggers.next().await {
            assert_eq!(expired_trigger, trigger_second);
        } else {
            return Err(TimerSchedulerError::NotFound);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_active_triggers() -> Result<(), TimerSchedulerError> {
        pause();

        let mut triggers = Triggers::new();

        let key = Key::from("active-key");
        let time = CompactDateTime::now()
            .map_err(TimerSchedulerError::DateTime)?
            .add_duration(CompactDuration::new(5))
            .map_err(TimerSchedulerError::DateTime)?; // 5 seconds in the future
        let trigger = Trigger {
            key: key.clone(),
            time,
            span: Span::current(),
        };

        // Insert the trigger
        triggers.insert(trigger.clone()).await?;

        // Verify the trigger is active
        assert!(triggers.active().contains(&key, time).await);

        // Remove the trigger
        triggers.remove(&trigger).await?;

        // Verify the trigger is no longer active
        assert!(!triggers.active().contains(&key, time).await);

        Ok(())
    }
}
