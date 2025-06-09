//! A delay-based trigger queue for scheduling and expiring timer events.
//!
//! This module provides `TriggerQueue`, a wrapper around
//! `tokio_util::time::DelayQueue` that schedules `Trigger` events to fire at
//! their specified times. It maintains an `ActiveTriggers` registry for
//! efficient membership checks and prevents duplicate scheduling of the same
//! trigger.

use crate::timers::Trigger;
use crate::timers::active::ActiveTriggers;
use ahash::HashMap;
use std::collections::hash_map::Entry;
use std::{future::poll_fn, time::Duration};
use tokio_util::time::{DelayQueue, delay_queue};

/// A queue of `Trigger` values that fire after a configured delay.
///
/// Internally, `TriggerQueue` uses a `DelayQueue<Trigger>` to manage timers
/// and a `HashMap<Trigger, delay_queue::Key>` to avoid duplicate scheduling.
/// It also updates an `ActiveTriggers` registry to allow fast membership
/// checks.
pub struct TriggerQueue {
    queue: DelayQueue<Trigger>,
    queue_keys: HashMap<Trigger, delay_queue::Key>,
    active: ActiveTriggers,
}

impl TriggerQueue {
    /// Creates a new, empty `TriggerQueue`.
    pub fn new() -> Self {
        Self {
            queue: DelayQueue::default(),
            queue_keys: HashMap::default(),
            active: ActiveTriggers::default(),
        }
    }

    /// Returns a reference to the registry of currently active triggers.
    ///
    /// The returned `ActiveTriggers` can be used to check membership of keys
    /// and times without affecting the queue.
    #[must_use]
    pub fn active_triggers(&self) -> &ActiveTriggers {
        &self.active
    }

    /// Inserts a `Trigger` into the queue for delayed firing.
    ///
    /// If the same `Trigger` (same key and time) is already scheduled, this
    /// call has no effect.
    ///
    /// # Arguments
    ///
    /// * `trigger` - The timer event to schedule.
    ///
    /// # Panics
    ///
    /// This method does not panic under normal operation.
    pub async fn insert(&mut self, trigger: Trigger) {
        // Skip scheduling if the trigger is already present.
        let Entry::Vacant(vacant) = self.queue_keys.entry(trigger.clone()) else {
            return;
        };

        // Compute delay until trigger time, defaulting to zero if in the past.
        let delay = trigger.time.duration_from_now().unwrap_or(Duration::ZERO);

        // Schedule the trigger and record its key in the map.
        let queue_key = self.queue.insert(trigger.clone(), delay);
        vacant.insert(queue_key);

        // Mark the trigger as active for fast membership checks.
        self.active.insert(trigger).await;
    }

    /// Waits for and returns the next expired `Trigger`, if any.
    ///
    /// This method asynchronously polls the internal delay queue for the
    /// next item whose delay has elapsed. Once retrieved, the trigger is
    /// removed from the `queue_keys` map.
    ///
    /// # Returns
    ///
    /// * `Some(Trigger)` when a scheduled trigger expires.
    /// * `None` if the underlying queue has been closed.
    pub async fn next(&mut self) -> Option<Trigger> {
        // Poll for the next expired item.
        let expired = poll_fn(|cx| self.queue.poll_expired(cx)).await?;
        // Remove its scheduling key to allow potential rescheduling.
        self.queue_keys.remove(expired.get_ref());
        Some(expired.into_inner())
    }

    /// Removes a scheduled `Trigger` from both the delay queue and the
    /// active registry.
    ///
    /// If the trigger was not previously scheduled, this call has no effect.
    ///
    /// # Arguments
    ///
    /// * `trigger` - The trigger to remove.
    ///
    /// # Panics
    ///
    /// This method does not panic under normal operation.
    pub async fn remove(&mut self, trigger: &Trigger) {
        // Look up and remove the trigger's delay queue key.
        let Some((owned_trigger, queue_key)) = self.queue_keys.remove_entry(trigger) else {
            return;
        };

        // Remove from the delay queue and deactivate in the registry.
        self.queue.remove(&queue_key);
        self.active
            .remove(&owned_trigger.key, owned_trigger.time)
            .await;
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::Key;
    use crate::timers::datetime::CompactDateTime;
    use crate::timers::duration::CompactDuration;
    use crate::timers::scheduler::TimerSchedulerError;
    use tokio::time::{Duration, advance, pause};
    use tracing::Span;

    #[tokio::test]
    async fn test_insert_and_next() -> Result<(), TimerSchedulerError> {
        pause();

        let mut triggers = TriggerQueue::new();

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
        triggers.insert(trigger.clone()).await;

        // Advance time by 1 second to simulate the trigger expiring
        advance(Duration::from_secs(1)).await;

        // Retrieve the next expired trigger
        let expired_trigger = triggers.next().await.expect("No expired trigger found");
        assert_eq!(expired_trigger, trigger);

        Ok(())
    }

    #[tokio::test]
    async fn test_remove_trigger() -> Result<(), TimerSchedulerError> {
        tokio::time::pause();

        let mut triggers = TriggerQueue::new();

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
        triggers.insert(trigger.clone()).await;

        // Remove the trigger
        triggers.remove(&trigger).await;

        // Verify the trigger is no longer active
        assert!(!triggers.active_triggers().contains(&key, time).await);

        // Advance time by 5 seconds to simulate the trigger's original expiration time
        tokio::time::advance(Duration::from_secs(5)).await;

        // Ensure that no trigger is emitted
        assert!(triggers.next().await.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_multiple_triggers() -> Result<(), TimerSchedulerError> {
        pause();

        let mut triggers = TriggerQueue::new();

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
        triggers.insert(trigger_first.clone()).await;
        triggers.insert(trigger_second.clone()).await;

        // Advance time by 1 second to simulate the first trigger expiring
        advance(Duration::from_secs(1)).await;

        // Retrieve the next expired trigger
        let expired_trigger = triggers.next().await.expect("No expired trigger found");
        assert_eq!(expired_trigger, trigger_first);

        // Advance time by another 1 second to simulate the second trigger expiring
        advance(Duration::from_secs(1)).await;

        // Retrieve the next expired trigger
        let expired_trigger = triggers.next().await.expect("No expired trigger found");
        assert_eq!(expired_trigger, trigger_second);

        Ok(())
    }

    #[tokio::test]
    async fn test_active_triggers() -> Result<(), TimerSchedulerError> {
        pause();

        let mut triggers = TriggerQueue::new();

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
        triggers.insert(trigger.clone()).await;

        // Verify the trigger is active
        assert!(triggers.active_triggers().contains(&key, time).await);

        // Remove the trigger
        triggers.remove(&trigger).await;

        // Verify the trigger is no longer active
        assert!(!triggers.active_triggers().contains(&key, time).await);

        Ok(())
    }
}
