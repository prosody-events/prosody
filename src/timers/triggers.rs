use crate::timers::active::ActiveTriggers;
use crate::timers::{TimerError, Trigger};
use ahash::HashMap;
use chrono::Utc;
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

    pub async fn insert(&mut self, trigger: Trigger) -> Result<(), TimerError> {
        let duration = trigger
            .time
            .signed_duration_since(Utc::now())
            .to_std()
            .map_err(TimerError::from)?;

        let queue_key = self.queue.insert(trigger.clone(), duration);
        self.queue_keys.insert(trigger.clone(), queue_key);
        self.active.insert(trigger).await;

        Ok(())
    }

    pub async fn next(&mut self) -> Option<Trigger> {
        let expired = poll_fn(|cx| self.queue.poll_expired(cx)).await?;
        Some(expired.into_inner())
    }

    pub async fn remove(&mut self, trigger: &Trigger) -> Result<(), TimerError> {
        let (trigger, queue_key) = self
            .queue_keys
            .remove_entry(trigger)
            .ok_or(TimerError::NotFound)?;

        self.queue.remove(&queue_key);
        self.active.remove(&trigger).await;

        Ok(())
    }
}
