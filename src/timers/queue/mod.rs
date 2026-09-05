//! A delay-based trigger queue for scheduling and expiring timer events.
//!
//! Provides [`TriggerQueue`], a wrapper around [`tokio_util::time::DelayQueue`]
//! that schedules [`Trigger`] events to fire at their specified times.
//! Maintains an [`ActiveTriggers`] registry for efficient membership checks
//! and prevents duplicate scheduling of the same trigger.

use crate::timers::Trigger;
use crate::timers::active::{ActiveTriggers, Item, TimerState};
use ahash::HashMap;
use std::collections::hash_map::Entry;
use std::{future::poll_fn, time::Duration};
use tokio_util::time::{DelayQueue, delay_queue};

/// A queue of [`Trigger`] values that fire after a configured delay.
///
/// Uses a [`DelayQueue<Trigger>`] to manage timers and a
/// [`HashMap<Trigger, delay_queue::Key>`] to avoid duplicate scheduling.
/// Updates an [`ActiveTriggers`] registry for fast membership checks.
pub struct TriggerQueue {
    queue: DelayQueue<Trigger>,
    queue_keys: HashMap<Trigger, delay_queue::Key>,
    active: ActiveTriggers,
}

/// What a queue effect kept in place.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Kept {
    /// The effect kept nothing back.
    Nothing,
    /// The queue kept a retained source whose tag differs from the key tag.
    Source,
}

impl TriggerQueue {
    /// Creates a new, empty [`TriggerQueue`].
    pub fn new() -> Self {
        Self {
            queue: DelayQueue::default(),
            queue_keys: HashMap::default(),
            active: ActiveTriggers::default(),
        }
    }

    /// Returns a reference to the registry of currently active triggers.
    ///
    /// The returned [`ActiveTriggers`] can be used to check membership of
    /// keys and times without affecting the queue.
    #[must_use]
    pub fn active_triggers(&self) -> &ActiveTriggers {
        &self.active
    }

    /// Inserts a [`Trigger`] into the queue for delayed firing.
    ///
    /// If the same [`Trigger`] (same key, time, and type) is already
    /// scheduled, the queued trigger adopts the new trigger's trace so that
    /// `onTimer` fires under the most recent caller's trace context.
    pub async fn insert(&mut self, trigger: Trigger) {
        if self.queue_keys.contains_key(&trigger) || self.active.queue(&trigger).await {
            self.enqueue(trigger);
        }
    }

    /// Waits for and returns the next expired [`Trigger`], if any.
    ///
    /// The pop has no await after removal, so a `select!` branch can cancel it
    /// safely. The caller must record delivery in the selected branch body.
    pub async fn next(&mut self) -> Option<Trigger> {
        let expired = poll_fn(|cx| self.queue.poll_expired(cx)).await?;
        self.queue_keys.remove(expired.get_ref());
        Some(expired.into_inner())
    }

    /// Remove a live schedule. `trigger.tag` is the key row tag; a queued item
    /// with another tag is a retained source and stays, including after
    /// delivery.
    pub(crate) async fn remove_if_live(&mut self, trigger: &Trigger) -> Kept {
        if let Some(queue_key) = self.queue_keys.get_mut(trigger) {
            let item = self.queue.remove(queue_key);
            if item.get_ref().tag != trigger.tag {
                let deadline = item.deadline();
                *queue_key = self.queue.insert_at(item.into_inner(), deadline);
                return Kept::Source;
            }
            self.queue_keys.remove(trigger);
        } else if matches!(
            self.active.get_state(&trigger.key, trigger.time, trigger.timer_type).await,
            Some(TimerState::Scheduled(Item::Delivered { tag })) if tag != trigger.tag
        ) {
            return Kept::Source;
        }
        self.active
            .remove(&trigger.key, trigger.time, trigger.timer_type)
            .await;
        Kept::Nothing
    }

    /// Adds a [`Trigger`] to the `DelayQueue` without modifying
    /// `ActiveTriggers`.
    ///
    /// Used for rescheduling: the caller has already set the state to
    /// `FiringRescheduled` and only needs the timer re-added to the queue.
    /// If the same [`Trigger`] is already in the queue, the queued trigger
    /// adopts the new trigger's trace.
    pub(crate) fn insert_queue_only(&mut self, trigger: Trigger) {
        self.enqueue(trigger);
    }

    /// Adds a trigger to the delay queue.
    ///
    /// If the trigger already exists (same key, time, and type), the queued
    /// trigger adopts the new trigger's trace so `onTimer` fires under the
    /// most recent caller's trace context.
    fn enqueue(&mut self, trigger: Trigger) {
        let vacant = match self.queue_keys.entry(trigger.clone()) {
            Entry::Occupied(occupied) => {
                occupied.key().adopt_trace_from(&trigger);
                return;
            }
            Entry::Vacant(vacant) => vacant,
        };

        let delay = trigger.time.duration_from_now().unwrap_or(Duration::ZERO);
        let queue_key = self.queue.insert(trigger, delay);
        vacant.insert(queue_key);
    }

    /// Change the item tag at its current location. Keep its deadline and
    /// trace.
    pub(crate) async fn retag(&mut self, trigger: &Trigger) {
        if let Some(queue_key) = self.queue_keys.get_mut(trigger) {
            let mut item = self.queue.remove(queue_key);
            let deadline = item.deadline();
            item.get_mut().tag = trigger.tag;
            *queue_key = self.queue.insert_at(item.into_inner(), deadline);
        } else {
            // The item can leave the queue before this command arrives.
            self.active.retag_delivered(trigger).await;
        }
    }

    /// Removes a [`Trigger`] from the `DelayQueue` without modifying
    /// `ActiveTriggers`.
    ///
    /// Used for canceling a reschedule: the caller transitions the state
    /// from `FiringRescheduled` back to `Firing` and only needs the timer
    /// removed from the queue.
    pub(crate) fn remove_queue_only(&mut self, trigger: &Trigger) -> Option<Trigger> {
        // Look up and remove the trigger's delay queue key.
        let queue_key = self.queue_keys.remove(trigger)?;

        // Remove from the delay queue only, not from ActiveTriggers.
        Some(self.queue.remove(&queue_key).into_inner())
    }
}

#[cfg(test)]
mod tests;
