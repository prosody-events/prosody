//! A delay-based trigger queue for scheduling and expiring timer events.
//!
//! Provides [`TriggerQueue`], a wrapper around [`tokio_util::time::DelayQueue`]
//! that schedules [`Trigger`] events to fire at their specified times.
//! Maintains an [`ActiveTriggers`] registry for efficient membership checks
//! and prevents duplicate scheduling of the same trigger.

use crate::timers::Trigger;
use crate::timers::active::ActiveTriggers;
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
        if self.enqueue(trigger.clone()) {
            self.active.insert(trigger).await;
        }
    }

    /// Waits for and returns the next expired [`Trigger`], if any.
    ///
    /// Asynchronously polls the internal delay queue for the next item whose
    /// delay has elapsed. Once retrieved, the trigger is removed from the
    /// `queue_keys` map. Returns `None` if the underlying queue has been
    /// closed.
    pub async fn next(&mut self) -> Option<Trigger> {
        // Poll for the next expired item.
        let expired = poll_fn(|cx| self.queue.poll_expired(cx)).await?;
        // Remove its scheduling key to allow potential rescheduling.
        self.queue_keys.remove(expired.get_ref());
        Some(expired.into_inner())
    }

    /// Removes a [`Trigger`] from both the delay queue and the active registry.
    ///
    /// Handles two cases:
    /// 1. Timer is still in the delay queue: removes from both queue and
    ///    [`ActiveTriggers`]
    /// 2. Timer was delivered but not yet fired: removes from
    ///    [`ActiveTriggers`] only (queue removal is a no-op)
    ///
    /// This is idempotent: calling remove on an already-removed trigger has no
    /// effect.
    pub async fn remove(&mut self, trigger: &Trigger) {
        // Remove from delay queue if present (may have already been delivered).
        if let Some(queue_key) = self.queue_keys.remove(trigger) {
            self.queue.remove(&queue_key);
        }

        // Always remove from ActiveTriggers. This handles the case where the
        // timer was delivered from the queue (removed from queue_keys) but not
        // yet transitioned to Firing via fire(). The remove is idempotent.
        self.active
            .remove(&trigger.key, trigger.time, trigger.timer_type)
            .await;
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

    /// Adds a trigger to the delay queue, returning `true` if newly inserted.
    ///
    /// If the trigger already exists (same key, time, and type), the queued
    /// trigger adopts the new trigger's trace so `onTimer` fires under the
    /// most recent caller's trace context, and returns `false`.
    fn enqueue(&mut self, trigger: Trigger) -> bool {
        let vacant = match self.queue_keys.entry(trigger.clone()) {
            Entry::Occupied(occupied) => {
                occupied.key().adopt_trace_from(&trigger);
                return false;
            }
            Entry::Vacant(vacant) => vacant,
        };

        let delay = trigger.time.duration_from_now().unwrap_or(Duration::ZERO);
        let queue_key = self.queue.insert(trigger, delay);
        vacant.insert(queue_key);
        true
    }

    /// Removes a [`Trigger`] from the `DelayQueue` without modifying
    /// `ActiveTriggers`.
    ///
    /// Used for canceling a reschedule: the caller transitions the state
    /// from `FiringRescheduled` back to `Firing` and only needs the timer
    /// removed from the queue.
    pub(crate) fn remove_queue_only(&mut self, trigger: &Trigger) {
        // Look up and remove the trigger's delay queue key.
        let Some(queue_key) = self.queue_keys.remove(trigger) else {
            return;
        };

        // Remove from the delay queue only, not from ActiveTriggers.
        self.queue.remove(&queue_key);
    }
}

#[cfg(test)]
mod tests;
