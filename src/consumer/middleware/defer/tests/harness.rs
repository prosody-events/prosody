//! Test harness for trace-based property testing.
//!
//! Provides [`TestHarness`] that executes traces against the defer middleware
//! and verification functions for checking invariants.

use super::context::TimerCapture;
use super::types::{MessageEvent, MessageOutcome, TimerEvent, TimerOutcome, TraceEvent};
use crate::consumer::middleware::defer::decider::TraceBasedDecider;
use crate::consumer::middleware::defer::store::DeferStore;
use crate::consumer::middleware::defer::store::key_ref::DeferKeyRef;
use crate::consumer::middleware::defer::store::memory::MemoryDeferStore;
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::{Key, Offset, Partition, Topic};
use color_eyre::eyre::eyre;
use std::sync::Arc;
use std::time::Duration;

// ============================================================================
// Verification Helpers (T032-T036)
// ============================================================================

/// Verifies timer coverage: every deferred key has an active timer.
pub async fn verify_timer_coverage(
    capture: &TimerCapture,
    store: &MemoryDeferStore,
    keys: &[Key],
    consumer_group: &str,
    topic: Topic,
    partition: Partition,
) -> color_eyre::Result<()> {
    for key in keys {
        let key_ref = DeferKeyRef::new(consumer_group, topic, partition, key);

        // Check if key is deferred in store
        let is_deferred = store
            .get_next_deferred_message(&key_ref)
            .await
            .map_err(|e| eyre!("store error: {e}"))?
            .is_some();

        // Check if timer is active
        let has_timer = capture.has_active_timer(key);

        if is_deferred && !has_timer {
            return Err(eyre!(
                "Timer coverage violation: key {:?} is deferred but has no active timer",
                key
            ));
        }

        if has_timer && !is_deferred {
            return Err(eyre!(
                "Timer coverage violation: key {:?} has timer but is not deferred",
                key
            ));
        }
    }
    Ok(())
}

// ============================================================================
// Test Harness (T059-T064)
// ============================================================================

/// Test harness for executing traces and verifying invariants.
///
/// Wraps the real defer middleware with controlled handler outcomes and
/// captured timer operations.
pub struct TestHarness {
    /// The decider for controlling deferral decisions.
    decider: TraceBasedDecider,
    /// Timer capture for verification.
    capture: TimerCapture,
    /// The underlying store (source of truth).
    store: MemoryDeferStore,
    /// Consumer group for key refs.
    consumer_group: Arc<str>,
    /// Topic for messages.
    topic: Topic,
    /// Partition for messages.
    partition: Partition,
    /// Key pool (generated from `key_count`).
    keys: Vec<Key>,
}

impl TestHarness {
    /// Creates a new test harness with the given key count.
    #[must_use]
    pub fn new(key_count: usize) -> Self {
        let keys: Vec<Key> = (0..key_count)
            .map(|i| Arc::from(format!("key-{i}")))
            .collect();

        Self {
            decider: TraceBasedDecider::new(),
            capture: TimerCapture::new(),
            store: MemoryDeferStore::new(),
            consumer_group: Arc::from("test-group"),
            topic: Topic::from("test-topic"),
            partition: Partition::from(0_i32),
            keys,
        }
    }

    /// Returns a reference to the timer capture for verification.
    #[must_use]
    pub fn capture(&self) -> &TimerCapture {
        &self.capture
    }

    /// Returns a reference to the store for verification.
    #[must_use]
    pub fn store(&self) -> &MemoryDeferStore {
        &self.store
    }

    /// Returns the key at the given index.
    #[must_use]
    pub fn key(&self, key_idx: usize) -> &Key {
        &self.keys[key_idx]
    }

    /// Creates a `DeferKeyRef` for the given key index.
    pub fn key_ref(&self, key_idx: usize) -> DeferKeyRef<'_> {
        DeferKeyRef::new(
            self.consumer_group.as_ref(),
            self.topic,
            self.partition,
            &self.keys[key_idx],
        )
    }

    /// Gets the retry count for a key from the store.
    pub async fn get_retry_count(&self, key_idx: usize) -> color_eyre::Result<Option<u32>> {
        let key_ref = self.key_ref(key_idx);
        self.store
            .is_deferred(&key_ref)
            .await
            .map_err(|e| eyre!("store error: {e}"))
    }

    /// Executes a message event.
    ///
    /// This simulates what `DeferHandler::on_message` does without requiring
    /// the full middleware stack.
    pub async fn execute_message(&mut self, event: &MessageEvent) -> color_eyre::Result<()> {
        let key = &self.keys[event.key_idx];
        let key_ref = DeferKeyRef::new(
            self.consumer_group.as_ref(),
            self.topic,
            self.partition,
            key,
        );

        // Check current state
        let is_deferred = self
            .store
            .get_next_deferred_message(&key_ref)
            .await
            .map_err(|e| eyre!("store error: {e}"))?
            .is_some();

        match &event.outcome {
            MessageOutcome::Queued => {
                // Key should already be deferred
                if !is_deferred {
                    return Err(eyre!("Invalid trace: Queued but key not deferred"));
                }

                // Add message to queue (no handler call)
                let retry_time = CompactDateTime::now()
                    .map_err(|e| eyre!("time error: {e}"))?
                    .add_duration(CompactDuration::new(60))
                    .map_err(|e| eyre!("time error: {e}"))?;

                self.store
                    .defer_additional_message(&key_ref, event.offset, retry_time)
                    .await
                    .map_err(|e| eyre!("store error: {e}"))?;
            }

            MessageOutcome::Success => {
                if is_deferred {
                    return Err(eyre!("Invalid trace: Success but key is deferred"));
                }
                // Handler succeeds, no deferral - nothing to do
            }

            MessageOutcome::Permanent => {
                if is_deferred {
                    return Err(eyre!("Invalid trace: Permanent but key is deferred"));
                }
                // Handler fails permanently - nothing to do
            }

            MessageOutcome::Transient { max_backoff, defer } => {
                if is_deferred {
                    return Err(eyre!("Invalid trace: Transient but key is deferred"));
                }

                // Set decider state
                self.decider.set_next(*defer);

                if *defer {
                    // Defer the message
                    let now = CompactDateTime::now().map_err(|e| eyre!("time error: {e}"))?;
                    let retry_time = now
                        .add_duration(CompactDuration::new(
                            max_backoff.as_secs().try_into().unwrap_or(60),
                        ))
                        .map_err(|e| eyre!("time error: {e}"))?;

                    self.store
                        .defer_first_message(&key_ref, event.offset, retry_time)
                        .await
                        .map_err(|e| eyre!("store error: {e}"))?;

                    // Record timer schedule
                    self.capture.record_schedule(key.clone(), retry_time);
                }
                // If defer=false, error propagates - nothing stored
            }
        }

        Ok(())
    }

    /// Executes a timer event.
    pub async fn execute_timer(&mut self, event: &TimerEvent) -> color_eyre::Result<()> {
        let key = &self.keys[event.key_idx];
        let key_ref = DeferKeyRef::new(
            self.consumer_group.as_ref(),
            self.topic,
            self.partition,
            key,
        );

        // Verify FIFO
        let Some((head_offset, retry_count)) = self
            .store
            .get_next_deferred_message(&key_ref)
            .await
            .map_err(|e| eyre!("store error: {e}"))?
        else {
            return Err(eyre!("Invalid trace: Timer but key not deferred"));
        };

        if head_offset != event.offset {
            return Err(eyre!(
                "FIFO violation: head {} != timer {}",
                head_offset,
                event.offset
            ));
        }

        match &event.outcome {
            TimerOutcome::Success | TimerOutcome::Permanent => {
                // Remove message from queue
                self.store
                    .remove_deferred_message(&key_ref, event.offset)
                    .await
                    .map_err(|e| eyre!("store error: {e}"))?;

                // Check if more messages in queue
                if self
                    .store
                    .get_next_deferred_message(&key_ref)
                    .await
                    .map_err(|e| eyre!("store error: {e}"))?
                    .is_none()
                {
                    // Queue empty - clear timer
                    self.capture.record_clear(key);
                } else {
                    // More messages - reschedule timer
                    let now = CompactDateTime::now().map_err(|e| eyre!("time error: {e}"))?;
                    let retry_time = now
                        .add_duration(CompactDuration::new(60))
                        .map_err(|e| eyre!("time error: {e}"))?;
                    self.capture.record_clear(key);
                    self.capture.record_schedule(key.clone(), retry_time);
                }
            }

            TimerOutcome::Transient { max_backoff } => {
                // Increment retry count
                self.store
                    .set_retry_count(&key_ref, retry_count + 1)
                    .await
                    .map_err(|e| eyre!("store error: {e}"))?;

                // Reschedule timer with backoff
                let now = CompactDateTime::now().map_err(|e| eyre!("time error: {e}"))?;
                let retry_time = now
                    .add_duration(CompactDuration::new(
                        max_backoff.as_secs().try_into().unwrap_or(60),
                    ))
                    .map_err(|e| eyre!("time error: {e}"))?;
                self.capture.record_clear(key);
                self.capture.record_schedule(key.clone(), retry_time);
            }
        }

        Ok(())
    }

    /// Executes a single trace event.
    pub async fn execute_event(&mut self, event: &TraceEvent) -> color_eyre::Result<()> {
        match event {
            TraceEvent::Message(msg) => self.execute_message(msg).await,
            TraceEvent::Timer(timer) => self.execute_timer(timer).await,
        }
    }

    /// Verifies all invariants after executing an event.
    pub async fn verify_invariants(&self) -> color_eyre::Result<()> {
        verify_timer_coverage(
            &self.capture,
            &self.store,
            &self.keys,
            &self.consumer_group,
            self.topic,
            self.partition,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tracing::init_test_logging;

    #[tokio::test]
    async fn harness_executes_simple_defer_sequence() {
        init_test_logging();

        let mut harness = TestHarness::new(1);

        // Message arrives and is deferred
        let msg = MessageEvent {
            key_idx: 0,
            offset: Offset::from(1_i64),
            outcome: MessageOutcome::Transient {
                max_backoff: Duration::from_secs(60),
                defer: true,
            },
        };
        harness.execute_message(&msg).await.ok();

        // Key should be deferred - create key_ref in scope
        {
            let key_ref = harness.key_ref(0);
            let deferred = harness.store.get_next_deferred_message(&key_ref).await;
            assert!(deferred.is_ok_and(|d| d.is_some()));
        };

        // Timer should be active
        let key = harness.key(0).clone();
        assert!(harness.capture.has_active_timer(&key));

        // Timer fires successfully
        let timer = TimerEvent {
            key_idx: 0,
            offset: Offset::from(1_i64),
            outcome: TimerOutcome::Success,
        };
        harness.execute_timer(&timer).await.ok();

        // Key should not be deferred
        {
            let key_ref = harness.key_ref(0);
            let deferred = harness.store.get_next_deferred_message(&key_ref).await;
            assert!(deferred.is_ok_and(|d| d.is_none()));
        };

        // Timer should be cleared
        assert!(!harness.capture.has_active_timer(&key));
    }

    #[tokio::test]
    async fn harness_queues_additional_messages() {
        init_test_logging();

        let mut harness = TestHarness::new(1);

        // First message defers
        let msg1 = MessageEvent {
            key_idx: 0,
            offset: Offset::from(1_i64),
            outcome: MessageOutcome::Transient {
                max_backoff: Duration::from_secs(60),
                defer: true,
            },
        };
        harness.execute_message(&msg1).await.ok();

        // Second message queues
        let msg2 = MessageEvent {
            key_idx: 0,
            offset: Offset::from(2_i64),
            outcome: MessageOutcome::Queued,
        };
        harness.execute_message(&msg2).await.ok();

        // Timer fires for first message
        let timer1 = TimerEvent {
            key_idx: 0,
            offset: Offset::from(1_i64),
            outcome: TimerOutcome::Success,
        };
        harness.execute_timer(&timer1).await.ok();

        // Key should still be deferred (has second message)
        {
            let key_ref = harness.key_ref(0);
            let deferred = harness.store.get_next_deferred_message(&key_ref).await;
            assert!(deferred.is_ok_and(|d| d.is_some()));
        };

        // Timer should still be active
        let key = harness.key(0).clone();
        assert!(harness.capture.has_active_timer(&key));
    }
}
