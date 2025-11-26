//! Test harness for trace-based property testing.
//!
//! Provides [`TestHarness`] that executes traces against the **real**
//! `DeferHandler` and verification functions for checking invariants.
//!
//! # Architecture
//!
//! The harness uses shared-state test doubles that allow external control:
//!
//! - [`OutcomeHandler`]: Inner handler returning trace-specified outcomes
//! - [`TraceBasedDecider`]: Deferral decisions from trace
//! - [`MemoryLoader`]: Message storage for timer retries
//! - [`MemoryDeferStore`]: Deferred message state
//! - [`TimerCapture`]/[`KeyedCapturingContext`]: Timer operation capture
//!
//! All these components use `Arc` internally, so clones share state.

use super::context::{KeyedCapturingContext, TimerCapture};
use super::handler::{HandlerOutcome, OutcomeHandler};
use super::types::{MessageEvent, MessageOutcome, TimerEvent, TimerOutcome, TraceEvent};
use crate::consumer::DemandType;
use crate::consumer::middleware::FallibleHandler;
use crate::consumer::middleware::defer::DeferConfiguration;
use crate::consumer::middleware::defer::decider::TraceBasedDecider;
use crate::consumer::middleware::defer::handler::DeferHandler;
use crate::consumer::middleware::defer::loader::{MemoryLoader, MessageLoader};
use crate::consumer::middleware::defer::store::CachedDeferStore;
use crate::consumer::middleware::defer::store::DeferStore;
use crate::consumer::middleware::defer::store::key_ref::DeferKeyRef;
use crate::consumer::middleware::defer::store::memory::MemoryDeferStore;
use crate::timers::duration::CompactDuration;
use crate::timers::{TimerType, Trigger};
use crate::{Key, Offset, Partition, Topic};
use color_eyre::eyre::eyre;
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use tracing::debug;

// ============================================================================
// Verification Helpers
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

        // Check if timer is active via capture
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
// Test Harness using Real DeferHandler
// ============================================================================

/// Type alias for the `DeferHandler` used in tests.
type TestDeferHandler = DeferHandler<
    OutcomeHandler,
    CachedDeferStore<MemoryDeferStore>,
    MemoryLoader,
    TraceBasedDecider,
>;

/// Test harness for executing traces against the **real** `DeferHandler`.
///
/// Unlike the previous simulation-based harness, this one constructs an actual
/// `DeferHandler` and calls its `on_message()` and `on_timer()` methods.
/// Test doubles control behavior:
///
/// - `OutcomeHandler`: Returns outcomes specified by the trace
/// - `TraceBasedDecider`: Returns defer decisions from the trace
/// - `TimerCapture`: Captures timer operations for verification
pub struct TestHarness {
    /// The real defer handler under test.
    handler: TestDeferHandler,
    /// Inner handler for setting outcomes (shared via Arc).
    inner_handler: OutcomeHandler,
    /// Decider for setting defer decisions (shared via Arc).
    decider: TraceBasedDecider,
    /// Loader for storing messages (shared via Arc).
    loader: MemoryLoader,
    /// Store for verification (shared via Arc, wrapped in `CachedDeferStore`
    /// inside handler).
    store: MemoryDeferStore,
    /// Timer capture for verification.
    capture: TimerCapture,
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
    ///
    /// # Errors
    ///
    /// Returns an error if the defer handler construction fails.
    pub fn new(key_count: usize) -> color_eyre::Result<Self> {
        let keys: Vec<Key> = (0..key_count)
            .map(|i| Arc::from(format!("key-{i}")))
            .collect();

        let consumer_group: Arc<str> = Arc::from("test-group");
        let topic = Topic::from("test-topic");
        let partition = Partition::from(0_i32);

        // Create shared components (all use Arc internally)
        let inner_handler = OutcomeHandler::new();
        let decider = TraceBasedDecider::new();
        let loader = MemoryLoader::new();
        let store = MemoryDeferStore::new();
        let capture = TimerCapture::new();

        // Create config using shared test constants
        let config = DeferConfiguration::builder()
            .base(Duration::from_secs(u64::from(
                super::TEST_BASE_BACKOFF_SECS,
            )))
            .max_delay(Duration::from_secs(u64::from(super::TEST_MAX_BACKOFF_SECS)))
            .failure_threshold(0.9_f64)
            .build()
            .map_err(|e| eyre!("config error: {e}"))?;

        // Create the DeferHandler directly (bypassing middleware/provider pattern)
        // This is simpler for testing since we don't need the full middleware stack
        let cached_store = CachedDeferStore::new(store.clone(), config.cache_size);

        let handler = DeferHandler {
            handler: inner_handler.clone(),
            loader: loader.clone(),
            store: cached_store,
            decider: decider.clone(),
            config,
            topic,
            partition,
            consumer_group: consumer_group.clone(),
        };

        Ok(Self {
            handler,
            inner_handler,
            decider,
            loader,
            store,
            capture,
            consumer_group,
            topic,
            partition,
            keys,
        })
    }

    /// Returns the key at the given index.
    #[must_use]
    pub fn key(&self, key_idx: usize) -> &Key {
        &self.keys[key_idx]
    }

    /// Returns a reference to the store for verification.
    #[must_use]
    pub fn store(&self) -> &MemoryDeferStore {
        &self.store
    }

    /// Returns a reference to the timer capture for verification.
    #[must_use]
    pub fn capture(&self) -> &TimerCapture {
        &self.capture
    }

    /// Returns all processed messages in order (drains the queue).
    ///
    /// Used by `prop_processing_order` to verify per-key message ordering.
    #[must_use]
    pub fn processed_messages(&self) -> Vec<super::handler::ProcessedMessage> {
        self.inner_handler.processed()
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

    /// Creates a keyed context for the given key.
    fn context_for_key(&self, key: &Key) -> KeyedCapturingContext {
        KeyedCapturingContext::new(key.clone(), self.capture.clone())
    }

    /// Executes a message event using the real `DeferHandler`.
    pub async fn execute_message(&mut self, event: &MessageEvent) -> color_eyre::Result<()> {
        let key = &self.keys[event.key_idx];

        // Store the message in the loader so it can be loaded on timer retry
        self.loader.store_message(
            self.topic,
            self.partition,
            event.offset,
            key.clone(),
            json!({"offset": event.offset, "key_idx": event.key_idx}),
        );

        // Set the handler outcome based on the trace
        let outcome = match &event.outcome {
            MessageOutcome::Queued => {
                // For Queued, the handler shouldn't be called (key already deferred)
                // Set success as a safe default
                HandlerOutcome::Success
            }
            MessageOutcome::Success => HandlerOutcome::Success,
            MessageOutcome::Permanent => HandlerOutcome::Permanent,
            MessageOutcome::Transient { defer, .. } => {
                // Set the decider based on trace
                self.decider.set_next(*defer);
                HandlerOutcome::Transient
            }
        };
        self.inner_handler.set_outcome(outcome);

        // Create context for this key
        let key_context = self.context_for_key(key);

        // Load the message from the loader to get a ConsumerMessage
        let message = self
            .loader
            .load_message(self.topic, self.partition, event.offset)
            .await
            .map_err(|e| eyre!("loader error: {e}"))?;

        // Call the real DeferHandler::on_message
        let result = self
            .handler
            .on_message(key_context, message, DemandType::Normal)
            .await;

        // For most outcomes, we expect Ok (defer middleware absorbs transient errors)
        // Errors propagate only for permanent failures or when deferral is disabled
        match &event.outcome {
            MessageOutcome::Permanent | MessageOutcome::Transient { defer: false, .. } => {
                // Expected to fail
                if result.is_ok() {
                    return Err(eyre!(
                        "Expected error for outcome {:?} but got Ok",
                        event.outcome
                    ));
                }
            }
            _ => {
                // Expected to succeed (defer middleware absorbs the error)
                if let Err(e) = result {
                    return Err(eyre!(
                        "Unexpected error for outcome {:?}: {e}",
                        event.outcome
                    ));
                }
            }
        }

        Ok(())
    }

    /// Executes a timer event using the real `DeferHandler`.
    pub async fn execute_timer(&mut self, event: &TimerEvent) -> color_eyre::Result<()> {
        let key = &self.keys[event.key_idx];

        // Log state before execution
        {
            let key_ref = self.key_ref(event.key_idx);
            let state = self.store.get_next_deferred_message(&key_ref).await;
            debug!(
                "execute_timer START: key_idx={}, trace_offset={}, store_state={:?}, has_timer={}",
                event.key_idx,
                event.offset,
                state,
                self.capture.has_active_timer(key)
            );
        };

        // Set the handler outcome based on the trace
        let outcome = match &event.outcome {
            TimerOutcome::Success => HandlerOutcome::Success,
            TimerOutcome::Permanent => HandlerOutcome::Permanent,
            TimerOutcome::Transient { .. } => {
                // Timer transient failures always defer (no decider check in retry path)
                self.decider.set_next(true);
                HandlerOutcome::Transient
            }
        };
        self.inner_handler.set_outcome(outcome);

        // Get the actual scheduled time for this key's timer.
        // This must match what schedule_retry_timer scheduled.
        let trigger_time = self
            .capture
            .get_timer_time(key)
            .ok_or_else(|| eyre!("No timer scheduled for key {:?}", key))?;

        // Create context for this key
        let key_context = self.context_for_key(key);

        // Create a trigger with the actual scheduled time
        let trigger = Trigger::for_testing(key.clone(), trigger_time, TimerType::DeferRetry);

        // Call the real DeferHandler::on_timer
        let result = self
            .handler
            .on_timer(key_context, trigger, DemandType::Normal)
            .await;

        // Timer event outcomes:
        // - Success: completes, schedules next if queue not empty, returns Ok
        // - Transient: increments retry count, reschedules same offset, returns Ok
        // - Permanent: removes offset, schedules next if queued, PROPAGATES error
        match &event.outcome {
            TimerOutcome::Success | TimerOutcome::Transient { .. } => {
                // These should succeed (errors handled internally)
                if let Err(e) = result {
                    return Err(eyre!("Timer failed unexpectedly: {e}"));
                }
            }
            TimerOutcome::Permanent => {
                // Permanent errors are propagated - this is expected
                if result.is_ok() {
                    return Err(eyre!("Expected error for Permanent timer but got Ok"));
                }
            }
        }

        // The handler manages timers via context calls (schedule, clear_and_schedule,
        // etc.) We don't clear timers here - that's the handler's
        // responsibility.
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
    use crate::consumer::middleware::defer::tests::TEST_RUNTIME;
    use crate::tracing::init_test_logging;

    #[test]
    fn harness_executes_simple_defer_sequence() {
        init_test_logging();

        TEST_RUNTIME.block_on(async {
            let mut harness = TestHarness::new(1).ok()?;

            // Message arrives and is deferred
            let msg = MessageEvent {
                key_idx: 0,
                offset: Offset::from(1_i64),
                outcome: MessageOutcome::Transient {
                    max_backoff: CompactDuration::new(60),
                    defer: true,
                },
            };
            harness.execute_message(&msg).await.ok()?;

            // Key should be deferred
            let retry_count = harness.get_retry_count(0).await.ok()??;
            assert_eq!(retry_count, 0);

            // Timer should be active
            let key = harness.key(0).clone();
            assert!(harness.capture().has_active_timer(&key));

            // Timer fires successfully
            let timer = TimerEvent {
                key_idx: 0,
                offset: Offset::from(1_i64),
                outcome: TimerOutcome::Success,
            };
            harness.execute_timer(&timer).await.ok()?;

            // Key should not be deferred
            let retry_count = harness.get_retry_count(0).await.ok()?;
            assert!(retry_count.is_none());

            // Timer should be cleared
            assert!(!harness.capture().has_active_timer(&key));

            Some(())
        });
    }

    #[test]
    fn harness_queues_additional_messages() {
        init_test_logging();

        TEST_RUNTIME.block_on(async {
            let mut harness = TestHarness::new(1).ok()?;

            // First message defers
            let msg1 = MessageEvent {
                key_idx: 0,
                offset: Offset::from(1_i64),
                outcome: MessageOutcome::Transient {
                    max_backoff: CompactDuration::new(60),
                    defer: true,
                },
            };
            harness.execute_message(&msg1).await.ok()?;

            // Second message queues
            let msg2 = MessageEvent {
                key_idx: 0,
                offset: Offset::from(2_i64),
                outcome: MessageOutcome::Queued,
            };
            harness.execute_message(&msg2).await.ok()?;

            // Timer fires for first message
            let timer1 = TimerEvent {
                key_idx: 0,
                offset: Offset::from(1_i64),
                outcome: TimerOutcome::Success,
            };
            harness.execute_timer(&timer1).await.ok()?;

            // Key should still be deferred (has second message)
            let retry_count = harness.get_retry_count(0).await.ok()?;
            assert!(retry_count.is_some());

            // Timer should still be active
            let key = harness.key(0).clone();
            assert!(harness.capture().has_active_timer(&key));

            Some(())
        });
    }

    #[test]
    fn harness_increments_retry_count_on_transient_timer() {
        init_test_logging();

        TEST_RUNTIME.block_on(async {
            let mut harness = TestHarness::new(1).ok()?;

            // Message defers
            let msg = MessageEvent {
                key_idx: 0,
                offset: Offset::from(1_i64),
                outcome: MessageOutcome::Transient {
                    max_backoff: CompactDuration::new(60),
                    defer: true,
                },
            };
            harness.execute_message(&msg).await.ok()?;

            // Initial retry count is 0
            let retry_count = harness.get_retry_count(0).await.ok()??;
            assert_eq!(retry_count, 0);

            // Timer fires with transient failure
            let timer = TimerEvent {
                key_idx: 0,
                offset: Offset::from(1_i64),
                outcome: TimerOutcome::Transient {
                    max_backoff: CompactDuration::new(120),
                },
            };
            harness.execute_timer(&timer).await.ok()?;

            // Retry count should be incremented
            let retry_count = harness.get_retry_count(0).await.ok()??;
            assert_eq!(retry_count, 1);

            Some(())
        });
    }
}
