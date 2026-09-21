//! Executes deferred-message traces through the real handler.

use super::context::{KeyedCapturingContext, TimerCapture};
use super::faults::{self, Fault, Pass};
use super::handler::{HandlerOutcome, OutcomeHandler};
use super::loader::{FailableLoader, LoaderFailureType};
use super::store::FailableStore;
use super::types::{MessageEvent, MessageOutcome, TimerEvent, TimerOutcome, TraceEvent};
use crate::consumer::DemandType;
use crate::consumer::message::ConsumerRecord;
use crate::consumer::middleware::FallibleHandler;
use crate::consumer::middleware::defer::DeferConfiguration;
use crate::consumer::middleware::defer::decider::TraceBasedDecider;
use crate::consumer::middleware::defer::message::handler::MessageDeferHandler;
use crate::consumer::middleware::defer::message::store::MessageDeferStore;
use crate::consumer::middleware::defer::message::store::memory::MemoryMessageDeferStore;
use crate::loader::{MemoryLoader, MessageLoader};
use crate::telemetry::Telemetry;
use crate::timers::{TimerType, Trigger};
use crate::{Key, Partition, Topic};
use color_eyre::eyre::eyre;
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use tracing::debug;

/// Type alias for the `MessageDeferHandler` used in tests.
type TestDeferHandler = MessageDeferHandler<
    OutcomeHandler,
    FailableStore<MemoryMessageDeferStore>,
    FailableLoader,
    TraceBasedDecider,
>;

/// Owns the handler, store, loader, and timer capture for each trace.
pub struct TestHarness {
    /// The real defer handler under test.
    pub(crate) handler: TestDeferHandler,
    /// Inner handler for setting outcomes (shared via Arc).
    pub(crate) inner_handler: OutcomeHandler,
    /// Decider for setting defer decisions (shared via Arc).
    pub(crate) decider: TraceBasedDecider,
    /// Loader for storing messages and injecting failures (shared via Arc).
    pub(super) loader: FailableLoader,
    pub(super) failable_store: FailableStore<MemoryMessageDeferStore>,
    /// Store for verification (shared via Arc inside handler).
    store: MemoryMessageDeferStore,
    /// Timer capture for verification.
    capture: TimerCapture,
    /// Topic for messages.
    pub(super) topic: Topic,
    /// Partition for messages.
    pub(super) partition: Partition,
    /// Key pool (generated from `key_count`).
    keys: Vec<Key>,
}

impl TestHarness {
    /// Runs an event with one fault and one possible redelivery.
    pub(super) async fn execute_faulted(
        &self,
        event: &TraceEvent,
        fault: Option<Fault>,
    ) -> color_eyre::Result<Vec<Pass>> {
        faults::execute_faulted(self, event, fault).await
    }

    /// Creates a new test harness with the given key count.
    pub fn new(key_count: usize) -> color_eyre::Result<Self> {
        let keys: Vec<Key> = (0..key_count)
            .map(|i| Arc::from(format!("key-{i}")))
            .collect();

        let topic = Topic::from("test-topic");
        let partition = Partition::from(0_i32);

        let inner_handler = OutcomeHandler::new();
        let decider = TraceBasedDecider::new();
        let memory_loader = MemoryLoader::new();
        let loader = FailableLoader::new(memory_loader);
        let store = MemoryMessageDeferStore::new();
        let capture = TimerCapture::new();
        let failable_store = FailableStore::new(store.clone(), capture.clone());

        let config = DeferConfiguration::builder()
            .base(Duration::from_secs(u64::from(
                super::TEST_BASE_BACKOFF_SECS,
            )))
            .max_delay(Duration::from_secs(u64::from(super::TEST_MAX_BACKOFF_SECS)))
            .failure_threshold(0.9_f64)
            .build()
            .map_err(|e| eyre!("config error: {e}"))?;

        let telemetry = Telemetry::new();
        let sender = telemetry.partition_sender(topic, partition);

        let handler = MessageDeferHandler {
            handler: inner_handler.clone(),
            loader: loader.clone(),
            store: failable_store.clone(),
            decider: decider.clone(),
            config,
            topic,
            partition,
            sender,
            source: Arc::from("test"),
            dedup_version: Arc::from("1"),
        };

        Ok(Self {
            handler,
            inner_handler,
            decider,
            loader,
            failable_store,
            store,
            capture,
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
    pub fn store(&self) -> &MemoryMessageDeferStore {
        &self.store
    }

    /// Returns a reference to the timer capture for verification.
    #[must_use]
    pub fn capture(&self) -> &TimerCapture {
        &self.capture
    }

    /// Returns all processed messages in order (drains the queue).
    #[must_use]
    pub fn processed_messages(&self) -> Vec<super::handler::ProcessedMessage> {
        self.inner_handler.processed()
    }

    /// Gets the retry count for a key from the store.
    pub async fn get_retry_count(&self, key_idx: usize) -> color_eyre::Result<Option<u32>> {
        let key = &self.keys[key_idx];
        self.store
            .is_deferred(key)
            .await
            .map_err(|e| eyre!("store error: {e}"))
    }

    /// Creates a keyed context for the given key.
    pub(super) fn context_for_key(&self, key: &Key) -> KeyedCapturingContext {
        KeyedCapturingContext::new(key.clone(), self.capture.clone())
    }

    pub(super) fn arm_message(&self, event: &MessageEvent) {
        let outcome = match &event.outcome {
            MessageOutcome::Queued | MessageOutcome::Success => HandlerOutcome::Success,
            MessageOutcome::Permanent => HandlerOutcome::Permanent,
            MessageOutcome::Transient { defer, .. } => {
                self.decider.set_next(*defer);
                HandlerOutcome::Transient
            }
        };
        self.inner_handler.set_outcome(outcome);
    }

    /// Executes a message event with the real `MessageDeferHandler`.
    pub async fn execute_message(&mut self, event: &MessageEvent) -> color_eyre::Result<()> {
        let key = &self.keys[event.key_idx];

        self.loader.store_message(
            self.topic,
            self.partition,
            event.offset,
            key.clone(),
            json!({"offset": event.offset, "key_idx": event.key_idx}),
        );

        self.arm_message(event);

        let key_context = self.context_for_key(key);

        let message = self
            .loader
            .load_message(self.topic, self.partition, event.offset)
            .await
            .map_err(|e| eyre!("loader error: {e}"))?;
        let ConsumerRecord::Message(message) = message else {
            return Err(eyre!("the loader returned an excise record"));
        };

        let result = self
            .handler
            .on_message(key_context, message, DemandType::Normal)
            .await;

        match &event.outcome {
            MessageOutcome::Permanent | MessageOutcome::Transient { defer: false, .. } => {
                if result.is_ok() {
                    return Err(eyre!(
                        "Expected error for outcome {:?} but got Ok",
                        event.outcome
                    ));
                }
            }
            _ => {
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

    pub(super) fn arm_timer(&self, event: &TimerEvent) {
        match &event.outcome {
            TimerOutcome::LoaderPermanent => {
                self.loader
                    .set_next_failure(Some(LoaderFailureType::Permanent));
            }
            TimerOutcome::LoaderTransient { .. } => {
                self.loader
                    .set_next_failure(Some(LoaderFailureType::Transient));
            }
            _ => {
                self.loader.set_next_failure(None);
            }
        }

        let outcome = match &event.outcome {
            TimerOutcome::Success => HandlerOutcome::Success,
            TimerOutcome::Permanent => HandlerOutcome::Permanent,
            TimerOutcome::Transient { .. } => {
                self.decider.set_next(true);
                HandlerOutcome::Transient
            }
            TimerOutcome::LoaderPermanent | TimerOutcome::LoaderTransient { .. } => {
                HandlerOutcome::Success
            }
        };
        self.inner_handler.set_outcome(outcome);
    }

    /// Executes a timer event with the real `MessageDeferHandler`.
    pub async fn execute_timer(
        &mut self,
        event: &TimerEvent,
        demand: DemandType,
    ) -> color_eyre::Result<()> {
        let key = &self.keys[event.key_idx];

        {
            let state = self.store.get_next_deferred_message(key).await;
            debug!(
                "execute_timer START: key_idx={}, trace_offset={}, store_state={:?}, has_timer={}",
                event.key_idx,
                event.offset,
                state,
                self.capture.has_active_timer(key)
            );
        };

        self.arm_timer(event);

        let trigger_time = self
            .capture
            .get_timer_time(key)
            .ok_or_else(|| eyre!("No timer scheduled for key {:?}", key))?;

        let key_context = self.context_for_key(key);

        let trigger = Trigger::for_testing(key.clone(), trigger_time, TimerType::DeferredMessage);

        let result = self.handler.on_timer(key_context, trigger, demand).await;

        match &event.outcome {
            TimerOutcome::Success
            | TimerOutcome::Transient { .. }
            | TimerOutcome::LoaderPermanent
            | TimerOutcome::LoaderTransient { .. } => {
                if let Err(e) = result {
                    return Err(eyre!("Timer failed unexpectedly: {e}"));
                }
            }
            TimerOutcome::Permanent => {
                if result.is_ok() {
                    return Err(eyre!("Expected error for Permanent timer but got Ok"));
                }
            }
        }

        Ok(())
    }

    /// Executes a single trace event.
    pub async fn execute_event(&mut self, event: &TraceEvent) -> color_eyre::Result<()> {
        match event {
            TraceEvent::Message(msg) => self.execute_message(msg).await,
            TraceEvent::Timer(timer) => self.execute_timer(timer, DemandType::Normal).await,
        }
    }
}
