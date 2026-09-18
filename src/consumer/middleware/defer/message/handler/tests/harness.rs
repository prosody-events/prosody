//! Dispatches trace events through the defer handler and settlement boundary.

use super::context::{KeyedCapturingContext, TimerCapture};
use super::handler::{HandlerOutcome, OutcomeHandler, ProcessedMessage};
use super::loader::{FailableLoader, LoaderFailureType};
use super::types::{
    Fault, MessageEvent, MessageOutcome, OutputEvent, Step, TimerEvent, TimerOutcome, TraceEvent,
};
use super::{FailableStore, FaultKind};
use crate::consumer::DemandType;
use crate::consumer::message::ConsumerRecord;
use crate::consumer::middleware::FallibleHandler;
use crate::consumer::middleware::defer::DeferConfiguration;
use crate::consumer::middleware::defer::decider::TraceBasedDecider;
use crate::consumer::middleware::defer::message::handler::MessageDeferHandler;
use crate::consumer::middleware::defer::message::store::MessageDeferStore;
use crate::consumer::middleware::defer::message::store::memory::MemoryMessageDeferStore;
use crate::consumer::middleware::providers::LeafHandler;
use crate::consumer::middleware::settle::settle;
use crate::consumer::middleware::tests::test_support::RecordingGuard;
use crate::loader::{MemoryLoader, MessageLoader};
use crate::telemetry::Telemetry;
use crate::timers::datetime::CompactDateTime;
use crate::timers::{TimerType, Trigger};
use crate::{Key, Offset, Partition, Topic};
use color_eyre::eyre::eyre;
use serde_json::json;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

// ============================================================================
// Verification Helpers
// ============================================================================

/// Verifies timer coverage: every deferred key has an active timer.
pub async fn verify_timer_coverage(
    capture: &TimerCapture,
    store: &MemoryMessageDeferStore,
    keys: &[Key],
) -> color_eyre::Result<()> {
    for key in keys {
        // Check if key is deferred in store (now uses &Key directly)
        let is_deferred = store
            .get_next_deferred_message(key)
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
// Test Harness using Real MessageDeferHandler
// ============================================================================

/// Type alias for the `MessageDeferHandler` used in tests.
type TestDeferHandler = MessageDeferHandler<
    LeafHandler<OutcomeHandler>,
    FailableStore<MemoryMessageDeferStore>,
    FailableLoader,
    TraceBasedDecider,
>;

/// What one dispatch of a source read and decided.
#[derive(Clone, Debug)]
pub struct Pass {
    /// The queue head and retry count before the dispatch.
    pub before: Option<(Offset, u32)>,
    /// The queue head and retry count after the dispatch.
    pub after: Option<(Offset, u32)>,
    /// True when the dispatch consumed a store or timer fault.
    pub consumed: bool,
    /// True when the boundary committed the source.
    pub committed: bool,
    /// The inner handler calls during the dispatch.
    pub calls: Vec<ProcessedMessage>,
    /// The timer times the dispatch scheduled for this key.
    pub scheduled: Vec<CompactDateTime>,
    /// The loader failure the dispatch consumed, if any.
    pub loader_failure: Option<LoaderFailureType>,
}

/// Owns the stores and timer capture for one trace.
/// Each aborted source receives at most one redelivery.
pub struct TestHarness {
    /// Dispatch records for the last event.
    pub passes: Vec<Pass>,
    /// The outer demand for interpreted timer events.
    pub demand: DemandType,
    next_offset: i64,
    /// The real defer handler under test.
    pub(crate) handler: TestDeferHandler,
    /// Inner handler for setting outcomes (shared via Arc).
    pub(crate) inner_handler: OutcomeHandler,
    /// Decider for setting defer decisions (shared via Arc).
    pub(crate) decider: TraceBasedDecider,
    /// Loader for storing messages and injecting failures (shared via Arc).
    loader: FailableLoader,
    /// Store for verification (shared via Arc inside handler).
    store: MemoryMessageDeferStore,
    /// Timer capture for verification.
    capture: TimerCapture,
    /// Topic for messages.
    topic: Topic,
    /// Partition for messages.
    partition: Partition,
    /// Key pool (generated from `key_count`).
    keys: Vec<Key>,
}

impl TestHarness {
    /// Creates a new test harness with the given key count.
    pub fn new(key_count: usize) -> color_eyre::Result<Self> {
        let keys: Vec<Key> = (0..key_count)
            .map(|i| Arc::from(format!("key-{i}")))
            .collect();

        let topic = Topic::from("test-topic");
        let partition = Partition::from(0_i32);

        // Create shared components (all use Arc internally)
        let inner_handler = OutcomeHandler::new();
        let decider = TraceBasedDecider::new();
        let memory_loader = MemoryLoader::new();
        let loader = FailableLoader::new(memory_loader);
        let store = MemoryMessageDeferStore::new();
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

        let telemetry = Telemetry::new();
        let sender = telemetry.partition_sender(topic, partition);

        let handler = MessageDeferHandler {
            handler: LeafHandler::new(inner_handler.clone()),
            loader: loader.clone(),
            store: FailableStore::new(store.clone()),
            decider: decider.clone(),
            config,
            topic,
            partition,
            sender,
            source: Arc::from("test"),
            dedup_version: Arc::from("1"),
        };

        Ok(Self {
            passes: Vec::with_capacity(2),
            demand: DemandType::Normal,
            next_offset: 0,
            handler,
            inner_handler,
            decider,
            loader,
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

    /// Gets the retry count for a key from the store.
    pub async fn get_retry_count(&self, key_idx: usize) -> color_eyre::Result<Option<u32>> {
        let key = &self.keys[key_idx];
        self.store
            .is_deferred(key)
            .await
            .map_err(|e| eyre!("store error: {e}"))
    }

    /// Creates a keyed context for the given key.
    fn context_for_key(&self, key: &Key) -> KeyedCapturingContext {
        KeyedCapturingContext::new(key.clone(), self.capture.clone())
    }

    /// Arms the first dispatch. Uncalled faults expire at the next event.
    fn arm(&self, key: &Key, fault: Option<Fault>) {
        self.handler.store.set_fault(None);
        self.capture.set_fault(None);
        self.loader.set_next_failure(None);
        match fault {
            Some(Fault::Store(op, kind)) => self.handler.store.set_fault(Some((op, kind))),
            Some(Fault::Timer(op, kind)) => self.capture.set_fault(Some((op, kind))),
            Some(Fault::LostTimer) => self.capture.drop_timers(key),
            Some(Fault::LoaderThenTimer(kind, op, timer_kind)) => {
                self.loader.set_next_failure(Some(match kind {
                    FaultKind::Transient => LoaderFailureType::Transient,
                    FaultKind::Permanent => LoaderFailureType::Permanent,
                }));
                self.capture.set_fault(Some((op, timer_kind)));
            }
            None => {}
        }
    }

    /// Dispatches a message and redelivers its source after an abort.
    pub async fn execute_message(&mut self, event: &MessageEvent) -> color_eyre::Result<()> {
        let key = self.keys[event.key_idx].clone();
        self.loader.store_message(
            self.topic,
            self.partition,
            event.offset,
            key.clone(),
            json!({"offset": event.offset, "key_idx": event.key_idx}),
        );
        self.passes.clear();
        self.arm(&key, event.fault);
        for _ in 0_u8..2 {
            let before = self.store.get_next_deferred_message(&key).await?;
            let pending = self.fault_pending();
            let outcome = match event.outcome {
                MessageOutcome::Queued | MessageOutcome::Success => HandlerOutcome::Success,
                MessageOutcome::Permanent => HandlerOutcome::Permanent,
                MessageOutcome::Transient { defer } => {
                    self.decider.set_next(defer);
                    HandlerOutcome::Transient
                }
            };
            self.inner_handler.set_outcome(outcome);
            let ConsumerRecord::Message(message) = self
                .loader
                .inner()
                .load_message(self.topic, self.partition, event.offset)
                .await?
            else {
                return Err(eyre!("The loader returned an excise record"));
            };
            let context = self.context_for_key(&key);
            let (guard, committed, aborted) = RecordingGuard::new();
            let result = self
                .handler
                .on_message(context.clone(), message, DemandType::Normal)
                .await;
            settle(&self.handler, context, guard, result).await;
            let committed = committed.load(Ordering::SeqCst);
            let aborted = aborted.load(Ordering::SeqCst);
            assert_eq!(committed + aborted, 1);
            self.record_pass(&key, before, pending, committed == 1, None)
                .await?;
            if committed == 1 {
                return Ok(());
            }
        }
        Err(eyre!("Message needs a third dispatch: {event:?}"))
    }

    /// Dispatches the same trigger again after an abort.
    /// A loader outcome overrides the loader kind in `LoaderThenTimer` on every
    /// pass.
    pub async fn execute_timer(
        &mut self,
        event: &TimerEvent,
        demand: DemandType,
    ) -> color_eyre::Result<()> {
        let key = self.keys[event.key_idx].clone();
        let time = self
            .capture
            .get_timer_time(&key)
            .ok_or_else(|| eyre!("No timer for {key:?}"))?;
        self.passes.clear();
        self.arm(&key, event.fault);
        // The fired source leaves the active set. An abort retains it for redelivery.
        self.capture.take_timer(&key, time);
        for _ in 0_u8..2 {
            let before = self.store.get_next_deferred_message(&key).await?;
            let pending = self.fault_pending();
            match event.outcome {
                TimerOutcome::LoaderPermanent => self
                    .loader
                    .set_next_failure(Some(LoaderFailureType::Permanent)),
                TimerOutcome::LoaderTransient => self
                    .loader
                    .set_next_failure(Some(LoaderFailureType::Transient)),
                _ => {}
            }
            let loader_failure = self.loader.pending_failure();
            self.inner_handler.set_outcome(match event.outcome {
                TimerOutcome::Permanent => HandlerOutcome::Permanent,
                TimerOutcome::Transient => HandlerOutcome::Transient,
                _ => HandlerOutcome::Success,
            });
            let context = self.context_for_key(&key);
            let trigger = Trigger::for_testing(key.clone(), time, TimerType::DeferredMessage);
            let (guard, committed, aborted) = RecordingGuard::new();
            let result = self
                .handler
                .on_timer(context.clone(), trigger, demand)
                .await;
            settle(&self.handler, context, guard, result).await;
            let committed = committed.load(Ordering::SeqCst);
            let aborted = aborted.load(Ordering::SeqCst);
            assert_eq!(committed + aborted, 1);
            self.record_pass(&key, before, pending, committed == 1, loader_failure)
                .await?;
            if committed == 1 {
                return Ok(());
            }
        }
        Err(eyre!("Timer needs a third dispatch: {event:?}"))
    }

    /// Turns a step into the event the real state allows.
    pub async fn interpret(&mut self, step: &Step) -> TraceEvent {
        let key_idx = usize::from(step.key_idx) % self.keys.len();
        let deferred = match self.store.is_deferred(&self.keys[key_idx]).await {
            Ok(count) => count.is_some(),
            Err(error) => match error {},
        };
        if deferred && step.roll.is_multiple_of(2) {
            let outcome = match (step.roll / 2) % 5 {
                0 => TimerOutcome::Success,
                1 => TimerOutcome::Permanent,
                2 => TimerOutcome::Transient,
                3 => TimerOutcome::LoaderPermanent,
                _ => TimerOutcome::LoaderTransient,
            };
            TraceEvent::Timer(TimerEvent {
                key_idx,
                outcome,
                fault: step.fault,
            })
        } else {
            self.next_offset += 1;
            let outcome = if deferred {
                MessageOutcome::Queued
            } else {
                match step.roll % 4 {
                    0 => MessageOutcome::Success,
                    1 => MessageOutcome::Permanent,
                    2 => MessageOutcome::Transient { defer: true },
                    _ => MessageOutcome::Transient { defer: false },
                }
            };
            TraceEvent::Message(MessageEvent {
                key_idx,
                offset: Offset::from(self.next_offset),
                outcome,
                fault: step.fault,
            })
        }
    }

    /// Interprets and executes one step.
    pub async fn execute_step(&mut self, step: &Step) -> color_eyre::Result<TraceEvent> {
        let event = self.interpret(step).await;
        match &event {
            TraceEvent::Message(message) => self.execute_message(message).await?,
            TraceEvent::Timer(timer) => self.execute_timer(timer, self.demand).await?,
        }
        Ok(event)
    }

    fn fault_pending(&self) -> bool {
        self.handler.store.fault_pending() || self.capture.fault_pending()
    }

    async fn record_pass(
        &mut self,
        key: &Key,
        before: Option<(Offset, u32)>,
        pending: bool,
        committed: bool,
        loader_failure: Option<LoaderFailureType>,
    ) -> color_eyre::Result<()> {
        let calls = self.inner_handler.processed();
        let scheduled = self
            .capture
            .drain_events()
            .into_iter()
            .filter_map(|event| match event {
                OutputEvent::Scheduled {
                    key: scheduled_key,
                    time,
                } if scheduled_key == *key => Some(time),
                _ => None,
            })
            .collect();
        self.passes.push(Pass {
            before,
            after: self.store.get_next_deferred_message(key).await?,
            consumed: pending && !self.fault_pending(),
            committed,
            calls,
            scheduled,
            loader_failure: loader_failure.filter(|_| self.loader.pending_failure().is_none()),
        });
        Ok(())
    }

    /// Verifies all invariants after executing an event.
    pub async fn verify_invariants(&self) -> color_eyre::Result<()> {
        verify_timer_coverage(&self.capture, &self.store, &self.keys).await
    }
}
