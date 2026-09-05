//! Each delivered event adds one to its key's counter, despite process crashes.
//! The harness uses real partition dispatch because mock contexts cannot test
//! manager and settle interactions.

use super::super::ShutdownPhase;
use super::super::dispatch::process_event;
use super::super::offsets::OffsetTracker;
use crate::consumer::message::{ConsumerMessage, UncommittedEvent};
use crate::consumer::middleware::deduplication::{
    DedupIdentity, DeduplicationHandler, DeduplicationStoreProvider, MemoryDeduplicationStore,
    MemoryDeduplicationStoreProvider,
};
use crate::consumer::middleware::{FallibleEventHandler, FallibleHandler, LeafHandler};
use crate::consumer::{DemandType, EventContext};
use crate::error::{ClassifyError, ErrorCategory};
use crate::loader::MemoryLoader;
use crate::otel::SpanRelation;
use crate::segment::partition_segment_id;
use crate::state::commit::{CommitManager, StoreTagSource};
use crate::state::descriptor::{DescriptorIdentity, Registered, ValueDescriptor, value_state};
use crate::state::manager::{PartitionStateProvider, StateManager, StateManagerProvider};
use crate::state::memory::{MemoryCellStore, MemoryCells, MemoryDescriptorIdentityStore};
use crate::state::publisher::NoPublisher;
use crate::state::registry::{CollectionDef, CollectionDefRegistry};
use crate::state::{CollectionId, PartitionBackend, SharedStateBackend, StateKey, StateName};
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::timers::slab::Slab;
use crate::timers::store::TriggerStore;
use crate::timers::store::memory::{InMemoryTriggerStore, memory_store};
use crate::timers::test_support::{setup_timer_manager_over, test_segment};
use crate::timers::{PendingTimer, TimerManager, TimerType, Trigger};
use crate::{Key, Topic};
use color_eyre::eyre::{Report, Result, bail, eyre};
use crossbeam_utils::CachePadded;
use futures::{Stream, StreamExt, TryStreamExt};
use parking_lot::Mutex;
use quickcheck::{QuickCheck, TestResult};
use serde_json::Value;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::task::{Context, Poll};
use std::time::Duration;
use thiserror::Error;
use tokio::runtime::Builder;
use tokio::sync::watch;
use tokio::time::timeout;
use tracing::Span;

const MAX_CHAIN: u32 = 3;
const KEYS: [&str; 3] = ["a", "b", "c"];
const IDENTITY: DedupIdentity<'static> = DedupIdentity {
    version: "1",
    group_id: "crash-test",
    topic: "crash-test",
    partition: 0,
};

type Timers = InMemoryTriggerStore;
type Oracle = CommitManager<MemoryDeduplicationStore, StoreTagSource<Timers>>;
type Backend = PartitionBackend<Oracle, MemoryDescriptorIdentityStore, MemoryCellStore<Oracle>>;
type State = StateManager<Backend, MemoryLoader<Value>>;

/// The inner future receives at most `left` polls. Drop stops the attempt.
struct PollBudget<F> {
    inner: Pin<Box<F>>,
    left: usize,
}

#[derive(Debug)]
enum Attempt<T> {
    Finished(T),
    Crashed,
}

/// These stores survive a crash. Each process gets new managers and caches.
struct Stores {
    timers: Timers,
    first_slab: u32,
    cells: MemoryCells,
    markers: MemoryDeduplicationStoreProvider,
    registry: Arc<CollectionDefRegistry>,
    counter: Registered<ValueDescriptor>,
    targets: Arc<Mutex<[(u64, CompactDuration); KEYS.len()]>>,
}

struct Process<S> {
    stream: Pin<Box<S>>,
    timers: TimerManager<Timers>,
    state: State,
    handler: DeduplicationHandler<LeafHandler<Counter>, MemoryDeduplicationStore>,
    shutdown: watch::Sender<ShutdownPhase>,
}

#[derive(Clone)]
struct Counter {
    value: Registered<ValueDescriptor>,
    targets: Arc<Mutex<[(u64, CompactDuration); KEYS.len()]>>,
}

impl FallibleEventHandler for DeduplicationHandler<LeafHandler<Counter>, MemoryDeduplicationStore> {}

impl<F: Future> Future for PollBudget<F> {
    type Output = Attempt<F::Output>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.left == 0 {
            return Poll::Ready(Attempt::Crashed);
        }
        self.left -= 1;
        let result = self.inner.as_mut().poll(cx);
        if result.is_pending() && self.left == 0 {
            cx.waker().wake_by_ref();
        }
        result.map(Attempt::Finished)
    }
}

impl Stores {
    fn new() -> Result<Self> {
        let descriptor: ValueDescriptor = value_state("counter");
        let mut registry = CollectionDefRegistry::default();
        registry.register(&descriptor, CollectionDef::new(None))?;
        Ok(Self {
            timers: memory_store(test_segment("crash-test", 300_u32)),
            first_slab: CompactDateTime::now()?.epoch_seconds() / 300,
            cells: MemoryCells::new(),
            markers: MemoryDeduplicationStoreProvider::new(),
            registry: Arc::new(registry),
            counter: Registered::new(descriptor),
            targets: Arc::new(Mutex::new([(0, CompactDuration::new(0)); KEYS.len()])),
        })
    }

    fn collection(&self, key: &str) -> Result<CollectionId> {
        let descriptor = self.counter.descriptor();
        Ok(CollectionId::new(
            StateKey::new(
                partition_segment_id(topic(), 0, IDENTITY.group_id),
                Key::from(key),
            ),
            descriptor.state_type(),
            StateName::try_new(descriptor.name())?,
        ))
    }

    async fn process(&self) -> Result<Process<impl Stream<Item = PendingTimer<Timers>>>> {
        let (stream, timers, shutdown) = setup_timer_manager_over(self.timers.clone()).await?;
        let oracle = CommitManager::new(
            self.markers.create_store(topic(), 0, IDENTITY.group_id),
            StoreTagSource(self.timers.clone()),
        );
        let provider = StateManagerProvider::new(
            SharedStateBackend::new(
                MemoryCellStore::new(self.cells.clone(), oracle.clone(), self.registry.clone()),
                MemoryDescriptorIdentityStore::new(),
                oracle,
            ),
            MemoryLoader::<Value>::new(),
            NoPublisher,
            self.registry.clone(),
            Arc::from(IDENTITY.group_id),
            CompactDuration::new(30),
        );
        let state = provider.acquire(topic(), 0, self.timers.clone()).await?;
        Ok(Process {
            stream: Box::pin(stream),
            timers,
            state,
            handler: DeduplicationHandler {
                inner: LeafHandler::new(Counter {
                    value: self.counter,
                    targets: self.targets.clone(),
                }),
                store: self.markers.create_store(topic(), 0, IDENTITY.group_id),
            },
            shutdown,
        })
    }

    async fn has_timers(&self) -> Result<bool> {
        let last = CompactDateTime::now()?.add_duration(CompactDuration::new(60))?;
        for slab in
            self.first_slab..=last.epoch_seconds() / self.timers.segment().slab_size.seconds()
        {
            if !self
                .timers
                .get_slab_triggers_all_types(Slab::new(slab, self.timers.segment().slab_size))
                .try_collect::<Vec<_>>()
                .await?
                .is_empty()
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn check(&self, expected: &[u64; KEYS.len()], messages: usize) -> Result<()> {
        for (key, count) in KEYS.iter().zip(expected) {
            let collection = self.collection(key)?;
            if !self.cells.provisional_coordinates(&collection).is_empty()
                || self.cells.unsettled_marker_of(&collection).is_some()
            {
                bail!("key {key}: provisional state remains");
            }
            let coordinates = self.cells.stored_coordinates(&collection);
            let actual = match coordinates.as_slice() {
                [] => 0,
                [cell] => {
                    let bytes = self
                        .cells
                        .read_committed(&collection, cell)
                        .ok_or_else(|| eyre!("key {key}: counter has no value"))?;
                    serde_json::from_slice::<u64>(&bytes)?
                }
                _ => bail!("key {key}: counter has more than one cell"),
            };
            if actual != *count {
                bail!("key {key}: counter {actual}, expected {count}");
            }
        }
        if self.markers.marker_count() != messages {
            bail!(
                "marker count {}, expected {messages}",
                self.markers.marker_count()
            );
        }
        Ok(())
    }
}

impl<S: Stream<Item = PendingTimer<Timers>>> Process<S> {
    async fn dispatch(&self, event: UncommittedEvent<Timers, Value>, budget: usize) -> Attempt<()> {
        PollBudget {
            inner: Box::pin(process_event(
                event,
                &self.handler,
                &self.shutdown.subscribe(),
                &self.timers,
                &self.state,
                IDENTITY,
                SpanRelation::default(),
            )),
            left: budget,
        }
        .await
    }

    async fn message(&self, offset: i64, key: &str, budget: usize) -> Result<bool> {
        let offsets = OffsetTracker::new(
            topic(),
            0,
            1,
            Duration::from_secs(60),
            Arc::new(CachePadded::new(AtomicUsize::new(0))),
        );
        let message =
            ConsumerMessage::for_testing(topic(), 0, offset, Key::from(key), Value::Null)?;
        let event =
            UncommittedEvent::Message(message.into_uncommitted(offsets.take(offset).await?));
        self.dispatch(event, budget).await;
        Ok(offsets.shutdown().await == Some(offset))
    }

    async fn timer(&mut self, budget: usize) -> Result<Attempt<()>> {
        let pending = timeout(Duration::from_secs(600), self.stream.next())
            .await?
            .ok_or_else(|| eyre!("timer stream stopped"))?;
        Ok(self
            .dispatch(UncommittedEvent::Timer(pending), budget)
            .await)
    }

    async fn stop(&mut self) -> Result<()> {
        self.shutdown.send_replace(ShutdownPhase::Cancelling);
        let stream = &mut self.stream;
        while timeout(Duration::from_secs(600), stream.next())
            .await?
            .is_some()
        {}
        Ok(())
    }
}

impl Counter {
    async fn increment<C: EventContext<Payload = Value>>(&self, context: &C) -> Result<u64> {
        let value = context.state(self.value)?;
        let count = match value.get().await? {
            Some(value) => value
                .as_u64()
                .ok_or_else(|| eyre!("counter is not a number"))?,
            None => 0,
        };
        value.set(Value::from(count + 1)).await?;
        Ok(count + 1)
    }
}

impl FallibleHandler for Counter {
    type Error = HandlerError;
    type Output = ();
    type Payload = Value;

    async fn on_message<C>(
        &self,
        context: C,
        _message: ConsumerMessage<Value>,
        _demand: DemandType,
    ) -> Result<(), HandlerError>
    where
        C: EventContext<Payload = Value>,
    {
        self.increment(&context).await?;
        Ok(())
    }

    async fn on_excise<C>(
        &self,
        context: C,
        _message: ConsumerMessage<()>,
        _demand: DemandType,
    ) -> Result<(), HandlerError>
    where
        C: EventContext<Payload = Value>,
    {
        self.increment(&context).await?;
        Ok(())
    }

    async fn on_timer<C>(
        &self,
        context: C,
        timer: Trigger,
        _demand: DemandType,
    ) -> Result<(), HandlerError>
    where
        C: EventContext<Payload = Value>,
    {
        let result: Result<()> = async {
            let count = self.increment(&context).await?;
            let index = KEYS
                .iter()
                .position(|key| *key == timer.key.as_ref())
                .ok_or_else(|| eyre!("timer has an unknown key"))?;
            let (target, step) = self.targets.lock()[index];
            if count < target {
                context
                    .clear_and_schedule(timer.time.add_duration(step)?, TimerType::Application)
                    .await?;
            }
            Ok(())
        }
        .await;
        result.map_err(HandlerError::from)
    }

    async fn after_commit<C>(&self, _context: C, result: Result<(), HandlerError>)
    where
        C: EventContext<Payload = Value>,
    {
        assert!(result.is_ok(), "handler failed: {result:?}");
    }

    async fn after_abort<C>(&self, _context: C, result: Result<(), HandlerError>)
    where
        C: EventContext<Payload = Value>,
    {
        assert!(result.is_ok(), "handler failed: {result:?}");
    }

    async fn shutdown(self) {}
}

fn topic() -> Topic {
    Topic::from(IDENTITY.topic)
}

/// Tuple fields select the operation, key, and poll budget or time offset.
/// Vec and tuple shrinking remove operations and reduce each field.
#[test]
fn prop_crash_anywhere() {
    fn property(trace: Vec<(u8, u8, u8)>) -> TestResult {
        let result = Builder::new_current_thread()
            .enable_all()
            .start_paused(true)
            .build()
            .map_err(Report::from)
            .and_then(|runtime| runtime.block_on(run_trace(trace)));
        match result {
            Ok(()) => TestResult::passed(),
            Err(error) => TestResult::error(format!("{error:#}")),
        }
    }
    QuickCheck::new().quickcheck(property as fn(Vec<(u8, u8, u8)>) -> TestResult);
}

async fn run_trace(trace: Vec<(u8, u8, u8)>) -> Result<()> {
    let stores = Stores::new()?;
    let mut process = stores.process().await?;
    let mut expected = [0; KEYS.len()];
    let mut messages = 0;
    let mut budget = usize::MAX;
    let result: Result<()> = async {
        for (op, key, value) in trace.into_iter().take(32) {
            let index = usize::from(key) % KEYS.len();
            match op % 4 {
                0 => {
                    let offset = i64::try_from(messages)?;
                    messages += 1;
                    expected[index] += 1;
                    if !process.message(offset, KEYS[index], budget).await? {
                        process.stop().await?;
                        process = stores.process().await?;
                        if !process.message(offset, KEYS[index], usize::MAX).await? {
                            bail!("redelivery left offset {offset} uncommitted");
                        }
                    }
                    budget = usize::MAX;
                }
                1 => {
                    let time = CompactDateTime::now()?
                        .add_duration(CompactDuration::new(u32::from(value % 4)))?;
                    let step = CompactDuration::new(u32::from((value / 4) % 2));
                    let links = 1 + u32::from(value / 8) % MAX_CHAIN;
                    expected[index] += u64::from(links);
                    stores.targets.lock()[index] = (expected[index], step);
                    process
                        .timers
                        .schedule_trigger(Trigger::new(
                            Key::from(KEYS[index]),
                            time,
                            TimerType::Application,
                            Span::none(),
                        ))
                        .await?;
                    if matches!(process.timer(budget).await?, Attempt::Crashed) {
                        process.stop().await?;
                        process = stores.process().await?;
                    }
                    budget = usize::MAX;
                }
                2 => budget = usize::from(value % 24),
                _ => {
                    process.stop().await?;
                    process = stores.process().await?;
                }
            }
            // Finish each chain before a later root can replace its remaining links.
            let mut fires = 0;
            while stores.has_timers().await? {
                fires += 1;
                if fires > MAX_CHAIN + 2 {
                    bail!("timer chain did not stop");
                }
                process.timer(usize::MAX).await?;
            }
            stores.check(&expected, messages)?;
        }
        Ok(())
    }
    .await;
    process.stop().await?;
    result
}

/// A replacement before the state stage must not lose the first increment.
#[tokio::test(start_paused = true)]
async fn crash_before_stage_keeps_both_increments() -> Result<()> {
    let stores = Stores::new()?;
    let mut process = stores.process().await?;
    let time = CompactDateTime::now()?;
    stores.targets.lock()[0] = (2, CompactDuration::new(1));
    let trigger = Trigger::new(
        Key::from(KEYS[0]),
        time,
        TimerType::Application,
        Span::none(),
    );
    process.timers.schedule_trigger(trigger.clone()).await?;

    let result: Result<()> = async {
        assert!(matches!(process.timer(1).await?, Attempt::Crashed));
        process.stop().await?;
        process = stores.process().await?;
        for _ in 0_u8..2 {
            process.timer(usize::MAX).await?;
        }
        assert!(!stores.has_timers().await?, "timer rows remain");
        stores.check(&[2, 0, 0], 0)
    }
    .await;
    process.stop().await?;
    result
}

/// Every crash boundary preserves both increments at one timer coordinate.
/// The counter replays idempotently, so the manager property
/// `prop_same_coordinate_clear_preserves_timer_oracle` pins key tag rotation.
#[tokio::test(start_paused = true)]
async fn every_crash_point_keeps_rescheduled_chain() -> Result<()> {
    for budget in 1_usize..=24 {
        let stores = Stores::new()?;
        let mut process = stores.process().await?;
        stores.targets.lock()[0] = (2, CompactDuration::new(0));
        process
            .timers
            .schedule_trigger(Trigger::new(
                Key::from(KEYS[0]),
                CompactDateTime::now()?,
                TimerType::Application,
                Span::none(),
            ))
            .await?;

        let result: Result<()> = async {
            process.timer(budget).await?;
            process.stop().await?;
            process = stores.process().await?;
            let mut fires = 0;
            while stores.has_timers().await? {
                fires += 1;
                if fires > MAX_CHAIN + 2 {
                    bail!("timer chain did not stop");
                }
                process.timer(usize::MAX).await?;
            }
            stores.check(&[2, 0, 0], 0)
        }
        .await;
        process.stop().await?;
        result.map_err(|error| eyre!("poll budget {budget}: {error:#}"))?;
    }
    Ok(())
}

#[derive(Debug, Error)]
#[error("handler failed: {0:#}")]
struct HandlerError(#[from] Report);

impl ClassifyError for HandlerError {
    fn classify_error(&self) -> ErrorCategory {
        ErrorCategory::Terminal
    }
}
