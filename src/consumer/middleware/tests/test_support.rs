//! Shared scaffolding for middleware tests: the mock event context, the
//! scripted handler double and error, message/trigger fixtures, the defer
//! outcome trio, and the recording-session harness.

use std::convert::Infallible;
use std::future::{self, Future};
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use educe::Educe;
use futures::StreamExt;
use parking_lot::Mutex;
use serde_json::{Value, json};
use thiserror::Error;
use tokio::sync::{Semaphore, oneshot, watch};
use tracing::Span;
use uuid::Uuid;

use crate::Key;
use crate::consumer::event_context::{EventContext, StateAccessError, TerminationSignals};
use crate::consumer::message::{ConsumerMessage, ConsumerMessageValue};
use crate::consumer::middleware::{
    DemandType, FallibleHandler, RepinProof, Settlement, SettlementHandler,
};
use crate::consumer::partition::ShutdownPhase;
use crate::consumer::{Keyed, Uncommitted};
use crate::error::{ClassifyError, ErrorCategory};
use crate::loader::{MemoryLoader, MessageLoader};
use crate::state::cell::Committed;
use crate::state::cell_key::{CellKey, Coordinate, Section};
use crate::state::descriptor::tests::{FixedOracle, TestSession, test_session_parts};
use crate::state::descriptor::{Registered, StateDescriptor, ValueDescriptor, value_state};
use crate::state::dirty::DirtyStore;
use crate::state::memory::{MemoryCellStore, MemoryCells, MemoryDescriptorIdentityStore};
use crate::state::oracle::CommitOracle;
use crate::state::registry::{CollectionDef, CollectionDefRegistry};
use crate::state::session::{
    CellWrite, KeyedStateSession, LifecycleAccess, SessionParts, TerminationWatch,
};
use crate::state::store::CellStore;
use crate::state::tests::cell_suite::value_cell;
use crate::state::tests::support::UnavailableState;
use crate::state::{
    CollectionId, CommitDecision, EventRef, PartitionBackend, StateKey, StateName, StateType,
};
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::timers::{TimerType, Trigger, UncommittedTimer};

/// A context backed by a real keyed-state test session.
pub type Ctx = MockEventContext<Value, TestSession>;

/// A commit guard that reports when durability reaches the commit.
///
/// The guard then waits for test release. This pause makes the order between
/// commit and a later apply hook observable. The report is a one-shot because
/// a guard commits at most once.
pub struct GatedGuard {
    entered: oneshot::Sender<()>,
    release: oneshot::Receiver<()>,
    committed: Arc<AtomicUsize>,
    aborted: Arc<AtomicUsize>,
}

impl GatedGuard {
    /// Returns a guard, both gates, and its terminal counters.
    pub fn new() -> (
        Self,
        oneshot::Receiver<()>,
        oneshot::Sender<()>,
        Arc<AtomicUsize>,
        Arc<AtomicUsize>,
    ) {
        let (entered_tx, entered_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let committed: Arc<AtomicUsize> = Arc::default();
        let aborted: Arc<AtomicUsize> = Arc::default();
        (
            Self {
                entered: entered_tx,
                release: release_rx,
                committed: Arc::clone(&committed),
                aborted: Arc::clone(&aborted),
            },
            entered_rx,
            release_tx,
            committed,
            aborted,
        )
    }
}

impl Uncommitted for GatedGuard {
    async fn commit(self) {
        let _send_result = self.entered.send(());
        drop(self.release.await);
        self.committed.fetch_add(1, Ordering::SeqCst);
    }

    async fn abort(self) {
        self.aborted.fetch_add(1, Ordering::SeqCst);
        drop(self.entered);
    }
}

/// Returns the `cart` value descriptor.
pub fn cart() -> ValueDescriptor {
    value_state("cart")
}

/// Buffers one `cart` write through a real session.
///
/// The settle boundary owns the only stage. `configure` changes the context
/// before the write.
pub async fn buffered(
    configure: impl FnOnce(Ctx) -> Ctx,
) -> color_eyre::Result<(Ctx, MemoryCellStore<FixedOracle>, CollectionId)> {
    let mut registry = CollectionDefRegistry::default();
    registry.register(&cart(), CollectionDef::new(None))?;
    let state_key = StateKey::new(Uuid::from_u128(0x7), Arc::from("user-1"));
    let (session, cell_store) =
        test_session_parts(MemoryLoader::new(), registry, state_key.clone());
    let context = configure(MockEventContext::new().with_session(session));
    let handle = context
        .state(Registered::new(cart()))
        .map_err(|error| color_eyre::eyre::eyre!("bind cart: {error}"))?;
    handle.set(json!({ "x": 1_i32 })).await?;
    let cart_id = CollectionId::new(
        state_key,
        StateType::Application,
        StateName::try_new("cart")?,
    );
    Ok((context, cell_store, cart_id))
}

/// Reports whether `id` still has a provisional cell.
pub async fn is_provisional(
    cell_store: &MemoryCellStore<FixedOracle>,
    id: &CollectionId,
) -> color_eyre::Result<bool> {
    let stream = cell_store.provisional_cells(id);
    futures::pin_mut!(stream);
    Ok(stream.next().await.transpose()?.is_some())
}

/// Returns the resolved value from a settled `cart` cell.
pub async fn committed_value(
    cell_store: &MemoryCellStore<FixedOracle>,
    id: &CollectionId,
) -> color_eyre::Result<Option<Bytes>> {
    let probe = EventRef::Message {
        dedup_id: Uuid::from_u128(u128::MAX),
    };
    cell_store
        .get(id, &value_cell(), probe)
        .await
        .map(Committed::into_inner)
        .map_err(|error| color_eyre::eyre::eyre!("read committed: {error}"))
}

/// Timer-operation error the mock injects on demand, carrying the category to
/// classify as. The backstop arm is must-succeed (invariant 8), so it retries
/// **every** category forever — a `with_timer_failures(k, category)` context
/// exercises the retry-forever self-heal for each, including `Terminal` (which
/// `retry_step` retries rather than abandons) and `Permanent` (which the arm's
/// own loop retries past `retry_step`'s `Skip`).
#[derive(Debug, Error)]
#[error("mock timer operation failed ({0:?})")]
pub struct MockTimerError(ErrorCategory);

impl ClassifyError for MockTimerError {
    fn classify_error(&self) -> ErrorCategory {
        self.0
    }
}

/// Records timer operations for verification in tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimerOperation {
    /// Timer scheduled at given time with given type.
    Schedule(CompactDateTime, TimerType),
    /// All timers cleared and new one scheduled.
    ClearAndSchedule(CompactDateTime, TimerType),
    /// Single timer unscheduled.
    Unschedule(CompactDateTime, TimerType),
    /// All timers of given type cleared.
    ClearScheduled(TimerType),
}

/// Unified mock context for middleware tests.
///
/// Uses `tokio::sync::watch` channels (matching production `TimerContext`)
/// to avoid race conditions between flag checks and async notifications.
#[derive(Educe)]
#[educe(Clone(bound(S: Clone)))]
pub struct MockEventContext<P = Value, S = UnavailableState<P>> {
    /// Partition/consumer shutdown signal (sender for mutations).
    shutdown_tx: Arc<watch::Sender<ShutdownPhase>>,
    /// Partition/consumer shutdown signal (receiver for queries).
    shutdown_rx: watch::Receiver<ShutdownPhase>,

    /// Message-level cancellation signal (sender for mutations).
    cancel_tx: Arc<watch::Sender<bool>>,
    /// Message-level cancellation signal (receiver for queries).
    cancel_rx: watch::Receiver<bool>,

    /// Timer operation tracking (None = disabled).
    timer_operations: Option<Arc<Mutex<Vec<TimerOperation>>>>,

    /// Durable timer rows the mock's timer ops maintain, so `scheduled`
    /// answers from the same state `schedule`/`clear_and_schedule` mutate —
    /// like the real trigger store. Seed with
    /// [`with_durable_timer`](Self::with_durable_timer) to simulate a prior
    /// epoch's timer surviving a partition reacquisition; observe what an op
    /// left standing with [`durable_scheduled`](Self::durable_scheduled).
    durable_timers: Arc<Mutex<Vec<(CompactDateTime, TimerType)>>>,

    /// Number of leading timer schedules that fail (with
    /// [`Self::timer_fail_category`]) before schedules start succeeding —
    /// drives the arm's retry-forever self-heal. Shared so a clone observes
    /// the same countdown.
    timer_fail_count: Arc<AtomicUsize>,

    /// The category the leading failures classify as.
    timer_fail_category: ErrorCategory,

    /// When set, the next `scheduled()` read flips the shutdown watch as a
    /// side effect — see
    /// [`with_shutdown_on_timer_read`](Self::with_shutdown_on_timer_read).
    shutdown_on_timer_read: bool,

    /// Keyed-state session descriptor binds route to; defaults to the
    /// [`UnavailableState`] stub.
    session: S,

    /// Payload type pin ([`EventContext::Payload`]); defaults to
    /// `serde_json::Value` to match the default consumer codec.
    _payload: PhantomData<fn() -> P>,
}

impl<P> Default for MockEventContext<P>
where
    P: Clone + Send + Sync + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<P> MockEventContext<P>
where
    P: Clone + Send + Sync + 'static,
{
    /// Create a new mock context with default state (no signals active,
    /// keyed state unavailable).
    #[must_use]
    pub fn new() -> Self {
        let (shutdown_tx, shutdown_rx) = watch::channel(ShutdownPhase::default());
        let (cancel_tx, cancel_rx) = watch::channel(false);
        Self {
            shutdown_tx: Arc::new(shutdown_tx),
            shutdown_rx,
            cancel_tx: Arc::new(cancel_tx),
            cancel_rx,
            timer_operations: None,
            durable_timers: Arc::new(Mutex::new(Vec::new())),
            timer_fail_count: Arc::new(AtomicUsize::new(0)),
            timer_fail_category: ErrorCategory::Permanent,
            shutdown_on_timer_read: false,
            session: UnavailableState::new(),
            _payload: PhantomData,
        }
    }
}

impl<P, S> MockEventContext<P, S> {
    /// Replaces the keyed-state session descriptor binds route to.
    #[must_use]
    pub fn with_session<S2>(self, session: S2) -> MockEventContext<P, S2> {
        MockEventContext {
            shutdown_tx: self.shutdown_tx,
            shutdown_rx: self.shutdown_rx,
            cancel_tx: self.cancel_tx,
            cancel_rx: self.cancel_rx,
            timer_operations: self.timer_operations,
            durable_timers: self.durable_timers,
            timer_fail_count: self.timer_fail_count,
            timer_fail_category: self.timer_fail_category,
            shutdown_on_timer_read: self.shutdown_on_timer_read,
            session,
            _payload: PhantomData,
        }
    }

    /// Seeds a durable timer standing before the test runs — simulating a
    /// prior epoch's trigger surviving a partition reacquisition.
    #[must_use]
    pub fn with_durable_timer(self, time: CompactDateTime, timer_type: TimerType) -> Self {
        self.durable_timers.lock().push((time, timer_type));
        self
    }

    /// The durable scheduled times of `timer_type` (what `scheduled` answers).
    #[must_use]
    pub fn durable_scheduled(&self, timer_type: TimerType) -> Vec<CompactDateTime> {
        self.durable_timers
            .lock()
            .iter()
            .filter(|(_, t)| *t == timer_type)
            .map(|(time, _)| *time)
            .collect()
    }

    /// Make the first `count` timer schedules fail with `category`, then
    /// succeed — so the backstop arm's retry-forever loop self-heals after
    /// `count` retries. Run on a paused clock so the retry backoff advances
    /// instantly.
    #[must_use]
    pub fn with_timer_failures(self, count: usize, category: ErrorCategory) -> Self {
        Self {
            timer_fail_count: Arc::new(AtomicUsize::new(count)),
            timer_fail_category: category,
            ..self
        }
    }

    /// Start in shutdown state.
    ///
    /// Used for testing early-exit behavior in middleware.
    #[must_use]
    pub fn with_shutdown(self) -> Self {
        self.shutdown_tx.send_replace(ShutdownPhase::Cancelling);
        self
    }

    /// Flip the shutdown watch as a side effect of every `scheduled()` read —
    /// deterministic "shutdown arrives during the backstop arm" for the settle
    /// boundary's arm-shutdown rollback test (the read completes, then the
    /// arm's next retry step sees shutdown at its loop top).
    #[must_use]
    pub fn with_shutdown_on_timer_read(self) -> Self {
        Self {
            shutdown_on_timer_read: true,
            ..self
        }
    }

    /// Enable timer operation tracking.
    ///
    /// Use `timer_operations()` to retrieve recorded operations.
    #[must_use]
    pub fn with_timer_tracking(self) -> Self {
        Self {
            timer_operations: Some(Arc::new(Mutex::new(Vec::new()))),
            ..self
        }
    }

    /// Trigger partition/consumer shutdown signal.
    pub fn request_shutdown(&self) {
        self.shutdown_tx.send_replace(ShutdownPhase::Cancelling);
    }

    /// Trigger message-level cancellation signal.
    pub fn request_cancellation(&self) {
        self.cancel_tx.send_replace(true);
    }

    /// Get all recorded timer operations.
    ///
    /// Returns empty vec if timer tracking is not enabled.
    #[must_use]
    pub fn timer_operations(&self) -> Vec<TimerOperation> {
        self.timer_operations
            .as_ref()
            .map(|ops| ops.lock().clone())
            .unwrap_or_default()
    }

    /// Count scheduled timers of a specific type.
    ///
    /// Returns 0 if timer tracking is not enabled.
    #[must_use]
    pub fn count_scheduled(&self, timer_type: TimerType) -> usize {
        self.timer_operations.as_ref().map_or(0, |ops| {
            ops.lock()
                .iter()
                .filter(|op| match op {
                    TimerOperation::Schedule(_, t) | TimerOperation::ClearAndSchedule(_, t) => {
                        *t == timer_type
                    }
                    _ => false,
                })
                .count()
        })
    }

    /// Record a timer operation (internal helper).
    fn record(&self, op: TimerOperation) {
        if let Some(ops) = &self.timer_operations {
            ops.lock().push(op);
        }
    }

    /// The result a schedule returns: a `with_timer_failures` failure (of the
    /// configured category) while the countdown is still positive (decrementing
    /// it), otherwise success.
    fn timer_result(&self) -> Result<(), MockTimerError> {
        // fetch_update fails (leaving 0) once the countdown is exhausted; while
        // positive it decrements and we inject one more failure.
        match self
            .timer_fail_count
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
        {
            Ok(_) => Err(MockTimerError(self.timer_fail_category)),
            Err(_) => Ok(()),
        }
    }
}

impl<P, S> TerminationSignals for MockEventContext<P, S> {
    fn is_shutdown(&self) -> bool {
        *self.shutdown_rx.borrow() >= ShutdownPhase::Cancelling
    }

    fn is_message_cancelled(&self) -> bool {
        *self.cancel_rx.borrow()
    }

    fn on_shutdown(&self) -> impl Future<Output = ()> + Send + 'static {
        let mut rx = self.shutdown_rx.clone();
        async move {
            let _ = rx.wait_for(|v| *v >= ShutdownPhase::Cancelling).await;
        }
    }

    fn on_message_cancelled(&self) -> impl Future<Output = ()> + Send + 'static {
        let mut rx = self.cancel_rx.clone();
        async move {
            let _ = rx.wait_for(|&v| v).await;
        }
    }
}

impl<P, S> EventContext for MockEventContext<P, S>
where
    P: Send + Sync + 'static,
    S: CellWrite<Loader: MessageLoader<Payload = P>>,
{
    type Error = MockTimerError;
    type Payload = P;
    type State = S;

    fn state<DESC>(&self, registered: Registered<DESC>) -> Result<DESC::Handle<S>, StateAccessError>
    where
        DESC: StateDescriptor,
    {
        registered.descriptor().bind(&self.session)
    }

    fn redispatch(&self, proof: RepinProof) -> Self {
        // Field-wise rebuild (mirrors `with_session`) re-pinning the session:
        // a `recording_session`-backed mock re-pins the real
        // `KeyedStateSession`; the default `UnavailableState` re-pin is a clone.
        Self {
            shutdown_tx: self.shutdown_tx.clone(),
            shutdown_rx: self.shutdown_rx.clone(),
            cancel_tx: self.cancel_tx.clone(),
            cancel_rx: self.cancel_rx.clone(),
            timer_operations: self.timer_operations.clone(),
            durable_timers: self.durable_timers.clone(),
            timer_fail_count: self.timer_fail_count.clone(),
            timer_fail_category: self.timer_fail_category,
            shutdown_on_timer_read: self.shutdown_on_timer_read,
            session: self.session.repin(proof),
            _payload: PhantomData,
        }
    }

    fn should_cancel(&self) -> bool {
        *self.shutdown_rx.borrow() >= ShutdownPhase::Cancelling || *self.cancel_rx.borrow()
    }

    fn on_cancel(&self) -> impl Future<Output = ()> + Send + 'static {
        let mut shutdown_rx = self.shutdown_rx.clone();
        let mut cancel_rx = self.cancel_rx.clone();
        async move {
            tokio::select! {
                _ = shutdown_rx.wait_for(|v| *v >= ShutdownPhase::Cancelling) => {}
                _ = cancel_rx.wait_for(|&v| v) => {}
            }
        }
    }

    fn cancel(&self) {
        self.cancel_tx.send_replace(true);
    }

    fn uncancel(&self) {
        self.cancel_tx.send_replace(false);
    }

    fn schedule(
        &self,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.record(TimerOperation::Schedule(time, timer_type));
        let result = self.timer_result();
        if result.is_ok() {
            self.durable_timers.lock().push((time, timer_type));
        }
        future::ready(result)
    }

    fn clear_and_schedule(
        &self,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.record(TimerOperation::ClearAndSchedule(time, timer_type));
        let result = self.timer_result();
        if result.is_ok() {
            // Type-scoped singleton overwrite, like the real store.
            let mut timers = self.durable_timers.lock();
            timers.retain(|(_, t)| *t != timer_type);
            timers.push((time, timer_type));
        }
        future::ready(result)
    }

    fn unschedule(
        &self,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.record(TimerOperation::Unschedule(time, timer_type));
        self.durable_timers
            .lock()
            .retain(|entry| *entry != (time, timer_type));
        future::ready(Ok(()))
    }

    fn clear_scheduled(
        &self,
        timer_type: TimerType,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.record(TimerOperation::ClearScheduled(timer_type));
        self.durable_timers.lock().retain(|(_, t)| *t != timer_type);
        future::ready(Ok(()))
    }

    fn scheduled(
        &self,
        timer_type: TimerType,
    ) -> impl Future<Output = Result<Vec<CompactDateTime>, Self::Error>> + Send + 'static {
        if self.shutdown_on_timer_read {
            self.request_shutdown();
        }
        future::ready(Ok(self.durable_scheduled(timer_type)))
    }
}

/// Test-only convenience accessor to the full settlement session, standing in
/// for the deleted crate-wide `lifecycle()`. Production enforcement holds — the
/// settlement surface is reachable in shipping code only through settle's own
/// private `SettlementAccess` — while tests that legitimately drive
/// `finalize` / `get` / backstop accessors through the event's own session
/// keep a one-call binder. The North Star is production leak-fencing; tests are
/// allowed broad access.
pub trait TestLifecycleAccess: EventContext {
    /// Binds the event's session through [`LifecycleAccess`], returning it so
    /// the test drives the sealed lifecycle. Fails only when the context is
    /// terminated.
    fn test_lifecycle(&self) -> Result<Self::State, StateAccessError> {
        self.state(Registered::new(LifecycleAccess))
    }
}

impl<C: EventContext> TestLifecycleAccess for C {}

/// Test error carrying its classification. Display matches the per-file
/// originals (`test error (Transient)`) so no assertion text changes.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("test error ({0:?})")]
pub struct TestError(pub ErrorCategory);

impl ClassifyError for TestError {
    fn classify_error(&self) -> ErrorCategory {
        self.0
    }
}

/// A hook firing recorded by [`ScriptedHandler`], projected onto
/// `ErrorCategory` so logs stay `Eq`-comparable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScriptedHook {
    /// `on_message`/`on_timer` was invoked with this demand type.
    Invoke(DemandType),
    /// `after_commit` fired with this result shape.
    AfterCommit(Result<(), ErrorCategory>),
    /// `after_abort` fired with this result shape.
    AfterAbort(Result<(), ErrorCategory>),
}

/// Scripted `FallibleHandler` double: succeeds, fails a fixed sequence then
/// succeeds, or fails every call; counts calls, records demand types and hook
/// firings, and can request shutdown on invocation (simulating shutdown
/// mid-handler).
#[derive(Clone)]
pub struct ScriptedHandler {
    /// Remaining scripted failures, consumed front-first; empty ⇒ succeed.
    failures: Arc<Mutex<Vec<ErrorCategory>>>,
    /// When set, every call fails with this category — an explicit sticky
    /// failure, never a "long enough" sequence.
    sticky: Option<ErrorCategory>,
    calls: Arc<AtomicUsize>,
    demand_types: Arc<Mutex<Vec<DemandType>>>,
    hooks: Arc<Mutex<Vec<ScriptedHook>>>,
    /// When set, `request_shutdown()` fires on every invocation.
    shutdown_on_call: Option<MockEventContext>,
}

impl ScriptedHandler {
    /// A handler that succeeds on every call.
    #[must_use]
    pub fn success() -> Self {
        Self {
            failures: Arc::default(),
            sticky: None,
            calls: Arc::default(),
            demand_types: Arc::default(),
            hooks: Arc::default(),
            shutdown_on_call: None,
        }
    }

    /// A handler that fails `failures` front-first, then succeeds.
    #[must_use]
    pub fn failing_then_success(failures: Vec<ErrorCategory>) -> Self {
        Self {
            failures: Arc::new(Mutex::new(failures)),
            ..Self::success()
        }
    }

    /// A handler that fails every call with `category`.
    #[must_use]
    pub fn always_failing(category: ErrorCategory) -> Self {
        Self {
            sticky: Some(category),
            ..Self::success()
        }
    }

    /// Fire `request_shutdown()` on `ctx` at every invocation.
    #[must_use]
    pub fn with_shutdown_on_call(mut self, ctx: MockEventContext) -> Self {
        self.shutdown_on_call = Some(ctx);
        self
    }

    /// How many times the handler was invoked.
    #[must_use]
    pub fn call_count(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    /// The demand types the handler was invoked with, in order.
    #[must_use]
    pub fn recorded_demand_types(&self) -> Vec<DemandType> {
        self.demand_types.lock().clone()
    }

    /// The hook firings recorded so far, in order.
    #[must_use]
    pub fn hook_events(&self) -> Vec<ScriptedHook> {
        self.hooks.lock().clone()
    }

    /// One invocation: count it, record the demand type and invoke hook, fire
    /// any scripted shutdown, then return the scripted result.
    fn dispatch(&self, demand_type: DemandType) -> Result<(), TestError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.demand_types.lock().push(demand_type);
        self.hooks.lock().push(ScriptedHook::Invoke(demand_type));
        if let Some(ctx) = &self.shutdown_on_call {
            ctx.request_shutdown();
        }
        if let Some(category) = self.sticky {
            return Err(TestError(category));
        }
        let mut failures = self.failures.lock();
        if failures.is_empty() {
            Ok(())
        } else {
            Err(TestError(failures.remove(0)))
        }
    }
}

impl FallibleHandler for ScriptedHandler {
    type Error = TestError;
    type Output = ();
    type Payload = Value;

    async fn on_message<C>(
        &self,
        _context: C,
        _message: ConsumerMessage<Self::Payload>,
        demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        self.dispatch(demand_type)
    }

    async fn on_timer<C>(
        &self,
        _context: C,
        _trigger: Trigger,
        demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        self.dispatch(demand_type)
    }

    async fn after_commit<C>(&self, _context: C, result: Result<Self::Output, Self::Error>)
    where
        C: EventContext<Payload = Self::Payload>,
    {
        self.hooks
            .lock()
            .push(ScriptedHook::AfterCommit(result.map_err(|TestError(c)| c)));
    }

    async fn after_abort<C>(&self, _context: C, result: Result<Self::Output, Self::Error>)
    where
        C: EventContext<Payload = Self::Payload>,
    {
        self.hooks
            .lock()
            .push(ScriptedHook::AfterAbort(result.map_err(|TestError(c)| c)));
    }

    async fn shutdown(self) {}
}

impl SettlementHandler for ScriptedHandler {
    fn settlement(_result: Result<&Self::Output, &Self::Error>) -> Settlement {
        Settlement::Final
    }
}

/// Probe leaf whose settlement classification is `Bypassed` for every
/// result, so a wrapper's delegating rows are provably delegating in the
/// classification tables — a wrapper hardcoding `Final` fails against it.
#[derive(Clone)]
pub struct BypassedHandler;

impl FallibleHandler for BypassedHandler {
    type Error = TestError;
    type Output = ();
    type Payload = Value;

    async fn on_message<C>(
        &self,
        _context: C,
        _message: ConsumerMessage<Self::Payload>,
        _demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        Ok(())
    }

    async fn on_timer<C>(
        &self,
        _context: C,
        _trigger: Trigger,
        _demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        Ok(())
    }

    async fn shutdown(self) {}
}

impl SettlementHandler for BypassedHandler {
    fn settlement(_result: Result<&Self::Output, &Self::Error>) -> Settlement {
        Settlement::Bypassed
    }
}

/// Wraps `value` in a `ConsumerMessage` holding a fresh capacity permit.
///
/// Surfaces the (never-in-practice) permit-acquisition failure rather than
/// swallowing it, per the testing rules.
pub fn create_test_message_from(
    value: ConsumerMessageValue<Value>,
) -> color_eyre::Result<ConsumerMessage<Value>> {
    let semaphore = Arc::new(Semaphore::new(10));
    let permit = semaphore.try_acquire_owned()?;
    Ok(ConsumerMessage::new(value, Span::current(), permit))
}

/// [`create_test_message_from`] over the default value (topic `test-topic`,
/// partition 0, offset 0, key `test-key`).
pub fn create_test_message() -> color_eyre::Result<ConsumerMessage<Value>> {
    create_test_message_from(ConsumerMessageValue::default())
}

/// A trigger with the given key, fire time (seconds), and type.
#[must_use]
pub fn create_test_trigger_with(key: &str, time: u32, timer_type: TimerType) -> Trigger {
    Trigger::for_testing(Arc::from(key), CompactDateTime::from(time), timer_type)
}

/// Guard recording whether the trigger committed or aborted.
pub struct RecordingTimerGuard {
    committed: Arc<AtomicUsize>,
    aborted: Arc<AtomicUsize>,
}

impl Uncommitted for RecordingTimerGuard {
    async fn commit(self) {
        self.committed.fetch_add(1, Ordering::SeqCst);
    }

    async fn abort(self) {
        self.aborted.fetch_add(1, Ordering::SeqCst);
    }
}

/// Minimal [`UncommittedTimer`](crate::timers::UncommittedTimer) over a fixed
/// trigger, so a timer dispatch can run through `EventHandler::on_timer` and
/// its settle sequence. Returns `(timer, committed, aborted)` counters.
pub struct RecordingTimer {
    trigger: Trigger,
    committed: Arc<AtomicUsize>,
    aborted: Arc<AtomicUsize>,
}

impl RecordingTimer {
    /// A timer over `trigger` plus its shared commit/abort counters.
    #[must_use]
    pub fn new(trigger: Trigger) -> (Self, Arc<AtomicUsize>, Arc<AtomicUsize>) {
        let committed = Arc::new(AtomicUsize::new(0));
        let aborted = Arc::new(AtomicUsize::new(0));
        (
            Self {
                trigger,
                committed: committed.clone(),
                aborted: aborted.clone(),
            },
            committed,
            aborted,
        )
    }
}

impl Keyed for RecordingTimer {
    type Key = Key;

    fn key(&self) -> &Self::Key {
        &self.trigger.key
    }
}

impl Uncommitted for RecordingTimer {
    async fn commit(self) {
        self.committed.fetch_add(1, Ordering::SeqCst);
    }

    async fn abort(self) {
        self.aborted.fetch_add(1, Ordering::SeqCst);
    }
}

impl UncommittedTimer for RecordingTimer {
    type CommitGuard = RecordingTimerGuard;

    fn time(&self) -> CompactDateTime {
        self.trigger.time
    }

    fn timer_type(&self) -> TimerType {
        self.trigger.timer_type
    }

    fn span(&self) -> Span {
        Span::current()
    }

    fn into_inner(self) -> (Trigger, Self::CommitGuard) {
        let guard = RecordingTimerGuard {
            committed: self.committed.clone(),
            aborted: self.aborted.clone(),
        };
        (self.trigger, guard)
    }
}

/// [`create_test_trigger_with`] over defaults: `test-key`, t=1000, default
/// type.
#[must_use]
pub fn create_test_trigger() -> Trigger {
    create_test_trigger_with("test-key", 1000, TimerType::default())
}

/// Outcome a scripted defer `OutcomeHandler` returns for its next dispatch.
#[derive(Clone, Copy, Debug)]
pub enum HandlerOutcome {
    /// Handler succeeds.
    Success,
    /// Handler fails with a permanent error.
    Permanent,
    /// Handler fails with a transient error.
    Transient,
}

impl HandlerOutcome {
    /// The `FallibleHandler` result this outcome dictates.
    pub fn into_result(self) -> Result<(), OutcomeError> {
        match self {
            Self::Success => Ok(()),
            Self::Permanent => Err(OutcomeError::Permanent),
            Self::Transient => Err(OutcomeError::Transient),
        }
    }
}

/// Error a scripted [`HandlerOutcome`] dictates. Two variants only: the defer
/// suites never script a Terminal failure (the old struct's dead Terminal arm
/// was unreachable on both sides).
#[derive(Clone, Copy, Debug, Error)]
pub enum OutcomeError {
    /// A permanent handler failure.
    #[error("permanent test error")]
    Permanent,
    /// A transient handler failure.
    #[error("transient test error")]
    Transient,
}

impl ClassifyError for OutcomeError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::Permanent => ErrorCategory::Permanent,
            Self::Transient => ErrorCategory::Transient,
        }
    }
}

/// One-shot outcome slot shared by the defer `OutcomeHandler`s: the harness
/// sets the next outcome before each dispatch; taking it defaults to
/// [`HandlerOutcome::Success`].
#[derive(Clone, Debug, Default)]
pub struct OutcomeSlot(Arc<Mutex<Option<HandlerOutcome>>>);

impl OutcomeSlot {
    /// Queues the outcome for the next dispatch.
    pub fn set(&self, outcome: HandlerOutcome) {
        *self.0.lock() = Some(outcome);
    }

    /// Takes the queued outcome, defaulting to Success when none was set.
    #[must_use]
    pub fn take(&self) -> HandlerOutcome {
        self.0.lock().take().unwrap_or(HandlerOutcome::Success)
    }
}

// =========================================================================
// Recording-session harness for the settlement-boundary marker tests
// =========================================================================
//
// The marker-hygiene triangle — retry between attempts, settle on the final
// outcome, and every defer/route Err→Ok swallow — shares one observable
// contract: a `Bypassed` or discarded attempt's buffered writes never commit
// and no marker records for it. These parts build a **real**
// `KeyedStateSession` whose marker record routes through a recording oracle
// so each seam's test can assert that contract directly.

/// Oracle that logs every marker `settle` records and always resolves
/// Committed, so a test can read back exactly which markers `settle` certified.
#[derive(Clone)]
pub struct RecordingOracle {
    recorded: Arc<Mutex<Vec<Uuid>>>,
}

impl RecordingOracle {
    /// A fresh oracle with an empty log.
    #[must_use]
    pub fn new() -> Self {
        Self {
            recorded: Arc::default(),
        }
    }

    /// The shared log this oracle pushes every recorded marker into.
    #[must_use]
    pub fn recorded(&self) -> Arc<Mutex<Vec<Uuid>>> {
        self.recorded.clone()
    }
}

impl Default for RecordingOracle {
    fn default() -> Self {
        Self::new()
    }
}

impl CommitOracle for RecordingOracle {
    type Error = Infallible;

    async fn record_message(&self, dedup_id: Uuid) -> Result<(), Self::Error> {
        self.recorded.lock().push(dedup_id);
        Ok(())
    }

    async fn resolve<'a>(
        &'a self,
        _state_key: &'a StateKey,
        _event: EventRef,
    ) -> Result<CommitDecision, Self::Error> {
        Ok(CommitDecision::Committed)
    }
}

/// Backend of a [`recording_session`].
pub type RecordingBackend = PartitionBackend<
    RecordingOracle,
    MemoryDescriptorIdentityStore,
    MemoryCellStore<RecordingOracle>,
>;

/// Session type built by [`recording_session`].
pub type RecordingSession = KeyedStateSession<RecordingBackend, MemoryLoader<Value>>;

/// What [`recording_session`] hands back: the session, its durable cell store
/// (a clone sharing the durable `Arc`), the session's dirty store, and the
/// shared log of every marker the oracle recorded — the surfaces the
/// settlement-boundary marker tests assert on.
pub type RecordingParts = (
    RecordingSession,
    MemoryCellStore<RecordingOracle>,
    Arc<DirtyStore>,
    Arc<Mutex<Vec<Uuid>>>,
);

/// A real session over `registry` for `state_key` and `event` (the identity
/// the settle boundary reads its marker from); see [`RecordingParts`] for
/// the returned surfaces.
#[must_use]
pub fn recording_session(
    registry: CollectionDefRegistry,
    state_key: StateKey,
    event: EventRef,
) -> RecordingParts {
    recording_session_with_loader(registry, state_key, event, MemoryLoader::new())
}

/// [`recording_session`] over a caller-supplied loader, for reload tests
/// that seed messages into it.
#[must_use]
pub fn recording_session_with_loader(
    registry: CollectionDefRegistry,
    state_key: StateKey,
    event: EventRef,
    loader: MemoryLoader<Value>,
) -> RecordingParts {
    let registry = Arc::new(registry);
    let oracle = RecordingOracle::new();
    let recorded = oracle.recorded();
    let cell_store = MemoryCellStore::new(MemoryCells::new(), oracle.clone(), registry.clone());
    let dirty = Arc::new(DirtyStore::new());
    let (_shutdown_tx, shutdown_rx) = watch::channel(ShutdownPhase::default());
    let (_cancel_tx, cancel_rx) = watch::channel(false);
    let session = KeyedStateSession::new(SessionParts {
        cell: cell_store.clone(),
        dirty: dirty.clone(),
        oracle,
        loader,
        registry,
        state_key,
        event,
        recovery_delay: CompactDuration::new(30),
        armed: Arc::default(),
        termination: TerminationWatch::new(shutdown_rx, cancel_rx),
        publisher: None,
    });
    (session, cell_store, dirty, recorded)
}

/// The committed value at the single Value cell of `name` under `state_key`,
/// failing on a store read error or undecodable bytes.
pub async fn committed_json_value(
    cell_store: &MemoryCellStore<RecordingOracle>,
    state_key: StateKey,
    name: &str,
) -> color_eyre::Result<Option<Value>> {
    let id = CollectionId::new(state_key, StateType::Application, StateName::try_new(name)?);
    let cell = CellKey {
        section: Section::new(0),
        coordinate: Coordinate::empty(),
    };
    let probe = EventRef::Message {
        dedup_id: Uuid::from_u128(u128::MAX),
    };
    match Committed::into_inner(cell_store.get(&id, &cell, probe).await?) {
        Some(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
        None => Ok(None),
    }
}

/// Error of a [`StagingTransientHandler`] attempt, carrying its
/// classification.
#[derive(Debug, Error)]
#[error("staging attempt failed ({0:?})")]
pub struct StagingError(pub ErrorCategory);

impl ClassifyError for StagingError {
    fn classify_error(&self) -> ErrorCategory {
        self.0
    }
}

/// Which apply hook a [`StagingTransientHandler`] observed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StagingHook {
    /// `after_commit` fired — the dispatch was final.
    Commit,
    /// `after_abort` fired — the attempt was rolled back; a retry is coming.
    Abort,
}

/// Inner handler for the defer/route swallow tests: on every dispatch it
/// binds `cart`, buffers one write, then fails Transient — the exact attempt
/// whose swallow the settle boundary must classify `Bypassed` so nothing
/// stages and no marker records. Records its apply hooks so a test can prove
/// the swallow path (not a surfaced error) handled the dispatch.
#[derive(Clone, Default)]
pub struct StagingTransientHandler {
    hooks: Arc<Mutex<Vec<StagingHook>>>,
}

impl StagingTransientHandler {
    /// A handler staging one `cart` write per attempt.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The `cart` collection every attempt writes; register it in the
    /// session's registry.
    #[must_use]
    pub fn collection() -> ValueDescriptor {
        value_state("cart")
    }

    /// The apply hooks observed so far, in order.
    #[must_use]
    pub fn hooks(&self) -> Vec<StagingHook> {
        self.hooks.lock().clone()
    }

    /// One failed attempt: buffer a `cart` write, fail Transient.
    async fn stage<C>(&self, context: &C) -> Result<(), StagingError>
    where
        C: EventContext<Payload = Value>,
    {
        let handle = context
            .state(Registered::new(Self::collection()))
            .map_err(|_| StagingError(ErrorCategory::Terminal))?;
        handle
            .set(json!({ "attempt": 1_i32 }))
            .await
            .map_err(|_| StagingError(ErrorCategory::Terminal))?;
        Err(StagingError(ErrorCategory::Transient))
    }
}

impl FallibleHandler for StagingTransientHandler {
    type Error = StagingError;
    type Output = ();
    type Payload = Value;

    async fn on_message<C>(
        &self,
        context: C,
        _message: ConsumerMessage<Self::Payload>,
        _demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        self.stage(&context).await
    }

    async fn on_timer<C>(
        &self,
        context: C,
        _trigger: Trigger,
        _demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        self.stage(&context).await
    }

    async fn after_commit<C>(&self, _context: C, _result: Result<Self::Output, Self::Error>)
    where
        C: EventContext<Payload = Self::Payload>,
    {
        self.hooks.lock().push(StagingHook::Commit);
    }

    async fn after_abort<C>(&self, _context: C, _result: Result<Self::Output, Self::Error>)
    where
        C: EventContext<Payload = Self::Payload>,
    {
        self.hooks.lock().push(StagingHook::Abort);
    }

    async fn shutdown(self) {}
}

impl SettlementHandler for StagingTransientHandler {
    fn settlement(_result: Result<&Self::Output, &Self::Error>) -> Settlement {
        Settlement::Final
    }
}
