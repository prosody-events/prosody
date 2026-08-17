use super::*;

/// Test error carrying its classification. Display matches the per-file
/// originals (`test error (Transient)`) so no assertion text changes.
#[derive(Debug, PartialEq, Eq, Error)]
#[error("test error ({0:?})")]
pub struct TestError(pub ErrorCategory);

impl ClassifyError for TestError {
    fn classify_error(&self) -> ErrorCategory {
        self.0
    }
}

/// An [`EventHandler`] that commits every event and records nothing.
///
/// Startup and lifecycle tests build a real consumer and deliver nothing to it.
/// This is the handler they give it.
#[derive(Clone)]
pub struct SilentHandler;

impl EventHandler for SilentHandler {
    type Payload = Value;

    async fn on_message<C>(
        &self,
        _context: C,
        message: UncommittedMessage<Value>,
        _demand_type: DemandType,
    ) where
        C: EventContext<Payload = Self::Payload>,
    {
        let (_, uncommitted) = message.into_inner();
        uncommitted.commit().await;
    }

    async fn on_excise<C>(
        &self,
        _context: C,
        message: UncommittedMessage<()>,
        _demand_type: DemandType,
    ) where
        C: EventContext<Payload = Self::Payload>,
    {
        message.commit().await;
    }

    async fn on_timer<C, T>(&self, _context: C, timer: T, _demand_type: DemandType)
    where
        C: EventContext<Payload = Self::Payload>,
        T: UncommittedTimer,
    {
        timer.commit().await;
    }

    async fn shutdown(self) {}
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
    excisions: Arc<AtomicUsize>,
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
            excisions: Arc::default(),
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

    /// How many excise records the handler received.
    #[must_use]
    pub fn excision_count(&self) -> usize {
        self.excisions.load(Ordering::SeqCst)
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

    async fn on_excise<C>(
        &self,
        _context: C,
        _message: ConsumerMessage<()>,
        demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        self.excisions.fetch_add(1, Ordering::SeqCst);
        self.dispatch(demand_type)
    }
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

    async fn on_excise<C>(
        &self,
        _context: C,
        _message: ConsumerMessage<()>,
        _demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        Ok(())
    }

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
pub fn create_test_message_from<P>(
    value: ConsumerMessageValue<P>,
) -> color_eyre::Result<ConsumerMessage<P>> {
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
