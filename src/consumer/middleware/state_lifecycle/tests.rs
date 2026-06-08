//! Directed and property tests for the thin keyed-state lifecycle
//! middleware.
//!
//! Sessions are minted from a real [`StateManager`] (the partition loop's
//! mint path) and ride a [`MockEventContext`], so every test exercises the
//! exact `ctx.state(…)` route production handlers use. Covered:
//!
//! * seal → arm `StateRecovery` → marker → apply, with the
//!   timer-cleared-only-on-all-resolved invariant;
//! * the abort rollback arm;
//! * the defer-swallow session reset (successor of the deleted test that pinned
//!   the pre-rework partial-seal defect);
//! * the retry attempt-boundary session reset;
//! * the N10/T4 dispatch property re-pointed at manager-minted sessions through
//!   real lifecycle dispatch.
//!
//! [`StateManager`]: crate::state::manager::StateManager

use super::*;
use crate::codec::{Codec, JsonCodec};
use crate::consumer::Keyed;
use crate::consumer::message::ConsumerMessage;
use crate::consumer::middleware::deduplication::{DedupIdentity, dedup_uuid_for_message};
use crate::consumer::middleware::defer::config::DeferConfiguration;
use crate::consumer::middleware::defer::decider::TraceBasedDecider;
use crate::consumer::middleware::defer::segment::compute_segment_id;
use crate::consumer::middleware::defer::timer::handler::TimerDeferHandler;
use crate::consumer::middleware::defer::timer::store::memory::MemoryTimerDeferStore;
use crate::consumer::middleware::retry::{RetryConfiguration, RetryMiddleware};
use crate::consumer::middleware::tests::test_support::{MockEventContext, TimerOperation};
use crate::consumer::middleware::{FallibleHandler, FallibleHandlerProvider, HandlerMiddleware};
use crate::consumer::partition::ShutdownPhase;
use crate::loader::MemoryLoader;
use crate::state::descriptor::{ValueDescriptor, ValueStateError, value_state};
use crate::state::manager::{
    PartitionStateManager, PartitionStateProvider, StateManager, StateManagerProvider,
};
use crate::state::memory::{MemoryDirtyValueStoreProvider, MemoryDurableValueStore};
use crate::state::registry::{CollectionDef, CollectionDefRegistry};
use crate::state::session::{TerminationWatch, ValueStateSession};
use crate::state::tests::value_suite::{FixedOracle, finish_trace};
use crate::state::value::DurableWalStore;
use crate::state::{
    CollectionId, CollectionRef, CommitMode, DurableState, EventRef, SharedStateBackend, StateKey,
    StateName, StateType, TimerEventRef, ValueKind,
};
use crate::telemetry::Telemetry;
use crate::timers::Trigger;
use crate::timers::datetime::CompactDateTime as TestDateTime;
use crate::timers::duration::CompactDuration;
use crate::{Key, Offset, Topic};
use color_eyre::eyre::{Result, eyre};
use futures::executor;
use parking_lot::Mutex as SyncMutex;
use quickcheck::{Arbitrary, Gen, QuickCheck, TestResult};
use serde_json::Value;
use std::collections::VecDeque;
use std::iter;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;

const CART: ValueDescriptor = value_state("cart");

type TestManager = StateManager<
    MemoryDurableValueStore,
    FixedOracle,
    MemoryDirtyValueStoreProvider,
    MemoryDurableValueStore,
    MemoryLoader<Value>,
>;

type TestSession =
    ValueStateSession<MemoryDurableValueStore, MemoryDirtyValueStoreProvider, MemoryLoader<Value>>;

type TestContext = MockEventContext<Value, TestSession>;

fn registry_with_mode(mode: CommitMode) -> Result<Arc<CollectionDefRegistry>> {
    let mut registry = CollectionDefRegistry::new(Some(CompactDuration::new(3_600)));
    let def = CollectionDef::new(Some(CompactDuration::new(3_600))).with_commit_mode(mode);
    registry.register(&CART, def)?;
    Ok(Arc::new(registry))
}

/// Acquires a real state manager over the memory backend — the partition
/// loop's mint path for sessions.
async fn state_manager(durable: MemoryDurableValueStore, mode: CommitMode) -> Result<TestManager> {
    let provider = StateManagerProvider::new(
        SharedStateBackend::new(
            durable.clone(),
            FixedOracle::committed(),
            MemoryDirtyValueStoreProvider,
            durable,
        ),
        MemoryLoader::new(),
        registry_with_mode(mode)?,
        Arc::from("test-group"),
        CompactDuration::new(30),
    );
    provider
        .acquire(Topic::from("t"), 0)
        .await
        .map_err(|e| eyre!("acquire failed: {e}"))
}

fn termination() -> TerminationWatch {
    let (_shutdown_tx, shutdown_rx) = watch::channel(ShutdownPhase::default());
    let (_cancel_tx, cancel_rx) = watch::channel(false);
    TerminationWatch::new(shutdown_rx, cancel_rx)
}

fn test_message(offset: Offset) -> Result<ConsumerMessage<Value>> {
    ConsumerMessage::for_testing(Topic::from("t"), 0, offset, Arc::from("k"), Value::Null)
        .map_err(|e| eyre!("for_testing: {e}"))
}

/// Mints a session for `message` and wraps it in a tracking mock context —
/// exactly what the partition loop does per event.
fn message_context(manager: &TestManager, message: &ConsumerMessage<Value>) -> TestContext {
    // Mirror the partition loop: derive the dedup id through the canonical
    // function and hand the manager the resolved event ref.
    let dedup_id = dedup_uuid_for_message(
        DedupIdentity {
            version: "1",
            group_id: "test-group",
            topic: message.topic().as_ref(),
            partition: message.partition(),
        },
        message,
    );
    let session = manager.session(
        message.key().clone(),
        EventRef::Message { dedup_id },
        termination(),
    );
    MockEventContext::new()
        .with_timer_tracking()
        .with_session(session)
}

fn timer_context(manager: &TestManager, trigger: &Trigger) -> TestContext {
    let event = EventRef::Timer(TimerEventRef::new(
        trigger.timer_type,
        trigger.time,
        trigger.tag,
    ));
    let session = manager.session(trigger.key.clone(), event, termination());
    MockEventContext::new()
        .with_timer_tracking()
        .with_session(session)
}

fn cart_collection_id(manager_key: &Key) -> Result<CollectionId<ValueKind>> {
    Ok(CollectionId::new(
        StateKey::new(
            compute_segment_id(Topic::from("t"), 0, "test-group"),
            manager_key.clone(),
        ),
        StateType::Application,
        StateName::try_new("cart")?,
    ))
}

/// Read a partition expected to be `Idle`, returning its applied cell
/// decoded as the JSON value the default codec wrote.
async fn read_idle_applied(
    durable: &MemoryDurableValueStore,
    id: &CollectionId<ValueKind>,
) -> Result<Option<Value>> {
    match DurableWalStore::read_partition(durable, id).await? {
        DurableState::Idle { applied } => applied
            .map(|cell| serde_json::from_slice::<Value>(&cell))
            .transpose()
            .map_err(|e| eyre!("applied cell is not the codec's JSON: {e}")),
        other @ DurableState::Sealed { .. } => Err(eyre!("expected Idle, got {other:?}")),
    }
}

fn count_cleared(context: &TestContext) -> usize {
    context
        .timer_operations()
        .iter()
        .filter(|op| matches!(op, TimerOperation::ClearScheduled(TimerType::StateRecovery)))
        .count()
}

/// Handler that writes `CART = byte` and returns the configured result.
#[derive(Clone)]
struct WritingHandler {
    byte: u8,
    result: Result<(), ErrorCategory>,
}

impl WritingHandler {
    fn ok(byte: u8) -> Self {
        Self {
            byte,
            result: Ok(()),
        }
    }

    fn failing(byte: u8, category: ErrorCategory) -> Self {
        Self {
            byte,
            result: Err(category),
        }
    }

    async fn run<C>(&self, ctx: &C) -> Result<(), WritingError>
    where
        C: EventContext<Payload = Value>,
    {
        let cart = ctx.state(CART).map_err(ValueStateError::from)?;
        cart.set(Value::from(self.byte)).await?;
        match self.result {
            Ok(()) => Ok(()),
            Err(category) => Err(WritingError::Scripted(category)),
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum WritingError {
    #[error(transparent)]
    Cart(#[from] ValueStateError<<JsonCodec as Codec>::Error>),
    #[error("scripted failure")]
    Scripted(ErrorCategory),
}

impl ClassifyError for WritingError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::Cart(e) => e.classify_error(),
            Self::Scripted(category) => *category,
        }
    }
}

impl FallibleHandler for WritingHandler {
    type Error = WritingError;
    type Output = ();
    type Payload = Value;

    async fn on_message<C>(
        &self,
        ctx: C,
        _msg: ConsumerMessage<Value>,
        _demand: DemandType,
    ) -> Result<(), Self::Error>
    where
        C: EventContext<Payload = Value>,
    {
        self.run(&ctx).await
    }

    async fn on_timer<C>(
        &self,
        ctx: C,
        _trigger: Trigger,
        _demand: DemandType,
    ) -> Result<(), Self::Error>
    where
        C: EventContext<Payload = Value>,
    {
        self.run(&ctx).await
    }

    async fn shutdown(self) {}
}

fn lifecycle<T>(inner: T) -> StateLifecycleHandler<T> {
    StateLifecycleHandler { inner }
}

/// WAL mode: an inner `Ok` with dirty state seals the collection and arms
/// the `StateRecovery` backstop exactly once.
#[tokio::test]
async fn ok_with_dirty_state_seals_and_arms_recovery_timer() -> Result<()> {
    let durable = MemoryDurableValueStore::for_tests();
    let manager = state_manager(durable.clone(), CommitMode::Wal).await?;
    let handler = lifecycle(WritingHandler::ok(7));
    let message = test_message(0)?;
    let context = message_context(&manager, &message);

    handler
        .on_message(context.clone(), message, DemandType::Normal)
        .await
        .map_err(|e| eyre!("dispatch failed: {e}"))?;

    assert_eq!(
        context.count_scheduled(TimerType::StateRecovery),
        1,
        "a seal arms exactly one StateRecovery backstop"
    );
    let id = cart_collection_id(&Arc::from("k"))?;
    assert!(matches!(
        DurableWalStore::read_partition(&durable, &id).await?,
        DurableState::Sealed { .. }
    ));
    Ok(())
}

/// WAL mode with no state ops: nothing seals, nothing schedules.
#[tokio::test]
async fn ok_without_state_ops_does_not_schedule() -> Result<()> {
    /// Handler that never touches state.
    #[derive(Clone)]
    struct NoOp;
    impl FallibleHandler for NoOp {
        type Error = WritingError;
        type Output = ();
        type Payload = Value;

        async fn on_message<C>(
            &self,
            _ctx: C,
            _msg: ConsumerMessage<Value>,
            _demand: DemandType,
        ) -> Result<(), Self::Error>
        where
            C: EventContext<Payload = Value>,
        {
            Ok(())
        }

        async fn on_timer<C>(
            &self,
            _ctx: C,
            _trigger: Trigger,
            _demand: DemandType,
        ) -> Result<(), Self::Error>
        where
            C: EventContext<Payload = Value>,
        {
            Ok(())
        }

        async fn shutdown(self) {}
    }

    let durable = MemoryDurableValueStore::for_tests();
    let manager = state_manager(durable, CommitMode::Wal).await?;
    let handler = lifecycle(NoOp);
    let message = test_message(0)?;
    let context = message_context(&manager, &message);

    handler
        .on_message(context.clone(), message, DemandType::Normal)
        .await
        .map_err(|e| eyre!("dispatch failed: {e}"))?;
    assert_eq!(context.count_scheduled(TimerType::StateRecovery), 0);
    Ok(())
}

/// Direct mode: ops apply during finalize; no seal, no backstop timer.
#[tokio::test]
async fn direct_mode_applies_without_seal_or_timer() -> Result<()> {
    let durable = MemoryDurableValueStore::for_tests();
    let manager = state_manager(durable.clone(), CommitMode::Direct).await?;
    let handler = lifecycle(WritingHandler::ok(9));
    let message = test_message(0)?;
    let context = message_context(&manager, &message);

    handler
        .on_message(context.clone(), message, DemandType::Normal)
        .await
        .map_err(|e| eyre!("dispatch failed: {e}"))?;

    assert_eq!(context.count_scheduled(TimerType::StateRecovery), 0);
    let id = cart_collection_id(&Arc::from("k"))?;
    assert_eq!(
        read_idle_applied(&durable, &id).await?,
        Some(Value::from(9_i32)),
        "direct mode applies during finalize"
    );
    Ok(())
}

/// The commit lifecycle: seal → marker → `after_commit` applies the
/// recorded set and clears the backstop exactly once.
#[tokio::test]
async fn after_commit_applies_and_clears_timer() -> Result<()> {
    let durable = MemoryDurableValueStore::for_tests();
    let manager = state_manager(durable.clone(), CommitMode::Wal).await?;
    let handler = lifecycle(WritingHandler::ok(7));
    let message = test_message(0)?;
    let context = message_context(&manager, &message);

    let output = handler
        .on_message(context.clone(), message, DemandType::Normal)
        .await;
    handler.after_commit(context.clone(), output).await;

    assert_eq!(
        count_cleared(&context),
        1,
        "exactly one clear_scheduled(StateRecovery) fires"
    );
    let id = cart_collection_id(&Arc::from("k"))?;
    assert_eq!(
        read_idle_applied(&durable, &id).await?,
        Some(Value::from(7_i32))
    );
    Ok(())
}

/// C1: when an `apply_sealed` fails, `after_commit` must leave the
/// one-shot `StateRecovery` timer armed so the sweep retries. The failure
/// is induced by replacing the WAL out-of-band with one sealed under a
/// different event before the apply hook runs.
#[tokio::test]
async fn after_commit_apply_failure_leaves_timer_armed() -> Result<()> {
    use crate::state::EventRef;
    use crate::state::value::ValueOp;
    use uuid::Uuid;

    let durable = MemoryDurableValueStore::for_tests();
    let manager = state_manager(durable.clone(), CommitMode::Wal).await?;
    let handler = lifecycle(WritingHandler::ok(7));
    let message = test_message(0)?;
    let context = message_context(&manager, &message);

    let output = handler
        .on_message(context.clone(), message, DemandType::Normal)
        .await;

    // Replace the WAL with one sealed under a different event: the
    // session's recorded apply now mismatches and fails.
    let id = cart_collection_id(&Arc::from("k"))?;
    let collection_ref = CollectionRef::new(id.clone(), None);
    let foreign = EventRef::Message {
        dedup_id: Uuid::from_u128(999),
    };
    match DurableWalStore::read_partition(&durable, &id).await? {
        DurableState::Sealed { wal, .. } => {
            durable
                .rollback_sealed(&collection_ref, wal.event())
                .await?;
        }
        DurableState::Idle { .. } => return Err(eyre!("expected a sealed WAL")),
    }
    durable
        .seal(
            &collection_ref,
            foreign,
            vec![ValueOp::Set {
                payload: bytes::Bytes::from_static(b"61"),
            }],
        )
        .await?;

    handler.after_commit(context.clone(), output).await;

    assert_eq!(
        count_cleared(&context),
        0,
        "apply failure must leave the recovery timer armed"
    );
    assert!(matches!(
        DurableWalStore::read_partition(&durable, &id).await?,
        DurableState::Sealed { .. }
    ));
    Ok(())
}

/// The abort lifecycle: seal → marker abort → `after_abort` rolls the
/// recorded set back and clears the backstop.
#[tokio::test]
async fn after_abort_rolls_back_and_clears_timer() -> Result<()> {
    let durable = MemoryDurableValueStore::for_tests();
    let manager = state_manager(durable.clone(), CommitMode::Wal).await?;
    let handler = lifecycle(WritingHandler::ok(13));
    let message = test_message(0)?;
    let context = message_context(&manager, &message);

    let output = handler
        .on_message(context.clone(), message, DemandType::Normal)
        .await;
    handler.after_abort(context.clone(), output).await;

    assert_eq!(count_cleared(&context), 1);
    let id = cart_collection_id(&Arc::from("k"))?;
    assert_eq!(
        read_idle_applied(&durable, &id).await?,
        None,
        "rollback restored pre-seal state"
    );
    Ok(())
}

/// An inner error propagates as `Inner` and the lifecycle seals nothing —
/// finalize only runs on inner `Ok`.
#[tokio::test]
async fn inner_error_propagates_without_sealing() -> Result<()> {
    let durable = MemoryDurableValueStore::for_tests();
    let manager = state_manager(durable.clone(), CommitMode::Wal).await?;
    let handler = lifecycle(WritingHandler::failing(7, ErrorCategory::Permanent));
    let message = test_message(0)?;
    let context = message_context(&manager, &message);

    let err = handler
        .on_message(context.clone(), message, DemandType::Normal)
        .await
        .err()
        .ok_or_else(|| eyre!("inner error must propagate"))?;
    assert!(matches!(err, StateLifecycleError::Inner(_)));
    assert_eq!(context.count_scheduled(TimerType::StateRecovery), 0);

    let id = cart_collection_id(&Arc::from("k"))?;
    assert!(
        matches!(
            DurableWalStore::read_partition(&durable, &id).await?,
            DurableState::Idle { applied: None }
        ),
        "an errored dispatch must not seal its dirty ops"
    );
    Ok(())
}

/// A context whose session is the `UnavailableState` stub (a consumer
/// without keyed state) passes straight through: finalize is a no-op.
#[tokio::test]
async fn stateless_context_passes_through() -> Result<()> {
    #[derive(Clone)]
    struct Plain;
    impl FallibleHandler for Plain {
        type Error = WritingError;
        type Output = u8;
        type Payload = Value;

        async fn on_message<C>(
            &self,
            _ctx: C,
            _msg: ConsumerMessage<Value>,
            _demand: DemandType,
        ) -> Result<u8, Self::Error>
        where
            C: EventContext<Payload = Value>,
        {
            Ok(42)
        }

        async fn on_timer<C>(
            &self,
            _ctx: C,
            _trigger: Trigger,
            _demand: DemandType,
        ) -> Result<u8, Self::Error>
        where
            C: EventContext<Payload = Value>,
        {
            Ok(42)
        }

        async fn shutdown(self) {}
    }

    let handler = lifecycle(Plain);
    let context = MockEventContext::<Value>::new().with_timer_tracking();
    let output = handler
        .on_message(context.clone(), test_message(0)?, DemandType::Normal)
        .await
        .map_err(|e| eyre!("stateless dispatch failed: {e}"))?;
    assert_eq!(output, 42, "Output passes through untouched");
    assert_eq!(context.count_scheduled(TimerType::StateRecovery), 0);
    handler.after_commit(context, Ok(output)).await;
    Ok(())
}

/// Successor of the deleted
/// `defer_swallow_seals_failed_attempts_partial_writes_today` test: with the
/// session reset at the defer-swallow boundary, a handler that writes state and
/// fails Transient — absorbed by the timer-defer middleware into `Ok(Deferred)`
/// — seals **nothing**, and the apply hook leaves durable state untouched. The
/// failed attempt's partial write is gone; the `DeferredTimer` retry re-runs
/// the handler from clean state.
#[tokio::test]
async fn defer_swallow_resets_session_so_partial_writes_never_seal() -> Result<()> {
    let durable = MemoryDurableValueStore::for_tests();
    let manager = state_manager(durable.clone(), CommitMode::Wal).await?;

    let decider = TraceBasedDecider::new();
    decider.set_next(true);
    let defer_config = DeferConfiguration::builder()
        .enabled(true)
        .base(Duration::from_secs(1))
        .max_delay(Duration::from_hours(1))
        .failure_threshold(0.9_f64)
        .build()
        .map_err(|e| eyre!("config error: {e}"))?;
    let telemetry = Telemetry::new();
    let topic = Topic::from("t");
    let defer_handler = TimerDeferHandler {
        handler: WritingHandler::failing(7, ErrorCategory::Transient),
        store: MemoryTimerDeferStore::default(),
        decider,
        config: defer_config,
        topic,
        partition: 0,
        sender: telemetry.partition_sender(topic, 0),
        source: Arc::from("test"),
    };
    let handler = lifecycle(defer_handler);

    let key: Key = Arc::from("k");
    let trigger = Trigger::for_testing(
        key.clone(),
        TestDateTime::from(1_000_u32),
        TimerType::Application,
    );
    let context = timer_context(&manager, &trigger);

    let output = handler
        .on_timer(context.clone(), trigger, DemandType::Normal)
        .await
        .map_err(|e| eyre!("defer must swallow the transient error: {e}"))?;

    assert_eq!(
        context.count_scheduled(TimerType::StateRecovery),
        0,
        "the reset session finalizes empty — nothing seals, nothing arms"
    );

    handler.after_commit(context.clone(), Ok(output)).await;

    let id = cart_collection_id(&key)?;
    assert!(
        matches!(
            DurableWalStore::read_partition(&durable, &id).await?,
            DurableState::Idle { applied: None }
        ),
        "the failed attempt's partial write must not reach durable state"
    );
    Ok(())
}

/// Retry attempt boundaries reset the session: a flaky handler's failed
/// first attempt buffers a write that must be invisible to the second
/// attempt, and only the second attempt's value lands durably.
#[tokio::test]
async fn retry_reset_isolates_attempts() -> Result<()> {
    /// Writes a poison value and fails Transient on the first attempt;
    /// asserts clean state and writes the good value on the second.
    #[derive(Clone)]
    struct Flaky {
        attempts: Arc<SyncMutex<u32>>,
        saw_dirty_state: Arc<SyncMutex<bool>>,
    }

    impl FallibleHandler for Flaky {
        type Error = WritingError;
        type Output = ();
        type Payload = Value;

        async fn on_message<C>(
            &self,
            ctx: C,
            _msg: ConsumerMessage<Value>,
            _demand: DemandType,
        ) -> Result<(), Self::Error>
        where
            C: EventContext<Payload = Value>,
        {
            let attempt = {
                let mut attempts = self.attempts.lock();
                *attempts += 1;
                *attempts
            };
            let cart = ctx.state(CART).map_err(ValueStateError::from)?;
            if attempt == 1 {
                cart.set(Value::from(66_i32)).await?;
                Err(WritingError::Scripted(ErrorCategory::Transient))
            } else {
                if cart.get().await?.is_some() {
                    *self.saw_dirty_state.lock() = true;
                }
                cart.set(Value::from(7_i32)).await?;
                Ok(())
            }
        }

        async fn on_timer<C>(
            &self,
            _ctx: C,
            _trigger: Trigger,
            _demand: DemandType,
        ) -> Result<(), Self::Error>
        where
            C: EventContext<Payload = Value>,
        {
            Ok(())
        }

        async fn shutdown(self) {}
    }

    let durable = MemoryDurableValueStore::for_tests();
    let manager = state_manager(durable.clone(), CommitMode::Wal).await?;
    let flaky = Flaky {
        attempts: Arc::new(SyncMutex::new(0)),
        saw_dirty_state: Arc::new(SyncMutex::new(false)),
    };
    let saw_dirty_state = flaky.saw_dirty_state.clone();

    // The real production order: retry OUTSIDE the lifecycle layer.
    let retry = RetryMiddleware::new(
        RetryConfiguration::builder()
            .base(Duration::from_millis(1))
            .max_retries(3_u32)
            .max_delay(Duration::from_millis(2))
            .build()
            .map_err(|e| eyre!("retry config: {e}"))?,
    )
    .map_err(|e| eyre!("retry middleware: {e}"))?;
    let provider = StateLifecycleMiddleware.layer(retry).into_provider(flaky);
    let handler = provider.handler_for_partition(Topic::from("t"), 0);

    let message = test_message(0)?;
    let context = message_context(&manager, &message);
    handler
        .on_message(context.clone(), message, DemandType::Normal)
        .await
        .map_err(|e| eyre!("retried dispatch failed: {e}"))?;
    handler.after_commit(context.clone(), Ok::<(), _>(())).await;

    assert!(
        !*saw_dirty_state.lock(),
        "the failed attempt's dirty write must be invisible to the next attempt"
    );
    let id = cart_collection_id(&Arc::from("k"))?;
    assert_eq!(
        read_idle_applied(&durable, &id).await?,
        Some(Value::from(7_i32)),
        "only the successful attempt's value lands"
    );
    Ok(())
}

// --- N10/T4 dispatch property, re-pointed at manager-minted sessions ---

/// One step of a dispatch trace: a value op plus the event outcome the
/// apply hook receives.
#[derive(Clone, Debug)]
enum Step {
    Set(u8),
    Clear,
}

/// Which dispatch entrypoint carries the step. Both route to the same key,
/// so a trace interleaving them proves state persists across event kinds.
#[derive(Clone, Copy, Debug)]
enum EventKind {
    Message,
    Timer,
}

#[derive(Clone, Debug)]
struct TraceEvent {
    kind: EventKind,
    step: Step,
    commit: bool,
}

#[derive(Clone, Debug)]
struct DispatchTrace(Vec<TraceEvent>);

impl Arbitrary for Step {
    fn arbitrary(g: &mut Gen) -> Self {
        if bool::arbitrary(g) {
            Self::Set(u8::arbitrary(g))
        } else {
            Self::Clear
        }
    }
}

impl Step {
    /// Minimal shrink: a `Set` shrinks toward `Clear` and smaller bytes.
    fn shrink_step(self) -> Box<dyn Iterator<Item = Self>> {
        match self {
            Self::Clear => Box::new(iter::empty()),
            Self::Set(b) => Box::new(iter::once(Self::Clear).chain(b.shrink().map(Self::Set))),
        }
    }
}

impl Arbitrary for TraceEvent {
    fn arbitrary(g: &mut Gen) -> Self {
        let kind = if bool::arbitrary(g) {
            EventKind::Message
        } else {
            EventKind::Timer
        };
        Self {
            kind,
            step: Step::arbitrary(g),
            commit: bool::arbitrary(g),
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        let kind = self.kind;
        let commit = self.commit;
        let kind_shrink = match kind {
            EventKind::Message => None,
            EventKind::Timer => Some(Self {
                kind: EventKind::Message,
                step: self.step.clone(),
                commit,
            }),
        };
        Box::new(
            kind_shrink
                .into_iter()
                .chain(self.step.clone().shrink_step().map(move |step| Self {
                    kind,
                    step,
                    commit,
                })),
        )
    }
}

impl Arbitrary for DispatchTrace {
    fn arbitrary(g: &mut Gen) -> Self {
        let len = usize::arbitrary(g) % 12;
        Self((0..len).map(|_| TraceEvent::arbitrary(g)).collect())
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        Box::new(self.0.shrink().map(DispatchTrace))
    }
}

/// The error a [`ScriptedHandler`] surfaces: access or codec failure from
/// the `CART` handle.
type CartError = ValueStateError<<JsonCodec as Codec>::Error>;

/// N10 — a plain [`FallibleHandler`] whose *generic* methods reach keyed
/// state through `ctx.state(CART)`. Each dispatch records the cell value
/// observed *before* applying the next scripted op, so the property can
/// assert read-your-committed-writes across message and timer events.
#[derive(Clone)]
struct ScriptedHandler {
    script: Arc<SyncMutex<VecDeque<Step>>>,
    observed: Arc<SyncMutex<Vec<Option<Value>>>>,
}

impl ScriptedHandler {
    fn new() -> Self {
        Self {
            script: Arc::new(SyncMutex::new(VecDeque::new())),
            observed: Arc::new(SyncMutex::new(Vec::new())),
        }
    }

    async fn run_step<C>(&self, ctx: &C) -> Result<(), CartError>
    where
        C: EventContext<Payload = Value>,
    {
        let cart = ctx.state(CART)?;
        let before = cart.get().await?;
        self.observed.lock().push(before);
        let step = self.script.lock().pop_front();
        match step {
            Some(Step::Set(byte)) => cart.set(Value::from(byte)).await,
            Some(Step::Clear) => cart.clear().await,
            None => Ok(()),
        }
    }
}

impl FallibleHandler for ScriptedHandler {
    type Error = CartError;
    type Output = ();
    type Payload = Value;

    async fn on_message<C>(
        &self,
        ctx: C,
        _msg: ConsumerMessage<Value>,
        _demand: DemandType,
    ) -> Result<(), Self::Error>
    where
        C: EventContext<Payload = Value>,
    {
        self.run_step(&ctx).await
    }

    async fn on_timer<C>(
        &self,
        ctx: C,
        _trigger: Trigger,
        _demand: DemandType,
    ) -> Result<(), Self::Error>
    where
        C: EventContext<Payload = Value>,
    {
        self.run_step(&ctx).await
    }

    async fn shutdown(self) {}
}

/// N10/T4 — dispatch property, WAL arm: committed events apply the WAL,
/// aborted events roll it back. See [`run_dispatch_trace`].
#[test]
fn prop_lifecycle_dispatch_matches_durable_model_wal() {
    fn property(trace: DispatchTrace) -> TestResult {
        let input_dbg = format!("{trace:#?}");
        let result = executor::block_on(run_dispatch_trace(trace, CommitMode::Wal));
        finish_trace(
            result,
            "WAL dispatch diverged from durable model",
            &input_dbg,
        )
    }
    QuickCheck::new().quickcheck(property as fn(DispatchTrace) -> TestResult);
}

/// N10/T4 — dispatch property, Direct arm: every op applies during
/// dispatch; the apply hooks have nothing to undo, so an aborted event
/// leaves the op in place. See [`run_dispatch_trace`].
#[test]
fn prop_lifecycle_dispatch_matches_durable_model_direct() {
    fn property(trace: DispatchTrace) -> TestResult {
        let input_dbg = format!("{trace:#?}");
        let result = executor::block_on(run_dispatch_trace(trace, CommitMode::Direct));
        finish_trace(
            result,
            "Direct dispatch diverged from durable model",
            &input_dbg,
        )
    }
    QuickCheck::new().quickcheck(property as fn(DispatchTrace) -> TestResult);
}

/// Drives a trace end-to-end through the **real** post-rework chain: the
/// state manager mints one session per event (the partition loop's mint
/// path), the generic [`ScriptedHandler`] binds `CART` via `ctx.state(…)`
/// inside [`StateLifecycleHandler`] dispatch, and the seals resolve
/// through the real `after_commit` / `after_abort` apply hooks. Three
/// invariants per event:
///
/// 1. The handler observed the model's pre-state through `cart.get()` — state
///    persists across interleaved message and timer events on one key.
/// 2. The durable applied cell decodes to the model value (commit applies /
///    abort rolls back in `Wal`; both keep the op in `Direct`).
/// 3. The WAL is resolved — the partition always returns to `Idle`.
async fn run_dispatch_trace(trace: DispatchTrace, commit_mode: CommitMode) -> Result<bool> {
    let durable = MemoryDurableValueStore::for_tests();
    let manager = state_manager(durable.clone(), commit_mode).await?;
    let inner = ScriptedHandler::new();
    let handler = lifecycle(inner.clone());
    let key: Key = Arc::from("k");
    let id = cart_collection_id(&key)?;

    let mut model: Option<Value> = None;
    let mut expected_observed: Vec<Option<Value>> = Vec::new();

    for (idx, event) in trace.0.into_iter().enumerate() {
        expected_observed.push(model.clone());
        inner.script.lock().push_back(event.step.clone());

        let output = match event.kind {
            EventKind::Message => {
                // Distinct offsets give distinct dedup-derived events.
                let msg = test_message(idx as Offset)?;
                let context = message_context(&manager, &msg);
                let output = handler
                    .on_message(context.clone(), msg, DemandType::Normal)
                    .await;
                (context, output)
            }
            EventKind::Timer => {
                // Distinct times give distinct timer events.
                let trigger = Trigger::for_testing(
                    key.clone(),
                    TestDateTime::from(idx as u32 + 1),
                    TimerType::Application,
                );
                let context = timer_context(&manager, &trigger);
                let output = handler
                    .on_timer(context.clone(), trigger, DemandType::Normal)
                    .await;
                (context, output)
            }
        };
        let (context, output) = output;
        output.map_err(|e| eyre!("dispatch failed: {e}"))?;

        let applied_if_committed = match &event.step {
            Step::Set(byte) => Some(Value::from(*byte)),
            Step::Clear => None,
        };

        if event.commit {
            handler.after_commit(context, Ok(())).await;
        } else {
            handler.after_abort(context, Ok(())).await;
        }
        match commit_mode {
            CommitMode::Wal => {
                if event.commit {
                    model = applied_if_committed;
                }
            }
            // Direct mode applied during dispatch; abort cannot undo it.
            CommitMode::Direct => model = applied_if_committed,
        }

        // Invariants 2 + 3: the WAL must be resolved (Idle) and the
        // applied cell must decode to the model value.
        if read_idle_applied(&durable, &id).await? != model {
            return Ok(false);
        }
    }

    // Invariant 1: the handler observed every pre-state.
    Ok(*inner.observed.lock() == expected_observed)
}
