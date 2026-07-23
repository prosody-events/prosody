pub mod test_support;

// Tests for the `after_commit` / `after_abort` apply hooks plumbed
// through the blanket `FallibleEventHandler -> EventHandler` impl.
//
// These tests pin down the strictly-per-invocation apply-hook
// invariant for the default durability boundary: for every inner
// invocation of `on_message` / `on_timer` that runs and returns,
// exactly one of `after_commit` / `after_abort` fires, carrying the
// handler's typed `Result<Output, Error>` for that invocation. At
// this boundary (no rescue / defer / retry middleware in the stack)
// each call into `EventHandler::on_message` performs exactly one
// inner invocation, so the per-invocation invariant collapses to
// "one apply hook per call": `Ok` / `Permanent` / `Transient` are
// final invocations (`after_commit`), and `Terminal` is a non-final
// invocation (`after_abort`, the broker / timer will redeliver and
// produce a fresh invocation paired with its own apply hook).

use std::error::Error as StdError;
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use crossbeam_utils::CachePadded;
use parking_lot::Mutex;

use super::settle::{ArmOutcome, arm_backstop};
use super::*;
use crate::consumer::EventHandler;
use crate::consumer::Uncommitted;
use crate::consumer::message::ConsumerMessage;
use crate::consumer::middleware::tests::test_support::{MockEventContext, create_test_message};
use crate::consumer::partition::offsets::OffsetTracker;
use crate::error::ErrorCategory;
use crate::timers::TimerType;
use crate::timers::Trigger;
use crate::timers::datetime::CompactDateTime;

/// Test error with a fixed classification and a tag naming the raising site,
/// so expected hook logs stay distinguishable.
#[derive(Debug, Clone, PartialEq, Eq)]
struct TestError(ErrorCategory, &'static str);

impl Display for TestError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "test error ({}): {:?}", self.1, self.0)
    }
}

impl StdError for TestError {}

impl ClassifyError for TestError {
    fn classify_error(&self) -> ErrorCategory {
        self.0
    }
}

impl FallibleEventHandler for ProbeHandler {}

impl SettlementHandler for ProbeHandler {
    fn settlement(_result: Result<&Self::Output, &Self::Error>) -> Settlement {
        Settlement::Final
    }
}

/// Records every lifecycle hook firing for later assertion.
#[derive(Debug, Clone, PartialEq, Eq)]
enum HookEvent {
    Handler,
    AfterCommit(Result<u64, TestError>),
    AfterAbort(Result<u64, TestError>),
}

/// Probe handler whose `Output` is a `u64` sentinel; records every
/// lifecycle hook into a shared log.
#[derive(Clone)]
struct ProbeHandler {
    sentinel: u64,
    result: Result<(), TestError>,
    log: Arc<Mutex<Vec<HookEvent>>>,
}

impl ProbeHandler {
    fn ok(sentinel: u64) -> Self {
        Self {
            sentinel,
            result: Ok(()),
            log: Arc::default(),
        }
    }

    fn err(sentinel: u64, error: TestError) -> Self {
        Self {
            sentinel,
            result: Err(error),
            log: Arc::default(),
        }
    }
}

impl FallibleHandler for ProbeHandler {
    type Error = TestError;
    type Output = u64;
    type Payload = serde_json::Value;

    async fn on_message<C>(
        &self,
        _context: C,
        _message: ConsumerMessage<Self::Payload>,
        _demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        self.log.lock().push(HookEvent::Handler);
        self.result.clone().map(|()| self.sentinel)
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
        self.log.lock().push(HookEvent::Handler);
        self.result.clone().map(|()| self.sentinel)
    }

    async fn after_commit<C>(&self, _context: C, result: Result<Self::Output, Self::Error>)
    where
        C: EventContext<Payload = Self::Payload>,
    {
        self.log.lock().push(HookEvent::AfterCommit(result));
    }

    async fn after_abort<C>(&self, _context: C, result: Result<Self::Output, Self::Error>)
    where
        C: EventContext<Payload = Self::Payload>,
    {
        self.log.lock().push(HookEvent::AfterAbort(result));
    }

    async fn shutdown(self) {}
}

/// A commit guard that records whether it was committed or aborted, so a
/// test can assert which terminal the durability sequence chose.
struct RecordingGuard {
    committed: Arc<AtomicUsize>,
    aborted: Arc<AtomicUsize>,
}

impl Uncommitted for RecordingGuard {
    async fn commit(self) {
        self.committed.fetch_add(1, Ordering::SeqCst);
    }

    async fn abort(self) {
        self.aborted.fetch_add(1, Ordering::SeqCst);
    }
}

fn make_offset_tracker() -> OffsetTracker {
    let version = Arc::new(CachePadded::new(AtomicUsize::new(0)));
    OffsetTracker::new("test-topic".into(), 0, 10, Duration::from_secs(5), version)
}

#[tokio::test]
async fn after_commit_or_abort_fires_with_expected_log_per_outcome() -> color_eyre::Result<()> {
    // One row per `on_message` outcome category the blanket impl
    // distinguishes; each row's exact-log assertion also proves the
    // 1:1 invariant (exactly one apply hook fires per dispatch) for
    // that category, since the expected log contains exactly one
    // `AfterCommit`/`AfterAbort` entry.
    let permanent = TestError(ErrorCategory::Permanent, "permanent");
    let transient = TestError(ErrorCategory::Transient, "transient");
    let terminal = TestError(ErrorCategory::Terminal, "terminal");
    let cases: [(ProbeHandler, Vec<HookEvent>, &str); 4] = [
        (
            ProbeHandler::ok(42),
            vec![HookEvent::Handler, HookEvent::AfterCommit(Ok(42))],
            "handler runs first, then after_commit with Ok(sentinel)",
        ),
        (
            ProbeHandler::err(0, permanent.clone()),
            vec![HookEvent::Handler, HookEvent::AfterCommit(Err(permanent))],
            "Permanent error commits the marker; after_commit fires with Err",
        ),
        (
            // Transient (when no retry middleware is in front) commits
            // like Permanent at the blanket-impl level.
            ProbeHandler::err(0, transient.clone()),
            vec![HookEvent::Handler, HookEvent::AfterCommit(Err(transient))],
            "Transient error at the blanket-impl level commits + fires after_commit",
        ),
        (
            ProbeHandler::err(0, terminal.clone()),
            vec![HookEvent::Handler, HookEvent::AfterAbort(Err(terminal))],
            "Terminal error aborts the marker; after_abort fires with Err",
        ),
    ];

    for (handler, expected_log, description) in cases {
        let log = handler.log.clone();
        let context = MockEventContext::new();
        let tracker = make_offset_tracker();
        let uncommitted_offset = tracker.take(0).await?;
        let message = create_test_message()?.into_uncommitted(uncommitted_offset);

        EventHandler::on_message(&handler, context, message, DemandType::Normal).await;

        assert_eq!(log.lock().clone(), expected_log, "{description}");
    }
    Ok(())
}

#[tokio::test]
async fn after_commit_for_timer_path_with_ok_output() {
    // Timer arm of the blanket impl: build a minimal `UncommittedTimer`
    // and verify the same lifecycle.
    use std::sync::OnceLock;

    use crate::Key;
    use crate::consumer::{Keyed, Uncommitted};
    use crate::timers::UncommittedTimer;

    struct MockUncommittedTimer {
        committed: Arc<AtomicUsize>,
        aborted: Arc<AtomicUsize>,
    }

    impl Keyed for MockUncommittedTimer {
        type Key = Key;

        fn key(&self) -> &Self::Key {
            static KEY: OnceLock<Key> = OnceLock::new();
            KEY.get_or_init(|| "test-key".into())
        }
    }

    impl Uncommitted for MockUncommittedTimer {
        async fn commit(self) {
            self.committed.fetch_add(1, Ordering::SeqCst);
        }

        async fn abort(self) {
            self.aborted.fetch_add(1, Ordering::SeqCst);
        }
    }

    impl UncommittedTimer for MockUncommittedTimer {
        type CommitGuard = RecordingGuard;

        fn time(&self) -> CompactDateTime {
            CompactDateTime::from(0_u32)
        }

        fn timer_type(&self) -> TimerType {
            TimerType::Application
        }

        fn span(&self) -> tracing::Span {
            tracing::Span::current()
        }

        fn into_inner(self) -> (Trigger, Self::CommitGuard) {
            let trigger = Trigger::for_testing("test-key".into(), self.time(), self.timer_type());
            let guard = RecordingGuard {
                committed: self.committed.clone(),
                aborted: self.aborted.clone(),
            };
            (trigger, guard)
        }
    }

    let handler = ProbeHandler::ok(99);
    let log = handler.log.clone();
    let context = MockEventContext::new();
    let committed = Arc::new(AtomicUsize::new(0));
    let aborted = Arc::new(AtomicUsize::new(0));
    let timer = MockUncommittedTimer {
        committed: committed.clone(),
        aborted: aborted.clone(),
    };

    EventHandler::on_timer(&handler, context, timer, DemandType::Normal).await;

    assert_eq!(committed.load(Ordering::SeqCst), 1, "marker committed once");
    assert_eq!(aborted.load(Ordering::SeqCst), 0, "marker not aborted");
    assert_eq!(
        log.lock().clone(),
        vec![HookEvent::Handler, HookEvent::AfterCommit(Ok(99))],
        "timer Ok path: handler then after_commit with sentinel",
    );
}

/// Minimal pass-through middleware to verify the composition contract.
/// Stands in for any real `FallibleHandler` middleware that wraps an
/// inner with `type Output = Inner::Output` and forwards apply hooks.
struct PassThroughMiddleware<T> {
    inner: T,
}

impl<T> FallibleHandler for PassThroughMiddleware<T>
where
    T: FallibleHandler,
{
    type Error = T::Error;
    type Output = T::Output;
    type Payload = T::Payload;

    async fn on_message<C>(
        &self,
        context: C,
        message: ConsumerMessage<Self::Payload>,
        demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        self.inner.on_message(context, message, demand_type).await
    }

    async fn on_timer<C>(
        &self,
        context: C,
        trigger: Trigger,
        demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        self.inner.on_timer(context, trigger, demand_type).await
    }

    async fn after_commit<C>(&self, context: C, result: Result<Self::Output, Self::Error>)
    where
        C: EventContext<Payload = Self::Payload>,
    {
        self.inner.after_commit(context, result).await;
    }

    async fn after_abort<C>(&self, context: C, result: Result<Self::Output, Self::Error>)
    where
        C: EventContext<Payload = Self::Payload>,
    {
        self.inner.after_abort(context, result).await;
    }

    async fn shutdown(self) {
        self.inner.shutdown().await;
    }
}

impl<T> FallibleEventHandler for PassThroughMiddleware<T> where T: FallibleHandler {}

impl<T> SettlementHandler for PassThroughMiddleware<T>
where
    T: SettlementHandler,
{
    fn settlement(result: Result<&Self::Output, &Self::Error>) -> Settlement {
        T::settlement(result)
    }
}

#[tokio::test]
async fn pass_through_middleware_forwards_output_to_inner_after_commit() -> color_eyre::Result<()> {
    let inner = ProbeHandler::ok(7);
    let log = inner.log.clone();
    let middleware = PassThroughMiddleware { inner };
    let context = MockEventContext::new();
    let tracker = make_offset_tracker();
    let uncommitted_offset = tracker.take(0).await?;
    let message = create_test_message()?.into_uncommitted(uncommitted_offset);

    EventHandler::on_message(&middleware, context, message, DemandType::Normal).await;

    // The inner handler observes both Handler (it ran) and AfterCommit
    // (the middleware forwarded with Ok(7)).
    assert_eq!(
        log.lock().clone(),
        vec![HookEvent::Handler, HookEvent::AfterCommit(Ok(7))],
        "pass-through middleware forwards typed output unchanged",
    );
    Ok(())
}

#[tokio::test]
async fn pass_through_middleware_forwards_after_abort_on_terminal() -> color_eyre::Result<()> {
    let err = TestError(ErrorCategory::Terminal, "terminal");
    let inner = ProbeHandler::err(0, err.clone());
    let log = inner.log.clone();
    let middleware = PassThroughMiddleware { inner };
    let context = MockEventContext::new();
    let tracker = make_offset_tracker();
    let uncommitted_offset = tracker.take(0).await?;
    let message = create_test_message()?.into_uncommitted(uncommitted_offset);

    EventHandler::on_message(&middleware, context, message, DemandType::Normal).await;

    assert_eq!(
        log.lock().clone(),
        vec![HookEvent::Handler, HookEvent::AfterAbort(Err(err))],
        "pass-through middleware forwards after_abort on terminal",
    );
    Ok(())
}

/// The settle boundary's single staged-rollback site: shutdown at the
/// backstop arm — after a successful stage, before any marker record attempt —
/// rolls the staged cells back to their committed base. `abandon` has no
/// state access, and a rollback past `certify` does not compile
/// (`Promotable` has no rollback), so rollback-after-a-marker-record-attempt
/// is unwritable rather than tested.
mod staged_rollback {
    use super::*;
    use crate::loader::MemoryLoader;
    use crate::state::cell::Committed;
    use crate::state::descriptor::tests::{FixedOracle, TestSession, test_session_parts};
    use crate::state::descriptor::{Registered, ValueDescriptor, value_state};
    use crate::state::memory::MemoryCellStore;
    use crate::state::registry::{CollectionDef, CollectionDefRegistry};
    use crate::state::store::CellStore;
    use crate::state::tests::cell_suite::value_cell;
    use crate::state::{CollectionId, EventRef, StateKey, StateName, StateType};
    use bytes::Bytes;
    use color_eyre::eyre::{Result, bail, eyre};
    use futures::StreamExt;
    use quickcheck::{QuickCheck, TestResult};
    use serde_json::json;
    use tokio::runtime::Builder;
    use uuid::Uuid;

    /// Whether the durable cell at `id` is still provisional — the public,
    /// non-resolving way to distinguish a staged cell from a resolved one (the
    /// resolving `get` would mutate a provisional cell).
    pub(super) async fn is_provisional(
        cell_store: &MemoryCellStore<FixedOracle>,
        id: &CollectionId,
    ) -> Result<bool> {
        let stream = cell_store.provisional_cells(id);
        futures::pin_mut!(stream);
        Ok(stream.next().await.transpose()?.is_some())
    }

    /// The resolved committed value of the collection's single Value cell —
    /// call only on a known-settled cell (a resolving `get` heals a
    /// still-provisional one). Distinguishes "rolled back to the absent
    /// base" (`None`) from "wrongly committed the staged value"
    /// (`Some(..)`), which `is_provisional` alone cannot: both settle the
    /// cell.
    async fn committed_value(
        cell_store: &MemoryCellStore<FixedOracle>,
        id: &CollectionId,
    ) -> Result<Option<Bytes>> {
        let probe = EventRef::Message {
            dedup_id: Uuid::from_u128(u128::MAX),
        };
        cell_store
            .get(id, &value_cell(), probe)
            .await
            .map(Committed::into_inner)
            .map_err(|e| eyre!("read committed: {e}"))
    }

    pub(super) type Ctx = MockEventContext<serde_json::Value, TestSession>;

    /// The `cart` descriptor with the default JSON codec.
    fn cart() -> ValueDescriptor {
        value_state("cart")
    }

    /// Buffers one value write on `cart` through a real session — no finalize:
    /// `settle` owns the only stage, from the intact dirty buffer. Returns the
    /// context (ready to settle), the durable store, and the cell id.
    /// `configure` applies context modifiers (`with_timer_failures`,
    /// `with_shutdown`, ...) before the write.
    pub(super) async fn buffered(
        configure: impl FnOnce(Ctx) -> Ctx,
    ) -> Result<(Ctx, MemoryCellStore<FixedOracle>, CollectionId)> {
        let mut registry = CollectionDefRegistry::default();
        registry.register(&cart(), CollectionDef::new(None))?;
        let state_key = StateKey::new(Uuid::from_u128(0x7), Arc::from("user-1"));
        let (session, cell_store) =
            test_session_parts(MemoryLoader::new(), registry, state_key.clone());
        let context: Ctx = configure(MockEventContext::new().with_session(session));

        let handle = context
            .state(Registered::new(cart()))
            .map_err(|e| eyre!("bind cart: {e}"))?;
        handle.set(json!({ "x": 1_i32 })).await?;

        let cart_id = CollectionId::new(
            state_key,
            StateType::Application,
            StateName::try_new("cart")?,
        );
        Ok((context, cell_store, cart_id))
    }

    /// Shutdown between a successful stage and the backstop arm drives
    /// settle's ONE reachable rollback: the guard aborts, the staged cell
    /// rolls back to its committed base (here absent, so the cell resolves
    /// away), and `after_abort` fires. The abort itself proves the staged arm
    /// ran — a `Clean` finalize would have committed instead.
    #[tokio::test]
    async fn arm_shutdown_rolls_the_staged_cells_back() -> Result<()> {
        let (context, cell_store, cart_id) = buffered(Ctx::with_shutdown_on_timer_read).await?;
        let handler = ProbeHandler::ok(0);
        let log = handler.log.clone();
        let committed = Arc::new(AtomicUsize::new(0));
        let aborted = Arc::new(AtomicUsize::new(0));
        let guard = RecordingGuard {
            committed: committed.clone(),
            aborted: aborted.clone(),
        };

        settle(&handler, context, guard, Ok(0)).await;

        assert_eq!(
            aborted.load(Ordering::SeqCst),
            1,
            "arm-shutdown must abort the guard"
        );
        assert_eq!(committed.load(Ordering::SeqCst), 0);
        assert!(
            !is_provisional(&cell_store, &cart_id).await?,
            "the receipt's rollback must settle the staged cell",
        );
        assert_eq!(
            committed_value(&cell_store, &cart_id).await?,
            None,
            "rollback restores the absent committed base — not the staged value",
        );
        assert_eq!(
            log.lock().clone(),
            vec![HookEvent::AfterAbort(Ok(0))],
            "the arm-shutdown abort fires after_abort exactly once",
        );
        Ok(())
    }

    /// The permanent-finalize-skip arm of `settle_committed`: a **Permanent**
    /// stage failure is the one durability step the sequence skips rather than
    /// retries (a genuine data rejection cannot self-heal). The documented
    /// posture is commit-defensively: the offset still commits (no livelock on
    /// an unstageable event), the marker record is skipped (a present marker
    /// must certify a durable stage — invariant: marker present ⇒ stage
    /// durable), and the backstop is armed defensively so the sweep resolves
    /// whatever partial stage may have landed.
    #[tokio::test]
    async fn permanent_finalize_skip_commits_unmarked_with_backstop_armed() -> Result<()> {
        use crate::consumer::middleware::tests::test_support::RecordingOracle;
        use crate::consumer::partition::ShutdownPhase;
        use crate::state::EventRef;
        use crate::state::PartitionBackend;
        use crate::state::dirty::DirtyStore;
        use crate::state::memory::{MemoryCells, MemoryDescriptorIdentityStore};
        use crate::state::session::{KeyedStateSession, SessionParts, TerminationWatch};
        use crate::state::tests::cell_suite::FailingCellStore;
        use crate::timers::duration::CompactDuration;
        use tokio::sync::watch;

        type SkipStore = FailingCellStore<MemoryCellStore<RecordingOracle>>;
        type SkipBackend =
            PartitionBackend<RecordingOracle, MemoryDescriptorIdentityStore, SkipStore>;

        let mut registry = CollectionDefRegistry::default();
        registry.register(&cart(), CollectionDef::new(None))?;
        let registry = Arc::new(registry);
        let oracle = RecordingOracle::new();
        let recorded = oracle.recorded();
        // Poison the STAGE path: `write_provisional` on `cart` fails
        // Permanent, so `finalize` inside `settle` hits `StepOutcome::Skip`.
        let cell_store = FailingCellStore::failing_write_provisional(
            MemoryCellStore::new(MemoryCells::new(), oracle.clone(), registry.clone()),
            StateName::try_new("cart")?,
            ErrorCategory::Permanent,
        );
        let (_shutdown_tx, shutdown_rx) = watch::channel(ShutdownPhase::default());
        let (_cancel_tx, cancel_rx) = watch::channel(false);
        let session: KeyedStateSession<SkipBackend, MemoryLoader<serde_json::Value>> =
            KeyedStateSession::new(SessionParts {
                cell: cell_store,
                dirty: Arc::new(DirtyStore::new()),
                oracle,
                loader: MemoryLoader::new(),
                registry,
                state_key: StateKey::new(Uuid::from_u128(0x5C1), Arc::from("user-1")),
                event: EventRef::Message {
                    dedup_id: Uuid::new_v4(),
                },
                recovery_delay: CompactDuration::new(30),
                armed: Arc::default(),
                termination: TerminationWatch::new(shutdown_rx, cancel_rx),
                publisher: None,
            });
        let context = MockEventContext::new()
            .with_session(session)
            .with_timer_tracking();

        // Buffer a write; do NOT finalize — `settle` owns the stage and must
        // hit the poison itself. The session's message EventRef carries the
        // marker the skip path must NOT record.
        let handle = context
            .state(Registered::new(cart()))
            .map_err(|e| eyre!("bind cart: {e}"))?;
        handle.set(json!({ "x": 1_i32 })).await?;

        let handler = ProbeHandler::ok(0);
        let committed = Arc::new(AtomicUsize::new(0));
        let aborted = Arc::new(AtomicUsize::new(0));
        let guard = RecordingGuard {
            committed: committed.clone(),
            aborted: aborted.clone(),
        };

        settle(&handler, context.clone(), guard, Ok(0)).await;

        assert_eq!(
            committed.load(Ordering::SeqCst),
            1,
            "a permanently-unstageable event still commits (no livelock)",
        );
        assert_eq!(aborted.load(Ordering::SeqCst), 0);
        assert!(
            recorded.lock().is_empty(),
            "the marker must NOT record over an uncertain stage",
        );
        assert_eq!(
            context.count_scheduled(TimerType::StateRecovery),
            1,
            "the backstop must be armed defensively for the sweep",
        );
        Ok(())
    }

    /// Records whether its `after_commit` typed-handle read answered or hit the
    /// stale-pin fence — witnessing that the permanent-`Skip` arm re-stamps
    /// the hook context.
    #[derive(Clone)]
    struct SkipReadProbe {
        read: Arc<Mutex<Option<Result<(), String>>>>,
    }

    impl FallibleHandler for SkipReadProbe {
        type Error = TestError;
        type Output = u64;
        type Payload = serde_json::Value;

        async fn on_message<C>(
            &self,
            _context: C,
            _message: ConsumerMessage<Self::Payload>,
            _demand_type: DemandType,
        ) -> Result<Self::Output, Self::Error>
        where
            C: EventContext<Payload = Self::Payload>,
        {
            Ok(0)
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
            Ok(0)
        }

        async fn after_commit<C>(&self, context: C, _result: Result<Self::Output, Self::Error>)
        where
            C: EventContext<Payload = Self::Payload>,
        {
            let outcome = match context.state(Registered::new(cart())) {
                Ok(handle) => handle.get().await.map(|_| ()).map_err(|e| e.to_string()),
                Err(e) => Err(format!("bind: {e}")),
            };
            *self.read.lock() = Some(outcome);
        }

        async fn after_abort<C>(&self, _context: C, _result: Result<Self::Output, Self::Error>)
        where
            C: EventContext<Payload = Self::Payload>,
        {
        }

        async fn shutdown(self) {}
    }

    impl SettlementHandler for SkipReadProbe {
        fn settlement(_result: Result<&Self::Output, &Self::Error>) -> Settlement {
            Settlement::Final
        }
    }

    /// The permanent-finalize-`Skip` arm must fire `after_commit` through
    /// `fire_apply_hook`, not a direct call: a settle context left STALE by a
    /// nested retry's epoch bump is re-stamped current before the hook reads.
    /// Without the stamp the hook's typed read errors `Terminated` — the one
    /// hook-fire site that used to bypass the stamp. Red-proven by reverting
    /// the arm to `handler.after_commit(context, result)`: the read then
    /// reports `Terminated`.
    #[tokio::test]
    async fn permanent_skip_hook_reads_through_the_stamp() -> Result<()> {
        use crate::consumer::middleware::tests::test_support::RecordingOracle;
        use crate::consumer::partition::ShutdownPhase;
        use crate::state::PartitionBackend;
        use crate::state::dirty::DirtyStore;
        use crate::state::memory::{MemoryCells, MemoryDescriptorIdentityStore};
        use crate::state::session::sealed::StateLifecycle;
        use crate::state::session::{KeyedStateSession, SessionParts, TerminationWatch};
        use crate::state::tests::cell_suite::FailingCellStore;
        use crate::timers::duration::CompactDuration;
        use tokio::sync::watch;

        type SkipStore = FailingCellStore<MemoryCellStore<RecordingOracle>>;
        type SkipBackend =
            PartitionBackend<RecordingOracle, MemoryDescriptorIdentityStore, SkipStore>;

        let mut registry = CollectionDefRegistry::default();
        registry.register(&cart(), CollectionDef::new(None))?;
        let registry = Arc::new(registry);
        let oracle = RecordingOracle::new();
        // Poison the STAGE path so `settle`'s own `finalize` hits Skip.
        let cell_store = FailingCellStore::failing_write_provisional(
            MemoryCellStore::new(MemoryCells::new(), oracle.clone(), registry.clone()),
            StateName::try_new("cart")?,
            ErrorCategory::Permanent,
        );
        let (_shutdown_tx, shutdown_rx) = watch::channel(ShutdownPhase::default());
        let (_cancel_tx, cancel_rx) = watch::channel(false);
        let session: KeyedStateSession<SkipBackend, MemoryLoader<serde_json::Value>> =
            KeyedStateSession::new(SessionParts {
                cell: cell_store,
                dirty: Arc::new(DirtyStore::new()),
                oracle,
                loader: MemoryLoader::new(),
                registry,
                state_key: StateKey::new(Uuid::from_u128(0x5C2), Arc::from("user-1")),
                event: EventRef::Message {
                    dedup_id: Uuid::new_v4(),
                },
                recovery_delay: CompactDuration::new(30),
                armed: Arc::default(),
                termination: TerminationWatch::new(shutdown_rx, cancel_rx),
                publisher: None,
            });

        // A nested retry's epoch bump: `reset` discards the (empty) dirty and
        // bumps the shared epoch, leaving THIS clone pinned stale. Buffer the
        // poisoned write through a live re-pinned clone (shared dirty overlay),
        // so `finalize` stages it and hits the poison — while the settle still
        // receives the stale clone.
        session.reset(RepinProof::for_test()).await;
        let live = session.repin(RepinProof::for_test());
        let live_ctx = MockEventContext::new().with_session(live);
        let live_handle = live_ctx
            .state(Registered::new(cart()))
            .map_err(|e| eyre!("bind live: {e}"))?;
        live_handle.set(json!({ "x": 1_i32 })).await?;

        let context = MockEventContext::new().with_session(session);
        let read = Arc::new(Mutex::new(None));
        let handler = SkipReadProbe { read: read.clone() };
        let committed = Arc::new(AtomicUsize::new(0));
        let aborted = Arc::new(AtomicUsize::new(0));
        let guard = RecordingGuard {
            committed: committed.clone(),
            aborted: aborted.clone(),
        };

        settle(&handler, context, guard, Ok(0)).await;

        assert_eq!(
            committed.load(Ordering::SeqCst),
            1,
            "the permanent-skip arm still commits",
        );
        match read.lock().clone() {
            Some(Ok(())) => {}
            Some(Err(e)) => bail!("the Skip-arm hook read was fenced: {e}"),
            None => bail!("after_commit never fired"),
        }
        Ok(())
    }

    /// Abort-only-on-shutdown, end to end through `settle`'s success path: a
    /// shutdown is the *sole* thing that stops the durability sequence short
    /// of a commit — a shutdown seen before the finalize stage abandons with
    /// nothing ever staged, so nothing is left provisional; the offset aborts
    /// either way (the event redelivers and re-runs). Every store failure
    /// instead retries forever.
    ///
    /// The never-abort-except-shutdown invariant, as a property over a
    /// generated leading-failure count, the **category** those failures
    /// classify as, and a shutdown flag: `settle`'s success path aborts the
    /// offset **iff** shutdown (leaving nothing provisional), and
    /// otherwise self-heals to a commit **no matter how many** failures —
    /// of **any** category — the arm hits first (the arm is must-succeed,
    /// invariant 8) — then records the marker, commits, and promotes the
    /// cell. Generating the category is what exercises the retry-forever
    /// fold in `retry_step`: `Terminal` retries rather than abandons, and
    /// `Permanent` is retried by the arm's own loop past `retry_step`'s
    /// `Skip`. Each iteration runs on its own paused single-thread
    /// runtime so the retry backoff advances instantly and never blocks.
    #[test]
    fn prop_settle_aborts_iff_shutdown() {
        fn property(fail_count: u8, category_sel: u8, shutdown: bool) -> TestResult {
            // A small bound keeps each iteration's paused-clock retry loop fast
            // while still crossing the zero / non-zero boundary.
            let fail_count = usize::from(fail_count % 6);
            let category = match category_sel % 3 {
                0 => ErrorCategory::Transient,
                1 => ErrorCategory::Permanent,
                _ => ErrorCategory::Terminal,
            };
            let runtime = Builder::new_current_thread()
                .enable_time()
                .start_paused(true)
                .build();
            let Ok(runtime) = runtime else {
                return TestResult::error("failed to build paused runtime");
            };
            runtime.block_on(async move {
                let configure = |c: Ctx| {
                    let c = c.with_timer_failures(fail_count, category);
                    if shutdown { c.with_shutdown() } else { c }
                };
                let Ok((context, cell_store, cart_id)) = buffered(configure).await else {
                    return TestResult::error("failed to buffer the write");
                };
                let handler = ProbeHandler::ok(0);
                let committed = Arc::new(AtomicUsize::new(0));
                let aborted = Arc::new(AtomicUsize::new(0));
                let guard = RecordingGuard {
                    committed: committed.clone(),
                    aborted: aborted.clone(),
                };

                settle(&handler, context, guard, Ok(0)).await;

                let committed = committed.load(Ordering::SeqCst);
                let aborted = aborted.load(Ordering::SeqCst);
                let provisional = cell_store.provisional_cells(&cart_id);
                futures::pin_mut!(provisional);
                let still_provisional = matches!(provisional.next().await, Some(Ok(_)));

                if shutdown {
                    // Abort iff shutdown: the offset aborts, and nothing was
                    // ever staged (settle's finalize step sees shutdown first).
                    if aborted != 1 || committed != 0 || still_provisional {
                        return TestResult::error(format!(
                            "shutdown must abort with nothing staged: committed={committed} \
                             aborted={aborted} provisional={still_provisional}"
                        ));
                    }
                } else {
                    // No shutdown: self-heal to a commit however many failures first.
                    if committed != 1 || aborted != 0 || still_provisional {
                        return TestResult::error(format!(
                            "non-shutdown must self-heal to commit: committed={committed} \
                             aborted={aborted} provisional={still_provisional}"
                        ));
                    }
                }
                TestResult::passed()
            })
        }
        QuickCheck::new().quickcheck(property as fn(u8, u8, bool) -> TestResult);
    }
}

/// First-write publication at the settle boundary and the mid-handler
/// `commit()` path: a `Published` collection's routing row is written *before*
/// its committed state, a failing publication store blocks the write (never
/// settles unpublished), and shutdown during publication abandons without
/// staging.
mod settle_publication {
    use super::*;
    use crate::loader::MemoryLoader;
    use crate::state::cell::Committed;
    use crate::state::descriptor::tests::{FixedOracle, TestSession, test_session_with_publisher};
    use crate::state::descriptor::{Registered, ValueDescriptor, value_state};
    use crate::state::first_write::{PartitionCounts, PublicationBackend, PublisherTemplate};
    use crate::state::memory::MemoryCellStore;
    use crate::state::registry::{CollectionDef, CollectionDefRegistry, StateVisibility};
    use crate::state::store::CellStore;
    use crate::state::tests::cell_suite::value_cell;
    use crate::state::tests::support::ScriptedPublicationStore;
    use crate::state::{CollectionId, EventRef, StateKey, StateName, StateType, StoreOutcome};
    use crate::state_reader::PartitionCount;
    use crate::subsystem::SubsystemName;
    use bytes::Bytes;
    use color_eyre::eyre::{Result, eyre};
    use internment::Intern;
    use serde_json::json;
    use std::time::Duration;
    use tokio::time::{advance, timeout};
    use uuid::Uuid;

    type Ctx = MockEventContext<serde_json::Value, TestSession>;

    const GROUP: &str = "group-a";
    const SUBSYSTEM: &str = "orders";
    const TOPIC: &str = "orders-topic";

    fn cart() -> ValueDescriptor {
        value_state("cart")
    }

    fn published_registry() -> Result<CollectionDefRegistry> {
        let mut registry = CollectionDefRegistry::default();
        registry
            .register(
                &cart(),
                CollectionDef {
                    visibility: StateVisibility::Published,
                    ..CollectionDef::new(None)
                },
            )
            .map_err(|e| eyre!("register cart: {e}"))?;
        Ok(registry)
    }

    fn publisher_template(
        store: ScriptedPublicationStore,
        count: i32,
    ) -> Result<PublisherTemplate> {
        Ok(PublisherTemplate::new(
            SubsystemName::try_new(SUBSYSTEM).map_err(|e| eyre!("subsystem: {e}"))?,
            Arc::from(GROUP),
            Arc::new(PublicationBackend::Scripted(store)),
            Arc::new(PartitionCounts::Memory(PartitionCount::try_from(count)?)),
            Arc::new(published_registry()?),
        ))
    }

    /// The resolved committed value of the collection's single Value cell — a
    /// probe read against the durable store (a distinct probe identity so it
    /// never aliases the event under test).
    async fn committed_value(
        cell_store: &MemoryCellStore<FixedOracle>,
        id: &CollectionId,
    ) -> Result<Option<Bytes>> {
        let probe = EventRef::Message {
            dedup_id: Uuid::from_u128(u128::MAX),
        };
        cell_store
            .get(id, &value_cell(), probe)
            .await
            .map(Committed::into_inner)
            .map_err(|e| eyre!("read committed: {e}"))
    }

    /// Buffers one value write on a Published `cart` through a real session
    /// carrying a first-write publisher over `store`. No finalize — `settle`
    /// owns the only stage. Returns the settle-ready context, the durable
    /// store, and the cell id.
    async fn buffered_published(
        configure: impl FnOnce(Ctx) -> Ctx,
        store: ScriptedPublicationStore,
        count: i32,
    ) -> Result<(Ctx, MemoryCellStore<FixedOracle>, CollectionId)> {
        let state_key = StateKey::new(Uuid::from_u128(0x7), Arc::from("user-1"));
        let publisher = publisher_template(store, count)?.bind(Intern::<str>::from(TOPIC));
        let (session, cell_store) = test_session_with_publisher(
            MemoryLoader::new(),
            published_registry()?,
            state_key.clone(),
            publisher,
        );
        let context: Ctx = configure(MockEventContext::new().with_session(session));
        let handle = context
            .state(Registered::new(cart()))
            .map_err(|e| eyre!("bind cart: {e}"))?;
        handle.set(json!({ "x": 1_i32 })).await?;
        let cart_id = CollectionId::new(
            state_key,
            StateType::Application,
            StateName::try_new("cart").map_err(|e| eyre!("name: {e}"))?,
        );
        Ok((context, cell_store, cart_id))
    }

    fn subsystem() -> Result<SubsystemName> {
        SubsystemName::try_new(SUBSYSTEM).map_err(|e| eyre!("subsystem: {e}"))
    }

    fn cart_state_name() -> Result<StateName> {
        StateName::try_new("cart").map_err(|e| eyre!("name: {e}"))
    }

    fn wishlist_state_name() -> Result<StateName> {
        StateName::try_new("wishlist").map_err(|e| eyre!("name: {e}"))
    }

    /// A registry with BOTH `cart` and `wishlist` registered `Published`, so a
    /// truthful-set test can register two published collections and write only
    /// one.
    fn two_published_registry() -> Result<CollectionDefRegistry> {
        let mut registry = published_registry()?;
        let wishlist: ValueDescriptor = value_state("wishlist");
        registry
            .register(
                &wishlist,
                CollectionDef {
                    visibility: StateVisibility::Published,
                    ..CollectionDef::new(None)
                },
            )
            .map_err(|e| eyre!("register wishlist: {e}"))?;
        Ok(registry)
    }

    /// Arm (a): the routing row is written BEFORE the durable state. The gated
    /// upsert parks in settle step 0; while it is parked the cell is not yet
    /// durable (finalize is step 1). Releasing the gate lets settle stage and
    /// commit, and the row lands with the live count.
    #[tokio::test]
    async fn publication_precedes_the_durable_write() -> Result<()> {
        let store = ScriptedPublicationStore::gated();
        let (context, cell_store, cart_id) = buffered_published(|c| c, store.clone(), 3).await?;
        let handler = ProbeHandler::ok(0);
        let committed = Arc::new(AtomicUsize::new(0));
        let aborted = Arc::new(AtomicUsize::new(0));
        let guard = RecordingGuard {
            committed: committed.clone(),
            aborted: aborted.clone(),
        };

        let task = tokio::spawn(async move {
            settle(&handler, context, guard, Ok(0)).await;
        });

        // Wait until the gated upsert has entered settle step 0 and blocked.
        timeout(Duration::from_secs(5), store.wait_entered())
            .await
            .map_err(|_| eyre!("gated upsert never entered"))?;
        // The barrier precedes the stage, so nothing is durable yet.
        assert_eq!(
            committed_value(&cell_store, &cart_id).await?,
            None,
            "the cell must not be durable while publication is still blocked",
        );
        assert_eq!(committed.load(Ordering::SeqCst), 0);

        // Release the barrier; settle now stages, commits, and promotes.
        store.release();
        task.await.map_err(|e| eyre!("settle task: {e}"))?;

        let rows = store.rows(&subsystem()?, &cart_state_name()?).await;
        assert_eq!(rows.len(), 1, "the routing row landed");
        assert_eq!(
            i32::from(rows[0].partition_count),
            3_i32,
            "with the live count"
        );
        assert_eq!(committed.load(Ordering::SeqCst), 1);
        assert_eq!(
            aborted.load(Ordering::SeqCst),
            0,
            "the guard never aborts while publication is pending",
        );
        assert!(
            committed_value(&cell_store, &cart_id).await?.is_some(),
            "the cell is durable after release",
        );
        Ok(())
    }

    /// Arm (f): a failing publication store BLOCKS the durable write — settle's
    /// must-succeed publish loop retries forever, so while the store fails no
    /// cell is durable and nothing commits. Once the store heals, both the row
    /// and the cell land and the guard commits exactly once.
    #[tokio::test(start_paused = true)]
    async fn failed_publication_blocks_the_write() -> Result<()> {
        let store = ScriptedPublicationStore::failing();
        let (context, cell_store, cart_id) = buffered_published(|c| c, store.clone(), 3).await?;
        let handler = ProbeHandler::ok(0);
        let committed = Arc::new(AtomicUsize::new(0));
        let aborted = Arc::new(AtomicUsize::new(0));
        let guard = RecordingGuard {
            committed: committed.clone(),
            aborted: aborted.clone(),
        };

        let task = tokio::spawn(async move {
            settle(&handler, context, guard, Ok(0)).await;
        });

        // The publish loop attempted and failed at least once.
        timeout(Duration::from_secs(5), store.wait_errored())
            .await
            .map_err(|_| eyre!("gated upsert never errored"))?;
        assert_eq!(
            committed_value(&cell_store, &cart_id).await?,
            None,
            "no durable write while publication keeps failing",
        );
        assert_eq!(committed.load(Ordering::SeqCst), 0);

        // Heal the store and advance past the retry backoff so the loop
        // succeeds and settle finishes.
        store.heal();
        advance(Duration::from_secs(2)).await;
        timeout(Duration::from_secs(5), task)
            .await
            .map_err(|_| eyre!("settle did not finish after the store healed"))?
            .map_err(|e| eyre!("settle task: {e}"))?;

        let rows = store.rows(&subsystem()?, &cart_state_name()?).await;
        assert_eq!(
            rows.len(),
            1,
            "the routing row landed once the store healed"
        );
        assert_eq!(
            committed.load(Ordering::SeqCst),
            1,
            "committed exactly once"
        );
        assert_eq!(
            aborted.load(Ordering::SeqCst),
            0,
            "the guard never aborts while publication retries",
        );
        assert!(
            committed_value(&cell_store, &cart_id).await?.is_some(),
            "the cell is durable after the store healed",
        );
        Ok(())
    }

    /// Shutdown observed at settle step 0 abandons the event before anything
    /// stages: the guard aborts, no cell is durable, and no routing row lands.
    #[tokio::test]
    async fn shutdown_during_publication_abandons() -> Result<()> {
        // A store that would fail forever, but shutdown short-circuits the loop
        // before any upsert is attempted.
        let store = ScriptedPublicationStore::failing();
        let (context, cell_store, cart_id) = buffered_published(|c| c, store.clone(), 3).await?;
        // Request shutdown AFTER the write is buffered (the write itself needs a
        // live session); settle's publish loop then sees shutdown at its top.
        context.request_shutdown();
        let handler = ProbeHandler::ok(0);
        let committed = Arc::new(AtomicUsize::new(0));
        let aborted = Arc::new(AtomicUsize::new(0));
        let guard = RecordingGuard {
            committed: committed.clone(),
            aborted: aborted.clone(),
        };

        settle(&handler, context, guard, Ok(0)).await;

        assert_eq!(aborted.load(Ordering::SeqCst), 1, "shutdown abandons");
        assert_eq!(committed.load(Ordering::SeqCst), 0);
        assert_eq!(
            committed_value(&cell_store, &cart_id).await?,
            None,
            "nothing staged when shutdown pre-empts publication",
        );
        assert!(
            store
                .rows(&subsystem()?, &cart_state_name()?)
                .await
                .is_empty(),
            "no routing row is written on the shutdown-abandon path",
        );
        Ok(())
    }

    /// Arm (i): the mid-handler `commit()` path publishes before its direct
    /// durable write. A successful publication lets `commit()` write the cell;
    /// a failing publication store makes `commit()` return `Err` and leaves NO
    /// durable cell (the routing row gates `write_resolved`).
    #[tokio::test]
    async fn commit_path_publishes_before_write_resolved() -> Result<()> {
        // Success: commit publishes then writes.
        let store = ScriptedPublicationStore::new();
        let state_key = StateKey::new(Uuid::from_u128(0x9), Arc::from("user-1"));
        let publisher = publisher_template(store.clone(), 3)?.bind(Intern::<str>::from(TOPIC));
        let (session, cell_store) = test_session_with_publisher(
            MemoryLoader::new(),
            published_registry()?,
            state_key.clone(),
            publisher,
        );
        let context: Ctx = MockEventContext::new().with_session(session);
        let handle = context
            .state(Registered::new(cart()))
            .map_err(|e| eyre!("bind cart: {e}"))?;
        handle.set(json!({ "x": 1_i32 })).await?;
        assert_eq!(
            handle.commit().await.map_err(|e| eyre!("commit: {e}"))?,
            StoreOutcome::Applied,
            "commit() durably applied the cell",
        );

        let cart_id = CollectionId::new(state_key, StateType::Application, cart_state_name()?);
        let rows = store.rows(&subsystem()?, &cart_state_name()?).await;
        assert_eq!(rows.len(), 1, "commit() published a routing row");
        assert_eq!(i32::from(rows[0].partition_count), 3_i32);
        assert!(
            committed_value(&cell_store, &cart_id).await?.is_some(),
            "commit() wrote the cell durably",
        );

        // Failure: a failing store makes commit() error and write nothing.
        let store = ScriptedPublicationStore::failing();
        let state_key = StateKey::new(Uuid::from_u128(0xA), Arc::from("user-2"));
        let publisher = publisher_template(store.clone(), 3)?.bind(Intern::<str>::from(TOPIC));
        let (session, cell_store) = test_session_with_publisher(
            MemoryLoader::new(),
            published_registry()?,
            state_key.clone(),
            publisher,
        );
        let context: Ctx = MockEventContext::new().with_session(session);
        let handle = context
            .state(Registered::new(cart()))
            .map_err(|e| eyre!("bind cart: {e}"))?;
        handle.set(json!({ "x": 1_i32 })).await?;
        assert!(
            handle.commit().await.is_err(),
            "commit() fails when publication fails",
        );
        let cart_id = CollectionId::new(state_key, StateType::Application, cart_state_name()?);
        assert_eq!(
            committed_value(&cell_store, &cart_id).await?,
            None,
            "no durable cell when publication gates the write",
        );
        assert!(
            store
                .rows(&subsystem()?, &cart_state_name()?)
                .await
                .is_empty(),
            "no routing row when the failing store rejects the upsert",
        );
        Ok(())
    }

    /// Arm (b): the truthful set. Publication advertises only the collections
    /// the event actually WROTE — `publish_first_writes` iterates the dirty
    /// overlay's `touched_collections`, never the registered set. This is the
    /// defining choice of first-write publication over subscription
    /// enumeration.
    ///
    /// Two `Published` collections are registered; the event writes only
    /// `cart`. Settling publishes a row for `cart` and NONE for the unwritten
    /// `wishlist`. Falsify by enumerating the registry instead of the touched
    /// overlay in `KeyedStateSession::publish_first_writes`: `wishlist` gains a
    /// row and the no-row assertion goes red.
    #[tokio::test]
    async fn only_written_published_collections_are_advertised() -> Result<()> {
        let store = ScriptedPublicationStore::new();
        let state_key = StateKey::new(Uuid::from_u128(0xB), Arc::from("user-1"));
        let publisher = PublisherTemplate::new(
            subsystem()?,
            Arc::from(GROUP),
            Arc::new(PublicationBackend::Scripted(store.clone())),
            Arc::new(PartitionCounts::Memory(PartitionCount::try_from(3_i32)?)),
            Arc::new(two_published_registry()?),
        )
        .bind(Intern::<str>::from(TOPIC));
        let (session, _cell_store) = test_session_with_publisher(
            MemoryLoader::new(),
            two_published_registry()?,
            state_key,
            publisher,
        );
        let context: Ctx = MockEventContext::new().with_session(session);
        // Write ONLY cart; wishlist is Published but untouched by this event.
        let handle = context
            .state(Registered::new(cart()))
            .map_err(|e| eyre!("bind cart: {e}"))?;
        handle.set(json!({ "x": 1_i32 })).await?;

        let handler = ProbeHandler::ok(0);
        let committed = Arc::new(AtomicUsize::new(0));
        let aborted = Arc::new(AtomicUsize::new(0));
        let guard = RecordingGuard {
            committed: committed.clone(),
            aborted: aborted.clone(),
        };
        settle(&handler, context, guard, Ok(0)).await;

        assert_eq!(
            committed.load(Ordering::SeqCst),
            1,
            "the written event commits"
        );
        assert_eq!(
            store.upserts_for("cart", TOPIC),
            1,
            "the written collection is advertised"
        );
        assert_eq!(
            store.upserts_for("wishlist", TOPIC),
            0,
            "the unwritten published collection is NOT advertised",
        );
        assert!(
            store
                .rows(&subsystem()?, &wishlist_state_name()?)
                .await
                .is_empty(),
            "no routing row for a published collection that never wrote",
        );
        let cart_rows = store.rows(&subsystem()?, &cart_state_name()?).await;
        assert_eq!(
            cart_rows.len(),
            1,
            "exactly the written collection's row lands"
        );
        Ok(())
    }
}

/// Post-settle hook visibility: `finalize` drains the event's dirty overlay
/// on success, so the apply hooks read the **lower store** — the per-cell
/// committed projection, where an own-event provisional cell answers its
/// committed base `prev` — never the event's pre-settle overlay. One pin per
/// ruled-on window: the arm-shutdown rollback's `after_abort` reads the
/// restored committed base; the ambiguous marker-record shutdown's
/// `after_abort` reads `prev` (staged cells deliberately left provisional);
/// the `Incomplete`-promote `after_commit` reads the mixed per-cell view
/// (promoted cells the new values, un-promoted cells `prev`).
mod hook_visibility {
    use super::staged_rollback::{Ctx, buffered, is_provisional};
    use super::*;
    use crate::codec::JsonCodec;
    use crate::consumer::middleware::tests::test_support::RecordingOracle;
    use crate::consumer::middleware::tests::test_support::TestLifecycleAccess;
    use crate::consumer::partition::ShutdownPhase;
    use crate::loader::MemoryLoader;
    use crate::state::descriptor::value_state;
    use crate::state::dirty::DirtyStore;
    use crate::state::memory::{MemoryCellStore, MemoryCells, MemoryDescriptorIdentityStore};
    use crate::state::oracle::CommitOracle;
    use crate::state::registry::{CollectionDef, CollectionDefRegistry};
    use crate::state::session::{
        CellRead, CellWrite, KeyedStateSession, SessionParts, TerminationWatch,
    };
    use crate::state::store::CellStore;
    use crate::state::tests::cell_suite::{FailingCellStore, value_cell};
    use crate::state::{
        CollectionId, CollectionRef, CommitDecision, EventRef, PartitionBackend, StateKey,
        StateName, StateType,
    };
    use crate::timers::duration::CompactDuration;
    use bytes::Bytes;
    use color_eyre::eyre::Result;
    use tokio::sync::watch;
    use uuid::Uuid;

    /// Which apply hook fired.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum Hook {
        Commit,
        Abort,
    }

    /// Per hook firing, the bytes each probed collection's Value cell
    /// answered — read through the event's own session
    /// (`context.test_lifecycle()`), exactly the view a user hook gets. Read
    /// errors are captured as strings so an erroring read fails the exact
    /// assertion instead of vanishing.
    type HookReads = Vec<(Hook, Vec<Result<Option<Bytes>, String>>)>;

    /// Probe handler recording a raw read of each named collection inside
    /// every apply hook. `on_message`/`on_timer` are unused — the settle
    /// boundary is driven directly.
    #[derive(Clone)]
    struct HookProbe {
        names: Vec<StateName>,
        reads: Arc<Mutex<HookReads>>,
    }

    impl HookProbe {
        fn new(names: Vec<StateName>) -> Self {
            Self {
                names,
                reads: Arc::default(),
            }
        }

        fn reads(&self) -> HookReads {
            self.reads.lock().clone()
        }

        async fn record<C>(&self, hook: Hook, context: &C)
        where
            C: EventContext,
        {
            let mut values = Vec::with_capacity(self.names.len());
            match context.test_lifecycle() {
                Ok(session) => {
                    for name in &self.names {
                        values.push(
                            session
                                .get(StateType::Application, name, &value_cell())
                                .await
                                .map_err(|e| e.to_string()),
                        );
                    }
                }
                Err(e) => values.push(Err(format!("lifecycle bind failed: {e}"))),
            }
            self.reads.lock().push((hook, values));
        }
    }

    impl FallibleHandler for HookProbe {
        type Error = TestError;
        type Output = u64;
        type Payload = serde_json::Value;

        async fn on_message<C>(
            &self,
            _context: C,
            _message: ConsumerMessage<Self::Payload>,
            _demand_type: DemandType,
        ) -> Result<Self::Output, Self::Error>
        where
            C: EventContext<Payload = Self::Payload>,
        {
            Ok(0)
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
            Ok(0)
        }

        async fn after_commit<C>(&self, context: C, _result: Result<Self::Output, Self::Error>)
        where
            C: EventContext<Payload = Self::Payload>,
        {
            self.record(Hook::Commit, &context).await;
        }

        async fn after_abort<C>(&self, context: C, _result: Result<Self::Output, Self::Error>)
        where
            C: EventContext<Payload = Self::Payload>,
        {
            self.record(Hook::Abort, &context).await;
        }

        async fn shutdown(self) {}
    }

    impl SettlementHandler for HookProbe {
        fn settlement(_result: Result<&Self::Output, &Self::Error>) -> Settlement {
            Settlement::Final
        }
    }

    /// Window 1 — `after_abort` after the arm-shutdown rollback reads the
    /// **restored committed base**: the drain removed the dirty overlay and
    /// the receipt's rollback settled the staged cell back to `prev` before
    /// the hook fires, so the hook sees the truthful base, not the aborted
    /// write's bytes.
    #[tokio::test]
    async fn arm_shutdown_after_abort_reads_the_restored_committed_base() -> Result<()> {
        let (context, cell_store, cart_id) = buffered(Ctx::with_shutdown_on_timer_read).await?;
        // Seed the committed base the rollback restores. Safe after the
        // buffered set: finalize captures `prev` later, inside settle.
        cell_store
            .write_resolved(
                &CollectionRef::new(cart_id.clone(), None),
                &[(value_cell(), Some(Bytes::from_static(b"base")))],
                &[],
            )
            .await?;
        let handler = HookProbe::new(vec![StateName::try_new("cart")?]);
        let committed = Arc::new(AtomicUsize::new(0));
        let aborted = Arc::new(AtomicUsize::new(0));
        let guard = RecordingGuard {
            committed: committed.clone(),
            aborted: aborted.clone(),
        };

        settle(&handler, context, guard, Ok(0)).await;

        assert_eq!(aborted.load(Ordering::SeqCst), 1, "arm-shutdown aborts");
        assert_eq!(committed.load(Ordering::SeqCst), 0);
        assert!(
            !is_provisional(&cell_store, &cart_id).await?,
            "the receipt's rollback settled the staged cell before the hook",
        );
        assert_eq!(
            handler.reads(),
            vec![(Hook::Abort, vec![Ok(Some(Bytes::from_static(b"base")))])],
            "after_abort reads the restored committed base, not the aborted write",
        );
        Ok(())
    }

    /// Oracle whose every `record_message` flips shutdown on the stored
    /// context and fails Transient, so the record loop's next top sees
    /// shutdown — the ambiguous marker-record window (the marker MAY be
    /// durable, so nothing may roll back). `resolve` answers `NotCommitted`
    /// but is never consulted here: the hook's own-event read short-circuits
    /// to `prev` without an oracle read.
    #[derive(Clone)]
    struct FlushTripOracle {
        trip: MockEventContext,
        attempts: Arc<AtomicUsize>,
    }

    impl FlushTripOracle {
        fn new(trip: MockEventContext) -> Self {
            Self {
                trip,
                attempts: Arc::default(),
            }
        }
    }

    impl CommitOracle for FlushTripOracle {
        type Error = TestError;

        async fn record_message(&self, _dedup_id: Uuid) -> Result<(), Self::Error> {
            self.attempts.fetch_add(1, Ordering::SeqCst);
            self.trip.request_shutdown();
            Err(TestError(ErrorCategory::Transient, "record"))
        }

        async fn resolve<'a>(
            &'a self,
            _state_key: &'a StateKey,
            _event: EventRef,
        ) -> Result<CommitDecision, Self::Error> {
            Ok(CommitDecision::NotCommitted)
        }
    }

    /// Window 2 — `after_abort` in the ambiguous marker-record shutdown window
    /// reads `prev`: a record attempt was made, so the staged cells are
    /// deliberately left provisional (`certify` consumed the receipt — no
    /// rollback compiles), and the hook's own-event read short-circuits to
    /// the committed base without settling anything.
    #[tokio::test(start_paused = true)]
    async fn ambiguous_record_shutdown_after_abort_reads_prev() -> Result<()> {
        type TripBackend = PartitionBackend<
            FlushTripOracle,
            MemoryDescriptorIdentityStore,
            MemoryCellStore<FlushTripOracle>,
        >;

        let cart = StateName::try_new("cart")?;
        let mut registry = CollectionDefRegistry::default();
        registry.register(&value_state::<JsonCodec>("cart"), CollectionDef::new(None))?;
        let registry = Arc::new(registry);
        // The stored clone shares the Arc'd shutdown watch with the typed
        // context below, so the oracle's flip is visible to settle's polls.
        let base: MockEventContext = MockEventContext::new();
        let oracle = FlushTripOracle::new(base.clone());
        let cells = MemoryCells::new();
        let cell_store = MemoryCellStore::new(cells.clone(), oracle.clone(), registry.clone());
        let state_key = StateKey::new(Uuid::from_u128(0xF1), Arc::from("user-1"));
        let cart_id = CollectionId::new(state_key.clone(), StateType::Application, cart.clone());

        cell_store
            .write_resolved(
                &CollectionRef::new(cart_id.clone(), None),
                &[(value_cell(), Some(Bytes::from_static(b"prev")))],
                &[],
            )
            .await?;

        let (_shutdown_tx, shutdown_rx) = watch::channel(ShutdownPhase::default());
        let (_cancel_tx, cancel_rx) = watch::channel(false);
        let session: KeyedStateSession<TripBackend, MemoryLoader<serde_json::Value>> =
            KeyedStateSession::new(SessionParts {
                cell: cell_store,
                dirty: Arc::new(DirtyStore::new()),
                oracle: oracle.clone(),
                loader: MemoryLoader::new(),
                registry,
                state_key,
                event: EventRef::Message {
                    dedup_id: Uuid::from_u128(0xF1),
                },
                recovery_delay: CompactDuration::new(30),
                armed: Arc::default(),
                termination: TerminationWatch::new(shutdown_rx, cancel_rx),
                publisher: None,
            });
        session
            .set(StateType::Application, &cart, &value_cell(), b"staged")
            .await?;
        let context = base.with_session(session);

        let handler = HookProbe::new(vec![cart]);
        let committed = Arc::new(AtomicUsize::new(0));
        let aborted = Arc::new(AtomicUsize::new(0));
        let guard = RecordingGuard {
            committed: committed.clone(),
            aborted: aborted.clone(),
        };

        settle(&handler, context, guard, Ok(0)).await;

        assert_eq!(
            aborted.load(Ordering::SeqCst),
            1,
            "the ambiguous record abandons"
        );
        assert_eq!(committed.load(Ordering::SeqCst), 0);
        assert_eq!(
            oracle.attempts.load(Ordering::SeqCst),
            1,
            "exactly one record attempt preceded the shutdown — the ambiguity trigger",
        );
        assert_eq!(
            handler.reads(),
            vec![(Hook::Abort, vec![Ok(Some(Bytes::from_static(b"prev")))])],
            "after_abort reads prev through the own-event short-circuit",
        );
        assert!(
            !cells.provisional_coordinates(&cart_id).is_empty(),
            "the staged cell stays provisional for the armed sweep — the hook read settled nothing",
        );
        Ok(())
    }

    /// Window 3 — the `Incomplete`-promote `after_commit` reads the **mixed
    /// per-cell committed projection**: the promoted collection answers its
    /// new value, the un-promoted one answers `prev` through the own-event
    /// short-circuit — never uncommitted bytes, never a durable write from
    /// the hook read itself.
    #[tokio::test]
    async fn incomplete_promote_after_commit_reads_the_mixed_per_cell_view() -> Result<()> {
        type SplitStore = FailingCellStore<MemoryCellStore<RecordingOracle>>;
        type SplitBackend =
            PartitionBackend<RecordingOracle, MemoryDescriptorIdentityStore, SplitStore>;

        let cart = StateName::try_new("cart")?;
        let wishlist = StateName::try_new("wishlist")?;
        let mut registry = CollectionDefRegistry::default();
        for name in ["cart", "wishlist"] {
            registry.register(&value_state::<JsonCodec>(name), CollectionDef::new(None))?;
        }
        let registry = Arc::new(registry);
        let oracle = RecordingOracle::new();
        let recorded = oracle.recorded();
        let cells = MemoryCells::new();
        // Poison cart's PROMOTE path only (`commit_provisional` fails
        // Permanent); the stage and the seeding writes stay healthy.
        let store = FailingCellStore::new(
            MemoryCellStore::new(cells.clone(), oracle.clone(), registry.clone()),
            cart.clone(),
        );
        let state_key = StateKey::new(Uuid::from_u128(0xF2), Arc::from("user-1"));
        let cart_id = CollectionId::new(state_key.clone(), StateType::Application, cart.clone());
        let wishlist_id =
            CollectionId::new(state_key.clone(), StateType::Application, wishlist.clone());

        for (id, base) in [(&cart_id, b"A0"), (&wishlist_id, b"B0")] {
            store
                .write_resolved(
                    &CollectionRef::new(id.clone(), None),
                    &[(value_cell(), Some(Bytes::from_static(base)))],
                    &[],
                )
                .await?;
        }

        let (_shutdown_tx, shutdown_rx) = watch::channel(ShutdownPhase::default());
        let (_cancel_tx, cancel_rx) = watch::channel(false);
        let session: KeyedStateSession<SplitBackend, MemoryLoader<serde_json::Value>> =
            KeyedStateSession::new(SessionParts {
                cell: store,
                dirty: Arc::new(DirtyStore::new()),
                oracle,
                loader: MemoryLoader::new(),
                registry,
                state_key,
                event: EventRef::Message {
                    dedup_id: Uuid::from_u128(0xF2),
                },
                recovery_delay: CompactDuration::new(30),
                armed: Arc::default(),
                termination: TerminationWatch::new(shutdown_rx, cancel_rx),
                publisher: None,
            });
        session
            .set(StateType::Application, &cart, &value_cell(), b"A1")
            .await?;
        session
            .set(StateType::Application, &wishlist, &value_cell(), b"B1")
            .await?;
        let context = MockEventContext::new().with_session(session);

        let handler = HookProbe::new(vec![cart, wishlist]);
        let committed = Arc::new(AtomicUsize::new(0));
        let aborted = Arc::new(AtomicUsize::new(0));
        let guard = RecordingGuard {
            committed: committed.clone(),
            aborted: aborted.clone(),
        };

        settle(&handler, context, guard, Ok(0)).await;

        assert_eq!(committed.load(Ordering::SeqCst), 1, "the event committed");
        assert_eq!(aborted.load(Ordering::SeqCst), 0);
        assert_eq!(
            recorded.lock().as_slice(),
            [Uuid::from_u128(0xF2)],
            "the session's own message marker recorded before the commit",
        );
        assert_eq!(
            handler.reads(),
            vec![(
                Hook::Commit,
                vec![
                    // Un-promoted: own-event provisional short-circuits to prev.
                    Ok(Some(Bytes::from_static(b"A0"))),
                    // Promoted: the new committed value.
                    Ok(Some(Bytes::from_static(b"B1"))),
                ],
            )],
            "after_commit reads the mixed per-cell committed projection",
        );
        // Raw residue probes: the hook read issued no durable write — cart is
        // still provisional for the armed sweep, wishlist promoted clean.
        assert!(
            !cells.provisional_coordinates(&cart_id).is_empty(),
            "cart stays provisional after the Incomplete promote",
        );
        assert!(
            cells.provisional_coordinates(&wishlist_id).is_empty(),
            "wishlist promoted clean",
        );
        Ok(())
    }
}

/// `ArmState` amortization: while a `StateRecovery` backstop stands for a key,
/// later stateful commits on that key skip re-arming, so a burst issues at most
/// one timer-store write per backstop generation.
mod backstop_amortization {
    use super::*;
    use crate::consumer::middleware::tests::test_support::TestLifecycleAccess;
    use crate::loader::MemoryLoader;
    use crate::state::StateKey;
    use crate::state::descriptor::tests::test_session_with_armed;
    use crate::state::descriptor::{Registered, ValueDescriptor, value_state};
    use crate::state::manager::ArmedKeys;
    use crate::state::registry::{CollectionDef, CollectionDefRegistry};
    use crate::state::session::Finalized;
    use crate::state::session::sealed::StateLifecycle;
    use color_eyre::eyre::{Result, bail, eyre};
    use serde_json::json;
    use uuid::Uuid;

    fn cart() -> ValueDescriptor {
        value_state("cart")
    }

    /// Five commits on one key, all sharing the partition's `armed` set, arm
    /// the backstop exactly once: the first commit schedules, the rest skip
    /// while it stands.
    #[tokio::test]
    async fn commits_while_armed_schedule_at_most_once() -> Result<()> {
        const COMMITS: usize = 5;
        let armed: ArmedKeys = Arc::default();
        let state_key = StateKey::new(Uuid::from_u128(0x9), Arc::from("hot-key"));
        let mut total_scheduled = 0;

        for i in 0..COMMITS {
            let mut registry = CollectionDefRegistry::default();
            registry.register(&cart(), CollectionDef::new(None))?;
            // A fresh session per event, all sharing the one `armed` set and key
            // — exactly how the manager mints sessions for a partition.
            let (session, _store) = test_session_with_armed(
                MemoryLoader::new(),
                registry,
                state_key.clone(),
                armed.clone(),
            );
            let context = MockEventContext::new()
                .with_session(session)
                .with_timer_tracking();

            // Stage a cell so arming is warranted.
            let handle = context
                .state(Registered::new(cart()))
                .map_err(|e| eyre!("bind: {e}"))?;
            handle.set(json!({ "i": i as i32 })).await?;
            let lifecycle = context
                .test_lifecycle()
                .map_err(|e| eyre!("lifecycle: {e}"))?;
            let Finalized::Staged(staged) = lifecycle
                .finalize()
                .await
                .map_err(|e| eyre!("finalize: {e}"))?
            else {
                bail!("expected a staged receipt");
            };

            let outcome = arm_backstop(&context, &lifecycle, staged.recovery_delay()).await;
            assert!(
                matches!(outcome, ArmOutcome::Armed),
                "arm must succeed every commit"
            );
            total_scheduled += context.count_scheduled(TimerType::StateRecovery);
        }

        assert_eq!(
            total_scheduled, 1,
            "only the first commit of the armed generation schedules a backstop"
        );
        Ok(())
    }

    /// The amortization is per key: a commit on a different key arms its own
    /// backstop even while the first key's stands.
    #[tokio::test]
    async fn a_different_key_arms_independently() -> Result<()> {
        let armed: ArmedKeys = Arc::default();
        let mut scheduled = 0;

        for raw_key in ["key-a", "key-b"] {
            let state_key = StateKey::new(Uuid::from_u128(0xA), Arc::from(raw_key));
            let mut registry = CollectionDefRegistry::default();
            registry.register(&cart(), CollectionDef::new(None))?;
            let (session, _store) =
                test_session_with_armed(MemoryLoader::new(), registry, state_key, armed.clone());
            let context = MockEventContext::new()
                .with_session(session)
                .with_timer_tracking();
            let handle = context
                .state(Registered::new(cart()))
                .map_err(|e| eyre!("bind: {e}"))?;
            handle.set(json!({ "x": 1_i32 })).await?;
            let lifecycle = context
                .test_lifecycle()
                .map_err(|e| eyre!("lifecycle: {e}"))?;
            let Finalized::Staged(staged) = lifecycle
                .finalize()
                .await
                .map_err(|e| eyre!("finalize: {e}"))?
            else {
                bail!("expected a staged receipt");
            };
            arm_backstop(&context, &lifecycle, staged.recovery_delay()).await;
            scheduled += context.count_scheduled(TimerType::StateRecovery);
        }

        assert_eq!(scheduled, 2, "each distinct key arms its own backstop");
        Ok(())
    }
}

/// `arm_backstop` is arm-if-sooner: it (re-)arms the per-key `StateRecovery`
/// backstop only when a newly-staged commit's fire is strictly sooner than the
/// standing one. A per-collection `recovery_within` can thereby *tighten* the
/// single timer, while a later, looser commit keeps the tighter one — so every
/// staged cell is swept no later than its own bound and the amortized single
/// timer is preserved.
mod arm_backstop {
    use super::*;
    use crate::Key;
    use crate::codec::JsonCodec;
    use crate::consumer::middleware::tests::test_support::TestLifecycleAccess;
    use crate::consumer::middleware::tests::test_support::TimerOperation;
    use crate::loader::MemoryLoader;
    use crate::state::StateKey;
    use crate::state::descriptor::tests::{TestSession, test_session_with_armed};
    use crate::state::descriptor::{Registered, value_state};
    use crate::state::manager::ArmedKeys;
    use crate::state::registry::{CollectionDef, CollectionDefRegistry};
    use crate::state::session::Finalized;
    use crate::state::session::sealed::StateLifecycle;
    use crate::timers::duration::CompactDuration;
    use color_eyre::eyre::{Result, bail, eyre};
    use futures::executor;
    use quickcheck::{QuickCheck, TestResult};
    use serde_json::json;
    use uuid::Uuid;

    const FLOOR_SECS: u32 = 30;

    /// The fire time of the most recent `StateRecovery` `clear_and_schedule`,
    /// or `None` if the arm did not (re-)schedule one.
    fn scheduled_recovery_fire(ops: &[TimerOperation]) -> Option<CompactDateTime> {
        ops.iter().rev().find_map(|op| match op {
            TimerOperation::ClearAndSchedule(fire, TimerType::StateRecovery) => Some(*fire),
            _ => None,
        })
    }

    /// Stages a cell in one `ReadCommitted` value collection per entry in
    /// `bounds` (each carrying that `recovery_within`), sharing `armed`/`key`
    /// with prior events, then runs `arm_backstop`. `durable` seeds a
    /// `StateRecovery` timer standing in the mock's durable store (a prior
    /// epoch's backstop). Brackets the arm with `now` so the caller can bound
    /// the scheduled fire time; returns the context for op/durable inspection.
    async fn run_arm(
        bounds: &[Option<u32>],
        key: &StateKey,
        armed: &ArmedKeys,
        durable: Option<CompactDateTime>,
    ) -> Result<(
        CompactDateTime,
        CompactDateTime,
        MockEventContext<serde_json::Value, TestSession>,
    )> {
        let mut registry = CollectionDefRegistry::default();
        for (i, within) in bounds.iter().enumerate() {
            registry.register(
                &value_state::<JsonCodec>(&format!("c{i}")),
                CollectionDef {
                    recovery_within: within.map(CompactDuration::new),
                    ..CollectionDef::new(None)
                },
            )?;
        }
        let (session, _store) =
            test_session_with_armed(MemoryLoader::new(), registry, key.clone(), armed.clone());
        let mut context = MockEventContext::new()
            .with_session(session)
            .with_timer_tracking();
        if let Some(time) = durable {
            context = context.with_durable_timer(time, TimerType::StateRecovery);
        }
        for i in 0..bounds.len() {
            let handle = context
                .state(Registered::new(value_state::<JsonCodec>(&format!("c{i}"))))
                .map_err(|e| eyre!("bind: {e}"))?;
            handle.set(json!({ "v": i as i32 })).await?;
        }
        let lifecycle = context
            .test_lifecycle()
            .map_err(|e| eyre!("lifecycle: {e}"))?;
        let Finalized::Staged(staged) = lifecycle
            .finalize()
            .await
            .map_err(|e| eyre!("finalize: {e}"))?
        else {
            bail!("expected a staged receipt (the props stage at least one collection)");
        };
        let before = CompactDateTime::now()?;
        arm_backstop(&context, &lifecycle, staged.recovery_delay()).await;
        let after = CompactDateTime::now()?;
        Ok((before, after, context))
    }

    /// Arm-if-sooner **and** the convergence bound, as one property over
    /// `(bounds, standing)`:
    ///
    /// - **Fire time** — a staged event schedules its `StateRecovery` sweep at
    ///   `now + min(recovery_delay, tightest touched recovery_within)`, so no
    ///   provisional cell outlives its collection's bound and the fire never
    ///   exceeds the floor (hence stays below every collection's TTL, the
    ///   `TtlBelowRecoveryDelay` invariant — no dedup-margin regression).
    /// - **Arm-if-sooner** — a standing backstop is re-armed **iff** the new
    ///   fire is strictly sooner; on re-arm `ArmedKeys` holds the scheduled
    ///   fire, and when kept the standing fire is left untouched.
    ///
    /// `standing`: `None` = unarmed → arm; `Some(true)` = a far-future standing
    /// fire (looser) → must tighten; `Some(false)` = a far-past one (tighter) →
    /// must keep. The far-future/past extremes make the strict-`<` decision
    /// immune to sub-second wall-clock drift across the bracketing `now` reads.
    #[test]
    fn prop_arm_backstop_arms_iff_new_fire_is_sooner() {
        async fn check(bounds: &[Option<u32>], standing: Option<bool>) -> Result<()> {
            let armed: ArmedKeys = Arc::default();
            let raw_key: Key = Arc::from("k");
            let key = StateKey::new(Uuid::from_u128(0xC0), raw_key.clone());

            // Seed the standing backstop at an extreme so the decision cannot
            // flip on sub-second drift: MAX is always later than the new fire
            // (must tighten), MIN always earlier-or-equal (must keep).
            let seed = standing.map(|future| {
                if future {
                    CompactDateTime::MAX
                } else {
                    CompactDateTime::MIN
                }
            });
            if let Some(seed) = seed {
                let _ = armed.insert_async(raw_key.clone(), seed).await;
            }

            let (before, after, context) = run_arm(bounds, &key, &armed, None).await?;
            let ops = context.timer_operations();
            let delay = bounds.iter().filter_map(|o| *o).fold(FLOOR_SECS, u32::min);
            let stored = armed.read_async(&raw_key, |_, &f| f).await;
            let scheduled = scheduled_recovery_fire(&ops);

            // Re-arm unless a sooner-or-equal backstop (the far-past seed)
            // already stands.
            if standing == Some(false) {
                if scheduled.is_some() {
                    return Err(eyre!("a sooner-or-equal standing backstop must not re-arm"));
                }
                if stored != seed {
                    return Err(eyre!(
                        "the kept path must leave the standing fire untouched"
                    ));
                }
                return Ok(());
            }

            let fire = scheduled.ok_or_else(|| eyre!("expected an arm, none scheduled"))?;
            let (lo, hi) = (
                before.epoch_seconds() + delay,
                after.epoch_seconds() + delay,
            );
            if !(lo..=hi).contains(&fire.epoch_seconds()) {
                return Err(eyre!(
                    "fire {} not in [{lo},{hi}] (delay {delay}s ≤ floor {FLOOR_SECS}s)",
                    fire.epoch_seconds(),
                ));
            }
            if stored != Some(fire) {
                return Err(eyre!("stored fire {stored:?} != scheduled {fire:?}"));
            }
            Ok(())
        }

        fn prop(raw: Vec<Option<u16>>, standing: Option<bool>) -> TestResult {
            // ≥1 collection (so something stages); bounded count keeps the
            // interned-name set small.
            if raw.is_empty() || raw.len() > 6 {
                return TestResult::discard();
            }
            let bounds: Vec<Option<u32>> = raw.into_iter().map(|o| o.map(u32::from)).collect();
            match executor::block_on(check(&bounds, standing)) {
                Ok(()) => TestResult::passed(),
                Err(e) => TestResult::error(e.to_string()),
            }
        }
        QuickCheck::new().quickcheck(prop as fn(Vec<Option<u16>>, Option<bool>) -> TestResult);
    }

    /// Never-loosen across reacquisition: `ArmedKeys` is minted empty per
    /// acquisition while a prior epoch's backstop survives in the durable
    /// trigger store, so the first arm on a key must consult the durable
    /// store. A sooner standing durable fire is kept (no singleton overwrite)
    /// and seeded into `ArmedKeys`; a later one is tightened. Either way the
    /// standing durable fire never moves later.
    ///
    /// `standing`: `None` = no durable backstop (plain first arm);
    /// `Some(false)` = far-past durable fire (sooner) → must keep;
    /// `Some(true)` = far-future durable fire (later) → must tighten. The
    /// extremes make the decision immune to sub-second wall-clock drift, as in
    /// `prop_arm_backstop_arms_iff_new_fire_is_sooner`.
    #[test]
    fn prop_reacquisition_never_loosens_standing_backstop() {
        async fn check(bounds: &[Option<u32>], standing: Option<bool>) -> Result<()> {
            // Fresh per-acquisition RAM: the durable seed is the only record
            // of the prior epoch's backstop.
            let armed: ArmedKeys = Arc::default();
            let raw_key: Key = Arc::from("k");
            let key = StateKey::new(Uuid::from_u128(0xC1), raw_key.clone());
            let durable = standing.map(|future| {
                if future {
                    CompactDateTime::MAX
                } else {
                    CompactDateTime::MIN
                }
            });

            let (before, after, context) = run_arm(bounds, &key, &armed, durable).await?;
            let ops = context.timer_operations();
            let now_durable = context.durable_scheduled(TimerType::StateRecovery);
            let stored = armed.read_async(&raw_key, |_, &f| f).await;
            let scheduled = scheduled_recovery_fire(&ops);
            let delay = bounds.iter().filter_map(|o| *o).fold(FLOOR_SECS, u32::min);

            if standing == Some(false) {
                if scheduled.is_some() {
                    return Err(eyre!(
                        "a sooner durable backstop must not be overwritten (loosened)"
                    ));
                }
                if now_durable != vec![CompactDateTime::MIN] {
                    return Err(eyre!(
                        "the sooner durable fire must be left standing, got {now_durable:?}"
                    ));
                }
                if stored != Some(CompactDateTime::MIN) {
                    return Err(eyre!(
                        "the durable fire must seed the fresh ArmedKeys, got {stored:?}"
                    ));
                }
                return Ok(());
            }

            // No durable backstop, or a later (far-future) one: arm/tighten.
            let fire = scheduled.ok_or_else(|| eyre!("expected an arm, none scheduled"))?;
            let (lo, hi) = (
                before.epoch_seconds() + delay,
                after.epoch_seconds() + delay,
            );
            if !(lo..=hi).contains(&fire.epoch_seconds()) {
                return Err(eyre!("fire {} not in [{lo},{hi}]", fire.epoch_seconds()));
            }
            if now_durable != vec![fire] {
                return Err(eyre!(
                    "the singleton overwrite must leave exactly the new fire standing, got \
                     {now_durable:?}"
                ));
            }
            if stored != Some(fire) {
                return Err(eyre!("stored fire {stored:?} != scheduled {fire:?}"));
            }
            Ok(())
        }

        fn prop(raw: Vec<Option<u16>>, standing: Option<bool>) -> TestResult {
            if raw.is_empty() || raw.len() > 6 {
                return TestResult::discard();
            }
            let bounds: Vec<Option<u32>> = raw.into_iter().map(|o| o.map(u32::from)).collect();
            match executor::block_on(check(&bounds, standing)) {
                Ok(()) => TestResult::passed(),
                Err(e) => TestResult::error(e.to_string()),
            }
        }
        QuickCheck::new().quickcheck(prop as fn(Vec<Option<u16>>, Option<bool>) -> TestResult);
    }
}

/// The success-path marker record is **must-succeed**: `settle` retries a
/// failed record of ANY category — Transient, Terminal, and Permanent alike —
/// until the marker lands. The marker is framework bookkeeping, never a data
/// rejection: skipping a Permanent failure would commit the offset with the
/// stage uncertified, and the armed sweep would then silently roll a
/// successful handler's writes back with no redelivery to replay them. The
/// marker itself is the session's boundary-readable event identity
/// (`message_marker()`), so these pins also prove the identity sources: a
/// message session records its `EventRef` dedup id; a pure timer session
/// records nothing.
mod marker_record_must_succeed {
    use super::*;
    use crate::consumer::partition::ShutdownPhase;
    use crate::loader::MemoryLoader;
    use crate::state::cell::Committed;
    use crate::state::cell_key::{CellKey, Coordinate, Section};
    use crate::state::descriptor::{Registered, ValueDescriptor, value_state};
    use crate::state::dirty::DirtyStore;
    use crate::state::memory::{MemoryCellStore, MemoryCells, MemoryDescriptorIdentityStore};
    use crate::state::oracle::CommitOracle;
    use crate::state::registry::{CollectionDef, CollectionDefRegistry};
    use crate::state::session::{KeyedStateSession, SessionParts, TerminationWatch};
    use crate::state::store::CellStore;
    use crate::state::{
        CollectionId, CommitDecision, EventRef, PartitionBackend, StateKey, StateName, StateType,
        TimerEventRef,
    };
    use crate::timers::datetime::CompactDateTime;
    use crate::timers::duration::CompactDuration;
    use color_eyre::eyre::{Result, eyre};
    use futures::StreamExt;
    use quickcheck::{QuickCheck, TestResult};
    use serde_json::json;
    use thiserror::Error;
    use tokio::runtime::Builder;
    use tokio::sync::watch;
    use uuid::Uuid;

    /// Marker-store failure with a configured classification.
    #[derive(Debug, Error)]
    #[error("mock marker store failed ({0:?})")]
    struct MockMarkerError(ErrorCategory);

    impl ClassifyError for MockMarkerError {
        fn classify_error(&self) -> ErrorCategory {
            self.0
        }
    }

    /// Oracle whose `record_message` fails a configured number of times with a
    /// configured category before succeeding, logging every recorded id;
    /// `resolve` always answers Committed.
    #[derive(Clone)]
    struct FlakyMarkerOracle {
        remaining: Arc<AtomicUsize>,
        category: ErrorCategory,
        recorded: Arc<Mutex<Vec<Uuid>>>,
    }

    impl FlakyMarkerOracle {
        fn new(fail_count: usize, category: ErrorCategory) -> Self {
            Self {
                remaining: Arc::new(AtomicUsize::new(fail_count)),
                category,
                recorded: Arc::default(),
            }
        }

        fn recorded(&self) -> Vec<Uuid> {
            self.recorded.lock().clone()
        }
    }

    impl CommitOracle for FlakyMarkerOracle {
        type Error = MockMarkerError;

        async fn record_message(&self, dedup_id: Uuid) -> Result<(), Self::Error> {
            // While the countdown is positive, decrement it and inject one
            // more failure; once exhausted, record the marker.
            if self
                .remaining
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok()
            {
                return Err(MockMarkerError(self.category));
            }
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

    type FlakyBackend = PartitionBackend<
        FlakyMarkerOracle,
        MemoryDescriptorIdentityStore,
        MemoryCellStore<FlakyMarkerOracle>,
    >;
    type FlakySession = KeyedStateSession<FlakyBackend, MemoryLoader<serde_json::Value>>;

    /// The fixed message dedup id the sessions below carry on their
    /// `EventRef` — the identity the boundary reads and records.
    const DEDUP_ID: Uuid = Uuid::from_u128(0xFEE1);

    fn cart() -> ValueDescriptor {
        value_state("cart")
    }

    /// A real session for `event` whose marker record routes through
    /// `oracle`, plus the shared durable cell store and the `cart`
    /// collection id for post-settle inspection.
    fn flaky_session(
        oracle: FlakyMarkerOracle,
        event: EventRef,
    ) -> Result<(
        FlakySession,
        MemoryCellStore<FlakyMarkerOracle>,
        CollectionId,
    )> {
        let mut registry = CollectionDefRegistry::default();
        registry.register(&cart(), CollectionDef::new(None))?;
        let registry = Arc::new(registry);
        let cell_store = MemoryCellStore::new(MemoryCells::new(), oracle.clone(), registry.clone());
        let (_shutdown_tx, shutdown_rx) = watch::channel(ShutdownPhase::default());
        let (_cancel_tx, cancel_rx) = watch::channel(false);
        let state_key = StateKey::new(Uuid::from_u128(0xD), Arc::from("user-1"));
        let session = KeyedStateSession::new(SessionParts {
            cell: cell_store.clone(),
            dirty: Arc::new(DirtyStore::new()),
            oracle,
            loader: MemoryLoader::new(),
            registry,
            state_key: state_key.clone(),
            event,
            recovery_delay: CompactDuration::new(30),
            armed: Arc::default(),
            termination: TerminationWatch::new(shutdown_rx, cancel_rx),
            publisher: None,
        });
        let cart_id = CollectionId::new(
            state_key,
            StateType::Application,
            StateName::try_new("cart")?,
        );
        Ok((session, cell_store, cart_id))
    }

    /// Asserts the `cart` cell has no durable residue — neither a
    /// provisional cell nor a committed value — via raw probes that no
    /// resolving read can heal.
    async fn assert_no_durable_cart(
        cell_store: &MemoryCellStore<FlakyMarkerOracle>,
        cart_id: &CollectionId,
    ) -> Result<()> {
        let provisional = cell_store.provisional_cells(cart_id);
        futures::pin_mut!(provisional);
        assert!(
            provisional.next().await.transpose()?.is_none(),
            "no provisional cell may exist",
        );
        let probe = EventRef::Message {
            dedup_id: Uuid::from_u128(u128::MAX),
        };
        let cell = CellKey {
            section: Section::new(0),
            coordinate: Coordinate::empty(),
        };
        assert_eq!(
            Committed::into_inner(cell_store.get(cart_id, &cell, probe).await?),
            None,
            "no committed value may exist",
        );
        Ok(())
    }

    /// The error-path marker gate, Permanent direction: a **final** Permanent
    /// error records the session's message marker best-effort — with NO stage
    /// (the buffered write must leave no durable residue) — so the
    /// failed-but-final message deduplicates instead of re-running its
    /// failure on every redelivery.
    #[tokio::test]
    async fn err_permanent_records_the_marker_with_no_stage() -> Result<()> {
        let oracle = FlakyMarkerOracle::new(0, ErrorCategory::Transient);
        let (session, cell_store, cart_id) =
            flaky_session(oracle.clone(), EventRef::Message { dedup_id: DEDUP_ID })?;
        let context = MockEventContext::new().with_session(session);
        // Stage a dirty write so "no stage" is a real claim, not vacuous.
        let handle = context
            .state(Registered::new(cart()))
            .map_err(|e| eyre!("bind cart: {e}"))?;
        handle.set(json!({ "x": 1_i32 })).await?;

        let handler = ProbeHandler::ok(0);
        let committed = Arc::new(AtomicUsize::new(0));
        let aborted = Arc::new(AtomicUsize::new(0));
        let guard = RecordingGuard {
            committed: committed.clone(),
            aborted: aborted.clone(),
        };

        settle(
            &handler,
            context,
            guard,
            Err(TestError(ErrorCategory::Permanent, "final")),
        )
        .await;

        assert_eq!(
            oracle.recorded(),
            vec![DEDUP_ID],
            "a final Permanent error must record the message marker so the failure deduplicates",
        );
        assert_no_durable_cart(&cell_store, &cart_id).await?;
        assert_eq!(committed.load(Ordering::SeqCst), 1, "final errors commit");
        assert_eq!(aborted.load(Ordering::SeqCst), 0);
        Ok(())
    }

    /// The error-path marker gate, Transient direction: the record is gated
    /// to Permanent. A Transient final (no retry layer below took it) is not
    /// handled, so its marker must not certify anything.
    #[tokio::test]
    async fn err_transient_never_records_the_marker() -> Result<()> {
        let oracle = FlakyMarkerOracle::new(0, ErrorCategory::Transient);
        let (session, cell_store, cart_id) =
            flaky_session(oracle.clone(), EventRef::Message { dedup_id: DEDUP_ID })?;
        let context = MockEventContext::new().with_session(session);
        let handle = context
            .state(Registered::new(cart()))
            .map_err(|e| eyre!("bind cart: {e}"))?;
        handle.set(json!({ "x": 1_i32 })).await?;

        let handler = ProbeHandler::ok(0);
        let committed = Arc::new(AtomicUsize::new(0));
        let aborted = Arc::new(AtomicUsize::new(0));
        let guard = RecordingGuard {
            committed: committed.clone(),
            aborted: aborted.clone(),
        };

        settle(
            &handler,
            context,
            guard,
            Err(TestError(ErrorCategory::Transient, "final")),
        )
        .await;

        assert!(
            oracle.recorded().is_empty(),
            "a Transient final error must NOT record a marker over never-staged state",
        );
        assert_no_durable_cart(&cell_store, &cart_id).await?;
        assert_eq!(committed.load(Ordering::SeqCst), 1, "final errors commit");
        assert_eq!(aborted.load(Ordering::SeqCst), 0);
        Ok(())
    }

    /// A pure timer never records a message marker on any path:
    /// `message_marker()` is `None` on a timer session with no reload
    /// override, so the Ok, Permanent, and Transient finals all settle
    /// marker-free (the trigger commit is the timer's dedup).
    #[tokio::test]
    async fn pure_timer_never_records_a_message_marker() -> Result<()> {
        let finals: [Result<u64, TestError>; 3] = [
            Ok(0),
            Err(TestError(ErrorCategory::Permanent, "final")),
            Err(TestError(ErrorCategory::Transient, "final")),
        ];
        for result in finals {
            let oracle = FlakyMarkerOracle::new(0, ErrorCategory::Transient);
            let timer = EventRef::Timer(TimerEventRef::new(
                TimerType::Application,
                CompactDateTime::from(1000_u32),
                0,
            ));
            let (session, _cell_store, _cart_id) = flaky_session(oracle.clone(), timer)?;
            let context = MockEventContext::new().with_session(session);
            let handler = ProbeHandler::ok(0);
            let committed = Arc::new(AtomicUsize::new(0));
            let aborted = Arc::new(AtomicUsize::new(0));
            let guard = RecordingGuard {
                committed: committed.clone(),
                aborted: aborted.clone(),
            };

            settle(&handler, context, guard, result).await;

            assert!(
                oracle.recorded().is_empty(),
                "a pure timer must never record a message marker",
            );
            assert_eq!(committed.load(Ordering::SeqCst), 1, "the trigger commits");
            assert_eq!(aborted.load(Ordering::SeqCst), 0);
        }
        Ok(())
    }

    /// However many leading marker-record failures of whatever category the
    /// oracle throws, `settle`'s success path self-heals: the offset commits
    /// exactly once, the marker is recorded exactly once (the stage is
    /// certified), and the staged cell is promoted — never left provisional
    /// for the sweep to roll back. Each iteration runs on its own paused
    /// runtime so the retry backoff advances instantly.
    #[test]
    fn prop_marker_record_self_heals_to_certified_commit() {
        fn property(fail_count: u8, category_sel: u8) -> TestResult {
            let fail_count = usize::from(fail_count % 6);
            let category = match category_sel % 3 {
                0 => ErrorCategory::Transient,
                1 => ErrorCategory::Permanent,
                _ => ErrorCategory::Terminal,
            };
            let runtime = Builder::new_current_thread()
                .enable_time()
                .start_paused(true)
                .build();
            let Ok(runtime) = runtime else {
                return TestResult::error("failed to build paused runtime");
            };
            runtime.block_on(async move {
                let oracle = FlakyMarkerOracle::new(fail_count, category);
                let event = EventRef::Message { dedup_id: DEDUP_ID };
                let (session, cell_store, cart_id) = match flaky_session(oracle.clone(), event) {
                    Ok(parts) => parts,
                    Err(e) => return TestResult::error(format!("setup: {e}")),
                };
                let context = MockEventContext::new().with_session(session);
                let Ok(handle) = context.state(Registered::new(cart())) else {
                    return TestResult::error("bind failed");
                };
                if let Err(e) = handle.set(json!({ "x": 1_i32 })).await {
                    return TestResult::error(format!("set: {e}"));
                }

                let handler = ProbeHandler::ok(0);
                let committed = Arc::new(AtomicUsize::new(0));
                let aborted = Arc::new(AtomicUsize::new(0));
                let guard = RecordingGuard {
                    committed: committed.clone(),
                    aborted: aborted.clone(),
                };

                settle(&handler, context, guard, Ok(0)).await;

                let committed = committed.load(Ordering::SeqCst);
                let aborted = aborted.load(Ordering::SeqCst);
                let recorded = oracle.recorded();
                let provisional = cell_store.provisional_cells(&cart_id);
                futures::pin_mut!(provisional);
                let still_provisional = matches!(provisional.next().await, Some(Ok(_)));
                let probe = EventRef::Message {
                    dedup_id: Uuid::from_u128(u128::MAX),
                };
                let value = match cell_store
                    .get(
                        &cart_id,
                        &CellKey {
                            section: Section::new(0),
                            coordinate: Coordinate::empty(),
                        },
                        probe,
                    )
                    .await
                {
                    Ok(committed) => Committed::into_inner(committed),
                    Err(e) => return TestResult::error(format!("read back: {e}")),
                };

                if committed != 1
                    || aborted != 0
                    || recorded != vec![DEDUP_ID]
                    || still_provisional
                    || value.is_none()
                {
                    return TestResult::error(format!(
                        "category={category:?} fail_count={fail_count}: committed={committed} \
                         aborted={aborted} recorded={recorded:?} provisional={still_provisional} \
                         promoted={}",
                        value.is_some()
                    ));
                }
                TestResult::passed()
            })
        }
        QuickCheck::new().quickcheck(property as fn(u8, u8) -> TestResult);
    }
}

/// Settlement classification tables for the wrappers without their own
/// tests module: the pure pass-throughs (retry mid-stack, log, timeout,
/// telemetry), `OptionHandler`'s per-branch delegation, and the `LeafHandler`
/// chain terminator. Delegation is proven
/// against [`BypassedHandler`], whose classification is `Bypassed` for every
/// result — a wrapper hardcoding `Final` fails those rows.
mod settlement_classification {
    use super::*;
    use crate::consumer::middleware::log::LogHandler;
    use crate::consumer::middleware::optional::{OptionError, OptionHandler, OptionOutput};
    use crate::consumer::middleware::providers::LeafHandler;
    use crate::consumer::middleware::retry::RetryHandler;
    use crate::consumer::middleware::telemetry::TelemetryHandler;
    use crate::consumer::middleware::tests::test_support::{
        BypassedHandler, ScriptedHandler, TestError as SupportError,
    };
    use crate::consumer::middleware::timeout::TimeoutHandler;
    use crate::consumer::middleware::{Settlement, SettlementHandler};

    /// The pure pass-throughs (retry mid-stack, log, timeout, telemetry, the
    /// test pass-through) delegate both sides verbatim.
    #[test]
    fn passthrough_wrappers_delegate_settlement() {
        fn assert_delegates<W, P>(label: &str)
        where
            W: SettlementHandler<Output = (), Error = SupportError>,
            P: SettlementHandler<Output = (), Error = SupportError>,
        {
            let ok: Result<(), SupportError> = Ok(());
            let err: Result<(), SupportError> = Err(SupportError(ErrorCategory::Permanent));
            assert_eq!(W::settlement(ok.as_ref()), Settlement::Final, "{label} Ok");
            assert_eq!(
                W::settlement(err.as_ref()),
                Settlement::Final,
                "{label} Err"
            );
            // Over a Bypassed probe, both sides stay Bypassed — the wrapper
            // is delegating, not hardcoding Final.
            assert_eq!(
                P::settlement(ok.as_ref()),
                Settlement::Bypassed,
                "{label} probe Ok"
            );
            assert_eq!(
                P::settlement(err.as_ref()),
                Settlement::Bypassed,
                "{label} probe Err"
            );
        }

        assert_delegates::<RetryHandler<ScriptedHandler>, RetryHandler<BypassedHandler>>("retry");
        assert_delegates::<LogHandler<ScriptedHandler>, LogHandler<BypassedHandler>>("log");
        assert_delegates::<TimeoutHandler<ScriptedHandler>, TimeoutHandler<BypassedHandler>>(
            "timeout",
        );
        assert_delegates::<TelemetryHandler<ScriptedHandler>, TelemetryHandler<BypassedHandler>>(
            "telemetry",
        );
        assert_delegates::<
            PassThroughMiddleware<ScriptedHandler>,
            PassThroughMiddleware<BypassedHandler>,
        >("pass-through");
    }

    /// The chain terminator classifies `Final` on both sides — the leaf's
    /// result is the event's own outcome, by definition.
    #[test]
    fn leaf_handler_is_final_on_both_sides() {
        type Subject = LeafHandler<ScriptedHandler>;
        let ok: Result<(), SupportError> = Ok(());
        let err: Result<(), SupportError> = Err(SupportError(ErrorCategory::Permanent));
        assert_eq!(Subject::settlement(ok.as_ref()), Settlement::Final);
        assert_eq!(Subject::settlement(err.as_ref()), Settlement::Final);
    }

    /// `OptionHandler` delegates to whichever branch produced the result,
    /// on both sides.
    #[test]
    fn option_handler_delegates_per_branch() {
        type Subject = OptionHandler<ScriptedHandler, BypassedHandler>;
        type Out = OptionOutput<(), ()>;
        type Err_ = OptionError<SupportError, SupportError>;

        let rows: Vec<(&str, Result<Out, Err_>, Settlement)> = vec![
            (
                "Enabled Ok delegates to the enabled branch (Final)",
                Ok(OptionOutput::Enabled(())),
                Settlement::Final,
            ),
            (
                "Disabled Ok delegates to the disabled branch (Bypassed probe)",
                Ok(OptionOutput::Disabled(())),
                Settlement::Bypassed,
            ),
            (
                "Enabled Err delegates to the enabled branch (Final)",
                Err(OptionError::Enabled(SupportError(ErrorCategory::Permanent))),
                Settlement::Final,
            ),
            (
                "Disabled Err delegates to the disabled branch (Bypassed probe)",
                Err(OptionError::Disabled(SupportError(
                    ErrorCategory::Permanent,
                ))),
                Settlement::Bypassed,
            ),
        ];
        for (label, result, expected) in rows {
            assert_eq!(Subject::settlement(result.as_ref()), expected, "{label}");
        }
    }
}

/// Settled-view discard: on every settle path that did **not** successfully
/// finalize, the boundary discards this event's uncommitted overlay under the
/// held closed gate before the apply hook fires, so a hook read — and a leaked
/// hook-window read — observes fully-settled committed truth with no
/// aborted-attempt residue, while an explicit mid-handler `commit()` floor
/// survives. Also pins the apply-hook contract (a hook mutation errors
/// `SessionClosed`, `rollback()` is an effectless `NoOp`), teardown fencing
/// (post-`terminate` ops error `Terminated`), and graceful hook-window reads.
mod settled_view {
    use super::*;
    use crate::codec::{JsonCodec, JsonCodecError};
    use crate::consumer::middleware::tests::test_support::TestLifecycleAccess;
    use crate::loader::MemoryLoader;
    use crate::state::access::StateAccessError;
    use crate::state::cell::Committed;
    use crate::state::descriptor::tests::{FixedOracle, TestSession, test_session_parts};
    use crate::state::descriptor::{CellStateError, Registered, ValueHandle, value_state};
    use crate::state::memory::MemoryCellStore;
    use crate::state::registry::{CollectionDef, CollectionDefRegistry};
    use crate::state::session::sealed::StateLifecycle;
    use crate::state::store::CellStore;
    use crate::state::tests::cell_suite::value_cell;
    use crate::state::{CollectionId, EventRef, StateKey, StateName, StateType, StoreOutcome};
    use color_eyre::eyre::{Result, bail, eyre};
    use serde_json::{Value, json};
    use std::marker::PhantomData;
    use std::sync::Arc;
    use uuid::Uuid;

    type Ctx = MockEventContext<Value, TestSession>;
    type Handle = ValueHandle<TestSession, JsonCodec>;

    const FLOOR: &str = "floor";
    const PENDING: &str = "pending";

    /// The settlement classification a [`ViewProbe`] reports, as a ZST marker
    /// so the associated (self-less) `settlement()` can read it.
    trait Classify: Clone + Send + Sync + 'static {
        const SETTLEMENT: Settlement;
    }

    #[derive(Clone)]
    struct AsFinal;
    impl Classify for AsFinal {
        const SETTLEMENT: Settlement = Settlement::Final;
    }

    #[derive(Clone)]
    struct AsBypassed;
    impl Classify for AsBypassed {
        const SETTLEMENT: Settlement = Settlement::Bypassed;
    }

    /// The name of the [`StateAccessError`] variant a fenced op hit, so an
    /// assertion names the exact fence rather than matching an opaque value.
    fn err_tag(error: &CellStateError<JsonCodecError>) -> String {
        match error {
            CellStateError::Access(StateAccessError::SessionClosed) => "SessionClosed".into(),
            CellStateError::Access(StateAccessError::Terminated) => "Terminated".into(),
            CellStateError::Access(StateAccessError::Unavailable) => "Unavailable".into(),
            other => format!("other: {other}"),
        }
    }

    /// What a fired hook observed through the event's own (stamped) session.
    #[derive(Clone, Default)]
    struct HookObservation {
        /// The `commit()`-floored collection's read — must survive the discard.
        floor: Option<Result<Option<Value>, String>>,
        /// The uncommitted collection's read — must be the committed base
        /// (`None`), never the discarded buffered set.
        pending: Option<Result<Option<Value>, String>>,
        /// A hook mutation attempt: the error-variant tag, or `Ok` if it
        /// wrongly succeeded.
        mutation: Option<Result<(), String>>,
        /// A hook `rollback()` outcome — must be an effectless `NoOp`.
        rollback: Option<StoreOutcome>,
    }

    /// Probe reading `floor` + `pending` and attempting a mutation/rollback
    /// through typed handles inside whichever apply hook fires, classifying
    /// settlement by `M`.
    #[derive(Clone)]
    struct ViewProbe<M> {
        seen: Arc<Mutex<Option<HookObservation>>>,
        _marker: PhantomData<fn() -> M>,
    }

    impl<M: Classify> ViewProbe<M> {
        fn new() -> Self {
            Self {
                seen: Arc::default(),
                _marker: PhantomData,
            }
        }

        fn observation(&self) -> Option<HookObservation> {
            self.seen.lock().clone()
        }

        async fn observe<C>(&self, context: &C)
        where
            C: EventContext<Payload = Value>,
        {
            let mut obs = HookObservation::default();
            match (handle(context, FLOOR), handle(context, PENDING)) {
                (Ok(floor), Ok(pending)) => {
                    obs.floor = Some(floor.get().await.map_err(|e| e.to_string()));
                    obs.pending = Some(pending.get().await.map_err(|e| e.to_string()));
                    // A hook mutation must be fenced: the gate is Closed.
                    obs.mutation = Some(pending.set(json!("hook")).await.map_err(|e| err_tag(&e)));
                    // rollback() on a closed session is an effectless NoOp.
                    obs.rollback = Some(pending.rollback().await);
                }
                _ => obs.pending = Some(Err("handle bind failed".into())),
            }
            *self.seen.lock() = Some(obs);
        }
    }

    impl<M: Classify> FallibleHandler for ViewProbe<M> {
        type Error = TestError;
        type Output = u64;
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
            Ok(0)
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
            Ok(0)
        }

        async fn after_commit<C>(&self, context: C, _result: Result<Self::Output, Self::Error>)
        where
            C: EventContext<Payload = Self::Payload>,
        {
            self.observe(&context).await;
        }

        async fn after_abort<C>(&self, context: C, _result: Result<Self::Output, Self::Error>)
        where
            C: EventContext<Payload = Self::Payload>,
        {
            self.observe(&context).await;
        }

        async fn shutdown(self) {}
    }

    impl<M: Classify> SettlementHandler for ViewProbe<M> {
        fn settlement(_result: Result<&Self::Output, &Self::Error>) -> Settlement {
            M::SETTLEMENT
        }
    }

    /// Binds `name`'s typed Value handle off `context` — exactly what a user
    /// hook does.
    fn handle<C>(context: &C, name: &str) -> Result<ValueHandle<C::State, JsonCodec>>
    where
        C: EventContext,
    {
        context
            .state(Registered::new(value_state::<JsonCodec>(name)))
            .map_err(|e| eyre!("bind {name}: {e}"))
    }

    /// Builds a two-collection session: `floor` is set **and** `commit()`ed
    /// (durable, drained from the buffer — the commit-now floor), `pending` is
    /// set but left buffered (uncommitted). Returns the ready context, the
    /// durable store, and both collection ids.
    async fn two_collections() -> Result<(
        Ctx,
        MemoryCellStore<FixedOracle>,
        CollectionId,
        CollectionId,
    )> {
        let mut registry = CollectionDefRegistry::default();
        registry.register(&value_state::<JsonCodec>(FLOOR), CollectionDef::new(None))?;
        registry.register(&value_state::<JsonCodec>(PENDING), CollectionDef::new(None))?;
        let state_key = StateKey::new(Uuid::from_u128(0x5D), Arc::from("user-1"));
        let (session, cell_store) =
            test_session_parts(MemoryLoader::new(), registry, state_key.clone());
        let context: Ctx = MockEventContext::new().with_session(session);

        let floor: Handle = handle(&context, FLOOR)?;
        floor.set(json!("floor")).await?;
        assert_eq!(
            floor.commit().await?,
            StoreOutcome::Applied,
            "the floor commit() must land a durable write",
        );
        let pending: Handle = handle(&context, PENDING)?;
        pending.set(json!("pending")).await?;

        let floor_id = CollectionId::new(
            state_key.clone(),
            StateType::Application,
            StateName::try_new(FLOOR)?,
        );
        let pending_id = CollectionId::new(
            state_key,
            StateType::Application,
            StateName::try_new(PENDING)?,
        );
        Ok((context, cell_store, floor_id, pending_id))
    }

    /// Whether a collection's Value cell holds any committed bytes on the
    /// durable store — read through a foreign probe event, so a still-buffered
    /// write is invisible.
    async fn durably_present(
        cell_store: &MemoryCellStore<FixedOracle>,
        id: &CollectionId,
    ) -> Result<bool> {
        let probe = EventRef::Message {
            dedup_id: Uuid::from_u128(u128::MAX),
        };
        cell_store
            .get(id, &value_cell(), probe)
            .await
            .map(|c| Committed::into_inner(c).is_some())
            .map_err(|e| eyre!("committed read: {e}"))
    }

    fn guard() -> (RecordingGuard, Arc<AtomicUsize>, Arc<AtomicUsize>) {
        let committed = Arc::new(AtomicUsize::new(0));
        let aborted = Arc::new(AtomicUsize::new(0));
        let g = RecordingGuard {
            committed: committed.clone(),
            aborted: aborted.clone(),
        };
        (g, committed, aborted)
    }

    async fn run_and_observe<M: Classify>(
        probe: &ViewProbe<M>,
        context: Ctx,
        g: RecordingGuard,
        result: Result<u64, TestError>,
    ) -> Result<HookObservation> {
        settle(probe, context, g, result).await;
        probe.observation().ok_or_else(|| eyre!("a hook must fire"))
    }

    /// Drives one non-finalized arm end to end: buffers the floor+pending,
    /// settles the probe with `result`, and asserts the settled-view discard,
    /// the commit-now floor, and the apply-hook contract.
    async fn run_arm<M: Classify>(
        arm: &str,
        result: Result<u64, TestError>,
        commits: bool,
    ) -> Result<()> {
        let (context, cell_store, floor_id, pending_id) = two_collections().await?;
        let (g, committed, aborted) = guard();
        let probe = ViewProbe::<M>::new();
        let obs = run_and_observe(&probe, context, g, result).await?;
        assert_arm(arm, &obs, &committed, &aborted, commits)?;
        // Durable truth: the floor is committed, the discarded pending is not.
        assert!(
            durably_present(&cell_store, &floor_id).await?,
            "{arm}: the commit-now floor is durable",
        );
        assert!(
            !durably_present(&cell_store, &pending_id).await?,
            "{arm}: the discarded pending write never became durable",
        );
        Ok(())
    }

    fn assert_arm(
        arm: &str,
        obs: &HookObservation,
        committed: &Arc<AtomicUsize>,
        aborted: &Arc<AtomicUsize>,
        commits: bool,
    ) -> Result<()> {
        if commits {
            assert_eq!(committed.load(Ordering::SeqCst), 1, "{arm}: commits");
            assert_eq!(aborted.load(Ordering::SeqCst), 0, "{arm}: not aborted");
        } else {
            assert_eq!(aborted.load(Ordering::SeqCst), 1, "{arm}: aborts");
            assert_eq!(committed.load(Ordering::SeqCst), 0, "{arm}: not committed");
        }
        // Settled-view discard: no aborted-overlay residue.
        match &obs.pending {
            Some(Ok(None)) => {}
            other => bail!("{arm}: pending must read the committed base None, got {other:?}"),
        }
        // Commit-now floor survives the discard.
        match &obs.floor {
            Some(Ok(Some(v))) if *v == json!("floor") => {}
            other => bail!("{arm}: floor must survive as \"floor\", got {other:?}"),
        }
        // Apply-hook contract: a mutation is fenced SessionClosed.
        match &obs.mutation {
            Some(Err(tag)) if tag == "SessionClosed" => {}
            other => bail!("{arm}: a hook mutation must error SessionClosed, got {other:?}"),
        }
        // rollback() stays an effectless NoOp.
        assert_eq!(
            obs.rollback,
            Some(StoreOutcome::NoOp),
            "{arm}: hook rollback() must be a NoOp",
        );
        Ok(())
    }

    /// Non-finalized arms — no aborted-overlay residue and the apply-hook
    /// contract, over final Permanent, final Transient,
    /// `Bypassed`, and the direct `abandon` (Terminal). Each fires its hook
    /// after the boundary discards the uncommitted overlay, so the hook sees
    /// `pending == None` (base, residue gone) and `floor == "floor"`
    /// (commit-now floor survived); a hook mutation is fenced `SessionClosed`
    /// and `rollback()` is a `NoOp`; and `pending` is not durable while `floor`
    /// is. Falsify by deleting the `discard_uncommitted` line on the arm under
    /// test in `settle.rs`: `pending` then reads the buffered `"pending"`.
    #[tokio::test]
    async fn non_finalized_arms_discard_overlay_keeping_commit_floor() -> Result<()> {
        // Final Permanent, final Transient, and Bypassed all commit the guard;
        // the direct abandon (Terminal) aborts it. Each fires a hook.
        run_arm::<AsFinal>(
            "final-permanent",
            Err(TestError(ErrorCategory::Permanent, "final")),
            true,
        )
        .await?;
        run_arm::<AsFinal>(
            "final-transient",
            Err(TestError(ErrorCategory::Transient, "final")),
            true,
        )
        .await?;
        run_arm::<AsBypassed>("bypassed", Ok(0), true).await?;
        run_arm::<AsFinal>(
            "terminal-abandon",
            Err(TestError(ErrorCategory::Terminal, "final")),
            false,
        )
        .await?;
        Ok(())
    }

    /// The permanent finalize-**failure** arm (settled-view cleanup plus the
    /// commit-now floor): a Permanent stage
    /// failure hits `StepOutcome::Skip`, which commits defensively but is NOT a
    /// successful finalize — so `finalize` never drained the buffer and the
    /// boundary must discard it under the held permit before the hook. The hook
    /// then sees `pending == None` (residue gone) and the commit-now `floor`
    /// survives. Falsify by deleting the `discard_uncommitted` line in the Skip
    /// arm of `settle_committed`: `pending` reads the buffered `"pending"`.
    #[tokio::test]
    async fn permanent_finalize_failure_discards_overlay_keeping_floor() -> Result<()> {
        use crate::consumer::middleware::tests::test_support::RecordingOracle;
        use crate::consumer::partition::ShutdownPhase;
        use crate::state::PartitionBackend;
        use crate::state::dirty::DirtyStore;
        use crate::state::memory::{MemoryCells, MemoryDescriptorIdentityStore};
        use crate::state::session::{KeyedStateSession, SessionParts, TerminationWatch};
        use crate::state::tests::cell_suite::FailingCellStore;
        use crate::timers::duration::CompactDuration;
        use tokio::sync::watch;

        type SkipStore = FailingCellStore<MemoryCellStore<RecordingOracle>>;
        type SkipBackend =
            PartitionBackend<RecordingOracle, MemoryDescriptorIdentityStore, SkipStore>;
        type SkipSession = KeyedStateSession<SkipBackend, MemoryLoader<Value>>;

        let mut registry = CollectionDefRegistry::default();
        registry.register(&value_state::<JsonCodec>(FLOOR), CollectionDef::new(None))?;
        registry.register(&value_state::<JsonCodec>(PENDING), CollectionDef::new(None))?;
        let registry = Arc::new(registry);
        let oracle = RecordingOracle::new();
        // Poison PENDING's stage so `settle`'s own `finalize` hits Skip; FLOOR's
        // mid-handler `commit()` uses `write_resolved` and is untouched.
        let cell_store = FailingCellStore::failing_write_provisional(
            MemoryCellStore::new(MemoryCells::new(), oracle.clone(), registry.clone()),
            StateName::try_new(PENDING)?,
            ErrorCategory::Permanent,
        );
        let (_s, shutdown_rx) = watch::channel(ShutdownPhase::default());
        let (_c, cancel_rx) = watch::channel(false);
        let session: SkipSession = KeyedStateSession::new(SessionParts {
            cell: cell_store,
            dirty: Arc::new(DirtyStore::new()),
            oracle,
            loader: MemoryLoader::new(),
            registry,
            state_key: StateKey::new(Uuid::from_u128(0x5D5), Arc::from("user-1")),
            event: EventRef::Message {
                dedup_id: Uuid::new_v4(),
            },
            recovery_delay: CompactDuration::new(30),
            armed: Arc::default(),
            termination: TerminationWatch::new(shutdown_rx, cancel_rx),
            publisher: None,
        });
        let context = MockEventContext::new()
            .with_session(session)
            .with_timer_tracking();

        // FLOOR: durable commit-now floor. PENDING: buffered, to be discarded.
        let floor: ValueHandle<SkipSession, JsonCodec> = context
            .state(Registered::new(value_state::<JsonCodec>(FLOOR)))
            .map_err(|e| eyre!("bind floor: {e}"))?;
        floor.set(json!("floor")).await?;
        assert_eq!(floor.commit().await?, StoreOutcome::Applied);
        context
            .state(Registered::new(value_state::<JsonCodec>(PENDING)))
            .map_err(|e| eyre!("bind pending: {e}"))?
            .set(json!("pending"))
            .await?;

        let (g, committed, aborted) = guard();
        let probe = ViewProbe::<AsFinal>::new();
        settle(&probe, context, g, Ok(0)).await;

        let obs = probe.observation().ok_or_else(|| eyre!("hook fires"))?;
        // The permanent-skip arm commits defensively.
        assert_arm(
            "permanent-finalize-failure",
            &obs,
            &committed,
            &aborted,
            true,
        )?;
        Ok(())
    }

    /// Teardown fence plus graceful hook-window read: a current-pin handle
    /// captured before settle reads committed data during the hook window (the
    /// gate is Closed, reads allowed — graceful completion), then errors
    /// `Terminated` once the session is torn down (`terminate`, the sync half
    /// the scope's `Drop` runs). Falsify by dropping the `is_terminated()` term
    /// in `ensure_live`: the post-teardown read returns `Ok` instead.
    #[tokio::test]
    async fn leaked_handle_reads_in_window_then_terminated_after_teardown() -> Result<()> {
        let (context, _cell_store, _floor_id, _pending_id) = two_collections().await?;
        let (g, committed, _aborted) = guard();
        // A current-pin handle leaked past the hook: settle never bumps the
        // epoch, so this clone stays current through settlement.
        let leaked: Handle = handle(&context, FLOOR)?;
        let session = context.test_lifecycle().map_err(|e| eyre!("bind: {e}"))?;

        let probe = ViewProbe::<AsFinal>::new();
        // Graceful window read happens inside the probe's hook (floor == base).
        settle(&probe, context, g, Ok(0)).await;
        assert_eq!(committed.load(Ordering::SeqCst), 1);
        let obs = probe.observation().ok_or_else(|| eyre!("hook fires"))?;
        assert_eq!(
            obs.floor,
            Some(Ok(Some(json!("floor")))),
            "the hook-window read returns committed data (graceful completion)",
        );

        // Before teardown the leaked current-pin handle still reads committed.
        assert_eq!(
            leaked.get().await.map_err(|e| e.to_string()),
            Ok(Some(json!("floor"))),
            "a current-pin read is admitted before teardown",
        );

        // Teardown: the scope's Drop flips termination synchronously.
        session.terminate();

        match leaked.get().await {
            Err(CellStateError::Access(StateAccessError::Terminated)) => Ok(()),
            other => bail!("a post-teardown read must error Terminated, got {other:?}"),
        }
    }

    /// Current-pin precedence under shutdown: after settle closes the gate,
    /// a current-pin mutation errors `SessionClosed` even when the session is
    /// also terminated, because `mutate_permit` checks the closed gate before
    /// the termination watch. The stale-pin `Terminated`-not-`SessionClosed`
    /// half is pinned in
    /// `gate_suite::stale_mutator_on_closed_session_is_terminated_not_closed`.
    /// Falsify by swapping the closed/termination order in `mutate_permit`: the
    /// current-pin mutation then hits the termination check first and errors
    /// `Terminated`. (Swapping pin/closed is inert here — the handle is
    /// current-pin, so the pin check never fires; that ordering is the
    /// stale-pin sibling's to pin, above.)
    #[tokio::test]
    async fn current_pin_hook_mutation_is_closed_even_under_shutdown() -> Result<()> {
        let (context, _cell_store, _floor_id, _pending_id) = two_collections().await?;
        let leaked: Handle = handle(&context, PENDING)?;
        let session = context.test_lifecycle().map_err(|e| eyre!("bind: {e}"))?;
        // Close the gate the way settle does (acquire, mark Closed, drop the
        // permit before the mutation), then flip termination: the gate stays
        // Closed AND is_terminated() is true, current pin.
        let permit = session.close_gate().await;
        drop(permit);
        session.terminate();
        match leaked.set(json!("x")).await {
            Err(CellStateError::Access(StateAccessError::SessionClosed)) => Ok(()),
            other => bail!("closed-before-terminated precedence broke: {other:?}"),
        }
    }
}
