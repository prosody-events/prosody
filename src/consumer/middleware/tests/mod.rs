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

use super::*;
use crate::consumer::EventHandler;
use crate::consumer::message::ConsumerMessage;
use crate::consumer::middleware::tests::test_support::MockEventContext;
use crate::consumer::partition::offsets::OffsetTracker;
use crate::error::ErrorCategory;
use crate::timers::TimerType;
use crate::timers::Trigger;
use crate::timers::datetime::CompactDateTime;

/// Test error with a fixed classification. Equality compares the
/// classification discriminant + tag, since `ErrorCategory` itself is
/// only `Copy + Clone + Debug + Serialize`.
#[derive(Debug, Clone)]
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

impl PartialEq for TestError {
    fn eq(&self, other: &Self) -> bool {
        // Compare classification discriminants + tags.
        let cat_eq = matches!(
            (self.0, other.0),
            (ErrorCategory::Transient, ErrorCategory::Transient)
                | (ErrorCategory::Permanent, ErrorCategory::Permanent)
                | (ErrorCategory::Terminal, ErrorCategory::Terminal)
        );
        cat_eq && self.1 == other.1
    }
}

impl Eq for TestError {}

impl FallibleEventHandler for ProbeHandler {}

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

fn make_offset_tracker() -> OffsetTracker {
    let version = Arc::new(CachePadded::new(AtomicUsize::new(0)));
    OffsetTracker::new("test-topic".into(), 0, 10, Duration::from_secs(5), version)
}

fn make_test_message() -> Option<ConsumerMessage<serde_json::Value>> {
    use crate::consumer::message::ConsumerMessageValue;
    use std::sync::Arc;
    use tokio::sync::Semaphore;
    let semaphore = Arc::new(Semaphore::new(10));
    let permit = semaphore.try_acquire_owned().ok()?;
    Some(ConsumerMessage::new(
        ConsumerMessageValue::default(),
        tracing::Span::current(),
        permit,
    ))
}

#[tokio::test]
async fn after_commit_fires_with_ok_output_after_handler_success() -> color_eyre::Result<()> {
    let handler = ProbeHandler::ok(42);
    let log = handler.log.clone();
    let context = MockEventContext::new();
    let tracker = make_offset_tracker();
    let uncommitted_offset = tracker.take(0).await?;
    let message = make_test_message()
        .ok_or_else(|| color_eyre::eyre::eyre!("failed to construct test message"))?
        .into_uncommitted(uncommitted_offset);

    EventHandler::on_message(&handler, context, message, DemandType::Normal).await;

    assert_eq!(
        log.lock().clone(),
        vec![HookEvent::Handler, HookEvent::AfterCommit(Ok(42))],
        "handler runs first, then after_commit with Ok(sentinel)",
    );
    Ok(())
}

#[tokio::test]
async fn after_commit_fires_with_err_after_permanent_error() -> color_eyre::Result<()> {
    let err = TestError(ErrorCategory::Permanent, "permanent");
    let handler = ProbeHandler::err(0, err.clone());
    let log = handler.log.clone();
    let context = MockEventContext::new();
    let tracker = make_offset_tracker();
    let uncommitted_offset = tracker.take(0).await?;
    let message = make_test_message()
        .ok_or_else(|| color_eyre::eyre::eyre!("failed to construct test message"))?
        .into_uncommitted(uncommitted_offset);

    EventHandler::on_message(&handler, context, message, DemandType::Normal).await;

    assert_eq!(
        log.lock().clone(),
        vec![HookEvent::Handler, HookEvent::AfterCommit(Err(err))],
        "Permanent error commits the marker; after_commit fires with Err",
    );
    Ok(())
}

#[tokio::test]
async fn after_commit_fires_with_err_after_transient_error() -> color_eyre::Result<()> {
    // Transient (when no retry middleware is in front) commits like
    // Permanent at the blanket-impl level.
    let err = TestError(ErrorCategory::Transient, "transient");
    let handler = ProbeHandler::err(0, err.clone());
    let log = handler.log.clone();
    let context = MockEventContext::new();
    let tracker = make_offset_tracker();
    let uncommitted_offset = tracker.take(0).await?;
    let message = make_test_message()
        .ok_or_else(|| color_eyre::eyre::eyre!("failed to construct test message"))?
        .into_uncommitted(uncommitted_offset);

    EventHandler::on_message(&handler, context, message, DemandType::Normal).await;

    assert_eq!(
        log.lock().clone(),
        vec![HookEvent::Handler, HookEvent::AfterCommit(Err(err))],
        "Transient error at the blanket-impl level commits + fires after_commit",
    );
    Ok(())
}

#[tokio::test]
async fn after_abort_fires_with_err_after_terminal_error() -> color_eyre::Result<()> {
    let err = TestError(ErrorCategory::Terminal, "terminal");
    let handler = ProbeHandler::err(0, err.clone());
    let log = handler.log.clone();
    let context = MockEventContext::new();
    let tracker = make_offset_tracker();
    let uncommitted_offset = tracker.take(0).await?;
    let message = make_test_message()
        .ok_or_else(|| color_eyre::eyre::eyre!("failed to construct test message"))?
        .into_uncommitted(uncommitted_offset);

    EventHandler::on_message(&handler, context, message, DemandType::Normal).await;

    assert_eq!(
        log.lock().clone(),
        vec![HookEvent::Handler, HookEvent::AfterAbort(Err(err))],
        "Terminal error aborts the marker; after_abort fires with Err",
    );
    Ok(())
}

#[tokio::test]
async fn hook_1_to_1_invariant_one_apply_per_dispatch() -> color_eyre::Result<()> {
    // The load-bearing invariant, stated **per inner invocation**:
    // for every individual call to the inner `on_message` /
    // `on_timer` that ran and returned, exactly one apply hook
    // (after_commit OR after_abort) fires — not both, not neither,
    // and never coalesced across multiple invocations. This is what
    // 2PC handlers and rescue middleware rely on to know they are
    // guaranteed a single finalization signal per inner invocation.
    //
    // At this default boundary the blanket impl performs exactly one
    // inner invocation per outer call, so this test exercises the
    // 1:1 case directly. Wrapping middleware that re-invokes the
    // inner (e.g. a retry loop) must preserve the same invariant
    // per invocation; that is verified in the `retry` module's
    // tests.
    for category in [
        ErrorCategory::Permanent,
        ErrorCategory::Transient,
        ErrorCategory::Terminal,
    ] {
        let handler = ProbeHandler::err(0, TestError(category, "x"));
        let log = handler.log.clone();
        let context = MockEventContext::new();
        let tracker = make_offset_tracker();
        let uncommitted_offset = tracker.take(0).await?;
        let message = make_test_message()
            .ok_or_else(|| color_eyre::eyre::eyre!("failed to construct test message"))?
            .into_uncommitted(uncommitted_offset);

        EventHandler::on_message(&handler, context, message, DemandType::Normal).await;

        let recorded = log.lock().clone();
        let commit_count = recorded
            .iter()
            .filter(|e| matches!(e, HookEvent::AfterCommit(_)))
            .count();
        let abort_count = recorded
            .iter()
            .filter(|e| matches!(e, HookEvent::AfterAbort(_)))
            .count();
        assert_eq!(
            commit_count + abort_count,
            1,
            "{category:?}: exactly one apply hook should fire per dispatch ({recorded:?})",
        );
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

    struct MockGuard {
        committed: Arc<AtomicUsize>,
        aborted: Arc<AtomicUsize>,
    }

    impl Uncommitted for MockGuard {
        async fn commit(self) {
            self.committed.fetch_add(1, Ordering::SeqCst);
        }

        async fn abort(self) {
            self.aborted.fetch_add(1, Ordering::SeqCst);
        }
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
        type CommitGuard = MockGuard;

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
            let guard = MockGuard {
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

#[tokio::test]
async fn pass_through_middleware_forwards_output_to_inner_after_commit() -> color_eyre::Result<()> {
    let inner = ProbeHandler::ok(7);
    let log = inner.log.clone();
    let middleware = PassThroughMiddleware { inner };
    let context = MockEventContext::new();
    let tracker = make_offset_tracker();
    let uncommitted_offset = tracker.take(0).await?;
    let message = make_test_message()
        .ok_or_else(|| color_eyre::eyre::eyre!("failed to construct test message"))?
        .into_uncommitted(uncommitted_offset);

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
    let message = make_test_message()
        .ok_or_else(|| color_eyre::eyre::eyre!("failed to construct test message"))?
        .into_uncommitted(uncommitted_offset);

    EventHandler::on_message(&middleware, context, message, DemandType::Normal).await;

    assert_eq!(
        log.lock().clone(),
        vec![HookEvent::Handler, HookEvent::AfterAbort(Err(err))],
        "pass-through middleware forwards after_abort on terminal",
    );
    Ok(())
}

/// Invariant 7 (rollback-only-before-flush): inline `abandon` rolls a staged
/// set back to its committed base ONLY before a marker flush is attempted.
/// After an attempt the flush's durability is ambiguous, so the staged cells
/// must stay provisional for the armed sweep to resolve through the oracle —
/// rolling back here could erase a committed write that redelivery then
/// dedup-filters away.
mod rollback_safety {
    use super::*;
    use crate::consumer::Uncommitted;
    use crate::loader::MemoryLoader;
    use crate::state::descriptor::tests::{FixedOracle, TestSession, test_session_parts};
    use crate::state::descriptor::{Registered, ValueDescriptor, value_state};
    use crate::state::memory::MemoryCellStore;
    use crate::state::registry::{CollectionDef, CollectionDefRegistry};
    use crate::state::session::LifecycleAccessExt;
    use crate::state::session::sealed::FinalizeOutcome;
    use crate::state::store::CellStore;
    use crate::state::{CollectionId, StateKey, StateName, StateType};
    use color_eyre::eyre::{Result, eyre};
    use futures::StreamExt;
    use quickcheck::{QuickCheck, TestResult};
    use serde_json::json;
    use tokio::runtime::Builder;
    use uuid::Uuid;

    /// Whether the durable cell at `id` is still provisional — the public,
    /// non-resolving way to distinguish a staged cell from a resolved one (the
    /// resolving `get` would mutate a provisional cell).
    async fn is_provisional(
        cell_store: &MemoryCellStore<FixedOracle>,
        id: &CollectionId,
    ) -> Result<bool> {
        let stream = cell_store.provisional_cells(id);
        futures::pin_mut!(stream);
        Ok(stream.next().await.transpose()?.is_some())
    }

    /// A commit guard that records nothing — `abandon` only `abort`s it.
    struct NoopGuard;

    impl Uncommitted for NoopGuard {
        async fn commit(self) {}

        async fn abort(self) {}
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

    type Ctx = MockEventContext<serde_json::Value, TestSession>;

    /// The `cart` descriptor with the default JSON codec.
    fn cart() -> ValueDescriptor {
        value_state("cart")
    }

    /// Stages one provisional cell on `cart` through a real session and returns
    /// the context (ready to abandon / settle), the durable store, and the cell
    /// id. `configure` applies context modifiers (`with_timer_failures`,
    /// `with_shutdown`, ...) before staging.
    async fn staged(
        configure: impl FnOnce(Ctx) -> Ctx,
    ) -> Result<(Ctx, MemoryCellStore<FixedOracle>, CollectionId)> {
        let mut registry = CollectionDefRegistry::default();
        registry.register(&cart(), CollectionDef::new(None))?;
        let state_key = StateKey::new(Uuid::from_u128(0x7), Arc::from("user-1"));
        let (session, cell_store) =
            test_session_parts(MemoryLoader::new(), registry, state_key.clone());
        let context: Ctx = configure(MockEventContext::new().with_session(session));

        // Write a value and finalize → one provisional cell staged durably.
        let handle = context
            .state(Registered::new(cart()))
            .map_err(|e| eyre!("bind cart: {e}"))?;
        handle.set(json!({ "x": 1_i32 })).await?;
        let lifecycle = context.lifecycle().map_err(|e| eyre!("lifecycle: {e}"))?;
        let outcome = lifecycle
            .finalize()
            .await
            .map_err(|e| eyre!("finalize: {e}"))?;
        assert_eq!(outcome, FinalizeOutcome::Staged);

        let cart_id = CollectionId::new(
            state_key,
            StateType::Application,
            StateName::try_new("cart")?,
        );
        assert!(
            is_provisional(&cell_store, &cart_id).await?,
            "the cell must be provisional after staging"
        );
        Ok((context, cell_store, cart_id))
    }

    /// After a marker-flush attempt, abandon must NOT roll back: the cell stays
    /// provisional for the sweep to resolve through the oracle.
    #[tokio::test]
    async fn after_marker_flush_keeps_the_cell_provisional() -> Result<()> {
        let (context, cell_store, cart_id) = staged(|c| c).await?;
        let handler = ProbeHandler::ok(0);

        abandon(
            &handler,
            context,
            NoopGuard,
            Err(TestError(ErrorCategory::Terminal, "shutdown mid-flush")),
            RollbackSafety::AfterMarkerFlush,
        )
        .await;

        assert!(
            is_provisional(&cell_store, &cart_id).await?,
            "AfterMarkerFlush must leave the staged cell provisional",
        );
        Ok(())
    }

    /// Before any marker flush, abandon rolls the staged cell back to its
    /// committed base — here the empty base, so the cell resolves to absent.
    #[tokio::test]
    async fn before_marker_flush_rolls_the_cell_back() -> Result<()> {
        let (context, cell_store, cart_id) = staged(|c| c).await?;
        let handler = ProbeHandler::ok(0);

        abandon(
            &handler,
            context,
            NoopGuard,
            Err(TestError(ErrorCategory::Terminal, "terminal handler error")),
            RollbackSafety::BeforeMarkerFlush,
        )
        .await;

        assert!(
            !is_provisional(&cell_store, &cart_id).await?,
            "BeforeMarkerFlush must roll the staged cell back to resolved",
        );
        Ok(())
    }

    /// Abort-only-on-shutdown, end to end through `settle`'s success path: a
    /// shutdown is the *sole* thing that stops the durability sequence short of
    /// a commit — it abandons *before* the marker flush, so the staged cell
    /// rolls back to its committed base and the offset aborts (the event
    /// redelivers and re-runs). Every store failure instead retries forever.
    /// This is exercised by the `prop_settle_aborts_iff_shutdown` property
    /// below (its `shutdown` / non-`shutdown` × `fail_count` axes subsume the
    /// abort-on-shutdown and retry-forever-self-heal directed cases).
    ///
    /// The never-abort-except-shutdown invariant, as a property over a
    /// generated leading-failure count, the **category** those failures
    /// classify as, and a shutdown flag: `settle`'s success path aborts the
    /// offset **iff** shutdown (rolling the staged cell back), and
    /// otherwise self-heals to a commit **no matter how many** failures —
    /// of **any** category — the arm hits first (the arm is must-succeed,
    /// invariant 8) — then flushes the marker, commits, and promotes the
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
                let Ok((context, cell_store, cart_id)) = staged(configure).await else {
                    return TestResult::error("failed to stage");
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
                    // Abort iff shutdown: the offset aborts and the cell rolls back.
                    if aborted != 1 || committed != 0 || still_provisional {
                        return TestResult::error(format!(
                            "shutdown must abort+rollback: committed={committed} \
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

/// `ArmState` amortization: while a `StateRecovery` backstop stands for a key,
/// later stateful commits on that key skip re-arming, so a burst issues at most
/// one timer-store write per backstop generation.
mod backstop_amortization {
    use super::*;
    use crate::loader::MemoryLoader;
    use crate::state::StateKey;
    use crate::state::descriptor::tests::test_session_with_armed;
    use crate::state::descriptor::{Registered, ValueDescriptor, value_state};
    use crate::state::registry::{CollectionDef, CollectionDefRegistry};
    use crate::state::session::{ArmedKeys, LifecycleAccessExt};
    use color_eyre::eyre::{Result, eyre};
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
            let lifecycle = context.lifecycle().map_err(|e| eyre!("lifecycle: {e}"))?;
            lifecycle
                .finalize()
                .await
                .map_err(|e| eyre!("finalize: {e}"))?;

            let outcome = arm_backstop(&context, &lifecycle).await;
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
            let lifecycle = context.lifecycle().map_err(|e| eyre!("lifecycle: {e}"))?;
            lifecycle
                .finalize()
                .await
                .map_err(|e| eyre!("finalize: {e}"))?;
            arm_backstop(&context, &lifecycle).await;
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
    use crate::consumer::middleware::tests::test_support::TimerOperation;
    use crate::loader::MemoryLoader;
    use crate::state::StateKey;
    use crate::state::descriptor::tests::test_session_with_armed;
    use crate::state::descriptor::{Registered, value_state};
    use crate::state::registry::{CollectionDef, CollectionDefRegistry};
    use crate::state::session::{ArmedKeys, LifecycleAccessExt};
    use crate::timers::duration::CompactDuration;
    use color_eyre::eyre::{Result, eyre};
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
    /// with prior events, then runs `arm_backstop`. Brackets the arm with `now`
    /// so the caller can bound the scheduled fire time.
    async fn run_arm(
        bounds: &[Option<u32>],
        key: &StateKey,
        armed: &ArmedKeys,
    ) -> Result<(CompactDateTime, CompactDateTime, Vec<TimerOperation>)> {
        let mut registry = CollectionDefRegistry::default();
        for (i, within) in bounds.iter().enumerate() {
            registry.register(
                &value_state::<JsonCodec>(&format!("c{i}")),
                CollectionDef::new(None).with_recovery_within(within.map(CompactDuration::new)),
            )?;
        }
        let (session, _store) =
            test_session_with_armed(MemoryLoader::new(), registry, key.clone(), armed.clone());
        let context = MockEventContext::new()
            .with_session(session)
            .with_timer_tracking();
        for i in 0..bounds.len() {
            let handle = context
                .state(Registered::new(value_state::<JsonCodec>(&format!("c{i}"))))
                .map_err(|e| eyre!("bind: {e}"))?;
            handle.set(json!({ "v": i as i32 })).await?;
        }
        let lifecycle = context.lifecycle().map_err(|e| eyre!("lifecycle: {e}"))?;
        lifecycle
            .finalize()
            .await
            .map_err(|e| eyre!("finalize: {e}"))?;
        let before = CompactDateTime::now()?;
        arm_backstop(&context, &lifecycle).await;
        let after = CompactDateTime::now()?;
        Ok((before, after, context.timer_operations()))
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

            let (before, after, ops) = run_arm(bounds, &key, &armed).await?;
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
}
