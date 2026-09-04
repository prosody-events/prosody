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
use std::future::ready;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use crossbeam_utils::CachePadded;
use parking_lot::Mutex;

use super::settle::{ArmOutcome, arm_backstop};
use super::*;
use crate::consumer::EventHandler;
use crate::consumer::Uncommitted;
use crate::consumer::message::{ConsumerMessage, ConsumerMessageValue};
use crate::consumer::middleware::tests::test_support::{
    MockEventContext, create_test_message, create_test_message_from,
};
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

    fn on_excise<C>(
        &self,
        _context: C,
        _message: ConsumerMessage<()>,
        _demand_type: DemandType,
    ) -> impl Future<Output = Result<Self::Output, Self::Error>>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        self.log.lock().push(HookEvent::Handler);
        ready(self.result.clone().map(|()| self.sentinel))
    }

    fn on_message<C>(
        &self,
        _context: C,
        _message: ConsumerMessage<Self::Payload>,
        _demand_type: DemandType,
    ) -> impl Future<Output = Result<Self::Output, Self::Error>>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        self.log.lock().push(HookEvent::Handler);
        ready(self.result.clone().map(|()| self.sentinel))
    }

    fn on_timer<C>(
        &self,
        _context: C,
        _trigger: Trigger,
        _demand_type: DemandType,
    ) -> impl Future<Output = Result<Self::Output, Self::Error>>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        self.log.lock().push(HookEvent::Handler);
        ready(self.result.clone().map(|()| self.sentinel))
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

impl RecordingGuard {
    /// A fresh guard and the two counters it records into, in
    /// `(guard, committed, aborted)` order.
    fn new() -> (Self, Arc<AtomicUsize>, Arc<AtomicUsize>) {
        let committed: Arc<AtomicUsize> = Arc::default();
        let aborted: Arc<AtomicUsize> = Arc::default();
        (
            Self {
                committed: committed.clone(),
                aborted: aborted.clone(),
            },
            committed,
            aborted,
        )
    }
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

    async fn on_excise<C>(
        &self,
        context: C,
        message: ConsumerMessage<()>,
        demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        self.inner.on_excise(context, message, demand_type).await
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
mod staged_rollback;

mod arm_backstop;
mod backstop_amortization;
/// Post-settle hook visibility: `finalize` drains the event's dirty overlay
/// on success, so the apply hooks read the **lower store** — the per-cell
/// committed projection, where an own-event provisional cell answers its
/// committed base `prev` — never the event's pre-settle overlay. One pin per
/// ruled-on window: the arm-shutdown rollback's `after_abort` reads the
/// restored committed base; the ambiguous marker-record shutdown's
/// `after_abort` reads `prev` (staged cells deliberately left provisional);
/// the `Incomplete`-promote `after_commit` reads the mixed per-cell view
/// (promoted cells the new values, un-promoted cells `prev`).
mod hook_visibility;
mod marker_record_must_succeed;
mod settled_view;
mod settlement_classification;
