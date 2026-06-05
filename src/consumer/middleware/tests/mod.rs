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
