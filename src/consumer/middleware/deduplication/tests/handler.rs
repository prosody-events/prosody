//! Unit tests for the deduplication handler.

use crate::Topic;
use crate::consumer::DemandType;
use crate::consumer::Keyed;
use crate::consumer::event_context::EventContext;
use crate::consumer::message::{ConsumerMessage, ConsumerMessageValue};
use crate::consumer::middleware::deduplication::{
    DedupIdentity, DeduplicationConfiguration, DeduplicationHandler, DeduplicationMiddleware,
    DeduplicationStore, MemoryDeduplicationStore, MemoryDeduplicationStoreProvider, dedup_uuid,
    dedup_uuid_for_message,
};
use crate::consumer::middleware::tests::test_support::{
    MockEventContext, create_test_message_from,
};
use crate::consumer::middleware::{ClassifyError, ErrorCategory, FallibleHandler};
use crate::state::tests::support::UnavailableState;
use crate::timers::TimerType;
use crate::timers::Trigger;
use crate::timers::datetime::CompactDateTime;
use serde_json::json;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use thiserror::Error;

#[derive(Clone, Debug, Error)]
enum TestError {
    #[error("permanent test error")]
    Permanent,
    #[error("transient test error")]
    Transient,
    #[error("terminal test error")]
    Terminal,
}

impl ClassifyError for TestError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::Permanent => ErrorCategory::Permanent,
            Self::Transient => ErrorCategory::Transient,
            Self::Terminal => ErrorCategory::Terminal,
        }
    }
}

#[derive(Clone)]
struct MockHandler {
    call_count: Arc<AtomicUsize>,
    error: Option<TestError>,
}

impl MockHandler {
    fn new(error: Option<TestError>) -> Self {
        Self {
            call_count: Arc::new(AtomicUsize::new(0)),
            error,
        }
    }

    fn success() -> Self {
        Self::new(None)
    }

    fn call_count(&self) -> usize {
        self.call_count.load(Ordering::Relaxed)
    }
}

impl FallibleHandler for MockHandler {
    type Error = TestError;
    type Output = ();
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
        self.call_count.fetch_add(1, Ordering::Relaxed);
        if let Some(ref e) = self.error {
            Err(e.clone())
        } else {
            Ok(())
        }
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
        self.call_count.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    async fn shutdown(self) {}
}

fn create_handler_with<T>(
    inner: T,
    version: &str,
    group_id: &str,
    topic: &str,
    partition: i32,
) -> DeduplicationHandler<T, MemoryDeduplicationStore> {
    DeduplicationHandler {
        inner,
        store: MemoryDeduplicationStore::new(),
        version: version.to_owned(),
        group_id: Arc::from(group_id),
        topic: Topic::from(topic),
        partition,
    }
}

fn create_handler<T>(inner: T) -> DeduplicationHandler<T, MemoryDeduplicationStore> {
    create_handler_with(inner, "1", "test-group", "test-topic", 0)
}

fn create_test_message(
    key: &str,
    event_id: Option<&str>,
) -> color_eyre::Result<ConsumerMessage<serde_json::Value>> {
    let payload = match event_id {
        Some(id) => json!({ "id": id }),
        None => json!({}),
    };
    create_test_message_from(ConsumerMessageValue {
        key: key.into(),
        payload,
        ..Default::default()
    })
}

/// A row already present in the store (written by a prior committed
/// dispatch's marker flush) filters the message before the handler runs.
/// Pre-seeded because the middleware no longer writes the store itself —
/// the marker is flushed later, at the `settle` boundary.
#[tokio::test]
async fn seeded_id_filters_before_handler() -> color_eyre::Result<()> {
    let handler = create_handler(MockHandler::success());
    let context = MockEventContext::new();

    let msg = create_test_message("key1", Some("evt1"))?;
    let id = handler.dedup_uuid_for_message(&msg);
    handler.store.insert(id).await?;

    let result = FallibleHandler::on_message(&handler, context, msg, DemandType::Normal).await;
    assert!(matches!(result, Ok(None)), "a seeded id is filtered");
    assert_eq!(handler.inner.call_count(), 0, "filtered before the handler");
    Ok(())
}

#[tokio::test]
async fn cache_miss_runs_handler() -> color_eyre::Result<()> {
    let handler = create_handler(MockHandler::success());
    let context = MockEventContext::new();

    let msg = create_test_message("key1", Some("evt1"))?;

    let result = FallibleHandler::on_message(&handler, context, msg, DemandType::Normal).await;
    assert!(matches!(result, Ok(Some(()))));
    assert_eq!(handler.inner.call_count(), 1);
    Ok(())
}

/// The middleware registers the message's commit marker in the session on a
/// final outcome (`Ok` or `Permanent`) and not on a non-final one
/// (`Transient`/`Terminal`); either way it never writes the store itself —
/// the flush happens later at the `settle` boundary. Subsumes the old
/// per-error-class insert tests.
#[tokio::test]
async fn marker_registered_for_final_outcomes_only() -> color_eyre::Result<()> {
    let cases: [(Option<TestError>, bool); 4] = [
        (None, true),
        (Some(TestError::Permanent), true),
        (Some(TestError::Transient), false),
        (Some(TestError::Terminal), false),
    ];
    for (error, expect_registered) in cases {
        let handler = create_handler(MockHandler::new(error));
        let session = UnavailableState::<serde_json::Value>::new();
        let context = MockEventContext::new().with_session(session.clone());

        let msg = create_test_message("key1", Some("evt1"))?;
        let id = handler.dedup_uuid_for_message(&msg);

        let _ = FallibleHandler::on_message(&handler, context, msg, DemandType::Normal).await;
        assert_eq!(
            handler.inner.call_count(),
            1,
            "handler runs on a cache miss"
        );

        let registered = session.registered_markers();
        assert_eq!(
            !registered.is_empty(),
            expect_registered,
            "marker registered iff the outcome is final (Ok | Permanent)"
        );
        if expect_registered {
            assert_eq!(
                registered,
                vec![id],
                "the registered marker is the message id"
            );
        }
        assert!(
            !handler.store.exists(id).await?,
            "the middleware never writes the dedup store — settle flushes the marker"
        );
    }
    Ok(())
}

#[tokio::test]
async fn different_event_ids_both_processed() -> color_eyre::Result<()> {
    let handler = create_handler(MockHandler::success());
    let context = MockEventContext::new();

    let msg1 = create_test_message("key1", Some("evt1"))?;
    let msg2 = create_test_message("key1", Some("evt2"))?;

    let _ = FallibleHandler::on_message(&handler, context.clone(), msg1, DemandType::Normal).await;
    let _ = FallibleHandler::on_message(&handler, context, msg2, DemandType::Normal).await;
    assert_eq!(handler.inner.call_count(), 2);
    Ok(())
}

#[tokio::test]
async fn timer_passthrough() {
    let handler = create_handler(MockHandler::success());
    let context = MockEventContext::new();
    let trigger = Trigger::for_testing(
        "test-key".into(),
        CompactDateTime::from(1000_u32),
        TimerType::default(),
    );

    let result = FallibleHandler::on_timer(&handler, context, trigger, DemandType::Normal).await;
    assert!(result.is_ok());
    assert_eq!(handler.inner.call_count(), 1);
}

#[test]
fn dedup_uuid_is_deterministic() -> color_eyre::Result<()> {
    let handler = create_handler(MockHandler::success());
    let msg1 = create_test_message("key1", Some("evt1"))?;
    let msg2 = create_test_message("key1", Some("evt1"))?;
    assert_eq!(
        handler.dedup_uuid_for_message(&msg1),
        handler.dedup_uuid_for_message(&msg2),
    );
    Ok(())
}

#[test]
fn dedup_uuid_differs_by_dimension() -> color_eyre::Result<()> {
    let handler = create_handler(MockHandler::success());
    let base_msg = create_test_message("key1", Some("evt1"))?;
    let base = handler.dedup_uuid_for_message(&base_msg);

    // Different version
    let h = create_handler_with(MockHandler::success(), "2", "test-group", "test-topic", 0);
    assert_ne!(base, h.dedup_uuid_for_message(&base_msg));

    // Different group
    let h = create_handler_with(MockHandler::success(), "1", "other-group", "test-topic", 0);
    assert_ne!(base, h.dedup_uuid_for_message(&base_msg));

    // Different topic
    let h = create_handler_with(MockHandler::success(), "1", "test-group", "other-topic", 0);
    assert_ne!(base, h.dedup_uuid_for_message(&base_msg));

    // Different partition
    let h = create_handler_with(MockHandler::success(), "1", "test-group", "test-topic", 1);
    assert_ne!(base, h.dedup_uuid_for_message(&base_msg));

    // Different key
    let diff_key_msg = create_test_message("key2", Some("evt1"))?;
    assert_ne!(base, handler.dedup_uuid_for_message(&diff_key_msg));

    // Different event_id
    let diff_evt_msg = create_test_message("key1", Some("evt2"))?;
    assert_ne!(base, handler.dedup_uuid_for_message(&diff_evt_msg));

    // Offset fallback (no event_id) differs from event_id path
    let offset_msg = create_test_message("key1", None)?;
    assert_ne!(base, handler.dedup_uuid_for_message(&offset_msg));

    Ok(())
}

#[test]
fn ttl_exceeding_max_rejected() {
    let config = DeduplicationConfiguration {
        version: "1".to_owned(),
        cache_capacity: NonZeroUsize::MIN,
        ttl: Duration::from_secs(700_000_000),
    };
    let result = DeduplicationMiddleware::<_, serde_json::Value>::new(
        config,
        "group",
        MemoryDeduplicationStoreProvider::new(),
    );
    assert!(result.is_err());
}

#[test]
fn ttl_below_minimum_rejected() {
    let config = DeduplicationConfiguration {
        version: "1".to_owned(),
        cache_capacity: NonZeroUsize::MIN,
        ttl: Duration::from_secs(30),
    };
    let result = DeduplicationMiddleware::<_, serde_json::Value>::new(
        config,
        "group",
        MemoryDeduplicationStoreProvider::new(),
    );
    assert!(result.is_err());
}

/// Probe handler that records work-stage and apply-hook events.
///
/// `Handler` records an `on_message`/`on_timer` invocation; `InnerAfterCommit`
/// / `InnerAfterAbort` record the corresponding apply hook. Tests assert at
/// most one apply event per `Handler` event, and zero when no `Handler` fired.
#[derive(Clone, Default)]
struct ApplyProbe {
    log: Arc<parking_lot::Mutex<Vec<ApplyEvent>>>,
    error: Option<TestError>,
}

impl ApplyProbe {
    fn failing(error: TestError) -> Self {
        Self {
            log: Arc::default(),
            error: Some(error),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ApplyEvent {
    Handler,
    InnerAfterCommit,
    InnerAfterAbort,
}

impl FallibleHandler for ApplyProbe {
    type Error = TestError;
    type Output = ();
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
        self.log.lock().push(ApplyEvent::Handler);
        match &self.error {
            Some(e) => Err(e.clone()),
            None => Ok(()),
        }
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
        self.log.lock().push(ApplyEvent::Handler);
        match &self.error {
            Some(e) => Err(e.clone()),
            None => Ok(()),
        }
    }

    async fn after_commit<C>(&self, _context: C, _result: Result<Self::Output, Self::Error>)
    where
        C: EventContext<Payload = Self::Payload>,
    {
        self.log.lock().push(ApplyEvent::InnerAfterCommit);
    }

    async fn after_abort<C>(&self, _context: C, _result: Result<Self::Output, Self::Error>)
    where
        C: EventContext<Payload = Self::Payload>,
    {
        self.log.lock().push(ApplyEvent::InnerAfterAbort);
    }

    async fn shutdown(self) {}
}

#[tokio::test]
async fn dedup_skip_does_not_invoke_inner_after_commit() -> color_eyre::Result<()> {
    // First message goes through, second is deduplicated → inner.after_commit
    // must fire on the first dispatch but NOT on the second.
    let inner = ApplyProbe::default();
    let log = inner.log.clone();
    let handler = create_handler(inner);
    let context = MockEventContext::new();

    let msg1 = create_test_message("key1", Some("evt1"))?;
    let msg2 = create_test_message("key1", Some("evt1"))?;

    // First dispatch: inner runs, on_message returns Ok(Some(())).
    let result1 =
        FallibleHandler::on_message(&handler, context.clone(), msg1, DemandType::Normal).await;
    assert!(matches!(result1, Ok(Some(()))));
    // The dedup middleware's after_commit must forward the inner half.
    FallibleHandler::after_commit(&handler, context.clone(), result1).await;

    // Simulate the `settle` boundary flushing the registered marker after the
    // first commit (the middleware no longer writes the store itself).
    handler
        .store
        .insert(handler.dedup_uuid_for_message(&msg2))
        .await?;

    // Second dispatch: deduplicated, on_message returns Ok(None).
    let result2 =
        FallibleHandler::on_message(&handler, context.clone(), msg2, DemandType::Normal).await;
    assert!(matches!(result2, Ok(None)));
    FallibleHandler::after_commit(&handler, context, result2).await;

    assert_eq!(
        log.lock().clone(),
        vec![ApplyEvent::Handler, ApplyEvent::InnerAfterCommit],
        "second dispatch must NOT invoke inner.after_commit (handler never ran)",
    );
    Ok(())
}

#[tokio::test]
async fn dedup_passthrough_forwards_after_commit_for_handler_ok() -> color_eyre::Result<()> {
    // Sanity: when the handler runs, inner.after_commit must receive the Ok
    // forwarded through DeduplicationHandler::after_commit.
    let inner = ApplyProbe::default();
    let log = inner.log.clone();
    let handler = create_handler(inner);
    let context = MockEventContext::new();

    let msg = create_test_message("key1", Some("evt-fresh"))?;

    let result =
        FallibleHandler::on_message(&handler, context.clone(), msg, DemandType::Normal).await;
    assert!(matches!(result, Ok(Some(()))));
    FallibleHandler::after_commit(&handler, context, result).await;

    assert_eq!(
        log.lock().clone(),
        vec![ApplyEvent::Handler, ApplyEvent::InnerAfterCommit],
        "passthrough: inner.after_commit fires when inner ran successfully",
    );
    Ok(())
}

#[tokio::test]
async fn dedup_passthrough_forwards_after_commit_for_handler_err() -> color_eyre::Result<()> {
    // When the inner runs and returns an `Err`, the dedup middleware must
    // forward that `Err` through whichever apply hook the framework chooses.
    // Here we simulate the framework treating the dispatch as final (e.g. a
    // permanent-classification error routed to a DLQ) by calling
    // `after_commit` with the inner-typed error wrapped by the dedup layer.
    let inner = ApplyProbe::failing(TestError::Permanent);
    let log = inner.log.clone();
    let handler = create_handler(inner);
    let context = MockEventContext::new();

    let msg = create_test_message("key1", Some("evt-err"))?;

    let result =
        FallibleHandler::on_message(&handler, context.clone(), msg, DemandType::Normal).await;
    assert!(result.is_err());
    FallibleHandler::after_commit(&handler, context, result).await;

    assert_eq!(
        log.lock().clone(),
        vec![ApplyEvent::Handler, ApplyEvent::InnerAfterCommit],
        "inner.after_commit must receive the inner-typed Err when the dispatch is final",
    );
    Ok(())
}

/// The dedup id a message's *writer* (the deduplication handler) produces
/// must equal the id any *reader* derives via the canonical
/// [`dedup_uuid_for_message`] free function — the exact call shape the
/// keyed-state recovery oracle uses. If these diverged, recovery would look
/// committed message state up under the wrong id and always read
/// `NotCommitted`, silently rolling state back.
///
/// Exercised with and without an `event_id` because [`dedup_uuid`] selects a
/// different hash branch (`event_id` vs offset) for each; the regression that
/// motivated this — a reader hardcoding `version=""` and `event_id=None` —
/// is pinned by the inequality asserts against the buggy form.
#[test]
fn dedup_id_writer_matches_canonical_reader_derivation() -> color_eyre::Result<()> {
    const VERSION: &str = "1";
    const GROUP: &str = "test-group";
    const TOPIC: &str = "test-topic";
    const PARTITION: i32 = 3;

    let handler = create_handler_with(MockHandler::success(), VERSION, GROUP, TOPIC, PARTITION);

    for event_id in [Some("evt-1"), None] {
        let msg = create_test_message("key-a", event_id)?;

        let writer_id = handler.dedup_uuid_for_message(&msg);
        let reader_id = dedup_uuid_for_message(
            DedupIdentity {
                version: VERSION,
                group_id: GROUP,
                topic: TOPIC,
                partition: PARTITION,
            },
            &msg,
        );
        assert_eq!(
            writer_id, reader_id,
            "writer and canonical reader derivations must agree (event_id = {event_id:?})"
        );

        // Regression guard: the original buggy reader hardcoded an empty
        // version and a `None` event_id. For a message carrying an event_id
        // that takes the wrong hash branch *and* the wrong version, so the
        // ids must differ — proving the test would have failed on the bug.
        let buggy_id = dedup_uuid(
            "",
            GROUP,
            TOPIC,
            PARTITION,
            msg.key().as_bytes(),
            None,
            msg.offset(),
        );
        if event_id.is_some() {
            assert_ne!(
                writer_id, buggy_id,
                "the buggy reader derivation must not collide with the writer id"
            );
        }
    }
    Ok(())
}
