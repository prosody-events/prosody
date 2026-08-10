//! Unit tests for the deduplication handler: the stateless filter over the
//! session's boundary-readable message marker, the dedup-id derivation, and
//! the settlement classification table.

use crate::consumer::DemandType;
use crate::consumer::EventHandler;
use crate::consumer::Keyed;
use crate::consumer::event_context::EventContext;
use crate::consumer::message::{ConsumerMessage, ConsumerMessageValue, Record};
use crate::consumer::middleware::deduplication::{
    DedupIdentity, DeduplicationConfiguration, DeduplicationError, DeduplicationHandler,
    DeduplicationMiddleware, DeduplicationStore, MemoryDeduplicationStore,
    MemoryDeduplicationStoreProvider, dedup_uuid, dedup_uuid_for_message,
};
use crate::consumer::middleware::tests::test_support::{
    MockEventContext, RecordingSession, create_test_message_from, recording_session,
};
use crate::consumer::middleware::{
    ClassifyError, ErrorCategory, FallibleEventHandler, FallibleHandler, Settlement,
    SettlementHandler,
};
use crate::consumer::partition::offsets::OffsetTracker;
use crate::state::registry::CollectionDefRegistry;
use crate::state::{EventRef, StateKey};
use crate::timers::TimerType;
use crate::timers::Trigger;
use crate::timers::datetime::CompactDateTime;
use crossbeam_utils::CachePadded;
use serde_json::json;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use thiserror::Error;
use uuid::Uuid;

#[derive(Clone, Debug, Error)]
enum TestError {
    #[error("permanent test error")]
    Permanent,
    #[error("transient test error")]
    Transient,
}

impl ClassifyError for TestError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::Permanent => ErrorCategory::Permanent,
            Self::Transient => ErrorCategory::Transient,
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

    async fn on_excise<C>(
        &self,
        context: C,
        message: ConsumerMessage<Self::Payload>,
        demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        FallibleHandler::on_message(self, context, message, demand_type).await
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

impl SettlementHandler for MockHandler {
    fn settlement(_result: Result<&Self::Output, &Self::Error>) -> Settlement {
        Settlement::Final
    }
}

fn create_handler<T>(inner: T) -> DeduplicationHandler<T, MemoryDeduplicationStore> {
    DeduplicationHandler {
        inner,
        store: MemoryDeduplicationStore::new(),
    }
}

/// The fixed identity the tests derive message dedup ids under — the free
/// function stands in for the partition loop's `EventRef` derivation.
fn test_identity() -> DedupIdentity<'static> {
    DedupIdentity {
        version: "1",
        group_id: "test-group",
        topic: "test-topic",
        partition: 0,
    }
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
        record: Record::Message(payload),
        ..Default::default()
    })
}

/// A session-backed mock context whose `EventRef::Message` carries
/// `dedup_id` — the identity the filter reads.
fn session_context(dedup_id: Uuid) -> MockEventContext<serde_json::Value, RecordingSession> {
    let state_key = StateKey::new(Uuid::from_u128(0xDD), Arc::from("test-key"));
    let (session, _cell_store, _dirty, _recorded) = recording_session(
        CollectionDefRegistry::default(),
        state_key,
        EventRef::Message { dedup_id },
    );
    MockEventContext::new().with_session(session)
}

/// A row already present in the store (written by a prior committed
/// dispatch's settle-boundary record) filters the message before the handler
/// runs. The filter reads the session's `EventRef` dedup id — the same
/// identity the boundary records.
#[tokio::test]
async fn seeded_id_filters_before_handler() -> color_eyre::Result<()> {
    let handler = create_handler(MockHandler::success());

    let msg = create_test_message("key1", Some("evt1"))?;
    let id = dedup_uuid_for_message(test_identity(), &msg);
    handler.store.insert(id).await?;
    let context = session_context(id);

    let result = FallibleHandler::on_message(&handler, context, msg, DemandType::Normal).await;
    assert!(matches!(result, Ok(None)), "a seeded id is filtered");
    assert_eq!(handler.inner.call_count(), 0, "filtered before the handler");
    Ok(())
}

#[tokio::test]
async fn cache_miss_runs_handler() -> color_eyre::Result<()> {
    let handler = create_handler(MockHandler::success());

    let msg = create_test_message("key1", Some("evt1"))?;
    let context = session_context(dedup_uuid_for_message(test_identity(), &msg));

    let result = FallibleHandler::on_message(&handler, context, msg, DemandType::Normal).await;
    assert!(matches!(result, Ok(Some(()))));
    assert_eq!(handler.inner.call_count(), 1);
    Ok(())
}

/// The filter keys on the exact session identity: a store row for a
/// *different* message's id never filters this one.
#[tokio::test]
async fn a_different_message_id_is_not_filtered() -> color_eyre::Result<()> {
    let handler = create_handler(MockHandler::success());

    let msg1 = create_test_message("key1", Some("evt1"))?;
    let msg2 = create_test_message("key1", Some("evt2"))?;
    handler
        .store
        .insert(dedup_uuid_for_message(test_identity(), &msg1))
        .await?;

    let context = session_context(dedup_uuid_for_message(test_identity(), &msg2));
    let result = FallibleHandler::on_message(&handler, context, msg2, DemandType::Normal).await;
    assert!(matches!(result, Ok(Some(()))));
    assert_eq!(handler.inner.call_count(), 1);
    Ok(())
}

/// A context with no marker source (the stateless default session)
/// dispatches unfiltered — the filter cannot key without an identity.
#[tokio::test]
async fn no_marker_source_dispatches_unfiltered() -> color_eyre::Result<()> {
    let handler = create_handler(MockHandler::success());
    let context = MockEventContext::new();

    let msg = create_test_message("key1", Some("evt1"))?;
    let result = FallibleHandler::on_message(&handler, context, msg, DemandType::Normal).await;
    assert!(matches!(result, Ok(Some(()))));
    assert_eq!(handler.inner.call_count(), 1);
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

/// The settlement classification table: every Output and error variant. The
/// probe inner classifies everything `Bypassed`, so the delegating rows are
/// proven to delegate (a hardcoded `Final` would fail them).
#[test]
fn settlement_classification_table() {
    /// Inner probe whose classification is `Bypassed` for every result, so
    /// delegation is observable.
    #[derive(Clone)]
    struct BypassedProbe;

    impl FallibleHandler for BypassedProbe {
        type Error = TestError;
        type Output = ();
        type Payload = serde_json::Value;

        async fn on_excise<C>(
            &self,
            context: C,
            message: ConsumerMessage<Self::Payload>,
            demand_type: DemandType,
        ) -> Result<Self::Output, Self::Error>
        where
            C: EventContext<Payload = Self::Payload>,
        {
            FallibleHandler::on_message(self, context, message, demand_type).await
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

    impl SettlementHandler for BypassedProbe {
        fn settlement(_result: Result<&Self::Output, &Self::Error>) -> Settlement {
            Settlement::Bypassed
        }
    }

    type Subject = DeduplicationHandler<MockHandler, MemoryDeduplicationStore>;
    type Probe = DeduplicationHandler<BypassedProbe, MemoryDeduplicationStore>;
    type SubjectResult = Result<Option<()>, DeduplicationError<TestError>>;
    let rows: Vec<(&str, SubjectResult, Settlement)> = vec![
        (
            "Ok(Some) delegates (leaf Final)",
            Ok(Some(())),
            Settlement::Final,
        ),
        (
            "Ok(None) dedup hit is Bypassed",
            Ok(None),
            Settlement::Bypassed,
        ),
        (
            "Err(Inner) delegates (leaf Final)",
            Err(DeduplicationError::Inner(TestError::Permanent)),
            Settlement::Final,
        ),
        (
            "Err(Store) filter-read failure is Bypassed",
            Err(DeduplicationError::Store(Box::new(TestError::Transient))),
            Settlement::Bypassed,
        ),
    ];
    for (label, result, expected) in rows {
        assert_eq!(Subject::settlement(result.as_ref()), expected, "{label}");
    }

    // Delegation proof: over a Bypassed-classifying inner, the delegating
    // rows stay Bypassed — the wrapper is not hardcoding Final.
    let ok: Result<Option<()>, DeduplicationError<TestError>> = Ok(Some(()));
    assert_eq!(Probe::settlement(ok.as_ref()), Settlement::Bypassed);
    let err: Result<Option<()>, DeduplicationError<TestError>> =
        Err(DeduplicationError::Inner(TestError::Permanent));
    assert_eq!(Probe::settlement(err.as_ref()), Settlement::Bypassed);
}

impl FallibleEventHandler for DeduplicationHandler<MockHandler, MemoryDeduplicationStore> {}

/// A dedup skip records no second marker: the store is pre-seeded with the
/// session's dedup id, the boundary is driven end to end, and the skip
/// (`Ok(None)`, `Bypassed`) commits the offset without re-recording — the
/// oracle log stays empty (seeding wrote the store, never the log).
#[tokio::test]
async fn dedup_skip_records_no_second_marker() -> color_eyre::Result<()> {
    let msg = create_test_message("key1", Some("evt1"))?;
    let id = dedup_uuid_for_message(test_identity(), &msg);

    let state_key = StateKey::new(Uuid::from_u128(0xDD), Arc::from("key1"));
    let (session, _cell_store, _dirty, recorded) = recording_session(
        CollectionDefRegistry::default(),
        state_key,
        EventRef::Message { dedup_id: id },
    );
    let context = MockEventContext::new().with_session(session);

    let inner = MockHandler::success();
    let handler = create_handler(inner.clone());
    handler.store.insert(id).await?;

    let version = Arc::new(CachePadded::new(AtomicUsize::new(0)));
    let tracker = OffsetTracker::new("test-topic".into(), 0, 10, Duration::from_secs(5), version);
    let uncommitted = tracker.take(0).await?;
    let message = msg.into_uncommitted(uncommitted);

    EventHandler::on_message(&handler, context, message, DemandType::Normal).await;

    assert_eq!(inner.call_count(), 0, "the skip short-circuits the inner");
    assert!(
        recorded.lock().is_empty(),
        "a dedup skip must not record a second marker",
    );
    assert_eq!(
        tracker.shutdown().await,
        Some(0),
        "the skipped dispatch commits the offset",
    );
    Ok(())
}

#[test]
fn dedup_uuid_is_deterministic() -> color_eyre::Result<()> {
    let msg1 = create_test_message("key1", Some("evt1"))?;
    let msg2 = create_test_message("key1", Some("evt1"))?;
    assert_eq!(
        dedup_uuid_for_message(test_identity(), &msg1),
        dedup_uuid_for_message(test_identity(), &msg2),
    );
    Ok(())
}

#[test]
fn dedup_uuid_differs_by_dimension() -> color_eyre::Result<()> {
    let base_msg = create_test_message("key1", Some("evt1"))?;
    let base = dedup_uuid_for_message(test_identity(), &base_msg);

    let variants = [
        DedupIdentity {
            version: "2",
            ..test_identity()
        },
        DedupIdentity {
            group_id: "other-group",
            ..test_identity()
        },
        DedupIdentity {
            topic: "other-topic",
            ..test_identity()
        },
        DedupIdentity {
            partition: 1,
            ..test_identity()
        },
    ];
    for identity in variants {
        assert_ne!(base, dedup_uuid_for_message(identity, &base_msg));
    }

    // Different key
    let diff_key_msg = create_test_message("key2", Some("evt1"))?;
    assert_ne!(base, dedup_uuid_for_message(test_identity(), &diff_key_msg));

    // Different event_id
    let diff_evt_msg = create_test_message("key1", Some("evt2"))?;
    assert_ne!(base, dedup_uuid_for_message(test_identity(), &diff_evt_msg));

    // Offset fallback (no event_id) differs from event_id path
    let offset_msg = create_test_message("key1", None)?;
    assert_ne!(base, dedup_uuid_for_message(test_identity(), &offset_msg));

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
        &config,
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
        &config,
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

    async fn on_excise<C>(
        &self,
        context: C,
        message: ConsumerMessage<Self::Payload>,
        demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        FallibleHandler::on_message(self, context, message, demand_type).await
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

    let msg1 = create_test_message("key1", Some("evt1"))?;
    let msg2 = create_test_message("key1", Some("evt1"))?;
    let id = dedup_uuid_for_message(test_identity(), &msg1);

    // First dispatch: inner runs, on_message returns Ok(Some(())).
    let context1 = session_context(id);
    let result1 =
        FallibleHandler::on_message(&handler, context1.clone(), msg1, DemandType::Normal).await;
    assert!(matches!(result1, Ok(Some(()))));
    // The dedup middleware's after_commit must forward the inner half.
    FallibleHandler::after_commit(&handler, context1, result1).await;

    // Simulate the `settle` boundary recording the marker after the first
    // commit (the middleware never writes the store itself).
    handler.store.insert(id).await?;

    // Second dispatch: deduplicated, on_message returns Ok(None).
    let context2 = session_context(id);
    let result2 =
        FallibleHandler::on_message(&handler, context2.clone(), msg2, DemandType::Normal).await;
    assert!(matches!(result2, Ok(None)));
    FallibleHandler::after_commit(&handler, context2, result2).await;

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

/// The dedup id every deriver produces must agree: the partition loop's
/// `EventRef` derivation, the message-defer reload override, and the
/// keyed-state recovery oracle all call the canonical
/// [`dedup_uuid_for_message`] with the same [`DedupIdentity`]. If a reader
/// diverged, recovery would look committed message state up under the wrong
/// id and always read `NotCommitted`, silently rolling state back.
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

    let identity = DedupIdentity {
        version: VERSION,
        group_id: GROUP,
        topic: TOPIC,
        partition: PARTITION,
    };

    for event_id in [Some("evt-1"), None] {
        let msg = create_test_message("key-a", event_id)?;

        let writer_id = dedup_uuid_for_message(identity, &msg);
        let reader_id = dedup_uuid(
            VERSION,
            GROUP,
            TOPIC,
            PARTITION,
            msg.key().as_bytes(),
            msg.record()
                .message()
                .and_then(|payload| payload.get("id"))
                .and_then(|v| v.as_str())
                .map(str::as_bytes),
            msg.offset(),
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
