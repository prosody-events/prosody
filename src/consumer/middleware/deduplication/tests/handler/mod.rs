//! Unit tests for the deduplication handler: the stateless filter over the
//! session's boundary-readable message marker, the dedup-id derivation, and
//! the settlement classification table.

use crate::consumer::DemandType;
use crate::consumer::EventHandler;
use crate::consumer::Keyed;
use crate::consumer::event_context::EventContext;
use crate::consumer::message::{ConsumerMessage, ConsumerMessageValue};
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
use std::future::ready;
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

    fn on_excise<C>(
        &self,
        _context: C,
        _message: ConsumerMessage<()>,
        _demand_type: DemandType,
    ) -> impl Future<Output = Result<Self::Output, Self::Error>>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        self.call_count.fetch_add(1, Ordering::Relaxed);
        ready(if let Some(ref error) = self.error {
            Err(error.clone())
        } else {
            Ok(())
        })
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
        self.call_count.fetch_add(1, Ordering::Relaxed);
        ready(if let Some(ref e) = self.error {
            Err(e.clone())
        } else {
            Ok(())
        })
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
        self.call_count.fetch_add(1, Ordering::Relaxed);
        ready(Ok(()))
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
        payload,
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

        fn on_excise<C>(
            &self,
            _context: C,
            _message: ConsumerMessage<()>,
            _demand_type: DemandType,
        ) -> impl Future<Output = Result<Self::Output, Self::Error>>
        where
            C: EventContext<Payload = Self::Payload>,
        {
            ready(Ok(()))
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
            ready(Ok(()))
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
            ready(Ok(()))
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
mod contracts;
