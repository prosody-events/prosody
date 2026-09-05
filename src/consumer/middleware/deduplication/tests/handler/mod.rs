//! Check marker lookup, handler calls, and settlement.

use crate::consumer::DemandType;
use crate::consumer::EventHandler;
use crate::consumer::Keyed;
use crate::consumer::event_context::EventContext;
use crate::consumer::message::{ConsumerMessage, ConsumerMessageValue};
use crate::consumer::middleware::deduplication::{
    DedupIdentity, DeduplicationConfiguration, DeduplicationError, DeduplicationHandler,
    DeduplicationMiddleware, DeduplicationOutput, DeduplicationStore, MemoryDeduplicationStore,
    MemoryDeduplicationStoreProvider, Presence, dedup_uuid, dedup_uuid_for_message,
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

/// Supply each lookup result without a network store.
#[derive(Clone)]
struct LookupStore(Result<Presence, TestError>);

impl DeduplicationStore for LookupStore {
    type Error = TestError;

    async fn recorded(&self, id: Uuid) -> Result<bool, Self::Error> {
        Ok(!matches!(self.lookup(id).await?, Presence::Absent))
    }

    fn lookup(&self, _id: Uuid) -> impl Future<Output = Result<Presence, Self::Error>> {
        ready(self.0.clone())
    }

    fn insert(&self, _id: Uuid) -> impl Future<Output = Result<(), Self::Error>> {
        ready(Ok(()))
    }
}

#[tokio::test]
async fn lookup_controls_dispatch() -> color_eyre::Result<()> {
    for (presence, output, settlement) in [
        (
            Ok(Presence::Absent),
            Some(DeduplicationOutput::Ran(())),
            Settlement::Final,
        ),
        (
            Ok(Presence::Settled),
            Some(DeduplicationOutput::Repeated),
            Settlement::Bypassed,
        ),
        (
            Ok(Presence::Inherited),
            Some(DeduplicationOutput::Redelivered),
            Settlement::Duplicate,
        ),
        (Err(TestError::Transient), None, Settlement::Bypassed),
    ] {
        for excise in [false, true] {
            let handler = DeduplicationHandler {
                inner: MockHandler::success(),
                store: LookupStore(presence.clone()),
            };
            let message = create_test_message("key1", Some("evt1"))?;
            let context = session_context(dedup_uuid_for_message(test_identity(), &message));
            let result = if excise {
                FallibleHandler::on_excise(
                    &handler,
                    context,
                    create_test_message_from(ConsumerMessageValue {
                        key: "key1".into(),
                        payload: (),
                        ..Default::default()
                    })?,
                    DemandType::Normal,
                )
                .await
            } else {
                FallibleHandler::on_message(&handler, context, message, DemandType::Normal).await
            };
            assert_eq!(result.as_ref().map_err(|_| ()), output.as_ref().ok_or(()));
            assert_eq!(
                DeduplicationHandler::<MockHandler, LookupStore>::settlement(result.as_ref()),
                settlement
            );
            assert_eq!(
                handler.inner.call_count(),
                usize::from(matches!(presence, Ok(Presence::Absent)))
            );
        }
    }
    Ok(())
}

/// Supply the partition identity for message markers.
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

/// Set the message identity that the filter reads.
fn session_context(dedup_id: Uuid) -> MockEventContext<serde_json::Value, RecordingSession> {
    let state_key = StateKey::new(Uuid::from_u128(0xDD), Arc::from("test-key"));
    let (session, _cell_store, _dirty, _recorded) = recording_session(
        CollectionDefRegistry::default(),
        state_key,
        EventRef::Message { dedup_id },
    );
    MockEventContext::new().with_session(session)
}

/// A stored marker prevents a second handler call.
#[tokio::test]
async fn seeded_id_filters_before_handler() -> color_eyre::Result<()> {
    let handler = create_handler(MockHandler::success());

    let msg = create_test_message("key1", Some("evt1"))?;
    let id = dedup_uuid_for_message(test_identity(), &msg);
    handler.store.insert(id).await?;
    let context = session_context(id);

    let result = FallibleHandler::on_message(&handler, context, msg, DemandType::Normal).await;
    assert!(
        matches!(result, Ok(DeduplicationOutput::Repeated)),
        "a seeded id is filtered"
    );
    assert_eq!(handler.inner.call_count(), 0, "filtered before the handler");
    Ok(())
}

#[tokio::test]
async fn cache_miss_runs_handler() -> color_eyre::Result<()> {
    let handler = create_handler(MockHandler::success());

    let msg = create_test_message("key1", Some("evt1"))?;
    let context = session_context(dedup_uuid_for_message(test_identity(), &msg));

    let result = FallibleHandler::on_message(&handler, context, msg, DemandType::Normal).await;
    assert!(matches!(result, Ok(DeduplicationOutput::Ran(()))));
    assert_eq!(handler.inner.call_count(), 1);
    Ok(())
}

/// A marker for another message cannot filter this message.
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
    assert!(matches!(result, Ok(DeduplicationOutput::Ran(()))));
    assert_eq!(handler.inner.call_count(), 1);
    Ok(())
}

/// A context without a message identity cannot filter messages.
#[tokio::test]
async fn no_marker_source_dispatches_unfiltered() -> color_eyre::Result<()> {
    let handler = create_handler(MockHandler::success());
    let context = MockEventContext::new();

    let msg = create_test_message("key1", Some("evt1"))?;
    let result = FallibleHandler::on_message(&handler, context, msg, DemandType::Normal).await;
    assert!(matches!(result, Ok(DeduplicationOutput::Ran(()))));
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
    assert!(matches!(result, Ok(DeduplicationOutput::Ran(()))));
    assert_eq!(handler.inner.call_count(), 1);
}

/// Check each result variant. The probe detects a lost inner classification.
#[test]
fn settlement_classification_table() {
    /// Always return `Bypassed` to check delegation.
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
    type SubjectResult = Result<DeduplicationOutput<()>, DeduplicationError<TestError>>;
    let rows: Vec<(&str, SubjectResult, Settlement)> = vec![
        (
            "Ran delegates",
            Ok(DeduplicationOutput::Ran(())),
            Settlement::Final,
        ),
        (
            "Repeated bypasses the sweep",
            Ok(DeduplicationOutput::Repeated),
            Settlement::Bypassed,
        ),
        (
            "Redelivered requests the sweep",
            Ok(DeduplicationOutput::Redelivered),
            Settlement::Duplicate,
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

    // Preserve the inner classification for success and failure.
    let ok: Result<DeduplicationOutput<()>, DeduplicationError<TestError>> =
        Ok(DeduplicationOutput::Ran(()));
    assert_eq!(Probe::settlement(ok.as_ref()), Settlement::Bypassed);
    let err: Result<DeduplicationOutput<()>, DeduplicationError<TestError>> =
        Err(DeduplicationError::Inner(TestError::Permanent));
    assert_eq!(Probe::settlement(err.as_ref()), Settlement::Bypassed);
}

impl FallibleEventHandler for DeduplicationHandler<MockHandler, MemoryDeduplicationStore> {}

/// A repeated message commits its offset without a second marker.
mod contracts;
