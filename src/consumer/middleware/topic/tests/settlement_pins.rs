use super::*;
use crate::Key;
use crate::consumer::message::ConsumerMessageValue;
use crate::consumer::message::UncommittedMessage;
use crate::consumer::middleware::tests::test_support::{
    MockEventContext as SessionContext, RecordingParts, StagingError, StagingHook,
    StagingTransientHandler, committed_json_value, recording_session,
};
use crate::consumer::middleware::{FallibleEventHandler, Settlement, SettlementHandler};
use crate::consumer::partition::offsets::OffsetTracker;
use crate::consumer::receipted_sealed;
use crate::consumer::{EventHandler, Receipted, ReceiptedSource, Uncommitted};
use crate::state::manager::EventStateScope;
use crate::state::registry::{CollectionDef, CollectionDefRegistry};
use crate::state::{EventRef, StateKey};
use crossbeam_utils::CachePadded;
use std::future::ready;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::Semaphore;
use uuid::Uuid;

impl FallibleEventHandler for FailureTopicHandler<StagingTransientHandler, JsonCodec> {}

const DEDUP_ID: Uuid = Uuid::from_u128(0xDEF3);

fn session_fixture() -> color_eyre::Result<(RecordingParts, StateKey)> {
    let mut registry = CollectionDefRegistry::default();
    registry.register(
        &StagingTransientHandler::collection(),
        CollectionDef::new(None),
    )?;
    let state_key = StateKey::new(Uuid::from_u128(0xD3), Arc::from("user-1"));
    let parts = recording_session(
        registry,
        state_key.clone(),
        EventRef::Message { dedup_id: DEDUP_ID },
    );
    Ok((parts, state_key))
}

async fn uncommitted_message()
-> color_eyre::Result<(UncommittedMessage<serde_json::Value>, OffsetTracker)> {
    let version = Arc::new(CachePadded::new(AtomicUsize::new(0)));
    let tracker = OffsetTracker::new("test-topic".into(), 0, 10, Duration::from_secs(5), version);
    let uncommitted_offset = tracker.take(0).await?;
    let semaphore = Arc::new(Semaphore::new(1));
    let permit = semaphore.try_acquire_owned()?;
    let message = ConsumerMessage::new(
        ConsumerMessageValue::default(),
        tracing::Span::current(),
        permit,
    )
    .into_uncommitted(uncommitted_offset);
    Ok((message, tracker))
}

/// `Ok(Routed)` records nothing and stages nothing: the outcome lives in
/// the DLQ, so the settle boundary bypasses stage and marker while
/// committing the offset; the dirty residue dies with the scope drop.
#[tokio::test]
async fn routed_swallow_records_nothing_and_stages_nothing() -> color_eyre::Result<()> {
    let ((session, cell_store, dirty, recorded), state_key) = session_fixture()?;
    let scope = EventStateScope::new(session);

    let inner = StagingTransientHandler::new();
    let handler = make_handler(inner.clone())?;

    let context = SessionContext::new().with_session(scope.handle());
    let (message, tracker) = uncommitted_message().await?;

    EventHandler::on_message(&handler, context, message, DemandType::Normal).await;

    // The route is FINAL (no retry is coming), so unlike the defer
    // swallows the inner sees `after_commit(Err)` — the routing pinned by
    // the "commit x Routed(Err)" row of `apply_hook_routing_matrix`.
    assert_eq!(inner.hooks(), vec![StagingHook::Commit]);
    // The Bypassed contract: nothing from the failed attempt settles.
    // These are also the positive control that the ROUTED swallow ran:
    // had the error surfaced instead (no route), the boundary would have
    // recorded the Permanent marker or re-driven.
    assert_eq!(
        committed_json_value(&cell_store, state_key, "cart").await?,
        None,
        "the failed attempt's buffered write must not commit",
    );
    assert!(
        recorded.lock().is_empty(),
        "the routed attempt must record no marker — a redelivery of the routed message must not \
         be dedup-filtered",
    );
    assert_eq!(
        tracker.shutdown().await,
        Some(0),
        "the routed dispatch commits the offset",
    );
    drop(scope);
    let key: Key = Arc::from("user-1");
    assert!(
        dirty.touched(&key).is_empty(),
        "the scope drop sweeps the routed attempt's dirty residue",
    );
    Ok(())
}

/// `DlqSendFailed { inner: Transient, producer: Permanent }` records NO
/// marker: the message is neither handled nor in the DLQ, so a marker
/// would silently dedup-filter its redelivery. Driven at the settle
/// level (the producer cannot be made to fail on demand in a unit
/// test); the classification cells below are the pure-function pin.
#[tokio::test]
async fn dlq_send_failed_with_transient_inner_records_no_marker() -> color_eyre::Result<()> {
    use crate::codec::JsonCodecError;
    use crate::consumer::middleware::settle;

    let ((session, _cell_store, _dirty, recorded), _state_key) = session_fixture()?;
    let scope = EventStateScope::new(session);
    let context = SessionContext::new().with_session(scope.handle());

    let inner = StagingTransientHandler::new();
    let handler = make_handler(inner)?;

    let committed = Arc::new(AtomicUsize::new(0));
    let aborted = Arc::new(AtomicUsize::new(0));
    let guard = Guard {
        committed: committed.clone(),
        aborted: aborted.clone(),
    };

    // The composite the stack would surface: a Transient inner under a
    // Permanent producer error (a serialization rejection).
    let result: Result<
        FailureTopicOutput<(), StagingError>,
        FailureTopicError<StagingError, JsonCodecError>,
    > = Err(FailureTopicError::DlqSendFailed {
        inner: StagingError(ErrorCategory::Transient),
        producer: permanent_producer()?,
    });

    settle(&handler, context, guard, result).await;

    assert!(
        recorded.lock().is_empty(),
        "a Transient inner under a Permanent producer error must record NO marker",
    );
    assert_eq!(
        committed.load(Ordering::SeqCst),
        1,
        "the Bypassed final commits the offset",
    );
    assert_eq!(aborted.load(Ordering::SeqCst), 0);
    Ok(())
}

/// Offset guard recording which terminal the settle-level pin chose.
struct Guard {
    committed: Arc<AtomicUsize>,
    aborted: Arc<AtomicUsize>,
}

impl Uncommitted for Guard {
    async fn commit(self) {
        self.committed.fetch_add(1, Ordering::SeqCst);
    }

    async fn abort(self) {
        self.aborted.fetch_add(1, Ordering::SeqCst);
    }
}

impl receipted_sealed::Sealed for Guard {}

impl Receipted for Guard {
    type Source = Self;

    fn receipt(self) -> impl Future<Output = Self::Source> + Send {
        ready(self)
    }
}

impl ReceiptedSource for Guard {
    async fn retire(self) {
        self.commit().await;
    }

    async fn keep(self) {}
}

/// A Permanent producer error (a payload serialization rejection), for
/// the `DlqSendFailed` cells.
fn permanent_producer() -> color_eyre::Result<ProducerError<JsonCodecError>> {
    let Err(serde_err) = serde_json::from_str::<serde_json::Value>("{") else {
        color_eyre::eyre::bail!("malformed JSON must fail to parse");
    };
    Ok(ProducerError::Serialization(JsonCodecError::Serde(
        serde_err,
    )))
}

/// The settlement classification table for the failure-topic wrapper:
/// every Output and error variant, including the `DlqSendFailed` guard
/// cells (inner-Permanent delegates; inner-Transient/Terminal bypass).
/// Delegation is proven against a `Bypassed`-classifying probe: the
/// `Inner(Ok)`, `Handler`, and Permanent-inner `DlqSendFailed` rows
/// delegate, so a wrapper hardcoding `Final` on them fails the probe.
#[test]
fn settlement_classification_table() -> color_eyre::Result<()> {
    use crate::codec::JsonCodecError;
    use crate::consumer::middleware::tests::test_support::BypassedHandler;

    type Subject = FailureTopicHandler<StagingTransientHandler, JsonCodec>;
    type Out = FailureTopicOutput<(), StagingError>;
    type TableErr = FailureTopicError<StagingError, JsonCodecError>;
    type Probe = FailureTopicHandler<BypassedHandler, JsonCodec>;
    type ProbeOut = FailureTopicOutput<(), TestError>;
    type ProbeErr = FailureTopicError<TestError, JsonCodecError>;

    fn dlq(inner: ErrorCategory) -> color_eyre::Result<TableErr> {
        Ok(FailureTopicError::DlqSendFailed {
            inner: StagingError(inner),
            producer: permanent_producer()?,
        })
    }

    let rows: Vec<(&str, Result<Out, TableErr>, Settlement)> = vec![
        (
            "Inner(Ok) delegates to the leaf's Final",
            Ok(FailureTopicOutput::Inner(())),
            Settlement::Final,
        ),
        (
            "Routed is Bypassed (outcome lives in the DLQ)",
            Ok(FailureTopicOutput::Routed(StagingError(
                ErrorCategory::Transient,
            ))),
            Settlement::Bypassed,
        ),
        (
            "Handler delegates to the leaf's Final",
            Err(FailureTopicError::Handler(StagingError(
                ErrorCategory::Permanent,
            ))),
            Settlement::Final,
        ),
        (
            "DlqSendFailed with a Permanent inner delegates (it would have certified)",
            Err(dlq(ErrorCategory::Permanent)?),
            Settlement::Final,
        ),
        (
            "DlqSendFailed with a Transient inner is Bypassed (never certifies)",
            Err(dlq(ErrorCategory::Transient)?),
            Settlement::Bypassed,
        ),
        (
            "DlqSendFailed with a Terminal inner is Bypassed (never certifies)",
            Err(dlq(ErrorCategory::Terminal)?),
            Settlement::Bypassed,
        ),
    ];
    for (label, result, expected) in rows {
        assert_eq!(Subject::settlement(result.as_ref()), expected, "{label}");
    }

    // Delegation proof: over a Bypassed-classifying inner the delegating
    // rows (`Inner(Ok)`, `Handler`, Permanent-inner `DlqSendFailed`) stay
    // Bypassed.
    let inner: Result<ProbeOut, ProbeErr> = Ok(FailureTopicOutput::Inner(()));
    assert_eq!(Probe::settlement(inner.as_ref()), Settlement::Bypassed);
    let handler: Result<ProbeOut, ProbeErr> = Err(FailureTopicError::Handler(TestError(
        ErrorCategory::Permanent,
    )));
    assert_eq!(Probe::settlement(handler.as_ref()), Settlement::Bypassed);
    let dlq_permanent: Result<ProbeOut, ProbeErr> = Err(FailureTopicError::DlqSendFailed {
        inner: TestError(ErrorCategory::Permanent),
        producer: permanent_producer()?,
    });
    assert_eq!(
        Probe::settlement(dlq_permanent.as_ref()),
        Settlement::Bypassed
    );
    Ok(())
}
