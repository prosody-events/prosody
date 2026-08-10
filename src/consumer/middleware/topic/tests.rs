use super::*;
use crate::JsonCodec;
use crate::codec::JsonCodecError;
use crate::consumer::middleware::tests::test_support::{MockEventContext, TestError};
use crate::error::ErrorCategory;
use crate::producer::ProducerConfiguration;
use crate::telemetry::Telemetry;
use parking_lot::Mutex;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use std::sync::Arc;

// === Error Classification Tests ===

#[test]
fn handler_error_delegates_classification_transient() {
    let error: FailureTopicError<TestError, JsonCodecError> =
        FailureTopicError::Handler(TestError(ErrorCategory::Transient));
    assert!(matches!(error.classify_error(), ErrorCategory::Transient));
}

#[test]
fn handler_error_delegates_classification_permanent() {
    let error: FailureTopicError<TestError, JsonCodecError> =
        FailureTopicError::Handler(TestError(ErrorCategory::Permanent));
    assert!(matches!(error.classify_error(), ErrorCategory::Permanent));
}

#[test]
fn handler_error_delegates_classification_terminal() {
    let error: FailureTopicError<TestError, JsonCodecError> =
        FailureTopicError::Handler(TestError(ErrorCategory::Terminal));
    assert!(matches!(error.classify_error(), ErrorCategory::Terminal));
}

#[test]
fn dlq_send_failed_classifies_by_producer_error() {
    // Kafka errors with transient error codes should be classified as transient.
    // `DlqSendFailed` must classify on the producer error, not the inner
    // handler error, so the outer retry layer reacts to the producer-level
    // failure.
    let kafka_error = KafkaError::MessageProduction(RDKafkaErrorCode::BrokerNotAvailable);
    let error: FailureTopicError<TestError, JsonCodecError> = FailureTopicError::DlqSendFailed {
        // Inner is Permanent, but we expect the classification to follow
        // the producer error (Transient), not the inner.
        inner: TestError(ErrorCategory::Permanent),
        producer: ProducerError::Kafka(kafka_error),
    };
    assert!(
        matches!(error.classify_error(), ErrorCategory::Transient),
        "DlqSendFailed should classify by the producer error, not the inner",
    );
}

// === Apply-hook wiring tests ===
//
// These tests construct a `FailureTopicHandler` directly and drive its
// apply hooks with synthetic `Result<Output, Error>` values. We are not
// exercising the full dispatch path here — we only verify that each
// arm of the new routing matrix forwards the correct
// `Result<inner::Output, inner::Error>` to the inner handler.

/// Records the inner-result a probe handler observes in each apply hook.
#[derive(Debug, Clone, PartialEq, Eq)]
enum InnerHookEvent {
    Commit(Result<u64, TestError>),
    Abort(Result<u64, TestError>),
}

/// Probe inner handler that records every apply-hook call and returns a
/// configurable result from `on_timer` (defaults to `Ok(0)` for tests
/// that don't drive the timer dispatch path).
#[derive(Clone)]
struct Probe {
    log: Arc<Mutex<Vec<InnerHookEvent>>>,
    timer_result: Arc<Mutex<Result<u64, TestError>>>,
}

impl Probe {
    fn new() -> Self {
        Self {
            log: Arc::default(),
            timer_result: Arc::new(Mutex::new(Ok(0))),
        }
    }

    fn returning_timer(timer_result: Result<u64, TestError>) -> Self {
        Self {
            log: Arc::default(),
            timer_result: Arc::new(Mutex::new(timer_result)),
        }
    }
}

impl FallibleHandler for Probe {
    type Error = TestError;
    type Output = u64;
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
        self.timer_result.lock().clone()
    }

    async fn after_commit<C>(&self, _context: C, result: Result<Self::Output, Self::Error>)
    where
        C: EventContext<Payload = Self::Payload>,
    {
        self.log.lock().push(InnerHookEvent::Commit(result));
    }

    async fn after_abort<C>(&self, _context: C, result: Result<Self::Output, Self::Error>)
    where
        C: EventContext<Payload = Self::Payload>,
    {
        self.log.lock().push(InnerHookEvent::Abort(result));
    }

    async fn shutdown(self) {}
}

/// Constructs a `FailureTopicHandler` over an inner handler using a mock
/// producer (no real Kafka connection required).
fn make_handler<T>(inner: T) -> color_eyre::Result<FailureTopicHandler<T, JsonCodec>> {
    // The mock-flag short-circuits the bootstrap-server lookup, but the
    // builder still validates the field, so we supply a sentinel value
    // along with a non-empty source system.
    let config = ProducerConfiguration::builder()
        .bootstrap_servers(vec!["mock:9092".to_owned()])
        .source_system("test")
        .mock(true)
        .build()?;
    let telemetry = Telemetry::default();
    let producer = ProsodyProducer::new(&config, telemetry.sender())?;
    Ok(FailureTopicHandler {
        topic: "dlq".into(),
        producer,
        group_id: "group".to_owned(),
        handler: inner,
    })
}

fn dlq_send_failed_err(category: ErrorCategory) -> FailureTopicError<TestError, JsonCodecError> {
    FailureTopicError::DlqSendFailed {
        inner: TestError(category),
        producer: ProducerError::Kafka(KafkaError::MessageProduction(
            RDKafkaErrorCode::BrokerNotAvailable,
        )),
    }
}

/// Exhaustive routing matrix: every combination of apply hook
/// (`after_commit` / `after_abort`) and result variant (`Inner`, `Routed`,
/// `Handler`, `DlqSendFailed`) must forward exactly one
/// `Result<inner::Output, inner::Error>` call to the inner handler, per the
/// work-centric invariant documented on `FailureTopicHandler::after_commit`
/// / `after_abort`. Each row's `expected` makes a failing combination
/// self-identifying.
#[tokio::test]
async fn apply_hook_routing_matrix() -> color_eyre::Result<()> {
    enum Hook {
        Commit,
        Abort,
    }

    type HandlerResult =
        Result<FailureTopicOutput<u64, TestError>, FailureTopicError<TestError, JsonCodecError>>;

    let rows: Vec<(&str, Hook, HandlerResult, InnerHookEvent)> = vec![
        (
            "commit x Inner(Ok): dispatch is final, forward Ok",
            Hook::Commit,
            Ok(FailureTopicOutput::Inner(42)),
            InnerHookEvent::Commit(Ok(42)),
        ),
        (
            "commit x Routed(Err): DLQ accepted, marker commits, inner will not be re-dispatched",
            Hook::Commit,
            Ok(FailureTopicOutput::Routed(TestError(
                ErrorCategory::Permanent,
            ))),
            InnerHookEvent::Commit(Err(TestError(ErrorCategory::Permanent))),
        ),
        (
            "commit x Handler(Err): terminal error the framework chose to commit rather than abort",
            Hook::Commit,
            Err(FailureTopicError::Handler(TestError(
                ErrorCategory::Terminal,
            ))),
            InnerHookEvent::Commit(Err(TestError(ErrorCategory::Terminal))),
        ),
        (
            "commit x DlqSendFailed(Err): outer treats the producer error as final (no retry), \
             inner error still forwarded",
            Hook::Commit,
            Err(dlq_send_failed_err(ErrorCategory::Transient)),
            InnerHookEvent::Commit(Err(TestError(ErrorCategory::Transient))),
        ),
        (
            "abort x Inner(Ok): inner succeeded but the outer aborted (e.g. shutdown intervened)",
            Hook::Abort,
            Ok(FailureTopicOutput::Inner(7)),
            InnerHookEvent::Abort(Ok(7)),
        ),
        (
            "abort x Routed(Err): rare path, outer aborted despite the DLQ accepting the routed \
             message, re-dispatch is coming",
            Hook::Abort,
            Ok(FailureTopicOutput::Routed(TestError(
                ErrorCategory::Permanent,
            ))),
            InnerHookEvent::Abort(Err(TestError(ErrorCategory::Permanent))),
        ),
        (
            "abort x Handler(Err): terminal error, marker aborted",
            Hook::Abort,
            Err(FailureTopicError::Handler(TestError(
                ErrorCategory::Terminal,
            ))),
            InnerHookEvent::Abort(Err(TestError(ErrorCategory::Terminal))),
        ),
        (
            "abort x DlqSendFailed(Err): outer retry will re-drive the whole stack including the \
             inner",
            Hook::Abort,
            Err(dlq_send_failed_err(ErrorCategory::Permanent)),
            InnerHookEvent::Abort(Err(TestError(ErrorCategory::Permanent))),
        ),
    ];

    for (label, hook, result, expected) in rows {
        let inner = Probe::new();
        let log = inner.log.clone();
        let handler = make_handler(inner)?;

        match hook {
            Hook::Commit => handler.after_commit(MockEventContext::new(), result).await,
            Hook::Abort => handler.after_abort(MockEventContext::new(), result).await,
        }

        let events = log.lock().clone();
        assert_eq!(
            events.len(),
            1,
            "{label}: exactly one inner hook should fire"
        );
        assert_eq!(events[0], expected, "{label}: unexpected inner hook event");
    }
    Ok(())
}

// === on_timer dispatch tests ===
//
// The on_timer impl is a pure pass-through: `Ok` becomes `Inner`, `Err`
// becomes `Handler` regardless of category. One Err and one Ok test cover
// the full code path.

fn test_trigger() -> Trigger {
    use crate::timers::TimerType;
    use crate::timers::datetime::CompactDateTime;
    Trigger::new(
        Arc::from("key"),
        CompactDateTime::from(0_u32),
        TimerType::Application,
        tracing::Span::current(),
    )
}

#[tokio::test]
async fn on_timer_err_propagates_as_handler() -> color_eyre::Result<()> {
    // Timer errors are not rescued; outer retry/telemetry see the inner's
    // classification unchanged.
    let inner = Probe::returning_timer(Err(TestError(ErrorCategory::Transient)));
    let handler = make_handler(inner)?;

    let result = handler
        .on_timer(MockEventContext::new(), test_trigger(), DemandType::Normal)
        .await;

    assert!(
        matches!(
            result,
            Err(FailureTopicError::Handler(TestError(
                ErrorCategory::Transient
            ))),
        ),
        "expected Err(Handler(Transient)); got {result:?}",
    );
    Ok(())
}

#[tokio::test]
async fn on_timer_ok_returns_inner_output() -> color_eyre::Result<()> {
    let inner = Probe::returning_timer(Ok(7));
    let handler = make_handler(inner)?;

    let result = handler
        .on_timer(MockEventContext::new(), test_trigger(), DemandType::Normal)
        .await;

    assert!(matches!(result, Ok(FailureTopicOutput::Inner(7))));
    Ok(())
}

// === Configuration Tests ===

#[test]
fn configuration_requires_non_empty_failure_topic() {
    let config = FailureTopicConfiguration {
        failure_topic: String::new(),
    };
    assert!(
        config.validate().is_err(),
        "Empty failure topic should fail validation"
    );
}

#[test]
fn configuration_accepts_valid_failure_topic() {
    let config = FailureTopicConfiguration {
        failure_topic: String::from("dlq-topic"),
    };
    assert!(
        config.validate().is_ok(),
        "Non-empty failure topic should pass validation"
    );
}

/// The routed swallow through the settle boundary: the inner attempt buffers
/// a `cart` write and fails Transient; the DLQ route swallows that error into
/// `Ok(Routed)` — classified `Bypassed`, so the offset commits while nothing
/// stages and no marker records (the same result-typed guarantee both defer
/// swallow points get). Plus the `DlqSendFailed` divergence pin and the
/// settlement classification table.
mod settlement_pins {
    use super::*;
    use crate::Key;
    use crate::consumer::EventHandler;
    use crate::consumer::Uncommitted;
    use crate::consumer::message::ConsumerMessageValue;
    use crate::consumer::message::UncommittedMessage;
    use crate::consumer::middleware::tests::test_support::{
        MockEventContext as SessionContext, RecordingParts, StagingError, StagingHook,
        StagingTransientHandler, committed_value, recording_session,
    };
    use crate::consumer::middleware::{FallibleEventHandler, Settlement, SettlementHandler};
    use crate::consumer::partition::offsets::OffsetTracker;
    use crate::state::manager::EventStateScope;
    use crate::state::registry::{CollectionDef, CollectionDefRegistry};
    use crate::state::{EventRef, StateKey};
    use crossbeam_utils::CachePadded;
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
        let tracker =
            OffsetTracker::new("test-topic".into(), 0, 10, Duration::from_secs(5), version);
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
            committed_value(&cell_store, state_key, "cart").await?,
            None,
            "the failed attempt's buffered write must not commit",
        );
        assert!(
            recorded.lock().is_empty(),
            "the routed attempt must record no marker — a redelivery of the routed message must \
             not be dedup-filtered",
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
}
