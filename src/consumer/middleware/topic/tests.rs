use super::*;
use crate::codec::JsonCodecError;
use crate::consumer::middleware::tests::test_support::MockEventContext;
use crate::error::ErrorCategory;
use crate::producer::ProducerConfiguration;
use crate::telemetry::Telemetry;
use parking_lot::Mutex;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use std::error::Error;
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::sync::Arc;

/// Test error type with configurable classification.
#[derive(Debug, Clone)]
struct TestError(ErrorCategory);

impl Display for TestError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "test error ({:?})", self.0)
    }
}

impl Error for TestError {}

impl ClassifyError for TestError {
    fn classify_error(&self) -> ErrorCategory {
        self.0
    }
}

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
#[derive(Debug, Clone)]
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

/// Constructs a `FailureTopicHandler` over a probe inner using a mock
/// producer (no real Kafka connection required).
fn make_handler(inner: Probe) -> color_eyre::Result<FailureTopicHandler<Probe, crate::JsonCodec>> {
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

#[tokio::test]
async fn after_commit_routed_forwards_inner_err_to_inner() -> color_eyre::Result<()> {
    // DLQ accepted: marker commits, the inner will not be re-dispatched,
    // so the inner's apply hook MUST fire as `after_commit(Err(inner))`.
    let inner = Probe::new();
    let log = inner.log.clone();
    let handler = make_handler(inner)?;

    let result: Result<
        FailureTopicOutput<u64, TestError>,
        FailureTopicError<TestError, JsonCodecError>,
    > = Ok(FailureTopicOutput::Routed(TestError(
        ErrorCategory::Permanent,
    )));
    handler.after_commit(MockEventContext::new(), result).await;

    let events = log.lock().clone();
    assert_eq!(events.len(), 1, "exactly one inner hook should fire");
    assert!(
        matches!(
            &events[0],
            InnerHookEvent::Commit(Err(TestError(ErrorCategory::Permanent))),
        ),
        "expected Commit(Err(Permanent)), got {:?}",
        events[0],
    );
    Ok(())
}

#[tokio::test]
async fn after_commit_inner_ok_forwards_inner_output() -> color_eyre::Result<()> {
    let inner = Probe::new();
    let log = inner.log.clone();
    let handler = make_handler(inner)?;

    let result: Result<
        FailureTopicOutput<u64, TestError>,
        FailureTopicError<TestError, JsonCodecError>,
    > = Ok(FailureTopicOutput::Inner(42));
    handler.after_commit(MockEventContext::new(), result).await;

    let events = log.lock().clone();
    assert_eq!(events.len(), 1);
    assert!(
        matches!(&events[0], InnerHookEvent::Commit(Ok(42))),
        "inner Ok output should be forwarded to inner.after_commit",
    );
    Ok(())
}

#[tokio::test]
async fn after_commit_dlq_send_failed_forwards_inner_err() -> color_eyre::Result<()> {
    // Even though `DlqSendFailed` would normally route through the outer
    // retry layer (and thus surface to the inner via `after_abort`),
    // when the framework decides the marker commits anyway, we still
    // owe the inner a typed `Err(inner)` here.
    let inner = Probe::new();
    let log = inner.log.clone();
    let handler = make_handler(inner)?;

    let result: Result<
        FailureTopicOutput<u64, TestError>,
        FailureTopicError<TestError, JsonCodecError>,
    > = Err(dlq_send_failed_err(ErrorCategory::Transient));
    handler.after_commit(MockEventContext::new(), result).await;

    let events = log.lock().clone();
    assert_eq!(events.len(), 1);
    assert!(
        matches!(
            &events[0],
            InnerHookEvent::Commit(Err(TestError(ErrorCategory::Transient))),
        ),
        "expected Commit(Err(Transient)), got {:?}",
        events[0],
    );
    Ok(())
}

#[tokio::test]
async fn after_abort_dlq_send_failed_forwards_inner_err() -> color_eyre::Result<()> {
    // Outer retry path: the producer error fired the outer retry, the
    // whole stack will be re-dispatched, so the inner sees
    // `after_abort(Err(inner))`.
    let inner = Probe::new();
    let log = inner.log.clone();
    let handler = make_handler(inner)?;

    let result: Result<
        FailureTopicOutput<u64, TestError>,
        FailureTopicError<TestError, JsonCodecError>,
    > = Err(dlq_send_failed_err(ErrorCategory::Permanent));
    handler.after_abort(MockEventContext::new(), result).await;

    let events = log.lock().clone();
    assert_eq!(events.len(), 1);
    assert!(
        matches!(
            &events[0],
            InnerHookEvent::Abort(Err(TestError(ErrorCategory::Permanent))),
        ),
        "expected Abort(Err(Permanent)), got {:?}",
        events[0],
    );
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
