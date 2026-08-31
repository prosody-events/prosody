//! The result request survives a deferred retry.
//!
//! A deferred attempt reaches `after_abort`, so it answers nothing. The reload
//! arrives as a message dispatch inside the defer layer's timer handling, and
//! it answers with the reloaded record's own request. The defer round trip must
//! carry the request because the answer follows the message, not the attempt.

use super::super::RespondHandler;
use super::{
    Fixture, ResultProbeCodec, offset_tracker, requesting, requesting_under, serialize_count,
};
use crate::consumer::message::ConsumerRecord;
use crate::consumer::middleware::FallibleEventHandler;
use crate::consumer::middleware::defer::DeferConfiguration;
use crate::consumer::middleware::defer::decider::TraceBasedDecider;
use crate::consumer::middleware::defer::message::handler::MessageDeferHandler;
use crate::consumer::middleware::defer::message::store::MessageDeferStore;
use crate::consumer::middleware::defer::message::store::memory::MemoryMessageDeferStore;
use crate::consumer::middleware::providers::LeafHandler;
use crate::consumer::middleware::tests::test_support::{
    MockEventContext, RecordingTimer, ScriptedHandler, create_test_trigger_with,
};
use crate::consumer::{DemandType, EventHandler};
use crate::error::{ClassifyError, ErrorCategory};
use crate::loader::MessageLoader;
use crate::otel::SpanRelation;
use crate::peer::response::RequestId;
use crate::peer::response::frame::FrameResult;
use crate::peer::response::frame::tests::decode_frame;
use crate::peer::router::loopback::{TestRouter, paused, peer};
use crate::related_span;
use crate::telemetry::Telemetry;
use crate::test_util::{captured_spans, named, sampled_remote_context};
use crate::timers::TimerType;
use crate::{Key, Offset, Partition, Topic};
use color_eyre::Result;
use color_eyre::eyre::{ensure, eyre};
use opentelemetry::trace::{TraceContextExt, TraceId};
use serde_json::Value;
use std::future::ready;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tracing::{Instrument, info_span};
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// The key both the deferred message and its retry timer carry.
const KEY: &str = "deferred";

/// The peer the deferred record asks for its answer.
const TARGET: u8 = 3;

/// The request the deferred record names.
const REQUEST: u8 = 44;

/// The span one outbound response opens.
const SENT: &str = "request.response.send";

/// The span the loader opens for the record it rebuilds. It is the reloaded
/// record's own span, so the answer must hang directly under it.
const RELOADED: &str = "load";

/// A loader whose reloaded record still asks for a response.
///
/// The shared in-memory loader cannot serve here: every record it rebuilds
/// carries no result request, so it could not tell a lost request from a
/// suppressed answer.
#[derive(Clone)]
struct RequestLoader;

/// The composition this suite drives: defer outside respond, respond directly
/// around the leaf.
type DeferStack = MessageDeferHandler<
    RespondHandler<LeafHandler<ScriptedHandler>, ResultProbeCodec, TestRouter>,
    MemoryMessageDeferStore,
    RequestLoader,
    TraceBasedDecider,
>;

/// The defer layer is the durability boundary in this composition, as it is in
/// a consumer that runs no retry middleware.
impl FallibleEventHandler for DeferStack {}

/// The deferred attempt answers nothing, and the reload answers once with the
/// reloaded record's own request.
#[test]
fn a_deferred_reload_answers_with_the_reloaded_request() -> Result<()> {
    paused()?.block_on(async {
        let fixture = Fixture::<ResultProbeCodec>::new()?;
        let leaf = ScriptedHandler::failing_then_success(vec![ErrorCategory::Transient]);
        let store = MemoryMessageDeferStore::new();
        let handler = defer_handler(&fixture, leaf.clone(), store.clone())?;

        let tracker = offset_tracker();
        let message = requesting(TARGET, REQUEST, KEY)?.into_uncommitted(tracker.take(0).await?);
        EventHandler::on_message(
            &handler,
            MockEventContext::new().with_timer_tracking(),
            message,
            DemandType::Normal,
        )
        .await;

        assert_eq!(
            store.is_deferred(&Key::from(KEY)).await?,
            Some(0),
            "the transient failure is deferred rather than retried in place",
        );
        assert_eq!(
            serialize_count(),
            0,
            "a deferred attempt reaches after_abort, so it sends no response",
        );

        let (timer, ..) = RecordingTimer::new(create_test_trigger_with(
            KEY,
            1000,
            TimerType::DeferredMessage,
        ));
        EventHandler::on_timer(
            &handler,
            MockEventContext::new().with_timer_tracking(),
            timer,
            DemandType::Normal,
        )
        .await;
        drop(handler);

        assert_eq!(leaf.call_count(), 2, "the reload re-ran the leaf");
        let mut drained = fixture.drain().await?;
        assert_eq!(
            drained.len(),
            1,
            "only the reload answers, and it answers once",
        );
        let mut delivery = drained.remove(0);
        let frame = decode_frame(&mut delivery.bytes)?;
        assert_eq!(frame.header.target, peer(TARGET));
        assert_eq!(frame.header.request, RequestId::from_bytes([REQUEST; 16]));
        assert!(matches!(frame.result, FrameResult::Success(_)));
        assert!(
            frame.header.relay.is_none(),
            "a responder never sets the relay"
        );
        Ok(())
    })
}

/// The answer hangs under the reloaded record's own span, not under the timer
/// that carried the reload.
///
/// That distinction is load-bearing rather than cosmetic. A deferred retry
/// fires as a timer, and a timer dispatch starts a trace of its own under the
/// shipped defaults. So a context taken when the answer is sent would put
/// every deferred response outside the requester's trace. The context travels
/// with the request instead. Both values come from the same record.
///
/// The parent edge is read span to span, never against a constant the fixture
/// also seeds the loader from: a comparison of one constant with itself would
/// hold whatever the code under test carried.
#[test]
fn a_deferred_reload_answers_inside_the_records_own_trace() -> Result<()> {
    let mut dispatched: Result<TraceId> = Err(eyre!("the deferred reload never ran"));
    let spans = captured_spans(|| dispatched = reload_under_a_fresh_timer_trace());
    let timer_trace = dispatched?;

    let record = named(&spans, RELOADED)?;
    let sent = named(&spans, SENT)?;
    ensure!(
        record.span_context.trace_id() != timer_trace,
        "the timer must dispatch in a different trace, or this proves nothing"
    );
    ensure!(
        sent.parent_span_id == record.span_context.span_id()
            && sent.span_context.trace_id() == record.span_context.trace_id(),
        "{SENT} hangs under {:?} in {:?}, not under the reloaded record's own span",
        sent.parent_span_id,
        sent.span_context.trace_id()
    );
    Ok(())
}

/// Defers one requesting message, then reloads it from a timer that runs in a
/// trace of its own. Returns that timer's trace id.
fn reload_under_a_fresh_timer_trace() -> Result<TraceId> {
    paused()?.block_on(async {
        let fixture = Fixture::<ResultProbeCodec>::new()?;
        let leaf = ScriptedHandler::failing_then_success(vec![ErrorCategory::Transient]);
        let handler = defer_handler(&fixture, leaf, MemoryMessageDeferStore::new())?;

        let tracker = offset_tracker();
        let message = requesting(TARGET, REQUEST, KEY)?.into_uncommitted(tracker.take(0).await?);
        EventHandler::on_message(
            &handler,
            MockEventContext::new().with_timer_tracking(),
            message,
            DemandType::Normal,
        )
        .await;

        let dispatch = info_span!("peer.test.timer");
        let timer_trace = dispatch.context().span().span_context().trace_id();
        let (timer, ..) = RecordingTimer::new(create_test_trigger_with(
            KEY,
            1000,
            TimerType::DeferredMessage,
        ));
        async {
            EventHandler::on_timer(
                &handler,
                MockEventContext::new().with_timer_tracking(),
                timer,
                DemandType::Normal,
            )
            .await;
        }
        .instrument(dispatch)
        .await;
        drop(handler);

        let drained = fixture.drain().await?;
        ensure!(
            drained.len() == 1,
            "the reload must have answered exactly once, not {} times",
            drained.len()
        );
        Ok(timer_trace)
    })
}

/// The defer layer over the respond layer, with a store the test can read.
fn defer_handler(
    fixture: &Fixture<ResultProbeCodec>,
    leaf: ScriptedHandler,
    store: MemoryMessageDeferStore,
) -> Result<DeferStack> {
    let topic = Topic::from(super::TOPIC);
    let partition = Partition::from(super::PARTITION);
    let telemetry = Telemetry::new();
    Ok(MessageDeferHandler {
        handler: RespondHandler::new(LeafHandler::new(leaf), Arc::clone(&fixture.responder)),
        loader: RequestLoader,
        store,
        decider: TraceBasedDecider::new(),
        config: DeferConfiguration::builder()
            .enabled(true)
            .base(Duration::from_secs(1))
            .max_delay(Duration::from_hours(1))
            .failure_threshold(0.9_f64)
            .build()?,
        topic,
        partition,
        sender: telemetry.partition_sender(topic, partition),
        source: Arc::from("test-group"),
        dedup_version: Arc::from("1"),
    })
}

impl MessageLoader for RequestLoader {
    type Error = RequestLoaderError;
    type Payload = Value;

    fn load_message(
        &self,
        _topic: Topic,
        _partition: Partition,
        _offset: Offset,
    ) -> impl Future<Output = Result<ConsumerRecord<Value>, RequestLoaderError>> {
        // The Kafka loader parents a reloaded record's span on the record's own
        // propagated context, so a reload rejoins the trace the request began
        // in. This double does the same, from one fixed remote context.
        let load = related_span!(SpanRelation::Child, sampled_remote_context(), "load");
        ready(
            requesting_under(TARGET, REQUEST, KEY, load)
                .map(ConsumerRecord::Message)
                .map_err(|_| RequestLoaderError::Unavailable),
        )
    }

    async fn try_load_message(
        &self,
        topic: Topic,
        partition: Partition,
        offset: Offset,
    ) -> Result<ConsumerRecord<Value>, RequestLoaderError> {
        self.load_message(topic, partition, offset).await
    }
}

impl ClassifyError for RequestLoaderError {
    fn classify_error(&self) -> ErrorCategory {
        ErrorCategory::Transient
    }
}

/// Why the requesting loader could not rebuild a record.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
enum RequestLoaderError {
    /// The harness could not build the record.
    #[error("the requesting loader could not rebuild the record")]
    Unavailable,
}
