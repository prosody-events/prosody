//! The request tag survives a deferred retry.
//!
//! A deferred attempt reaches `after_abort`, so it answers nothing. The reload
//! arrives as a message dispatch inside the defer layer's timer handling, and
//! it answers with the reloaded record's own tag. That is why the defer round
//! trip has to carry the tag: the answer follows the message, not the attempt.

use super::super::RespondHandler;
use super::{Drained, Fixture, ResultProbeCodec, cap, offset_tracker, tagged};
use crate::consumer::message::ConsumerMessage;
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
use crate::response::frame::decode::decode_frame;
use crate::response::{RequestId, ResponseStatus};
use crate::router::loopback::{node, paused};
use crate::telemetry::Telemetry;
use crate::timers::TimerType;
use crate::{Key, Offset, Partition, Topic};
use color_eyre::Result;
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;

/// The key both the deferred message and its retry timer carry.
const KEY: &str = "deferred";

/// The node the deferred record asks for its answer.
const TARGET: u8 = 3;

/// The request the deferred record names.
const REQUEST: u8 = 44;

/// A loader whose reloaded record still asks for a response.
///
/// The shared in-memory loader cannot serve here: every record it rebuilds
/// carries no request tag, so it could not tell a lost tag from a suppressed
/// answer.
#[derive(Clone)]
struct TaggedLoader;

/// The composition this suite drives: defer outside respond, respond directly
/// around the leaf.
type DeferStack = MessageDeferHandler<
    RespondHandler<LeafHandler<ScriptedHandler>, ResultProbeCodec>,
    MemoryMessageDeferStore,
    TaggedLoader,
    TraceBasedDecider,
>;

/// The defer layer is the durability boundary in this composition, as it is in
/// a consumer that runs no retry middleware.
impl FallibleEventHandler for DeferStack {}

/// The deferred attempt answers nothing, and the reload answers once with the
/// reloaded record's own tag.
#[test]
fn a_deferred_reload_answers_with_the_reloaded_tag() -> Result<()> {
    paused()?.block_on(async {
        let fixture = Fixture::<ResultProbeCodec>::new(1, 2)?;
        let leaf = ScriptedHandler::failing_then_success(vec![ErrorCategory::Transient]);
        let store = MemoryMessageDeferStore::new();
        let handler = defer_handler(&fixture, leaf.clone(), store.clone())?;

        let tracker = offset_tracker();
        let message = tagged(TARGET, REQUEST, KEY)?.into_uncommitted(tracker.take(0).await?);
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
            fixture.admitted(),
            0,
            "a deferred attempt reaches after_abort, so it reserves no send slot",
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
        let mut drained: Drained = fixture.drain().await?;
        assert_eq!(
            drained.deliveries.len(),
            1,
            "only the reload answers, and it answers once",
        );
        let mut delivery = drained.deliveries.remove(0);
        let frame = decode_frame(&mut delivery.bytes, cap()?)?;
        assert_eq!(frame.header.target, node(TARGET));
        assert_eq!(frame.header.request, RequestId::from_bytes([REQUEST; 16]));
        assert_eq!(frame.header.status, ResponseStatus::Success);
        Ok(())
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
        loader: TaggedLoader,
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

impl MessageLoader for TaggedLoader {
    type Error = TaggedLoaderError;
    type Payload = Value;

    async fn load_message(
        &self,
        _topic: Topic,
        _partition: Partition,
        _offset: Offset,
    ) -> Result<ConsumerMessage<Value>, TaggedLoaderError> {
        tagged(TARGET, REQUEST, KEY).map_err(|_| TaggedLoaderError::Unavailable)
    }

    async fn try_load_message(
        &self,
        topic: Topic,
        partition: Partition,
        offset: Offset,
    ) -> Result<ConsumerMessage<Value>, TaggedLoaderError> {
        self.load_message(topic, partition, offset).await
    }
}

impl ClassifyError for TaggedLoaderError {
    fn classify_error(&self) -> ErrorCategory {
        ErrorCategory::Transient
    }
}

/// Why the tagged loader could not rebuild a record.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
enum TaggedLoaderError {
    /// The harness could not build the record.
    #[error("the tagged loader could not rebuild the record")]
    Unavailable,
}
