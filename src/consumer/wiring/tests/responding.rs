//! Responding consumer wiring from termination through shutdown.

use super::{
    Event, EventLog, RecordingBackend, RecordingDirectory, bounded, common_config, consumer_config,
    peer_config, recording_memory_deps,
};
use crate::codec::Codec;
use crate::consumer::error::{ConsumerError, PeerInitError};
use crate::consumer::message::{ConsumerMessage, ConsumerMessageValue};
use crate::consumer::middleware::HandlerMiddleware;
use crate::consumer::middleware::deduplication::MemoryDeduplicationStoreProvider;
use crate::consumer::middleware::defer::DeferConfiguration;
use crate::consumer::middleware::log::LogMiddleware;
use crate::consumer::middleware::monopolization::MonopolizationConfiguration;
use crate::consumer::middleware::respond::Responder;
use crate::consumer::middleware::retry::RetryConfiguration;
use crate::consumer::middleware::tests::test_support::{
    MockEventContext, ScriptedHandler, TestError,
};
use crate::consumer::partition::offsets::OffsetTracker;
use crate::consumer::wiring::build_common_middleware;
use crate::consumer::wiring::memory_deps;
use crate::consumer::wiring::peer::{
    PeerAttachment, PreparedResponder, prepare_requester, prepare_responding,
};
use crate::consumer::{
    ConsumerSetup, DemandType, EventHandler, HandlerProvider, Managers,
    PipelineMiddlewareConfiguration, ProsodyConsumer, TypedConsumerSetup,
};
use crate::error::ErrorCategory;
use crate::heartbeat::HeartbeatRegistry;
use crate::high_level::config::TriggerStoreConfiguration;
use crate::response::frame::FrameCap;
use crate::response::frame::decode::decode_frame;
use crate::response::headers::RequestTag;
use crate::response::{RequestId, ResponseStatus};
use crate::router::loopback::{TestRouter, collect_deliveries, config, node};
use crate::state_reader::StateReaderDependencies;
use crate::subsystem::SubsystemName;
use crate::telemetry::Telemetry;
use crate::{JsonCodec, Partition, PeerConfiguration, Topic};
use color_eyre::Result;
use color_eyre::eyre::{bail, ensure, eyre};
use crossbeam_utils::CachePadded;
use parking_lot::Mutex;
use serde_json::Value;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::Semaphore;
use tracing::Span;

const RESPONSE_CAP_BYTES: usize = 4096;

/// Destinations enough that one encode scratch each, at the widest ceiling a
/// frame may carry, is over the budget one sender may commit to.
const SCRATCH_DESTINATIONS: usize = 8;

const SUBSYSTEM: &str = "billing";
const TOPIC: &str = "responding-wiring";
const PARTITION: Partition = 0;

#[derive(Default)]
struct SomeResponseCodec;

impl Codec for SomeResponseCodec {
    type Error = ResponseCodecError;
    type Payload = Result<(), TestError>;

    const FORMAT_ID: &'static str = "wiring-result";

    fn deserialize(&mut self, buf: &mut [u8]) -> Result<Self::Payload, Self::Error> {
        match buf.first() {
            Some(0) => Ok(Ok(())),
            Some(_) => Ok(Err(TestError(ErrorCategory::Permanent))),
            None => Err(ResponseCodecError),
        }
    }

    fn serialize(&mut self, payload: Self::Payload, buf: &mut Vec<u8>) -> Result<(), Self::Error> {
        buf.push(u8::from(payload.is_err()));
        Ok(())
    }
}

/// Production termination captures a request tag and delivers one response.
///
/// The attachment also reports the name the decode path admits. That name is
/// read from the responder, so it is the one the delivered frame claims.
#[tokio::test]
async fn the_responding_wiring_answers_a_tagged_message() -> Result<()> {
    let log: EventLog = Arc::new(Mutex::new(Vec::new()));
    let directory = RecordingDirectory::new(Arc::clone(&log), false);
    let backend = RecordingBackend {
        directory: directory.clone(),
    };
    let consumer = consumer_config("responding-wiring-termination")?;
    let peer_config = peer_config(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))?;
    let managers: Arc<Managers<Value>> = Arc::default();
    let heartbeats = HeartbeatRegistry::new(consumer.group_id.clone(), consumer.stall_threshold);
    let peer = prepare_requester(&peer_config, &backend, true, managers, &heartbeats).await?;
    let (router, mut deliveries) = TestRouter::new(config(1, 1))?;
    let (responder, workers) = Responder::<SomeResponseCodec>::new_without_local(
        &router,
        FrameCap::new(RESPONSE_CAP_BYTES)?,
        SubsystemName::try_new(SUBSYSTEM)?,
    )?;
    let prepared = PreparedResponder::from_parts(peer, responder, workers);
    let common = common_config(None, Some(SubsystemName::try_new(SUBSYSTEM)?))?;
    let middleware = build_common_middleware::<_, Value>(
        &common,
        &consumer,
        Telemetry::new(),
        MemoryDeduplicationStoreProvider::new(),
    )?
    .layer(LogMiddleware::new());
    let (provider, peer) = prepared.terminate(&middleware, ScriptedHandler::success());
    let admitted = peer.responder();
    let handler = provider.handler_for_partition(Topic::from(TOPIC), PARTITION);
    let tracker = OffsetTracker::new(
        Topic::from(TOPIC),
        PARTITION,
        10,
        Duration::from_secs(5),
        Arc::new(CachePadded::new(AtomicUsize::new(0))),
    );
    let semaphore = Arc::new(Semaphore::new(1));
    let message = ConsumerMessage::new(
        ConsumerMessageValue {
            key: "request-key".into(),
            topic: Topic::from(TOPIC),
            partition: PARTITION,
            request: Some(RequestTag::new(RequestId::from_bytes([9; 16]), node(1))),
            ..Default::default()
        },
        Span::current(),
        semaphore.try_acquire_owned()?,
    )
    .into_uncommitted(tracker.take(0).await?);
    EventHandler::on_message(
        &handler,
        MockEventContext::new(),
        message,
        DemandType::Normal,
    )
    .await;
    drop(handler);
    drop(provider);
    bounded("prepared peer abandonment", peer.abandon()).await?;
    drop(router);

    let mut recorded = collect_deliveries(&mut deliveries).await;
    ensure!(
        recorded.len() == 1,
        "the transport did not receive one response"
    );
    let mut delivery = recorded.remove(0);
    let frame = decode_frame(&mut delivery.bytes, FrameCap::new(RESPONSE_CAP_BYTES)?)?;
    assert_eq!(frame.header.request, RequestId::from_bytes([9; 16]));
    assert_eq!(frame.header.subsystem, SubsystemName::try_new(SUBSYSTEM)?);
    assert_eq!(frame.header.status, ResponseStatus::Success);
    ensure!(
        admitted.as_ref() == Some(&frame.header.subsystem),
        "the consumer admits a request under a name its own answer does not claim"
    );
    Ok(())
}

/// Preparation carries the responder's own name out to startup.
///
/// The decode path admits a request tag for the name the attachment reports,
/// and preparation is the only production step that fills it. A consumer whose
/// attachment reports nothing parses no tag and answers nothing.
#[tokio::test]
async fn the_prepared_peer_admits_the_name_its_responder_answers_with() -> Result<()> {
    let log: EventLog = Arc::new(Mutex::new(Vec::new()));
    let backend = RecordingBackend {
        directory: RecordingDirectory::new(log, false),
    };
    let consumer = consumer_config("responding-wiring-admits")?;
    let peer_config = peer_config(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))?;
    let subsystem = SubsystemName::try_new(SUBSYSTEM)?;
    let managers: Arc<Managers<Value>> = Arc::default();
    let heartbeats = HeartbeatRegistry::new(consumer.group_id.clone(), consumer.stall_threshold);
    let prepared = prepare_responding::<SomeResponseCodec, _, _>(
        &peer_config,
        &backend,
        true,
        subsystem.clone(),
        managers,
        &heartbeats,
    )
    .await?;
    let common = common_config(None, Some(subsystem.clone()))?;
    let middleware = build_common_middleware::<_, Value>(
        &common,
        &consumer,
        Telemetry::new(),
        MemoryDeduplicationStoreProvider::new(),
    )?
    .layer(LogMiddleware::new());
    let (provider, peer) = prepared.terminate(&middleware, ScriptedHandler::success());
    let admitted = peer.responder();
    // The provider holds the last responder clone, and abandonment joins the
    // workers that clone keeps open.
    drop(provider);
    bounded("prepared peer abandonment", peer.abandon()).await?;
    ensure!(
        admitted == Some(subsystem),
        "the prepared peer admits {admitted:?}, not the name its responder answers with"
    );
    Ok(())
}

/// The responder is sized by the runtime's own frame ceiling.
///
/// A sender commits to one encode scratch per destination, at the ceiling it
/// frames against. This peer names a ceiling and a destination table whose
/// product is over that budget. So preparation refuses, and the refusal states
/// that product. Only the ceiling the prepared runtime carries gives the number
/// this test reads.
#[tokio::test]
async fn the_prepared_responder_is_sized_by_the_runtimes_frame_cap() -> Result<()> {
    let log: EventLog = Arc::new(Mutex::new(Vec::new()));
    let backend = RecordingBackend {
        directory: RecordingDirectory::new(log, false),
    };
    let consumer = consumer_config("responding-wiring-scratch")?;
    // One connection of one stream keeps the widest ceiling inside the
    // listener's receive budget, so the scratch budget is the one rule this
    // configuration breaks.
    let peer = PeerConfiguration::builder()
        .bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .reflection(false)
        .frame_bytes(FrameCap::MAX_BYTES)
        .max_connections(1_usize)
        .max_concurrent_streams(1_u32)
        .max_destinations(SCRATCH_DESTINATIONS)
        .build()?;
    let managers: Arc<Managers<Value>> = Arc::default();
    let heartbeats = HeartbeatRegistry::new(consumer.group_id.clone(), consumer.stall_threshold);
    let prepared = prepare_responding::<SomeResponseCodec, _, _>(
        &peer,
        &backend,
        true,
        SubsystemName::try_new(SUBSYSTEM)?,
        managers,
        &heartbeats,
    )
    .await;
    match prepared {
        Err(ConsumerError::Peer(PeerInitError::Fleet { message })) => {
            let asked = SCRATCH_DESTINATIONS * FrameCap::MAX_BYTES;
            ensure!(
                message.contains(&asked.to_string()),
                "the responder asks for {asked} bytes of scratch: {message}",
            );
            Ok(())
        }
        Err(error) => bail!("preparation failed for another reason: {error}"),
        Ok(_) => bail!("preparation took a scratch budget one sender cannot commit to"),
    }
}

/// One pipeline test proves the shared constructor path.
///
/// The other mode tails differ only in their concrete outer middleware. Each
/// tail takes its provider from `PreparedResponder::terminate`, and that
/// return type puts the responder inside every layer a mode adds.
#[tokio::test(flavor = "multi_thread")]
async fn a_responding_consumer_starts_and_stops() -> Result<()> {
    let log: EventLog = Arc::new(Mutex::new(Vec::new()));
    let directory = RecordingDirectory::new(Arc::clone(&log), false);
    let consumer_config = consumer_config("responding-wiring-start")?;
    let peer = peer_config(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))?;
    let common = common_config(Some(peer), Some(SubsystemName::try_new(SUBSYSTEM)?))?;
    let trigger_store = TriggerStoreConfiguration::InMemory;
    let setup = ConsumerSetup {
        consumer: &consumer_config,
        trigger_store: &trigger_store,
        common: &common,
    };
    let memory = memory_deps(&setup);
    let deps = recording_memory_deps(&memory, directory);
    let typed = TypedConsumerSetup {
        consumer: &consumer_config,
        common: &common,
        deps,
    };
    let consumer = Box::pin(bounded(
        "responding consumer startup",
        ProsodyConsumer::<JsonCodec>::pipeline_responding_consumer_with_backend::<
            ScriptedHandler,
            SomeResponseCodec,
            _,
        >(
            typed,
            pipeline_config()?,
            Telemetry::new(),
            ScriptedHandler::success(),
        ),
    ))
    .await??;
    let outcome: Result<()> = async {
        ensure!(
            log.lock()
                .iter()
                .any(|event| matches!(event, Event::Registered { .. })),
            "startup did not register the peer"
        );
        Ok(())
    }
    .await;
    bounded("responding consumer shutdown", consumer.shutdown()).await??;
    outcome
}

#[tokio::test]
async fn a_responding_consumer_without_peer_configuration_is_refused() -> Result<()> {
    let error = refused_consumer(None, Some(SubsystemName::try_new(SUBSYSTEM)?))
        .await?
        .ok_or_else(|| eyre!("the responding consumer started without peer configuration"))?;
    assert!(matches!(
        error,
        ConsumerError::Peer(PeerInitError::PeerRequired)
    ));
    Ok(())
}

#[tokio::test]
async fn a_responding_consumer_without_a_subsystem_is_refused() -> Result<()> {
    let peer = peer_config(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))?;
    let error = refused_consumer(Some(peer), None)
        .await?
        .ok_or_else(|| eyre!("the responding consumer started without a subsystem"))?;
    assert!(matches!(
        error,
        ConsumerError::Peer(PeerInitError::SubsystemRequired)
    ));
    Ok(())
}

async fn refused_consumer(
    peer: Option<PeerConfiguration>,
    subsystem: Option<SubsystemName>,
) -> Result<Option<ConsumerError>> {
    let consumer_config = consumer_config("responding-wiring-refused")?;
    let common = common_config(peer, subsystem)?;
    let trigger_store = TriggerStoreConfiguration::InMemory;
    let setup = ConsumerSetup {
        consumer: &consumer_config,
        trigger_store: &trigger_store,
        common: &common,
    };
    let deps: StateReaderDependencies<JsonCodec, _> = memory_deps(&setup);
    let result = ProsodyConsumer::<JsonCodec>::pipeline_responding_consumer_with_backend::<
        ScriptedHandler,
        SomeResponseCodec,
        _,
    >(
        TypedConsumerSetup {
            consumer: &consumer_config,
            common: &common,
            deps,
        },
        pipeline_config()?,
        Telemetry::new(),
        ScriptedHandler::success(),
    )
    .await;
    match result {
        Ok(consumer) => {
            let shutdown = consumer.shutdown().await;
            if let Err(error) = shutdown {
                bail!("unexpected consumer shutdown failed: {error}");
            }
            Ok(None)
        }
        Err(error) => Ok(Some(error)),
    }
}

fn pipeline_config() -> Result<PipelineMiddlewareConfiguration> {
    Ok(PipelineMiddlewareConfiguration {
        retry: RetryConfiguration::builder().build()?,
        monopolization: MonopolizationConfiguration::builder().build()?,
        defer: DeferConfiguration::builder().build()?,
    })
}

#[derive(Debug, Error)]
#[error("the response payload is empty")]
struct ResponseCodecError;
