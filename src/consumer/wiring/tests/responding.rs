//! Responding consumer wiring from termination through shutdown.

use super::{
    Event, EventLog, RecordingBackend, RecordingDirectory, common_config, consumer_config,
    peer_config, recording_memory_deps,
};
use crate::codec::Codec;
use crate::consumer::error::{ConsumerError, PeerInitError};
use crate::consumer::middleware::HandlerMiddleware;
use crate::consumer::middleware::deduplication::MemoryDeduplicationStoreProvider;
use crate::consumer::middleware::defer::DeferConfiguration;
use crate::consumer::middleware::log::LogMiddleware;
use crate::consumer::middleware::monopolization::MonopolizationConfiguration;
use crate::consumer::middleware::retry::RetryConfiguration;
use crate::consumer::middleware::tests::test_support::{ScriptedHandler, TestError};
use crate::consumer::wiring::build_common_middleware;
use crate::consumer::wiring::memory_deps;
use crate::consumer::{
    ConsumerSetup, PipelineMiddlewareConfiguration, ProsodyConsumer, TypedConsumerSetup,
};
use crate::error::ErrorCategory;
use crate::high_level::config::TriggerStoreConfiguration;
use crate::peer::{PeerAttachment, prepare_responding};
use crate::response::frame::FrameCap;
use crate::state_reader::StateReaderDependencies;
use crate::subsystem::SubsystemName;
use crate::telemetry::Telemetry;
use crate::{JsonCodec, PeerConfiguration};
use color_eyre::Result;
use color_eyre::eyre::{bail, ensure, eyre};
use parking_lot::Mutex;
use serde_json::Value;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use thiserror::Error;

/// Destinations enough that one encode scratch each, at the widest ceiling a
/// frame may carry, is over the budget one sender may commit to.
const SCRATCH_DESTINATIONS: usize = 8;

const SUBSYSTEM: &str = "billing";

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
    let prepared =
        prepare_responding::<SomeResponseCodec, _>(&peer_config, &backend, true, subsystem.clone())
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
    peer.abandon().await;
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
    let prepared = prepare_responding::<SomeResponseCodec, _>(
        &peer,
        &backend,
        true,
        SubsystemName::try_new(SUBSYSTEM)?,
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
    let consumer = ProsodyConsumer::<JsonCodec>::pipeline_responding_consumer_with_backend::<
        ScriptedHandler,
        SomeResponseCodec,
        _,
    >(
        typed,
        pipeline_config()?,
        Telemetry::new(),
        ScriptedHandler::success(),
    )
    .await?;
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
    consumer.shutdown().await?;
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
