//! Responding consumer wiring from termination through shutdown.

use super::{
    Event, EventLog, RecordingDirectory, common_config, consumer_config, peer_config,
    recording_memory_deps,
};
use crate::JsonCodec;
use crate::codec::Codec;
use crate::consumer::middleware::defer::DeferConfiguration;
use crate::consumer::middleware::monopolization::MonopolizationConfiguration;
use crate::consumer::middleware::retry::RetryConfiguration;
use crate::consumer::middleware::tests::test_support::ScriptedHandler;
use crate::consumer::wiring::memory_deps;
use crate::consumer::{
    ConsumerSetup, PipelineMiddlewareConfiguration, ProsodyConsumer, Responding, TypedConsumerSetup,
};
use crate::high_level::config::TriggerStoreConfiguration;
use crate::peer::Router;
use crate::peer::runtime::prepare_router;
use crate::subsystem::SubsystemName;
use crate::telemetry::Telemetry;
use color_eyre::Result;
use color_eyre::eyre::ensure;
use parking_lot::Mutex;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use thiserror::Error;

const SUBSYSTEM: &str = "billing";

#[derive(Default)]
struct SomeResponseCodec;

impl Codec for SomeResponseCodec {
    type Error = ResponseCodecError;
    type Payload = ();

    const FORMAT_ID: &'static str = "wiring-result";

    fn deserialize(&mut self, buf: &mut [u8]) -> Result<Self::Payload, Self::Error> {
        buf.first()
            .copied()
            .filter(|byte| *byte == 0)
            .map(|_| ())
            .ok_or(ResponseCodecError)
    }

    fn deserialize_owned(
        &mut self,
        mut buf: bytes::BytesMut,
    ) -> Result<Self::Payload, Self::Error> {
        self.deserialize(&mut buf)
    }

    fn serialize(&mut self, (): Self::Payload, buf: &mut Vec<u8>) -> Result<(), Self::Error> {
        buf.push(0);
        Ok(())
    }

    fn serialize_ref(
        &mut self,
        _payload: &Self::Payload,
        buf: &mut Vec<u8>,
    ) -> Result<(), Self::Error> {
        buf.push(0);
        Ok(())
    }
}

/// One pipeline test proves the shared constructor path.
#[tokio::test(flavor = "multi_thread")]
async fn an_explicit_response_subsystem_needs_no_keyed_state_subsystem() -> Result<()> {
    let log: EventLog = Arc::new(Mutex::new(Vec::new()));
    let directory = RecordingDirectory::new(Arc::clone(&log), false);
    let consumer_config = consumer_config("responding-wiring-start")?;
    let peer = peer_config(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))?;
    let common = common_config(None)?;
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
    let router = prepare_router(&peer, typed.deps.backend().as_ref()).await?;
    let consumer =
        ProsodyConsumer::<JsonCodec>::pipeline_consumer_with_policy::<ScriptedHandler, _, _>(
            typed,
            pipeline_config()?,
            Telemetry::new(),
            ScriptedHandler::success(),
            Responding::<SomeResponseCodec, _>::new(&router, SubsystemName::try_new(SUBSYSTEM)?),
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
    consumer.shutdown().await;
    router.shutdown().await?;
    outcome
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
