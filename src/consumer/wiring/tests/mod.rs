//! Scaffolding for the peer lifecycle tests: a recording directory, a
//! recording handler provider, and one `initialize_consumer` runner.

use super::peer::PeerAttachment;
use super::runtime::{StartupServices, initialize_consumer};
use super::state::{KeyedStateInputs, memory_state_provider};
use crate::consumer::event_context::EventContext;
use crate::consumer::handler::{DemandType, EventHandler, HandlerProvider, Uncommitted};
use crate::consumer::kafka_context::PartitionProviders;
use crate::consumer::message::UncommittedMessage;
use crate::consumer::middleware::CloneProvider;
use crate::consumer::middleware::deduplication::{
    DEFAULT_IDEMPOTENCE_VERSION, MemoryDeduplicationStoreProvider,
};
use crate::consumer::{
    ConsumerConfiguration, ConsumerError, KafkaObserver, Managers, ProsodyConsumer,
};
use crate::heartbeat::HeartbeatRegistry;
use crate::loader::MemoryLoader;
use crate::router::NodeId;
use crate::router::directory::memory::{MEMORY_DIRECTORY_CAPACITY, MemoryNodeDirectory};
use crate::router::directory::{NodeDirectory, NodeRegistration, RegistrationTtl};
use crate::state::config::KeyedStateConfiguration;
use crate::state::memory::{MemoryCells, MemoryDescriptorIdentityStore};
use crate::state_reader::PeerDirectoryBackend;
use crate::telemetry::Telemetry;
use crate::timers::UncommittedTimer;
use crate::timers::store::memory::InMemoryTriggerStoreProvider;
use crate::{JsonCodec, Partition, PeerConfiguration, Topic};
use color_eyre::Result;
use parking_lot::Mutex;
use serde_json::Value;
use std::net::{Ipv4Addr, SocketAddr, TcpListener};
use std::sync::Arc;
use thiserror::Error;

mod lifecycle;

const TOPIC: &str = "peer-lifecycle";

type EventLog = Arc<Mutex<Vec<Event>>>;

#[derive(Clone, Debug, Eq, PartialEq)]
enum Event {
    Registered { port: u16, port_held: bool },
    RegisterFailed { port: u16, port_held: bool },
    Deregistered,
    ProviderDropped,
}

#[derive(Clone)]
struct RecordingDirectory {
    log: EventLog,
    inner: MemoryNodeDirectory,
    fail_register: bool,
}

#[derive(Clone)]
struct RecordingBackend {
    directory: RecordingDirectory,
}

struct RecordingProvider {
    inner: CloneProvider<SilentHandler>,
    log: EventLog,
}

#[derive(Clone)]
struct SilentHandler;

impl RecordingDirectory {
    fn new(log: EventLog, fail_register: bool) -> Self {
        Self {
            log,
            inner: MemoryNodeDirectory::new(MEMORY_DIRECTORY_CAPACITY, RegistrationTtl::DEFAULT),
            fail_register,
        }
    }
}

impl NodeDirectory for RecordingDirectory {
    type Error = RecordingError;

    fn ttl(&self) -> RegistrationTtl {
        self.inner.ttl()
    }

    async fn register(&self, registration: &NodeRegistration) -> Result<(), Self::Error> {
        match self.inner.register(registration).await {
            Ok(()) => {}
            Err(error) => match error {},
        }
        let port = registration.direct.port;
        let port_held = TcpListener::bind((Ipv4Addr::LOCALHOST, port)).is_err();
        let event = if self.fail_register {
            Event::RegisterFailed { port, port_held }
        } else {
            Event::Registered { port, port_held }
        };
        self.log.lock().push(event);
        if self.fail_register {
            Err(RecordingError)
        } else {
            Ok(())
        }
    }

    async fn read(&self, node: NodeId) -> Result<Option<NodeRegistration>, Self::Error> {
        match self.inner.read(node).await {
            Ok(registration) => Ok(registration),
            Err(error) => match error {},
        }
    }

    async fn deregister(&self, registration: &NodeRegistration) -> Result<(), Self::Error> {
        match self.inner.deregister(registration).await {
            Ok(()) => {}
            Err(error) => match error {},
        }
        self.log.lock().push(Event::Deregistered);
        Ok(())
    }
}

impl PeerDirectoryBackend for RecordingBackend {
    type Directory = RecordingDirectory;

    async fn node_directory(
        &self,
        _lease: RegistrationTtl,
    ) -> Result<Self::Directory, ConsumerError> {
        Ok(self.directory.clone())
    }
}

impl RecordingProvider {
    fn new(log: EventLog) -> Self {
        Self {
            inner: CloneProvider::new(SilentHandler),
            log,
        }
    }
}

impl HandlerProvider for RecordingProvider {
    type Handler = SilentHandler;

    fn handler_for_partition(&self, topic: Topic, partition: Partition) -> Self::Handler {
        self.inner.handler_for_partition(topic, partition)
    }
}

impl Drop for RecordingProvider {
    fn drop(&mut self) {
        self.log.lock().push(Event::ProviderDropped);
    }
}

impl EventHandler for SilentHandler {
    type Payload = Value;

    async fn on_message<C>(
        &self,
        _context: C,
        message: UncommittedMessage<Value>,
        _demand_type: DemandType,
    ) where
        C: EventContext<Payload = Self::Payload>,
    {
        let (_, uncommitted) = message.into_inner();
        uncommitted.commit().await;
    }

    async fn on_timer<C, T>(&self, _context: C, timer: T, _demand_type: DemandType)
    where
        C: EventContext<Payload = Self::Payload>,
        T: UncommittedTimer,
    {
        timer.commit().await;
    }

    async fn shutdown(self) {}
}

fn peer_config(bind: SocketAddr) -> Result<PeerConfiguration> {
    Ok(PeerConfiguration::builder()
        .bind(bind)
        .reflection(false)
        .build()?)
}

fn consumer_config(group: &str) -> Result<ConsumerConfiguration> {
    Ok(ConsumerConfiguration::builder()
        .bootstrap_servers(vec!["unused-in-mock-mode:9092".to_owned()])
        .group_id(group)
        .subscribed_topics(vec![TOPIC.to_owned()])
        .mock(true)
        .probe_port(None)
        .build()?)
}

async fn start<A: PeerAttachment + 'static>(
    config: &ConsumerConfiguration,
    managers: Arc<Managers<Value>>,
    heartbeats: HeartbeatRegistry,
    log: EventLog,
    peer: A,
) -> Result<ProsodyConsumer<JsonCodec>, ConsumerError> {
    let telemetry = Telemetry::new();
    let keyed_state = KeyedStateInputs::new(
        KeyedStateConfiguration::builder().build()?,
        config,
        DEFAULT_IDEMPOTENCE_VERSION,
    )?;
    let state = memory_state_provider::<JsonCodec>(
        &keyed_state,
        MemoryDeduplicationStoreProvider::new(),
        MemoryCells::new(),
        MemoryDescriptorIdentityStore::new(),
        MemoryLoader::new(),
        None,
    );
    Box::pin(initialize_consumer::<_, _, _, JsonCodec, _>(
        config,
        RecordingProvider::new(log),
        PartitionProviders {
            triggers: InMemoryTriggerStoreProvider::new(),
            state,
        },
        StartupServices {
            version: keyed_state.version.clone(),
            telemetry: &telemetry,
            heartbeats,
            observer: KafkaObserver::new(&config.group_id),
            managers,
            responder: None,
        },
        peer,
    ))
    .await
}

#[derive(Debug, Error)]
#[error("scripted registration failure")]
struct RecordingError;
