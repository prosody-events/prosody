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
    DEFAULT_IDEMPOTENCE_VERSION, DeduplicationConfiguration, MemoryDeduplicationStoreProvider,
};
use crate::consumer::middleware::scheduler::SchedulerConfiguration;
use crate::consumer::middleware::tests::test_support::SilentHandler;
use crate::consumer::middleware::timeout::TimeoutConfiguration;
use crate::consumer::partition::{PartitionConfiguration, PartitionManager};
use crate::consumer::storage::{ComponentsOf, ConsumerStorageBackend, ConsumerStorageInputs};
use crate::consumer::{
    CommonConfiguration, ConsumerConfiguration, ConsumerError, KafkaObserver, Managers,
    ProsodyConsumer,
};
use crate::heartbeat::HeartbeatRegistry;
use crate::loader::MemoryLoader;
use crate::otel::SpanRelation;
use crate::router::NodeId;
use crate::router::directory::memory::{MEMORY_DIRECTORY_CAPACITY, MemoryNodeDirectory};
use crate::router::directory::{NodeDirectory, NodeRegistration, RegistrationTtl};
use crate::state::config::KeyedStateConfiguration;
use crate::state::memory::{MemoryCells, MemoryDescriptorIdentityStore};
use crate::state_reader::PeerDirectoryBackend;
use crate::state_reader::{MemoryReaderBackend, ReaderBackend, StateReaderDependencies};
use crate::subsystem::SubsystemName;
use crate::telemetry::Telemetry;
use crate::timers::UncommittedTimer;
use crate::timers::duration::CompactDuration;
use crate::timers::store::memory::InMemoryTriggerStoreProvider;
use crate::{JsonCodec, Partition, PeerConfiguration, Topic};
use color_eyre::Result;
use color_eyre::eyre::eyre;
use crossbeam_utils::CachePadded;
use parking_lot::Mutex;
use serde_json::Value;
use std::array::from_fn;
use std::future::Future;
use std::marker::PhantomData;
use std::net::{Ipv4Addr, SocketAddr, TcpListener};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::Semaphore;
use tokio::time::timeout;

mod lifecycle;
mod responding;
mod selection;

const TOPIC: &str = "peer-lifecycle";

/// The topic of the manager [`retain_manager`] leaves in the shared map. No
/// rebalance names it, so only the shutdown sweep can end it.
const RETAINED_TOPIC: &str = "peer-lifecycle-retained";

/// The partition of that manager.
const RETAINED_PARTITION: Partition = 0;

/// The deadline for each consumer lifecycle step these tests await.
///
/// A consumer that starts or stops finishes each step at once. So this deadline
/// is a hang-guard and never the assertion. Without it, a step that never
/// finishes reports nothing: the poll loop only ends when the shutdown flag is
/// set, and an await on its join handle has no other detector.
///
/// The deadline names the failure; it cannot end the process. The poll loop
/// runs on a blocking task, and a runtime drop waits for one, so a defect that
/// makes the loop immortal hangs the test binary after this deadline already
/// failed the test. The `slow-timeout` backstop in `.config/nextest.toml` is
/// what bounds that.
const HANG_GUARD: Duration = Duration::from_secs(30);

type EventLog = Arc<Mutex<Vec<Event>>>;

#[derive(Clone, Debug, Eq, PartialEq)]
enum Event {
    Registered { port: u16, port_held: bool },
    RegisterFailed { port: u16, port_held: bool },
    Deregistered,
    ProviderDropped,
    ManagerSwept,
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

#[derive(Clone)]
struct RecordingMemoryBackend {
    inner: Arc<MemoryReaderBackend<JsonCodec>>,
    directory: RecordingDirectory,
}

struct RecordingProvider {
    inner: CloneProvider<SilentHandler>,
    log: EventLog,
}

/// The handler of the retained partition manager. It records the one moment
/// the shutdown sweep ends that manager.
#[derive(Clone)]
struct SweptHandler {
    log: EventLog,
}

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

impl ReaderBackend<JsonCodec> for RecordingMemoryBackend {
    type Cells = <MemoryReaderBackend<JsonCodec> as ReaderBackend<JsonCodec>>::Cells;
    type Identities = <MemoryReaderBackend<JsonCodec> as ReaderBackend<JsonCodec>>::Identities;
    type Loader = <MemoryReaderBackend<JsonCodec> as ReaderBackend<JsonCodec>>::Loader;
    type Publications = <MemoryReaderBackend<JsonCodec> as ReaderBackend<JsonCodec>>::Publications;

    fn cells(&self) -> &Self::Cells {
        self.inner.cells()
    }

    fn publications(&self) -> &Self::Publications {
        self.inner.publications()
    }

    fn identities(&self) -> &Self::Identities {
        self.inner.identities()
    }

    fn loader(&self) -> &Self::Loader {
        self.inner.loader()
    }
}

impl ConsumerStorageBackend<JsonCodec> for RecordingMemoryBackend {
    type Dedup = <MemoryReaderBackend<JsonCodec> as ConsumerStorageBackend<JsonCodec>>::Dedup;
    type EventLoader =
        <MemoryReaderBackend<JsonCodec> as ConsumerStorageBackend<JsonCodec>>::EventLoader;
    type Messages = <MemoryReaderBackend<JsonCodec> as ConsumerStorageBackend<JsonCodec>>::Messages;
    type State = <MemoryReaderBackend<JsonCodec> as ConsumerStorageBackend<JsonCodec>>::State;
    type Timers = <MemoryReaderBackend<JsonCodec> as ConsumerStorageBackend<JsonCodec>>::Timers;
    type Trigger = <MemoryReaderBackend<JsonCodec> as ConsumerStorageBackend<JsonCodec>>::Trigger;

    fn build_consumer_components(
        &self,
        inputs: ConsumerStorageInputs,
        keyed_state: &KeyedStateInputs,
        observer: KafkaObserver,
    ) -> impl Future<Output = Result<ComponentsOf<JsonCodec, Self>, ConsumerError>> + Send {
        self.inner
            .build_consumer_components(inputs, keyed_state, observer)
    }
}

impl PeerDirectoryBackend for RecordingMemoryBackend {
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

impl EventHandler for SweptHandler {
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

    async fn shutdown(self) {
        self.log.lock().push(Event::ManagerSwept);
    }
}

/// Leaves one partition manager in the shared map that no rebalance will
/// remove, so the shutdown sweep has something to shut down.
///
/// This is the state a failed `close_queue` leaves behind: rdkafka skips its
/// close-poll loop, the final revoke never dispatches, and the manager stays.
/// Its handler records the end of its task, which is what makes the sweep's
/// position in the shutdown order observable.
fn retain_manager(
    config: &ConsumerConfiguration,
    managers: &Managers<Value>,
    log: EventLog,
) -> Result<()> {
    let keyed_state = KeyedStateInputs::new(
        KeyedStateConfiguration::builder().build()?,
        config,
        DEFAULT_IDEMPOTENCE_VERSION,
    )?;
    let partition_config = PartitionConfiguration {
        group_id: Arc::from(config.group_id.as_str()),
        buffer_size: 1,
        max_uncommitted: 1,
        allowed_events: None,
        shutdown_timeout: Duration::from_secs(1),
        stall_threshold: config.stall_threshold,
        watermark_version: Arc::new(CachePadded::new(AtomicUsize::new(0))),
        version: keyed_state.version.clone(),
        trigger_provider: InMemoryTriggerStoreProvider::new(),
        state_provider: memory_state_provider::<JsonCodec>(
            &keyed_state,
            MemoryDeduplicationStoreProvider::new(),
            MemoryCells::new(),
            MemoryDescriptorIdentityStore::new(),
            MemoryLoader::new(),
            None,
        ),
        timer_slab_size: CompactDuration::new(30),
        timer_semaphores: Arc::new(from_fn(|_| Arc::new(Semaphore::new(1)))),
        telemetry_sender: Telemetry::new().sender(),
        timer_spans: SpanRelation::default(),
        _payload: PhantomData,
    };
    let topic = Topic::from(RETAINED_TOPIC);
    let manager = PartitionManager::new(
        partition_config,
        SweptHandler { log },
        topic,
        RETAINED_PARTITION,
    );
    managers
        .write()
        .insert((topic, RETAINED_PARTITION), manager);
    Ok(())
}

/// Awaits one consumer lifecycle step under [`HANG_GUARD`]. The error names the
/// step that did not finish.
async fn bounded<F: Future>(step: &'static str, future: F) -> Result<F::Output> {
    timeout(HANG_GUARD, future)
        .await
        .map_err(|_| eyre!("hang-guard: {step} did not finish"))
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

fn common_config(
    peer: Option<PeerConfiguration>,
    subsystem: Option<SubsystemName>,
) -> Result<CommonConfiguration> {
    let keyed_state = KeyedStateConfiguration::builder()
        .subsystem(subsystem)
        .build()?;
    Ok(CommonConfiguration {
        scheduler: SchedulerConfiguration::builder().build()?,
        timeout: TimeoutConfiguration::builder().build()?,
        dedup: DeduplicationConfiguration::builder().build()?,
        keyed_state,
        peer,
    })
}

fn recording_memory_deps(
    deps: &StateReaderDependencies<JsonCodec, MemoryReaderBackend<JsonCodec>>,
    directory: RecordingDirectory,
) -> StateReaderDependencies<JsonCodec, RecordingMemoryBackend> {
    let backend = RecordingMemoryBackend {
        inner: Arc::clone(deps.backend()),
        directory,
    };
    StateReaderDependencies::from_parts(backend, deps.cache().clone())
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
        },
        peer,
    ))
    .await
}

#[derive(Debug, Error)]
#[error("scripted registration failure")]
struct RecordingError;
