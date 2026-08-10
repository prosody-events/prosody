//! Optional type erasure for foreign-language client wrappers.

use crate::cassandra::config::{CassandraConfigurationBuilder, CassandraConfigurationBuilderError};
use crate::consumer::MockConfigurationError;
use crate::high_level::config::ModeConfiguration;
use crate::high_level::state::ConsumerState;
use crate::high_level::{
    CassandraHighLevelClient, ClientBackend, ClientHandler, ConsumerBuilders, HighLevelClient,
    HighLevelClientError, MemoryHighLevelClient, Mode, Wire, WireError,
};
use crate::producer::{ProducerConfiguration, ProducerConfigurationBuilder};
use crate::requester::{RequestError, ResponseError};
use crate::state_reader::ConsumerReaderBackend;
use crate::subsystem::SubsystemName;
use crate::{EventIdentity, EventType, Topic};
use async_trait::async_trait;
use opentelemetry::propagation::TextMapCompositePropagator;
use std::error::Error as StdError;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;

mod readers;

pub use readers::{
    ErasedDequeReader, ErasedDirection, ErasedMapReader, ErasedReadCache, ErasedReaderBuildError,
    ErasedValueReader, SharedDequeReader, SharedMapReader, SharedValueReader,
};

/// Consumer lifecycle state materialized across an FFI boundary.
#[derive(Clone, Debug)]
pub enum ErasedConsumerState<T> {
    /// No valid consumer configuration exists.
    Unconfigured,
    /// Consumer configuration failed.
    ConfigurationFailed(String),
    /// The consumer is ready to subscribe.
    Configured(ErasedConsumerConfiguration),
    /// The consumer is running.
    Running {
        /// Configuration retained across the running transition.
        config: ErasedConsumerConfiguration,
        /// Handler retained by the running client.
        handler: T,
    },
}

/// Owned consumer details needed by native wrapper diagnostics.
#[derive(Clone, Debug)]
pub struct ErasedConsumerConfiguration {
    /// Operational mode.
    pub mode: Mode,
    /// Subscribed topic names or patterns.
    pub topics: Vec<String>,
    /// Kafka consumer group.
    pub group_id: String,
}

/// Lifecycle operations used by foreign-language client wrappers.
///
/// Rust callers use [`HighLevelClient`] directly and pay no type-erasure cost.
#[async_trait]
pub trait ErasedHighLevelClient<T>: Send + Sync
where
    T: ClientHandler,
{
    /// Sends one event.
    async fn send(
        &self,
        topic: Topic,
        key: String,
        payload: T::Payload,
    ) -> Result<(), HighLevelClientError<WireError<T>>>;
    /// Sends one request and returns one result per subsystem.
    async fn request(
        &self,
        headers: Vec<(String, String)>,
        topic: Topic,
        key: String,
        payload: T::Payload,
        subsystems: Vec<SubsystemName>,
        timeout: Duration,
    ) -> Result<Vec<Result<T::Output, ResponseError>>, RequestError<WireError<T>>>;
    /// Starts consuming.
    async fn subscribe(&self, handler: T) -> Result<(), HighLevelClientError<WireError<T>>>;
    /// Stops consuming.
    async fn unsubscribe(&self) -> Result<(), HighLevelClientError<WireError<T>>>;
    /// Returns the current lifecycle state.
    async fn consumer_state(&self) -> ErasedConsumerState<T>;
    /// Builds a read-only view of one published value collection.
    async fn value_state(
        &self,
        subsystem: String,
        name: String,
        cache: ErasedReadCache,
    ) -> Result<SharedValueReader<Wire<T>>, ErasedReaderBuildError<WireError<T>>>;
    /// Builds a read-only view of one published string-keyed map collection.
    async fn map_state(
        &self,
        subsystem: String,
        name: String,
        cache: ErasedReadCache,
    ) -> Result<SharedMapReader<Wire<T>>, ErasedReaderBuildError<WireError<T>>>;
    /// Builds a read-only view of one published deque collection.
    async fn deque_state(
        &self,
        subsystem: String,
        name: String,
        cache: ErasedReadCache,
    ) -> Result<SharedDequeReader<Wire<T>>, ErasedReaderBuildError<WireError<T>>>;
    /// Returns the assigned partition count.
    async fn assigned_partition_count(&self) -> u32;
    /// Reports whether any consumer heartbeat is stalled.
    async fn is_stalled(&self) -> bool;
    /// Producer configuration.
    fn producer_config(&self) -> &ProducerConfiguration;
    /// Trace-context propagator.
    fn propagator(&self) -> &TextMapCompositePropagator;
    /// Configured source system.
    fn source_system(&self) -> &str;
}

/// Shared erased client representation stored by native FFI wrappers.
pub type SharedHighLevelClient<T> = Arc<dyn ErasedHighLevelClient<T>>;

/// Constructs and erases the backend selected by an FFI configuration.
///
/// # Errors
///
/// Backend selection and backend-specific configuration validation happen
/// here so every foreign-language client follows the same construction path.
pub async fn new_erased<T>(
    mode: Mode,
    producer: &mut ProducerConfigurationBuilder,
    consumers: &ConsumerBuilders,
    cassandra: &CassandraConfigurationBuilder,
) -> Result<SharedHighLevelClient<T>, ErasedClientBuildError<WireError<T>>>
where
    T: ClientHandler + Clone + Send + Sync + 'static,
    T::Payload: EventIdentity + EventType + Clone,
    T::Output: Sync + 'static,
    T::Error: Sync + 'static,
{
    let mock = consumers.consumer.configured_mock()?;

    if mock {
        Ok(Arc::new(ErasedClient(
            MemoryHighLevelClient::new(mode, producer, consumers).await?,
        )))
    } else {
        let cassandra = cassandra.build()?;
        Ok(Arc::new(ErasedClient(
            CassandraHighLevelClient::new(cassandra, mode, producer, consumers).await?,
        )))
    }
}

struct ErasedClient<T, B>(HighLevelClient<T, B>)
where
    T: ClientHandler,
    T::Payload: EventIdentity,
    B: ClientBackend<Wire<T>>;

#[async_trait]
impl<T, B> ErasedHighLevelClient<T> for ErasedClient<T, B>
where
    T: ClientHandler + Clone + Send + Sync + 'static,
    T::Payload: EventIdentity + EventType + Clone,
    T::Output: Sync + 'static,
    T::Error: Sync + 'static,
    B: ClientBackend<Wire<T>>,
    B::Reader: ConsumerReaderBackend<Wire<T>>,
{
    async fn send(
        &self,
        topic: Topic,
        key: String,
        payload: T::Payload,
    ) -> Result<(), HighLevelClientError<WireError<T>>> {
        self.0.send(topic, &key, payload).await
    }

    async fn request(
        &self,
        headers: Vec<(String, String)>,
        topic: Topic,
        key: String,
        payload: T::Payload,
        subsystems: Vec<SubsystemName>,
        timeout: Duration,
    ) -> Result<Vec<Result<T::Output, ResponseError>>, RequestError<WireError<T>>> {
        self.0
            .request_owned(headers, topic, key, payload, subsystems, timeout)
            .await
    }

    async fn subscribe(&self, handler: T) -> Result<(), HighLevelClientError<WireError<T>>> {
        self.0.subscribe_inner(handler).await
    }

    async fn unsubscribe(&self) -> Result<(), HighLevelClientError<WireError<T>>> {
        self.0.unsubscribe().await
    }

    async fn consumer_state(&self) -> ErasedConsumerState<T> {
        match &*self.0.consumer_state().await {
            ConsumerState::Unconfigured => ErasedConsumerState::Unconfigured,
            ConsumerState::ConfigurationFailed(error) => {
                ErasedConsumerState::ConfigurationFailed(error.to_string())
            }
            ConsumerState::Configured { config, .. } => {
                ErasedConsumerState::Configured(erased_config(config))
            }
            ConsumerState::Running {
                config, handler, ..
            } => ErasedConsumerState::Running {
                config: erased_config(config),
                handler: handler.clone(),
            },
        }
    }

    async fn value_state(
        &self,
        subsystem: String,
        name: String,
        cache: ErasedReadCache,
    ) -> Result<SharedValueReader<Wire<T>>, ErasedReaderBuildError<WireError<T>>> {
        readers::value(&self.0, subsystem, &name, cache).await
    }

    async fn map_state(
        &self,
        subsystem: String,
        name: String,
        cache: ErasedReadCache,
    ) -> Result<SharedMapReader<Wire<T>>, ErasedReaderBuildError<WireError<T>>> {
        readers::map(&self.0, subsystem, &name, cache).await
    }

    async fn deque_state(
        &self,
        subsystem: String,
        name: String,
        cache: ErasedReadCache,
    ) -> Result<SharedDequeReader<Wire<T>>, ErasedReaderBuildError<WireError<T>>> {
        readers::deque(&self.0, subsystem, &name, cache).await
    }

    async fn assigned_partition_count(&self) -> u32 {
        self.0.assigned_partition_count().await
    }

    async fn is_stalled(&self) -> bool {
        self.0.is_stalled().await
    }

    fn producer_config(&self) -> &ProducerConfiguration {
        self.0.producer_config()
    }

    fn propagator(&self) -> &TextMapCompositePropagator {
        self.0.propagator()
    }

    fn source_system(&self) -> &str {
        self.0.source_system()
    }
}

fn erased_config(config: &ModeConfiguration) -> ErasedConsumerConfiguration {
    let consumer = config.consumer_config();
    ErasedConsumerConfiguration {
        mode: config.mode(),
        topics: consumer.subscribed_topics.clone(),
        group_id: consumer.group_id.clone(),
    }
}

/// Failure to construct an erased FFI client.
#[derive(Debug, Error)]
pub enum ErasedClientBuildError<E>
where
    E: StdError + Send + Sync + 'static,
{
    /// The existing mock-mode environment override could not be parsed.
    #[error(transparent)]
    MockConfiguration(#[from] MockConfigurationError),
    /// Cassandra configuration failed for a live client.
    #[error(transparent)]
    CassandraConfiguration(#[from] CassandraConfigurationBuilderError),
    /// Concrete client construction failed.
    #[error(transparent)]
    Client(#[from] HighLevelClientError<E>),
}
