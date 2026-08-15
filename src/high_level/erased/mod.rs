//! Optional type erasure for foreign-language client wrappers.

use crate::cassandra::config::{CassandraConfigurationBuilder, CassandraConfigurationBuilderError};
use crate::codec::ErasedStateCodec;
use crate::consumer::MockConfigurationError;
use crate::high_level::codecs::StateCodec;
use crate::high_level::config::ModeConfiguration;
use crate::high_level::state::ConsumerState;
use crate::high_level::{
    CassandraHighLevelClient, ClientBackend, ClientHandler, ConsumerBuilders, HighLevelClient,
    HighLevelClientError, MemoryHighLevelClient, MessageCodec, MessageCodecError, Mode,
};
use crate::peer::requester::{RequestError, SubsystemOutcomes};
use crate::producer::{ProducerConfiguration, ProducerConfigurationBuilder};
use crate::state_reader::ConsumerReaderBackend;
use crate::subsystem::SubsystemName;
use crate::{EventIdentity, EventType, Topic};
use async_trait::async_trait;
use opentelemetry::propagation::TextMapCompositePropagator;
use std::error::Error as StdError;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::RwLock;

mod readers;

pub(super) use readers::{deque, map, value};

pub use readers::{
    ErasedDequeReader, ErasedDirection, ErasedMapReader, ErasedReadCache, ErasedReaderBuildError,
    ErasedValueReader, SharedDequeReader, SharedMapReader, SharedValueReader,
};

/// Consumer lifecycle state materialized across an FFI boundary.
#[derive(Clone, Debug)]
pub enum ErasedConsumerState<T> {
    /// The client is shut down.
    Shutdown,
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

#[async_trait]
trait ErasedHighLevelClient<T>: Send + Sync
where
    T: ClientHandler,
{
    async fn send(
        &self,
        topic: Topic,
        key: String,
        payload: T::Payload,
    ) -> Result<(), HighLevelClientError<MessageCodecError<T>>>;
    /// Sends one excise record.
    async fn excise(
        &self,
        topic: Topic,
        key: String,
    ) -> Result<(), HighLevelClientError<MessageCodecError<T>>>;
    async fn request(
        &self,
        headers: Vec<(String, String)>,
        topic: Topic,
        key: String,
        payload: T::Payload,
        subsystems: Vec<SubsystemName>,
        timeout: Duration,
    ) -> Result<SubsystemOutcomes<T::Output>, RequestError<MessageCodecError<T>>>;
    async fn request_excise(
        &self,
        headers: Vec<(String, String)>,
        topic: Topic,
        key: String,
        subsystems: Vec<SubsystemName>,
        timeout: Duration,
    ) -> Result<SubsystemOutcomes<T::Output>, RequestError<MessageCodecError<T>>>;
    async fn subscribe(&self, handler: T)
    -> Result<(), HighLevelClientError<MessageCodecError<T>>>;
    async fn unsubscribe(&self) -> Result<(), HighLevelClientError<MessageCodecError<T>>>;
    async fn shutdown(self: Box<Self>) -> Result<(), HighLevelClientError<MessageCodecError<T>>>;
    async fn consumer_state(&self) -> ErasedConsumerState<T>;
    async fn value_state(
        &self,
        subsystem: String,
        name: String,
        cache: ErasedReadCache,
    ) -> Result<SharedValueReader<StateCodec<T>>, ErasedReaderBuildError<MessageCodecError<T>>>
    where
        T::Payload: ErasedStateCodec;
    async fn map_state(
        &self,
        subsystem: String,
        name: String,
        cache: ErasedReadCache,
    ) -> Result<SharedMapReader<StateCodec<T>>, ErasedReaderBuildError<MessageCodecError<T>>>
    where
        T::Payload: ErasedStateCodec;
    async fn deque_state(
        &self,
        subsystem: String,
        name: String,
        cache: ErasedReadCache,
    ) -> Result<SharedDequeReader<StateCodec<T>>, ErasedReaderBuildError<MessageCodecError<T>>>
    where
        T::Payload: ErasedStateCodec;
    async fn assigned_partition_count(&self) -> u32;
    async fn is_stalled(&self) -> bool;
    fn producer_config(&self) -> &ProducerConfiguration;
    fn propagator(&self) -> &Arc<TextMapCompositePropagator>;
}

/// Shared FFI client with one concrete lifecycle owner.
///
/// Operations hold shared access until they finish. Shutdown takes exclusive
/// access and consumes the concrete client.
///
/// Each foreign binding polls each operation in an independent task. Do not
/// retain an operation future and await shutdown from the same task.
pub struct SharedHighLevelClient<T>
where
    T: ClientHandler,
{
    client: Arc<RwLock<Option<Box<dyn ErasedHighLevelClient<T>>>>>,
    producer_config: Arc<ProducerConfiguration>,
    propagator: Arc<TextMapCompositePropagator>,
}

impl<T> Clone for SharedHighLevelClient<T>
where
    T: ClientHandler,
{
    fn clone(&self) -> Self {
        Self {
            client: Arc::clone(&self.client),
            producer_config: Arc::clone(&self.producer_config),
            propagator: Arc::clone(&self.propagator),
        }
    }
}

impl<T> SharedHighLevelClient<T>
where
    T: ClientHandler,
{
    fn new(client: Box<dyn ErasedHighLevelClient<T>>) -> Self {
        let producer_config = Arc::new(client.producer_config().clone());
        let propagator = Arc::clone(client.propagator());
        Self {
            client: Arc::new(RwLock::new(Some(client))),
            producer_config,
            propagator,
        }
    }

    /// Sends one event.
    ///
    /// # Errors
    ///
    /// Returns an error if the client is shut down or Kafka rejects the event.
    pub async fn send(
        &self,
        topic: Topic,
        key: String,
        payload: T::Payload,
    ) -> Result<(), HighLevelClientError<MessageCodecError<T>>> {
        let guard = self.client.read().await;
        let client = guard.as_deref().ok_or(HighLevelClientError::Closed)?;
        client.send(topic, key, payload).await
    }

    /// Sends one excise record.
    ///
    /// # Errors
    ///
    /// Returns an error if the client is shut down or Kafka rejects the record.
    pub async fn excise(
        &self,
        topic: Topic,
        key: String,
    ) -> Result<(), HighLevelClientError<MessageCodecError<T>>> {
        let guard = self.client.read().await;
        let client = guard.as_deref().ok_or(HighLevelClientError::Closed)?;
        client.excise(topic, key).await
    }

    /// Sends one request and returns one result per subsystem.
    ///
    /// # Errors
    ///
    /// Returns an error if the request is invalid or cannot start.
    pub async fn request(
        &self,
        headers: Vec<(String, String)>,
        topic: Topic,
        key: String,
        payload: T::Payload,
        subsystems: Vec<SubsystemName>,
        timeout: Duration,
    ) -> Result<SubsystemOutcomes<T::Output>, RequestError<MessageCodecError<T>>> {
        let guard = self.client.read().await;
        let Some(client) = guard.as_deref() else {
            return Err(RequestError::ShuttingDown);
        };
        client
            .request(headers, topic, key, payload, subsystems, timeout)
            .await
    }

    /// Sends one excise request and returns one result per subsystem.
    ///
    /// # Errors
    ///
    /// Returns an error if the request is invalid or cannot start.
    pub async fn request_excise(
        &self,
        headers: Vec<(String, String)>,
        topic: Topic,
        key: String,
        subsystems: Vec<SubsystemName>,
        timeout: Duration,
    ) -> Result<SubsystemOutcomes<T::Output>, RequestError<MessageCodecError<T>>> {
        let guard = self.client.read().await;
        let Some(client) = guard.as_deref() else {
            return Err(RequestError::ShuttingDown);
        };
        client
            .request_excise(headers, topic, key, subsystems, timeout)
            .await
    }

    /// Starts consuming.
    ///
    /// # Errors
    ///
    /// Returns an error if the client is shut down or cannot subscribe.
    pub async fn subscribe(
        &self,
        handler: T,
    ) -> Result<(), HighLevelClientError<MessageCodecError<T>>> {
        let guard = self.client.read().await;
        let client = guard.as_deref().ok_or(HighLevelClientError::Closed)?;
        client.subscribe(handler).await
    }

    /// Stops consuming.
    ///
    /// # Errors
    ///
    /// Returns an error if the client is shut down or is not subscribed.
    pub async fn unsubscribe(&self) -> Result<(), HighLevelClientError<MessageCodecError<T>>> {
        let guard = self.client.read().await;
        let client = guard.as_deref().ok_or(HighLevelClientError::Closed)?;
        client.unsubscribe().await
    }

    /// Shuts down the client and all its services.
    ///
    /// # Errors
    ///
    /// Returns an error if the client is shut down or a service cannot stop.
    pub async fn shutdown(self) -> Result<(), HighLevelClientError<MessageCodecError<T>>> {
        let client = {
            let mut guard = self.client.write().await;
            guard.take().ok_or(HighLevelClientError::Closed)?
        };
        client.shutdown().await
    }

    /// Returns the current lifecycle state.
    pub async fn consumer_state(&self) -> ErasedConsumerState<T> {
        let guard = self.client.read().await;
        let Some(client) = guard.as_deref() else {
            return ErasedConsumerState::Shutdown;
        };
        client.consumer_state().await
    }

    /// Builds a read-only view of one published value collection.
    ///
    /// # Errors
    ///
    /// Returns an error if the client is shut down or the reader is invalid.
    pub async fn value_state(
        &self,
        subsystem: String,
        name: String,
        cache: ErasedReadCache,
    ) -> Result<SharedValueReader<StateCodec<T>>, ErasedReaderBuildError<MessageCodecError<T>>>
    where
        T::Payload: ErasedStateCodec,
    {
        let guard = self.client.read().await;
        let client = guard.as_deref().ok_or(HighLevelClientError::Closed)?;
        client.value_state(subsystem, name, cache).await
    }

    /// Builds a read-only view of one published string-keyed map collection.
    ///
    /// # Errors
    ///
    /// Returns an error if the client is shut down or the reader is invalid.
    pub async fn map_state(
        &self,
        subsystem: String,
        name: String,
        cache: ErasedReadCache,
    ) -> Result<SharedMapReader<StateCodec<T>>, ErasedReaderBuildError<MessageCodecError<T>>>
    where
        T::Payload: ErasedStateCodec,
    {
        let guard = self.client.read().await;
        let client = guard.as_deref().ok_or(HighLevelClientError::Closed)?;
        client.map_state(subsystem, name, cache).await
    }

    /// Builds a read-only view of one published deque collection.
    ///
    /// # Errors
    ///
    /// Returns an error if the client is shut down or the reader is invalid.
    pub async fn deque_state(
        &self,
        subsystem: String,
        name: String,
        cache: ErasedReadCache,
    ) -> Result<SharedDequeReader<StateCodec<T>>, ErasedReaderBuildError<MessageCodecError<T>>>
    where
        T::Payload: ErasedStateCodec,
    {
        let guard = self.client.read().await;
        let client = guard.as_deref().ok_or(HighLevelClientError::Closed)?;
        client.deque_state(subsystem, name, cache).await
    }

    /// Returns the assigned partition count.
    pub async fn assigned_partition_count(&self) -> u32 {
        let guard = self.client.read().await;
        let Some(client) = guard.as_deref() else {
            return 0;
        };
        client.assigned_partition_count().await
    }

    /// Reports whether any consumer heartbeat is stalled.
    pub async fn is_stalled(&self) -> bool {
        let guard = self.client.read().await;
        let Some(client) = guard.as_deref() else {
            return false;
        };
        client.is_stalled().await
    }

    /// Returns the producer configuration.
    #[must_use]
    pub fn producer_config(&self) -> &ProducerConfiguration {
        &self.producer_config
    }

    /// Returns the trace-context propagator.
    #[must_use]
    pub fn propagator(&self) -> &TextMapCompositePropagator {
        &self.propagator
    }

    /// Returns the configured source system.
    #[must_use]
    pub fn source_system(&self) -> &str {
        &self.producer_config.source_system
    }
}

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
) -> Result<SharedHighLevelClient<T>, ErasedClientBuildError<MessageCodecError<T>>>
where
    T: ClientHandler + Clone + Send + Sync + 'static,
    T::Payload: EventIdentity + EventType + Clone,
    T::Output: Sync + 'static,
    T::Error: Sync + 'static,
{
    let mock = consumers.consumer.configured_mock()?;

    if mock {
        Ok(SharedHighLevelClient::new(Box::new(ErasedClient(
            MemoryHighLevelClient::new(mode, producer, consumers).await?,
        ))))
    } else {
        let cassandra = cassandra.build()?;
        Ok(SharedHighLevelClient::new(Box::new(ErasedClient(
            CassandraHighLevelClient::new(cassandra, mode, producer, consumers).await?,
        ))))
    }
}

struct ErasedClient<T, B>(HighLevelClient<T, B>)
where
    T: ClientHandler,
    T::Payload: EventIdentity,
    B: ClientBackend<MessageCodec<T>>;

#[async_trait]
impl<T, B> ErasedHighLevelClient<T> for ErasedClient<T, B>
where
    T: ClientHandler + Clone + Send + Sync + 'static,
    T::Payload: EventIdentity + EventType + Clone,
    T::Output: Sync + 'static,
    T::Error: Sync + 'static,
    B: ClientBackend<MessageCodec<T>>,
    B::Reader: ConsumerReaderBackend<MessageCodec<T>>,
{
    async fn send(
        &self,
        topic: Topic,
        key: String,
        payload: T::Payload,
    ) -> Result<(), HighLevelClientError<MessageCodecError<T>>> {
        self.0.send(topic, &key, payload).await
    }

    async fn excise(
        &self,
        topic: Topic,
        key: String,
    ) -> Result<(), HighLevelClientError<MessageCodecError<T>>> {
        self.0.excise(topic, &key).await
    }

    async fn request(
        &self,
        headers: Vec<(String, String)>,
        topic: Topic,
        key: String,
        payload: T::Payload,
        subsystems: Vec<SubsystemName>,
        timeout: Duration,
    ) -> Result<SubsystemOutcomes<T::Output>, RequestError<MessageCodecError<T>>> {
        self.0
            .request_owned(headers, topic, key, payload, subsystems, timeout)
            .await
    }

    async fn request_excise(
        &self,
        headers: Vec<(String, String)>,
        topic: Topic,
        key: String,
        subsystems: Vec<SubsystemName>,
        timeout: Duration,
    ) -> Result<SubsystemOutcomes<T::Output>, RequestError<MessageCodecError<T>>> {
        self.0
            .request_excise_owned(headers, topic, key, subsystems, timeout)
            .await
    }

    async fn subscribe(
        &self,
        handler: T,
    ) -> Result<(), HighLevelClientError<MessageCodecError<T>>> {
        self.0.subscribe_inner(handler).await
    }

    async fn unsubscribe(&self) -> Result<(), HighLevelClientError<MessageCodecError<T>>> {
        self.0.unsubscribe().await
    }

    async fn shutdown(self: Box<Self>) -> Result<(), HighLevelClientError<MessageCodecError<T>>> {
        self.0.shutdown().await
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
    ) -> Result<SharedValueReader<StateCodec<T>>, ErasedReaderBuildError<MessageCodecError<T>>>
    where
        T::Payload: ErasedStateCodec,
    {
        value(&self.0, subsystem, &name, cache).await
    }

    async fn map_state(
        &self,
        subsystem: String,
        name: String,
        cache: ErasedReadCache,
    ) -> Result<SharedMapReader<StateCodec<T>>, ErasedReaderBuildError<MessageCodecError<T>>>
    where
        T::Payload: ErasedStateCodec,
    {
        map(&self.0, subsystem, &name, cache).await
    }

    async fn deque_state(
        &self,
        subsystem: String,
        name: String,
        cache: ErasedReadCache,
    ) -> Result<SharedDequeReader<StateCodec<T>>, ErasedReaderBuildError<MessageCodecError<T>>>
    where
        T::Payload: ErasedStateCodec,
    {
        deque(&self.0, subsystem, &name, cache).await
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

    fn propagator(&self) -> &Arc<TextMapCompositePropagator> {
        &self.0.propagator
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
