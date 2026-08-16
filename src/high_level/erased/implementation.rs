use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use opentelemetry::propagation::TextMapCompositePropagator;

use super::{
    ConsumerReaderBackend, ConsumerState, ErasedConsumerState, ErasedHighLevelClient,
    ErasedReadCache, ErasedReaderBuildError, ErasedStateCodec, EventIdentity, EventType,
    HighLevelClient, HighLevelClientError, MessageCodec, MessageCodecError, ProducerConfiguration,
    RequestError, SharedDequeReader, SharedMapReader, SharedValueReader, StateCodec, SubsystemName,
    SubsystemOutcomes, Topic, deque, erased_config, map, value,
};
use crate::high_level::{ClientBackend, ClientHandler};

pub(super) struct ErasedClient<T, B>(pub(super) HighLevelClient<T, B>)
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
