//! High-level client: one handle owning both a producer and a consumer.
//!
//! [`HighLevelClient`] is built from [`ConsumerBuilders`] and a [`Mode`], then
//! driven through [`subscribe`](HighLevelClient::subscribe) /
//! [`unsubscribe`](HighLevelClient::unsubscribe). The shared infrastructure it
//! hands to consumers and readers alike lives in `deps`; topic reconciliation
//! in `topics`; the consumer's state machine in [`state`].

use crate::consumer::{
    LowLatencyMiddlewareConfiguration, NoResponses, PipelineMiddlewareConfiguration,
    ProsodyConsumer, Responding, ResponsePolicy,
};
pub use crate::high_level::config::ConsumerBuilders;
use crate::high_level::config::ModeConfiguration;
pub use crate::high_level::error::HighLevelClientError;
pub use crate::high_level::mode::Mode;
use crate::high_level::state::{ConsumerState, ConsumerStateView};
use crate::peer::Router;
use crate::peer::requester::{ProsodyRequester, RequestError, SubsystemOutcomes};
use crate::producer::{ProducerConfiguration, ProsodyProducer};
use crate::state::descriptor::{Registered, StateDescriptor};
use crate::state_reader::ConsumerReaderBackend;
use crate::state_reader::StateReaderDependencies;
use crate::state_reader::{StateReader, StateReaderClient};
use crate::subsystem::SubsystemName;
use crate::telemetry::Telemetry;
use crate::{Codec, Topic};
use educe::Educe;
use opentelemetry::propagation::TextMapCompositePropagator;
use std::mem::take;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, OnceCell};
use tracing::info;

mod backend;
mod codecs;
pub mod config;
mod construction;
mod deps;
pub mod erased;
mod error;
pub mod mode;
pub mod state;
mod topics;

pub use backend::{CassandraClientBackend, ClientBackend, MemoryClientBackend};
pub use codecs::{ClientHandler, CodecSet, Codecs, JsonBinaryCodecs, JsonCodecs};
#[doc(hidden)]
pub use deps::ReaderConfiguration;

/// High-level client using Cassandra storage.
pub type CassandraHighLevelClient<T> = HighLevelClient<T, CassandraClientBackend<MessageCodec<T>>>;

/// High-level client using in-memory storage.
pub type MemoryHighLevelClient<T> = HighLevelClient<T, MemoryClientBackend<MessageCodec<T>>>;

use codecs::{MessageCodec, ResponseCodec};
type MessageCodecError<T> = <MessageCodec<T> as Codec>::Error;
type ClientStateReader<T, B, D> =
    StateReader<D, MessageCodec<T>, <B as ClientBackend<MessageCodec<T>>>::Reader>;

#[cfg(test)]
mod tests;

/// A combined client that manages both producer and consumer operations.
#[derive(Educe)]
#[educe(Debug)]
pub struct HighLevelClient<T, B>
where
    T: ClientHandler,
    T::Payload: crate::EventIdentity,
    B: ClientBackend<MessageCodec<T>>,
{
    producer: ProsodyProducer<MessageCodec<T>>,
    producer_config: ProducerConfiguration,
    consumer: Mutex<ConsumerState<T, MessageCodec<T>>>,
    #[educe(Debug(ignore))]
    reader: OnceCell<StateReaderClient<MessageCodec<T>, B::Reader>>,
    #[educe(Debug(ignore))]
    reader_config: Option<ReaderConfiguration>,
    backend: B,
    #[educe(Debug(ignore))]
    requester: ProsodyRequester<MessageCodec<T>, ResponseCodec<T>>,
    #[educe(Debug(ignore))]
    subsystem: Option<SubsystemName>,
    #[educe(Debug(ignore))]
    router: B::Router,
    propagator: Arc<TextMapCompositePropagator>,
    telemetry: Telemetry,
}

impl<T, B> HighLevelClient<T, B>
where
    T: ClientHandler,
    T::Payload: crate::EventIdentity,
    B: ClientBackend<MessageCodec<T>>,
{
    async fn reader(
        &self,
    ) -> Result<
        StateReaderClient<MessageCodec<T>, B::Reader>,
        HighLevelClientError<MessageCodecError<T>>,
    >
    where
        T::Payload: Clone,
    {
        let config = self
            .reader_config
            .as_ref()
            .ok_or(HighLevelClientError::UnconfiguredConsumer)?;
        self.reader
            .get_or_try_init(|| async {
                deps::build(config, &self.backend)
                    .await
                    .map(StateReaderClient::new)
            })
            .await
            .cloned()
    }

    /// Returns a reference to the internal `ProsodyProducer`.
    pub fn producer(&self) -> &ProsodyProducer<MessageCodec<T>> {
        &self.producer
    }

    /// Returns a reference to the producer configuration.
    pub fn producer_config(&self) -> &ProducerConfiguration {
        &self.producer_config
    }

    /// Returns a view of the current consumer state.
    pub async fn consumer_state(&self) -> ConsumerStateView<'_, T, MessageCodec<T>> {
        ConsumerStateView(self.consumer.lock().await)
    }

    /// Returns a reference to the OpenTelemetry propagator.
    pub fn propagator(&self) -> &TextMapCompositePropagator {
        self.propagator.as_ref()
    }

    /// Returns the configured source system identifier.
    ///
    /// The source system identifies the originating service in produced
    /// messages, enabling message tracing and loop detection.
    #[must_use]
    pub fn source_system(&self) -> &str {
        &self.producer_config.source_system
    }

    /// Returns a reference to the shared telemetry instance.
    pub fn telemetry(&self) -> &Telemetry {
        &self.telemetry
    }

    /// Sends a message to the specified topic.
    ///
    /// # Errors
    ///
    /// Returns a `HighLevelClientError` if the send operation fails.
    pub async fn send(
        &self,
        topic: Topic,
        key: &str,
        payload: T::Payload,
    ) -> Result<(), HighLevelClientError<MessageCodecError<T>>> {
        self.producer.send([], topic, key, payload).await?;
        Ok(())
    }

    /// Sends one request and returns one result per subsystem.
    ///
    /// # Errors
    ///
    /// Returns [`RequestError`] for invalid arguments, a produce failure, or
    /// shutdown.
    pub async fn request<'a, H>(
        &self,
        headers: H,
        topic: Topic,
        key: &str,
        payload: T::Payload,
        subsystems: &[SubsystemName],
        timeout: Duration,
    ) -> Result<SubsystemOutcomes<T::Output>, RequestError<MessageCodecError<T>>>
    where
        H: IntoIterator<Item = (&'a str, &'a str)> + Send,
        H::IntoIter: ExactSizeIterator + Send,
    {
        self.requester
            .request(headers, topic, key, payload, subsystems, timeout)
            .await
    }

    /// Sends one request from owned FFI values.
    pub(crate) async fn request_owned(
        &self,
        headers: Vec<(String, String)>,
        topic: Topic,
        key: String,
        payload: T::Payload,
        subsystems: Vec<SubsystemName>,
        timeout: Duration,
    ) -> Result<SubsystemOutcomes<T::Output>, RequestError<MessageCodecError<T>>> {
        self.request(
            headers
                .iter()
                .map(|(name, value)| (name.as_str(), value.as_str())),
            topic,
            &key,
            payload,
            &subsystems,
            timeout,
        )
        .await
    }

    /// Registers a keyed-state collection, returning the [`Registered`]
    /// capability handle a handler binds via `ctx.state(...)`.
    ///
    /// Call this while the consumer is `Configured`, before
    /// [`subscribe`](Self::subscribe) freezes the registrations into the
    /// running consumer. Tokens survive the `unsubscribe`/re-subscribe cycle,
    /// so a re-subscribe needs no re-registration.
    ///
    /// # Errors
    ///
    /// Returns [`HighLevelClientError::AlreadySubscribed`] when the consumer
    /// is already running (registrations are frozen). A published descriptor
    /// without a configured subsystem returns
    /// [`HighLevelClientError::StateRegistration`].
    pub async fn register<D>(
        &self,
        descriptor: D,
    ) -> Result<Registered<D>, HighLevelClientError<MessageCodecError<T>>>
    where
        D: StateDescriptor,
    {
        let mut guard = self.consumer.lock().await;
        match &mut *guard {
            ConsumerState::Unconfigured | ConsumerState::ConfigurationFailed(_) => {
                Err(HighLevelClientError::UnconfiguredConsumer)
            }
            ConsumerState::Configured { config } => config.register(descriptor).map_err(Into::into),
            ConsumerState::Running { .. } => Err(HighLevelClientError::AlreadySubscribed),
        }
    }

    /// The client's retained shared bundle, if one has been built. This test
    /// hook lets the composition suite seed committed state into the exact
    /// stores that the running consumer and the client's readers share.
    #[cfg(test)]
    pub(crate) fn retained_deps(
        &self,
    ) -> Option<StateReaderDependencies<MessageCodec<T>, B::Reader>> {
        self.reader.get().map(StateReaderClient::deps)
    }

    /// Composes a standalone [`StateReader`] over this client's one shared
    /// bundle, for `descriptor` routed under `subsystem`.
    ///
    /// Valid once the consumer is `Configured` or `Running`. Both draw from
    /// the same retained bundle. A reader built before `subscribe` and one
    /// built after therefore share one session, loader, and memory store.
    ///
    /// # Errors
    ///
    /// Returns [`HighLevelClientError::StateReader`] when the descriptor is
    /// rejected.
    pub async fn state<D>(
        &self,
        subsystem: SubsystemName,
        descriptor: D,
    ) -> Result<ClientStateReader<T, B, D>, HighLevelClientError<MessageCodecError<T>>>
    where
        D: StateDescriptor,
        T::Payload: Clone,
    {
        self.reader()
            .await?
            .state(subsystem, descriptor)
            .map_err(HighLevelClientError::StateReader)
    }

    async fn build_consumer<RP>(
        config: ModeConfiguration,
        shared: StateReaderDependencies<MessageCodec<T>, B::Reader>,
        producer: ProsodyProducer<MessageCodec<T>>,
        telemetry: Telemetry,
        handler: T,
        response: RP,
    ) -> (
        Result<ProsodyConsumer<MessageCodec<T>>, HighLevelClientError<MessageCodecError<T>>>,
        ModeConfiguration,
    )
    where
        T: Clone,
        T::Payload: crate::EventType + Clone,
        B::Reader: ConsumerReaderBackend<MessageCodec<T>>,
        RP: ResponsePolicy<T>,
    {
        let built =
            match &config {
                ModeConfiguration::Pipeline {
                    consumer,
                    retry,
                    monopolization,
                    defer,
                    common,
                } => Box::pin(
                    ProsodyConsumer::<MessageCodec<T>>::pipeline_consumer_with_policy::<
                        T,
                        B::Reader,
                        RP,
                    >(
                        deps::consumer_setup::<MessageCodec<T>, B>(consumer, common, &shared),
                        PipelineMiddlewareConfiguration {
                            retry: retry.clone(),
                            monopolization: monopolization.clone(),
                            defer: defer.clone(),
                        },
                        telemetry,
                        handler,
                        response,
                    ),
                )
                .await
                .map_err(Into::into),
                ModeConfiguration::LowLatency {
                    consumer,
                    retry,
                    failure_topic,
                    common,
                } => Box::pin(ProsodyConsumer::low_latency_consumer_with_policy::<
                    T,
                    B::Reader,
                    RP,
                >(
                    deps::consumer_setup::<MessageCodec<T>, B>(consumer, common, &shared),
                    LowLatencyMiddlewareConfiguration {
                        retry: retry.clone(),
                        failure_topic: failure_topic.clone(),
                    },
                    producer,
                    telemetry,
                    handler,
                    response,
                ))
                .await
                .map_err(Into::into),
                ModeConfiguration::BestEffort { consumer, common } => Box::pin(
                    ProsodyConsumer::<MessageCodec<T>>::best_effort_consumer_with_policy::<
                        T,
                        B::Reader,
                        RP,
                    >(
                        deps::consumer_setup::<MessageCodec<T>, B>(consumer, common, &shared),
                        telemetry,
                        handler,
                        response,
                    ),
                )
                .await
                .map_err(Into::into),
            };
        (built, config)
    }

    async fn subscribe_inner(
        &self,
        handler: T,
    ) -> Result<(), HighLevelClientError<MessageCodecError<T>>>
    where
        T: Clone,
        T::Output: Sync + 'static,
        T::Error: Sync + 'static,
        T::Payload: crate::EventType + Clone,
        B::Reader: ConsumerReaderBackend<MessageCodec<T>>,
    {
        let mut guard = self.consumer.lock().await;

        let config = match take(&mut *guard) {
            ConsumerState::Unconfigured => return Err(HighLevelClientError::UnconfiguredConsumer),
            ConsumerState::ConfigurationFailed(error) => {
                return Err(HighLevelClientError::ConsumerConfiguration(error));
            }
            ConsumerState::Configured { config } => config,
            running @ ConsumerState::Running { .. } => {
                *guard = running;
                return Err(HighLevelClientError::AlreadySubscribed);
            }
        };

        let shared = match self.reader().await {
            Ok(reader) => reader.deps(),
            Err(error) => {
                *guard = ConsumerState::Configured { config };
                return Err(error);
            }
        };

        let (built, config) = if let Some(subsystem) = &self.subsystem {
            Self::build_consumer(
                config,
                shared,
                self.producer.clone(),
                self.telemetry.clone(),
                handler.clone(),
                Responding::<ResponseCodec<T>, _>::new(&self.router, subsystem.clone()),
            )
            .await
        } else {
            Self::build_consumer(
                config,
                shared,
                self.producer.clone(),
                self.telemetry.clone(),
                handler.clone(),
                NoResponses,
            )
            .await
        };

        let consumer = match built {
            Ok(consumer) => consumer,
            Err(error) => {
                // Restore the configured state so a transient build failure
                // stays retryable.
                *guard = ConsumerState::Configured { config };
                return Err(error);
            }
        };

        *guard = ConsumerState::Running {
            consumer,
            config,
            handler,
        };

        Ok(())
    }

    /// Unsubscribes the consumer.
    ///
    /// # Errors
    ///
    /// Returns a `HighLevelClientError` if the consumer is not currently
    /// subscribed.
    pub async fn unsubscribe(&self) -> Result<(), HighLevelClientError<MessageCodecError<T>>> {
        let consumer = {
            let mut guard = self.consumer.lock().await;
            match take(&mut *guard) {
                state @ (ConsumerState::Unconfigured
                | ConsumerState::ConfigurationFailed(_)
                | ConsumerState::Configured { .. }) => {
                    *guard = state;
                    return Err(HighLevelClientError::NotSubscribed);
                }
                ConsumerState::Running {
                    consumer, config, ..
                } => {
                    *guard = ConsumerState::Configured { config };
                    consumer
                }
            }
        };

        info!("shutting down consumer");
        consumer.shutdown().await;
        Ok(())
    }

    /// Shuts down the consumer and all client services.
    ///
    /// The method consumes the client. No operation can start after shutdown.
    ///
    /// # Errors
    ///
    /// Returns a `HighLevelClientError` if a client service cannot stop.
    pub async fn shutdown(self) -> Result<(), HighLevelClientError<MessageCodecError<T>>> {
        if let ConsumerState::Running { consumer, .. } = self.consumer.into_inner() {
            info!("shutting down consumer");
            consumer.shutdown().await;
        }

        self.router.shutdown().await?;
        Ok(())
    }

    /// Returns the number of partitions assigned to the consumer.
    ///
    /// Returns 0 if the consumer is not in the Running state.
    pub async fn assigned_partition_count(&self) -> u32 {
        let ConsumerState::Running { ref consumer, .. } = *self.consumer_state().await else {
            return 0;
        };

        consumer.assigned_partition_count()
    }

    /// Checks if the consumer is stalled.
    ///
    /// Returns `false` if the consumer is not in the Running state.
    pub async fn is_stalled(&self) -> bool {
        let ConsumerState::Running { ref consumer, .. } = *self.consumer_state().await else {
            return false;
        };

        consumer.is_stalled()
    }
}

// Concrete impls keep consumer construction internals out of ClientBackend's
// public bounds.
macro_rules! impl_subscribe {
    ($backend:ident) => {
        impl<T> HighLevelClient<T, $backend<MessageCodec<T>>>
        where
            T: ClientHandler + Clone,
            T::Payload: crate::EventIdentity + crate::EventType + Clone,
            T::Output: Sync + 'static,
            T::Error: Sync + 'static,
        {
            /// Subscribes the consumer with the provided handler.
            ///
            /// A configured subsystem answers peer requests. Without one, the
            /// consumer processes events without answers.
            ///
            /// # Errors
            ///
            /// Returns an error when the consumer is unconfigured, already
            /// subscribed, or cannot be initialized.
            pub fn subscribe(
                &self,
                handler: T,
            ) -> impl Future<Output = Result<(), HighLevelClientError<MessageCodecError<T>>>> + Send + '_
            {
                self.subscribe_inner(handler)
            }
        }
    };
}

impl_subscribe!(MemoryClientBackend);
impl_subscribe!(CassandraClientBackend);
