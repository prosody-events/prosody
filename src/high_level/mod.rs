//! High-level client: one handle owning both a producer and a consumer.
//!
//! [`HighLevelClient`] is built from [`ConsumerBuilders`] and a [`Mode`], then
//! driven through [`subscribe`](HighLevelClient::subscribe) /
//! [`unsubscribe`](HighLevelClient::unsubscribe). The shared infrastructure it
//! hands to consumers and readers alike lives in `deps`; topic reconciliation
//! in `topics`; the consumer's state machine in [`state`].

use crate::consumer::middleware::FallibleHandler;
use crate::consumer::{
    LowLatencyMiddlewareConfiguration, PipelineMiddlewareConfiguration, ProsodyConsumer,
};
pub use crate::high_level::config::ConsumerBuilders;
use crate::high_level::config::ModeConfiguration;
pub use crate::high_level::error::HighLevelClientError;
pub use crate::high_level::mode::Mode;
use crate::high_level::state::{ConsumerState, ConsumerStateView};
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
use std::future::Future;
use std::mem::take;
use tokio::sync::{Mutex, OnceCell};
use tracing::info;

mod backend;
pub mod config;
mod construction;
mod deps;
pub mod erased;
mod error;
pub mod mode;
pub mod state;
mod topics;

pub use backend::{CassandraClientBackend, ClientBackend, MemoryClientBackend};
#[doc(hidden)]
pub use deps::ReaderConfiguration;

/// High-level client using Cassandra storage.
pub type CassandraHighLevelClient<T, C = crate::JsonCodec> =
    HighLevelClient<T, C, CassandraClientBackend<C>>;

/// High-level client using in-memory storage.
pub type MemoryHighLevelClient<T, C = crate::JsonCodec> =
    HighLevelClient<T, C, MemoryClientBackend<C>>;

#[cfg(test)]
mod tests;

/// A combined client that manages both producer and consumer operations.
#[derive(Educe)]
#[educe(Debug)]
pub struct HighLevelClient<T, C, B>
where
    C: Codec,
    C::Payload: crate::EventIdentity,
    B: ClientBackend<C>,
{
    producer: ProsodyProducer<C>,
    producer_config: ProducerConfiguration,
    consumer: Mutex<ConsumerState<T, C>>,
    #[educe(Debug(ignore))]
    reader: OnceCell<StateReaderClient<C, B::Reader>>,
    #[educe(Debug(ignore))]
    reader_config: Option<ReaderConfiguration>,
    backend: B,
    propagator: TextMapCompositePropagator,
    telemetry: Telemetry,
}

impl<T, C, B> HighLevelClient<T, C, B>
where
    C: Codec,
    C::Payload: crate::EventIdentity,
    B: ClientBackend<C>,
{
    async fn reader(
        &self,
    ) -> Result<StateReaderClient<C, B::Reader>, HighLevelClientError<C::Error>>
    where
        C::Payload: Clone,
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
    pub fn producer(&self) -> &ProsodyProducer<C> {
        &self.producer
    }

    /// Returns a reference to the producer configuration.
    pub fn producer_config(&self) -> &ProducerConfiguration {
        &self.producer_config
    }

    /// Returns a view of the current consumer state.
    pub async fn consumer_state(&self) -> ConsumerStateView<'_, T, C> {
        ConsumerStateView(self.consumer.lock().await)
    }

    /// Returns a reference to the OpenTelemetry propagator.
    pub fn propagator(&self) -> &TextMapCompositePropagator {
        &self.propagator
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
        payload: C::Payload,
    ) -> Result<(), HighLevelClientError<C::Error>> {
        self.producer.send([], topic, key, payload).await?;
        Ok(())
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
    /// is already running (registrations are frozen), or
    /// [`HighLevelClientError::UnconfiguredConsumer`] when there is no valid
    /// consumer configuration to register against. A published descriptor
    /// without a configured subsystem returns
    /// [`HighLevelClientError::StateRegistration`].
    pub async fn register<D>(
        &self,
        descriptor: D,
    ) -> Result<Registered<D>, HighLevelClientError<C::Error>>
    where
        D: StateDescriptor,
    {
        let mut guard = self.consumer.lock().await;
        match &mut *guard {
            ConsumerState::Configured { config } => config.register(descriptor).map_err(Into::into),
            ConsumerState::Running { .. } => Err(HighLevelClientError::AlreadySubscribed),
            ConsumerState::Unconfigured | ConsumerState::ConfigurationFailed(_) => {
                Err(HighLevelClientError::UnconfiguredConsumer)
            }
        }
    }

    /// The client's retained shared bundle, if one has been built. This test
    /// hook lets the composition suite seed committed state into the exact
    /// stores that the running consumer and the client's readers share.
    #[cfg(test)]
    pub(crate) fn retained_deps(&self) -> Option<StateReaderDependencies<C, B::Reader>> {
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
    /// [`HighLevelClientError::UnconfiguredConsumer`] if the client has no
    /// consumer configuration; [`HighLevelClientError::StateReader`] if the
    /// dependencies cannot be constructed or the descriptor is rejected.
    pub async fn state<D>(
        &self,
        subsystem: SubsystemName,
        descriptor: D,
    ) -> Result<StateReader<D, C, B::Reader>, HighLevelClientError<C::Error>>
    where
        D: StateDescriptor,
        C::Payload: Clone,
    {
        self.reader()
            .await?
            .state(subsystem, descriptor)
            .map_err(HighLevelClientError::StateReader)
    }

    async fn build_consumer(
        config: ModeConfiguration,
        shared: StateReaderDependencies<C, B::Reader>,
        producer: ProsodyProducer<C>,
        telemetry: Telemetry,
        handler: T,
    ) -> (
        Result<ProsodyConsumer<C>, HighLevelClientError<C::Error>>,
        ModeConfiguration,
    )
    where
        T: FallibleHandler<Payload = C::Payload> + Clone,
        C::Payload: crate::EventType + Clone,
        B::Reader: ConsumerReaderBackend<C>,
    {
        let built = match &config {
            ModeConfiguration::Pipeline {
                consumer,
                retry,
                monopolization,
                defer,
                common,
            } => Box::pin(ProsodyConsumer::<C>::pipeline_consumer_with_backend::<
                T,
                B::Reader,
            >(
                deps::consumer_setup::<C, B>(consumer, common, &shared),
                PipelineMiddlewareConfiguration {
                    retry: retry.clone(),
                    monopolization: monopolization.clone(),
                    defer: defer.clone(),
                },
                telemetry,
                handler,
            ))
            .await
            .map_err(Into::into),
            ModeConfiguration::LowLatency {
                consumer,
                retry,
                failure_topic,
                common,
            } => Box::pin(ProsodyConsumer::low_latency_consumer_with_backend::<
                T,
                B::Reader,
            >(
                deps::consumer_setup::<C, B>(consumer, common, &shared),
                LowLatencyMiddlewareConfiguration {
                    retry: retry.clone(),
                    failure_topic: failure_topic.clone(),
                },
                producer,
                telemetry,
                handler,
            ))
            .await
            .map_err(Into::into),
            ModeConfiguration::BestEffort { consumer, common } => {
                Box::pin(ProsodyConsumer::<C>::best_effort_consumer::<T, B::Reader>(
                    deps::consumer_setup::<C, B>(consumer, common, &shared),
                    telemetry,
                    handler,
                ))
                .await
                .map_err(Into::into)
            }
        };
        (built, config)
    }

    async fn build_responding_consumer<R>(
        config: ModeConfiguration,
        shared: StateReaderDependencies<C, B::Reader>,
        producer: ProsodyProducer<C>,
        telemetry: Telemetry,
        handler: T,
    ) -> (
        Result<ProsodyConsumer<C>, HighLevelClientError<C::Error>>,
        ModeConfiguration,
    )
    where
        T: FallibleHandler<Payload = C::Payload> + Clone,
        T::Output: Sync + 'static,
        T::Error: Sync + 'static,
        C::Payload: crate::EventType + Clone,
        B::Reader: ConsumerReaderBackend<C>,
        R: Codec<Payload = Result<T::Output, T::Error>>,
    {
        let built = match &config {
            ModeConfiguration::Pipeline {
                consumer,
                retry,
                monopolization,
                defer,
                common,
            } => Box::pin(
                ProsodyConsumer::<C>::pipeline_responding_consumer_with_backend::<T, R, B::Reader>(
                    deps::consumer_setup::<C, B>(consumer, common, &shared),
                    PipelineMiddlewareConfiguration {
                        retry: retry.clone(),
                        monopolization: monopolization.clone(),
                        defer: defer.clone(),
                    },
                    telemetry,
                    handler,
                ),
            )
            .await
            .map_err(Into::into),
            ModeConfiguration::LowLatency {
                consumer,
                retry,
                failure_topic,
                common,
            } => Box::pin(
                ProsodyConsumer::low_latency_responding_consumer_with_backend::<T, R, B::Reader>(
                    deps::consumer_setup::<C, B>(consumer, common, &shared),
                    LowLatencyMiddlewareConfiguration {
                        retry: retry.clone(),
                        failure_topic: failure_topic.clone(),
                    },
                    producer,
                    telemetry,
                    handler,
                ),
            )
            .await
            .map_err(Into::into),
            ModeConfiguration::BestEffort { consumer, common } => {
                Box::pin(ProsodyConsumer::<C>::best_effort_responding_consumer::<
                    T,
                    R,
                    B::Reader,
                >(
                    deps::consumer_setup::<C, B>(consumer, common, &shared),
                    telemetry,
                    handler,
                ))
                .await
                .map_err(Into::into)
            }
        };
        (built, config)
    }

    async fn subscribe_with<F, Fut>(
        &self,
        handler: T,
        assemble: F,
    ) -> Result<(), HighLevelClientError<C::Error>>
    where
        T: FallibleHandler<Payload = C::Payload> + Clone,
        C::Payload: crate::EventType + Clone,
        B::Reader: ConsumerReaderBackend<C>,
        F: FnOnce(
            ModeConfiguration,
            StateReaderDependencies<C, B::Reader>,
            ProsodyProducer<C>,
            Telemetry,
            T,
        ) -> Fut,
        Fut: Future<
            Output = (
                Result<ProsodyConsumer<C>, HighLevelClientError<C::Error>>,
                ModeConfiguration,
            ),
        >,
    {
        let mut guard = self.consumer.lock().await;

        // Take the state out. Only `Configured` proceeds; the others restore
        // themselves (or leave `Unconfigured`) and return their errors.
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

        let (built, config) = assemble(
            config,
            shared,
            self.producer.clone(),
            self.telemetry.clone(),
            handler.clone(),
        )
        .await;

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

    async fn subscribe_inner(&self, handler: T) -> Result<(), HighLevelClientError<C::Error>>
    where
        T: FallibleHandler<Payload = C::Payload> + Clone,
        C::Payload: crate::EventType + Clone,
        B::Reader: ConsumerReaderBackend<C>,
    {
        self.subscribe_with(handler, |config, shared, producer, telemetry, handler| {
            Self::build_consumer(config, shared, producer, telemetry, handler)
        })
        .await
    }

    async fn subscribe_responding_inner<R>(
        &self,
        handler: T,
    ) -> Result<(), HighLevelClientError<C::Error>>
    where
        T: FallibleHandler<Payload = C::Payload> + Clone,
        T::Output: Sync + 'static,
        T::Error: Sync + 'static,
        C::Payload: crate::EventType + Clone,
        B::Reader: ConsumerReaderBackend<C>,
        R: Codec<Payload = Result<T::Output, T::Error>>,
    {
        self.subscribe_with(handler, |config, shared, producer, telemetry, handler| {
            Self::build_responding_consumer::<R>(config, shared, producer, telemetry, handler)
        })
        .await
    }

    /// Unsubscribes the consumer.
    ///
    /// # Errors
    ///
    /// Returns a `HighLevelClientError` if the consumer is not currently
    /// subscribed.
    pub async fn unsubscribe(&self) -> Result<(), HighLevelClientError<C::Error>> {
        let consumer = {
            let mut guard = self.consumer.lock().await;

            // Restore `Configured`. Dropping the running consumer removes its
            // heartbeat registrations from the retained shared registry.
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
        consumer.shutdown().await?;
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
    ($backend:ty) => {
        impl<T, C> HighLevelClient<T, C, $backend>
        where
            C: Codec,
            C::Payload: crate::EventIdentity + crate::EventType + Clone,
            T: FallibleHandler<Payload = C::Payload> + Clone,
        {
            /// Subscribes the consumer with the provided handler.
            ///
            /// # Errors
            ///
            /// Returns an error when the consumer is unconfigured, already
            /// subscribed, or cannot be initialized.
            pub fn subscribe(
                &self,
                handler: T,
            ) -> impl Future<Output = Result<(), HighLevelClientError<C::Error>>> + Send + '_ {
                self.subscribe_inner(handler)
            }

            /// Subscribes a consumer that answers peer requests.
            ///
            /// The answer moves the handler's final result, so an answered
            /// record fires no `after_commit` on `handler`. See
            /// [`FallibleHandler`](crate::consumer::middleware::FallibleHandler).
            ///
            /// # Errors
            ///
            /// Returns an error when the consumer is unconfigured, already
            /// subscribed, or cannot be initialized.
            pub async fn subscribe_responding<R>(
                &self,
                handler: T,
            ) -> Result<(), HighLevelClientError<C::Error>>
            where
                T::Output: Sync + 'static,
                T::Error: Sync + 'static,
                R: Codec<Payload = Result<T::Output, T::Error>>,
            {
                self.subscribe_responding_inner::<R>(handler).await
            }
        }
    };
}

impl_subscribe!(MemoryClientBackend<C>);
impl_subscribe!(CassandraClientBackend<C>);
