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
#[cfg(test)]
use crate::state_reader::SharedDeps;
use crate::state_reader::StateReader;
use crate::subsystem::SubsystemName;
use crate::telemetry::Telemetry;
use crate::{Codec, Topic};
use educe::Educe;
use opentelemetry::propagation::TextMapCompositePropagator;
use std::mem::take;
use tokio::sync::Mutex;
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
    /// The consumer state. On its `Configured` and `Running` variants it owns
    /// the one shared infrastructure bundle ([`SharedDeps`]). The bundle is
    /// built lazily on first use, by [`Self::state`] or [`Self::subscribe`],
    /// and retained across the `Configured → Running` transition. No state
    /// transition builds a second session, loader, or memory store.
    consumer: Mutex<ConsumerState<T, C, B>>,
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
    /// Returns a reference to the internal `ProsodyProducer`.
    pub fn producer(&self) -> &ProsodyProducer<C> {
        &self.producer
    }

    /// Returns a reference to the producer configuration.
    pub fn producer_config(&self) -> &ProducerConfiguration {
        &self.producer_config
    }

    /// Returns a view of the current consumer state.
    pub async fn consumer_state(&self) -> ConsumerStateView<'_, T, C, B> {
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
    /// consumer configuration to register against.
    pub async fn register<D>(
        &self,
        descriptor: D,
    ) -> Result<Registered<D>, HighLevelClientError<C::Error>>
    where
        D: StateDescriptor,
    {
        let mut guard = self.consumer.lock().await;
        match &mut *guard {
            ConsumerState::Configured { config, .. } => Ok(config.register(descriptor)),
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
    pub(crate) async fn retained_deps(&self) -> Option<SharedDeps<C, B::Reader>> {
        match &*self.consumer.lock().await {
            ConsumerState::Configured { deps, .. } => deps.clone(),
            ConsumerState::Running { deps, .. } => Some(deps.clone()),
            ConsumerState::Unconfigured | ConsumerState::ConfigurationFailed(_) => None,
        }
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
    /// bundle cannot connect or the descriptor is rejected.
    pub async fn state<D>(
        &self,
        subsystem: SubsystemName,
        descriptor: D,
    ) -> Result<StateReader<D, C, B::Reader>, HighLevelClientError<C::Error>>
    where
        D: StateDescriptor,
        C::Payload: Clone,
    {
        let mut guard = self.consumer.lock().await;
        let deps = deps::get_or_build(&mut guard, &self.backend).await?;
        StateReader::new(&deps, subsystem, descriptor).map_err(HighLevelClientError::StateReader)
    }

    /// Subscribes the consumer with the provided handler.
    ///
    /// # Errors
    ///
    /// Returns a `HighLevelClientError` if:
    /// - The consumer is unconfigured.
    /// - The consumer is already subscribed.
    /// - Consumer initialization fails.
    async fn subscribe_inner(&self, handler: T) -> Result<(), HighLevelClientError<C::Error>>
    where
        T: FallibleHandler<Payload = C::Payload> + Clone,
        C::Payload: crate::EventType + Clone,
        B::Reader: ConsumerReaderBackend<C>,
    {
        let mut guard = self.consumer.lock().await;

        // Take the state out. Only `Configured` proceeds; the others restore
        // themselves (or leave `Unconfigured`) and return their errors.
        let (config, existing_deps) = match take(&mut *guard) {
            ConsumerState::Unconfigured => return Err(HighLevelClientError::UnconfiguredConsumer),
            ConsumerState::ConfigurationFailed(error) => {
                return Err(HighLevelClientError::ConsumerConfiguration(error));
            }
            ConsumerState::Configured { config, deps } => (config, deps),
            running @ ConsumerState::Running { .. } => {
                *guard = running;
                return Err(HighLevelClientError::AlreadySubscribed);
            }
        };

        // Build (or reuse the memoized) bundle now that we own the config, so
        // the running consumer and any reader share it. A build failure here
        // must stay retryable, so restore `Configured` (no bundle) and return.
        let shared = match existing_deps {
            Some(shared) => shared,
            None => match deps::build(&config, &self.backend).await {
                Ok(shared) => shared,
                Err(error) => {
                    *guard = ConsumerState::Configured { config, deps: None };
                    return Err(error);
                }
            },
        };

        // Build the consumer. `take` moved the config out, so any failure must
        // undo both: the match below restores `Configured` and drops the
        // bundle. See there for why each step is needed.
        let trigger_store = self.backend.trigger_store();
        let built: Result<_, HighLevelClientError<C::Error>> = match &config {
            ModeConfiguration::Pipeline {
                consumer,
                retry,
                monopolization,
                defer,
                common,
            } => ProsodyConsumer::<C>::pipeline_consumer_with_backend::<T, B::Reader>(
                deps::consumer_setup::<C, B>(consumer, &trigger_store, common, &shared),
                PipelineMiddlewareConfiguration {
                    retry: retry.clone(),
                    monopolization: monopolization.clone(),
                    defer: defer.clone(),
                },
                self.telemetry.clone(),
                handler.clone(),
            )
            .await
            .map_err(Into::into),
            ModeConfiguration::LowLatency {
                consumer,
                retry,
                failure_topic,
                common,
            } => ProsodyConsumer::low_latency_consumer_with_backend::<T, B::Reader>(
                deps::consumer_setup::<C, B>(consumer, &trigger_store, common, &shared),
                LowLatencyMiddlewareConfiguration {
                    retry: retry.clone(),
                    failure_topic: failure_topic.clone(),
                },
                self.producer.clone(),
                self.telemetry.clone(),
                handler.clone(),
            )
            .await
            .map_err(Into::into),
            ModeConfiguration::BestEffort { consumer, common } => {
                ProsodyConsumer::<C>::best_effort_consumer::<T, B::Reader>(
                    deps::consumer_setup::<C, B>(consumer, &trigger_store, common, &shared),
                    self.telemetry.clone(),
                    handler.clone(),
                )
                .await
                .map_err(Into::into)
            }
        };

        let consumer = match built {
            Ok(consumer) => consumer,
            Err(error) => {
                // Restore the configured state so a transient build failure
                // stays retryable. Drop the bundle: its open scylla session,
                // live rdkafka poll thread, and registered heartbeat would
                // otherwise be stranded. The next `subscribe` rebuilds a
                // fresh one.
                *guard = ConsumerState::Configured { config, deps: None };
                return Err(error);
            }
        };

        *guard = ConsumerState::Running {
            consumer,
            config,
            handler,
            deps: shared,
        };

        Ok(())
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

            // Restore `Configured` without a bundle: the taken `Running.deps`
            // is dropped here. Its heartbeat registry holds this consumer's
            // poll-loop heartbeat, which stops beating at shutdown. Reusing the
            // same registry on a later `subscribe` would count that dead
            // heartbeat in `is_stalled` forever and grow the registry with no
            // removal path. The next `subscribe` rebuilds a fresh bundle.
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
                    *guard = ConsumerState::Configured { config, deps: None };
                    consumer
                }
            }
        };

        info!("shutting down consumer");
        consumer.shutdown().await;
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
        }
    };
}

impl_subscribe!(MemoryClientBackend<C>);
impl_subscribe!(CassandraClientBackend<C>);
