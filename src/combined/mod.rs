//! Combined client module for managing both producer and consumer operations.
//!
//! This module provides a `CombinedClient` struct that encapsulates both
//! producer and consumer functionality, allowing for unified management of
//! message production and consumption in various operational modes.

use crate::combined::config::ModeConfiguration;
use crate::combined::mode::Mode;
use crate::combined::state::ConsumerState;
use crate::consumer::failure::retry::RetryConfigurationBuilder;
use crate::consumer::failure::topic::FailureTopicConfigurationBuilder;
use crate::consumer::failure::FallibleHandler;
use crate::consumer::{ConsumerConfigurationBuilder, ConsumerError, ProsodyConsumer};
use crate::producer::{
    ProducerConfiguration, ProducerConfigurationBuilder, ProducerConfigurationBuilderError,
    ProducerError, ProsodyProducer,
};
use crate::propagator::new_propagator;
use crate::{Key, Payload, Topic};
use opentelemetry::propagation::TextMapCompositePropagator;
use std::mem::take;
use thiserror::Error;
use tracing::info;

pub mod config;
pub mod mode;
pub mod state;

/// A combined client that manages both producer and consumer operations.
#[derive(Debug)]
pub struct CombinedClient<T> {
    producer: ProsodyProducer,
    producer_config: ProducerConfiguration,
    consumer: ConsumerState<T>,
    propagator: TextMapCompositePropagator,
}

impl<T> CombinedClient<T> {
    /// Returns a reference to the internal `ProsodyProducer`.
    pub fn producer(&self) -> &ProsodyProducer {
        &self.producer
    }

    /// Returns a reference to the producer configuration.
    pub fn producer_config(&self) -> &ProducerConfiguration {
        &self.producer_config
    }

    /// Returns a reference to the current consumer state.
    pub fn consumer_state(&self) -> &ConsumerState<T> {
        &self.consumer
    }

    /// Returns a reference to an OpenTelemetry propagator.
    pub fn propagator(&self) -> &TextMapCompositePropagator {
        &self.propagator
    }

    /// Creates a new `CombinedClient` with the specified configurations.
    ///
    /// # Arguments
    ///
    /// * `mode` - The operational mode for the client.
    /// * `producer_builder` - Builder for the producer configuration.
    /// * `consumer_builder` - Builder for the consumer configuration.
    /// * `retry_builder` - Builder for the retry configuration.
    /// * `failure_topic_builder` - Builder for the failure topic configuration.
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing the new `CombinedClient` if successful,
    /// or a `CombinedClientError` if initialization fails.
    ///
    /// # Errors
    ///
    /// This function will return an error if any of the configuration builds
    /// fail or if the producer initialization fails.
    pub fn new(
        mode: Mode,
        producer_builder: &ProducerConfigurationBuilder,
        consumer_builder: &ConsumerConfigurationBuilder,
        retry_builder: &RetryConfigurationBuilder,
        failure_topic_builder: &FailureTopicConfigurationBuilder,
    ) -> Result<Self, CombinedClientError> {
        let producer_config = producer_builder.build()?;
        let producer = ProsodyProducer::new(&producer_config)?;
        let consumer =
            ConsumerState::build(mode, consumer_builder, retry_builder, failure_topic_builder);

        Ok(Self {
            producer,
            producer_config,
            consumer,
            propagator: new_propagator(),
        })
    }

    /// Sends a message to the specified topic.
    ///
    /// # Arguments
    ///
    /// * `topic` - The topic to send the message to.
    /// * `key` - The key associated with the message.
    /// * `payload` - The payload of the message.
    ///
    /// # Returns
    ///
    /// Returns a `Result` indicating success or failure of the send operation.
    ///
    /// # Errors
    ///
    /// This function will return an error if the send operation fails.
    pub async fn send(
        &self,
        topic: Topic,
        key: &Key,
        payload: &Payload,
    ) -> Result<(), CombinedClientError> {
        self.producer.send([], topic, key, payload).await?;
        Ok(())
    }

    /// Subscribes the consumer with the provided handler.
    ///
    /// # Arguments
    ///
    /// * `handler` - The handler to process consumed messages.
    ///
    /// # Returns
    ///
    /// Returns a `Result` indicating success or failure of the subscription.
    ///
    /// # Errors
    ///
    /// This function will return an error if:
    /// - The consumer is unconfigured
    /// - The consumer is already subscribed
    /// - The consumer initialization fails
    pub fn subscribe(&mut self, handler: T) -> Result<(), CombinedClientError>
    where
        T: FallibleHandler,
    {
        let config = match take(&mut self.consumer) {
            ConsumerState::Unconfigured => return Err(CombinedClientError::UnconfiguredConsumer),
            ConsumerState::Configured(config) => config,
            running @ ConsumerState::Running { .. } => {
                self.consumer = running;
                return Err(CombinedClientError::AlreadySubscribed);
            }
        };

        let consumer = match &config {
            ModeConfiguration::Pipeline { consumer, retry } => {
                ProsodyConsumer::pipeline_consumer(consumer, retry.clone(), handler.clone())?
            }
            ModeConfiguration::LowLatency {
                consumer,
                retry,
                failure_topic,
            } => ProsodyConsumer::low_latency_consumer(
                consumer,
                retry.clone(),
                failure_topic.clone(),
                self.producer.clone(),
                handler.clone(),
            )?,
        };

        self.consumer = ConsumerState::Running {
            consumer,
            config,
            handler,
        };

        Ok(())
    }

    /// Unsubscribes the consumer.
    ///
    /// # Returns
    ///
    /// Returns a `Result` indicating success or failure of the unsubscription.
    ///
    /// # Errors
    ///
    /// This function will return an error if the consumer is not currently
    /// subscribed.
    pub async fn unsubscribe(&mut self) -> Result<(), CombinedClientError> {
        let consumer = match take(&mut self.consumer) {
            state @ (ConsumerState::Unconfigured | ConsumerState::Configured(_)) => {
                self.consumer = state;
                return Err(CombinedClientError::NotSubscribed);
            }
            ConsumerState::Running {
                consumer, config, ..
            } => {
                self.consumer = ConsumerState::Configured(config);
                consumer
            }
        };

        info!("shutting down consumer");
        consumer.shutdown().await;
        Ok(())
    }
}

/// Errors that can occur in the `CombinedClient` operations.
#[derive(Debug, Error)]
pub enum CombinedClientError {
    /// Error when the producer configuration is invalid.
    #[error("invalid producer configuration: {0:#}")]
    ProducerConfiguration(#[from] ProducerConfigurationBuilderError),

    /// Error when initializing the producer fails.
    #[error("failed to initialize producer: {0:#}")]
    Producer(#[from] ProducerError),

    /// Error when initializing the consumer fails.
    #[error("failed to initialize consumer: {0:#}")]
    Consumer(#[from] ConsumerError),

    /// Error when attempting to use an unconfigured consumer.
    #[error("unconfigured consumer; create a client with a valid consumer configuration")]
    UnconfiguredConsumer,

    /// Error when attempting to subscribe an already subscribed consumer.
    #[error("consumer is already subscribed")]
    AlreadySubscribed,

    /// Error when attempting to unsubscribe a not subscribed consumer.
    #[error("consumer is not subscribed")]
    NotSubscribed,
}
