//! Configuration for different operational modes of the Prosody client.
//!
//! This module defines the `ModeConfiguration` enum to encapsulate
//! configuration settings for Pipeline and Low-Latency modes, along with
//! methods for building and accessing configuration details. It also includes a
//! custom error type for handling configuration-related errors.

use crate::Topic;
use crate::consumer::failure::retry::{
    RetryConfiguration, RetryConfigurationBuilder, RetryConfigurationBuilderError,
};
use crate::consumer::failure::topic::{
    FailureTopicConfiguration, FailureTopicConfigurationBuilder,
    FailureTopicConfigurationBuilderError,
};
use crate::consumer::{
    ConsumerConfiguration, ConsumerConfigurationBuilder, ConsumerConfigurationBuilderError,
};
use crate::high_level::mode::Mode;
use crate::timers::store::cassandra::{
    CassandraConfiguration, CassandraConfigurationBuilder, CassandraConfigurationBuilderError,
};
use thiserror::Error;

/// Configuration for timer storage backends.
#[derive(Debug)]
pub enum TriggerStoreConfiguration {
    /// In-memory storage for testing and mock mode.
    InMemory,
    /// Cassandra-based persistent storage.
    Cassandra(CassandraConfiguration),
}

/// Configuration for different operational modes of the Prosody client.
#[derive(Debug)]
pub enum ModeConfiguration {
    /// Configuration for Pipeline mode.
    Pipeline {
        /// The consumer configuration.
        consumer: ConsumerConfiguration,
        /// The retry configuration.
        retry: RetryConfiguration,
        /// The trigger store configuration.
        trigger_store: TriggerStoreConfiguration,
    },
    /// Configuration for Low-Latency mode.
    LowLatency {
        /// The consumer configuration.
        consumer: ConsumerConfiguration,
        /// The retry configuration.
        retry: RetryConfiguration,
        /// The failure topic configuration.
        failure_topic: FailureTopicConfiguration,
        /// The trigger store configuration.
        trigger_store: TriggerStoreConfiguration,
    },
    /// Configuration for Best-Effort mode.
    BestEffort {
        /// The consumer configuration.
        consumer: ConsumerConfiguration,
        /// The trigger store configuration.
        trigger_store: TriggerStoreConfiguration,
    },
}

impl ModeConfiguration {
    /// Builds a new `ModeConfiguration` based on the provided mode and
    /// configuration builders.
    ///
    /// # Arguments
    ///
    /// * `mode` - The operational mode for the configuration.
    /// * `consumer_builder` - Builder for the consumer configuration.
    /// * `retry_builder` - Builder for the retry configuration.
    /// * `failure_topic_builder` - Builder for the failure topic configuration.
    /// * `cassandra_builder` - Builder for the Cassandra configuration.
    ///
    /// # Returns
    ///
    /// A `Result` containing the built `ModeConfiguration` if successful, or a
    /// `ModeConfigurationError` if any of the builds fail.
    ///
    /// # Errors
    ///
    /// Returns an error if any of the configuration builds fail.
    pub(crate) fn build(
        mode: Mode,
        consumer_builder: &ConsumerConfigurationBuilder,
        retry_builder: &RetryConfigurationBuilder,
        failure_topic_builder: &FailureTopicConfigurationBuilder,
        cassandra_builder: &CassandraConfigurationBuilder,
    ) -> Result<Self, ModeConfigurationError> {
        let consumer = consumer_builder.build()?;
        let retry = retry_builder.build()?;

        // Create trigger store configuration based on mock mode
        let trigger_store = if consumer.mock {
            TriggerStoreConfiguration::InMemory
        } else {
            let cassandra_config = cassandra_builder.build()?;
            TriggerStoreConfiguration::Cassandra(cassandra_config)
        };

        Ok(match mode {
            Mode::Pipeline => Self::Pipeline {
                consumer,
                retry,
                trigger_store,
            },
            Mode::LowLatency => {
                let failure_topic = failure_topic_builder.build()?;
                Self::LowLatency {
                    consumer,
                    retry,
                    failure_topic,
                    trigger_store,
                }
            }
            Mode::BestEffort => Self::BestEffort {
                consumer,
                trigger_store,
            },
        })
    }

    /// Returns topics mentioned in the configuration.
    ///
    /// # Returns
    ///
    /// A vector of `Topic`s configured for the current mode.
    #[must_use]
    pub fn configured_topics(&self) -> Vec<Topic> {
        match self {
            Self::Pipeline { consumer, .. } | ModeConfiguration::BestEffort { consumer, .. } => {
                subscription(consumer).collect()
            }
            Self::LowLatency {
                consumer,
                failure_topic,
                ..
            } => {
                let mut topics = Vec::with_capacity(consumer.subscribed_topics.len() + 1);
                topics.extend(subscription(consumer));
                topics.push(failure_topic.failure_topic.as_str().into());
                topics
            }
        }
    }

    /// Returns the mode of the configuration.
    ///
    /// # Returns
    ///
    /// The `Mode` corresponding to this configuration.
    #[must_use]
    pub fn mode(&self) -> Mode {
        match self {
            ModeConfiguration::Pipeline { .. } => Mode::Pipeline,
            ModeConfiguration::LowLatency { .. } => Mode::LowLatency,
            ModeConfiguration::BestEffort { .. } => Mode::BestEffort,
        }
    }

    /// Returns a reference to the consumer configuration.
    ///
    /// # Returns
    ///
    /// A reference to the `ConsumerConfiguration` for this mode.
    #[must_use]
    pub fn consumer_config(&self) -> &ConsumerConfiguration {
        match self {
            ModeConfiguration::Pipeline { consumer, .. }
            | ModeConfiguration::LowLatency { consumer, .. }
            | ModeConfiguration::BestEffort { consumer, .. } => consumer,
        }
    }
}

/// Errors that can occur during mode configuration operations.
#[derive(Debug, Error)]
pub enum ModeConfigurationError {
    /// Error when the consumer configuration is invalid.
    #[error("invalid consumer configuration: {0:#}")]
    Consumer(#[from] ConsumerConfigurationBuilderError),

    /// Error when the retry configuration is invalid.
    #[error("invalid retry configuration: {0:#}")]
    Retry(#[from] RetryConfigurationBuilderError),

    /// Error when the failure topic configuration is invalid.
    #[error("invalid failure topic configuration: {0:#}")]
    FailureTopic(#[from] FailureTopicConfigurationBuilderError),

    /// Error when the Cassandra configuration is invalid.
    #[error("invalid cassandra configuration: {0:#}")]
    Cassandra(#[from] CassandraConfigurationBuilderError),
}

/// Creates an iterator over the subscribed topics in a consumer configuration.
///
/// # Arguments
///
/// * `consumer` - The consumer configuration to extract topics from.
///
/// # Returns
///
/// An iterator that yields `Topic`s from the consumer's subscribed topics.
fn subscription(consumer: &ConsumerConfiguration) -> impl Iterator<Item = Topic> + '_ {
    consumer
        .subscribed_topics
        .iter()
        .map(AsRef::as_ref)
        .map(Topic::from)
}
