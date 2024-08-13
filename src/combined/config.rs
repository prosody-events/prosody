//! Configuration for different operational modes of the Prosody client.
//!
//! This module defines the `ModeConfiguration` enum to encapsulate
//! configuration settings for Pipeline and Low-Latency modes, along with
//! methods for building and accessing configuration details. It also includes a
//! custom error type for handling configuration-related errors.

use crate::combined::mode::Mode;
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
use thiserror::Error;

/// Configuration for different operational modes of the Prosody client.
#[derive(Debug)]
pub enum ModeConfiguration {
    /// Configuration for Pipeline mode.
    Pipeline {
        /// The consumer configuration.
        consumer: ConsumerConfiguration,
        /// The retry configuration.
        retry: RetryConfiguration,
    },
    /// Configuration for Low-Latency mode.
    LowLatency {
        /// The consumer configuration.
        consumer: ConsumerConfiguration,
        /// The retry configuration.
        retry: RetryConfiguration,
        /// The failure topic configuration.
        failure_topic: FailureTopicConfiguration,
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
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing the built `ModeConfiguration` if
    /// successful, or a `ModeConfigurationError` if any of the builds fail.
    ///
    /// # Errors
    ///
    /// This function will return an error if any of the configuration builds
    /// fail.
    pub(crate) fn build(
        mode: Mode,
        consumer_builder: &ConsumerConfigurationBuilder,
        retry_builder: &RetryConfigurationBuilder,
        failure_topic_builder: &FailureTopicConfigurationBuilder,
    ) -> Result<Self, ModeConfigurationError> {
        let consumer = consumer_builder.build()?;
        let retry = retry_builder.build()?;

        Ok(match mode {
            Mode::Pipeline => Self::Pipeline { consumer, retry },
            Mode::LowLatency => {
                let failure_topic = failure_topic_builder.build()?;
                Self::LowLatency {
                    consumer,
                    retry,
                    failure_topic,
                }
            }
        })
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
        }
    }

    /// Returns a reference to the consumer configuration.
    ///
    /// # Returns
    ///
    /// A reference to the `ConsumerConfiguration` for this mode.
    #[must_use]
    pub fn consumer(&self) -> &ConsumerConfiguration {
        match self {
            ModeConfiguration::Pipeline { consumer, .. }
            | ModeConfiguration::LowLatency { consumer, .. } => consumer,
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
}
