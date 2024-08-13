//! State management for the consumer in a message processing system.
//!
//! This module defines the `ConsumerState` enum to represent different states
//! of the consumer, along with methods for building and displaying the state.
//! It also includes a custom error type for handling state-related errors.

use crate::combined::config::ModeConfiguration;
use crate::combined::mode::Mode;
use crate::consumer::failure::retry::RetryConfigurationBuilder;
use crate::consumer::failure::topic::FailureTopicConfigurationBuilder;
use crate::consumer::{ConsumerConfigurationBuilder, ProsodyConsumer};
use std::fmt;
use std::fmt::{Display, Formatter};
use thiserror::Error;

/// Current state of the consumer.
#[derive(Default)]
pub enum ConsumerState<T> {
    /// The consumer is not yet configured.
    #[default]
    Unconfigured,
    /// The consumer is configured but not running.
    Configured(ModeConfiguration),
    /// The consumer is actively running.
    Running {
        /// The active Prosody consumer instance.
        consumer: ProsodyConsumer,
        /// The configuration used for this consumer.
        config: ModeConfiguration,
        /// The handler for processing messages.
        handler: T,
    },
}

impl<T> ConsumerState<T> {
    /// Builds a new `ConsumerState` based on the provided configuration.
    ///
    /// # Arguments
    ///
    /// * `mode` - The operating mode for the consumer.
    /// * `consumer_builder` - Builder for the consumer configuration.
    /// * `retry_builder` - Builder for the retry configuration.
    /// * `failure_topic_builder` - Builder for the failure topic configuration.
    ///
    /// # Returns
    ///
    /// Returns a `ConsumerState::Configured` if the build is successful,
    /// otherwise returns `ConsumerState::Unconfigured`.
    pub(crate) fn build(
        mode: Mode,
        consumer_builder: &ConsumerConfigurationBuilder,
        retry_builder: &RetryConfigurationBuilder,
        failure_topic_builder: &FailureTopicConfigurationBuilder,
    ) -> Self {
        ModeConfiguration::build(mode, consumer_builder, retry_builder, failure_topic_builder)
            .map(ConsumerState::Configured)
            .unwrap_or_default()
    }
}

impl<T> Display for ConsumerState<T> {
    /// Formats the `ConsumerState` as a human-readable string.
    ///
    /// # Arguments
    ///
    /// * `f` - A mutable reference to the `Formatter`.
    ///
    /// # Returns
    ///
    /// A `fmt::Result` indicating whether the operation was successful.
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let state = match self {
            ConsumerState::Unconfigured => "unconfigured",
            ConsumerState::Configured(_) => "configured",
            ConsumerState::Running { .. } => "running",
        };

        f.write_str(state)
    }
}

/// Errors that can occur during consumer state operations.
#[derive(Debug, Error)]
pub enum ConsumerStateError {}
