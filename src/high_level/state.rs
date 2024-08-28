//! State management for the consumer in a message processing system.
//!
//! This module defines the `ConsumerState` enum to represent different states
//! of the consumer, along with methods for building and displaying the state.
//! It also includes a custom error type for handling state-related errors.

use crate::consumer::failure::retry::RetryConfigurationBuilder;
use crate::consumer::failure::topic::FailureTopicConfigurationBuilder;
use crate::consumer::{ConsumerConfigurationBuilder, ProsodyConsumer};
use crate::high_level::config::ModeConfiguration;
use crate::high_level::mode::Mode;
use parking_lot::MutexGuard;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::ops::Deref;
use thiserror::Error;

/// A wrapper around a mutex guard for `ConsumerState`.
///
/// This type provides a view into the current state of the consumer,
/// allowing read-only access to the underlying `ConsumerState`.
pub struct ConsumerStateView<'a, T>(pub(crate) MutexGuard<'a, ConsumerState<T>>);

impl<'a, T> Deref for ConsumerStateView<'a, T> {
    type Target = ConsumerState<T>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Represents the current state of the consumer.
#[derive(Debug, Default)]
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

    /// Retrieves a reference to the `ModeConfiguration` if available.
    ///
    /// # Returns
    ///
    /// A reference to the `ModeConfiguration` if the consumer is in the
    /// `Configured` or `Running` state.
    ///
    /// # Errors
    ///
    /// Returns `ConsumerStateError::UnconfiguredConsumer` if the consumer is in
    /// the `Unconfigured` state.
    pub fn mode_configuration(&self) -> Result<&ModeConfiguration, ConsumerStateError> {
        match self {
            ConsumerState::Unconfigured => Err(ConsumerStateError::UnconfiguredConsumer),
            ConsumerState::Configured(config) | ConsumerState::Running { config, .. } => Ok(config),
        }
    }
}

impl<T> Display for ConsumerState<T> {
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
pub enum ConsumerStateError {
    /// Attempted to use an unconfigured consumer.
    #[error("unconfigured consumer; create a client with a valid consumer configuration")]
    UnconfiguredConsumer,
}
