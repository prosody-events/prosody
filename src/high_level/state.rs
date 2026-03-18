//! State management for the consumer in a message processing system.
//!
//! This module defines the `ConsumerState` enum to represent different states
//! of the consumer, along with methods for building and displaying the state.
//! It also includes a custom error type for handling state-related errors.

use crate::consumer::ProsodyConsumer;
use crate::high_level::config::{
    ModeConfiguration, ModeConfigurationBuildParams, ModeConfigurationError,
};
use std::fmt;
use std::fmt::{Display, Formatter};
use std::ops::Deref;
use tokio::sync::MutexGuard;
use tracing::info;

/// A wrapper around a mutex guard for `ConsumerState`.
///
/// This type provides a view into the current state of the consumer,
/// allowing read-only access to the underlying `ConsumerState`.
pub struct ConsumerStateView<'a, T>(pub(crate) MutexGuard<'a, ConsumerState<T>>);

impl<T> Deref for ConsumerStateView<'_, T> {
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
    /// The consumer configuration failed during build.
    ConfigurationFailed(ModeConfigurationError),
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
    /// * `params` - The build parameters containing all required configuration
    ///   builders.
    ///
    /// # Returns
    ///
    /// Returns a `ConsumerState::Configured` if the build is successful,
    /// otherwise returns `ConsumerState::ConfigurationFailed` with the error.
    pub(crate) fn build(params: &ModeConfigurationBuildParams) -> Self {
        match ModeConfiguration::build(params) {
            Ok(configuration) => Self::Configured(configuration),
            Err(error) => {
                info!("disabling consumer (safe to ignore if you're only producing): {error:#}");
                Self::ConfigurationFailed(error)
            }
        }
    }
}

impl<T> Display for ConsumerState<T> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let state = match self {
            ConsumerState::Unconfigured => "unconfigured",
            ConsumerState::ConfigurationFailed(_) => "configuration failed",
            ConsumerState::Configured(_) => "configured",
            ConsumerState::Running { .. } => "running",
        };

        f.write_str(state)
    }
}
