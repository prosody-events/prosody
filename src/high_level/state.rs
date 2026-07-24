//! State management for the consumer in a message processing system.
//!
//! This module defines the `ConsumerState` enum to represent different states
//! of the consumer, along with methods for building and displaying the state.
//! It also includes a custom error type for handling state-related errors.

use crate::Codec;
use crate::consumer::ProsodyConsumer;
use crate::high_level::config::{
    ModeConfiguration, ModeConfigurationBuildParams, ModeConfigurationError,
};
use crate::state_reader::SharedDeps;
use educe::Educe;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::ops::Deref;
use tokio::sync::MutexGuard;
use tracing::info;

/// A wrapper around a mutex guard for `ConsumerState`.
///
/// This type provides a view into the current state of the consumer,
/// allowing read-only access to the underlying `ConsumerState`.
pub struct ConsumerStateView<'a, T, C: Codec>(pub(crate) MutexGuard<'a, ConsumerState<T, C>>);

impl<T, C: Codec> Deref for ConsumerStateView<'_, T, C> {
    type Target = ConsumerState<T, C>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Represents the current state of the consumer.
///
/// The shared infrastructure bundle ([`SharedDeps`]) lives only on states that
/// have a config to build it from. `Configured` holds it as an `Option`, built
/// lazily on the first call to
/// [`state`](crate::high_level::HighLevelClient::state) or
/// [`subscribe`](crate::high_level::HighLevelClient::subscribe). `Running`
/// always holds it, because `subscribe` builds it before starting the consumer.
/// `Unconfigured` and `ConfigurationFailed` have no config, so they cannot
/// carry a bundle.
#[derive(Educe, Default)]
#[educe(Debug)]
pub enum ConsumerState<T, C: Codec> {
    /// The consumer is not yet configured.
    #[default]
    Unconfigured,
    /// The consumer configuration failed during build.
    ConfigurationFailed(ModeConfigurationError),
    /// The consumer is configured but not running.
    Configured {
        /// The configuration to run when subscribed.
        config: ModeConfiguration,
        /// The shared bundle, or `None` until first built. Built lazily and
        /// reused when the consumer moves to `Running`.
        #[educe(Debug(ignore))]
        deps: Option<SharedDeps<C>>,
    },
    /// The consumer is actively running.
    Running {
        /// The active Prosody consumer instance.
        consumer: ProsodyConsumer<C>,
        /// The configuration used for this consumer.
        config: ModeConfiguration,
        /// The handler for processing messages.
        handler: T,
        /// The shared bundle handed to the running consumer and reused by any
        /// reader built while running.
        #[educe(Debug(ignore))]
        deps: SharedDeps<C>,
    },
}

impl<T, C: Codec> ConsumerState<T, C> {
    /// Builds a new `ConsumerState` from the given configuration, returning
    /// [`ConsumerState::Configured`] on success or
    /// [`ConsumerState::ConfigurationFailed`] with the error otherwise.
    pub(crate) fn build(params: &ModeConfigurationBuildParams) -> Self {
        match ModeConfiguration::build(params) {
            Ok(configuration) => Self::Configured {
                config: configuration,
                deps: None,
            },
            Err(error) => {
                info!("disabling consumer (safe to ignore if you're only producing): {error:#}");
                Self::ConfigurationFailed(error)
            }
        }
    }
}

impl<T, C: Codec> Display for ConsumerState<T, C> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let state = match self {
            ConsumerState::Unconfigured => "unconfigured",
            ConsumerState::ConfigurationFailed(_) => "configuration failed",
            ConsumerState::Configured { .. } => "configured",
            ConsumerState::Running { .. } => "running",
        };

        f.write_str(state)
    }
}
