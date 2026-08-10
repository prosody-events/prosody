//! High-level consumer subscription state.

use crate::Codec;
use crate::consumer::ProsodyConsumer;
use crate::high_level::config::{
    ModeConfiguration, ModeConfigurationBuildParams, ModeConfigurationError,
};
use educe::Educe;
use std::fmt::{self, Display, Formatter};
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
/// Reader infrastructure belongs to the high-level client's reader component,
/// not this subscription state machine.
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
    },
    /// The consumer is actively running.
    Running {
        /// The active Prosody consumer instance.
        consumer: ProsodyConsumer<C>,
        /// The configuration used for this consumer.
        config: ModeConfiguration,
        /// The handler for processing messages.
        handler: T,
    },
}

impl<T, C: Codec> ConsumerState<T, C> {
    /// Builds the consumer configuration without rejecting producer-only use.
    pub(crate) fn build(params: &ModeConfigurationBuildParams<'_>) -> Self {
        match ModeConfiguration::build(params) {
            Ok(config) => Self::Configured { config },
            Err(error) => {
                info!("consumer is disabled until subscribe: {error:#}");
                Self::ConfigurationFailed(error)
            }
        }
    }
}

impl<T, C: Codec> Display for ConsumerState<T, C> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Unconfigured => "unconfigured",
            Self::ConfigurationFailed(_) => "configuration failed",
            Self::Configured { .. } => "configured",
            Self::Running { .. } => "running",
        })
    }
}
