//! Bounded route preferences for response destinations.

use crate::router::PeerId;
use crate::router::fleet::config::{FleetConfiguration, FleetConfigurationError};
use quick_cache::sync::Cache;
use std::convert::Infallible;
use std::sync::Arc;
use validator::Validate;

pub(crate) mod config;
mod destination;

pub(crate) use self::destination::Destination;

/// Route preferences shared by all responders in one process.
pub(crate) struct DestinationFleet {
    destinations: Cache<PeerId, Arc<Destination>>,
    config: FleetConfiguration,
}

impl DestinationFleet {
    /// Builds one bounded preference cache.
    ///
    /// # Errors
    ///
    /// Returns [`FleetConfigurationError::Invalid`] when delivery policy is
    /// invalid.
    pub(crate) fn new(config: FleetConfiguration) -> Result<Self, FleetConfigurationError> {
        config.validate()?;
        Ok(Self {
            destinations: Cache::new(config.peer_capacity),
            config,
        })
    }

    /// Returns the preference record for `peer`.
    pub(crate) fn destination(&self, peer: PeerId) -> Arc<Destination> {
        match self
            .destinations
            .get_or_insert_with(&peer, || Ok::<_, Infallible>(Arc::default()))
        {
            Ok(destination) => destination,
            Err(never) => match never {},
        }
    }

    /// Returns the delivery policy.
    pub(crate) const fn config(&self) -> FleetConfiguration {
        self.config
    }
}
