//! User-facing configuration for the always-on keyed-state layer.

use super::registry::{CollectionDef, CollectionDefRegistry, RegisterStateError};
use crate::state::descriptor::{DescriptorIdentity, StructuralIdentity};
use crate::timers::duration::CompactDuration;
use crate::util::from_env_with_fallback;
use derive_builder::Builder;
use std::path::{Path, PathBuf};
use std::{env, process};
use validator::{Validate, ValidationError};

/// Environment variable for the local fjall workspace directory.
const FJALL_CACHE_DIR_ENV: &str = "PROSODY_FJALL_CACHE_DIR";

/// Default delay between sealing and the `StateRecovery` sweep.
const DEFAULT_RECOVERY_DELAY_SECS: u32 = 30;

/// Configuration for the pipeline consumer's keyed-state layer.
///
/// The layer is always present; with no registered descriptors it is inert
/// (no durable identity I/O, no seals). Register collections with
/// [`Self::state`]:
///
/// ```
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use prosody::consumer::KeyedStateConfiguration;
/// use prosody::state::descriptor::{ValueDescriptor, value_state};
/// use prosody::state::registry::CollectionDef;
///
/// const CART: ValueDescriptor = value_state("cart");
///
/// let keyed_state = KeyedStateConfiguration::builder()
///     .build()?
///     .state(&CART, CollectionDef::new(None));
/// # Ok(())
/// # }
/// ```
#[derive(Builder, Clone, Debug, Validate)]
pub struct KeyedStateConfiguration {
    /// Root directory for the local fjall workspace (committed-value cache
    /// + dirty overlays).
    ///
    /// Production deployments mount this (e.g. a Kubernetes `emptyDir`) and
    /// **must** set it — the cache is wiped on process restart, so the mount
    /// needs no persistence. Defaults to a per-process temporary directory
    /// so unconfigured consumers (and consumers that never register state)
    /// work out of the box.
    ///
    /// Environment variable: `PROSODY_FJALL_CACHE_DIR`
    #[builder(default = "from_env_with_fallback(FJALL_CACHE_DIR_ENV, default_cache_dir())?")]
    #[validate(custom(function = "validate_cache_dir"))]
    pub cache_dir: PathBuf,

    /// Middleware-wide default TTL for collections whose
    /// [`CollectionDef`] does not override it. `None` means indefinite
    /// retention.
    #[builder(default)]
    pub default_ttl: Option<CompactDuration>,

    /// Delay between sealing a WAL and the `StateRecovery` backstop sweep.
    #[builder(default = "CompactDuration::new(DEFAULT_RECOVERY_DELAY_SECS)")]
    pub recovery_delay: CompactDuration,

    #[builder(setter(skip), default)]
    registrations: Vec<(&'static str, StructuralIdentity, CollectionDef)>,
}

impl Default for KeyedStateConfiguration {
    fn default() -> Self {
        Self {
            cache_dir: from_env_with_fallback(FJALL_CACHE_DIR_ENV, default_cache_dir())
                .unwrap_or_else(|_| default_cache_dir()),
            default_ttl: None,
            recovery_delay: CompactDuration::new(DEFAULT_RECOVERY_DELAY_SECS),
            registrations: Vec::new(),
        }
    }
}

impl KeyedStateConfiguration {
    /// Creates a new builder.
    #[must_use]
    pub fn builder() -> KeyedStateConfigurationBuilder {
        KeyedStateConfigurationBuilder::default()
    }

    /// Registers `descriptor`'s collection with operational settings `def`.
    ///
    /// Name validation, TTL-ceiling rejection, and identity-conflict
    /// rejection all run at consumer build, the fallible boundary.
    #[must_use]
    pub fn state<DESC>(mut self, descriptor: &DESC, def: CollectionDef) -> Self
    where
        DESC: DescriptorIdentity,
    {
        self.registrations
            .push((descriptor.name(), descriptor.structural_identity(), def));
        self
    }

    /// Returns whether any collections are registered.
    #[must_use]
    pub fn has_registrations(&self) -> bool {
        !self.registrations.is_empty()
    }

    /// Builds the collection registry from the registrations and
    /// `default_ttl`.
    ///
    /// # Errors
    ///
    /// Returns [`RegisterStateError`] on an empty descriptor name, a TTL
    /// over Cassandra's `USING TTL` ceiling, or an identity conflict.
    pub(crate) fn build_registry(&self) -> Result<CollectionDefRegistry, RegisterStateError> {
        let mut registry = CollectionDefRegistry::new(self.default_ttl);
        for (name, identity, def) in &self.registrations {
            registry.register_identity(name, identity.clone(), def.clone())?;
        }
        Ok(registry)
    }
}

/// Per-process fallback fjall workspace, used when [`FJALL_CACHE_DIR_ENV`] is
/// unset. Wiped on restart, so it needs no persistence.
fn default_cache_dir() -> PathBuf {
    env::temp_dir().join(format!("prosody-keyed-state-{}", process::id()))
}

fn validate_cache_dir(cache_dir: &Path) -> Result<(), ValidationError> {
    if cache_dir.as_os_str().is_empty() {
        return Err(ValidationError::new("cache_dir_empty"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::KeyedStateConfiguration;
    use crate::cassandra::MAX_CASSANDRA_TTL_SECS;
    use crate::state::descriptor::{ValueDescriptor, value_state};
    use crate::state::registry::{CollectionDef, RegisterStateError};
    use crate::timers::duration::CompactDuration;
    use color_eyre::eyre::Result;
    use std::path::PathBuf;
    use validator::Validate;

    const CART: ValueDescriptor = value_state("cart");

    /// `MAX_CASSANDRA_TTL_SECS` fits a `u32`, so the ceiling and one second
    /// past it are both representable as a `CompactDuration`.
    const CEILING_SECS: u32 = MAX_CASSANDRA_TTL_SECS as u32;

    #[test]
    fn empty_cache_dir_is_rejected() -> Result<()> {
        let config = KeyedStateConfiguration::builder()
            .cache_dir(PathBuf::new())
            .build()?;
        assert!(
            config.validate().is_err(),
            "empty cache_dir must fail validation"
        );
        Ok(())
    }

    /// Indefinite retention (`None`) is always allowed — the TTL ceiling
    /// guards only oversized `Some` values, never the opt-out.
    #[test]
    fn collection_ttl_none_is_allowed() -> Result<()> {
        let config = KeyedStateConfiguration::builder()
            .build()?
            .state(&CART, CollectionDef::new(None));
        assert!(config.build_registry().is_ok());
        Ok(())
    }

    #[test]
    fn collection_ttl_at_the_ceiling_is_allowed() -> Result<()> {
        let ttl = CompactDuration::new(CEILING_SECS);
        let config = KeyedStateConfiguration::builder()
            .build()?
            .state(&CART, CollectionDef::new(Some(ttl)));
        assert!(config.build_registry().is_ok());
        Ok(())
    }

    #[test]
    fn collection_ttl_over_the_ceiling_is_rejected() -> Result<()> {
        let over = CEILING_SECS + 1;
        let config = KeyedStateConfiguration::builder()
            .build()?
            .state(&CART, CollectionDef::new(Some(CompactDuration::new(over))));
        assert!(matches!(
            config.build_registry(),
            Err(RegisterStateError::Ttl { seconds, .. }) if seconds == over
        ));
        Ok(())
    }
}
